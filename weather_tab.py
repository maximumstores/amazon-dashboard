# weather_tab.py — вкладка "Погода × Продажи" для dashboard.py (MERINO BI)
#
# Контекстный МОНИТОРИНГ (не прогноз): где сейчас экстремальная погода
# и как распределены продажи по штатам. Погодный эффект на дневной спрос
# слабый (детренд-корреляция ≈ −0.04), поэтому это наблюдательный инструмент.
#
# Источники (Postgres):
#   weather.weather_all   — погода по штатам и датам (history+forecast)
#   public.sales_weather  — VIEW: заказ -> ZIP -> штат -> погода
#
# Подключается из dashboard.py:
#   from weather_tab import show_weather_tab
#   show_weather_tab(get_engine())

import streamlit as st
import pandas as pd
import plotly.express as px


# ============================================================
# DATA LOADERS (кэш 30 мин)
# ============================================================

@st.cache_data(ttl=1800)
def _load_weather_today(_engine):
    """Текущая погода по штатам (на сегодня; берём свежайшую загрузку)."""
    sql = """
        SELECT DISTINCT ON (state_code)
               state_code, state_name,
               temperature_2m_max_f, temperature_2m_max_c,
               temperature_2m_min_f, temperature_2m_min_c,
               precipitation_sum_mm,
               thunderstorm, flood_risk, weather_emoji, weather_desc, kind
        FROM weather.weather_all
        WHERE date = CURRENT_DATE
        ORDER BY state_code, loaded_at DESC
    """
    return pd.read_sql(sql, _engine)


@st.cache_data(ttl=1800)
def _load_sales_by_state(_engine, days=30):
    """Агрегат продаж по штатам за последние N дней."""
    sql = f"""
        SELECT state_code,
               SUM(quantity)  AS units,
               COUNT(*)       AS orders
        FROM public.sales_weather
        WHERE order_date >= CURRENT_DATE - INTERVAL '{int(days)} days'
          AND quantity IS NOT NULL
        GROUP BY state_code
    """
    return pd.read_sql(sql, _engine)


@st.cache_data(ttl=1800)
def _load_forecast(_engine, days=16):
    """Прогноз вперёд по штатам."""
    sql = f"""
        SELECT state_code, state_name, date,
               temperature_2m_max_f, temperature_2m_max_c,
               precipitation_sum_mm,
               thunderstorm, flood_risk, weather_emoji, weather_desc
        FROM weather.weather_all
        WHERE kind = 'forecast'
          AND date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '{int(days)} days'
        ORDER BY state_code, date
    """
    return pd.read_sql(sql, _engine)


# ============================================================
# MAIN TAB
# ============================================================

def show_weather_tab(engine):
    st.header("🌦️ Погода × Продажи (мониторинг)")
    st.caption(
        "Контекстный мониторинг: где сейчас экстремальная погода и как "
        "распределены продажи по штатам. Это НЕ прогноз спроса — погодный "
        "эффект на дневные продажи слабый (детренд-корреляция ≈ −0.04). "
        "Полезно как контекст и для логистики (задержки доставки при flood/гроза)."
    )

    # --- переключатель единиц температуры ---
    unit = st.radio(
        "Единицы температуры",
        ["°F", "°C"],
        horizontal=True,
        index=1,  # по умолчанию °C
        key="weather_unit",
    )
    is_c = unit == "°C"
    temp_col = "temperature_2m_max_c" if is_c else "temperature_2m_max_f"
    temp_min_col = "temperature_2m_min_c" if is_c else "temperature_2m_min_f"
    unit_label = "°C" if is_c else "°F"

    today = _load_weather_today(engine)
    if today.empty:
        st.warning(
            "Нет данных о погоде на сегодня в weather.weather_all. "
            "Проверь, отработал ли loader #15 (Weather) сегодня."
        )
        return

    sales = _load_sales_by_state(engine, days=30)
    df = today.merge(sales, on="state_code", how="left")
    df["units"] = df["units"].fillna(0)
    df["orders"] = df["orders"].fillna(0)

    # пороги экстрима в выбранных единицах (95°F≈35°C жара, 32°F=0°C мороз)
    hot_threshold = 35.0 if is_c else 95.0
    cold_threshold = 0.0 if is_c else 32.0

    # ---------- метрики ----------
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Штатов с продажами (30д)", int((df["units"] > 0).sum()))
    c2.metric("Units всего (30д)", f"{int(df['units'].sum()):,}")
    c3.metric("⛈️ Гроза сейчас", int(df["thunderstorm"].fillna(False).sum()))
    c4.metric("🌊 Flood-риск сейчас", int(df["flood_risk"].fillna(False).sum()))

    src_kind = df["kind"].mode().iat[0] if not df["kind"].dropna().empty else "?"
    st.caption(f"Погода на сегодня (источник: {src_kind}). Единицы: {unit_label}.")

    # ---------- две карты бок о бок ----------
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader(f"🌡️ Температура сегодня ({unit_label})")
        fig_t = px.choropleth(
            df, locations="state_code", locationmode="USA-states",
            color=temp_col, scope="usa",
            color_continuous_scale="RdYlBu_r",
            hover_name="state_name",
            hover_data={
                temp_col: ":.0f",
                "weather_desc": True,
                "units": ":,",
                "state_code": False,
            },
            labels={temp_col: f"Макс {unit_label}", "units": "Units 30д"},
        )
        fig_t.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=380,
                            coloraxis_colorbar=dict(title=unit_label))
        st.plotly_chart(fig_t, use_container_width=True)

    with col_r:
        st.subheader("📦 Продажи 30д")
        fig_s = px.choropleth(
            df, locations="state_code", locationmode="USA-states",
            color="units", scope="usa",
            color_continuous_scale="Greens",
            hover_name="state_name",
            hover_data={"units": ":,", "orders": ":,", "state_code": False},
            labels={"units": "Units 30д"},
        )
        fig_s.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=380,
                            coloraxis_colorbar=dict(title="Units"))
        st.plotly_chart(fig_s, use_container_width=True)

    # ---------- экстремальная погода там, где есть продажи ----------
    st.subheader("⚠️ Экстремальная погода в штатах с продажами")
    mask = (
        (df["thunderstorm"].fillna(False)
         | df["flood_risk"].fillna(False)
         | (df[temp_col] > hot_threshold)
         | (df[temp_col] < cold_threshold))
        & (df["units"] > 0)
    )
    extreme = df[mask].copy()
    if extreme.empty:
        st.success("Сейчас нет экстремальной погоды в штатах с активными продажами.")
    else:
        extreme = extreme.sort_values("units", ascending=False)
        extreme["статус"] = (
            extreme["weather_emoji"].fillna("") + " "
            + extreme["weather_desc"].fillna("")
        )
        show = extreme[[
            "state_name", "статус", temp_col,
            "precipitation_sum_mm", "units", "orders",
        ]].rename(columns={
            "state_name": "Штат", temp_col: f"Макс {unit_label}",
            "precipitation_sum_mm": "Осадки мм",
            "units": "Units 30д", "orders": "Заказы 30д",
        })
        st.dataframe(show, use_container_width=True, hide_index=True)
        st.caption(
            "💡 Логистика: при flood/гроза в этих штатах возможны задержки "
            "FBA-доставки и рост возвратов — учитывай в Tender-планировании."
        )

    # ---------- прогноз: ближайшие похолодания (топ по продажам) ----------
    with st.expander(f"🔮 Прогноз 16 дней — температура ({unit_label}) в топ-штатах"):
        fc = _load_forecast(engine, days=16)
        if fc.empty:
            st.info("Прогнозных строк нет (forecast в weather_all пуст).")
        else:
            fc_temp_col = "temperature_2m_max_c" if is_c else "temperature_2m_max_f"
            top_states = (
                df.sort_values("units", ascending=False)
                .head(8)["state_code"].tolist()
            )
            fc_top = fc[fc["state_code"].isin(top_states)].copy()
            if fc_top.empty:
                st.info("Нет прогноза по топ-штатам продаж.")
            else:
                pivot = fc_top.pivot_table(
                    index="date", columns="state_code",
                    values=fc_temp_col, aggfunc="first",
                )
                st.line_chart(pivot)
                st.caption(
                    f"Макс. температура ({unit_label}) на 16 дней вперёд по топ-8 "
                    "штатам продаж. Резкое падение линии = похолодание."
                )

    st.divider()
    st.caption(
        "Данные обновляются ежедневно (loader #15). Кэш страницы — 30 мин. "
        "Корреляционный анализ погода↔продажи: см. analyze_weather_detrend.py."
    )
