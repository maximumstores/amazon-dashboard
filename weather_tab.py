# weather_tab.py — вкладка "Погода × Продажи" для dashboard.py (MERINO BI)
#
# Контекстный МОНИТОРИНГ (не прогноз): погода + продажи по штатам.
# Погодный эффект на дневной спрос слабый (детренд-корреляция ≈ −0.04).
#
# Источники (Postgres):
#   weather.weather_all   — погода по штатам и датам (history+forecast)
#   public.sales_weather  — VIEW: заказ -> ZIP -> штат -> погода
# Население штатов: Census Bureau API (с fallback на зашитые данные).
#
# Подключается из dashboard.py:
#   from weather_tab import show_weather_tab
#   show_weather_tab(get_engine())

import os
import streamlit as st
import pandas as pd
import plotly.express as px

try:
    import requests
except ImportError:
    requests = None


# ============================================================
# НАСЕЛЕНИЕ ШТАТОВ — fallback (Census 2023 est.)
# ============================================================
_POP_FALLBACK = {
    "CA": 38965193, "TX": 30503301, "FL": 22610726, "NY": 19571216,
    "PA": 12961683, "IL": 12549689, "OH": 11785935, "GA": 11029227,
    "NC": 10835491, "MI": 10037261, "NJ": 9290841,  "VA": 8715698,
    "WA": 7812880,  "AZ": 7431344,  "TN": 7126489,  "MA": 7001399,
    "IN": 6862199,  "MO": 6196156,  "MD": 6180253,  "WI": 5910955,
    "CO": 5877610,  "MN": 5737915,  "SC": 5373555,  "AL": 5108468,
    "LA": 4573749,  "KY": 4526154,  "OR": 4233358,  "OK": 4053824,
    "CT": 3617176,  "UT": 3417734,  "IA": 3207004,  "NV": 3194176,
    "AR": 3067732,  "MS": 2939690,  "KS": 2940546,  "NM": 2114371,
    "NE": 1978379,  "ID": 1964726,  "WV": 1770071,  "HI": 1435138,
    "NH": 1402054,  "ME": 1395722,  "MT": 1132812,  "RI": 1095962,
    "DE": 1031890,  "SD": 919318,   "ND": 783926,   "AK": 733406,
    "VT": 647464,   "WY": 584057,
}


# ============================================================
# DATA LOADERS
# ============================================================

@st.cache_data(ttl=86400)
def _load_population():
    """Население штатов: Census API -> fallback на зашитые данные."""
    key = os.environ.get("CENSUS_API_KEY", "")
    if key and requests is not None:
        try:
            r = requests.get(
                "https://api.census.gov/data/2023/acs/acs1",
                params={"get": "NAME,B01003_001E", "for": "state:*", "key": key},
                timeout=20,
            )
            if r.status_code == 200:
                data = r.json()
                name_to_code = {
                    "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR",
                    "California":"CA","Colorado":"CO","Connecticut":"CT","Delaware":"DE",
                    "Florida":"FL","Georgia":"GA","Hawaii":"HI","Idaho":"ID",
                    "Illinois":"IL","Indiana":"IN","Iowa":"IA","Kansas":"KS",
                    "Kentucky":"KY","Louisiana":"LA","Maine":"ME","Maryland":"MD",
                    "Massachusetts":"MA","Michigan":"MI","Minnesota":"MN",
                    "Mississippi":"MS","Missouri":"MO","Montana":"MT","Nebraska":"NE",
                    "Nevada":"NV","New Hampshire":"NH","New Jersey":"NJ",
                    "New Mexico":"NM","New York":"NY","North Carolina":"NC",
                    "North Dakota":"ND","Ohio":"OH","Oklahoma":"OK","Oregon":"OR",
                    "Pennsylvania":"PA","Rhode Island":"RI","South Carolina":"SC",
                    "South Dakota":"SD","Tennessee":"TN","Texas":"TX","Utah":"UT",
                    "Vermont":"VT","Virginia":"VA","Washington":"WA",
                    "West Virginia":"WV","Wisconsin":"WI","Wyoming":"WY",
                }
                pop = {}
                for row in data[1:]:
                    name, value = row[0], row[1]
                    code = name_to_code.get(name)
                    if code and value is not None:
                        pop[code] = int(value)
                if len(pop) >= 50:
                    return pop, "Census API 2023"
        except Exception:
            pass
    return dict(_POP_FALLBACK), "fallback (Census 2023 est.)"


@st.cache_data(ttl=1800)
def _load_weather_today(_engine):
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

    unit = st.radio("Единицы температуры", ["°F", "°C"],
                    horizontal=True, index=1, key="weather_unit")
    is_c = unit == "°C"
    temp_col = "temperature_2m_max_c" if is_c else "temperature_2m_max_f"
    unit_label = "°C" if is_c else "°F"

    today = _load_weather_today(engine)
    if today.empty:
        st.warning("Нет данных о погоде на сегодня в weather.weather_all. "
                   "Проверь, отработал ли loader #15 (Weather) сегодня.")
        return

    sales = _load_sales_by_state(engine, days=30)
    df = today.merge(sales, on="state_code", how="left")
    df["units"] = df["units"].fillna(0)
    df["orders"] = df["orders"].fillna(0)

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

    # ---------- карта температуры (на всю ширину) ----------
    st.subheader(f"🌡️ Температура сегодня ({unit_label})")
    st.caption(
        "Что показывает: макс. температуру в каждом штате сегодня. "
        "Синее — холодно, красное — жарко. Наведи на штат для деталей."
    )
    fig_t = px.choropleth(
        df, locations="state_code", locationmode="USA-states",
        color=temp_col, scope="usa", color_continuous_scale="RdYlBu_r",
        hover_name="state_name",
        hover_data={temp_col: ":.0f", "weather_desc": True,
                    "units": ":,", "state_code": False},
        labels={temp_col: f"Макс {unit_label}", "units": "Units 30д"},
    )
    fig_t.update_layout(
        margin=dict(l=0, r=0, t=0, b=0), height=480,
        coloraxis_colorbar=dict(title=unit_label),
        geo=dict(bgcolor="rgba(0,0,0,0)", lakecolor="rgba(0,0,0,0)"),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig_t, use_container_width=True)

    # ---------- карта продаж (на всю ширину) ----------
    st.subheader("📦 Продажи 30д")
    st.caption(
        "Что показывает: сколько units продано в штате за 30 дней. "
        "Темнее зелёный — больше продаж. Крупные штаты (CA/TX/FL) "
        "ожидаемо лидируют — это абсолютные числа, без поправки на население."
    )
    fig_s = px.choropleth(
        df, locations="state_code", locationmode="USA-states",
        color="units", scope="usa", color_continuous_scale="Greens",
        hover_name="state_name",
        hover_data={"units": ":,", "orders": ":,", "state_code": False},
        labels={"units": "Units 30д"},
    )
    fig_s.update_layout(
        margin=dict(l=0, r=0, t=0, b=0), height=480,
        coloraxis_colorbar=dict(title="Units"),
        geo=dict(bgcolor="rgba(0,0,0,0)", lakecolor="rgba(0,0,0,0)"),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig_s, use_container_width=True)

    # ============================================================
    # PENETRATION INDEX
    # ============================================================
    st.divider()
    st.subheader("🎯 Penetration Index — где недопроникновение")
    st.markdown(
        "**Зачем это:** карта «Продажи 30д» выше всегда подсвечивает крупные "
        "штаты — там просто больше людей. Penetration Index убирает этот эффект "
        "и показывает, где ты продаёшь **сильнее или слабее, чем размер рынка**."
    )

    pop_map, pop_src = _load_population()
    pen = df[["state_code", "state_name", "units", "orders"]].copy()
    pen["population"] = pen["state_code"].map(pop_map)
    pen = pen.dropna(subset=["population"])

    total_units = pen["units"].sum()
    total_pop = pen["population"].sum()
    if total_units > 0 and total_pop > 0:
        pen["sales_share"] = pen["units"] / total_units
        pen["pop_share"] = pen["population"] / total_pop
        pen["index"] = (pen["sales_share"] / pen["pop_share"]).round(2)

        st.caption(
            f"Как считается: индекс = (доля твоих продаж в штате) / (доля "
            f"населения штата в США). Например, штат даёт 4% продаж, а это 2% "
            f"населения → индекс 2.0 (вдвое сильнее рынка). Население: {pop_src}."
        )

        fig_p = px.choropleth(
            pen, locations="state_code", locationmode="USA-states",
            color="index", scope="usa",
            color_continuous_scale="RdBu", range_color=[0, 2],
            color_continuous_midpoint=1.0,
            hover_name="state_name",
            hover_data={"index": ":.2f", "units": ":,",
                        "population": ":,", "state_code": False},
            labels={"index": "Penetration"},
        )
        fig_p.update_layout(
            margin=dict(l=0, r=0, t=0, b=0), height=520,
            coloraxis_colorbar=dict(title="Index"),
            geo=dict(bgcolor="rgba(0,0,0,0)", lakecolor="rgba(0,0,0,0)"),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_p, use_container_width=True)
        st.caption("🔵 синий = сильнее рынка (index>1) · 🔴 красный = недобор (index<1)")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**🔴 Топ недопроникновение (потенциал роста)**")
            st.caption(
                "Штаты, где продаёшь МЕНЬШЕ, чем позволяет их население "
                "(индекс < 1). Сюда можно расти — но учти: для merino юг "
                "(MS, AR, LA) структурно слабее из-за тёплого климата, "
                "это не всегда «упущенная выгода»."
            )
            under = pen[pen["units"] > 0].nsmallest(10, "index")[
                ["state_name", "index", "units", "population"]
            ].rename(columns={"state_name": "Штат", "index": "Индекс",
                              "units": "Units 30д", "population": "Население"})
            st.dataframe(under, use_container_width=True, hide_index=True)
        with col2:
            st.markdown("**🔵 Топ перепроникновение (сильнее рынка)**")
            st.caption(
                "Штаты, где продаёшь БОЛЬШЕ, чем их доля населения "
                "(индекс > 1). Твоя сильная база — обычно холодные/аутдор "
                "штаты (AK, CO, WA, OR), где merino востребован. "
                "Малые штаты тут могут давать завышенный индекс на малых объёмах."
            )
            over = pen.nlargest(10, "index")[
                ["state_name", "index", "units", "population"]
            ].rename(columns={"state_name": "Штат", "index": "Индекс",
                              "units": "Units 30д", "population": "Население"})
            st.dataframe(over, use_container_width=True, hide_index=True)

    # ---------- экстремальная погода ----------
    st.divider()
    st.subheader("⚠️ Экстремальная погода в штатах с продажами")
    st.caption(
        "Что показывает: штаты, где сейчас одновременно (а) экстрим — гроза, "
        "flood-риск, жара или мороз — И (б) есть твои продажи. Сигнал для "
        "логистики: в этих штатах возможны задержки FBA-доставки и рост возвратов."
    )
    mask = (
        (df["thunderstorm"].fillna(False) | df["flood_risk"].fillna(False)
         | (df[temp_col] > hot_threshold) | (df[temp_col] < cold_threshold))
        & (df["units"] > 0)
    )
    extreme = df[mask].copy()
    if extreme.empty:
        st.success("Сейчас нет экстремальной погоды в штатах с активными продажами.")
    else:
        extreme = extreme.sort_values("units", ascending=False)
        extreme["статус"] = (extreme["weather_emoji"].fillna("") + " "
                             + extreme["weather_desc"].fillna(""))
        show = extreme[["state_name", "статус", temp_col,
                        "precipitation_sum_mm", "units", "orders"]].rename(columns={
            "state_name": "Штат", temp_col: f"Макс {unit_label}",
            "precipitation_sum_mm": "Осадки мм",
            "units": "Units 30д", "orders": "Заказы 30д"})
        st.dataframe(show, use_container_width=True, hide_index=True)

    # ---------- прогноз ----------
    with st.expander(f"🔮 Прогноз 16 дней — температура ({unit_label}) в топ-штатах"):
        st.caption(
            "Что показывает: прогноз макс. температуры на 16 дней вперёд по "
            "топ-8 штатам твоих продаж. Резкое падение линии = приближается "
            "похолодание (повод заранее проверить сток тёплых SKU)."
        )
        fc = _load_forecast(engine, days=16)
        if fc.empty:
            st.info("Прогнозных строк нет (forecast в weather_all пуст).")
        else:
            fc_temp_col = "temperature_2m_max_c" if is_c else "temperature_2m_max_f"
            top_states = df.sort_values("units", ascending=False).head(8)["state_code"].tolist()
            fc_top = fc[fc["state_code"].isin(top_states)].copy()
            if fc_top.empty:
                st.info("Нет прогноза по топ-штатам продаж.")
            else:
                pivot = fc_top.pivot_table(index="date", columns="state_code",
                                           values=fc_temp_col, aggfunc="first")
                st.line_chart(pivot)

    st.divider()
    st.caption("Данные обновляются ежедневно (loader #15). Кэш страницы — 30 мин.")
