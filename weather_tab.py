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

# Google Sheets, куда пишет loader #15 (тот же ID, что в 15_weather_loader.py).
# Можно переопределить через env SPREADSHEET_ID.
WEATHER_SHEET_ID = os.environ.get(
    "SPREADSHEET_ID", "1sVL1U8cixv8BSyQVtf1GNnf3kvSEGUAjbYyHVS3DeuM"
)
WEATHER_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{WEATHER_SHEET_ID}"


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

def show_weather_tab(engine, ai_fn=None):
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
    st.plotly_chart(fig_t, use_container_width=True, key="weather_map_temp")

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
    st.plotly_chart(fig_s, use_container_width=True, key="weather_map_sales")

    # ============================================================
    # PENETRATION INDEX
    # ============================================================
    st.divider()
    st.subheader("🎯 Где продаём лучше или хуже рынка")
    st.markdown(
        "**Зачем это:** карта «Продажи 30д» выше всегда подсвечивает крупные "
        "штаты — там просто больше людей. Здесь мы делим продажи на население "
        "и видим, где ты продаёшь **сильнее или слабее, чем людей там живёт**."
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
            f"Как считается: делим долю твоих продаж в штате на долю его "
            f"населения. Пример: штат даёт 4% продаж, а в нём живёт 2% людей "
            f"страны → показатель 2.0 (продаёшь вдвое сильнее, чем людей). "
            f"Население: {pop_src}."
        )

        fig_p = px.choropleth(
            pen, locations="state_code", locationmode="USA-states",
            color="index", scope="usa",
            color_continuous_scale="RdBu", range_color=[0, 2],
            color_continuous_midpoint=1.0,
            hover_name="state_name",
            hover_data={"index": ":.2f", "units": ":,",
                        "population": ":,", "state_code": False},
            labels={"index": "Продажи/Люди"},
        )
        fig_p.update_layout(
            margin=dict(l=0, r=0, t=0, b=0), height=520,
            coloraxis_colorbar=dict(title="Показ."),
            geo=dict(bgcolor="rgba(0,0,0,0)", lakecolor="rgba(0,0,0,0)"),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_p, use_container_width=True, key="weather_map_penetration")
        st.caption("🔵 синий = продаём МНОГО (больше, чем людей) · 🔴 красный = продаём МАЛО (меньше, чем людей)")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**🔴 Продаём мало — куда можно расти**")
            st.caption(
                "Штаты, где продаёшь МЕНЬШЕ, чем там людей живёт. Сюда можно "
                "расти — но учти: для merino тёплый юг (MS, AR, LA) слабее "
                "из-за климата, это не всегда упущенная выгода."
            )
            under = pen[pen["units"] > 0].nsmallest(10, "index")[
                ["state_name", "index", "units", "population"]
            ].rename(columns={"state_name": "Штат", "index": "Показатель",
                              "units": "Units 30д", "population": "Население"})
            st.dataframe(under, use_container_width=True, hide_index=True, key="weather_tbl_under")
        with col2:
            st.markdown("**🔵 Продаём много — наша сильная база**")
            st.caption(
                "Штаты, где продаёшь БОЛЬШЕ, чем там людей живёт. Твоя сильная "
                "база — обычно холодные/аутдор штаты (AK, CO, WA, OR), где "
                "merino нужен. Маленькие штаты могут давать большой показатель "
                "на малых числах — не обманись."
            )
            over = pen.nlargest(10, "index")[
                ["state_name", "index", "units", "population"]
            ].rename(columns={"state_name": "Штат", "index": "Показатель",
                              "units": "Units 30д", "population": "Население"})
            st.dataframe(over, use_container_width=True, hide_index=True, key="weather_tbl_over")

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
        extreme["units"] = extreme["units"].fillna(0).astype(int)
        extreme["orders"] = extreme["orders"].fillna(0).astype(int)
        show = extreme[["state_name", "статус", temp_col,
                        "precipitation_sum_mm", "units", "orders"]].rename(columns={
            "state_name": "Штат", temp_col: f"Макс {unit_label}",
            "precipitation_sum_mm": "Осадки мм",
            "units": "Units 30д", "orders": "Заказы 30д"})
        st.dataframe(show, use_container_width=True, hide_index=True, key="weather_tbl_extreme")

    # ---------- прогноз: КАРТА со слайдером дней ----------
    st.divider()
    st.subheader(f"🔮 Прогноз температуры ({unit_label}) — карта по дням")
    st.caption(
        "Что показывает: прогноз макс. температуры по всем штатам на выбранный "
        "день. Двигай слайдер вперёд — видно, где и когда ожидается похолодание "
        "(синие штаты). Повод заранее проверить сток тёплых SKU в этих регионах."
    )
    fc = _load_forecast(engine, days=16)
    if fc.empty:
        st.info("Прогнозных строк нет (forecast в weather_all пуст).")
    else:
        fc_temp_col = "temperature_2m_max_c" if is_c else "temperature_2m_max_f"
        fc = fc.copy()
        fc["date"] = pd.to_datetime(fc["date"])
        days_avail = sorted(fc["date"].dt.date.unique())

        if len(days_avail) == 0:
            st.info("Нет дат в прогнозе.")
        else:
            # слайдер выбора дня прогноза
            sel_day = st.select_slider(
                "День прогноза",
                options=days_avail,
                value=days_avail[0],
                format_func=lambda d: d.strftime("%a %d %b"),
                key="weather_fc_day",
            )
            fc_day = fc[fc["date"].dt.date == sel_day].copy()

            # объединим с продажами, чтобы в hover были units
            fc_day = fc_day.merge(
                df[["state_code", "units"]], on="state_code", how="left"
            )
            fc_day["units"] = fc_day["units"].fillna(0)

            fig_fc = px.choropleth(
                fc_day, locations="state_code", locationmode="USA-states",
                color=fc_temp_col, scope="usa",
                color_continuous_scale="RdYlBu_r",
                hover_name="state_name",
                hover_data={fc_temp_col: ":.0f", "weather_desc": True,
                            "units": ":,", "state_code": False},
                labels={fc_temp_col: f"Макс {unit_label}", "units": "Units 30д"},
            )
            fig_fc.update_layout(
                margin=dict(l=0, r=0, t=0, b=0), height=480,
                coloraxis_colorbar=dict(title=unit_label),
                geo=dict(bgcolor="rgba(0,0,0,0)", lakecolor="rgba(0,0,0,0)"),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_fc, use_container_width=True, key="weather_map_forecast")

            # таблица: прогноз по штатам с продажами на выбранный день
            st.markdown(f"**Прогноз на {sel_day.strftime('%a %d %b')} — штаты с продажами**")
            fc_tbl = fc_day[fc_day["units"] > 0].copy()
            if fc_tbl.empty:
                st.info("Нет штатов с продажами на этот день.")
            else:
                fc_tbl = fc_tbl.sort_values(fc_temp_col)  # от холодных к тёплым
                fc_tbl["статус"] = (fc_tbl["weather_emoji"].fillna("") + " "
                                    + fc_tbl["weather_desc"].fillna(""))
                fc_tbl["units"] = fc_tbl["units"].fillna(0).astype(int)
                fc_show = fc_tbl[["state_name", "статус", fc_temp_col,
                                  "precipitation_sum_mm", "units"]].rename(columns={
                    "state_name": "Штат", fc_temp_col: f"Макс {unit_label}",
                    "precipitation_sum_mm": "Осадки мм", "units": "Units 30д"})
                st.dataframe(fc_show, use_container_width=True, hide_index=True, key="weather_tbl_forecast")
                st.caption(
                    "Отсортировано от холодных к тёплым. Верх таблицы = где "
                    "прохладнее всего в этот день при наличии продаж."
                )

    # ============================================================
    # AI-АНАЛИЗ (через call_ai из dashboard.py, передан как ai_fn)
    # ============================================================
    st.divider()
    st.subheader("🤖 AI-анализ погоды и продаж")
    if ai_fn is None:
        st.caption("AI-анализ недоступен (ai_fn не передан из dashboard.py).")
    else:
        st.caption(
            "Нажми кнопку — AI посмотрит на penetration index, экстрим-погоду "
            "и прогноз, и даст 3-4 конкретных вывода. Учитывает, что погода НЕ "
            "предсказывает дневной спрос (фокус на гео-потенциале и логистике)."
        )
        # выбор провайдера — пишем в тот же session_state, что читает call_ai
        cur_provider = st.session_state.get("ai_provider", "")
        default_idx = 0 if cur_provider.startswith("Claude") else 1
        provider = st.radio(
            "AI-модель", ["Claude", "Gemini"],
            horizontal=True, index=default_idx, key="weather_ai_provider",
            help="Claude лучше следует ограничениям промпта. "
                 "Выбор синхронизируется с вкладкой «AI Дашборд».",
        )
        st.session_state["ai_provider"] = provider
        if st.button("🧠 Сгенерировать AI-анализ", key="weather_ai_btn"):
            # --- собираем компактную сводку для промпта ---
            top_sales = (df.sort_values("units", ascending=False)
                         .head(8)[["state_name", "units"]])
            top_sales_str = "; ".join(
                f"{r['state_name']}: {int(r['units'])} units"
                for _, r in top_sales.iterrows()
            )

            # Тёплые южные штаты исключаем из "потенциала роста" —
            # их низкий индекс структурный (merino там не сезон), не недоработка.
            WARM_SOUTH = {"MS", "AR", "AL", "LA", "OK", "TX", "FL", "GA",
                          "SC", "MS", "TN", "NM", "AZ", "NV", "HI"}
            pen_lines = ""
            try:
                under_pool = pen[(pen["units"] > 0)
                                 & (~pen["state_code"].isin(WARM_SOUTH))]
                under5 = under_pool.nsmallest(5, "index")
                over5 = pen.nlargest(5, "index")
                pen_lines = (
                    "Недопроникновение в ПРОХЛАДНЫХ/умеренных штатах "
                    "(реальный потенциал роста, тёплый юг уже исключён): "
                    + ("; ".join(f"{r['state_name']} idx={r['index']:.2f} "
                                 f"({int(r['units'])} units)"
                                 for _, r in under5.iterrows())
                       or "нет явных кандидатов")
                    + ". Перепроникновение (index>1, сильная база для удержания): "
                    + "; ".join(f"{r['state_name']} idx={r['index']:.2f} "
                                f"({int(r['units'])} units)"
                                for _, r in over5.iterrows())
                )
            except Exception:
                pen_lines = "(penetration index недоступен)"

            extreme_str = "нет"
            if not extreme.empty:
                extreme_str = "; ".join(
                    f"{r['state_name']}: {r['weather_desc']} "
                    f"{r[temp_col]:.0f}{unit_label} ({int(r['units'])} units)"
                    for _, r in extreme.head(8).iterrows()
                )

            prompt = f"""Ты — аналитик Amazon FBA для бренда merino.tech (шерстяная одежда merino, сезонный товар: бельё, носки, штаны, лонгсливы). Анализируй данные "погода × продажи" по штатам США.

ВАЖНЫЙ КОНТЕКСТ: статистический анализ показал, что погода ПОЧТИ НЕ предсказывает дневной спрос (детренд-корреляция ≈ −0.04). Поэтому НЕ давай советов вида "похолодает → закупай больше". Фокусируйся на: (1) географическом потенциале через penetration index, (2) логистических рисках от экстремальной погоды, (3) портрете покупателя.

ДАННЫЕ:
- Топ-8 штатов по продажам (30д): {top_sales_str}
- Penetration index: {pen_lines}
- Экстремальная погода сейчас в штатах с продажами: {extreme_str}
- Всего units за 30д: {int(df['units'].sum())}, штатов с продажами: {int((df['units']>0).sum())}

Дай 3-4 КОНКРЕТНЫХ вывода с действиями. Кратко, по делу, на русском. Каждый вывод — заголовок + 1-2 предложения объяснения."""

            with st.spinner("AI анализирует данные..."):
                result = ai_fn(prompt)
            st.markdown(result)

    # ============================================================
    # ФУТЕР: ссылка на Google Sheets
    # ============================================================
    st.divider()
    col_a, col_b = st.columns([3, 1])
    with col_a:
        st.caption(
            "Данные обновляются ежедневно (loader #15). Кэш страницы — 30 мин. "
            "Те же данные пишутся в Google Sheets (листы weather_history, "
            "weather_forecast, weather_alerts, flood_alerts и др.)."
        )
    with col_b:
        st.link_button("📊 Открыть Google Sheets", WEATHER_SHEET_URL,
                       use_container_width=True)
