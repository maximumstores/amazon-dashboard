# weather_tab.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


@st.cache_data(ttl=1800)
def _load_weather_map(_engine):
    """Агрегат заказов по штату + текущая погода + алерты."""
    sql = """
    WITH sales AS (
        SELECT state_code,
               SUM(quantity)        AS units_30d,
               COUNT(*)             AS orders_30d
        FROM public.sales_weather
        WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY state_code
    ),
    today AS (
        SELECT DISTINCT ON (state_code)
               state_code, state_name,
               temperature_2m_max_f, precipitation_sum_mm,
               thunderstorm, flood_risk, weather_emoji, weather_desc
        FROM weather.weather_all
        WHERE date = CURRENT_DATE
        ORDER BY state_code, loaded_at DESC
    )
    SELECT t.state_code, t.state_name,
           t.temperature_2m_max_f, t.precipitation_sum_mm,
           t.thunderstorm, t.flood_risk, t.weather_emoji, t.weather_desc,
           COALESCE(s.units_30d, 0)  AS units_30d,
           COALESCE(s.orders_30d, 0) AS orders_30d
    FROM today t
    LEFT JOIN sales s ON s.state_code = t.state_code
    ORDER BY units_30d DESC;
    """
    return pd.read_sql(sql, _engine)


@st.cache_data(ttl=1800)
def _load_active_alerts(_engine):
    sql = """
    SELECT state_code, event, severity, headline, area_desc, expires
    FROM weather.weather_all w
    WHERE FALSE
    """  # заглушка — алерты лежат в Google Sheets, не в Postgres
    return pd.DataFrame()


def show_weather_tab(engine):
    st.header("🌦️ Погода × Продажи (мониторинг)")
    st.caption(
        "Контекстный мониторинг: где сейчас экстремальная погода и как "
        "распределены продажи. Не прогноз — погодный эффект на дневной спрос "
        "слабый (детренд-корреляция ≈ −0.04)."
    )

    df = _load_weather_map(engine)
    if df.empty:
        st.warning("Нет данных о погоде на сегодня. Проверь, отработал ли loader #15.")
        return

    # --- метрики сверху
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Штатов с продажами (30д)", int((df["units_30d"] > 0).sum()))
    c2.metric("Units всего (30д)", f"{int(df['units_30d'].sum()):,}")
    c3.metric("⛈️ Гроза сейчас", int(df["thunderstorm"].sum()))
    c4.metric("🌊 Flood-риск сейчас", int(df["flood_risk"].sum()))

    # --- КАРТА: цвет = температура, текст = эмодзи погоды
    st.subheader("Карта: температура + продажи по штатам")
    fig = px.choropleth(
        df,
        locations="state_code",
        locationmode="USA-states",
        color="temperature_2m_max_f",
        scope="usa",
        color_continuous_scale="RdYlBu_r",
        hover_name="state_name",
        hover_data={
            "temperature_2m_max_f": ":.0f",
            "units_30d": ":,",
            "weather_desc": True,
            "state_code": False,
        },
        labels={"temperature_2m_max_f": "Макс °F", "units_30d": "Units 30д"},
    )
    fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=500)
    st.plotly_chart(fig, use_container_width=True)

    # --- КАРТА 2: размер пузыря = продажи (где покупают)
    st.subheader("Где покупают (продажи 30д по штатам)")
    fig2 = px.choropleth(
        df,
        locations="state_code",
        locationmode="USA-states",
        color="units_30d",
        scope="usa",
        color_continuous_scale="Greens",
        hover_name="state_name",
        hover_data={"units_30d": ":,", "orders_30d": ":,"},
        labels={"units_30d": "Units 30д"},
    )
    fig2.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=500)
    st.plotly_chart(fig2, use_container_width=True)

    # --- таблица экстремальной погоды там, где есть продажи
    st.subheader("⚠️ Экстремальная погода в штатах с продажами")
    extreme = df[
        (df["thunderstorm"] | df["flood_risk"]
         | (df["temperature_2m_max_f"] > 95)
         | (df["temperature_2m_max_f"] < 32))
        & (df["units_30d"] > 0)
    ].copy()
    if extreme.empty:
        st.success("Сейчас нет экстремальной погоды в штатах с активными продажами.")
    else:
        extreme["статус"] = extreme["weather_emoji"] + " " + extreme["weather_desc"].fillna("")
        st.dataframe(
            extreme[["state_name", "статус", "temperature_2m_max_f",
                     "precipitation_sum_mm", "units_30d", "orders_30d"]]
            .rename(columns={
                "state_name": "Штат", "temperature_2m_max_f": "Макс °F",
                "precipitation_sum_mm": "Осадки мм",
                "units_30d": "Units 30д", "orders_30d": "Заказы 30д",
            }),
            use_container_width=True, hide_index=True,
        )
        st.caption(
            "💡 Для логистики: при flood/гроза возможны задержки FBA-доставки "
            "и рост возвратов в этих штатах."
        )
