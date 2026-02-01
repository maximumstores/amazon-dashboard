import streamlit as st
import pandas as pd
import psycopg2
import os
import plotly.express as px
import plotly.graph_objects as go
import io
from sklearn.linear_model import LinearRegression
import numpy as np
import datetime as dt

st.set_page_config(page_title="Amazon FBA Ultimate BI", layout="wide")

# --- СЛОВНИК ПЕРЕКЛАДІВ ---
translations = {
    "UA": {
        "title": "📦 Amazon FBA: Фінансовий Центр",
        "update_btn": "🔄 Оновити дані",
        "sidebar_title": "🔍 Фільтри",
        "date_label": "📅 Дата:",
        "store_label": "🏪 Магазин:",
        "all_stores": "Всі",
        
        # ВКЛАДКИ
        "tab1": "📊 Головний Дашборд",
        "tab2": "💰 Фінанси (CFO Mode)",
        "tab3": "🐢 Здоров'я складу (Aging)",
        "tab4": "🧠 AI Прогноз",
        "tab5": "📋 Таблиця даних",

        # МЕТРИКИ
        "total_sku": "Всього SKU",
        "total_avail": "Штук на складі",
        "total_value": "💰 Вартість складу (Cost)",
        "potential_rev": "💵 Потенційний виторг",
        "avg_price": "Середня ціна",
        "velocity_30": "Продажів за 30 днів",
        
        # ГРАФІКИ
        "chart_value_treemap": "💰 Де заморожені гроші? (Розмір = Сума $)",
        "chart_velocity": "🚀 Швидкість продажів vs Залишки",
        "chart_age": "⏳ Вік інвентарю (Aging Breakdown)",
        "top_money_sku": "🏆 Топ SKU за вартістю залишків",
        
        # AI
        "ai_header": "🧠 AI Прогноз залишків",
        "ai_select": "Оберіть SKU:",
        "ai_days": "Горизонт прогнозу:",
        "ai_result_date": "📅 Дата Sold-out:",
        "ai_result_days": "Днів залишилось:",
        "ai_ok": "✅ Запасів вистачить",
        
        "col_sku": "SKU",
        "col_name": "Назва",
        "col_avail": "Доступно",
        "col_price": "Ціна",
        "col_value": "Сума ($)",
        "col_velocity": "Продажі (30д)",
        "footer_date": "📅 Дані оновлено:"
    },
    "EN": {
        "title": "📦 Amazon FBA: Financial Hub",
        "update_btn": "🔄 Refresh Data",
        "sidebar_title": "🔍 Filters",
        "date_label": "📅 Date:",
        "store_label": "🏪 Store:",
        "all_stores": "All",
        
        "tab1": "📊 Main Dashboard",
        "tab2": "💰 Finance (CFO Mode)",
        "tab3": "🐢 Inventory Health",
        "tab4": "🧠 AI Forecast",
        "tab5": "📋 Data Table",

        "total_sku": "Total SKU",
        "total_avail": "Total Units",
        "total_value": "💰 Inventory Value",
        "potential_rev": "💵 Potential Revenue",
        "avg_price": "Avg Price",
        "velocity_30": "Sales (30 days)",
        
        "chart_value_treemap": "💰 Where is the money? (Size = Value $)",
        "chart_velocity": "🚀 Sales Velocity vs Stock Level",
        "chart_age": "⏳ Inventory Age Breakdown",
        "top_money_sku": "🏆 Top SKU by Inventory Value",
        
        "ai_header": "🧠 AI Inventory Forecast",
        "ai_select": "Select SKU:",
        "ai_days": "Forecast Days:",
        "ai_result_date": "📅 Sold-out Date:",
        "ai_result_days": "Days left:",
        "ai_ok": "✅ Stock sufficient",
        
        "col_sku": "SKU",
        "col_name": "Name",
        "col_avail": "Available",
        "col_price": "Price",
        "col_value": "Value ($)",
        "col_velocity": "Sales (30d)",
        "footer_date": "📅 Last update:"
    },
    "RU": {
        "title": "📦 Amazon FBA: Финансовый Центр",
        "update_btn": "🔄 Обновить данные",
        "sidebar_title": "🔍 Фильтры",
        "date_label": "📅 Дата:",
        "store_label": "🏪 Магазин:",
        "all_stores": "Все",
        
        "tab1": "📊 Главный Дашборд",
        "tab2": "💰 Финансы (CFO Mode)",
        "tab3": "🐢 Здоровье склада",
        "tab4": "🧠 AI Прогноз",
        "tab5": "📋 Таблица",

        "total_sku": "Всего SKU",
        "total_avail": "Штук на складе",
        "total_value": "💰 Стоимость склада",
        "potential_rev": "💵 Потенциальная выручка",
        "avg_price": "Средняя цена",
        "velocity_30": "Продажи (30 дней)",
        
        "chart_value_treemap": "💰 Где заморожены деньги? (Размер = Сума $)",
        "chart_velocity": "🚀 Скорость продаж vs Остатки",
        "chart_age": "⏳ Возраст инвентаря (Aging)",
        "top_money_sku": "🏆 Топ SKU по стоимости остатков",
        
        "ai_header": "🧠 AI Прогноз остатков",
        "ai_select": "Выберите SKU:",
        "ai_days": "Горизонт прогноза:",
        "ai_result_date": "📅 Дата Sold-out:",
        "ai_result_days": "Дней осталось:",
        "ai_ok": "✅ Запасов хватит",
        
        "col_sku": "SKU",
        "col_name": "Название",
        "col_avail": "Доступно",
        "col_price": "Цена",
        "col_value": "Сумма ($)",
        "col_velocity": "Продажи (30д)",
        "footer_date": "📅 Данные обновлены:"
    }
}

# --- ВИБІР МОВИ ---
lang_option = st.sidebar.selectbox("Language / Мова / Язык", ["UA 🇺🇦", "EN 🇺🇸", "RU 🌍"], index=0)
if "UA" in lang_option: lang = "UA"
elif "EN" in lang_option: lang = "EN"
else: lang = "RU"
t = translations[lang]

st.title(t["title"])

DATABASE_URL = os.getenv("DATABASE_URL")

@st.cache_data(ttl=60)
def load_data():
    conn = psycopg2.connect(DATABASE_URL)
    # Читаємо всі колонки
    df = pd.read_sql("SELECT * FROM fba_inventory ORDER BY created_at DESC", conn)
    conn.close()
    return df

if st.button(t["update_btn"]):
    st.cache_data.clear()
    st.rerun()

df = load_data()

# --- ПІДГОТОВКА ДАНИХ ---
# Конвертуємо числа. Якщо колонки 'Price' ще немає в базі, створимо її з нулями
if 'Price' not in df.columns:
    df['Price'] = 0.0

numeric_cols = ['Available', 'Inbound', 'FBA Reserved Quantity', 'Total Quantity', 'Price', 'Velocity', 
                'Upto 90 Days', '91 to 180 Days', '181 to 270 Days', '271 to 365 Days', 'More than 365 Days']

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

# Основна фінансова формула: Value = Available * Price
df['Stock Value'] = df['Available'] * df['Price']

df['created_at'] = pd.to_datetime(df['created_at'])
df['date'] = df['created_at'].dt.date

# --- ФІЛЬТРИ ---
st.sidebar.header(t["sidebar_title"])
dates = sorted(df['date'].unique(), reverse=True)
selected_date = st.sidebar.selectbox(t["date_label"], dates, index=0)

stores = [t["all_stores"]] + list(df['Store Name'].unique())
selected_store = st.sidebar.selectbox(t["store_label"], stores)

df_filtered = df[df['date'] == selected_date]
if selected_store != t["all_stores"]:
    df_filtered = df_filtered[df_filtered['Store Name'] == selected_store]

# --- TABS ---
tab1, tab2, tab3, tab4, tab5 = st.tabs([t["tab1"], t["tab2"], t["tab3"], t["tab4"], t["tab5"]])

# === TAB 1: OVERVIEW ===
with tab1:
    st.subheader(f"{t['tab1']} ({selected_date})")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric(t["total_sku"], len(df_filtered))
    col2.metric(t["total_avail"], int(df_filtered['Available'].sum()))
    # Якщо Price = 0, показуємо 0, інакше суму
    total_val = df_filtered['Stock Value'].sum()
    col3.metric(t["total_value"], f"${total_val:,.2f}")
    
    velocity_sum = df_filtered['Velocity'].sum() * 30 # Velocity зазвичай денне, множимо на 30
    col4.metric(t["velocity_30"], f"{int(velocity_sum)} units")

    st.markdown("---")
    
    # Графік: Топ 10 SKU по кількості
    fig_bar = px.bar(
        df_filtered.nlargest(15, 'Available'), 
        x='Available', y='SKU', orientation='h', 
        title="🏆 Top SKU (Qty)", text='Available', color='Available'
    )
    fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig_bar, use_container_width=True)

# === TAB 2: FINANCE (CFO MODE) ===
with tab2:
    st.header(t["tab2"])
    
    if total_val == 0:
        st.warning("⚠️ Увага: Схоже, що в базі даних немає цін (Price = 0). Оновіть ETL-скрипт, щоб підтягнути ціни!")
    
    # KPI
    f_col1, f_col2, f_col3 = st.columns(3)
    f_col1.metric("💰 Total Inventory Value", f"${total_val:,.2f}")
    
    avg_price = df_filtered[df_filtered['Price'] > 0]['Price'].mean()
    f_col2.metric(t["avg_price"], f"${avg_price:,.2f}")
    
    # Treemap - Найкрутіший графік для фінансів
    st.subheader(t["chart_value_treemap"])
    # Беремо тільки ті, де Value > 0
    df_money = df_filtered[df_filtered['Stock Value'] > 0]
    
    if not df_money.empty:
        fig_tree = px.treemap(
            df_money, 
            path=['Store Name', 'SKU'], 
            values='Stock Value',
            color='Stock Value',
            hover_data=['Product Name', 'Available', 'Price'],
            color_continuous_scale='RdYlGn_r' # Червоний - багато грошей заморожено
        )
        st.plotly_chart(fig_tree, use_container_width=True)
    else:
        st.info("No financial data available.")

    # Топ товарів по грошах
    st.subheader(t["top_money_sku"])
    st.dataframe(
        df_filtered[['SKU', 'Available', 'Price', 'Stock Value']]
        .sort_values('Stock Value', ascending=False).head(10)
        .style.format({'Price': "${:.2f}", 'Stock Value': "${:.2f}"}),
        use_container_width=True
    )

# === TAB 3: HEALTH & AGING ===
with tab3:
    st.header(t["tab3"])
    
    # Підготовка даних для Aging
    age_cols = ['Upto 90 Days', '91 to 180 Days', '181 to 270 Days', '271 to 365 Days', 'More than 365 Days']
    # Перевіряємо, чи є ці колонки
    valid_age_cols = [c for c in age_cols if c in df_filtered.columns]
    
    if valid_age_cols:
        age_sums = df_filtered[valid_age_cols].sum().reset_index()
        age_sums.columns = ['Age Group', 'Units']
        
        c1, c2 = st.columns([1, 1])
        
        with c1:
            st.subheader(t["chart_age"])
            fig_pie = px.pie(age_sums, values='Units', names='Age Group', hole=0.4)
            st.plotly_chart(fig_pie, use_container_width=True)
            
        with c2:
            st.subheader(t["chart_velocity"])
            # Scatter plot: Price vs Velocity
            fig_scatter = px.scatter(
                df_filtered, 
                x='Available', 
                y='Velocity', 
                size='Stock Value' if total_val > 0 else 'Available',
                color='Store Name',
                hover_name='SKU',
                log_x=True, 
                title="Stock Level vs Velocity (Log Scale)"
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.warning("No Aging data found in database.")

# === TAB 4: AI FORECAST ===
with tab4:
    st.header(t["ai_header"])
    
    skus = sorted(df['SKU'].unique())
    col_ai1, col_ai2 = st.columns([1, 1])
    with col_ai1:
        target_sku = st.selectbox(t["ai_select"], skus)
    with col_ai2:
        forecast_days = st.slider(t["ai_days"], 7, 90, 30)

    sku_data = df[df['SKU'] == target_sku].copy().sort_values('date')
    sku_data['date_ordinal'] = sku_data['created_at'].map(dt.datetime.toordinal)

    if len(sku_data) >= 3:
        X = sku_data[['date_ordinal']]
        y = sku_data['Available']
        model = LinearRegression()
        model.fit(X, y)
        
        last_date = sku_data['created_at'].max()
        future_dates = [last_date + dt.timedelta(days=x) for x in range(1, forecast_days + 1)]
        future_ordinal = np.array([d.toordinal() for d in future_dates]).reshape(-1, 1)
        predictions = [max(0, int(p)) for p in model.predict(future_ordinal)]
        
        df_forecast = pd.DataFrame({'date': future_dates, 'Predicted': predictions})

st.sidebar.markdown("---")
st.sidebar.info(f"{t['footer_date']} {dates[0] if dates else '-'}")
