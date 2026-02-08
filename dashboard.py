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
        "tab6": "🛒 Замовлення",

        # МЕТРИКИ
        "total_sku": "Всього SKU",
        "total_avail": "Штук на складі",
        "total_value": "💰 Вартість складу (Cost)",
        "potential_rev": "💵 Потенційний виторг",
        "avg_price": "Середня ціна",
        "velocity_30": "Продажі (30 днів)",
        
        # ГРАФІКИ
        "chart_value_treemap": "💰 Де заморожені гроші? (Розмір = Сума $)",
        "chart_velocity": "🚀 Швидкість продажів vs Залишки",
        "chart_age": "⏳ Вік інвентарю (Aging Breakdown)",
        "top_money_sku": "🏆 Топ SKU за вартістю залишків",
        "top_qty_sku": "🏆 Топ SKU за кількістю",
        
        # AI
        "ai_header": "🧠 AI Прогноз залишків",
        "ai_select": "Оберіть SKU:",
        "ai_days": "Горизонт прогнозу:",
        "ai_result_date": "📅 Дата Sold-out:",
        "ai_result_days": "Днів залишилось:",
        "ai_ok": "✅ Запасів вистачить",
        "ai_error": "Недостатньо даних для прогнозу (треба мінімум 3 дні історії)",
        
        "col_sku": "SKU",
        "col_name": "Назва",
        "col_avail": "Доступно",
        "col_price": "Ціна",
        "col_value": "Сума ($)",
        "col_velocity": "Продажі (30д)",
        "footer_date": "📅 Дані оновлено:",
        "download_excel": "📥 Завантажити Excel"
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
        "tab6": "🛒 Orders",

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
        "top_qty_sku": "🏆 Top SKU by Quantity",
        
        "ai_header": "🧠 AI Inventory Forecast",
        "ai_select": "Select SKU:",
        "ai_days": "Forecast Days:",
        "ai_result_date": "📅 Sold-out Date:",
        "ai_result_days": "Days left:",
        "ai_ok": "✅ Stock sufficient",
        "ai_error": "Not enough data for forecast",
        
        "col_sku": "SKU",
        "col_name": "Name",
        "col_avail": "Available",
        "col_price": "Price",
        "col_value": "Value ($)",
        "col_velocity": "Sales (30d)",
        "footer_date": "📅 Last update:",
        "download_excel": "📥 Download Excel"
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
        "tab6": "🛒 Заказы",

        "total_sku": "Всего SKU",
        "total_avail": "Штук на складе",
        "total_value": "💰 Стоимость склада",
        "potential_rev": "💵 Потенциальная выручка",
        "avg_price": "Средняя цена",
        "velocity_30": "Продажи (30 дней)",
        
        "chart_value_treemap": "💰 Где заморожены деньги? (Размер = Сумма $)",
        "chart_velocity": "🚀 Скорость продаж vs Остатки",
        "chart_age": "⏳ Возраст инвентаря (Aging)",
        "top_money_sku": "🏆 Топ SKU по стоимости остатков",
        "top_qty_sku": "🏆 Топ SKU по количеству",
        
        "ai_header": "🧠 AI Прогноз остатков",
        "ai_select": "Выберите SKU:",
        "ai_days": "Горизонт прогноза:",
        "ai_result_date": "📅 Дата Sold-out:",
        "ai_result_days": "Дней осталось:",
        "ai_ok": "✅ Запасов хватит",
        "ai_error": "Недостаточно данных для прогноза",
        
        "col_sku": "SKU",
        "col_name": "Название",
        "col_avail": "Доступно",
        "col_price": "Цена",
        "col_value": "Сумма ($)",
        "col_velocity": "Продажи (30д)",
        "footer_date": "📅 Данные обновлены:",
        "download_excel": "📥 Скачать Excel"
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
    try:
        conn = psycopg2.connect(DATABASE_URL)
        df = pd.read_sql("SELECT * FROM fba_inventory ORDER BY created_at DESC", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Помилка підключення до бази даних: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_orders():
    """Завантаження замовлень з БД"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        
        # Беремо тільки останнє завантаження (щоб не було дублів)
        df = pd.read_sql("""
            SELECT * FROM orders 
            WHERE created_at = (SELECT MAX(created_at) FROM orders)
        """, conn)
        conn.close()
        
        # Конвертуємо типи даних
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0)
        df['Item Price'] = pd.to_numeric(df['Item Price'], errors='coerce').fillna(0)
        df['Item Tax'] = pd.to_numeric(df['Item Tax'], errors='coerce').fillna(0)
        df['Shipping Price'] = pd.to_numeric(df['Shipping Price'], errors='coerce').fillna(0)
        
        # Загальна сума замовлення
        df['Total Price'] = df['Item Price'] + df['Item Tax'] + df['Shipping Price']
        
        return df
    except Exception as e:
        st.error(f"Помилка завантаження orders: {e}")
        return pd.DataFrame()

if st.button(t["update_btn"]):
    st.cache_data.clear()
    st.rerun()

df = load_data()

if df.empty:
    st.warning("База даних порожня. Запустіть amazon_etl.py")
    st.stop()

# --- ПІДГОТОВКА ДАНИХ (ЗАХИСТ ВІД ПОМИЛОК) ---

# 1. Перевіряємо наявність колонки Price. Якщо немає - створюємо з нулями.
if 'Price' not in df.columns:
    df['Price'] = 0.0

# 2. Конвертуємо всі числові колонки безпечно
numeric_cols = ['Available', 'Inbound', 'FBA Reserved Quantity', 'Total Quantity', 'Price', 'Velocity', 
                'Upto 90 Days', '91 to 180 Days', '181 to 270 Days', '271 to 365 Days', 'More than 365 Days']

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    else:
        df[col] = 0 # Якщо якоїсь колонки взагалі немає

# 3. Основна фінансова формула: Value = Available * Price
df['Stock Value'] = df['Available'] * df['Price']

df['created_at'] = pd.to_datetime(df['created_at'])
df['date'] = df['created_at'].dt.date

# --- ФІЛЬТРИ ---
st.sidebar.header(t["sidebar_title"])

dates = sorted(df['date'].unique(), reverse=True)
if dates:
    selected_date = st.sidebar.selectbox(t["date_label"], dates, index=0)
else:
    selected_date = None
    st.sidebar.warning("Немає дат в базі")

stores = [t["all_stores"]] + list(df['Store Name'].unique())
selected_store = st.sidebar.selectbox(t["store_label"], stores)

# Фільтрація
if selected_date:
    df_filtered = df[df['date'] == selected_date]
else:
    df_filtered = df

if selected_store != t["all_stores"]:
    df_filtered = df_filtered[df_filtered['Store Name'] == selected_store]

# --- ВІДОБРАЖЕННЯ ВМІСТУ ---

if df_filtered.empty:
    st.info("Дані за вибраними фільтрами відсутні.")
else:
    # --- TABS ---
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        t["tab1"], t["tab2"], t["tab3"], t["tab4"], t["tab5"], t["tab6"]
    ])

    # === TAB 1: OVERVIEW ===
    with tab1:
        st.subheader(f"{t['tab1']} ({selected_date})")
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric(t["total_sku"], len(df_filtered))
        col2.metric(t["total_avail"], int(df_filtered['Available'].sum()))
        
        total_val = df_filtered['Stock Value'].sum()
        col3.metric(t["total_value"], f"${total_val:,.2f}")
        
        velocity_sum = df_filtered['Velocity'].sum() * 30 
        col4.metric(t["velocity_30"], f"{int(velocity_sum)} units")

        st.markdown("---")
        
        # Графік: Топ SKU по кількості
        if not df_filtered.empty:
            fig_bar = px.bar(
                df_filtered.nlargest(15, 'Available'), 
                x='Available', y='SKU', orientation='h', 
                title=t["top_qty_sku"], text='Available', color='Available'
            )
            fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar, use_container_width=True)

    # === TAB 2: FINANCE (CFO MODE) ===
    with tab2:
        st.header(t["tab2"])
        
        if total_val == 0:
            st.warning("⚠️ Увага: Ціна = 0. Запустіть оновлений amazon_etl.py, щоб завантажити ціни!")
        
        # KPI
        f_col1, f_col2 = st.columns(2)
        f_col1.metric("💰 Total Inventory Value", f"${total_val:,.2f}")
        
        avg_price = df_filtered[df_filtered['Price'] > 0]['Price'].mean()
        if pd.isna(avg_price): avg_price = 0
        f_col2.metric(t["avg_price"], f"${avg_price:,.2f}")
        
        # Treemap
        st.subheader(t["chart_value_treemap"])
        df_money = df_filtered[df_filtered['Stock Value'] > 0]
        
        if not df_money.empty:
            fig_tree = px.treemap(
                df_money, 
                path=['Store Name', 'SKU'], 
                values='Stock Value',
                color='Stock Value',
                hover_data=['Product Name', 'Available', 'Price'],
                color_continuous_scale='RdYlGn_r'
            )
            st.plotly_chart(fig_tree, use_container_width=True)
        else:
            st.info("Немає даних про вартість товарів.")

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
        
        age_cols = ['Upto 90 Days', '91 to 180 Days', '181 to 270 Days', '271 to 365 Days', 'More than 365 Days']
        valid_age_cols = [c for c in age_cols if c in df_filtered.columns]
        
        if valid_age_cols and df_filtered[valid_age_cols].sum().sum() > 0:
            age_sums = df_filtered[valid_age_cols].sum().reset_index()
            age_sums.columns = ['Age Group', 'Units']
            
            c1, c2 = st.columns([1, 1])
            
            with c1:
                st.subheader(t["chart_age"])
                fig_pie = px.pie(age_sums, values='Units', names='Age Group', hole=0.4)
                st.plotly_chart(fig_pie, use_container_width=True)
                
            with c2:
                st.subheader(t["chart_velocity"])
                fig_scatter = px.scatter(
                    df_filtered, 
                    x='Available', 
                    y='Velocity', 
                    size='Stock Value' if total_val > 0 else 'Available',
                    color='Store Name',
                    hover_name='SKU',
                    log_x=True, 
                    title="Stock vs Velocity"
                )
                st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.warning("Дані про вік інвентарю (Aging) відсутні. Перевірте звіт AGED у ETL.")

    # === TAB 4: AI FORECAST ===
    with tab4:
        st.header(t["ai_header"])
        
        skus = sorted(df['SKU'].unique())
        if skus:
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
                
                sold_out = df_forecast[df_forecast['Predicted'] == 0]
                
                c_res1, c_res2 = st.columns(2)
                if not sold_out.empty:
                    s_date = sold_out.iloc[0]['date'].date()
                    days_left = (s_date - dt.date.today()).days
                    c_res1.error(f"{t['ai_result_date']} **{s_date}**")
                    c_res2.metric(t['ai_result_days'], f"{days_left}")
                else:
                    c_res1.success(t["ai_ok"])

                fig = go.Figure()
                fig.add_trace(go.Scatter(x=sku_data['date'], y=sku_data['Available'], mode='lines+markers', name='History'))
                fig.add_trace(go.Scatter(x=df_forecast['date'], y=df_forecast['Predicted'], mode='lines', name='Forecast', line=dict(dash='dash', color='red')))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning(t["ai_error"])
        else:
            st.info("Немає SKU для аналізу")

    # === TAB 5: TABLE ===
    with tab5:
        st.subheader("📋 Data Table")
        
        # Excel Export - з обробкою помилок
        buffer = io.BytesIO()
        
        # Підготовка даних
        df_excel = df_filtered.copy()
        df_excel = df_excel.fillna('')
        
        for col in df_excel.select_dtypes(include=['object']).columns:
            df_excel[col] = df_excel[col].astype(str).str[:32000]
        
        try:
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_excel.to_excel(writer, index=False, sheet_name='Inventory')
            buffer.seek(0)
            
            st.download_button(
                label=t["download_excel"], 
                data=buffer, 
                file_name=f"inventory_{selected_date}.xlsx", 
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error(f"Помилка експорту Excel: {e}")
            csv_data = df_filtered.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download CSV", 
                data=csv_data, 
                file_name=f"inventory_{selected_date}.csv", 
                mime="text/csv"
            )
        
        st.dataframe(df_filtered, use_container_width=True)

    # === TAB 6: ORDERS ANALYTICS ===
    with tab6:
        st.header("🛒 Orders Analytics")
        
        df_orders = load_orders()
        
        if df_orders.empty:
            st.warning("⚠️ Дані про замовлення відсутні. Запустіть amazon_orders_loader.py")
        else:
            # Фільтр по датам
            st.sidebar.markdown("---")
            st.sidebar.subheader("🛒 Orders Filters")
            
            min_date = df_orders['Order Date'].min().date()
            max_date = df_orders['Order Date'].max().date()
            
            date_range = st.sidebar.date_input(
                "📅 Date Range:",
                value=(max_date - dt.timedelta(days=7), max_date),
                min_value=min_date,
                max_value=max_date
            )
            
            if len(date_range) == 2:
                start_date, end_date = date_range
                df_orders_filtered = df_orders[
                    (df_orders['Order Date'].dt.date >= start_date) &
                    (df_orders['Order Date'].dt.date <= end_date)
                ]
            else:
                df_orders_filtered = df_orders
            
            # === KPI METRICS ===
            st.subheader("📊 Key Metrics")
            
            col1, col2, col3, col4 = st.columns(4)
            
            total_orders = df_orders_filtered['Order ID'].nunique()
            total_items = df_orders_filtered['Quantity'].sum()
            total_revenue = df_orders_filtered['Total Price'].sum()
            avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
            
            col1.metric("📦 Total Orders", f"{total_orders:,}")
            col2.metric("📦 Total Items", f"{int(total_items):,}")
            col3.metric("💰 Total Revenue", f"${total_revenue:,.2f}")
            col4.metric("💵 Avg Order Value", f"${avg_order_value:.2f}")
            
            st.markdown("---")
            
            # === ГРАФІКИ ===
            chart_col1, chart_col2 = st.columns(2)
            
            with chart_col1:
                st.subheader("📈 Orders per Day")
                
                orders_per_day = df_orders_filtered.groupby(
                    df_orders_filtered['Order Date'].dt.date
                ).agg({
                    'Order ID': 'nunique',
                    'Total Price': 'sum'
                }).reset_index()
                orders_per_day.columns = ['Date', 'Orders', 'Revenue']
                
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=orders_per_day['Date'],
                    y=orders_per_day['Orders'],
                    mode='lines+markers',
                    name='Orders',
                    line=dict(color='blue', width=2)
                ))
                fig_trend.update_layout(
                    xaxis_title="Date",
                    yaxis_title="Number of Orders",
                    hovermode='x unified'
                )
                st.plotly_chart(fig_trend, use_container_width=True)
            
            with chart_col2:
                st.subheader("💰 Revenue per Day")
                
                fig_revenue = go.Figure()
                fig_revenue.add_trace(go.Bar(
                    x=orders_per_day['Date'],
                    y=orders_per_day['Revenue'],
                    name='Revenue',
                    marker_color='green'
                ))
                fig_revenue.update_layout(
                    xaxis_title="Date",
                    yaxis_title="Revenue ($)",
                    hovermode='x unified'
                )
                st.plotly_chart(fig_revenue, use_container_width=True)
            
            st.markdown("---")
            
            # === TOP SKU ===
            top_col1, top_col2 = st.columns(2)
            
            with top_col1:
                st.subheader("🏆 Top 10 SKU by Orders")
                
                top_sku_orders = df_orders_filtered.groupby('SKU').agg({
                    'Order ID': 'count',
                    'Quantity': 'sum'
                }).reset_index()
                top_sku_orders.columns = ['SKU', 'Order Count', 'Quantity']
                top_sku_orders = top_sku_orders.sort_values('Order Count', ascending=False).head(10)
                
                fig_top_orders = px.bar(
                    top_sku_orders,
                    x='Order Count',
                    y='SKU',
                    orientation='h',
                    text='Order Count',
                    title='',
                    color='Order Count',
                    color_continuous_scale='Blues'
                )
                fig_top_orders.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_top_orders, use_container_width=True)
            
            with top_col2:
                st.subheader("💰 Top 10 SKU by Revenue")
                
                top_sku_revenue = df_orders_filtered.groupby('SKU').agg({
                    'Total Price': 'sum',
                    'Quantity': 'sum'
                }).reset_index()
                top_sku_revenue.columns = ['SKU', 'Revenue', 'Quantity']
                top_sku_revenue = top_sku_revenue.sort_values('Revenue', ascending=False).head(10)
                
                fig_top_revenue = px.bar(
                    top_sku_revenue,
                    x='Revenue',
                    y='SKU',
                    orientation='h',
                    text='Revenue',
                    title='',
                    color='Revenue',
                    color_continuous_scale='Greens'
                )
                fig_top_revenue.update_traces(texttemplate='$%{text:.2f}')
                fig_top_revenue.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_top_revenue, use_container_width=True)
            
            st.markdown("---")
            
            # === ORDERS BY STATUS ===
            st.subheader("📊 Orders by Status")
            
            status_counts = df_orders_filtered.groupby('Order Status').agg({
                'Order ID': 'nunique'
            }).reset_index()
            status_counts.columns = ['Status', 'Orders']
            
            fig_status = px.pie(
                status_counts,
                values='Orders',
                names='Status',
                hole=0.4,
                title='Distribution by Order Status'
            )
            st.plotly_chart(fig_status, use_container_width=True)
            
            st.markdown("---")
            
            # === DETAILED TABLE ===
            st.subheader("📋 Orders Details")
            
            # Фільтр по SKU
            unique_skus = ['All'] + sorted(df_orders_filtered['SKU'].unique().tolist())
            selected_sku = st.selectbox("Filter by SKU:", unique_skus)
            
            if selected_sku != 'All':
                df_display = df_orders_filtered[df_orders_filtered['SKU'] == selected_sku]
            else:
                df_display = df_orders_filtered
            
            # Показуємо тільки важливі колонки
            display_cols = [
                'Order Date', 'Order ID', 'SKU', 'Product Name',
                'Quantity', 'Item Price', 'Total Price', 'Order Status',
                'Fulfillment Channel', 'Ship City', 'Ship State', 'Ship Country'
            ]
            
            df_show = df_display[display_cols].sort_values('Order Date', ascending=False)
            
            st.dataframe(
                df_show.style.format({
                    'Item Price': '${:.2f}',
                    'Total Price': '${:.2f}',
                    'Quantity': '{:.0f}'
                }),
                use_container_width=True
            )
            
            # Excel Export - з обробкою помилок
            buffer = io.BytesIO()
            
            # Підготовка даних для Excel (очищення проблемних значень)
            df_excel = df_show.copy()
            
            # 1. Конвертуємо дати в строки (уникаємо NaT проблем)
            if 'Order Date' in df_excel.columns:
                df_excel['Order Date'] = df_excel['Order Date'].astype(str).replace('NaT', '')
            
            # 2. Заміна NaN та None на порожні строки
            df_excel = df_excel.fillna('')
            
            # 3. Обмежуємо довжину текстових полів (Excel limit = 32,767 символів)
            for col in df_excel.select_dtypes(include=['object']).columns:
                df_excel[col] = df_excel[col].astype(str).str[:32000]
            
            # 4. Записуємо в Excel
            try:
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_excel.to_excel(writer, index=False, sheet_name='Orders')
                buffer.seek(0)
                
                st.download_button(
                    label="📥 Download Orders Excel",
                    data=buffer,
                    file_name=f"orders_{start_date}_to_{end_date}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except Exception as e:
                st.error(f"Помилка експорту Excel: {e}")
                st.info("Спробуйте експорт в CSV замість цього:")
                
                # Fallback: CSV export
                csv_data = df_show.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Orders CSV",
                    data=csv_data,
                    file_name=f"orders_{start_date}_to_{end_date}.csv",
                    mime="text/csv"
                )

# Footer info
st.sidebar.markdown("---")
if dates:
    st.sidebar.info(f"{t['footer_date']} {dates[0]}")
