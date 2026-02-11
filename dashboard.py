import streamlit as st
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
import io
from sklearn.linear_model import LinearRegression
import numpy as np
import datetime as dt
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Завантаження змінних середовища
load_dotenv()

st.set_page_config(page_title="Amazon FBA Ultimate BI", layout="wide", page_icon="📦")

# --- НАЛАШТУВАННЯ БАЗИ ДАНИХ (SQLAlchemy) ---
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

def get_engine():
    """Створює підключення до БД"""
    return create_engine(DATABASE_URL)

# --- СЛОВНИК ПЕРЕКЛАДІВ ---
translations = {
    "UA": {
        "title": "📦 Amazon FBA: Business Intelligence Hub",
        "update_btn": "🔄 Оновити дані",
        "sidebar_title": "🔍 Фільтри",
        "date_label": "📅 Дата:",
        "store_label": "🏪 Магазин:",
        "all_stores": "Всі",
        
        "total_sku": "Всього SKU",
        "total_avail": "Штук на складі",
        "total_value": "💰 Вартість складу",
        "velocity_30": "Продажі (30 днів)",
        
        "chart_value_treemap": "💰 Де заморожені гроші?",
        "chart_velocity": "🚀 Швидкість vs Залишки",
        "chart_age": "⏳ Вік інвентарю",
        "top_money_sku": "🏆 Топ SKU за вартістю",
        "top_qty_sku": "🏆 Топ SKU за кількістю",
        "avg_price": "Середня ціна",
        
        "ai_header": "🧠 AI Прогноз залишків",
        "ai_select": "Оберіть SKU:",
        "ai_days": "Горизонт прогнозу:",
        "ai_result_date": "📅 Дата Sold-out:",
        "ai_result_days": "Днів залишилось:",
        "ai_ok": "✅ Запасів вистачить",
        "ai_error": "Недостатньо даних для прогнозу",
        
        "footer_date": "📅 Дані оновлено:",
        "download_excel": "📥 Завантажити Excel",

        # --- Settlements ---
        "settlements_title": "🏦 Фінансові виплати (Settlements)",
        "net_payout": "Чиста виплата",
        "gross_sales": "Валові продажі",
        "total_fees": "Всього комісій",
        "total_refunds": "Повернення коштів",
        "chart_payout_trend": "📉 Динаміка виплат",
        "chart_fee_breakdown": "💸 Структура витрат",
        "currency_select": "💱 Валюта:",
    },
    "EN": {
        "title": "📦 Amazon FBA: Business Intelligence Hub",
        "update_btn": "🔄 Refresh Data",
        "sidebar_title": "🔍 Filters",
        "date_label": "📅 Date:",
        "store_label": "🏪 Store:",
        "all_stores": "All",
        
        "total_sku": "Total SKU",
        "total_avail": "Total Units",
        "total_value": "💰 Inventory Value",
        "velocity_30": "Sales (30 days)",
        
        "chart_value_treemap": "💰 Where is the money?",
        "chart_velocity": "🚀 Velocity vs Stock",
        "chart_age": "⏳ Inventory Age",
        "top_money_sku": "🏆 Top SKU by Value",
        "top_qty_sku": "🏆 Top SKU by Quantity",
        "avg_price": "Avg Price",
        
        "ai_header": "🧠 AI Inventory Forecast",
        "ai_select": "Select SKU:",
        "ai_days": "Forecast Days:",
        "ai_result_date": "📅 Sold-out Date:",
        "ai_result_days": "Days left:",
        "ai_ok": "✅ Stock sufficient",
        "ai_error": "Not enough data",
        
        "footer_date": "📅 Last update:",
        "download_excel": "📥 Download Excel",

        # --- Settlements ---
        "settlements_title": "🏦 Financial Settlements (Payouts)",
        "net_payout": "Net Payout",
        "gross_sales": "Gross Sales",
        "total_fees": "Total Fees",
        "total_refunds": "Total Refunds",
        "chart_payout_trend": "📉 Payout Trend",
        "chart_fee_breakdown": "💸 Fee Breakdown",
        "currency_select": "💱 Currency:",
    },
    "RU": {
        "title": "📦 Amazon FBA: Business Intelligence Hub",
        "update_btn": "🔄 Обновить",
        "sidebar_title": "🔍 Фильтры",
        "date_label": "📅 Дата:",
        "store_label": "🏪 Магазин:",
        "all_stores": "Все",
        
        "total_sku": "Всего SKU",
        "total_avail": "Штук на складе",
        "total_value": "💰 Стоимость склада",
        "velocity_30": "Продажи (30 дней)",
        
        "chart_value_treemap": "💰 Где деньги?",
        "chart_velocity": "🚀 Скорость vs Остатки",
        "chart_age": "⏳ Возраст инвентаря",
        "top_money_sku": "🏆 Топ SKU по стоимости",
        "top_qty_sku": "🏆 Топ SKU по количеству",
        "avg_price": "Средняя цена",
        
        "ai_header": "🧠 AI Прогноз остатков",
        "ai_select": "Выберите SKU:",
        "ai_days": "Горизонт прогноза:",
        "ai_result_date": "📅 Дата Sold-out:",
        "ai_result_days": "Дней осталось:",
        "ai_ok": "✅ Запасов хватит",
        "ai_error": "Недостаточно данных",
        
        "footer_date": "📅 Данные обновлены:",
        "download_excel": "📥 Скачать Excel",

        # --- Settlements ---
        "settlements_title": "🏦 Финансовые выплаты (Settlements)",
        "net_payout": "Чистая выплата",
        "gross_sales": "Валовые продажи",
        "total_fees": "Всего комиссий",
        "total_refunds": "Возвраты средств",
        "chart_payout_trend": "📉 Динамика выплат",
        "chart_fee_breakdown": "💸 Структура расходов",
        "currency_select": "💱 Валюта:",
    }
}

# ============================================
# ФУНКЦІЇ ЗАВАНТАЖЕННЯ ДАНИХ
# ============================================

@st.cache_data(ttl=60)
def load_data():
    """Load Inventory Data"""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT * FROM fba_inventory ORDER BY created_at DESC"), conn)
        return df
    except Exception as e:
        st.error(f"Помилка підключення до БД (Inventory): {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_orders():
    """Load Orders Data with proper price calculation"""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT * FROM orders ORDER BY \"Order Date\" DESC"), conn)
        
        if df.empty:
            return pd.DataFrame()
        
        # Виправлення дат
        df['Order Date'] = pd.to_datetime(df['Order Date'], dayfirst=True, errors='coerce')
        
        # 🔥 ВИПРАВЛЕНО: Шукаємо правильні назви колонок
        column_mappings = {
            'Quantity': ['Quantity', 'quantity', 'qty'],
            'Item Price': ['Item Price', 'item-price', 'item_price', 'price'],
            'Item Tax': ['Item Tax', 'item-tax', 'item_tax', 'tax'],
            'Shipping Price': ['Shipping Price', 'shipping-price', 'shipping_price', 'shipping'],
        }
        
        for target_col, possible_names in column_mappings.items():
            found = False
            for col_name in possible_names:
                if col_name in df.columns:
                    df[target_col] = pd.to_numeric(df[col_name], errors='coerce').fillna(0)
                    found = True
                    break
            if not found:
                df[target_col] = 0
        
        # 🔥 ОБЧИСЛЕННЯ Total Price
        df['Total Price'] = df['Item Price'] * df['Quantity']
        
        # Debug output (буде видно в консолі Streamlit)
        total_revenue = df['Total Price'].sum()
        total_items = df['Quantity'].sum()
        print(f"📊 Orders loaded: {len(df)} rows")
        print(f"💰 Total Revenue: ${total_revenue:,.2f}")
        print(f"📦 Total Items: {total_items}")
        
        return df
        
    except Exception as e:
        st.error(f"Помилка завантаження orders: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_settlements():
    """Load Financial Settlements Data"""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT * FROM settlements ORDER BY \"Posted Date\" DESC"), conn)
        
        if df.empty: 
            return pd.DataFrame()

        # Чистка даних
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0.0)
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0)
        df['Posted Date'] = pd.to_datetime(df['Posted Date'], dayfirst=True, errors='coerce')
        
        if 'Currency' not in df.columns:
            df['Currency'] = 'USD'
        
        # Видаляємо рядки з невалідними датами
        df = df.dropna(subset=['Posted Date'])
        
        return df
    except Exception as e:
        st.error(f"Error loading settlements: {e}")
        return pd.DataFrame()

# ============================================
# REPORT FUNCTIONS
# ============================================

def show_overview(df_filtered, t, selected_date):
    """📊 Головний Дашборд"""
    
    st.markdown("### 📊 Business Dashboard Overview")
    st.caption(f"Data snapshot: {selected_date}")
    
    # === KEY METRICS ===
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(label=t["total_sku"], value=len(df_filtered))
    with col2:
        st.metric(label=t["total_avail"], value=f"{int(df_filtered['Available'].sum()):,}")
    with col3:
        total_val = df_filtered['Stock Value'].sum()
        st.metric(label=t["total_value"], value=f"${total_val:,.0f}")
    with col4:
        velocity_sum = df_filtered['Velocity'].sum() * 30
        st.metric(label=t["velocity_30"], value=f"{int(velocity_sum):,} units")
    
    st.markdown("---")
    
    # === NAVIGATION CARDS ===
    col1, col2, col3 = st.columns(3)
    
    with col1:
        with st.container(border=True):
            st.markdown(f"#### {t['settlements_title']}")
            st.markdown("Actual Payouts, Net Profit, Fees")
            if st.button("🏦 View Finance (Payouts) →", key="btn_settlements", width='stretch', type="primary"):
                st.session_state.report_choice = "🏦 Settlements (Payouts)"
                st.rerun()

    with col2:
        with st.container(border=True):
            st.markdown("#### 🛒 Orders Analytics")
            st.markdown("Sales Trends, Top Products")
            if st.button("📊 View Orders Report →", key="btn_orders", width='stretch', type="primary"):
                st.session_state.report_choice = "🛒 Orders Analytics"
                st.rerun()
    
    with col3:
        with st.container(border=True):
            st.markdown("#### 💰 Inventory Value")
            st.markdown("Money map, Pricing analytics")
            if st.button("💰 View Inventory Value →", key="btn_finance", width='stretch', type="primary"):
                st.session_state.report_choice = "💰 Inventory Value (CFO)"
                st.rerun()
    
    st.markdown("")

    # ROW 2
    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True):
            st.markdown("#### 🧠 AI Forecast")
            st.markdown("Sold-out predictions")
            if st.button("🧠 View AI Forecast →", key="btn_ai", width='stretch', type="primary"):
                st.session_state.report_choice = "🧠 AI Forecast"
                st.rerun()

    with col2:
        with st.container(border=True):
            st.markdown("#### 🐢 Inventory Health")
            st.markdown("Aging analysis")
            if st.button("🐢 View Health Report →", key="btn_health", width='stretch', type="primary"):
                st.session_state.report_choice = "🐢 Inventory Health (Aging)"
                st.rerun()

    with col3:
        with st.container(border=True):
            st.markdown("#### 📋 FBA Data Table")
            st.markdown("Full excel export")
            if st.button("📋 View FBA Data →", key="btn_table", width='stretch', type="primary"):
                st.session_state.report_choice = "📋 FBA Inventory Table"
                st.rerun()

    st.markdown("---")
    
    # === QUICK CHART ===
    st.markdown("### 📊 Quick Overview: Top 15 SKU by Stock Level")
    
    if not df_filtered.empty:
        df_top = df_filtered.nlargest(15, 'Available')
        fig_bar = px.bar(
            df_top, x='Available', y='SKU', orientation='h',
            text='Available', color='Available', color_continuous_scale='Blues'
        )
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, height=400)
        st.plotly_chart(fig_bar, width='stretch')


def show_settlements(t):
    """💰 Actual Financial Settlements Report"""
    
    df_settlements = load_settlements()
    
    if df_settlements.empty:
        st.warning("⚠️ No settlement data found. Please run 'amazon_settlement_loader.py'.")
        return

    st.sidebar.markdown("---")
    st.sidebar.subheader("💰 Settlement Filters")
    
    # 1. CURRENCY FILTER
    try:
        currencies = ['All'] + sorted(df_settlements['Currency'].dropna().unique().tolist())
        selected_currency = st.sidebar.selectbox(t["currency_select"], currencies, index=1 if "USD" in currencies else 0)
    except Exception as e:
        st.error(f"Error loading currencies: {e}")
        selected_currency = 'All'
    
    # 2. DATE FILTER
    try:
        min_date = df_settlements['Posted Date'].min().date()
        max_date = df_settlements['Posted Date'].max().date()
        
        date_range = st.sidebar.date_input(
            "📅 Transaction Date:",
            value=(max_date - dt.timedelta(days=30), max_date),
            min_value=min_date,
            max_value=max_date
        )
    except Exception as e:
        st.error(f"Error with dates: {e}")
        date_range = []
    
    # APPLY FILTERS
    df_filtered = df_settlements.copy()
    
    if selected_currency != 'All':
        df_filtered = df_filtered[df_filtered['Currency'] == selected_currency]
    
    if len(date_range) == 2:
        start_date, end_date = date_range
        mask = (df_filtered['Posted Date'].dt.date >= start_date) & \
               (df_filtered['Posted Date'].dt.date <= end_date)
        df_filtered = df_filtered[mask]

    if df_filtered.empty:
        st.warning("No data for selected filters")
        return

    # --- KPI ---
    st.markdown(f"### {t['settlements_title']}")
    
    col1, col2, col3, col4 = st.columns(4)
    
    try:
        net_payout = df_filtered['Amount'].sum()
        gross_sales = df_filtered[(df_filtered['Transaction Type'] == 'Order') & (df_filtered['Amount'] > 0)]['Amount'].sum()
        refunds = df_filtered[df_filtered['Transaction Type'] == 'Refund']['Amount'].sum()
        fees = df_filtered[(df_filtered['Amount'] < 0) & (df_filtered['Transaction Type'] != 'Refund')]['Amount'].sum()

        currency_symbol = "$" if selected_currency in ['USD', 'CAD', 'All'] else ""

        col1.metric(t['net_payout'], f"{currency_symbol}{net_payout:,.2f}")
        col2.metric(t['gross_sales'], f"{currency_symbol}{gross_sales:,.2f}")
        col3.metric(t['total_refunds'], f"{currency_symbol}{refunds:,.2f}")
        col4.metric(t['total_fees'], f"{currency_symbol}{fees:,.2f}")
    except Exception as e:
        st.error(f"Error calculating metrics: {e}")
        return
    
    st.markdown("---")

    # --- CHARTS ---
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader(t['chart_payout_trend'])
        try:
            daily_trend = df_filtered.groupby(df_filtered['Posted Date'].dt.date)['Amount'].sum().reset_index()
            daily_trend.columns = ['Date', 'Net Amount']
            
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Bar(
                x=daily_trend['Date'],
                y=daily_trend['Net Amount'],
                marker_color=daily_trend['Net Amount'].apply(lambda x: 'green' if x >= 0 else 'red'),
            ))
            fig_trend.update_layout(height=400, yaxis_title=f"Net Amount ({selected_currency})")
            st.plotly_chart(fig_trend, width='stretch')
        except Exception as e:
            st.error(f"Chart error: {e}")

    with col2:
        st.subheader(t['chart_fee_breakdown'])
        try:
            df_costs = df_filtered[df_filtered['Amount'] < 0]
            if not df_costs.empty:
                cost_breakdown = df_costs.groupby('Transaction Type')['Amount'].sum().abs().reset_index()
                fig_pie = px.pie(cost_breakdown, values='Amount', names='Transaction Type', hole=0.4)
                fig_pie.update_layout(height=400)
                st.plotly_chart(fig_pie, width='stretch')
            else:
                st.info("No costs in selected period")
        except Exception as e:
            st.error(f"Pie chart error: {e}")
            
    # --- TABLE ---
    st.markdown("#### 📋 Transaction Details")
    try:
        display_cols = ['Posted Date', 'Transaction Type', 'Order ID', 'Amount', 'Currency', 'Description']
        available_cols = [c for c in display_cols if c in df_filtered.columns]
        
        st.dataframe(
            df_filtered[available_cols].sort_values('Posted Date', ascending=False).head(100), 
            width='stretch'
        )
    except Exception as e:
        st.error(f"Table error: {e}")


def show_inventory_finance(df_filtered, t):
    """💰 Фінанси складу (CFO Mode)"""
    total_val = df_filtered['Stock Value'].sum()
    
    col1, col2, col3 = st.columns(3)
    col1.metric("💰 Total Inventory Value", f"${total_val:,.2f}")
    
    avg_price = df_filtered[df_filtered['Price'] > 0]['Price'].mean()
    col2.metric(t["avg_price"], f"${avg_price:,.2f}" if not pd.isna(avg_price) else "$0")
    
    total_units = df_filtered['Available'].sum()
    avg_value_per_unit = total_val / total_units if total_units > 0 else 0
    col3.metric("💵 Avg Value per Unit", f"${avg_value_per_unit:.2f}")
    
    st.markdown("---")
    st.subheader(t["chart_value_treemap"])
    
    df_money = df_filtered[df_filtered['Stock Value'] > 0]
    if not df_money.empty:
        fig_tree = px.treemap(
            df_money, path=['Store Name', 'SKU'], values='Stock Value',
            color='Stock Value', color_continuous_scale='RdYlGn_r'
        )
        st.plotly_chart(fig_tree, width='stretch')
    
    st.subheader(t["top_money_sku"])
    df_top = df_filtered[['SKU', 'Product Name', 'Available', 'Price', 'Stock Value']].sort_values('Stock Value', ascending=False).head(10)
    st.dataframe(df_top.style.format({'Price': "${:.2f}", 'Stock Value': "${:,.2f}"}), width='stretch')

def show_aging(df_filtered, t):
    """🐢 Здоров'я складу (Aging)"""
    
    # Перевірка чи DataFrame не порожній
    if df_filtered.empty:
        st.warning("Немає даних для відображення")
        return
    
    age_cols = ['Upto 90 Days', '91 to 180 Days', '181 to 270 Days', '271 to 365 Days', 'More than 365 Days']
    
    # Знаходимо які колонки дійсно існують
    valid_age_cols = [c for c in age_cols if c in df_filtered.columns]
    
    if not valid_age_cols:
        st.warning("Дані про вік інвентарю відсутні. Перевірте звіт AGED у ETL.")
        return
    
    try:
        # Конвертуємо колонки в числа
        df_age = df_filtered[valid_age_cols].copy()
        for col in valid_age_cols:
            df_age[col] = pd.to_numeric(df_age[col], errors='coerce').fillna(0)
        
        # Перевіряємо чи є дані
        total_aged = df_age.sum().sum()
        
        if total_aged == 0:
            st.info("Всі товари свіжі - немає застарілого інвентарю")
            return
        
        # Створюємо підсумок по групам
        age_sums = df_age.sum().reset_index()
        age_sums.columns = ['Age Group', 'Units']
        age_sums = age_sums[age_sums['Units'] > 0]  # Тільки ненульові
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader(t["chart_age"])
            fig_pie = px.pie(age_sums, values='Units', names='Age Group', hole=0.4)
            fig_pie.update_layout(height=400)
            st.plotly_chart(fig_pie, width='stretch')
            
        with col2:
            st.subheader(t["chart_velocity"])
            # Перевіряємо чи є потрібні колонки
            if 'Available' in df_filtered.columns and 'Velocity' in df_filtered.columns and 'Stock Value' in df_filtered.columns:
                # Фільтруємо валідні дані
                df_scatter = df_filtered[
                    (df_filtered['Available'] > 0) & 
                    (df_filtered['Velocity'] >= 0) & 
                    (df_filtered['Stock Value'] > 0)
                ].copy()
                
                if not df_scatter.empty:
                    fig_scatter = px.scatter(
                        df_scatter, 
                        x='Available', 
                        y='Velocity', 
                        size='Stock Value',
                        color='Store Name' if 'Store Name' in df_scatter.columns else None,
                        hover_name='SKU',
                        log_x=True
                    )
                    fig_scatter.update_layout(height=400)
                    st.plotly_chart(fig_scatter, width='stretch')
                else:
                    st.info("Недостатньо даних для графіка velocity")
            else:
                st.warning("Відсутні колонки для графіка velocity")
                
    except Exception as e:
        st.error(f"Помилка обробки даних aging: {e}")
        st.info("Спробуйте перезавантажити сторінку або оберіть іншу дату")


def show_ai_forecast(df, t):
    """🧠 AI Прогноз"""
    st.markdown("### Select SKU for Forecast")
    skus = sorted(df['SKU'].unique())
    
    if skus:
        col1, col2 = st.columns([2, 1])
        target_sku = col1.selectbox(t["ai_select"], skus)
        forecast_days = col2.slider(t["ai_days"], 7, 90, 30)

        sku_data = df[df['SKU'] == target_sku].copy().sort_values('date')
        sku_data['date_ordinal'] = sku_data['created_at'].map(dt.datetime.toordinal)

        if len(sku_data) >= 3:
            X = sku_data[['date_ordinal']]
            y = sku_data['Available']
            model = LinearRegression().fit(X, y)
            
            last_date = sku_data['created_at'].max()
            future_dates = [last_date + dt.timedelta(days=x) for x in range(1, forecast_days + 1)]
            future_ordinal = np.array([d.toordinal() for d in future_dates]).reshape(-1, 1)
            predictions = [max(0, int(p)) for p in model.predict(future_ordinal)]
            
            df_forecast = pd.DataFrame({'date': future_dates, 'Predicted': predictions})
            
            sold_out = df_forecast[df_forecast['Predicted'] == 0]
            if not sold_out.empty:
                s_date = sold_out.iloc[0]['date'].date()
                st.error(f"{t['ai_result_date']} **{s_date}**")
            else:
                st.success(t['ai_ok'])

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=sku_data['date'], y=sku_data['Available'], name='Historical'))
            fig.add_trace(go.Scatter(x=df_forecast['date'], y=df_forecast['Predicted'], name='Forecast', line=dict(dash='dash', color='red')))
            st.plotly_chart(fig, width='stretch')
        else:
            st.warning(t["ai_error"])
    else:
        st.info("No SKU available")


def show_data_table(df_filtered, t, selected_date):
    """📋 Таблиця даних FBA Inventory"""
    st.markdown("### 📊 FBA Inventory Dataset")
    
    csv = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button(label="📥 Download CSV", data=csv, file_name="fba_inventory.csv", mime="text/csv")
    
    st.dataframe(df_filtered, width='stretch', height=600)


def show_orders():
    """🛒 Замовлення"""
    df_orders = load_orders()
    if df_orders.empty:
        st.warning("⚠️ Дані відсутні. Запустіть amazon_orders_loader.py")
        return
        
    st.sidebar.markdown("---")
    st.sidebar.subheader("🛒 Orders Filters")
    
    min_date = df_orders['Order Date'].min().date()
    max_date = df_orders['Order Date'].max().date()
    
    date_range = st.sidebar.date_input("📅 Date Range:", value=(max_date - dt.timedelta(days=7), max_date), min_value=min_date, max_value=max_date)
    
    if len(date_range) == 2:
        df_filtered = df_orders[(df_orders['Order Date'].dt.date >= date_range[0]) & (df_orders['Order Date'].dt.date <= date_range[1])]
    else:
        df_filtered = df_orders

    col1, col2, col3 = st.columns(3)
    col1.metric("📦 Orders", df_filtered['Order ID'].nunique())
    col2.metric("💰 Revenue", f"${df_filtered['Total Price'].sum():,.2f}")
    col3.metric("📦 Items", int(df_filtered['Quantity'].sum()))
    
    st.markdown("#### 📈 Orders per Day")
    daily = df_filtered.groupby(df_filtered['Order Date'].dt.date)['Total Price'].sum().reset_index()
    fig = px.bar(daily, x='Order Date', y='Total Price', title="Daily Revenue")
    st.plotly_chart(fig, width='stretch')
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🏆 Top 10 SKU by Revenue")
        top_sku = df_filtered.groupby('SKU')['Total Price'].sum().nlargest(10).reset_index()
        fig2 = px.bar(top_sku, x='Total Price', y='SKU', orientation='h')
        fig2.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig2, width='stretch')
    
    with col2:
        st.markdown("#### 📊 Order Status Distribution")
        if 'Order Status' in df_filtered.columns:
            status_counts = df_filtered['Order Status'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Count']
            fig3 = px.pie(status_counts, values='Count', names='Status', hole=0.4)
            st.plotly_chart(fig3, width='stretch')
        else:
            st.info("Order Status column not available")

# ============================================
# MAIN APP LOGIC
# ============================================

if 'report_choice' not in st.session_state:
    st.session_state.report_choice = "🏠 Overview"

lang_option = st.sidebar.selectbox("🌍 Language", ["UA 🇺🇦", "EN 🇺🇸", "RU 🌍"], index=0)
lang = "UA" if "UA" in lang_option else "EN" if "EN" in lang_option else "RU"
t = translations[lang]

if st.sidebar.button(t["update_btn"], width='stretch'):
    st.cache_data.clear()
    st.rerun()

df = load_data()

if not df.empty:
    numeric_cols = ['Available', 'Price', 'Velocity', 'Stock Value']
    for col in numeric_cols:
        if col not in df.columns: df[col] = 0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    df['Stock Value'] = df['Available'] * df['Price']
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['date'] = df['created_at'].dt.date

    st.sidebar.header(t["sidebar_title"])
    dates = sorted(df['date'].unique(), reverse=True)
    selected_date = st.sidebar.selectbox(t["date_label"], dates) if dates else None
    
    stores = [t["all_stores"]] + list(df['Store Name'].unique()) if 'Store Name' in df.columns else [t["all_stores"]]
    selected_store = st.sidebar.selectbox(t["store_label"], stores)

    df_filtered = df[df['date'] == selected_date] if selected_date else df
    if selected_store != t["all_stores"]:
        df_filtered = df_filtered[df_filtered['Store Name'] == selected_store]
else:
    df_filtered = pd.DataFrame()
    selected_date = None

st.sidebar.markdown("---")
st.sidebar.header("📊 Reports")

report_options = [
    "🏠 Overview",
    "🏦 Settlements (Payouts)",
    "💰 Inventory Value (CFO)",
    "🛒 Orders Analytics",
    "🐢 Inventory Health (Aging)",
    "🧠 AI Forecast",
    "📋 FBA Inventory Table"
]

current_index = 0
if st.session_state.report_choice in report_options:
    current_index = report_options.index(st.session_state.report_choice)

report_choice = st.sidebar.radio("Select Report:", report_options, index=current_index)
st.session_state.report_choice = report_choice

if report_choice == "🏠 Overview":
    show_overview(df_filtered, t, selected_date)
elif report_choice == "🏦 Settlements (Payouts)":
    show_settlements(t)
elif report_choice == "💰 Inventory Value (CFO)":
    show_inventory_finance(df_filtered, t)
elif report_choice == "🛒 Orders Analytics":
    show_orders()
elif report_choice == "🐢 Inventory Health (Aging)":
    show_aging(df_filtered, t)
elif report_choice == "🧠 AI Forecast":
    show_ai_forecast(df, t)
elif report_choice == "📋 FBA Inventory Table":
    show_data_table(df_filtered, t, selected_date)

st.sidebar.markdown("---")
st.sidebar.caption("📦 Amazon FBA BI System v2.5 (Orders Fixed)")
