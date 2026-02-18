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

load_dotenv()

st.set_page_config(page_title="Amazon FBA Ultimate BI", layout="wide", page_icon="📦")

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

def get_engine():
    return create_engine(
        DATABASE_URL,
        connect_args={"options": "-csearch_path=spapi,public"}
    )

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
        "settlements_title": "🏦 Фінансові виплати (Settlements)",
        "net_payout": "Чиста виплата",
        "gross_sales": "Валові продажі",
        "total_fees": "Всього комісій",
        "total_refunds": "Повернення коштів",
        "chart_payout_trend": "📉 Динаміка виплат",
        "chart_fee_breakdown": "💸 Структура витрат",
        "currency_select": "💱 Валюта:",
        "sales_traffic_title": "📈 Sales & Traffic",
        "st_sessions": "Сесії",
        "st_page_views": "Перегляди",
        "st_units": "Замовлено штук",
        "st_conversion": "Конверсія",
        "st_revenue": "Дохід",
        "st_buy_box": "Buy Box %",
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
        "settlements_title": "🏦 Financial Settlements (Payouts)",
        "net_payout": "Net Payout",
        "gross_sales": "Gross Sales",
        "total_fees": "Total Fees",
        "total_refunds": "Total Refunds",
        "chart_payout_trend": "📉 Payout Trend",
        "chart_fee_breakdown": "💸 Fee Breakdown",
        "currency_select": "💱 Currency:",
        "sales_traffic_title": "📈 Sales & Traffic",
        "st_sessions": "Sessions",
        "st_page_views": "Page Views",
        "st_units": "Units Ordered",
        "st_conversion": "Conversion",
        "st_revenue": "Revenue",
        "st_buy_box": "Buy Box %",
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
        "settlements_title": "🏦 Финансовые выплаты (Settlements)",
        "net_payout": "Чистая выплата",
        "gross_sales": "Валовые продажи",
        "total_fees": "Всего комиссий",
        "total_refunds": "Возвраты средств",
        "chart_payout_trend": "📉 Динамика выплат",
        "chart_fee_breakdown": "💸 Структура расходов",
        "currency_select": "💱 Валюта:",
        "sales_traffic_title": "📈 Sales & Traffic",
        "st_sessions": "Сессии",
        "st_page_views": "Просмотры",
        "st_units": "Заказано штук",
        "st_conversion": "Конверсия",
        "st_revenue": "Доход",
        "st_buy_box": "Buy Box %",
    }
}

# ============================================
# DATA LOADERS
# ============================================

@st.cache_data(ttl=60)
def load_data():
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
    try:
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text('SELECT * FROM orders ORDER BY "Order Date" DESC'), conn)
        if df.empty:
            return pd.DataFrame()
        df['Order Date'] = pd.to_datetime(df['Order Date'], dayfirst=True, errors='coerce')
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
        df['Total Price'] = df['Item Price'] * df['Quantity']
        return df
    except Exception as e:
        st.error(f"Помилка завантаження orders: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_settlements():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text('SELECT * FROM settlements ORDER BY "Posted Date" DESC'), conn)
        if df.empty:
            return pd.DataFrame()
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0.0)
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0)
        df['Posted Date'] = pd.to_datetime(df['Posted Date'], dayfirst=True, errors='coerce')
        if 'Currency' not in df.columns:
            df['Currency'] = 'USD'
        df = df.dropna(subset=['Posted Date'])
        return df
    except Exception as e:
        st.error(f"Error loading settlements: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_sales_traffic():
    """Load Sales & Traffic via psycopg2 directly (same as loader)"""
    import psycopg2
    import psycopg2.extras

    db_url = DATABASE_URL
    if not db_url:
        st.error("❌ DATABASE_URL not set")
        return pd.DataFrame()

    # psycopg2 требует postgresql://, а не postgres://
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("SELECT * FROM spapi.sales_traffic ORDER BY report_date DESC")
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=columns)

        numeric_cols = [
            'sessions', 'page_views', 'units_ordered', 'units_ordered_b2b',
            'total_order_items', 'total_order_items_b2b',
            'ordered_product_sales', 'ordered_product_sales_b2b',
            'session_percentage', 'page_views_percentage',
            'buy_box_percentage', 'unit_session_percentage',
            'mobile_sessions', 'mobile_page_views',
            'browser_sessions', 'browser_page_views',
            'mobile_session_percentage', 'mobile_page_views_percentage',
            'mobile_unit_session_percentage', 'mobile_buy_box_percentage',
            'browser_session_percentage', 'browser_page_views_percentage',
            'browser_unit_session_percentage', 'browser_buy_box_percentage',
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')
        df = df.dropna(subset=['report_date'])
        return df

    except Exception as e:
        st.error(f"❌ Sales & Traffic error: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


# ============================================
# REPORT FUNCTIONS
# ============================================

def show_overview(df_filtered, t, selected_date):
    st.markdown("### 📊 Business Dashboard Overview")
    st.caption(f"Data snapshot: {selected_date}")

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

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        with st.container(border=True):
            st.markdown(f"#### {t['settlements_title']}")
            st.markdown("Actual Payouts, Net Profit, Fees")
            if st.button("🏦 View Finance (Payouts) →", key="btn_settlements", use_container_width=True, type="primary"):
                st.session_state.report_choice = "🏦 Settlements (Payouts)"
                st.rerun()
    with col2:
        with st.container(border=True):
            st.markdown("#### 📈 Sales & Traffic")
            st.markdown("Sessions, Conversions, Buy Box")
            if st.button("📈 View Traffic →", key="btn_sales_traffic", use_container_width=True, type="primary"):
                st.session_state.report_choice = "📈 Sales & Traffic"
                st.rerun()
    with col3:
        with st.container(border=True):
            st.markdown("#### 🛒 Orders Analytics")
            st.markdown("Sales Trends, Top Products")
            if st.button("📊 View Orders Report →", key="btn_orders", use_container_width=True, type="primary"):
                st.session_state.report_choice = "🛒 Orders Analytics"
                st.rerun()
    with col4:
        with st.container(border=True):
            st.markdown("#### 📦 Returns Analytics")
            st.markdown("Return rates, Problem SKUs")
            if st.button("📦 View Returns →", key="btn_returns", use_container_width=True, type="primary"):
                st.session_state.report_choice = "📦 Returns Analytics"
                st.rerun()

    st.markdown("")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        with st.container(border=True):
            st.markdown("#### 💰 Inventory Value")
            st.markdown("Money map, Pricing analytics")
            if st.button("💰 View Inventory Value →", key="btn_finance", use_container_width=True, type="primary"):
                st.session_state.report_choice = "💰 Inventory Value (CFO)"
                st.rerun()
    with col2:
        with st.container(border=True):
            st.markdown("#### 🧠 AI Forecast")
            st.markdown("Sold-out predictions")
            if st.button("🧠 View AI Forecast →", key="btn_ai", use_container_width=True, type="primary"):
                st.session_state.report_choice = "🧠 AI Forecast"
                st.rerun()
    with col3:
        with st.container(border=True):
            st.markdown("#### 🐢 Inventory Health")
            st.markdown("Aging analysis")
            if st.button("🐢 View Health Report →", key="btn_health", use_container_width=True, type="primary"):
                st.session_state.report_choice = "🐢 Inventory Health (Aging)"
                st.rerun()
    with col4:
        with st.container(border=True):
            st.markdown("#### 📋 FBA Data Table")
            st.markdown("Full excel export")
            if st.button("📋 View FBA Data →", key="btn_table", use_container_width=True, type="primary"):
                st.session_state.report_choice = "📋 FBA Inventory Table"
                st.rerun()

    st.markdown("---")
    st.markdown("### 📊 Quick Overview: Top 15 SKU by Stock Level")
    if not df_filtered.empty:
        df_top = df_filtered.nlargest(15, 'Available')
        fig_bar = px.bar(
            df_top, x='Available', y='SKU', orientation='h',
            text='Available', color='Available', color_continuous_scale='Blues'
        )
        fig_bar.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
        st.plotly_chart(fig_bar, use_container_width=True)


def show_sales_traffic(t):
    """📈 Sales & Traffic Report"""

    df_st = load_sales_traffic()

    if df_st.empty:
        st.warning("⚠️ No Sales & Traffic data found.")

        # --- INLINE DEBUG (без кеша) ---
        import psycopg2
        db_url = os.getenv("DATABASE_URL", "")
        st.code(f"DATABASE_URL starts with: {db_url[:30]}...")
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM spapi.sales_traffic")
            count = cur.fetchone()[0]
            st.success(f"✅ psycopg2 connected! spapi.sales_traffic has {count} rows")
            if count > 0:
                cur.execute("SELECT report_date, child_asin, sessions FROM spapi.sales_traffic LIMIT 3")
                rows = cur.fetchall()
                st.write("Sample rows:", rows)
            cur.close()
            conn.close()
        except Exception as e:
            st.error(f"❌ psycopg2 error: {e}")
        return

    # === SIDEBAR FILTERS ===
    st.sidebar.markdown("---")
    st.sidebar.subheader("📈 Sales & Traffic Filters")

    min_date = df_st['report_date'].min().date()
    max_date = df_st['report_date'].max().date()

    date_range = st.sidebar.date_input(
        "📅 Date Range:",
        value=(max(min_date, max_date - dt.timedelta(days=14)), max_date),
        min_value=min_date,
        max_value=max_date,
        key="st_date_range"
    )

    if len(date_range) == 2:
        mask = (df_st['report_date'].dt.date >= date_range[0]) & \
               (df_st['report_date'].dt.date <= date_range[1])
        df_filtered = df_st[mask]
    else:
        df_filtered = df_st

    if df_filtered.empty:
        st.warning("No data for selected period")
        return

    # === KPIs ===
    st.markdown(f"### {t['sales_traffic_title']}")
    st.caption(f"Period: {date_range[0]} → {date_range[1]}" if len(date_range) == 2 else "")

    total_sessions   = int(df_filtered['sessions'].sum())
    total_page_views = int(df_filtered['page_views'].sum())
    total_units      = int(df_filtered['units_ordered'].sum())
    total_revenue    = df_filtered['ordered_product_sales'].sum()
    avg_conversion   = (total_units / total_sessions * 100) if total_sessions > 0 else 0
    avg_buy_box      = df_filtered['buy_box_percentage'].mean()

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric(t["st_sessions"],    f"{total_sessions:,}")
    col2.metric(t["st_page_views"],  f"{total_page_views:,}")
    col3.metric(t["st_units"],       f"{total_units:,}")
    col4.metric(t["st_revenue"],     f"${total_revenue:,.2f}")
    col5.metric(t["st_conversion"],  f"{avg_conversion:.2f}%")
    col6.metric(t["st_buy_box"],     f"{avg_buy_box:.1f}%")

    st.markdown("---")

    # === DAILY TRENDS ===
    st.markdown("### 📈 Daily Trends")

    daily = df_filtered.groupby(df_filtered['report_date'].dt.date).agg({
        'sessions': 'sum',
        'page_views': 'sum',
        'units_ordered': 'sum',
        'ordered_product_sales': 'sum',
    }).reset_index()
    daily.columns = ['Date', 'Sessions', 'Page Views', 'Units', 'Revenue']
    daily['Conversion %'] = (daily['Units'] / daily['Sessions'] * 100).fillna(0)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 👁 Sessions & Page Views")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=daily['Date'], y=daily['Sessions'], name='Sessions', marker_color='#4472C4'))
        fig.add_trace(go.Scatter(x=daily['Date'], y=daily['Page Views'], name='Page Views',
                                 mode='lines+markers', line=dict(color='#ED7D31', width=2), yaxis='y2'))
        fig.update_layout(
            yaxis=dict(title='Sessions'),
            yaxis2=dict(title='Page Views', overlaying='y', side='right'),
            height=380, legend=dict(orientation='h', y=1.12)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### 💰 Revenue & Units")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=daily['Date'], y=daily['Revenue'], name='Revenue $', marker_color='#70AD47'))
        fig.add_trace(go.Scatter(x=daily['Date'], y=daily['Units'], name='Units',
                                 mode='lines+markers', line=dict(color='#FFC000', width=2), yaxis='y2'))
        fig.update_layout(
            yaxis=dict(title='Revenue $'),
            yaxis2=dict(title='Units Ordered', overlaying='y', side='right'),
            height=380, legend=dict(orientation='h', y=1.12)
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### 📊 Conversion Rate Trend")
    fig_conv = go.Figure()
    fig_conv.add_trace(go.Scatter(
        x=daily['Date'], y=daily['Conversion %'],
        mode='lines+markers+text',
        text=[f"{v:.1f}%" for v in daily['Conversion %']],
        textposition='top center',
        line=dict(color='#5B9BD5', width=3),
        marker=dict(size=8),
    ))
    fig_conv.update_layout(height=300, yaxis_title='Conversion %')
    st.plotly_chart(fig_conv, use_container_width=True)

    st.markdown("---")

    # === TOP ASINs ===
    st.markdown("### 🏆 Top ASINs Performance")

    asin_col = 'child_asin' if 'child_asin' in df_filtered.columns else df_filtered.columns[0]

    asin_stats = df_filtered.groupby(asin_col).agg({
        'sessions': 'sum',
        'page_views': 'sum',
        'units_ordered': 'sum',
        'ordered_product_sales': 'sum',
        'buy_box_percentage': 'mean',
    }).reset_index()
    asin_stats.columns = ['ASIN', 'Sessions', 'Page Views', 'Units', 'Revenue', 'Buy Box %']
    asin_stats['Conv %'] = (asin_stats['Units'] / asin_stats['Sessions'] * 100).fillna(0)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 💰 Top 15 by Revenue")
        top_rev = asin_stats.nlargest(15, 'Revenue')
        fig = px.bar(top_rev, x='Revenue', y='ASIN', orientation='h',
                     text='Revenue', color='Revenue', color_continuous_scale='Greens')
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=450)
        fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### 👁 Top 15 by Sessions")
        top_sess = asin_stats.nlargest(15, 'Sessions')
        fig = px.bar(top_sess, x='Sessions', y='ASIN', orientation='h',
                     text='Sessions', color='Sessions', color_continuous_scale='Blues')
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=450)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # === OPPORTUNITY MAP ===
    st.markdown("### 📊 Sessions vs Conversion (Opportunity Map)")
    st.caption("Big circles = more revenue. Red = low conversion, Green = high. Top-right = winners!")

    asin_scatter = asin_stats[asin_stats['Sessions'] > 0].copy()
    if not asin_scatter.empty:
        fig_scatter = px.scatter(
            asin_scatter, x='Sessions', y='Conv %',
            size='Revenue', color='Conv %',
            hover_data=['ASIN', 'Units', 'Revenue', 'Buy Box %'],
            color_continuous_scale='RdYlGn', size_max=40,
        )
        avg_sessions = asin_scatter['Sessions'].median()
        avg_conv     = asin_scatter['Conv %'].median()
        fig_scatter.add_hline(y=avg_conv, line_dash="dash", line_color="gray", opacity=0.5,
                              annotation_text=f"Median Conv: {avg_conv:.1f}%")
        fig_scatter.add_vline(x=avg_sessions, line_dash="dash", line_color="gray", opacity=0.5,
                              annotation_text=f"Median Sessions: {int(avg_sessions)}")
        fig_scatter.update_layout(height=500)
        st.plotly_chart(fig_scatter, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 🔴 High Traffic, Low Conversion (Fix Listing!)")
            problem = asin_scatter[
                (asin_scatter['Sessions'] > avg_sessions) & (asin_scatter['Conv %'] < avg_conv)
            ].sort_values('Sessions', ascending=False).head(10)
            if not problem.empty:
                st.dataframe(
                    problem[['ASIN', 'Sessions', 'Conv %', 'Units', 'Revenue', 'Buy Box %']].style.format({
                        'Conv %': '{:.2f}%', 'Revenue': '${:,.2f}', 'Buy Box %': '{:.1f}%'
                    }),
                    use_container_width=True
                )
            else:
                st.success("No underperformers found!")

        with col2:
            st.markdown("#### 🟢 Stars: High Traffic + High Conversion")
            stars = asin_scatter[
                (asin_scatter['Sessions'] > avg_sessions) & (asin_scatter['Conv %'] > avg_conv)
            ].sort_values('Revenue', ascending=False).head(10)
            if not stars.empty:
                st.dataframe(
                    stars[['ASIN', 'Sessions', 'Conv %', 'Units', 'Revenue', 'Buy Box %']].style.format({
                        'Conv %': '{:.2f}%', 'Revenue': '${:,.2f}', 'Buy Box %': '{:.1f}%'
                    }),
                    use_container_width=True
                )
            else:
                st.info("Not enough data yet")

    st.markdown("---")

    # === MOBILE vs BROWSER ===
    if 'mobile_sessions' in df_filtered.columns and 'browser_sessions' in df_filtered.columns:
        total_mobile  = int(df_filtered['mobile_sessions'].sum())
        total_browser = int(df_filtered['browser_sessions'].sum())
        if total_mobile > 0 or total_browser > 0:
            st.markdown("### 📱 Mobile vs Browser")
            col1, col2 = st.columns(2)
            with col1:
                fig_device = px.pie(
                    values=[total_mobile, total_browser],
                    names=['Mobile App', 'Browser'],
                    hole=0.45,
                    color_discrete_sequence=['#4472C4', '#ED7D31']
                )
                fig_device.update_layout(height=350)
                st.plotly_chart(fig_device, use_container_width=True)
            with col2:
                st.metric("📱 Mobile Sessions",  f"{total_mobile:,}")
                st.metric("💻 Browser Sessions", f"{total_browser:,}")
                mobile_pct = total_mobile / (total_mobile + total_browser) * 100 if (total_mobile + total_browser) > 0 else 0
                st.metric("📱 Mobile Share", f"{mobile_pct:.1f}%")
            st.markdown("---")

    # === BUY BOX ===
    st.markdown("### 🏷 Buy Box Analysis")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### ⚠️ Low Buy Box ASINs (<80%)")
        low_bb = asin_stats[asin_stats['Buy Box %'] < 80].sort_values('Buy Box %').head(15)
        if not low_bb.empty:
            fig = px.bar(low_bb, x='Buy Box %', y='ASIN', orientation='h',
                         color='Buy Box %', color_continuous_scale='RdYlGn', text='Buy Box %')
            fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
            fig.update_traces(texttemplate='%{text:.0f}%', textposition='outside')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.success("All ASINs have Buy Box > 80%! 🎉")
    with col2:
        st.markdown("#### 📊 Buy Box Distribution")
        fig = px.histogram(asin_stats, x='Buy Box %', nbins=20, color_discrete_sequence=['#4472C4'])
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # === FULL TABLE ===
    st.markdown("### 📋 Full ASIN Data")
    asin_display = asin_stats.sort_values('Revenue', ascending=False)
    st.dataframe(
        asin_display.style.format({
            'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Buy Box %': '{:.1f}%',
        }).background_gradient(subset=['Revenue'], cmap='Greens')
         .background_gradient(subset=['Conv %'], cmap='RdYlGn'),
        use_container_width=True, height=500
    )

    csv = asin_display.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Sales & Traffic CSV",
        data=csv,
        file_name=f"sales_traffic_{date_range[0]}_{date_range[1]}.csv" if len(date_range) == 2 else "sales_traffic.csv",
        mime="text/csv"
    )


def show_settlements(t):
    df_settlements = load_settlements()
    if df_settlements.empty:
        st.warning("⚠️ No settlement data found. Please run 'amazon_settlement_loader.py'.")
        return

    st.sidebar.markdown("---")
    st.sidebar.subheader("💰 Settlement Filters")

    currencies = ['All'] + sorted(df_settlements['Currency'].dropna().unique().tolist())
    selected_currency = st.sidebar.selectbox(t["currency_select"], currencies, index=1 if "USD" in currencies else 0)

    min_date = df_settlements['Posted Date'].min().date()
    max_date = df_settlements['Posted Date'].max().date()
    date_range = st.sidebar.date_input(
        "📅 Transaction Date:",
        value=(max_date - dt.timedelta(days=30), max_date),
        min_value=min_date, max_value=max_date
    )

    df_filtered = df_settlements.copy()
    if selected_currency != 'All':
        df_filtered = df_filtered[df_filtered['Currency'] == selected_currency]
    if len(date_range) == 2:
        mask = (df_filtered['Posted Date'].dt.date >= date_range[0]) & \
               (df_filtered['Posted Date'].dt.date <= date_range[1])
        df_filtered = df_filtered[mask]

    if df_filtered.empty:
        st.warning("No data for selected filters")
        return

    st.markdown(f"### {t['settlements_title']}")

    net_payout  = df_filtered['Amount'].sum()
    gross_sales = df_filtered[(df_filtered['Transaction Type'] == 'Order') & (df_filtered['Amount'] > 0)]['Amount'].sum()
    refunds     = df_filtered[df_filtered['Transaction Type'] == 'Refund']['Amount'].sum()
    fees        = df_filtered[(df_filtered['Amount'] < 0) & (df_filtered['Transaction Type'] != 'Refund')]['Amount'].sum()
    currency_symbol = "$" if selected_currency in ['USD', 'CAD', 'All'] else ""

    col1, col2, col3, col4 = st.columns(4)
    col1.metric(t['net_payout'],    f"{currency_symbol}{net_payout:,.2f}")
    col2.metric(t['gross_sales'],   f"{currency_symbol}{gross_sales:,.2f}")
    col3.metric(t['total_refunds'], f"{currency_symbol}{refunds:,.2f}")
    col4.metric(t['total_fees'],    f"{currency_symbol}{fees:,.2f}")

    st.markdown("---")

    col1, col2 = st.columns([2, 1])
    with col1:
        st.subheader(t['chart_payout_trend'])
        daily_trend = df_filtered.groupby(df_filtered['Posted Date'].dt.date)['Amount'].sum().reset_index()
        daily_trend.columns = ['Date', 'Net Amount']
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Bar(
            x=daily_trend['Date'], y=daily_trend['Net Amount'],
            marker_color=daily_trend['Net Amount'].apply(lambda x: 'green' if x >= 0 else 'red'),
        ))
        fig_trend.update_layout(height=400, yaxis_title=f"Net Amount ({selected_currency})")
        st.plotly_chart(fig_trend, use_container_width=True)

    with col2:
        st.subheader(t['chart_fee_breakdown'])
        df_costs = df_filtered[df_filtered['Amount'] < 0]
        if not df_costs.empty:
            cost_breakdown = df_costs.groupby('Transaction Type')['Amount'].sum().abs().reset_index()
            fig_pie = px.pie(cost_breakdown, values='Amount', names='Transaction Type', hole=0.4)
            fig_pie.update_layout(height=400)
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("No costs in selected period")

    st.markdown("#### 📋 Transaction Details")
    display_cols = ['Posted Date', 'Transaction Type', 'Order ID', 'Amount', 'Currency', 'Description']
    available_cols = [c for c in display_cols if c in df_filtered.columns]
    st.dataframe(
        df_filtered[available_cols].sort_values('Posted Date', ascending=False).head(100),
        use_container_width=True
    )


def show_returns():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            df_returns = pd.read_sql(text('SELECT * FROM returns ORDER BY "Return Date" DESC'), conn)
            df_orders  = pd.read_sql(text("SELECT * FROM orders"), conn)
    except Exception as e:
        st.error(f"Error loading returns: {e}")
        return

    if df_returns.empty:
        st.warning("⚠️ No returns data. Run amazon_returns_loader.py")
        return

    df_returns['Return Date'] = pd.to_datetime(df_returns['Return Date'], errors='coerce')
    df_returns['Day of Week'] = df_returns['Return Date'].dt.day_name()

    if 'Price' not in df_returns.columns and not df_orders.empty:
        try:
            price_col_found = None
            for col in ['Item Price', 'item-price', 'item_price', 'price', 'Price']:
                if col in df_orders.columns:
                    price_col_found = col
                    break
            if price_col_found:
                df_orders[price_col_found] = pd.to_numeric(df_orders[price_col_found], errors='coerce')
                price_map = df_orders.groupby('SKU')[price_col_found].mean().to_dict()
                df_returns['Price'] = df_returns['SKU'].map(price_map).fillna(0)
            else:
                df_returns['Price'] = 0
        except:
            df_returns['Price'] = 0
    elif 'Price' not in df_returns.columns:
        df_returns['Price'] = 0

    df_returns['Price']        = pd.to_numeric(df_returns['Price'], errors='coerce').fillna(0)
    df_returns['Quantity']     = pd.to_numeric(df_returns['Quantity'], errors='coerce').fillna(1)
    df_returns['Return Value'] = df_returns['Price'] * df_returns['Quantity']

    st.sidebar.markdown("---")
    st.sidebar.subheader("📦 Returns Filters")

    min_date = df_returns['Return Date'].min().date()
    max_date = df_returns['Return Date'].max().date()
    date_range = st.sidebar.date_input(
        "📅 Return Date:",
        value=(max_date - dt.timedelta(days=30), max_date),
        min_value=min_date, max_value=max_date
    )

    selected_store = 'All'
    if 'Store Name' in df_returns.columns:
        stores = ['All'] + sorted(df_returns['Store Name'].dropna().unique().tolist())
        selected_store = st.sidebar.selectbox("🏪 Store:", stores)

    if len(date_range) == 2:
        mask = (df_returns['Return Date'].dt.date >= date_range[0]) & \
               (df_returns['Return Date'].dt.date <= date_range[1])
        df_filtered = df_returns[mask]
    else:
        df_filtered = df_returns

    if selected_store != 'All':
        df_filtered = df_filtered[df_filtered['Store Name'] == selected_store]

    st.markdown("### 📦 Returns Overview")

    total_returns      = len(df_filtered)
    unique_skus        = df_filtered['SKU'].nunique()
    unique_orders      = df_filtered['Order ID'].nunique()
    total_return_value = df_filtered['Return Value'].sum()
    avg_return_value   = df_filtered['Return Value'].mean()

    return_rate = 0
    try:
        if not df_orders.empty:
            for col in ['Order ID', 'order-id', 'order_id', 'OrderID']:
                if col in df_orders.columns:
                    total_orders = df_orders[col].nunique()
                    return_rate  = (unique_orders / total_orders * 100) if total_orders > 0 else 0
                    break
    except:
        pass

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("📦 Total Returns",  f"{total_returns:,}")
    col2.metric("📦 Unique SKUs",    unique_skus)
    col3.metric("📊 Return Rate",    f"{return_rate:.1f}%")
    col4.metric("💰 Return Value",   f"${total_return_value:,.2f}")
    col5.metric("💵 Avg Return",     f"${avg_return_value:.2f}")

    st.markdown("---")
    st.markdown("### 💰 Financial Impact")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("#### 💵 Return Value by SKU (Top 10)")
        top_value = df_filtered.groupby('SKU')['Return Value'].sum().nlargest(10).reset_index()
        fig = px.bar(top_value, x='Return Value', y='SKU', orientation='h',
                     text='Return Value', color='Return Value', color_continuous_scale='Reds')
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=350)
        fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### 📊 Daily Return Value")
        daily_value = df_filtered.groupby(df_filtered['Return Date'].dt.date)['Return Value'].sum().reset_index()
        daily_value.columns = ['Date', 'Value']
        fig = px.area(daily_value, x='Date', y='Value', line_shape='spline', color_discrete_sequence=['#FF6B6B'])
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col3:
        if 'Reason' in df_filtered.columns:
            st.markdown("#### 💸 Return Value by Reason")
            reason_value = df_filtered.groupby('Reason')['Return Value'].sum().nlargest(8).reset_index()
            fig = px.pie(reason_value, values='Return Value', names='Reason',
                         hole=0.4, color_discrete_sequence=px.colors.sequential.RdBu)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 🏆 Top 15 Returned SKUs")
        top_skus = df_filtered['SKU'].value_counts().head(15).reset_index()
        top_skus.columns = ['SKU', 'Returns']
        fig = px.bar(top_skus, x='Returns', y='SKU', orientation='h',
                     color='Returns', color_continuous_scale='Oranges', text='Returns')
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=450)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        if 'Reason' in df_filtered.columns:
            st.markdown("#### 📊 Return Reasons Distribution")
            reasons = df_filtered['Reason'].value_counts().head(10).reset_index()
            reasons.columns = ['Reason', 'Count']
            fig = px.pie(reasons, values='Count', names='Reason', hole=0.4,
                         color_discrete_sequence=px.colors.sequential.RdBu)
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.markdown("### 📋 Recent Returns (Last 100)")
    display_cols    = ['Return Date', 'SKU', 'Product Name', 'Quantity', 'Price', 'Return Value', 'Reason', 'Status']
    available_cols  = [c for c in display_cols if c in df_filtered.columns]
    st.dataframe(
        df_filtered[available_cols].sort_values('Return Date', ascending=False).head(100).style.format({
            'Price': '${:.2f}', 'Return Value': '${:.2f}'
        }),
        use_container_width=True
    )

    st.markdown("---")
    csv = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Returns Data (CSV)",
        data=csv,
        file_name=f"returns_{date_range[0]}_{date_range[1]}.csv",
        mime="text/csv"
    )


def show_inventory_finance(df_filtered, t):
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
        st.plotly_chart(fig_tree, use_container_width=True)

    st.subheader(t["top_money_sku"])
    df_top = df_filtered[['SKU', 'Product Name', 'Available', 'Price', 'Stock Value']]\
        .sort_values('Stock Value', ascending=False).head(10)
    st.dataframe(df_top.style.format({'Price': "${:.2f}", 'Stock Value': "${:,.2f}"}), use_container_width=True)


def show_aging(df_filtered, t):
    if df_filtered.empty:
        st.warning("No data")
        return

    age_cols       = ['Upto 90 Days', '91 to 180 Days', '181 to 270 Days', '271 to 365 Days', 'More than 365 Days']
    valid_age_cols = [c for c in age_cols if c in df_filtered.columns]

    if not valid_age_cols:
        st.warning("Aging data not available. Check AGED report in ETL.")
        return

    df_age = df_filtered[valid_age_cols].copy()
    for col in valid_age_cols:
        df_age[col] = pd.to_numeric(df_age[col], errors='coerce').fillna(0)

    if df_age.sum().sum() == 0:
        st.info("All inventory is fresh — no aged stock")
        return

    age_sums = df_age.sum().reset_index()
    age_sums.columns = ['Age Group', 'Units']
    age_sums = age_sums[age_sums['Units'] > 0]

    col1, col2 = st.columns(2)
    with col1:
        st.subheader(t["chart_age"])
        fig_pie = px.pie(age_sums, values='Units', names='Age Group', hole=0.4)
        fig_pie.update_layout(height=400)
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        st.subheader(t["chart_velocity"])
        if all(c in df_filtered.columns for c in ['Available', 'Velocity', 'Stock Value']):
            df_scatter = df_filtered[
                (df_filtered['Available'] > 0) &
                (df_filtered['Velocity'] >= 0) &
                (df_filtered['Stock Value'] > 0)
            ].copy()
            if not df_scatter.empty:
                fig_scatter = px.scatter(
                    df_scatter, x='Available', y='Velocity', size='Stock Value',
                    color='Store Name' if 'Store Name' in df_scatter.columns else None,
                    hover_name='SKU', log_x=True
                )
                fig_scatter.update_layout(height=400)
                st.plotly_chart(fig_scatter, use_container_width=True)


def show_ai_forecast(df, t):
    st.markdown("### Select SKU for Forecast")
    skus = sorted(df['SKU'].unique())
    if not skus:
        st.info("No SKU available")
        return

    col1, col2 = st.columns([2, 1])
    target_sku    = col1.selectbox(t["ai_select"], skus)
    forecast_days = col2.slider(t["ai_days"], 7, 90, 30)

    sku_data = df[df['SKU'] == target_sku].copy().sort_values('created_at')
    sku_data['date_ordinal'] = sku_data['created_at'].map(dt.datetime.toordinal)

    if len(sku_data) >= 3:
        X = sku_data[['date_ordinal']]
        y = sku_data['Available']
        model = LinearRegression().fit(X, y)

        last_date    = sku_data['created_at'].max()
        future_dates = [last_date + dt.timedelta(days=x) for x in range(1, forecast_days + 1)]
        future_ord   = np.array([d.toordinal() for d in future_dates]).reshape(-1, 1)
        predictions  = [max(0, int(p)) for p in model.predict(future_ord)]

        df_forecast = pd.DataFrame({'date': future_dates, 'Predicted': predictions})
        sold_out = df_forecast[df_forecast['Predicted'] == 0]
        if not sold_out.empty:
            st.error(f"{t['ai_result_date']} **{sold_out.iloc[0]['date'].date()}**")
        else:
            st.success(t['ai_ok'])

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=sku_data['created_at'], y=sku_data['Available'], name='Historical'))
        fig.add_trace(go.Scatter(x=df_forecast['date'], y=df_forecast['Predicted'],
                                 name='Forecast', line=dict(dash='dash', color='red')))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning(t["ai_error"])


def show_data_table(df_filtered, t, selected_date):
    st.markdown("### 📊 FBA Inventory Dataset")
    csv = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button(label="📥 Download CSV", data=csv, file_name="fba_inventory.csv", mime="text/csv")
    st.dataframe(df_filtered, use_container_width=True, height=600)


def show_orders():
    df_orders = load_orders()
    if df_orders.empty:
        st.warning("⚠️ No orders data. Run amazon_orders_loader.py")
        return

    st.sidebar.markdown("---")
    st.sidebar.subheader("🛒 Orders Filters")

    min_date = df_orders['Order Date'].min().date()
    max_date = df_orders['Order Date'].max().date()
    date_range = st.sidebar.date_input(
        "📅 Date Range:",
        value=(max_date - dt.timedelta(days=7), max_date),
        min_value=min_date, max_value=max_date
    )

    if len(date_range) == 2:
        df_filtered = df_orders[
            (df_orders['Order Date'].dt.date >= date_range[0]) &
            (df_orders['Order Date'].dt.date <= date_range[1])
        ]
    else:
        df_filtered = df_orders

    col1, col2, col3 = st.columns(3)
    col1.metric("📦 Orders",  df_filtered['Order ID'].nunique())
    col2.metric("💰 Revenue", f"${df_filtered['Total Price'].sum():,.2f}")
    col3.metric("📦 Items",   int(df_filtered['Quantity'].sum()))

    st.markdown("#### 📈 Daily Revenue")
    daily = df_filtered.groupby(df_filtered['Order Date'].dt.date)['Total Price'].sum().reset_index()
    fig = px.bar(daily, x='Order Date', y='Total Price', title="Daily Revenue")
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 🏆 Top 10 SKU by Revenue")
        top_sku = df_filtered.groupby('SKU')['Total Price'].sum().nlargest(10).reset_index()
        fig2 = px.bar(top_sku, x='Total Price', y='SKU', orientation='h')
        fig2.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig2, use_container_width=True)
    with col2:
        if 'Order Status' in df_filtered.columns:
            st.markdown("#### 📊 Order Status Distribution")
            status_counts = df_filtered['Order Status'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Count']
            fig3 = px.pie(status_counts, values='Count', names='Status', hole=0.4)
            st.plotly_chart(fig3, use_container_width=True)


# ============================================
# MAIN
# ============================================

if 'report_choice' not in st.session_state:
    st.session_state.report_choice = "🏠 Overview"

lang_option = st.sidebar.selectbox("🌍 Language", ["UA 🇺🇦", "EN 🇺🇸", "RU 🌍"], index=0)
lang = "UA" if "UA" in lang_option else "EN" if "EN" in lang_option else "RU"
t = translations[lang]

if st.sidebar.button(t["update_btn"], use_container_width=True):
    st.cache_data.clear()
    st.rerun()

df = load_data()

if not df.empty:
    for col in ['Available', 'Price', 'Velocity', 'Stock Value']:
        if col not in df.columns:
            df[col] = 0
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
    df_filtered   = pd.DataFrame()
    selected_date = None

st.sidebar.markdown("---")
st.sidebar.header("📊 Reports")
report_options = [
    "🏠 Overview",
    "📈 Sales & Traffic",
    "🏦 Settlements (Payouts)",
    "💰 Inventory Value (CFO)",
    "🛒 Orders Analytics",
    "📦 Returns Analytics",
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
elif report_choice == "📈 Sales & Traffic":
    show_sales_traffic(t)
elif report_choice == "🏦 Settlements (Payouts)":
    show_settlements(t)
elif report_choice == "💰 Inventory Value (CFO)":
    show_inventory_finance(df_filtered, t)
elif report_choice == "🛒 Orders Analytics":
    show_orders()
elif report_choice == "📦 Returns Analytics":
    show_returns()
elif report_choice == "🐢 Inventory Health (Aging)":
    show_aging(df_filtered, t)
elif report_choice == "🧠 AI Forecast":
    show_ai_forecast(df, t)
elif report_choice == "📋 FBA Inventory Table":
    show_data_table(df_filtered, t, selected_date)

st.sidebar.markdown("---")
st.sidebar.caption("📦 Amazon FBA BI System v3.1")
