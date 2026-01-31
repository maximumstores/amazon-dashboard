import streamlit as st
import pandas as pd
import psycopg2
import os
import plotly.express as px  # Добавили библиотеку для красивых графиков

st.set_page_config(page_title="Amazon FBA Inventory", layout="wide")
st.title("📦 Amazon FBA Inventory Dashboard")

DATABASE_URL = os.getenv("DATABASE_URL")

@st.cache_data(ttl=60)
def load_data():
    conn = psycopg2.connect(DATABASE_URL)
    df = pd.read_sql("SELECT * FROM fba_inventory ORDER BY created_at DESC", conn)
    conn.close()
    return df

# Кнопка оновлення
if st.button("🔄 Оновити дані"):
    st.cache_data.clear()
    st.rerun()

df = load_data()

# --- ПІДГОТОВКА ДАНИХ ---
df['Available'] = pd.to_numeric(df['Available'], errors='coerce').fillna(0)
df['Inbound'] = pd.to_numeric(df['Inbound'], errors='coerce').fillna(0)
df['FBA Reserved Quantity'] = pd.to_numeric(df['FBA Reserved Quantity'], errors='coerce').fillna(0)
df['Total Quantity'] = pd.to_numeric(df['Total Quantity'], errors='coerce').fillna(0)
df['created_at'] = pd.to_datetime(df['created_at'])
df['date'] = df['created_at'].dt.date

# --- SIDEBAR ФІЛЬТРИ ---
st.sidebar.header("🔍 Фільтри")

# Фільтр по даті
dates = sorted(df['date'].unique(), reverse=True)
selected_date = st.sidebar.selectbox("📅 Дата:", dates, index=0)

# Логіка для порівняння з попередньою датою (Delta)
previous_date = None
if len(dates) > 1:
    try:
        current_index = dates.index(selected_date)
        if current_index + 1 < len(dates):
            previous_date = dates[current_index + 1]
    except ValueError:
        pass

# Фільтр по магазину
stores = ["Всі"] + list(df['Store Name'].unique())
selected_store = st.sidebar.selectbox("🏪 Магазин:", stores)

# Фільтруємо дані
df_filtered = df[df['date'] == selected_date]
df_prev = df[df['date'] == previous_date] if previous_date else pd.DataFrame()

if selected_store != "Всі":
    df_filtered = df_filtered[df_filtered['Store Name'] == selected_store]
    if not df_prev.empty:
        df_prev = df_prev[df_prev['Store Name'] == selected_store]

# --- ВКЛАДКИ (TABS) ---
tab1, tab2, tab3 = st.tabs(["📊 Головний Дашборд", "📋 Детальна Таблиця", "📈 Аналітика та Тренди"])

# === TAB 1: DASHBOARD ===
with tab1:
    st.subheader(f"Зведення за {selected_date}")
    
    # Розрахунок метрик і різниці (Delta)
    curr_avail = int(df_filtered['Available'].sum())
    curr_inbound = int(df_filtered['Inbound'].sum())
    curr_reserved = int(df_filtered['FBA Reserved Quantity'].sum())
    
    delta_avail = (curr_avail - int(df_prev['Available'].sum())) if not df_prev.empty else 0
    delta_inbound = (curr_inbound - int(df_prev['Inbound'].sum())) if not df_prev.empty else 0
    delta_reserved = (curr_reserved - int(df_prev['FBA Reserved Quantity'].sum())) if not df_prev.empty else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Всього SKU", len(df_filtered))
    col2.metric("Total Available", curr_avail, delta=delta_avail)
    col3.metric("Total Inbound", curr_inbound, delta=delta_inbound)
    col4.metric("Total Reserved", curr_reserved, delta=delta_reserved)

    st.markdown("---")
    
    # Графік Top 10 (Plotly - інтерактивний)
    st.subheader("🏆 Top 15 товарів по залишках")
    top15 = df_filtered.nlargest(15, 'Available')
    
    fig_bar = px.bar(
        top15, 
        x='Available', 
        y='SKU', 
        orientation='h',
        text='Available',
        hover_data=['Product Name'],
        title="Топ SKU на складі",
        color='Available',
        color_continuous_scale='Blues'
    )
    fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}) # Сортування
    st.plotly_chart(fig_bar, use_container_width=True)

# === TAB 2: TABLE (INVENTORY) ===
with tab2:
    st.subheader("📋 Повний список інвентарю")
    
    # Функція для розфарбовування (Conditional Formatting)
    def highlight_stock(val):
        if val == 0:
            return 'background-color: #ffcccc; color: black' # Червоний для 0
        elif val < 10:
            return 'background-color: #ffffcc; color: black' # Жовтий для малого залишку
        return ''

    # Показуємо таблицю з кольорами
    # Вибираємо потрібні колонки
    display_cols = ['SKU', 'ASIN', 'Product Name', 'Available', 'Inbound', 'FBA Reserved Quantity', 'Days of Supply']
    
    # Перевіряємо чи є колонка 'Days of Supply' в датафреймі, щоб не було помилки
    final_cols = [c for c in display_cols if c in df_filtered.columns]
    
    st.dataframe(
        df_filtered[final_cols].style.applymap(highlight_stock, subset=['Available']),
        use_container_width=True,
        height=800
    )

# === TAB 3: HISTORY ===
with tab3:
    col_hist1, col_hist2 = st.columns([2, 1])
    
    with col_hist1:
        st.subheader("📈 Динаміка залишків (Всі дні)")
        
        # Групуємо по датах
        if selected_store != "Всі":
            df_history = df[df['Store Name'] == selected_store]
        else:
            df_history = df

        daily_totals = df_history.groupby('date').agg({
            'Available': 'sum',
            'Inbound': 'sum',
            'FBA Reserved Quantity': 'sum'
        }).reset_index().sort_values('date')

        # Красивий графік Plotly Line Chart
        fig_line = px.line(
            daily_totals, 
            x='date', 
            y=['Available', 'Inbound'], 
            markers=True,
            title="Загальна динаміка стоку"
        )
        st.plotly_chart(fig_line, use_container_width=True)

    with col_hist2:
        st.subheader("🔍 Аналіз конкретного SKU")
        skus = sorted(df['SKU'].unique())
        selected_sku = st.selectbox("Введіть або виберіть SKU:", skus)

        sku_history = df[df['SKU'] == selected_sku][['date', 'Available', 'Inbound', 'Total Quantity']]
        sku_history = sku_history.groupby('date').first().reset_index().sort_values('date')

        if not sku_history.empty:
            st.metric("Поточний Available", int(sku_history.iloc[-1]['Available']))
            
            fig_sku = px.area(
                sku_history, 
                x='date', 
                y='Available', 
                title=f"Історія {selected_sku}"
            )
            st.plotly_chart(fig_sku, use_container_width=True)
        else:
            st.info("Немає даних")

# Футер
st.sidebar.markdown("---")
st.sidebar.info(f"📅 Останнє оновлення: {dates[0] if dates else 'Н/Д'}")
