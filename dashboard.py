import streamlit as st
import pandas as pd
import psycopg2
import os

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

# Конвертуємо типи
df['Available'] = pd.to_numeric(df['Available'], errors='coerce').fillna(0)
df['Inbound'] = pd.to_numeric(df['Inbound'], errors='coerce').fillna(0)
df['FBA Reserved Quantity'] = pd.to_numeric(df['FBA Reserved Quantity'], errors='coerce').fillna(0)
df['Total Quantity'] = pd.to_numeric(df['Total Quantity'], errors='coerce').fillna(0)
df['created_at'] = pd.to_datetime(df['created_at'])
df['date'] = df['created_at'].dt.date

# Sidebar фільтри
st.sidebar.header("🔍 Фільтри")

# Фільтр по даті
dates = sorted(df['date'].unique(), reverse=True)
selected_date = st.sidebar.selectbox("📅 Дата:", dates, index=0)

# Фільтр по магазину
stores = ["Всі"] + list(df['Store Name'].unique())
selected_store = st.sidebar.selectbox("🏪 Магазин:", stores)

# Фільтруємо дані
df_filtered = df[df['date'] == selected_date]
if selected_store != "Всі":
    df_filtered = df_filtered[df_filtered['Store Name'] == selected_store]

# Метрики поточного дня
st.subheader(f"📊 Дані за {selected_date}")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Всього SKU", len(df_filtered))
col2.metric("Total Available", int(df_filtered['Available'].sum()))
col3.metric("Total Inbound", int(df_filtered['Inbound'].sum()))
col4.metric("Total Reserved", int(df_filtered['FBA Reserved Quantity'].sum()))

# Таблиця
st.subheader("📋 Інвентар")
st.dataframe(
    df_filtered[['SKU', 'ASIN', 'Product Name', 'Available', 'Inbound', 'FBA Reserved Quantity', 'Days of Supply']],
    use_container_width=True
)

# Графік Top 10
st.subheader("📊 Top 10 по Available")
top10 = df_filtered.nlargest(10, 'Available')[['SKU', 'Available']]
st.bar_chart(top10.set_index('SKU'))

# === ІСТОРІЯ / ТРЕНДИ ===
st.subheader("📈 Тренд загального стоку")

# Групуємо по датах
if selected_store != "Всі":
    df_history = df[df['Store Name'] == selected_store]
else:
    df_history = df

daily_totals = df_history.groupby('date').agg({
    'Available': 'sum',
    'Inbound': 'sum',
    'FBA Reserved Quantity': 'sum'
}).reset_index()

daily_totals = daily_totals.sort_values('date')

st.line_chart(daily_totals.set_index('date')[['Available', 'Inbound']])

# Тренд по конкретному SKU
st.subheader("📈 Тренд по SKU")
skus = sorted(df['SKU'].unique())
selected_sku = st.selectbox("Виберіть SKU:", skus)

sku_history = df[df['SKU'] == selected_sku][['date', 'Available', 'Inbound', 'Total Quantity']]
sku_history = sku_history.groupby('date').first().reset_index().sort_values('date')

if len(sku_history) > 1:
    st.line_chart(sku_history.set_index('date')[['Available', 'Inbound']])
else:
    st.info("Недостатньо даних для графіка (потрібно більше 1 дня)")

# Футер
st.sidebar.markdown("---")
st.sidebar.info(f"📅 Дат в базі: {len(dates)}\n\n📦 Всього записів: {len(df)}")
