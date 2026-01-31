import streamlit as st
import pandas as pd
import psycopg2
import os

st.set_page_config(page_title="Amazon FBA Inventory", layout="wide")
st.title("📦 Amazon FBA Inventory Dashboard")

DATABASE_URL = os.getenv("DATABASE_URL")

@st.cache_data
def load_data():
    conn = psycopg2.connect(DATABASE_URL)
    df = pd.read_sql("SELECT * FROM fba_inventory", conn)
    conn.close()
    return df

df = load_data()

# Конвертуємо в числа
df['Available'] = pd.to_numeric(df['Available'], errors='coerce').fillna(0)
df['Inbound'] = pd.to_numeric(df['Inbound'], errors='coerce').fillna(0)
df['FBA Reserved Quantity'] = pd.to_numeric(df['FBA Reserved Quantity'], errors='coerce').fillna(0)

# Метрики
col1, col2, col3, col4 = st.columns(4)
col1.metric("Всього SKU", len(df))
col2.metric("Total Available", int(df['Available'].sum()))
col3.metric("Total Inbound", int(df['Inbound'].sum()))
col4.metric("Total Reserved", int(df['FBA Reserved Quantity'].sum()))

# Фільтр по магазину
stores = df['Store Name'].unique()
selected_store = st.selectbox("Виберіть магазин:", ["Всі"] + list(stores))

if selected_store != "Всі":
    df = df[df['Store Name'] == selected_store]

# Таблиця
st.subheader("📋 Інвентар")
st.dataframe(df[['SKU', 'ASIN', 'Product Name', 'Available', 'Inbound', 'FBA Reserved Quantity', 'Days of Supply']], use_container_width=True)

# Графік
st.subheader("📊 Top 10 по Available")
top10 = df.nlargest(10, 'Available')[['SKU', 'Available']]
st.bar_chart(top10.set_index('SKU'))
