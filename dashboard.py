import streamlit as st
import pandas as pd
import psycopg2
import os
import plotly.express as px
import io

st.set_page_config(page_title="Amazon FBA Inventory", layout="wide")

# --- СЛОВНИК ПЕРЕКЛАДІВ (UA / EN / RU) ---
translations = {
    "UA": {
        "title": "📦 Amazon FBA Склад",
        "update_btn": "🔄 Оновити дані",
        "sidebar_title": "🔍 Фільтри",
        "date_label": "📅 Дата:",
        "store_label": "🏪 Магазин:",
        "all_stores": "Всі",
        "tab1": "📊 Головний Дашборд",
        "tab2": "📋 Детальна Таблиця (Excel)",
        "tab3": "📈 Аналітика та Тренди",
        "summary": "Зведення за",
        "total_sku": "Всього SKU",
        "total_avail": "Всього Доступно",
        "total_inbound": "В дорозі (Inbound)",
        "total_reserved": "В резерві",
        "top_chart": "🏆 Top 15 товарів по залишках",
        "table_header": "📋 Повний список інвентарю",
        "download_excel": "📥 Завантажити Excel",
        "chart_history": "📈 Динаміка залишків",
        "chart_sku": "🔍 Аналіз конкретного SKU",
        "select_sku": "Виберіть SKU:",
        "no_data": "Немає даних",
        "footer_date": "📅 Останнє оновлення:",
        # Назви колонок для таблиці
        "col_sku": "SKU",
        "col_name": "Назва товару",
        "col_avail": "Доступно",
        "col_inbound": "Їде (Inbound)",
        "col_reserved": "Резерв",
        "col_days": "Днів запасу"
    },
    "EN": {
        "title": "📦 Amazon FBA Inventory",
        "update_btn": "🔄 Refresh Data",
        "sidebar_title": "🔍 Filters",
        "date_label": "📅 Date:",
        "store_label": "🏪 Store:",
        "all_stores": "All",
        "tab1": "📊 Main Dashboard",
        "tab2": "📋 Detailed Table (Excel)",
        "tab3": "📈 Analytics & Trends",
        "summary": "Summary for",
        "total_sku": "Total SKU",
        "total_avail": "Total Available",
        "total_inbound": "Total Inbound",
        "total_reserved": "Total Reserved",
        "top_chart": "🏆 Top 15 SKU by Availability",
        "table_header": "📋 Full Inventory List",
        "download_excel": "📥 Download Excel",
        "chart_history": "📈 Inventory Dynamics",
        "chart_sku": "🔍 Specific SKU Analysis",
        "select_sku": "Select SKU:",
        "no_data": "No data",
        "footer_date": "📅 Last update:",
        "col_sku": "SKU",
        "col_name": "Product Name",
        "col_avail": "Available",
        "col_inbound": "Inbound",
        "col_reserved": "Reserved",
        "col_days": "Days of Supply"
    },
    "RU": {
        "title": "📦 Amazon FBA Склад",
        "update_btn": "🔄 Обновить данные",
        "sidebar_title": "🔍 Фильтры",
        "date_label": "📅 Дата:",
        "store_label": "🏪 Магазин:",
        "all_stores": "Все",
        "tab1": "📊 Главный Дашборд",
        "tab2": "📋 Таблица (Excel)",
        "tab3": "📈 Аналитика и Тренды",
        "summary": "Сводка за",
        "total_sku": "Всего SKU",
        "total_avail": "Всего Доступно",
        "total_inbound": "В пути (Inbound)",
        "total_reserved": "В резерве",
        "top_chart": "🏆 Top 15 товаров по остаткам",
        "table_header": "📋 Полный список инвентаря",
        "download_excel": "📥 Скачать Excel",
        "chart_history": "📈 Динамика остатков",
        "chart_sku": "🔍 Анализ конкретного SKU",
        "select_sku": "Выберите SKU:",
        "no_data": "Нет данных",
        "footer_date": "📅 Последнее обновление:",
        "col_sku": "SKU",
        "col_name": "Название товара",
        "col_avail": "Доступно",
        "col_inbound": "В пути",
        "col_reserved": "Резерв",
        "col_days": "Дней запаса"
    }
}

# --- ВИБІР МОВИ ---
lang_option = st.sidebar.selectbox("Language / Мова / Язык", ["UA 🇺🇦", "EN 🇺🇸", "RU 🌍"], index=0)
if "UA" in lang_option: lang = "UA"
elif "EN" in lang_option: lang = "EN"
else: lang = "RU"

# Отримуємо тексти для вибраної мови
t = translations[lang]

st.title(t["title"])

DATABASE_URL = os.getenv("DATABASE_URL")

@st.cache_data(ttl=60)
def load_data():
    conn = psycopg2.connect(DATABASE_URL)
    df = pd.read_sql("SELECT * FROM fba_inventory ORDER BY created_at DESC", conn)
    conn.close()
    return df

# Кнопка оновлення
if st.button(t["update_btn"]):
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
st.sidebar.header(t["sidebar_title"])

dates = sorted(df['date'].unique(), reverse=True)
selected_date = st.sidebar.selectbox(t["date_label"], dates, index=0)

previous_date = None
if len(dates) > 1:
    try:
        current_index = dates.index(selected_date)
        if current_index + 1 < len(dates):
            previous_date = dates[current_index + 1]
    except ValueError:
        pass

stores = [t["all_stores"]] + list(df['Store Name'].unique())
selected_store = st.sidebar.selectbox(t["store_label"], stores)

df_filtered = df[df['date'] == selected_date]
df_prev = df[df['date'] == previous_date] if previous_date else pd.DataFrame()

if selected_store != t["all_stores"]:
    df_filtered = df_filtered[df_filtered['Store Name'] == selected_store]
    if not df_prev.empty:
        df_prev = df_prev[df_prev['Store Name'] == selected_store]

# --- ВКЛАДКИ ---
tab1, tab2, tab3 = st.tabs([t["tab1"], t["tab2"], t["tab3"]])

# === TAB 1: DASHBOARD ===
with tab1:
    st.subheader(f"{t['summary']} {selected_date}")
    
    curr_avail = int(df_filtered['Available'].sum())
    curr_inbound = int(df_filtered['Inbound'].sum())
    curr_reserved = int(df_filtered['FBA Reserved Quantity'].sum())
    
    delta_avail = (curr_avail - int(df_prev['Available'].sum())) if not df_prev.empty else 0
    delta_inbound = (curr_inbound - int(df_prev['Inbound'].sum())) if not df_prev.empty else 0
    delta_reserved = (curr_reserved - int(df_prev['FBA Reserved Quantity'].sum())) if not df_prev.empty else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric(t["total_sku"], len(df_filtered))
    col2.metric(t["total_avail"], curr_avail, delta=delta_avail)
    col3.metric(t["total_inbound"], curr_inbound, delta=delta_inbound)
    col4.metric(t["total_reserved"], curr_reserved, delta=delta_reserved)

    st.markdown("---")
    
    st.subheader(t["top_chart"])
    top15 = df_filtered.nlargest(15, 'Available')
    
    fig_bar = px.bar(
        top15, 
        x='Available', 
        y='SKU', 
        orientation='h',
        text='Available',
        hover_data=['Product Name'],
        title=t["top_chart"],
        color='Available',
        color_continuous_scale='Blues'
    )
    fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig_bar, use_container_width=True)

# === TAB 2: TABLE ===
with tab2:
    col_t1, col_t2 = st.columns([3, 1])
    with col_t1:
        st.subheader(t["table_header"])
    
    with col_t2:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            export_cols = ['SKU', 'ASIN', 'Product Name', 'Available', 'Inbound', 'FBA Reserved Quantity', 'Total Quantity', 'Days of Supply']
            final_export_cols = [c for c in export_cols if c in df_filtered.columns]
            df_filtered[final_export_cols].to_excel(writer, index=False, sheet_name='Inventory')
            
        buffer.seek(0)
        st.download_button(
            label=t["download_excel"],
            data=buffer,
            file_name=f"inventory_{selected_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    def highlight_stock(val):
        if val == 0:
            return 'background-color: #ffcccc; color: black'
        elif val < 10:
            return 'background-color: #ffffcc; color: black'
        return ''

    # Перейменування колонок для відображення
    display_map = {
        'SKU': t['col_sku'],
        'Product Name': t['col_name'],
        'Available': t['col_avail'],
        'Inbound': t['col_inbound'],
        'FBA Reserved Quantity': t['col_reserved'],
        'Days of Supply': t['col_days'],
        'ASIN': 'ASIN'
    }
    
    # Створюємо копію для відображення
    show_df = df_filtered.copy()
    existing_cols = [c for c in display_map.keys() if c in show_df.columns]
    show_df = show_df[existing_cols].rename(columns=display_map)
    
    # Підсвітка по перейменованій колонці
    st.dataframe(
        show_df.style.applymap(highlight_stock, subset=[t['col_avail']]),
        use_container_width=True,
        height=800
    )

# === TAB 3: HISTORY ===
with tab3:
    col_hist1, col_hist2 = st.columns([2, 1])
    
    with col_hist1:
        st.subheader(t["chart_history"])
        
        if selected_store != t["all_stores"]:
            df_history = df[df['Store Name'] == selected_store]
        else:
            df_history = df

        daily_totals = df_history.groupby('date').agg({
            'Available': 'sum',
            'Inbound': 'sum',
            'FBA Reserved Quantity': 'sum'
        }).reset_index().sort_values('date')

        # Перейменування легенди графіка
        rename_dict = {'Available': t['total_avail'], 'Inbound': t['total_inbound']}
        
        fig_line = px.line(
            daily_totals, 
            x='date', 
            y=['Available', 'Inbound'], 
            markers=True,
            title=t["chart_history"]
        )
        # Оновлюємо назви в легенді
        new_names = {k: v for k, v in rename_dict.items()}
        fig_line.for_each_trace(lambda t: t.update(name = new_names.get(t.name, t.name)))
        
        st.plotly_chart(fig_line, use_container_width=True)

    with col_hist2:
        st.subheader(t["chart_sku"])
        skus = sorted(df['SKU'].unique())
        selected_sku = st.selectbox(t["select_sku"], skus)

        sku_history = df[df['SKU'] == selected_sku][['date', 'Available', 'Inbound', 'Total Quantity']]
        sku_history = sku_history.groupby('date').first().reset_index().sort_values('date')

        if not sku_history.empty:
            st.metric(f"{t['col_avail']}", int(sku_history.iloc[-1]['Available']))
            fig_sku = px.area(sku_history, x='date', y='Available', title=f"{selected_sku}")
            st.plotly_chart(fig_sku, use_container_width=True)
        else:
            st.info(t["no_data"])

st.sidebar.markdown("---")
st.sidebar.info(f"{t['footer_date']} {dates[0] if dates else '-'}")
