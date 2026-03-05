import streamlit as st
import pandas as pd
import os
import re
import psycopg2
import requests
import threading
import queue
import time
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import datetime as dt
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
try:
    import google.generativeai as genai  # TODO: migrate to google.genai
    GEMINI_OK = True
except ImportError:
    GEMINI_OK = False

load_dotenv()

def ensure_ai_chat_table():
    """Створює таблицю ai_chat_history якщо не існує."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ai_chat_history (
                    id          SERIAL PRIMARY KEY,
                    session_id  TEXT NOT NULL,
                    username    TEXT,
                    section     TEXT,
                    role        TEXT,  -- 'user' або 'assistant'
                    message     TEXT,
                    created_at  TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.commit()
    except Exception as e:
        pass  # не критично якщо не вдалось

def save_chat_message(session_id, username, section, role, message):
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO ai_chat_history (session_id, username, section, role, message)
                VALUES (:sid, :user, :sec, :role, :msg)
            """), {"sid": session_id, "user": username, "sec": section, "role": role, "msg": message})
            conn.commit()
    except Exception:
        pass

def load_chat_history(session_id, section):
    try:
        engine = get_engine()
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT role, message FROM ai_chat_history
                WHERE session_id = :sid AND section = :sec
                ORDER BY created_at ASC LIMIT 50
            """), {"sid": session_id, "sec": section}).fetchall()
        return [{"role": r[0], "content": r[1]} for r in rows]
    except Exception:
        return []


st.set_page_config(page_title="Amazon FBA Ultimate BI", layout="wide", page_icon="📦")
ensure_ai_chat_table()

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
        "reviews_title": "⭐ Відгуки покупців",
        "total_reviews": "Всього відгуків",
        "avg_review_rating": "Середній рейтинг",
        "verified_pct": "Верифіковані (%)",
        "star_dist": "Розподіл по зірках",
        "worst_asin": "Проблемні ASIN (1-2★)",
        "ov_title": "📊 Огляд бізнесу",
        "ov_top_sku": "### 📊 Топ 15 SKU за залишками",
        "st_daily_trends": "### 📈 Щоденна динаміка",
        "st_sessions_views": "#### 👁 Сесії та перегляди",
        "st_revenue_units": "#### 💰 Дохід та замовлення",
        "st_top_asins": "### 🏆 Топ ASINи",
        "st_top_revenue": "#### 💰 Топ 15 за доходом",
        "st_top_sessions": "#### 👁 Топ 15 за сесіями",
        "st_full_data": "### 📋 Всі дані по ASINах",
        "st_download": "📥 Завантажити CSV",
        "st_filters": "📈 Фільтри Sales & Traffic",
        "st_date_range": "📅 Діапазон дат:",
        "ret_title": "### 📦 Огляд повернень",
        "ret_total": "📦 Всього повернень",
        "ret_unique_sku": "📦 Унікальних SKU",
        "ret_rate": "📊 Рівень повернень",
        "ret_value": "💰 Вартість повернень",
        "ret_avg": "💵 Сер. вартість",
        "ret_by_sku": "#### 💵 Вартість по SKU (Топ 10)",
        "ret_daily": "#### 📊 Щоденна вартість",
        "ret_by_reason": "#### 💸 По причинах",
        "ret_top_sku": "#### 🏆 Топ 15 SKU повернень",
        "ret_reasons": "#### 📊 Причини повернень",
        "ret_filters": "📦 Фільтри повернень",
        "ret_date": "📅 Дата повернення:",
        "ret_download": "📥 Завантажити CSV повернень",
        "ord_title": "### 🛒 Аналітика замовлень",
        "ins_neg_trend": "Тренд негативу",
        "ins_verified": "Верифікація",
        "ins_pos_rate": "Рівень позитиву",
        "rev_auto_insights": "🧠 Автоінсайти",
        "rev_worst_asin": "🔴 НАЙГІРШИЙ ASIN",
        "rev_best_asin": "🟢 НАЙКРАЩИЙ ASIN",
        "rev_worst_country": "🔴 НАЙГІРША КРАЇНА",
        "rev_best_country": "🟢 НАЙКРАЩА КРАЇНА",
        "rev_reviews_count": "відг.",
        "rev_main_asin": "📦 Головний:",
        "rev_heatmap": "### 🔥 Теплова карта: ASIN × Країна",
        "rev_heatmap_hint": "Клікни на ASIN у таблиці нижче — відкриється його сторінка на Amazon",
        "rev_asin_compare": "### 📊 Порівняння ASINів",
        "rev_star_dist": "### 📊 Загальний розподіл зірок",
        "rev_texts": "### 📋 Тексти відгуків (до 100 на кожну зірку, max 500)",
        "rev_sort_hint": "Сортування: спочатку 1★ — щоб проблеми були першими",
        "rev_dl_balanced": "📥 Скачати по фільтру",
        "rev_dl_all": "📥 Скачати все з бази",
        "rev_dl_balanced_hint": "Вибрані ASIN / країна — до 100 відгуків на зірку",
        "rev_dl_all_hint": "Всі відгуки з бази даних без обмежень",
        "rev_shown": "Показано {n} з {total} відгуків",
        "rev_click_hint": "👆 Клікни на рядок — побачиш детальний аналіз цього ASIN · Посилання відкриють Amazon у новій вкладці",
        "rev_select_hint": "👇 Вибери ASIN для детального аналізу:",
        "rev_goto_asin": "📦 Перейти до ASIN:",
        "rev_not_selected": "— не вибрано —",
        "rev_back": "← Назад до всіх ASINів",
        "about_title": "## ℹ️ Про Merino BI Dashboard",
        "about_caption": "Amazon Intelligence Platform — повний контроль над FBA бізнесом в одному місці",
        "about_modules": "### 📦 Модулі системи",
        "about_pipeline": "### ⚙️ Data Pipeline",
        "about_features": "**✅ Особливості**",
        "about_stack": "**🔧 Технічний стек**",
        "about_footer": "MR.EQUIPP LIMITED · Built with obsession · v5.0",
        "rev_asins_in_filter": "📦 ASINів у фільтрі",
        "insights_title": "🧠 Інсайти",
        "insight_rating_health": "Здоров'я рейтингу",
        "insight_loyalty": "Лояльність",
        "insight_toxic": "Токсичний ASIN",
        "insight_neg_level": "Рівень негативу",
        "insight_verified": "Верифікація",
        "rev_table_by_country": "📋 Зведена таблиця по країнах",
        "rev_count_by_country": "📊 Відгуків по країнах",
        "rev_neg_by_country": "🔴 % Негативних по країнах",
        "rev_rating_by_country": "⭐ Рейтинг по країнах",
        "rev_country_analysis": "🌍 Аналіз по країнах",
        "rev_star_filter": "⭐ Рейтинг:",
        "rev_country_filter": "🌍 Країна (маркетплейс):",
        "rev_filters": "⭐ Фільтри відгуків",
        "all_countries": "Всі країни",
        "all_asins": "Всі ASINи",
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
        "reviews_title": "⭐ Customer Reviews",
        "total_reviews": "Total Reviews",
        "avg_review_rating": "Average Rating",
        "verified_pct": "Verified (%)",
        "star_dist": "Star Distribution",
        "worst_asin": "Problematic ASINs (1-2★)",
        "ov_title": "📊 Business Overview",
        "ov_top_sku": "### 📊 Top 15 SKU by Stock",
        "st_daily_trends": "### 📈 Daily Trends",
        "st_sessions_views": "#### 👁 Sessions & Page Views",
        "st_revenue_units": "#### 💰 Revenue & Units",
        "st_top_asins": "### 🏆 Top ASINs Performance",
        "st_top_revenue": "#### 💰 Top 15 by Revenue",
        "st_top_sessions": "#### 👁 Top 15 by Sessions",
        "st_full_data": "### 📋 Full ASIN Data",
        "st_download": "📥 Download CSV",
        "st_filters": "📈 Sales & Traffic Filters",
        "st_date_range": "📅 Date Range:",
        "ret_title": "### 📦 Returns Overview",
        "ret_total": "📦 Total Returns",
        "ret_unique_sku": "📦 Unique SKUs",
        "ret_rate": "📊 Return Rate",
        "ret_value": "💰 Return Value",
        "ret_avg": "💵 Avg Return",
        "ret_by_sku": "#### 💵 Return Value by SKU (Top 10)",
        "ret_daily": "#### 📊 Daily Return Value",
        "ret_by_reason": "#### 💸 Return Value by Reason",
        "ret_top_sku": "#### 🏆 Top 15 Returned SKUs",
        "ret_reasons": "#### 📊 Return Reasons",
        "ret_filters": "📦 Returns Filters",
        "ret_date": "📅 Return Date:",
        "ret_download": "📥 Download Returns CSV",
        "ord_title": "### 🛒 Orders Analytics",
        "insight_rating_health": "Rating Health",
        "insight_loyalty": "Loyalty",
        "insight_toxic": "Toxic ASIN",
        "insight_neg_level": "Negative Level",
        "insight_verified": "Verification",
        "rev_auto_insights": "🧠 Auto Insights",
        "rev_worst_asin": "🔴 WORST ASIN",
        "rev_best_asin": "🟢 BEST ASIN",
        "rev_worst_country": "🔴 WORST COUNTRY",
        "rev_best_country": "🟢 BEST COUNTRY",
        "rev_reviews_count": "rev.",
        "rev_main_asin": "📦 Main:",
        "rev_heatmap": "### 🔥 Heatmap: ASIN × Country",
        "rev_heatmap_hint": "Click ASIN in table below — opens Amazon page",
        "rev_asin_compare": "### 📊 ASIN Comparison",
        "rev_star_dist": "### 📊 Overall Star Distribution",
        "rev_texts": "### 📋 Review Texts (up to 100 per star, max 500)",
        "rev_sort_hint": "Sorted: 1★ first — problems first",
        "rev_dl_balanced": "📥 Download filtered",
        "rev_dl_all": "📥 Download all from DB",
        "rev_dl_balanced_hint": "Selected ASIN / country — up to 100 per star",
        "rev_dl_all_hint": "All reviews from database, no limits",
        "rev_shown": "Showing {n} of {total} reviews",
        "rev_click_hint": "👆 Click row to see detailed ASIN analysis · Links open Amazon in new tab",
        "rev_select_hint": "👇 Select ASIN for detailed analysis:",
        "rev_goto_asin": "📦 Go to ASIN:",
        "rev_not_selected": "— not selected —",
        "rev_back": "← Back to all ASINs",
        "about_title": "## ℹ️ About Merino BI Dashboard",
        "about_caption": "Amazon Intelligence Platform — full control over your FBA business in one place",
        "about_modules": "### 📦 System Modules",
        "about_pipeline": "### ⚙️ Data Pipeline",
        "about_features": "**✅ Features**",
        "about_stack": "**🔧 Tech Stack**",
        "about_footer": "MR.EQUIPP LIMITED · Built with obsession · v5.0",
        "rev_asins_in_filter": "📦 ASINs in filter",
        "insights_title": "🧠 Insights",
        "insight_rating_health": "Rating Health",
        "insight_loyalty": "Loyalty",
        "insight_toxic": "Toxic ASIN",
        "insight_neg_level": "Negative Level",
        "insight_verified": "Verification",
        "rev_table_by_country": "📋 Summary Table by Country",
        "rev_count_by_country": "📊 Reviews by Country",
        "rev_neg_by_country": "🔴 % Negative by Country",
        "rev_rating_by_country": "⭐ Rating by Country",
        "rev_country_analysis": "🌍 Country Analysis",
        "rev_star_filter": "⭐ Rating:",
        "rev_country_filter": "🌍 Country (marketplace):",
        "rev_filters": "⭐ Review Filters",
        "all_countries": "All countries",
        "all_asins": "All ASINs",
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
        "reviews_title": "⭐ Отзывы покупателей",
        "total_reviews": "Всего отзывов",
        "avg_review_rating": "Средний рейтинг",
        "verified_pct": "Верифицированные (%)",
        "star_dist": "Распределение по звездам",
        "worst_asin": "Проблемные ASIN (1-2★)",
        "ov_title": "📊 Обзор бизнеса",
        "ov_top_sku": "### 📊 Топ 15 SKU по остаткам",
        "st_daily_trends": "### 📈 Ежедневная динамика",
        "st_sessions_views": "#### 👁 Сессии и просмотры",
        "st_revenue_units": "#### 💰 Доход и заказы",
        "st_top_asins": "### 🏆 Топ ASINы",
        "st_top_revenue": "#### 💰 Топ 15 по доходу",
        "st_top_sessions": "#### 👁 Топ 15 по сессиям",
        "st_full_data": "### 📋 Все данные по ASINам",
        "st_download": "📥 Скачать CSV",
        "st_filters": "📈 Фильтры Sales & Traffic",
        "st_date_range": "📅 Диапазон дат:",
        "ret_title": "### 📦 Обзор возвратов",
        "ret_total": "📦 Всего возвратов",
        "ret_unique_sku": "📦 Уникальных SKU",
        "ret_rate": "📊 Уровень возвратов",
        "ret_value": "💰 Стоимость возвратов",
        "ret_avg": "💵 Ср. стоимость",
        "ret_by_sku": "#### 💵 Стоимость по SKU (Топ 10)",
        "ret_daily": "#### 📊 Ежедневная стоимость",
        "ret_by_reason": "#### 💸 По причинам",
        "ret_top_sku": "#### 🏆 Топ 15 SKU возвратов",
        "ret_reasons": "#### 📊 Причины возвратов",
        "ret_filters": "📦 Фильтры возвратов",
        "ret_date": "📅 Дата возврата:",
        "ret_download": "📥 Скачать CSV возвратов",
        "ord_title": "### 🛒 Аналитика заказов",
        "insight_rating_health": "Здоровье рейтинга",
        "insight_loyalty": "Лояльность",
        "insight_toxic": "Токсичный ASIN",
        "insight_neg_level": "Уровень негатива",
        "insight_verified": "Верификация",
        "rev_auto_insights": "🧠 Автоинсайты",
        "rev_worst_asin": "🔴 ХУДШИЙ ASIN",
        "rev_best_asin": "🟢 ЛУЧШИЙ ASIN",
        "rev_worst_country": "🔴 ХУДШАЯ СТРАНА",
        "rev_best_country": "🟢 ЛУЧШАЯ СТРАНА",
        "rev_reviews_count": "отзыв.",
        "rev_main_asin": "📦 Главный:",
        "rev_heatmap": "### 🔥 Тепловая карта: ASIN × Страна",
        "rev_heatmap_hint": "Нажми на ASIN в таблице — откроется страница Amazon",
        "rev_asin_compare": "### 📊 Сравнение ASINов",
        "rev_star_dist": "### 📊 Общее распределение звёзд",
        "rev_texts": "### 📋 Тексты отзывов (до 100 на звезду, макс 500)",
        "rev_sort_hint": "Сортировка: сначала 1★ — проблемы первыми",
        "rev_dl_balanced": "📥 Скачать по фильтру",
        "rev_dl_all": "📥 Скачать всё из базы",
        "rev_dl_balanced_hint": "Выбранный ASIN / страна — до 100 отзывов на звезду",
        "rev_dl_all_hint": "Все отзывы из базы данных без ограничений",
        "rev_shown": "Показано {n} из {total} отзывов",
        "rev_click_hint": "👆 Нажми на строку — увидишь детальный анализ · Ссылки откроют Amazon в новой вкладке",
        "rev_select_hint": "👇 Выбери ASIN для детального анализа:",
        "rev_goto_asin": "📦 Перейти к ASIN:",
        "rev_not_selected": "— не выбрано —",
        "rev_back": "← Назад ко всем ASINам",
        "about_title": "## ℹ️ О Merino BI Dashboard",
        "about_caption": "Amazon Intelligence Platform — полный контроль над FBA бизнесом в одном месте",
        "about_modules": "### 📦 Модули системы",
        "about_pipeline": "### ⚙️ Data Pipeline",
        "about_features": "**✅ Особенности**",
        "about_stack": "**🔧 Технологии**",
        "about_footer": "MR.EQUIPP LIMITED · Built with obsession · v5.0",
        "rev_asins_in_filter": "📦 ASINов в фильтре",
        "insights_title": "🧠 Инсайты",
        "insight_rating_health": "Здоровье рейтинга",
        "insight_loyalty": "Лояльность",
        "insight_toxic": "Токсичный ASIN",
        "insight_neg_level": "Уровень негатива",
        "insight_verified": "Верификация",
        "rev_table_by_country": "📋 Сводная таблица по странам",
        "rev_count_by_country": "📊 Отзывов по странам",
        "rev_neg_by_country": "🔴 % Негативных по странам",
        "rev_rating_by_country": "⭐ Рейтинг по странам",
        "rev_country_analysis": "🌍 Анализ по странам",
        "rev_star_filter": "⭐ Рейтинг:",
        "rev_country_filter": "🌍 Страна (маркетплейс):",
        "rev_filters": "⭐ Фильтры отзывов",
        "all_countries": "Все страны",
        "all_asins": "Все ASINы",
    }
}


DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

def get_engine():
    return create_engine(
        DATABASE_URL,
        connect_args={"options": "-csearch_path=spapi,public", "connect_timeout": 10},
        pool_timeout=10,
        pool_pre_ping=True,
    )

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
            'Quantity':       ['Quantity', 'quantity', 'qty'],
            'Item Price':     ['Item Price', 'item-price', 'item_price', 'price'],
            'Item Tax':       ['Item Tax', 'item-tax', 'item_tax', 'tax'],
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
        df['Amount']      = pd.to_numeric(df['Amount'], errors='coerce').fillna(0.0)
        df['Quantity']    = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0)
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
    import psycopg2
    import psycopg2.extras
    db_url = DATABASE_URL
    if not db_url:
        return pd.DataFrame()
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    conn = None
    try:
        conn = psycopg2.connect(db_url, connect_timeout=10)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM spapi.sales_traffic ORDER BY report_date DESC")
        rows    = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=columns)
        numeric_cols = [
            'sessions','page_views','units_ordered','units_ordered_b2b',
            'total_order_items','total_order_items_b2b',
            'ordered_product_sales','ordered_product_sales_b2b',
            'session_percentage','page_views_percentage',
            'buy_box_percentage','unit_session_percentage',
            'mobile_sessions','mobile_page_views',
            'browser_sessions','browser_page_views',
            'mobile_session_percentage','mobile_page_views_percentage',
            'mobile_unit_session_percentage','mobile_buy_box_percentage',
            'browser_session_percentage','browser_page_views_percentage',
            'browser_unit_session_percentage','browser_buy_box_percentage',
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')
        if 'created_at' in df.columns:
            created = pd.to_datetime(df['created_at'], errors='coerce').dt.normalize()
            if df['report_date'].isna().all():
                df['report_date'] = created
            elif df['report_date'].isna().any():
                mask = df['report_date'].isna()
                df.loc[mask, 'report_date'] = created[mask]
        df['report_date'] = df['report_date'].dt.normalize()
        df = df.dropna(subset=['report_date'])
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


@st.cache_data(ttl=60)
def load_returns():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            df_returns = pd.read_sql(text('SELECT * FROM returns ORDER BY "Return Date" DESC'), conn)
            df_orders  = pd.read_sql(text("SELECT * FROM orders"), conn)
        return df_returns, df_orders
    except Exception:
        return pd.DataFrame(), pd.DataFrame()


@st.cache_data(ttl=60)
def load_reviews():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text('SELECT * FROM amazon_reviews ORDER BY review_date DESC'), conn)
        if df.empty:
            return pd.DataFrame()
        df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce')
        df['rating']      = pd.to_numeric(df['rating'], errors='coerce').fillna(0).astype(int)
        if 'is_verified' in df.columns:
            df['is_verified'] = df['is_verified'].astype(bool)
        if 'domain' in df.columns:
            df['domain'] = df['domain'].str.lower().str.strip()
        return df
    except Exception:
        return pd.DataFrame()


# ============================================
# HELPERS
# ============================================

def insight_card(emoji, title, text, color=""):
    # color map: старі темні кольори → нові акцентні для border
    color_to_border = {
        "#0d2b1e": "#22c55e",   # зелений
        "#1a2b1e": "#22c55e",
        "#2b2400": "#f59e0b",   # жовтий
        "#2b0d0d": "#ef4444",   # червоний
        "#1a1a2e": "#6366f1",   # синій
        "#1e293b": "#4472C4",
    }
    border_color = color_to_border.get(color, "#4472C4")
    # Визначаємо колір фону і тексту через CSS змінні — адаптивно
    st.markdown(
        f'<div style="border-left:4px solid {border_color};border-radius:6px;'
        f'padding:12px 16px;margin-bottom:8px;'
        f'background:color-mix(in srgb, {border_color} 10%, transparent);">'
        f'<div style="font-size:14px;font-weight:700;margin-bottom:3px;color:{border_color}">{emoji} {title}</div>'
        f'<div style="font-size:13px;line-height:1.5;opacity:0.9;">{text}</div>'
        f'</div>',
        unsafe_allow_html=True
    )


def balanced_reviews(df, max_per_star=100):
    parts = [df[df['rating'] == s].head(max_per_star) for s in [1, 2, 3, 4, 5]]
    return pd.concat(parts, ignore_index=True) if parts else df


DOMAIN_LABELS = {
    'com':    '🇺🇸 USA (com)',
    'ca':     '🇨🇦 Canada (ca)',
    'de':     '🇩🇪 Germany (de)',
    'co.uk':  '🇬🇧 UK (co.uk)',
    'it':     '🇮🇹 Italy (it)',
    'es':     '🇪🇸 Spain (es)',
    'fr':     '🇫🇷 France (fr)',
    'co.jp':  '🇯🇵 Japan (co.jp)',
    'com.au': '🇦🇺 Australia (com.au)',
    'com.mx': '🇲🇽 Mexico (com.mx)',
    'nl':     '🇳🇱 Netherlands (nl)',
    'pl':     '🇵🇱 Poland (pl)',
    'se':     '🇸🇪 Sweden (se)',
}


# ============================================
# INSIGHT FUNCTIONS
# ============================================

def insights_sales_traffic(df_filtered, asin_stats):
    st.markdown("---")
    st.markdown("### 🧠 Автоматические инсайты")
    total_sessions = int(df_filtered['sessions'].sum())
    total_units    = int(df_filtered['units_ordered'].sum())
    total_revenue  = df_filtered['ordered_product_sales'].sum()
    avg_conv       = (total_units / total_sessions * 100) if total_sessions > 0 else 0
    avg_buy_box    = df_filtered['buy_box_percentage'].mean()
    mob            = df_filtered['mobile_sessions'].sum() if 'mobile_sessions' in df_filtered.columns else 0
    bro            = df_filtered['browser_sessions'].sum() if 'browser_sessions' in df_filtered.columns else 0
    mobile_pct     = (mob / (mob + bro) * 100) if (mob + bro) > 0 else 0
    avg_conv_all   = asin_stats['Conv %'].median()
    low_conv       = asin_stats[(asin_stats['Sessions'] > asin_stats['Sessions'].median()) & (asin_stats['Conv %'] < avg_conv_all)]
    low_bb         = asin_stats[asin_stats['Buy Box %'] < 80]
    rev_per_sess   = total_revenue / total_sessions if total_sessions > 0 else 0
    cols = st.columns(2)
    i = 0
    if avg_conv >= 12:   txt, em, col = f"Конверсия <b>{avg_conv:.1f}%</b> — выше нормы. Масштабируй рекламу!", "🟢", "#0d2b1e"
    elif avg_conv >= 8:  txt, em, col = f"Конверсия <b>{avg_conv:.1f}%</b> — в норме. Потенциал через A+.", "🟡", "#2b2400"
    else:                txt, em, col = f"Конверсия <b>{avg_conv:.1f}%</b> — ниже нормы. Проверь фото и цену.", "🔴", "#2b0d0d"
    with cols[i%2]: insight_card(em, "Конверсия", txt, col); i+=1
    if avg_buy_box >= 95:  txt, em, col = f"Buy Box <b>{avg_buy_box:.1f}%</b> — отлично!", "🟢", "#0d2b1e"
    elif avg_buy_box >= 80: txt, em, col = f"Buy Box <b>{avg_buy_box:.1f}%</b> — норма. {len(low_bb)} ASINов теряют.", "🟡", "#2b2400"
    else:                   txt, em, col = f"Buy Box <b>{avg_buy_box:.1f}%</b> — критично! Проверь репрайсер.", "🔴", "#2b0d0d"
    with cols[i%2]: insight_card(em, "Buy Box", txt, col); i+=1
    txt = f"<b>{mobile_pct:.0f}%</b> мобильного трафика {'— норма.' if mobile_pct >= 60 else '— ниже среднего ~65%.'}"
    with cols[i%2]: insight_card("📱", "Мобайл", txt, "#1a1a2e"); i+=1
    if len(low_conv) > 0:
        top = low_conv.nlargest(1,'Sessions').iloc[0]
        txt, em, col = f"<b>{len(low_conv)} ASINов</b> с высоким трафиком и низкой конверсией. Критичный: <b>{top['ASIN']}</b>.", "🔴", "#2b0d0d"
    else: txt, em, col = "Все ASINы с высоким трафиком конвертят хорошо!", "🟢", "#0d2b1e"
    with cols[i%2]: insight_card(em, "Упущенная выручка", txt, col); i+=1
    with cols[i%2]: insight_card("💡", "Цена сессии", f"Каждая сессия → <b>${rev_per_sess:.2f}</b>. +1000 сессий = +${rev_per_sess*1000:,.0f}.", "#1a1a2e"); i+=1
    if not asin_stats.empty:
        top = asin_stats.nlargest(1,'Revenue').iloc[0]
        top_pct = top['Revenue']/total_revenue*100 if total_revenue > 0 else 0
        with cols[i%2]: insight_card("🏆", "Главный ASIN", f"<b>{top['ASIN']}</b> = ${top['Revenue']:,.0f} ({top_pct:.0f}%).", "#1a2b1e")


def insights_settlements(df_filtered):
    st.markdown("---")
    st.markdown("### 🧠 Автоматические инсайты")
    net     = df_filtered['Amount'].sum()
    gross   = df_filtered[(df_filtered['Transaction Type']=='Order')&(df_filtered['Amount']>0)]['Amount'].sum()
    fees    = df_filtered[(df_filtered['Amount']<0)&(df_filtered['Transaction Type']!='Refund')&(~df_filtered['Transaction Type'].str.lower().str.contains('other',na=False))]['Amount'].sum()
    refunds = df_filtered[df_filtered['Transaction Type']=='Refund']['Amount'].sum()
    fee_pct    = abs(fees)/gross*100 if gross>0 else 0
    refund_pct = abs(refunds)/gross*100 if gross>0 else 0
    margin_pct = net/gross*100 if gross>0 else 0
    cols = st.columns(2); i = 0
    if margin_pct >= 30:  txt, em, col = f"Маржа <b>{margin_pct:.1f}%</b> — отлично!", "🟢", "#0d2b1e"
    elif margin_pct >= 15: txt, em, col = f"Маржа <b>{margin_pct:.1f}%</b> — норма для FBA.", "🟡", "#2b2400"
    else:                  txt, em, col = f"Маржа <b>{margin_pct:.1f}%</b> — низко! Анализируй расходы.", "🔴", "#2b0d0d"
    with cols[i%2]: insight_card(em, "Чистая маржа", txt, col); i+=1
    if fee_pct <= 30:  txt, em, col = f"Комиссии <b>{fee_pct:.1f}%</b> — в норме.", "🟢", "#0d2b1e"
    elif fee_pct <= 40: txt, em, col = f"Комиссии <b>{fee_pct:.1f}%</b> — немного высоко.", "🟡", "#2b2400"
    else:               txt, em, col = f"Комиссии <b>{fee_pct:.1f}%</b> — слишком высоко!", "🔴", "#2b0d0d"
    with cols[i%2]: insight_card(em, "Нагрузка комиссий", txt, col); i+=1
    if refund_pct <= 3:  txt, em, col = f"Возвраты <b>{refund_pct:.1f}%</b> — отлично.", "🟢", "#0d2b1e"
    elif refund_pct <= 8: txt, em, col = f"Возвраты <b>{refund_pct:.1f}%</b> — умеренно.", "🟡", "#2b2400"
    else:                 txt, em, col = f"Возвраты <b>{refund_pct:.1f}%</b> — критично!", "🔴", "#2b0d0d"
    with cols[i%2]: insight_card(em, "Возвраты", txt, col); i+=1
    with cols[i%2]: insight_card("💰", "Итог", f"Продажи <b>${gross:,.0f}</b> → на руки <b>${net:,.0f}</b>. Комиссии: ${abs(fees):,.0f}.", "#1a1a2e")


def insights_returns(df_filtered, return_rate):
    st.markdown("---")
    st.markdown("### 🧠 Автоматические инсайты")
    total_val  = df_filtered['Return Value'].sum()
    top_reason = df_filtered['Reason'].value_counts().index[0] if 'Reason' in df_filtered.columns and not df_filtered.empty else None
    top_sku    = df_filtered['SKU'].value_counts().index[0] if not df_filtered.empty else None
    cols = st.columns(2); i = 0
    if return_rate <= 3:  txt, em, col = f"Возвраты <b>{return_rate:.1f}%</b> — отлично.", "🟢", "#0d2b1e"
    elif return_rate <= 8: txt, em, col = f"Возвраты <b>{return_rate:.1f}%</b> — приемлемо.", "🟡", "#2b2400"
    else:                  txt, em, col = f"Возвраты <b>{return_rate:.1f}%</b> — опасно!", "🔴", "#2b0d0d"
    with cols[i%2]: insight_card(em, "Уровень возвратов", txt, col); i+=1
    with cols[i%2]: insight_card("💸", "Ущерб", f"Возвраты стоят <b>${total_val:,.0f}</b>.", "#2b1a00"); i+=1
    if top_reason:
        with cols[i%2]: insight_card("🔍", "Главная причина", f"<b>«{top_reason}»</b>", "#1a1a2e"); i+=1
    if top_sku:
        count = df_filtered['SKU'].value_counts().iloc[0]
        with cols[i%2]: insight_card("⚠️", "Проблемный SKU", f"<b>{top_sku}</b> ({count} возвратов).", "#2b0d0d")


def insights_inventory(df_filtered):
    st.markdown("---")
    st.markdown("### 🧠 Автоматические инсайты")
    total_val   = df_filtered['Stock Value'].sum()
    total_units = df_filtered['Available'].sum()
    avg_vel     = df_filtered['Velocity'].mean() if 'Velocity' in df_filtered.columns else 0
    top_frozen  = df_filtered.nlargest(1,'Stock Value').iloc[0] if not df_filtered.empty else None
    dead_stock  = df_filtered[df_filtered['Velocity']==0] if 'Velocity' in df_filtered.columns else pd.DataFrame()
    cols = st.columns(2); i = 0
    months = int(total_units/avg_vel/30) if avg_vel > 0 else 0
    with cols[i%2]: insight_card("🧊","Заморозка капитала",f"Заморожено <b>${total_val:,.0f}</b>. Запас на {months if avg_vel>0 else '∞'} мес.","#1a1a2e"); i+=1
    if top_frozen is not None:
        pct = top_frozen['Stock Value']/total_val*100 if total_val > 0 else 0
        with cols[i%2]: insight_card("🏦","Главный актив",f"<b>{top_frozen['SKU']}</b> держит ${top_frozen['Stock Value']:,.0f} ({pct:.0f}%).","#1a2b1e"); i+=1
    if len(dead_stock) > 0:
        dead_val = dead_stock['Stock Value'].sum()
        with cols[i%2]: insight_card("☠️","Мёртвый сток",f"<b>{len(dead_stock)} SKU</b> без продаж — ${dead_val:,.0f}. Рассмотри ликвидацию.","#2b0d0d"); i+=1
    days = int(total_units/(avg_vel*30)*30) if avg_vel > 0 else 999
    if days <= 30:   txt, em, col = f"Запасов на <b>{days} дней</b> — риск out of stock!", "🔴", "#2b0d0d"
    elif days <= 60: txt, em, col = f"Запасов на <b>{days} дней</b> — планируй поставку.", "🟡", "#2b2400"
    else:            txt, em, col = f"Запасов на <b>{days} дней</b> — достаточно.", "🟢", "#0d2b1e"
    with cols[i%2]: insight_card(em,"Оборачиваемость",txt,col)


def insights_orders(df_filtered):
    st.markdown("---")
    st.markdown("### 🧠 Автоматические инсайты")
    total_rev    = df_filtered['Total Price'].sum()
    total_orders = df_filtered['Order ID'].nunique()
    avg_order    = total_rev/total_orders if total_orders > 0 else 0
    days         = max((df_filtered['Order Date'].max()-df_filtered['Order Date'].min()).days,1)
    rev_per_day  = total_rev/days
    top_sku      = df_filtered.groupby('SKU')['Total Price'].sum().nlargest(1)
    cols = st.columns(2); i = 0
    with cols[i%2]: insight_card("🛒","Средний чек",f"<b>${avg_order:.2f}</b>. +10% к AOV = +${total_rev*0.1:,.0f}.","#1a1a2e"); i+=1
    with cols[i%2]: insight_card("📈","Дневная выручка",f"<b>${rev_per_day:,.0f}/день</b>. Прогноз на месяц: ${rev_per_day*30:,.0f}.","#1a2b1e"); i+=1
    if not top_sku.empty:
        sku_name, sku_rev = top_sku.index[0], top_sku.iloc[0]
        pct = sku_rev/total_rev*100 if total_rev > 0 else 0
        with cols[i%2]: insight_card("⚡","Концентрация риска",f"<b>{sku_name}</b> = {pct:.0f}% (${sku_rev:,.0f}). Диверсифицируй.","#2b1a00")


def insights_reviews(df, asin=None):
    st.markdown("---")
    label = f"ASIN {asin}" if asin else "всем ASINам"
    st.markdown(f"### {t['insights_title']} по {label}")
    total = len(df)
    if total == 0:
        st.info("Нет данных для инсайтов.")
        return
    avg_rating = df['rating'].mean()
    neg_df     = df[df['rating'] <= 2]
    pos_df     = df[df['rating'] >= 4]
    neg_pct    = len(neg_df)/total*100
    pos_pct    = len(pos_df)/total*100
    cols = st.columns(2); i = 0
    if avg_rating >= 4.4:   txt, em, col = f"Средний балл <b>{avg_rating:.1f}★</b> — отлично! Сильное социальное доверие.", "🟢", "#0d2b1e"
    elif avg_rating >= 4.0: txt, em, col = f"Средний балл <b>{avg_rating:.1f}★</b> — норма, риск упасть ниже 4.0.", "🟡", "#2b2400"
    else:                   txt, em, col = f"Средний балл <b>{avg_rating:.1f}★</b> — критично! Режет конверсию и удорожает PPC.", "🔴", "#2b0d0d"
    with cols[i%2]: insight_card(em,t["insight_rating_health"],txt,col); i+=1
    if neg_pct <= 10:  txt, em, col = f"Всего <b>{neg_pct:.1f}%</b> негативных (1-2★). Продукт оправдывает ожидания.", "🟢", "#0d2b1e"
    elif neg_pct <= 20: txt, em, col = f"<b>{neg_pct:.1f}%</b> негативных — системная проблема. Читай тексты 1★.", "🟡", "#2b2400"
    else:               txt, em, col = f"<b>{neg_pct:.1f}%</b> негативных — критично! Срочно фикси продукт или листинг.", "🔴", "#2b0d0d"
    with cols[i%2]: insight_card(em,"Уровень негатива",txt,col); i+=1
    with cols[i%2]: insight_card("💚",t["insight_loyalty"],f"<b>{pos_pct:.1f}%</b> позитивних (4-5★)","#0d2b1e" if pos_pct>=70 else "#2b2400"); i+=1
    if 'is_verified' in df.columns:
        ver_pct = df['is_verified'].mean()*100
        with cols[i%2]: insight_card("✅","Верификация",f"<b>{ver_pct:.1f}%</b> верифицированы {'— высокое доверие у Amazon.' if ver_pct>=80 else '— следи за политикой.'}","#1a1a2e"); i+=1
    if asin is None and not neg_df.empty and 'asin' in neg_df.columns:
        worst = neg_df['asin'].value_counts()
        if not worst.empty:
            with cols[i%2]: insight_card("⚠️",t["insight_toxic"],f"<b>{worst.index[0]}</b> — {worst.iloc[0]} негативных. Начни анализ с него.","#2b0d0d")


# ============================================
# OVERVIEW CONSOLIDATED INSIGHTS
# ============================================

def show_overview_insights(df_inventory):
    st.markdown("---")
    st.markdown("## 🧠 Business Intelligence: Зведені інсайти")
    st.caption("Автоматичний аналіз всіх модулів")

    df_settlements = load_settlements()
    df_st          = load_sales_traffic()
    df_orders      = load_orders()
    df_ret_raw, df_ord_raw = load_returns()
    df_reviews     = load_reviews()

    df_returns  = pd.DataFrame()
    return_rate = 0
    if not df_ret_raw.empty:
        df_ret = df_ret_raw.copy()
        df_ret['Return Date'] = pd.to_datetime(df_ret['Return Date'], errors='coerce')
        if 'Price' not in df_ret.columns and not df_ord_raw.empty:
            for col in ['Item Price','item-price','item_price','price']:
                if col in df_ord_raw.columns:
                    df_ord_raw[col] = pd.to_numeric(df_ord_raw[col], errors='coerce')
                    df_ret['Price'] = df_ret['SKU'].map(df_ord_raw.groupby('SKU')[col].mean()).fillna(0)
                    break
        if 'Price' not in df_ret.columns: df_ret['Price'] = 0
        df_ret['Price']        = pd.to_numeric(df_ret['Price'], errors='coerce').fillna(0)
        df_ret['Quantity']     = pd.to_numeric(df_ret.get('Quantity',1), errors='coerce').fillna(1)
        df_ret['Return Value'] = df_ret['Price'] * df_ret['Quantity']
        df_returns = df_ret
        if not df_ord_raw.empty:
            for col in ['Order ID','order-id','order_id','OrderID']:
                if col in df_ord_raw.columns:
                    total_orders = df_ord_raw[col].nunique()
                    unique_ret   = df_returns['Order ID'].nunique() if 'Order ID' in df_returns.columns else 0
                    return_rate  = unique_ret/total_orders*100 if total_orders > 0 else 0
                    break

    tabs = st.tabs(["💰 Inventory","🏦 Settlements","📈 Sales & Traffic","🛒 Orders","📦 Returns","⭐ Reviews"])

    with tabs[0]:
        if not df_inventory.empty and 'Stock Value' in df_inventory.columns:
            insights_inventory(df_inventory)
        else: st.info("📦 Дані по інвентарю відсутні")

    with tabs[1]:
        if not df_settlements.empty:
            max_d  = df_settlements['Posted Date'].max()
            df_s30 = df_settlements[df_settlements['Posted Date'] >= max_d - dt.timedelta(days=30)]
            insights_settlements(df_s30 if not df_s30.empty else df_settlements)
        else: st.info("🏦 Дані по виплатах відсутні.")

    with tabs[2]:
        if not df_st.empty:
            max_d   = df_st['report_date'].max()
            df_use  = df_st[df_st['report_date'] >= max_d - dt.timedelta(days=14)]
            df_use  = df_use if not df_use.empty else df_st
            asin_col = 'child_asin' if 'child_asin' in df_use.columns else df_use.columns[0]
            as_ = df_use.groupby(asin_col).agg({'sessions':'sum','units_ordered':'sum','ordered_product_sales':'sum','buy_box_percentage':'mean'}).reset_index()
            as_.columns = ['ASIN','Sessions','Units','Revenue','Buy Box %']
            as_['Conv %'] = (as_['Units']/as_['Sessions']*100).fillna(0)
            insights_sales_traffic(df_use, as_)
        else: st.info("📈 Дані Sales & Traffic відсутні.")

    with tabs[3]:
        if not df_orders.empty:
            max_d  = df_orders['Order Date'].max()
            df_o30 = df_orders[df_orders['Order Date'] >= max_d - dt.timedelta(days=30)]
            insights_orders(df_o30 if not df_o30.empty else df_orders)
        else: st.info("🛒 Дані замовлень відсутні.")

    with tabs[4]:
        if not df_returns.empty:
            max_d  = df_returns['Return Date'].max()
            df_r30 = df_returns[df_returns['Return Date'] >= max_d - dt.timedelta(days=30)]
            insights_returns(df_r30 if not df_r30.empty else df_returns, return_rate)
        else: st.info("📦 Дані повернень відсутні.")

    with tabs[5]:
        if not df_reviews.empty: insights_reviews(df_reviews, asin=None)
        else: st.info("⭐ Дані відгуків відсутні.")


# ============================================
# ⭐ REVIEWS MODULE
# ============================================

def make_amazon_url(domain, asin):
    return f"https://www.amazon.{domain}/dp/{asin}"


def show_global_insights(df, has_domain):
    st.markdown(f"### {t['rev_auto_insights']}")

    asin_stats, dom_stats = None, None

    if 'asin' in df.columns:
        asin_stats = df.groupby('asin').agg(
            Reviews=('rating', 'count'),
            Rating=('rating', 'mean'),
            Neg=('rating', lambda x: (x <= 2).sum()),
        ).reset_index()
        asin_stats['Neg %'] = (asin_stats['Neg'] / asin_stats['Reviews'] * 100).round(1)
        asin_stats = asin_stats[asin_stats['Reviews'] >= 5]

    if has_domain and 'domain' in df.columns:
        dom_stats = df.groupby('domain').agg(
            Reviews=('rating', 'count'),
            Rating=('rating', 'mean'),
            Neg=('rating', lambda x: (x <= 2).sum()),
        ).reset_index()
        dom_stats['Neg %'] = (dom_stats['Neg'] / dom_stats['Reviews'] * 100).round(1)
        dom_stats = dom_stats[dom_stats['Reviews'] >= 5]

    col1, col2, col3, col4 = st.columns(4)

    if asin_stats is not None and not asin_stats.empty:
        worst_a = asin_stats.loc[asin_stats['Neg %'].idxmax()]
        best_a  = asin_stats.loc[asin_stats['Rating'].idxmax()]

        worst_asin_country = ""
        if has_domain and 'domain' in df.columns:
            asin_dom = df[df['asin'] == worst_a['asin']].groupby('domain')['rating'].count()
            if not asin_dom.empty:
                top_dom = asin_dom.idxmax()
                worst_asin_country = DOMAIN_LABELS.get(top_dom, top_dom)

        best_asin_country = ""
        if has_domain and 'domain' in df.columns:
            asin_dom2 = df[df['asin'] == best_a['asin']].groupby('domain')['rating'].count()
            if not asin_dom2.empty:
                top_dom2 = asin_dom2.idxmax()
                best_asin_country = DOMAIN_LABELS.get(top_dom2, top_dom2)

        neg_pct = worst_a['Neg %']
        bar_color = "#F44336" if neg_pct > 20 else "#FFC107"
        country_line = f"<div style='font-size:11px;color:#aaa;margin-top:2px'>🌍 {worst_asin_country}</div>" if worst_asin_country else ""

        with col1:
            st.markdown(f"""
            <div style="background:#1e1e2e;border-left:5px solid {bar_color};border-radius:10px;padding:16px 20px;height:140px">
              <div style="font-size:11px;color:#888;margin-bottom:4px">🔴 НАЙГІРШИЙ ASIN</div>
              <div style="font-size:20px;font-weight:800;color:#fff;letter-spacing:1px">{worst_a['asin']}</div>
              {country_line}
              <div style="display:flex;gap:12px;margin-top:8px;flex-wrap:wrap">
                <span style="color:#aaa;font-size:12px">⭐ {worst_a['Rating']:.2f}★</span>
                <span style="color:{bar_color};font-size:12px;font-weight:700">🔴 {neg_pct:.1f}% neg</span>
                <span style="color:#666;font-size:12px">{int(worst_a['Reviews'])} {t['rev_reviews_count']}</span>
              </div>
              <div style="margin-top:10px;background:#2a2a3e;border-radius:4px;height:5px">
                <div style="width:{min(neg_pct,100):.0f}%;background:{bar_color};border-radius:4px;height:5px"></div>
              </div>
            </div>""", unsafe_allow_html=True)

        rating_color = "#4CAF50" if best_a['Rating'] >= 4.4 else "#FFC107"
        country_line2 = f"<div style='font-size:11px;color:#aaa;margin-top:2px'>🌍 {best_asin_country}</div>" if best_asin_country else ""
        with col2:
            st.markdown(f"""
            <div style="background:#1e1e2e;border-left:5px solid {rating_color};border-radius:10px;padding:16px 20px;height:140px">
              <div style="font-size:11px;color:#888;margin-bottom:4px">🟢 НАЙКРАЩИЙ ASIN</div>
              <div style="font-size:20px;font-weight:800;color:#fff;letter-spacing:1px">{best_a['asin']}</div>
              {country_line2}
              <div style="display:flex;gap:12px;margin-top:8px;flex-wrap:wrap">
                <span style="color:{rating_color};font-size:12px;font-weight:700">⭐ {best_a['Rating']:.2f}★</span>
                <span style="color:#aaa;font-size:12px">🔴 {best_a['Neg %']:.1f}% neg</span>
                <span style="color:#666;font-size:12px">{int(best_a['Reviews'])} {t['rev_reviews_count']}</span>
              </div>
              <div style="margin-top:10px;background:#2a2a3e;border-radius:4px;height:5px">
                <div style="width:{((best_a['Rating']-1)/4*100):.0f}%;background:{rating_color};border-radius:4px;height:5px"></div>
              </div>
            </div>""", unsafe_allow_html=True)

    if dom_stats is not None and not dom_stats.empty:
        worst_d = dom_stats.loc[dom_stats['Neg %'].idxmax()]
        best_d  = dom_stats.loc[dom_stats['Rating'].idxmax()]
        worst_label = DOMAIN_LABELS.get(worst_d['domain'], worst_d['domain'])
        best_label  = DOMAIN_LABELS.get(best_d['domain'], best_d['domain'])

        worst_country_asin = ""
        if 'asin' in df.columns:
            df_wdom = df[df['domain'] == worst_d['domain']]
            if not df_wdom.empty:
                per_asin = df_wdom.groupby('asin').agg(
                    Neg=('rating', lambda x: (x<=2).sum()),
                    Reviews=('rating','count')
                ).reset_index()
                per_asin['Neg %'] = per_asin['Neg'] / per_asin['Reviews'] * 100
                per_asin = per_asin[per_asin['Reviews'] >= 3]
                if not per_asin.empty:
                    worst_country_asin = per_asin.loc[per_asin['Neg %'].idxmax(), 'asin']

        best_country_asin = ""
        if 'asin' in df.columns:
            df_bdom = df[df['domain'] == best_d['domain']]
            if not df_bdom.empty:
                per_asin2 = df_bdom.groupby('asin').agg(
                    Rating=('rating','mean'), Reviews=('rating','count')
                ).reset_index()
                per_asin2 = per_asin2[per_asin2['Reviews'] >= 3]
                if not per_asin2.empty:
                    best_country_asin = per_asin2.loc[per_asin2['Rating'].idxmax(), 'asin']

        neg_pct = worst_d['Neg %']
        bar_color = "#F44336" if neg_pct > 20 else "#FFC107"
        asin_line_w = f"<div style='font-size:11px;color:#aaa;margin-top:2px'>{t['rev_main_asin']} {worst_country_asin}</div>" if worst_country_asin else ""
        asin_line_b = f"<div style='font-size:11px;color:#aaa;margin-top:2px'>{t['rev_main_asin']} {best_country_asin}</div>" if best_country_asin else ""

        with col3:
            st.markdown(f"""
            <div style="background:#1e1e2e;border-left:5px solid {bar_color};border-radius:10px;padding:16px 20px;height:140px">
              <div style="font-size:11px;color:#888;margin-bottom:4px">🔴 НАЙГІРША КРАЇНА</div>
              <div style="font-size:18px;font-weight:800;color:#fff">{worst_label}</div>
              {asin_line_w}
              <div style="display:flex;gap:12px;margin-top:8px;flex-wrap:wrap">
                <span style="color:#aaa;font-size:12px">⭐ {worst_d['Rating']:.2f}★</span>
                <span style="color:{bar_color};font-size:12px;font-weight:700">🔴 {neg_pct:.1f}% neg</span>
                <span style="color:#666;font-size:12px">{int(worst_d['Reviews'])} {t['rev_reviews_count']}</span>
              </div>
              <div style="margin-top:10px;background:#2a2a3e;border-radius:4px;height:5px">
                <div style="width:{min(neg_pct,100):.0f}%;background:{bar_color};border-radius:4px;height:5px"></div>
              </div>
            </div>""", unsafe_allow_html=True)

        rating_color = "#4CAF50" if best_d['Rating'] >= 4.4 else "#FFC107"
        with col4:
            st.markdown(f"""
            <div style="background:#1e1e2e;border-left:5px solid {rating_color};border-radius:10px;padding:16px 20px;height:140px">
              <div style="font-size:11px;color:#888;margin-bottom:4px">🟢 НАЙКРАЩА КРАЇНА</div>
              <div style="font-size:18px;font-weight:800;color:#fff">{best_label}</div>
              {asin_line_b}
              <div style="display:flex;gap:12px;margin-top:8px;flex-wrap:wrap">
                <span style="color:{rating_color};font-size:12px;font-weight:700">⭐ {best_d['Rating']:.2f}★</span>
                <span style="color:#aaa;font-size:12px">🔴 {best_d['Neg %']:.1f}% neg</span>
                <span style="color:#666;font-size:12px">{int(best_d['Reviews'])} {t['rev_reviews_count']}</span>
              </div>
              <div style="margin-top:10px;background:#2a2a3e;border-radius:4px;height:5px">
                <div style="width:{((best_d['Rating']-1)/4*100):.0f}%;background:{rating_color};border-radius:4px;height:5px"></div>
              </div>
            </div>""", unsafe_allow_html=True)


def show_single_asin_detail(df_asin, asin, has_domain):
    total = len(df_asin)
    if total == 0:
        st.info("Немає відгуків по цьому ASIN.")
        return

    avg_r   = df_asin['rating'].mean()
    neg_cnt = int((df_asin['rating'] <= 2).sum())
    pos_cnt = int((df_asin['rating'] >= 4).sum())
    neg_pct = neg_cnt / total * 100

    r_color = "#4CAF50" if avg_r >= 4.4 else "#FFC107" if avg_r >= 4.0 else "#F44336"
    n_color = "#4CAF50" if neg_pct <= 10 else "#FFC107" if neg_pct <= 20 else "#F44336"

    st.markdown(f"""
    <div style="background:#1e1e2e;border-radius:12px;padding:18px 24px;margin-bottom:16px;display:flex;gap:40px;align-items:center">
      <div>
        <div style="font-size:11px;color:#888">ASIN</div>
        <div style="font-size:20px;font-weight:800;color:#fff;letter-spacing:1px">{asin}</div>
      </div>
      <div>
        <div style="font-size:11px;color:#888">Середній рейтинг</div>
        <div style="font-size:28px;font-weight:800;color:{r_color}">{avg_r:.2f}★</div>
      </div>
      <div>
        <div style="font-size:11px;color:#888">Всього відгуків</div>
        <div style="font-size:28px;font-weight:800;color:#fff">{total}</div>
      </div>
      <div>
        <div style="font-size:11px;color:#888">🔴 Негативних</div>
        <div style="font-size:28px;font-weight:800;color:{n_color}">{neg_pct:.1f}%</div>
      </div>
      <div>
        <div style="font-size:11px;color:#888">🟢 Позитивних</div>
        <div style="font-size:28px;font-weight:800;color:#4CAF50">{pos_cnt/total*100:.1f}%</div>
      </div>
    </div>""", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### ⭐ Розподіл зірок")
        star_counts = df_asin['rating'].value_counts().reindex([5,4,3,2,1]).fillna(0).reset_index()
        star_counts.columns = ['Stars', 'Count']
        star_counts['Pct'] = (star_counts['Count'] / total * 100).round(1)
        star_counts['label'] = star_counts['Stars'].astype(str) + '★'
        color_map = {5:'#4CAF50',4:'#8BC34A',3:'#FFC107',2:'#FF9800',1:'#F44336'}
        fig = go.Figure(go.Bar(
            x=star_counts['Count'], y=star_counts['label'], orientation='h',
            marker_color=[color_map.get(int(s),'#888') for s in star_counts['Stars']],
            text=[f"{c:.0f} ({p:.0f}%)" for c,p in zip(star_counts['Count'], star_counts['Pct'])],
            textposition='outside'
        ))
        fig.update_layout(
            yaxis=dict(categoryorder='array', categoryarray=['1★','2★','3★','4★','5★']),
            height=260, margin=dict(l=5,r=60,t=10,b=10)
        )
        st.plotly_chart(fig, width="stretch")

    with col2:
        if has_domain and 'domain' in df_asin.columns and df_asin['domain'].nunique() > 1:
            st.markdown("#### 🌍 Рейтинг по країнах для цього ASIN")
            dom_s = df_asin.groupby('domain').agg(
                Reviews=('rating','count'), Rating=('rating','mean'),
                Neg=('rating', lambda x: (x<=2).sum())
            ).reset_index()
            dom_s['Neg %'] = (dom_s['Neg']/dom_s['Reviews']*100).round(1)
            dom_s['Country'] = dom_s['domain'].map(lambda x: DOMAIN_LABELS.get(x, x))
            dom_s = dom_s.sort_values('Rating', ascending=True)
            colors = ['#F44336' if r<4.0 else '#FFC107' if r<4.4 else '#4CAF50' for r in dom_s['Rating']]
            fig2 = go.Figure(go.Bar(
                x=dom_s['Rating'], y=dom_s['Country'], orientation='h',
                marker_color=colors,
                text=[f"{r:.2f}★  {n:.0f}% neg" for r,n in zip(dom_s['Rating'], dom_s['Neg %'])],
                textposition='outside'
            ))
            fig2.add_vline(x=4.0, line_dash="dash", line_color="orange")
            fig2.update_layout(height=260, xaxis_range=[1,5.8], margin=dict(l=5,r=80,t=10,b=10))
            st.plotly_chart(fig2, width="stretch")
        else:
            st.markdown("#### 📊 Рейтинг по часу")
            if 'review_date' in df_asin.columns:
                df_time = df_asin.dropna(subset=['review_date']).copy()
                df_time['month'] = df_time['review_date'].dt.to_period('M').astype(str)
                monthly = df_time.groupby('month')['rating'].mean().reset_index()
                fig_t = px.line(monthly, x='month', y='rating', markers=True)
                fig_t.add_hline(y=4.0, line_dash='dash', line_color='orange')
                fig_t.update_layout(height=260, yaxis_range=[1,5])
                st.plotly_chart(fig_t, width="stretch")

    st.markdown("#### 🔴 Останні негативні відгуки (1-2★)")
    neg_df = df_asin[df_asin['rating'] <= 2].sort_values('review_date', ascending=False).head(5)
    if not neg_df.empty:
        for _, row in neg_df.iterrows():
            domain_str = f" · {DOMAIN_LABELS.get(row.get('domain',''), row.get('domain',''))}" if 'domain' in neg_df.columns else ""
            date_str = str(row['review_date'])[:10] if pd.notna(row.get('review_date')) else ''
            stars = '★' * int(row['rating']) + '☆' * (5 - int(row['rating']))
            title = row.get('title', '') or ''
            content = (row.get('content', '') or '')[:300]
            st.markdown(f"""
            <div style="background:#1e1e2e;border-left:4px solid #F44336;border-radius:8px;padding:12px 16px;margin-bottom:8px">
              <div style="display:flex;justify-content:space-between;margin-bottom:6px">
                <span style="color:#F44336;font-weight:700">{stars}</span>
                <span style="color:#666;font-size:12px">{date_str}{domain_str}</span>
              </div>
              <div style="color:#fff;font-weight:600;margin-bottom:4px">{title}</div>
              <div style="color:#aaa;font-size:13px;line-height:1.5">{content}{"..." if len(row.get("content","") or "") > 300 else ""}</div>
            </div>""", unsafe_allow_html=True)
    else:
        st.success("🎉 Негативних відгуків немає!")


def show_asin_links_table(df, has_domain):
    st.markdown("### 🔗 Всі ASINи — огляд по країнах")
    st.caption(t["rev_click_hint"])

    if 'asin' not in df.columns:
        st.info("Немає даних про ASINи.")
        return None, None

    # Визначаємо колонку дати
    date_col = None
    for c in ['review_date', 'scraped_at', 'created_at', 'date']:
        if c in df.columns:
            date_col = c
            break

    if has_domain and 'domain' in df.columns:
        agg_dict = dict(
            Reviews=('rating', 'count'),
            Rating=('rating', 'mean'),
            Neg=('rating', lambda x: (x <= 2).sum()),
        )
        if date_col:
            agg_dict['Остання дата'] = (date_col, 'max')
        combos = df.groupby(['asin', 'domain']).agg(**agg_dict).reset_index()
        combos['Neg %'] = (combos['Neg'] / combos['Reviews'] * 100).round(1)
        combos['Country'] = combos['domain'].map(lambda x: DOMAIN_LABELS.get(x, f'🌍 {x}'))
        combos['🔗 Amazon'] = combos.apply(
            lambda r: f"https://www.amazon.{r['domain']}/dp/{r['asin']}", axis=1
        )
        combos = combos.sort_values(['Neg %'], ascending=False)
        cols_to_take = ['asin', 'Country', 'Reviews', 'Rating', 'Neg %']
        if date_col: cols_to_take.append('Остання дата')
        cols_to_take += ['domain', '🔗 Amazon']
        table_df = combos[cols_to_take].rename(
            columns={'asin': 'ASIN', 'domain': '_domain'}
        ).reset_index(drop=True)
    else:
        agg_dict = dict(
            Reviews=('rating', 'count'),
            Rating=('rating', 'mean'),
            Neg=('rating', lambda x: (x <= 2).sum()),
        )
        if date_col:
            agg_dict['Остання дата'] = (date_col, 'max')
        asin_stats = df.groupby('asin').agg(**agg_dict).reset_index()
        asin_stats['Neg %'] = (asin_stats['Neg'] / asin_stats['Reviews'] * 100).round(1)
        asin_stats['🔗 Amazon'] = asin_stats['asin'].apply(lambda a: f"https://www.amazon.com/dp/{a}")
        asin_stats['_domain'] = 'com'
        cols_to_take = ['asin', 'Reviews', 'Rating', 'Neg %']
        if date_col: cols_to_take.append('Остання дата')
        cols_to_take += ['_domain', '🔗 Amazon']
        table_df = asin_stats[cols_to_take].rename(
            columns={'asin': 'ASIN'}
        ).reset_index(drop=True)

    table_df['Rating'] = table_df['Rating'].round(2)

    # Форматуємо дату
    if 'Остання дата' in table_df.columns:
        table_df['Остання дата'] = pd.to_datetime(table_df['Остання дата'], errors='coerce').dt.strftime('%Y-%m-%d')

    st.dataframe(
        table_df.drop(columns=['_domain']),
        column_config={
            "🔗 Amazon": st.column_config.LinkColumn("🔗 Amazon", display_text="Відкрити →"),
            "Rating": st.column_config.NumberColumn("⭐ Rating", format="%.2f ★"),
            "Остання дата": st.column_config.TextColumn("📅 Остання дата"),
            "Neg %": st.column_config.NumberColumn("🔴 Neg %", format="%.1f%%"),
            "Reviews": st.column_config.NumberColumn("📝 Відгуків"),
        },
        width="stretch",
        hide_index=True,
        height=min(400, 45 + len(table_df) * 35),
    )

    st.caption(t["rev_select_hint"])

    # ── Фільтр по країнах + вибір ASIN ──
    sel_col, country_col = st.columns([2, 2])

    with country_col:
        if '_domain' in table_df.columns:
            all_domains = sorted(table_df['_domain'].dropna().unique().tolist())
            domain_options = ["🌍 " + t.get("all_countries", "All")] + [
                DOMAIN_LABELS.get(d, d) for d in all_domains
            ]
            sel_domain = st.selectbox("🌍 Країна:", domain_options, key="asin_jump_domain")
            # Filter table by selected domain
            if sel_domain != domain_options[0]:
                chosen_domain = all_domains[domain_options.index(sel_domain) - 1]
                filtered_for_select = table_df[table_df['_domain'] == chosen_domain]
            else:
                chosen_domain = None
                filtered_for_select = table_df
        else:
            filtered_for_select = table_df
            chosen_domain = None

    asin_list = filtered_for_select['ASIN'].unique().tolist()

    with sel_col:
        # Add country flag to each ASIN option
        def asin_label(asin):
            rows = filtered_for_select[filtered_for_select['ASIN'] == asin]
            if not rows.empty and '_domain' in rows.columns:
                dom = rows.iloc[0]['_domain']
                flag = DOMAIN_LABELS.get(dom, dom).split()[0] if dom else ""
                return f"{flag} {asin}" if flag else asin
            return asin

        asin_labels = [t["rev_not_selected"]] + [asin_label(a) for a in asin_list]
        asin_map    = {asin_label(a): a for a in asin_list}

        chosen_label = st.selectbox(t["rev_goto_asin"], asin_labels, key="asin_table_jump")

    not_selected_values = {"— не вибрано —", "— not selected —", "— не выбрано —"}
    if chosen_label and chosen_label not in not_selected_values:
        chosen = asin_map.get(chosen_label, chosen_label)
        matched = table_df[table_df['ASIN'] == chosen]
        if chosen_domain:
            matched = matched[matched['_domain'] == chosen_domain]
        if matched.empty:
            matched = table_df[table_df['ASIN'] == chosen]
        if not matched.empty:
            row = matched.iloc[0]
            return chosen, row['_domain']

    return None, None


def show_reviews(t):
    df_all = load_reviews()
    if df_all.empty:
        st.warning("⚠️ Не знайдено даних про відгуки. Перевірте ETL-скрипт (Apify → Postgres).")
        return

    has_domain = 'domain' in df_all.columns

    st.sidebar.markdown("---")
    st.sidebar.subheader(t["rev_filters"])

    selected_domains = []
    if has_domain:
        all_domains = sorted(df_all['domain'].dropna().unique().tolist())
        domain_display_list = [DOMAIN_LABELS.get(d, f'🌍 {d}') for d in all_domains]
        display_to_code = {DOMAIN_LABELS.get(d, f'🌍 {d}'): d for d in all_domains}
        sel_domain_display = st.sidebar.multiselect(
            t["rev_country_filter"], domain_display_list, default=[], key="rev_domain"
        )
        selected_domains = [display_to_code[d] for d in sel_domain_display if d in display_to_code]

    jumped_asin = st.session_state.pop('rev_asin_jump', None)

    df_for_asin = df_all.copy()
    if selected_domains:
        df_for_asin = df_for_asin[df_for_asin['domain'].isin(selected_domains)]
    asins = sorted(df_for_asin['asin'].dropna().unique().tolist()) if 'asin' in df_for_asin.columns else []
    asin_options = ['🌐 Всі ASINи'] + asins

    default_asin_idx = 0
    if jumped_asin and jumped_asin in asins:
        default_asin_idx = asin_options.index(jumped_asin)
        # Видаляємо старий ключ щоб index спрацював
        st.session_state.pop('rev_asin', None)

    sel_raw = st.sidebar.selectbox("📦 ASIN:", asin_options, index=default_asin_idx, key="rev_asin")
    selected_asin = None if sel_raw == '🌐 Всі ASINи' else sel_raw

    star_filter = st.sidebar.multiselect(t["rev_star_filter"], [5, 4, 3, 2, 1], default=[], key="rev_stars")

    if selected_asin and has_domain:
        st.sidebar.markdown("---")
        st.sidebar.markdown("**🔗 Відкрити на Amazon:**")
        asin_domains = sorted(df_all[df_all['asin'] == selected_asin]['domain'].dropna().unique().tolist())
        for dom in asin_domains:
            url = make_amazon_url(dom, selected_asin)
            flag = DOMAIN_LABELS.get(dom, '🌍').split(' ')[0]
            label = DOMAIN_LABELS.get(dom, dom).split('(')[0].strip()
            st.sidebar.markdown(f"[{flag} {label}]({url})")
        st.sidebar.markdown("---")
        if st.sidebar.button(t["rev_back"], width="stretch"):
            st.session_state['rev_asin_jump'] = None
            st.rerun()

    df = df_all.copy()
    if selected_domains:
        df = df[df['domain'].isin(selected_domains)]
    if selected_asin:
        df = df[df['asin'] == selected_asin]
    if star_filter:
        df = df[df['rating'].isin(star_filter)]

    if df.empty:
        st.warning("Немає відгуків за цими фільтрами.")
        return

    asin_label    = selected_asin if selected_asin else t["all_asins"]
    country_label = ", ".join([DOMAIN_LABELS.get(d, d) for d in selected_domains]) if selected_domains else t["all_countries"]

    # ── Кнопка назад — одразу перед заголовком ──
    if selected_asin is not None:
        if st.button(t["rev_back"], key="back_top", type="secondary"):
            st.session_state.pop("rev_asin", None)
            st.rerun()

    if selected_asin:
        first_domain = df['domain'].dropna().iloc[0] if has_domain and not df.empty else 'com'
        amazon_url = make_amazon_url(first_domain, selected_asin)
        st.markdown(
            f"### {t['reviews_title']} — "
            f"<a href='{amazon_url}' target='_blank' style='color:#5B9BD5'>{selected_asin} 🔗</a>"
            f" | 🌍 {country_label}",
            unsafe_allow_html=True
        )
    else:
        st.markdown(f"### {t['reviews_title']} — {asin_label} | 🌍 {country_label}")

    total_revs   = len(df)
    avg_rating   = df['rating'].mean()
    verified_pct = df['is_verified'].mean() * 100 if 'is_verified' in df.columns and total_revs > 0 else 0
    neg_count    = int((df['rating'] <= 2).sum())
    pos_count    = int((df['rating'] >= 4).sum())
    total_asins = df['asin'].nunique() if 'asin' in df.columns else 0
    total_asins_db = df_all['asin'].nunique() if 'asin' in df_all.columns else 0

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric(t["total_reviews"],     f"{total_revs:,}")
    c2.metric(t["rev_asins_in_filter"],  f"{total_asins:,}",
              delta=f"з {total_asins_db} в базі" if total_asins != total_asins_db else None,
              delta_color="off")
    c3.metric(t["avg_review_rating"], f"{avg_rating:.2f} ⭐")
    c4.metric(t["verified_pct"],      f"{verified_pct:.1f}%")
    c5.metric("🔴 Негативних (1-2★)", f"{neg_count:,}")
    c6.metric("🟢 Позитивних (4-5★)", f"{pos_count:,}")

    st.markdown("---")
    show_global_insights(df_all if selected_asin is None else df, has_domain)
    st.markdown("---")

    if selected_asin is not None:
        show_single_asin_detail(df, selected_asin, has_domain)
        st.markdown("---")
        if st.button(t["rev_back"], key="back_bottom", type="secondary"):
            st.session_state.pop("rev_asin", None)
            st.rerun()

    if has_domain and selected_asin is None:
        st.markdown(f"### {t['rev_country_analysis']}")

        domain_stats = df.groupby('domain').agg(
            Reviews=('rating', 'count'),
            Rating=('rating', 'mean'),
            Neg=('rating', lambda x: (x <= 2).sum()),
            Pos=('rating', lambda x: (x >= 4).sum()),
        ).reset_index()
        domain_stats['Neg %'] = (domain_stats['Neg'] / domain_stats['Reviews'] * 100).round(1)
        domain_stats['Pos %'] = (domain_stats['Pos'] / domain_stats['Reviews'] * 100).round(1)
        domain_stats['Country'] = domain_stats['domain'].map(lambda x: DOMAIN_LABELS.get(x, f'🌍 {x}'))

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"#### {t['rev_rating_by_country']}")
            ds_sort = domain_stats.sort_values('Rating', ascending=True)
            colors = ['#F44336' if r < 4.0 else '#FFC107' if r < 4.4 else '#4CAF50' for r in ds_sort['Rating']]
            fig = go.Figure(go.Bar(
                x=ds_sort['Rating'], y=ds_sort['Country'], orientation='h',
                marker_color=colors,
                text=[f"{v:.2f}★" for v in ds_sort['Rating']], textposition='outside'
            ))
            fig.add_vline(x=4.0, line_dash="dash", line_color="orange", annotation_text="4.0")
            fig.update_layout(height=max(280, len(ds_sort) * 50), xaxis_range=[1, 5.5],
                              margin=dict(l=10, r=60, t=20, b=20))
            st.plotly_chart(fig, width="stretch")

        with col2:
            st.markdown(f"#### {t['rev_neg_by_country']}")
            ds_neg = domain_stats.sort_values('Neg %', ascending=False)
            neg_colors = ['#F44336' if v > 20 else '#FFC107' if v > 10 else '#4CAF50' for v in ds_neg['Neg %']]
            fig2 = go.Figure(go.Bar(
                x=ds_neg['Neg %'], y=ds_neg['Country'], orientation='h',
                marker_color=neg_colors,
                text=[f"{v:.1f}%" for v in ds_neg['Neg %']], textposition='outside'
            ))
            fig2.update_layout(height=max(280, len(ds_neg) * 50), margin=dict(l=10, r=60, t=20, b=20))
            st.plotly_chart(fig2, width="stretch")

        with col3:
            st.markdown(f"#### {t['rev_count_by_country']}")
            fig3 = px.pie(domain_stats, values='Reviews', names='Country', hole=0.4,
                          color_discrete_sequence=px.colors.qualitative.Set3)
            fig3.update_layout(height=max(280, len(domain_stats) * 50))
            st.plotly_chart(fig3, width="stretch")

        st.markdown(f"#### {t['rev_table_by_country']}")
        disp = domain_stats[['Country', 'Reviews', 'Rating', 'Neg %', 'Pos %']].sort_values('Rating', ascending=False)
        st.dataframe(
            disp.style
                .format({'Rating': '{:.2f}', 'Neg %': '{:.1f}%', 'Pos %': '{:.1f}%'})
                .background_gradient(subset=['Rating'], cmap='RdYlGn')
                .background_gradient(subset=['Neg %'], cmap='RdYlGn_r'),
            width="stretch"
        )

        if 'asin' in df.columns and df['domain'].nunique() > 1:
            st.markdown("---")
            st.markdown(t["rev_heatmap"])
            st.caption(t["rev_heatmap_hint"])

            pivot = df.groupby(['asin', 'domain'])['rating'].mean().reset_index()
            pivot_table = pivot.pivot(index='asin', columns='domain', values='rating')
            pivot_table.columns = [DOMAIN_LABELS.get(c, f'🌍 {c}') for c in pivot_table.columns]

            fig_heat = go.Figure(data=go.Heatmap(
                z=pivot_table.values,
                x=list(pivot_table.columns),
                y=list(pivot_table.index),
                colorscale='RdYlGn',
                zmin=1, zmax=5,
                text=[[f"{v:.2f}" if not pd.isna(v) else "—" for v in row] for row in pivot_table.values],
                texttemplate="%{text}",
                colorbar=dict(title="★ Рейтинг"),
            ))
            fig_heat.update_layout(
                height=max(350, len(pivot_table) * 45 + 100),
                xaxis_title="Країна", yaxis_title="ASIN",
                margin=dict(l=20, r=20, t=30, b=20)
            )
            st.plotly_chart(fig_heat, width="stretch")
            st.caption("🟢 ≥4.4★ відмінно · 🟡 4.0–4.4★ норма · 🔴 <4.0★ проблема")

        st.markdown("---")

    if selected_asin is None:
        clicked_asin, clicked_domain = show_asin_links_table(df, has_domain)
        if clicked_asin:
            st.session_state['rev_asin_jump'] = clicked_asin
            st.rerun()
        st.markdown("---")

    if selected_asin is None and 'asin' in df.columns:
        st.markdown(t["rev_asin_compare"])

        asin_stats = df.groupby('asin').agg(
            Reviews=('rating', 'count'),
            Rating=('rating', 'mean'),
            Neg=('rating', lambda x: (x <= 2).sum()),
            Pos=('rating', lambda x: (x >= 4).sum()),
        ).reset_index()
        asin_stats.columns = ['ASIN', 'Відгуків', 'Рейтинг', 'Негативних', 'Позитивних']
        asin_stats['Neg %'] = (asin_stats['Негативних'] / asin_stats['Відгуків'] * 100).round(1)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### ⭐ Середній рейтинг по ASINах")
            asin_sort = asin_stats.sort_values('Рейтинг', ascending=True)
            colors = ['#F44336' if r < 4.0 else '#FFC107' if r < 4.4 else '#4CAF50' for r in asin_sort['Рейтинг']]
            fig = go.Figure(go.Bar(
                x=asin_sort['Рейтинг'], y=asin_sort['ASIN'], orientation='h',
                marker_color=colors,
                text=[f"{v:.2f}★" for v in asin_sort['Рейтинг']], textposition='outside'
            ))
            fig.add_vline(x=4.0, line_dash="dash", line_color="orange", annotation_text="Поріг 4.0")
            fig.update_layout(height=max(300, len(asin_sort) * 38), xaxis_range=[1, 5.5])
            st.plotly_chart(fig, width="stretch")

        with col2:
            st.markdown("#### 🔴 % Негативних по ASINах")
            asin_neg = asin_stats.sort_values('Neg %', ascending=False)
            neg_colors = ['#F44336' if v > 20 else '#FFC107' if v > 10 else '#4CAF50' for v in asin_neg['Neg %']]
            fig2 = go.Figure(go.Bar(
                x=asin_neg['Neg %'], y=asin_neg['ASIN'], orientation='h',
                marker_color=neg_colors,
                text=[f"{v:.1f}%" for v in asin_neg['Neg %']], textposition='outside'
            ))
            fig2.update_layout(height=max(300, len(asin_neg) * 38))
            st.plotly_chart(fig2, width="stretch")

        st.markdown("#### 📋 Зведена таблиця по ASINах")
        st.dataframe(
            asin_stats.sort_values('Рейтинг').style
                .format({'Рейтинг': '{:.2f}', 'Neg %': '{:.1f}%'})
                .background_gradient(subset=['Рейтинг'], cmap='RdYlGn')
                .background_gradient(subset=['Neg %'], cmap='RdYlGn_r'),
            width="stretch"
        )

        if 'product_attributes' in df.columns:
            st.markdown("---")
            st.markdown("### 🎨 Які варіанти (Size / Color) збирають негатив?")

            df_attr = df.copy()
            df_attr['product_attributes'] = df_attr['product_attributes'].fillna('').astype(str)

            def parse_attr(s):
                size, color = None, None
                for part in s.split(','):
                    part = part.strip()
                    if part.lower().startswith('size:'):
                        size = part.split(':', 1)[1].strip()
                    elif part.lower().startswith('color:'):
                        color = part.split(':', 1)[1].strip()
                return pd.Series({'Size': size or 'N/A', 'Color': color or 'N/A'})

            parsed = df_attr['product_attributes'].apply(parse_attr)
            df_attr = pd.concat([df_attr.reset_index(drop=True), parsed], axis=1)

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 📏 Рейтинг по Size")
                size_stats = df_attr[df_attr['Size'] != 'N/A'].groupby('Size').agg(
                    Відгуків=('rating', 'count'),
                    Рейтинг=('rating', 'mean'),
                    Neg=('rating', lambda x: (x <= 2).sum()),
                ).reset_index()
                size_stats['Neg %'] = (size_stats['Neg'] / size_stats['Відгуків'] * 100).round(1)
                size_stats = size_stats[size_stats['Відгуків'] >= 3].sort_values('Рейтинг', ascending=True)
                if not size_stats.empty:
                    colors_s = ['#F44336' if r < 3.5 else '#FFC107' if r < 4.2 else '#4CAF50' for r in size_stats['Рейтинг']]
                    fig_s = go.Figure(go.Bar(
                        x=size_stats['Рейтинг'], y=size_stats['Size'], orientation='h',
                        marker_color=colors_s,
                        text=[f"{r:.2f}★ ({n:.0f}% neg)" for r, n in zip(size_stats['Рейтинг'], size_stats['Neg %'])],
                        textposition='outside',
                    ))
                    fig_s.add_vline(x=4.0, line_dash="dash", line_color="orange")
                    fig_s.update_layout(height=max(280, len(size_stats) * 40), xaxis_range=[1, 5.8])
                    st.plotly_chart(fig_s, width="stretch")
                else:
                    st.info("Недостатньо даних по розмірах")

            with col2:
                st.markdown("#### 🎨 Рейтинг по Color")
                color_stats = df_attr[df_attr['Color'] != 'N/A'].groupby('Color').agg(
                    Відгуків=('rating', 'count'),
                    Рейтинг=('rating', 'mean'),
                    Neg=('rating', lambda x: (x <= 2).sum()),
                ).reset_index()
                color_stats['Neg %'] = (color_stats['Neg'] / color_stats['Відгуків'] * 100).round(1)
                color_stats = color_stats[color_stats['Відгуків'] >= 3].sort_values('Рейтинг', ascending=True)
                if not color_stats.empty:
                    colors_c = ['#F44336' if r < 3.5 else '#FFC107' if r < 4.2 else '#4CAF50' for r in color_stats['Рейтинг']]
                    fig_c = go.Figure(go.Bar(
                        x=color_stats['Рейтинг'], y=color_stats['Color'], orientation='h',
                        marker_color=colors_c,
                        text=[f"{r:.2f}★ ({n:.0f}% neg)" for r, n in zip(color_stats['Рейтинг'], color_stats['Neg %'])],
                        textposition='outside',
                    ))
                    fig_c.add_vline(x=4.0, line_dash="dash", line_color="orange")
                    fig_c.update_layout(height=max(280, len(color_stats) * 40), xaxis_range=[1, 5.8])
                    st.plotly_chart(fig_c, width="stretch")
                else:
                    st.info("Недостатньо даних по кольорах")

            st.markdown("#### ⚠️ Топ проблемних варіантів (рейтинг < 4.0, мін. 3 відгуки)")
            df_v = df_attr[df_attr['Size'] != 'N/A'].copy()
            group_cols = ['asin', 'Size', 'Color'] if 'asin' in df_v.columns else ['Size', 'Color']
            var_group = df_v.groupby(group_cols).agg(
                Відгуків=('rating', 'count'),
                Рейтинг=('rating', 'mean'),
                Neg=('rating', lambda x: (x <= 2).sum()),
            ).reset_index()
            var_group['Neg %'] = (var_group['Neg'] / var_group['Відгуків'] * 100).round(1)
            problem = var_group[(var_group['Рейтинг'] < 4.0) & (var_group['Відгуків'] >= 3)].sort_values('Neg %', ascending=False).head(20)
            if not problem.empty:
                st.dataframe(
                    problem.style
                        .format({'Рейтинг': '{:.2f}', 'Neg %': '{:.1f}%'})
                        .background_gradient(subset=['Рейтинг'], cmap='RdYlGn')
                        .background_gradient(subset=['Neg %'], cmap='RdYlGn_r'),
                    width="stretch"
                )
            else:
                st.success("🎉 Всі варіанти мають рейтинг ≥ 4.0")

        st.markdown("---")
        st.markdown(t["rev_star_dist"])

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"#### {t['star_dist']}")
        star_counts = df['rating'].value_counts().reindex([5, 4, 3, 2, 1]).fillna(0).reset_index()
        star_counts.columns = ['Зірки', 'Кількість']
        star_counts['label'] = star_counts['Зірки'].astype(str) + '★'
        color_map = {5: '#4CAF50', 4: '#8BC34A', 3: '#FFC107', 2: '#FF9800', 1: '#F44336'}
        fig_stars = go.Figure(go.Bar(
            x=star_counts['Кількість'], y=star_counts['label'], orientation='h',
            marker_color=[color_map.get(int(s), '#888') for s in star_counts['Зірки']],
            text=star_counts['Кількість'], textposition='outside'
        ))
        fig_stars.update_layout(
            yaxis=dict(categoryorder='array', categoryarray=['1★', '2★', '3★', '4★', '5★']),
            height=300, margin=dict(l=10, r=40, t=20, b=20)
        )
        st.plotly_chart(fig_stars, width="stretch")

    with col2:
        st.markdown(f"#### {t['worst_asin']}")
        bad = df_all[df_all['rating'] <= 2]
        if selected_domains and has_domain:
            bad = bad[bad['domain'].isin(selected_domains)]
        if 'asin' in bad.columns and not bad.empty:
            bad_asins = bad['asin'].value_counts().head(8).reset_index()
            bad_asins.columns = ['ASIN', 'Негативних']
            fig_bad = px.bar(bad_asins, x='ASIN', y='Негативних', text='Негативних',
                             color='Негативних', color_continuous_scale='Reds')
            fig_bad.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig_bad, width="stretch")
        else:
            st.success("🎉 Негативних відгуків не знайдено!")

    insights_reviews(df, asin=selected_asin)

    # ── AI Chat ──
    neg_examples = df[df['rating'] <= 2][['asin','domain','rating','title','content']].head(10).to_string() if not df.empty else ""
    ctx_rev = f"""Amazon Reviews аналіз:
- Всього відгуків: {len(df)} | Середній рейтинг: {df['rating'].mean():.2f}★
- Негативних (1-2★): {int((df['rating']<=2).sum())} | Позитивних (4-5★): {int((df['rating']>=4).sum())}
- ASIN: {selected_asin or 'всі'} | Країни: {', '.join(selected_domains) if selected_domains else 'всі'}
Приклади негативних відгуків:
{neg_examples}"""
    show_ai_chat(ctx_rev, [
        "Які ASIN мають рейтинг нижче 3.5★? Скільки негативних?",
        "Які топ-3 скарги по відгуках за останній місяць?",
        "Порівняй рейтинги по країнах — де найгірше?",
    ], "reviews")

    st.markdown("---")
    st.markdown(t["rev_texts"])
    st.caption(t["rev_sort_hint"])

    display_cols = ['review_date', 'asin', 'domain', 'rating', 'title', 'content', 'product_attributes', 'author', 'is_verified']

    # ── Фільтри над таблицею ──
    fa, fb, fc = st.columns([2, 2, 1])

    with fb:
        if has_domain:
            dl_domains_raw = sorted(df['domain'].dropna().unique().tolist())
            dl_domain_opts = ["🌍 " + t.get("all_countries", "All")] + [DOMAIN_LABELS.get(d, d) for d in dl_domains_raw]
            dl_domain_label = st.selectbox("🌍 Країна:", dl_domain_opts, key="dl_domain_filter")
            dl_domain_idx = dl_domain_opts.index(dl_domain_label) - 1
            dl_domain = dl_domains_raw[dl_domain_idx] if dl_domain_idx >= 0 else None
        else:
            dl_domain = None

    with fa:
        if 'asin' in df.columns:
            df_for_asin = df[df['domain'] == dl_domain] if dl_domain else df
            asin_opts = sorted(df_for_asin['asin'].dropna().unique().tolist())
            dl_asins = ["✅ " + t.get("all_asins", "All")] + asin_opts
            dl_asin = st.selectbox("📦 ASIN:", dl_asins, key="dl_asin_filter")
        else:
            dl_asin = None

    # ── Застосовуємо фільтри ──
    df_dl = df.copy()
    if dl_domain:
        df_dl = df_dl[df_dl['domain'] == dl_domain]
    if dl_asin and not dl_asin.startswith("✅"):
        df_dl = df_dl[df_dl['asin'] == dl_asin]

    with fc:
        st.markdown("<div style='margin-top:28px'></div>", unsafe_allow_html=True)
        st.caption(f"📊 {len(df_dl)} відгуків")

    # ── Таблиця реагує на фільтри ──
    df_table = balanced_reviews(df_dl, max_per_star=100).sort_values('rating', ascending=True)
    available_cols = [c for c in display_cols if c in df_table.columns]
    dl_cols = [c for c in display_cols if c in df_dl.columns]

    st.dataframe(df_table[available_cols], width="stretch", height=450)

    star_summary = df_table['rating'].value_counts().sort_index(ascending=False)
    summary_str  = " | ".join([f"{s}★: {c}" for s, c in star_summary.items()])
    st.caption(t["rev_shown"].format(n=len(df_table), total=len(df_dl)) + f" · {summary_str}")

    # ── Кнопки скачування ──
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(t["rev_dl_balanced"],
            df_table[available_cols].to_csv(index=False).encode('utf-8'),
            f"reviews_balanced_{asin_label}.csv", "text/csv")
        st.caption(t["rev_dl_balanced_hint"])
    with col2:
        st.download_button(t["rev_dl_all"],
            df_all[dl_cols].to_csv(index=False).encode('utf-8'),
            f"reviews_full_{asin_label}.csv", "text/csv")
        st.caption(t["rev_dl_all_hint"])


# ============================================
# OTHER REPORT FUNCTIONS
# ============================================


# ════════════════════════════════════════════
# AI CHAT BLOCK — вставляється в кожен розділ
# ════════════════════════════════════════════

def get_db_schema():
    """Повертає реальну схему БД для SQL генерації."""
    schema = """
РЕАЛЬНІ ТАБЛИЦІ В БАЗІ ДАНИХ PostgreSQL (використовуй ТОЧНО ці назви):

1. spapi.sales_traffic — трафік і продажі по ASIN (схема: spapi)
   ВАЖЛИВО: таблиця в схемі spapi, запит: FROM spapi.sales_traffic
   Колонки (точні назви):
   - report_date (DATE) — дата звіту
   - child_asin (TEXT) — ASIN товару
   - parent_asin (TEXT)

   ⚠️ ВСІ ЧИСЛОВІ КОЛОНКИ ЗБЕРІГАЮТЬСЯ ЯК TEXT — ЗАВЖДИ ДОДАВАЙ CAST:
   - sessions → CAST(sessions AS FLOAT)
   - page_views → CAST(page_views AS FLOAT)
   - page_views_percentage → CAST(page_views_percentage AS FLOAT)
   - buy_box_percentage → CAST(buy_box_percentage AS FLOAT)
   - units_ordered → CAST(units_ordered AS FLOAT)
   - units_ordered_b2b → CAST(units_ordered_b2b AS FLOAT)
   - unit_session_percentage → CAST(unit_session_percentage AS FLOAT)
   - ordered_product_sales → CAST(ordered_product_sales AS FLOAT)
   - ordered_product_sales_b2b → CAST(ordered_product_sales_b2b AS FLOAT)
   - total_order_items → CAST(total_order_items AS FLOAT)
   - session_percentage → CAST(session_percentage AS FLOAT)

   Правильний приклад:
   SELECT child_asin,
          SUM(CAST(ordered_product_sales AS FLOAT)) AS revenue,
          SUM(CAST(units_ordered AS FLOAT)) AS units,
          AVG(CAST(buy_box_percentage AS FLOAT)) AS avg_bb
   FROM spapi.sales_traffic
   WHERE report_date >= CURRENT_DATE-30
   GROUP BY child_asin

2. amazon_reviews — відгуки покупців
   Колонки (точні назви):
   - id, asin (TEXT), domain (TEXT), rating (INT 1-5)
   - title (TEXT), content (TEXT), author (TEXT)
   - review_date (DATE), is_verified (BOOL)
   - product_attributes (TEXT), scraped_at (TIMESTAMP)

3. settlements — фінансові розрахунки Amazon
   Колонки (ТОЧНІ назви з лоадера v2.6, всі в подвійних лапках):
   - "Settlement ID" (TEXT)
   - "Settlement Start Date" (TEXT), "Settlement End Date" (TEXT)
   - "Deposit Date" (TEXT)
   - "Total Amount" (TEXT) — загальна сума виплати
   - "Currency" (TEXT) — 'USD', 'EUR', 'GBP' etc
   - "Transaction Type" (TEXT) — 'Order', 'Refund', 'other-transaction' etc
   - "Order ID" (TEXT)
   - "SKU" (TEXT)
   - "Description" (TEXT)
   - "Quantity" (TEXT)
   - "Marketplace" (TEXT)
   - "Amount Type" (TEXT) — тип суми
   - "Amount Description" (TEXT)
   - "Amount" (TEXT) — ⚠️ TEXT! Кастуй: CAST("Amount" AS FLOAT). Від'ємне = витрати
   - "Fulfillment ID" (TEXT)
   - "Posted Date" (TEXT) — формат DD.MM.YYYY (31.12.2025)! Кастуй: TO_DATE("Posted Date", 'DD.MM.YYYY')
   - "Posted Date Time" (TEXT) — з часом
   ⚠️ ASIN колонки НЕ ІСНУЄ в settlements!
   Приклад: SELECT SUM(CAST("Amount" AS FLOAT)) FROM settlements WHERE "Transaction Type"='Order'

4. fba_inventory — FBA Inventory (два лоадери пишуть в одну таблицю!)
   created_at (TIMESTAMP) — час запису, використовуй для фільтрації останнього знімку

   ГРУПА А — колонки від amazon_fba_inventory_loader.py v3.0 (назви з великої, в лапках):
   - "SKU" (TEXT), "ASIN" (TEXT), "FNSKU" (TEXT)
   - "Product Name" (TEXT), "Store Name" (TEXT), "Market Place" (TEXT)
   - "Available" (TEXT) — доступно, кастуй: CAST("Available" AS INT)
   - "Price" (TEXT) — ціна, кастуй: CAST("Price" AS FLOAT)
   - "Velocity" (TEXT) — продажів/день, кастуй: CAST("Velocity" AS FLOAT)
   - "Inbound" (TEXT), "Total Quantity" (TEXT), "FBA Reserved Quantity" (TEXT)
   - "Units Shipped Last 7 Days" (TEXT), "Units Shipped Last 30 Days" (TEXT)
   - "Days of Supply" (TEXT)
   - "Upto 90 Days", "91 to 180 Days", "181 to 270 Days", "271 to 365 Days", "More than 365 Days" (TEXT) — вік запасів
   ⚠️ "Stock Value" НЕ існує в БД — рахуй: CAST("Available" AS FLOAT) * CAST("Price" AS FLOAT)

   ГРУПА Б — колонки від inventory_planning_loader.py v1.0 (назви lowercase):
   - snapshot_date (DATE), sku (TEXT), asin (TEXT)
   - available (BIGINT), your_price (DOUBLE PRECISION)
   - units_shipped_t7/t30/t60/t90 (BIGINT)
   - days_of_supply (BIGINT), sell_through (DOUBLE PRECISION)
   - inv_age_0_to_90_days, inv_age_91_to_180_days etc (BIGINT)
   - estimated_storage_cost_next_month (DOUBLE PRECISION)
   - recommended_action (TEXT)

   ВИКОРИСТОВУЙ ГРУПУ А для більшості запитів (оперативні дані):
   -- Останній знімок:
   SELECT "SKU","ASIN","Available","Price","Velocity","Days of Supply",
          CAST("Available" AS FLOAT)*CAST("Price" AS FLOAT) AS stock_value
   FROM fba_inventory
   WHERE created_at >= NOW() - INTERVAL '24 hours'
     AND "Available" IS NOT NULL AND "SKU" IS NOT NULL
   ORDER BY stock_value DESC

5. returns — повернення FBA (лоадер v1.0, TRUNCATE при кожному завантаженні)
   ⚠️ Всі колонки TEXT! TRUNCATE при кожному завантаженні.
   Колонки (ТОЧНІ назви з лоадера):
   - "Order ID" (TEXT), "Order Date" (TEXT), "Return Date" (TEXT)
   - "SKU" (TEXT), "ASIN" (TEXT), "FNSKU" (TEXT), "Product Name" (TEXT)
   - "Quantity" (TEXT) — кастуй: CAST("Quantity" AS INT)
   - "Fulfillment Center" (TEXT)
   - "Detailed Disposition" (TEXT) — 'SELLABLE', 'DAMAGED', 'CUSTOMER_DAMAGED' etc
   - "Reason" (TEXT) — причина повернення: 'CUSTOMER_RETURN', 'DEFECTIVE' etc
   - "Status" (TEXT)
   - "License Plate Number" (TEXT)
   - "Customer Comments" (TEXT) — коментар покупця
   ⚠️ "Return Value" НЕ існує в БД — це обчислюється в Python через join з orders!
   ⚠️ "ASIN" колонка є, але може бути порожньою — краще фільтрувати по "SKU"
   Приклад: SELECT "SKU", COUNT(*) as cnt, SUM(CAST("Quantity" AS INT)) as units FROM returns GROUP BY "SKU" ORDER BY units DESC LIMIT 20
   Приклад топ причин: SELECT "Reason", COUNT(*) FROM returns GROUP BY "Reason" ORDER BY 2 DESC

6. orders — замовлення (лоадер v1.1, TRUNCATE при кожному завантаженні)
   ⚠️ Всі колонки TEXT! CAST при числових операціях.
   Колонки (ТОЧНІ назви з лоадера):
   - "Order ID" (TEXT) — amazon-order-id
   - "Order Date" (TEXT) — purchase-date, кастуй: CAST("Order Date" AS TIMESTAMP)
   - "SKU" (TEXT), "ASIN" (TEXT), "Product Name" (TEXT)
   - "Quantity" (TEXT) — кастуй: CAST("Quantity" AS INT)
   - "Item Price" (TEXT) — кастуй: CAST("Item Price" AS FLOAT)
   - "Item Tax" (TEXT) — кастуй: CAST("Item Tax" AS FLOAT)
   - "Shipping Price" (TEXT) — кастуй: CAST("Shipping Price" AS FLOAT)
   - "Order Status" (TEXT) — 'Shipped', 'Pending', 'Cancelled' etc
   - "Fulfillment Channel" (TEXT) — 'AFN' (FBA) або 'MFN' (FBM)
   - "Ship City" (TEXT), "Ship State" (TEXT), "Ship Country" (TEXT)
   ⚠️ НЕ існує колонка "Price" або "Currency" або "Status" — тільки "Item Price" і "Order Status"!
   Приклад: SELECT "SKU", SUM(CAST("Quantity" AS INT)) as units, SUM(CAST("Item Price" AS FLOAT)) as revenue FROM orders GROUP BY "SKU" ORDER BY revenue DESC LIMIT 20

7. advertising — Amazon Advertising (⚠️ ТАБЛИЦЯ ЩЕ НЕ ІСНУЄ в БД! Не використовуй поки що) (Sponsored Products кампанії)
   ⚠️ Всі колонки TEXT! TRUNCATE при кожному завантаженні (тільки поточні дані).
   Колонки (ТОЧНІ назви):
   - "Campaign ID" (TEXT), "Campaign Name" (TEXT), "Campaign Status" (TEXT)
   - "Impressions" (TEXT) — кастуй: CAST("Impressions" AS INT)
   - "Clicks" (TEXT) — кастуй: CAST("Clicks" AS INT)
   - "CTR" (TEXT) — % кліків, кастуй: CAST("CTR" AS FLOAT)
   - "Spend" (TEXT) — витрати $, кастуй: CAST("Spend" AS FLOAT)
   - "CPC" (TEXT) — cost per click, кастуй: CAST("CPC" AS FLOAT)
   - "Orders" (TEXT) — кастуй: CAST("Orders" AS INT)
   - "Sales" (TEXT) — продажі $, кастуй: CAST("Sales" AS FLOAT)
   - "ACOS" (TEXT) — % витрат від продажів, кастуй: CAST("ACOS" AS FLOAT)
   - "ROAS" (TEXT) — return on ad spend, кастуй: CAST("ROAS" AS FLOAT)
   Приклад: SELECT "Campaign Name", CAST("Spend" AS FLOAT) as spend, CAST("ACOS" AS FLOAT) as acos FROM advertising ORDER BY spend DESC LIMIT 10

8. ai_chat_history — історія AI чату
   Колонки: id, session_id, username, section, role, message, created_at

КРИТИЧНІ ПРАВИЛА ПРО ТИПИ ДАНИХ:
- НІКОЛИ не використовуй unicode символи ≥ ≤ ≠ — в SQL! Тільки ASCII: >= <= != --
- При розрахунку % зростання ЗАВЖДИ використовуй NULLIF щоб уникнути ділення на нуль:
  ПРАВИЛЬНО: (new - old) / NULLIF(old, 0) * 100
  НЕПРАВИЛЬНО: (new - old) / old * 100
- При використанні LAG з дефолтом 0: LAG(col, 1, 0) може бути 0 → використовуй NULLIF(LAG(col,1,0), 0)
- buy_box_percentage, unit_session_percentage — TEXT, завжди: CAST(колонка AS FLOAT)
- Якщо бачиш помилку "function avg(text)" — додай CAST(... AS FLOAT)
- Числові агрегації на TEXT колонках завжди потребують CAST

КРИТИЧНІ ПРАВИЛА:
- spapi.sales_traffic: завжди пиши FROM spapi.sales_traffic (не просто sales_traffic)
- spapi.sales_traffic: ASIN це child_asin, дата це report_date
- settlements, returns, orders, fba_inventory: колонки з великої літери брати в подвійні лапки "Column Name"
- Завжди LIMIT 50
- Тільки SELECT/WITH запити
"""
    return schema


def run_ai_sql_pipeline(question: str, section_key: str, gemini_model, context: str):
    """3-кроковий AI pipeline: SQL → PostgreSQL → Аналіз."""

    model = genai.GenerativeModel(gemini_model)
    schema = get_db_schema()

    # ── КРОК 1: Генеруємо SQL ──
    sql_prompt = f"""Ти — SQL експерт. Напиши PostgreSQL запит для відповіді на питання.

{schema}

ПИТАННЯ: {question}

ПРАВИЛА:
- Тільки SELECT запити (ніяких INSERT/UPDATE/DELETE)
- Використовуй реальні назви таблиць і колонок зі схеми
- Обмежуй результат LIMIT 50
- Для дат використовуй CURRENT_DATE
- Якщо питання не потребує SQL або незрозуміле — відповідай: NO_SQL

Відповідай ТІЛЬКИ SQL кодом без пояснень, без ```sql```, просто чистий SQL."""

    sql_resp = model.generate_content(sql_prompt)
    sql_query = sql_resp.text.strip().replace("```sql", "").replace("```", "").strip()

    if sql_query.upper().startswith("NO_SQL") or len(sql_query) < 10:
        return None, None, None

    # Безпека — тільки SELECT
    first_word = sql_query.split()[0].upper() if sql_query.split() else ""
    if first_word not in ("SELECT", "WITH"):
        return sql_query, None, "⚠️ Небезпечний запит заблоковано"

    # ── КРОК 2: Виконуємо SQL ──
    try:
        import re as _re
        # 1. Unicode → ASCII оператори (codepoint + literal)
        # Unicode → ASCII: encode/decode trick catches ANY unicode variant
        import unicodedata as _ud
        _uc_map = {'≥':'>=','≤':'<=','≠':'!=','—':'--','–':'-',
                   '−':'-','·':'*','’':"'",'‘':"'"}
        for _uc, _ac in _uc_map.items():
            sql_query = sql_query.replace(_uc, _ac)
        # Fix unquoted column refs: alias.SKU → alias."SKU" (case-sensitive columns)
        import re as _re
        _cols_upper = ['SKU','ASIN','FNSKU']
        for _col in _cols_upper:
            # alias.SKU → alias."SKU" but not alias."SKU" already
            sql_query = _re.sub(
                rf'([a-zA-Z_]\w*)\.{_col}(?![\w"])',
                rf'\1."{_col}"',
                sql_query
            )
        # Auto-cast "Order Date" TEXT → TIMESTAMP перед порівнянням
        import re as _re2
        sql_query = _re2.sub(
            r'"Order Date"\s*(>=|<=|>|<|=|<>)\s*',
            lambda m: f'CAST("Order Date" AS TIMESTAMP) {m.group(1)} ',
            sql_query
        )
        # Auto-cast report_date (TEXT) перед порівнянням
        def _fix_rd(m):
            return f'CAST(report_date AS DATE) {m.group(1)}'
        sql_query = _re.sub(
            r'(?<![A-Za-z_])report_date\s*(>=|<=|>|<|=|<>)',
            _fix_rd,
            sql_query
        )
        # Фільтруємо порожні report_date в sales_traffic
        if 'spapi.sales_traffic' in sql_query:
            sql_query = _re.sub(
                r'(FROM spapi\.sales_traffic\s+WHERE\s)',
                r"\1report_date != '' AND report_date IS NOT NULL AND ",
                sql_query
            )
        # 3. Auto-fix NULLIF для відомих TEXT колонок — з будь-яким аліасом або без
        # Regex: CAST([alias.]"Column" AS TYPE) → CAST(NULLIF([alias.]"Column", '') AS TYPE)
        for _col, _type in [
            ('"Available"','INT'),('"Available"','FLOAT'),
            ('"Price"','FLOAT'),('"Velocity"','FLOAT'),
            ('"Days of Supply"','FLOAT'),('"Days of Supply"','INT'),
            ('"Quantity"','INT'),('"Item Price"','FLOAT'),
            ('"Item Tax"','FLOAT'),('"Amount"','FLOAT'),
            ('"Spend"','FLOAT'),('"Sales"','FLOAT'),
            ('"ACOS"','FLOAT'),('"ROAS"','FLOAT'),
        ]:
            # З аліасом: CAST(alias."Col" AS TYPE) → CAST(NULLIF(alias."Col",'') AS TYPE)
            sql_query = _re.sub(
                rf'CAST\(([a-zA-Z_]\w*\.)?{_re.escape(_col)}\s+AS\s+{_type}\)',
                lambda m, c=_col, t=_type: f"CAST(NULLIF({m.group(1) or ''}{c}, '') AS {t})",
                sql_query
            )
        # (legacy pairs below kept for safety)
        # 2.5 Захист від ділення на нуль — NULLIF для знаменника
        # LAG(..., 1, 0) / LAG(...) → може бути 0, замінюємо на NULLIF
        sql_query = _re.sub(
            r'\)\s*/\s*LAG\(',
            r') / NULLIF(LAG(',
            sql_query
        )
        sql_query = _re.sub(
            r'NULLIF\(LAG\(([^)]+)\)\)',
            r'NULLIF(LAG(), 0)',
            sql_query
        )
        # 3. Simple string replace для TEXT колонок — без regex lambda
        _nullif_pairs = [
            ('"Available"', 'FLOAT'), ('"Available"', 'INT'),
            ('"Price"', 'FLOAT'), ('"Velocity"', 'FLOAT'),
            ('"Days of Supply"', 'FLOAT'), ('"Days of Supply"', 'INT'),
            ('"Quantity"', 'INT'), ('"Item Price"', 'FLOAT'),
            ('"Item Tax"', 'FLOAT'), ('"Shipping Price"', 'FLOAT'),
            ('"Total Amount"', 'FLOAT'), ('"Amount"', 'FLOAT'),
            ('"Impressions"', 'INT'), ('"Clicks"', 'INT'),
            ('"Spend"', 'FLOAT'), ('"Sales"', 'FLOAT'),
            ('"ACOS"', 'FLOAT'), ('"ROAS"', 'FLOAT'),
            ('"CTR"', 'FLOAT'), ('"CPC"', 'FLOAT'), ('"Orders"', 'INT'),
        ]
        for _col, _type in _nullif_pairs:
            _old = f'CAST({_col} AS {_type})'
            _new = f"CAST(NULLIF({_col}, '') AS {_type})"
            sql_query = sql_query.replace(_old, _new)
        engine = get_engine()
        import pandas as _pd
        with engine.connect() as _conn:
            df_result = _pd.read_sql(text(sql_query), _conn)
        if df_result.empty:
            return sql_query, df_result, None
    except Exception as e:
        return sql_query, None, f"SQL помилка: {e}"

    # ── КРОК 3: AI аналізує результат ──
    result_str = df_result.to_string(index=False, max_rows=30)

    analysis_prompt = f"""Ти — експерт з Amazon FBA бізнесу.

Питання користувача: {question}

Результат SQL запиту:
{result_str}

Додатковий контекст розділу:
{context[:500]}

Дай конкретну, actionable відповідь з числами з результату.
Стисло, по суті. Виділяй ключові числа жирним (**число**)."""

    analysis_resp = model.generate_content(analysis_prompt)
    return sql_query, df_result, analysis_resp.text


def show_ai_chat(context: str, preset_questions: list, section_key: str):
    """AI-чат з 3-кроковим SQL pipeline і пам'яттю в БД."""
    st.markdown("---")
    st.markdown("### 🤖 AI Інсайти")

    # ── Ключ Gemini ──
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    if not gemini_key:
        gemini_key = st.secrets.get("GEMINI_API_KEY", "") if hasattr(st, "secrets") else ""
    if not gemini_key:
        st.info("💡 Додай GEMINI_API_KEY в Streamlit Secrets щоб активувати AI-чат")
        return
    if not GEMINI_OK:
        st.warning("pip install google-generativeai")
        return

    # ── Ротація ключів для збільшення квоти ──
    gemini_key2 = os.environ.get("GEMINI_API_KEY_2", "")
    if not gemini_key2 and hasattr(st, "secrets"):
        gemini_key2 = st.secrets.get("GEMINI_API_KEY_2", "")

    if gemini_key2:
        # Ротація по лічильнику запитів
        if "gemini_key_counter" not in st.session_state:
            st.session_state["gemini_key_counter"] = 0
        st.session_state["gemini_key_counter"] += 1
        # Чергуємо ключі кожен запит
        active_key = gemini_key if st.session_state["gemini_key_counter"] % 2 == 0 else gemini_key2
    else:
        active_key = gemini_key

    genai.configure(api_key=active_key)
    gemini_model = os.environ.get("GEMINI_MODEL", "")
    if not gemini_model:
        gemini_model = st.secrets.get("GEMINI_MODEL", "gemini-1.5-flash") if hasattr(st, "secrets") else "gemini-1.5-flash"

    # ── ID сесії ──
    if "ai_session_id" not in st.session_state:
        import uuid, datetime as _dt
        _user = st.session_state.get("user", {})
        _uid  = str(_user.get("id", "anon"))
        _date = _dt.date.today().isoformat()
        st.session_state["ai_session_id"] = f"{_uid}_{_date}_{str(uuid.uuid4())[:6]}"
    session_id = st.session_state["ai_session_id"]

    # ── Завантажуємо історію ──
    hist_key = f"ai_history_{section_key}"
    if hist_key not in st.session_state:
        st.session_state[hist_key] = load_chat_history(session_id, section_key)
    history = st.session_state[hist_key]

    # ── Показуємо попередні повідомлення ──
    if history:
        for msg in history[-10:]:
            if msg["role"] == "user":
                st.chat_message("user").markdown(msg["content"])
            else:
                st.chat_message("assistant").markdown(msg["content"])

        if st.button("🗑 Очистити чат", key=f"ai_clear_{section_key}"):
            st.session_state[hist_key] = []
            try:
                engine = get_engine()
                with engine.connect() as conn:
                    conn.execute(text("DELETE FROM ai_chat_history WHERE session_id=:sid AND section=:sec"),
                                 {"sid": session_id, "sec": section_key})
                    conn.commit()
            except Exception:
                pass
            st.rerun()

    # ── Швидкі кнопки ──
    ai_cols = st.columns(len(preset_questions))
    auto_q = None
    for i, (col, q) in enumerate(zip(ai_cols, preset_questions)):
        if col.button(q, key=f"ai_btn_{section_key}_{i}", use_container_width=True):
            auto_q = q

    # ── Поле вводу ──
    user_q = st.chat_input("💬 Питання про ваші дані...", key=f"ai_input_{section_key}")
    final_q = auto_q or user_q

    if final_q:
        st.chat_message("user").markdown(final_q)

        with st.spinner("🔍 Крок 1: AI складає SQL..."):
            try:
                sql_query, df_result, analysis = run_ai_sql_pipeline(
                    final_q, section_key, gemini_model, context
                )
            except Exception as e:
                st.error(f"Помилка: {e}")
                return

        if sql_query:
            with st.expander("🔍 SQL запит", expanded=True):
                st.code(sql_query, language="sql")

        if isinstance(analysis, str) and analysis.startswith("⚠️"):
            st.error(analysis)
            return

        if df_result is not None and not df_result.empty:
            with st.expander(f"⚡ Результат з БД ({len(df_result)} рядків)", expanded=False):
                st.dataframe(df_result, use_container_width=True)

        if analysis and not analysis.startswith("SQL помилка"):
            answer_md = analysis
        elif df_result is not None and df_result.empty:
            answer_md = "📭 Запит виконався, але даних не знайдено по цьому питанню."
        elif analysis and analysis.startswith("SQL помилка"):
            # Fallback — відповідаємо на основі контексту
            with st.spinner("🤖 AI аналізує контекст..."):
                try:
                    m = genai.GenerativeModel(gemini_model)
                    fallback_prompt = "Amazon FBA експерт. Дані: " + context[:1000] + "\nПитання: " + final_q + "\nВідповідь:"
                    r = m.generate_content(fallback_prompt)
                    answer_md = r.text + "\n\n*⚠️ SQL не спрацював: " + str(analysis) + "*"
                except Exception as e2:
                    answer_md = f"Помилка: {e2}"
        else:
            answer_md = "Не вдалось отримати відповідь."

        with st.chat_message("assistant"):
            st.markdown(answer_md)

        # ── Зберігаємо ──
        history.append({"role": "user", "content": final_q})
        history.append({"role": "assistant", "content": answer_md})
        st.session_state[hist_key] = history

        username = st.session_state.get("user", {}).get("email", "unknown")
        save_chat_message(session_id, username, section_key, "user", final_q)
        save_chat_message(session_id, username, section_key, "assistant", answer_md)






def show_inventory_unified():
    st.markdown("### 📦 Склад (Inventory) — spapi.inventory_unified")
    st.caption("Всі колонки з таблиці spapi.inventory_unified")

    import psycopg2, pandas as _pd
    try:
        conn = psycopg2.connect(os.environ.get("DATABASE_URL", ""))

        # Рядків і колонок
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM spapi.inventory_unified")
        total_rows = cur.fetchone()[0]

        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'spapi' AND table_name = 'inventory_unified'
            ORDER BY ordinal_position
        """)
        cols_info = cur.fetchall()
        cur.close()

        col1, col2, col3 = st.columns(3)
        col1.metric("📊 Рядків", f"{total_rows:,}")
        col2.metric("📋 Колонок", len(cols_info))
        col3.metric("🗄️ Таблиця", "spapi.inventory_unified")

        st.markdown("---")

        # Фільтри
        c1, c2, c3 = st.columns([2, 2, 1])
        search_sku  = c1.text_input("🔍 Пошук по SKU", "")
        search_asin = c2.text_input("🔍 Пошук по ASIN", "")
        limit       = c3.selectbox("Рядків", [100, 500, 1000, 5000], index=0)

        # SQL з фільтрами
        where = []
        params = []
        if search_sku:
            where.append("sku ILIKE %s")
            params.append(f"%{search_sku}%")
        if search_asin:
            where.append("asin ILIKE %s")
            params.append(f"%{search_asin}%")

        where_clause = "WHERE " + " AND ".join(where) if where else ""
        sql = f"SELECT * FROM spapi.inventory_unified {where_clause} ORDER BY snapshot_date DESC LIMIT %s"
        params.append(limit)

        df = _pd.read_sql(sql, conn, params=params)
        conn.close()

        st.caption(f"Показано {len(df):,} з {total_rows:,} рядків")
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Кнопка завантаження
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "⬇️ Завантажити CSV",
            csv,
            "inventory_unified.csv",
            "text/csv"
        )

        # Інфо по колонках
        with st.expander("📋 Всі колонки таблиці"):
            cols_df = _pd.DataFrame(cols_info, columns=["Колонка", "Тип даних"])
            st.dataframe(cols_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Помилка: {e}")

def show_etl_status():
    st.markdown("### 📊 ETL Status — стан завантажувачів")
    st.caption("Актуальні дані з БД")

    import psycopg2, datetime as _dt
    try:
        conn = psycopg2.connect(os.environ.get("DATABASE_URL", ""))
        cur = conn.cursor()

        def q(sql):
            try:
                cur.execute(sql)
                r = cur.fetchone()
                return (r[0], r[1]) if r else (0, None)
            except Exception:
                conn.rollback()
                return (0, None)

        sql_settlements = 'SELECT COUNT(*), MAX(TO_DATE("Posted Date", \'DD.MM.YYYY\')) FROM settlements WHERE "Posted Date" != \'\''
        sql_orders      = 'SELECT COUNT(*), MAX(CAST("Order Date" AS TIMESTAMP)) FROM orders'
        sql_returns     = 'SELECT COUNT(*), MAX(CAST("Return Date" AS TIMESTAMP)) FROM returns'
        sql_fba         = 'SELECT COUNT(*), MAX(created_at) FROM fba_inventory'
        sql_st          = "SELECT COUNT(*), MAX(CAST(report_date AS DATE)) FROM spapi.sales_traffic WHERE report_date != ''"
        sql_rev         = 'SELECT COUNT(*), MAX(created_at) FROM amazon_reviews'

        modules = [
            ("📦 FBA Inventory",   "fba_inventory",       q(sql_fba),         "3× / день"),
            ("🏦 Settlements",     "settlements",         q(sql_settlements), "3× / день"),
            ("🛒 Orders",          "orders",              q(sql_orders),      "2× / день"),
            ("📦 Returns",         "returns",             q(sql_returns),     "1× / день"),
            ("📈 Sales & Traffic", "spapi.sales_traffic", q(sql_st),          "2× / день"),
            ("⭐ Reviews",         "amazon_reviews",      q(sql_rev),         "за запитом"),
            ("📣 Advertising",     "advertising",         (0, None),          "—"),
        ]
        cur.close()
        conn.close()
    except Exception as e:
        st.error(f"DB помилка: {e}")
        return

    now = _dt.datetime.utcnow().date()
    data = []
    for name, table, (cnt, last), freq in modules:
        if cnt and cnt > 0:
            try:
                last_date = last.date() if hasattr(last, 'date') else _dt.date.fromisoformat(str(last)[:10])
                delta = (now - last_date).days
                age = "сьогодні" if delta == 0 else f"{delta}д тому"
                status = "✅ OK" if delta <= 1 else "⚠️ Застарів"
            except Exception:
                age = str(last)[:10] if last else "—"
                status = "✅ OK"
            data.append({"Модуль": name, "Таблиця": table, "Рядків": f"{cnt:,}",
                         "Останнє оновлення": age, "Частота": freq, "Статус": status})
        else:
            data.append({"Модуль": name, "Таблиця": table, "Рядків": "—",
                         "Останнє оновлення": "—", "Частота": freq, "Статус": "⏳ Немає даних"})

    import pandas as _pd
    st.dataframe(_pd.DataFrame(data), use_container_width=True, hide_index=True)

def show_about():
    st.markdown("""
<style>
/* Адаптивні кольори для світлої і темної теми */
:root {
  --ab-bg: #f8fafc;
  --ab-border: #e2e8f0;
  --ab-text: #1e293b;
  --ab-muted: #64748b;
  --ab-accent: #d97706;
  --ab-card: #ffffff;
}
@media (prefers-color-scheme: dark) {
  :root { --ab-bg:#0f1218; --ab-border:#1e2330; --ab-text:#e2e8f0; --ab-muted:#64748b; --ab-accent:#e8b84b; --ab-card:#161b24; }
}
[data-theme="dark"] {
  --ab-bg:#0f1218; --ab-border:#1e2330; --ab-text:#e2e8f0; --ab-muted:#64748b; --ab-accent:#e8b84b; --ab-card:#161b24;
}

.about-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:1px; background:var(--ab-border); border:1px solid var(--ab-border); margin:24px 0; }
.about-stat { background:var(--ab-card); padding:20px; text-align:center; }
.about-stat-num { font-size:28px; font-weight:800; color:var(--ab-accent); font-family:monospace; }
.about-stat-lbl { font-size:11px; color:var(--ab-muted); text-transform:uppercase; letter-spacing:1px; margin-top:4px; }
.module-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin:16px 0 32px; }
.mod { background:var(--ab-card); border:1px solid var(--ab-border); border-top:3px solid var(--c,#e8b84b); padding:16px; border-radius:4px; }
.mod-icon { font-size:20px; margin-bottom:8px; }
.mod-name { font-size:13px; font-weight:700; margin-bottom:4px; color:var(--ab-text); }
.mod-desc { font-size:12px; color:var(--ab-muted); line-height:1.5; }
.pipe { display:flex; align-items:center; flex-wrap:wrap; gap:4px; margin:16px 0 32px; }
.pipe-step { background:var(--ab-card); border:1px solid var(--ab-border); padding:8px 14px; font-size:12px; font-family:monospace; color:var(--ab-text); border-radius:3px; }
.pipe-arr { color:var(--ab-accent); padding:0 4px; font-weight:bold; }
</style>
""", unsafe_allow_html=True)

    st.markdown(t["about_title"])
    st.caption(t["about_caption"])
    st.markdown("---")

    # Stats
    st.markdown("""
<div class="about-grid">
  <div class="about-stat"><div class="about-stat-num">30+</div><div class="about-stat-lbl">Типів звітів</div></div>
  <div class="about-stat"><div class="about-stat-num">36×</div><div class="about-stat-lbl">Оновлень/день</div></div>
  <div class="about-stat"><div class="about-stat-num">9</div><div class="about-stat-lbl">Маркетплейсів</div></div>
  <div class="about-stat"><div class="about-stat-num">3</div><div class="about-stat-lbl">Мови інтерфейсу</div></div>
</div>""", unsafe_allow_html=True)

    # Modules
    st.markdown(t["about_modules"])
    st.markdown("""
<div class="module-grid">
  <div class="mod" style="--c:#5b9bd5"><div class="mod-icon">📈</div><div class="mod-name">Sales & Traffic</div><div class="mod-desc">Сесії, конверсія, Buy Box, дохід по ASIN і маркетплейсу щодня</div></div>
  <div class="mod" style="--c:#e8b84b"><div class="mod-icon">⭐</div><div class="mod-name">Amazon Reviews</div><div class="mod-desc">2500+ відгуків, аналіз по країнах, негатив, AI-аналіз проблем продукту</div></div>
  <div class="mod" style="--c:#4caf82"><div class="mod-icon">💰</div><div class="mod-name">Settlements</div><div class="mod-desc">Net Payout, комісії Amazon, рефанди, P&L по валютах і датах</div></div>
  <div class="mod" style="--c:#e05252"><div class="mod-icon">📦</div><div class="mod-name">Inventory Health</div><div class="mod-desc">Залишки, velocity, aging-аналіз, заморожені кошти по SKU</div></div>
  <div class="mod" style="--c:#a78bfa"><div class="mod-icon">🛒</div><div class="mod-name">Orders Analytics</div><div class="mod-desc">Тренди замовлень, топ SKU, сезонність, динаміка продажів</div></div>
  <div class="mod" style="--c:#f97316"><div class="mod-icon">🤖</div><div class="mod-name">AI Insights</div><div class="mod-desc">Gemini AI з контекстом реальних даних у кожному розділі</div></div>
</div>""", unsafe_allow_html=True)

    # Pipeline
    st.markdown(t["about_pipeline"])
    st.markdown("""
<div class="pipe">
  <span class="pipe-step">Amazon SP-API</span><span class="pipe-arr">→</span>
  <span class="pipe-step">12 ETL Loaders</span><span class="pipe-arr">→</span>
  <span class="pipe-step">PostgreSQL</span><span class="pipe-arr">→</span>
  <span class="pipe-step">Streamlit Cloud</span><span class="pipe-arr">→</span>
  <span class="pipe-step">Gemini AI</span><span class="pipe-arr">→</span>
  <span class="pipe-step">Insights & Actions</span>
</div>""", unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(t["about_features"])
        st.markdown("""
- Ролева авторизація: admin / user з правами по звітах  
- Мультимовність: 🇺🇦 UA / 🇺🇸 EN / 🌍 RU  
- Фільтри ASIN × Країна у всіх розділах  
- CSV-експорт: balanced вибірка або повний дамп БД  
- Дата останнього збору відгуків по кожному ASIN  
""")
    with col2:
        st.markdown(t["about_stack"])
        st.markdown("""
- **Backend**: Python, PostgreSQL, SQLAlchemy  
- **Frontend**: Streamlit, Plotly  
- **APIs**: Amazon SP-API, Advertising API, Apify  
- **AI**: Google Gemini  
- **Deploy**: Streamlit Cloud  
""")

    # ETL Status Table
    st.markdown("---")
    st.markdown("### 📊 Статус ETL модулів")
    st.markdown("""
<style>
.etl-table { width:100%; border-collapse:collapse; font-size:13px; margin-bottom:24px; }
.etl-table th { background:var(--ab-card); color:var(--ab-muted); font-weight:600;
    text-transform:uppercase; font-size:11px; letter-spacing:1px;
    padding:10px 14px; border-bottom:2px solid var(--ab-border); text-align:left; }
.etl-table td { padding:10px 14px; border-bottom:1px solid var(--ab-border); color:var(--ab-text); }
.etl-table tr:hover td { background:var(--ab-bg); }
.badge { display:inline-block; padding:2px 10px; border-radius:20px; font-size:11px; font-weight:700; }
.badge-ok  { background:#dcfce7; color:#166534; }
.badge-wip { background:#fef9c3; color:#854d0e; }
.badge-no  { background:#fee2e2; color:#991b1b; }
</style>
<table class="etl-table">
  <tr>
    <th>Модуль</th><th>Таблиця</th><th>Оновлення</th><th>Рядків</th><th>Статус</th>
  </tr>
  <tr><td>📦 FBA Inventory</td><td><code>fba_inventory</code></td><td>3× / день</td><td>~19,000</td><td><span class="badge badge-ok">✅ Активний</span></td></tr>
  <tr><td>🏦 Settlements</td><td><code>settlements</code></td><td>3× / день</td><td>~1,534,000</td><td><span class="badge badge-ok">✅ Активний</span></td></tr>
  <tr><td>🛒 Orders</td><td><code>orders</code></td><td>2× / день</td><td>~24,600</td><td><span class="badge badge-ok">✅ Активний</span></td></tr>
  <tr><td>📦 Returns</td><td><code>returns</code></td><td>1× / день</td><td>~4,200</td><td><span class="badge badge-ok">✅ Активний</span></td></tr>
  <tr><td>📈 Sales & Traffic</td><td><code>spapi.sales_traffic</code></td><td>2× / день</td><td>~22,500</td><td><span class="badge badge-ok">✅ Активний</span></td></tr>
  <tr><td>⭐ Reviews</td><td><code>amazon_reviews</code></td><td>за запитом</td><td>~2,600</td><td><span class="badge badge-ok">✅ Активний</span></td></tr>
  <tr><td>📣 Advertising</td><td><code>advertising</code></td><td>—</td><td>—</td><td><span class="badge badge-wip">⏳ В розробці</span></td></tr>
</table>
""", unsafe_allow_html=True)

    st.markdown("---")
    st.caption(t["about_footer"])


def show_overview(df_filtered, t, selected_date):
    st.markdown(f"### {t['ov_title']}")
    st.caption(f"📅 {selected_date}")

    # ── Завантажуємо додаткові дані ──
    df_st       = load_sales_traffic()
    df_settle   = load_settlements()
    df_reviews  = load_reviews()

    import datetime as _dt

    # ═══════════════════════════════════════════
    # БЛОК 1 — 🚨 ЩО ГОРИТЬ
    # ═══════════════════════════════════════════
    st.markdown("### 🚨 Що потребує уваги прямо зараз")

    alerts = []

    # Out-of-stock ризик
    if not df_filtered.empty and 'Velocity' in df_filtered.columns and 'Available' in df_filtered.columns:
        df_risk = df_filtered[df_filtered['Velocity'] > 0].copy()
        df_risk['days_left'] = (df_risk['Available'] / df_risk['Velocity']).round(0)
        critical = df_risk[df_risk['days_left'] < 14].sort_values('days_left')
        warning  = df_risk[(df_risk['days_left'] >= 14) & (df_risk['days_left'] < 30)].sort_values('days_left')
        if not critical.empty:
            alerts.append(("🔴", f"{len(critical)} SKU закінчаться за <14 днів", "critical", critical))
        if not warning.empty:
            alerts.append(("🟡", f"{len(warning)} SKU залишилось 14–30 днів", "warning", warning))

    # Buy Box < 80%
    if not df_st.empty:
        last_30 = df_st[df_st['report_date'] >= (df_st['report_date'].max() - pd.Timedelta(days=30))]
        asin_col = 'child_asin' if 'child_asin' in last_30.columns else last_30.columns[0]
        bb = last_30.groupby(asin_col)['buy_box_percentage'].mean()
        low_bb = bb[bb < 80]
        if not low_bb.empty:
            alerts.append(("🟠", f"{len(low_bb)} ASIN з Buy Box < 80%", "warning", None))

    # Погані відгуки
    if not df_reviews.empty and 'rating' in df_reviews.columns:
        bad_rev = df_reviews[df_reviews['rating'] <= 2]
        last_7d = pd.Timestamp.now() - pd.Timedelta(days=7)
        if 'review_date' in df_reviews.columns:
            recent_bad = bad_rev[pd.to_datetime(bad_rev['review_date'], errors='coerce') >= last_7d]
            if not recent_bad.empty:
                alerts.append(("⭐", f"{len(recent_bad)} відгуків 1-2★ за останні 7 днів", "warning", None))

    if alerts:
        for emoji, msg, severity, df_detail in alerts:
            color = "#2b0d0d" if severity == "critical" else "#2b2000"
            border = "#f04f5a" if severity == "critical" else "#f0a500"
            text_color = "#ffcccc" if severity == "critical" else "#ffe0a0"
            st.markdown(f"""
<div style="background:{color};border-left:3px solid {border};padding:10px 16px;margin:4px 0;border-radius:3px;font-size:14px;color:{text_color}">
{emoji} <b>{msg}</b>
</div>""", unsafe_allow_html=True)
            if df_detail is not None and not df_detail.empty:
                with st.expander(f"Показати SKU ({len(df_detail)})"):
                    show_cols = ['SKU','Available','days_left']
                    show_cols = [c for c in show_cols if c in df_detail.columns]
                    st.dataframe(df_detail[show_cols].head(10), use_container_width=True)
    else:
        st.success("✅ Все в нормі — критичних проблем не виявлено")

    # ═══════════════════════════════════════════
    # БЛОК 2 — 💰 ГРОШІ
    # ═══════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 💰 Фінанси — поточний стан")

    c1,c2,c3,c4,c5 = st.columns(5)

    # Склад
    stock_val = df_filtered['Stock Value'].sum() if 'Stock Value' in df_filtered.columns else 0
    total_units = int(df_filtered['Available'].sum()) if 'Available' in df_filtered.columns else 0
    c1.metric("💰 Вартість складу", f"${stock_val:,.0f}")
    c2.metric("📦 Штук на складі", f"{total_units:,}")

    # Settlements цей місяць vs минулий
    if not df_settle.empty and 'Amount' in df_settle.columns and 'Posted Date' in df_settle.columns:
        df_settle['Posted Date'] = pd.to_datetime(df_settle['Posted Date'], errors='coerce')
        now = pd.Timestamp.now()
        this_month = df_settle[
            (df_settle['Posted Date'].dt.month == now.month) &
            (df_settle['Posted Date'].dt.year == now.year)
        ]['Amount'].sum()
        prev_month = df_settle[
            (df_settle['Posted Date'].dt.month == (now - pd.DateOffset(months=1)).month) &
            (df_settle['Posted Date'].dt.year == (now - pd.DateOffset(months=1)).year)
        ]['Amount'].sum()
        delta_pct = ((this_month - prev_month) / abs(prev_month) * 100) if prev_month != 0 else 0
        c3.metric("🏦 Payout цей місяць", f"${this_month:,.0f}", f"{delta_pct:+.1f}% vs минулий")
    else:
        c3.metric("🏦 Payout", "—")

    # Продажі 30д
    if not df_st.empty:
        last_30_sales = df_st[df_st['report_date'] >= (df_st['report_date'].max() - pd.Timedelta(days=30))]
        prev_30_sales = df_st[
            (df_st['report_date'] >= (df_st['report_date'].max() - pd.Timedelta(days=60))) &
            (df_st['report_date'] < (df_st['report_date'].max() - pd.Timedelta(days=30)))
        ]
        rev_cur = last_30_sales['ordered_product_sales'].sum()
        rev_prv = prev_30_sales['ordered_product_sales'].sum()
        rev_delta = ((rev_cur - rev_prv) / abs(rev_prv) * 100) if rev_prv != 0 else 0
        c4.metric("📈 Дохід 30д", f"${rev_cur:,.0f}", f"{rev_delta:+.1f}% vs попередні 30д")
        c5.metric("🛒 Одиниць 30д", f"{int(last_30_sales['units_ordered'].sum()):,}")
    else:
        c4.metric("📈 Дохід 30д", "—")
        c5.metric("🛒 Одиниць 30д", "—")

    # ═══════════════════════════════════════════
    # БЛОК 3 — 📈 ТРЕНД + HEALTH TABLE
    # ═══════════════════════════════════════════
    st.markdown("---")
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown("### 📈 Тренд продажів — 60 днів")
        if not df_st.empty:
            daily = df_st.groupby(df_st['report_date'].dt.date).agg(
                Revenue=('ordered_product_sales','sum'),
                Units=('units_ordered','sum')
            ).reset_index().tail(60)
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=daily['report_date'], y=daily['Revenue'],
                name='Revenue $', marker_color='#4f8ef7', opacity=0.8
            ))
            fig.add_trace(go.Scatter(
                x=daily['report_date'], y=daily['Units'],
                name='Units', mode='lines', line=dict(color='#f0a500', width=2),
                yaxis='y2'
            ))
            fig.update_layout(
                height=280, margin=dict(l=0,r=0,t=10,b=0),
                yaxis=dict(title='Revenue $', showgrid=True, gridcolor='rgba(128,128,128,0.1)'),
                yaxis2=dict(title='Units', overlaying='y', side='right', showgrid=False),
                legend=dict(orientation='h', y=1.1),
                plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Немає даних Sales & Traffic")

    with col_right:
        st.markdown("### 🏥 SKU Health Check")
        if not df_filtered.empty and 'Velocity' in df_filtered.columns:
            df_health = df_filtered[df_filtered['Velocity'] > 0].copy()
            df_health['days_left'] = (df_health['Available'] / df_health['Velocity']).round(0).astype(int)
            df_health = df_health.nlargest(12, 'Available')[['SKU','Available','days_left']]
            def health_icon(d):
                if d < 14:   return "🔴"
                elif d < 30: return "🟡"
                else:        return "✅"
            df_health['Status'] = df_health['days_left'].apply(health_icon)
            df_health.columns = ['SKU','Залишок','Днів','⚡']
            st.dataframe(df_health, use_container_width=True, height=280, hide_index=True)
        else:
            st.info("Немає даних інвентаря")

    # ═══════════════════════════════════════════
    # AI CHAT
    # ═══════════════════════════════════════════
    # Збираємо контекст для AI
    _stock_str = f"${stock_val:,.0f}" if stock_val else "н/д"
    _alerts_str = "; ".join([a[1] for a in alerts]) if alerts else "критичних проблем немає"
    _rev_str = f"${rev_cur:,.0f} ({rev_delta:+.1f}%)" if not df_st.empty else "н/д"

    ctx_overview = f"""CEO Overview — стан бізнесу:
- Вартість складу: {_stock_str} | Штук: {total_units:,}
- Дохід 30д: {_rev_str}
- Алерти: {_alerts_str}
- SKU всього: {len(df_filtered)}
- SKU з ризиком out-of-stock <14д: {len(critical) if 'critical' in dir() and not critical.empty else 0}
"""
    show_ai_chat(ctx_overview, [
        "Загальний стан бізнесу — що найтерміновіше виправити?",
        "Які SKU під ризиком out-of-stock найближчі 14 днів?",
        "Де заморожені гроші при низьких продажах?",
    ], "overview")


def show_sales_traffic(t):
    df_st = load_sales_traffic()
    if df_st.empty:
        st.warning("⚠️ No Sales & Traffic data found."); return
    st.sidebar.markdown("---"); st.sidebar.subheader(t["st_filters"])
    min_date = df_st['report_date'].min().date()
    max_date = df_st['report_date'].max().date()
    date_range = st.sidebar.date_input(t["st_date_range"],
        value=(max(min_date, max_date-dt.timedelta(days=14)), max_date),
        min_value=min_date, max_value=max_date, key="st_date_range")
    if len(date_range)==2:
        mask = (df_st['report_date'].dt.date>=date_range[0])&(df_st['report_date'].dt.date<=date_range[1])
        df_filtered = df_st[mask]
    else:
        df_filtered = df_st
    if df_filtered.empty:
        st.warning("No data for selected period"); return
    st.markdown(f"### {t['sales_traffic_title']}")
    ts = int(df_filtered['sessions'].sum()); tpv = int(df_filtered['page_views'].sum())
    tu = int(df_filtered['units_ordered'].sum()); tr = df_filtered['ordered_product_sales'].sum()
    ac = tu/ts*100 if ts>0 else 0; ab = df_filtered['buy_box_percentage'].mean()
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric(t["st_sessions"],f"{ts:,}"); c2.metric(t["st_page_views"],f"{tpv:,}")
    c3.metric(t["st_units"],f"{tu:,}"); c4.metric(t["st_revenue"],f"${tr:,.2f}")
    c5.metric(t["st_conversion"],f"{ac:.2f}%"); c6.metric(t["st_buy_box"],f"{ab:.1f}%")
    st.markdown("---"); st.markdown(t["st_daily_trends"])
    daily = df_filtered.groupby(df_filtered['report_date'].dt.date).agg(
        {'sessions':'sum','page_views':'sum','units_ordered':'sum','ordered_product_sales':'sum'}).reset_index()
    daily.columns = ['Date','Sessions','Page Views','Units','Revenue']
    daily['Conversion %'] = (daily['Units']/daily['Sessions']*100).fillna(0)
    col1,col2 = st.columns(2)
    with col1:
        st.markdown(t["st_sessions_views"])
        fig = go.Figure()
        fig.add_trace(go.Bar(x=daily['Date'],y=daily['Sessions'],name='Sessions',marker_color='#4472C4'))
        fig.add_trace(go.Scatter(x=daily['Date'],y=daily['Page Views'],name='Page Views',mode='lines+markers',line=dict(color='#ED7D31',width=2),yaxis='y2'))
        fig.update_layout(yaxis=dict(title='Sessions'),yaxis2=dict(title='Page Views',overlaying='y',side='right'),height=380,legend=dict(orientation='h',y=1.12))
        st.plotly_chart(fig, width="stretch")
    with col2:
        st.markdown(t["st_revenue_units"])
        fig = go.Figure()
        fig.add_trace(go.Bar(x=daily['Date'],y=daily['Revenue'],name='Revenue $',marker_color='#70AD47'))
        fig.add_trace(go.Scatter(x=daily['Date'],y=daily['Units'],name='Units',mode='lines+markers',line=dict(color='#FFC000',width=2),yaxis='y2'))
        fig.update_layout(yaxis=dict(title='Revenue $'),yaxis2=dict(title='Units',overlaying='y',side='right'),height=380,legend=dict(orientation='h',y=1.12))
        st.plotly_chart(fig, width="stretch")
    fig_conv = go.Figure(go.Scatter(x=daily['Date'],y=daily['Conversion %'],mode='lines+markers+text',
        text=[f"{v:.1f}%" for v in daily['Conversion %']],textposition='top center',line=dict(color='#5B9BD5',width=3),marker=dict(size=8)))
    fig_conv.update_layout(height=300,yaxis_title='Conversion %')
    st.plotly_chart(fig_conv, width="stretch")
    st.markdown("---"); st.markdown(t["st_top_asins"])
    asin_col = 'child_asin' if 'child_asin' in df_filtered.columns else df_filtered.columns[0]
    as_ = df_filtered.groupby(asin_col).agg({'sessions':'sum','page_views':'sum','units_ordered':'sum','ordered_product_sales':'sum','buy_box_percentage':'mean'}).reset_index()
    as_.columns=['ASIN','Sessions','Page Views','Units','Revenue','Buy Box %']
    as_['Conv %'] = (as_['Units']/as_['Sessions']*100).fillna(0)
    col1,col2 = st.columns(2)
    with col1:
        st.markdown(t["st_top_revenue"])
        fig = px.bar(as_.nlargest(15,'Revenue'),x='Revenue',y='ASIN',orientation='h',text='Revenue',color='Revenue',color_continuous_scale='Greens')
        fig.update_layout(yaxis={'categoryorder':'total ascending'},height=450); fig.update_traces(texttemplate='$%{text:,.0f}',textposition='outside')
        st.plotly_chart(fig, width="stretch")
    with col2:
        st.markdown(t["st_top_sessions"])
        fig = px.bar(as_.nlargest(15,'Sessions'),x='Sessions',y='ASIN',orientation='h',text='Sessions',color='Sessions',color_continuous_scale='Blues')
        fig.update_layout(yaxis={'categoryorder':'total ascending'},height=450)
        st.plotly_chart(fig, width="stretch")
    st.markdown("---"); st.markdown(t["st_full_data"])
    st.dataframe(as_.sort_values('Revenue',ascending=False).style.format({'Revenue':'${:,.2f}','Conv %':'{:.2f}%','Buy Box %':'{:.1f}%'}),width="stretch",height=500)
    csv = as_.to_csv(index=False).encode('utf-8')
    st.download_button(t["st_download"], csv, "sales_traffic.csv","text/csv")
    insights_sales_traffic(df_filtered, as_)

    # ── AI Chat ──
    ctx = f"""Sales & Traffic за обраний період:
- Сесії: {ts:,} | Перегляди: {tpv:,} | Замовлення: {tu:,}
- Дохід: ${tr:,.2f} | Конверсія: {ac:.2f}% | Buy Box: {ab:.1f}%
- Топ ASIN за доходом: {as_.nlargest(3,'Revenue')[['ASIN','Revenue','Conv %']].to_string()}"""
    show_ai_chat(ctx, [
        "Який ASIN виріс найбільше за останні 7 днів?",
        "Які ASIN мають Buy Box нижче 80%?",
        "Де CVR вище середнього? Топ 5",
    ], "sales_traffic")


def show_settlements(t):
    df_settlements = load_settlements()
    if df_settlements.empty:
        st.warning("⚠️ No settlement data found."); return
    st.sidebar.markdown("---"); st.sidebar.subheader("💰 Settlement Filters")
    currencies = ['All'] + sorted(df_settlements['Currency'].dropna().unique().tolist())
    sel_cur = st.sidebar.selectbox(t["currency_select"], currencies, index=1 if "USD" in currencies else 0)
    min_date = df_settlements['Posted Date'].min().date()
    max_date = df_settlements['Posted Date'].max().date()
    date_range = st.sidebar.date_input("📅 Transaction Date:",value=(max_date-dt.timedelta(days=30),max_date),min_value=min_date,max_value=max_date)
    df_f = df_settlements.copy()
    if sel_cur != 'All': df_f = df_f[df_f['Currency']==sel_cur]
    if len(date_range)==2:
        df_f = df_f[(df_f['Posted Date'].dt.date>=date_range[0])&(df_f['Posted Date'].dt.date<=date_range[1])]
    if df_f.empty:
        st.warning("No data for selected filters"); return
    st.markdown(f"### {t['settlements_title']}")
    net = df_f['Amount'].sum()
    gross = df_f[(df_f['Transaction Type']=='Order')&(df_f['Amount']>0)]['Amount'].sum()
    refunds = df_f[df_f['Transaction Type']=='Refund']['Amount'].sum()
    fees = df_f[(df_f['Amount']<0)&(df_f['Transaction Type']!='Refund')]['Amount'].sum()
    sym = "$" if sel_cur in ['USD','CAD','All'] else ""
    c1,c2,c3,c4 = st.columns(4)
    c1.metric(t['net_payout'],f"{sym}{net:,.2f}"); c2.metric(t['gross_sales'],f"{sym}{gross:,.2f}")
    c3.metric(t['total_refunds'],f"{sym}{refunds:,.2f}"); c4.metric(t['total_fees'],f"{sym}{fees:,.2f}")
    st.markdown("---")
    col1,col2 = st.columns([2,1])
    with col1:
        st.subheader(t['chart_payout_trend'])
        dt_ = df_f.groupby(df_f['Posted Date'].dt.date)['Amount'].sum().reset_index()
        dt_.columns=['Date','Net Amount']
        fig = go.Figure(go.Bar(x=dt_['Date'],y=dt_['Net Amount'],marker_color=dt_['Net Amount'].apply(lambda x:'green' if x>=0 else 'red')))
        fig.update_layout(height=400,yaxis_title=f"Net Amount ({sel_cur})")
        st.plotly_chart(fig, width="stretch")
    with col2:
        st.subheader(t['chart_fee_breakdown'])
        df_costs = df_f[df_f['Amount']<0]
        if not df_costs.empty:
            cb = df_costs.groupby('Transaction Type')['Amount'].sum().abs().reset_index()
            fig = px.pie(cb,values='Amount',names='Transaction Type',hole=0.4)
            fig.update_layout(height=400)
            st.plotly_chart(fig, width="stretch")
        else: st.info("No costs in selected period")
    disp = ['Posted Date','Transaction Type','Order ID','Amount','Currency','Description']
    st.dataframe(df_f[[c for c in disp if c in df_f.columns]].sort_values('Posted Date',ascending=False).head(100),width="stretch")
    insights_settlements(df_f)

    # ── AI Chat ──
    ctx_set = f"""Settlement фінанси:
- Net Payout: {sym}{net:,.2f} | Gross Sales: {sym}{gross:,.2f}
- Refunds: {sym}{refunds:,.2f} | Fees: {sym}{fees:,.2f}
- Валюта: {sel_cur} | Комісія: {abs(fees)/gross*100:.1f}% від продажів"""
    show_ai_chat(ctx_set, [
        "Який місяць приніс найбільший net payout за рік?",
        "Яка частка рефандів від gross sales по місяцях?",
        "Де найвищі FBA fees? Топ SKU за комісіями",
    ], "settlements")


def show_returns(t=None):
    if t is None: t = translations.get("UA", {})
    df_ret_raw, df_orders = load_returns()
    if df_ret_raw.empty:
        st.warning("⚠️ No returns data."); return
    df_r = df_ret_raw.copy()
    df_r['Return Date'] = pd.to_datetime(df_r['Return Date'], errors='coerce')
    if 'Price' not in df_r.columns and not df_orders.empty:
        try:
            for col in ['Item Price','item-price','item_price','price','Price']:
                if col in df_orders.columns:
                    df_orders[col] = pd.to_numeric(df_orders[col],errors='coerce')
                    df_r['Price'] = df_r['SKU'].map(df_orders.groupby('SKU')[col].mean()).fillna(0)
                    break
        except: df_r['Price'] = 0
    elif 'Price' not in df_r.columns: df_r['Price'] = 0
    df_r['Price']        = pd.to_numeric(df_r['Price'],errors='coerce').fillna(0)
    df_r['Quantity']     = pd.to_numeric(df_r['Quantity'],errors='coerce').fillna(1)
    df_r['Return Value'] = df_r['Price'] * df_r['Quantity']
    st.sidebar.markdown("---"); st.sidebar.subheader(t["ret_filters"])
    min_date = df_r['Return Date'].min().date(); max_date = df_r['Return Date'].max().date()
    date_range = st.sidebar.date_input(t["ret_date"],value=(max_date-dt.timedelta(days=30),max_date),min_value=min_date,max_value=max_date)
    sel_store = 'All'
    if 'Store Name' in df_r.columns:
        stores = ['All'] + sorted(df_r['Store Name'].dropna().unique().tolist())
        sel_store = st.sidebar.selectbox("🏪 Store:", stores)
    df_f = df_r[(df_r['Return Date'].dt.date>=date_range[0])&(df_r['Return Date'].dt.date<=date_range[1])] if len(date_range)==2 else df_r
    if sel_store != 'All': df_f = df_f[df_f['Store Name']==sel_store]
    st.markdown(t["ret_title"])
    rr = 0
    try:
        if not df_orders.empty:
            for col in ['Order ID','order-id','order_id','OrderID']:
                if col in df_orders.columns:
                    rr = df_f['Order ID'].nunique()/df_orders[col].nunique()*100 if df_orders[col].nunique()>0 else 0
                    break
    except: pass
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric(t["ret_total"],f"{len(df_f):,}"); c2.metric(t["ret_unique_sku"],df_f['SKU'].nunique())
    c3.metric(t["ret_rate"],f"{rr:.1f}%"); c4.metric(t["ret_value"],f"${df_f['Return Value'].sum():,.2f}")
    c5.metric(t["ret_avg"],f"${df_f['Return Value'].mean():.2f}")
    st.markdown("---")
    col1,col2,col3 = st.columns(3)
    with col1:
        st.markdown(t["ret_by_sku"])
        tv = df_f.groupby('SKU')['Return Value'].sum().nlargest(10).reset_index()
        fig = px.bar(tv,x='Return Value',y='SKU',orientation='h',text='Return Value',color='Return Value',color_continuous_scale='Reds')
        fig.update_layout(yaxis={'categoryorder':'total ascending'},height=350); fig.update_traces(texttemplate='$%{text:,.0f}',textposition='outside')
        st.plotly_chart(fig, width="stretch")
    with col2:
        st.markdown(t["ret_daily"])
        dv = df_f.groupby(df_f['Return Date'].dt.date)['Return Value'].sum().reset_index(); dv.columns=['Date','Value']
        fig = px.area(dv,x='Date',y='Value',line_shape='spline',color_discrete_sequence=['#FF6B6B'])
        fig.update_layout(height=350); st.plotly_chart(fig, width="stretch")
    with col3:
        if 'Reason' in df_f.columns:
            st.markdown(t["ret_by_reason"])
            rv = df_f.groupby('Reason')['Return Value'].sum().nlargest(8).reset_index()
            fig = px.pie(rv,values='Return Value',names='Reason',hole=0.4,color_discrete_sequence=px.colors.sequential.RdBu)
            fig.update_layout(height=350); st.plotly_chart(fig, width="stretch")
    st.markdown("---")
    col1,col2 = st.columns(2)
    with col1:
        st.markdown(t["ret_top_sku"])
        ts = df_f['SKU'].value_counts().head(15).reset_index(); ts.columns=['SKU','Returns']
        fig = px.bar(ts,x='Returns',y='SKU',orientation='h',color='Returns',color_continuous_scale='Oranges',text='Returns')
        fig.update_layout(yaxis={'categoryorder':'total ascending'},height=450); st.plotly_chart(fig, width="stretch")
    with col2:
        if 'Reason' in df_f.columns:
            st.markdown(t["ret_reasons"])
            rs = df_f['Reason'].value_counts().head(10).reset_index(); rs.columns=['Reason','Count']
            fig = px.pie(rs,values='Count',names='Reason',hole=0.4,color_discrete_sequence=px.colors.sequential.RdBu)
            fig.update_layout(height=450); st.plotly_chart(fig, width="stretch")
    st.markdown("---")
    dc = ['Return Date','SKU','Product Name','Quantity','Price','Return Value','Reason','Status']
    st.dataframe(df_f[[c for c in dc if c in df_f.columns]].sort_values('Return Date',ascending=False).head(100).style.format({'Price':'${:.2f}','Return Value':'${:.2f}'}),width="stretch")
    st.download_button(t["ret_download"],df_f.to_csv(index=False).encode('utf-8'),"returns.csv","text/csv")
    insights_returns(df_f, rr)

    # ── AI Chat ──
    top_ret = df_f['SKU'].value_counts().head(5).to_string() if not df_f.empty else ""
    ctx_ret = f"""Returns аналіз:
- Всього повернень: {len(df_f)} | Return Rate: {rr:.1f}%
- Вартість повернень: ${df_f['Return Value'].sum():,.2f}
- Топ SKU за поверненнями: {top_ret}"""
    show_ai_chat(ctx_ret, [
        "Які SKU мають найбільше повернень за 30 днів?",
        "Які топ-3 причини повернень по всіх SKU?",
        "Порівняй return rate цього місяця vs попереднього",
    ], "returns")


def show_inventory_finance(df_filtered, t):
    tv = df_filtered['Stock Value'].sum(); tu = df_filtered['Available'].sum()
    ap = df_filtered[df_filtered['Price']>0]['Price'].mean()
    c1,c2,c3 = st.columns(3)
    c1.metric("💰 Total Inventory Value",f"${tv:,.2f}")
    c2.metric(t["avg_price"],f"${ap:,.2f}" if not pd.isna(ap) else "$0")
    c3.metric("💵 Avg Value per Unit",f"${tv/tu:.2f}" if tu>0 else "$0")
    st.markdown("---"); st.subheader(t["chart_value_treemap"])
    dm = df_filtered[df_filtered['Stock Value']>0]
    if not dm.empty:
        fig = px.treemap(dm,path=['Store Name','SKU'],values='Stock Value',color='Stock Value',color_continuous_scale='RdYlGn_r')
        st.plotly_chart(fig, width="stretch")
    st.subheader(t["top_money_sku"])
    dt_ = df_filtered[['SKU','Product Name','Available','Price','Stock Value']].sort_values('Stock Value',ascending=False).head(10)
    st.dataframe(dt_.style.format({'Price':"${:.2f}",'Stock Value':"${:,.2f}"}),width="stretch")
    insights_inventory(df_filtered)


def show_aging(df_filtered, t):
    if df_filtered.empty: st.warning("No data"); return
    age_cols = ['Upto 90 Days','91 to 180 Days','181 to 270 Days','271 to 365 Days','More than 365 Days']
    valid    = [c for c in age_cols if c in df_filtered.columns]
    if not valid: st.warning("Aging data not available."); return
    da = df_filtered[valid].copy()
    for c in valid: da[c] = pd.to_numeric(da[c],errors='coerce').fillna(0)
    if da.sum().sum()==0: st.info("All inventory is fresh"); return
    as_ = da.sum().reset_index(); as_.columns=['Age Group','Units']; as_ = as_[as_['Units']>0]
    col1,col2 = st.columns(2)
    with col1:
        st.subheader(t["chart_age"])
        fig = px.pie(as_,values='Units',names='Age Group',hole=0.4); fig.update_layout(height=400)
        st.plotly_chart(fig, width="stretch")
    with col2:
        st.subheader(t["chart_velocity"])
        if all(c in df_filtered.columns for c in ['Available','Velocity','Stock Value']):
            ds = df_filtered[(df_filtered['Available']>0)&(df_filtered['Velocity']>=0)&(df_filtered['Stock Value']>0)].copy()
            if not ds.empty:
                fig = px.scatter(ds,x='Available',y='Velocity',size='Stock Value',color='Store Name' if 'Store Name' in ds.columns else None,hover_name='SKU',log_x=True)
                fig.update_layout(height=400); st.plotly_chart(fig, width="stretch")

    # ── AI Chat ──
    slow = df_filtered[df_filtered['Velocity'] < 0.1][['SKU','Available','Stock Value']].head(5).to_string() if 'Velocity' in df_filtered.columns else ""
    _stock_val = df_filtered['Stock Value'].sum() if 'Stock Value' in df_filtered.columns else 0
    ctx_aging = f"""Inventory Health:
- SKU всього: {len(df_filtered)} | Загальна вартість: ${_stock_val:,.0f}
- Повільні SKU (Velocity<0.1): {slow}"""
    show_ai_chat(ctx_aging, [
        "Які SKU мають залишок більше 90 днів продажів?",
        "Топ 5 SKU де заморожено найбільше коштів",
        "Які SKU закінчаться за 14 днів при поточному темпі?",
    ], "aging")


def show_ai_forecast(df, t):
    st.markdown("### Select SKU for Forecast")
    skus = sorted([s for s in df['SKU'].unique() if s is not None and str(s).strip() != ''])
    if not skus: st.info("No SKU available"); return
    col1,col2 = st.columns([2,1])
    target_sku    = col1.selectbox(t["ai_select"],skus)
    forecast_days = col2.slider(t["ai_days"],7,90,30)
    sd = df[df['SKU']==target_sku].copy().sort_values('created_at')
    sd['date_ordinal'] = sd['created_at'].map(dt.datetime.toordinal)
    if len(sd)>=3:
        # LinearRegression замінено на numpy (без sklearn)
        import numpy as _np
        _coeffs = _np.polyfit(sd['date_ordinal'], sd['Available'], 1)
        model = type('M', (), {'predict': lambda self, X: _np.polyval(_coeffs, [x[0] for x in X])})()
        last  = sd['created_at'].max()
        fd    = [last+dt.timedelta(days=x) for x in range(1,forecast_days+1)]
        fo    = np.array([d.toordinal() for d in fd]).reshape(-1,1)
        preds = [max(0,int(p)) for p in model.predict(fo)]
        df_fc = pd.DataFrame({'date':fd,'Predicted':preds})
        so    = df_fc[df_fc['Predicted']==0]
        if not so.empty: st.error(f"{t['ai_result_date']} **{so.iloc[0]['date'].date()}**")
        else:             st.success(t['ai_ok'])
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=sd['created_at'],y=sd['Available'],name='Historical'))
        fig.add_trace(go.Scatter(x=df_fc['date'],y=df_fc['Predicted'],name='Forecast',line=dict(dash='dash',color='red')))
        st.plotly_chart(fig, width="stretch")
    else: st.warning(t["ai_error"])


def show_data_table(df_filtered, t, selected_date):
    st.markdown("### 📊 FBA Inventory Dataset")
    st.download_button("📥 Download CSV",df_filtered.to_csv(index=False).encode('utf-8'),"fba_inventory.csv","text/csv")
    st.dataframe(df_filtered, width="stretch", height=600)


def show_orders(t=None):
    if t is None: t = translations.get("UA", {})
    df_orders = load_orders()
    if df_orders.empty: st.warning("⚠️ No orders data."); return
    st.sidebar.markdown("---"); st.sidebar.subheader("🛒 Orders Filters")
    min_date = df_orders['Order Date'].min().date(); max_date = df_orders['Order Date'].max().date()
    date_range = st.sidebar.date_input(t["st_date_range"],value=(max_date-dt.timedelta(days=7),max_date),min_value=min_date,max_value=max_date)
    df_f = df_orders[(df_orders['Order Date'].dt.date>=date_range[0])&(df_orders['Order Date'].dt.date<=date_range[1])] if len(date_range)==2 else df_orders
    c1,c2,c3 = st.columns(3)
    c1.metric("📦 Orders",df_f['Order ID'].nunique()); c2.metric("💰 Revenue",f"${df_f['Total Price'].sum():,.2f}"); c3.metric("📦 Items",int(df_f['Quantity'].sum()))
    st.markdown("#### 📈 Daily Revenue")
    daily = df_f.groupby(df_f['Order Date'].dt.date)['Total Price'].sum().reset_index()
    fig = px.bar(daily,x='Order Date',y='Total Price',title="Daily Revenue")
    st.plotly_chart(fig, width="stretch")
    col1,col2 = st.columns(2)
    with col1:
        st.markdown("#### 🏆 Top 10 SKU by Revenue")
        ts = df_f.groupby('SKU')['Total Price'].sum().nlargest(10).reset_index()
        fig2 = px.bar(ts,x='Total Price',y='SKU',orientation='h'); fig2.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig2, width="stretch")
    with col2:
        if 'Order Status' in df_f.columns:
            st.markdown("#### 📊 Order Status")
            sc = df_f['Order Status'].value_counts().reset_index(); sc.columns=['Status','Count']
            fig3 = px.pie(sc,values='Count',names='Status',hole=0.4); st.plotly_chart(fig3, width="stretch")
    insights_orders(df_f)

    # ── AI Chat ──
    top_skus = df_f.groupby('SKU')['quantity'].sum().nlargest(5).to_string() if 'SKU' in df_f.columns and 'quantity' in df_f.columns else ""
    ctx_ord = f"""Orders аналіз: замовлень {len(df_f)}. Топ SKU: {top_skus}"""
    show_ai_chat(ctx_ord, [
        "Топ 5 SKU за кількістю замовлень за останні 30 днів",
        "Порівняй обсяг замовлень: цей тиждень vs минулий",
        "Які SKU не мали замовлень більше 14 днів?",
    ], "orders")


# ============================================
# 🕷 SCRAPER MANAGER — SIMPLIFIED
# ============================================

APIFY_TOKEN_DEFAULT = os.getenv("APIFY_TOKEN", "")
STARS_MAP = {5: "fiveStar", 4: "fourStar", 3: "threeStar", 2: "twoStar", 1: "oneStar"}
DOMAIN_FLAGS = {
    "com": "🇺🇸", "ca": "🇨🇦", "de": "🇩🇪", "co.uk": "🇬🇧",
    "it": "🇮🇹", "es": "🇪🇸", "fr": "🇫🇷", "co.jp": "🇯🇵",
    "com.au": "🇦🇺", "com.mx": "🇲🇽", "nl": "🇳🇱", "pl": "🇵🇱", "se": "🇸🇪",
}


def _scr_get_conn():
    from urllib.parse import urlparse
    r = urlparse(DATABASE_URL)
    return psycopg2.connect(
        database=r.path[1:], user=r.username, password=r.password,
        host=r.hostname, port=r.port, connect_timeout=10
    )


def _scr_ensure_table():
    conn = _scr_get_conn(); cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS amazon_reviews (
            id SERIAL PRIMARY KEY, asin VARCHAR(20), domain VARCHAR(20),
            review_id VARCHAR(100) UNIQUE, author VARCHAR(255), rating INTEGER,
            title TEXT, content TEXT, is_verified BOOLEAN,
            product_attributes TEXT, review_date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit(); cur.close(); conn.close()


def _scr_save(reviews, asin, domain):
    conn = _scr_get_conn(); cur = conn.cursor(); inserted = 0
    for rev in reviews:
        url = rev.get("reviewUrl", "")
        rid = url.split("/")[-1] if url else f"{asin}_{domain}_{rev.get('position','?')}"
        try:
            cur.execute("""
                INSERT INTO amazon_reviews
                (asin,domain,review_id,author,rating,title,content,is_verified,product_attributes,review_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (review_id) DO NOTHING
            """, (asin, domain, rid,
                  rev.get("author","Amazon User"), int(rev.get("ratingScore",0)),
                  rev.get("reviewTitle",""), rev.get("reviewDescription",""),
                  bool(rev.get("isVerified",False)), rev.get("variant",""), rev.get("date","")))
            if cur.rowcount > 0: inserted += 1
        except: pass
    conn.commit(); cur.close(); conn.close()
    return inserted


def _scr_count(asin, domain):
    try:
        conn = _scr_get_conn(); cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM amazon_reviews WHERE asin=%s AND domain=%s", (asin, domain))
        n = cur.fetchone()[0]; cur.close(); conn.close(); return n
    except: return 0


def _scr_parse_url(url):
    from urllib.parse import urlparse
    p = urlparse(url.strip())
    domain = p.netloc.replace("www.amazon.", "")
    m = re.search(r"([A-Z0-9]{10})", p.path)
    return domain, (m.group(1) if m else "UNKNOWN")


def _scr_worker(urls, max_per_star, log_q, progress_q, loop_mode, stop_event, apify_token):
    try:
        _scr_ensure_table()
    except Exception as e:
        log_q.put(f"❌ DB error: {e}"); progress_q.put({"done": True}); return

    endpoint = (
        f"https://api.apify.com/v2/acts/junglee~amazon-reviews-scraper"
        f"/run-sync-get-dataset-items?token={apify_token}"
    )
    cycle = 0
    while not stop_event.is_set():
        cycle += 1
        total_steps = len(urls) * 5
        step = 0
        cycle_total = 0

        if loop_mode:
            log_q.put(f"\n{'🔄'*20}")
            log_q.put(f"🔄  ЦИКЛ #{cycle} РОЗПОЧАТО")
            log_q.put(f"{'🔄'*20}")

        for url in urls:
            if stop_event.is_set(): break
            domain, asin = _scr_parse_url(url)
            flag = DOMAIN_FLAGS.get(domain, "🌍")
            log_q.put(f"\n{'='*50}")
            log_q.put(f"{flag}  {asin}  ·  amazon.{domain}  (цикл #{cycle})")
            log_q.put(f"{'='*50}")

            url_new = 0
            for star_num, star_text in STARS_MAP.items():
                if stop_event.is_set(): break
                step += 1
                pct = int(step / total_steps * 100)
                log_q.put(f"  ⏳ {star_num}★ — збираємо (max {max_per_star})...")
                progress_q.put({"pct": pct, "label": f"Цикл #{cycle} · {asin} · {star_num}★"})
                payload = {
                    "productUrls": [{"url": url}],
                    "filterByRatings": [star_text],
                    "maxReviews": max_per_star,
                    "sort": "recent",
                }
                try:
                    res = requests.post(endpoint, json=payload, timeout=360)
                    if res.status_code in (200, 201):
                        data = res.json()
                        if data:
                            ins = _scr_save(data, asin, domain)
                            url_new   += ins
                            cycle_total += ins
                            log_q.put(f"  ✅ {star_num}★: отримано {len(data)}, нових: {ins}")
                        else:
                            log_q.put(f"  ⚠️ {star_num}★: відгуків не знайдено")
                    else:
                        log_q.put(f"  ❌ {star_num}★: HTTP {res.status_code}")
                except Exception as e:
                    log_q.put(f"  ❌ {star_num}★: {e}")
                time.sleep(1.5)

            in_db = _scr_count(asin, domain)
            log_q.put(f"🎯 {asin}/{domain}: +{url_new} нових · в БД: {in_db}")
            time.sleep(3)

        if loop_mode and not stop_event.is_set():
            pause_min = 30
            log_q.put(f"\n🏁 Цикл #{cycle} завершено! +{cycle_total} нових.")
            log_q.put(f"⏸  Пауза {pause_min} хв перед наступним циклом...")
            progress_q.put({"pct": 100, "label": f"Цикл #{cycle} готово, пауза {pause_min} хв..."})
            for _ in range(pause_min * 12):
                if stop_event.is_set(): break
                time.sleep(5)
        else:
            break

    log_q.put(f"\n🏁 ЗБІР ЗУПИНЕНО після {cycle} цикл(ів)")
    progress_q.put({"pct": 100, "label": "Зупинено", "done": True, "total": cycle})


def _scr_init():
    defaults = {
        "scr_running": False, "scr_logs": [], "scr_pct": 0,
        "scr_label": "", "scr_done": False, "scr_cycles": 0,
        "scr_log_q": None, "scr_prog_q": None,
        "scr_stop_event": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state: st.session_state[k] = v


def _scr_flush():
    lq = st.session_state.scr_log_q
    pq = st.session_state.scr_prog_q
    if lq:
        while not lq.empty():
            try: st.session_state.scr_logs.append(lq.get_nowait())
            except: break
    if pq:
        while not pq.empty():
            try:
                msg = pq.get_nowait()
                if "pct"   in msg: st.session_state.scr_pct    = msg["pct"]
                if "label" in msg: st.session_state.scr_label  = msg["label"]
                if "done"  in msg and msg["done"]:
                    st.session_state.scr_done    = True
                    st.session_state.scr_running = False
                    st.session_state.scr_cycles  = msg.get("total", 0)
            except: break


def show_scraper_manager():
    _scr_init()
    _scr_flush()

    st.markdown("## 🕷 Scraper Reviews")

    # ── Статус ──
    if st.session_state.scr_running:
        st.info(f"🔄 {st.session_state.scr_label or 'Збір в процесі...'}")
    elif st.session_state.scr_done:
        st.success(f"✅ Готово! Циклів: **{st.session_state.scr_cycles}**")

    st.progress(st.session_state.scr_pct, text=st.session_state.scr_label or " ")
    st.markdown("---")

    # ── Форма ──
    urls_input = st.text_area(
        "🔗 Посилання Amazon (по одному на рядок):",
        height=180,
        placeholder=(
            "https://www.amazon.com/dp/B08HR2131Z\n"
            "https://www.amazon.de/dp/B08HWCL2RY\n"
            "https://www.amazon.co.uk/dp/B07XCDPRGZ"
        ),
        disabled=st.session_state.scr_running,
        key="scr_urls_input"
    )

    max_per_star = 100  # Amazon limit
    c2, c3 = st.columns([3, 1])
    with c2:
        loop_mode = st.toggle(
            "🔄 Нескінченний цикл (пауза 30 хв між проходами)",
            value=False,
            disabled=st.session_state.scr_running
        )
    with c3:
        st.markdown("<div style='margin-top:28px'>", unsafe_allow_html=True)
        if st.session_state.scr_running:
            if st.button("⛔ Зупинити", width="stretch", type="secondary"):
                if st.session_state.scr_stop_event:
                    st.session_state.scr_stop_event.set()
                st.session_state.scr_running = False
                st.session_state.scr_done    = True
                st.rerun()
        else:
            raw_lines = [u.strip() for u in (urls_input or "").splitlines() if u.strip()]
            if st.button("🚀 Запустити", width="stretch", type="primary",
                         disabled=not raw_lines):
                lq      = queue.Queue()
                pq      = queue.Queue()
                stop_ev = threading.Event()

                st.session_state.scr_logs       = []
                st.session_state.scr_pct        = 0
                st.session_state.scr_label      = "Старт..."
                st.session_state.scr_done       = False
                st.session_state.scr_running    = True
                st.session_state.scr_cycles     = 0
                st.session_state.scr_log_q      = lq
                st.session_state.scr_prog_q     = pq
                st.session_state.scr_stop_event = stop_ev

                threading.Thread(
                    target=_scr_worker,
                    args=(raw_lines, max_per_star, lq, pq,
                          loop_mode, stop_ev, APIFY_TOKEN_DEFAULT),
                    daemon=True
                ).start()
                st.rerun()

    st.markdown("---")

    # ── Логи ──
    st.markdown("### 📜 Логи")
    logs = st.session_state.scr_logs
    if logs:
        colored = []
        for line in logs[-100:]:
            if "===" in line:
                colored.append(f'<span style="color:#5B9BD5;font-weight:700">{line}</span>')
            elif "🔄" in line and "ЦИКЛ" in line:
                colored.append(f'<span style="color:#AB47BC;font-weight:800;font-size:14px">{line}</span>')
            elif "✅" in line:
                colored.append(f'<span style="color:#4CAF50">{line}</span>')
            elif "❌" in line:
                colored.append(f'<span style="color:#F44336">{line}</span>')
            elif "⚠️" in line:
                colored.append(f'<span style="color:#FFC107">{line}</span>')
            elif "🏁" in line or "ЗУПИНЕНО" in line:
                colored.append(f'<span style="color:#FFD700;font-weight:800">{line}</span>')
            elif "🎯" in line:
                colored.append(f'<span style="color:#AB47BC">{line}</span>')
            elif "⏸" in line:
                colored.append(f'<span style="color:#888;font-style:italic">{line}</span>')
            else:
                colored.append(f'<span style="color:#ccc">{line}</span>')

        st.markdown(f"""
        <div style="background:#0d1117;border:1px solid #30363d;border-radius:10px;
                    padding:16px 20px;font-family:'Courier New',monospace;font-size:13px;
                    line-height:1.7;max-height:520px;overflow-y:auto">
          {"<br>".join(colored)}
        </div>""", unsafe_allow_html=True)

        c1, c2, _ = st.columns([1, 1, 3])
        with c1:
            if st.button("🗑 Очистити логи", width="stretch"):
                st.session_state.scr_logs = []
                st.session_state.scr_done = False
                st.rerun()
        with c2:
            st.download_button(
                "📥 Зберегти лог",
                "\n".join(logs).encode(),
                "scraper_log.txt", "text/plain", width="stretch"
            )
    else:
        st.markdown("""
        <div style="background:#0d1117;border:1px solid #30363d;border-radius:10px;
                    padding:24px;color:#555;font-family:monospace;text-align:center">
          Логи з'являться після запуску...
        </div>""", unsafe_allow_html=True)

    # Auto-refresh поки іде
    if st.session_state.scr_running:
        time.sleep(2)
        st.rerun()


# ============================================
# MAIN
# ============================================

from auth import (
    ensure_tables, create_admin_if_not_exists,
    show_login, logout, can_view,
    show_admin_panel, ALL_REPORTS
)

# ── Init DB tables on first run ──
try:
    ensure_tables()
    create_admin_if_not_exists()
except Exception as e:
    st.error(f"DB init error: {e}")
    st.stop()

# ── Not logged in → show login form ──
if "user" not in st.session_state or not st.session_state.user:
    show_login()
    st.stop()

# ── Logged in ──
user = st.session_state.user

# Sidebar: user info + logout
st.sidebar.markdown(f"""
<div style="background:#1e1e2e;border-radius:8px;padding:10px 14px;margin-bottom:8px">
  <div style="font-size:14px;font-weight:700;color:#fff">{user['name'] or user['email']}</div>
  <div style="font-size:12px;color:#888">{user['email']}</div>
  <div style="font-size:11px;color:#AB47BC;margin-top:4px;font-weight:600">{user['role'].upper()}</div>
</div>""", unsafe_allow_html=True)
if st.sidebar.button("🚪 Вийти", width="stretch"):
    logout()

if 'report_choice' not in st.session_state:
    st.session_state.report_choice = "🏠 Overview"

lang_option = st.sidebar.selectbox("🌍 Language", ["UA 🇺🇦","EN 🇺🇸","RU 🌍"], index=0)
lang = "UA" if "UA" in lang_option else "EN" if "EN" in lang_option else "RU"
t    = translations[lang]

if st.sidebar.button(t["update_btn"], width="stretch"):
    st.cache_data.clear(); st.rerun()

df = load_data()

if not df.empty:
    for col in ['Available','Price','Velocity','Stock Value']:
        if col not in df.columns: df[col] = 0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['Stock Value'] = df['Available'] * df['Price']
    df['created_at']  = pd.to_datetime(df['created_at'])
    df['date']        = df['created_at'].dt.date
    st.sidebar.header(t["sidebar_title"])
    dates         = sorted(df['date'].unique(), reverse=True)
    selected_date = st.sidebar.selectbox(t["date_label"], dates) if dates else None
    stores        = [t["all_stores"]] + list(df['Store Name'].unique()) if 'Store Name' in df.columns else [t["all_stores"]]
    selected_store = st.sidebar.selectbox(t["store_label"], stores)
    df_filtered    = df[df['date']==selected_date] if selected_date else df
    if selected_store != t["all_stores"]:
        df_filtered = df_filtered[df_filtered['Store Name']==selected_store]
else:
    df_filtered = pd.DataFrame(); selected_date = None

st.sidebar.markdown("---")
st.sidebar.header("📊 Reports")

# Формуємо список доступних звітів для цього юзера
all_nav = [
    "🏠 Overview","📈 Sales & Traffic","🏦 Settlements (Payouts)",
    "💰 Inventory Value (CFO)","🛒 Orders Analytics","📦 Returns Analytics",
    "⭐ Amazon Reviews","🐢 Inventory Health (Aging)","🧠 AI Forecast",
    "📋 FBA Inventory Table","🕷 Scraper Reviews","📊 ETL Status","📦 Склад (Inventory)",
]
# Адмін бачить все + адмінку
if user["role"] == "admin":
    report_options = all_nav + ["👑 User Management", "ℹ️ Про додаток"]
else:
    report_options = [r for r in all_nav if can_view(r)] + ["ℹ️ Про додаток"]

if not report_options:
    st.warning("У вас немає доступу до жодного розділу. Зверніться до адміністратора.")
    st.stop()

if st.session_state.report_choice not in report_options:
    st.session_state.report_choice = report_options[0]

current_index = report_options.index(st.session_state.report_choice)
report_choice = st.sidebar.radio("Select Report:", report_options, index=current_index)
st.session_state.report_choice = report_choice

if   report_choice == "🏠 Overview":                show_overview(df_filtered, t, selected_date)
elif report_choice == "📈 Sales & Traffic":          show_sales_traffic(t)
elif report_choice == "🏦 Settlements (Payouts)":   show_settlements(t)
elif report_choice == "💰 Inventory Value (CFO)":   show_inventory_finance(df_filtered, t)
elif report_choice == "🛒 Orders Analytics":         show_orders(t)
elif report_choice == "📦 Returns Analytics":        show_returns(t)
elif report_choice == "⭐ Amazon Reviews":           show_reviews(t)
elif report_choice == "🐢 Inventory Health (Aging)":show_aging(df_filtered, t)
elif report_choice == "🧠 AI Forecast":              show_ai_forecast(df, t)
elif report_choice == "📋 FBA Inventory Table":      show_data_table(df_filtered, t, selected_date)
elif report_choice == "🕷 Scraper Reviews":          show_scraper_manager()
elif report_choice == "👑 User Management":          show_admin_panel()
elif report_choice == "📊 ETL Status":               show_etl_status()
elif report_choice == "📦 Склад (Inventory)":        show_inventory_unified()
elif report_choice == "ℹ️ Про додаток":              show_about()

st.sidebar.markdown("---")
st.sidebar.caption("📦 Amazon FBA BI System v5.0 🌍")
