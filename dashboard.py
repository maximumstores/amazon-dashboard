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
        "sales_traffic_title": "📈 Трафик (Sales & Traffic)",
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
        "sales_traffic_title": "📈 Трафик (Sales & Traffic)",
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
        "sales_traffic_title": "📈 Трафик (Sales & Traffic)",
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

# ══════════════════════════════════════════
# 🔌 API MODE — ?api=endpoint&key=SECRET
# Приклад: https://merino-bi.streamlit.app/?api=inventory&key=YOUR_KEY
# ══════════════════════════════════════════
import json as _json

def _api_response(data):
    """Відповідь у JSON через st.code щоб можна було парсити."""
    st.set_page_config(page_title="API", layout="centered")
    st.markdown("```json")
    st.code(_json.dumps(data, default=str, ensure_ascii=False, indent=2), language="json")
    st.stop()

_qp = st.query_params
_API_KEY = os.getenv("API_KEY", "merino2024")

if "api" in _qp:
    _endpoint = _qp.get("api", "")
    _key      = _qp.get("key", "")

    if _key != _API_KEY:
        _api_response({"error": "Unauthorized", "hint": "Pass ?key=YOUR_API_KEY"})

    _engine = get_engine()

    # ── GET /inventory ──
    if _endpoint == "inventory":
        try:
            with _engine.connect() as _c:
                _df = pd.read_sql(text("SELECT * FROM fba_inventory"), _c)
            _api_response({"status":"ok","count":len(_df),"data":_df.to_dict(orient="records")})
        except Exception as e:
            _api_response({"error": str(e)})

    # ── GET /finance ──
    elif _endpoint == "finance":
        _days = int(_qp.get("days", 30))
        try:
            with _engine.connect() as _c:
                _r = pd.read_sql(text(f"""
                    SELECT
                        SUM(CASE WHEN event_type='Shipment' AND charge_type='Principal' THEN NULLIF(amount,'')::numeric ELSE 0 END) AS gross,
                        SUM(CASE WHEN event_type IN ('ShipmentFee','RefundFee') THEN NULLIF(amount,'')::numeric ELSE 0 END) AS fees,
                        SUM(CASE WHEN event_type='Refund' AND charge_type='Principal' THEN NULLIF(amount,'')::numeric ELSE 0 END) AS refunds,
                        SUM(CASE WHEN event_type='ShipmentPromo' THEN NULLIF(amount,'')::numeric ELSE 0 END) AS promos,
                        SUM(CASE WHEN event_type='Adjustment' THEN NULLIF(amount,'')::numeric ELSE 0 END) AS adjustments,
                        COUNT(*) AS transactions
                    FROM finance_events
                    WHERE posted_date >= CURRENT_DATE - INTERVAL '{_days} days'
                """), _c).iloc[0]
            gross = float(_r['gross'] or 0)
            fees  = float(_r['fees']  or 0)
            refs  = float(_r['refunds'] or 0)
            promos= float(_r['promos'] or 0)
            adj   = float(_r['adjustments'] or 0)
            net   = gross + fees + refs + promos + adj
            _api_response({"status":"ok","period_days":_days,"gross":round(gross,2),
                "fees":round(fees,2),"refunds":round(refs,2),"promos":round(promos,2),
                "adjustments":round(adj,2),"net":round(net,2),
                "margin_pct":round(net/gross*100,1) if gross>0 else 0,
                "transactions":int(_r['transactions'])})
        except Exception as e:
            _api_response({"error": str(e)})

    # ── GET /orders ──
    elif _endpoint == "orders":
        _days = int(_qp.get("days", 30))
        try:
            with _engine.connect() as _c:
                _df = pd.read_sql(text(
                    f"SELECT amazon_order_id, purchase_date, sku, item_price, quantity, order_status "
                    f"FROM orders WHERE purchase_date >= CURRENT_DATE - INTERVAL '{_days} days' "
                    f"ORDER BY purchase_date DESC LIMIT 1000"
                ), _c)
            _api_response({"status":"ok","period_days":_days,"count":len(_df),
                           "data":_df.to_dict(orient="records")})
        except Exception as e:
            _api_response({"error": str(e)})

    # ── GET /buybox ──
    elif _endpoint == "buybox":
        try:
            with _engine.connect() as _c:
                _df = pd.read_sql(text(
                    "SELECT asin, sku, is_buybox_winner, price, fulfillment, marketplace "
                    "FROM pricing_buybox ORDER BY snapshot_time DESC"
                ), _c)
            winners = int((_df['is_buybox_winner'].astype(str).str.lower() == 'true').sum())
            _api_response({"status":"ok","total":len(_df),"winners":winners,
                           "win_rate_pct":round(winners/len(_df)*100,1) if len(_df)>0 else 0,
                           "data":_df.to_dict(orient="records")})
        except Exception as e:
            _api_response({"error": str(e)})

    # ── GET /alerts ──
    elif _endpoint == "alerts":
        alerts = []
        try:
            with _engine.connect() as _c:
                # Low Stock
                _inv = pd.read_sql(text("SELECT * FROM fba_inventory"), _c)
                for col in ['Available','Price','Velocity']:
                    if col in _inv.columns:
                        _inv[col] = pd.to_numeric(_inv[col].replace('',''), errors='coerce').fillna(0)
                if 'Velocity' in _inv.columns and 'Available' in _inv.columns:
                    _risk = _inv[_inv['Velocity'] > 0].copy()
                    _risk['days'] = (_risk['Available'] / _risk['Velocity']).round(0)
                    for _, row in _risk[_risk['days'] < 14].iterrows():
                        alerts.append({"type":"LOW_STOCK","sku":row.get('SKU',''),"days_left":int(row['days']),"available":int(row['Available'])})
                # Lost BuyBox
                _bb = pd.read_sql(text("SELECT asin, sku, price FROM pricing_buybox WHERE is_buybox_winner = false OR is_buybox_winner = 'False'"), _c)
                for _, row in _bb.iterrows():
                    alerts.append({"type":"LOST_BUYBOX","asin":row['asin'],"sku":row.get('sku',''),"price":float(row.get('price',0))})
        except Exception as e:
            alerts.append({"type":"ERROR","message":str(e)})
        _api_response({"status":"ok","alerts_count":len(alerts),"alerts":alerts})

    # ── GET /reviews ──
    elif _endpoint == "reviews":
        _limit = int(_qp.get("limit", 100))
        _rating = _qp.get("rating", "")
        try:
            where = f"WHERE rating <= {_rating}" if _rating else ""
            with _engine.connect() as _c:
                _df = pd.read_sql(text(
                    f"SELECT asin, domain, rating, title, review_date "
                    f"FROM amazon_reviews {where} "
                    f"ORDER BY review_date DESC LIMIT {_limit}"
                ), _c)
            _api_response({"status":"ok","count":len(_df),"data":_df.to_dict(orient="records")})
        except Exception as e:
            _api_response({"error": str(e)})

    # ── GET /endpoints (документація) ──
    elif _endpoint == "help":
        _api_response({"status":"ok","base_url":"https://YOUR_APP.streamlit.app","auth":"?key=API_KEY",
            "endpoints":{
                "inventory": "?api=inventory&key=K → всі SKU з залишками",
                "finance":   "?api=finance&key=K&days=30 → P&L за період",
                "orders":    "?api=orders&key=K&days=30 → замовлення",
                "buybox":    "?api=buybox&key=K → Buy Box статус",
                "alerts":    "?api=alerts&key=K → Low Stock + Lost BB",
                "reviews":   "?api=reviews&key=K&limit=100&rating=2 → відгуки",
            }})
    else:
        _api_response({"error": f"Unknown endpoint: {_endpoint}", "hint": "Use ?api=help for docs"})


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
            df = pd.read_sql(text('SELECT * FROM orders ORDER BY purchase_date DESC'), conn)
        if df.empty:
            return pd.DataFrame()
        # Маппінг реальних колонок → стандартні назви
        df['Order Date']   = pd.to_datetime(df['purchase_date'], errors='coerce')
        df['Order ID']     = df['amazon_order_id'] if 'amazon_order_id' in df.columns else df.get('order_id', '')
        df['SKU']          = df['sku'] if 'sku' in df.columns else ''
        df['Item Price']   = pd.to_numeric(df.get('item_price', 0), errors='coerce').fillna(0)
        df['Quantity']     = pd.to_numeric(df.get('quantity', 1), errors='coerce').fillna(1)
        df['Total Price']  = df['Item Price'] * df['Quantity']
        df['Order Status'] = df.get('order_status', '')
        df['Ship Country'] = df.get('ship_country', '')
        return df
    except Exception as e:
        st.error(f"Помилка завантаження orders: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_settlements():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Читаємо реальні назви колонок
            cols_df = pd.read_sql(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='settlements' ORDER BY ordinal_position"
            ), conn)
            real_cols = cols_df['column_name'].tolist()
            date_col = next((c for c in real_cols if c.lower() in ('posted_date','posted date')), None)
            order_clause = f'ORDER BY "{date_col}" DESC' if date_col else ''
            df = pd.read_sql(text(f'SELECT * FROM settlements {order_clause}'), conn)

        if df.empty:
            return pd.DataFrame()

        # Нормалізуємо назви до стандартних
        col_map = {}
        for c in df.columns:
            lc = c.lower().replace(' ', '_')
            if lc == 'posted_date':        col_map[c] = 'Posted Date'
            elif lc == 'transaction_type': col_map[c] = 'Transaction Type'
            elif lc == 'order_id':         col_map[c] = 'Order ID'
            elif lc == 'amount':           col_map[c] = 'Amount'
            elif lc == 'currency':         col_map[c] = 'Currency'
            elif lc == 'quantity':         col_map[c] = 'Quantity'
            elif lc == 'sku':              col_map[c] = 'SKU'
        df = df.rename(columns=col_map)

        df['Amount']      = pd.to_numeric(df.get('Amount', 0), errors='coerce').fillna(0.0)
        df['Quantity']    = pd.to_numeric(df.get('Quantity', 0), errors='coerce').fillna(0)
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
            df_returns = pd.read_sql(text('SELECT * FROM fba_returns ORDER BY return_date DESC'), conn)
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
    color_to_border = {
        "#0d2b1e": "#22c55e",
        "#1a2b1e": "#22c55e",
        "#2b2400": "#f59e0b",
        "#2b0d0d": "#ef4444",
        "#1a1a2e": "#6366f1",
        "#1e293b": "#4472C4",
    }
    border_color = color_to_border.get(color, "#4472C4")
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

    tabs = st.tabs(["💰 Inventory","🏦 Settlements","📈 Трафик (Sales & Traffic)","🛒 Orders","🔙 Повернення (Returns)","⭐ Reviews"])

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

    sel_col, country_col = st.columns([2, 2])

    with country_col:
        if '_domain' in table_df.columns:
            all_domains = sorted(table_df['_domain'].dropna().unique().tolist())
            domain_options = ["🌍 " + t.get("all_countries", "All")] + [
                DOMAIN_LABELS.get(d, d) for d in all_domains
            ]
            sel_domain = st.selectbox("🌍 Країна:", domain_options, key="asin_jump_domain")
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
        st.session_state.pop('rev_asin', None)

    sel_raw = st.sidebar.selectbox("📦 ASIN:", asin_options, index=default_asin_idx, key="rev_asin")
    selected_asin = None if sel_raw == '🌐 Всі ASINи' else sel_raw

    star_filter = st.sidebar.multiselect(t["rev_star_filter"], [5, 4, 3, 2, 1], default=[], key="rev_stars")

    if selected_asin and has_domain:
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
    total_asins  = df['asin'].nunique() if 'asin' in df.columns else 0
    total_asins_db = df_all['asin'].nunique() if 'asin' in df_all.columns else 0

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric(t["total_reviews"],      f"{total_revs:,}")
    c2.metric(t["rev_asins_in_filter"], f"{total_asins:,}",
              delta=f"з {total_asins_db} в базі" if total_asins != total_asins_db else None,
              delta_color="off")
    c3.metric(t["avg_review_rating"],  f"{avg_rating:.2f} ⭐")
    c4.metric(t["verified_pct"],       f"{verified_pct:.1f}%")
    c5.metric("🔴 Негативних (1-2★)",  f"{neg_count:,}")
    c6.metric("🟢 Позитивних (4-5★)",  f"{pos_count:,}")

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
                st.success("🎉 Всі варіанти мають рейтинг >= 4.0")

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

    neg_examples = df[df['rating'] <= 2][['asin', 'domain', 'rating', 'title', 'content']].head(10).to_string() if not df.empty else ""
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

    fa, fb, fc = st.columns([3, 2, 1])

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
            df_for_dl = df[df['domain'] == dl_domain] if dl_domain else df
            asin_opts = sorted(df_for_dl['asin'].dropna().unique().tolist())
            dl_asins_selected = st.multiselect(
                "📦 ASIN (мультивибір):",
                options=asin_opts,
                default=[],
                placeholder="Всі ASINи — або вибери скільки завгодно",
                key="dl_asin_multi"
            )
        else:
            dl_asins_selected = []

    df_dl = df.copy()
    if dl_domain:
        df_dl = df_dl[df_dl['domain'] == dl_domain]
    if dl_asins_selected:
        df_dl = df_dl[df_dl['asin'].isin(dl_asins_selected)]

    with fc:
        st.markdown("<div style='margin-top:28px'></div>", unsafe_allow_html=True)
        n_asins_info = f"{len(dl_asins_selected)} ASIN" if dl_asins_selected else "всі"
        st.caption(f"📊 {len(df_dl)} відгуків ({n_asins_info})")

    if dl_asins_selected and len(dl_asins_selected) > 1 and 'asin' in df_dl.columns:
        with st.expander(f"📊 Порівняння вибраних {len(dl_asins_selected)} ASINів", expanded=True):
            cmp = df_dl.groupby('asin').agg(
                Відгуків=('rating', 'count'),
                Рейтинг=('rating', 'mean'),
                Neg=('rating', lambda x: (x <= 2).sum()),
                Pos=('rating', lambda x: (x >= 4).sum()),
            ).reset_index()
            cmp['Neg %'] = (cmp['Neg'] / cmp['Відгуків'] * 100).round(1)
            cmp['Pos %'] = (cmp['Pos'] / cmp['Відгуків'] * 100).round(1)
            cmp = cmp.sort_values('Рейтинг', ascending=False)

            col_c1, col_c2 = st.columns(2)
            with col_c1:
                colors_cmp = ['#F44336' if r < 4.0 else '#FFC107' if r < 4.4 else '#4CAF50' for r in cmp['Рейтинг']]
                fig_cmp = go.Figure(go.Bar(
                    x=cmp['Рейтинг'], y=cmp['asin'], orientation='h',
                    marker_color=colors_cmp,
                    text=[f"{r:.2f}★" for r in cmp['Рейтинг']],
                    textposition='outside'
                ))
                fig_cmp.add_vline(x=4.0, line_dash="dash", line_color="orange")
                fig_cmp.update_layout(height=max(250, len(cmp) * 40), xaxis_range=[1, 5.5],
                                      title="⭐ Рейтинг", margin=dict(l=5, r=50, t=30, b=10))
                st.plotly_chart(fig_cmp, width="stretch")

            with col_c2:
                neg_colors_cmp = ['#F44336' if v > 20 else '#FFC107' if v > 10 else '#4CAF50' for v in cmp['Neg %']]
                fig_neg = go.Figure(go.Bar(
                    x=cmp['Neg %'], y=cmp['asin'], orientation='h',
                    marker_color=neg_colors_cmp,
                    text=[f"{v:.1f}%" for v in cmp['Neg %']],
                    textposition='outside'
                ))
                fig_neg.update_layout(height=max(250, len(cmp) * 40),
                                      title="🔴 % Негативних", margin=dict(l=5, r=50, t=30, b=10))
                st.plotly_chart(fig_neg, width="stretch")

            st.dataframe(
                cmp[['asin', 'Відгуків', 'Рейтинг', 'Neg %', 'Pos %']].style
                    .format({'Рейтинг': '{:.2f}', 'Neg %': '{:.1f}%', 'Pos %': '{:.1f}%'})
                    .background_gradient(subset=['Рейтинг'], cmap='RdYlGn')
                    .background_gradient(subset=['Neg %'], cmap='RdYlGn_r'),
                width='stretch', hide_index=True
            )

    df_table = balanced_reviews(df_dl, max_per_star=100).sort_values('rating', ascending=True)
    available_cols = [c for c in display_cols if c in df_table.columns]
    dl_cols = [c for c in display_cols if c in df_dl.columns]

    st.dataframe(df_table[available_cols], width="stretch", height=450)

    star_summary = df_table['rating'].value_counts().sort_index(ascending=False)
    summary_str  = " | ".join([f"{s}★: {c}" for s, c in star_summary.items()])
    st.caption(t["rev_shown"].format(n=len(df_table), total=len(df_dl)) + f" · {summary_str}")

    col1, col2 = st.columns(2)
    dl_label = "_".join(dl_asins_selected) if dl_asins_selected else asin_label
    with col1:
        st.download_button(t["rev_dl_balanced"],
            df_table[available_cols].to_csv(index=False).encode('utf-8'),
            f"reviews_balanced_{dl_label}.csv", "text/csv")
        st.caption(t["rev_dl_balanced_hint"])
    with col2:
        st.download_button(t["rev_dl_all"],
            df_all[dl_cols].to_csv(index=False).encode('utf-8'),
            f"reviews_full_{dl_label}.csv", "text/csv")
        st.caption(t["rev_dl_all_hint"])


# ============================================
# AI CHAT
# ============================================

def get_db_schema():
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

2. amazon_reviews — відгуки покупців
   Колонки: id, asin, domain, rating (INT 1-5), title, content, author,
   review_date (DATE), is_verified (BOOL), product_attributes, scraped_at

3. settlements — фінансові розрахунки Amazon
   Колонки з подвійними лапками: "Settlement ID", "Posted Date" (DD.MM.YYYY),
   "Transaction Type", "Order ID", "SKU", "Amount" (TEXT→CAST AS FLOAT),
   "Currency", "Quantity", "Marketplace"

4. fba_inventory — FBA Inventory
   Група А (великі літери, лапки): "SKU","ASIN","Available","Price","Velocity",
   "Days of Supply","Store Name","Market Place"
   ⚠️ "Stock Value" НЕ існує — рахуй: CAST("Available" AS FLOAT)*CAST("Price" AS FLOAT)

5. returns — повернення. Колонки: "Order ID","Return Date","SKU","ASIN",
   "Quantity","Reason","Status","Customer Comments"

6. orders — замовлення. Колонки: "Order ID","Order Date","SKU","ASIN",
   "Quantity","Item Price","Order Status","Ship Country"

КРИТИЧНІ ПРАВИЛА:
- spapi.sales_traffic: завжди FROM spapi.sales_traffic
- Завжди LIMIT 50, тільки SELECT/WITH
- Колонки з великої літери — в подвійних лапках
- NULLIF для уникнення ділення на нуль
"""
    return schema


def run_ai_sql_pipeline(question: str, section_key: str, gemini_model, context: str):
    model = genai.GenerativeModel(gemini_model)
    schema = get_db_schema()

    sql_prompt = f"""Ти — SQL експерт. Напиши PostgreSQL запит для відповіді на питання.

{schema}

ПИТАННЯ: {question}

ПРАВИЛА:
- Тільки SELECT запити
- Використовуй реальні назви таблиць і колонок зі схеми
- LIMIT 50
- Для дат використовуй CURRENT_DATE
- Якщо питання НЕ потребує SQL — відповідай рівно: NO_SQL

Відповідай ТІЛЬКИ SQL кодом без пояснень, без ```sql```, просто чистий SQL.
Або рівно NO_SQL якщо SQL не потрібен."""

    sql_resp = model.generate_content(sql_prompt)
    sql_query = sql_resp.text.strip().replace("```sql", "").replace("```", "").strip()

    if sql_query.upper().startswith("NO_SQL") or len(sql_query) < 10:
        chat_prompt = f"""Ти — AI асистент для Amazon FBA бізнесу MR.EQUIPP LIMITED.
Контекст: {context[:1500]}
Повідомлення: {question}
Відповідай на тій мові на якій написане повідомлення (UA/RU/EN). Коротко і по суті."""
        chat_resp = model.generate_content(chat_prompt)
        return None, None, chat_resp.text

    first_word = sql_query.split()[0].upper() if sql_query.split() else ""
    if first_word not in ("SELECT", "WITH"):
        return sql_query, None, "⚠️ Небезпечний запит заблоковано"

    try:
        import re as _re
        _uc_map = {'≥': '>=', '≤': '<=', '≠': '!=', '—': '--', '–': '-', '−': '-'}
        for _uc, _ac in _uc_map.items():
            sql_query = sql_query.replace(_uc, _ac)

        if 'spapi.sales_traffic' in sql_query:
            sql_query = _re.sub(
                r'(FROM spapi\.sales_traffic\s+WHERE\s)',
                r"\1report_date != '' AND report_date IS NOT NULL AND ",
                sql_query
            )

        _nullif_pairs = [
            ('"Available"', 'FLOAT'), ('"Available"', 'INT'),
            ('"Price"', 'FLOAT'), ('"Velocity"', 'FLOAT'),
            ('"Quantity"', 'INT'), ('"Item Price"', 'FLOAT'),
            ('"Amount"', 'FLOAT'),
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
        err_msg = str(e)
        try:
            fallback_prompt = f"""Amazon FBA асистент. Контекст: {context[:800]}
Питання: {question}
Відповідай КОРОТКО (3-5 речень), мовою питання."""
            fallback_resp = model.generate_content(fallback_prompt)
            return sql_query, None, fallback_resp.text + f"\n\n*⚠️ SQL помилка: {err_msg[:100]}*"
        except Exception as e2:
            return sql_query, None, f"SQL помилка: {err_msg}\nFallback: {e2}"

    result_str = df_result.to_string(index=False, max_rows=30)
    analysis_prompt = f"""Amazon FBA експерт.
Питання: {question}
Результат SQL: {result_str}
Контекст: {context[:500]}
Дай конкретну actionable відповідь з числами. Виділяй ключові числа **жирним**.
Відповідай мовою питання (UA/RU/EN)."""

    analysis_resp = model.generate_content(analysis_prompt)
    return sql_query, df_result, analysis_resp.text


def show_ai_chat(context: str, preset_questions: list, section_key: str):
    st.markdown("---")
    st.markdown("### 🤖 AI Інсайти")

    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    if not gemini_key:
        gemini_key = st.secrets.get("GEMINI_API_KEY", "") if hasattr(st, "secrets") else ""
    if not gemini_key:
        st.info("💡 Додай GEMINI_API_KEY в Streamlit Secrets щоб активувати AI-чат")
        return
    if not GEMINI_OK:
        st.warning("pip install google-generativeai")
        return

    gemini_key2 = os.environ.get("GEMINI_API_KEY_2", "")
    if not gemini_key2 and hasattr(st, "secrets"):
        gemini_key2 = st.secrets.get("GEMINI_API_KEY_2", "")

    if gemini_key2:
        if "gemini_key_counter" not in st.session_state:
            st.session_state["gemini_key_counter"] = 0
        st.session_state["gemini_key_counter"] += 1
        active_key = gemini_key if st.session_state["gemini_key_counter"] % 2 == 0 else gemini_key2
    else:
        active_key = gemini_key

    genai.configure(api_key=active_key)
    gemini_model = os.environ.get("GEMINI_MODEL", "")
    if not gemini_model:
        gemini_model = st.secrets.get("GEMINI_MODEL", "gemini-1.5-flash") if hasattr(st, "secrets") else "gemini-1.5-flash"

    if "ai_session_id" not in st.session_state:
        import uuid, datetime as _dt
        _user = st.session_state.get("user", {})
        _uid  = str(_user.get("id", "anon"))
        _date = _dt.date.today().isoformat()
        st.session_state["ai_session_id"] = f"{_uid}_{_date}_{str(uuid.uuid4())[:6]}"
    session_id = st.session_state["ai_session_id"]

    hist_key = f"ai_history_{section_key}"
    if hist_key not in st.session_state:
        st.session_state[hist_key] = load_chat_history(session_id, section_key)
    history = st.session_state[hist_key]

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

    ai_cols = st.columns(len(preset_questions))
    auto_q = None
    for i, (col, q) in enumerate(zip(ai_cols, preset_questions)):
        if col.button(q, key=f"ai_btn_{section_key}_{i}", width='stretch'):
            auto_q = q

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
                st.dataframe(df_result, width='stretch')

        if analysis and not analysis.startswith("SQL помилка"):
            answer_md = analysis
        elif df_result is not None and df_result.empty:
            answer_md = "📭 Запит виконався, але даних не знайдено."
        else:
            answer_md = "Не вдалось отримати відповідь."

        with st.chat_message("assistant"):
            st.markdown(answer_md)

        history.append({"role": "user", "content": final_q})
        history.append({"role": "assistant", "content": answer_md})
        st.session_state[hist_key] = history

        username = st.session_state.get("user", {}).get("email", "unknown")
        save_chat_message(session_id, username, section_key, "user", final_q)
        save_chat_message(session_id, username, section_key, "assistant", answer_md)


# ============================================
# INVENTORY UNIFIED
# ============================================

def show_inventory_unified():
    st.markdown("### 📦 Склад (Inventory)")
    engine = get_engine()

    # ── Схема ──
    try:
        with engine.connect() as conn:
            real_cols = pd.read_sql(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='fba_inventory' ORDER BY ordinal_position"
            ), conn)['column_name'].tolist()
    except Exception as e:
        st.error(f"Помилка схеми: {e}"); return

    col_map = {}
    for c in real_cols:
        lc = c.lower().replace(' ', '_').replace('-', '_')
        if lc in ('sku','seller_sku'):                     col_map['sku']        = c
        elif lc == 'asin':                                 col_map['asin']       = c
        elif lc in ('available','afn_fulfillable_quantity'):col_map['available']  = c
        elif lc in ('price','your_price'):                 col_map['price']      = c
        elif lc in ('velocity','sales_velocity'):          col_map['velocity']   = c
        elif lc in ('days_of_supply','days_supply'):       col_map['dos']        = c
        elif lc in ('inbound_quantity','inbound'):         col_map['inbound']    = c
        elif lc in ('reserved_quantity','reserved'):       col_map['reserved']   = c
        elif lc in ('store_name','marketplace','market_place'): col_map['store'] = c
        elif lc in ('product_name','title','name'):        col_map['name']       = c
        elif lc in ('afn_unsellable_quantity','unsellable'):col_map['unsellable']= c
        # Age колонки
        elif 'upto_90' in lc or '0_to_90' in lc or 'age_0' in lc:   col_map['age_0_90']    = c
        elif '91_to_180' in lc or 'age_91' in lc:                     col_map['age_91_180']  = c
        elif '181_to_270' in lc or 'age_181' in lc:                    col_map['age_181_270'] = c
        elif '271_to_365' in lc or 'age_271' in lc:                    col_map['age_271_365'] = c
        elif '365' in lc and ('plus' in lc or 'over' in lc or 'more' in lc): col_map['age_365_plus'] = c

    sku_c   = col_map.get('sku')
    asin_c  = col_map.get('asin')
    avail_c = col_map.get('available')
    price_c = col_map.get('price')
    vel_c   = col_map.get('velocity')
    dos_c   = col_map.get('dos')
    inb_c   = col_map.get('inbound')
    res_c   = col_map.get('reserved')
    store_c = col_map.get('store')
    name_c  = col_map.get('name')
    uns_c   = col_map.get('unsellable')
    age_cols = {k: v for k, v in col_map.items() if k.startswith('age_')}

    # ── Sidebar ──
    search_sku  = st.sidebar.text_input("🔍 SKU", "", key="inv_sku")
    search_asin = st.sidebar.text_input("🔍 ASIN", "", key="inv_asin")
    stores_list = []
    if store_c:
        try:
            with engine.connect() as conn:
                stores_list = sorted(pd.read_sql(text(
                    f'SELECT DISTINCT "{store_c}" FROM fba_inventory WHERE "{store_c}" IS NOT NULL'
                ), conn).iloc[:,0].dropna().tolist())
        except: pass
    sel_store    = st.sidebar.selectbox("🏪 Магазин:", ["Всі"] + stores_list, key="inv_store")
    alert_filter = st.sidebar.selectbox("⚠️ Фільтр:", ["Всі", "Low Stock <30д", "Out of Stock", "Inbound є"], key="inv_alert")

    # ── Дані ──
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT * FROM fba_inventory"), conn)
    except Exception as e:
        st.error(f"Помилка: {e}"); return
    if df.empty:
        st.warning("fba_inventory порожня"); return

    for c in [avail_c, price_c, vel_c, dos_c, inb_c, res_c, uns_c] + list(age_cols.values()):
        if c and c in df.columns:
            df[c] = pd.to_numeric(df[c].replace('', None), errors='coerce').fillna(0)

    df['_value'] = (df[avail_c] * df[price_c]) if (avail_c and price_c) else 0

    # DoS розрахунок
    if not dos_c and vel_c and avail_c:
        df['_dos'] = (df[avail_c] / df[vel_c].replace(0, float('nan'))).round(0).fillna(0)
        dos_use = '_dos'
    else:
        dos_use = dos_c

    # Фільтри
    df_f = df.copy()
    if search_sku and sku_c:
        df_f = df_f[df_f[sku_c].astype(str).str.contains(search_sku, case=False, na=False)]
    if search_asin and asin_c:
        df_f = df_f[df_f[asin_c].astype(str).str.contains(search_asin, case=False, na=False)]
    if sel_store != "Всі" and store_c:
        df_f = df_f[df_f[store_c] == sel_store]
    if alert_filter == "Low Stock <30д" and dos_use:
        df_f = df_f[(df_f[dos_use] > 0) & (df_f[dos_use] < 30)]
    elif alert_filter == "Out of Stock" and avail_c:
        df_f = df_f[df_f[avail_c] == 0]
    elif alert_filter == "Inbound є" and inb_c:
        df_f = df_f[df_f[inb_c] > 0]

    # KPI
    total_sku   = len(df_f)
    total_avail = int(df_f[avail_c].sum()) if avail_c else 0
    total_value = df_f['_value'].sum()
    total_inb   = int(df_f[inb_c].sum()) if inb_c else 0
    total_res   = int(df_f[res_c].sum()) if res_c else 0
    total_uns   = int(df_f[uns_c].sum()) if uns_c else 0
    oos_cnt     = int((df_f[avail_c] == 0).sum()) if avail_c else 0
    low30_cnt   = int(((df_f[dos_use] > 0) & (df_f[dos_use] < 30)).sum()) if dos_use else 0
    low14_cnt   = int(((df_f[dos_use] > 0) & (df_f[dos_use] < 14)).sum()) if dos_use else 0

    # ── Hero Card ──
    val_color = "#4CAF50" if total_value > 0 else "#888"
    st.markdown(f"""
<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border:1px solid #2d2d4a;
            border-radius:12px;padding:20px 28px;margin-bottom:16px;
            display:flex;align-items:center;gap:32px;flex-wrap:wrap">
  <div>
    <div style="font-size:12px;color:#888;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">
      📦 Вартість складу
    </div>
    <div style="font-size:48px;font-weight:900;color:{val_color};font-family:monospace;line-height:1">
      {_fmt(total_value)}
    </div>
    <div style="font-size:12px;color:#666;margin-top:6px">{total_sku} SKU &nbsp;·&nbsp; {total_avail:,} штук</div>
  </div>
  <div style="flex:1;min-width:200px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <span style="background:#1e2e1e;border:1px solid #2d4a30;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      ✅ Available <b style="color:#4CAF50">{total_avail:,}</b>
    </span>
    <span style="background:#1a2b2e;border:1px solid #2d404a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🚚 Inbound <b style="color:#5B9BD5">{total_inb:,}</b>
    </span>
    <span style="background:#2b2b1a;border:1px solid #4a4a2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🔒 Reserved <b style="color:#FFC107">{total_res:,}</b>
    </span>
    <span style="background:#2b1a1a;border:1px solid #4a2d2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📦 OOS <b style="color:#F44336">{oos_cnt}</b>
    </span>
  </div>
</div>""", unsafe_allow_html=True)

    # ── Інсайти ──
    _icols = st.columns(3)
    if low14_cnt > 0:
        with _icols[0]: insight_card("🔴", "Критичний Low Stock",
            f"<b>{low14_cnt} SKU</b> залишилось менше <b>14 днів</b> — терміново поповни!", "#2b0d0d")
    elif low30_cnt > 0:
        with _icols[0]: insight_card("🟡", "Low Stock <30д",
            f"<b>{low30_cnt} SKU</b> — менше 30 днів запасів", "#2b2400")
    else:
        with _icols[0]: insight_card("🟢", "Запаси в нормі",
            "Всі активні SKU мають достатній запас", "#0d2b1e")

    oos_pct = oos_cnt / total_sku * 100 if total_sku > 0 else 0
    if oos_pct > 50:
        with _icols[1]: insight_card("🔴", "Багато OOS",
            f"<b>{oos_cnt} SKU ({oos_pct:.0f}%)</b> без залишків — перевір активність листингів", "#2b0d0d")
    else:
        with _icols[1]: insight_card("🟡", f"OOS: {oos_cnt} SKU",
            f"{oos_cnt} SKU з нульовими залишками ({oos_pct:.0f}% від загального)", "#2b2400")

    avg_price_val = df_f[price_c][df_f[price_c] > 0].mean() if price_c else 0
    with _icols[2]: insight_card("💰", "Середня ціна",
        f"Avg Price: <b>${avg_price_val:.2f}</b> · Вартість складу: <b>{_fmt(total_value)}</b>", "#1a1a2e")

    st.markdown("---")

    # ── Алерти ──
    if low14_cnt > 0:
        urgent = df_f[(df_f[dos_use] > 0) & (df_f[dos_use] < 14)].sort_values(dos_use) if dos_use else pd.DataFrame()
        st.error(f"🔴 **{low14_cnt} SKU** закінчаться за **<14 днів** — терміново!")
        if not urgent.empty and sku_c:
            cols_u = [c for c in [sku_c, avail_c, vel_c, dos_use, price_c] if c and c in urgent.columns]
            st.dataframe(urgent[cols_u].rename(columns={sku_c:'SKU',avail_c:'Available',
                         vel_c:'Velocity',dos_use:'Days',price_c:'Price'}).head(10),
                         width="stretch", hide_index=True)
    if low30_cnt > 0:
        st.warning(f"🟡 **{low30_cnt} SKU** — менше 30 днів запасів")
    if oos_cnt > 0:
        st.warning(f"🟡 **{oos_cnt} SKU** з нульовими залишками (Out of Stock)")
    if total_uns > 0:
        st.error(f"❌ Unsellable: **{total_uns}** одиниць")

    st.markdown("---")

    # ── Графіки ──
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 💰 Де заморожені гроші (Treemap)")
        dm = df_f[df_f['_value'] > 0].copy()
        if not dm.empty and sku_c:
            path = [store_c, sku_c] if store_c and store_c in dm.columns else [sku_c]
            fig = px.treemap(dm, path=path, values='_value',
                             color='_value', color_continuous_scale='RdYlGn_r', height=380)
            fig.update_layout(margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig, width="stretch")

    with col2:
        st.markdown("#### 🏆 Топ 15 SKU за залишками")
        if avail_c and sku_c:
            top = df_f[df_f[avail_c] > 0].nlargest(15, avail_c)
            colors = []
            for _, row in top.iterrows():
                d = row.get(dos_use, 999) if dos_use else 999
                colors.append('#F44336' if d < 14 else '#FFC107' if d < 30 else '#4CAF50')
            fig2 = go.Figure(go.Bar(
                x=top[avail_c], y=top[sku_c], orientation='h',
                marker_color=colors,
                text=[f"{int(v)} units" for v in top[avail_c]], textposition='outside'
            ))
            fig2.update_layout(height=380, yaxis={'categoryorder':'total ascending'},
                               margin=dict(l=0,r=80,t=10,b=0))
            st.plotly_chart(fig2, width="stretch")

    # ── Age Analysis ──
    if age_cols:
        st.markdown("---")
        st.markdown("#### 📦 Aging — вік товару на складі")
        st.caption("🟢 <90д — норм · 🟡 91-180д — увага · 🔴 >180д — ризик LTSF-комісій")

        age_labels = {
            'age_0_90':    '🟢 0-90 днів',
            'age_91_180':  '🟡 91-180 днів',
            'age_181_270': '🟠 181-270 днів',
            'age_271_365': '🔴 271-365 днів',
            'age_365_plus':'⛔ 365+ днів',
        }
        age_data = []
        for key, label in age_labels.items():
            if key in age_cols:
                col_name = age_cols[key]
                total_units_age = df_f[col_name].sum()
                total_val_age   = (df_f[col_name] * df_f[price_c]).sum() if price_c else 0
                age_data.append({'Вік': label, 'Одиниць': int(total_units_age), 'Вартість': total_val_age})

        if age_data:
            df_age = pd.DataFrame(age_data)
            col1, col2 = st.columns(2)
            with col1:
                colors_age = ['#4CAF50','#FFC107','#FF9800','#F44336','#B71C1C'][:len(df_age)]
                fig_age = go.Figure(go.Bar(
                    x=df_age['Одиниць'], y=df_age['Вік'], orientation='h',
                    marker_color=colors_age,
                    text=[f"{v:,} units" for v in df_age['Одиниць']], textposition='outside'
                ))
                fig_age.update_layout(height=300, title="Одиниць по віку",
                                      margin=dict(l=0,r=80,t=30,b=0))
                st.plotly_chart(fig_age, width="stretch")
            with col2:
                fig_pie = px.pie(df_age, values='Вартість', names='Вік',
                                 color_discrete_sequence=['#4CAF50','#FFC107','#FF9800','#F44336','#B71C1C'],
                                 hole=0.4, title="Вартість по віку")
                fig_pie.update_layout(height=300)
                st.plotly_chart(fig_pie, width="stretch")

            # Топ старих SKU
            old_cols = [age_cols[k] for k in ['age_181_270','age_271_365','age_365_plus'] if k in age_cols]
            if old_cols and sku_c:
                df_f['_old_units'] = df_f[old_cols].sum(axis=1)
                old_sku = df_f[df_f['_old_units'] > 0].nlargest(10, '_old_units')
                if not old_sku.empty:
                    st.markdown("##### ⚠️ SKU з товаром старше 180 днів (ризик LTSF)")
                    show_c = [c for c in [sku_c, avail_c, price_c, '_value', '_old_units'] + old_cols if c in old_sku.columns]
                    st.dataframe(old_sku[show_c].rename(columns={
                        sku_c:'SKU', avail_c:'Available', price_c:'Price',
                        '_value':'Stock Value', '_old_units':'Old Units (180+d)'
                    }).style.format({'Price':'${:.2f}','Stock Value':'${:,.0f}'}),
                    width="stretch", hide_index=True)

    # ── Velocity Chart ──
    if vel_c and sku_c:
        st.markdown("---")
        st.markdown("#### 🚀 Топ 15 SKU за Velocity (продажів/місяць)")
        top_vel = df_f[df_f[vel_c] > 0].nlargest(15, vel_c)
        if not top_vel.empty:
            fig_v = go.Figure(go.Bar(
                x=top_vel[vel_c], y=top_vel[sku_c], orientation='h',
                marker_color='#5B9BD5',
                text=[f"{v:.1f}/міс" for v in top_vel[vel_c]], textposition='outside'
            ))
            fig_v.update_layout(height=max(300, len(top_vel)*35),
                                yaxis={'categoryorder':'total ascending'},
                                margin=dict(l=0,r=80,t=10,b=0))
            st.plotly_chart(fig_v, width="stretch")

    # ── DoS Chart ──
    if dos_use and sku_c:
        st.markdown("---")
        st.markdown("#### ⏳ Days of Supply (топ 20 ризикованих)")
        df_dos = df_f[(df_f[dos_use] > 0) & (df_f[dos_use] < 60)].nsmallest(20, dos_use)
        if not df_dos.empty:
            colors_dos = ['#F44336' if v < 14 else '#FFC107' if v < 30 else '#4CAF50' for v in df_dos[dos_use]]
            fig3 = go.Figure(go.Bar(
                x=df_dos[dos_use], y=df_dos[sku_c], orientation='h',
                marker_color=colors_dos,
                text=[f"{int(v)}д" for v in df_dos[dos_use]], textposition='outside'
            ))
            fig3.add_vline(x=14, line_dash="dash", line_color="#F44336", annotation_text="14д ⚠️")
            fig3.add_vline(x=30, line_dash="dash", line_color="#FFC107", annotation_text="30д")
            fig3.update_layout(height=max(300, len(df_dos)*35),
                               yaxis={'categoryorder':'total ascending'},
                               margin=dict(l=0,r=80,t=10,b=0))
            st.plotly_chart(fig3, width="stretch")

    # ── Таблиця ──
    st.markdown("---")
    st.markdown("#### 📋 Таблиця складу")
    show_map = {sku_c:'SKU', asin_c:'ASIN', name_c:'Назва', avail_c:'Available',
                inb_c:'Inbound', res_c:'Reserved', price_c:'Price',
                vel_c:'Velocity', dos_use:'DoS', store_c:'Store', '_value':'Value'}
    show_c = [c for c in show_map if c and c in df_f.columns]
    df_show = df_f[show_c].rename(columns=show_map).sort_values('DoS' if 'DoS' in [show_map[c] for c in show_c] else 'Available').head(500)
    fmt = {}
    if 'Price' in df_show.columns:  fmt['Price']  = '${:.2f}'
    if 'Value' in df_show.columns:  fmt['Value']  = '${:,.0f}'
    if 'DoS' in df_show.columns:    fmt['DoS']    = '{:.0f}'
    st.dataframe(df_show.style.format(fmt) if fmt else df_show,
                 width="stretch", hide_index=True, height=450)
    st.caption(f"Показано {len(df_show):,} з {len(df_f):,} SKU")
    st.download_button("📥 CSV", df_f.to_csv(index=False).encode(), "inventory.csv", "text/csv")

    # ── AI ──
    ctx = f"""Inventory: {total_sku} SKU | Available: {total_avail:,} | Value: {_fmt(total_value)}
Low Stock <14д: {low14_cnt} | Low Stock <30д: {low30_cnt} | OOS: {oos_cnt} | Inbound: {total_inb:,}"""
    show_ai_chat(ctx, [
        "Які SKU під ризиком out-of-stock найближчі 14 днів?",
        "Де найбільше заморожених грошей?",
        "Які SKU старше 180 днів — ризик LTSF?",
    ], "inventory")


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
        sql_returns     = 'SELECT COUNT(*), MAX(return_date) FROM fba_returns'
        sql_fba         = 'SELECT COUNT(*), MAX(created_at) FROM fba_inventory'
        sql_st          = "SELECT COUNT(*), MAX(CAST(report_date AS DATE)) FROM spapi.sales_traffic WHERE report_date != ''"
        sql_rev         = 'SELECT COUNT(*), MAX(created_at) FROM amazon_reviews'

        modules = [
            ("📦 FBA Inventory",   "fba_inventory",       q(sql_fba),         "3× / день"),
            ("🏦 Settlements",     "settlements",         q(sql_settlements), "3× / день"),
            ("🛒 Orders",          "orders",              q(sql_orders),      "2× / день"),
            ("🔙 Повернення (Returns)",         "returns",             q(sql_returns),     "1× / день"),
            ("📈 Трафик (Sales & Traffic)", "spapi.sales_traffic", q(sql_st),          "2× / день"),
            ("⭐ Reviews",         "amazon_reviews",      q(sql_rev),         "за запитом"),
            ("📣 Advertising",     "advertising",         (0, None),          "—"),
        ]
        cur.close()
        conn.close()
    except Exception as e:
        st.error(f"DB помилка: {e}")
        return

    now = _dt.datetime.now(_dt.timezone.utc).date()
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
    st.dataframe(_pd.DataFrame(data), width='stretch', hide_index=True)


def show_api_docs():
    st.markdown("## 🔌 API — доступ до даних")
    st.caption("REST-like API через URL параметри Streamlit")

    api_key = os.getenv("API_KEY", "merino2024")
    base    = "https://merino-bi.streamlit.app"

    st.markdown("### 🔑 Авторизація")
    st.code(f"?key={api_key}", language="bash")
    st.caption("Задай `API_KEY` в Streamlit Secrets для зміни ключа")

    st.markdown("---")
    st.markdown("### 📋 Ендпоінти")

    endpoints = [
        ("📦 Inventory",    f"{base}/?api=inventory&key={api_key}",               "Всі SKU з залишками з fba_inventory"),
        ("💰 Finance P&L",  f"{base}/?api=finance&key={api_key}&days=30",         "P&L за N днів з finance_events"),
        ("🛒 Orders",       f"{base}/?api=orders&key={api_key}&days=30",           "Замовлення за N днів"),
        ("🏆 Buy Box",      f"{base}/?api=buybox&key={api_key}",                   "Buy Box статус по ASIN"),
        ("🚨 Alerts",       f"{base}/?api=alerts&key={api_key}",                   "Low Stock + Lost Buy Box алерти"),
        ("⭐ Reviews",      f"{base}/?api=reviews&key={api_key}&limit=100&rating=2","Відгуки (фільтр по рейтингу)"),
        ("📖 Help",         f"{base}/?api=help&key={api_key}",                     "Документація всіх ендпоінтів"),
    ]

    for name, url, desc in endpoints:
        with st.expander(f"{name} — {desc}"):
            st.code(url, language="bash")
            st.markdown(f"[🔗 Відкрити в браузері]({url})")

    st.markdown("---")
    st.markdown("### 💡 Приклади використання")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Python:**")
        st.code(f"""import requests, json

url = "{base}/?api=finance&key={api_key}&days=30"
r = requests.get(url)
# парсимо JSON з відповіді
""", language="python")

    with col2:
        st.markdown("**Power BI / Excel:**")
        st.code(f"""= Json.Document(
    Web.Contents(
      "{base}/?api=inventory&key={api_key}"
    )
)""", language="text")

    st.markdown("---")
    st.markdown("### ⚙️ Параметри")
    st.dataframe(pd.DataFrame([
        {"Параметр": "api",    "Значення": "inventory / finance / orders / buybox / alerts / reviews / help", "Обов'язковий": "✅"},
        {"Параметр": "key",    "Значення": "API ключ з Streamlit Secrets",  "Обов'язковий": "✅"},
        {"Параметр": "days",   "Значення": "30 / 60 / 90 (для finance, orders)", "Обов'язковий": "❌"},
        {"Параметр": "limit",  "Значення": "100 (для reviews)",             "Обов'язковий": "❌"},
        {"Параметр": "rating", "Значення": "1-5 (для reviews)",             "Обов'язковий": "❌"},
    ]), width="stretch", hide_index=True)


def show_about():
    st.markdown("""
<style>
:root {
  --ab-bg: #f8fafc; --ab-border: #e2e8f0; --ab-text: #1e293b;
  --ab-muted: #64748b; --ab-accent: #d97706; --ab-card: #ffffff;
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

    st.markdown("""
<div class="about-grid">
  <div class="about-stat"><div class="about-stat-num">30+</div><div class="about-stat-lbl">Типів звітів</div></div>
  <div class="about-stat"><div class="about-stat-num">36×</div><div class="about-stat-lbl">Оновлень/день</div></div>
  <div class="about-stat"><div class="about-stat-num">9</div><div class="about-stat-lbl">Маркетплейсів</div></div>
  <div class="about-stat"><div class="about-stat-num">3</div><div class="about-stat-lbl">Мови інтерфейсу</div></div>
</div>""", unsafe_allow_html=True)

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

    st.markdown("---")
    st.caption(t["about_footer"])


def show_overview(df_filtered, t, selected_date):
    st.markdown(f"### {t['ov_title']}")
    st.caption(f"📅 {selected_date}")

    if df_filtered.empty:
        st.info("Немає даних по інвентарю. Перевір ETL або обери іншу дату.")
        return

    # ── KPI (тільки з df_filtered — вже в пам'яті, без запитів до БД) ──
    total_sku   = len(df_filtered)
    total_units = int(df_filtered['Available'].sum())
    total_value = df_filtered['Stock Value'].sum()
    avg_price   = df_filtered[df_filtered['Price'] > 0]['Price'].mean()
    vel_30      = df_filtered['Velocity'].sum() if 'Velocity' in df_filtered.columns else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("📦 SKU",             f"{total_sku:,}")
    c2.metric("🔢 Штук на складі",  f"{total_units:,}")
    c3.metric("💰 Вартість складу", f"${total_value:,.0f}")
    c4.metric("💵 Сер. ціна",       f"${avg_price:.2f}" if not pd.isna(avg_price) else "—")
    c5.metric("🚀 Velocity/міс",    f"{vel_30:,.0f}")

    st.markdown("---")

    # ── LOW STOCK ALERT ──
    st.markdown("### 🚨 Low Stock Alert")
    critical = pd.DataFrame()
    if 'Velocity' in df_filtered.columns:
        df_risk  = df_filtered[df_filtered['Velocity'] > 0].copy()
        df_risk['days_left'] = (df_risk['Available'] / df_risk['Velocity']).round(0)
        critical = df_risk[df_risk['days_left'] < 14].sort_values('days_left')
        warning  = df_risk[(df_risk['days_left'] >= 14) & (df_risk['days_left'] < 30)]
        if not critical.empty:
            st.error(f"🔴 **{len(critical)} SKU** закінчаться за **<14 днів**")
            cols_show = [c for c in ['SKU','Available','Velocity','days_left'] if c in critical.columns]
            st.dataframe(critical[cols_show].head(10), width='stretch', hide_index=True)
        if not warning.empty:
            st.warning(f"🟡 **{len(warning)} SKU** — залишилось 14–30 днів")
        if critical.empty and warning.empty:
            st.success("✅ Всі SKU в нормі")
    else:
        st.info("Немає даних Velocity")

    st.markdown("---")

    # ── ТОП 15 SKU ──
    st.markdown("### 📊 Топ 15 SKU за залишками")
    top_sku  = df_filtered.nlargest(15, 'Available')
    cols_top = [c for c in ['SKU','Product Name','Available','Price','Stock Value','Store Name'] if c in top_sku.columns]
    st.dataframe(
        top_sku[cols_top].style.format({'Price': '${:.2f}', 'Stock Value': '${:,.0f}'}),
        width='stretch', hide_index=True
    )

    st.markdown("---")

    # ── TREEMAP ──
    st.markdown("### 💰 Де заморожені гроші?")
    dm = df_filtered[df_filtered['Stock Value'] > 0]
    if not dm.empty:
        path = []
        if 'Store Name' in dm.columns: path.append('Store Name')
        path.append('SKU')
        fig = px.treemap(dm, path=path, values='Stock Value',
                         color='Stock Value', color_continuous_scale='RdYlGn_r', height=420)
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, width='stretch')

    # ── AI CHAT ──
    ctx_overview = f"""Inventory Overview:
- SKU: {total_sku} | Штук: {total_units:,} | Вартість: ${total_value:,.0f}
- Low Stock <14д: {len(critical)} SKU
- Дата: {selected_date}"""
    show_ai_chat(ctx_overview, [
        "Які SKU під ризиком out-of-stock найближчі 14 днів?",
        "Де найбільше заморожених грошей?",
        "Яка загальна оборотність складу?",
    ], "overview")


def show_sales_traffic(t):
    df_st = load_sales_traffic()
    if df_st.empty:
        st.warning("⚠️ No Sales & Traffic data found."); return
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

    ts  = int(df_filtered['sessions'].sum())
    tpv = int(df_filtered['page_views'].sum())
    tu  = int(df_filtered['units_ordered'].sum())
    tr  = df_filtered['ordered_product_sales'].sum()
    ac  = tu/ts*100 if ts>0 else 0
    ab  = df_filtered['buy_box_percentage'].mean()
    d1  = str(date_range[0]) if len(date_range)==2 else str(min_date)
    d2  = str(date_range[1]) if len(date_range)==2 else str(max_date)

    # ── Hero Card ──
    rev_color = "#4CAF50" if tr > 0 else "#888"
    st.markdown(f"""
<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border:1px solid #2d2d4a;
            border-radius:12px;padding:20px 28px;margin-bottom:16px;
            display:flex;align-items:center;gap:32px;flex-wrap:wrap">
  <div>
    <div style="font-size:12px;color:#888;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">
      📈 Дохід за період
    </div>
    <div style="font-size:48px;font-weight:900;color:{rev_color};font-family:monospace;line-height:1">
      {_fmt(tr)}
    </div>
    <div style="font-size:12px;color:#666;margin-top:6px">{d1} → {d2}</div>
  </div>
  <div style="flex:1;min-width:200px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <span style="background:#1e2e1e;border:1px solid #2d4a30;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      👁 Sessions <b style="color:#4CAF50">{ts:,}</b>
    </span>
    <span style="background:#1a2b2e;border:1px solid #2d404a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🛒 Units <b style="color:#5B9BD5">{tu:,}</b>
    </span>
    <span style="background:#2b2b1a;border:1px solid #4a4a2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📊 CVR <b style="color:#FFC107">{ac:.2f}%</b>
    </span>
    <span style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🏆 Buy Box <b style="color:#AB47BC">{ab:.1f}%</b>
    </span>
  </div>
</div>""", unsafe_allow_html=True)

    # ── Інсайти ──
    asin_col = 'child_asin' if 'child_asin' in df_filtered.columns else df_filtered.columns[0]
    as_ = df_filtered.groupby(asin_col).agg(
        {'sessions':'sum','units_ordered':'sum','ordered_product_sales':'sum','buy_box_percentage':'mean'}
    ).reset_index()
    as_.columns = ['ASIN','Sessions','Units','Revenue','Buy Box %']
    as_['Conv %'] = (as_['Units']/as_['Sessions']*100).fillna(0)
    insights_sales_traffic(df_filtered, as_)

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
    ctx = f"""Sales & Traffic: Сесії {ts:,} | Дохід ${tr:,.2f} | Конверсія {ac:.2f}% | Buy Box {ab:.1f}%"""
    show_ai_chat(ctx, [
        "Який ASIN виріс найбільше за останні 7 днів?",
        "Які ASIN мають Buy Box нижче 80%?",
        "Де CVR вище середнього? Топ 5",
    ], "sales_traffic")


def _fin_load(table, date_col=None, limit=5000):
    """Універсальний завантажувач фінансових таблиць."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            order = f'ORDER BY "{date_col}" DESC' if date_col else ''
            df = pd.read_sql(text(f'SELECT * FROM {table} {order} LIMIT {limit}'), conn)
        return df
    except Exception:
        return pd.DataFrame()


def _fmt(v):
    """$1.25M / $455K / $1,234"""
    a = abs(v)
    sign = "-" if v < 0 else ""
    if a >= 1_000_000: return f"{sign}${a/1_000_000:.2f}M"
    if a >= 1_000:     return f"{sign}${a/1_000:.1f}K"
    return f"{sign}${a:,.0f}"


def show_settlements(t):
    st.markdown("### 💰 Фінанси (Settlements / Fees)")
    st.caption("Виплати, комісії, компенсації — реальні дані SP-API")

    # ── Sidebar фільтри ──

    engine = get_engine()

    # Визначаємо діапазон дат з settlements
    try:
        with engine.connect() as conn:
            bounds = pd.read_sql(text(
                "SELECT MIN(posted_date)::date as mn, MAX(posted_date)::date as mx FROM settlements"
            ), conn).iloc[0]
        min_date = bounds['mn']
        max_date = bounds['mx']
    except Exception:
        min_date = dt.date(2024, 1, 1)
        max_date = dt.date.today()

    # Беремо date range з глобального sidebar (вже відрендерено вище)
    date_range = st.session_state.get("fin_date", (min_date, max_date))
    if not date_range or len(date_range) != 2:
        date_range = (min_date, max_date)
    d1, d2 = str(date_range[0]), str(date_range[1])

    # ══════════════════════════════════════════
    # KPI з settlements
    # ══════════════════════════════════════════
    # ── KPI з finance_events (повні дані 1.15M рядків) ──
    try:
        with engine.connect() as conn:
            fe_main = pd.read_sql(text("""
                SELECT
                    -- Gross БЕЗ Tax (Tax збирає Amazon для держави, не наші гроші)
                    SUM(CASE WHEN event_type = 'Shipment'
                         AND charge_type = 'Principal'
                         AND COALESCE(charge_type,'') != 'Tax'
                        THEN NULLIF(amount,'')::numeric ELSE 0 END)          AS gross_sales,
                    SUM(CASE WHEN event_type = 'Refund' AND charge_type = 'Principal'
                        THEN NULLIF(amount,'')::numeric ELSE 0 END)          AS refunds,
                    SUM(CASE WHEN event_type IN ('ShipmentFee','RefundFee')
                        THEN NULLIF(amount,'')::numeric ELSE 0 END)          AS fees,
                    SUM(CASE WHEN event_type = 'ShipmentPromo'
                        THEN NULLIF(amount,'')::numeric ELSE 0 END)          AS promos,
                    -- Adjustments: компенсації Amazon (lost/damaged)
                    SUM(CASE WHEN event_type = 'Adjustment'
                        THEN NULLIF(amount,'')::numeric ELSE 0 END)          AS adjustments,
                    SUM(CASE WHEN event_type = 'RefundFee'
                        THEN NULLIF(amount,'')::numeric ELSE 0 END)          AS refund_fees,
                    COUNT(*)                                                  AS total_rows
                FROM finance_events
                WHERE posted_date BETWEEN :d1 AND :d2
            """), conn, params={"d1": d1, "d2": d2}).iloc[0]
    except Exception as e:
        st.error(f"Помилка завантаження finance_events: {e}"); return

    gross       = float(fe_main['gross_sales']  or 0)
    refs        = float(fe_main['refunds']      or 0)
    fees        = float(fe_main['fees']         or 0)
    promos      = float(fe_main['promos']       or 0)
    adjustments = float(fe_main['adjustments']  or 0)
    refund_fees = float(fe_main['refund_fees']  or 0)
    rows        = int(fe_main['total_rows']     or 0)
    net         = gross + refs + fees + promos + adjustments  # без refund_fees (вже в fees)

    # orders з settlements
    orders = 0
    try:
        with engine.connect() as conn:
            ord_r = pd.read_sql(text(
                "SELECT COUNT(DISTINCT order_id) AS cnt FROM settlements "
                "WHERE posted_date BETWEEN :d1 AND :d2 AND order_id IS NOT NULL AND order_id != ''"
            ), conn, params={"d1": d1, "d2": d2}).iloc[0]
            orders = int(ord_r['cnt'] or 0)
    except Exception:
        pass

    margin_pct = net / gross * 100 if gross > 0 else 0

    # ── Hero метрика ──
    net_color = "#4CAF50" if net > 0 else "#F44336"
    st.markdown(f"""
<div style="background:linear-gradient(135deg,#1a2b1e,#0d1f12);border:1px solid #2d4a30;
            border-radius:12px;padding:20px 28px;margin-bottom:16px;
            display:flex;align-items:center;gap:32px;flex-wrap:wrap">
  <div>
    <div style="font-size:12px;color:#888;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">
      💰 Чистий заробіток за період
    </div>
    <div style="font-size:48px;font-weight:900;color:{net_color};font-family:monospace;line-height:1">
      {_fmt(net)}
    </div>
    <div style="font-size:12px;color:#666;margin-top:6px">{d1} → {d2} · {orders:,} замовлень</div>
  </div>
  <div style="flex:1;min-width:200px">
    <div style="display:flex;gap:8px;flex-wrap:wrap">
      <span style="background:#1e2e1e;border:1px solid #2d4a30;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
        📈 Gross <b style="color:#4CAF50">{_fmt(gross)}</b>
      </span>
      <span style="background:#2b1a1a;border:1px solid #4a2d2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
        💸 Fees <b style="color:#F44336">{_fmt(fees)}</b>
      </span>
      <span style="background:#2b1a1a;border:1px solid #4a2d2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
        🔄 Refunds <b style="color:#FF9800">{_fmt(refs)}</b>
      </span>
      <span style="background:#2b1a1a;border:1px solid #4a2d2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
        🎫 Promos <b style="color:#FF9800">{_fmt(promos)}</b>
      </span>
      <span style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
        📊 Маржа <b style="color:#5B9BD5">{margin_pct:.1f}%</b>
      </span>
    </div>
  </div>
</div>""", unsafe_allow_html=True)
    st.caption(f"📋 {rows:,} транзакцій · {d1} → {d2}")

    # ── АВТОІНСАЙТИ одразу після KPI ──
    st.markdown("---")
    insights_settlements_v2(net, gross, refs, fees)

    # ── WATERFALL P&L ──
    st.markdown("---")
    st.markdown("### 📊 P&L Waterfall")
    labels  = ["Gross Sales", "Amazon Fees", "Refunds", "Promotions", "Adjustments", "Net Payout"]
    values  = [gross, fees, refs, promos, adjustments, net]
    measure = ["absolute","relative","relative","relative","relative","total"]

    _hints = [
        "Продажі БЕЗ Tax<br>event_type=Shipment, charge_type=Principal<br>(Tax не наші гроші — Amazon перераховує державі)",
        "FBA Fulfillment Fee<br>Referral Fee (комісія Amazon)<br>+ RefundFee",
        "Повернення коштів покупцям<br>Refund Principal",
        "Купони та знижки<br>Lightning Deals, Promotions",
        "Компенсації від Amazon<br>Lost/Damaged на складі<br>REVERSAL_REIMBURSEMENT · +$44K",
        "Чиста виплата<br>Gross − Fees − Refunds − Promos + Adjustments<br>Маржа ~54.5%",
    ]

    fig_wf = go.Figure(go.Waterfall(
        orientation="v", measure=measure, x=labels, y=values,
        base=0,
        text=[f"${abs(v):,.0f}" for v in values], textposition="outside",
        hovertext=[f"${abs(v):,.0f}<br>{h}" for v, h in zip(values, _hints)],
        hoverinfo="text",
        connector={"line": {"color": "rgba(128,128,128,0.3)", "width": 1}},
        increasing={"marker": {"color": "#4CAF50"}},
        decreasing={"marker": {"color": "#F44336"}},
        totals={"marker":    {"color": "#4472C4"}},
    ))
    fig_wf.update_layout(
        height=420, margin=dict(l=0,r=0,t=30,b=0),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        yaxis=dict(tickprefix="$", showgrid=True, gridcolor="rgba(128,128,128,0.1)"),
        showlegend=False,
    )
    st.plotly_chart(fig_wf, width="stretch")
    st.markdown("---")

    # ══════════════════════════════════════════
    # ТАБИ
    # ══════════════════════════════════════════
    tabs = st.tabs(["🏦 Settlements", "📊 Finance Events", "💳 Event Groups"])

    # ── TAB 1: Settlements ────────────────────
    with tabs[0]:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 📈 Тренд — net за день")
            try:
                with engine.connect() as conn:
                    daily = pd.read_sql(text("""
                        SELECT posted_date::date AS date,
                               SUM(NULLIF(amount,'')::numeric) AS net
                        FROM settlements
                        WHERE posted_date >= :d1 AND posted_date <= :d2
                        GROUP BY 1 ORDER BY 1
                    """), conn, params={"d1": d1, "d2": d2})
                fig = go.Figure(go.Bar(
                    x=daily['date'], y=daily['net'],
                    marker_color=daily['net'].apply(lambda x: '#4CAF50' if x >= 0 else '#F44336')
                ))
                fig.update_layout(height=360, margin=dict(l=0,r=0,t=10,b=0))
                st.plotly_chart(fig, width="stretch")
            except Exception as e:
                st.error(str(e))

        with col2:
            st.markdown("#### 💸 По типах amount_type")
            try:
                with engine.connect() as conn:
                    by_type = pd.read_sql(text("""
                        SELECT amount_type,
                               SUM(NULLIF(amount,'')::numeric) AS total
                        FROM settlements
                        WHERE posted_date >= :d1 AND posted_date <= :d2
                          AND amount_type IS NOT NULL AND amount_type != ''
                        GROUP BY 1 ORDER BY 2 DESC
                        LIMIT 15
                    """), conn, params={"d1": d1, "d2": d2})
                costs = by_type[by_type['total'] < 0].copy()
                income = by_type[by_type['total'] > 0].copy()
                if not costs.empty:
                    costs['total'] = costs['total'].abs()
                    fig2 = px.pie(costs, values='total', names='amount_type', hole=0.4,
                                  title="Витрати (fees/refunds)")
                    fig2.update_layout(height=360)
                    st.plotly_chart(fig2, width="stretch")
            except Exception as e:
                st.error(str(e))

        st.markdown("#### 📊 По transaction_type")
        try:
            with engine.connect() as conn:
                by_tt = pd.read_sql(text("""
                    SELECT transaction_type,
                           COUNT(*) AS cnt,
                           SUM(NULLIF(amount,'')::numeric) AS total
                    FROM settlements
                    WHERE posted_date >= :d1 AND posted_date <= :d2
                    GROUP BY 1 ORDER BY ABS(SUM(NULLIF(amount,'')::numeric)) DESC
                """), conn, params={"d1": d1, "d2": d2})
            st.dataframe(
                by_tt.style.format({'total': '${:,.2f}'}),
                width="stretch", hide_index=True
            )
        except Exception as e:
            st.error(str(e))

        st.markdown("#### 📋 Останні транзакції")
        try:
            with engine.connect() as conn:
                df_raw = pd.read_sql(text("""
                    SELECT posted_date, transaction_type, amount_type,
                           order_id, sku, amount, currency
                    FROM settlements
                    WHERE posted_date >= :d1 AND posted_date <= :d2
                    ORDER BY posted_date DESC LIMIT 500
                """), conn, params={"d1": d1, "d2": d2})
            st.dataframe(df_raw, width="stretch", hide_index=True, height=400)
            st.download_button("📥 CSV",
                df_raw.to_csv(index=False).encode(), "settlements.csv", "text/csv")
        except Exception as e:
            st.error(str(e))

        # Debug: показуємо топ transaction_type якщо net=0
        if net == 0:
            st.info("⚠️ Net Payout = $0 — перевір фільтр transaction_type нижче у таблиці")

    # ── TAB 2: Finance Events ─────────────────
    with tabs[1]:
        st.markdown("#### 📊 Finance Events — по типах подій")
        try:
            with engine.connect() as conn:
                ev_types = pd.read_sql(text("""
                    SELECT event_type,
                           COUNT(*)           AS cnt,
                           SUM(NULLIF(amount,'')::numeric) AS total
                    FROM finance_events
                    WHERE posted_date >= :d1 AND posted_date <= :d2
                    GROUP BY 1 ORDER BY ABS(SUM(NULLIF(amount,'')::numeric)) DESC
                    LIMIT 30
                """), conn, params={"d1": d1, "d2": d2})

            if not ev_types.empty:
                col1, col2 = st.columns(2)
                with col1:
                    fig = px.bar(ev_types.sort_values('total'),
                                 x='total', y='event_type', orientation='h',
                                 color='total', color_continuous_scale='RdYlGn',
                                 title="Сума по event_type")
                    fig.update_layout(height=max(400, len(ev_types)*30))
                    st.plotly_chart(fig, width="stretch")
                with col2:
                    st.dataframe(
                        ev_types.style.format({'total': '${:,.2f}'}),
                        width="stretch", hide_index=True
                    )
        except Exception as e:
            st.error(f"finance_events: {e}")

        st.markdown("#### 💸 По charge_type (комісії)")
        try:
            with engine.connect() as conn:
                charges = pd.read_sql(text("""
                    SELECT charge_type,
                           COUNT(*)             AS cnt,
                           SUM(NULLIF(amount,'')::numeric) AS total
                    FROM finance_events
                    WHERE posted_date >= :d1 AND posted_date <= :d2
                      AND charge_type IS NOT NULL AND charge_type != ''
                    GROUP BY 1 ORDER BY SUM(NULLIF(amount,'')::numeric) ASC
                    LIMIT 20
                """), conn, params={"d1": d1, "d2": d2})

            if not charges.empty:
                col1, col2 = st.columns(2)
                with col1:
                    colors = ['#F44336' if v < 0 else '#4CAF50' for v in charges['total']]
                    fig2 = go.Figure(go.Bar(
                        x=charges['total'], y=charges['charge_type'],
                        orientation='h', marker_color=colors,
                        text=[f"${v:,.0f}" for v in charges['total']],
                        textposition='outside'
                    ))
                    fig2.update_layout(height=max(400, len(charges)*35),
                                       title="Charge Types (витрати знизу)")
                    st.plotly_chart(fig2, width="stretch")
                with col2:
                    st.dataframe(
                        charges.style.format({'total': '${:,.2f}'}),
                        width="stretch", hide_index=True
                    )
        except Exception as e:
            st.info(f"charge_type: {e}")

        st.markdown("#### 📦 По SKU (топ витрат)")
        try:
            with engine.connect() as conn:
                by_sku = pd.read_sql(text("""
                    SELECT seller_sku AS sku,
                           COUNT(*)             AS cnt,
                           SUM(NULLIF(amount,'')::numeric) AS total
                    FROM finance_events
                    WHERE posted_date >= :d1 AND posted_date <= :d2
                      AND seller_sku IS NOT NULL AND seller_sku != ''
                    GROUP BY 1 ORDER BY SUM(NULLIF(amount,'')::numeric) ASC
                    LIMIT 20
                """), conn, params={"d1": d1, "d2": d2})
            if not by_sku.empty:
                st.dataframe(by_sku.style.format({'total': '${:,.2f}'}),
                             width="stretch", hide_index=True)
        except Exception:
            pass  # колонка може не існувати

    # ── TAB 3: Event Groups (settlement periods) ──
    with tabs[2]:
        st.markdown("#### 💳 Settlement Periods (Finance Event Groups)")
        try:
            with engine.connect() as conn:
                grp = pd.read_sql(text("""
                    SELECT * FROM finance_event_groups
                    ORDER BY fund_transfer_date DESC
                """), conn)
                for _c in grp.columns:
                    if any(x in _c.lower() for x in ['total','amount','converted']):
                        grp[_c] = pd.to_numeric(grp[_c].replace('', None), errors='coerce').fillna(0)
            if not grp.empty:
                c1, c2, c3 = st.columns(3)
                # знаходимо колонку з total
                _tot_col = next((c for c in grp.columns if 'total' in c.lower()), None)
                _tot_sum = grp[_tot_col].sum() if _tot_col else 0
                _tot_last = grp[_tot_col].iloc[0] if (_tot_col and len(grp)) else 0
                c1.metric("💰 Всього виплат",   f"${_tot_sum:,.0f}")
                c2.metric("📋 Periods",          f"{len(grp)}")
                c3.metric("💱 Остання виплата", f"${_tot_last:,.0f}")
                st.dataframe(grp, width="stretch", hide_index=True)
            else:
                st.info("Даних немає")
        except Exception as e:
            st.error(f"finance_event_groups: {e}")

    # ── AI Chat ──
    st.markdown("---")
    ctx = f"""Фінанси settlements:
Net Payout: ${net:,.0f} | Gross: ${gross:,.0f} | Refunds: ${refs:,.0f} | Fees: ${fees:,.0f}
Orders: {orders:,} | Транзакцій: {rows:,} | Період: {d1} → {d2}"""
    show_ai_chat(ctx, [
        "Який місяць приніс найбільший net payout?",
        "Яка частка комісій від gross sales?",
        "Де найвищі витрати по charge_type?",
    ], "settlements")


def insights_settlements_v2(net, gross, refs, fees):
    st.markdown("---")
    st.markdown("### 🧠 Автоматичні інсайти")
    margin_pct = net/gross*100 if gross > 0 else 0
    fee_pct    = abs(fees)/gross*100 if gross > 0 else 0
    ref_pct    = abs(refs)/gross*100 if gross > 0 else 0
    cols = st.columns(3)
    if margin_pct >= 30:   txt, em, col = f"Маржа <b>{margin_pct:.1f}%</b> — відмінно!", "🟢", "#0d2b1e"
    elif margin_pct >= 15: txt, em, col = f"Маржа <b>{margin_pct:.1f}%</b> — норма для FBA.", "🟡", "#2b2400"
    else:                  txt, em, col = f"Маржа <b>{margin_pct:.1f}%</b> — низько!", "🔴", "#2b0d0d"
    with cols[0]: insight_card(em, "Чиста маржа", txt, col)
    if fee_pct <= 30:   txt, em, col = f"Комісії <b>{fee_pct:.1f}%</b> — норма.", "🟢", "#0d2b1e"
    elif fee_pct <= 40: txt, em, col = f"Комісії <b>{fee_pct:.1f}%</b> — трохи багато.", "🟡", "#2b2400"
    else:               txt, em, col = f"Комісії <b>{fee_pct:.1f}%</b> — занадто!", "🔴", "#2b0d0d"
    with cols[1]: insight_card(em, "Навантаження комісій", txt, col)
    if ref_pct <= 3:   txt, em, col = f"Повернення <b>{ref_pct:.1f}%</b> — відмінно.", "🟢", "#0d2b1e"
    elif ref_pct <= 8: txt, em, col = f"Повернення <b>{ref_pct:.1f}%</b> — помірно.", "🟡", "#2b2400"
    else:              txt, em, col = f"Повернення <b>{ref_pct:.1f}%</b> — критично!", "🔴", "#2b0d0d"
    with cols[2]: insight_card(em, "Повернення", txt, col)


def show_returns(t=None):
    if t is None: t = translations.get("UA", {})

    st.markdown("### 📦 Повернення (Returns)")

    engine = get_engine()

    # ── читаємо реальні колонки ──
    try:
        with engine.connect() as conn:
            cols_df = pd.read_sql(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='fba_returns' ORDER BY ordinal_position"
            ), conn)
        real_cols = cols_df['column_name'].tolist()
    except Exception as e:
        st.error(f"Помилка читання схеми returns: {e}"); return

    if not real_cols:
        st.warning("⚠️ Таблиця returns порожня або не існує"); return

    # ── маппінг колонок (будь-який регістр) ──
    col_map = {}
    for c in real_cols:
        lc = c.lower().replace(' ', '_').replace('-', '_')
        if lc in ('return_date', 'returndate', 'date'):  col_map['date']     = c
        elif lc in ('sku', 'seller_sku', 'msku'):      col_map['sku']      = c
        elif lc in ('asin', 'fnsku'):                  col_map['asin']     = c
        elif lc in ('quantity', 'qty'):                col_map['qty']      = c
        elif lc in ('reason', 'return_reason', 'detailed_disposition'): col_map['reason'] = c
        elif lc in ('status', 'return_status', 'disposition'): col_map['status'] = c
        elif lc in ('order_id', 'orderid', 'amazon_order_id'): col_map['order_id'] = c
        elif lc in ('product_name', 'title', 'name', 'product_description'): col_map['name'] = c

    date_c  = col_map.get('date',  real_cols[0])
    sku_c   = col_map.get('sku',   None)
    qty_c   = col_map.get('qty',   None)
    reason_c = col_map.get('reason', None)
    status_c = col_map.get('status', None)

    # ── sidebar фільтри ──

    try:
        with engine.connect() as conn:
            bounds = pd.read_sql(text(
                f'SELECT MIN("{date_c}")::date as mn, MAX("{date_c}")::date as mx FROM fba_returns'
            ), conn).iloc[0]
        min_date = bounds['mn']
        max_date = bounds['mx']
    except Exception:
        min_date = dt.date(2024, 1, 1)
        max_date = dt.date.today()

    date_range = st.sidebar.date_input(
        t.get("ret_date", "📅 Дата повернення:"),
        value=(max_date - dt.timedelta(days=30), max_date),
        min_value=min_date, max_value=max_date, key="ret_date_range"
    )
    if len(date_range) != 2:
        st.warning("Оберіть діапазон дат"); return
    d1, d2 = str(date_range[0]), str(date_range[1])

    # ── завантажуємо дані ──
    try:
        with engine.connect() as conn:
            df_f = pd.read_sql(text(
                f'SELECT * FROM fba_returns '
                f'WHERE "{date_c}" >= :d1 AND "{date_c}" <= :d2 '
                f'ORDER BY "{date_c}" DESC LIMIT 10000'
            ), conn, params={"d1": d1, "d2": d2})
    except Exception as e:
        st.error(f"Помилка завантаження returns: {e}"); return

    if df_f.empty:
        st.info("Немає повернень за цей період"); return

    # нормалізуємо типи
    df_f[date_c] = pd.to_datetime(df_f[date_c], errors='coerce')
    if qty_c:
        df_f[qty_c] = pd.to_numeric(df_f[qty_c], errors='coerce').fillna(1)

    # ── Return Value через orders (item_price по SKU) ──
    try:
        with engine.connect() as conn:
            oc = pd.read_sql(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='orders' ORDER BY ordinal_position"
            ), conn)['column_name'].tolist()
            price_col = next((c for c in oc if c.lower() in ('item_price','item-price','itemprice','price')), None)
            sku_o_col = next((c for c in oc if c.lower() in ('sku','seller_sku')), None)
            if price_col and sku_o_col and sku_c:
                _sql = (f'SELECT "{sku_o_col}" as sku,'
                        f' AVG(NULLIF("{price_col}",\'\')::numeric) as price'
                        f' FROM orders WHERE NULLIF("{price_col}",\'\') IS NOT NULL GROUP BY 1')
                prices = pd.read_sql(text(_sql), conn)
                price_map = prices.set_index('sku')['price'].to_dict()
                df_f['_price'] = df_f[sku_c].map(price_map).fillna(0)
            else:
                df_f['_price'] = 0
    except Exception:
        df_f['_price'] = 0

    qty_vals = df_f[qty_c] if qty_c else pd.Series([1]*len(df_f))
    df_f['Return Value'] = df_f['_price'] * qty_vals

    # ── KPI ──
    total_ret   = len(df_f)
    total_val   = df_f['Return Value'].sum()
    unique_sku  = df_f[sku_c].nunique() if sku_c else 0
    avg_val     = df_f['Return Value'].mean()

    # return rate vs orders
    rr = 0
    try:
        with engine.connect() as conn:
            o_count = pd.read_sql(text(
                f'SELECT COUNT(DISTINCT "{next((c for c in oc if c.lower() in ("order_id","orderid")), oc[0])}") as cnt FROM orders'
            ), conn).iloc[0]['cnt']
            ret_orders = df_f[col_map['order_id']].nunique() if 'order_id' in col_map else 0
            rr = ret_orders / o_count * 100 if o_count > 0 else 0
    except Exception:
        pass

    rr_color = "#4CAF50" if rr <= 3 else "#FFC107" if rr <= 8 else "#F44336"
    st.markdown(f"""
<div style="background:linear-gradient(135deg,#2b1a1a,#1f0d0d);border:1px solid #4a2d2d;
            border-radius:12px;padding:20px 28px;margin-bottom:16px;
            display:flex;align-items:center;gap:32px;flex-wrap:wrap">
  <div>
    <div style="font-size:12px;color:#888;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">
      📦 Повернення за період
    </div>
    <div style="font-size:48px;font-weight:900;color:{rr_color};font-family:monospace;line-height:1">
      {rr:.1f}%
    </div>
    <div style="font-size:12px;color:#666;margin-top:6px">{d1} → {d2} · {total_ret:,} повернень</div>
  </div>
  <div style="flex:1;min-width:200px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <span style="background:#2b1a1a;border:1px solid #4a2d2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📦 Всього <b style="color:#F44336">{total_ret:,}</b>
    </span>
    <span style="background:#1a2b2e;border:1px solid #2d404a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📦 SKU <b style="color:#5B9BD5">{unique_sku:,}</b>
    </span>
    <span style="background:#2b2b1a;border:1px solid #4a4a2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      💰 Вартість <b style="color:#FFC107">{_fmt(total_val)}</b>
    </span>
    <span style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      💵 Avg <b style="color:#AB47BC">${avg_val:.2f}</b>
    </span>
  </div>
</div>""", unsafe_allow_html=True)

    # ── Інсайти ──
    df_for_insights = df_f.copy()
    if sku_c and sku_c != 'SKU':       df_for_insights['SKU']    = df_for_insights[sku_c]
    if reason_c and reason_c != 'Reason': df_for_insights['Reason'] = df_for_insights[reason_c]
    if 'Return Value' not in df_for_insights.columns: df_for_insights['Return Value'] = 0
    insights_returns(df_for_insights, rr)

    st.markdown("---")

    # ── Графіки ──
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("#### 📅 Тренд повернень по днях")
        daily = df_f.groupby(df_f[date_c].dt.date).size().reset_index()
        daily.columns = ['Date', 'Count']
        fig = px.bar(daily, x='Date', y='Count', color_discrete_sequence=['#FF6B6B'])
        fig.update_layout(height=320, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig, width="stretch")

    with col2:
        if reason_c and reason_c in df_f.columns:
            st.markdown("#### 🔍 Причини повернень")
            rc = df_f[reason_c].value_counts().head(8).reset_index()
            rc.columns = ['Reason', 'Count']
            fig2 = px.pie(rc, values='Count', names='Reason', hole=0.4,
                          color_discrete_sequence=px.colors.sequential.RdBu)
            fig2.update_layout(height=320)
            st.plotly_chart(fig2, width="stretch")
        else:
            st.info("Колонка reason відсутня")

    with col3:
        if sku_c:
            st.markdown("#### 🏆 Топ SKU за кількістю")
            top_sku = df_f[sku_c].value_counts().head(10).reset_index()
            top_sku.columns = ['SKU', 'Returns']
            fig3 = px.bar(top_sku, x='Returns', y='SKU', orientation='h',
                          color='Returns', color_continuous_scale='Oranges')
            fig3.update_layout(height=320, yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig3, width="stretch")

    st.markdown("---")

    # ── Return Value charts (якщо є ціни) ──
    if total_val > 0:
        col1, col2 = st.columns(2)
        with col1:
            if sku_c:
                st.markdown("#### 💰 Топ SKU за вартістю повернень")
                tv = df_f.groupby(sku_c)['Return Value'].sum().nlargest(10).reset_index()
                fig4 = px.bar(tv, x='Return Value', y=sku_c, orientation='h',
                              color='Return Value', color_continuous_scale='Reds')
                fig4.update_traces(texttemplate='$%{x:,.0f}', textposition='outside')
                fig4.update_layout(height=350, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig4, width="stretch")
        with col2:
            if reason_c:
                st.markdown("#### 💸 Вартість по причинах")
                rv = df_f.groupby(reason_c)['Return Value'].sum().nlargest(8).reset_index()
                fig5 = px.pie(rv, values='Return Value', names=reason_c, hole=0.4)
                fig5.update_layout(height=350)
                st.plotly_chart(fig5, width="stretch")
        st.markdown("---")

    # ── Таблиця ──
    st.markdown("---")
    st.markdown("#### 📋 Деталі повернень")
    show_cols = [c for c in [date_c, sku_c, col_map.get('asin'), qty_c,
                              reason_c, status_c, col_map.get('order_id'), '_price', 'Return Value']
                 if c and c in df_f.columns]
    rename_map = {date_c:'Return Date', sku_c:'SKU', qty_c:'Qty',
                  reason_c:'Reason', status_c:'Status', '_price':'Price',
                  col_map.get('order_id'):'Order ID', col_map.get('asin'):'ASIN'}
    df_show = df_f[show_cols].rename(columns=rename_map).sort_values('Return Date', ascending=False).head(500)
    fmt = {}
    if 'Price' in df_show.columns:        fmt['Price']        = '${:.2f}'
    if 'Return Value' in df_show.columns: fmt['Return Value']  = '${:.2f}'
    st.dataframe(df_show.style.format(fmt) if fmt else df_show,
                 width="stretch", hide_index=True, height=400)
    st.caption(f"Показано {min(500,len(df_f)):,} з {len(df_f):,} повернень")
    st.download_button("📥 CSV", df_f.to_csv(index=False).encode(), "returns.csv", "text/csv")

    ctx_ret = f"""Returns: {total_ret} повернень | Rate {rr:.1f}% | Вартість ${total_val:,.0f} | Період {d1}→{d2}"""
    show_ai_chat(ctx_ret, [
        "Які SKU мають найбільше повернень за цей період?",
        "Які топ-3 причини повернень?",
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


def show_orders(t=None):
    if t is None: t = translations.get("UA", {})
    df_orders = load_orders()
    if df_orders.empty: st.warning("⚠️ No orders data."); return

    min_date = df_orders['Order Date'].min().date()
    max_date = df_orders['Order Date'].max().date()
    date_range = st.sidebar.date_input(
        t["st_date_range"], value=(min_date, max_date),
        min_value=min_date, max_value=max_date, key="ord_date"
    )
    df_f = df_orders[
        (df_orders['Order Date'].dt.date >= date_range[0]) &
        (df_orders['Order Date'].dt.date <= date_range[1])
    ] if len(date_range) == 2 else df_orders

    total_orders = df_f['Order ID'].nunique()
    total_rev    = df_f['Total Price'].sum()
    total_items  = int(df_f['Quantity'].sum())
    avg_order    = total_rev / total_orders if total_orders > 0 else 0
    days_span    = max((df_f['Order Date'].max() - df_f['Order Date'].min()).days, 1)
    rev_per_day  = total_rev / days_span
    d1 = str(date_range[0]) if len(date_range)==2 else str(min_date)
    d2 = str(date_range[1]) if len(date_range)==2 else str(max_date)

    # ── Hero Card ──
    st.markdown(f"""
<div style="background:linear-gradient(135deg,#1a2b1e,#0d1f12);border:1px solid #2d4a30;
            border-radius:12px;padding:20px 28px;margin-bottom:16px;
            display:flex;align-items:center;gap:32px;flex-wrap:wrap">
  <div>
    <div style="font-size:12px;color:#888;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">
      🛒 Виручка за період
    </div>
    <div style="font-size:48px;font-weight:900;color:#4CAF50;font-family:monospace;line-height:1">
      {_fmt(total_rev)}
    </div>
    <div style="font-size:12px;color:#666;margin-top:6px">{d1} → {d2}</div>
  </div>
  <div style="flex:1;min-width:200px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <span style="background:#1e2e1e;border:1px solid #2d4a30;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📦 Замовлень <b style="color:#4CAF50">{total_orders:,}</b>
    </span>
    <span style="background:#1a2b2e;border:1px solid #2d404a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📋 Одиниць <b style="color:#5B9BD5">{total_items:,}</b>
    </span>
    <span style="background:#2b2b1a;border:1px solid #4a4a2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      💵 Avg Order <b style="color:#FFC107">${avg_order:.2f}</b>
    </span>
    <span style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📈 /день <b style="color:#AB47BC">{_fmt(rev_per_day)}</b>
    </span>
  </div>
</div>""", unsafe_allow_html=True)

    # ── Інсайти ──
    _icols = st.columns(3)
    with _icols[0]: insight_card("🛒", "Середній чек",
        f"<b>${avg_order:.2f}</b> — +10% AOV = +{_fmt(total_rev*0.1)}", "#1a1a2e")
    with _icols[1]: insight_card("📈", "Дохід/день",
        f"<b>{_fmt(rev_per_day)}</b>/день · прогноз місяць: <b>{_fmt(rev_per_day*30)}</b>", "#1a2b1e")
    top_sku_rev = df_f.groupby('SKU')['Total Price'].sum().nlargest(1)
    if not top_sku_rev.empty:
        pct = top_sku_rev.iloc[0]/total_rev*100 if total_rev > 0 else 0
        with _icols[2]: insight_card("⚡", "Топ SKU",
            f"<b>{top_sku_rev.index[0]}</b> = {_fmt(top_sku_rev.iloc[0])} ({pct:.0f}%)", "#2b1a00")

    st.markdown("---")
    insights_orders(df_f)

    st.markdown("---")
    # ── Тренд ──
    st.markdown("#### 📈 Тренд виручки по днях")
    daily = df_f.groupby(df_f['Order Date'].dt.date)['Total Price'].sum().reset_index()
    daily.columns = ['Date', 'Revenue']
    fig = go.Figure(go.Bar(
        x=daily['Date'], y=daily['Revenue'],
        marker_color='#4CAF50', opacity=0.85,
        text=[_fmt(v) for v in daily['Revenue']], textposition='outside'
    ))
    fig.update_layout(height=320, margin=dict(l=0,r=0,t=10,b=0),
                      yaxis=dict(tickprefix="$"))
    st.plotly_chart(fig, width="stretch")

    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 🏆 Топ 15 SKU за виручкою")
        ts = df_f.groupby('SKU')['Total Price'].sum().nlargest(15).reset_index()
        fig2 = px.bar(ts, x='Total Price', y='SKU', orientation='h',
                      color='Total Price', color_continuous_scale='Greens',
                      text=[_fmt(v) for v in ts['Total Price']])
        fig2.update_layout(yaxis={'categoryorder':'total ascending'}, height=450)
        fig2.update_traces(textposition='outside')
        st.plotly_chart(fig2, width="stretch")
    with col2:
        if 'Order Status' in df_f.columns:
            st.markdown("#### 📊 Order Status")
            sc = df_f['Order Status'].value_counts().reset_index()
            sc.columns = ['Status', 'Count']
            fig3 = px.pie(sc, values='Count', names='Status', hole=0.4)
            fig3.update_layout(height=450)
            st.plotly_chart(fig3, width="stretch")

    # ── Таблиця ──
    st.markdown("---")
    st.markdown("#### 📋 Деталі замовлень")
    table_cols = [c for c in ['Order Date','Order ID','SKU','Item Price','Quantity','Total Price','Order Status','Ship Country'] if c in df_f.columns]
    st.dataframe(
        df_f[table_cols].sort_values('Order Date', ascending=False).head(500)
            .style.format({'Item Price':'${:.2f}','Total Price':'${:.2f}'}),
        width="stretch", hide_index=True, height=400
    )
    st.caption(f"Показано {min(500,len(df_f)):,} з {len(df_f):,} замовлень")
    st.download_button("📥 CSV", df_f[table_cols].to_csv(index=False).encode(), "orders.csv", "text/csv")

    ctx_ord = f"""Orders: {total_orders:,} замовлень | Revenue {_fmt(total_rev)} | Avg {avg_order:.2f} | /день {_fmt(rev_per_day)}"""
    show_ai_chat(ctx_ord, [
        "Топ 5 SKU за кількістю замовлень за останні 30 днів",
        "Порівняй обсяг замовлень: цей тиждень vs минулий",
        "Які SKU не мали замовлень більше 14 днів?",
    ], "orders")


# ============================================
# SCRAPER MANAGER
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

    apify_token = apify_token or os.getenv("APIFY_TOKEN", "")
    endpoint = (
        "https://api.apify.com/v2/acts/junglee~amazon-reviews-scraper"
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
            if not url.startswith("http"):
                url = "https://" + url
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
                            url_new     += ins
                            cycle_total += ins
                            log_q.put(f"  ✅ {star_num}★: отримано {len(data)}, нових: {ins}")
                        else:
                            log_q.put(f"  ⚠️ {star_num}★: відгуків не знайдено")
                    else:
                        try: err = res.json()
                        except: err = res.text[:200]
                        log_q.put(f"  ❌ {star_num}★: HTTP {res.status_code} → {err}")
                except Exception as e:
                    log_q.put(f"  ❌ {star_num}★: {e}")
                time.sleep(2)

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


def show_listings():
    st.markdown("### 📝 Листинги (Listings)")
    engine = get_engine()

    # ── Sidebar фільтри ──
    search_q    = st.sidebar.text_input("🔍 SKU / ASIN / Назва", "", key="lst_search")
    sel_mp      = st.sidebar.selectbox("🌍 Marketplace:", ["Всі", "Amazon.com", "Amazon.ca", "Amazon.de", "Amazon.co.uk"], key="lst_mp")
    sel_fc      = st.sidebar.selectbox("📦 Fulfillment:", ["Всі", "FBA (AMAZON_NA)", "FBM (DEFAULT)"], key="lst_fc")
    sel_status  = st.sidebar.selectbox("📊 Статус:", ["Всі", "Active", "Inactive"], key="lst_status")

    # ── Завантаження ──
    try:
        with engine.connect() as conn:
            df_all  = pd.read_sql(text("SELECT * FROM listings_all"),      conn)
            df_cat  = pd.read_sql(text("SELECT asin, brand, main_image_url, sales_rank, sales_rank_category, color, size, product_type FROM catalog_items"), conn)
            cnt_open = pd.read_sql(text("SELECT COUNT(*) as cnt FROM listings_open"),     conn).iloc[0]['cnt']
            cnt_inact= pd.read_sql(text("SELECT COUNT(*) as cnt FROM listings_inactive"), conn).iloc[0]['cnt']
    except Exception as e:
        st.error(f"Помилка: {e}"); return

    if df_all.empty:
        st.warning("listings_all порожня"); return

    # нормалізуємо
    df_all['price']    = pd.to_numeric(df_all['price'].replace('', None), errors='coerce').fillna(0)
    df_all['quantity'] = pd.to_numeric(df_all['quantity'].replace('', None), errors='coerce').fillna(0)
    df_all['open_date']= pd.to_datetime(df_all['open_date'], errors='coerce')

    # join з catalog
    df = df_all.merge(df_cat, left_on='asin1', right_on='asin', how='left')

    # фільтри
    df_f = df.copy()
    if search_q:
        mask = (df_f['seller_sku'].astype(str).str.contains(search_q, case=False, na=False) |
                df_f['asin1'].astype(str).str.contains(search_q, case=False, na=False) |
                df_f['item_name'].astype(str).str.contains(search_q, case=False, na=False))
        df_f = df_f[mask]
    if sel_mp != "Всі":
        df_f = df_f[df_f['marketplace'].astype(str).str.contains(sel_mp.replace("Amazon.",""), case=False, na=False)]
    if sel_fc == "FBA (AMAZON_NA)":
        df_f = df_f[df_f['fulfillment_channel'].astype(str).str.contains("AMAZON", case=False, na=False)]
    elif sel_fc == "FBM (DEFAULT)":
        df_f = df_f[~df_f['fulfillment_channel'].astype(str).str.contains("AMAZON", case=False, na=False)]
    if sel_status == "Active":
        df_f = df_f[df_f['status'].astype(str).str.lower() == 'active']
    elif sel_status == "Inactive":
        df_f = df_f[df_f['status'].astype(str).str.lower() != 'active']

    # KPI
    total       = len(df_f)
    active_cnt  = int((df_f['status'].astype(str).str.lower() == 'active').sum())
    inactive_cnt= total - active_cnt
    avg_price   = df_f[df_f['price'] > 0]['price'].mean()
    fba_cnt     = int(df_f['fulfillment_channel'].astype(str).str.contains("AMAZON", case=False, na=False).sum())
    fbm_cnt     = total - fba_cnt
    unique_asins= df_f['asin1'].nunique()

    # ── Hero Card ──
    st.markdown(f"""
<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border:1px solid #2d2d4a;
            border-radius:12px;padding:20px 28px;margin-bottom:16px;
            display:flex;align-items:center;gap:32px;flex-wrap:wrap">
  <div>
    <div style="font-size:12px;color:#888;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">
      📝 Каталог листингів
    </div>
    <div style="font-size:48px;font-weight:900;color:#5B9BD5;font-family:monospace;line-height:1">
      {total:,}
    </div>
    <div style="font-size:12px;color:#666;margin-top:6px">{unique_asins} унікальних ASIN · avg ${avg_price:.2f}</div>
  </div>
  <div style="flex:1;min-width:200px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <span style="background:#1e2e1e;border:1px solid #2d4a30;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      ✅ Active <b style="color:#4CAF50">{active_cnt:,}</b>
    </span>
    <span style="background:#2b1a1a;border:1px solid #4a2d2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      ❌ Inactive <b style="color:#F44336">{inactive_cnt:,}</b>
    </span>
    <span style="background:#1a2b2e;border:1px solid #2d404a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📦 FBA <b style="color:#5B9BD5">{fba_cnt:,}</b>
    </span>
    <span style="background:#2b2b1a;border:1px solid #4a4a2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🏪 FBM <b style="color:#FFC107">{fbm_cnt:,}</b>
    </span>
  </div>
</div>""", unsafe_allow_html=True)

    # ── Інсайти ──
    _icols = st.columns(3)
    active_pct = active_cnt/total*100 if total > 0 else 0
    if active_pct >= 80:   _em, _col = "🟢", "#0d2b1e"
    elif active_pct >= 50: _em, _col = "🟡", "#2b2400"
    else:                  _em, _col = "🔴", "#2b0d0d"
    with _icols[0]: insight_card(_em, "Активних листингів",
        f"<b>{active_cnt}</b> з {total} ({active_pct:.0f}%) активні", _col)
    fba_pct = fba_cnt/total*100 if total > 0 else 0
    with _icols[1]: insight_card("📦", "FBA vs FBM",
        f"FBA: <b>{fba_cnt}</b> ({fba_pct:.0f}%) · FBM: <b>{fbm_cnt}</b> ({100-fba_pct:.0f}%)", "#1a1a2e")
    with _icols[2]: insight_card("💰", "Середня ціна",
        f"Avg: <b>${avg_price:.2f}</b> · {total} SKU в каталозі", "#1e293b")

    st.markdown("---")

    # ── Чарти ──
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 📊 По маркетплейсах")
        mp_cnt = df_f['marketplace'].value_counts().reset_index()
        mp_cnt.columns = ['Marketplace', 'Count']
        fig = px.pie(mp_cnt, values='Count', names='Marketplace', hole=0.4,
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=320)
        st.plotly_chart(fig, width="stretch")

    with col2:
        st.markdown("#### 📊 Active vs Inactive по marketplace")
        status_mp = df_f.groupby(['marketplace','status']).size().reset_index(name='cnt')
        fig2 = px.bar(status_mp, x='marketplace', y='cnt', color='status',
                      color_discrete_map={'Active':'#4CAF50','Inactive':'#F44336'},
                      barmode='stack', height=320)
        fig2.update_layout(margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig2, width="stretch")

    # ── Ціновий розподіл ──
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 💰 Розподіл цін")
        df_prices = df_f[df_f['price'] > 0]
        if not df_prices.empty:
            fig3 = px.histogram(df_prices, x='price', nbins=30,
                                color_discrete_sequence=['#5B9BD5'], height=300)
            fig3.update_layout(margin=dict(l=0,r=0,t=10,b=0), xaxis_title="Ціна $")
            st.plotly_chart(fig3, width="stretch")

    with col2:
        # BSR якщо є дані, інакше Size
        df_f['sales_rank'] = pd.to_numeric(df_f.get('sales_rank', None), errors='coerce')
        df_bsr = df_f[df_f['sales_rank'].notna() & (df_f['sales_rank'] > 0)] if 'sales_rank' in df_f.columns else pd.DataFrame()

        if not df_bsr.empty:
            st.markdown("#### 📈 BSR топ 15 (найкращий rank)")
            top_bsr = df_bsr.nsmallest(15, 'sales_rank')
            fig4 = go.Figure(go.Bar(
                x=top_bsr['sales_rank'], y=top_bsr['seller_sku'], orientation='h',
                marker_color='#4CAF50',
                text=[f"#{int(v):,}" for v in top_bsr['sales_rank']], textposition='outside'
            ))
            fig4.update_layout(height=380, yaxis={'categoryorder':'total descending'},
                               xaxis_title="BSR (менше = краще)",
                               margin=dict(l=0,r=80,t=10,b=0))
            st.plotly_chart(fig4, width="stretch")
        elif 'size' in df_f.columns and df_f['size'].notna().any():
            st.markdown("#### 📏 По розміру (Size)")
            sz = df_f['size'].value_counts().head(12).reset_index()
            sz.columns = ['Size', 'Count']
            colors_sz = px.colors.qualitative.Set2[:len(sz)]
            fig4 = go.Figure(go.Bar(
                x=sz['Count'], y=sz['Size'], orientation='h',
                marker_color=colors_sz,
                text=sz['Count'], textposition='outside'
            ))
            fig4.update_layout(height=380, yaxis={'categoryorder':'total ascending'},
                               margin=dict(l=0,r=40,t=10,b=0))
            st.plotly_chart(fig4, width="stretch")
        else:
            st.markdown("#### 📦 По Fulfillment Channel")
            fc_cnt = df_f['fulfillment_channel'].value_counts().reset_index()
            fc_cnt.columns = ['FC', 'Count']
            fig4 = px.pie(fc_cnt, values='Count', names='FC', hole=0.4, height=380)
            st.plotly_chart(fig4, width="stretch")

    # ── Таблиця ──
    st.markdown("---")
    st.markdown("#### 📋 Каталог листингів")

    # вибираємо що показати
    show_cols = ['seller_sku','asin1','item_name','price','quantity','status',
                 'fulfillment_channel','marketplace','open_date']
    if 'brand' in df_f.columns:        show_cols.append('brand')
    if 'size' in df_f.columns:         show_cols.append('size')
    if 'color' in df_f.columns:        show_cols.append('color')
    if 'sales_rank' in df_f.columns:   show_cols.append('sales_rank')
    if 'main_image_url' in df_f.columns: show_cols.append('main_image_url')

    show_cols = [c for c in show_cols if c in df_f.columns]
    df_show = df_f[show_cols].sort_values('price', ascending=False).head(500).reset_index(drop=True)

    col_cfg = {
        'seller_sku':       st.column_config.TextColumn("SKU"),
        'asin1':            st.column_config.TextColumn("ASIN"),
        'item_name':        st.column_config.TextColumn("Назва", width="large"),
        'price':            st.column_config.NumberColumn("Ціна $", format="$%.2f"),
        'quantity':         st.column_config.NumberColumn("Qty"),
        'status':           st.column_config.TextColumn("Статус"),
        'fulfillment_channel': st.column_config.TextColumn("FC"),
        'marketplace':      st.column_config.TextColumn("MP"),
        'open_date':        st.column_config.DatetimeColumn("Open Date", format="YYYY-MM-DD"),
        'brand':            st.column_config.TextColumn("Brand"),
        'size':             st.column_config.TextColumn("Size"),
        'color':            st.column_config.TextColumn("Color"),
        'sales_rank':       st.column_config.NumberColumn("BSR", format="%d"),
        'main_image_url':   st.column_config.ImageColumn("Фото", width="small"),
    }

    st.dataframe(df_show, column_config=col_cfg,
                 width="stretch", hide_index=True, height=500)
    st.caption(f"Показано {len(df_show):,} з {len(df_f):,} листингів")
    st.download_button("📥 CSV", df_f[show_cols].to_csv(index=False).encode(), "listings.csv", "text/csv")

    # ── AI ──
    ctx = f"""Listings: {total} SKU | Active: {active_cnt} | Inactive: {inactive_cnt} | FBA: {fba_cnt} | Avg price: ${avg_price:.2f}"""
    show_ai_chat(ctx, [
        "Які SKU inactive більше 90 днів — можна деактивувати?",
        "Де найвищий BSR (найкращий rank)?",
        "Які FBM листинги варто перевести на FBA?",
    ], "listings")


def show_pricing():
    st.markdown("### 💲 Pricing / Buy Box")
    engine = get_engine()

    # ── Завантаження ──
    try:
        with engine.connect() as conn:
            df_curr  = pd.read_sql(text("SELECT * FROM pricing_current ORDER BY snapshot_time DESC"), conn)
            df_bb    = pd.read_sql(text("SELECT * FROM pricing_buybox ORDER BY snapshot_time DESC"), conn)
            df_comp  = pd.read_sql(text("SELECT * FROM pricing_competitive ORDER BY snapshot_time DESC"), conn)
            df_off   = pd.read_sql(text("SELECT * FROM pricing_offers ORDER BY snapshot_time DESC"), conn)
    except Exception as e:
        st.error(f"Помилка: {e}"); return

    # нормалізуємо числові
    for df, cols in [
        (df_curr,  ['listing_price','landed_price','shipping_price','regular_price','business_price']),
        (df_bb,    ['price']),
        (df_comp,  ['listing_price','landed_price','shipping']),
        (df_off,   ['price','listing_price','shipping']),
    ]:
        for c in cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c].replace('', None), errors='coerce').fillna(0)

    # is_buybox_winner → bool
    for df in [df_bb, df_off]:
        if 'is_buybox_winner' in df.columns:
            df['is_buybox_winner'] = df['is_buybox_winner'].astype(str).str.lower().isin(['true','1','yes'])

    # ── KPI ──
    total_asins   = df_curr['asin'].nunique() if not df_curr.empty else 0
    bb_winner_cnt = int(df_bb['is_buybox_winner'].sum()) if not df_bb.empty else 0
    bb_total      = len(df_bb)
    bb_pct        = bb_winner_cnt / bb_total * 100 if bb_total > 0 else 0
    avg_price     = df_curr['listing_price'].mean() if not df_curr.empty else 0
    snap_time     = str(df_curr['snapshot_time'].max())[:16] if not df_curr.empty else "—"

    bb_color = "#4CAF50" if bb_pct >= 80 else "#FFC107" if bb_pct >= 50 else "#F44336"
    st.markdown(f"""
<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border:1px solid #2d2d4a;
            border-radius:12px;padding:20px 28px;margin-bottom:16px;
            display:flex;align-items:center;gap:32px;flex-wrap:wrap">
  <div>
    <div style="font-size:12px;color:#888;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">
      🏆 Buy Box Win Rate
    </div>
    <div style="font-size:48px;font-weight:900;color:{bb_color};font-family:monospace;line-height:1">
      {bb_pct:.0f}%
    </div>
    <div style="font-size:12px;color:#666;margin-top:6px">Snapshot: {snap_time}</div>
  </div>
  <div style="flex:1;min-width:200px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <span style="background:#1e2e1e;border:1px solid #2d4a30;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🏆 BB Winners <b style="color:#4CAF50">{bb_winner_cnt}</b> / {bb_total}
    </span>
    <span style="background:#1a2b2e;border:1px solid #2d404a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📦 ASINs <b style="color:#5B9BD5">{total_asins}</b>
    </span>
    <span style="background:#2b2b1a;border:1px solid #4a4a2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      💰 Avg Price <b style="color:#FFC107">${avg_price:.2f}</b>
    </span>
    <span style="background:#2b1a1a;border:1px solid #4a2d2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      ❌ BB Lost <b style="color:#F44336">{bb_total - bb_winner_cnt}</b>
    </span>
  </div>
</div>""", unsafe_allow_html=True)

    # ── Інсайти ──
    _ic = st.columns(3)
    if bb_pct >= 80:   _em, _col = "🟢", "#0d2b1e"
    elif bb_pct >= 50: _em, _col = "🟡", "#2b2400"
    else:              _em, _col = "🔴", "#2b0d0d"
    with _ic[0]: insight_card(_em, "Buy Box Win Rate",
        f"Виграємо Buy Box у <b>{bb_pct:.0f}%</b> ({bb_winner_cnt}/{bb_total} ASIN)", _col)

    lost_bb = df_bb[~df_bb['is_buybox_winner']] if not df_bb.empty else pd.DataFrame()
    with _ic[1]: insight_card("⚠️", "Buy Box програш",
        f"<b>{len(lost_bb)} ASIN</b> без Buy Box — перевір ціну та репрайсер", "#2b2400" if len(lost_bb) < 5 else "#2b0d0d")

    # Різниця нашої ціни vs competitor
    if not df_comp.empty and not df_curr.empty:
        merged = df_curr.merge(df_comp[['asin','listing_price']].rename(columns={'listing_price':'comp_price'}),
                               on='asin', how='inner')
        if not merged.empty:
            merged['price_diff'] = merged['listing_price'] - merged['comp_price']
            over_cnt = int((merged['price_diff'] > 1).sum())
            with _ic[2]: insight_card("💸", "Вища за конкурента",
                f"<b>{over_cnt} ASIN</b> — наша ціна вища за competitive price на $1+", "#2b2400" if over_cnt > 0 else "#0d2b1e")
        else:
            with _ic[2]: insight_card("💰", "Competitive", "Немає даних для порівняння", "#1e293b")
    else:
        with _ic[2]: insight_card("💰", "Avg Listing Price", f"<b>${avg_price:.2f}</b>", "#1e293b")

    st.markdown("---")

    # ── ТАБИ ──
    tabs = st.tabs(["🏆 Buy Box", "💰 Current Prices", "📊 Competitive", "📋 All Offers"])

    # ── TAB 1: Buy Box ──
    with tabs[0]:
        if df_bb.empty:
            st.info("Немає даних pricing_buybox")
        else:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 🏆 Buy Box статус по ASIN")
                bb_status = df_bb.groupby('asin').agg(
                    is_winner=('is_buybox_winner','max'),
                    price=('price','mean'),
                    fulfillment=('fulfillment','first')
                ).reset_index()
                bb_status['Статус'] = bb_status['is_winner'].map({True:'✅ Winner', False:'❌ Lost'})
                bb_status = bb_status.sort_values('is_winner', ascending=False)

                colors_bb = ['#4CAF50' if w else '#F44336' for w in bb_status['is_winner']]
                fig = go.Figure(go.Bar(
                    x=bb_status['price'], y=bb_status['asin'], orientation='h',
                    marker_color=colors_bb,
                    text=[f"${v:.2f} {'✅' if w else '❌'}" for v,w in zip(bb_status['price'], bb_status['is_winner'])],
                    textposition='outside'
                ))
                fig.update_layout(height=max(350, len(bb_status)*35),
                                  xaxis_title="Ціна $",
                                  yaxis={'categoryorder':'total ascending'},
                                  margin=dict(l=0,r=80,t=10,b=0))
                st.plotly_chart(fig, width="stretch")

            with col2:
                st.markdown("#### 📊 Buy Box по Fulfillment")
                bb_fc = df_bb.groupby(['fulfillment','is_buybox_winner']).size().reset_index(name='cnt')
                bb_fc['label'] = bb_fc['is_buybox_winner'].map({True:'Winner',False:'Lost'})
                fig2 = px.bar(bb_fc, x='fulfillment', y='cnt', color='label',
                              color_discrete_map={'Winner':'#4CAF50','Lost':'#F44336'},
                              barmode='stack', height=380)
                st.plotly_chart(fig2, width="stretch")

            st.markdown("#### 📋 Buy Box деталі")
            st.dataframe(
                bb_status.rename(columns={'asin':'ASIN','price':'Price','fulfillment':'FC','Статус':'BB Status'})
                    .style.format({'Price':'${:.2f}'}),
                width="stretch", hide_index=True
            )
            st.download_button("📥 CSV", df_bb.to_csv(index=False).encode(), "buybox.csv", "text/csv")

    # ── TAB 2: Current Prices ──
    with tabs[1]:
        if df_curr.empty:
            st.info("Немає даних pricing_current")
        else:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 💰 Listing Price по ASIN")
                pc = df_curr[df_curr['listing_price'] > 0].copy()
                pc = pc.merge(df_bb[['asin','is_buybox_winner']].drop_duplicates('asin'), on='asin', how='left')
                pc['is_buybox_winner'] = pc['is_buybox_winner'].fillna(False)
                colors_p = ['#4CAF50' if w else '#5B9BD5' for w in pc['is_buybox_winner']]
                fig3 = go.Figure(go.Bar(
                    x=pc['listing_price'], y=pc['asin'], orientation='h',
                    marker_color=colors_p,
                    text=[f"${v:.2f}" for v in pc['listing_price']], textposition='outside'
                ))
                fig3.update_layout(height=max(350, len(pc)*30),
                                   yaxis={'categoryorder':'total ascending'},
                                   margin=dict(l=0,r=60,t=10,b=0))
                st.plotly_chart(fig3, width="stretch")
                st.caption("🟢 = Buy Box Winner")

            with col2:
                st.markdown("#### 💰 Regular vs Business Price")
                pr = df_curr[(df_curr['regular_price'] > 0) | (df_curr['business_price'] > 0)].copy()
                if not pr.empty:
                    fig4 = go.Figure()
                    fig4.add_trace(go.Bar(name='Regular', x=pr['asin'], y=pr['regular_price'], marker_color='#5B9BD5'))
                    fig4.add_trace(go.Bar(name='Business', x=pr['asin'], y=pr['business_price'], marker_color='#AB47BC'))
                    fig4.update_layout(barmode='group', height=380, xaxis_tickangle=-45)
                    st.plotly_chart(fig4, width="stretch")
                else:
                    st.info("Regular/Business price відсутні")

            show_c = [c for c in ['asin','sku','listing_price','landed_price','regular_price','business_price','fulfillment','marketplace','status'] if c in df_curr.columns]
            st.dataframe(df_curr[show_c].style.format({c:'${:.2f}' for c in ['listing_price','landed_price','regular_price','business_price'] if c in df_curr.columns}),
                         width="stretch", hide_index=True)
            st.download_button("📥 CSV", df_curr.to_csv(index=False).encode(), "pricing_current.csv", "text/csv")

    # ── TAB 3: Competitive ──
    with tabs[2]:
        if df_comp.empty:
            st.info("Немає даних pricing_competitive")
        else:
            # join наша ціна vs competitive
            merged_comp = df_curr[['asin','listing_price']].merge(
                df_comp[['asin','listing_price','landed_price','number_of_offers']].rename(
                    columns={'listing_price':'comp_price','landed_price':'comp_landed'}),
                on='asin', how='outer'
            )
            merged_comp['our_price']  = merged_comp['listing_price'].fillna(0)
            merged_comp['comp_price'] = merged_comp['comp_price'].fillna(0)
            merged_comp['diff']       = merged_comp['our_price'] - merged_comp['comp_price']
            merged_comp['diff_pct']   = (merged_comp['diff'] / merged_comp['comp_price'] * 100).fillna(0)

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 💸 Наша ціна vs Competitive")
                mc = merged_comp[merged_comp['our_price'] > 0].copy()
                fig5 = go.Figure()
                fig5.add_trace(go.Bar(name='Our Price',  x=mc['asin'], y=mc['our_price'],  marker_color='#5B9BD5'))
                fig5.add_trace(go.Bar(name='Competitor', x=mc['asin'], y=mc['comp_price'], marker_color='#F44336', opacity=0.7))
                fig5.update_layout(barmode='group', height=380, xaxis_tickangle=-45)
                st.plotly_chart(fig5, width="stretch")

            with col2:
                st.markdown("#### 📊 Різниця ціни (%)")
                mc_sorted = mc.sort_values('diff_pct', ascending=True)
                colors_diff = ['#F44336' if v > 5 else '#FFC107' if v > 0 else '#4CAF50' for v in mc_sorted['diff_pct']]
                fig6 = go.Figure(go.Bar(
                    x=mc_sorted['diff_pct'], y=mc_sorted['asin'], orientation='h',
                    marker_color=colors_diff,
                    text=[f"{v:+.1f}%" for v in mc_sorted['diff_pct']], textposition='outside'
                ))
                fig6.add_vline(x=0, line_dash="dash", line_color="#888")
                fig6.update_layout(height=max(350,len(mc_sorted)*35), xaxis_title="% різниця (+ = ми дорожче)",
                                   margin=dict(l=0,r=60,t=10,b=0))
                st.plotly_chart(fig6, width="stretch")

            st.dataframe(merged_comp.style.format({'our_price':'${:.2f}','comp_price':'${:.2f}','diff':'${:+.2f}','diff_pct':'{:+.1f}%'}),
                         width="stretch", hide_index=True)
            st.download_button("📥 CSV", df_comp.to_csv(index=False).encode(), "competitive.csv", "text/csv")

    # ── TAB 4: All Offers ──
    with tabs[3]:
        if df_off.empty:
            st.info("Немає даних pricing_offers")
        else:
            st.markdown(f"#### 📋 Всі офери ({len(df_off)} записів)")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("##### 📦 По offer_type")
                ot = df_off['offer_type'].value_counts().reset_index()
                ot.columns = ['Type','Count']
                fig7 = px.pie(ot, values='Count', names='Type', hole=0.4, height=300)
                st.plotly_chart(fig7, width="stretch")
            with col2:
                st.markdown("##### 🏆 BB Winners в офферах")
                bbo = df_off['is_buybox_winner'].value_counts().reset_index()
                bbo.columns = ['Winner','Count']
                fig8 = px.pie(bbo, values='Count', names='Winner', hole=0.4, height=300,
                              color_discrete_sequence=['#4CAF50','#F44336'])
                st.plotly_chart(fig8, width="stretch")

            show_o = [c for c in ['asin','offer_type','fulfillment','price','listing_price','shipping','is_buybox_winner','total_offer_count','seller_id'] if c in df_off.columns]
            st.dataframe(df_off[show_o].style.format({c:'${:.2f}' for c in ['price','listing_price','shipping'] if c in df_off.columns}),
                         width="stretch", hide_index=True)
            st.download_button("📥 CSV", df_off.to_csv(index=False).encode(), "offers.csv", "text/csv")

    # ── AI ──
    ctx = f"""Pricing/BuyBox: {total_asins} ASINs | BB Win Rate: {bb_pct:.0f}% ({bb_winner_cnt}/{bb_total}) | Avg Price: ${avg_price:.2f} | Lost BB: {bb_total-bb_winner_cnt}"""
    show_ai_chat(ctx, [
        "Які ASIN програють Buy Box — яка ціна у конкурента?",
        "Де наша ціна вища за competitive price?",
        "Які ASIN варто знизити ціну для виграшу Buy Box?",
    ], "pricing")


def show_fba_operations():
    st.markdown("### 📦 FBA Operations")
    engine = get_engine()

    # ── Завантаження ──
    try:
        with engine.connect() as conn:
            df_ship  = pd.read_sql(text("SELECT * FROM fba_shipments ORDER BY created_at DESC"), conn)
            df_items = pd.read_sql(text("SELECT * FROM fba_shipment_items"), conn)
            df_rem   = pd.read_sql(text("SELECT * FROM fba_removals ORDER BY order_date DESC"), conn)
        try:
            with engine.connect() as conn:
                df_nc = pd.read_sql(text("SELECT * FROM fba_inbound_noncompliance ORDER BY received_date DESC"), conn)
        except:
            df_nc = pd.DataFrame()
    except Exception as e:
        st.error(f"Помилка: {e}"); return

    # нормалізуємо числа
    for df, cols in [
        (df_items, ["quantity_shipped", "quantity_received", "quantity_in_case"]),
        (df_rem,   ["requested_quantity", "shipped_quantity", "disposed_quantity", "cancelled_quantity", "in_progress_quantity"]),
    ]:
        for c in cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c].replace("", None), errors="coerce").fillna(0)

    # ── KPI ──
    total_shipments  = len(df_ship)
    active_ship      = int((df_ship["shipment_status"].isin(["WORKING","SHIPPED","IN_TRANSIT","RECEIVING"])).sum()) if "shipment_status" in df_ship.columns else 0
    closed_ship      = int((df_ship["shipment_status"] == "CLOSED").sum()) if "shipment_status" in df_ship.columns else 0
    total_units_sent = int(df_items["quantity_shipped"].sum()) if "quantity_shipped" in df_items.columns else 0
    total_units_recv = int(df_items["quantity_received"].sum()) if "quantity_received" in df_items.columns else 0
    recv_rate        = total_units_recv / total_units_sent * 100 if total_units_sent > 0 else 0
    total_removals   = len(df_rem)
    removal_units    = int(df_rem["shipped_quantity"].sum()) if "shipped_quantity" in df_rem.columns else 0
    nc_count         = len(df_nc)

    # ── Hero ──
    nc_span = (f'<span style="background:#2b1a1a;border:1px solid #4a2d2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">⚠️ NonCompl <b style="color:#F44336">{nc_count}</b></span>') if nc_count > 0 else ""
    st.markdown(f"""
<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border:1px solid #2d2d4a;
            border-radius:12px;padding:20px 28px;margin-bottom:16px;
            display:flex;align-items:center;gap:32px;flex-wrap:wrap">
  <div>
    <div style="font-size:12px;color:#aaa;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">
      📦 FBA Operations
    </div>
    <div style="font-size:48px;font-weight:900;color:#5B9BD5;font-family:monospace;line-height:1">
      {total_shipments:,}
    </div>
    <div style="font-size:12px;color:#aaa;margin-top:6px">відвантажень всього</div>
  </div>
  <div style="flex:1;min-width:200px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <span style="background:#1e2e1e;border:1px solid #2d4a30;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🚀 Active <b style="color:#4CAF50">{active_ship}</b>
    </span>
    <span style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      ✅ Closed <b style="color:#aaa">{closed_ship}</b>
    </span>
    <span style="background:#1a2b2e;border:1px solid #2d404a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📬 Отримано <b style="color:#5B9BD5">{total_units_recv:,}</b> / {total_units_sent:,}
    </span>
    <span style="background:#2b2b1a;border:1px solid #4a4a2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🗑 Removals <b style="color:#FFC107">{total_removals}</b>
    </span>
    {nc_span}
  </div>
</div>""", unsafe_allow_html=True)

    # ── Інсайти ──
    _ic = st.columns(3)
    if recv_rate >= 98:   _em, _col = "🟢", "#0d2b1e"
    elif recv_rate >= 90: _em, _col = "🟡", "#2b2400"
    else:                 _em, _col = "🔴", "#2b0d0d"
    with _ic[0]: insight_card(_em, "Received Rate",
        f"Отримано <b>{recv_rate:.1f}%</b> — {total_units_recv:,} з {total_units_sent:,} одиниць", _col)
    with _ic[1]: insight_card("🚀", "Активні відвантаження",
        f"<b>{active_ship}</b> shipments в процесі · {closed_ship} закрито", "#1a1a2e")
    if nc_count > 0:
        with _ic[2]: insight_card("⚠️", "Non-Compliance",
            f"<b>{nc_count}</b> проблем з відповідністю — перевір в Seller Central", "#2b0d0d")
    else:
        with _ic[2]: insight_card("🟢", "Non-Compliance",
            "Проблем з відповідністю немає ✅", "#0d2b1e")

    st.markdown("---")

    # ── Таби ──
    tabs = st.tabs(["🚀 Shipments", "📋 Items", "🗑 Removals", "⚠️ Non-Compliance"])

    # ── TAB 1: Shipments ──
    with tabs[0]:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 📊 По статусу")
            if "shipment_status" in df_ship.columns:
                sc = df_ship["shipment_status"].value_counts().reset_index()
                sc.columns = ["Status", "Count"]
                colors_s = {"WORKING":"#5B9BD5","SHIPPED":"#FFC107","IN_TRANSIT":"#FF9800",
                            "RECEIVING":"#4CAF50","CLOSED":"#888","CANCELLED":"#F44336"}
                fig = px.bar(sc, x="Count", y="Status", orientation="h",
                             color="Status",
                             color_discrete_map=colors_s, height=300)
                fig.update_layout(showlegend=False, margin=dict(l=0,r=40,t=10,b=0))
                st.plotly_chart(fig, width="stretch")

        with col2:
            st.markdown("#### 🏭 По Destination FC")
            if "destination_fc" in df_ship.columns:
                fc = df_ship["destination_fc"].value_counts().head(10).reset_index()
                fc.columns = ["FC", "Count"]
                fig2 = px.pie(fc, values="Count", names="FC", hole=0.4, height=300)
                st.plotly_chart(fig2, width="stretch")

        st.markdown("#### 📋 Список відвантажень")
        show_s = [c for c in ["shipment_id","shipment_name","shipment_status","destination_fc",
                               "label_prep_type","confirmed_need_by_date","marketplace","created_at"]
                  if c in df_ship.columns]
        
        # sidebar filter
        statuses = ["Всі"] + sorted(df_ship["shipment_status"].dropna().unique().tolist()) if "shipment_status" in df_ship.columns else ["Всі"]
        sel_s = st.selectbox("Фільтр статус:", statuses, key="ship_status")
        df_s = df_ship[df_ship["shipment_status"] == sel_s] if sel_s != "Всі" else df_ship
        
        st.dataframe(df_s[show_s].head(200), width="stretch", hide_index=True, height=400)
        st.caption(f"{len(df_s)} відвантажень")
        st.download_button("📥 CSV Shipments", df_ship.to_csv(index=False).encode(), "shipments.csv", "text/csv")

    # ── TAB 2: Items ──
    with tabs[1]:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 📦 Топ 15 SKU за відправленою кількістю")
            if "sku" in df_items.columns and "quantity_shipped" in df_items.columns:
                top_sku = df_items.groupby("sku")["quantity_shipped"].sum().nlargest(15).reset_index()
                fig3 = go.Figure(go.Bar(
                    x=top_sku["quantity_shipped"], y=top_sku["sku"], orientation="h",
                    marker_color="#5B9BD5",
                    text=top_sku["quantity_shipped"].astype(int), textposition="outside"
                ))
                fig3.update_layout(height=420, yaxis={"categoryorder":"total ascending"},
                                   margin=dict(l=0,r=40,t=10,b=0))
                st.plotly_chart(fig3, width="stretch")

        with col2:
            st.markdown("#### 📊 Shipped vs Received")
            if "quantity_shipped" in df_items.columns and "quantity_received" in df_items.columns:
                # per shipment
                ship_agg = df_items.groupby("shipment_id").agg(
                    shipped=("quantity_shipped","sum"),
                    received=("quantity_received","sum")
                ).reset_index().head(20)
                fig4 = go.Figure()
                fig4.add_trace(go.Bar(name="Shipped", x=ship_agg["shipment_id"], y=ship_agg["shipped"], marker_color="#5B9BD5"))
                fig4.add_trace(go.Bar(name="Received", x=ship_agg["shipment_id"], y=ship_agg["received"], marker_color="#4CAF50"))
                fig4.update_layout(barmode="group", height=420, xaxis_tickangle=-45,
                                   margin=dict(l=0,r=0,t=10,b=80))
                st.plotly_chart(fig4, width="stretch")

        show_i = [c for c in ["shipment_id","sku","fnsku","asin","quantity_shipped","quantity_received","marketplace"]
                  if c in df_items.columns]
        st.dataframe(df_items[show_i].head(500), width="stretch", hide_index=True, height=400)
        st.download_button("📥 CSV Items", df_items.to_csv(index=False).encode(), "shipment_items.csv", "text/csv")

    # ── TAB 3: Removals ──
    with tabs[2]:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 📊 По Disposition")
            if "disposition" in df_rem.columns:
                disp = df_rem["disposition"].value_counts().reset_index()
                disp.columns = ["Disposition", "Count"]
                fig5 = px.pie(disp, values="Count", names="Disposition", hole=0.4,
                              color_discrete_sequence=["#F44336","#FF9800","#FFC107","#4CAF50"],
                              height=320)
                st.plotly_chart(fig5, width="stretch")

        with col2:
            st.markdown("#### 🏆 Топ SKU по Removals")
            if "sku" in df_rem.columns and "shipped_quantity" in df_rem.columns:
                top_rem = df_rem.groupby("sku")["shipped_quantity"].sum().nlargest(10).reset_index()
                fig6 = go.Figure(go.Bar(
                    x=top_rem["shipped_quantity"], y=top_rem["sku"], orientation="h",
                    marker_color="#F44336",
                    text=top_rem["shipped_quantity"].astype(int), textposition="outside"
                ))
                fig6.update_layout(height=320, yaxis={"categoryorder":"total ascending"},
                                   margin=dict(l=0,r=40,t=10,b=0))
                st.plotly_chart(fig6, width="stretch")

        show_r = [c for c in ["order_id","order_date","order_status","sku","fnsku","disposition",
                               "requested_quantity","shipped_quantity","disposed_quantity","marketplace"]
                  if c in df_rem.columns]
        st.dataframe(df_rem[show_r].head(300), width="stretch", hide_index=True, height=400)
        st.caption(f"{len(df_rem)} removal orders · {removal_units:,} одиниць")
        st.download_button("📥 CSV Removals", df_rem.to_csv(index=False).encode(), "removals.csv", "text/csv")

    # ── TAB 4: Non-Compliance ──
    with tabs[3]:
        if df_nc.empty:
            st.success("✅ Проблем з Non-Compliance немає!")
        else:
            st.error(f"⚠️ {len(df_nc)} Non-Compliance записів — перевір в Seller Central")
            show_nc = [c for c in ["shipment_id","shipment_name","sku","fnsku","asin","product_name",
                                    "quantity","noncompliance_type","fulfillment_center","received_date"]
                       if c in df_nc.columns]
            st.dataframe(df_nc[show_nc], width="stretch", hide_index=True)
            st.download_button("📥 CSV Non-Compliance", df_nc.to_csv(index=False).encode(), "noncompliance.csv", "text/csv")

    # ── AI ──
    ctx = f"""FBA Operations: {total_shipments} shipments | Active: {active_ship} | Received: {recv_rate:.1f}% | Removals: {total_removals} ({removal_units} units) | NonCompliance: {nc_count}"""
    show_ai_chat(ctx, [
        "Які відвантаження ще не отримані повністю?",
        "Які SKU найчастіше потрапляють у Removals?",
        "Яка середня різниця між shipped та received?",
    ], "fba_ops")



def show_tax(t=None):
    st.markdown("### 📋 Податки (Tax)")
    st.caption("Marketplace Facilitator Tax — Amazon збирає та перераховує автоматично")
    engine = get_engine()
    try:
        with engine.connect() as conn:
            kpi = pd.read_sql(text("""
                SELECT
                    SUM(CASE WHEN charge_type = 'Tax'
                        THEN NULLIF(amount,'')::numeric ELSE 0 END) AS fe_tax,
                    SUM(CASE WHEN charge_type = 'ShippingTax'
                        THEN NULLIF(amount,'')::numeric ELSE 0 END) AS fe_ship_tax,
                    SUM(CASE WHEN charge_type = 'GiftWrapTax'
                        THEN NULLIF(amount,'')::numeric ELSE 0 END) AS fe_gift_tax,
                    COUNT(CASE WHEN charge_type = 'Tax' THEN 1 END) AS fe_tax_rows,
                    SUM(CASE WHEN event_type='Shipment' AND charge_type='Principal'
                        THEN NULLIF(amount,'')::numeric ELSE 0 END) AS gross
                FROM finance_events
            """), conn).iloc[0]
            kpi_orders = pd.read_sql(text("""
                SELECT
                    SUM(CASE WHEN item_tax ~ '^[0-9.]+$'
                        THEN item_tax::numeric ELSE 0 END) AS item_tax_total,
                    COUNT(CASE WHEN item_tax ~ '^[0-9.]+$'
                        AND item_tax::numeric > 0 THEN 1 END) AS orders_with_tax,
                    COUNT(*) AS total_orders
                FROM orders
            """), conn).iloc[0]
    except Exception as e:
        st.error(f"Помилка: {e}"); return

    fe_tax    = float(kpi["fe_tax"] or 0)
    fe_ship   = float(kpi["fe_ship_tax"] or 0)
    fe_gift   = float(kpi["fe_gift_tax"] or 0)
    fe_rows   = int(kpi["fe_tax_rows"] or 0)
    gross     = float(kpi["gross"] or 0)
    total_tax = fe_tax + fe_ship + fe_gift
    tax_rate  = total_tax/gross*100 if gross else 0
    ord_with  = int(kpi_orders["orders_with_tax"] or 0)
    ord_total = int(kpi_orders["total_orders"] or 0)
    taxed_pct = ord_with/ord_total*100 if ord_total else 0

    st.markdown(f"""
<div style="background:#1a1a2e;border:1px solid #2a2a3e;border-radius:12px;
            padding:20px 28px;margin-bottom:16px">
  <div style="font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">
    📋 MARKETPLACE FACILITATOR TAX (Amazon збирає замість тебе)
  </div>
  <div style="font-size:42px;font-weight:800;color:#fff;font-family:monospace">
    ${total_tax:,.0f}
    <span style="font-size:20px;color:#aaa"> зібрано податків</span>
  </div>
  <div style="font-size:13px;color:#aaa;margin-top:6px">
    Tax rate {tax_rate:.1f}% від gross ${gross/1e6:.2f}M · {fe_rows:,} транзакцій
  </div>
  <div style="display:flex;gap:12px;margin-top:12px;flex-wrap:wrap">
    <span style="background:#1e293b;border:1px solid #6366f1;border-radius:6px;padding:4px 12px;font-size:13px;color:#a5b4fc">
      🛍 Item Tax ${fe_tax:,.0f}
    </span>
    <span style="background:#1e293b;border:1px solid #4472C4;border-radius:6px;padding:4px 12px;font-size:13px;color:#93c5fd">
      🚚 Shipping Tax ${fe_ship:,.0f}
    </span>
    <span style="background:#1e293b;border:1px solid #22c55e;border-radius:6px;padding:4px 12px;font-size:13px;color:#86efac">
      🎁 Gift Wrap ${fe_gift:,.0f}
    </span>
    <span style="background:#1e293b;border:1px solid #f59e0b;border-radius:6px;padding:4px 12px;font-size:13px;color:#fcd34d">
      {taxed_pct:.1f}% замовлень з податком
    </span>
  </div>
</div>""", unsafe_allow_html=True)

    st.info("💡 **Marketplace Facilitator Tax** — Amazon автоматично збирає та сплачує податки. Ці гроші **не твої** — тобі нічого робити не потрібно.")

    _ic = st.columns(3)
    rate_color = "#22c55e" if tax_rate <= 10 else "#f59e0b" if tax_rate <= 15 else "#ef4444"
    with _ic[0]: insight_card("📊", "Ставка податку",
        f"Avg tax rate <b>{tax_rate:.1f}%</b> від gross — {'норма для US' if tax_rate <= 12 else 'перевір розрахунок'}", "#1a1a2e")
    with _ic[1]: insight_card("🛍", "Замовлення з Tax",
        f"<b>{ord_with:,}</b> замовлень ({taxed_pct:.1f}%) мають item tax", "#1e293b")
    with _ic[2]: insight_card("✅", "Твоя дія",
        "Amazon сплачує замість тебе. Звіти: Seller Central → Tax → Reports", "#0d2b1e")

    st.markdown("---")
    tabs = st.tabs(["📈 Тренд", "🗺️ По штатах", "📋 Деталі"])

    with tabs[0]:
        try:
            with engine.connect() as conn:
                df_trend = pd.read_sql(text("""
                    SELECT
                        DATE_TRUNC('month', posted_date::date) AS month,
                        SUM(CASE WHEN charge_type='Tax'
                            THEN NULLIF(amount,'')::numeric ELSE 0 END) AS item_tax,
                        SUM(CASE WHEN charge_type='ShippingTax'
                            THEN NULLIF(amount,'')::numeric ELSE 0 END) AS ship_tax,
                        SUM(CASE WHEN charge_type IN ('Tax','ShippingTax','GiftWrapTax')
                            THEN NULLIF(amount,'')::numeric ELSE 0 END) AS total_tax,
                        SUM(CASE WHEN event_type='Shipment' AND charge_type='Principal'
                            THEN NULLIF(amount,'')::numeric ELSE 0 END) AS gross
                    FROM finance_events
                    WHERE posted_date IS NOT NULL AND posted_date != ''
                    GROUP BY 1 ORDER BY 1
                """), conn)
            if not df_trend.empty:
                df_trend["month"] = pd.to_datetime(df_trend["month"])
                df_trend["tax_rate"] = (df_trend["total_tax"] / df_trend["gross"].replace(0, float("nan")) * 100).round(2)
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("#### 📈 Податок по місяцях")
                    fig = go.Figure()
                    fig.add_trace(go.Bar(x=df_trend["month"], y=df_trend["item_tax"], name="Item Tax", marker_color="#4472C4"))
                    fig.add_trace(go.Bar(x=df_trend["month"], y=df_trend["ship_tax"], name="Shipping Tax", marker_color="#ED7D31"))
                    fig.update_layout(barmode="stack", height=350, yaxis_title="Tax $", legend=dict(orientation="h",y=1.1))
                    st.plotly_chart(fig, width="stretch")
                with col2:
                    st.markdown("#### 📊 Tax Rate % по місяцях")
                    fig2 = go.Figure(go.Scatter(
                        x=df_trend["month"], y=df_trend["tax_rate"],
                        mode="lines+markers+text",
                        text=[f"{v:.1f}%" for v in df_trend["tax_rate"].fillna(0)],
                        textposition="top center",
                        line=dict(color="#22c55e", width=3), marker=dict(size=8)
                    ))
                    fig2.add_hline(y=10, line_dash="dash", line_color="orange", annotation_text="10% avg")
                    fig2.update_layout(height=350, yaxis_title="Tax Rate %")
                    st.plotly_chart(fig2, width="stretch")
        except Exception as e:
            st.error(str(e))

    with tabs[1]:
        try:
            with engine.connect() as conn:
                df_states = pd.read_sql(text("""
                    SELECT ship_state AS state, COUNT(*) AS orders,
                        SUM(CASE WHEN item_tax ~ '^[0-9.]+$' THEN item_tax::numeric ELSE 0 END) AS item_tax,
                        SUM(CASE WHEN item_price ~ '^[0-9.]+$' THEN item_price::numeric ELSE 0 END) AS gross,
                        AVG(CASE WHEN item_tax ~ '^[0-9.]+$' AND item_tax::numeric > 0
                            AND item_price ~ '^[0-9.]+$' AND item_price::numeric > 0
                            THEN item_tax::numeric / item_price::numeric * 100 END) AS avg_tax_rate
                    FROM orders
                    WHERE ship_state IS NOT NULL AND ship_state != '' AND ship_country = 'US'
                    GROUP BY 1 ORDER BY item_tax DESC LIMIT 30
                """), conn)
            if not df_states.empty:
                df_states["avg_tax_rate"] = df_states["avg_tax_rate"].fillna(0).round(2)
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("#### 💰 Топ штатів по сумі")
                    fig = px.bar(df_states.head(20), x="item_tax", y="state", orientation="h",
                                 color="item_tax", color_continuous_scale="Blues", text="item_tax",
                                 labels={"item_tax":"Tax $","state":"Штат"})
                    fig.update_layout(height=500, yaxis={"categoryorder":"total ascending"})
                    fig.update_traces(texttemplate="$%{text:,.0f}")
                    st.plotly_chart(fig, width="stretch")
                with col2:
                    st.markdown("#### 📊 Avg Tax Rate %")
                    df_r = df_states[df_states["avg_tax_rate"] > 0].nlargest(20,"avg_tax_rate")
                    fig2 = px.bar(df_r, x="avg_tax_rate", y="state", orientation="h",
                                  color="avg_tax_rate", color_continuous_scale="RdYlGn_r", text="avg_tax_rate")
                    fig2.update_layout(height=500, yaxis={"categoryorder":"total ascending"})
                    fig2.update_traces(texttemplate="%{text:.1f}%")
                    st.plotly_chart(fig2, width="stretch")
                st.dataframe(df_states.style.format({"item_tax":"${:,.2f}","gross":"${:,.2f}","avg_tax_rate":"{:.2f}%"}),
                             width="stretch", hide_index=True)
                st.download_button("📥 CSV по штатах", df_states.to_csv(index=False).encode(), "tax_states.csv", "text/csv")
        except Exception as e:
            st.error(str(e))

    with tabs[2]:
        try:
            with engine.connect() as conn:
                df_det = pd.read_sql(text("""
                    SELECT posted_date, event_type, charge_type, order_id, seller_sku,
                           NULLIF(amount,'')::numeric AS amount, currency
                    FROM finance_events
                    WHERE charge_type IN ('Tax','ShippingTax','GiftWrapTax','SalesTaxCollectionFee')
                    ORDER BY posted_date DESC NULLS LAST LIMIT 500
                """), conn)
            if not df_det.empty:
                col1, col2 = st.columns(2)
                with col1:
                    ct = df_det["charge_type"].value_counts().reset_index()
                    ct.columns = ["Type","Count"]
                    fig = px.pie(ct, values="Count", names="Type", hole=0.4,
                                 color_discrete_sequence=px.colors.qualitative.Set2)
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, width="stretch")
                with col2:
                    by_type = df_det.groupby("charge_type")["amount"].agg(["sum","count"]).reset_index()
                    by_type.columns = ["charge_type","total","count"]
                    st.dataframe(by_type.style.format({"total":"${:,.2f}"}), width="stretch", hide_index=True)
                st.dataframe(df_det.style.format({"amount":"${:.2f}"}), width="stretch", hide_index=True, height=400)
                st.download_button("📥 CSV деталей", df_det.to_csv(index=False).encode(), "tax_details.csv", "text/csv")
        except Exception as e:
            st.error(str(e))

    st.markdown("---")
    ctx = f"""Tax: ${total_tax:,.0f} зібрано · rate {tax_rate:.1f}% · Item ${fe_tax:,.0f} · Ship ${fe_ship:,.0f} · {ord_with}/{ord_total} замовлень з tax"""
    show_ai_chat(ctx, [
        "Який штат має найвищу ставку податку?",
        "Як змінився tax rate за останні 3 місяці?",
        "Скільки зібрано податків за 2025 рік?",
    ], "tax")

def show_scraper_manager():
    _scr_init()
    _scr_flush()

    st.markdown("## 🕷 Scraper Reviews")

    if st.session_state.scr_running:
        st.info(f"🔄 {st.session_state.scr_label or 'Збір в процесі...'}")
    elif st.session_state.scr_done:
        st.success(f"✅ Готово! Циклів: **{st.session_state.scr_cycles}**")

    st.progress(st.session_state.scr_pct, text=st.session_state.scr_label or " ")
    st.markdown("---")

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

    max_per_star = 100
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

try:
    ensure_tables()
    create_admin_if_not_exists()
except Exception as e:
    st.error(f"DB init error: {e}")
    st.stop()

if "user" not in st.session_state or not st.session_state.user:
    show_login()
    st.stop()

user = st.session_state.user

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
    dates  = sorted(df['date'].unique(), reverse=True)
    stores = [t["all_stores"]] + list(df['Store Name'].unique()) if 'Store Name' in df.columns else [t["all_stores"]]
else:
    dates  = []
    stores = [t.get("all_stores","Всі")]

# ── Всі фільтри разом в одному місці ──
_cur_page = st.session_state.get("report_choice", "🏠 Overview")
st.sidebar.header(t["sidebar_title"])

# 📅 Дата і 🏪 Магазин — для інвентарних сторінок
selected_date  = st.sidebar.selectbox(t["date_label"], dates, key="sel_date") if dates else None
selected_store = st.sidebar.selectbox(t["store_label"], stores, key="sel_store")

# 📅 Діапазон — для фінансів (завжди показуємо, але використовується тільки в Фінанси)
try:
    _eng = get_engine()
    with _eng.connect() as _c:
        _b = pd.read_sql(text("SELECT MIN(posted_date)::date as mn, MAX(posted_date)::date as mx FROM settlements"), _c).iloc[0]
    _fin_min, _fin_max = _b['mn'], _b['mx']
except Exception:
    _fin_min, _fin_max = dt.date(2024,1,1), dt.date.today()
st.sidebar.date_input(
    "📅 Діапазон:", value=(_fin_min, _fin_max),
    min_value=_fin_min, max_value=_fin_max, key="fin_date"
)

if not df.empty:
    df_filtered = df[df['date']==selected_date] if selected_date else df
    if selected_store != t.get("all_stores","Всі"):
        df_filtered = df_filtered[df_filtered['Store Name']==selected_store]
else:
    df_filtered = pd.DataFrame()
    selected_date = None

# ── NAVIGATION ──
st.sidebar.markdown("---")

_nav_labels = {
    "UA": ("📊 Звіти", "── Інструменти ──"),
    "RU": ("📊 Отчёты", "── Инструменты ──"),
    "EN": ("📊 Reports", "── Tools ──"),
}
_lbl_reports, _lbl_tools_sep = _nav_labels.get(lang, _nav_labels["UA"])

main_nav = [
    "🏠 Overview",
    "📈 Трафик (Sales & Traffic)",
    "💰 Фінанси (Settlements)",
    "🛒 Продажи (Orders)",
    "📦 Склад (Inventory)",
    "🔙 Повернення (Returns)",
    "📝 Листинги (Listings)",
    "💲 Pricing / BuyBox",
    "📦 FBA Operations",
    "📋 Податки (Tax)",
    "⭐ Amazon Reviews",
]

tools_nav = [
    "📊 ETL Status",
    "🕷 Scraper Reviews",
    "ℹ️ Про додаток",
    "🔌 API",
]

if user["role"] == "admin":
    tools_nav_full = ["👑 User Management"] + tools_nav
else:
    tools_nav_full = tools_nav

if user["role"] != "admin":
    main_nav       = [r for r in main_nav       if can_view(r)]
    tools_nav_full = [r for r in tools_nav_full if can_view(r)]

all_real = main_nav + tools_nav_full
if not all_real:
    _no_access = {"UA": "У вас немає доступу до жодного розділу.", "RU": "У вас нет доступа ни к одному разделу.", "EN": "You have no access to any section."}
    st.warning(_no_access.get(lang, _no_access["UA"]))
    st.stop()

# Один radio з нe-вибираємим роздільником
_SEP = _lbl_tools_sep
all_nav = main_nav + [_SEP] + tools_nav_full

if st.session_state.report_choice not in all_real:
    st.session_state.report_choice = all_real[0]

_cur_idx = all_nav.index(st.session_state.report_choice) if st.session_state.report_choice in all_nav else 0

st.sidebar.markdown(f"**{_lbl_reports}**")
raw_choice = st.sidebar.radio(
    "nav", all_nav,
    index=_cur_idx,
    label_visibility="collapsed",
    key="nav_single"
)

# Якщо вибрали роздільник — повертаємось до попереднього
if raw_choice == _SEP:
    pass  # не міняємо report_choice
else:
    st.session_state.report_choice = raw_choice

report_choice = st.session_state.report_choice

# ── ROUTING ──
if   report_choice == "🏠 Overview":                show_overview(df_filtered, t, selected_date)
elif report_choice == "📈 Трафик (Sales & Traffic)":          show_sales_traffic(t)
elif report_choice == "💰 Фінанси (Settlements)":   show_settlements(t)
elif report_choice == "🛒 Продажи (Orders)":         show_orders(t)
elif report_choice == "📦 Склад (Inventory)":        show_inventory_unified()
elif report_choice == "🔙 Повернення (Returns)":                  show_returns(t)
elif report_choice == "📝 Листинги (Listings)":      show_listings()
elif report_choice == "💲 Pricing / BuyBox":         show_pricing()
elif report_choice == "📦 FBA Operations":           show_fba_operations()
elif report_choice == "📋 Податки (Tax)":            show_tax(t)
elif report_choice == "⭐ Amazon Reviews":           show_reviews(t)
elif report_choice == "📊 ETL Status":               show_etl_status()
elif report_choice == "🕷 Scraper Reviews":          show_scraper_manager()
elif report_choice == "👑 User Management":          show_admin_panel()
elif report_choice == "ℹ️ Про додаток":              show_about()
elif report_choice == "🔌 API":                       show_api_docs()

st.sidebar.markdown("---")
st.sidebar.caption("📦 Amazon FBA BI System v5.0 🌍")
