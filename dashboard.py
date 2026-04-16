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
import streamlit.components.v1 as components
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
    except Exception as e:
        import traceback
        print(f"❌ SALES TRAFFIC ERROR: {e}")
        traceback.print_exc()
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

    # ── TAB 4: Inventory Health ──
    with tabs[4]:
        st.markdown("#### 🏥 Stock Overview — Залишки + Velocity")
 
        try:
            with engine.connect() as conn:
                # Інвентар
                df_inv = pd.read_sql(text('SELECT * FROM fba_inventory'), conn)
                # Продажі за 90 днів
                df_ord = pd.read_sql(text("""
                    SELECT sku,
                        SUBSTRING(purchase_date,1,10)::date AS day,
                        COUNT(DISTINCT amazon_order_id) AS orders,
                        SUM(CASE WHEN quantity ~ '^[0-9]+$' THEN quantity::numeric ELSE 1 END) AS units,
                        SUM(CASE WHEN item_price ~ '^[0-9.]+$' THEN item_price::numeric ELSE 0 END) AS revenue
                    FROM orders
                    WHERE SUBSTRING(purchase_date,1,10)::date >= CURRENT_DATE - INTERVAL '90 days'
                      AND purchase_date IS NOT NULL AND purchase_date != ''
                    GROUP BY 1, 2
                    ORDER BY 2 DESC
                """), conn)
                # Listings для active/inactive
                try:
                    df_lst = pd.read_sql(text(
                        "SELECT seller_sku AS sku, status, fulfillment_channel FROM listings_all"
                    ), conn)
                except:
                    df_lst = pd.DataFrame()
        except Exception as e:
            st.error(f"Помилка: {e}")
            df_inv = pd.DataFrame()
            df_ord = pd.DataFrame()
            df_lst = pd.DataFrame()
 
        if df_inv.empty:
            st.warning("Немає даних inventory")
        else:
            # Нормалізуємо inventory
            for c in ["Available","Price","Velocity","Inbound","Reserved"]:
                if c in df_inv.columns:
                    df_inv[c] = pd.to_numeric(df_inv[c].replace("", None), errors="coerce").fillna(0)
            if "SKU" not in df_inv.columns:
                for c in df_inv.columns:
                    if c.lower() in ("sku","seller_sku"):
                        df_inv.rename(columns={c: "SKU"}, inplace=True)
                        break
 
            # Merge з listings status
            if not df_lst.empty and "SKU" in df_inv.columns:
                df_lst["status"] = df_lst["status"].astype(str).str.lower()
                status_map = df_lst.groupby("sku")["status"].first().to_dict()
                df_inv["listing_status"] = df_inv["SKU"].map(status_map).fillna("unknown")
            else:
                df_inv["listing_status"] = "unknown"
 
            # Merge з velocity з orders (реальний sold за 30д)
            if not df_ord.empty and "SKU" in df_inv.columns:
                last_30 = df_ord[pd.to_datetime(df_ord["day"]) >= (pd.Timestamp.now().normalize() - pd.Timedelta(days=30))]
                sold_30 = last_30.groupby("sku")["units"].sum().to_dict()
                rev_30  = last_30.groupby("sku")["revenue"].sum().to_dict()
                df_inv["sold_30d"]    = df_inv["SKU"].map(sold_30).fillna(0).astype(int)
                df_inv["revenue_30d"] = df_inv["SKU"].map(rev_30).fillna(0)
                df_inv["velocity_real"] = (df_inv["sold_30d"] / 30).round(2)
                df_inv["dos_real"] = (df_inv["Available"] / df_inv["velocity_real"].replace(0, float("nan"))).round(0).fillna(0)
            else:
                df_inv["sold_30d"] = 0
                df_inv["revenue_30d"] = 0
                df_inv["velocity_real"] = 0
                df_inv["dos_real"] = 0
 
            df_inv["stock_value"] = df_inv["Available"] * df_inv["Price"]
 
            # ── KPI ──
            active_cnt   = int((df_inv["listing_status"] == "active").sum())
            inactive_cnt = int((df_inv["listing_status"] != "active").sum())
            total_avail  = int(df_inv["Available"].sum())
            total_value  = df_inv["stock_value"].sum()
            total_sold   = int(df_inv["sold_30d"].sum())
            oos_cnt      = int((df_inv["Available"] == 0).sum())
            low14_cnt    = int(((df_inv["dos_real"] > 0) & (df_inv["dos_real"] < 14)).sum())
 
            c1,c2,c3,c4,c5,c6 = st.columns(6)
            c1.metric("✅ Active", f"{active_cnt}")
            c2.metric("❌ Inactive", f"{inactive_cnt}")
            c3.metric("📦 Available", f"{total_avail:,}")
            c4.metric("💰 Stock Value", f"${total_value:,.0f}")
            c5.metric("🛒 Sold 30д", f"{total_sold:,}")
            c6.metric("🔴 OOS", f"{oos_cnt}")
 
            # ── Інсайти ──
            _ic = st.columns(3)
            if low14_cnt > 0:
                with _ic[0]: insight_card("🔴", "Low Stock",
                    f"<b>{low14_cnt} SKU</b> закінчаться за <b><14 днів</b>", "#2b0d0d")
            else:
                with _ic[0]: insight_card("🟢", "Запаси",
                    "Всі активні SKU мають 14+ днів запасу", "#0d2b1e")
            active_pct = active_cnt / (active_cnt + inactive_cnt) * 100 if (active_cnt + inactive_cnt) > 0 else 0
            with _ic[1]: insight_card("📊", "Active Rate",
                f"<b>{active_pct:.0f}%</b> листингів активні ({active_cnt}/{active_cnt+inactive_cnt})",
                "#0d2b1e" if active_pct >= 80 else "#2b2400")
            turnover = total_avail / (total_sold / 30) if total_sold > 0 else 999
            with _ic[2]: insight_card("🔄", "Оборотність",
                f"Запас на <b>{turnover:.0f} днів</b> при поточному темпі продажів",
                "#0d2b1e" if turnover < 90 else "#2b2400" if turnover < 180 else "#2b0d0d")
 
            st.markdown("---")
 
            # ══════════════════════════════════
            # 1. STOCK TABLE — Active / Inactive
            # ══════════════════════════════════
            st.markdown("##### 📦 Таблиця залишків (Active / Inactive)")
 
            status_filter = st.selectbox("Фільтр:", ["Всі", "Active", "Inactive", "OOS", "Low Stock <14д"], key="health_filter")
            df_show = df_inv.copy()
            if status_filter == "Active":
                df_show = df_show[df_show["listing_status"] == "active"]
            elif status_filter == "Inactive":
                df_show = df_show[df_show["listing_status"] != "active"]
            elif status_filter == "OOS":
                df_show = df_show[df_show["Available"] == 0]
            elif status_filter == "Low Stock <14д":
                df_show = df_show[(df_show["dos_real"] > 0) & (df_show["dos_real"] < 14)]
 
            show_cols = [c for c in ["SKU","ASIN","Available","Price","stock_value",
                         "sold_30d","revenue_30d","velocity_real","dos_real","listing_status"]
                         if c in df_show.columns]
            rename = {"stock_value":"💰 Value","sold_30d":"🛒 Sold 30д",
                      "revenue_30d":"💰 Rev 30д","velocity_real":"⚡ Vel/день",
                      "dos_real":"📅 DoS","listing_status":"Status"}
 
            df_tbl = df_show[show_cols].rename(columns=rename).sort_values("📅 DoS").head(300)
            fmt = {}
            if "Price" in df_tbl.columns: fmt["Price"] = "${:.2f}"
            if "💰 Value" in df_tbl.columns: fmt["💰 Value"] = "${:,.0f}"
            if "💰 Rev 30д" in df_tbl.columns: fmt["💰 Rev 30д"] = "${:,.0f}"
            if "📅 DoS" in df_tbl.columns: fmt["📅 DoS"] = "{:.0f}"
 
            def color_status(val):
                if val == "active": return "color:#4CAF50"
                return "color:#F44336"
 
            styled = df_tbl.style.format(fmt)
            if "Status" in df_tbl.columns:
                styled = styled.applymap(color_status, subset=["Status"])
            st.dataframe(styled, width="stretch", hide_index=True, height=400)
            st.caption(f"{len(df_tbl)} з {len(df_show)} SKU")
 
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("##### 📊 Active vs Inactive")
                status_cnt = df_inv["listing_status"].value_counts().reset_index()
                status_cnt.columns = ["Status", "Count"]
                fig_s = px.pie(status_cnt, values="Count", names="Status", hole=0.4,
                               color_discrete_map={"active":"#4CAF50","inactive":"#F44336","unknown":"#888"},
                               height=300)
                st.plotly_chart(fig_s, width="stretch")
 
            with col2:
                st.markdown("##### 🏆 Топ 10 SKU по вартості")
                top_val = df_inv[df_inv["stock_value"] > 0].nlargest(10, "stock_value")
                if not top_val.empty:
                    fig_v = go.Figure(go.Bar(
                        x=top_val["stock_value"], y=top_val["SKU"], orientation="h",
                        marker_color="#5B9BD5",
                        text=[f"${v:,.0f}" for v in top_val["stock_value"]], textposition="outside"
                    ))
                    fig_v.update_layout(height=300, yaxis={"categoryorder":"total ascending"},
                                        margin=dict(l=0,r=80,t=10,b=0))
                    st.plotly_chart(fig_v, width="stretch")
 
            st.markdown("---")
 
            # ══════════════════════════════════
            # 2. SALES VELOCITY — по днях / тижнях
            # ══════════════════════════════════
            st.markdown("##### 📈 Продажі по днях / тижнях")
 
            if not df_ord.empty:
                gran = st.radio("Гранулярність:", ["День", "Тиждень"], horizontal=True, key="vel_gran")
 
                df_vel = df_ord.copy()
                df_vel["day"] = pd.to_datetime(df_vel["day"])
 
                if gran == "Тиждень":
                    df_agg = df_vel.resample("W", on="day").agg(
                        {"orders":"sum","units":"sum","revenue":"sum"}).reset_index()
                else:
                    df_agg = df_vel.groupby("day").agg(
                        {"orders":"sum","units":"sum","revenue":"sum"}).reset_index()
 
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"###### 🛒 Units sold ({gran})")
                    fig_u = go.Figure()
                    fig_u.add_trace(go.Bar(x=df_agg["day"], y=df_agg["units"],
                                           marker_color="#4CAF50", opacity=0.85,
                                           text=df_agg["units"].astype(int), textposition="outside"))
                    # MA-7
                    if gran == "День" and len(df_agg) >= 7:
                        ma = df_agg["units"].rolling(7, min_periods=1).mean()
                        fig_u.add_trace(go.Scatter(x=df_agg["day"], y=ma,
                                                   mode="lines", name="MA-7",
                                                   line=dict(color="#FFC107", width=2, dash="dot")))
                    fig_u.update_layout(height=320, margin=dict(l=0,r=0,t=10,b=0), showlegend=True)
                    st.plotly_chart(fig_u, width="stretch")
 
                with col2:
                    st.markdown(f"###### 💰 Revenue ({gran})")
                    fig_r = go.Figure()
                    fig_r.add_trace(go.Bar(x=df_agg["day"], y=df_agg["revenue"],
                                           marker_color="#5B9BD5", opacity=0.85,
                                           text=[f"${v:,.0f}" for v in df_agg["revenue"]], textposition="outside"))
                    if gran == "День" and len(df_agg) >= 7:
                        ma_r = df_agg["revenue"].rolling(7, min_periods=1).mean()
                        fig_r.add_trace(go.Scatter(x=df_agg["day"], y=ma_r,
                                                   mode="lines", name="MA-7",
                                                   line=dict(color="#FFC107", width=2, dash="dot")))
                    fig_r.update_layout(height=320, margin=dict(l=0,r=0,t=10,b=0),
                                        yaxis=dict(tickprefix="$"), showlegend=True)
                    st.plotly_chart(fig_r, width="stretch")
 
                # По SKU velocity
                st.markdown("##### 🏆 Топ 15 SKU по продажах (30д)")
                sku_vel = df_vel[df_vel["day"] >= (pd.Timestamp.now().normalize() - pd.Timedelta(days=30))]
                sku_agg = sku_vel.groupby("sku").agg(
                    units=("units","sum"), revenue=("revenue","sum"), orders=("orders","sum")
                ).reset_index().nlargest(15, "units")
 
                if not sku_agg.empty:
                    col1, col2 = st.columns(2)
                    with col1:
                        fig_su = go.Figure(go.Bar(
                            x=sku_agg["units"], y=sku_agg["sku"], orientation="h",
                            marker_color="#4CAF50",
                            text=[f"{int(v):,}" for v in sku_agg["units"]], textposition="outside"
                        ))
                        fig_su.update_layout(height=max(300, len(sku_agg)*35),
                                             yaxis={"categoryorder":"total ascending"},
                                             title="Units sold", margin=dict(l=0,r=60,t=30,b=0))
                        st.plotly_chart(fig_su, width="stretch")
 
                    with col2:
                        fig_sr = go.Figure(go.Bar(
                            x=sku_agg["revenue"], y=sku_agg["sku"], orientation="h",
                            marker_color="#5B9BD5",
                            text=[f"${v:,.0f}" for v in sku_agg["revenue"]], textposition="outside"
                        ))
                        fig_sr.update_layout(height=max(300, len(sku_agg)*35),
                                             yaxis={"categoryorder":"total ascending"},
                                             title="Revenue $", margin=dict(l=0,r=60,t=30,b=0))
                        st.plotly_chart(fig_sr, width="stretch")
 
                # Таблиця velocity
                st.markdown("##### 📋 Velocity таблиця (всі SKU з продажами)")
                sku_all = sku_vel.groupby("sku").agg(
                    units=("units","sum"), revenue=("revenue","sum"), orders=("orders","sum")
                ).reset_index()
                sku_all["vel/день"] = (sku_all["units"] / 30).round(2)
                sku_all = sku_all.sort_values("units", ascending=False)
                st.dataframe(
                    sku_all.rename(columns={"sku":"SKU","units":"Units 30д","revenue":"Revenue 30д","orders":"Orders"})
                        .style.format({"Revenue 30д":"${:,.0f}","vel/день":"{:.2f}"}),
                    width="stretch", hide_index=True, height=400
                )
                st.download_button("📥 CSV Velocity",
                    sku_all.to_csv(index=False).encode(), "velocity_30d.csv", "text/csv")
            else:
                st.warning("Немає даних orders за 90 днів")


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
    st.caption("Актуальні дані з БД · автооновлення при заході на сторінку")

    engine = get_engine()
    import datetime as _dt

    def q(sql):
        try:
            with engine.connect() as conn:
                r = pd.read_sql(text(sql), conn).iloc[0]
                return int(r.iloc[0] or 0), r.iloc[1]
        except:
            return 0, None

    modules = [
        ("📦 FBA Inventory",        "fba_inventory",              q("SELECT COUNT(*), MAX(created_at) FROM fba_inventory"),                                             "3× / день"),
        ("🏦 Finance Events",       "finance_events",             q("SELECT COUNT(*), MAX(posted_date) FROM finance_events"),                                           "3× / день"),
        ("💳 Settlements",          "settlements",                q("SELECT COUNT(*), MAX(posted_date) FROM settlements"),                                              "3× / день"),
        ("🛒 Orders",               "orders",                     q("SELECT COUNT(*), MAX(purchase_date) FROM orders"),                                                 "2× / день"),
        ("🔙 Returns",              "fba_returns",                q("SELECT COUNT(*), MAX(return_date::text) FROM fba_returns"),                                        "1× / день"),
        ("📈 Sales & Traffic",      "spapi.sales_traffic",        q("SELECT COUNT(*), MAX(report_date) FROM spapi.sales_traffic WHERE report_date != ''"),            "2× / день"),
        ("⭐ Reviews",              "amazon_reviews",             q("SELECT COUNT(*), MAX(created_at) FROM amazon_reviews"),                                            "за запитом"),
        ("📦 Shipments",            "fba_shipments",              q("SELECT COUNT(*), MAX(created_at) FROM fba_shipments"),                                             "1× / день"),
        ("📦 Shipment Items",       "fba_shipment_items",         q("SELECT COUNT(*), MAX(created_at) FROM fba_shipment_items"),                                        "1× / день"),
        ("🗑 Removals",             "fba_removals",               q("SELECT COUNT(*), MAX(created_at) FROM fba_removals"),                                              "1× / день"),
        ("💲 Pricing Current",      "pricing_current",            q("SELECT COUNT(*), MAX(created_at) FROM pricing_current"),                                           "3× / день"),
        ("🏆 Pricing BuyBox",       "pricing_buybox",             q("SELECT COUNT(*), MAX(created_at) FROM pricing_buybox"),                                            "3× / день"),
        ("📊 Pricing Competitive",  "pricing_competitive",        q("SELECT COUNT(*), MAX(created_at) FROM pricing_competitive"),                                       "3× / день"),
        ("📋 Listings All",         "listings_all",               q("SELECT COUNT(*), MAX(created_at) FROM listings_all"),                                              "1× / день"),
        ("📋 Listings Open",        "listings_open",              q("SELECT COUNT(*), MAX(created_at) FROM listings_open"),                                             "1× / день"),
        ("📗 Catalog Items",        "catalog_items",              q("SELECT COUNT(*), MAX(created_at) FROM catalog_items"),                                             "1× / день"),
        ("⚠️ Non-Compliance",       "fba_inbound_noncompliance",  q("SELECT COUNT(*), MAX(created_at) FROM fba_inbound_noncompliance"),                                "1× / день"),
        ("🤖 Agent Decisions",      "agent_decisions",            q("SELECT COUNT(*), MAX(created_at) FROM agent_decisions"),                                           "за подією"),
    ]

    now = _dt.datetime.now(_dt.timezone.utc).date()
    data = []; ok_cnt = warn_cnt = empty_cnt = 0

    for name, table, (cnt, last), freq in modules:
        if cnt and cnt > 0:
            try:
                last_str = str(last)[:10] if last else ""
                last_date = _dt.date.fromisoformat(last_str) if last_str else None
                if last_date:
                    delta = (now - last_date).days
                    age = "сьогодні" if delta == 0 else f"{delta}д тому"
                    if delta == 0:   status = "✅ OK";         ok_cnt += 1
                    elif delta <= 2: status = "🟡 Увага";     warn_cnt += 1
                    else:            status = "🔴 Застарів";  warn_cnt += 1
                else:
                    age = "—"; status = "✅ OK"; ok_cnt += 1
            except:
                age = str(last)[:10] if last else "—"; status = "✅ OK"; ok_cnt += 1
            data.append({"Модуль": name, "Таблиця": table, "Рядків": f"{cnt:,}", "Останнє": age, "Частота": freq, "Статус": status})
        else:
            data.append({"Модуль": name, "Таблиця": table, "Рядків": "—", "Останнє": "—", "Частота": freq, "Статус": "⏳ Порожня"})
            empty_cnt += 1

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("✅ OK",         ok_cnt)
    c2.metric("⚠️ Увага",     warn_cnt)
    c3.metric("⏳ Порожні",   empty_cnt)
    c4.metric("📊 Таблиць",   len(modules))

    def color_status(val):
        if "OK" in str(val):       return "color:#4CAF50"
        if "Увага" in str(val):    return "color:#FFC107"
        if "Застарів" in str(val): return "color:#F44336;font-weight:bold"
        return "color:#888"

    st.dataframe(pd.DataFrame(data).style.applymap(color_status, subset=["Статус"]),
                 width="stretch", hide_index=True, height=min(50+len(data)*35, 650))

    if warn_cnt > 0:   st.warning(f"⚠️ {warn_cnt} таблиць не оновлювались більше 1 дня")
    if empty_cnt > 0:  st.info(f"⏳ {empty_cnt} таблиць порожні")
    if ok_cnt == len(modules): st.success("✅ Всі завантажувачі працюють нормально!")


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
    st.markdown("## 🏠 Command Center")
    st.caption(f"Головна панель · {dt.date.today().strftime('%d %b %Y')}")

    engine = get_engine()

    # ══════════════════════════════════════
    # 1. ФІНАНСИ (з finance_events, 30 днів)
    # ══════════════════════════════════════
    net=gross=fees=refs=promos=adj=0; fin_orders=0
    try:
        with engine.connect() as conn:
            fr = pd.read_sql(text("""
                SELECT
                  SUM(CASE WHEN event_type='Shipment' AND charge_type='Principal'
                      THEN NULLIF(amount,'')::numeric ELSE 0 END) AS gross,
                  SUM(CASE WHEN event_type IN ('ShipmentFee','RefundFee')
                      THEN NULLIF(amount,'')::numeric ELSE 0 END) AS fees,
                  SUM(CASE WHEN event_type='Refund' AND charge_type='Principal'
                      THEN NULLIF(amount,'')::numeric ELSE 0 END) AS refs,
                  SUM(CASE WHEN event_type='ShipmentPromo'
                      THEN NULLIF(amount,'')::numeric ELSE 0 END) AS promos,
                  SUM(CASE WHEN event_type='Adjustment'
                      THEN NULLIF(amount,'')::numeric ELSE 0 END) AS adj
                FROM finance_events
                WHERE posted_date IS NOT NULL AND posted_date != ''
                  AND SUBSTRING(posted_date,1,10)::date >= CURRENT_DATE - INTERVAL '30 days'
            """), conn).iloc[0]
            gross = float(fr["gross"] or 0)
            fees  = float(fr["fees"]  or 0)
            refs  = float(fr["refs"]  or 0)
            promos= float(fr["promos"] or 0)
            adj   = float(fr["adj"]   or 0)
            net   = gross + fees + refs + promos + adj
            oc = pd.read_sql(text("SELECT COUNT(DISTINCT amazon_order_id) as cnt FROM orders WHERE SUBSTRING(purchase_date,1,10)::date >= CURRENT_DATE - INTERVAL '30 days'"), conn).iloc[0]
            fin_orders = int(oc["cnt"] or 0)
    except: pass

    margin_pct = net/gross*100 if gross > 0 else 0

    # ══════════════════════════════════════
    # 2. СКЛАД
    # ══════════════════════════════════════
    inv_sku=inv_units=inv_value=low14=oos=0
    if not df_filtered.empty:
        inv_sku   = len(df_filtered)
        inv_units = int(df_filtered['Available'].sum())
        inv_value = df_filtered['Stock Value'].sum()
        if 'Velocity' in df_filtered.columns:
            df_risk = df_filtered[df_filtered['Velocity'] > 0].copy()
            df_risk['_dos'] = (df_risk['Available'] / df_risk['Velocity']).round(0)
            low14 = int((df_risk['_dos'] < 14).sum())
        oos = int((df_filtered['Available'] == 0).sum())

    # ══════════════════════════════════════
    # 3. RETURNS + BUYBOX
    # ══════════════════════════════════════
    rr_pct=0; bb_pct=0; bb_lost=0
    try:
        with engine.connect() as conn:
            ret_r = pd.read_sql(text(
                "SELECT COUNT(DISTINCT order_id) as ret, "
                "(SELECT COUNT(DISTINCT amazon_order_id) FROM orders "
                " WHERE SUBSTRING(purchase_date,1,10)::date >= CURRENT_DATE - INTERVAL '30 days') as ord "
                "FROM fba_returns WHERE SUBSTRING(return_date::text,1,10)::date >= CURRENT_DATE - INTERVAL '30 days'"
            ), conn).iloc[0]
            rr_pct = float(ret_r["ret"] or 0) / float(ret_r["ord"] or 1) * 100
    except: pass
    try:
        with engine.connect() as conn:
            bb_r = pd.read_sql(text(
                "SELECT COUNT(*) as total, "
                "SUM(CASE WHEN is_buybox_winner=true OR is_buybox_winner='True' THEN 1 ELSE 0 END) as won "
                "FROM pricing_buybox"
            ), conn).iloc[0]
            total_bb = int(bb_r["total"] or 0)
            won_bb   = int(bb_r["won"]   or 0)
            bb_pct   = won_bb/total_bb*100 if total_bb > 0 else 0
            bb_lost  = total_bb - won_bb
    except: pass

    # ══════════════════════════════════════
    # HERO ROW — головні цифри
    # ══════════════════════════════════════
    margin_color = "#4CAF50" if margin_pct >= 50 else "#FFC107" if margin_pct >= 30 else "#F44336"
    rr_color     = "#4CAF50" if rr_pct <= 5 else "#FFC107" if rr_pct <= 10 else "#F44336"
    bb_color     = "#4CAF50" if bb_pct >= 80 else "#FFC107" if bb_pct >= 50 else "#F44336"
    stock_color  = "#F44336" if low14 > 0 else "#4CAF50"

    st.markdown(f"""
<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:20px">

  <div style="background:linear-gradient(135deg,#1a2b1e,#0d1f12);border:1px solid #2d4a30;
              border-radius:12px;padding:16px 20px">
    <div style="font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1px">💰 Виручка (30д)</div>
    <div style="font-size:36px;font-weight:900;color:#4CAF50;font-family:monospace;line-height:1.1">{_fmt(gross)}</div>
    <div style="font-size:12px;color:#aaa;margin-top:4px">Net <b style="color:{margin_color}">{_fmt(net)}</b> · Маржа <b style="color:{margin_color}">{margin_pct:.1f}%</b></div>
    <div style="font-size:11px;color:#666;margin-top:2px">Fees {_fmt(fees)} · Refunds {_fmt(refs)}</div>
  </div>

  <div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border:1px solid #2d2d4a;
              border-radius:12px;padding:16px 20px">
    <div style="font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1px">📦 Склад</div>
    <div style="font-size:36px;font-weight:900;color:#5B9BD5;font-family:monospace;line-height:1.1">{_fmt(inv_value)}</div>
    <div style="font-size:12px;color:#aaa;margin-top:4px">{inv_units:,} штук · {inv_sku} SKU</div>
    <div style="font-size:11px;color:{stock_color};margin-top:2px">{'🔴 ' + str(low14) + ' SKU <14д!' if low14 > 0 else '✅ Запаси в нормі'} · OOS: {oos}</div>
  </div>

  <div style="background:linear-gradient(135deg,#1a1a2e,#1f1a16);border:1px solid #3a2d1a;
              border-radius:12px;padding:16px 20px">
    <div style="font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1px">🛒 Замовлення (30д)</div>
    <div style="font-size:36px;font-weight:900;color:#FFC107;font-family:monospace;line-height:1.1">{fin_orders:,}</div>
    <div style="font-size:12px;color:#aaa;margin-top:4px">Returns <b style="color:{rr_color}">{rr_pct:.1f}%</b> · BuyBox <b style="color:{bb_color}">{bb_pct:.0f}%</b></div>
    <div style="font-size:11px;color:#666;margin-top:2px">BB Lost: {bb_lost} ASIN</div>
  </div>

</div>""", unsafe_allow_html=True)

    # ══════════════════════════════════════
    # МІНІ P&L WATERFALL
    # ══════════════════════════════════════
    st.markdown("#### 📊 P&L (30 днів)")
    col1, col2 = st.columns([2, 1])
    with col1:
        labels  = ["Gross", "Fees", "Refunds", "Promos", "Adj", "Net"]
        values  = [gross, fees, refs, promos, adj, net]
        measure = ["absolute","relative","relative","relative","relative","total"]
        fig_wf = go.Figure(go.Waterfall(
            orientation="v", measure=measure, x=labels, y=values,
            text=[_fmt(abs(v)) for v in values], textposition="outside",
            connector={"line":{"color":"rgba(128,128,128,0.3)","width":1}},
            increasing={"marker":{"color":"#4CAF50"}},
            decreasing={"marker":{"color":"#F44336"}},
            totals={"marker":{"color":"#4472C4"}},
        ))
        fig_wf.update_layout(height=280, margin=dict(l=0,r=0,t=20,b=0),
                             yaxis=dict(tickprefix="$", tickformat=".2s"))
        st.plotly_chart(fig_wf, width="stretch")

    with col2:
        st.markdown("**Зведення:**")
        rows = [
            ("💰 Gross",   _fmt(gross),  "#4CAF50"),
            ("💸 Fees",    _fmt(fees),   "#F44336"),
            ("🔄 Refunds", _fmt(refs),   "#F44336"),
            ("🎫 Promos",  _fmt(promos), "#FF9800"),
            ("✅ Net",     _fmt(net),    margin_color),
            ("📊 Маржа",   f"{margin_pct:.1f}%", margin_color),
        ]
        for label, val, color in rows:
            st.markdown(f'<div style="display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid #222"><span style="color:#888;font-size:13px">{label}</span><b style="color:{color};font-size:13px">{val}</b></div>', unsafe_allow_html=True)

    st.markdown("---")

    # ══════════════════════════════════════
    # LOW STOCK + ТОП SKU
    # ══════════════════════════════════════
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🚨 Low Stock (<14 днів)")
        if not df_filtered.empty and 'Velocity' in df_filtered.columns:
            df_risk = df_filtered[df_filtered['Velocity'] > 0].copy()
            df_risk['_dos'] = (df_risk['Available'] / df_risk['Velocity']).round(0)
            crit = df_risk[df_risk['_dos'] < 14].sort_values('_dos')
            if not crit.empty:
                cols_s = [c for c in ['SKU','Available','Velocity','_dos'] if c in crit.columns]
                st.dataframe(crit[cols_s].rename(columns={'_dos':'Days'}).head(8),
                             width="stretch", hide_index=True, height=250)
            else:
                st.success("✅ Всі SKU в нормі!")
        else:
            st.info("Немає даних Velocity")

    with col2:
        st.markdown("#### 🏆 Топ 5 SKU (продажі 30д)")
        try:
            with engine.connect() as conn:
                df_top = pd.read_sql(text(
                    "SELECT sku, SUM(item_price::numeric) as rev, COUNT(*) as orders "
                    "FROM orders WHERE SUBSTRING(purchase_date,1,10)::date >= CURRENT_DATE - INTERVAL '30 days' "
                    "AND item_price ~ '^[0-9.]+$' "
                    "GROUP BY sku ORDER BY rev DESC LIMIT 5"
                ), conn)
            if not df_top.empty:
                fig_top = go.Figure(go.Bar(
                    x=df_top["rev"], y=df_top["sku"], orientation="h",
                    marker_color="#4472C4",
                    text=[_fmt(v) for v in df_top["rev"]], textposition="outside"
                ))
                fig_top.update_layout(height=250, margin=dict(l=0,r=60,t=10,b=0),
                                      yaxis={"categoryorder":"total ascending"})
                st.plotly_chart(fig_top, width="stretch")
        except Exception as e:
            st.info(f"Немає даних: {e}")

    st.markdown("---")

    # ══════════════════════════════════════
    # RETURNS ТРЕНД + BUYBOX LOST + MoM
    # ══════════════════════════════════════
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("#### 🔴 Returns тренд")
        try:
            with engine.connect() as conn:
                df_ret_trend = pd.read_sql(text("""
                    SELECT m.month,
                        COALESCE(r.returns,0) AS returns,
                        COALESCE(o.orders,1) AS orders
                    FROM (
                        SELECT SUBSTRING(return_date::text,1,7) AS month
                        FROM fba_returns
                        WHERE SUBSTRING(return_date::text,1,10)::date >= CURRENT_DATE - INTERVAL '6 months'
                        GROUP BY 1
                    ) m
                    LEFT JOIN (
                        SELECT SUBSTRING(return_date::text,1,7) AS month, COUNT(DISTINCT order_id) AS returns
                        FROM fba_returns GROUP BY 1
                    ) r ON r.month = m.month
                    LEFT JOIN (
                        SELECT SUBSTRING(purchase_date,1,7) AS month, COUNT(DISTINCT amazon_order_id) AS orders
                        FROM orders GROUP BY 1
                    ) o ON o.month = m.month
                    ORDER BY 1
                """), conn)
            if not df_ret_trend.empty:
                df_ret_trend['rr'] = (df_ret_trend['returns'] / df_ret_trend['orders'].replace(0,1) * 100).clip(0,50).round(1)
                colors_ret = ['#F44336' if v > 10 else '#FFC107' if v > 5 else '#4CAF50' for v in df_ret_trend['rr']]
                fig_ret = go.Figure(go.Bar(
                    x=df_ret_trend['month'], y=df_ret_trend['rr'],
                    marker_color=colors_ret,
                    text=[f"{v:.1f}%" for v in df_ret_trend['rr']], textposition='outside',
                    customdata=df_ret_trend[['returns','orders']].values,
                    hovertemplate="<b>%{x}</b><br>Rate: %{y:.1f}%<br>Returns: %{customdata[0]}<br>Orders: %{customdata[1]}<extra></extra>"
                ))
                fig_ret.add_hline(y=8, line_dash="dash", line_color="#FFC107", annotation_text="8% норма")
                fig_ret.update_layout(height=220, margin=dict(l=0,r=0,t=20,b=0), yaxis_title="Return Rate %")
                st.plotly_chart(fig_ret, width="stretch")
            else:
                st.info("Немає даних за 6 місяців")
        except Exception as e:
            st.info(str(e))

    with col2:
        st.markdown("#### ⚠️ BuyBox Lost ASIN")
        try:
            with engine.connect() as conn:
                df_bb_lost = pd.read_sql(text(
                    "SELECT asin, price FROM pricing_buybox "
                    "WHERE is_buybox_winner::text NOT IN ('true','True','t','1','yes') "
                    "ORDER BY price DESC LIMIT 8"
                ), conn)
            if not df_bb_lost.empty:
                df_bb_lost['price'] = pd.to_numeric(df_bb_lost['price'], errors='coerce').fillna(0)
                st.dataframe(df_bb_lost.style.format({'price':'${:.2f}'}),
                             width="stretch", hide_index=True, height=220)
            else:
                st.success("✅ Всі ASIN мають Buy Box!")
        except Exception as e:
            st.info(str(e))

    with col3:
        st.markdown("#### 📊 Цей місяць vs минулий")
        try:
            with engine.connect() as conn:
                df_mom = pd.read_sql(text("""
                    SELECT
                        SUM(CASE WHEN SUBSTRING(purchase_date,1,7) = TO_CHAR(CURRENT_DATE,'YYYY-MM')
                            AND item_price ~ '^[0-9.]+$' THEN item_price::numeric ELSE 0 END) AS this_month,
                        SUM(CASE WHEN SUBSTRING(purchase_date,1,7) = TO_CHAR(CURRENT_DATE - INTERVAL '1 month','YYYY-MM')
                            AND item_price ~ '^[0-9.]+$' THEN item_price::numeric ELSE 0 END) AS last_month,
                        COUNT(DISTINCT CASE WHEN SUBSTRING(purchase_date,1,7) = TO_CHAR(CURRENT_DATE,'YYYY-MM')
                            THEN amazon_order_id END) AS this_orders,
                        COUNT(DISTINCT CASE WHEN SUBSTRING(purchase_date,1,7) = TO_CHAR(CURRENT_DATE - INTERVAL '1 month','YYYY-MM')
                            THEN amazon_order_id END) AS last_orders
                    FROM orders
                """), conn).iloc[0]
            this_m = float(df_mom['this_month'] or 0)
            last_m = float(df_mom['last_month'] or 0)
            chg    = (this_m - last_m) / last_m * 100 if last_m > 0 else 0
            chg_color = "#4CAF50" if chg >= 0 else "#F44336"
            chg_icon  = "📈" if chg >= 0 else "📉"
            st.markdown(f"""
<div style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:8px;padding:12px">
  <div style="font-size:11px;color:#888">Поточний місяць</div>
  <div style="font-size:28px;font-weight:800;color:#4CAF50">{_fmt(this_m)}</div>
  <div style="font-size:12px;color:#aaa">{int(df_mom['this_orders'] or 0):,} замовлень</div>
  <div style="margin-top:8px;font-size:11px;color:#888">Минулий місяць</div>
  <div style="font-size:20px;font-weight:700;color:#aaa">{_fmt(last_m)}</div>
  <div style="margin-top:8px;font-size:20px;font-weight:800;color:{chg_color}">{chg_icon} {chg:+.1f}%</div>
</div>""", unsafe_allow_html=True)
        except Exception as e:
            st.info(str(e))

    # Inbound shipments
    st.markdown("---")
    st.markdown("#### 🚚 Inbound shipments (активні)")
    try:
        with engine.connect() as conn:
            df_inb = pd.read_sql(text(
                "SELECT s.shipment_id, s.shipment_name, s.shipment_status, s.destination_fc, "
                "SUM(NULLIF(i.quantity_shipped,'')::numeric) as shipped, SUM(NULLIF(i.quantity_received,'')::numeric) as received "
                "FROM fba_shipments s "
                "LEFT JOIN fba_shipment_items i ON s.shipment_id = i.shipment_id "
                "WHERE s.shipment_status IN ('WORKING','SHIPPED','IN_TRANSIT','RECEIVING') "
                "GROUP BY 1,2,3,4 ORDER BY MAX(s.created_at) DESC LIMIT 10"
            ), conn)
        if not df_inb.empty:
            df_inb['received'] = df_inb['received'].fillna(0).astype(int)
            df_inb['shipped']  = df_inb['shipped'].fillna(0).astype(int)
            df_inb['%'] = (df_inb['received'] / df_inb['shipped'].replace(0,1) * 100).round(0).astype(int)
            st.dataframe(df_inb, width="stretch", hide_index=True, height=200)
        else:
            st.info("Немає активних відвантажень")
    except Exception as e:
        st.info(str(e))

    st.markdown("---")

    # ══════════════════════════════════════
    # ТРЕНДИ
    # ══════════════════════════════════════
    st.markdown("#### 📈 Тренди (30 днів)")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Денна виручка**")
        try:
            with engine.connect() as conn:
                df_daily = pd.read_sql(text(
                    "SELECT SUBSTRING(purchase_date,1,10)::date AS d, "
                    "SUM(item_price::numeric) AS rev "
                    "FROM orders "
                    "WHERE SUBSTRING(purchase_date,1,10)::date >= CURRENT_DATE - INTERVAL '30 days' "
                    "AND item_price ~ '^[0-9.]+$' "
                    "GROUP BY 1 ORDER BY 1"
                ), conn)
            if not df_daily.empty:
                fig_d = go.Figure(go.Bar(
                    x=df_daily["d"], y=df_daily["rev"],
                    marker_color="#4CAF50", opacity=0.85,
                    text=[_fmt(v) for v in df_daily["rev"]], textposition="outside"
                ))
                fig_d.update_layout(height=240, margin=dict(l=0,r=0,t=10,b=0),
                                    yaxis=dict(tickprefix="$", tickformat=".2s"))
                st.plotly_chart(fig_d, width="stretch")
        except Exception as e:
            st.error(f"Виручка: {e}")

    with col2:
        st.markdown("**Щоденні замовлення**")
        try:
            with engine.connect() as conn:
                df_ord_d = pd.read_sql(text(
                    "SELECT SUBSTRING(purchase_date,1,10)::date AS d, COUNT(DISTINCT amazon_order_id) AS cnt "
                    "FROM orders "
                    "WHERE SUBSTRING(purchase_date,1,10)::date >= CURRENT_DATE - INTERVAL '30 days' "
                    "GROUP BY 1 ORDER BY 1"
                ), conn)
            if not df_ord_d.empty:
                fig_od = go.Figure(go.Scatter(
                    x=df_ord_d["d"], y=df_ord_d["cnt"],
                    mode="lines+markers",
                    line=dict(color="#5B9BD5", width=2),
                    fill="tozeroy", fillcolor="rgba(91,155,213,0.15)"
                ))
                fig_od.update_layout(height=240, margin=dict(l=0,r=0,t=10,b=0))
                st.plotly_chart(fig_od, width="stretch")
        except Exception as e:
            st.error(f"Замовлення: {e}")

    st.markdown("---")

    # ══════════════════════════════════════
    # AI COMMAND CENTER
    # ══════════════════════════════════════
    ctx_cc = f"""Command Center — {dt.date.today()}
Фінанси (30д): Gross {_fmt(gross)} · Net {_fmt(net)} · Маржа {margin_pct:.1f}%
Склад: {inv_units:,} units · {inv_sku} SKU · Low Stock <14д: {low14} · OOS: {oos}
Замовлення (30д): {fin_orders:,} · Returns: {rr_pct:.1f}% · BuyBox: {bb_pct:.0f}% (lost: {bb_lost})"""
    show_ai_chat(ctx_cc, [
        "Що найбільше потребує уваги прямо зараз?",
        "Які SKU під ризиком out-of-stock найближчі 14 днів?",
        "Як покращити маржу з поточних показників?",
        "Де найбільше втрат — fees, refunds чи promotions?",
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

def show_sqp(t=None):
    """
    📊 Brand Analytics — Search Query Performance (SQP)
    - Hero KPI
    - Топ пошукові запити по click share
    - Наші ASIN: які отримують трафік
    - Конкуренти: хто ще кликається по наших запитах
    - Тренд click share по тижнях (коли є кілька тижнів)
    """
    if t is None: t = translations.get("UA", {})

    engine = get_engine()

    # ══════════════════════════════════════════════════════
    # 1. ЗАВАНТАЖЕННЯ ДАНИХ
    # ══════════════════════════════════════════════════════
    try:
        with engine.connect() as conn:
            # Всі наявні тижні
            weeks = pd.read_sql(text("""
                SELECT DISTINCT report_date
                FROM spapi.brand_analytics_sqp
                ORDER BY report_date DESC
            """), conn)

            if weeks.empty:
                st.warning("⚠️ Немає даних SQP. Запусти `python 16_brand_analytics_loader.py`")
                return

            available_weeks = weeks['report_date'].tolist()

            # Вибір тижня
            sel_week = st.selectbox(
                "📅 Тиждень:",
                available_weeks,
                format_func=lambda d: f"{d} → {d + pd.Timedelta(days=6)}",
                key="sqp_week"
            )

            # Дані за обраний тиждень
            df = pd.read_sql(text("""
                SELECT search_term, search_freq_rank,
                       asin_1, click_share_1, conv_share_1,
                       asin_2, click_share_2, conv_share_2,
                       asin_3, click_share_3, conv_share_3
                FROM spapi.brand_analytics_sqp
                WHERE report_date = :week
                ORDER BY search_freq_rank ASC
            """), conn, params={"week": sel_week})

            # Наші ASIN
            our_asins_df = pd.read_sql(text("""
                SELECT DISTINCT asin1 AS asin FROM listings_all
                WHERE asin1 IS NOT NULL AND asin1 != ''
            """), conn)
            our_asins = set(our_asins_df['asin'].tolist())

    except Exception as e:
        st.error(f"DB error: {e}")
        return

    if df.empty:
        st.info(f"Немає даних за тиждень {sel_week}")
        return

    # ── Numeric convert ──
    for c in ['search_freq_rank', 'click_share_1', 'conv_share_1',
              'click_share_2', 'conv_share_2', 'click_share_3', 'conv_share_3']:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    # ── Визначити позицію нашого ASIN (1, 2, 3 або none) ──
    def _our_position(row):
        for i in [1, 2, 3]:
            if row.get(f'asin_{i}', '') in our_asins:
                return i
        return 0

    df['our_position'] = df.apply(_our_position, axis=1)
    df['our_asin'] = df.apply(
        lambda r: r[f'asin_{r["our_position"]}'] if r['our_position'] > 0 else '', axis=1
    )
    df['our_click_share'] = df.apply(
        lambda r: r[f'click_share_{r["our_position"]}'] if r['our_position'] > 0 else 0, axis=1
    )
    df['our_conv_share'] = df.apply(
        lambda r: r[f'conv_share_{r["our_position"]}'] if r['our_position'] > 0 else 0, axis=1
    )

    # ══════════════════════════════════════════════════════
    # 2. HERO KPI
    # ══════════════════════════════════════════════════════
    total_terms     = len(df)
    pos1_count      = int((df['our_position'] == 1).sum())
    pos2_count      = int((df['our_position'] == 2).sum())
    pos3_count      = int((df['our_position'] == 3).sum())
    avg_click_share = df[df['our_click_share'] > 0]['our_click_share'].mean() * 100 if (df['our_click_share'] > 0).any() else 0
    avg_conv_share  = df[df['our_conv_share'] > 0]['our_conv_share'].mean() * 100 if (df['our_conv_share'] > 0).any() else 0
    best_rank       = int(df[df['our_position'] > 0]['search_freq_rank'].min()) if (df['our_position'] > 0).any() else 0
    unique_asins    = df[df['our_asin'] != '']['our_asin'].nunique()

    st.markdown(f"""
<div style="background:linear-gradient(135deg,#1a1e2e,#0d1222);border:1px solid #2d3a5a;
            border-radius:12px;padding:20px 28px;margin-bottom:16px;
            display:flex;align-items:center;gap:32px;flex-wrap:wrap">
  <div>
    <div style="font-size:12px;color:#888;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">
      📊 Search Query Performance
    </div>
    <div style="font-size:48px;font-weight:900;color:#5B9BD5;font-family:monospace;line-height:1">
      {total_terms:,}
    </div>
    <div style="font-size:12px;color:#666;margin-top:6px">search terms з нашими ASIN · тиждень {sel_week}</div>
  </div>
  <div style="flex:1;min-width:200px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <span style="background:#1e2e1e;border:1px solid #2d4a30;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🥇 #1 позиція <b style="color:#4CAF50">{pos1_count}</b>
    </span>
    <span style="background:#2b2b1a;border:1px solid #4a4a2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🥈 #2 <b style="color:#FFC107">{pos2_count}</b>
    </span>
    <span style="background:#2b1a1a;border:1px solid #4a2d2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🥉 #3 <b style="color:#FF9800">{pos3_count}</b>
    </span>
    <span style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📊 Avg CTR <b style="color:#AB47BC">{avg_click_share:.2f}%</b>
    </span>
    <span style="background:#1a2b2e;border:1px solid #2d404a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🔄 Avg CVR <b style="color:#5B9BD5">{avg_conv_share:.2f}%</b>
    </span>
  </div>
</div>""", unsafe_allow_html=True)

    # Інсайти
    _ic = st.columns(3)
    with _ic[0]:
        insight_card("🏆", "Найкращий ранг",
            f"Топ запит на позиції <b>#{best_rank:,}</b> з 2.7M+ запитів Amazon", "#1a2b1e")
    with _ic[1]:
        insight_card("📦", "ASINів в пошуку",
            f"<b>{unique_asins}</b> унікальних ASIN отримують кліки з пошуку", "#1e293b")
    with _ic[2]:
        if pos1_count >= 5:
            _em, _col = "🟢", "#0d2b1e"
            _txt = f"<b>{pos1_count}</b> запитів де ми #1 — хороша видимість"
        elif pos1_count >= 1:
            _em, _col = "🟡", "#2b2400"
            _txt = f"Тільки <b>{pos1_count}</b> запитів де ми #1 — є куди рости"
        else:
            _em, _col = "🔴", "#2b0d0d"
            _txt = "Ми ніде не #1 по кліках — фокус на SEO і PPC"
        insight_card(_em, "Позиція #1", _txt, _col)

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 3. ТОП ЗАПИТИ
    # ══════════════════════════════════════════════════════
    st.markdown("### 🔍 Топ пошукові запити")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 📊 По Click Share (наші найкращі)")
        top_cs = df[df['our_click_share'] > 0].nlargest(20, 'our_click_share').copy()
        if not top_cs.empty:
            top_cs['Click Share %'] = (top_cs['our_click_share'] * 100).round(2)
            top_cs['Conv Share %']  = (top_cs['our_conv_share'] * 100).round(2)

            fig_cs = go.Figure(go.Bar(
                x=top_cs['Click Share %'],
                y=top_cs['search_term'],
                orientation='h',
                marker_color=['#4CAF50' if p == 1 else '#FFC107' if p == 2 else '#FF9800'
                              for p in top_cs['our_position']],
                text=[f"{v:.2f}% (#{p})" for v, p in zip(top_cs['Click Share %'], top_cs['our_position'])],
                textposition='outside'
            ))
            fig_cs.update_layout(
                height=max(400, len(top_cs) * 25),
                yaxis={'categoryorder': 'total ascending'},
                xaxis_title='Click Share %',
                margin=dict(l=0, r=80, t=10, b=0)
            )
            st.plotly_chart(fig_cs, use_container_width=True)

    with col2:
        st.markdown("#### 🏆 По Search Frequency Rank (найпопулярніші)")
        top_rank = df[df['our_position'] > 0].nsmallest(20, 'search_freq_rank').copy()
        if not top_rank.empty:
            top_rank['Click Share %'] = (top_rank['our_click_share'] * 100).round(2)

            fig_rank = go.Figure(go.Bar(
                x=top_rank['search_freq_rank'],
                y=top_rank['search_term'],
                orientation='h',
                marker_color='#5B9BD5',
                text=[f"#{int(r):,}" for r in top_rank['search_freq_rank']],
                textposition='outside'
            ))
            fig_rank.update_layout(
                height=max(400, len(top_rank) * 25),
                yaxis={'categoryorder': 'total descending'},
                xaxis_title='Search Frequency Rank (lower = more popular)',
                margin=dict(l=0, r=80, t=10, b=0)
            )
            st.plotly_chart(fig_rank, use_container_width=True)

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 4. ПОВНА ТАБЛИЦЯ
    # ══════════════════════════════════════════════════════
    st.markdown("### 📋 Всі search terms")

    # Пошук
    search_q = st.text_input("🔍 Пошук по запиту:", "", key="sqp_search")
    df_show = df.copy()
    if search_q:
        df_show = df_show[df_show['search_term'].str.contains(search_q, case=False, na=False)]

    # Формуємо таблицю
    table = df_show[[
        'search_term', 'search_freq_rank', 'our_position', 'our_asin',
        'our_click_share', 'our_conv_share',
        'asin_1', 'click_share_1', 'asin_2', 'click_share_2', 'asin_3', 'click_share_3'
    ]].copy()

    table['our_click_share'] = (table['our_click_share'] * 100).round(2)
    table['our_conv_share']  = (table['our_conv_share'] * 100).round(2)
    table['click_share_1']   = (table['click_share_1'] * 100).round(2)
    table['click_share_2']   = (table['click_share_2'] * 100).round(2)
    table['click_share_3']   = (table['click_share_3'] * 100).round(2)

    table = table.rename(columns={
        'search_term':     'Search Term',
        'search_freq_rank': 'Rank',
        'our_position':    'Our Pos',
        'our_asin':        'Our ASIN',
        'our_click_share': 'Our CTR %',
        'our_conv_share':  'Our CVR %',
        'asin_1':          '#1 ASIN',
        'click_share_1':   '#1 CTR %',
        'asin_2':          '#2 ASIN',
        'click_share_2':   '#2 CTR %',
        'asin_3':          '#3 ASIN',
        'click_share_3':   '#3 CTR %',
    })

    # Кольорова підсвітка позиції
    def _pos_color(row):
        styles = [''] * len(row)
        pos_idx = list(row.index).index('Our Pos') if 'Our Pos' in row.index else -1
        if pos_idx >= 0:
            v = row['Our Pos']
            if v == 1:   styles[pos_idx] = 'background-color:#1a3d1a;color:#4CAF50;font-weight:bold'
            elif v == 2: styles[pos_idx] = 'background-color:#3d3d1a;color:#FFC107'
            elif v == 3: styles[pos_idx] = 'background-color:#3d2a1a;color:#FF9800'
        return styles

    st.dataframe(
        table.head(200).style.format({
            'Rank':     '{:,}',
            'Our CTR %': '{:.2f}%',
            'Our CVR %': '{:.2f}%',
            '#1 CTR %':  '{:.2f}%',
            '#2 CTR %':  '{:.2f}%',
            '#3 CTR %':  '{:.2f}%',
        }).apply(_pos_color, axis=1),
        use_container_width=True, hide_index=True, height=500
    )
    st.caption(f"{len(df_show)} terms" + (f" (фільтр: '{search_q}')" if search_q else ""))

    st.download_button("📥 CSV всі terms",
        df_show.to_csv(index=False).encode(),
        f"sqp_{sel_week}.csv", "text/csv", key="dl_sqp")

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 5. НАШІ ASIN — ХТО ОТРИМУЄ КЛІКИ
    # ══════════════════════════════════════════════════════
    st.markdown("### 📦 Наші ASIN в пошуку")
    st.caption("Скільки search terms і яка середня позиція / click share по кожному ASIN")

    asin_perf = df[df['our_asin'] != ''].groupby('our_asin').agg(
        terms_count=('search_term', 'count'),
        avg_position=('our_position', 'mean'),
        avg_click_share=('our_click_share', 'mean'),
        avg_conv_share=('our_conv_share', 'mean'),
        best_rank=('search_freq_rank', 'min'),
        pos1_count=('our_position', lambda x: (x == 1).sum()),
    ).reset_index().sort_values('terms_count', ascending=False)

    asin_perf['avg_click_share'] = (asin_perf['avg_click_share'] * 100).round(2)
    asin_perf['avg_conv_share']  = (asin_perf['avg_conv_share'] * 100).round(2)
    asin_perf['avg_position']    = asin_perf['avg_position'].round(1)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🏆 По кількості terms")
        top_asin = asin_perf.head(15)
        fig_a = go.Figure(go.Bar(
            x=top_asin['terms_count'], y=top_asin['our_asin'],
            orientation='h', marker_color='#5B9BD5',
            text=[f"{c} terms · CTR {cs:.2f}%" for c, cs in
                  zip(top_asin['terms_count'], top_asin['avg_click_share'])],
            textposition='outside'
        ))
        fig_a.update_layout(
            height=max(400, len(top_asin) * 30),
            yaxis={'categoryorder': 'total ascending'},
            margin=dict(l=0, r=120, t=10, b=0)
        )
        st.plotly_chart(fig_a, use_container_width=True)

    with col2:
        st.markdown("#### 📊 По Click Share")
        top_cs_asin = asin_perf.nlargest(15, 'avg_click_share')
        fig_ac = go.Figure(go.Bar(
            x=top_cs_asin['avg_click_share'], y=top_cs_asin['our_asin'],
            orientation='h',
            marker_color=['#4CAF50' if v >= 10 else '#FFC107' if v >= 5 else '#F44336'
                          for v in top_cs_asin['avg_click_share']],
            text=[f"{v:.2f}%" for v in top_cs_asin['avg_click_share']],
            textposition='outside'
        ))
        fig_ac.update_layout(
            height=max(400, len(top_cs_asin) * 30),
            yaxis={'categoryorder': 'total ascending'},
            xaxis_title='Avg Click Share %',
            margin=dict(l=0, r=80, t=10, b=0)
        )
        st.plotly_chart(fig_ac, use_container_width=True)

    st.dataframe(
        asin_perf.rename(columns={
            'our_asin': 'ASIN', 'terms_count': 'Terms',
            'avg_position': 'Avg Pos', 'avg_click_share': 'Avg CTR %',
            'avg_conv_share': 'Avg CVR %', 'best_rank': 'Best Rank',
            'pos1_count': '#1 Count'
        }).style.format({
            'Avg CTR %': '{:.2f}%', 'Avg CVR %': '{:.2f}%',
            'Avg Pos': '{:.1f}', 'Best Rank': '{:,}'
        }),
        use_container_width=True, hide_index=True, height=400
    )

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 6. КОНКУРЕНТИ — хто ще кликається по наших запитах
    # ══════════════════════════════════════════════════════
    st.markdown("### 🏴 Конкуренти по наших запитах")
    st.caption("ASINи інших брендів які теж отримують кліки на search terms де ми присутні")

    # Збираємо всі ASIN з 3 слотів (де не наш)
    competitor_rows = []
    for _, row in df[df['our_position'] > 0].iterrows():
        for i in [1, 2, 3]:
            asin_i = row.get(f'asin_{i}', '')
            if asin_i and asin_i not in our_asins:
                competitor_rows.append({
                    'asin': asin_i,
                    'click_share': row.get(f'click_share_{i}', 0),
                    'position': i,
                    'search_term': row['search_term'],
                    'search_rank': row['search_freq_rank'],
                })

    if competitor_rows:
        comp_df = pd.DataFrame(competitor_rows)
        comp_agg = comp_df.groupby('asin').agg(
            appearances=('search_term', 'count'),
            avg_click_share=('click_share', 'mean'),
            avg_position=('position', 'mean'),
            unique_terms=('search_term', 'nunique'),
            best_rank=('search_rank', 'min'),
            top_terms=('search_term', lambda x: ' · '.join(x.value_counts().head(5).index.tolist())),
        ).reset_index().sort_values('appearances', ascending=False)

        comp_agg['avg_click_share'] = (comp_agg['avg_click_share'] * 100).round(2)
        comp_agg['avg_position'] = comp_agg['avg_position'].round(1)

        # Топ 20 конкурентів
        st.markdown("#### 🏴 Топ 20 конкурентів")
        top_comp = comp_agg.head(20)

        fig_comp = go.Figure(go.Bar(
            x=top_comp['appearances'], y=top_comp['asin'],
            orientation='h', marker_color='#F44336',
            text=[f"{a} terms · CTR {cs:.1f}%" for a, cs in
                  zip(top_comp['appearances'], top_comp['avg_click_share'])],
            textposition='outside'
        ))
        fig_comp.update_layout(
            height=max(400, len(top_comp) * 30),
            yaxis={'categoryorder': 'total ascending'},
            xaxis_title='Appearances on our keywords',
            margin=dict(l=0, r=140, t=10, b=0)
        )
        st.plotly_chart(fig_comp, use_container_width=True)

        # ── HTML таблиця з клікабельними ASIN + запити ──
        st.markdown("#### 📋 Деталі конкурентів (клікни ASIN → Amazon)")

        comp_html = """
<style>
.comp-table { width:100%; border-collapse:collapse; font-size:13px;
              background:#0e1117; color:#fff; border-radius:8px; overflow:hidden; }
.comp-table th { background:#1a1a2e; color:#aaa; font-weight:600; padding:10px 12px;
                 text-align:left; border-bottom:2px solid #2d2d4a; font-size:11px;
                 text-transform:uppercase; }
.comp-table td { padding:8px 12px; border-bottom:1px solid #1f1f2e; vertical-align:top; }
.comp-table tr:hover { background:#1d1d2e; }
.comp-link { color:#5B9BD5; text-decoration:none; font-weight:600; font-family:monospace; }
.comp-link:hover { color:#82b1ff; text-decoration:underline; }
.comp-terms { color:#888; font-size:11px; line-height:1.6; margin-top:4px; }
.comp-badge { display:inline-block; background:#2b1a1a; border:1px solid #4a2d2d;
              border-radius:4px; padding:2px 8px; font-size:11px; color:#F44336;
              margin:2px 4px 2px 0; }
</style>
<table class='comp-table'>
<thead><tr>
  <th>Competitor ASIN</th>
  <th>Terms</th>
  <th>Avg CTR</th>
  <th>Avg Pos</th>
  <th>Запити де з'являється</th>
</tr></thead><tbody>
"""
        for _, row in comp_agg.head(30).iterrows():
            asin = row['asin']
            url = f"https://www.amazon.com/dp/{asin}"
            terms_list = row.get('top_terms', '')
            terms_badges = ''.join([
                f"<span class='comp-badge'>{t.strip()}</span>"
                for t in terms_list.split(' · ') if t.strip()
            ])

            comp_html += f"""
<tr>
  <td><a class='comp-link' href='{url}' target='_blank'>🔗 {asin}</a></td>
  <td style='text-align:center'>{int(row['appearances'])}</td>
  <td style='text-align:center'>{row['avg_click_share']:.2f}%</td>
  <td style='text-align:center'>{row['avg_position']:.1f}</td>
  <td><div class='comp-terms'>{terms_badges}</div></td>
</tr>
"""

        comp_html += "</tbody></table>"
        st.components.v1.html(comp_html, height=min(800, 60 + len(comp_agg.head(30)) * 55), scrolling=True)

        st.download_button("📥 CSV конкуренти",
            comp_agg.to_csv(index=False).encode(),
            f"competitors_{sel_week}.csv", "text/csv", key="dl_comp")
    else:
        st.info("Немає конкурентів на наших запитах")

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 7. ТРЕНД ПО ТИЖНЯХ (якщо є кілька тижнів)
    # ══════════════════════════════════════════════════════
    if len(available_weeks) > 1:
        st.markdown("### 📈 Тренд по тижнях")

        try:
            with engine.connect() as conn:
                trend_df = pd.read_sql(text("""
                    SELECT report_date,
                           COUNT(*) AS total_terms,
                           COUNT(*) FILTER (WHERE asin_1 IN (SELECT DISTINCT asin1 FROM listings_all WHERE asin1 != '')
                                             OR asin_2 IN (SELECT DISTINCT asin1 FROM listings_all WHERE asin1 != '')
                                             OR asin_3 IN (SELECT DISTINCT asin1 FROM listings_all WHERE asin1 != ''))
                               AS our_terms,
                           AVG(click_share_1) * 100 AS avg_cs
                    FROM spapi.brand_analytics_sqp
                    GROUP BY report_date
                    ORDER BY report_date
                """), conn)

            if not trend_df.empty and len(trend_df) > 1:
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=trend_df['report_date'], y=trend_df['our_terms'],
                    mode='lines+markers+text', name='Search terms',
                    text=trend_df['our_terms'], textposition='top center',
                    line=dict(color='#5B9BD5', width=3), marker=dict(size=10)
                ))
                fig_trend.update_layout(
                    height=350, margin=dict(l=0, r=0, t=30, b=0),
                    title="Кількість search terms з нашими ASIN по тижнях",
                    yaxis_title='Terms'
                )
                st.plotly_chart(fig_trend, use_container_width=True)
        except Exception as e:
            st.caption(f"Тренд недоступний: {e}")

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 8. AI CHAT
    # ══════════════════════════════════════════════════════
    top_terms_str = ", ".join(df.nsmallest(10, 'search_freq_rank')['search_term'].tolist())
    ctx = (f"Brand Analytics SQP: {total_terms} terms · "
           f"#1 позиція: {pos1_count} · Avg CTR: {avg_click_share:.2f}% · "
           f"Avg CVR: {avg_conv_share:.2f}% · ASINів: {unique_asins} · "
           f"Топ terms: {top_terms_str}")

    show_ai_chat(ctx, [
        "Які запити мають найвищий CTR?",
        "На яких запитах ми програємо конкурентам?",
        "Які нові ключовики варто додати в PPC?",
        "Порівняй CTR по категоріях товарів",
    ], "sqp")
    
def show_reviews(t=None):
    """
    ⭐ Amazon Reviews
    - Hero KPI + інсайти
    - Heatmap ASIN × Country
    - Фільтри: рейтинг, країна, ASIN
    - Worst/Best ASIN
    - Тексти відгуків (balanced до 100 на зірку)
    - AI чат
    """
    if t is None: t = translations.get("UA", {})

    df = load_reviews()
    if df.empty:
        st.warning("⚠️ Немає даних відгуків")
        return

    # ══════════════════════════════════════════════════════
    # 1. SIDEBAR ФІЛЬТРИ
    # ══════════════════════════════════════════════════════
    st.sidebar.markdown("### ⭐ Фільтри відгуків")

    # Країни
    all_countries = sorted(df['domain'].dropna().unique().tolist()) if 'domain' in df.columns else []
    country_labels = [DOMAIN_LABELS.get(c, c) for c in all_countries]
    country_map = dict(zip(country_labels, all_countries))

    sel_countries = st.sidebar.multiselect(
        "🌍 Країни:",
        country_labels,
        default=country_labels,
        key="rev_countries"
    )
    sel_domains = [country_map[c] for c in sel_countries] if sel_countries else all_countries

    # Рейтинг
    sel_stars = st.sidebar.multiselect(
        "⭐ Рейтинг:",
        [1, 2, 3, 4, 5],
        default=[1, 2, 3, 4, 5],
        key="rev_stars"
    )

    # Verified
    only_verified = st.sidebar.checkbox("✅ Тільки Verified", value=False, key="rev_verified")

    # ASIN
    search_asin = st.sidebar.text_input("🔍 ASIN", "", key="rev_asin_search")

    # ══════════════════════════════════════════════════════
    # 2. ФІЛЬТРАЦІЯ
    # ══════════════════════════════════════════════════════
    df_f = df.copy()
    if sel_domains and 'domain' in df_f.columns:
        df_f = df_f[df_f['domain'].isin(sel_domains)]
    if sel_stars:
        df_f = df_f[df_f['rating'].isin(sel_stars)]
    if only_verified and 'is_verified' in df_f.columns:
        df_f = df_f[df_f['is_verified'] == True]
    if search_asin:
        df_f = df_f[df_f['asin'].astype(str).str.contains(search_asin, case=False, na=False)]

    if df_f.empty:
        st.warning("Немає відгуків за фільтрами")
        return

    # ══════════════════════════════════════════════════════
    # 3. KPI — HERO CARD
    # ══════════════════════════════════════════════════════
    total_rev    = len(df_f)
    avg_rating   = df_f['rating'].mean()
    neg_cnt      = int((df_f['rating'] <= 2).sum())
    pos_cnt      = int((df_f['rating'] >= 4).sum())
    neg_pct      = neg_cnt / total_rev * 100 if total_rev > 0 else 0
    pos_pct      = pos_cnt / total_rev * 100 if total_rev > 0 else 0
    verified_pct = df_f['is_verified'].mean() * 100 if 'is_verified' in df_f.columns else 0
    unique_asins = df_f['asin'].nunique()
    unique_countries = df_f['domain'].nunique() if 'domain' in df_f.columns else 0

    rating_color = "#4CAF50" if avg_rating >= 4.3 else "#FFC107" if avg_rating >= 4.0 else "#F44336"

    st.markdown(f"""
<div style="background:linear-gradient(135deg,#1a2b1e,#0d1f12);border:1px solid #2d4a30;
            border-radius:12px;padding:20px 28px;margin-bottom:16px;
            display:flex;align-items:center;gap:32px;flex-wrap:wrap">
  <div>
    <div style="font-size:12px;color:#888;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">
      ⭐ Середній рейтинг
    </div>
    <div style="font-size:48px;font-weight:900;color:{rating_color};font-family:monospace;line-height:1">
      {avg_rating:.2f}★
    </div>
    <div style="font-size:12px;color:#666;margin-top:6px">{total_rev:,} відгуків · {unique_asins} ASIN · {unique_countries} країн</div>
  </div>
  <div style="flex:1;min-width:200px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <span style="background:#1e2e1e;border:1px solid #2d4a30;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🟢 Pos (4-5★) <b style="color:#4CAF50">{pos_pct:.1f}%</b>
    </span>
    <span style="background:#2b1a1a;border:1px solid #4a2d2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🔴 Neg (1-2★) <b style="color:#F44336">{neg_pct:.1f}%</b>
    </span>
    <span style="background:#1a2b2e;border:1px solid #2d404a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      ✅ Verified <b style="color:#5B9BD5">{verified_pct:.0f}%</b>
    </span>
    <span style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📦 ASINs <b style="color:#AB47BC">{unique_asins}</b>
    </span>
  </div>
</div>""", unsafe_allow_html=True)

    # ── Інсайти ──
    insights_reviews(df_f, asin=None)

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 4. РОЗПОДІЛ ЗІРОК + ТРЕНД
    # ══════════════════════════════════════════════════════
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 📊 Розподіл по зірках")
        star_cnt = df_f['rating'].value_counts().sort_index().reset_index()
        star_cnt.columns = ['Rating', 'Count']
        star_cnt['Label'] = star_cnt['Rating'].apply(lambda x: '★' * int(x))
        colors_star = ['#F44336', '#FF6B6B', '#FFC107', '#4CAF50', '#2E7D32']
        star_cnt['color'] = star_cnt['Rating'].apply(lambda x: colors_star[int(x)-1])

        fig = go.Figure(go.Bar(
            x=star_cnt['Label'], y=star_cnt['Count'],
            marker_color=star_cnt['color'],
            text=star_cnt['Count'], textposition='outside'
        ))
        fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=0),
                          xaxis_title='Rating', yaxis_title='Count')
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### 📈 Тренд рейтингу по місяцях")
        df_f['month'] = df_f['review_date'].dt.to_period('M').astype(str)
        monthly = df_f.groupby('month').agg(
            avg_rating=('rating', 'mean'),
            count=('rating', 'count')
        ).reset_index().tail(12)

        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=monthly['month'], y=monthly['count'],
            name='Відгуків', marker_color='#5B9BD5', opacity=0.6, yaxis='y2'
        ))
        fig2.add_trace(go.Scatter(
            x=monthly['month'], y=monthly['avg_rating'],
            name='Avg rating', mode='lines+markers+text',
            text=[f"{v:.2f}" for v in monthly['avg_rating']],
            textposition='top center',
            line=dict(color='#FFC107', width=3), marker=dict(size=10)
        ))
        fig2.update_layout(
            height=320, margin=dict(l=0, r=0, t=10, b=0),
            yaxis=dict(title='Rating', range=[1, 5]),
            yaxis2=dict(title='Count', overlaying='y', side='right'),
            legend=dict(orientation='h', y=1.12)
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 5. WORST / BEST ASIN
    # ══════════════════════════════════════════════════════
    asin_stats = df_f.groupby('asin').agg(
        count=('rating', 'count'),
        avg_rating=('rating', 'mean'),
        neg_pct=('rating', lambda x: (x <= 2).sum() / len(x) * 100),
    ).reset_index()
    asin_stats = asin_stats[asin_stats['count'] >= 5]  # мін 5 відгуків

    if not asin_stats.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 🔴 Worst ASINs (5+ відгуків)")
            worst = asin_stats.nsmallest(10, 'avg_rating')
            fig_w = go.Figure(go.Bar(
                x=worst['avg_rating'], y=worst['asin'], orientation='h',
                marker_color='#F44336',
                text=[f"{v:.2f}★ · {c} rev" for v, c in zip(worst['avg_rating'], worst['count'])],
                textposition='outside'
            ))
            fig_w.update_layout(height=max(350, len(worst) * 35),
                                yaxis={'categoryorder': 'total descending'},
                                xaxis=dict(range=[1, 5]),
                                margin=dict(l=0, r=120, t=10, b=0))
            st.plotly_chart(fig_w, use_container_width=True)

        with col2:
            st.markdown("#### 🟢 Best ASINs (5+ відгуків)")
            best = asin_stats.nlargest(10, 'avg_rating')
            fig_b = go.Figure(go.Bar(
                x=best['avg_rating'], y=best['asin'], orientation='h',
                marker_color='#4CAF50',
                text=[f"{v:.2f}★ · {c} rev" for v, c in zip(best['avg_rating'], best['count'])],
                textposition='outside'
            ))
            fig_b.update_layout(height=max(350, len(best) * 35),
                                yaxis={'categoryorder': 'total ascending'},
                                xaxis=dict(range=[1, 5]),
                                margin=dict(l=0, r=120, t=10, b=0))
            st.plotly_chart(fig_b, use_container_width=True)

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 6. HEATMAP ASIN × COUNTRY
    # ══════════════════════════════════════════════════════
    if 'domain' in df_f.columns and df_f['domain'].nunique() > 1:
        st.markdown("### 🔥 Heatmap: ASIN × Country")
        st.caption("Клітинки = середній рейтинг · мінімум 3 відгуки для показу")

        # Top 20 ASINs за кількістю
        top_asins = df_f['asin'].value_counts().head(20).index.tolist()
        df_heat = df_f[df_f['asin'].isin(top_asins)]

        heat = df_heat.groupby(['asin', 'domain']).agg(
            avg_r=('rating', 'mean'),
            cnt=('rating', 'count')
        ).reset_index()
        heat = heat[heat['cnt'] >= 3]

        if not heat.empty:
            pivot = heat.pivot(index='asin', columns='domain', values='avg_r')

            fig_heat = go.Figure(data=go.Heatmap(
                z=pivot.values,
                x=pivot.columns,
                y=pivot.index,
                colorscale='RdYlGn',
                zmin=1, zmax=5,
                text=pivot.values,
                texttemplate='%{text:.1f}',
                textfont={"size": 11},
                colorbar=dict(title='Rating')
            ))
            fig_heat.update_layout(
                height=max(400, len(pivot) * 25),
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title='Country', yaxis_title='ASIN'
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 7. ПО КРАЇНАХ
    # ══════════════════════════════════════════════════════
    if 'domain' in df_f.columns and df_f['domain'].nunique() > 1:
        st.markdown("### 🌍 Аналіз по країнах")

        country_stats = df_f.groupby('domain').agg(
            count=('rating', 'count'),
            avg_rating=('rating', 'mean'),
            neg_pct=('rating', lambda x: (x <= 2).sum() / len(x) * 100),
            pos_pct=('rating', lambda x: (x >= 4).sum() / len(x) * 100),
        ).reset_index().sort_values('count', ascending=False)
        country_stats['Label'] = country_stats['domain'].map(DOMAIN_LABELS).fillna(country_stats['domain'])

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("#### 📊 Кількість")
            fig_c1 = px.bar(country_stats, x='count', y='Label', orientation='h',
                            color='count', color_continuous_scale='Blues',
                            text='count', height=350)
            fig_c1.update_layout(yaxis={'categoryorder': 'total ascending'},
                                 showlegend=False, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig_c1, use_container_width=True)

        with col2:
            st.markdown("#### ⭐ Рейтинг")
            fig_c2 = go.Figure(go.Bar(
                x=country_stats['avg_rating'], y=country_stats['Label'],
                orientation='h',
                marker_color=['#4CAF50' if v >= 4.3 else '#FFC107' if v >= 4 else '#F44336'
                              for v in country_stats['avg_rating']],
                text=[f"{v:.2f}★" for v in country_stats['avg_rating']],
                textposition='outside'
            ))
            fig_c2.update_layout(height=350, xaxis=dict(range=[1, 5]),
                                 yaxis={'categoryorder': 'total ascending'},
                                 margin=dict(l=0, r=60, t=10, b=0))
            st.plotly_chart(fig_c2, use_container_width=True)

        with col3:
            st.markdown("#### 🔴 % Негативу")
            fig_c3 = go.Figure(go.Bar(
                x=country_stats['neg_pct'], y=country_stats['Label'],
                orientation='h',
                marker_color=['#4CAF50' if v <= 10 else '#FFC107' if v <= 20 else '#F44336'
                              for v in country_stats['neg_pct']],
                text=[f"{v:.1f}%" for v in country_stats['neg_pct']],
                textposition='outside'
            ))
            fig_c3.update_layout(height=350,
                                 yaxis={'categoryorder': 'total ascending'},
                                 margin=dict(l=0, r=60, t=10, b=0))
            st.plotly_chart(fig_c3, use_container_width=True)

        st.dataframe(
            country_stats[['Label', 'count', 'avg_rating', 'pos_pct', 'neg_pct']].rename(columns={
                'Label': 'Країна', 'count': 'Відгуків',
                'avg_rating': 'Avg ★', 'pos_pct': '% Pos', 'neg_pct': '% Neg'
            }).style.format({
                'Avg ★': '{:.2f}', '% Pos': '{:.1f}%', '% Neg': '{:.1f}%'
            }),
            use_container_width=True, hide_index=True
        )

        st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 8. ТЕКСТИ ВІДГУКІВ
    # ══════════════════════════════════════════════════════
    st.markdown("### 📋 Тексти відгуків")
    st.caption("Balanced вибірка: до 100 на кожну зірку · 1★ зверху щоб проблеми були першими")

    # Balanced за зірками, 1★ зверху
    parts = []
    for s in [1, 2, 3, 4, 5]:
        parts.append(df_f[df_f['rating'] == s].head(100))
    df_balanced = pd.concat(parts, ignore_index=True) if parts else df_f

    show_cols = [c for c in ['review_date', 'rating', 'asin', 'domain', 'title',
                              'content', 'author', 'is_verified']
                 if c in df_balanced.columns]

    df_show = df_balanced[show_cols].copy()
    if 'domain' in df_show.columns:
        df_show['Country'] = df_show['domain'].map(DOMAIN_LABELS).fillna(df_show['domain'])
        df_show = df_show.drop(columns=['domain'])
    if 'review_date' in df_show.columns:
        df_show['review_date'] = df_show['review_date'].dt.strftime('%Y-%m-%d')

    df_show = df_show.rename(columns={
        'review_date': 'Date', 'rating': '★', 'asin': 'ASIN',
        'title': 'Title', 'content': 'Content', 'author': 'Author',
        'is_verified': '✅'
    })

    st.dataframe(df_show, use_container_width=True, hide_index=True, height=500)
    st.caption(f"Показано {len(df_show):,} з {len(df_f):,} відгуків")

    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            "📥 CSV (balanced)",
            df_balanced.to_csv(index=False).encode(),
            "reviews_balanced.csv", "text/csv"
        )
    with col2:
        st.download_button(
            "📥 CSV (всі за фільтром)",
            df_f.to_csv(index=False).encode(),
            "reviews_all.csv", "text/csv"
        )

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 9. AI CHAT
    # ══════════════════════════════════════════════════════
    ctx = (f"Reviews: {total_rev:,} відгуків | Avg {avg_rating:.2f}★ | "
           f"Neg {neg_pct:.1f}% | Pos {pos_pct:.1f}% | Verified {verified_pct:.0f}% | "
           f"ASINs {unique_asins} | Countries {unique_countries}")
    show_ai_chat(ctx, [
        "Які ASINи мають найбільше негативних відгуків?",
        "Головні скарги в 1-2★ відгуках",
        "Різниця між US і DE відгуками",
        "Що найчастіше хвалять в 5★ відгуках?",
    ], "reviews")


def show_orders(t=None):
    """
    🛒 Продажі (Orders) v2.0
    Матриця: день × ASIN / Parent ASIN
    - Продажі (units, revenue)
    - Impressions (page_views з sales_traffic)
    - Offer type (купон/прайм/дил)
    - CVR (units / page_views × 100)
    """
    if t is None: t = translations.get("UA", {})

    engine = get_engine()

    # ══════════════════════════════════════════════════════
    # 1. SIDEBAR ФІЛЬТРИ
    # ══════════════════════════════════════════════════════
    try:
        with engine.connect() as conn:
            bounds = pd.read_sql(text("""
                SELECT MIN(SUBSTRING(purchase_date,1,10))::date AS mn,
                       MAX(SUBSTRING(purchase_date,1,10))::date AS mx
                FROM orders
                WHERE purchase_date IS NOT NULL AND purchase_date != ''
            """), conn).iloc[0]
        min_date = bounds['mn']
        max_date = bounds['mx']
    except Exception:
        min_date = dt.date(2024, 1, 1)
        max_date = dt.date.today()

    date_range = st.sidebar.date_input(
        "📅 Період:",
        value=(max(min_date, max_date - dt.timedelta(days=30)), max_date),
        min_value=min_date, max_value=max_date, key="ord_v2_date"
    )
    if len(date_range) != 2:
        st.warning("Оберіть діапазон дат"); return
    d1, d2 = str(date_range[0]), str(date_range[1])

    search_asin = st.sidebar.text_input("🔍 ASIN", "", key="ord_v2_asin")
    search_sku  = st.sidebar.text_input("🔍 SKU", "", key="ord_v2_sku")
    gran = st.sidebar.selectbox("📊 Гранулярність:", ["День", "Тиждень"], key="ord_v2_gran")

    st.markdown("### 🛒 Продажі (Orders)")

    # ══════════════════════════════════════════════════════
    # 2. ЗАВАНТАЖЕННЯ ДАНИХ
    # ══════════════════════════════════════════════════════
    where_extra = ""
    params = {"d1": d1, "d2": d2}
    if search_asin:
        where_extra += " AND o.asin ILIKE :asin_q"
        params["asin_q"] = f"%{search_asin}%"
    if search_sku:
        where_extra += " AND o.sku ILIKE :sku_q"
        params["sku_q"] = f"%{search_sku}%"

    try:
        with engine.connect() as conn:
            df_orders = pd.read_sql(text(f"""
                SELECT
                    SUBSTRING(o.purchase_date, 1, 10)::date  AS date,
                    o.asin,
                    MIN(o.sku)                               AS sku,
                    SUM(CASE WHEN o.quantity ~ '^[0-9]+$'
                        THEN o.quantity::numeric ELSE 1 END)  AS units,
                    SUM(CASE WHEN o.item_price ~ '^[0-9.]+$'
                        THEN o.item_price::numeric ELSE 0 END) AS revenue,
                    COUNT(DISTINCT o.amazon_order_id)         AS orders_cnt,
                    STRING_AGG(DISTINCT NULLIF(o.promotion_ids,''), ', ') AS promo_ids,
                    STRING_AGG(DISTINCT NULLIF(o.price_designation,''), ', ') AS price_desig
                FROM orders o
                WHERE SUBSTRING(o.purchase_date, 1, 10)::date BETWEEN :d1 AND :d2
                  AND o.purchase_date IS NOT NULL AND o.purchase_date != ''
                  AND o.asin IS NOT NULL AND o.asin != ''
                  {where_extra}
                GROUP BY 1, 2
                ORDER BY 1 DESC, revenue DESC
            """), conn, params=params)

            df_traffic = pd.read_sql(text("""
                SELECT
                    report_date::date                        AS date,
                    child_asin                               AS asin,
                    MAX(parent_asin)                         AS parent_asin,
                    SUM(NULLIF(page_views,'')::numeric)      AS impressions,
                    SUM(NULLIF(sessions,'')::numeric)        AS sessions
                FROM spapi.sales_traffic
                WHERE report_date != '' AND report_date IS NOT NULL
                  AND child_asin != '' AND child_asin IS NOT NULL
                  AND granularity = 'SKU'
                  AND report_date::date BETWEEN :d1 AND :d2
                GROUP BY 1, 2
            """), conn, params={"d1": d1, "d2": d2})

    except Exception as e:
        st.error(f"Помилка завантаження: {e}")
        return

    if df_orders.empty:
        st.warning("Немає замовлень за обраний період"); return

    # ── Нормалізація типів ──
    df_orders['date'] = pd.to_datetime(df_orders['date'])
    for c in ['units', 'revenue', 'orders_cnt']:
        df_orders[c] = pd.to_numeric(df_orders[c], errors='coerce').fillna(0)

    if not df_traffic.empty:
        df_traffic['date'] = pd.to_datetime(df_traffic['date'])
        for c in ['impressions', 'sessions']:
            df_traffic[c] = pd.to_numeric(df_traffic[c], errors='coerce').fillna(0)

    # ── JOIN: orders + traffic ──
    if not df_traffic.empty:
        df = df_orders.merge(
            df_traffic[['date', 'asin', 'parent_asin', 'impressions', 'sessions']],
            on=['date', 'asin'], how='left'
        )
    else:
        df = df_orders.copy()
        df['parent_asin'] = ''
        df['impressions'] = 0
        df['sessions'] = 0

    df['impressions'] = df['impressions'].fillna(0)
    df['sessions']    = df['sessions'].fillna(0)
    df['parent_asin'] = df['parent_asin'].fillna('')

    # ── Offer type icon ──
    # Системні промо Amazon які НЕ є реальними акціями
    _SYSTEM_PROMOS = {'amazon asin', 'free shipping', 'core', 'automatedshipping',
                      'shipping', 'auto', 'none', 'na', 'nan', ''}

    def _offer_icon(promo, desig):
        promo = str(promo).strip() if promo else ''
        desig = str(desig).strip() if desig else ''
        # Фільтруємо системні промо
        promo_parts = [p.strip() for p in promo.split(',')]
        real_promos = [p for p in promo_parts if p.lower() not in _SYSTEM_PROMOS]
        promo_joined = ' '.join(real_promos).lower()
        desig_l = desig.lower()
        icons = []
        if 'coupon' in promo_joined or 'voucher' in promo_joined:  icons.append('🏷️')
        if 'lightning' in promo_joined or 'deal' in promo_joined:  icons.append('⚡')
        if 'prime' in desig_l and 'exclusive' in desig_l:          icons.append('🅿️')
        if 'sns' in promo_joined or 'subscribe' in promo_joined:   icons.append('🔄')
        # Тільки якщо є реальні промо і жоден конкретний тип не спрацював
        if real_promos and not icons:                              icons.append('🎫')
        return ' '.join(icons) if icons else ''

    df['offer'] = df.apply(lambda r: _offer_icon(r.get('promo_ids'), r.get('price_desig')), axis=1)

    # ── CVR ──
    df['cvr'] = (df['units'] / df['sessions'].replace(0, float('nan')) * 100).fillna(0).round(2)

    # ══════════════════════════════════════════════════════
    # 3. KPI — HERO CARD
    # ══════════════════════════════════════════════════════
    total_rev     = df['revenue'].sum()
    total_units   = int(df['units'].sum())
    total_orders  = int(df['orders_cnt'].sum())
    total_sess    = int(df['sessions'].sum())
    days_span     = max((df['date'].max() - df['date'].min()).days, 1)
    rev_per_day   = total_rev / days_span
    unique_asins  = df['asin'].nunique()
    avg_price     = total_rev / total_units if total_units > 0 else 0
    promo_orders  = int((df['offer'] != '').sum())
    promo_pct     = promo_orders / len(df) * 100 if len(df) > 0 else 0

    # ── REAL CVR: на рівні parent_asin ──
    # У Amazon S&T report поле page_views показується по кожному child SKU,
    # але продажі розподілені нерівномірно. Для реального CVR бренду/родини
    # треба SUM всіх impressions (by parent) / SUM всіх units (by parent).
    # Беремо traffic без JOIN з orders (бо dead SKUs не в orders).
    try:
        with engine.connect() as conn:
            parent_traffic = pd.read_sql(text("""
                SELECT
                    SUM(NULLIF(page_views,'')::numeric)    AS total_pv,
                    SUM(NULLIF(sessions,'')::numeric)       AS total_sess,
                    SUM(NULLIF(units_ordered,'')::numeric)  AS total_units_traffic
                FROM spapi.sales_traffic
                WHERE report_date::date BETWEEN :d1 AND :d2
                  AND child_asin != '' AND child_asin IS NOT NULL
                  AND granularity = 'SKU'
            """), conn, params={"d1": d1, "d2": d2}).iloc[0]

        traffic_total_pv    = int(parent_traffic['total_pv'] or 0)
        traffic_total_sess  = int(parent_traffic['total_sess'] or 0)
        traffic_total_units = int(parent_traffic['total_units_traffic'] or 0)
    except Exception:
        traffic_total_pv = int(df['impressions'].sum())
        traffic_total_sess = int(df['sessions'].sum())
        traffic_total_units = total_units

    total_impr = traffic_total_pv  # повна сума impressions (не тільки по SKU з orders)
    # CVR по page_views — units з traffic (щоб метчилось з impressions)
    cvr_pv   = (traffic_total_units / traffic_total_pv * 100) if traffic_total_pv > 0 else 0
    # CVR по sessions — це класичний Amazon CVR
    cvr_sess = (traffic_total_units / traffic_total_sess * 100) if traffic_total_sess > 0 else 0
    avg_cvr  = cvr_sess  # в KPI картці показуємо sessions-based (це те що в Seller Central)

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
    <div style="font-size:12px;color:#666;margin-top:6px">{d1} → {d2} · {unique_asins} ASIN</div>
  </div>
  <div style="flex:1;min-width:200px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <span style="background:#1e2e1e;border:1px solid #2d4a30;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📦 Units <b style="color:#4CAF50">{total_units:,}</b>
    </span>
    <span style="background:#1a2b2e;border:1px solid #2d404a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      🛒 Orders <b style="color:#5B9BD5">{total_orders:,}</b>
    </span>
    <span style="background:#2b2b1a;border:1px solid #4a4a2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      👁 Sessions <b style="color:#FFC107">{traffic_total_sess:,}</b>
    </span>
    <span style="background:#2b2b1a;border:1px solid #4a4a2d;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📄 Impr <b style="color:#FFC107">{total_impr:,}</b>
    </span>
    <span style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      📊 CVR <b style="color:#AB47BC">{cvr_sess:.2f}%</b>
      <span style="color:#666;font-size:11px">(units/sess)</span>
    </span>
    <span style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:6px;padding:6px 12px;font-size:13px;color:#fff">
      💵 Avg <b style="color:#FF9800">${avg_price:.2f}</b>
    </span>
  </div>
</div>""", unsafe_allow_html=True)

    # ── Інсайти ──
    _ic = st.columns(3)
    with _ic[0]:
        insight_card("📈", "Дохід/день",
            f"<b>{_fmt(rev_per_day)}</b>/день · прогноз місяць: <b>{_fmt(rev_per_day * 30)}</b>", "#1a2b1e")
    with _ic[1]:
        # Sessions-based CVR — норма Amazon 8-15%, низько <5%, добре >12%
        if avg_cvr >= 12:   _txt, _em, _col = f"CVR <b>{avg_cvr:.2f}%</b> — вище середнього по Amazon!", "🟢", "#0d2b1e"
        elif avg_cvr >= 5:  _txt, _em, _col = f"CVR <b>{avg_cvr:.2f}%</b> — в нормі (Amazon avg 10%).", "🟡", "#2b2400"
        else:               _txt, _em, _col = f"CVR <b>{avg_cvr:.2f}%</b> — нижче норми. Перевір фото/ціну.", "🔴", "#2b0d0d"
        insight_card(_em, "Конверсія (Units/Sessions)", _txt, _col)
    with _ic[2]:
        insight_card("🎫", "Промо-замовлення",
            f"<b>{promo_pct:.0f}%</b> рядків мають промо (купон/дил/прайм)", "#1a1a2e")

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 4. ТРЕНДИ ПО ДНЯХ
    # ══════════════════════════════════════════════════════
    st.markdown("### 📈 Щоденна динаміка")

    daily = df.groupby('date').agg(
        units=('units', 'sum'), revenue=('revenue', 'sum'),
        orders_cnt=('orders_cnt', 'sum'), impressions=('impressions', 'sum'),
        sessions=('sessions', 'sum'),
    ).reset_index()
    # CVR standard = units/sessions (Amazon Seller Central metric)
    daily['cvr'] = (daily['units'] / daily['sessions'].replace(0, float('nan')) * 100).fillna(0).round(2)

    if gran == "Тиждень":
        daily = daily.set_index('date').resample('W').agg({
            'units': 'sum', 'revenue': 'sum', 'orders_cnt': 'sum',
            'impressions': 'sum', 'sessions': 'sum'
        }).reset_index()
        daily['cvr'] = (daily['units'] / daily['sessions'].replace(0, float('nan')) * 100).fillna(0).round(2)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 💰 Виручка & Units")
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=daily['date'], y=daily['revenue'],
            name='Revenue', marker_color='#4CAF50', opacity=0.85,
            text=[_fmt(v) for v in daily['revenue']], textposition='outside'
        ))
        fig.add_trace(go.Scatter(
            x=daily['date'], y=daily['units'],
            name='Units', mode='lines+markers',
            line=dict(color='#FFC107', width=2), yaxis='y2'
        ))
        if gran == "День" and len(daily) >= 7:
            ma = daily['revenue'].rolling(7, min_periods=1).mean()
            fig.add_trace(go.Scatter(
                x=daily['date'], y=ma, name='MA-7',
                mode='lines', line=dict(color='#AB47BC', width=1.5, dash='dot')
            ))
        fig.update_layout(
            height=380, margin=dict(l=0, r=0, t=10, b=0),
            yaxis=dict(title='Revenue $', tickprefix='$'),
            yaxis2=dict(title='Units', overlaying='y', side='right'),
            legend=dict(orientation='h', y=1.12)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### 👁 Impressions & CVR")
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=daily['date'], y=daily['impressions'],
            name='Impressions', marker_color='#5B9BD5', opacity=0.8
        ))
        fig2.add_trace(go.Scatter(
            x=daily['date'], y=daily['cvr'],
            name='CVR %', mode='lines+markers+text',
            text=[f"{v:.1f}%" for v in daily['cvr']], textposition='top center',
            line=dict(color='#FF6B6B', width=2.5), yaxis='y2'
        ))
        fig2.update_layout(
            height=380, margin=dict(l=0, r=0, t=10, b=0),
            yaxis=dict(title='Impressions'),
            yaxis2=dict(title='CVR %', overlaying='y', side='right'),
            legend=dict(orientation='h', y=1.12)
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 5. ЗВЕДЕНА ТАБЛИЦЯ ПО ASIN
    # ══════════════════════════════════════════════════════
    st.markdown("### 🏆 Зведена по ASIN")

    asin_agg = df.groupby(['asin', 'parent_asin']).agg(
        sku=('sku', 'first'), units=('units', 'sum'), revenue=('revenue', 'sum'),
        orders_cnt=('orders_cnt', 'sum'), impressions=('impressions', 'sum'),
        sessions=('sessions', 'sum'),
    ).reset_index()
    # CVR = units / sessions * 100 (стандарт Amazon Seller Central = unit_session_percentage)
    asin_agg['cvr'] = (asin_agg['units'] / asin_agg['sessions'].replace(0, float('nan')) * 100).fillna(0).round(2)
    asin_agg['avg_price'] = (asin_agg['revenue'] / asin_agg['units'].replace(0, float('nan'))).fillna(0).round(2)
    asin_agg = asin_agg.sort_values('revenue', ascending=False)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 💰 Топ 15 ASIN за виручкою")
        top15 = asin_agg.nlargest(15, 'revenue')
        fig3 = go.Figure(go.Bar(
            x=top15['revenue'], y=top15['asin'], orientation='h',
            marker_color='#4CAF50',
            text=[_fmt(v) for v in top15['revenue']], textposition='outside'
        ))
        fig3.update_layout(height=max(350, len(top15) * 35),
                           yaxis={'categoryorder': 'total ascending'},
                           margin=dict(l=0, r=80, t=10, b=0))
        st.plotly_chart(fig3, use_container_width=True)

    with col2:
        st.markdown("#### 📊 CVR по ASIN (units/sessions)")
        cvr_df = asin_agg[asin_agg['sessions'] > 0].nlargest(15, 'sessions')
        if not cvr_df.empty:
            colors_cvr = ['#4CAF50' if v >= 10 else '#FFC107' if v >= 5 else '#F44336' for v in cvr_df['cvr']]
            fig4 = go.Figure(go.Bar(
                x=cvr_df['cvr'], y=cvr_df['asin'], orientation='h',
                marker_color=colors_cvr,
                text=[f"{v:.1f}%" for v in cvr_df['cvr']], textposition='outside'
            ))
            fig4.add_vline(x=10, line_dash="dash", line_color="#FFC107", annotation_text="Amazon avg 10%")
            fig4.update_layout(height=max(350, len(cvr_df) * 35),
                               yaxis={'categoryorder': 'total ascending'},
                               xaxis_title='CVR % (units/sessions)',
                               margin=dict(l=0, r=60, t=10, b=0))
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("Немає даних impressions з sales_traffic")

    # ── Таблиця ASIN ──
    st.markdown("#### 📋 Зведена таблиця по ASIN")
    show_asin = asin_agg.rename(columns={
        'asin': 'ASIN', 'parent_asin': 'Parent', 'sku': 'SKU',
        'units': 'Units', 'revenue': 'Revenue', 'orders_cnt': 'Orders',
        'impressions': 'Impr', 'sessions': 'Sessions',
        'cvr': 'CVR %', 'avg_price': 'Avg Price'
    })
    # Конвертуємо числові в int для коректного відображення
    for c in ['Units', 'Orders', 'Impr', 'Sessions']:
        if c in show_asin.columns:
            show_asin[c] = pd.to_numeric(show_asin[c], errors='coerce').fillna(0).astype(int)

    sa_head = show_asin.head(200).copy()
    t_units_sa = int(sa_head['Units'].sum())
    t_sess_sa  = int(sa_head['Sessions'].sum())
    t_rev_sa   = float(sa_head['Revenue'].sum())
    asin_total_row = pd.DataFrame([{
        'ASIN':     f'📊 TOTAL ({len(sa_head)})',
        'Parent':   '',
        'SKU':      '',
        'Units':    t_units_sa,
        'Revenue':  t_rev_sa,
        'Orders':   int(sa_head['Orders'].sum()),
        'Impr':     int(sa_head['Impr'].sum()),
        'Sessions': t_sess_sa,
        'CVR %':    round(t_units_sa / t_sess_sa * 100, 2) if t_sess_sa > 0 else 0,
        'Avg Price': round(t_rev_sa / t_units_sa, 2) if t_units_sa > 0 else 0,
    }])
    sa_with_total = pd.concat([sa_head, asin_total_row], ignore_index=True)

    def _highlight_total_asin(row):
        if str(row.get('ASIN', '')).startswith('📊 TOTAL'):
            return ['background-color:#1a2b1e;color:#4CAF50;font-weight:bold'] * len(row)
        return [''] * len(row)

    st.dataframe(
        sa_with_total.style.format({
            'Units':     '{:,}',
            'Orders':    '{:,}',
            'Impr':      '{:,}',
            'Sessions':  '{:,}',
            'Revenue':   '${:,.0f}',
            'Avg Price': '${:.2f}',
            'CVR %':     '{:.2f}%'
        }).apply(_highlight_total_asin, axis=1),
        use_container_width=True, hide_index=True, height=440
    )
    st.caption(f"{len(asin_agg)} унікальних ASIN")

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 5.5 DEAD SKU ANALYSIS — трафік є, продажів майже немає
    # ══════════════════════════════════════════════════════
    st.markdown("### ⚠️ Dead SKUs — трафік без продажів")
    st.caption("SKU з великим трафіком але низькою конверсією — OOS, проблеми з листингом або невдалі варіанти")

    try:
        with engine.connect() as conn:
            dead_sku_df = pd.read_sql(text("""
                WITH traffic_agg AS (
                    SELECT
                        child_asin                                AS asin,
                        MAX(parent_asin)                          AS parent_asin,
                        SUM(NULLIF(page_views,'')::numeric)       AS pv,
                        SUM(NULLIF(sessions,'')::numeric)         AS sess,
                        SUM(NULLIF(units_ordered,'')::numeric)    AS units_traffic,
                        SUM(NULLIF(ordered_product_sales,'')::numeric) AS rev_traffic,
                        AVG(NULLIF(buy_box_percentage,'')::numeric)    AS avg_bb
                    FROM spapi.sales_traffic
                    WHERE report_date::date BETWEEN :d1 AND :d2
                      AND child_asin != '' AND child_asin IS NOT NULL
                      AND granularity = 'SKU'
                    GROUP BY child_asin
                )
                SELECT *,
                    CASE WHEN sess > 0 THEN units_traffic / sess * 100 ELSE 0 END AS cvr_sess,
                    CASE WHEN pv > 0   THEN units_traffic / pv * 100   ELSE 0 END AS cvr_pv
                FROM traffic_agg
                WHERE sess > 100  -- тільки SKU з відчутним трафіком
                ORDER BY sess DESC
            """), conn, params={"d1": d1, "d2": d2})
    except Exception as e:
        st.error(f"Dead SKU error: {e}")
        dead_sku_df = pd.DataFrame()

    if not dead_sku_df.empty:
        # Тонка настройка порогів:
        # Dead   = CVR < 0.5% при 500+ sessions (реально мертві)
        # Weak   = CVR 0.5-2% при 500+ sessions (слабі, потребують уваги)
        # Healthy = CVR >= 5%
        dead = dead_sku_df[
            (dead_sku_df['sess'] >= 500) &
            (dead_sku_df['cvr_sess'] < 0.5)
        ].copy().sort_values('sess', ascending=False)

        weak = dead_sku_df[
            (dead_sku_df['sess'] >= 500) &
            (dead_sku_df['cvr_sess'] >= 0.5) &
            (dead_sku_df['cvr_sess'] < 2)
        ].copy().sort_values('sess', ascending=False)

        healthy = dead_sku_df[dead_sku_df['cvr_sess'] >= 5].copy()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🔴 Dead SKUs", f"{len(dead):,}",
                  f"CVR <0.5% @ 500+ sess")
        c2.metric("🟡 Weak SKUs", f"{len(weak):,}",
                  f"CVR 0.5-2% @ 500+ sess")
        c3.metric("💸 Втрачений трафік",
                  f"{int(dead['sess'].sum()):,}",
                  f"sessions на dead SKUs")
        c4.metric("🟢 Healthy (CVR 5%+)",
                  f"{len(healthy):,}")

        if len(dead) > 0:
            # Estimated missed revenue (if CVR was avg 10%)
            avg_price_calc = avg_price if avg_price > 0 else 40
            missed_rev = int(dead['sess'].sum() * 0.10 * avg_price_calc)
            st.error(
                f"🔴 **{len(dead)} реально мертвих SKU** — трафік йде, але майже ніхто не купує. "
                f"Втрачений потенціал: **~${missed_rev:,}** якби CVR був 10%."
            )

            st.markdown("#### 🔴 Dead SKUs — CVR < 0.5% з вагомим трафіком")
            dead_show = dead.head(20).rename(columns={
                'asin': 'ASIN', 'parent_asin': 'Parent',
                'pv': 'Page Views', 'sess': 'Sessions',
                'units_traffic': 'Units', 'rev_traffic': 'Revenue',
                'cvr_sess': 'CVR %', 'avg_bb': 'Buy Box %'
            })[['ASIN', 'Parent', 'Page Views', 'Sessions', 'Units', 'Revenue', 'CVR %', 'Buy Box %']]

            for c in ['Page Views', 'Sessions', 'Units']:
                dead_show[c] = dead_show[c].fillna(0).astype(int)

            st.dataframe(
                dead_show.style.format({
                    'Page Views': '{:,}',
                    'Sessions':   '{:,}',
                    'Units':      '{:,}',
                    'Revenue':    '${:,.0f}',
                    'CVR %':      '{:.2f}%',
                    'Buy Box %':  '{:.1f}%'
                }).background_gradient(subset=['Sessions'], cmap='Reds'),
                use_container_width=True, hide_index=True, height=400
            )

        if len(weak) > 0:
            with st.expander(f"🟡 Weak SKUs — CVR 0.5-2% ({len(weak)} SKU)", expanded=False):
                weak_show = weak.head(30).rename(columns={
                    'asin': 'ASIN', 'parent_asin': 'Parent',
                    'pv': 'Page Views', 'sess': 'Sessions',
                    'units_traffic': 'Units', 'rev_traffic': 'Revenue',
                    'cvr_sess': 'CVR %', 'avg_bb': 'Buy Box %'
                })[['ASIN', 'Parent', 'Page Views', 'Sessions', 'Units', 'Revenue', 'CVR %', 'Buy Box %']]
                for c in ['Page Views', 'Sessions', 'Units']:
                    weak_show[c] = weak_show[c].fillna(0).astype(int)
                st.dataframe(
                    weak_show.style.format({
                        'Page Views': '{:,}',
                        'Sessions':   '{:,}',
                        'Units':      '{:,}',
                        'Revenue':    '${:,.0f}',
                        'CVR %':      '{:.2f}%',
                        'Buy Box %':  '{:.1f}%'
                    }),
                    use_container_width=True, hide_index=True, height=400
                )
                st.caption("Ці SKU працюють, але слабкіше середнього — є що покращити")

        if len(dead) == 0 and len(weak) == 0:
            st.success("✅ Усі SKU з трафіком 500+ sessions мають нормальну конверсію!")

        # Завжди даємо CSV експорт всіх
        st.download_button(
            "📥 CSV всі SKU з трафіком",
            dead_sku_df.to_csv(index=False).encode(),
            "sku_performance.csv", "text/csv"
        )

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 6. ДЕТАЛЬНА ТАБЛИЦЯ: Parent ASIN → Child SKU (drill-down)
    # ══════════════════════════════════════════════════════
    st.markdown("### 📋 Деталі: Parent ASIN → Child SKU")
    st.caption("Parent з сумою ↓ клік по '▶ Розгорнути' → child SKU · 🏷️ купон · ⚡ дил · 🅿️ прайм")

    # ── Гранулярність періоду ──
    _gc1, _gc2, _gc3 = st.columns([1, 1, 2])
    with _gc1:
        detail_gran = st.radio(
            "📅 Період:",
            ["День", "Тиждень", "Місяць"],
            horizontal=True,
            key="detail_gran"
        )
    with _gc2:
        max_parents = st.selectbox(
            "Показати parent-ів:",
            [10, 20, 50, 100, 500],
            index=1, key="max_parents"
        )
    with _gc3:
        search_parent = st.text_input(
            "🔍 Пошук Parent/ASIN/SKU:",
            "", key="search_detail"
        )

    # ── Підготовка df: date_bucket по гранулярності ──
    df_work = df.copy()
    if detail_gran == "Тиждень":
        df_work['period'] = df_work['date'].dt.to_period('W').apply(lambda p: p.start_time)
    elif detail_gran == "Місяць":
        df_work['period'] = df_work['date'].dt.to_period('M').apply(lambda p: p.start_time)
    else:  # День
        df_work['period'] = df_work['date']

    # ── Parent як primary key (fallback: asin якщо немає parent) ──
    df_work['parent_key'] = df_work['parent_asin'].where(
        df_work['parent_asin'] != '', df_work['asin']
    )

    # ── Агрегація по (period, parent) ──
    parent_agg = df_work.groupby(['period', 'parent_key']).agg(
        units=('units', 'sum'),
        revenue=('revenue', 'sum'),
        orders_cnt=('orders_cnt', 'sum'),
        impressions=('impressions', 'sum'),
        sessions=('sessions', 'sum'),
        offer=('offer', lambda x: ' '.join(sorted(set(v for v in x if v)))),
        child_count=('asin', 'nunique'),
    ).reset_index()
    parent_agg['cvr'] = (parent_agg['units'] / parent_agg['sessions'].replace(0, float('nan')) * 100).fillna(0).round(2)
    parent_agg = parent_agg.sort_values(['period', 'revenue'], ascending=[False, False])

    # ── Search фільтр ──
    if search_parent:
        mask_parent = parent_agg['parent_key'].str.contains(search_parent, case=False, na=False)
        matched_parents_by_child = df_work[
            df_work['asin'].str.contains(search_parent, case=False, na=False) |
            df_work['sku'].astype(str).str.contains(search_parent, case=False, na=False)
        ]['parent_key'].unique()
        parent_agg = parent_agg[
            mask_parent | parent_agg['parent_key'].isin(matched_parents_by_child)
        ]

    if parent_agg.empty:
        st.info("Немає даних за фільтром")
    else:
        # Обмежуємо кількість parent-ів щоб не рендерити тисячі expander-ів
        # Групуємо по parent, беремо топ по total revenue
        parent_totals = parent_agg.groupby('parent_key').agg(
            total_rev=('revenue', 'sum')
        ).reset_index().nlargest(max_parents, 'total_rev')
        top_parents = parent_totals['parent_key'].tolist()
        parent_agg = parent_agg[parent_agg['parent_key'].isin(top_parents)]

        # ── KPI над таблицею ──
        _tc1, _tc2, _tc3, _tc4 = st.columns(4)
        _tc1.metric("👨‍👧 Parent-ів показано", f"{len(top_parents):,}")
        _tc2.metric("📦 Total units",           f"{int(parent_agg['units'].sum()):,}")
        _tc3.metric("💰 Total revenue",         _fmt(parent_agg['revenue'].sum()))
        _tc4.metric("📅 Рядків періоду",         f"{len(parent_agg):,}")

        # ── Формат дати залежно від гранулярності ──
        if detail_gran == "День":
            date_fmt = '%Y-%m-%d'
        elif detail_gran == "Тиждень":
            date_fmt = 'Тиж. %Y-%m-%d'
        else:
            date_fmt = '%Y-%m'

        # ── Рендер: parent summary row → expander з child-ами ──
        # Для продуктивності — групуємо по parent і показуємо всі його періоди разом

        # Спочатку: загальна таблиця parent-сумаризована (без drill-down)
        st.markdown("#### 📊 Parent ASIN — сумарно по обраних періодах")
        parent_total = parent_agg.groupby('parent_key').agg(
            units=('units', 'sum'),
            revenue=('revenue', 'sum'),
            orders_cnt=('orders_cnt', 'sum'),
            impressions=('impressions', 'sum'),
            sessions=('sessions', 'sum'),
            child_count=('child_count', 'max'),
            periods=('period', 'nunique'),
        ).reset_index()
        parent_total['cvr'] = (parent_total['units'] / parent_total['sessions'].replace(0, float('nan')) * 100).fillna(0).round(2)
        parent_total = parent_total.sort_values('revenue', ascending=False)

        pt_show = parent_total.rename(columns={
            'parent_key': 'Parent ASIN',
            'units': 'Units', 'revenue': 'Revenue',
            'orders_cnt': 'Orders', 'impressions': 'Impr',
            'sessions': 'Sessions', 'cvr': 'CVR %',
            'child_count': '# Children', 'periods': '# Periods'
        })
        for c in ['Units', 'Orders', 'Impr', 'Sessions', '# Children', '# Periods']:
            if c in pt_show.columns:
                pt_show[c] = pd.to_numeric(pt_show[c], errors='coerce').fillna(0).astype(int)

        # ── TOTAL row ──
        pt_head = pt_show.head(max_parents).copy()
        total_units_p = int(pt_head['Units'].sum())
        total_sess_p  = int(pt_head['Sessions'].sum())
        total_row = pd.DataFrame([{
            'Parent ASIN': f'📊 TOTAL ({len(pt_head)})',
            'Units':      total_units_p,
            'Revenue':    float(pt_head['Revenue'].sum()),
            'Orders':     int(pt_head['Orders'].sum()),
            'Impr':       int(pt_head['Impr'].sum()),
            'Sessions':   total_sess_p,
            'CVR %':      round(total_units_p / total_sess_p * 100, 2) if total_sess_p > 0 else 0,
            '# Children': int(pt_head['# Children'].sum()) if '# Children' in pt_head.columns else 0,
            '# Periods':  int(pt_head['# Periods'].max()) if '# Periods' in pt_head.columns else 0,
        }])
        pt_with_total = pd.concat([pt_head, total_row], ignore_index=True)

        def _highlight_total(row):
            if str(row['Parent ASIN']).startswith('📊 TOTAL'):
                return ['background-color:#1a2b1e;color:#4CAF50;font-weight:bold'] * len(row)
            return [''] * len(row)

        st.dataframe(
            pt_with_total.style.format({
                'Units':    '{:,}',
                'Orders':   '{:,}',
                'Impr':     '{:,}',
                'Sessions': '{:,}',
                'Revenue':  '${:,.0f}',
                'CVR %':    '{:.2f}%'
            }).apply(_highlight_total, axis=1),
            use_container_width=True, hide_index=True, height=440
        )

        st.markdown("---")

        # ── Drill-down: HTML ієрархічна таблиця (Helium10-style) ──
        st.markdown("#### 🔍 Детальна таблиця з розгортанням")
        st.caption("Клік на ▶ розгортає child SKU · зелений CVR ≥5% · червоний <1%")

        # Selectbox для періоду (якщо обрано — показуємо тільки цей період, інакше всі)
        all_periods = sorted(parent_agg['period'].unique(), reverse=True)
        period_labels = [pd.Timestamp(p).strftime(date_fmt) for p in all_periods]
        period_map = dict(zip(period_labels, all_periods))

        _pc1, _pc2 = st.columns([2, 3])
        with _pc1:
            sel_period_drill = st.selectbox(
                f"📅 Оберіть {detail_gran.lower()}:",
                options=["🔄 Всі періоди разом"] + period_labels,
                key="drill_period"
            )

        # Формуємо дані для рендеру
        if sel_period_drill == "🔄 Всі періоди разом":
            # Групуємо по parent сумарно
            parents_to_render = parent_total.sort_values('revenue', ascending=False).to_dict('records')
            period_filter = None
        else:
            period_ts = period_map[sel_period_drill]
            parents_to_render = parent_agg[parent_agg['period'] == period_ts].sort_values('revenue', ascending=False).to_dict('records')
            for p in parents_to_render:
                p['parent_key'] = p.get('parent_key', '')
            period_filter = period_ts

        # ── Рендер HTML таблиці ──
        def _cvr_color(v):
            try:
                v = float(v)
                if v >= 5:  return '#4CAF50'
                if v >= 1:  return '#FFC107'
                return '#F44336'
            except: return '#888'

        def _fmt_int(v):
            try: return f"{int(v):,}"
            except: return str(v)

        def _fmt_money(v):
            try: return f"${float(v):,.0f}"
            except: return str(v)

        # Готуємо HTML
        html_parts = []
        html_parts.append("""
<style>
.ph-table { width:100%; border-collapse:collapse; font-size:13px;
            background:#0e1117; color:#fff; border-radius:8px; overflow:hidden; }
.ph-table th { background:#1a1a2e; color:#aaa; font-weight:600;
               padding:10px 12px; text-align:right; border-bottom:2px solid #2d2d4a;
               font-size:11px; text-transform:uppercase; letter-spacing:0.5px; }
.ph-table th.left { text-align:left; }
.ph-table td { padding:8px 12px; border-bottom:1px solid #1f1f2e; text-align:right;
               font-variant-numeric:tabular-nums; }
.ph-table td.left { text-align:left; }
.ph-table .parent-row { background:#16213e; font-weight:600; cursor:pointer;
                         transition:background 0.15s; }
.ph-table .parent-row:hover { background:#1d2a4a; }
.ph-table .parent-row .arrow { display:inline-block; transition:transform 0.2s;
                                color:#5B9BD5; width:12px; }
.ph-table .parent-row.expanded .arrow { transform:rotate(90deg); }
.ph-table .child-row { background:#0a0d14; display:none; font-size:12px; color:#bbb; }
.ph-table .child-row.visible { display:table-row; }
.ph-table .child-row td { padding:6px 12px; padding-left:36px; }
.ph-table .child-row td.left { padding-left:48px; }
.cvr-high { color:#4CAF50; font-weight:600; }
.cvr-mid  { color:#FFC107; }
.cvr-low  { color:#F44336; }
.offer-badge { background:#2b2b1a; border:1px solid #4a4a2d; border-radius:4px;
               padding:2px 6px; font-size:11px; color:#FFC107; margin-left:6px; }
.total-row { background:#1a2b1e !important; border-top:3px solid #4CAF50;
             font-size:14px; color:#4CAF50 !important; }
.total-row td { padding:12px; border-bottom:none; }
</style>

<table class='ph-table'>
<thead>
  <tr>
    <th class='left'>Parent / Child</th>
    <th>Units</th>
    <th>Revenue</th>
    <th>Orders</th>
    <th>Impr</th>
    <th>Sessions</th>
    <th>CVR</th>
  </tr>
</thead>
<tbody>
""")

        for idx, prow in enumerate(parents_to_render[:max_parents]):
            pkey = prow.get('parent_key', '')
            p_units = int(prow.get('units', 0) or 0)
            p_rev   = float(prow.get('revenue', 0) or 0)
            p_ord   = int(prow.get('orders_cnt', 0) or 0)
            p_impr  = int(prow.get('impressions', 0) or 0)
            p_sess  = int(prow.get('sessions', 0) or 0)
            p_cvr   = float(prow.get('cvr', 0) or 0)
            p_offer = prow.get('offer', '') if period_filter else ''

            cvr_cls = 'cvr-high' if p_cvr >= 5 else ('cvr-mid' if p_cvr >= 1 else 'cvr-low')
            offer_html = f"<span class='offer-badge'>{p_offer}</span>" if p_offer else ""

            # Parent row
            html_parts.append(f"""
<tr class='parent-row' onclick='toggleParent_{idx}(this)'>
  <td class='left'><span class='arrow'>▶</span> <code>{pkey}</code>{offer_html}</td>
  <td>{_fmt_int(p_units)}</td>
  <td>{_fmt_money(p_rev)}</td>
  <td>{_fmt_int(p_ord)}</td>
  <td>{_fmt_int(p_impr)}</td>
  <td>{_fmt_int(p_sess)}</td>
  <td class='{cvr_cls}'>{p_cvr:.2f}%</td>
</tr>
""")

            # Children для цього parent
            if period_filter is not None:
                children = df_work[(df_work['parent_key'] == pkey) & (df_work['period'] == period_filter)]
            else:
                children = df_work[df_work['parent_key'] == pkey]

            if not children.empty:
                child_grouped = children.groupby(['asin', 'sku']).agg(
                    units=('units', 'sum'),
                    revenue=('revenue', 'sum'),
                    orders_cnt=('orders_cnt', 'sum'),
                    impressions=('impressions', 'sum'),
                    sessions=('sessions', 'sum'),
                    offer=('offer', lambda x: ' '.join(sorted(set(v for v in x if v)))),
                ).reset_index()
                child_grouped['cvr'] = (child_grouped['units'] / child_grouped['sessions'].replace(0, float('nan')) * 100).fillna(0)
                child_grouped = child_grouped.sort_values('revenue', ascending=False)

                for _, crow in child_grouped.iterrows():
                    c_cvr = float(crow['cvr'] or 0)
                    c_cvr_cls = 'cvr-high' if c_cvr >= 5 else ('cvr-mid' if c_cvr >= 1 else 'cvr-low')
                    c_offer_html = f"<span class='offer-badge'>{crow['offer']}</span>" if crow.get('offer') else ""

                    html_parts.append(f"""
<tr class='child-row child-of-{idx}'>
  <td class='left'><code>{crow['asin']}</code> · <small>{crow['sku'] or ''}</small>{c_offer_html}</td>
  <td>{_fmt_int(crow['units'])}</td>
  <td>${float(crow['revenue']):,.2f}</td>
  <td>{_fmt_int(crow['orders_cnt'])}</td>
  <td>{_fmt_int(crow['impressions'])}</td>
  <td>{_fmt_int(crow['sessions'])}</td>
  <td class='{c_cvr_cls}'>{c_cvr:.2f}%</td>
</tr>
""")

            # JS toggler для цього parent
            html_parts.append(f"""
<script>
function toggleParent_{idx}(row) {{
    row.classList.toggle('expanded');
    document.querySelectorAll('.child-of-{idx}').forEach(r => r.classList.toggle('visible'));
}}
</script>
""")

        html_parts.append("</tbody>")

        # ── TOTAL ROW ──
        t_units = int(sum(p.get('units', 0) or 0 for p in parents_to_render[:max_parents]))
        t_rev   = float(sum(p.get('revenue', 0) or 0 for p in parents_to_render[:max_parents]))
        t_ord   = int(sum(p.get('orders_cnt', 0) or 0 for p in parents_to_render[:max_parents]))
        t_impr  = int(sum(p.get('impressions', 0) or 0 for p in parents_to_render[:max_parents]))
        t_sess  = int(sum(p.get('sessions', 0) or 0 for p in parents_to_render[:max_parents]))
        t_cvr   = (t_units / t_sess * 100) if t_sess > 0 else 0
        t_cvr_cls = 'cvr-high' if t_cvr >= 10 else ('cvr-mid' if t_cvr >= 5 else 'cvr-low')

        html_parts.append(f"""
<tfoot>
  <tr class='total-row'>
    <td class='left'><b>📊 TOTAL ({len(parents_to_render[:max_parents])} parents)</b></td>
    <td><b>{_fmt_int(t_units)}</b></td>
    <td><b>{_fmt_money(t_rev)}</b></td>
    <td><b>{_fmt_int(t_ord)}</b></td>
    <td><b>{_fmt_int(t_impr)}</b></td>
    <td><b>{_fmt_int(t_sess)}</b></td>
    <td class='{t_cvr_cls}'><b>{t_cvr:.2f}%</b></td>
  </tr>
</tfoot>
""")
        html_parts.append("</table>")

        # Рендер
        total_rows = len(parents_to_render[:max_parents])
        # Грубо: висота = 60 (header) + parent-и × 40 + запас
        html_height = min(800, 100 + total_rows * 42)

        st.components.v1.html("".join(html_parts), height=html_height, scrolling=True)

        st.caption(f"Показано {min(max_parents, len(parents_to_render))} parent ASIN. Клік на рядок → розгорне child SKU.")

        # CSV з усіма даними
        st.download_button(
            "📥 CSV (всі parent × period)",
            parent_agg.to_csv(index=False).encode(),
            f"orders_parent_{detail_gran.lower()}.csv", "text/csv",
            key="dl_parent_period"
        )

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 7. OFFER ANALYSIS
    # ══════════════════════════════════════════════════════
    st.markdown("### 🎫 Аналіз промо-акцій (Offer)")
    col1, col2 = st.columns(2)

    with col1:
        df['has_promo'] = df['offer'] != ''
        promo_rev = df.groupby('has_promo')['revenue'].sum()
        promo_labels = {True: '🎫 З промо', False: '📦 Без промо'}
        promo_data = pd.DataFrame({
            'Type': [promo_labels.get(k, k) for k in promo_rev.index],
            'Revenue': promo_rev.values
        })
        fig5 = px.pie(promo_data, values='Revenue', names='Type', hole=0.4,
                      color_discrete_sequence=['#FF9800', '#4CAF50'], height=320)
        fig5.update_layout(margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig5, use_container_width=True)

    with col2:
        st.markdown("#### 📊 CVR: промо vs без промо")
        promo_agg = df.groupby('has_promo').agg(
            units=('units', 'sum'), impressions=('impressions', 'sum')
        ).reset_index()
        promo_agg['cvr'] = (promo_agg['units'] / promo_agg['impressions'].replace(0, float('nan')) * 100).fillna(0)
        promo_agg['label'] = promo_agg['has_promo'].map(promo_labels)
        if len(promo_agg) == 2:
            _pc1, _pc2 = st.columns(2)
            for idx, row in promo_agg.iterrows():
                _col_to = _pc1 if idx == 0 else _pc2
                _col_to.metric(row['label'], f"{row['cvr']:.2f}%",
                               f"{int(row['units'])} units / {int(row['impressions'])} impr")
        else:
            st.info("Недостатньо даних для порівняння")

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 8. MoM ПОРІВНЯННЯ
    # ══════════════════════════════════════════════════════
    st.markdown("### 📊 Порівняння з попереднім періодом")
    try:
        period_days = (date_range[1] - date_range[0]).days
        prev_d1 = str(date_range[0] - dt.timedelta(days=period_days))
        prev_d2 = str(date_range[0] - dt.timedelta(days=1))

        with engine.connect() as conn:
            prev = pd.read_sql(text("""
                SELECT
                    SUM(CASE WHEN item_price ~ '^[0-9.]+$' THEN item_price::numeric ELSE 0 END) AS revenue,
                    SUM(CASE WHEN quantity ~ '^[0-9]+$' THEN quantity::numeric ELSE 1 END) AS units,
                    COUNT(DISTINCT amazon_order_id) AS orders
                FROM orders
                WHERE SUBSTRING(purchase_date,1,10)::date BETWEEN :d1 AND :d2
                  AND purchase_date IS NOT NULL AND purchase_date != ''
            """), conn, params={"d1": prev_d1, "d2": prev_d2}).iloc[0]

        prev_rev = float(prev['revenue'] or 0)
        prev_units = int(prev['units'] or 0)
        prev_orders = int(prev['orders'] or 0)

        # Перевірка: якщо попередній період < 5% поточного — дані не репрезентативні
        has_meaningful_prev = prev_rev > total_rev * 0.05 if total_rev > 0 else prev_rev > 0

        def _delta_str(cur, prev_v):
            if prev_v == 0: return None
            pct = (cur - prev_v) / prev_v * 100
            if abs(pct) > 1000: return None  # нереалістичний delta
            return f"{pct:+.1f}%"

        _mc1, _mc2, _mc3 = st.columns(3)
        _mc1.metric("💰 Revenue", _fmt(total_rev),
                    _delta_str(total_rev, prev_rev) if has_meaningful_prev else None)
        _mc2.metric("📦 Units", f"{total_units:,}",
                    _delta_str(total_units, prev_units) if has_meaningful_prev else None)
        _mc3.metric("🛒 Orders", f"{total_orders:,}",
                    _delta_str(total_orders, prev_orders) if has_meaningful_prev else None)

        # Показуємо суми попереднього періоду
        st.caption(
            f"Попередній період ({prev_d1} → {prev_d2}): "
            f"Revenue {_fmt(prev_rev)} · Units {prev_units:,} · Orders {prev_orders:,}"
            + (" · ⚠️ замало даних для порівняння" if not has_meaningful_prev else "")
        )
    except Exception:
        st.info("Немає даних за попередній період")

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 8.5 AVERAGE ORDER COMPOSITION — структура замовлення
    # ══════════════════════════════════════════════════════
    st.markdown("### 🧺 Структура замовлення (Order Composition)")
    st.caption("AOV тренд + units/order: як змінюється поведінка покупців у часі")

    try:
        with engine.connect() as conn:
            # Беремо raw orders без агрегації для точного підрахунку units per order
            df_comp = pd.read_sql(text("""
                SELECT
                    SUBSTRING(o.purchase_date, 1, 10)::date  AS date,
                    o.amazon_order_id,
                    SUM(CASE WHEN o.quantity ~ '^[0-9]+$'
                        THEN o.quantity::numeric ELSE 1 END)  AS units,
                    SUM(CASE WHEN o.item_price ~ '^[0-9.]+$'
                        THEN o.item_price::numeric ELSE 0 END) AS revenue,
                    COUNT(DISTINCT o.sku)                     AS unique_skus
                FROM orders o
                WHERE SUBSTRING(o.purchase_date, 1, 10)::date BETWEEN :d1 AND :d2
                  AND o.purchase_date IS NOT NULL AND o.purchase_date != ''
                  AND o.amazon_order_id IS NOT NULL
                GROUP BY 1, 2
            """), conn, params={"d1": d1, "d2": d2})
    except Exception as e:
        st.error(f"Composition error: {e}")
        df_comp = pd.DataFrame()

    if not df_comp.empty:
        df_comp['date']     = pd.to_datetime(df_comp['date'])
        df_comp['units']    = pd.to_numeric(df_comp['units'], errors='coerce').fillna(0)
        df_comp['revenue']  = pd.to_numeric(df_comp['revenue'], errors='coerce').fillna(0)

        # ── KPI композиції ──
        avg_units_per_order = df_comp['units'].mean()
        avg_aov             = df_comp['revenue'].mean()
        median_aov          = df_comp['revenue'].median()
        multi_unit_orders   = int((df_comp['units'] > 1).sum())
        multi_unit_pct      = multi_unit_orders / len(df_comp) * 100
        multi_sku_orders    = int((df_comp['unique_skus'] > 1).sum())
        multi_sku_pct       = multi_sku_orders / len(df_comp) * 100

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("📦 Units / Order",    f"{avg_units_per_order:.2f}")
        c2.metric("💰 AOV (avg)",         f"${avg_aov:.2f}")
        c3.metric("📊 AOV (median)",      f"${median_aov:.2f}")
        c4.metric("🛍 Multi-unit orders", f"{multi_unit_pct:.1f}%",
                  f"{multi_unit_orders:,} замовлень")

        # ── Тренд AOV + Units/Order по днях ──
        daily_comp = df_comp.groupby('date').agg(
            orders=('amazon_order_id', 'nunique'),
            units=('units', 'sum'),
            revenue=('revenue', 'sum'),
        ).reset_index()
        daily_comp['aov']            = (daily_comp['revenue'] / daily_comp['orders']).round(2)
        daily_comp['units_per_order'] = (daily_comp['units']   / daily_comp['orders']).round(3)

        if gran == "Тиждень":
            daily_comp = daily_comp.set_index('date').resample('W').agg({
                'orders': 'sum', 'units': 'sum', 'revenue': 'sum'
            }).reset_index()
            daily_comp['aov']            = (daily_comp['revenue'] / daily_comp['orders']).round(2)
            daily_comp['units_per_order'] = (daily_comp['units']   / daily_comp['orders']).round(3)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 💰 AOV тренд (середній чек)")
            fig_aov = go.Figure()
            fig_aov.add_trace(go.Scatter(
                x=daily_comp['date'], y=daily_comp['aov'],
                mode='lines+markers', name='AOV',
                line=dict(color='#4CAF50', width=2.5),
                fill='tozeroy', fillcolor='rgba(76,175,80,0.1)'
            ))
            # Середня лінія
            fig_aov.add_hline(y=avg_aov, line_dash="dash", line_color="#FFC107",
                              annotation_text=f"Avg ${avg_aov:.2f}")
            # MA-7 для days
            if gran == "День" and len(daily_comp) >= 7:
                ma_aov = daily_comp['aov'].rolling(7, min_periods=1).mean()
                fig_aov.add_trace(go.Scatter(
                    x=daily_comp['date'], y=ma_aov, name='MA-7',
                    mode='lines', line=dict(color='#AB47BC', width=1.5, dash='dot')
                ))
            fig_aov.update_layout(
                height=320, margin=dict(l=0, r=0, t=10, b=0),
                yaxis=dict(tickprefix='$'),
                legend=dict(orientation='h', y=1.12)
            )
            st.plotly_chart(fig_aov, use_container_width=True)

        with col2:
            st.markdown("#### 🧺 Units per Order")
            colors_upo = ['#4CAF50' if v >= avg_units_per_order else '#F44336' for v in daily_comp['units_per_order']]
            fig_upo = go.Figure(go.Bar(
                x=daily_comp['date'], y=daily_comp['units_per_order'],
                marker_color=colors_upo, opacity=0.85,
                text=[f"{v:.2f}" for v in daily_comp['units_per_order']],
                textposition='outside'
            ))
            fig_upo.add_hline(y=avg_units_per_order, line_dash="dash",
                              line_color="#FFC107",
                              annotation_text=f"Avg {avg_units_per_order:.2f}")
            fig_upo.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=0),
                                  yaxis_title='Units / Order')
            st.plotly_chart(fig_upo, use_container_width=True)

        # ── Розподіл розмірів замовлень ──
        st.markdown("#### 📊 Розподіл замовлень за кількістю одиниць")

        def _bucket(u):
            u = int(u)
            if u == 1:    return '1️⃣ Solo (1 unit)'
            if u == 2:    return '2️⃣ Pair (2 units)'
            if u <= 5:    return '3️⃣ Small (3-5)'
            if u <= 10:   return '4️⃣ Medium (6-10)'
            return '5️⃣ Bulk (10+)'

        df_comp['bucket'] = df_comp['units'].apply(_bucket)
        bucket_stats = df_comp.groupby('bucket').agg(
            orders=('amazon_order_id', 'nunique'),
            units=('units', 'sum'),
            revenue=('revenue', 'sum'),
        ).reset_index().sort_values('bucket')
        bucket_stats['avg_aov']  = (bucket_stats['revenue'] / bucket_stats['orders']).round(2)
        bucket_stats['orders_pct'] = (bucket_stats['orders'] / bucket_stats['orders'].sum() * 100).round(1)
        bucket_stats['rev_pct']    = (bucket_stats['revenue'] / bucket_stats['revenue'].sum() * 100).round(1)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("##### 🛒 Замовлень за розміром")
            fig_b1 = go.Figure(go.Bar(
                x=bucket_stats['bucket'], y=bucket_stats['orders'],
                marker_color=['#4472C4', '#5B9BD5', '#70AD47', '#FFC107', '#F44336'][:len(bucket_stats)],
                text=[f"{int(v):,} ({p:.0f}%)" for v, p in zip(bucket_stats['orders'], bucket_stats['orders_pct'])],
                textposition='outside'
            ))
            fig_b1.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig_b1, use_container_width=True)

        with col2:
            st.markdown("##### 💰 Виручка за розміром")
            fig_b2 = go.Figure(go.Bar(
                x=bucket_stats['bucket'], y=bucket_stats['revenue'],
                marker_color=['#4472C4', '#5B9BD5', '#70AD47', '#FFC107', '#F44336'][:len(bucket_stats)],
                text=[f"${v/1000:.1f}K ({p:.0f}%)" for v, p in zip(bucket_stats['revenue'], bucket_stats['rev_pct'])],
                textposition='outside'
            ))
            fig_b2.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0),
                                 yaxis=dict(tickprefix='$'))
            st.plotly_chart(fig_b2, use_container_width=True)

        # Таблиця bucket
        st.dataframe(
            bucket_stats.rename(columns={
                'bucket': 'Розмір', 'orders': 'Orders', 'units': 'Units',
                'revenue': 'Revenue', 'avg_aov': 'AOV', 'orders_pct': '% Orders', 'rev_pct': '% Revenue'
            }).style.format({
                'Revenue': '${:,.0f}', 'AOV': '${:.2f}',
                '% Orders': '{:.1f}%', '% Revenue': '{:.1f}%'
            }),
            use_container_width=True, hide_index=True
        )

        # ── Інсайти композиції ──
        _icc = st.columns(3)

        # Solo orders share
        solo = bucket_stats[bucket_stats['bucket'].str.contains('Solo', na=False)]
        solo_pct = float(solo['orders_pct'].iloc[0]) if not solo.empty else 0
        if solo_pct >= 80:
            _txt = f"<b>{solo_pct:.0f}%</b> замовлень — solo (1 unit). Низький cross-sell."
            _em, _col = "🔴", "#2b0d0d"
        elif solo_pct >= 60:
            _txt = f"<b>{solo_pct:.0f}%</b> — solo. Є потенціал для bundle."
            _em, _col = "🟡", "#2b2400"
        else:
            _txt = f"<b>{solo_pct:.0f}%</b> solo · решта multi-unit. Хороший AOV mix."
            _em, _col = "🟢", "#0d2b1e"
        with _icc[0]: insight_card(_em, "Solo замовлення", _txt, _col)

        # AOV trend (cur vs first half)
        if len(daily_comp) >= 4:
            half = len(daily_comp) // 2
            aov_first  = daily_comp['aov'].iloc[:half].mean()
            aov_second = daily_comp['aov'].iloc[half:].mean()
            aov_chg    = (aov_second - aov_first) / aov_first * 100 if aov_first > 0 else 0
            if aov_chg >= 5:    _em, _col = "🟢", "#0d2b1e"
            elif aov_chg >= -5: _em, _col = "🟡", "#2b2400"
            else:               _em, _col = "🔴", "#2b0d0d"
            with _icc[1]: insight_card(_em, "AOV тренд",
                f"AOV {'↑' if aov_chg >= 0 else '↓'} <b>{aov_chg:+.1f}%</b> "
                f"(перша половина ${aov_first:.2f} → друга ${aov_second:.2f})", _col)

        # Multi-SKU rate
        with _icc[2]:
            insight_card("🛍", "Multi-SKU кошики",
                f"<b>{multi_sku_pct:.1f}%</b> замовлень містять різні SKU "
                f"({multi_sku_orders:,} з {len(df_comp):,})", "#1e293b")

    else:
        st.info("Немає даних для аналізу композиції замовлень")

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 9. INSIGHTS (стара функція — залишаємо)
    # ══════════════════════════════════════════════════════
    # Підготуємо df для insights_orders
    _df_compat = df.copy()
    _df_compat['Order Date']  = _df_compat['date']
    _df_compat['Order ID']    = ''  # grouped, не по order
    _df_compat['SKU']         = _df_compat['sku']
    _df_compat['Total Price'] = _df_compat['revenue']
    _df_compat['Item Price']  = _df_compat['revenue']
    _df_compat['Quantity']    = _df_compat['units']
    # Фейковий Order ID для nunique
    _df_compat['Order ID']    = range(len(_df_compat))
    insights_orders(_df_compat)

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 10. AI CHAT
    # ══════════════════════════════════════════════════════
    ctx = (f"Orders v2: {total_orders:,} замовлень | Revenue {_fmt(total_rev)} | "
           f"Units {total_units:,} | Impr {total_impr:,} | CVR {avg_cvr:.2f}% | "
           f"Промо {promo_pct:.0f}% | Avg Price ${avg_price:.2f} | "
           f"Період {d1}→{d2}")
    show_ai_chat(ctx, [
        "Які ASIN мають найвищу CVR?",
        "Де промо дають найкращий ефект?",
        "Які SKU в bundle/multi-unit замовленнях?",
        "Як змінився AOV за останній тиждень?",
    ], "orders_v2")


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
            df_all = pd.read_sql(text("SELECT * FROM listings_all"), conn)
            df_cat = pd.read_sql(text("SELECT asin, brand, main_image_url, sales_rank, sales_rank_category, color, size, product_type FROM catalog_items"), conn)
    except Exception as e:
        st.error(f"Помилка: {e}"); return

    if df_all.empty:
        st.warning("listings_all порожня"); return

    df_all['price']    = pd.to_numeric(df_all['price'].replace('', None), errors='coerce').fillna(0)
    df_all['quantity'] = pd.to_numeric(df_all['quantity'].replace('', None), errors='coerce').fillna(0)
    df_all['open_date']= pd.to_datetime(df_all['open_date'], errors='coerce')

    df = df_all.merge(df_cat, left_on='asin1', right_on='asin', how='left')

    # ── Фільтри ──
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

    # ── KPI ──
    total        = len(df_f)
    active_cnt   = int((df_f['status'].astype(str).str.lower() == 'active').sum())
    inactive_cnt = total - active_cnt
    avg_price    = df_f[df_f['price'] > 0]['price'].mean() if total > 0 else 0
    fba_cnt      = int(df_f['fulfillment_channel'].astype(str).str.contains("AMAZON", case=False, na=False).sum())
    fbm_cnt      = total - fba_cnt
    unique_asins = df_f['asin1'].nunique()

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
    active_pct = active_cnt / total * 100 if total > 0 else 0
    if active_pct >= 80:   _em, _col = "🟢", "#0d2b1e"
    elif active_pct >= 50: _em, _col = "🟡", "#2b2400"
    else:                  _em, _col = "🔴", "#2b0d0d"
    with _icols[0]: insight_card(_em, "Активних листингів",
        f"<b>{active_cnt}</b> з {total} ({active_pct:.0f}%) активні", _col)
    fba_pct = fba_cnt / total * 100 if total > 0 else 0
    with _icols[1]: insight_card("📦", "FBA vs FBM",
        f"FBA: <b>{fba_cnt}</b> ({fba_pct:.0f}%) · FBM: <b>{fbm_cnt}</b> ({100-fba_pct:.0f}%)", "#1a1a2e")
    with _icols[2]: insight_card("💰", "Середня ціна",
        f"Avg: <b>${avg_price:.2f}</b> · {total} SKU в каталозі", "#1e293b")

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # ТАБИ
    # ══════════════════════════════════════════════════════
    tabs = st.tabs(["📋 По SKU", "🔗 По ASIN", "⚠️ Дефекти", "🆕 Нові листинги", "❓ Без catalog"])

    # ══════════════════════════════════════
    # TAB 0 — Каталог по SKU
    # ══════════════════════════════════════
    with tabs[0]:
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
            st.markdown("#### 📊 Active vs Inactive")
            status_mp = df_f.groupby(['marketplace', 'status']).size().reset_index(name='cnt')
            fig2 = px.bar(status_mp, x='marketplace', y='cnt', color='status',
                          color_discrete_map={'Active': '#4CAF50', 'Inactive': '#F44336'},
                          barmode='stack', height=320)
            fig2.update_layout(margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig2, width="stretch")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 💰 Розподіл цін")
            df_prices = df_f[df_f['price'] > 0]
            if not df_prices.empty:
                fig3 = px.histogram(df_prices, x='price', nbins=30,
                                    color_discrete_sequence=['#5B9BD5'], height=300)
                fig3.update_layout(margin=dict(l=0, r=0, t=10, b=0), xaxis_title="Ціна $")
                st.plotly_chart(fig3, width="stretch")
        with col2:
            df_f['sales_rank'] = pd.to_numeric(df_f.get('sales_rank', None), errors='coerce')
            if 'size' in df_f.columns and df_f['size'].notna().any() and df_f['size'].astype(str).str.strip().ne('').any():
                st.markdown("#### 📏 По розміру (Size)")
                sz = df_f[df_f['size'].notna() & (df_f['size'].astype(str).str.strip() != '')]['size'].value_counts().head(12).reset_index()
                sz.columns = ['Size', 'Count']
                fig4 = go.Figure(go.Bar(
                    x=sz['Count'], y=sz['Size'], orientation='h',
                    marker_color=px.colors.qualitative.Set2[:len(sz)],
                    text=sz['Count'], textposition='outside'
                ))
                fig4.update_layout(height=300, yaxis={'categoryorder': 'total ascending'},
                                   margin=dict(l=0, r=40, t=10, b=0))
                st.plotly_chart(fig4, width="stretch")
            else:
                st.markdown("#### 📦 По Fulfillment Channel")
                fc_cnt = df_f['fulfillment_channel'].value_counts().reset_index()
                fc_cnt.columns = ['FC', 'Count']
                fig4 = px.pie(fc_cnt, values='Count', names='FC', hole=0.4, height=300)
                st.plotly_chart(fig4, width="stretch")

        st.markdown("#### 📋 Каталог листингів")
        show_cols = ['seller_sku', 'asin1', 'item_name', 'price', 'quantity', 'status',
                     'fulfillment_channel', 'marketplace', 'open_date']
        for c in ['brand', 'size', 'color', 'sales_rank', 'main_image_url']:
            if c in df_f.columns: show_cols.append(c)
        show_cols = [c for c in show_cols if c in df_f.columns]
        df_show = df_f[show_cols].sort_values('price', ascending=False).reset_index(drop=True)
        col_cfg = {
            'seller_sku':          st.column_config.TextColumn("SKU"),
            'asin1':               st.column_config.TextColumn("ASIN"),
            'item_name':           st.column_config.TextColumn("Назва", width="large"),
            'price':               st.column_config.NumberColumn("Ціна $", format="$%.2f"),
            'quantity':            st.column_config.NumberColumn("Qty"),
            'status':              st.column_config.TextColumn("Статус"),
            'fulfillment_channel': st.column_config.TextColumn("FC"),
            'marketplace':         st.column_config.TextColumn("MP"),
            'open_date':           st.column_config.DatetimeColumn("Open Date", format="YYYY-MM-DD"),
            'brand':               st.column_config.TextColumn("Brand"),
            'size':                st.column_config.TextColumn("Size"),
            'color':               st.column_config.TextColumn("Color"),
            'sales_rank':          st.column_config.NumberColumn("BSR", format="%d"),
            'main_image_url':      st.column_config.ImageColumn("Фото", width="small"),
        }
        st.dataframe(df_show, column_config=col_cfg, width="stretch", hide_index=True, height=500)
        st.caption(f"Показано {len(df_show):,} з {len(df_f):,} листингів")
        st.download_button("📥 CSV (по SKU)", df_f[show_cols].to_csv(index=False).encode(),
                           "listings_by_sku.csv", "text/csv", key="dl_sku")

    # ══════════════════════════════════════
    # TAB 1 — Каталог по ASIN (дедупліковано)
    # ══════════════════════════════════════
    with tabs[1]:
        st.markdown("#### 🔗 Каталог по ASIN — дедупліковано")
        st.caption("Один рядок на ASIN · розміри згруповані · Active має пріоритет")

        df_asin = df_f.copy()
        df_asin['_active'] = (df_asin['status'].astype(str).str.lower() == 'active').astype(int)

        # Групуємо розміри і кольори в рядок
        def join_unique(series):
            vals = [str(v).strip() for v in series if pd.notna(v) and str(v).strip()]
            return ', '.join(sorted(set(vals))) if vals else ''

        agg_dict = {
            'seller_sku':          'first',
            'item_name':           'first',
            'price':               'mean',
            'quantity':            'sum',
            '_active':             'max',
            'fulfillment_channel': 'first',
            'marketplace':         'first',
        }
        if 'brand' in df_asin.columns:         agg_dict['brand']         = 'first'
        if 'size' in df_asin.columns:          agg_dict['size']          = join_unique
        if 'color' in df_asin.columns:         agg_dict['color']         = join_unique
        if 'sales_rank' in df_asin.columns:    agg_dict['sales_rank']    = 'first'
        if 'main_image_url' in df_asin.columns:agg_dict['main_image_url']= 'first'

        sku_counts = df_asin.groupby('asin1').size().reset_index(name='sku_count')
        df_grouped = df_asin.groupby('asin1').agg(agg_dict).reset_index()
        df_grouped['status'] = df_grouped['_active'].map({1: 'Active', 0: 'Inactive'})
        df_grouped['price']  = df_grouped['price'].round(2)
        df_grouped = df_grouped.merge(sku_counts, on='asin1', how='left').drop(columns=['_active'])
        df_grouped['fc'] = df_grouped['fulfillment_channel'].apply(
            lambda x: 'FBA' if 'AMAZON' in str(x).upper() else 'FBM')

        # KPI
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🔗 Унікальних ASIN", f"{len(df_grouped):,}")
        c2.metric("✅ Active", f"{(df_grouped['status'] == 'Active').sum():,}")
        c3.metric("❌ Inactive", f"{(df_grouped['status'] != 'Active').sum():,}")
        c4.metric("💰 Avg Price", f"${df_grouped['price'].mean():.2f}")

        asin_filter = st.selectbox("Фільтр:", ["Всі", "Active", "Inactive"], key="asin_dedup_filter")
        df_show_a = df_grouped.copy()
        if asin_filter == "Active":    df_show_a = df_show_a[df_show_a['status'] == 'Active']
        elif asin_filter == "Inactive": df_show_a = df_show_a[df_show_a['status'] != 'Active']

        show_a = [c for c in ['asin1', 'seller_sku', 'item_name', 'size', 'color', 'price',
                               'sku_count', 'status', 'fc', 'brand', 'marketplace', 'main_image_url']
                  if c in df_show_a.columns]
        col_cfg_a = {
            'asin1':          st.column_config.TextColumn("ASIN"),
            'seller_sku':     st.column_config.TextColumn("SKU (first)"),
            'item_name':      st.column_config.TextColumn("Title", width="large"),
            'size':           st.column_config.TextColumn("Sizes"),
            'color':          st.column_config.TextColumn("Colors"),
            'price':          st.column_config.NumberColumn("Avg Price", format="$%.2f"),
            'sku_count':      st.column_config.NumberColumn("SKU Count"),
            'status':         st.column_config.TextColumn("Status"),
            'fc':             st.column_config.TextColumn("FC"),
            'brand':          st.column_config.TextColumn("Brand"),
            'marketplace':    st.column_config.TextColumn("MP"),
            'main_image_url': st.column_config.ImageColumn("Фото", width="small"),
        }
        st.dataframe(df_show_a[show_a].sort_values('price', ascending=False),
                     column_config=col_cfg_a, width="stretch", hide_index=True, height=500)
        st.caption(f"{len(df_show_a):,} унікальних ASIN")
        st.download_button("📥 CSV (по ASIN)", df_grouped.to_csv(index=False).encode(),
                           "listings_by_asin.csv", "text/csv", key="dl_asin")

    # ══════════════════════════════════════
    # TAB 2 — Дефекти
    # ══════════════════════════════════════
    with tabs[2]:
        st.markdown("#### ⚠️ Дефекти листингів")
        st.caption("ASIN з проблемами: suppressed, missing info, pricing alerts")
        try:
            with engine.connect() as conn:
                df_def = pd.read_sql(text("SELECT * FROM listings_defects"), conn)
            if df_def.empty:
                st.success("✅ Дефектів немає!")
            else:
                c1, c2, c3 = st.columns(3)
                c1.metric("⚠️ Всього дефектів", f"{len(df_def):,}")
                c2.metric("📦 Унікальних SKU",
                          f"{df_def['seller_sku'].nunique():,}" if 'seller_sku' in df_def.columns else "—")
                if 'alert_type' in df_def.columns:
                    c3.metric("🔴 Типів алертів", f"{df_def['alert_type'].nunique()}")

                col1, col2 = st.columns(2)
                with col1:
                    if 'alert_type' in df_def.columns:
                        at = df_def['alert_type'].value_counts().reset_index()
                        at.columns = ['Alert Type', 'Count']
                        fig_d = px.pie(at, values='Count', names='Alert Type', hole=0.4, height=300)
                        st.plotly_chart(fig_d, width="stretch")
                with col2:
                    if 'issue_description' in df_def.columns:
                        st.markdown("**Топ проблеми:**")
                        issues = df_def['issue_description'].value_counts().head(10).reset_index()
                        issues.columns = ['Issue', 'Count']
                        st.dataframe(issues, width="stretch", hide_index=True)

                show_d = [c for c in ['seller_sku', 'asin', 'product_name', 'brand', 'price',
                                       'alert_type', 'issue_description', 'status_change_date']
                          if c in df_def.columns]
                st.dataframe(df_def[show_d], width="stretch", hide_index=True, height=400)
                st.download_button("📥 CSV Дефекти", df_def.to_csv(index=False).encode(),
                                   "listings_defects.csv", "text/csv", key="dl_def")
        except Exception as e:
            st.info(f"Таблиця listings_defects не існує або порожня: {e}")

    # ══════════════════════════════════════
    # TAB 3 — Нові листинги
    # ══════════════════════════════════════
    with tabs[3]:
        st.markdown("#### 🆕 Нові листинги")
        days_new = st.selectbox("За останні:", [7, 14, 30, 60, 90], index=2, key="new_days")
        st.caption(f"Листинги додані за останні {days_new} днів (по open_date)")

        if 'open_date' in df_f.columns:
            df_new = df_f[df_f['open_date'].notna()].copy()
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=days_new)
            df_new = df_new[df_new['open_date'] >= cutoff].sort_values('open_date', ascending=False)

            if df_new.empty:
                st.info(f"Немає нових листингів за останні {days_new} днів")
            else:
                c1, c2, c3 = st.columns(3)
                c1.metric("🆕 Нових", f"{len(df_new):,}")
                c2.metric("✅ Active", f"{(df_new['status'] == 'Active').sum():,}")
                c3.metric("💰 Avg Price", f"${df_new['price'].mean():.2f}")

                daily_new = df_new.groupby(df_new['open_date'].dt.date).size().reset_index(name='count')
                daily_new.columns = ['Date', 'Count']
                fig_n = px.bar(daily_new, x='Date', y='Count',
                               color_discrete_sequence=['#4CAF50'], height=250)
                fig_n.update_layout(margin=dict(l=0, r=0, t=10, b=0))
                st.plotly_chart(fig_n, width="stretch")

                show_n = [c for c in ['seller_sku', 'asin1', 'item_name', 'price', 'status',
                                       'open_date', 'size', 'brand'] if c in df_new.columns]
                st.dataframe(df_new[show_n], width="stretch", hide_index=True, height=400)
                st.download_button("📥 CSV Нові", df_new[show_n].to_csv(index=False).encode(),
                                   "new_listings.csv", "text/csv", key="dl_new")
        else:
            st.warning("Колонка open_date відсутня")

    # ══════════════════════════════════════
    # TAB 4 — Без catalog даних
    # ══════════════════════════════════════
    with tabs[4]:
        st.markdown("#### ❓ ASIN без catalog даних")
        st.caption("Size, brand, color — відсутні. Старі/видалені ASIN яких немає в Amazon Catalog API")

        has_brand = df_f.get('brand', pd.Series(dtype=str)).fillna('').str.strip() != ''
        has_size  = df_f.get('size', pd.Series(dtype=str)).fillna('').str.strip() != ''
        df_no_cat = df_f[~has_brand & ~has_size].copy()
        df_no_cat_dedup = df_no_cat.drop_duplicates(subset='asin1')

        c1, c2, c3 = st.columns(3)
        c1.metric("❓ ASIN без catalog", f"{len(df_no_cat_dedup):,}")
        c2.metric("✅ З них Active", f"{(df_no_cat_dedup['status'] == 'Active').sum():,}")
        c3.metric("❌ Inactive", f"{(df_no_cat_dedup['status'] != 'Active').sum():,}")

        active_no_cat = (df_no_cat_dedup['status'] == 'Active').sum()
        if active_no_cat > 0:
            st.warning(f"⚠️ {active_no_cat} Active ASIN без catalog даних — варто перезапустити catalog backfill")

        show_nc = [c for c in ['asin1', 'seller_sku', 'item_name', 'price', 'status',
                                'fulfillment_channel', 'marketplace'] if c in df_no_cat_dedup.columns]
        st.dataframe(df_no_cat_dedup[show_nc].sort_values('status'),
                     width="stretch", hide_index=True, height=400)
        st.caption(f"{len(df_no_cat_dedup):,} ASIN без size/brand")
        st.download_button("📥 CSV без catalog", df_no_cat_dedup[show_nc].to_csv(index=False).encode(),
                           "no_catalog_data.csv", "text/csv", key="dl_nocat")

    # ── AI ──
    ctx = f"""Listings: {total} SKU | Active: {active_cnt} | Inactive: {inactive_cnt} | FBA: {fba_cnt} | Avg price: ${avg_price:.2f} | Unique ASIN: {unique_asins}"""
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
    tabs = st.tabs(["🚀 Shipments", "📋 Items", "🗑 Removals", "⚠️ Non-Compliance", "🏥 Inventory Health"]) 

    # ── TAB 0: Shipments ──
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

        statuses = ["Всі"] + sorted(df_ship["shipment_status"].dropna().unique().tolist()) if "shipment_status" in df_ship.columns else ["Всі"]
        sel_s = st.selectbox("Фільтр статус:", statuses, key="ship_status")
        df_s = df_ship[df_ship["shipment_status"] == sel_s] if sel_s != "Всі" else df_ship

        st.dataframe(df_s[show_s].head(200), width="stretch", hide_index=True, height=400)
        st.caption(f"{len(df_s)} відвантажень")
        st.download_button("📥 CSV Shipments", df_ship.to_csv(index=False).encode(), "shipments.csv", "text/csv")

    # ── TAB 1: Items ──
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

    # ── TAB 2: Removals ──
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

    # ── TAB 3: Non-Compliance ──
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

    # ── TAB 4: Inventory Health (з fba_inventory + orders) ──
    with tabs[4]:
        st.markdown("#### 🏥 Stock Overview — Залишки + Velocity")

        try:
            with engine.connect() as conn:
                df_inv = pd.read_sql(text('SELECT * FROM fba_inventory'), conn)
                df_ord = pd.read_sql(text("""
                    SELECT sku,
                        SUBSTRING(purchase_date,1,10)::date AS day,
                        COUNT(DISTINCT amazon_order_id) AS orders,
                        SUM(CASE WHEN quantity ~ '^[0-9]+$' THEN quantity::numeric ELSE 1 END) AS units,
                        SUM(CASE WHEN item_price ~ '^[0-9.]+$' THEN item_price::numeric ELSE 0 END) AS revenue
                    FROM orders
                    WHERE SUBSTRING(purchase_date,1,10)::date >= CURRENT_DATE - INTERVAL '90 days'
                      AND purchase_date IS NOT NULL AND purchase_date != ''
                    GROUP BY 1, 2
                    ORDER BY 2 DESC
                """), conn)
                try:
                    df_lst = pd.read_sql(text(
                        "SELECT seller_sku AS sku, status, fulfillment_channel FROM listings_all"
                    ), conn)
                except:
                    df_lst = pd.DataFrame()
        except Exception as e:
            st.error(f"Помилка: {e}")
            df_inv = pd.DataFrame()
            df_ord = pd.DataFrame()
            df_lst = pd.DataFrame()

        if df_inv.empty:
            st.warning("Немає даних inventory")
        else:
            for c in ["Available","Price","Velocity","Inbound","Reserved"]:
                if c in df_inv.columns:
                    df_inv[c] = pd.to_numeric(df_inv[c].replace("", None), errors="coerce").fillna(0)
            if "SKU" not in df_inv.columns:
                for c in df_inv.columns:
                    if c.lower() in ("sku","seller_sku"):
                        df_inv.rename(columns={c: "SKU"}, inplace=True)
                        break

            if not df_lst.empty and "SKU" in df_inv.columns:
                df_lst["status"] = df_lst["status"].astype(str).str.lower()
                status_map = df_lst.groupby("sku")["status"].first().to_dict()
                df_inv["listing_status"] = df_inv["SKU"].map(status_map).fillna("unknown")
            else:
                df_inv["listing_status"] = "unknown"

            # Конвертуємо day → Timestamp (PostgreSQL повертає date object)
            if not df_ord.empty and "day" in df_ord.columns:
                df_ord["day"] = pd.to_datetime(df_ord["day"])

            if not df_ord.empty and "SKU" in df_inv.columns:
                last_30 = df_ord[df_ord["day"] >= (pd.Timestamp.now().normalize() - pd.Timedelta(days=30))]
                sold_30 = last_30.groupby("sku")["units"].sum().to_dict()
                rev_30  = last_30.groupby("sku")["revenue"].sum().to_dict()
                df_inv["sold_30d"]      = df_inv["SKU"].map(sold_30).fillna(0).astype(int)
                df_inv["revenue_30d"]   = df_inv["SKU"].map(rev_30).fillna(0)
                df_inv["velocity_real"] = (df_inv["sold_30d"] / 30).round(2)
                df_inv["dos_real"]      = (df_inv["Available"] / df_inv["velocity_real"].replace(0, float("nan"))).round(0).fillna(0)
            else:
                df_inv["sold_30d"] = 0
                df_inv["revenue_30d"] = 0
                df_inv["velocity_real"] = 0
                df_inv["dos_real"] = 0

            df_inv["stock_value"] = df_inv["Available"] * df_inv["Price"]

            # KPI
            active_cnt   = int((df_inv["listing_status"] == "active").sum())
            inactive_cnt = int((df_inv["listing_status"] != "active").sum())
            total_avail  = int(df_inv["Available"].sum())
            total_value  = df_inv["stock_value"].sum()
            total_sold   = int(df_inv["sold_30d"].sum())
            oos_cnt      = int((df_inv["Available"] == 0).sum())
            low14_cnt    = int(((df_inv["dos_real"] > 0) & (df_inv["dos_real"] < 14)).sum())

            c1,c2,c3,c4,c5,c6 = st.columns(6)
            c1.metric("✅ Active", f"{active_cnt}")
            c2.metric("❌ Inactive", f"{inactive_cnt}")
            c3.metric("📦 Available", f"{total_avail:,}")
            c4.metric("💰 Stock Value", f"${total_value:,.0f}")
            c5.metric("🛒 Sold 30д", f"{total_sold:,}")
            c6.metric("🔴 OOS", f"{oos_cnt}")

            # Інсайти
            _ic2 = st.columns(3)
            if low14_cnt > 0:
                with _ic2[0]: insight_card("🔴", "Low Stock",
                    f"<b>{low14_cnt} SKU</b> закінчаться за <b><14 днів</b>", "#2b0d0d")
            else:
                with _ic2[0]: insight_card("🟢", "Запаси",
                    "Всі активні SKU мають 14+ днів запасу", "#0d2b1e")
            active_pct = active_cnt / (active_cnt + inactive_cnt) * 100 if (active_cnt + inactive_cnt) > 0 else 0
            with _ic2[1]: insight_card("📊", "Active Rate",
                f"<b>{active_pct:.0f}%</b> листингів активні ({active_cnt}/{active_cnt+inactive_cnt})",
                "#0d2b1e" if active_pct >= 80 else "#2b2400")
            turnover = total_avail / (total_sold / 30) if total_sold > 0 else 999
            with _ic2[2]: insight_card("🔄", "Оборотність",
                f"Запас на <b>{turnover:.0f} днів</b> при поточному темпі продажів",
                "#0d2b1e" if turnover < 90 else "#2b2400" if turnover < 180 else "#2b0d0d")

            st.markdown("---")

            # Stock Table
            st.markdown("##### 📦 Таблиця залишків (Active / Inactive)")
            status_filter = st.selectbox("Фільтр:", ["Всі", "Active", "Inactive", "OOS", "Low Stock <14д"], key="health_filter_ops")
            df_show_h = df_inv.copy()
            if status_filter == "Active":
                df_show_h = df_show_h[df_show_h["listing_status"] == "active"]
            elif status_filter == "Inactive":
                df_show_h = df_show_h[df_show_h["listing_status"] != "active"]
            elif status_filter == "OOS":
                df_show_h = df_show_h[df_show_h["Available"] == 0]
            elif status_filter == "Low Stock <14д":
                df_show_h = df_show_h[(df_show_h["dos_real"] > 0) & (df_show_h["dos_real"] < 14)]

            show_cols_h = [c for c in ["SKU","ASIN","Available","Price","stock_value",
                         "sold_30d","revenue_30d","velocity_real","dos_real","listing_status"]
                         if c in df_show_h.columns]
            rename_h = {"stock_value":"💰 Value","sold_30d":"🛒 Sold 30д",
                      "revenue_30d":"💰 Rev 30д","velocity_real":"⚡ Vel/день",
                      "dos_real":"📅 DoS","listing_status":"Status"}

            df_tbl_h = df_show_h[show_cols_h].rename(columns=rename_h).sort_values("📅 DoS").head(300)
            fmt_h = {}
            if "Price" in df_tbl_h.columns: fmt_h["Price"] = "${:.2f}"
            if "💰 Value" in df_tbl_h.columns: fmt_h["💰 Value"] = "${:,.0f}"
            if "💰 Rev 30д" in df_tbl_h.columns: fmt_h["💰 Rev 30д"] = "${:,.0f}"
            if "📅 DoS" in df_tbl_h.columns: fmt_h["📅 DoS"] = "{:.0f}"

            def color_status_h(val):
                if val == "active": return "color:#4CAF50"
                return "color:#F44336"

            styled_h = df_tbl_h.style.format(fmt_h)
            if "Status" in df_tbl_h.columns:
                styled_h = styled_h.map(color_status_h, subset=["Status"])
            st.dataframe(styled_h, width="stretch", hide_index=True, height=400)
            st.caption(f"{len(df_tbl_h)} з {len(df_show_h)} SKU")

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("##### 📊 Active vs Inactive")
                status_cnt = df_inv["listing_status"].value_counts().reset_index()
                status_cnt.columns = ["Status", "Count"]
                fig_s = px.pie(status_cnt, values="Count", names="Status", hole=0.4,
                               color_discrete_map={"active":"#4CAF50","inactive":"#F44336","unknown":"#888"},
                               height=300)
                st.plotly_chart(fig_s, width="stretch")

            with col2:
                st.markdown("##### 🏆 Топ 10 SKU по вартості")
                top_val = df_inv[df_inv["stock_value"] > 0].nlargest(10, "stock_value")
                if not top_val.empty:
                    fig_v = go.Figure(go.Bar(
                        x=top_val["stock_value"], y=top_val["SKU"], orientation="h",
                        marker_color="#5B9BD5",
                        text=[f"${v:,.0f}" for v in top_val["stock_value"]], textposition="outside"
                    ))
                    fig_v.update_layout(height=300, yaxis={"categoryorder":"total ascending"},
                                        margin=dict(l=0,r=80,t=10,b=0))
                    st.plotly_chart(fig_v, width="stretch")

            st.markdown("---")

            # Sales Velocity
            st.markdown("##### 📈 Продажі по днях / тижнях")
            if not df_ord.empty:
                gran = st.radio("Гранулярність:", ["День", "Тиждень"], horizontal=True, key="vel_gran_ops")
                df_vel = df_ord.copy()
                df_vel["day"] = pd.to_datetime(df_vel["day"])

                if gran == "Тиждень":
                    df_agg = df_vel.resample("W", on="day").agg(
                        {"orders":"sum","units":"sum","revenue":"sum"}).reset_index()
                else:
                    df_agg = df_vel.groupby("day").agg(
                        {"orders":"sum","units":"sum","revenue":"sum"}).reset_index()

                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"###### 🛒 Units sold ({gran})")
                    fig_u = go.Figure()
                    fig_u.add_trace(go.Bar(x=df_agg["day"], y=df_agg["units"],
                                           marker_color="#4CAF50", opacity=0.85,
                                           text=df_agg["units"].astype(int), textposition="outside"))
                    if gran == "День" and len(df_agg) >= 7:
                        ma = df_agg["units"].rolling(7, min_periods=1).mean()
                        fig_u.add_trace(go.Scatter(x=df_agg["day"], y=ma,
                                                   mode="lines", name="MA-7",
                                                   line=dict(color="#FFC107", width=2, dash="dot")))
                    fig_u.update_layout(height=320, margin=dict(l=0,r=0,t=10,b=0), showlegend=True)
                    st.plotly_chart(fig_u, width="stretch")

                with col2:
                    st.markdown(f"###### 💰 Revenue ({gran})")
                    fig_r = go.Figure()
                    fig_r.add_trace(go.Bar(x=df_agg["day"], y=df_agg["revenue"],
                                           marker_color="#5B9BD5", opacity=0.85,
                                           text=[f"${v:,.0f}" for v in df_agg["revenue"]], textposition="outside"))
                    if gran == "День" and len(df_agg) >= 7:
                        ma_r = df_agg["revenue"].rolling(7, min_periods=1).mean()
                        fig_r.add_trace(go.Scatter(x=df_agg["day"], y=ma_r,
                                                   mode="lines", name="MA-7",
                                                   line=dict(color="#FFC107", width=2, dash="dot")))
                    fig_r.update_layout(height=320, margin=dict(l=0,r=0,t=10,b=0),
                                        yaxis=dict(tickprefix="$"), showlegend=True)
                    st.plotly_chart(fig_r, width="stretch")

                # Топ SKU velocity
                st.markdown("##### 🏆 Топ 15 SKU по продажах (30д)")
                sku_vel = df_vel[df_vel["day"] >= (pd.Timestamp.now().normalize() - pd.Timedelta(days=30))]
                sku_agg = sku_vel.groupby("sku").agg(
                    units=("units","sum"), revenue=("revenue","sum"), orders=("orders","sum")
                ).reset_index().nlargest(15, "units")

                if not sku_agg.empty:
                    col1, col2 = st.columns(2)
                    with col1:
                        fig_su = go.Figure(go.Bar(
                            x=sku_agg["units"], y=sku_agg["sku"], orientation="h",
                            marker_color="#4CAF50",
                            text=[f"{int(v):,}" for v in sku_agg["units"]], textposition="outside"
                        ))
                        fig_su.update_layout(height=max(300, len(sku_agg)*35),
                                             yaxis={"categoryorder":"total ascending"},
                                             title="Units sold", margin=dict(l=0,r=60,t=30,b=0))
                        st.plotly_chart(fig_su, width="stretch")

                    with col2:
                        fig_sr = go.Figure(go.Bar(
                            x=sku_agg["revenue"], y=sku_agg["sku"], orientation="h",
                            marker_color="#5B9BD5",
                            text=[f"${v:,.0f}" for v in sku_agg["revenue"]], textposition="outside"
                        ))
                        fig_sr.update_layout(height=max(300, len(sku_agg)*35),
                                             yaxis={"categoryorder":"total ascending"},
                                             title="Revenue $", margin=dict(l=0,r=60,t=30,b=0))
                        st.plotly_chart(fig_sr, width="stretch")

                # Velocity таблиця
                st.markdown("##### 📋 Velocity таблиця (всі SKU з продажами)")
                sku_all = sku_vel.groupby("sku").agg(
                    units=("units","sum"), revenue=("revenue","sum"), orders=("orders","sum")
                ).reset_index()
                sku_all["vel/день"] = (sku_all["units"] / 30).round(2)
                sku_all = sku_all.sort_values("units", ascending=False)
                st.dataframe(
                    sku_all.rename(columns={"sku":"SKU","units":"Units 30д","revenue":"Revenue 30д","orders":"Orders"})
                        .style.format({"Revenue 30д":"${:,.0f}","vel/день":"{:.2f}"}),
                    width="stretch", hide_index=True, height=400
                )
                st.download_button("📥 CSV Velocity",
                    sku_all.to_csv(index=False).encode(), "velocity_30d.csv", "text/csv")
            else:
                st.warning("Немає даних orders за 90 днів")


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
                    SELECT posted_date, event_type, charge_type,
                           NULLIF(amount,'')::numeric AS amount
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

def show_restock_agent(t=None):
    import json, requests as _req
    st.markdown("### 📦 Restock Agent")
    st.caption("AI аналіз залишків · Gemini рекомендації · Human-in-the-loop рішення")

    engine = get_engine()
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY","")
    GEMINI_MODEL   = os.environ.get("GEMINI_MODEL","gemini-1.5-flash")

    # ── Ensure decisions table ──
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.agent_decisions (
                    id SERIAL PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    agent_type TEXT, sku TEXT, asin TEXT, alert_type TEXT,
                    recommendation TEXT, ai_analysis TEXT, details TEXT,
                    decision TEXT DEFAULT 'PENDING', decided_at TIMESTAMP, message_id TEXT
                )
            """))
            conn.commit()
    except: pass

    # ── Аналіз inventory ──
    def analyze_inventory():
        try:
            with engine.connect() as conn:
                df = pd.read_sql(text("""
                    SELECT "SKU" as sku, "ASIN" as asin, "Product Name" as name,
                        COALESCE(NULLIF("Available",'')::numeric, 0) as available,
                        COALESCE(NULLIF("Inbound",'')::numeric, 0) as inbound,
                        COALESCE(NULLIF("Velocity",'')::numeric, 0) as velocity,
                        COALESCE(NULLIF("Days of Supply",'')::numeric, 0) as dos,
                        COALESCE(NULLIF("Price",'')::numeric, 0) as price,
                        "Store Name" as store
                    FROM public.fba_inventory
                    WHERE COALESCE(NULLIF("Velocity",'')::numeric, 0) > 0
                    ORDER BY COALESCE(NULLIF("Days of Supply",'')::numeric, 999) ASC
                """), conn)
            df['recommended'] = ((df['velocity'] * 90) - df['available'] - df['inbound']).clip(lower=0).astype(int)
            df['value']       = df['recommended'] * df['price']
            df['alert_type']  = df['dos'].apply(lambda x: 'CRITICAL' if x < 14 else 'WARNING' if x < 30 else 'OK')
            return df
        except Exception as e:
            st.error(f"Помилка inventory: {e}"); return pd.DataFrame()

    def get_sales_trend(sku):
        try:
            with engine.connect() as conn:
                df = pd.read_sql(text("""
                    SELECT SUBSTRING(purchase_date,1,10)::date as day,
                        COUNT(*) as orders,
                        SUM(COALESCE(NULLIF(quantity,'')::numeric,1)) as units
                    FROM orders WHERE sku = :sku
                      AND SUBSTRING(purchase_date,1,10)::date >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY 1 ORDER BY 1
                """), conn, params={"sku": sku})
            if df.empty: return "Даних за 30 днів немає"
            return "\n".join([f"{r['day']}: {int(r['units'])} units" for _, r in df.iterrows()])
        except: return "Помилка отримання тренду"

    def get_sku_fees(asin):
        """Реальні fees з finance_events по ASIN"""
        try:
            with engine.connect() as conn:
                df = pd.read_sql(text("""
                    SELECT charge_type,
                           AVG(ABS(NULLIF(amount,'')::numeric)) as avg_fee,
                           COUNT(*) as cnt
                    FROM finance_events
                    WHERE asin = :asin
                      AND charge_type IN ('Commission','FBAPerUnitFulfillmentFee',
                                          'FixedClosingFee','ShippingChargeback')
                      AND NULLIF(amount,'')::numeric < 0
                    GROUP BY charge_type
                """), conn, params={"asin": asin})
            if df.empty:
                return None
            fees = {}
            for _, r in df.iterrows():
                fees[r['charge_type']] = round(float(r['avg_fee']), 2)
            return fees
        except:
            return None

    def gemini_analyze(row, trend):
        fees = get_sku_fees(row['asin'])
        commission  = fees.get('Commission', 0) if fees else 0
        fba_fee     = fees.get('FBAPerUnitFulfillmentFee', 0) if fees else 0
        total_fees  = commission + fba_fee
        net_unit    = float(row['price']) - total_fees if row['price'] > 0 else 0
        margin_pct  = net_unit / float(row['price']) * 100 if row['price'] > 0 else 0
        roi_total   = net_unit * int(row['recommended'])

        fees_block = ""
        if fees:
            fees_block = (f"\nReальні fees (з finance_events):\n"
                         f"  Commission:  -${commission:.2f}/unit\n"
                         f"  FBA Fee:     -${fba_fee:.2f}/unit\n"
                         f"  Net/unit:    ${net_unit:.2f}\n"
                         f"  Маржа:       {margin_pct:.0f}%\n"
                         f"  ROI поставки {int(row['recommended'])} units: ${roi_total:,.0f} чистими\n")
        else:
            fees_block = "\nFees: немає даних у finance_events для цього ASIN\n"

        if not GEMINI_API_KEY:
            return (f"РЕКОМЕНДАЦІЯ: {int(row['recommended'])} units\n"
                    f"МАРЖА: ${net_unit:.2f}/unit ({margin_pct:.0f}%)\n"
                    f"ROI поставки: ${roi_total:,.0f} чистими\n"
                    f"ПРИЧИНА: Базовий розрахунок (90д покриття)\n"
                    f"РИЗИК: Out of stock за {int(row['dos'])} днів\n"
                    f"ПРІОРИТЕТ: {'КРИТИЧНИЙ' if row['dos'] < 14 else 'ВИСОКИЙ'}")
        prompt = (f"Amazon FBA inventory менеджер (merino wool apparel).\n\n"
                  f"SKU: {row['sku']} | {str(row['name'])[:50]}\n"
                  f"Available: {int(row['available'])} | Inbound: {int(row['inbound'])} | Velocity: {row['velocity']}/день\n"
                  f"Days of Supply: {int(row['dos'])} | Ціна: ${row['price']} | Базова рек: {int(row['recommended'])} units\n"
                  f"{fees_block}\n"
                  f"Тренд продажів (30д):\n{trend}\n\n"
                  f"Відповідь строго у форматі:\n"
                  f"РЕКОМЕНДАЦІЯ: [число] units\n"
                  f"МАРЖА: $[net/unit] ([%]%)\n"
                  f"ROI поставки: $[сума] чистими\n"
                  f"ПРИЧИНА: [1 речення]\n"
                  f"РИЗИК: [1 речення]\n"
                  f"ПРІОРИТЕТ: [КРИТИЧНИЙ/ВИСОКИЙ/СЕРЕДНІЙ]")
        try:
            r = _req.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
                json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=20
            )
            return r.json()["candidates"][0]["content"]["parts"][0]["text"]
        except:
            return f"РЕКОМЕНДАЦІЯ: {int(row['recommended'])} units\nПРИЧИНА: Базовий розрахунок\nРИЗИК: OOS за {int(row['dos'])} днів\nПРІОРИТЕТ: КРИТИЧНИЙ"

    def save_decision(sku, asin, alert_type, recommendation, ai_analysis, details):
        try:
            with engine.connect() as conn:
                result = conn.execute(text(
                    "INSERT INTO public.agent_decisions "
                    "(agent_type, sku, asin, alert_type, recommendation, ai_analysis, details) "
                    "VALUES ('restock', :sku, :asin, :at, :rec, :ai, :det) RETURNING id"
                ), {"sku": sku, "asin": asin, "at": alert_type,
                    "rec": recommendation, "ai": ai_analysis, "det": json.dumps(details, default=str)})
                conn.commit()
                return result.fetchone()[0]
        except Exception as e:
            st.error(f"Save error: {e}"); return None

    def update_decision(dec_id, decision):
        try:
            with engine.connect() as conn:
                conn.execute(text(
                    "UPDATE public.agent_decisions SET decision=:d, decided_at=NOW() WHERE id=:id"
                ), {"d": decision, "id": dec_id})
                conn.commit()
        except Exception as e:
            st.error(f"Update error: {e}")

    def load_decisions():
        try:
            with engine.connect() as conn:
                return pd.read_sql(text(
                    "SELECT id, created_at, sku, asin, alert_type, recommendation, decision, decided_at "
                    "FROM public.agent_decisions WHERE agent_type='restock' ORDER BY created_at DESC LIMIT 50"
                ), conn)
        except: return pd.DataFrame()

    # ── UI ──
    df_inv = analyze_inventory()
    if df_inv.empty: st.warning("Немає даних inventory"); return

    critical = df_inv[df_inv['alert_type']=='CRITICAL']
    warning  = df_inv[df_inv['alert_type']=='WARNING']
    ok       = df_inv[df_inv['alert_type']=='OK']

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("🔴 Критичних (<14д)",   len(critical))
    c2.metric("🟡 Попереджень (<30д)", len(warning))
    c3.metric("✅ В нормі",            len(ok))
    c4.metric("💰 Вартість поповнення",
              f"${df_inv[df_inv['alert_type']!='OK']['value'].sum():,.0f}")
    st.markdown("---")

    # ── Алерти ──
    st.markdown("### 🚨 Алерти — потребують рішення")
    alerts_df = df_inv[df_inv['alert_type'].isin(['CRITICAL','WARNING'])].copy()
    alerts_df = alerts_df[alerts_df['recommended'] > 0]

    if alerts_df.empty:
        st.success("✅ Всі SKU в нормі — поповнення не потрібне")
    else:
        for _, row in alerts_df.iterrows():
            icon  = "🔴" if row['alert_type']=='CRITICAL' else "🟡"
            color = "#2b0d0d" if row['alert_type']=='CRITICAL' else "#2b2400"
            bc    = "#ef4444" if row['alert_type']=='CRITICAL' else "#f59e0b"
            dos_str = f"{int(row['dos'])}д" if row['dos'] > 0 else "OOS"

            with st.expander(
                f"{icon} {row['sku']} — {dos_str} залишилось · рек. {int(row['recommended'])} units (~${row['value']:,.0f})",
                expanded=(row['alert_type']=='CRITICAL')
            ):
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"""
<div style="background:{color};border-left:4px solid {bc};border-radius:8px;padding:14px 18px">
  <div style="font-size:16px;font-weight:700;color:#fff">{row['sku']}</div>
  <div style="font-size:12px;color:#aaa;margin-bottom:8px">{row['asin']}</div>
  <div style="font-size:13px;color:#ddd;margin-bottom:8px">{str(row['name'])[:60]}</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;font-size:13px;color:#fff">
    <div>📦 Available: <b>{int(row['available'])}</b></div>
    <div>🚛 Inbound: <b>{int(row['inbound'])}</b></div>
    <div>⚡ Velocity: <b>{row['velocity']:.2f}/д</b></div>
    <div>📅 Days: <b style="color:{bc}">{dos_str}</b></div>
    <div>💰 Price: <b>${row['price']}</b></div>
    <div>📬 Рек: <b>{int(row['recommended'])} units</b></div>
  </div>
</div>""", unsafe_allow_html=True)

                with col2:
                    ai_key = f"ai_{row['sku']}"
                    if ai_key not in st.session_state:
                        st.session_state[ai_key] = None
                    if st.session_state[ai_key] is None:
                        if st.button(f"🤖 Gemini аналіз", key=f"btn_ai_{row['sku']}",
                                     type="primary", use_container_width=True):
                            with st.spinner("Gemini аналізує..."):
                                trend = get_sales_trend(row['sku'])
                                ai    = gemini_analyze(row, trend)
                                st.session_state[ai_key] = ai
                    if st.session_state.get(ai_key):
                        st.markdown(f'''
<div style="background:#1a1a2e;border:1px solid #6366f1;border-radius:8px;
            padding:14px 18px;font-size:13px;line-height:1.7;white-space:pre-wrap;color:#fff">{st.session_state[ai_key]}</div>''',
                            unsafe_allow_html=True)


                st.markdown("**Ваше рішення:**")
                b1, b2, b3 = st.columns(3)
                dec_key = f"dec_{row['sku']}"
                if dec_key not in st.session_state:
                    st.session_state[dec_key] = None

                if st.session_state[dec_key] is None:
                    with b1:
                        if st.button("✅ Так, замовити", key=f"yes_{row['sku']}",
                                     use_container_width=True, type="primary"):
                            ai = st.session_state.get(ai_key, "Без AI аналізу")
                            did = save_decision(row['sku'], row['asin'], row['alert_type'],
                                f"Send {int(row['recommended'])} units", ai, row.to_dict())
                            if did: update_decision(did, "YES"); st.session_state[dec_key] = "YES"; st.rerun()
                    with b2:
                        if st.button("❌ Пропустити", key=f"no_{row['sku']}", use_container_width=True):
                            ai = st.session_state.get(ai_key, "Без AI аналізу")
                            did = save_decision(row['sku'], row['asin'], row['alert_type'],
                                f"Skip {row['sku']}", ai, row.to_dict())
                            if did: update_decision(did, "NO"); st.session_state[dec_key] = "NO"; st.rerun()
                    with b3:
                        if st.button("⏰ Пізніше", key=f"later_{row['sku']}", use_container_width=True):
                            st.session_state[dec_key] = "LATER"; st.rerun()
                else:
                    icons = {"YES":"✅ Замовити", "NO":"❌ Пропустити", "LATER":"⏰ Пізніше"}
                    st.success(f"Рішення: **{icons.get(st.session_state[dec_key], '')}**")
                    if st.button("↩️ Змінити", key=f"reset_{row['sku']}"):
                        st.session_state[dec_key] = None; st.rerun()

    st.markdown("---")

    # ── Таблиця ──
    st.markdown("### 📋 Всі SKU — статус")
    show_df = df_inv[['sku','asin','available','inbound','velocity','dos','price','recommended','value','alert_type']].copy()
    show_df.columns = ['SKU','ASIN','Avail','Inbound','Velocity','DoS','Price','Рек.','Вартість','Статус']

    def color_status(val):
        if val == 'CRITICAL': return 'background-color:#2b0d0d;color:#ef4444'
        if val == 'WARNING':  return 'background-color:#2b2400;color:#f59e0b'
        return 'background-color:#0d2b1e;color:#22c55e'

    st.dataframe(show_df.style.applymap(color_status, subset=['Статус'])
                 .format({'Price':'${:.2f}','Вартість':'${:,.0f}','Velocity':'{:.2f}','DoS':'{:.0f}'}),
                 width="stretch", hide_index=True, height=400)

    st.markdown("---")

    # ── Історія ──
    st.markdown("### 📊 Історія рішень")
    df_dec = load_decisions()
    if not df_dec.empty:
        c1,c2,c3 = st.columns(3)
        c1.metric("✅ YES",     len(df_dec[df_dec['decision']=='YES']))
        c2.metric("❌ NO",      len(df_dec[df_dec['decision']=='NO']))
        c3.metric("⏳ PENDING", len(df_dec[df_dec['decision']=='PENDING']))
        st.dataframe(df_dec[['created_at','sku','alert_type','recommendation','decision','decided_at']],
                     width="stretch", hide_index=True, height=300)
    else:
        st.info("Рішень ще немає — запусти аналіз вище")

def show_forecast(t=None):
    import requests as _req
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import PolynomialFeatures
    import numpy as np
    import datetime as _dt

    st.markdown("### 📈 Прогноз продажів (ML + AI)")
    st.caption("Лінійна регресія + Gemini аналіз · на основі реальних даних orders")

    engine = get_engine()
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY","")
    GEMINI_MODEL   = os.environ.get("GEMINI_MODEL","gemini-1.5-flash")

    # ── Sidebar ──
    forecast_days = st.sidebar.selectbox("📅 Горизонт прогнозу:", [7, 14, 30, 60, 90], index=2, key="fc_days")
    granularity   = st.sidebar.selectbox("📊 Гранулярність:", ["День", "Тиждень", "Місяць"], index=0, key="fc_gran")
    history_days  = st.sidebar.selectbox("📖 Історія (днів):", [30, 60, 90, 180, 365], index=3, key="fc_hist")
    sku_filter    = st.sidebar.text_input("🔍 SKU (опц.):", "", key="fc_sku")

    # ── Завантаження даних ──
    try:
        with engine.connect() as conn:
            where_sku = "AND sku = :sku" if sku_filter else ""
            df_raw = pd.read_sql(text(f"""
                SELECT
                    SUBSTRING(purchase_date,1,10)::date AS day,
                    COUNT(DISTINCT amazon_order_id) AS orders,
                    SUM(CASE WHEN item_price ~ '^[0-9.]+$' THEN item_price::numeric ELSE 0 END) AS revenue,
                    SUM(CASE WHEN quantity ~ '^[0-9]+$' THEN quantity::numeric ELSE 1 END) AS units
                FROM orders
                WHERE SUBSTRING(purchase_date,1,10)::date >= CURRENT_DATE - INTERVAL '{history_days} days'
                  AND purchase_date IS NOT NULL AND purchase_date != ''
                  {where_sku}
                GROUP BY 1 ORDER BY 1
            """), conn, params={"sku": sku_filter} if sku_filter else {})
    except Exception as e:
        st.error(f"Помилка завантаження: {e}"); return

    if df_raw.empty or len(df_raw) < 7:
        st.warning("Недостатньо даних для прогнозу (мінімум 7 днів)"); return

    df_raw['day'] = pd.to_datetime(df_raw['day'])

    # Агрегація
    if granularity == "Тиждень":
        df = df_raw.resample('W', on='day').agg({'orders':'sum','revenue':'sum','units':'sum'}).reset_index()
    elif granularity == "Місяць":
        df = df_raw.resample('MS', on='day').agg({'orders':'sum','revenue':'sum','units':'sum'}).reset_index()
    else:
        df = df_raw.copy()

    df = df.dropna().reset_index(drop=True)
    n = len(df)

    # ── KPI ──
    avg_rev   = df['revenue'].mean()
    avg_ord   = df['orders'].mean()
    total_rev = df['revenue'].sum()
    trend_7   = df['revenue'].tail(7).mean() if n >= 7 else avg_rev
    trend_prev= df['revenue'].iloc[-14:-7].mean() if n >= 14 else avg_rev
    trend_chg = (trend_7 - trend_prev) / trend_prev * 100 if trend_prev > 0 else 0
    trend_col = "#4CAF50" if trend_chg >= 0 else "#F44336"

    st.markdown(f"""
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px">
  <div style="background:#1a2b1e;border:1px solid #2d4a30;border-radius:8px;padding:14px;text-align:center">
    <div style="font-size:11px;color:#888">📊 Avg/день</div>
    <div style="font-size:24px;font-weight:800;color:#4CAF50">{_fmt(avg_rev)}</div>
  </div>
  <div style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:8px;padding:14px;text-align:center">
    <div style="font-size:11px;color:#888">📦 Avg замовлень</div>
    <div style="font-size:24px;font-weight:800;color:#5B9BD5">{avg_ord:.0f}</div>
  </div>
  <div style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:8px;padding:14px;text-align:center">
    <div style="font-size:11px;color:#888">💰 Всього ({history_days}д)</div>
    <div style="font-size:24px;font-weight:800;color:#FFC107">{_fmt(total_rev)}</div>
  </div>
  <div style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:8px;padding:14px;text-align:center">
    <div style="font-size:11px;color:#888">📈 Тренд (7д vs 7д)</div>
    <div style="font-size:24px;font-weight:800;color:{trend_col}">{trend_chg:+.1f}%</div>
  </div>
</div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ══════════════════════════════════════
    # ML ПРОГНОЗ
    # ══════════════════════════════════════
    X = np.arange(n).reshape(-1,1)
    y_rev = df['revenue'].values
    y_ord = df['orders'].values

    # Polynomial regression degree 2
    poly = PolynomialFeatures(degree=2)
    X_poly = poly.fit_transform(X)
    lr_rev = LinearRegression().fit(X_poly, y_rev)
    lr_ord = LinearRegression().fit(X_poly, y_ord)

    # Майбутні точки
    X_future = np.arange(n, n + forecast_days).reshape(-1,1)
    X_future_poly = poly.transform(X_future)
    y_pred_rev = lr_rev.predict(X_future_poly).clip(min=0)
    y_pred_ord = lr_ord.predict(X_future_poly).clip(min=0)

    # Дати прогнозу
    last_date = df['day'].max()
    if granularity == "Тиждень":
        future_dates = pd.date_range(last_date + pd.Timedelta(days=7), periods=forecast_days//7 or 1, freq='W')
        y_pred_rev = y_pred_rev[:len(future_dates)]
        y_pred_ord = y_pred_ord[:len(future_dates)]
    elif granularity == "Місяць":
        future_dates = pd.date_range(last_date + pd.DateOffset(months=1), periods=forecast_days//30 or 1, freq='MS')
        y_pred_rev = y_pred_rev[:len(future_dates)]
        y_pred_ord = y_pred_ord[:len(future_dates)]
    else:
        future_dates = pd.date_range(last_date + pd.Timedelta(days=1), periods=forecast_days)

    df_forecast = pd.DataFrame({'day': future_dates, 'rev_pred': y_pred_rev, 'ord_pred': y_pred_ord})
    total_forecast = df_forecast['rev_pred'].sum()
    avg_forecast   = df_forecast['rev_pred'].mean()

    # ── Графік ──
    st.markdown(f"#### 📈 Прогноз виручки на {forecast_days} днів")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['day'], y=df['revenue'],
        name='Фактична виручка',
        mode='lines', line=dict(color='#4CAF50', width=2),
        fill='tozeroy', fillcolor='rgba(76,175,80,0.1)'
    ))
    # Moving average
    ma = df['revenue'].rolling(7, min_periods=1).mean()
    fig.add_trace(go.Scatter(
        x=df['day'], y=ma,
        name='MA-7', mode='lines',
        line=dict(color='#FFC107', width=1.5, dash='dot')
    ))
    fig.add_trace(go.Scatter(
        x=df_forecast['day'], y=df_forecast['rev_pred'],
        name=f'Прогноз ({forecast_days}д)',
        mode='lines+markers',
        line=dict(color='#5B9BD5', width=2, dash='dash'),
        marker=dict(size=5)
    ))
    # Confidence interval (±20%)
    fig.add_trace(go.Scatter(
        x=pd.concat([df_forecast['day'], df_forecast['day'][::-1]]),
        y=pd.concat([df_forecast['rev_pred']*1.2, df_forecast['rev_pred'][::-1]*0.8]),
        fill='toself', fillcolor='rgba(91,155,213,0.1)',
        line=dict(color='rgba(255,255,255,0)'),
        name='Довірчий інтервал ±20%', showlegend=True
    ))
    # vline removed - incompatible with date axis in this plotly version
    fig.update_layout(height=380, margin=dict(l=0,r=0,t=10,b=0),
                      legend=dict(orientation='h', y=1.12),
                      yaxis=dict(tickprefix="$", tickformat=".2s"))
    st.plotly_chart(fig, width="stretch")

    # ── Прогноз деталі ──
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 📊 Прогноз замовлень")
        fig2 = go.Figure(go.Bar(
            x=df_forecast['day'], y=df_forecast['ord_pred'],
            marker_color='#5B9BD5', opacity=0.8,
            text=[f"{int(v)}" for v in df_forecast['ord_pred']], textposition='outside'
        ))
        fig2.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig2, width="stretch")

    with col2:
        st.markdown("#### 💰 Підсумок прогнозу")
        fc_month = df_forecast['rev_pred'].sum() / forecast_days * 30
        fc_year  = df_forecast['rev_pred'].mean() * 365
        st.markdown(f"""
<div style="background:#1a1a2e;border:1px solid #2d2d4a;border-radius:8px;padding:16px">
  <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #222">
    <span style="color:#888">📅 Прогноз за {forecast_days}д</span>
    <b style="color:#5B9BD5">{_fmt(total_forecast)}</b>
  </div>
  <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #222">
    <span style="color:#888">📊 Avg/день</span>
    <b style="color:#4CAF50">{_fmt(avg_forecast)}</b>
  </div>
  <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #222">
    <span style="color:#888">📆 Прогноз/місяць</span>
    <b style="color:#FFC107">{_fmt(fc_month)}</b>
  </div>
  <div style="display:flex;justify-content:space-between;padding:6px 0">
    <span style="color:#888">🗓 Прогноз/рік</span>
    <b style="color:#AB47BC">{_fmt(fc_year)}</b>
  </div>
</div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ══════════════════════════════════════
    # GEMINI AI АНАЛІЗ
    # ══════════════════════════════════════
    st.markdown("#### 🤖 Gemini AI — аналіз прогнозу")

    if "forecast_analysis" not in st.session_state:
        st.session_state.forecast_analysis = None

    if st.button("🤖 Запустити Gemini аналіз", type="primary", use_container_width=False):
        with st.spinner("Gemini аналізує тренди..."):
            # Prepare data summary
            weekly = df_raw.resample('W', on='day')['revenue'].sum()
            weekly_str = "\n".join([f"{d.date()}: ${v:,.0f}" for d,v in weekly.tail(8).items()])

            # Best/worst days
            best_day  = df_raw.nlargest(1,'revenue').iloc[0]
            worst_day = df_raw[df_raw['revenue']>0].nsmallest(1,'revenue').iloc[0]

            prompt = f"""Ти — Amazon FBA бізнес-аналітик для merino wool бренду.

ДАНІ ПРОДАЖІВ (останні {history_days} днів):
Загальна виручка: ${total_rev:,.0f}
Середня/день: ${avg_rev:,.0f}
Тренд (7д): {trend_chg:+.1f}%
Найкращий день: {best_day['day'].date()} — ${best_day['revenue']:,.0f}
Найгірший день: {worst_day['day'].date()} — ${worst_day['revenue']:,.0f}

Тижневі дані (останні 8 тижнів):
{weekly_str}

ML ПРОГНОЗ на {forecast_days} днів:
Прогнозована виручка: ${total_forecast:,.0f}
Середня/день: ${avg_forecast:,.0f}
Прогноз/місяць: ${fc_month:,.0f}
Прогноз/рік: ${fc_year:,.0f}

Дай структурований аналіз:
ТРЕНД: [опис тренду — зростання/падіння/стабільність]
СЕЗОННІСТЬ: [чи є сезонні паттерни?]
ПРОГНОЗ: [чи реалістичний ML прогноз?]
РИЗИКИ: [що може погіршити результат?]
МОЖЛИВОСТІ: [де є потенціал зростання?]
ДІЇ: [3 конкретних кроки для покращення продажів]"""

            if GEMINI_API_KEY:
                try:
                    r = _req.post(
                        f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
                        json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=30
                    )
                    analysis = r.json()["candidates"][0]["content"]["parts"][0]["text"]
                except Exception as e:
                    analysis = f"Помилка Gemini: {e}"
            else:
                analysis = f"""ТРЕНД: Виручка {trend_chg:+.1f}% за останні 7 днів vs попередні 7 днів.
СЕЗОННІСТЬ: Потрібні дані за 12+ місяців для виявлення сезонності.
ПРОГНОЗ: ML прогнозує ${total_forecast:,.0f} за {forecast_days} днів (${avg_forecast:,.0f}/день).
РИЗИКИ: Повернення 16%+ може суттєво знизити net виручку.
МОЖЛИВОСТІ: Зростання +{trend_chg:.0f}% вказує на позитивну динаміку.
ДІЇ: 1) Знизити повернення 2) Збільшити AOV через bundle 3) Розширити асортимент"""
            st.session_state.forecast_analysis = analysis

    if st.session_state.forecast_analysis:
        st.markdown(st.session_state.forecast_analysis)

    st.markdown("---")

    # ── По SKU прогноз ──
    st.markdown("#### 🏆 Топ 10 SKU — прогноз (trend-based)")
    try:
        with engine.connect() as conn:
            df_sku = pd.read_sql(text(f"""
                SELECT sku,
                    SUM(CASE WHEN SUBSTRING(purchase_date,1,10)::date >= CURRENT_DATE - INTERVAL '30 days'
                        AND item_price ~ '^[0-9.]+$' THEN item_price::numeric ELSE 0 END) AS rev_30d,
                    SUM(CASE WHEN SUBSTRING(purchase_date,1,10)::date >= CURRENT_DATE - INTERVAL '60 days'
                        AND SUBSTRING(purchase_date,1,10)::date < CURRENT_DATE - INTERVAL '30 days'
                        AND item_price ~ '^[0-9.]+$' THEN item_price::numeric ELSE 0 END) AS rev_prev30d,
                    COUNT(DISTINCT CASE WHEN SUBSTRING(purchase_date,1,10)::date >= CURRENT_DATE - INTERVAL '30 days'
                        THEN amazon_order_id END) AS orders_30d
                FROM orders
                WHERE item_price ~ '^[0-9.]+$'
                GROUP BY sku HAVING SUM(CASE WHEN SUBSTRING(purchase_date,1,10)::date >= CURRENT_DATE - INTERVAL '30 days'
                    AND item_price ~ '^[0-9.]+$' THEN item_price::numeric ELSE 0 END) > 0
                ORDER BY rev_30d DESC LIMIT 10
            """), conn)
        if not df_sku.empty:
            df_sku['trend'] = df_sku.apply(
                lambda r: (r['rev_30d'] - r['rev_prev30d']) / r['rev_prev30d'] * 100
                          if r['rev_prev30d'] > 0 else 0, axis=1
            ).round(1)
            df_sku['forecast_30d'] = (df_sku['rev_30d'] * (1 + df_sku['trend']/100)).clip(lower=0)
            df_sku['trend_str'] = df_sku['trend'].apply(lambda x: f"+{x:.0f}%" if x >= 0 else f"{x:.0f}%")

            fig3 = go.Figure()
            fig3.add_trace(go.Bar(name='Факт 30д', x=df_sku['sku'], y=df_sku['rev_30d'], marker_color='#4CAF50'))
            fig3.add_trace(go.Bar(name='Прогноз 30д', x=df_sku['sku'], y=df_sku['forecast_30d'],
                                  marker_color='#5B9BD5', opacity=0.8))
            fig3.update_layout(barmode='group', height=350, xaxis_tickangle=-30,
                               margin=dict(l=0,r=0,t=10,b=60))
            st.plotly_chart(fig3, width="stretch")

            st.dataframe(
                df_sku[['sku','rev_30d','rev_prev30d','trend_str','forecast_30d','orders_30d']]
                    .rename(columns={'sku':'SKU','rev_30d':'Факт 30д','rev_prev30d':'Попередні 30д',
                                     'trend_str':'Тренд','forecast_30d':'Прогноз 30д','orders_30d':'Замовлення'})
                    .style.format({'Факт 30д':'${:,.0f}','Попередні 30д':'${:,.0f}','Прогноз 30д':'${:,.0f}'}),
                width="stretch", hide_index=True
            )
    except Exception as e:
        st.error(str(e))

    # ── AI Chat ──
    ctx_fc = f"""Forecast: {forecast_days}д прогноз=${_fmt(total_forecast)} · avg/д={_fmt(avg_forecast)}
Тренд: {trend_chg:+.1f}% · Історія {history_days}д · Gross={_fmt(total_rev)}"""
    show_ai_chat(ctx_fc, [
        "Який прогноз продажів на наступний місяць?",
        "Які SKU ростуть найшвидше?",
        "Коли очікується пік продажів?",
    ], "forecast")

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
    "📊 Brand Analytics",
    "── AI Агенти ──",
    "📦 Restock Agent",
    "📈 Прогноз (Forecast)",
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
elif report_choice == "📦 Restock Agent":            show_restock_agent(t)
elif report_choice == "📈 Прогноз (Forecast)":       show_forecast(t)
elif report_choice == "📊 Brand Analytics":          show_sqp(t)
elif report_choice == "⭐ Amazon Reviews":           show_reviews(t)
elif report_choice == "📊 ETL Status":               show_etl_status()
elif report_choice == "🕷 Scraper Reviews":          show_scraper_manager()
elif report_choice == "👑 User Management":          show_admin_panel()
elif report_choice == "ℹ️ Про додаток":              show_about()
elif report_choice == "🔌 API":                       show_api_docs()

st.sidebar.markdown("---")
st.sidebar.caption("📦 Amazon FBA BI System v5.0 🌍")
