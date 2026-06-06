# keepa_problem_listings.py
# -*- coding: utf-8 -*-
"""
Keepa Problematic Listing Monitor — страница для merino-bi (amazon-dashboard).

Подключается в dashboard.py через show_keepa_problems().
Список ASIN собирается по 9 sellerId через Keepa Seller API, кэш на сутки.

Логика проблемного листинга:
  нет картинки  ИЛИ  нет sales rank  ИЛИ  productType==3  ИЛИ  пустой title
  — но только если товар СТАРШЕ порога (по listedSince/trackingSince).
Два уровня:  warn (🟡)   critical (🔴)
"""

import os
import time
import datetime as dt

import requests
import pandas as pd
import streamlit as st

# ---------- конфиг ----------
KEEPA_EPOCH = dt.datetime(2011, 1, 1, tzinfo=dt.timezone.utc)
KEEPA_PRODUCT = "https://api.keepa.com/product"
KEEPA_SELLER = "https://api.keepa.com/seller"

WARN_AGE_DAYS = 14
CRITICAL_AGE_DAYS = 60

# 9 магазинов (sellerId). domain: 1=US.
SELLERS = {
    "MERINO-TECH":            "A1CIEK2S8OQ2KI",
    "MR EQUIPP":              "A37H2U93KUS3PG",
    "SPORTACUS":              "A1PVHTCXB8GYOZ",
    "eLeaf":                  "A2U82Y816KDFW2",
    "World Sports Fanatics":  "A22R19FL33RC6G",
    "GearGuyz":               "A2HW00NP6SRCPY",
    "eLiquidation Warehouse": "A2Y6GR7W4L633D",
    "TopSale Direct":         "ACZNV91B6U7W2",
    "SimplSolutions LLC":     "A1WHU08Q71ZS3E",
}
SELLER_BY_ID = {v: k for k, v in SELLERS.items()}


# ---------- утилиты ----------
def keepa_minutes_to_date(minutes):
    if not minutes or minutes <= 0:
        return None
    return KEEPA_EPOCH + dt.timedelta(minutes=minutes)


def age_days(product):
    raw = product.get("listedSince") or product.get("trackingSince")
    d = keepa_minutes_to_date(raw)
    return None if d is None else (dt.datetime.now(dt.timezone.utc) - d).days


def has_image(product):
    csv = product.get("imagesCSV")
    return bool(csv and csv.strip())


def has_rank(product):
    sr = product.get("salesRanks")
    if isinstance(sr, dict) and len(sr) > 0:
        return True
    cur = (product.get("stats") or {}).get("current") or []
    return len(cur) > 3 and cur[3] is not None and cur[3] > 0


def reasons(product):
    r = []
    if product.get("productType") == 3:
        r.append("недоступен (productType=3)")
    if not has_image(product):
        r.append("нет фото")
    if not has_rank(product):
        r.append("нет sales rank")
    if not (product.get("title") or "").strip():
        r.append("пустой title")
    return r


def classify(product, warn, crit):
    rs = reasons(product)
    if not rs:
        return None
    a = age_days(product)
    if a is None or a < warn:
        return None
    seller = "?"
    sids = product.get("sellerIds")
    if sids:
        seller = SELLER_BY_ID.get(sids[0], "?")
    return {
        "asin": product.get("asin"),
        "seller": seller,
        "title": (product.get("title") or "—")[:80],
        "age_days": a,
        "reasons": ", ".join(rs),
        "level": "🔴 critical" if a >= crit else "🟡 warn",
    }


def _sleep_for_tokens(data, need):
    """Если токенов мало — ждём пополнения (refillIn в мс)."""
    left = data.get("tokensLeft")
    refill = data.get("refillIn", 0)
    if left is not None and left < need and refill:
        time.sleep(min(refill / 1000 + 1, 120))


# ---------- сбор данных (кэш через st.cache_data) ----------
@st.cache_data(ttl=24 * 3600, show_spinner=False)
def get_all_asins(api_key, domain=1):
    """Тянет полный asinList всех 9 продавцов с пагинацией storefront."""
    all_asins = set()
    for sid in SELLERS.values():
        page = 0
        while True:
            params = {"key": api_key, "domain": domain, "seller": sid,
                      "storefront": 1, "page": page}
            resp = requests.get(KEEPA_SELLER, params=params, timeout=60)
            if resp.status_code == 429:
                time.sleep(60)
                continue
            resp.raise_for_status()
            data = resp.json()
            s = (data.get("sellers") or {}).get(sid, {})
            page_asins = s.get("asinList") or []
            total = s.get("totalStorefrontAsins")
            all_asins.update(page_asins)
            _sleep_for_tokens(data, 100)
            if len(page_asins) < 100 or (total and len(all_asins) >= total):
                break
            page += 1
    return sorted(all_asins)


@st.cache_data(ttl=3 * 3600, show_spinner=False)
def fetch_products(api_key, asins, domain=1):
    """Грузит товары пачками по 100 ASIN."""
    out = []
    for i in range(0, len(asins), 100):
        chunk = asins[i:i + 100]
        params = {"key": api_key, "domain": domain,
                  "asin": ",".join(chunk), "stats": 1}
        while True:
            resp = requests.get(KEEPA_PRODUCT, params=params, timeout=60)
            if resp.status_code == 429:
                time.sleep(60)
                continue
            resp.raise_for_status()
            data = resp.json()
            out.extend(data.get("products", []))
            _sleep_for_tokens(data, 100)
            break
    return out


# ---------- UI ----------
def show_keepa_problems():
    st.title("🔍 Keepa — проблемные листинги")

    api_key = os.environ.get("KEEPA_API_KEY", "")
    if not api_key:
        try:
            api_key = st.secrets["KEEPA_API_KEY"]
        except Exception:
            api_key = ""
    if not api_key:
        st.error("Нет KEEPA_API_KEY (в env или st.secrets).")
        return

    c1, c2, c3 = st.columns(3)
    domain = c1.selectbox("Маркетплейс", [1, 2, 3],
                          format_func={1: "US", 2: "UK", 3: "DE"}.get,
                          key="keepa_domain")
    warn = c2.number_input("🟡 warn, дней", 1, 365, WARN_AGE_DAYS,
                           key="keepa_warn")
    crit = c3.number_input("🔴 critical, дней", 1, 365, CRITICAL_AGE_DAYS,
                           key="keepa_crit")

    if st.button("🔄 Обновить (сброс кэша)", key="keepa_refresh"):
        get_all_asins.clear()
        fetch_products.clear()

    with st.spinner("Собираю ASIN по 9 магазинам..."):
        asins = get_all_asins(api_key, domain)
    st.caption(f"Уникальных ASIN: {len(asins)}")
    if not asins:
        st.warning("Список ASIN пуст — Keepa ничего не вернул.")
        return

    with st.spinner(f"Тяну {len(asins)} товаров из Keepa..."):
        products = fetch_products(api_key, asins, domain)

    problems = [r for p in products if (r := classify(p, warn, crit))]
    if not problems:
        st.success("Проблемных листингов не найдено ✅")
        return

    df = pd.DataFrame(problems).sort_values(
        ["level", "age_days"], ascending=[True, False])

    crit_n = int(df["level"].str.contains("critical").sum())
    warn_n = len(df) - crit_n
    m1, m2, m3 = st.columns(3)
    m1.metric("Всего проблемных", len(df))
    m2.metric("🔴 Critical", crit_n)
    m3.metric("🟡 Warn", warn_n)

    flt = st.multiselect("Магазин", sorted(df["seller"].unique()),
                         key="keepa_seller_filter")
    if flt:
        df = df[df["seller"].isin(flt)]

    df["link"] = "https://keepa.com/#!product/" + str(domain) + "-" + df["asin"]
    st.dataframe(
        df[["level", "seller", "asin", "age_days", "reasons", "title", "link"]],
        use_container_width=True, hide_index=True,
        column_config={
            "link": st.column_config.LinkColumn("Keepa", display_text="открыть"),
            "age_days": st.column_config.NumberColumn("возраст, д"),
        },
    )

    st.download_button("⬇️ CSV", df.to_csv(index=False).encode("utf-8"),
                       "keepa_problems.csv", "text/csv",
                       key="keepa_csv")
 
