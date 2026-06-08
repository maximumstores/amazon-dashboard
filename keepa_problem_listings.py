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


# ---------- сбор данных (генераторы, чтобы стримить прогресс) ----------
def iter_seller_asins(api_key, domain=1):
    """Идёт по 9 продавцам; yield (имя_магазина, накопленный_set) после каждой страницы."""
    all_asins = set()
    for name, sid in SELLERS.items():
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
            try:
                total = int(s.get("totalStorefrontAsins"))
            except (TypeError, ValueError):
                total = None
            all_asins.update(page_asins)
            yield name, all_asins
            _sleep_for_tokens(data, 100)
            if len(page_asins) < 100 or (total and len(all_asins) >= total):
                break
            page += 1


def iter_products(api_key, asins, domain=1):
    """Грузит товары пачками по 100; yield (номер_пачки, всего_пачек, список_товаров_пачки)."""
    total_chunks = (len(asins) + 99) // 100
    for idx, i in enumerate(range(0, len(asins), 100), start=1):
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
            yield idx, total_chunks, data.get("products", [])
            _sleep_for_tokens(data, 100)
            break


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

    run = st.button("▶️ Запустить проверку", key="keepa_run", type="primary")

    # показать прошлый результат, если есть и не запускаем заново
    if not run and "keepa_last_df" in st.session_state:
        st.caption("Последний результат (нажми «Запустить проверку» для обновления):")
        _render_result(st.session_state["keepa_last_df"], domain)
        return
    if not run:
        st.info("Нажми «Запустить проверку» — результаты будут появляться по ходу.")
        return

    # ---- конвейер: собираем ASIN и сразу проверяем пачками ----
    status = st.empty()
    metrics_box = st.empty()
    table_box = st.empty()

    seen = set()        # все ASIN, что уже видели в витринах
    checked = set()     # ASIN, что уже отправляли в /product
    problems = []

    def _flush(buf):
        """Проверить пачку ASIN через Keepa и дописать проблемные в таблицу."""
        if not buf:
            return
        for _, _, prod_chunk in iter_products(api_key, buf, domain):
            for p in prod_chunk:
                r = classify(p, warn, crit)
                if r:
                    problems.append(r)
        _redraw()

    def _redraw():
        status.write(f"📥 Собрано ASIN: **{len(seen)}** · проверено: "
                     f"**{len(checked)}** · найдено проблемных: **{len(problems)}**")
        if not problems:
            return
        df = pd.DataFrame(problems).sort_values(
            ["level", "age_days"], ascending=[True, False])
        crit_n = int(df["level"].str.contains("critical").sum())
        with metrics_box.container():
            m1, m2, m3 = st.columns(3)
            m1.metric("Всего проблемных", len(df))
            m2.metric("🔴 Critical", crit_n)
            m3.metric("🟡 Warn", len(df) - crit_n)
        df["link"] = ("https://keepa.com/#!product/"
                      + str(domain) + "-" + df["asin"])
        table_box.dataframe(
            df[["level", "seller", "asin", "age_days", "reasons", "title", "link"]],
            use_container_width=True, hide_index=True,
            column_config={
                "link": st.column_config.LinkColumn("Keepa", display_text="открыть"),
                "age_days": st.column_config.NumberColumn("возраст, д"),
            },
        )

    status.write("📥 Собираю ASIN по магазинам…")
    for name, acc in iter_seller_asins(api_key, domain):
        # появились новые ASIN — копим в очередь
        new = acc - seen
        seen |= acc
        # как только непроверенных >= 100 — проверяем пачку сразу
        pending = sorted(seen - checked)
        while len(pending) >= 100:
            batch = pending[:100]
            checked.update(batch)
            _flush(batch)
            pending = sorted(seen - checked)
        status.write(f"📥 Собираю ASIN… магазин: **{name}** · собрано: "
                     f"**{len(seen)}** · проверено: **{len(checked)}** · "
                     f"проблемных: **{len(problems)}**")

    # добиваем хвост (всё, что осталось непроверенным)
    tail = sorted(seen - checked)
    if tail:
        checked.update(tail)
        _flush(tail)

    if not seen:
        st.warning("Список ASIN пуст — Keepa ничего не вернул.")
        return

    status.write(f"✅ Готово · ASIN: **{len(seen)}** · проблемных: **{len(problems)}**")
    if not problems:
        st.success("Проблемных листингов не найдено ✅")
        st.session_state.pop("keepa_last_df", None)
        return

    df = pd.DataFrame(problems).sort_values(
        ["level", "age_days"], ascending=[True, False]).reset_index(drop=True)
    st.session_state["keepa_last_df"] = df
    st.download_button("⬇️ CSV", df.to_csv(index=False).encode("utf-8"),
                       "keepa_problems.csv", "text/csv", key="keepa_csv")


def _render_result(df, domain):
    """Отрисовка сохранённого результата (метрики + фильтр + таблица + CSV)."""
    crit_n = int(df["level"].str.contains("critical").sum())
    m1, m2, m3 = st.columns(3)
    m1.metric("Всего проблемных", len(df))
    m2.metric("🔴 Critical", crit_n)
    m3.metric("🟡 Warn", len(df) - crit_n)

    flt = st.multiselect("Магазин", sorted(df["seller"].unique()),
                         key="keepa_seller_filter")
    view = df[df["seller"].isin(flt)] if flt else df

    view = view.copy()
    view["link"] = "https://keepa.com/#!product/" + str(domain) + "-" + view["asin"]
    st.dataframe(
        view[["level", "seller", "asin", "age_days", "reasons", "title", "link"]],
        use_container_width=True, hide_index=True,
        column_config={
            "link": st.column_config.LinkColumn("Keepa", display_text="открыть"),
            "age_days": st.column_config.NumberColumn("возраст, д"),
        },
    )
    st.download_button("⬇️ CSV", view.to_csv(index=False).encode("utf-8"),
                       "keepa_problems.csv", "text/csv", key="keepa_csv_saved")
