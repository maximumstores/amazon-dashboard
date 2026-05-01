"""
review_requests_tab.py — ALL-IN-ONE
SP-API Review Request Sender + Streamlit UI для dashboard.py

Підключення в dashboard.py:
    from review_requests_tab import show_review_requests_tab
    show_review_requests_tab(engine)
"""

import os, time
import requests
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timezone, timedelta
from sqlalchemy import text
from dotenv import load_dotenv
load_dotenv()

# ─── Конфіг ─────────────────────────────────────────────────
SP_CLIENT_ID     = os.getenv("LWA_CLIENT_ID") or os.getenv("LWA_CLIENT_Id", "")
SP_CLIENT_SECRET = os.getenv("LWA_CLIENT_SECRET", "")
SP_REFRESH_TOKEN = os.getenv("LWA_REFRESH_TOKEN", "")
SP_API_BASE      = "https://sellingpartnerapi-na.amazon.com"
MARKETPLACE_ID   = os.getenv("MARKETPLACE_ID", "ATVPDKIKX0DER")
STORE_NAME       = "MR.EQUIPP"
MAX_SEND_DEFAULT = 200

# ─── DDL (auto-migration) ───────────────────────────────────
# Розбито на список окремих statement'ів — деякі драйвери виконують
# тільки перший SQL з multi-statement рядка.
DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS public.review_requests (
        id             SERIAL PRIMARY KEY,
        order_id       VARCHAR(50)  UNIQUE NOT NULL,
        status         VARCHAR(20)  NOT NULL DEFAULT 'sent',
        error_msg      TEXT,
        marketplace_id VARCHAR(30)  NOT NULL DEFAULT 'ATVPDKIKX0DER',
        store_name     VARCHAR(50)  NOT NULL DEFAULT 'MR.EQUIPP',
        sent_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rr_order_id ON public.review_requests (order_id)",
    "CREATE INDEX IF NOT EXISTS idx_rr_sent_at  ON public.review_requests (sent_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rr_store    ON public.review_requests (store_name, sent_at DESC)",
    """
    CREATE OR REPLACE VIEW public.v_review_requests_daily AS
    SELECT
        DATE(sent_at AT TIME ZONE 'Europe/Kiev') AS day,
        store_name,
        COUNT(*) FILTER (WHERE status = 'sent')    AS cnt_sent,
        COUNT(*) FILTER (WHERE status = 'already') AS cnt_already,
        COUNT(*) FILTER (WHERE status = 'failed')  AS cnt_failed,
        COUNT(*) FILTER (WHERE status = 'outside') AS cnt_outside,
        COUNT(*)                                    AS cnt_total
    FROM public.review_requests GROUP BY 1, 2 ORDER BY 1 DESC, 2
    """,
    """
    CREATE OR REPLACE VIEW public.v_review_requests_summary AS
    SELECT
        store_name,
        COUNT(*) FILTER (WHERE status = 'sent')    AS total_sent,
        COUNT(*) FILTER (WHERE status = 'already') AS total_already,
        COUNT(*) FILTER (WHERE status = 'failed')  AS total_failed,
        COUNT(*) FILTER (WHERE status = 'outside') AS total_outside,
        COUNT(*) FILTER (WHERE status = 'sent' AND sent_at >= NOW() - INTERVAL '24 hours') AS sent_today,
        COUNT(*) FILTER (WHERE status = 'sent' AND sent_at >= NOW() - INTERVAL '7 days')   AS sent_7d,
        MAX(sent_at) AS last_run_at
    FROM public.review_requests GROUP BY store_name
    """,
]

# ════════════════════════════════════════════════════════════
#  SENDER ЛОГІКА
# ════════════════════════════════════════════════════════════
_access_token  = None
_token_expires = 0

def _get_token() -> str:
    global _access_token, _token_expires
    if _access_token and time.time() < _token_expires - 60:
        return _access_token
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token", "refresh_token": SP_REFRESH_TOKEN,
        "client_id": SP_CLIENT_ID,     "client_secret": SP_CLIENT_SECRET,
    }, timeout=15)
    r.raise_for_status()
    d = r.json()
    _access_token  = d["access_token"]
    _token_expires = time.time() + d.get("expires_in", 3600)
    return _access_token


def _get_shipped_orders(log_fn) -> list:
    token  = _get_token()
    after  = (datetime.now(timezone.utc) - timedelta(days=33)).strftime("%Y-%m-%dT%H:%M:%SZ")
    before = (datetime.now(timezone.utc) - timedelta(days=8)).strftime("%Y-%m-%dT%H:%M:%SZ")
    orders, next_token, page = [], None, 0

    while True:
        page += 1
        params = ({"MarketplaceIds": MARKETPLACE_ID, "NextToken": next_token}
                  if next_token else
                  {"MarketplaceIds": MARKETPLACE_ID, "OrderStatuses": "Shipped",
                   "CreatedAfter": after, "CreatedBefore": before})

        resp = requests.get(f"{SP_API_BASE}/orders/v0/orders",
                            params=params, headers={"x-amz-access-token": token}, timeout=30)
        if resp.status_code == 429:
            log_fn("⏳ 429 — чекаємо 60с..."); time.sleep(60); continue
        if resp.status_code != 200:
            log_fn(f"❌ getOrders {resp.status_code}: {resp.text[:200]}"); break

        data  = resp.json().get("payload", {})
        batch = data.get("Orders", [])
        orders.extend(batch)
        log_fn(f"   Page {page}: +{len(batch)} (total: {len(orders)})")
        next_token = data.get("NextToken")
        if not next_token: break
        time.sleep(2)
    return orders


def _send_one(order_id: str) -> tuple:
    token = _get_token()
    resp  = requests.post(
        f"{SP_API_BASE}/solicitations/v1/orders/{order_id}/solicitations/productReviewAndSellerFeedback",
        params={"marketplaceIds": MARKETPLACE_ID},
        headers={"x-amz-access-token": token, "Content-Type": "application/json"},
        timeout=15,
    )
    if resp.status_code in (200, 201): return "sent", ""
    if resp.status_code == 429:        time.sleep(5); return _send_one(order_id)
    error = resp.json().get("errors", [{}])[0].get("message", resp.text[:100])
    return "failed", error


def _load_existing(engine) -> set:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT order_id FROM public.review_requests WHERE store_name = :s"),
            {"s": STORE_NAME}).fetchall()
    return {r[0] for r in rows}


def _save_batch(engine, batch: list):
    if not batch: return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO public.review_requests (order_id,status,error_msg,marketplace_id,store_name,sent_at)
            VALUES (:oid,:st,:err,:mid,:sn,NOW())
            ON CONFLICT (order_id) DO UPDATE
                SET status=EXCLUDED.status, error_msg=EXCLUDED.error_msg, sent_at=EXCLUDED.sent_at
        """), [{"oid":oid,"st":st,"err":err,"mid":MARKETPLACE_ID,"sn":STORE_NAME}
               for oid,st,err in batch])


def run_sender(engine, max_send: int, log_fn):
    try:
        log_fn("=" * 52)
        log_fn(f"REVIEW REQUEST SENDER — {STORE_NAME} | MAX={max_send}")
        log_fn("=" * 52)
        if not SP_REFRESH_TOKEN:
            log_fn("❌ LWA_REFRESH_TOKEN не задано!"); log_fn("__DONE__"); return

        existing = _load_existing(engine)
        log_fn(f"📋 В БД вже: {len(existing)}")
        log_fn("📡 Fetching shipped orders (8–33 days)...")
        orders = _get_shipped_orders(log_fn)
        log_fn(f"📋 Shipped всього: {len(orders)}")

        new_ids = [o["AmazonOrderId"] for o in orders
                   if o.get("AmazonOrderId") and o["AmazonOrderId"] not in existing]
        log_fn(f"📬 Нові: {len(new_ids)} | Надсилаємо макс: {max_send}")

        stats, pending = {"sent":0,"already":0,"outside":0,"failed":0}, []

        for i, oid in enumerate(new_ids[:max_send], 1):
            status, error = _send_one(oid)
            if status == "sent":
                stats["sent"] += 1
                pending.append((oid, "sent", None))
                if stats["sent"] <= 5 or stats["sent"] % 20 == 0:
                    log_fn(f"   ✅ [{stats['sent']}/{min(len(new_ids),max_send)}] {oid}")
            elif "already" in error.lower():
                stats["already"] += 1; pending.append((oid, "already", None))
            elif "outside" in error.lower() or "5-30" in error:
                stats["outside"] += 1; pending.append((oid, "outside", error[:200]))
            else:
                stats["failed"] += 1; pending.append((oid, "failed", error[:200]))
                log_fn(f"   ❌ {oid} | {error[:70]}")

            time.sleep(1)
            if i % 50 == 0:
                _save_batch(engine, pending); pending = []
                log_fn(f"   💾 Збережено {i} записів")

        if pending: _save_batch(engine, pending)

        log_fn("=" * 52)
        log_fn(f"✅ Sent: {stats['sent']}  ⏭️ Already: {stats['already']}  "
               f"⏰ Outside: {stats['outside']}  ❌ Failed: {stats['failed']}")
        log_fn("=" * 52)
        log_fn("__DONE__")
    except Exception as e:
        log_fn(f"❌ EXCEPTION: {e}"); log_fn("__DONE__")


# ════════════════════════════════════════════════════════════
#  STREAMLIT UI
# ════════════════════════════════════════════════════════════
def _css():
    st.markdown("""<style>
    .rr-card{background:#1e2030;border-radius:10px;padding:16px 20px;
             text-align:center;border:1px solid #2d3148;}
    .rr-val  {font-size:2rem;font-weight:700;margin:4px 0;}
    .rr-label{font-size:.78rem;color:#8b8fa8;text-transform:uppercase;letter-spacing:.06em;}
    .rr-green{color:#4ade80;}.rr-blue{color:#60a5fa;}
    .rr-red{color:#f87171;}.rr-gray{color:#94a3b8;}
    .rr-log{background:#0f1117;border:1px solid #2d3148;border-radius:8px;
            padding:14px;font-family:monospace;font-size:.78rem;color:#e2e8f0;
            max-height:320px;overflow-y:auto;white-space:pre-wrap;}
    </style>""", unsafe_allow_html=True)


def _qdf(engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    """Виконати SELECT і повернути DataFrame. Помилки показуємо явно."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            rows = result.fetchall()
            cols = list(result.keys())
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"❌ SQL error: {type(e).__name__}: {e}")
        st.code(sql, language="sql")
        return pd.DataFrame()


def show_review_requests_tab(engine):
    # Auto-migration — кожен statement окремим execute()
    for stmt in DDL_STATEMENTS:
        try:
            with engine.begin() as conn:
                conn.execute(text(stmt))
        except Exception as e:
            st.warning(f"⚠️ Migration step failed: {type(e).__name__}: {e}")
            st.code(stmt[:300], language="sql")

    _css()
    st.markdown("## ⭐ Review Request Sender")
    st.caption(f"SP-API Solicitations v1 · {STORE_NAME} · вікно 8–33 дні після Shipped")

    # ── 🔧 ДІАГНОСТИКА (тимчасова — можна прибрати після фікса) ──
    with st.expander("🔧 Діагностика підключення до БД", expanded=True):
        # 1) Який URL у engine (без пароля)
        try:
            url = engine.url
            safe_url = f"{url.drivername}://{url.username or ''}@{url.host or ''}:{url.port or ''}/{url.database or ''}"
            st.code(f"engine.url = {safe_url}", language="text")
        except Exception as e:
            st.error(f"engine.url error: {e}")

        # 2) Скільки рядків у public.review_requests через цей engine
        diag_total = _qdf(engine, "SELECT COUNT(*) AS n FROM public.review_requests")
        if not diag_total.empty:
            st.write(f"**Всього рядків в `public.review_requests` (через UI engine): `{int(diag_total.iloc[0]['n'])}`**")

        # 3) Групування по store_name
        diag_group = _qdf(engine, """
            SELECT store_name, COUNT(*) AS cnt, MAX(sent_at) AS last_sent
            FROM public.review_requests
            GROUP BY store_name
            ORDER BY cnt DESC
        """)
        if not diag_group.empty:
            st.write("**store_name у БД:**")
            st.dataframe(diag_group, hide_index=True, use_container_width=True)
        else:
            st.warning("⚠️ Таблиця `review_requests` порожня **в тій БД, до якої підключений UI**. "
                       "Перевір `DATABASE_URL` в Streamlit secrets — він може вказувати на іншу БД, "
                       "ніж .env вашого sender'а.")

        # 4) Контекст підключення
        ctx = _qdf(engine, """
            SELECT current_database() AS db,
                   current_user        AS usr,
                   current_schema()    AS schema,
                   current_schemas(true)::text AS search_path
        """)
        if not ctx.empty:
            st.write("**Контекст підключення:**")
            st.dataframe(ctx, hide_index=True, use_container_width=True)

        # 5) Усі таблиці review_requests у БД (по всіх схемах) — з лічильниками
        all_tabs = _qdf(engine, """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_name = 'review_requests'
            ORDER BY table_schema
        """)
        if not all_tabs.empty:
            st.write(f"**Таблиці `review_requests` у схемах:** "
                     f"`{', '.join(all_tabs['table_schema'].tolist())}`")
            for sch in all_tabs['table_schema'].tolist():
                cnt = _qdf(engine, f'SELECT COUNT(*) AS n FROM "{sch}".review_requests')
                if not cnt.empty:
                    st.write(f"   • `{sch}.review_requests`: **{int(cnt.iloc[0]['n'])}** рядків")

        # 6) Views у всіх схемах
        all_views = _qdf(engine, """
            SELECT table_schema, table_name FROM information_schema.views
            WHERE table_name LIKE 'v_review_requests%'
            ORDER BY table_schema, table_name
        """)
        if not all_views.empty:
            st.write("**Views (схема.назва):**")
            st.dataframe(all_views, hide_index=True, use_container_width=True)
        else:
            st.error("❌ Views `v_review_requests_*` НЕ створені.")

        # 5) Що бачить UI під своїм STORE_NAME
        ui_view = _qdf(
            engine,
            "SELECT * FROM public.v_review_requests_summary WHERE store_name = :s",
            {"s": STORE_NAME},
        )
        st.write(f"**SELECT з `v_review_requests_summary` WHERE store_name='{STORE_NAME}':**")
        if not ui_view.empty:
            st.dataframe(ui_view, hide_index=True, use_container_width=True)
        else:
            st.warning(f"⚠️ Порожньо для store_name='{STORE_NAME}'. Якщо вище у списку є інше ім'я — "
                       f"sender пише під ним. Поправ `STORE_NAME` у sender'і або в UI.")

    st.divider()

    # ── KPI ─────────────────────────────────────────────────
    summary = _qdf(
        engine,
        "SELECT * FROM public.v_review_requests_summary WHERE store_name = :s",
        {"s": STORE_NAME},
    )
    if not summary.empty:
        r      = summary.iloc[0]
        lr     = r.get("last_run_at")
        lr_str = pd.Timestamp(lr).strftime("%d.%m.%Y %H:%M") if pd.notna(lr) else "—"
        cols   = st.columns(5)
        cards  = [
            ("Всього відправлено", int(r.get("total_sent",   0)), "green"),
            ("Сьогодні",           int(r.get("sent_today",   0)), "blue"),
            ("За 7 днів",          int(r.get("sent_7d",      0)), "blue"),
            ("Already (дублі)",    int(r.get("total_already",0)), "gray"),
            ("Помилки",            int(r.get("total_failed", 0)), "red"),
        ]
        for col, (label, val, color) in zip(cols, cards):
            with col:
                st.markdown(
                    f'<div class="rr-card"><div class="rr-label">{label}</div>'
                    f'<div class="rr-val rr-{color}">{val:,}</div></div>',
                    unsafe_allow_html=True)
        st.caption(f"Останній запуск: **{lr_str}**")
    else:
        st.info("Даних ще немає — запусти sender нижче.")
        # Діагностика: подивимось, що реально лежить у БД
        diag = _qdf(engine, """
            SELECT store_name, COUNT(*) AS cnt
            FROM public.review_requests GROUP BY store_name ORDER BY cnt DESC
        """)
        if not diag.empty:
            st.caption("🔎 Діагностика: store_name у таблиці `review_requests`")
            st.dataframe(diag, hide_index=True, use_container_width=True)
            st.caption(f"UI шукає `store_name = '{STORE_NAME}'`")

    st.divider()

    # ── Графік ──────────────────────────────────────────────
    st.markdown("### 📊 Відправлено по днях")
    daily = _qdf(
        engine,
        """
        SELECT * FROM public.v_review_requests_daily
        WHERE store_name = :s ORDER BY day ASC LIMIT 60
        """,
        {"s": STORE_NAME},
    )
    if not daily.empty:
        daily["day"] = pd.to_datetime(daily["day"])
        fig = go.Figure()
        fig.add_bar(x=daily["day"], y=daily["cnt_sent"],    name="Відправлено", marker_color="#4ade80")
        fig.add_bar(x=daily["day"], y=daily["cnt_already"], name="Вже було",    marker_color="#94a3b8")
        fig.add_bar(x=daily["day"], y=daily["cnt_failed"],  name="Помилки",     marker_color="#f87171")
        fig.update_layout(
            barmode="stack", height=280,
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e2e8f0", margin=dict(l=0,r=0,t=10,b=0),
            legend=dict(orientation="h", y=-0.25),
            xaxis=dict(gridcolor="#2d3148"), yaxis=dict(gridcolor="#2d3148"),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Даних для графіка ще немає.")

    st.divider()

    # ── Таблиця ─────────────────────────────────────────────
    st.markdown("### 📋 Останні відправлення")
    cf1, cf2, _ = st.columns([1, 1, 2])
    with cf1:
        sf = st.selectbox("Статус", ["Всі","sent","already","failed","outside"], key="rr_sf")
    with cf2:
        lf = st.selectbox("Показати", [100, 300, 500, 1000], key="rr_lf")

    sql = """
        SELECT order_id, status, error_msg,
               TO_CHAR(sent_at AT TIME ZONE 'Europe/Kiev','DD.MM.YYYY HH24:MI') AS sent_at_kyiv
        FROM public.review_requests
        WHERE store_name = :s
    """
    params = {"s": STORE_NAME}
    if sf != "Всі":
        sql += " AND status = :st"
        params["st"] = sf
    sql += " ORDER BY sent_at DESC LIMIT :lim"
    params["lim"] = int(lf)

    tbl = _qdf(engine, sql, params)
    if not tbl.empty:
        icons = {"sent":"✅ sent","already":"⏭️ already","failed":"❌ failed","outside":"⏰ outside"}
        tbl["status"] = tbl["status"].map(lambda s: icons.get(s, s))
        tbl.columns   = ["Order ID","Статус","Помилка","Час (Київ)"]
        st.dataframe(tbl, use_container_width=True, height=400, hide_index=True)
        st.caption(f"Показано {len(tbl):,} записів")
    else:
        st.info("Таблиця порожня.")


