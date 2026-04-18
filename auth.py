"""
auth.py — спільна авторизація для merino-bi та listing-analyze.
Одна БД users, але ролі окремо для кожного застосунку:
  • role         — глобальна (fallback; 'admin' = супер-адмін)
  • bi_role      — роль у merino-bi ('admin' / 'viewer' / NULL)
  • listing_role — роль у listing-analyze ('admin' / 'viewer' / NULL)

Таблиці: users, user_permissions
"""

import os
import bcrypt
import psycopg2
import streamlit as st
from datetime import datetime
from urllib.parse import urlparse

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Всі доступні звіти merino-bi
ALL_REPORTS = [
    "🏠 Overview",
    "📈 Sales & Traffic",
    "🏦 Settlements (Payouts)",
    "💰 Inventory Value (CFO)",
    "🛒 Orders Analytics",
    "📦 Returns Analytics",
    "⭐ Amazon Reviews",
    "🐢 Inventory Health (Aging)",
    "🧠 AI Forecast",
    "📋 FBA Inventory Table",
    "🕷 Scraper Reviews",
]


# ─── DB ───────────────────────────────────────────────────────────────────────

def get_conn():
    _url = DATABASE_URL
    try:
        _url = st.secrets.get("DATABASE_URL", DATABASE_URL) or DATABASE_URL
    except Exception:
        pass
    r = urlparse(_url)
    return psycopg2.connect(
        database=r.path[1:], user=r.username, password=r.password,
        host=r.hostname, port=r.port, connect_timeout=10
    )


def ensure_tables():
    """Створює таблиці та докидає колонки per-app ролей якщо їх ще нема."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id         SERIAL PRIMARY KEY,
            email      TEXT UNIQUE NOT NULL,
            password   TEXT NOT NULL,
            name       TEXT,
            role       TEXT DEFAULT 'viewer',
            is_active  BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            last_login TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS user_permissions (
            user_id  INTEGER REFERENCES users(id) ON DELETE CASCADE,
            report   TEXT NOT NULL,
            can_view BOOLEAN DEFAULT TRUE,
            PRIMARY KEY (user_id, report)
        );

        ALTER TABLE users ADD COLUMN IF NOT EXISTS bi_role      TEXT;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS listing_role TEXT;
    """)
    conn.commit(); cur.close(); conn.close()


def create_admin_if_not_exists():
    """Створює першого адміна якщо таблиця порожня."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    count = cur.fetchone()[0]
    if count == 0:
        default_email    = os.getenv("ADMIN_EMAIL", "admin@company.com")
        default_password = os.getenv("ADMIN_PASSWORD", "admin123")
        hashed = bcrypt.hashpw(default_password.encode(), bcrypt.gensalt()).decode()
        cur.execute("""
            INSERT INTO users (email, password, name, role, bi_role, listing_role, is_active)
            VALUES (%s, %s, %s, 'admin', 'admin', 'admin', TRUE)
        """, (default_email, hashed, "Administrator"))
        conn.commit()
        print(f"✅ Створено адміна: {default_email} / {default_password}")
    cur.close(); conn.close()


# ─── AUTH FUNCTIONS ───────────────────────────────────────────────────────────

def verify_login(email: str, password: str):
    """Перевіряє email + пароль. Повертає dict юзера або None."""
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT id, email, password, name, role, is_active, bi_role, listing_role
            FROM users WHERE email = %s
        """, (email.strip().lower(),))
        row = cur.fetchone()
        cur.close(); conn.close()

        if not row:
            return None
        uid, em, pwd_hash, name, role, is_active, bi_role, listing_role = row
        if not is_active:
            return None
        if bcrypt.checkpw(password.encode(), pwd_hash.encode()):
            conn2 = get_conn(); cur2 = conn2.cursor()
            cur2.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (uid,))
            conn2.commit(); cur2.close(); conn2.close()
            return {
                "id": uid, "email": em, "name": name, "role": role,
                "bi_role": bi_role, "listing_role": listing_role,
            }
        return None
    except Exception as e:
        st.error(f"DB error: {e}")
        return None


def effective_role(app: str = "bi") -> str:
    """Ефективна роль поточного юзера для застосунку. app: 'bi' | 'listing'."""
    u = st.session_state.get("user") or {}
    return u.get(f"{app}_role") or u.get("role") or "viewer"


def get_user_permissions(user_id: int) -> set:
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT report FROM user_permissions
            WHERE user_id = %s AND can_view = TRUE
        """, (user_id,))
        rows = cur.fetchall()
        cur.close(); conn.close()
        return {r[0] for r in rows}
    except:
        return set()


def can_view(report: str) -> bool:
    user = st.session_state.get("user")
    if not user:
        return False
    if effective_role("bi") == "admin":
        return True
    perms = st.session_state.get("permissions", set())
    return report in perms


# ─── LOGIN FORM ───────────────────────────────────────────────────────────────

def _register_user(email: str, name: str, password: str) -> tuple:
    if not email or "@" not in email:
        return False, "Невірний email"
    if not name or len(name.strip()) < 2:
        return False, "Введіть ім'я (мін. 2 символи)"
    if not password or len(password) < 6:
        return False, "Пароль мінімум 6 символів"
    try:
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (email, password, name, role, is_active)
            VALUES (%s, %s, %s, 'viewer', FALSE)
        """, (email.strip().lower(), hashed, name.strip()))
        conn.commit(); cur.close(); conn.close()
        return True, "OK"
    except Exception as e:
        err = str(e)
        if "unique" in err.lower() or "duplicate" in err.lower():
            return False, "Цей email вже зареєстрований"
        return False, f"Помилка: {err}"


def show_login():
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        <div style="text-align:center;padding:32px 0 20px">
            <img src="https://merino.tech/cdn/shop/files/MT_logo_1.png?v=1685099753&width=260"
                 style="max-width:220px">
            <div style="font-size:12px;color:#aaa;margin-top:8px">Business Intelligence Hub</div>
        </div>
        """, unsafe_allow_html=True)

        tab_login, tab_reg = st.tabs(["🔐 Вхід", "📝 Реєстрація"])

        with tab_login:
            email    = st.text_input("📧 Email", placeholder="your@email.com", key="login_email")
            password = st.text_input("🔑 Пароль", type="password", key="login_password")
            if st.button("Увійти →", type="primary", width="stretch"):
                if not email or not password:
                    st.error("Введіть email і пароль")
                else:
                    user = verify_login(email, password)
                    if user:
                        st.session_state.user = user
                        st.session_state.permissions = (
                            set(ALL_REPORTS) if effective_role("bi") == "admin"
                            else get_user_permissions(user["id"])
                        )
                        st.rerun()
                    else:
                        st.error("❌ Невірний email або пароль")

        with tab_reg:
            st.caption("Після реєстрації адмін активує ваш акаунт")
            reg_name  = st.text_input("👤 Ім'я", placeholder="Ваше ім'я", key="reg_name")
            reg_email = st.text_input("📧 Email", placeholder="your@email.com", key="reg_email")
            reg_pass  = st.text_input("🔑 Пароль", type="password", key="reg_pass")
            reg_pass2 = st.text_input("🔑 Повторіть пароль", type="password", key="reg_pass2")
            if st.button("Зареєструватись", type="primary", width="stretch"):
                if reg_pass != reg_pass2:
                    st.error("Паролі не співпадають")
                else:
                    ok, msg = _register_user(reg_email, reg_name, reg_pass)
                    if ok:
                        st.success("✅ Заявку надіслано! Очікуйте активації адміністратором.")
                    else:
                        st.error(f"❌ {msg}")


def logout():
    for key in ["user", "permissions"]:
        st.session_state.pop(key, None)
    st.rerun()


# ─── ADMIN DATA ──────────────────────────────────────────────────────────────

def load_all_users():
    """Повертає рядки з per-app ролями."""
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT id, email, name, role, is_active, created_at, last_login,
                   bi_role, listing_role
            FROM users ORDER BY created_at DESC
        """)
        rows = cur.fetchall(); cur.close(); conn.close()
        return rows
    except:
        return []


def load_user_perms(user_id: int) -> set:
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT report FROM user_permissions
            WHERE user_id = %s AND can_view = TRUE
        """, (user_id,))
        rows = cur.fetchall(); cur.close(); conn.close()
        return {r[0] for r in rows}
    except:
        return set()


def save_user_perms(user_id: int, reports: list):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM user_permissions WHERE user_id = %s", (user_id,))
        for report in reports:
            cur.execute("""
                INSERT INTO user_permissions (user_id, report, can_view)
                VALUES (%s, %s, TRUE)
            """, (user_id, report))
        conn.commit(); cur.close(); conn.close()
        return True
    except Exception as e:
        st.error(f"Помилка: {e}")
        return False


def create_user(email: str, password: str, name: str, role: str,
                bi_role: str = None, listing_role: str = None) -> bool:
    try:
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (email, password, name, role, bi_role, listing_role, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, TRUE)
        """, (email.strip().lower(), hashed, name, role, bi_role, listing_role))
        conn.commit(); cur.close(); conn.close()
        return True
    except Exception as e:
        st.error(f"Помилка: {e}")
        return False


def update_user_status(user_id: int, is_active: bool):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE users SET is_active = %s WHERE id = %s", (is_active, user_id))
        conn.commit(); cur.close(); conn.close()
    except: pass


def update_user_role(user_id: int, role: str):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE users SET role = %s WHERE id = %s", (role, user_id))
        conn.commit(); cur.close(); conn.close()
    except: pass


def update_user_app_role(user_id: int, app: str, role):
    """app: 'bi' | 'listing'. role: 'admin' | 'viewer' | None."""
    col = {"bi": "bi_role", "listing": "listing_role"}.get(app)
    if not col: return
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(f"UPDATE users SET {col} = %s WHERE id = %s", (role, user_id))
        conn.commit(); cur.close(); conn.close()
    except: pass


def delete_user(user_id: int):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit(); cur.close(); conn.close()
    except: pass


def change_password(user_id: int, new_password: str) -> bool:
    try:
        hashed = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE users SET password = %s WHERE id = %s", (hashed, user_id))
        conn.commit(); cur.close(); conn.close()
        return True
    except:
        return False


# ─── ADMIN PANEL ─────────────────────────────────────────────────────────────

_ROLE_OPTS  = ["🔗 як глобальна", "👑 admin", "👤 viewer"]
_ROLE_TO_DB = {"🔗 як глобальна": None, "👑 admin": "admin", "👤 viewer": "viewer"}
_DB_TO_ROLE = {None: "🔗 як глобальна", "admin": "👑 admin", "viewer": "👤 viewer"}


def _load_cross_app_stats():
    """Підтягує активність у listing-analyze + кількість прав на звіти BI."""
    listing_by_email = {}
    perms_by_uid = {}
    try:
        conn = get_conn(); cur = conn.cursor()
        # listing_analysis існує лише якщо listing-analyze працює з тією ж БД
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_name = 'listing_analysis'
        """)
        if cur.fetchone():
            cur.execute("""
                SELECT analyzed_by,
                       COUNT(*) as total,
                       COUNT(*) FILTER (WHERE analyzed_at > NOW() - INTERVAL '7 days') as d7,
                       COUNT(*) FILTER (WHERE analyzed_at > NOW() - INTERVAL '30 days') as d30
                FROM listing_analysis
                WHERE analyzed_by IS NOT NULL AND analyzed_by != ''
                GROUP BY analyzed_by
            """)
            for r in cur.fetchall():
                listing_by_email[r[0]] = {"total": r[1], "d7": r[2], "d30": r[3]}
        cur.execute("SELECT user_id, COUNT(*) FROM user_permissions GROUP BY user_id")
        for r in cur.fetchall():
            perms_by_uid[r[0]] = r[1]
        cur.close(); conn.close()
    except Exception:
        pass
    return listing_by_email, perms_by_uid


def show_admin_panel():
    """Кабінет merino-bi: фільтр BI/всі, бейджі, per-app ролі, статистика."""
    st.markdown("## ⚙️ Кабінет — merino-bi")
    st.caption("Ролі тут впливають ТІЛЬКИ на merino-bi. У listing-analyze свій кабінет. «Як глобальна» = NULL, наслідує колонку role.")

    tab_users, tab_create = st.tabs(["👥 Користувачі", "➕ Додати"])

    # ── Список ──────────────────────────────────────────────────────────
    with tab_users:
        users = load_all_users()
        if not users:
            st.info("Юзерів немає")
            return

        listing_by_email, perms_by_uid = _load_cross_app_stats()
        current_user_id = st.session_state.user["id"]

        _bi_filter = st.radio(
            "Показати:",
            ["🟢 Тільки merino-bi", "🌐 Всіх з БД"],
            horizontal=True, key="bi_user_filter", label_visibility="collapsed",
            help="• Тільки BI = мають bi_role, права на звіти або global admin\n• Всі з БД = включно з тими хто тільки в listing-analyze"
        )
        _only_bi = _bi_filter.startswith("🟢")

        def _is_bi(u):
            uid, email, name, role, is_active, crt, ll, bi_r, lr = u
            return (bi_r is not None) or ((perms_by_uid.get(uid, 0) or 0) > 0) \
                or (role == "admin" and bi_r is None)

        def _is_la(u):
            uid, email, name, role, is_active, crt, ll, bi_r, lr = u
            return (lr is not None) or (email in listing_by_email) \
                or (role == "admin" and lr is None)

        bi_members = [u for u in users if _is_bi(u)]
        la_only    = [u for u in users if not _is_bi(u) and _is_la(u)]
        display    = bi_members if _only_bi else users

        st.caption(
            f"🟢 BI: **{len(bi_members)}** · "
            f"🔵 Тільки listing: **{len(la_only)}** · "
            f"Всього: **{len(users)}**"
        )

        if _only_bi and len(bi_members) < len(users):
            st.info(
                f"💡 {len(users) - len(bi_members)} юзер(ів) лише в listing-analyze — "
                "перемкни «🌐 Всіх з БД» і признач їм роль у merino-bi."
            )

        if not display:
            st.info("Юзерів під цей фільтр не знайдено")
            return

        for row in display:
            uid, email, name, role, is_active, created_at, last_login, bi_role, listing_role = row
            is_self = uid == current_user_id
            is_global_admin = role == "admin"

            last_str    = last_login.strftime("%d.%m.%Y %H:%M") if last_login else "ніколи"
            created_str = created_at.strftime("%d.%m.%Y") if created_at else ""

            is_bi_m = _is_bi(row)
            is_la_m = _is_la(row)

            # Бейджі
            _badges = []
            if is_global_admin:
                _badges.append('<span style="background:#7c2d12;color:#fbbf24;border-radius:4px;padding:1px 6px;font-size:0.68rem;font-weight:700">👑 GLOBAL</span>')
            if is_bi_m:
                _badges.append('<span style="background:#064e3b;color:#6ee7b7;border-radius:4px;padding:1px 6px;font-size:0.68rem;font-weight:700">🟢 BI</span>')
            if is_la_m:
                _badges.append('<span style="background:#1e3a5f;color:#93c5fd;border-radius:4px;padding:1px 6px;font-size:0.68rem;font-weight:700">🔵 LISTING</span>')
            if not _badges:
                _badges.append('<span style="background:#1e293b;color:#64748b;border-radius:4px;padding:1px 6px;font-size:0.68rem">⚪ лише в БД</span>')

            _perms_cnt = perms_by_uid.get(uid, 0)
            _la_stats  = listing_by_email.get(email, {})

            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
                with c1:
                    self_label = " *(це ви)*" if is_self else ""
                    st.markdown(f"**{name or '—'}**{self_label}")
                    st.caption(email)
                    st.markdown(" ".join(_badges), unsafe_allow_html=True)
                with c2:
                    eff_bi   = bi_role or role
                    eff_list = listing_role or role
                    st.markdown(f"**BI:** `{eff_bi.upper()}`" + (" 🔗" if bi_role is None else ""))
                    st.markdown(f"**Listing:** `{eff_list.upper()}`" + (" 🔗" if listing_role is None else ""))
                with c3:
                    st.markdown(f"**Статус:** " + ("🟢 Активний" if is_active else "🔴 Вимкнений"))
                    st.caption(f"Глобальна: `{role.upper()}`")
                with c4:
                    st.caption(f"📅 {created_str}")
                    st.caption(f"🕐 {last_str}")

                # ── Stats pills ───────────────────────────────────────
                _pills = []
                if eff_bi == "admin":
                    _pills.append(
                        '<span style="background:#0f172a;border-left:3px solid #fbbf24;border-radius:4px;padding:3px 8px;font-size:0.72rem">'
                        '📊 Звіти BI: <b style="color:#fbbf24">всі (admin)</b></span>'
                    )
                elif _perms_cnt:
                    _pills.append(
                        f'<span style="background:#0f172a;border-left:3px solid #22c55e;border-radius:4px;padding:3px 8px;font-size:0.72rem">'
                        f'📊 Звіти BI: <b style="color:#22c55e">{_perms_cnt}</b> / {len(ALL_REPORTS)}</span>'
                    )
                if _la_stats:
                    _d7 = _la_stats.get("d7", 0)
                    _c  = "#22c55e" if _d7 >= 3 else ("#f59e0b" if _d7 >= 1 else "#94a3b8")
                    _pills.append(
                        f'<span style="background:#0f172a;border-left:3px solid {_c};border-radius:4px;padding:3px 8px;font-size:0.72rem">'
                        f'🔵 Listing: <b>{_la_stats.get("total",0)}</b> аналізів '
                        f'<span style="color:#64748b">· 7д {_d7} · 30д {_la_stats.get("d30",0)}</span></span>'
                    )
                if _pills:
                    st.markdown(
                        '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px">' + ''.join(_pills) + '</div>',
                        unsafe_allow_html=True
                    )

                # ── Налаштування ────────────────────────────────────
                if not is_self:
                    with st.expander(f"⚙️ Налаштування — {name or email}"):
                        # Ролі: global + BI + Listing
                        r1, r2, r3 = st.columns(3)
                        with r1:
                            new_role = st.selectbox(
                                "Глобальна роль:", ["admin", "viewer"],
                                index=0 if role == "admin" else 1,
                                key=f"role_{uid}",
                                help="Fallback якщо per-app роль = «як глобальна»."
                            )
                        with r2:
                            new_bi = st.selectbox(
                                "🟢 Роль у merino-bi:", _ROLE_OPTS,
                                index=_ROLE_OPTS.index(_DB_TO_ROLE.get(bi_role, _ROLE_OPTS[0])),
                                key=f"bi_role_{uid}"
                            )
                        with r3:
                            new_list = st.selectbox(
                                "🔵 Роль у listing-analyze:", _ROLE_OPTS,
                                index=_ROLE_OPTS.index(_DB_TO_ROLE.get(listing_role, _ROLE_OPTS[0])),
                                key=f"list_role_{uid}"
                            )
                        if st.button("💾 Зберегти ролі", key=f"save_roles_{uid}", width="stretch", type="primary"):
                            update_user_role(uid, new_role)
                            update_user_app_role(uid, "bi", _ROLE_TO_DB[new_bi])
                            update_user_app_role(uid, "listing", _ROLE_TO_DB[new_list])
                            st.success("Ролі оновлено!"); st.rerun()

                        # Статус / видалення
                        s1, s2 = st.columns(2)
                        with s1:
                            if is_active:
                                if st.button("🚫 Деактивувати", key=f"deact_{uid}", width="stretch"):
                                    update_user_status(uid, False); st.rerun()
                            else:
                                if st.button("✅ Активувати", key=f"act_{uid}", width="stretch", type="primary"):
                                    update_user_status(uid, True); st.rerun()
                        with s2:
                            if st.button("🗑 Видалити", key=f"del_{uid}", width="stretch"):
                                delete_user(uid); st.rerun()

                        # Пароль
                        st.markdown("---")
                        pw1, pw2 = st.columns([2, 1])
                        with pw1:
                            new_pw = st.text_input("🔑 Новий пароль:", type="password", key=f"pw_{uid}")
                        with pw2:
                            st.markdown("<div style='margin-top:28px'>", unsafe_allow_html=True)
                            if st.button("💾 Змінити пароль", key=f"save_pw_{uid}", width="stretch"):
                                if new_pw and len(new_pw) >= 6:
                                    change_password(uid, new_pw)
                                    st.success("Пароль змінено!")
                                else:
                                    st.error("Мін. 6 символів")

                        # Права на звіти (якщо ефективна BI-роль ≠ admin)
                        eff_bi_new = _ROLE_TO_DB[new_bi] or new_role
                        if eff_bi_new != "admin":
                            st.markdown("---")
                            st.markdown("**📊 Доступ до звітів (merino-bi):**")
                            current_perms = load_user_perms(uid)
                            available = list(ALL_REPORTS)
                            if f"sel_{uid}" not in st.session_state:
                                st.session_state[f"sel_{uid}"] = list(current_perms & set(available))
                            ca, cb = st.columns([1, 4])
                            with ca:
                                if st.button("✅ Всі", key=f"btn_all_{uid}", width="stretch"):
                                    st.session_state[f"sel_{uid}"] = list(available)
                                if st.button("❌ Жодного", key=f"btn_none_{uid}", width="stretch"):
                                    st.session_state[f"sel_{uid}"] = []
                            with cb:
                                cols = st.columns(2)
                                selected = []
                                for i, rep in enumerate(available):
                                    checked = rep in st.session_state[f"sel_{uid}"]
                                    if cols[i % 2].checkbox(rep, value=checked, key=f"chk_{uid}_{i}"):
                                        selected.append(rep)
                            if st.button("💾 Зберегти доступи", key=f"save_perms_{uid}", type="primary", width="stretch"):
                                save_user_perms(uid, selected)
                                st.session_state[f"sel_{uid}"] = selected
                                st.success("Доступи оновлено!")

    # ── Створити юзера ──────────────────────────────────────────────────
    with tab_create:
        st.markdown("### ➕ Новий користувач")
        with st.container(border=True):
            col1, col2 = st.columns(2)
            with col1:
                new_email = st.text_input("📧 Email:", key="new_email")
                new_name  = st.text_input("👤 Ім'я:", key="new_name")
                new_pass  = st.text_input("🔑 Пароль:", type="password", key="new_pass")
            with col2:
                new_role  = st.selectbox("Глобальна роль:", ["viewer", "admin"], key="new_role")
                new_bi    = st.selectbox("🟢 Роль у merino-bi:", _ROLE_OPTS, key="new_bi_role")
                new_list  = st.selectbox("🔵 Роль у listing-analyze:", _ROLE_OPTS, key="new_list_role")

            eff_bi_new = _ROLE_TO_DB[new_bi] or new_role
            selected_reports = []
            if eff_bi_new != "admin":
                st.markdown("**📊 Доступ до звітів (merino-bi):**")
                available = list(ALL_REPORTS)
                if "new_sel" not in st.session_state:
                    st.session_state["new_sel"] = list(available)
                ca, cb = st.columns([1, 4])
                with ca:
                    if st.button("✅ Всі", key="btn_all_new", width="stretch"):
                        st.session_state["new_sel"] = list(available)
                    if st.button("❌ Жодного", key="btn_none_new", width="stretch"):
                        st.session_state["new_sel"] = []
                with cb:
                    cols = st.columns(2)
                    for i, rep in enumerate(available):
                        checked = rep in st.session_state["new_sel"]
                        if cols[i % 2].checkbox(rep, value=checked, key=f"new_chk_{i}"):
                            selected_reports.append(rep)

            if st.button("✅ Створити", type="primary", width="stretch"):
                if not new_email or not new_pass:
                    st.error("Email і пароль обов'язкові")
                elif len(new_pass) < 6:
                    st.error("Пароль мінімум 6 символів")
                else:
                    ok = create_user(
                        new_email, new_pass, new_name, new_role,
                        bi_role=_ROLE_TO_DB[new_bi],
                        listing_role=_ROLE_TO_DB[new_list],
                    )
                    if ok:
                        if eff_bi_new != "admin" and selected_reports:
                            conn = get_conn(); cur = conn.cursor()
                            cur.execute("SELECT id FROM users WHERE email = %s", (new_email.strip().lower(),))
                            r = cur.fetchone()
                            cur.close(); conn.close()
                            if r:
                                save_user_perms(r[0], selected_reports)
                        st.success(f"✅ Юзер {new_email} створений!")
                        st.rerun()


# Alias для listing-analyze (якщо імпортує під цим іменем)
show_listing_admin_panel = show_admin_panel
