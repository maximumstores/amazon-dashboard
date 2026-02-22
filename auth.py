"""
auth.py — авторизація для Amazon FBA Dashboard
Таблиці: users, user_permissions
"""

import os
import bcrypt
import psycopg2
import streamlit as st
from datetime import datetime
from urllib.parse import urlparse

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Всі доступні звіти
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
    r = urlparse(DATABASE_URL)
    return psycopg2.connect(
        database=r.path[1:], user=r.username, password=r.password,
        host=r.hostname, port=r.port, connect_timeout=10
    )


def ensure_tables():
    """Створює таблиці users і user_permissions якщо не існують."""
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
    """)
    conn.commit(); cur.close(); conn.close()


def create_admin_if_not_exists():
    """Створює першого адміна якщо таблиця порожня."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    count = cur.fetchone()[0]
    if count == 0:
        # Дефолтний адмін — змінити пароль після першого входу!
        default_email    = os.getenv("ADMIN_EMAIL", "admin@company.com")
        default_password = os.getenv("ADMIN_PASSWORD", "admin123")
        hashed = bcrypt.hashpw(default_password.encode(), bcrypt.gensalt()).decode()
        cur.execute("""
            INSERT INTO users (email, password, name, role, is_active)
            VALUES (%s, %s, %s, 'admin', TRUE)
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
            SELECT id, email, password, name, role, is_active
            FROM users WHERE email = %s
        """, (email.strip().lower(),))
        row = cur.fetchone()
        cur.close(); conn.close()

        if not row:
            return None
        uid, em, pwd_hash, name, role, is_active = row
        if not is_active:
            return None
        if bcrypt.checkpw(password.encode(), pwd_hash.encode()):
            # Оновити last_login
            conn2 = get_conn(); cur2 = conn2.cursor()
            cur2.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (uid,))
            conn2.commit(); cur2.close(); conn2.close()
            return {"id": uid, "email": em, "name": name, "role": role}
        return None
    except Exception as e:
        st.error(f"DB error: {e}")
        return None


def get_user_permissions(user_id: int) -> set:
    """Повертає set звітів до яких є доступ."""
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
    """Перевіряє чи поточний юзер може бачити звіт."""
    user = st.session_state.get("user")
    if not user:
        return False
    if user["role"] == "admin":
        return True
    perms = st.session_state.get("permissions", set())
    return report in perms


# ─── LOGIN FORM ───────────────────────────────────────────────────────────────

def show_login():
    """Відображає форму входу."""
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        <div style="text-align:center;margin-bottom:32px">
            <div style="font-size:48px">📦</div>
            <div style="font-size:24px;font-weight:800;color:#fff">Amazon FBA Dashboard</div>
            <div style="font-size:14px;color:#888;margin-top:4px">Business Intelligence Hub</div>
        </div>
        """, unsafe_allow_html=True)

        with st.container(border=True):
            st.markdown("### 🔐 Вхід")
            email    = st.text_input("📧 Email", placeholder="your@email.com", key="login_email")
            password = st.text_input("🔑 Пароль", type="password", key="login_password")

            if st.button("Увійти →", type="primary", width="stretch"):
                if not email or not password:
                    st.error("Введіть email і пароль")
                else:
                    user = verify_login(email, password)
                    if user:
                        st.session_state.user = user
                        if user["role"] != "admin":
                            st.session_state.permissions = get_user_permissions(user["id"])
                        else:
                            st.session_state.permissions = set(ALL_REPORTS)
                        st.success(f"Ласкаво просимо, {user['name'] or user['email']}!")
                        st.rerun()
                    else:
                        st.error("❌ Невірний email або пароль")


def logout():
    """Вихід з системи."""
    for key in ["user", "permissions"]:
        st.session_state.pop(key, None)
    st.rerun()


# ─── ADMIN PANEL ─────────────────────────────────────────────────────────────

def load_all_users():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT id, email, name, role, is_active, created_at, last_login
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
    """Зберігає список дозволених звітів для юзера."""
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


def create_user(email: str, password: str, name: str, role: str) -> bool:
    try:
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (email, password, name, role, is_active)
            VALUES (%s, %s, %s, %s, TRUE)
        """, (email.strip().lower(), hashed, name, role))
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


def show_admin_panel():
    """Повна адмін-панель управління юзерами."""
    st.markdown("## 👑 Адмін-панель")

    tab_users, tab_create = st.tabs(["👥 Користувачі", "➕ Додати"])

    # ── Список юзерів ──
    with tab_users:
        users = load_all_users()
        if not users:
            st.info("Юзерів немає")
            return

        current_user_id = st.session_state.user["id"]

        for row in users:
            uid, email, name, role, is_active, created_at, last_login = row
            is_self = uid == current_user_id
            is_admin = role == "admin"

            status_color = "#4CAF50" if is_active else "#555"
            status_text  = "● Активний" if is_active else "○ Вимкнений"
            role_color   = "#FFD700" if is_admin else "#5B9BD5"
            last_str     = last_login.strftime("%d.%m.%Y %H:%M") if last_login else "ніколи"
            created_str  = created_at.strftime("%d.%m.%Y") if created_at else ""

            with st.container():
                st.markdown(f"""
                <div style="background:#1e1e2e;border-left:4px solid {status_color};
                            border-radius:8px;padding:12px 16px;margin-bottom:4px">
                  <div style="display:flex;justify-content:space-between;align-items:center">
                    <div>
                      <span style="font-size:16px;font-weight:800;color:#fff">{name or email}</span>
                      <span style="color:#888;font-size:12px;margin-left:10px">{email}</span>
                      {" <span style='color:#aaa;font-size:11px'>(це ви)</span>" if is_self else ""}
                    </div>
                    <div style="display:flex;gap:16px;align-items:center">
                      <span style="color:{role_color};font-size:13px;font-weight:700">{role.upper()}</span>
                      <span style="color:{status_color};font-size:12px">{status_text}</span>
                    </div>
                  </div>
                  <div style="font-size:11px;color:#555;margin-top:6px">
                    📅 Зареєстрований: {created_str} · 🕐 Останній вхід: {last_str}
                  </div>
                </div>""", unsafe_allow_html=True)

                # Управління (не для себе)
                if not is_self:
                    with st.expander(f"⚙️ Налаштування — {name or email}"):
                        col1, col2, col3 = st.columns(3)

                        # Роль
                        with col1:
                            new_role = st.selectbox(
                                "Роль:", ["admin", "viewer"],
                                index=0 if role == "admin" else 1,
                                key=f"role_{uid}"
                            )
                            if st.button("💾 Зберегти роль", key=f"save_role_{uid}", width="stretch"):
                                update_user_role(uid, new_role)
                                st.success("Роль оновлено!"); st.rerun()

                        # Статус
                        with col2:
                            st.markdown("<div style='margin-top:4px'>", unsafe_allow_html=True)
                            if is_active:
                                if st.button("🚫 Деактивувати", key=f"deact_{uid}", width="stretch"):
                                    update_user_status(uid, False); st.rerun()
                            else:
                                if st.button("✅ Активувати", key=f"act_{uid}", width="stretch", type="primary"):
                                    update_user_status(uid, True); st.rerun()

                        # Видалити
                        with col3:
                            st.markdown("<div style='margin-top:4px'>", unsafe_allow_html=True)
                            if st.button("🗑 Видалити", key=f"del_{uid}", width="stretch"):
                                delete_user(uid); st.rerun()

                        # Пароль
                        st.markdown("---")
                        col_pw1, col_pw2 = st.columns([2, 1])
                        with col_pw1:
                            new_pw = st.text_input("🔑 Новий пароль:", type="password", key=f"pw_{uid}")
                        with col_pw2:
                            st.markdown("<div style='margin-top:28px'>", unsafe_allow_html=True)
                            if st.button("💾 Змінити пароль", key=f"save_pw_{uid}", width="stretch"):
                                if new_pw and len(new_pw) >= 6:
                                    change_password(uid, new_pw)
                                    st.success("Пароль змінено!")
                                else:
                                    st.error("Мін. 6 символів")

                        # Права доступу (тільки для не-адміна)
                        if new_role != "admin" and role != "admin":
                            st.markdown("---")
                            st.markdown("**📊 Доступ до звітів:**")
                            current_perms = load_user_perms(uid)
                            available = [r for r in ALL_REPORTS if r != "🕷 Scraper Reviews"]
                            selected = st.multiselect(
                                "Оберіть звіти:",
                                available,
                                default=list(current_perms & set(available)),
                                key=f"perms_{uid}"
                            )
                            if st.button("💾 Зберегти доступи", key=f"save_perms_{uid}", type="primary", width="stretch"):
                                save_user_perms(uid, selected)
                                st.success("Доступи оновлено!")

    # ── Створити юзера ──
    with tab_create:
        st.markdown("### ➕ Новий користувач")
        with st.container(border=True):
            col1, col2 = st.columns(2)
            with col1:
                new_email = st.text_input("📧 Email:", key="new_email")
                new_name  = st.text_input("👤 Ім'я:", key="new_name")
            with col2:
                new_pass  = st.text_input("🔑 Пароль:", type="password", key="new_pass")
                new_role  = st.selectbox("Роль:", ["viewer", "admin"], key="new_role")

            # Доступи якщо не адмін
            selected_reports = []
            if new_role == "viewer":
                st.markdown("**📊 Доступ до звітів:**")
                available = [r for r in ALL_REPORTS if r != "🕷 Scraper Reviews"]
                selected_reports = st.multiselect(
                    "Оберіть звіти:",
                    available,
                    default=["🏠 Overview", "⭐ Amazon Reviews"],
                    key="new_perms"
                )

            if st.button("✅ Створити", type="primary", width="stretch"):
                if not new_email or not new_pass:
                    st.error("Email і пароль обов'язкові")
                elif len(new_pass) < 6:
                    st.error("Пароль мінімум 6 символів")
                else:
                    ok = create_user(new_email, new_pass, new_name, new_role)
                    if ok:
                        # Зберегти права якщо viewer
                        if new_role == "viewer" and selected_reports:
                            conn = get_conn(); cur = conn.cursor()
                            cur.execute("SELECT id FROM users WHERE email = %s", (new_email.strip().lower(),))
                            row = cur.fetchone()
                            cur.close(); conn.close()
                            if row:
                                save_user_perms(row[0], selected_reports)
                        st.success(f"✅ Юзер {new_email} створений!")
                        st.rerun()
