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

# Всі доступні звіти merino-bi (синхронізовано з main_nav / tools_nav у quantum_backend.py.py)
ALL_REPORTS = [
    # Main nav
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
    # AI Agents
    "📦 Restock Agent",
    "📈 Прогноз (Forecast)",
    # Tools
    "📊 ETL Status",
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
    # Дефолт: якщо явних обмежень нема → viewer бачить ВСЕ.
    # Адмін у Кабінеті знімає галочки щоб обмежити доступ.
    if not perms:
        return True
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
                        if effective_role("bi") == "admin":
                            st.session_state.permissions = set(ALL_REPORTS)
                        else:
                            _uperms = get_user_permissions(user["id"])
                            # Дефолт: viewer без явних обмежень → бачить ВСЕ звіти
                            st.session_state.permissions = _uperms if _uperms else set(ALL_REPORTS)
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

# ── i18n для кабінету. Мову беремо з st.session_state["lang"] (ставиться в головному app) ──
_I18N = {
    "title":         {"UA": "⚙️ Кабінет — merino-bi",                  "EN": "⚙️ Cabinet — merino-bi",                 "RU": "⚙️ Кабинет — merino-bi"},
    "subtitle":      {"UA": "Ролі тут впливають ТІЛЬКИ на merino-bi. У listing-analyze свій кабінет. «Як глобальна» = NULL, наслідує колонку role.",
                      "EN": "Roles here affect ONLY merino-bi. listing-analyze has its own cabinet. «As global» = NULL, inherits the role column.",
                      "RU": "Роли здесь влияют ТОЛЬКО на merino-bi. У listing-analyze свой кабинет. «Как глобальная» = NULL, наследует колонку role."},
    "tab_users":     {"UA": "👥 Користувачі", "EN": "👥 Users", "RU": "👥 Пользователи"},
    "tab_add":       {"UA": "➕ Додати",       "EN": "➕ Add",   "RU": "➕ Добавить"},
    "no_users":      {"UA": "Юзерів немає",    "EN": "No users", "RU": "Пользователей нет"},
    "show":          {"UA": "Показати:",       "EN": "Show:",    "RU": "Показать:"},
    "only_bi":       {"UA": "🟢 Тільки merino-bi", "EN": "🟢 merino-bi only", "RU": "🟢 Только merino-bi"},
    "all_db":        {"UA": "🌐 Всіх з БД",         "EN": "🌐 All from DB",    "RU": "🌐 Все из БД"},
    "filter_help":   {"UA": "• Тільки BI = мають bi_role, права на звіти або global admin\n• Всі з БД = включно з тими хто тільки в listing-analyze",
                      "EN": "• Only BI = has bi_role, report perms or global admin\n• All from DB = including listing-only users",
                      "RU": "• Только BI = есть bi_role, права на отчёты или global admin\n• Все из БД = включая тех кто только в listing-analyze"},
    "counter":       {"UA": "🟢 BI: **{b}** · 🔵 Тільки listing: **{l}** · Всього: **{t}**",
                      "EN": "🟢 BI: **{b}** · 🔵 Listing only: **{l}** · Total: **{t}**",
                      "RU": "🟢 BI: **{b}** · 🔵 Только listing: **{l}** · Всего: **{t}**"},
    "hint_listing":  {"UA": "💡 {n} юзер(ів) лише в listing-analyze — перемкни «🌐 Всіх з БД» і признач їм роль у merino-bi.",
                      "EN": "💡 {n} user(s) are only in listing-analyze — switch to «🌐 All from DB» and assign them a merino-bi role.",
                      "RU": "💡 {n} пользователь(ей) только в listing-analyze — переключи «🌐 Все из БД» и назначь им роль в merino-bi."},
    "none_filtered": {"UA": "Юзерів під цей фільтр не знайдено", "EN": "No users match this filter", "RU": "Пользователей под этот фильтр не найдено"},
    "you":           {"UA": "(це ви)",  "EN": "(you)",       "RU": "(это вы)"},
    "role_bi":       {"UA": "BI:",      "EN": "BI:",         "RU": "BI:"},
    "role_listing":  {"UA": "Listing:", "EN": "Listing:",    "RU": "Listing:"},
    "status":        {"UA": "Статус:",  "EN": "Status:",     "RU": "Статус:"},
    "active":        {"UA": "🟢 Активний", "EN": "🟢 Active", "RU": "🟢 Активный"},
    "inactive":      {"UA": "🔴 Вимкнений", "EN": "🔴 Disabled", "RU": "🔴 Отключён"},
    "global":        {"UA": "Глобальна:", "EN": "Global:",    "RU": "Глобальная:"},
    "reports_all":   {"UA": "📊 Звіти BI: всі (admin)",  "EN": "📊 BI reports: all (admin)",  "RU": "📊 Отчёты BI: все (admin)"},
    "reports_cnt":   {"UA": "📊 Звіти BI: **{n}** / {total}", "EN": "📊 BI reports: **{n}** / {total}", "RU": "📊 Отчёты BI: **{n}** / {total}"},
    "listing_stat":  {"UA": "🔵 Listing: **{t}** аналізів · 7д {d7} · 30д {d30}",
                      "EN": "🔵 Listing: **{t}** analyses · 7d {d7} · 30d {d30}",
                      "RU": "🔵 Listing: **{t}** анализов · 7д {d7} · 30д {d30}"},
    "settings":      {"UA": "⚙️ Налаштування — {name}", "EN": "⚙️ Settings — {name}", "RU": "⚙️ Настройки — {name}"},
    "global_role":   {"UA": "Глобальна роль:", "EN": "Global role:", "RU": "Глобальная роль:"},
    "role_bi_full":  {"UA": "🟢 Роль у merino-bi:", "EN": "🟢 Role in merino-bi:", "RU": "🟢 Роль в merino-bi:"},
    "role_la_full":  {"UA": "🔵 Роль у listing-analyze:", "EN": "🔵 Role in listing-analyze:", "RU": "🔵 Роль в listing-analyze:"},
    "global_help":   {"UA": "Fallback якщо per-app роль = «як глобальна».",
                      "EN": "Fallback if per-app role = «as global».",
                      "RU": "Fallback если per-app роль = «как глобальная»."},
    "save_roles":    {"UA": "💾 Зберегти ролі", "EN": "💾 Save roles", "RU": "💾 Сохранить роли"},
    "roles_saved":   {"UA": "Ролі оновлено!", "EN": "Roles updated!", "RU": "Роли обновлены!"},
    "deact":         {"UA": "🚫 Деактивувати", "EN": "🚫 Deactivate", "RU": "🚫 Деактивировать"},
    "act":           {"UA": "✅ Активувати",   "EN": "✅ Activate",   "RU": "✅ Активировать"},
    "del":           {"UA": "🗑 Видалити",     "EN": "🗑 Delete",     "RU": "🗑 Удалить"},
    "new_pw":        {"UA": "🔑 Новий пароль:", "EN": "🔑 New password:", "RU": "🔑 Новый пароль:"},
    "save_pw":       {"UA": "💾 Змінити пароль", "EN": "💾 Change password", "RU": "💾 Сменить пароль"},
    "pw_saved":      {"UA": "Пароль змінено!", "EN": "Password changed!", "RU": "Пароль изменён!"},
    "pw_min":        {"UA": "Мін. 6 символів", "EN": "Min. 6 characters", "RU": "Мин. 6 символов"},
    "reports_acc":   {"UA": "**📊 Доступ до звітів (merino-bi):**", "EN": "**📊 Report access (merino-bi):**", "RU": "**📊 Доступ к отчётам (merino-bi):**"},
    "reports_hint":  {"UA": "💡 Дефолт: viewer бачить ВСЕ звіти. Зніми галочку щоб приховати конкретний.",
                      "EN": "💡 Default: viewer sees ALL reports. Uncheck to hide specific ones.",
                      "RU": "💡 По умолчанию: viewer видит ВСЕ отчёты. Сними галочку чтобы скрыть конкретный."},
    "all_btn":       {"UA": "✅ Всі", "EN": "✅ All", "RU": "✅ Все"},
    "none_btn":      {"UA": "❌ Жодного", "EN": "❌ None", "RU": "❌ Никого"},
    "save_perms":    {"UA": "💾 Зберегти доступи", "EN": "💾 Save access", "RU": "💾 Сохранить доступы"},
    "perms_saved":   {"UA": "Доступи оновлено!", "EN": "Access updated!", "RU": "Доступы обновлены!"},
    "new_user_h":    {"UA": "### ➕ Новий користувач", "EN": "### ➕ New user", "RU": "### ➕ Новый пользователь"},
    "email":         {"UA": "📧 Email:", "EN": "📧 Email:", "RU": "📧 Email:"},
    "name":          {"UA": "👤 Ім'я:",  "EN": "👤 Name:",  "RU": "👤 Имя:"},
    "password":      {"UA": "🔑 Пароль:", "EN": "🔑 Password:", "RU": "🔑 Пароль:"},
    "create":        {"UA": "✅ Створити", "EN": "✅ Create", "RU": "✅ Создать"},
    "email_req":     {"UA": "Email і пароль обов'язкові", "EN": "Email and password required", "RU": "Email и пароль обязательны"},
    "pw_min_long":   {"UA": "Пароль мінімум 6 символів", "EN": "Password min. 6 characters", "RU": "Пароль минимум 6 символов"},
    "user_created":  {"UA": "✅ Юзер {email} створений!", "EN": "✅ User {email} created!", "RU": "✅ Пользователь {email} создан!"},
    # Viewer cabinet
    "my_cabinet":    {"UA": "⚙️ Мій кабінет", "EN": "⚙️ My cabinet", "RU": "⚙️ Мой кабинет"},
    "my_subtitle":   {"UA": "Перегляд власного профілю та доступних звітів.",
                      "EN": "View your profile and available reports.",
                      "RU": "Просмотр своего профиля и доступных отчётов."},
    "my_role_bi":    {"UA": "Роль BI: `{r}`", "EN": "BI role: `{r}`", "RU": "Роль BI: `{r}`"},
    "my_all":        {"UA": "✅ Доступ до ВСІХ звітів merino-bi (немає обмежень)",
                      "EN": "✅ Access to ALL merino-bi reports (no restrictions)",
                      "RU": "✅ Доступ ко ВСЕМ отчётам merino-bi (нет ограничений)"},
    "my_available":  {"UA": "**📊 Доступних звітів: {n} / {total}**",
                      "EN": "**📊 Available reports: {n} / {total}**",
                      "RU": "**📊 Доступных отчётов: {n} / {total}**"},
    "my_contact":    {"UA": "💬 Щоб отримати доступ до додаткових звітів — звернись до адміна.",
                      "EN": "💬 To get access to more reports — contact admin.",
                      "RU": "💬 Чтобы получить доступ к дополнительным отчётам — обратись к админу."},
    # Role selector
    "role_opt_global": {"UA": "🔗 як глобальна", "EN": "🔗 as global", "RU": "🔗 как глобальная"},
}

def _t(key, **kwargs):
    lang = st.session_state.get("lang", "UA")
    val = _I18N.get(key, {}).get(lang) or _I18N.get(key, {}).get("UA", key)
    return val.format(**kwargs) if kwargs else val


def _role_opts():
    return [_t("role_opt_global"), "👑 admin", "👤 viewer"]

def _role_to_db(val):
    return {_t("role_opt_global"): None, "👑 admin": "admin", "👤 viewer": "viewer"}.get(val)

def _db_to_role(val):
    return {None: _t("role_opt_global"), "admin": "👑 admin", "viewer": "👤 viewer"}.get(val)




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


def _show_viewer_cabinet():
    """Полегшений кабінет для viewer — бачить тільки себе, без управління іншими."""
    u = st.session_state.get("user") or {}
    st.markdown(f"## {_t('my_cabinet')}")
    st.caption(_t("my_subtitle"))

    with st.container(border=True):
        c1, c2 = st.columns([2, 3])
        with c1:
            st.markdown(f"**👤 {u.get('name','—')}**")
            st.caption(u.get("email",""))
            st.markdown(_t("my_role_bi", r=effective_role('bi').upper()))
        with c2:
            my_perms = get_user_permissions(u.get("id", 0))
            if not my_perms:
                st.success(_t("my_all"))
            else:
                st.markdown(_t("my_available", n=len(my_perms), total=len(ALL_REPORTS)))
                for rep in ALL_REPORTS:
                    icon = "✅" if rep in my_perms else "🚫"
                    st.markdown(f'<span style="color:#64748b;font-size:0.85rem">{icon} {rep}</span>', unsafe_allow_html=True)

    st.info(_t("my_contact"))


def show_admin_panel():
    """Кабінет merino-bi: admin → управління юзерами; viewer → лише власний профіль."""
    # Для viewer — полегшена версія (бачить лише себе)
    if effective_role("bi") != "admin":
        _show_viewer_cabinet()
        return

    st.markdown(f"## {_t('title')}")
    st.caption(_t("subtitle"))

    tab_users, tab_create = st.tabs([_t("tab_users"), _t("tab_add")])

    # ── Список ──────────────────────────────────────────────────────────
    with tab_users:
        users = load_all_users()
        if not users:
            st.info(_t("no_users"))
            return

        listing_by_email, perms_by_uid = _load_cross_app_stats()
        current_user_id = st.session_state.user["id"]

        _bi_filter = st.radio(
            _t("show"),
            [_t("only_bi"), _t("all_db")],
            horizontal=True, key="bi_user_filter", label_visibility="collapsed",
            help=_t("filter_help")
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

        st.caption(_t("counter", b=len(bi_members), l=len(la_only), t=len(users)))

        if _only_bi and len(bi_members) < len(users):
            st.info(_t("hint_listing", n=len(users) - len(bi_members)))

        if not display:
            st.info(_t("none_filtered"))
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
                    self_label = f" *{_t('you')}*" if is_self else ""
                    st.markdown(f"**{name or '—'}**{self_label}")
                    st.caption(email)
                    st.markdown(" ".join(_badges), unsafe_allow_html=True)
                with c2:
                    eff_bi   = bi_role or role
                    eff_list = listing_role or role
                    st.markdown(f"**{_t('role_bi')}** `{eff_bi.upper()}`" + (" 🔗" if bi_role is None else ""))
                    st.markdown(f"**{_t('role_listing')}** `{eff_list.upper()}`" + (" 🔗" if listing_role is None else ""))
                with c3:
                    st.markdown(f"**{_t('status')}** " + (_t("active") if is_active else _t("inactive")))
                    st.caption(f"{_t('global')} `{role.upper()}`")
                with c4:
                    st.caption(f"📅 {created_str}")
                    st.caption(f"🕐 {last_str}")

                # ── Stats pills ───────────────────────────────────────
                _pills = []
                if eff_bi == "admin":
                    _pills.append(
                        f'<span style="background:#0f172a;color:#e2e8f0;border-left:3px solid #fbbf24;border-radius:4px;padding:3px 8px;font-size:0.72rem">'
                        f'{_t("reports_all")}</span>'.replace("**","<b>",1).replace("**","</b>",1)
                    )
                elif _perms_cnt:
                    _pills.append(
                        f'<span style="background:#0f172a;color:#e2e8f0;border-left:3px solid #22c55e;border-radius:4px;padding:3px 8px;font-size:0.72rem">'
                        f'{_t("reports_cnt", n=_perms_cnt, total=len(ALL_REPORTS))}</span>'.replace("**","<b>",1).replace("**","</b>",1)
                    )
                if _la_stats:
                    _d7 = _la_stats.get("d7", 0)
                    _c  = "#22c55e" if _d7 >= 3 else ("#f59e0b" if _d7 >= 1 else "#94a3b8")
                    _pills.append(
                        f'<span style="background:#0f172a;color:#e2e8f0;border-left:3px solid {_c};border-radius:4px;padding:3px 8px;font-size:0.72rem">'
                        f'{_t("listing_stat", t=_la_stats.get("total",0), d7=_d7, d30=_la_stats.get("d30",0))}</span>'.replace("**","<b>",1).replace("**","</b>",1)
                    )
                if _pills:
                    st.markdown(
                        '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px">' + ''.join(_pills) + '</div>',
                        unsafe_allow_html=True
                    )

                # ── Налаштування ────────────────────────────────────
                if not is_self:
                    with st.expander(_t("settings", name=name or email)):
                        _opts = _role_opts()
                        # Ролі: global + BI + Listing
                        r1, r2, r3 = st.columns(3)
                        with r1:
                            new_role = st.selectbox(
                                _t("global_role"), ["admin", "viewer"],
                                index=0 if role == "admin" else 1,
                                key=f"role_{uid}",
                                help=_t("global_help")
                            )
                        with r2:
                            new_bi = st.selectbox(
                                _t("role_bi_full"), _opts,
                                index=_opts.index(_db_to_role(bi_role)),
                                key=f"bi_role_{uid}"
                            )
                        with r3:
                            new_list = st.selectbox(
                                _t("role_la_full"), _opts,
                                index=_opts.index(_db_to_role(listing_role)),
                                key=f"list_role_{uid}"
                            )
                        if st.button(_t("save_roles"), key=f"save_roles_{uid}", width="stretch", type="primary"):
                            update_user_role(uid, new_role)
                            update_user_app_role(uid, "bi", _role_to_db(new_bi))
                            update_user_app_role(uid, "listing", _role_to_db(new_list))
                            st.success(_t("roles_saved")); st.rerun()

                        # Статус / видалення
                        s1, s2 = st.columns(2)
                        with s1:
                            if is_active:
                                if st.button(_t("deact"), key=f"deact_{uid}", width="stretch"):
                                    update_user_status(uid, False); st.rerun()
                            else:
                                if st.button(_t("act"), key=f"act_{uid}", width="stretch", type="primary"):
                                    update_user_status(uid, True); st.rerun()
                        with s2:
                            if st.button(_t("del"), key=f"del_{uid}", width="stretch"):
                                delete_user(uid); st.rerun()

                        # Пароль
                        st.markdown("---")
                        pw1, pw2 = st.columns([2, 1])
                        with pw1:
                            new_pw = st.text_input(_t("new_pw"), type="password", key=f"pw_{uid}")
                        with pw2:
                            st.markdown("<div style='margin-top:28px'>", unsafe_allow_html=True)
                            if st.button(_t("save_pw"), key=f"save_pw_{uid}", width="stretch"):
                                if new_pw and len(new_pw) >= 6:
                                    change_password(uid, new_pw)
                                    st.success(_t("pw_saved"))
                                else:
                                    st.error(_t("pw_min"))

                        # Права на звіти (якщо ефективна BI-роль ≠ admin)
                        eff_bi_new = _role_to_db(new_bi) or new_role
                        if eff_bi_new != "admin":
                            st.markdown("---")
                            st.markdown(_t("reports_acc"))
                            st.caption(_t("reports_hint"))
                            current_perms = load_user_perms(uid)
                            available = list(ALL_REPORTS)
                            if f"sel_{uid}" not in st.session_state:
                                st.session_state[f"sel_{uid}"] = (
                                    list(current_perms & set(available))
                                    if current_perms else list(available)
                                )
                            ca, cb = st.columns([1, 4])
                            with ca:
                                if st.button(_t("all_btn"), key=f"btn_all_{uid}", width="stretch"):
                                    st.session_state[f"sel_{uid}"] = list(available)
                                if st.button(_t("none_btn"), key=f"btn_none_{uid}", width="stretch"):
                                    st.session_state[f"sel_{uid}"] = []
                            with cb:
                                cols = st.columns(2)
                                selected = []
                                for i, rep in enumerate(available):
                                    checked = rep in st.session_state[f"sel_{uid}"]
                                    if cols[i % 2].checkbox(rep, value=checked, key=f"chk_{uid}_{i}"):
                                        selected.append(rep)
                            if st.button(_t("save_perms"), key=f"save_perms_{uid}", type="primary", width="stretch"):
                                save_user_perms(uid, selected)
                                st.session_state[f"sel_{uid}"] = selected
                                st.success(_t("perms_saved"))

    # ── Створити юзера ──────────────────────────────────────────────────
    with tab_create:
        st.markdown(_t("new_user_h"))
        with st.container(border=True):
            _opts_new = _role_opts()
            col1, col2 = st.columns(2)
            with col1:
                new_email = st.text_input(_t("email"), key="new_email")
                new_name  = st.text_input(_t("name"),  key="new_name")
                new_pass  = st.text_input(_t("password"), type="password", key="new_pass")
            with col2:
                new_role  = st.selectbox(_t("global_role"), ["viewer", "admin"], key="new_role")
                new_bi    = st.selectbox(_t("role_bi_full"), _opts_new, key="new_bi_role")
                new_list  = st.selectbox(_t("role_la_full"), _opts_new, key="new_list_role")

            eff_bi_new = _role_to_db(new_bi) or new_role
            selected_reports = []
            if eff_bi_new != "admin":
                st.markdown(_t("reports_acc"))
                available = list(ALL_REPORTS)
                if "new_sel" not in st.session_state:
                    st.session_state["new_sel"] = list(available)
                ca, cb = st.columns([1, 4])
                with ca:
                    if st.button(_t("all_btn"), key="btn_all_new", width="stretch"):
                        st.session_state["new_sel"] = list(available)
                    if st.button(_t("none_btn"), key="btn_none_new", width="stretch"):
                        st.session_state["new_sel"] = []
                with cb:
                    cols = st.columns(2)
                    for i, rep in enumerate(available):
                        checked = rep in st.session_state["new_sel"]
                        if cols[i % 2].checkbox(rep, value=checked, key=f"new_chk_{i}"):
                            selected_reports.append(rep)

            if st.button(_t("create"), type="primary", width="stretch"):
                if not new_email or not new_pass:
                    st.error(_t("email_req"))
                elif len(new_pass) < 6:
                    st.error(_t("pw_min_long"))
                else:
                    ok = create_user(
                        new_email, new_pass, new_name, new_role,
                        bi_role=_role_to_db(new_bi),
                        listing_role=_role_to_db(new_list),
                    )
                    if ok:
                        if eff_bi_new != "admin" and selected_reports:
                            conn = get_conn(); cur = conn.cursor()
                            cur.execute("SELECT id FROM users WHERE email = %s", (new_email.strip().lower(),))
                            r = cur.fetchone()
                            cur.close(); conn.close()
                            if r:
                                save_user_perms(r[0], selected_reports)
                        st.success(_t("user_created", email=new_email))
                        st.rerun()


# Alias для listing-analyze (якщо імпортує під цим іменем)
show_listing_admin_panel = show_admin_panel

