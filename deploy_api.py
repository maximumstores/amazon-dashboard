"""
Деплой api.py на Heroku — запусти один раз
Автоматично встановлює Heroku CLI якщо потрібно
"""
import subprocess
import sys
import os
import urllib.request

HEROKU_APP = "mrequipp-api"
MAIN_APP   = "amazon-dashboard"

def run(cmd, check=True):
    print(f"→ {cmd}")
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if r.stdout: print(r.stdout.strip())
    if r.stderr and r.stderr.strip(): print(r.stderr.strip())
    if check and r.returncode != 0:
        print(f"❌ Помилка: {r.returncode}")
        sys.exit(1)
    return r

def check_heroku():
    r = subprocess.run("heroku --version", shell=True, capture_output=True)
    return r.returncode == 0

print("🚀 Деплой MR.EQUIPP API на Heroku\n" + "="*40)

# 0. Перевірка/встановлення Heroku CLI
if not check_heroku():
    print("\n0️⃣  Встановлюємо Heroku CLI...")
    installer = "heroku-x64.exe"
    url = "https://cli-assets.heroku.com/channels/stable/heroku-x64.exe"
    print(f"  Завантажуємо {url}...")
    urllib.request.urlretrieve(url, installer)
    print("  Встановлюємо...")
    subprocess.run(f"{installer} /S", shell=True)
    os.remove(installer)
    # Оновлюємо PATH
    os.environ["PATH"] += r";C:\Program Files\heroku\bin"
    if not check_heroku():
        print("  ⚠️  Перезапусти термінал після встановлення Heroku CLI і запусти знову")
        sys.exit(0)
    print("  ✅ Heroku CLI встановлено")

# 1. Логін
print("\n1️⃣  Логін в Heroku (відкриється браузер)...")
run("heroku login")

# 2. Створити app
print("\n2️⃣  Створюємо Heroku app...")
r = run(f"heroku create {HEROKU_APP}", check=False)
if "already" in (r.stdout + r.stderr).lower():
    print(f"  ℹ️  App {HEROKU_APP} вже існує")

# 3. DATABASE_URL
print("\n3️⃣  Копіюємо DATABASE_URL...")
r = run(f"heroku config:get DATABASE_URL --app {MAIN_APP}", check=False)
db_url = r.stdout.strip()
if db_url and db_url.startswith("postgres"):
    run(f'heroku config:set DATABASE_URL="{db_url}" --app {HEROKU_APP}')
    print("  ✅ DATABASE_URL встановлено")
else:
    print("  ⚠️  Введи DATABASE_URL вручну:")
    db_url = input("  DATABASE_URL: ").strip()
    if db_url:
        run(f'heroku config:set DATABASE_URL="{db_url}" --app {HEROKU_APP}')

# 4. API ключ
print("\n4️⃣  API_KEY...")
run(f'heroku config:set API_KEY="merino2024" --app {HEROKU_APP}')

# 5. Procfile
print("\n5️⃣  Procfile...")
with open("Procfile", "w") as f:
    f.write("web: uvicorn api:app --host 0.0.0.0 --port $PORT\n")
print("  ✅ Procfile створено")

# 6. requirements.txt
print("\n6️⃣  requirements.txt...")
existing = open("requirements.txt").read() if os.path.exists("requirements.txt") else ""
with open("requirements.txt", "a") as f:
    if "fastapi" not in existing: f.write("\nfastapi\n")
    if "uvicorn"  not in existing: f.write("uvicorn\n")
print("  ✅ OK")

# 7. Git + деплой
print("\n7️⃣  Деплоїмо...")
run("git add api.py Procfile requirements.txt deploy_api.py")
run('git commit -m "Add FastAPI"', check=False)
run(f"git push heroku main")

print(f"""
✅ Готово! API живий:
   https://{HEROKU_APP}.herokuapp.com/inventory?key=merino2024
   https://{HEROKU_APP}.herokuapp.com/finance?key=merino2024&days=30
   https://{HEROKU_APP}.herokuapp.com/alerts?key=merino2024
""")
