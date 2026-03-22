"""
Деплой api.py на Heroku — запусти один раз
"""
import subprocess
import sys
import os

HEROKU_APP = "mrequipp-api"
MAIN_APP   = "amazon-dashboard"  # змінити на назву основного app

def run(cmd, check=True):
    print(f"→ {cmd}")
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if r.stdout: print(r.stdout.strip())
    if r.stderr: print(r.stderr.strip())
    if check and r.returncode != 0:
        print(f"❌ Помилка: {r.returncode}")
        sys.exit(1)
    return r

print("🚀 Деплой MR.EQUIPP API на Heroku\n" + "="*40)

# 1. Створити app
print("\n1️⃣  Створюємо Heroku app...")
r = run(f"heroku create {HEROKU_APP}", check=False)
if "already taken" in (r.stderr or "") or "already exists" in (r.stderr or ""):
    print(f"  ℹ️  App {HEROKU_APP} вже існує — продовжуємо")

# 2. Взяти DATABASE_URL з основного app
print("\n2️⃣  Копіюємо DATABASE_URL...")
r = run(f"heroku config:get DATABASE_URL --app {MAIN_APP}", check=False)
db_url = r.stdout.strip()
if db_url:
    run(f'heroku config:set DATABASE_URL="{db_url}" --app {HEROKU_APP}')
    print("  ✅ DATABASE_URL встановлено")
else:
    print("  ⚠️  DATABASE_URL не знайдено — встанови вручну:")
    print(f"  heroku config:set DATABASE_URL=... --app {HEROKU_APP}")

# 3. API ключ
print("\n3️⃣  Встановлюємо API_KEY...")
run(f'heroku config:set API_KEY="merino2024" --app {HEROKU_APP}')

# 4. Створити Procfile
print("\n4️⃣  Створюємо Procfile...")
with open("Procfile", "w") as f:
    f.write("web: uvicorn api:app --host 0.0.0.0 --port $PORT\n")
print("  ✅ Procfile створено")

# 5. Додати fastapi/uvicorn в requirements.txt
print("\n5️⃣  Оновлюємо requirements.txt...")
req_path = "requirements.txt"
existing = open(req_path).read() if os.path.exists(req_path) else ""
with open(req_path, "a") as f:
    if "fastapi" not in existing: f.write("\nfastapi\n")
    if "uvicorn"  not in existing: f.write("uvicorn\n")
print("  ✅ requirements.txt оновлено")

# 6. Git commit + push
print("\n6️⃣  Деплоїмо на Heroku...")
run("git add api.py Procfile requirements.txt")
run('git commit -m "Add FastAPI"', check=False)
run(f"git push heroku main")

# 7. Перевірка
print("\n7️⃣  Перевіряємо...")
run(f"heroku open --app {HEROKU_APP}", check=False)

print(f"""
✅ Готово! API доступний:
   https://{HEROKU_APP}.herokuapp.com/
   https://{HEROKU_APP}.herokuapp.com/inventory?key=merino2024
   https://{HEROKU_APP}.herokuapp.com/?api=help&key=merino2024
""")
