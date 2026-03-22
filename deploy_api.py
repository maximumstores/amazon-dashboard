"""
MR.EQUIPP — скачування через FastAPI
"""
import requests
import pandas as pd
from datetime import datetime

BASE_URL = "https://mrequipp-api-33065495432e.herokuapp.com"
API_KEY  = "merino2024"

def get(endpoint, **params):
    url = f"{BASE_URL}/{endpoint}"
    r = requests.get(url, params={"key": API_KEY, **params}, timeout=60)
    print(f"  → HTTP {r.status_code} | {len(r.text)} bytes")
    if r.status_code != 200:
        print(f"  ❌ Response: {r.text[:300]}")
        return None
    try:
        return r.json()
    except Exception as e:
        print(f"  ❌ JSON error: {e}")
        print(f"  Response: {r.text[:300]}")
        return None

def save(data, name):
    if data and isinstance(data, list):
        df = pd.DataFrame(data)
        fname = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        df.to_csv(fname, index=False)
        print(f"  💾 {fname} ({len(df)} рядків)")

# Проверка доступности
print("🔌 Перевірка API...")
try:
    r = requests.get(f"{BASE_URL}/", params={"key": API_KEY}, timeout=10)
    print(f"  HTTP {r.status_code}: {r.text[:200]}")
except Exception as e:
    print(f"  ❌ Не доступний: {e}")
    print(f"\n  ⚠️  Переконайся що api.py задеплоєний на Heroku:")
    print(f"  heroku logs --tail --app mrequipp-api")
    exit(1)

print("\n📦 Inventory...")
d = get("inventory")
if d: print(f"  ✅ {d['count']} SKU"); save(d.get('data',[]), "inventory")

print("\n💰 Finance...")
d = get("finance", days=30)
if d: print(f"  ✅ Net: ${d['net']:,.0f} | Маржа: {d['margin_pct']}%")

print("\n🛒 Orders...")
d = get("orders", days=30)
if d: print(f"  ✅ {d['count']} замовлень"); save(d.get('data',[]), "orders")

print("\n🏆 Buy Box...")
d = get("buybox")
if d: print(f"  ✅ Win Rate: {d['win_rate_pct']}%"); save(d.get('data',[]), "buybox")

print("\n🚨 Alerts...")
d = get("alerts")
if d:
    print(f"  ✅ {d['alerts_count']} алертів")
    for a in d.get('alerts', []):
        if a['type'] == 'LOW_STOCK':    print(f"     🔴 {a.get('sku','')} — {a.get('days_left',0)}д")
        elif a['type'] == 'LOST_BUYBOX': print(f"     ⚠️  {a.get('asin','')} @ ${a.get('price',0)}")

print("\n✅ Готово!")
