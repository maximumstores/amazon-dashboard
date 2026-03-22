"""
MR.EQUIPP BI — FastAPI
Деплой: додай до Heroku поруч з ETL скриптами
"""
import os
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text

app = FastAPI(title="MR.EQUIPP BI API", version="1.0")

DATABASE_URL = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://")
API_KEY      = os.getenv("API_KEY", "merino2024")

def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})

def auth(key: str):
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# ── Docs ──
@app.get("/")
def root():
    return {"status": "ok", "endpoints": [
        "/inventory", "/finance", "/orders",
        "/buybox", "/alerts", "/reviews"
    ], "auth": "?key=API_KEY"}

# ── Inventory ──
@app.get("/inventory")
def inventory(key: str = Query(...)):
    auth(key)
    with get_engine().connect() as conn:
        df = pd.read_sql(text("SELECT * FROM fba_inventory"), conn)
    return {"status": "ok", "count": len(df), "data": df.to_dict(orient="records")}

# ── Finance ──
@app.get("/finance")
def finance(key: str = Query(...), days: int = 30):
    auth(key)
    with get_engine().connect() as conn:
        r = pd.read_sql(text(f"""
            SELECT
                SUM(CASE WHEN event_type='Shipment' AND charge_type='Principal'
                    THEN NULLIF(amount,'')::numeric ELSE 0 END) AS gross,
                SUM(CASE WHEN event_type IN ('ShipmentFee','RefundFee')
                    THEN NULLIF(amount,'')::numeric ELSE 0 END) AS fees,
                SUM(CASE WHEN event_type='Refund' AND charge_type='Principal'
                    THEN NULLIF(amount,'')::numeric ELSE 0 END) AS refunds,
                SUM(CASE WHEN event_type='ShipmentPromo'
                    THEN NULLIF(amount,'')::numeric ELSE 0 END) AS promos,
                SUM(CASE WHEN event_type='Adjustment'
                    THEN NULLIF(amount,'')::numeric ELSE 0 END) AS adjustments,
                COUNT(*) AS transactions
            FROM finance_events
            WHERE posted_date >= CURRENT_DATE - INTERVAL '{days} days'
        """), conn).iloc[0]
    gross = float(r['gross'] or 0)
    fees  = float(r['fees']  or 0)
    refs  = float(r['refunds'] or 0)
    promos= float(r['promos'] or 0)
    adj   = float(r['adjustments'] or 0)
    net   = gross + fees + refs + promos + adj
    return {"status": "ok", "period_days": days,
            "gross": round(gross, 2), "fees": round(fees, 2),
            "refunds": round(refs, 2), "promos": round(promos, 2),
            "adjustments": round(adj, 2), "net": round(net, 2),
            "margin_pct": round(net/gross*100, 1) if gross > 0 else 0,
            "transactions": int(r['transactions'])}

# ── Orders ──
@app.get("/orders")
def orders(key: str = Query(...), days: int = 30):
    auth(key)
    with get_engine().connect() as conn:
        cols = pd.read_sql(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='orders' ORDER BY ordinal_position"
        ), conn)['column_name'].tolist()
    date_col = next((c for c in cols if c.lower() in ('purchase_date','order_date','date')), cols[0])
    with get_engine().connect() as conn:
        df = pd.read_sql(text(
            f"SELECT * FROM orders "
            f"WHERE "{date_col}" >= CURRENT_DATE - INTERVAL '{days} days' "
            f"ORDER BY "{date_col}" DESC LIMIT 1000"
        ), conn)
    return {"status": "ok", "period_days": days, "count": len(df),
            "data": df.to_dict(orient="records")}

# ── Buy Box ──
@app.get("/buybox")
def buybox(key: str = Query(...)):
    auth(key)
    with get_engine().connect() as conn:
        df = pd.read_sql(text(
            "SELECT asin, sku, is_buybox_winner, price, fulfillment, marketplace "
            "FROM pricing_buybox ORDER BY snapshot_time DESC"
        ), conn)
    df['is_buybox_winner'] = df['is_buybox_winner'].astype(str).str.lower().isin(['true','1'])
    winners = int(df['is_buybox_winner'].sum())
    return {"status": "ok", "total": len(df), "winners": winners,
            "win_rate_pct": round(winners/len(df)*100, 1) if len(df) > 0 else 0,
            "data": df.to_dict(orient="records")}

# ── Alerts ──
@app.get("/alerts")
def alerts(key: str = Query(...)):
    auth(key)
    result = []
    engine = get_engine()
    # Low Stock
    try:
        with engine.connect() as conn:
            inv = pd.read_sql(text("SELECT * FROM fba_inventory"), conn)
        for col in ['Available', 'Velocity']:
            if col in inv.columns:
                inv[col] = pd.to_numeric(inv[col].replace('', None), errors='coerce').fillna(0)
        if 'Velocity' in inv.columns:
            risk = inv[inv['Velocity'] > 0].copy()
            risk['days'] = (risk['Available'] / risk['Velocity']).round(0)
            for _, row in risk[risk['days'] < 14].iterrows():
                result.append({"type": "LOW_STOCK", "sku": row.get('SKU', ''),
                                "days_left": int(row['days']), "available": int(row['Available'])})
    except Exception as e:
        result.append({"type": "ERROR", "source": "inventory", "message": str(e)})
    # Lost BuyBox
    try:
        with engine.connect() as conn:
            bb = pd.read_sql(text(
                "SELECT asin, sku, price FROM pricing_buybox "
                "WHERE is_buybox_winner = false OR is_buybox_winner = 'False'"
            ), conn)
        for _, row in bb.iterrows():
            result.append({"type": "LOST_BUYBOX", "asin": row['asin'],
                           "sku": row.get('sku', ''), "price": float(row.get('price', 0))})
    except Exception as e:
        result.append({"type": "ERROR", "source": "buybox", "message": str(e)})
    return {"status": "ok", "alerts_count": len(result), "alerts": result}

# ── Reviews ──
@app.get("/reviews")
def reviews(key: str = Query(...), limit: int = 100, rating: int = None):
    auth(key)
    where = f"WHERE rating <= {rating}" if rating else ""
    with get_engine().connect() as conn:
        df = pd.read_sql(text(
            f"SELECT asin, domain, rating, title, review_date "
            f"FROM amazon_reviews {where} "
            f"ORDER BY review_date DESC LIMIT {limit}"
        ), conn)
    return {"status": "ok", "count": len(df), "data": df.to_dict(orient="records")}
