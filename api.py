"""
MR.EQUIPP BI — FastAPI
https://mrequipp-api-33065495432e.herokuapp.com
"""
import os
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text

app = FastAPI(title="MR.EQUIPP BI API", version="1.0")

DATABASE_URL = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://")
API_KEY      = os.getenv("API_KEY", "merino2024")

def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True,
                         connect_args={"connect_timeout": 10})

def auth(key: str):
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

def get_cols(table: str) -> list:
    with get_engine().connect() as conn:
        return pd.read_sql(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name=:t ORDER BY ordinal_position"
        ), conn, params={"t": table})["column_name"].tolist()

@app.get("/")
def root():
    return {"status": "ok", "endpoints": [
        "/inventory", "/finance", "/orders",
        "/buybox", "/alerts", "/reviews", "/shipments"
    ], "auth": "?key=API_KEY"}

@app.get("/inventory")
def inventory(key: str = Query(...)):
    auth(key)
    with get_engine().connect() as conn:
        df = pd.read_sql(text("SELECT * FROM fba_inventory"), conn)
    return {"status": "ok", "count": len(df), "data": df.to_dict(orient="records")}

@app.get("/finance")
def finance(key: str = Query(...), days: int = 30):
    auth(key)
    cols  = get_cols("finance_events")
    amt   = next((c for c in cols if c.lower() in ("amount", "amount_value")), None)
    etype = next((c for c in cols if c.lower() in ("event_type", "type")), None)
    charge= next((c for c in cols if c.lower() in ("charge_type", "charge")), None)
    dated = next((c for c in cols if c.lower() in ("posted_date", "date", "event_date")), None)
    if not amt or not etype:
        return {"status": "error", "message": "Columns not found: " + str(cols)}
    a = 'NULLIF("' + amt + '", \'\')::numeric'
    e = '"' + etype + '"'
    c = '"' + charge + '"' if charge else "''"
    d = '"' + dated + '"' if dated else "created_at"
    sql = (
        "SELECT "
        "SUM(CASE WHEN " + e + "='Shipment' AND " + c + "='Principal' THEN " + a + " ELSE 0 END) AS gross,"
        "SUM(CASE WHEN " + e + " IN ('ShipmentFee','RefundFee') THEN " + a + " ELSE 0 END) AS fees,"
        "SUM(CASE WHEN " + e + "='Refund' AND " + c + "='Principal' THEN " + a + " ELSE 0 END) AS refunds,"
        "SUM(CASE WHEN " + e + "='ShipmentPromo' THEN " + a + " ELSE 0 END) AS promos,"
        "SUM(CASE WHEN " + e + "='Adjustment' THEN " + a + " ELSE 0 END) AS adjustments,"
        "COUNT(*) AS transactions "
        "FROM finance_events "
        "WHERE " + d + " >= CURRENT_DATE - INTERVAL '" + str(days) + " days'"
    )
    with get_engine().connect() as conn:
        r = pd.read_sql(text(sql), conn).iloc[0]
    gross  = float(r["gross"] or 0)
    fees   = float(r["fees"]  or 0)
    refs   = float(r["refunds"] or 0)
    promos = float(r["promos"] or 0)
    adj    = float(r["adjustments"] or 0)
    net    = gross + fees + refs + promos + adj
    return {
        "status": "ok", "period_days": days,
        "gross": round(gross, 2), "fees": round(fees, 2),
        "refunds": round(refs, 2), "promos": round(promos, 2),
        "adjustments": round(adj, 2), "net": round(net, 2),
        "margin_pct": round(net / gross * 100, 1) if gross > 0 else 0,
        "transactions": int(r["transactions"])
    }

@app.get("/orders")
def orders(key: str = Query(...), days: int = 30):
    auth(key)
    cols     = get_cols("orders")
    date_col = next((c for c in cols if c.lower() in ("purchase_date", "order_date", "date")), cols[0])
    sql      = "SELECT * FROM orders WHERE \"" + date_col + "\" >= CURRENT_DATE - INTERVAL '" + str(days) + " days' ORDER BY \"" + date_col + "\" DESC LIMIT 1000"
    with get_engine().connect() as conn:
        df = pd.read_sql(text(sql), conn)
    return {"status": "ok", "period_days": days, "count": len(df),
            "data": df.to_dict(orient="records")}

@app.get("/buybox")
def buybox(key: str = Query(...)):
    auth(key)
    with get_engine().connect() as conn:
        df = pd.read_sql(text(
            "SELECT asin, sku, is_buybox_winner, price, fulfillment, marketplace "
            "FROM pricing_buybox ORDER BY snapshot_time DESC"
        ), conn)
    df["is_buybox_winner"] = df["is_buybox_winner"].astype(str).str.lower().isin(["true", "1"])
    winners = int(df["is_buybox_winner"].sum())
    return {"status": "ok", "total": len(df), "winners": winners,
            "win_rate_pct": round(winners / len(df) * 100, 1) if len(df) > 0 else 0,
            "data": df.to_dict(orient="records")}

@app.get("/alerts")
def alerts(key: str = Query(...)):
    auth(key)
    result = []
    try:
        with get_engine().connect() as conn:
            inv = pd.read_sql(text("SELECT * FROM fba_inventory"), conn)
        for col in ["Available", "Velocity"]:
            if col in inv.columns:
                inv[col] = pd.to_numeric(inv[col].replace("", None), errors="coerce").fillna(0)
        if "Velocity" in inv.columns and "Available" in inv.columns:
            risk = inv[inv["Velocity"] > 0].copy()
            risk["days"] = (risk["Available"] / risk["Velocity"]).round(0)
            for _, row in risk[risk["days"] < 14].iterrows():
                result.append({"type": "LOW_STOCK", "sku": row.get("SKU", ""),
                                "days_left": int(row["days"]), "available": int(row["Available"])})
    except Exception as e:
        result.append({"type": "ERROR", "source": "inventory", "message": str(e)})
    try:
        with get_engine().connect() as conn:
            bb = pd.read_sql(text(
                "SELECT asin, sku, price FROM pricing_buybox "
                "WHERE is_buybox_winner = false OR is_buybox_winner = 'False'"
            ), conn)
        for _, row in bb.iterrows():
            result.append({"type": "LOST_BUYBOX", "asin": row["asin"],
                           "sku": row.get("sku", ""), "price": float(row.get("price", 0))})
    except Exception as e:
        result.append({"type": "ERROR", "source": "buybox", "message": str(e)})
    return {"status": "ok", "alerts_count": len(result), "alerts": result}

@app.get("/reviews")
def reviews(key: str = Query(...), limit: int = 100, rating: int = None):
    auth(key)
    where = "WHERE rating <= " + str(rating) if rating else ""
    with get_engine().connect() as conn:
        df = pd.read_sql(text(
            "SELECT asin, domain, rating, title, review_date "
            "FROM amazon_reviews " + where +
            " ORDER BY review_date DESC LIMIT " + str(limit)
        ), conn)
    return {"status": "ok", "count": len(df), "data": df.to_dict(orient="records")}

@app.get("/shipments")
def shipments(key: str = Query(...)):
    auth(key)
    try:
        with get_engine().connect() as conn:
            df = pd.read_sql(text("SELECT * FROM fba_shipments ORDER BY created_at DESC LIMIT 500"), conn)
        return {"status": "ok", "count": len(df), "data": df.to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}
