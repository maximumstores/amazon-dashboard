# margin_tab.py — вкладка "Маржа по SKU" для dashboard.py (MERINO BI)
#
# Показывает реальную прибыль по каждому SKU:
#   выручка (Principal) − комиссии Amazon (Commission, FBA fee, промо, refunds)
#   − себестоимость (COGS, вводится вручную) = чистая прибыль
#
# COGS хранится в public.sku_cogs (заполняется тут же через редактор).
# Без COGS показывает "маржу после Amazon".
#
# Источник комиссий: public.finance_events (charge_type).
# Подключение в dashboard.py:
#   from margin_tab import show_margin_tab
#   show_margin_tab(get_engine())

import streamlit as st
import pandas as pd
from sqlalchemy import text

# charge_type, которые НЕ вычитаем (проходные налоги/обёртка — не наши расходы/доходы)
_PASSTHROUGH = ("Tax", "GiftWrap", "GiftWrapTax", "ShippingTax", "ShippingCharge")


@st.cache_data(ttl=1800)
def _load_margin(_engine, days=30):
    """Маржа после Amazon по SKU за N дней (из finance_events)."""
    sql = f"""
        SELECT sku,
               SUM(CASE WHEN charge_type='Principal' THEN amount::numeric ELSE 0 END) AS revenue,
               SUM(CASE WHEN charge_type NOT IN {_PASSTHROUGH}
                        THEN amount::numeric ELSE 0 END)                              AS net_after_amazon,
               SUM(CASE WHEN charge_type='Principal' THEN quantity::numeric ELSE 0 END) AS units
        FROM public.finance_events
        WHERE sku IS NOT NULL AND sku <> ''
          AND amount ~ '^-?[0-9.]+$'
          AND event_type IN ('Shipment','Refund','ShipmentPromo','RefundFee','ShipmentFee')
          AND posted_date >= (CURRENT_DATE - INTERVAL '{int(days)} days')::text
        GROUP BY sku
        HAVING SUM(CASE WHEN charge_type='Principal' THEN amount::numeric ELSE 0 END) > 0
    """
    return pd.read_sql(sql, _engine)


@st.cache_data(ttl=300)
def _load_cogs(_engine):
    try:
        return pd.read_sql("SELECT sku, cost FROM public.sku_cogs", _engine)
    except Exception:
        return pd.DataFrame(columns=["sku", "cost"])


def _save_cogs(engine, df):
    """Сохранить COGS в public.sku_cogs (upsert по sku)."""
    rows = [(str(r["sku"]), float(r["cost"]))
            for _, r in df.iterrows()
            if pd.notna(r.get("cost")) and str(r.get("sku", "")).strip()]
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.sku_cogs (
                sku text PRIMARY KEY, cost numeric(10,2),
                currency text DEFAULT 'USD', note text,
                updated_at timestamptz NOT NULL DEFAULT now())
        """))
        for sku, cost in rows:
            conn.execute(text("""
                INSERT INTO public.sku_cogs (sku, cost, updated_at)
                VALUES (:sku, :cost, now())
                ON CONFLICT (sku) DO UPDATE SET cost=EXCLUDED.cost, updated_at=now()
            """), {"sku": sku, "cost": cost})
    return len(rows)


def show_margin_tab(engine):
    st.header("💰 Маржа по SKU")
    st.caption(
        "Реальная прибыль на единицу: выручка минус комиссии Amazon "
        "(Commission ~15%, FBA fee, промо, возвраты) минус себестоимость (COGS). "
        "Вскрывает товары, которые много продаются, но мало (или ничего) приносят."
    )

    period = st.radio("Период", [30, 60, 90], horizontal=True,
                      format_func=lambda d: f"{d} дней", key="margin_period")

    m = _load_margin(engine, days=period)
    if m.empty:
        st.warning("Нет данных в finance_events за период.")
        return

    cogs = _load_cogs(engine)
    df = m.merge(cogs, on="sku", how="left")
    df["units"] = df["units"].fillna(0)

    # расчёты
    df["amazon_fees"] = df["revenue"] - df["net_after_amazon"]   # сколько забрал Amazon
    df["cogs_total"] = df["cost"].fillna(0) * df["units"]
    df["profit"] = df["net_after_amazon"] - df["cogs_total"]      # прибыль после COGS
    df["profit_per_unit"] = df.apply(
        lambda r: round(r["profit"] / r["units"], 2) if r["units"] > 0 else None, axis=1)
    df["margin_pct"] = df.apply(
        lambda r: round(100 * r["profit"] / r["revenue"], 1) if r["revenue"] > 0 else None, axis=1)

    has_cogs = df["cost"].notna()
    n_with_cogs = int(has_cogs.sum())

    # ---------- метрики ----------
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("SKU с продажами", len(df))
    c2.metric("С заполненным COGS", f"{n_with_cogs}/{len(df)}")
    c3.metric("Выручка", f"${df['revenue'].sum():,.0f}")
    c4.metric("Amazon забрал", f"${df['amazon_fees'].sum():,.0f}")

    if n_with_cogs > 0:
        prof = df.loc[has_cogs, "profit"].sum()
        st.metric("Чистая прибыль (где COGS заполнен)", f"${prof:,.0f}")

    # ---------- убыточные / низкомаржинальные (только где есть COGS) ----------
    if n_with_cogs > 0:
        st.subheader("🔴 Убыточные и низкомаржинальные SKU")
        st.caption("Где прибыль на единицу отрицательная или маржа < 15% — "
                   "после всех комиссий и себестоимости. Это деньги впустую.")
        bad = df[has_cogs & ((df["profit_per_unit"] < 0) | (df["margin_pct"] < 15))]
        if bad.empty:
            st.success("Убыточных SKU нет — все позиции с заполненным COGS прибыльны.")
        else:
            show = bad.sort_values("profit_per_unit")[
                ["sku", "units", "revenue", "cost", "profit_per_unit", "margin_pct"]
            ].rename(columns={
                "sku": "SKU", "units": "Продано", "revenue": "Выручка $",
                "cost": "COGS $", "profit_per_unit": "Прибыль/ед $", "margin_pct": "Маржа %"})
            st.dataframe(show, use_container_width=True, hide_index=True,
                         key="margin_bad")

    # ---------- редактор COGS ----------
    st.divider()
    st.subheader("✏️ Себестоимость (COGS) по SKU")
    st.caption(
        "Введи закупочную цену за единицу в колонке COGS $ и нажми «Сохранить». "
        "Сохраняется в базу — вводить нужно один раз. Пустой COGS = считается "
        "только «маржа после Amazon» (без себестоимости)."
    )
    with st.expander("❓ Как заполнять COGS — инструкция"):
        st.markdown(
            "**Шаги:**\n"
            "1. В колонке **COGS $** кликни на ячейку напротив SKU.\n"
            "2. Введи закупочную цену за единицу (сколько платишь поставщику "
            "за эту вещь).\n"
            "3. Заполни хотя бы топ-10–20 SKU (они вверху, дают основную выручку).\n"
            "4. Нажми **💾 Сохранить COGS**.\n"
            "5. Обнови страницу → появится блок **🔴 Убыточные** и реальная прибыль.\n\n"
            "**Пример:** SKU `62-G8A7-BDY9` — продано 193 шт, после Amazon $4242. "
            "Если вещь стоит тебе $8 у поставщика → COGS всего 193×$8 = $1544, "
            "реальная прибыль = $4242 − $1544 = **$2698**. Вот это настоящие деньги, "
            "а не «после Amazon».\n\n"
            "💡 **Совет:** COGS обычно похож внутри категории. По SKU видно тип: "
            "`SS` = футболка, `PNT` = штаны, `SOX` = носки, `LS` = лонгслив. "
            "Можно заполнить быстро по категориям — даже примерная цена покажет, "
            "кто прибыльный, а кто в минусе."
        )

    editor_df = df.sort_values("revenue", ascending=False)[
        ["sku", "units", "revenue", "net_after_amazon", "cost"]
    ].rename(columns={
        "sku": "SKU", "units": "Продано", "revenue": "Выручка $",
        "net_after_amazon": "После Amazon $", "cost": "COGS $"})
    editor_df["Продано"] = editor_df["Продано"].astype(int)
    editor_df["Выручка $"] = editor_df["Выручка $"].round(0)
    editor_df["После Amazon $"] = editor_df["После Amazon $"].round(0)

    edited = st.data_editor(
        editor_df,
        use_container_width=True, hide_index=True, height=400,
        disabled=["SKU", "Продано", "Выручка $", "После Amazon $"],
        column_config={
            "COGS $": st.column_config.NumberColumn(
                "COGS $", help="Закупочная цена за единицу", min_value=0, step=0.5,
                format="%.2f"),
        },
        key="margin_cogs_editor",
    )

    if st.button("💾 Сохранить COGS", key="margin_save_cogs"):
        to_save = edited[["SKU", "COGS $"]].rename(columns={"SKU": "sku", "COGS $": "cost"})
        n = _save_cogs(engine, to_save)
        _load_cogs.clear()   # сбросить кэш
        _load_margin.clear()
        st.success(f"Сохранено {n} значений COGS. Обнови страницу для пересчёта маржи.")

    # ---------- полная таблица маржи ----------
    st.divider()
    st.subheader("📊 Полная таблица маржи")
    full = df.sort_values("revenue", ascending=False)[
        ["sku", "units", "revenue", "amazon_fees", "cogs_total", "profit",
         "profit_per_unit", "margin_pct"]
    ].rename(columns={
        "sku": "SKU", "units": "Продано", "revenue": "Выручка $",
        "amazon_fees": "Amazon $", "cogs_total": "COGS всего $",
        "profit": "Прибыль $", "profit_per_unit": "Прибыль/ед $", "margin_pct": "Маржа %"})
    st.dataframe(full, use_container_width=True, hide_index=True, key="margin_full")
    st.caption("Если COGS не заполнен — «Прибыль» = после Amazon (себестоимость не учтена).")
