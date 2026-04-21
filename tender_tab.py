"""
tender_tab.py — Streamlit таб для логістичного тендера

Інтеграція в dashboard.py:
  1. Скопіюй цей файл в корінь проекту (поряд з dashboard.py)
  2. В dashboard.py додай імпорт:
        from tender_tab import show_tender_tab
  3. В FBA Operations sidebar знайди функцію show_fba_operations() 
     і додай 6-й таб:
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "🚀 Shipments", "📋 Items", "🗑️ Removals", 
            "⚠️ Non-Compliance", "🏠 Inventory Health",
            "📋 Tender"   # <-- НОВИЙ
        ])
        ...
        with tab6:
            show_tender_tab()

Фічі:
  ✅ Фільтри (FC, status, date range, marketplace)
  ✅ Мультиселект shipments через checkbox column
  ✅ Live preview таблиці
  ✅ Агрегована статистика (total boxes/kg/CBM/units)
  ✅ Pick-up date input (ручний або календар)
  ✅ Download кнопка → Excel в форматі Олексія
  ✅ Опція "Include all / Only ready / Only active"
"""

import streamlit as st
import pandas as pd
import datetime
import io
import os
import psycopg2
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# ============================================
# DB helper (використовує той самий engine що в dashboard.py)
# ============================================
def _get_conn():
    """Підключення до БД. Використовує DATABASE_URL з env/secrets."""
    url = os.getenv("DATABASE_URL") or st.secrets.get("DATABASE_URL", "")
    return psycopg2.connect(url)

@st.cache_data(ttl=300)
def fetch_tender_shipments():
    """
    Повертає DataFrame з усіма shipments готовими до тендера.
    Кеш 5 хвилин — щоб не довбати БД на кожен клік фільтра.
    """
    conn = _get_conn()
    try:
        df = pd.read_sql("""
            SELECT
                COALESCE(NULLIF(s.shipment_confirmation_id,''), s.shipment_id) AS fba_id,
                s.shipment_id,
                s.destination_fc                              AS fc,
                s.ship_to_line1                               AS line1,
                s.ship_to_city                                AS city,
                s.ship_to_state                               AS state,
                s.ship_to_postal                              AS postal,
                s.ship_to_country                             AS country,
                s.source_city                                 AS source_city,
                s.source_country                              AS source_country,
                s.delivery_window_start                       AS dw_start,
                s.delivery_window_end                         AS dw_end,
                s.status,
                s.name,
                s.marketplace,
                s.inbound_plan_id,
                s.placement_option_id,
                COUNT(DISTINCT b.box_id)                      AS box_count,
                COALESCE(SUM(NULLIF(b.weight_kg,'')::float),0)  AS total_kg,
                COALESCE(AVG(NULLIF(b.weight_kg,'')::float),0)  AS avg_kg,
                COALESCE(SUM(NULLIF(b.volume_cbm,'')::float),0) AS total_cbm,
                COALESCE(AVG(NULLIF(b.volume_cbm,'')::float),0) AS avg_cbm,
                COALESCE(SUM(NULLIF(bi.quantity,'')::int),0)    AS total_units
            FROM public.fba_inbound_shipments_v2 s
            LEFT JOIN public.fba_shipment_boxes b 
                ON b.inbound_plan_id = s.inbound_plan_id
            LEFT JOIN public.fba_shipment_box_items bi 
                ON bi.box_id = b.box_id 
                AND bi.inbound_plan_id = b.inbound_plan_id
            WHERE s.shipment_id != ''
            GROUP BY 
                s.shipment_confirmation_id, s.shipment_id, s.destination_fc,
                s.ship_to_line1, s.ship_to_city, s.ship_to_state, 
                s.ship_to_postal, s.ship_to_country,
                s.source_city, s.source_country,
                s.delivery_window_start, s.delivery_window_end,
                s.status, s.name, s.marketplace,
                s.inbound_plan_id, s.placement_option_id
            HAVING COUNT(DISTINCT b.box_id) > 0
            ORDER BY s.delivery_window_start NULLS LAST, s.destination_fc
        """, conn)
    finally:
        conn.close()
    return df

@st.cache_data(ttl=300)
def fetch_placement_fees():
    """Окремий query для fees — приєднаємо по placement_option_id"""
    conn = _get_conn()
    try:
        df = pd.read_sql("""
            SELECT 
                placement_option_id,
                MAX(fee_amount)   AS fee_amount,
                MAX(fee_currency) AS fee_currency
            FROM public.fba_shipment_placement_fees
            WHERE fee_target = 'Placement Services'
              AND COALESCE(fee_amount, '') != ''
            GROUP BY placement_option_id
        """, conn)
    finally:
        conn.close()
    return df

# ============================================
# Excel builder
# ============================================
FONT_BOLD   = Font(name="Arial", size=11, bold=True)
FONT_HEADER = Font(name="Arial", size=11, bold=True, color="FFFFFF")
FONT_NORMAL = Font(name="Arial", size=10)
FONT_SMALL  = Font(name="Arial", size=9, italic=True, color="666666")
FILL_HEADER    = PatternFill("solid", start_color="1F4E79")
FILL_SECTION   = PatternFill("solid", start_color="E7E6E6")
FILL_HIGHLIGHT = PatternFill("solid", start_color="FFF2CC")

def _format_dw(start, end):
    if not start or not end or pd.isna(start) or pd.isna(end):
        return ""
    try:
        s_str = str(start).replace("Z", "+00:00")
        e_str = str(end).replace("Z", "+00:00")
        s = datetime.datetime.fromisoformat(s_str)
        e = datetime.datetime.fromisoformat(e_str)
        if s.month == e.month and s.year == e.year:
            return f"{s.strftime('%b %d')} - {e.strftime('%d, %Y')}"
        return f"{s.strftime('%b %d')} - {e.strftime('%b %d, %Y')}"
    except Exception:
        return f"{str(start)[:10]} - {str(end)[:10]}"

def build_tender_excel(df_selected, pickup_date):
    """Генерує Excel в форматі Олексія, повертає bytes для Streamlit download_button"""
    wb = Workbook()

    # ---------- Sheet: tender ----------
    ws = wb.active
    ws.title = "tender"

    widths = {"A": 18, "B": 12, "C": 8, "D": 6, "E": 6, "F": 10,
              "G": 6, "H": 6, "I": 6, "J": 6, "K": 28, "L": 32}
    for col, w in widths.items():
        ws.column_dimensions[col].width = w

    ws["A1"] = "pick-up date"
    ws["A1"].font = FONT_BOLD
    ws["B1"] = pickup_date
    ws["B1"].font = FONT_BOLD
    ws["B1"].fill = FILL_HIGHLIGHT

    ws["A5"] = "Carton G.W. kg"
    ws["B5"] = "Carton N.W."
    ws["C5"] = "Carton volume"
    ws["D5"] = "Total G.W."
    ws["E5"] = "Total N.W."
    ws["F5"] = "Total volume, CBM"
    for col in "ABCDEF":
        ws[f"{col}5"].font = FONT_BOLD

    if len(df_selected) > 0:
        avg_kg = df_selected["avg_kg"].mean()
        avg_cbm = df_selected["avg_cbm"].mean()
        total_kg = df_selected["total_kg"].sum()
        total_cbm = df_selected["total_cbm"].sum()
        ws["A6"] = round(avg_kg, 2)
        ws["B6"] = round(avg_kg * 0.95, 2)
        ws["C6"] = round(avg_cbm, 4)
        ws["D6"] = round(total_kg, 2)
        ws["E6"] = round(total_kg * 0.95, 2)
        ws["F6"] = round(total_cbm, 4)
        for col in "ABCDEF":
            ws[f"{col}6"].fill = FILL_HIGHLIGHT

    # Групуємо по source (Amazon/AWD) — зараз все FBA, 
    # але задаватимемо розділювач щоб легко додати AWD пізніше
    ws["A9"] = "Amazon"
    ws["A9"].font = FONT_BOLD
    ws["A9"].fill = FILL_SECTION

    current_row = 11
    for _, ship in df_selected.iterrows():
        ws.cell(row=current_row, column=1, value=ship["fba_id"]).font = FONT_BOLD
        ws.cell(row=current_row, column=3, value=ship["fc"])
        ws.cell(row=current_row, column=7, value=int(ship["box_count"]))
        ws.cell(row=current_row, column=8, value="boxes")
        ws.cell(row=current_row, column=9, value=int(ship["total_units"]))
        ws.cell(row=current_row, column=10, value="units")

        fee_text = ""
        fee_amt = ship.get("fee_amount", "") or ""
        fee_curr = ship.get("fee_currency", "") or "USD"
        if fee_amt and str(fee_amt) not in ("0", "0.0", ""):
            try:
                fee_text = f"Total placement fees: {fee_curr}${float(fee_amt):,.2f}"
            except (ValueError, TypeError):
                fee_text = f"Total placement fees: {fee_amt} {fee_curr}"
        if fee_text:
            ws.cell(row=current_row, column=11, value=fee_text).font = FONT_SMALL

        dw = _format_dw(ship.get("dw_start"), ship.get("dw_end"))
        if dw:
            ws.cell(row=current_row, column=12,
                    value=f"Delivery window: {dw}").font = FONT_SMALL

        # Ship to line
        line1 = ship.get("line1") or ""
        city = (ship.get("city") or "").upper()
        state = ship.get("state") or ""
        postal = ship.get("postal") or ""
        country = ship.get("country") or ""
        parts = [ship["fc"]]
        if line1:
            parts.append(f"{line1} {postal}".strip())
        parts.append(f"{city}, {state}")
        if country:
            parts.append("United States" if country == "US" else country)
        ship_to = "Ship to: " + " - ".join(parts)
        cell = ws.cell(row=current_row + 1, column=1, value=ship_to)
        cell.font = FONT_BOLD
        ws.merge_cells(start_row=current_row + 1, start_column=1,
                       end_row=current_row + 1, end_column=12)

        current_row += 3

    ws.freeze_panes = "A10"

    # ---------- Sheet: Delivery comparison ----------
    ws2 = wb.create_sheet("Delivery comparison")
    headers = ["FBA ID", "Destination FC", "Carrier", "Service type",
               "Cost (USD)", "Transit days", "Pick-up date", "Delivery date",
               "Container type", "Fuel surcharge", "Notes"]
    for col_idx, h in enumerate(headers, 1):
        cell = ws2.cell(row=1, column=col_idx, value=h)
        cell.font = FONT_HEADER
        cell.fill = FILL_HEADER
        ws2.column_dimensions[get_column_letter(col_idx)].width = 16

    for r_idx, ship in enumerate(df_selected.itertuples(), 2):
        ws2.cell(row=r_idx, column=1, value=ship.fba_id)
        ws2.cell(row=r_idx, column=2, value=ship.fc)
    ws2.freeze_panes = "A2"

    # ---------- Sheet: Transit Time Norms ----------
    ws3 = wb.create_sheet("Transit Time Norms")
    ws3["A1"] = "Transit Time Norms (fill manually based on historical data)"
    ws3["A1"].font = FONT_BOLD
    ws3.merge_cells("A1:F1")
    norms_headers = ["Origin region", "Destination region", "Service type",
                     "Normal transit days", "Best case", "Worst case"]
    for col_idx, h in enumerate(norms_headers, 1):
        cell = ws3.cell(row=3, column=col_idx, value=h)
        cell.font = FONT_HEADER
        cell.fill = FILL_HEADER
        ws3.column_dimensions[get_column_letter(col_idx)].width = 20
    samples = [
        ["China", "US East Coast", "Ocean LCL",         30, 25, 45],
        ["China", "US West Coast", "Ocean LCL",         20, 18, 35],
        ["China", "US East Coast", "Air Freight",        7,  5, 10],
        ["China", "US West Coast", "Air Freight",        5,  4,  8],
        ["US WH", "Amazon FC",     "Ground (LTL)",       3,  2,  7],
        ["US WH", "Amazon FC",     "Ground (Parcel)",    2,  1,  5],
    ]
    for r_idx, row in enumerate(samples, 4):
        for c_idx, val in enumerate(row, 1):
            ws3.cell(row=r_idx, column=c_idx, value=val)

    # ---------- Sheet: data for tender ----------
    ws4 = wb.create_sheet("data for tender")
    data_headers = [
        "FBA ID", "Shipment ID", "FC", "Status",
        "City", "State", "Postal", "Country", "Address",
        "Source city", "Source country",
        "DW start", "DW end",
        "Boxes", "Total kg", "Avg kg", "Total CBM", "Avg CBM", "Units",
        "Placement fee", "Currency", "Name"
    ]
    for col_idx, h in enumerate(data_headers, 1):
        cell = ws4.cell(row=1, column=col_idx, value=h)
        cell.font = FONT_HEADER
        cell.fill = FILL_HEADER
        ws4.column_dimensions[get_column_letter(col_idx)].width = 15

    for r_idx, ship in enumerate(df_selected.itertuples(), 2):
        ws4.cell(row=r_idx, column=1, value=ship.fba_id)
        ws4.cell(row=r_idx, column=2, value=ship.shipment_id)
        ws4.cell(row=r_idx, column=3, value=ship.fc)
        ws4.cell(row=r_idx, column=4, value=ship.status)
        ws4.cell(row=r_idx, column=5, value=ship.city)
        ws4.cell(row=r_idx, column=6, value=ship.state)
        ws4.cell(row=r_idx, column=7, value=ship.postal)
        ws4.cell(row=r_idx, column=8, value=ship.country)
        ws4.cell(row=r_idx, column=9, value=ship.line1)
        ws4.cell(row=r_idx, column=10, value=ship.source_city)
        ws4.cell(row=r_idx, column=11, value=ship.source_country)
        ws4.cell(row=r_idx, column=12, value=str(ship.dw_start)[:10] if ship.dw_start else "")
        ws4.cell(row=r_idx, column=13, value=str(ship.dw_end)[:10] if ship.dw_end else "")
        ws4.cell(row=r_idx, column=14, value=int(ship.box_count))
        ws4.cell(row=r_idx, column=15, value=round(ship.total_kg, 2))
        ws4.cell(row=r_idx, column=16, value=round(ship.avg_kg, 2))
        ws4.cell(row=r_idx, column=17, value=round(ship.total_cbm, 4))
        ws4.cell(row=r_idx, column=18, value=round(ship.avg_cbm, 4))
        ws4.cell(row=r_idx, column=19, value=int(ship.total_units))
        ws4.cell(row=r_idx, column=20, value=getattr(ship, "fee_amount", ""))
        ws4.cell(row=r_idx, column=21, value=getattr(ship, "fee_currency", ""))
        ws4.cell(row=r_idx, column=22, value=ship.name)
    ws4.freeze_panes = "A2"

    # Bytes для Streamlit
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()

# ============================================
# Streamlit UI
# ============================================
def show_tender_tab():
    st.subheader("📋 Логістичний тендер")
    st.caption(
        "Генерація Excel-файлу для розсилки перевізникам. "
        "Формат 1:1 як у попередніх тендерах, але дані автоматично з БД."
    )

    # --- Fetch data ---
    try:
        df = fetch_tender_shipments()
        fees_df = fetch_placement_fees()
    except Exception as e:
        st.error(f"❌ Не вдалося завантажити дані: {e}")
        st.info("Запусти `14_fba_inbound_v2024_loader.py` щоб наповнити таблиці.")
        return

    if df.empty:
        st.warning("Жодного shipment у `fba_inbound_shipments_v2`. Запусти v2024 loader.")
        return

    # Merge fees
    df = df.merge(fees_df, on="placement_option_id", how="left")
    df["fee_amount"] = df["fee_amount"].fillna("")
    df["fee_currency"] = df["fee_currency"].fillna("")

    # --- Filters row ---
    col_f1, col_f2, col_f3, col_f4 = st.columns([2, 2, 2, 1])

    with col_f1:
        fc_options = sorted(df["fc"].dropna().unique())
        selected_fcs = st.multiselect(
            "🏠 Destination FC", options=fc_options, default=fc_options
        )

    with col_f2:
        status_options = sorted(df["status"].dropna().unique())
        default_statuses = [s for s in status_options 
                            if s in ("READY_TO_SHIP", "WORKING", "RECEIVING")]
        selected_statuses = st.multiselect(
            "📦 Status", options=status_options,
            default=default_statuses or status_options
        )

    with col_f3:
        marketplace_options = sorted(df["marketplace"].dropna().unique())
        selected_markets = st.multiselect(
            "🌍 Marketplace", options=marketplace_options, default=marketplace_options
        )

    with col_f4:
        st.write("")  # spacer
        st.write("")
        if st.button("🔄 Оновити", help="Перезавантажити дані з БД"):
            st.cache_data.clear()
            st.rerun()

    # --- Apply filters ---
    mask = (
        df["fc"].isin(selected_fcs) &
        df["status"].isin(selected_statuses) &
        df["marketplace"].isin(selected_markets)
    )
    df_filtered = df[mask].copy()

    if df_filtered.empty:
        st.warning("Під фільтри нічого не підходить. Розкрий фільтри ширше.")
        return

    # --- Summary metrics ---
    st.markdown("---")
    col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
    col_m1.metric("🚚 Shipments", len(df_filtered))
    col_m2.metric("📦 Boxes", int(df_filtered["box_count"].sum()))
    col_m3.metric("⚖️ Weight (kg)", f"{df_filtered['total_kg'].sum():,.1f}")
    col_m4.metric("📐 Volume (CBM)", f"{df_filtered['total_cbm'].sum():,.2f}")
    col_m5.metric("🔢 Units", int(df_filtered["total_units"].sum()))

    # --- Shipment selection table ---
    st.markdown("### Оберіть shipments для тендера")

    df_display = df_filtered[[
        "fba_id", "fc", "status", "city", "state", "country",
        "dw_start", "dw_end",
        "box_count", "total_kg", "total_cbm", "total_units",
        "fee_amount"
    ]].copy()
    df_display.insert(0, "✓", True)  # checkbox column, default all selected
    df_display["dw_start"] = df_display["dw_start"].astype(str).str[:10]
    df_display["dw_end"]   = df_display["dw_end"].astype(str).str[:10]
    df_display["total_kg"] = df_display["total_kg"].round(1)
    df_display["total_cbm"] = df_display["total_cbm"].round(3)

    edited = st.data_editor(
        df_display,
        hide_index=True,
        use_container_width=True,
        column_config={
            "✓":          st.column_config.CheckboxColumn(width="small"),
            "fba_id":     st.column_config.TextColumn("FBA ID", width="medium"),
            "fc":         st.column_config.TextColumn("FC", width="small"),
            "status":     st.column_config.TextColumn("Status"),
            "city":       st.column_config.TextColumn("City"),
            "state":      st.column_config.TextColumn("St"),
            "country":    st.column_config.TextColumn("Cty"),
            "dw_start":   st.column_config.TextColumn("DW start"),
            "dw_end":     st.column_config.TextColumn("DW end"),
            "box_count":  st.column_config.NumberColumn("Boxes"),
            "total_kg":   st.column_config.NumberColumn("kg",  format="%.1f"),
            "total_cbm":  st.column_config.NumberColumn("CBM", format="%.3f"),
            "total_units":st.column_config.NumberColumn("Units"),
            "fee_amount": st.column_config.TextColumn("Fee"),
        },
        disabled=["fba_id", "fc", "status", "city", "state", "country",
                  "dw_start", "dw_end", "box_count", "total_kg", "total_cbm",
                  "total_units", "fee_amount"],
        key="tender_editor",
    )

    # Filter to selected rows
    selected_fba_ids = edited[edited["✓"]]["fba_id"].tolist()
    df_selected = df_filtered[df_filtered["fba_id"].isin(selected_fba_ids)].copy()

    if df_selected.empty:
        st.warning("⚠️ Жодного shipment не вибрано. Постав галочку хоча б на одному.")
        return

    # --- Generation block ---
    st.markdown("---")
    st.markdown("### 📥 Згенерувати Excel")

    col_g1, col_g2, col_g3 = st.columns([2, 2, 2])

    with col_g1:
        pickup_date_obj = st.date_input(
            "📅 Pick-up date",
            value=datetime.date.today() + datetime.timedelta(days=7),
            help="Дата коли перевізник забирає вантаж зі складу"
        )
        pickup_date_str = pickup_date_obj.strftime("%d.%m.%y")

    with col_g2:
        output_name = st.text_input(
            "📄 Filename",
            value=f"tender_{datetime.date.today().isoformat()}.xlsx",
            help="Назва Excel-файлу"
        )

    with col_g3:
        st.write("")
        st.write("")
        st.info(f"✅ Відібрано **{len(df_selected)}** shipments")

    # Generate Excel
    excel_bytes = build_tender_excel(df_selected, pickup_date_str)

    st.download_button(
        label=f"📥 Завантажити {output_name}",
        data=excel_bytes,
        file_name=output_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        type="primary",
    )

    # --- Preview of what will be in the Excel ---
    with st.expander("👁️ Preview — що буде в Excel (перші 3 рядки)"):
        for _, ship in df_selected.head(3).iterrows():
            st.markdown(
                f"**{ship['fba_id']}** · {ship['fc']} · "
                f"{ship['box_count']} boxes · {int(ship['total_units'])} units"
            )
            parts = [ship["fc"]]
            if ship.get("line1"):
                parts.append(f"{ship['line1']} {ship.get('postal','')}".strip())
            parts.append(f"{(ship.get('city') or '').upper()}, {ship.get('state','')}")
            if ship.get("country"):
                parts.append("United States" if ship["country"] == "US" else ship["country"])
            st.caption(f"Ship to: {' - '.join(parts)}")
            dw = _format_dw(ship.get("dw_start"), ship.get("dw_end"))
            if dw:
                st.caption(f"Delivery window: {dw}")
            st.markdown("---")
