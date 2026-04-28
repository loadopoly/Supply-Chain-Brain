"""WIP Aging Review — programmatic recreation of Francesco "the Lion"
Bernacchia's manual Excel workflow. Toggle MAKE / BUY, pull the 5 source
reports from Oracle Fusion (or upload them), and download the full multi-tab
workbook the Lion would normally hand-build.

See: pipeline/src/wip/  +  docs/Lions Lectures/WIP Review/
"""
from __future__ import annotations

import datetime as _dt
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Ensure pipeline/src is importable when run via `streamlit run app.py`
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.wip import (  # noqa: E402
    AGING_BUCKETS,
    BACKOFFICE_TRANSFER_MODIFIERS,
    build_wip_analysis,
    compute_kpis,
    endpoints,
    export_workbook,
    load_source,
)
from src.brain.operator_shell import render_operator_sidebar_fallback  # noqa: E402

st.set_page_config(page_title="WIP Aging Review", page_icon="🏗️", layout="wide")
render_operator_sidebar_fallback()
st.title("🏗️ WIP Aging Review")
st.caption(
    "Programmatic recreation of the Lion's manual Excel workflow. "
    "Pulls the 5 Oracle Fusion BIP reports, reproduces the WIP Analysis tab "
    "(raw + Backoffice-cleaned aging buckets), and lets you download the full "
    "MAKE / BUY workbook."
)

# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Run parameters")
    side = st.radio("Side", ["MAKE", "BUY", "ALL"], index=0, horizontal=True)
    plant = st.text_input("Plant", value="Wilson Rd")
    inv_org = st.text_input("Inventory Org", value=endpoints.DEFAULT_INVENTORY_ORG)
    legal_entity = st.text_input("Legal Entity", value=endpoints.DEFAULT_LEGAL_ENTITY)
    txn_from = st.date_input(
        "Transactions since", value=_dt.date.fromisoformat(endpoints.DEFAULT_TRANSACTION_FROM)
    )
    as_of = st.date_input("As of", value=_dt.date.today())
    completed_window = st.number_input(
        "Completed-WO window (days)", min_value=7, max_value=365, value=30, step=7
    )
    write_off_threshold = st.number_input(
        "Write-off threshold (days)", min_value=180, max_value=720,
        value=360, step=30,
    )
    st.divider()
    source_mode = st.radio(
        "Data source", ["Oracle Fusion (live)", "Upload Excel files"], index=1
    )

# ---------------------------------------------------------------------------
# Endpoint catalog (always visible)
# ---------------------------------------------------------------------------
with st.expander("📡 Source reports & endpoints", expanded=False):
    st.dataframe(pd.DataFrame(endpoints.list_report_summary()), hide_index=True, width="stretch")

# ---------------------------------------------------------------------------
# Source loading
# ---------------------------------------------------------------------------
sources = None

if source_mode == "Oracle Fusion (live)":
    st.subheader("1️⃣  Pull from Oracle Fusion")
    if st.button("📥 Pull all 5 reports", type="primary"):
        try:
            from src.connections.oracle_fusion import OracleFusionSession
        except Exception as exc:
            st.error(f"Oracle Fusion connector unavailable: {exc}")
        else:
            with st.spinner("Connecting to Oracle Fusion BIP…"):
                sess = OracleFusionSession()
                try:
                    sess.connect()
                except Exception as exc:
                    st.error(f"Connect failed: {exc}")
                    sess = None
            if sess is not None:
                params_override = {
                    "inventory_org": inv_org,
                    "legal_entity": legal_entity,
                    "from_date": txn_from.isoformat(),
                }
                pulled: dict[str, pd.DataFrame] = {}
                for key, rep in endpoints.SOURCE_REPORTS.items():
                    sql = rep.sql
                    for k, v in {**rep.parameters, **params_override}.items():
                        sql = sql.replace(f":{k}", f"'{v}'")
                    with st.spinner(f"Pulling {rep.title}…"):
                        try:
                            pulled[key] = sess.execute_sql(sql, max_rows=200_000)
                            st.success(f"{rep.title}: {len(pulled[key]):,} rows")
                        except Exception as exc:
                            st.error(f"{rep.title} failed: {exc}")
                            pulled[key] = pd.DataFrame()
                if all(k in pulled for k in endpoints.SOURCE_REPORTS):
                    sources = load_source(
                        inventory_onhand=pulled["inventory_onhand"],
                        transactions=pulled["inventory_transactions"],
                        demand=pulled["demand_supply_plan"],
                        wo_materials_shortage=pulled["wo_materials_shortage"],
                        wo_history=pulled["wo_history"],
                    )
                    st.session_state["wip_sources"] = sources
else:
    st.subheader("1️⃣  Upload the 5 source workbooks")
    cols = st.columns(2)
    uploads: dict[str, object] = {}
    bindings = [
        ("inventory_onhand", "On-Hand with Cost (by Locator)"),
        ("inventory_transactions", "Inventory Transaction Report"),
        ("demand_supply_plan", "Demand from Supply Plan"),
        ("wo_materials_shortage", "Materials Shortage (MOWR)"),
        ("wo_history", "Work Orders (WO History)"),
    ]
    for i, (key, label) in enumerate(bindings):
        with cols[i % 2]:
            uploads[key] = st.file_uploader(label, type=["xlsx", "xls"], key=f"up_{key}")
    if st.button("📦 Load uploads", type="primary"):
        missing = [label for k, label in bindings if uploads[k] is None]
        if missing:
            st.warning("Missing: " + ", ".join(missing))
        else:
            with st.spinner("Parsing workbooks…"):
                sources = load_source(
                    inventory_onhand=uploads["inventory_onhand"],
                    transactions=uploads["inventory_transactions"],
                    demand=uploads["demand_supply_plan"],
                    wo_materials_shortage=uploads["wo_materials_shortage"],
                    wo_history=uploads["wo_history"],
                )
                st.session_state["wip_sources"] = sources
                st.success("Loaded.")

if sources is None:
    sources = st.session_state.get("wip_sources")

# ---------------------------------------------------------------------------
# Build WIP Analysis
# ---------------------------------------------------------------------------
if sources is None:
    st.info("Pull or upload the 5 reports above to begin.")
    st.stop()

st.subheader("2️⃣  Build WIP Analysis")
with st.spinner("Computing aging buckets…"):
    wip = build_wip_analysis(
        sources, side=side, as_of=as_of,
        write_off_threshold_days=int(write_off_threshold),
    )
    kpis = compute_kpis(
        wip, completed_window_days=int(completed_window),
        sources=sources, as_of=as_of,
    )

# KPI strip
kcols = st.columns(len(kpis))
for col, (label, val) in zip(kcols, kpis.items()):
    if isinstance(val, float):
        col.metric(label, f"${val:,.0f}" if "$" in label or "TOTAL" in label or "WRITE" in label else f"{val:,.1f}")
    else:
        col.metric(label, f"{val:,}")

# Filters
st.subheader("3️⃣  WIP Analysis table")
fcol1, fcol2, fcol3 = st.columns([2, 2, 1])
with fcol1:
    item_filter = st.text_input("Item Name contains", "")
with fcol2:
    supply_options = sorted(s for s in wip["Supply type"].dropna().unique().tolist() if s)
    supply_sel = st.multiselect("Supply type", supply_options, default=supply_options)
with fcol3:
    write_off_only = st.checkbox("Write-off candidates only", value=False)

view = wip.copy()
if item_filter:
    view = view[view["Item Name"].astype(str).str.contains(item_filter, case=False, na=False)]
if supply_sel:
    view = view[view["Supply type"].isin(supply_sel)]
if write_off_only:
    view = view[view["Write-Off Flag"] == 1]

st.dataframe(view, hide_index=True, width="stretch", height=480)
st.caption(
    f"{len(view):,} of {len(wip):,} items shown · "
    f"Raw_xx = absolute Quantity aged ≥ xx days · "
    f"Mod_xx = Backoffice-cleaned (transfers excluded) qty aged ≥ xx days · "
    f"buckets: {AGING_BUCKETS}"
)

# Backoffice reference
with st.expander("📘 Backoffice modifier table (Transaction Type → Modifier)"):
    bo = pd.DataFrame(
        [
            {"Transaction type": k,
             "Group": "Transfer (excluded)" if v == 1 else "WIP (counted)",
             "Modifier": v}
            for k, v in BACKOFFICE_TRANSFER_MODIFIERS.items()
        ]
    )
    st.dataframe(bo, hide_index=True, width="stretch")

# ---------------------------------------------------------------------------
# Excel export
# ---------------------------------------------------------------------------
st.subheader("4️⃣  Download Excel workbook")
yymmdd = as_of.strftime("%y%m%d")
default_name = f"{side} {plant} WIP Aging review {yymmdd}.xlsx"
if st.button("🧮 Generate workbook"):
    with st.spinner("Rendering workbook…"):
        blob = export_workbook(wip, sources, kpis, side=side, as_of=as_of, plant=plant)
    st.download_button(
        "⬇️ Download " + default_name, data=blob, file_name=default_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
    )

st.caption(
    "Workbook tabs: Instructions · WIP Analysis · Transaction report · "
    "PIVOT Transaction1 · Manage WO Report · MOWR PIVOT · WO History · "
    "Demand · Pivot Demand · Backoffice."
)
