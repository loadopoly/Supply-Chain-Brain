"""
EDAP Data Query Console — page 0
Unified search across Azure SQL (edap-replica-cms-sqldb) and Oracle Fusion Cloud (DEV13).
Moved here so app.py can use st.navigation() for reliable MPA routing.
"""
from pathlib import Path
import sys
import json
import time
import threading
import requests
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.connections import azure_sql
from src.connections.oracle_fusion import OracleFusionSession
from src.brain.db_registry import bootstrap_default_connectors
from src.brain.findings_index import lookup_findings, record_finding

bootstrap_default_connectors()

st.markdown("""
<style>
.hit-header { background:#1e3a5f; color:white; padding:6px 12px; border-radius:4px;
              font-weight:600; margin-bottom:4px; font-size:13px; }
.source-oracle { background:#c8430a; }
.source-azure  { background:#0078d4; }
.metric-box { background:#f0f4f8; border-left:4px solid #0078d4;
              padding:8px 14px; border-radius:4px; margin:4px 0; }
.section-title { font-size:16px; font-weight:700; color:#1e3a5f; margin:12px 0 6px; }
.stream-box { background:#f8f9fa; border:1px solid #dee2e6; border-radius:6px;
              padding:12px; margin:6px 0; }
</style>
""", unsafe_allow_html=True)

# ── Connection cache ─────────────────────────────────────────────────────────
@st.cache_resource(show_spinner="Connecting to Azure SQL …")
def _azure_conn_holder():
    return {"conn": azure_sql.get_connection()}

def get_azure_conn():
    holder = _azure_conn_holder()
    holder["conn"] = azure_sql.get_or_reconnect(holder["conn"])
    return holder["conn"]

@st.cache_resource(show_spinner="Connecting to Oracle Fusion …")
def get_oracle_session():
    sess = OracleFusionSession()
    sess.connect()
    return sess

# ── Schema ──────────────────────────────────────────────────────────────────
AZURE_SEARCH_TABLES = {
    "edap_dw_replica": {
        "dim_part":               ["part_number", "part_description"],
        "dim_customer":           ["customer_name", "customer_number"],
        "dim_supplier":           ["supplier_name"],
        "dim_po_contract":        ["supplier_name"],
        "fact_sales_order_header":["sales_order_number"],
        "fact_sales_order_line":  ["sales_order_number"],
        "fact_ap_invoices":       ["invoice_number"],
        "fact_ar_invoices":       ["sales_order_number", "ar_invoice_number"],
        "fact_ofs_extract_order_header": ["order_number"],
        "fact_ofs_extract_order_lines":  ["order_number"],
        "fact_ofs_extract_service_request": ["sr_number"],
        "fact_ofs_extract_cx_ib":           ["asset_number"],
    },
    "stg_replica": {
        "fact_quote_header": ["quote_number"],
        "fact_quote_line":   ["quote_number"],
    },
    "rpt_replica": {
        "pdm_materials":       ["ITEM_NUMBER", "DESCRIPTION"],
        "pdm_one_astec_xref":  ["one_astec_part_number", "legacy_part_number"],
        "pdm_item_xref":       ["ITEM_NUMBER", "RELATED_ITEM_NUMBER"],
        "bti_trade_compliance_extract": ["item", "description"],
    },
}

UPSTREAM_TABLES   = [
    "dim_supplier","dim_po_contract","fact_po_receipt","fact_po_contract_part",
    "fact_part_cost","fact_ap_invoice_lines","fact_ap_invoices",
    "fact_inventory_open_orders","fact_po_receipt",
]
DOWNSTREAM_TABLES = [
    "fact_sales_order_header","fact_sales_order_line","fact_sales_order_line (by part)",
    "fact_ar_invoices","fact_ar_invoice_lines",
    "fact_ofs_extract_order_header","fact_ofs_extract_order_lines",
    "fact_ofs_extract_service_request",
]
PART_TABLES = [
    "dim_part","dim_part_code","fact_inventory_on_hand",
    "fact_inventory_open_orders","fact_inventory_open_mfg_orders",
    "fact_inventory_open_mfg","pdm_materials","pdm_one_astec_xref",
    "stg_replica.fact_part_cost",
]

QUERY_TIMEOUT = 12

def _query(conn, sql, params=None):
    cursor = conn.cursor()
    cursor.timeout = QUERY_TIMEOUT
    try:
        cursor.execute(sql, params or [])
        cols = [d[0] for d in cursor.description]
        return pd.DataFrame.from_records(cursor.fetchall(), columns=cols)
    except Exception:
        return pd.DataFrame()
    finally:
        cursor.close()

def search_azure(conn, term):
    results = {}
    for schema, tables in AZURE_SEARCH_TABLES.items():
        for table, search_cols in tables.items():
            if not search_cols:
                continue
            where  = " OR ".join(f"[{c}] LIKE ?" for c in search_cols)
            params = [f"%{term}%"] * len(search_cols)
            df = _query(conn, f"SELECT TOP 200 * FROM [{schema}].[{table}] WHERE {where}", params)
            if not df.empty:
                results[f"{schema}.{table}"] = df
    return results

def search_by_part_keys(conn, part_keys):
    if not part_keys:
        return {}
    keys_sql = ", ".join(str(k) for k in part_keys)
    results  = {}
    queries  = {
        "edap_dw_replica.dim_part_code":              f"SELECT * FROM [edap_dw_replica].[dim_part_code] WHERE part_key IN ({keys_sql})",
        "edap_dw_replica.fact_inventory_on_hand":     f"SELECT TOP 10 * FROM [edap_dw_replica].[fact_inventory_on_hand] WHERE part_key IN ({keys_sql}) ORDER BY snapshot_day_key DESC",
        "edap_dw_replica.fact_inventory_open_orders": f"SELECT TOP 20 * FROM [edap_dw_replica].[fact_inventory_open_orders] WHERE part_key IN ({keys_sql}) ORDER BY snapshot_day_key DESC",
        "edap_dw_replica.fact_inventory_open_mfg_orders": f"SELECT TOP 20 * FROM [edap_dw_replica].[fact_inventory_open_mfg_orders] WHERE part_key IN ({keys_sql}) ORDER BY snapshot_date_key DESC",
        "edap_dw_replica.fact_sales_order_line (by part)": f"SELECT TOP 30 * FROM [edap_dw_replica].[fact_sales_order_line] WHERE part_key IN ({keys_sql}) ORDER BY due_date_key DESC",
        "edap_dw_replica.fact_ap_invoice_lines":      f"SELECT TOP 30 * FROM [edap_dw_replica].[fact_ap_invoice_lines] WHERE part_key IN ({keys_sql}) ORDER BY invoice_date_key DESC",
        "edap_dw_replica.fact_po_contract_part":      f"SELECT TOP 20 * FROM [edap_dw_replica].[fact_po_contract_part] WHERE part_key IN ({keys_sql})",
        "edap_dw_replica.fact_po_receipt":            f"SELECT TOP 20 * FROM [edap_dw_replica].[fact_po_receipt] WHERE part_key IN ({keys_sql}) ORDER BY receipt_date_key DESC",
        "stg_replica.fact_part_cost":                 f"SELECT * FROM [stg_replica].[fact_part_cost] WHERE part_key IN ({keys_sql})",
    }
    for label, sql in queries.items():
        df = _query(conn, sql)
        if not df.empty:
            results[label] = df
    return results

def search_oracle(oracle_sess, term):
    results = {}
    host    = oracle_sess.host
    s       = oracle_sess.session
    base    = f"{host}/fscmRestApi/resources/11.13.18.05"
    queries = [
        ("Items",           f"{base}/items?q=ItemNumber={term}&limit=50&fields=ItemNumber,Description,ItemType,UOM"),
        ("Purchase Orders", f"{base}/purchaseOrders?q=OrderNumber={term}&limit=50"),
        ("Suppliers",       f"{base}/suppliers?q=SupplierName={term}&limit=50&fields=SupplierName,SupplierNumber,Status"),
        ("AP Invoices",     f"{base}/invoices?q=InvoiceNumber={term}&limit=50"),
        ("Sales Orders",    f"{base}/receivablesInvoices?q=TransactionNumber={term}&limit=50"),
    ]
    for label, url in queries:
        try:
            r = s.get(url, timeout=20)
            if r.status_code == 200:
                items = r.json().get("items", [])
                if items:
                    results[f"Oracle Fusion — {label}"] = pd.DataFrame(items)
            elif r.status_code == 401:
                results["_auth_needed"] = pd.DataFrame([{
                    "status": "Oracle Fusion REST requires Basic auth or IDCS OAuth.",
                    "action": "Set ORACLE_FUSION_USER/ORACLE_FUSION_PASS in env or connections.yaml.",
                }])
                break
        except Exception as e:
            results[f"Oracle Fusion — {label} (error)"] = pd.DataFrame([{"error": str(e)}])
    return results

def build_upstream(conn, term, azure_results):
    return {k: v for k, v in azure_results.items() if k.split(".")[-1] in UPSTREAM_TABLES}

def build_downstream(conn, azure_results):
    return {k: v for k, v in azure_results.items() if k.split(".")[-1] in DOWNSTREAM_TABLES}

def get_part_details(conn, part_number):
    df = _query(conn, "SELECT * FROM [edap_dw_replica].[dim_part] WHERE [part_number] LIKE ?", [f"%{part_number}%"])
    return df if not df.empty else None

def _candidate_id_columns(df):
    pats = ("part_key","part_number","supplier_key","supplier_name",
            "order_number","po_number","invoice_number","asset_number",
            "customer_number","item_number","sr_number")
    return [c for c in df.columns if any(p in c.lower() for p in pats)]

def render_results(results, source):
    if not results:
        st.info(f"No matches found in {source}.")
        return
    for table_key, df in results.items():
        if table_key == "_auth_needed":
            st.warning(df.iloc[0]["status"])
            st.code(df.iloc[0]["action"])
            continue
        label_class = "source-oracle" if "Oracle" in table_key else "source-azure"
        st.markdown(f'<div class="hit-header {label_class}">📋 {table_key} — {len(df)} row(s)</div>',
                    unsafe_allow_html=True)
        try:
            ev = st.dataframe(df, use_container_width=True, hide_index=True,
                              key=f"hit_{table_key}", on_select="rerun", selection_mode="multi-row")
            sel_idx = getattr(getattr(ev, "selection", None), "rows", []) or []
        except TypeError:
            st.dataframe(df, use_container_width=True, hide_index=True)
            sel_idx = []
        if sel_idx:
            with st.container(border=True):
                st.markdown(f"### 🔎 Drill-down · {len(sel_idx)} row(s) from `{table_key}`")
                rows   = df.iloc[sel_idx]
                id_cols = _candidate_id_columns(df)
                for _, row in rows.iterrows():
                    label_bits = [f"{c}={row[c]}" for c in id_cols if pd.notna(row.get(c))][:3]
                    with st.expander(" · ".join(label_bits) or "row", expanded=True):
                        cL, cR = st.columns([2, 1])
                        with cL:
                            st.json({k: (str(v) if pd.notna(v) else None) for k, v in row.items()},
                                    expanded=False)
                        with cR:
                            st.markdown("**Cross-page findings**")
                            any_found = False
                            for c in id_cols:
                                k = row.get(c)
                                if pd.isna(k):
                                    continue
                                kind = ("part" if "part" in c.lower() else
                                        "supplier" if "supplier" in c.lower() else "row")
                                hits = lookup_findings(kind=kind, key=str(k), limit=10)
                                if hits:
                                    any_found = True
                                    st.caption(f"`{c}` → {kind}")
                                    st.dataframe(pd.DataFrame([{"page": h["page"], "score": h["score"],
                                                                "when": h["created_at"]} for h in hits]),
                                                 hide_index=True, use_container_width=True)
                            if not any_found:
                                st.caption("No cross-page findings yet.")

# ── Main UI ──────────────────────────────────────────────────────────────────
st.title("🔍 EDAP Query Console")
st.caption("Unified search across Azure SQL & Oracle Fusion Cloud · Real-time aggregation")

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight('Query Console', ctx)
st.divider()

col_input, col_btn = st.columns([5, 1])
with col_input:
    search_term = st.text_input(
        label="Search",
        placeholder="Enter part number, order number, invoice, supplier, or any value … (e.g. 544-11555-22)",
        label_visibility="collapsed",
    )
with col_btn:
    run = st.button("Search", type="primary", use_container_width=True)

show_raw_sql = st.sidebar.checkbox("Enable raw SQL mode", value=False)

if show_raw_sql:
    st.markdown("---")
    st.markdown("**Raw SQL Query** (runs against Azure SQL `edap-replica-cms-sqldb`)")
    raw_sql = st.text_area("SQL", height=80,
                            placeholder="SELECT TOP 100 * FROM [edap_dw_replica].[dim_part] WHERE part_number LIKE '%544%'")
    if st.button("Run SQL", type="secondary") and raw_sql.strip():
        try:
            conn = get_azure_conn()
            df   = pd.read_sql(raw_sql, conn)
            st.success(f"{len(df)} rows returned")
            st.dataframe(df, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Query error: {e}")

if (run or search_term) and search_term.strip():
    term = search_term.strip()
    st.markdown("---")
    with st.spinner(f"Searching Azure SQL for **{term}** …"):
        conn          = get_azure_conn()
        azure_results = search_azure(conn, term)
        part_df_check = azure_results.get("edap_dw_replica.dim_part")
        if part_df_check is not None and not part_df_check.empty and "part_key" in part_df_check.columns:
            pkeys = part_df_check["part_key"].dropna().astype(int).tolist()
            azure_results.update(search_by_part_keys(conn, pkeys))

    with st.spinner(f"Searching Oracle Fusion (DEV13) for **{term}** …"):
        try:
            oracle_sess    = get_oracle_session()
            oracle_results = search_oracle(oracle_sess, term)
        except Exception as e:
            oracle_results = {"_error": pd.DataFrame([{"error": str(e)}])}

    total_azure  = sum(len(v) for v in azure_results.values())
    total_oracle = sum(len(v) for k, v in oracle_results.items() if not k.startswith("_"))
    all_tables   = len(azure_results) + len([k for k in oracle_results if not k.startswith("_")])

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Tables matched",    all_tables)
    m2.metric("Azure SQL rows",    total_azure)
    m3.metric("Oracle Fusion rows", total_oracle)
    m4.metric("Search term",       f'"{term}"')
    st.markdown("---")

    part_df = get_part_details(conn, term)
    if part_df is not None:
        st.markdown('<div class="section-title">🔩 Part Master (dim_part)</div>', unsafe_allow_html=True)
        st.dataframe(part_df, use_container_width=True, hide_index=True)
        st.markdown("---")

    upstream   = build_upstream(conn, term, azure_results)
    downstream = build_downstream(conn, azure_results)
    part_hits  = {k: v for k, v in azure_results.items()
                  if any(t in k for t in PART_TABLES)}
    other_hits = {k: v for k, v in azure_results.items()
                  if k not in upstream and k not in downstream and k not in part_hits}

    tab_up, tab_down, tab_part, tab_all, tab_oracle = st.tabs([
        f"⬆ Upstream ({len(upstream)})",
        f"⬇ Downstream ({len(downstream)})",
        f"📦 Part/Inventory ({len(part_hits)})",
        f"📊 All Azure ({len(azure_results)})",
        f"🔴 Oracle Fusion ({len([k for k in oracle_results if not k.startswith('_')])})",
    ])
    with tab_up:
        render_results(upstream, "Upstream")
    with tab_down:
        render_results(downstream, "Downstream")
    with tab_part:
        render_results(part_hits, "Part/Inventory")
    with tab_all:
        render_results(azure_results, "Azure SQL") if azure_results else st.warning("No Azure matches.")
    with tab_oracle:
        render_results(oracle_results, "Oracle Fusion")
else:
    st.markdown("<br>", unsafe_allow_html=True)
    st.info("Enter a value above to search both databases simultaneously.")
    st.markdown("""
**Examples:**
- Part number: `544-11555-22`
- Order number: `1234567`
- Invoice: `INV-2024-001`
- Supplier name: `Acme`
- Customer: `ASTEC`
    """)
