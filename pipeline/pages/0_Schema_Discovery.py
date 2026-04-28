"""
Page 0 — Schema Discovery & Column Mapping
Introspects both Azure SQL databases, caches discovered schemas to
config/schema_cache.json, and shows the live column→semantic mapping
so all other pages can self-heal their column lookups.
"""
from pathlib import Path
import sys
import json
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.operator_shell import render_operator_sidebar_fallback
from src.brain.db_registry import bootstrap_default_connectors, read_sql
from src.brain.col_resolver import (
    discover_all_key_tables,
    discover_table_columns,
    resolve,
    PATTERNS,
    _CACHE_FILE,
    _load_cache,
)

st.session_state["_page"] = "schema_discovery"
render_operator_sidebar_fallback()
bootstrap_default_connectors()

st.markdown("## 🔬 Schema Discovery & Column Mapping")
st.caption(
    "Live introspection of Azure SQL schemas. "
    "Results are cached to `config/schema_cache.json` — all other pages auto-resolve columns from this cache."
)

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight('Schema Discovery', ctx)
st.divider()


# ── Toolbar ──────────────────────────────────────────────────────────────────
col_refresh, col_status = st.columns([2, 3])
run_discovery = col_refresh.button("🔄 Refresh Schema Cache", type="primary")
cache_exists = _CACHE_FILE.exists()
if cache_exists:
    cache = _load_cache()
    tables_cached = len(cache)
    col_status.success(f"✅ Cache: {tables_cached} table(s) — `{_CACHE_FILE.name}`")
else:
    col_status.warning("⚠️ No schema cache yet — click Refresh.")

st.divider()

# ── Discovery run ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=0, show_spinner=False)  # no ttl — use the explicit button
def _run_discovery():
    return discover_all_key_tables("azure_sql")

if run_discovery or not cache_exists:
    with st.spinner("Querying INFORMATION_SCHEMA for all key tables …"):
        discovered = _run_discovery()
    st.success(f"✅ Discovered {len(discovered)} tables, cache updated.")
    st.cache_data.clear()
else:
    # Load from cache
    discovered = {}
    cache = _load_cache()
    for key, cols in cache.items():
        connector, schema, table = key.split("|", 2)
        logical = f"{schema}.{table}"
        discovered[logical] = cols

# ── Show results ──────────────────────────────────────────────────────────────
LOGICAL_NAMES = {
    "parts":             ("edap_dw_replica", "dim_part"),
    "suppliers":         ("edap_dw_replica", "dim_supplier"),
    "po_receipts":       ("edap_dw_replica", "fact_po_receipt"),
    "sales_order_lines": ("edap_dw_replica", "fact_sales_order_line"),
    "on_hand":           ("edap_dw_replica", "fact_inventory_on_hand"),
    "open_purchase":     ("edap_dw_replica", "fact_inventory_open_orders"),
    "open_mfg":          ("edap_dw_replica", "fact_inventory_open_mfg_orders"),
    "part_cost":         ("stg_replica",     "fact_part_cost"),
    "po_contract_part":  ("edap_dw_replica", "fact_po_contract_part"),
    "ap_invoice_lines":  ("edap_dw_replica", "fact_ap_invoice_lines"),
}

import re

@st.cache_data(show_spinner=False)
def get_schema_reviews():
    """Parse DATA_DICTIONARY and EDAP_DASHBOARD_TABLES for schema reviews."""
    reviews = {}
    docs_dir = Path(__file__).resolve().parents[1] / "docs"
    
    dict_path = docs_dir / "DATA_DICTIONARY.md"
    if dict_path.exists():
        content = dict_path.read_text(encoding="utf-8")
        sections = content.split("### ")
        for sec in sections[1:]:
            lines = sec.strip().split("\n")
            if not lines: continue
            title_line = lines[0].replace(">", "").strip()
            table_match = re.search(r'`([^`]+)`', title_line)
            table_name = table_match.group(1).lower() if table_match else title_line.lower().split()[0]
            
            desc_lines = []
            for line in lines[1:]:
                cl = line.replace("> ", "").replace(">", "").strip()
                if cl.startswith("#"): break
                if cl and not cl.startswith("|") and "--" not in cl:
                    desc_lines.append(cl)
            
            if desc_lines:
                reviews[table_name] = "\n".join(desc_lines)
                
    dash_path = docs_dir / "EDAP_DASHBOARD_TABLES.md"
    if dash_path.exists():
        content = dash_path.read_text(encoding="utf-8")
        sections = content.split("## ")
        for sec in sections[1:]:
            lines = sec.strip().split("\n")
            if not lines: continue
            title_line = lines[0].strip()
            
            tables_mentioned = list(set(re.findall(r'`([^`]+)`', sec)))
            if tables_mentioned:
                dash_info = [f"**Dashboard Usage:** {title_line}"]
                for line in lines[1:8]:
                    if line.strip().startswith("**Grain:**") or line.strip().startswith("**Primary source tables:**"):
                        dash_info.append(line.strip())
                dash_text = "\n".join(dash_info)
                
                for t in tables_mentioned:
                    t_lower = t.lower()
                    if t_lower in reviews:
                        if "Dashboard Usage:" not in reviews[t_lower]:
                            reviews[t_lower] += "\n\n---\n" + dash_text
                    else:
                        reviews[t_lower] = dash_text
                        
    return reviews


# Also list all tables across all registered connectors for broader exploration
st.subheader("🗂 Unified Database Explorer")
st.caption("Browse all schemas, tables, and columns across all attached databases.")

from src.brain import db_registry
all_connectors = db_registry.list_connectors()
conn_names = [c.name for c in all_connectors]
selected_connector_name = st.selectbox("Select Database Connector", conn_names)

if selected_connector_name:
    conn_obj = db_registry.get(selected_connector_name)
    
    if conn_obj.kind == "sql":
        tbl_sql = """
        SELECT TABLE_SCHEMA, TABLE_NAME,
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c2
                WHERE c2.TABLE_SCHEMA = t.TABLE_SCHEMA AND c2.TABLE_NAME = t.TABLE_NAME) AS col_count
        FROM INFORMATION_SCHEMA.TABLES t
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        tbl_df = read_sql(selected_connector_name, tbl_sql)
        if not tbl_df.attrs.get("_error") and not tbl_df.empty:
            search_term = st.text_input(f"Filter table name in {selected_connector_name}", key="tbl_filter")
            if search_term:
                mask = tbl_df.apply(lambda r: search_term.lower() in str(r).lower(), axis=1)
                tbl_df = tbl_df[mask]
            st.dataframe(tbl_df, use_container_width=True, hide_index=True)
            
            # Manual explorer
            if "TABLE_SCHEMA" in tbl_df.columns:
                schema_sel = st.selectbox("Schema", sorted(tbl_df["TABLE_SCHEMA"].unique()), key=f"{selected_connector_name}_schema")
                if schema_sel and "TABLE_NAME" in tbl_df.columns:
                    table_sel = st.selectbox("Table", sorted(tbl_df[tbl_df["TABLE_SCHEMA"] == schema_sel]["TABLE_NAME"].unique()), key=f"{selected_connector_name}_table")
                    if table_sel and st.button("Inspect columns", key=f"inspect_btn_{selected_connector_name}"):
                        reviews = get_schema_reviews()
                        t_key = table_sel.lower()
                        if t_key in reviews:
                            st.info(f"**Data Dictionary & Dashboard Review for `{table_sel}`:**\n\n{reviews[t_key]}")
                            
                        cols = discover_table_columns(selected_connector_name, schema_sel, table_sel, force=True)
                        st.write(f"**{schema_sel}.{table_sel}** — {len(cols)} columns:")
                        # Show with semantic resolution
                        rows = []
                        for c in cols:
                            sems = [sem for sem, pats in PATTERNS.items()
                                    if any(p.lower() in c.lower() or c.lower() in p.lower() for p in pats)]
                            rows.append({"column": c, "semantic_roles": ", ".join(sems) if sems else "—"})
                        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.info(f"Could not enumerate tables for {selected_connector_name} — INFORMATION_SCHEMA may be restricted or unsupported.")
            
    elif conn_obj.name == "oracle_fusion":
        with st.spinner("Fetching Oracle Fusion subject areas..."):
            try:
                try:
                    sess = conn_obj.handle()
                    schemas = sess.list_subject_areas()
                except Exception:
                    conn_obj.reset()
                    sess = conn_obj.handle()
                    schemas = sess.list_subject_areas()
                if isinstance(schemas, list):
                    schema_names = [s if isinstance(s, str) else s.get("name", str(s)) for s in schemas]
                    schema_sel = st.selectbox("Subject Area", sorted(schema_names), key="oracle_schema")
                    if schema_sel:
                        with st.spinner("Fetching tables..."):
                            tables = sess.list_tables(schema_sel)
                            if isinstance(tables, list):
                                table_names = [t if isinstance(t, str) else t.get("name", str(t)) for t in tables]
                                table_sel = st.selectbox("Table", sorted(table_names), key="oracle_table")
                                if table_sel and st.button("Inspect columns", key="oracle_inspect"):
                                    reviews = get_schema_reviews()
                                    t_key = table_sel.lower()
                                    if t_key in reviews:
                                        st.info(f"**Data Dictionary & Dashboard Review for `{table_sel}`:**\n\n{reviews[t_key]}")
                                        
                                    with st.spinner("Fetching columns..."):
                                        try:
                                            cols = sess.list_columns(table_sel)
                                            st.write(f"**{schema_sel}.{table_sel}** — {len(cols)} columns:")
                                            
                                            # format oracle columns gracefully
                                            if cols and isinstance(cols, list):
                                                col_rows = []
                                                for c in cols:
                                                    name = c.get('name', str(c)) if isinstance(c, dict) else str(c)
                                                    datatype = c.get('dataType', '—') if isinstance(c, dict) else '—'
                                                    col_rows.append({"column": name, "type": datatype})
                                                st.dataframe(pd.DataFrame(col_rows), use_container_width=True, hide_index=True)
                                            else:
                                                st.json(cols)
                                        except Exception as e:
                                            st.error(f"Failed to fetch columns: {e}")
                            else:
                                st.info("No tables found or format error.")
                else:
                    st.warning("Could not fetch Oracle subject areas. Check connection or format.")
            except Exception as e:
                st.error(f"Failed to connect to Oracle Fusion: {e}")

st.divider()

# ── Per-table semantic mapping view ──────────────────────────────────────────
st.subheader("📋 Logical Table → Column Mapping")
st.caption(
    "Green = column resolved · Orange = not found. "
    "Use the actual column names below to fix any pages that are failing."
)

SEMANTIC_ROLES = [
    "part_key", "supplier_key", "quantity", "on_hand_qty", "open_qty",
    "unit_cost", "promise_date", "receipt_date", "order_date", "lead_time_days",
    "days_late", "site", "commodity", "buyer",
]

summary_rows = []
for logical_name, (schema, table) in LOGICAL_NAMES.items():
    cols = discover_table_columns("azure_sql", schema, table)
    row = {"Logical Table": logical_name, "Physical": f"{schema}.{table}",
           "# Cols": len(cols)}
    for role in SEMANTIC_ROLES:
        found = resolve(cols, role)
        row[role] = f"✅ {found}" if found else "—"
    summary_rows.append(row)

summary_df = pd.DataFrame(summary_rows)

# Show with colour-coding via st.dataframe column config
col_cfg = {}
for role in SEMANTIC_ROLES:
    col_cfg[role] = st.column_config.TextColumn(role.replace("_", " ").title(), width="medium")

st.dataframe(summary_df, use_container_width=True, hide_index=True, column_config=col_cfg)

# ── Per-table column dump ─────────────────────────────────────────────────────
st.divider()
st.subheader("🔎 Full Column Listing (with sample data)")
selected_logical = st.selectbox("Choose table to inspect", list(LOGICAL_NAMES.keys()))

if selected_logical:
    schema, table = LOGICAL_NAMES[selected_logical]
    cols = discover_table_columns("azure_sql", schema, table)

    if not cols:
        st.error(
            f"No columns discovered for [{schema}].[{table}]. "
            "The table may not exist or you may need to refresh the cache."
        )
    else:
        st.write(f"**{len(cols)} columns** in `[{schema}].[{table}]`:")
        col_rows = []
        for c in cols:
            sems = [sem for sem, pats in PATTERNS.items()
                    if any(p.lower() in c.lower() or c.lower() in p.lower() for p in pats)]
            col_rows.append({
                "Column (original)": c,
                "Normalized": c.lower().replace(" ", "_"),
                "Semantic matches": ", ".join(sems) if sems else "—"
            })
        st.dataframe(pd.DataFrame(col_rows), use_container_width=True, hide_index=True)

        # Sample data (TOP 5)
        if st.button(f"Preview 5 rows from {table}", key="preview_btn"):
            sample_sql = f"SELECT TOP 5 * FROM [{schema}].[{table}]"
            sample_df = read_sql("azure_sql", sample_sql)
            if sample_df.attrs.get("_error"):
                st.error(f"Sample error: {sample_df.attrs['_error']}")
            else:
                st.dataframe(sample_df, use_container_width=True, hide_index=True)

# ── Resolved SQL snippets helper ─────────────────────────────────────────────
st.divider()
st.subheader("🛠 Resolved SQL Snippet Builder")
st.caption("Use this to build corrected SQL for any page based on discovered column names.")

tbl_choice = st.selectbox("Source table", list(LOGICAL_NAMES.keys()), key="sql_tbl")
if tbl_choice:
    sch2, tbl2 = LOGICAL_NAMES[tbl_choice]
    cols2 = discover_table_columns("azure_sql", sch2, tbl2)
    if cols2:
        roles_to_show = st.multiselect(
            "Semantic columns to include",
            SEMANTIC_ROLES,
            default=["part_key", "supplier_key", "quantity", "receipt_date"],
            key="sql_roles"
        )
        select_parts = []
        for role in roles_to_show:
            found = resolve(cols2, role)
            if found:
                select_parts.append(f"    [{found}]   AS {role}")
            else:
                select_parts.append(f"    -- (no match for {role})")
        sql_snippet = (
            f"SELECT TOP 100\n"
            + ",\n".join(select_parts)
            + f"\nFROM [{sch2}].[{tbl2}]"
        )
        st.code(sql_snippet, language="sql")
    else:
        st.info("No columns discovered — click 'Refresh Schema Cache' first.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "💾 Schema cache stored at `pipeline/config/schema_cache.json`. "
    "Delete this file to force a full re-discovery. "
    "All 14 app pages use `col_resolver.resolve()` to adapt to the actual column names automatically."
)
