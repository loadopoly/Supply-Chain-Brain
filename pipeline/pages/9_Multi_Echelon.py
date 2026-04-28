"""Multi-Echelon Safety Stock — Graves-Willems guaranteed-service with rich Plotly visualizations."""
from __future__ import annotations
from pathlib import Path
import sys
import pandas as pd
import numpy as np
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.db_registry import bootstrap_default_connectors, list_connectors, read_sql
from src.brain.research.multi_echelon import safety_stock_per_stage, total_holding_cost
from src.brain import load_config
from src.brain.col_resolver import discover_table_columns, resolve
from src.brain.label_resolver import get_supplier_labels
from src.brain.global_filters import date_key_window, get_global_window
from src.brain.operator_shell import render_operator_sidebar_fallback

# set_page_config handled by app.py st.navigation()
render_operator_sidebar_fallback()
bootstrap_default_connectors()

import plotly.express as px
import plotly.graph_objects as go

st.markdown("## 🏗️ Multi-Echelon Safety Stock")
st.caption("Graves-Willems guaranteed-service model · optimal stock placement across network · MIT CTL framework")

cfg = (load_config() or {}).get("multi_echelon", {})
sl = st.slider("Service level", 0.80, 0.999,
               float(cfg.get("default_service_level", 0.95)), 0.005)

connectors = list_connectors()


def _build_me_sql() -> str:
    def _d(c): return f"TRY_CONVERT(date, CONVERT(varchar(8), [{c}]), 112)"
    cols = discover_table_columns("azure_sql", "edap_dw_replica", "fact_po_receipt")
    sup_col     = resolve(cols, "supplier_key") or "supplier_key"
    order_col   = resolve(cols, "due_date")   or "due_date_key"
    receipt_col = resolve(cols, "receipt_date") or "receipt_date_key"
    price_col   = resolve(cols, "unit_cost")    or "unit_price"
    return f"""
WITH lt AS (
    SELECT [{sup_col}] AS supplier_key,
           AVG(CAST(DATEDIFF(day,
               {_d(order_col)},
               {_d(receipt_col)}) AS float)) AS T_i,
           STDEV(CAST(DATEDIFF(day,
               {_d(order_col)},
               {_d(receipt_col)}) AS float)) AS sigma_i,
           AVG(TRY_CONVERT(float, [{price_col}])) AS unit_cost
    FROM [edap_dw_replica].[fact_po_receipt] WITH (NOLOCK)
    WHERE {_d(receipt_col)} IS NOT NULL
      AND {_d(order_col)}   IS NOT NULL
      AND DATEDIFF(day,
              {_d(order_col)},
              {_d(receipt_col)}) BETWEEN 0 AND 730
    GROUP BY [{sup_col}]
)
SELECT TOP 100
       CAST(supplier_key AS varchar(64)) AS stage_id,
       ISNULL(T_i, 0)          AS T_i,
       7.0                     AS S_i,
       0.0                     AS SI_i,
       ISNULL(sigma_i, 1.0)    AS sigma_i,
       ISNULL(unit_cost, 1.0)  AS unit_cost
FROM lt
WHERE T_i IS NOT NULL
ORDER BY T_i DESC
"""


def _get_me_sql() -> str:
    # Always rebuild dynamically so Global Timeline date bounds apply
    # Fallback to the static query but wrapped in the timeline logic.
    _sk, _ek = date_key_window()
    site = st.session_state.get("g_site", "")
    w_site = ""
    if site:
        w_site = f"AND business_unit_key IN (SELECT business_unit_key FROM edap_dw_replica.dim_business_unit WITH (NOLOCK) WHERE business_unit_id = '{site}')"

    try:
        sql = _build_me_sql()
        # insert timeline constraint right before GROUP BY
        sql = sql.replace("GROUP BY", f"AND receipt_date_key BETWEEN {_sk} AND {_ek} {w_site}\n    GROUP BY")
        return sql
    except Exception:
        return f"""
WITH lt AS (
    SELECT [supplier_key],
           AVG(CAST(DATEDIFF(day,
               TRY_CONVERT(date, CONVERT(varchar(8), [due_date_key]), 112),
               TRY_CONVERT(date, CONVERT(varchar(8), [receipt_date_key]), 112)) AS float)) AS T_i,
           STDEV(CAST(DATEDIFF(day,
               TRY_CONVERT(date, CONVERT(varchar(8), [due_date_key]), 112),
               TRY_CONVERT(date, CONVERT(varchar(8), [receipt_date_key]), 112)) AS float)) AS sigma_i,
           1.0 AS unit_cost
    FROM [edap_dw_replica].[fact_po_receipt] WITH (NOLOCK)
    WHERE TRY_CONVERT(date, CONVERT(varchar(8), [receipt_date_key]), 112) IS NOT NULL
      AND receipt_date_key BETWEEN {_sk} AND {_ek} {w_site}
      AND DATEDIFF(day,
          TRY_CONVERT(date, CONVERT(varchar(8), [due_date_key]), 112),
          TRY_CONVERT(date, CONVERT(varchar(8), [receipt_date_key]), 112)) BETWEEN 0 AND 730
    GROUP BY [supplier_key]
)
SELECT TOP 100 CAST(supplier_key AS varchar(64)) AS stage_id,
       ISNULL(T_i, 0) AS T_i, 7.0 AS S_i, 0.0 AS SI_i,
       ISNULL(sigma_i, 1.0) AS sigma_i, 1.0 AS unit_cost
FROM lt WHERE T_i IS NOT NULL ORDER BY T_i DESC
"""

DEFAULT_SQL = _get_me_sql()

with st.expander("🔍 Source / upload", expanded=False):
    src_name = st.selectbox("Source connector", [c.name for c in connectors], key="me_cn")
    sql = st.text_area("SQL", value=DEFAULT_SQL, height=120, key="me_sql")
    uploaded = st.file_uploader("…or upload a CSV with the same columns", type=["csv"], key="me_csv")

src_name = st.session_state.get("me_cn", connectors[0].name if connectors else "azure_sql")
sql      = st.session_state.get("me_sql", DEFAULT_SQL)

@st.cache_data(ttl=600, show_spinner="Pulling supply-stage view from replica …")
def _load(cn: str, q: str):
    return read_sql(cn, q, timeout_s=120)

if uploaded:
    stages = pd.read_csv(uploaded)
    st.caption(f"📄 CSV upload · {len(stages):,} stages")
else:
    stages = _load(src_name, sql)
    if stages.attrs.get("_error"):
        st.error(stages.attrs["_error"])
        st.code(sql, language="sql")
        with st.expander("ℹ️ Fix: create view or remap SQL", expanded=True):
            st.markdown("""
Create a view or update the SQL to return:
`stage_id, T_i, S_i, SI_i, sigma_i, unit_cost`
""")
        st.stop()
    if stages.empty:
        st.warning("Live `vw_supply_stage` returned 0 rows.")
        st.stop()
    st.markdown(f"🟢 **Live** · {len(stages):,} stages")

result = safety_stock_per_stage(stages, service_level=sl)
if "error" in result.columns:
    st.error(result.iloc[0]["error"])
    st.stop()

cost = total_holding_cost(result, holding_rate=float(cfg.get("holding_rate", 0.22)))

# Resolve supplier names for stage_id (raw key → "NAME (key)" for charts/tables)
_sup_lbl = get_supplier_labels()
if "stage_id" in result.columns:
    result["stage_label"] = result["stage_id"].astype(str).map(
        lambda k: _sup_lbl.get(k, k)
    )

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight('Multi Echelon', ctx)

# ── KPI strip ────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
k1.metric("🏭 Stages",             len(result))
k2.metric("📦 Σ Safety Stock",     f"{result['safety_stock'].sum():,.0f}" if "safety_stock" in result.columns else "—")
k3.metric("💰 Annual Holding $",   f"${cost:,.0f}")
k4.metric("📊 Service Level",      f"{sl:.1%}")

st.divider()

tab1, tab2, tab3 = st.tabs(["🏗️ Waterfall","🔀 Network Sankey","📋 Stage Detail"])

with tab1:
    st.subheader("Safety Stock Waterfall by Stage")
    if "safety_stock" in result.columns and "stage_id" in result.columns:
        sorted_r = result.sort_values("safety_stock", ascending=False)
        _x_col = "stage_label" if "stage_label" in sorted_r.columns else "stage_id"
        fig_wf = go.Figure(go.Waterfall(
            orientation="v",
            x=sorted_r[_x_col].astype(str).tolist(),
            y=sorted_r["safety_stock"].tolist(),
            connector=dict(line=dict(color="rgba(100,116,139,0.5)", width=1)),
            increasing=dict(marker_color="#38bdf8"),
            decreasing=dict(marker_color="#ef4444"),
            totals=dict(marker_color="#22c55e"),
            text=[f"{v:,.0f}" for v in sorted_r["safety_stock"]],
            textposition="outside",
        ))
        fig_wf.update_layout(
            height=450, template="plotly",
            title=f"Safety Stock per Stage (SL={sl:.1%})",
            xaxis_title="Stage", yaxis_title="Safety Stock Units",
            xaxis_tickangle=-45,
        )
        st.plotly_chart(fig_wf, use_container_width=True)

        # Cost breakdown
        if "unit_cost" in result.columns:
            result["holding_cost"] = result["safety_stock"] * result["unit_cost"] * float(cfg.get("holding_rate", 0.22))
            fig_cost = px.treemap(
                result, path=["stage_label" if "stage_label" in result.columns else "stage_id"], values="holding_cost",
                color="holding_cost", color_continuous_scale="RdYlGn_r",
                title="Annual Holding Cost Treemap by Stage",
                template="plotly",
            )
            fig_cost.update_layout(paper_bgcolor="#0f172a", height=380)
            st.plotly_chart(fig_cost, use_container_width=True)

with tab2:
    st.subheader("Supply Network Flow — Sankey Diagram")
    if "stage_id" in result.columns and "safety_stock" in result.columns:
        stage_list = result["stage_id"].astype(str).tolist()
        # Build a simple sequential chain from stage ordering
        if len(stage_list) >= 2:
            src_nodes = list(range(len(stage_list) - 1))
            tgt_nodes = list(range(1, len(stage_list)))
            values    = result["safety_stock"].tolist()[1:]
            fig_sk = go.Figure(go.Sankey(
                node=dict(
                    pad=15, thickness=20,
                    label=stage_list,
                    color=["#38bdf8"] * len(stage_list),
                ),
                link=dict(
                    source=src_nodes, target=tgt_nodes,
                    value=[max(0, v) for v in values],
                    color=["rgba(56,189,248,0.3)"] * len(values),
                ),
            ))
            fig_sk.update_layout(
                height=400, template="plotly",
                title="Safety Stock Flow Across Supply Network Stages",
            )
            st.plotly_chart(fig_sk, use_container_width=True)
        else:
            st.info("Need ≥ 2 stages for Sankey diagram.")

with tab3:
    st.subheader("📋 Stage-Level Detail")
    row_sel = st.dataframe(result, use_container_width=True, hide_index=True,
                           on_select="rerun", selection_mode="single-row", key="me_tbl")
    if row_sel and row_sel.get("selection",{}).get("rows"):
        idx = row_sel["selection"]["rows"][0]
        r = result.iloc[idx]
        st.divider()
        st.subheader(f"🔍 Stage Detail: `{r.get('stage_id','')}`")
        detail_cols = st.columns(min(4, len(r)))
        for i, (col_name, val) in enumerate(r.items()):
            detail_cols[i % 4].metric(str(col_name).replace("_"," ").title(), f"{val:,.2f}" if isinstance(val, float) else str(val))

