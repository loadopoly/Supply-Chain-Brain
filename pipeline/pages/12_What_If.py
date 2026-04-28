"""What-If Sandbox — clone, mutate, recompute KPI suite, diff vs baseline — best-in-class Plotly."""
from __future__ import annotations
from pathlib import Path
import sys
import json
import pandas as pd
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.operator_shell import render_operator_sidebar_fallback
from src.brain.db_registry import bootstrap_default_connectors, list_connectors, read_sql
from src.brain.whatif import (
    create_snapshot, list_snapshots, apply_mutation_to_dataframe, diff_kpi,
)
from src.brain.eoq import deviation_table, EOQInputs
from src.brain.findings_index import log_decision
from src.brain.col_resolver import discover_table_columns, resolve

# set_page_config handled by app.py st.navigation()
render_operator_sidebar_fallback()
bootstrap_default_connectors()

import plotly.express as px
import plotly.graph_objects as go

st.markdown("## 🧪 What-If Sandbox")
st.caption("Clone live state · mutate scenario · replay KPIs · diff vs baseline · MIT SC Design Lab")

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight('What If', ctx)
st.divider()


tab_snap, tab_mut, tab_diff = st.tabs(["📸 Snapshots","⚙️ Mutation","📊 KPI Diff"])

with tab_snap:
    st.subheader("📸 Scenario Snapshots")
    snaps = list_snapshots()
    if snaps:
        snap_df = pd.DataFrame([{
            "Name":      s.name,
            "Created":   s.metadata.get("created",""),
            "Mutations": json.dumps(s.metadata.get("mutations",{})),
        } for s in snaps])
        st.dataframe(snap_df, use_container_width=True, hide_index=True)
    else:
        st.info("No snapshots yet — take one below.")

    new_name = st.text_input("Snapshot name", value="scenario_1")
    if st.button("📸 Take snapshot from current findings", type="primary"):
        snap = create_snapshot(new_name, mutations={})
        st.success(f"✅ Snapshot saved: `{snap.db_path}`")

with tab_mut:
    st.subheader("⚙️ Mutation Parameters")

    def _build_wi_sql() -> str:
        sol = discover_table_columns("azure_sql", "edap_dw_replica", "fact_sales_order_line")
        por = discover_table_columns("azure_sql", "edap_dw_replica", "fact_po_receipt")
        s_part   = resolve(sol, "part_key")      or "part_key"
        s_qty    = resolve(sol, "quantity")       or "qty"
        r_sup    = resolve(por, "supplier_key")   or "supplier_key"
        r_part   = resolve(por, "part_key")       or "part_key"
        r_price  = resolve(por, "unit_cost")      or "unit_price"
        return (
            f"SELECT TOP 2000\n"
            f"       CAST(s.[{s_part}] AS varchar(64))       AS part_id,\n"
            f"       SUM(TRY_CONVERT(float, s.[{s_qty}]))    AS demand_hat_annual,\n"
            f"       MAX(CAST(r.[{r_sup}] AS varchar(64)))   AS supplier_key,\n"
            f"       AVG(TRY_CONVERT(float, r.[{r_price}]))  AS unit_cost\n"
            f"FROM [edap_dw_replica].[fact_sales_order_line] s\n"
            f"LEFT JOIN [edap_dw_replica].[fact_po_receipt] r ON r.[{r_part}] = s.[{s_part}]\n"
            f"GROUP BY s.[{s_part}]"
        )

    DEFAULT_SQL = _build_wi_sql()
    with st.expander("🔍 Source", expanded=False):
        src = st.selectbox("Connector", [c.name for c in list_connectors()], key="wi_cn")
        sql = st.text_area("SQL (returns part_id, demand_hat_annual, supplier_key, unit_cost)",
                           value=DEFAULT_SQL, height=100, key="wi_sql")

    kind = st.selectbox("Mutation type", ["scale_demand","override_lead_time","consolidate_supplier"],
                        key="wi_kind")
    mut: dict = {"kind": kind}

    m1, m2 = st.columns(2)
    if kind == "scale_demand":
        mut["factor"] = m1.slider("Demand × factor", 0.5, 2.0, 1.10, 0.05, key="wi_factor")
        m2.markdown(f"**Effect:** Multiply all demand by `{mut['factor']}×`")
    elif kind == "override_lead_time":
        mut["value"] = m1.number_input("Target lead time (days)", 1, 365, 14, key="wi_lt")
        m2.markdown(f"**Effect:** Set all lead times to `{mut['value']}d`")
    else:
        mut["from"] = m1.text_input("Consolidate FROM supplier_key", key="wi_from")
        mut["to"]   = m2.text_input("Consolidate INTO supplier_key", key="wi_to")

    src_name = st.session_state.get("wi_cn", "azure_sql")
    sql_val  = st.session_state.get("wi_sql", DEFAULT_SQL)

    @st.cache_data(ttl=600, show_spinner="Pulling part demand from replica …")
    def _load_wi(cn_: str, q: str):
        return read_sql(cn_, q)

    if st.button("▶️ Run Scenario", type="primary"):
        df = _load_wi(src_name, sql_val)
        if df.attrs.get("_error"):
            st.error(df.attrs["_error"])
            st.code(sql_val, language="sql")
        elif df.empty:
            st.warning("0 rows returned from replica.")
        else:
            st.session_state["wi_base_df"]  = df
            st.session_state["wi_mut"]      = mut
            st.session_state["wi_scen_df"]  = apply_mutation_to_dataframe(df, mut)
            st.success(f"✅ {len(df):,} parts loaded · scenario computed")

with tab_diff:
    st.subheader("📊 KPI Comparison: Baseline vs Scenario")

    if "wi_base_df" not in st.session_state:
        st.info("Run a mutation scenario in the ⚙️ Mutation tab first.")
        st.stop()

    base_df = st.session_state["wi_base_df"]
    scen_df = st.session_state["wi_scen_df"]
    mut_used = st.session_state.get("wi_mut", {})

    st.caption(f"Mutation applied: `{json.dumps(mut_used)}`")

    # Compute KPIs
    def _kpis(d: pd.DataFrame) -> dict:
        try:
            dev = deviation_table(d.assign(
                qty_on_hand_plus_open=0,
                annual_demand=d.get("demand_hat_annual", pd.Series(0, index=d.index))
            ), EOQInputs())
            return {
                "total_dollar_at_risk": float(dev.get("dollar_at_risk", pd.Series(dtype=float)).sum()),
                "median_abs_dev_z":     float(dev.get("abs_dev_z",    pd.Series(dtype=float)).median() or 0),
                "n_parts":              int(len(dev)),
            }
        except Exception:
            return {"total_dollar_at_risk": 0.0, "median_abs_dev_z": 0.0, "n_parts": len(d)}

    base_kpi = _kpis(base_df)
    scen_kpi = _kpis(scen_df)

    diff = diff_kpi(base_kpi, scen_kpi)

    # KPI cards
    n_kpis = len(base_kpi)
    cols = st.columns(n_kpis)
    for i, (kn, bv) in enumerate(base_kpi.items()):
        sv = scen_kpi.get(kn, bv)
        delta = sv - bv
        pct   = (delta / max(abs(bv), 1e-6)) * 100
        cols[i].metric(
            kn.replace("_"," ").title(),
            f"{sv:,.1f}",
            delta=f"{delta:+,.1f} ({pct:+.1f}%)",
            delta_color="inverse" if "risk" in kn else "normal",
        )

    # Waterfall chart
    kpi_names = list(base_kpi.keys())
    base_vals = [base_kpi[k] for k in kpi_names]
    scen_vals = [scen_kpi[k] for k in kpi_names]
    changes   = [s - b for s, b in zip(scen_vals, base_vals)]

    fig_wf = go.Figure(go.Waterfall(
        x=kpi_names, y=changes,
        measure=["relative"] * len(kpi_names),
        decreasing=dict(marker_color="#22c55e"),
        increasing=dict(marker_color="#ef4444"),
        connector=dict(line=dict(color="rgba(100,116,139,0.5)")),
        text=[f"{v:+,.1f}" for v in changes], textposition="outside",
    ))
    fig_wf.update_layout(height=380, template="plotly",
                          title="KPI Change: Scenario vs Baseline (green = improvement)",
                          xaxis_title="KPI", yaxis_title="Δ Value")
    st.plotly_chart(fig_wf, use_container_width=True)

    # Side-by-side comparison bars
    compare_df = pd.DataFrame({
        "KPI": kpi_names * 2,
        "Value": base_vals + scen_vals,
        "Scenario": ["Baseline"] * len(kpi_names) + ["Scenario"] * len(kpi_names),
    })
    fig_cmp = px.bar(compare_df, x="KPI", y="Value", color="Scenario",
                      barmode="group", title="Baseline vs Scenario KPI Comparison",
                      template="plotly",
                      color_discrete_map={"Baseline":"#38bdf8","Scenario":"#f97316"})
    fig_cmp.update_layout(height=350)
    st.plotly_chart(fig_cmp, use_container_width=True)

    # Full diff table
    st.subheader("Full KPI Diff Table")
    st.dataframe(diff, use_container_width=True, hide_index=True)

    log_decision(page="whatif", action=mut_used.get("kind","?"), inputs=mut_used,
                 model="deviation_table_replay", confidence=None)
