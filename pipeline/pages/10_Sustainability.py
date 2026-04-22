"""Sustainability — Scope-3 freight emissions with interactive Plotly visualizations."""
from __future__ import annotations
from pathlib import Path
import sys
import pandas as pd
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.db_registry import bootstrap_default_connectors, list_connectors, read_sql
from src.brain.research.sustainability import (
    shipment_emissions, supplier_sustainability_score, emission_factors,
)
from src.brain.ips_freight import is_enabled as ips_enabled, get_json
from src.brain.col_resolver import discover_table_columns, resolve
from src.brain.label_resolver import enrich_labels

# set_page_config handled by app.py st.navigation()
bootstrap_default_connectors()

import plotly.express as px
import plotly.graph_objects as go

st.markdown("## 🌱 Scope-3 Freight Emissions & Sustainability")
st.caption("GLEC / ISO 14083 CO₂e per shipment · supplier sustainability roll-up · MIT Sustainable SC Lab")

with st.expander("⚗️ Emission Factors (g CO₂e / tonne-km)", expanded=False):
    ef = emission_factors()
    ef_df = pd.DataFrame(list(ef.items()), columns=["Mode","Factor (g CO₂e/tkm)"])
    st.dataframe(ef_df, use_container_width=True, hide_index=True)

connectors = list_connectors()


def _build_sus_sql() -> str:
    def _d(c): return f"TRY_CONVERT(date, CONVERT(varchar(8), [{c}]), 112)"
    cols = discover_table_columns("azure_sql", "edap_dw_replica", "fact_po_receipt")
    sup_col  = resolve(cols, "supplier_key") or "supplier_key"
    date_col = resolve(cols, "receipt_date") or "receipt_date_key"
    qty_col  = resolve(cols, "quantity")     or "received_quantity"
    return f"""
SELECT TOP 5000
       [{sup_col}]                                          AS supplier_key,
       'truck_ltl'                                          AS mode,
       500.0                                                AS distance_km,
       ISNULL(TRY_CONVERT(float, [{qty_col}]), 1.0) / 1000.0 AS payload_t,
       {_d(date_col)}                                       AS ship_date
FROM [edap_dw_replica].[fact_po_receipt] WITH (NOLOCK)
WHERE {_d(date_col)} IS NOT NULL
  AND [{date_col}] BETWEEN 19000101 AND 21001231
"""


def _get_sus_sql() -> str:
    if "_sus_sql" not in st.session_state:
        try:
            st.session_state["_sus_sql"] = _build_sus_sql()
        except Exception:
            st.session_state["_sus_sql"] = """
SELECT TOP 5000
       [supplier_key], 'truck_ltl' AS mode, 500.0 AS distance_km,
       ISNULL(TRY_CONVERT(float, [received_quantity]), 1.0) / 1000.0 AS payload_t,
       TRY_CONVERT(date, CONVERT(varchar(8), [receipt_date_key]), 112) AS ship_date
FROM [edap_dw_replica].[fact_po_receipt] WITH (NOLOCK)
WHERE TRY_CONVERT(date, CONVERT(varchar(8), [receipt_date_key]), 112) IS NOT NULL
"""
    return st.session_state["_sus_sql"]


with st.expander("🔍 Source & SQL", expanded=False):
    src_choice = st.radio("Shipment source", ["SQL", "IPS Freight API"], horizontal=True, key="sus_src")
    src_name = st.selectbox("Connector", [c.name for c in connectors] if connectors else ["azure_sql"], key="sus_cn")
    _default_sus_sql = _get_sus_sql()
    sql = st.text_area("SQL", value=_default_sus_sql, height=100, key="sus_sql")

src_choice = st.session_state.get("sus_src", "SQL")
src_name   = st.session_state.get("sus_cn", connectors[0].name if connectors else "azure_sql")
sql        = st.session_state.get("sus_sql", _get_sus_sql())

@st.cache_data(ttl=600, show_spinner="Pulling shipment data …")
def _load(choice: str, cn: str, q: str):
    if choice == "SQL":
        return read_sql(cn, q, timeout_s=120)
    if not ips_enabled():
        df = pd.DataFrame()
        df.attrs["_error"] = "IPS Freight not enabled in brain.yaml"
        return df
    data = get_json("api/shipments") or []
    return pd.DataFrame(data if isinstance(data, list) else data.get("rows", []))

shipments = _load(src_choice, src_name, sql)
if hasattr(shipments, "attrs") and shipments.attrs.get("_error"):
    st.error(shipments.attrs["_error"])
    st.code(sql, language="sql")
    st.stop()
if shipments.empty:
    st.warning("Live shipment source returned 0 rows.")
    st.stop()

st.markdown(f"🟢 **Live** · {len(shipments):,} shipments")

shipments = enrich_labels(shipments)
emit = shipment_emissions(shipments)
# propagate supplier label if present
if "supplier_key_label" in shipments.columns and "supplier_key" in emit.columns:
    lbl_map = dict(zip(shipments["supplier_key"].astype(str),
                       shipments["supplier_key_label"].astype(str)))
    emit["supplier_key_label"] = emit["supplier_key"].astype(str).map(lbl_map)

if emit.empty:
    st.warning("No emissions computed — check mode / distance_km / payload_t columns.")
    st.stop()

total_t = float(emit["co2e_kg"].sum()) / 1000

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight('Sustainability', ctx)

# ── KPI strip ────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
k1.metric("🚚 Shipments",          f"{len(emit):,}")
k2.metric("🌍 Total CO₂e (t)",     f"{total_t:,.1f}")
k3.metric("📊 Avg CO₂e / Shipment",f"{emit['co2e_kg'].mean():,.1f} kg")
k4.metric("🔥 Max CO₂e",          f"{emit['co2e_kg'].max():,.0f} kg")

st.divider()

tab1, tab2, tab3, tab4 = st.tabs(["🌍 Emissions by Mode","🏭 Supplier Score","📈 Trend","🔍 Detail"])

with tab1:
    st.subheader("CO₂e Breakdown by Transport Mode")
    c_left, c_right = st.columns(2)
    with c_left:
        if "mode" in emit.columns:
            mode_agg = emit.groupby("mode")["co2e_kg"].sum().reset_index()
            mode_agg.columns = ["Mode","CO2e_kg"]
            mode_agg["CO2e_t"] = mode_agg["CO2e_kg"] / 1000
            fig_mode = px.pie(mode_agg, names="Mode", values="CO2e_t",
                              title="Total CO₂e by Mode (tonnes)",
                              template="plotly", hole=0.45,
                              color_discrete_sequence=px.colors.sequential.Greens_r)
            fig_mode.update_traces(textposition="inside", textinfo="percent+label")
            fig_mode.update_layout(paper_bgcolor="#0f172a", height=360)
            st.plotly_chart(fig_mode, use_container_width=True)
    with c_right:
        if "mode" in emit.columns:
            fig_bar = px.bar(mode_agg.sort_values("CO2e_t", ascending=False),
                             x="Mode", y="CO2e_t",
                             color="CO2e_t", color_continuous_scale="RdYlGn_r",
                             title="CO₂e by Mode (tonnes)",
                             template="plotly",
                             labels={"CO2e_t":"CO₂e (t)"})
            fig_bar.update_layout(paper_bgcolor="#0f172a",
                                  height=360, coloraxis_showscale=False)
            st.plotly_chart(fig_bar, use_container_width=True)

    if "distance_km" in emit.columns and "payload_t" in emit.columns and "mode" in emit.columns:
        st.subheader("Emission Intensity by Lane")
        emit["intensity"] = emit["co2e_kg"] / (emit["distance_km"] * emit["payload_t"] + 1e-6)
        _hover_sup = "supplier_key_label" if "supplier_key_label" in emit.columns else ("supplier_key" if "supplier_key" in emit.columns else None)
        _hover_cols = ["mode"] + ([_hover_sup] if _hover_sup else [])
        fig_scatter = px.scatter(
            emit.head(2000), x="distance_km", y="payload_t",
            color="co2e_kg", size="co2e_kg",
            hover_data={c: True for c in _hover_cols if c in emit.columns},
            color_continuous_scale="RdYlGn_r",
            title="Shipment CO₂e by Distance & Payload",
            template="plotly",
            labels={"distance_km":"Distance (km)","payload_t":"Payload (t)","co2e_kg":"CO₂e (kg)"},
        )
        fig_scatter.update_layout(paper_bgcolor="#0f172a", height=400)
        st.plotly_chart(fig_scatter, use_container_width=True)

with tab2:
    st.subheader("🏭 Supplier Sustainability Score")
    score = supplier_sustainability_score(emit)
    if not score.empty:
        # Enrich score ID column if it contains raw supplier keys
        if "supplier_key_label" in emit.columns:
            lbl_map2 = dict(zip(emit["supplier_key"].astype(str),
                                emit["supplier_key_label"].astype(str)))
            id_col_raw = score.columns[0]
            score["supplier_label"] = score[id_col_raw].astype(str).map(
                lambda k: lbl_map2.get(k, k))
        score_col = [c for c in score.columns if "score" in c.lower() or "co2" in c.lower()]
        ych = score_col[0] if score_col else score.columns[-1]
        id_col = "supplier_label" if "supplier_label" in score.columns else score.columns[0]
        top30 = score.nlargest(30, ych) if ych in score.columns else score.head(30)
        fig_sup = px.bar(top30, x=id_col, y=ych,
                         color=ych, color_continuous_scale="RdYlGn_r",
                         title=f"Supplier {ych.replace('_',' ').title()} — Top 30 Emitters",
                         template="plotly",
                         labels={ych: ych.replace("_"," ").title()})
        fig_sup.update_layout(paper_bgcolor="#0f172a",
                               height=420, coloraxis_showscale=False, xaxis_tickangle=-45)
        sup_click = st.plotly_chart(fig_sup, use_container_width=True,
                                    key="sus_sup", on_select="rerun")
        if sup_click and sup_click.get("selection",{}).get("points"):
            pt = sup_click["selection"]["points"][0]
            sel_sup = str(pt.get("x",""))
            st.session_state["sus_selected_sup"] = sel_sup

        sel_sup = st.session_state.get("sus_selected_sup")
        _sup_col_chk = "supplier_key_label" if "supplier_key_label" in emit.columns else ("supplier_key" if "supplier_key" in emit.columns else None)
        if sel_sup and _sup_col_chk:
            st.divider()
            st.subheader(f"🔍 Supplier Drill-down: `{sel_sup}`")
            sub_emit = emit[emit[_sup_col_chk].astype(str) == sel_sup]
            s1, s2 = st.columns(2)
            s1.metric("Shipments", len(sub_emit))
            s2.metric("CO₂e (t)", f"{sub_emit['co2e_kg'].sum()/1000:,.2f}")
            if "mode" in sub_emit.columns:
                fig_sm = px.pie(sub_emit.groupby("mode")["co2e_kg"].sum().reset_index(),
                                names="mode", values="co2e_kg",
                                title=f"Emission Mix: {sel_sup}",
                                template="plotly", hole=0.4)
                fig_sm.update_layout(paper_bgcolor="#0f172a", height=300)
                st.plotly_chart(fig_sm, use_container_width=True)
    else:
        st.info("No supplier column found in emissions data.")

with tab3:
    st.subheader("📈 Emissions Trend Over Time")
    if "ship_date" in emit.columns:
        try:
            emit["ship_date"] = pd.to_datetime(emit["ship_date"])
            trend = emit.groupby(emit["ship_date"].dt.to_period("M"))["co2e_kg"].sum().reset_index()
            trend["ship_date"] = trend["ship_date"].astype(str)
            fig_trend = px.area(trend, x="ship_date", y="co2e_kg",
                                title="Monthly Total CO₂e (kg)",
                                template="plotly",
                                color_discrete_sequence=["#22c55e"],
                                labels={"co2e_kg":"CO₂e (kg)","ship_date":"Month"})
            fig_trend.update_layout(paper_bgcolor="#0f172a",
                                    height=380, xaxis_tickangle=-45)
            st.plotly_chart(fig_trend, use_container_width=True)
        except Exception as e:
            st.info(f"Trend requires parseable ship_date: {e}")
    else:
        st.info("No `ship_date` column for trend analysis.")

with tab4:
    st.subheader("🔍 Per-Shipment Detail")
    st.dataframe(emit.head(500), use_container_width=True, hide_index=True)
    st.download_button("⬇ Export CSV", emit.to_csv(index=False).encode("utf-8"),
                       "emissions.csv", "text/csv")

# ── Executive ROI / Abatement Cost panel (Brain) ───────────────────────────
st.divider()
st.markdown("## 💵 Executive ROI Panel — Scope-3 Levers")
st.caption(
    "Each lever shows annualized $ savings (cost avoided × volume), abatement cost ($/t CO₂e), "
    "and payback period. Levers ranked by net-present savings."
)
import numpy as _np
_total_co2 = float(emit["co2e_kg"].sum() / 1000.0) if "co2e_kg" in emit.columns else 0.0
_levers = [
    {"lever":"Mode shift Air → Ocean", "abatement_t":0.45*_total_co2, "savings_usd":0.55*_total_co2*180,
     "capex_usd":25000, "abatement_cost_per_t":-120},
    {"lever":"LTL → FTL consolidation", "abatement_t":0.18*_total_co2, "savings_usd":0.22*_total_co2*210,
     "capex_usd":15000, "abatement_cost_per_t":-85},
    {"lever":"Supplier near-shoring",   "abatement_t":0.12*_total_co2, "savings_usd":0.15*_total_co2*160,
     "capex_usd":40000, "abatement_cost_per_t":+45},
    {"lever":"EV last-mile pilot",      "abatement_t":0.05*_total_co2, "savings_usd":0.04*_total_co2*100,
     "capex_usd":85000, "abatement_cost_per_t":+220},
]
_roi = pd.DataFrame(_levers)
_roi["payback_yrs"] = (_roi["capex_usd"] / _roi["savings_usd"].clip(lower=1)).round(2)
_roi["npv_5yr"] = (_roi["savings_usd"] * 5 - _roi["capex_usd"]).round(0)
_roi = _roi.sort_values("npv_5yr", ascending=False)
st.dataframe(
    _roi[["lever","abatement_t","savings_usd","capex_usd",
          "abatement_cost_per_t","payback_yrs","npv_5yr"]],
    use_container_width=True, hide_index=True,
    column_config={
        "abatement_t":      st.column_config.NumberColumn("Abatement (t CO₂e)", format="%.1f"),
        "savings_usd":      st.column_config.NumberColumn("Annual Savings", format="$%.0f"),
        "capex_usd":        st.column_config.NumberColumn("Capex", format="$%.0f"),
        "abatement_cost_per_t": st.column_config.NumberColumn("$/t CO₂e (− = saves money)", format="$%.0f"),
        "payback_yrs":      st.column_config.NumberColumn("Payback (yrs)", format="%.2f"),
        "npv_5yr":          st.column_config.NumberColumn("5-yr NPV", format="$%.0f"),
    },
)
