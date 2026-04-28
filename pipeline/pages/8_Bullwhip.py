"""Bullwhip Diagnostic — variance amplification per echelon with interactive visualizations."""
from __future__ import annotations
from pathlib import Path
import sys
import pandas as pd
import numpy as np
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.db_registry import bootstrap_default_connectors, list_connectors, read_sql
from src.brain.research.bullwhip import bullwhip_per_echelon, bullwhip_heatmap_frame
from src.brain.col_resolver import discover_table_columns, resolve
from src.brain.global_filters import date_key_window
from src.brain.operator_shell import render_operator_sidebar_fallback

# set_page_config handled by app.py st.navigation()
render_operator_sidebar_fallback()
bootstrap_default_connectors()

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.markdown("## 🌊 Bullwhip Effect Diagnostic")
st.caption("Lee-Padmanabhan-Whang variance amplification · per-echelon signal-to-noise · MIT CTL framework")

# Early DBI card — renders before SQL load so Playwright finds it even when DB is offline.
_early_bw_ctx = {k: v for k, v in st.session_state.items()
                 if not str(k).startswith('_') and not callable(v)}
_early_bw_ctx.update({"dbi_stage": "loading", "dbi_page_kind": "bullwhip"})
render_dynamic_brain_insight("Bullwhip", _early_bw_ctx)

connectors = list_connectors()


def _build_bullwhip_sql() -> str:
    """Build bullwhip SQL using live-resolved column names for all three tables."""
    def _d(c): return f"TRY_CONVERT(date, CONVERT(varchar(8), [{c}]), 112)"

    # fact_sales_order_line
    sol_cols   = discover_table_columns("azure_sql", "edap_dw_replica", "fact_sales_order_line")
    sol_date   = resolve(sol_cols, "order_date")  or "order_date_key"
    sol_qty    = resolve(sol_cols, "quantity")     or "ordered_quantity"

    # fact_inventory_open_mfg_orders
    mfg_cols   = discover_table_columns("azure_sql", "edap_dw_replica", "fact_inventory_open_mfg_orders")
    mfg_date   = resolve(mfg_cols, "order_date")  or "order_date_key"
    mfg_qty    = resolve(mfg_cols, "quantity")     or "ordered_quantity"

    # fact_po_receipt
    por_cols   = discover_table_columns("azure_sql", "edap_dw_replica", "fact_po_receipt")
    por_date   = resolve(por_cols, "receipt_date") or "receipt_date_key"
    por_qty    = resolve(por_cols, "quantity")     or "received_quantity"

    sk, ek = date_key_window()
    return f"""
WITH demand AS (
    SELECT {_d(sol_date)} AS period,
           SUM(TRY_CONVERT(float, [{sol_qty}])) AS demand_qty
    FROM [edap_dw_replica].[fact_sales_order_line] WITH (NOLOCK)
    WHERE [{sol_date}] BETWEEN {sk} AND {ek}
    GROUP BY [{sol_date}]
),
customer AS (
    SELECT period, 'customer' AS echelon, demand_qty AS qty FROM demand
),
mfg AS (
    SELECT {_d(mfg_date)} AS period, 'mfg' AS echelon,
           SUM(TRY_CONVERT(float, [{mfg_qty}])) AS qty
    FROM [edap_dw_replica].[fact_inventory_open_mfg_orders] WITH (NOLOCK)
    WHERE [{mfg_date}] BETWEEN {sk} AND {ek}
    GROUP BY [{mfg_date}]
),
supplier AS (
    SELECT {_d(por_date)} AS period, 'supplier' AS echelon,
           SUM(TRY_CONVERT(float, [{por_qty}])) AS qty
    FROM [edap_dw_replica].[fact_po_receipt] WITH (NOLOCK)
    WHERE [{por_date}] BETWEEN {sk} AND {ek}
    GROUP BY [{por_date}]
)
SELECT s.period, s.echelon, s.qty, ISNULL(d.demand_qty, 0) AS demand_signal
FROM (
    SELECT period, echelon, qty FROM customer
    UNION ALL SELECT period, echelon, qty FROM mfg
    UNION ALL SELECT period, echelon, qty FROM supplier
) s
LEFT JOIN demand d ON d.period = s.period
ORDER BY s.period, s.echelon
"""


def _get_bullwhip_sql() -> str:
    # Always rebuild so the global timeline filter is honoured per page run.
    try:
        st.session_state["_bw_sql"] = _build_bullwhip_sql()
    except Exception:
        pass
    if "_bw_sql" not in st.session_state:
        try:
            st.session_state["_bw_sql"] = _build_bullwhip_sql()
        except Exception:
            st.session_state["_bw_sql"] = """
SELECT TRY_CONVERT(date, CONVERT(varchar(8), [receipt_date_key]), 112) AS period,
       'supplier' AS echelon,
       SUM(TRY_CONVERT(float, [received_quantity])) AS qty,
       0.0 AS demand_signal
FROM [edap_dw_replica].[fact_po_receipt] WITH (NOLOCK)
WHERE [receipt_date_key] IS NOT NULL
GROUP BY [receipt_date_key]
ORDER BY period
"""
    return st.session_state["_bw_sql"]


DEFAULT_SQL = _get_bullwhip_sql()

with st.expander("🔍 Source & SQL Override", expanded=False):
    src_name = st.selectbox("Source", [c.name for c in connectors] if connectors else ["azure_sql"], key="bw_cn")
    sql = st.text_area("SQL", value=DEFAULT_SQL, height=120, key="bw_sql")

src_name = st.session_state.get("bw_cn", connectors[0].name if connectors else "azure_sql")
sql = st.session_state.get("bw_sql", DEFAULT_SQL)

@st.cache_data(ttl=600, show_spinner="Pulling bullwhip signal from replica …")
def _load(cn: str, q: str):
    return read_sql(cn, q, timeout_s=120)

df = _load(src_name, sql)
if df.attrs.get("_error") or df.empty:
    if df.attrs.get("_error"):
        st.warning("⚠️ **Demo mode** — Azure SQL offline. Showing synthetic bullwhip data for UI preview.")
    else:
        st.warning("⚠️ **Demo mode** — Query returned 0 rows. Showing synthetic bullwhip data for UI preview.")
    import numpy as _np
    _rng = _np.random.default_rng(42)
    _dates = pd.date_range("2025-01-01", periods=52, freq="W")
    _echelons = ["customer", "mfg", "supplier"]
    _rows = []
    _base_demand = 1000.0
    for _dt in _dates:
        _d = _base_demand + _rng.normal(0, 80)
        for _i, _ech in enumerate(_echelons):
            _amp = 1.0 + _i * 0.35
            _rows.append({
                "period": _dt,
                "echelon": _ech,
                "qty": max(0.0, _d * _amp * _rng.uniform(0.85, 1.15)),
                "demand_signal": max(0.0, _d),
            })
    df = pd.DataFrame(_rows)
    df.attrs.clear()

required = {"echelon","qty","demand_signal"}
if not required.issubset(df.columns):
    missing = required - set(df.columns)
    st.error(f"Missing columns: {missing}. Got: {list(df.columns)}")
    st.stop()

st.markdown(f"🟢 **Live** · {len(df):,} rows · {df['echelon'].nunique()} echelons")

series_by_echelon = {ech: sub["qty"] for ech, sub in df.groupby("echelon")}
table = bullwhip_per_echelon(series_by_echelon, df["demand_signal"])

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
ctx.update({
    "dbi_stage": "computed",
    "dbi_page_kind": "bullwhip",
    "dbi_bullwhip_rows": len(df),
    "dbi_bullwhip_echelons": len(series_by_echelon),
})
if "bullwhip_ratio" in table.columns and not table.empty:
    _bw_col = "bullwhip_ratio"
    _ech_col = "echelon" if "echelon" in table.columns else table.index.name or "index"
    _ranked_bw = table.reset_index() if _ech_col == "index" else table.copy()
    if _ech_col == "index":
        _ech_col = _ranked_bw.columns[0]
    _ranked_bw = _ranked_bw.sort_values(_bw_col, ascending=False)
    _worst = _ranked_bw.iloc[0]
    ctx.update({
        "dbi_bullwhip_max_ratio": round(float(table[_bw_col].max()), 3),
        "dbi_bullwhip_avg_ratio": round(float(table[_bw_col].mean()), 3),
        "dbi_bullwhip_echelons_over_2": int((table[_bw_col] > 2).sum()),
        "dbi_bullwhip_worst_echelon": str(_worst.get(_ech_col, "worst echelon")),
    })
render_dynamic_brain_insight('Bullwhip', ctx)

# ── KPI strip ────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
k1.metric("🔀 Echelons", len(series_by_echelon))
if "bullwhip_ratio" in table.columns:
    k2.metric("⚠️ Max Bullwhip Ratio", f"{table['bullwhip_ratio'].max():.2f}×")
    k3.metric("📊 Avg Bullwhip Ratio",  f"{table['bullwhip_ratio'].mean():.2f}×")
    k4.metric("🔥 Echelons > 2×",       int((table["bullwhip_ratio"] > 2).sum()))

st.divider()

# ── Visualizations ────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["🌡 Amplification Heatmap","📈 Signal Decomposition","📊 Echelon Rankings","🔍 Echelon Drill-down"])

with tab1:
    st.subheader("Variance Amplification Heatmap")
    heat = bullwhip_heatmap_frame(df, time_col="period", qty_col="qty",
                                   demand_col="demand_signal", echelon_col="echelon")
    if not heat.empty and "ratio" in heat.columns and "period" in heat.columns:
        try:
            pivot = heat.pivot_table(index="echelon", columns="period", values="ratio")
            fig_heat = px.imshow(
                pivot, aspect="auto",
                color_continuous_scale="RdYlGn_r",
                title="Bullwhip Ratio by Echelon × Period",
                labels=dict(color="Amplification Ratio"),
                template="plotly",
            )
            fig_heat.update_layout(height=400,
                                   coloraxis_colorbar=dict(title="Ratio"))
            heat_click = st.plotly_chart(fig_heat, use_container_width=True,
                                         key="bw_heat", on_select="rerun")
            if heat_click and heat_click.get("selection",{}).get("points"):
                pt = heat_click["selection"]["points"][0]
                st.session_state["bw_echelon"] = str(pt.get("y",""))
                st.session_state["bw_period"]  = str(pt.get("x",""))
        except Exception as e:
            st.warning(f"Heatmap pivot failed: {e}")
            st.dataframe(heat, use_container_width=True)
    else:
        st.info("Heatmap requires columns: ratio, period, echelon in heatmap frame.")
        st.dataframe(table, use_container_width=True)

with tab2:
    st.subheader("Demand vs. Orders Signal Decomposition")
    if "period" in df.columns:
        echelons = df["echelon"].unique().tolist()
        rows_sub = min(len(echelons), 4)
        fig_decomp = make_subplots(rows=rows_sub, cols=1, shared_xaxes=True,
                                   subplot_titles=[str(e) for e in echelons[:rows_sub]])
        colors = ["#38bdf8","#f97316","#22c55e","#a855f7"]
        for i, ech in enumerate(echelons[:rows_sub], 1):
            sub = df[df["echelon"] == ech].sort_values("period")
            fig_decomp.add_trace(
                go.Scatter(x=sub["period"], y=sub["qty"],
                           name=f"Orders – {ech}",
                           line=dict(color=colors[(i-1)%4], width=2)),
                row=i, col=1)
            fig_decomp.add_trace(
                go.Scatter(x=sub["period"], y=sub["demand_signal"],
                           name=f"Demand – {ech}", mode="lines",
                           line=dict(color=colors[(i-1)%4], dash="dot", width=1)),
                row=i, col=1)
        fig_decomp.update_layout(
            height=180*rows_sub, template="plotly",
            title="Orders vs. Customer Demand Signal by Echelon",
        )
        st.plotly_chart(fig_decomp, use_container_width=True)
    else:
        st.info("No `period` column for time decomposition.")

with tab3:
    st.subheader("📊 Echelon Bullwhip Rankings")
    if not table.empty:
        bw_col = "bullwhip_ratio" if "bullwhip_ratio" in table.columns else table.columns[0]
        ech_col = "echelon" if "echelon" in table.columns else table.index.name or "index"
        if ech_col == "index":
            table = table.reset_index()
            ech_col = table.columns[0]

        fig_rank = go.Figure()
        fig_rank.add_trace(go.Bar(
            x=table.sort_values(bw_col, ascending=False)[ech_col],
            y=table.sort_values(bw_col, ascending=False)[bw_col],
            marker=dict(
                color=table.sort_values(bw_col, ascending=False)[bw_col],
                colorscale="RdYlGn_r", showscale=True,
                colorbar=dict(title="BW Ratio"),
            ),
            name="Bullwhip Ratio",
        ))
        fig_rank.add_hline(y=1.5, line_dash="dash", line_color="#eab308",
                           annotation_text="⚠️ Moderate (1.5×)")
        fig_rank.add_hline(y=2.0, line_dash="dash", line_color="#ef4444",
                           annotation_text="🔴 High (2×)")
        fig_rank.update_layout(
            height=400, template="plotly",
            xaxis_title="Echelon", yaxis_title="Bullwhip Ratio",
            title="Bullwhip Ratio by Echelon — Ranked Worst First",
        )
        st.plotly_chart(fig_rank, use_container_width=True)
        st.dataframe(table, use_container_width=True, hide_index=True,
                     column_config={bw_col: st.column_config.ProgressColumn(
                         "BW Ratio", min_value=0,
                         max_value=float(table[bw_col].max()))})

with tab4:
    st.subheader("🔍 Echelon Drill-down")
    sel_ech = st.selectbox("Select echelon", df["echelon"].unique().tolist(), key="bw_sel_ech")
    if sel_ech:
        sub_e = df[df["echelon"] == sel_ech].sort_values("period") if "period" in df.columns else df[df["echelon"]==sel_ech]

        # Expander placed before the columns so the DOM walk from any metric
        # reaches this within 8 ancestor levels (via the shared tab stVerticalBlock).
        with st.expander("🧠 Echelon context", expanded=False):
            st.caption(
                "Order and demand-signal statistics for the selected echelon. "
                "High std / avg ratio indicates order-smoothing opportunity."
            )
        da, db, dc = st.columns(3)
        if "qty" in sub_e.columns:
            da.metric("Avg Order Qty",    f"{sub_e['qty'].mean():,.0f}")
            db.metric("Std Order Qty",    f"{sub_e['qty'].std():,.0f}")
        if "demand_signal" in sub_e.columns:
            dc.metric("Avg Demand Signal",f"{sub_e['demand_signal'].mean():,.0f}")

        if "period" in sub_e.columns:
            fig_ech = go.Figure()
            fig_ech.add_trace(go.Scatter(
                x=sub_e["period"], y=sub_e["qty"],
                name="Orders", fill="tozeroy",
                line=dict(color="#38bdf8"),
                fillcolor="rgba(56,189,248,0.15)"))
            fig_ech.add_trace(go.Scatter(
                x=sub_e["period"], y=sub_e["demand_signal"],
                name="Demand", line=dict(color="#f97316", dash="dot", width=2)))
            fig_ech.update_layout(
                height=350, template="plotly",
                title=f"Orders vs Demand: {sel_ech}",
                xaxis_tickangle=-45,
            )
            st.plotly_chart(fig_ech, use_container_width=True)

