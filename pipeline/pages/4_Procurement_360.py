"""Page 4 — Procurement 360: lead time, DIO, planned obsolescence, shared vendors, leverage points."""
from pathlib import Path
import sys
import streamlit as st
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.db_registry import bootstrap_default_connectors
from src.brain.data_access import fetch_logical, query_df
from src.brain.graph_context import GraphContext, HAS_NX
from src.brain.cleaning import safe_div
from src.brain.findings_index import record_findings_bulk
from src.brain.col_resolver import resolve
from src.brain.label_resolver import enrich_labels, get_supplier_labels, get_part_labels
from src.brain.global_filters import date_key_window
from src.brain.dynamic_insight import render_dynamic_brain_insight

# set_page_config handled by app.py st.navigation()
st.session_state["_page"] = "procurement_360"
bootstrap_default_connectors()

import plotly.express as px
import plotly.graph_objects as go
import networkx as nx

st.markdown("## 🏭 Procurement 360")
st.caption("Lead Time · DIO · Planned Obsolescence · Shared Vendors · Graph Leverage · CVaR Risk · MIT CTL framework")

# ── Pull core frames ─────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="Loading procurement frames …")
def _load(site: str, sk: int, ek: int):
    w_parts = f"business_unit_id = '{site}'" if site else None
    w_other = f"business_unit_key IN (SELECT business_unit_key FROM edap_dw_replica.dim_business_unit WITH (NOLOCK) WHERE business_unit_id = '{site}')" if site else None
    # Apply global timeline window to receipts (yyyymmdd integer date_key).
    w_recv  = (w_other + " AND " if w_other else "") + \
              f"receipt_date_key BETWEEN {sk} AND {ek}"

    parts     = fetch_logical("azure_sql", "parts",            top=5000,  where=w_parts)
    recv      = fetch_logical("azure_sql", "po_receipts",      top=10000, where=w_recv)
    on_hand   = fetch_logical("azure_sql", "on_hand",          top=10000, where=w_other)
    cost      = fetch_logical("azure_sql", "part_cost",        top=10000, where=None)   # global or w_other?
    contract  = fetch_logical("azure_sql", "po_contract_part", top=5000,  where=w_other)
    suppliers = fetch_logical("azure_sql", "suppliers",       top=20000, where=None)
    return parts, recv, on_hand, cost, contract, suppliers

site = st.session_state.get("g_site", "")
_sk, _ek = date_key_window()
try:
    parts, recv, on_hand, cost, contract, suppliers = _load(site, _sk, _ek)
except Exception:
    # If receipt_date_key isn't present in the logical mapping, retry without filter.
    @st.cache_data(ttl=300, show_spinner="Loading procurement frames (no date filter) …")
    def _load_nodate(site: str):
        w_parts = f"business_unit_id = '{site}'" if site else None
        w_other = f"business_unit_key IN (SELECT business_unit_key FROM edap_dw_replica.dim_business_unit WITH (NOLOCK) WHERE business_unit_id = '{site}')" if site else None
        return (
            fetch_logical("azure_sql", "parts",            top=5000,  where=w_parts),
            fetch_logical("azure_sql", "po_receipts",      top=10000, where=w_other),
            fetch_logical("azure_sql", "on_hand",          top=10000, where=w_other),
            fetch_logical("azure_sql", "part_cost",        top=10000, where=None),
            fetch_logical("azure_sql", "po_contract_part", top=5000,  where=w_other),
            fetch_logical("azure_sql", "suppliers",       top=20000, where=None),
        )
    parts, recv, on_hand, cost, contract, suppliers = _load_nodate(site)

any_data = not all(f.empty for f in [parts, recv, on_hand])
if not any_data:
    st.error("No data loaded from replica. Check connectors in ⚙️ Connectors page.")
    st.stop()

# ── Enrich all frames with human-readable labels ─────────────────────────────
with st.spinner("Resolving supplier / part names …"):
    recv     = enrich_labels(recv)
    on_hand  = enrich_labels(on_hand)
    parts    = enrich_labels(parts)

# ── Label lookups (for chart axis labelling and tooltips) ─────────────────────
supplier_names = get_supplier_labels()
part_names     = get_part_labels()

# ── Adaptive column resolution from actual DataFrames ─────────────────────────
# Resolves semantic role → actual column name using col_resolver's pattern library.
# This replaces all hardcoded column name assumptions.
def _rc(df: pd.DataFrame, semantic: str) -> str | None:
    """Resolve semantic name against actual df columns."""
    return resolve(list(df.columns), semantic) if not df.empty else None

# po_receipts resolved columns
_recv_part_col     = _rc(recv, "part_key")      or "part_key"
_recv_sup_col      = _rc(recv, "supplier_key")  or "supplier_key"
_recv_promise_col  = _rc(recv, "promise_date")
_recv_receipt_col  = _rc(recv, "receipt_date")
_recv_qty_col      = _rc(recv, "quantity")
_recv_lt_col       = _rc(recv, "lead_time_days")

# on_hand resolved columns
_oh_part_col       = _rc(on_hand, "part_key")   or "part_key"
_oh_qty_col        = _rc(on_hand, "on_hand_qty")

# Normalize: make sure we use columns that exist in the dataframes
def _col_or_none(df: pd.DataFrame, col: str | None) -> str | None:
    return col if (col and col in df.columns) else None

_recv_promise_col = _col_or_none(recv, _recv_promise_col)
_recv_receipt_col = _col_or_none(recv, _recv_receipt_col)
_recv_qty_col     = _col_or_none(recv, _recv_qty_col)
_recv_lt_col      = _col_or_none(recv, _recv_lt_col)
_oh_qty_col       = _col_or_none(on_hand, _oh_qty_col)

def _label_supplier(k):
    s = str(k); return supplier_names.get(s, s)
def _label_part(k):
    s = str(k); return part_names.get(s, s)

def _as_date_series(series: pd.Series) -> pd.Series:
    raw = pd.to_numeric(series, errors="coerce")
    ymd = raw.where(raw.between(19700101, 20991231)).astype("Int64").astype("string")
    parsed_ymd = pd.to_datetime(ymd, format="%Y%m%d", errors="coerce")
    parsed_native = pd.to_datetime(series, errors="coerce")
    return parsed_ymd.fillna(parsed_native)

# Prefer *_label columns already added by enrich_labels
_recv_sup_display  = "supplier_key_label"  if "supplier_key_label"  in recv.columns else _recv_sup_col
_recv_part_display = "part_key_label"       if "part_key_label"       in recv.columns else _recv_part_col

# Attach human labels onto recv for charts (if not already done by enrich_labels)
if not recv.empty and "supplier_key" in recv.columns and "supplier_key_label" not in recv.columns:
    recv = recv.copy()
    recv["supplier_key_label"] = recv["supplier_key"].astype(str).map(_label_supplier)
if not recv.empty and "part_key" in recv.columns and "part_key_label" not in recv.columns:
    recv = recv.copy()
    recv["part_key_label"] = recv["part_key"].astype(str).map(_label_part)

st.markdown(f"🟢 **Live** · parts:{len(parts):,} · receipts:{len(recv):,} · "
            f"on-hand:{len(on_hand):,} · suppliers:{len(suppliers):,}")

# ── Build lead time ────────────────────────────────────────────────────────────
if not recv.empty and _recv_promise_col and _recv_receipt_col:
    recv = recv.copy()
    _lt_days = (
        _as_date_series(recv[_recv_receipt_col]) -
        _as_date_series(recv[_recv_promise_col])
    ).dt.days
    recv["lead_time_days"] = _lt_days.where(_lt_days.between(0, 730))
elif not recv.empty and _recv_lt_col:
    recv = recv.copy()
    _lt_days = pd.to_numeric(recv[_recv_lt_col], errors="coerce")
    recv["lead_time_days"] = _lt_days.where(_lt_days.between(0, 730))

# ── KPI Strip ───────────────────────────────────────────────────────────────
_sup_nunique = recv[_recv_sup_col].nunique() if _recv_sup_col in recv.columns else "—"
_lt_mean_value = recv["lead_time_days"].dropna().mean() if "lead_time_days" in recv.columns else np.nan
_lt_mean = f"{_lt_mean_value:.0f}d" if pd.notna(_lt_mean_value) else "—"
k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    st.metric("📦 Parts", f"{len(parts):,}")
    with st.expander("🟢 Brain · Parts", expanded=False):
        st.markdown(f"**Population:** `{len(parts):,}` active part masters in scope.")
with k2:
    st.metric("📋 PO Receipts", f"{len(recv):,}")
    with st.expander("🟢 Brain · PO Receipts", expanded=False):
        st.markdown(f"**Volume:** `{len(recv):,}` PO receipt events — supply-side relationship source.")
with k3:
    st.metric("🏭 Suppliers", str(_sup_nunique))
    with st.expander("🟢 Brain · Suppliers", expanded=False):
        st.markdown(f"**Diversity:** `{_sup_nunique}` unique suppliers identified across receipts.")
with k4:
    st.metric("⏱ Avg Lead Time", _lt_mean)
    with st.expander("🟢 Brain · Lead Time", expanded=False):
        st.markdown(f"**Mean lead time:** `{_lt_mean}` — baseline for OTD and CVaR analysis.")
with k5:
    st.metric("📊 On-hand Rows", f"{len(on_hand):,}")
    with st.expander("🟢 Brain · On-hand", expanded=False):
        st.markdown(f"**Inventory snapshot:** `{len(on_hand):,}` on-hand quantity rows loaded.")

st.divider()

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "⏱ Lead Time","📦 DIO","🪦 Obsolescence","🕸 Leverage","⚖ CVaR Risk","🧠 Causal"
])

with tab1:
    st.subheader("⏱ Lead Time Analysis")
    ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
    render_dynamic_brain_insight("Lead Time Analysis", ctx)

    if not recv.empty and "lead_time_days" in recv.columns:
        sup_key = _recv_sup_display if _recv_sup_display in recv.columns else (
                  _recv_sup_col if _recv_sup_col in recv.columns else None)
        prt_key = _recv_part_display if _recv_part_display in recv.columns else (
                  _recv_part_col if _recv_part_col in recv.columns else None)
        group_cols = [c for c in (sup_key, prt_key) if c]
        summary = (recv.groupby(group_cols)["lead_time_days"]
                   .agg(receipts="count", lt_mean="mean", lt_std="std", lt_p50="median",
                        lt_p95=lambda x: x.quantile(0.95))
                   .reset_index()
                   .sort_values("lt_mean", ascending=False))

        # Heat/bar split
        c_l, c_r = st.columns([3,2])
        with c_l:
            top30 = summary.head(30)
            xc = group_cols[0] if group_cols else summary.columns[0]
            fig_lt = px.bar(top30, x=xc, y=["lt_mean","lt_p95"], barmode="group",
                            title="Avg vs P95 Lead Time — Top 30 Worst",
                            template="plotly",
                            color_discrete_map={"lt_mean":"#38bdf8","lt_p95":"#ef4444"},
                            labels={"value":"Days","variable":"Metric"})
            fig_lt.update_layout(paper_bgcolor="#0f172a",
                                  height=400, xaxis_tickangle=-45)
            lt_click = st.plotly_chart(fig_lt, use_container_width=True,
                                        key="lt_bar", on_select="rerun")
            if lt_click and lt_click.get("selection",{}).get("points"):
                pt = lt_click["selection"]["points"][0]
                st.session_state["proc_supplier"] = str(pt.get("x",""))
        with c_r:
            if "lead_time_days" in recv.columns:
                fig_dist = px.histogram(recv.dropna(subset=["lead_time_days"]),
                                         x="lead_time_days", nbins=50,
                                         color_discrete_sequence=["#38bdf8"],
                                         title="Lead-Time Distribution (All Receipts)",
                                         template="plotly",
                                         labels={"lead_time_days":"Days"})
                fig_dist.add_vline(x=recv["lead_time_days"].median(), line_dash="dash",
                                    line_color="#22c55e", annotation_text="P50")
                fig_dist.update_layout(paper_bgcolor="#0f172a", height=400)
                st.plotly_chart(fig_dist, use_container_width=True)

        # Drill-down on selected supplier
        sel_sup = st.session_state.get("proc_supplier")
        if sel_sup and _recv_sup_col in recv.columns:
            st.divider()
            st.subheader(f"🔍 Supplier Drill-down: `{sel_sup}`")
            ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
            render_dynamic_brain_insight("f Supplier Drill-down sel_sup", ctx)
            sub_s = recv[recv[_recv_sup_col].astype(str) == sel_sup]
            d1, d2, d3 = st.columns(3)
            d1.metric("Receipts",   len(sub_s))
            d2.metric("Avg LT",     f"{sub_s['lead_time_days'].mean():.0f}d")
            d3.metric("P95 LT",     f"{sub_s['lead_time_days'].quantile(0.95):.0f}d")
            if _recv_part_col in sub_s.columns:
                fig_sp = px.box(sub_s, x=_recv_part_col, y="lead_time_days",
                                 title=f"LT by Part: {sel_sup}",
                                 template="plotly", points="outliers")
                fig_sp.update_layout(paper_bgcolor="#0f172a",
                                      height=320, xaxis_tickangle=-45)
                st.plotly_chart(fig_sp, use_container_width=True)
    else:
        if not (_recv_promise_col and _recv_receipt_col):
            found = ", ".join(c for c in [_recv_promise_col, _recv_receipt_col] if c)
            st.warning(
                f"Lead time dates not resolved from po_receipts columns. "
                f"Found: [{found or 'none'}]. Visit **Schema Discovery** page to see all available columns."
            )
        else:
            st.info("No lead_time_days — dates resolved but no data returned.")

with tab2:
    st.subheader("📦 Days of Inventory Outstanding (DIO)")
    ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
    render_dynamic_brain_insight("Days of Inventory Outstanding DIO", ctx)
    if on_hand.empty:
        st.info("No on-hand inventory rows returned for the selected plant/window. DIO needs a live inventory snapshot plus receipt usage.")
    elif not recv.empty and _oh_part_col in on_hand.columns:
        avg_oh  = on_hand.groupby(_oh_part_col)[_oh_qty_col].mean().reset_index() \
                  if _oh_qty_col else pd.DataFrame()
        recv_qty_col = _recv_qty_col or (_col_or_none(recv, _rc(recv, "quantity")))
        avg_use = recv.groupby(_recv_part_col)[recv_qty_col].sum().reset_index() \
                  if recv_qty_col else pd.DataFrame()
        if not avg_oh.empty and not avg_use.empty:
            merged = avg_oh.merge(avg_use, on=_oh_part_col, how="left").fillna(0)
            oh_q = _oh_qty_col or avg_oh.columns[-1]
            use_q = recv_qty_col or avg_use.columns[-1]
            merged["dio_days"] = safe_div(merged[oh_q] * 365.0, merged[use_q])
            merged = merged.replace([np.inf,-np.inf], np.nan).dropna(subset=["dio_days"])
            merged = merged.sort_values("dio_days", ascending=False)
            # add human label
            if "part_key_label" in merged.columns:
                x_col_dio = "part_key_label"
            else:
                merged["part_key_label"] = merged[_oh_part_col].astype(str).map(_label_part)
                x_col_dio = "part_key_label"

            # Gauge-style metric
            d1, d2, d3 = st.columns(3)
            d1.metric("Avg DIO (days)",  f"{merged['dio_days'].mean():.0f}")
            d2.metric("Max DIO (days)",  f"{merged['dio_days'].max():.0f}")
            d3.metric("Parts > 90d DIO", int((merged["dio_days"] > 90).sum()))

            fig_dio = px.bar(merged.head(40), x=x_col_dio, y="dio_days",
                              color="dio_days", color_continuous_scale="RdYlGn_r",
                              title="Days of Inventory Outstanding — Top 40 Parts",
                              template="plotly",
                              labels={"dio_days":"DIO (days)","part_key_label":"Part"})
            fig_dio.add_hline(y=90, line_dash="dash", line_color="#eab308",
                               annotation_text="⚠️ 90d threshold")
            fig_dio.update_layout(paper_bgcolor="#0f172a",
                                   height=420, xaxis_tickangle=-45, coloraxis_showscale=False)
            dio_click = st.plotly_chart(fig_dio, use_container_width=True,
                                         key="dio_bar", on_select="rerun")
            if dio_click and dio_click.get("selection",{}).get("points"):
                pt = dio_click["selection"]["points"][0]
                st.session_state["proc_part"] = str(pt.get("x",""))

            st.dataframe(merged.head(200), use_container_width=True, hide_index=True,
                          column_config={"dio_days": st.column_config.ProgressColumn(
                              "DIO Days", min_value=0, max_value=float(merged["dio_days"].max()))})
        else:
            missing = []
            if not _oh_qty_col: missing.append(f"on_hand qty in on_hand (cols: {list(on_hand.columns)[:8]})")
            if not recv_qty_col: missing.append(f"qty received in po_receipts (cols: {list(recv.columns)[:8]})")
            st.warning(f"Cannot compute DIO — unresolved: {'; '.join(missing) or 'unknown'}. Visit **Schema Discovery** for column details.")
    else:
        st.info(f"on_hand part key column not resolved. on_hand columns: {list(on_hand.columns)[:10]}")

with tab3:
    st.subheader("🪦 Planned Obsolescence Candidates")
    ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
    render_dynamic_brain_insight("Planned Obsolescence Candidates", ctx)
    st.caption(
        "Parts that haven’t been received in N days. The Brain shows the **plant**, the "
        "**$ at risk** at that plant, and any **other plants still consuming the part** "
        "(intercompany-transfer candidates)."
    )
    N = st.slider("Inactive threshold (days)", 365, 3650, 1460, step=365)
    if not recv.empty and _recv_receipt_col and _recv_part_col in recv.columns:
        last = recv.groupby(_recv_part_col)[_recv_receipt_col].max().reset_index(name="last_receipt")
        last["last_receipt"] = pd.to_datetime(last["last_receipt"], errors="coerce")
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=N)
        obsolete = last[last["last_receipt"] < cutoff].sort_values("last_receipt")
        obsolete["days_inactive"] = (pd.Timestamp.now() - obsolete["last_receipt"]).dt.days
        obsolete["part_description"] = (
            obsolete["part_description"] if "part_description" in obsolete.columns
            else obsolete[_recv_part_col].astype(str).map(_label_part)
        )

        ob1, ob2 = st.columns(2)
        with ob1:
            st.metric("🪦 Obsolescence Candidates", f"{len(obsolete):,}")
            with st.expander("🟢 Brain · Obsolescence", expanded=False):
                st.markdown(f"**At risk:** `{len(obsolete):,}` parts inactive beyond threshold. Review for write-down.")
        with ob2:
            _avg_inactive = f"{obsolete['days_inactive'].mean():.0f}d" if not obsolete.empty else "—"
            st.metric("Avg Days Inactive", _avg_inactive)
            with st.expander("🟢 Brain · Inactivity", expanded=False):
                st.markdown(f"**Mean inactivity:** `{_avg_inactive}` — longer durations signal higher write-down risk.")

        if not obsolete.empty:
            fig_ob = px.histogram(obsolete, x="days_inactive", nbins=30,
                                   color_discrete_sequence=["#f97316"],
                                   title=f"Distribution of Inactive Days (threshold={N}d)",
                                   template="plotly",
                                   labels={"days_inactive":"Days Inactive"})
            fig_ob.update_layout(paper_bgcolor="#0f172a", height=320)
            st.plotly_chart(fig_ob, use_container_width=True)

            # Treemap if cost available
            if not cost.empty and "part_key" in cost.columns:
                obs_cost = obsolete.merge(cost, left_on=_recv_part_col,
                                          right_on="part_key", how="left")
                _cost_col = _col_or_none(obs_cost, _rc(obs_cost, "unit_cost"))
                _oh_obs_col = _col_or_none(obs_cost, _rc(obs_cost, "on_hand_qty"))
                if _cost_col and _oh_obs_col:
                    obs_cost["at_risk_$"] = obs_cost[_cost_col] * obs_cost[_oh_obs_col]
                    obs_cost = obs_cost.dropna(subset=["at_risk_$"])
                    if not obs_cost.empty:
                        fig_tree = px.treemap(obs_cost, path=["part_key_label" if "part_key_label" in obs_cost.columns else "part_key"], values="at_risk_$",
                                              color="days_inactive", color_continuous_scale="RdYlGn_r",
                                              title="Obsolescence $ at Risk Treemap",
                                              template="plotly")
                        fig_tree.update_layout(paper_bgcolor="#0f172a", height=380)
                        st.plotly_chart(fig_tree, use_container_width=True)

            st.dataframe(obsolete.head(300), use_container_width=True, hide_index=True)

            # ── Intercompany-transfer candidates ───────────────────────────
            try:
                # Pull a wider on_hand pull (no site filter) once per session
                _ic_key = "_ic_oh_global"
                if _ic_key not in st.session_state:
                    st.session_state[_ic_key] = fetch_logical("azure_sql", "on_hand", top=20000, where=None)
                _oh_all = st.session_state[_ic_key]
                if not _oh_all.empty and _oh_part_col in _oh_all.columns and "business_unit_key" in _oh_all.columns:
                    obs_keys = obsolete[_recv_part_col].astype(str).unique().tolist()
                    consumers = (_oh_all[_oh_all[_oh_part_col].astype(str).isin(obs_keys)]
                                  .groupby([_oh_part_col, "business_unit_key"])[_oh_qty_col or _oh_part_col]
                                  .agg("sum" if _oh_qty_col else "size")
                                  .reset_index())
                    consumers = consumers.rename(columns={
                        _oh_part_col: "part_key", "business_unit_key": "plant",
                        (_oh_qty_col or _oh_part_col): "on_hand"
                    })
                    consumers = consumers[consumers["on_hand"] > 0]
                    if site:
                        consumers = consumers[consumers["plant"].astype(str) != str(site)]
                    if not consumers.empty:
                        st.markdown("#### 🔁 Intercompany Transfer Candidates")
                        st.caption(
                            "Other plants still consuming these otherwise-obsolete parts. "
                            "Transferring stock here avoids write-off and saves new procurement."
                        )
                        st.dataframe(consumers.head(200), use_container_width=True, hide_index=True)
                    else:
                        st.info("No other plant currently consumes these parts — write-off is the cleanest path.")
            except Exception as _ic_e:
                st.caption(f"Intercompany scan unavailable: {_ic_e}")
    else:
        if not _recv_receipt_col:
            st.warning(
                f"Receipt date column not resolved. po_receipts columns: {list(recv.columns)[:12]}. "
                "Visit **Schema Discovery** page to identify the correct date column."
            )
        else:
            st.info(f"Missing `{_recv_part_col}` on po_receipts.")

with tab4:
    st.subheader("🕸 Shared Vendors & Leverage Points")
    ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
    render_dynamic_brain_insight("Shared Vendors  Leverage Points", ctx)
    st.caption(
        "**Actionable view first:** the table below ranks suppliers by how many of your "
        "active parts they already serve — the higher the count, the easier a consolidation. "
        "Open the expander for the full network graph (advanced)."
    )
    if not HAS_NX:
        st.warning("`networkx` not installed — graph analysis disabled.")
    elif recv.empty or _recv_sup_col not in recv.columns or _recv_part_col not in recv.columns:
        st.info(f"Need supplier key + part key on po_receipts. "
                f"Resolved: sup={_recv_sup_col!r}, part={_recv_part_col!r}. "
                f"Columns: {list(recv.columns)[:10]}")
    else:
        g = GraphContext()
        g.add_edges(recv.head(5000), "supplier", _recv_sup_col, "part", _recv_part_col, "supplied")
        cdf = g.centrality(top_n=50)

        if not cdf.empty:
            # Network graph (advanced)
            _show_net = st.toggle("🕸️ Show full network graph (advanced)", value=False, key="proc_net_show")
            if _show_net:
                try:
                    G = g.graph
                    pos = nx.spring_layout(G, seed=42, k=0.5)
                    edge_x, edge_y = [], []
                    for edge in list(G.edges())[:500]:
                        x0,y0 = pos.get(edge[0],(0,0))
                        x1,y1 = pos.get(edge[1],(0,0))
                        edge_x += [x0,x1,None]; edge_y += [y0,y1,None]

                    node_x = [pos.get(n,(0,0))[0] for n in G.nodes()]
                    node_y = [pos.get(n,(0,0))[1] for n in G.nodes()]
                    node_labels = [str(n)[:20] for n in G.nodes()]
                    node_kinds  = [G.nodes[n].get("kind","?") for n in G.nodes()]

                    fig_net = go.Figure()
                    fig_net.add_trace(go.Scatter(x=edge_x, y=edge_y, mode="lines",
                                                 line=dict(width=0.5,color="#64748b"),
                                                 hoverinfo="none", showlegend=False))
                    color_map = {"supplier":"#38bdf8","part":"#f97316","default":"#a855f7"}
                    for kind_val in set(node_kinds):
                        mask = [k == kind_val for k in node_kinds]
                        fig_net.add_trace(go.Scatter(
                            x=[node_x[i] for i,m in enumerate(mask) if m],
                            y=[node_y[i] for i,m in enumerate(mask) if m],
                            text=[node_labels[i] for i,m in enumerate(mask) if m],
                            mode="markers+text", textposition="top center",
                            name=kind_val.title(),
                            marker=dict(size=8, color=color_map.get(kind_val,"#a855f7")),
                            hovertemplate="%{text}<extra></extra>",
                        ))
                    fig_net.update_layout(
                        height=520, template="plotly", showlegend=True,
                        title="Supplier-Part Supply Network (Spring Layout)",
                        xaxis=dict(showgrid=False,zeroline=False,showticklabels=False),
                        yaxis=dict(showgrid=False,zeroline=False,showticklabels=False),
                    )
                    st.plotly_chart(fig_net, use_container_width=True)
                except Exception as e:
                    st.info(f"Network graph error: {e}")

            # Centrality ranking bar chart
            id_col = "node_id" if "node_id" in cdf.columns else cdf.columns[0]
            ev_col = "eigenvector" if "eigenvector" in cdf.columns else (
                     "betweenness" if "betweenness" in cdf.columns else cdf.columns[-1])
            fig_cen = px.bar(cdf.head(25), x=id_col, y=ev_col,
                              color=ev_col, color_continuous_scale="Plasma",
                              title=f"Top 25 Leverage Nodes by {ev_col.title()} Centrality",
                              template="plotly")
            fig_cen.update_layout(paper_bgcolor="#0f172a",
                                   height=380, xaxis_tickangle=-45, coloraxis_showscale=False)
            st.plotly_chart(fig_cen, use_container_width=True)

        # Shared vendor scan
        parts_in_view = recv[_recv_part_col].dropna().astype(str).head(500).unique()
        shared = g.shared_suppliers(parts_in_view)
        if shared:
            sh_df = pd.DataFrame([{
                "supplier_key": k,
                "supplier_name": _label_supplier(k),
                "n_parts": len(v),
                "parts_sample": ", ".join(_label_part(p) for p in v[:5])
            } for k,v in shared.items()]).sort_values("n_parts", ascending=False)
            st.subheader("Consolidation Candidates — Suppliers Serving Multiple Parts")
            ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
            render_dynamic_brain_insight("Consolidation Candidates  Suppliers Serving Multiple Parts", ctx)
            fig_sh = px.bar(sh_df.head(20), x="supplier_name", y="n_parts",
                             color="n_parts", color_continuous_scale="Blues",
                             title="Shared Vendors: Parts per Supplier (leverage points)",
                             template="plotly",
                             labels={"n_parts":"Parts Supplied","supplier_name":"Supplier"})
            fig_sh.update_layout(paper_bgcolor="#0f172a",
                                  height=360, xaxis_tickangle=-45, coloraxis_showscale=False)
            st.plotly_chart(fig_sh, use_container_width=True)
            st.dataframe(sh_df.head(100), use_container_width=True, hide_index=True)
            record_findings_bulk("procurement_360","supplier",
                [{"key":r["supplier_key"],"score":r["n_parts"],
                  "payload":{"n_parts":r["n_parts"]}}
                 for _,r in sh_df.head(100).iterrows()])

with tab5:
    st.subheader("⚖ CVaR Risk-Aware Supplier Design")
    ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
    render_dynamic_brain_insight("CVaR Risk-Aware Supplier Design", ctx)
    st.caption(
        "**What this is:** a tail-risk view (Conditional Value-at-Risk) on supplier total cost, "
        "factoring in disruption probability, lead-time variance, and unit cost. The Brain auto-derives "
        "these inputs from your live PO receipts + part cost so you don't have to upload anything."
    )
    from src.brain.research.risk_design import supplier_cost_scenarios
    required = {"supplier_key","unit_cost","lead_time_mean","lead_time_std",
                "disruption_prob","annual_demand"}
    src_df = parts.copy() if required.issubset(parts.columns) else pd.DataFrame()

    # ------ Auto-derive CVaR scenarios from live data when explicit cols absent
    if src_df.empty and not recv.empty and _recv_sup_col in recv.columns and "lead_time_days" in recv.columns:
        try:
            qty_col = _recv_qty_col or _rc(recv, "quantity")
            base = (recv.groupby(_recv_sup_col)
                        .agg(lead_time_mean=("lead_time_days", "mean"),
                             lead_time_std=("lead_time_days", "std"),
                             receipts=(_recv_sup_col, "size"),
                             annual_demand=(qty_col, "sum") if qty_col else (_recv_sup_col, "size"))
                        .reset_index())
            base = base.rename(columns={_recv_sup_col: "supplier_key"})
            base["lead_time_std"] = base["lead_time_std"].fillna(0)
            # disruption proxy: % of receipts more than 30 days late
            late = (recv.assign(_late=lambda d: d["lead_time_days"] > 30)
                        .groupby(_recv_sup_col)["_late"].mean()
                        .reset_index().rename(columns={_recv_sup_col:"supplier_key", "_late":"disruption_prob"}))
            base = base.merge(late, on="supplier_key", how="left")
            base["disruption_prob"] = base["disruption_prob"].fillna(0.05).clip(0.01, 0.5)
            # unit cost from cost frame if available
            uc_col = _rc(cost, "unit_cost") if not cost.empty else None
            if uc_col and not cost.empty:
                cost_avg = cost.groupby("supplier_key")[uc_col].mean().reset_index().rename(
                    columns={uc_col: "unit_cost"}) if "supplier_key" in cost.columns else pd.DataFrame()
                if not cost_avg.empty:
                    base = base.merge(cost_avg, on="supplier_key", how="left")
            if "unit_cost" not in base.columns:
                base["unit_cost"] = 1.0
            base["unit_cost"] = base["unit_cost"].fillna(base["unit_cost"].median() or 1.0)
            base["annual_demand"] = pd.to_numeric(base["annual_demand"], errors="coerce").fillna(0)
            base = base[[c for c in ("supplier_key", "unit_cost", "lead_time_mean", "lead_time_std", "disruption_prob", "annual_demand") if c in base.columns]]
            for c in required:
                if c not in base.columns:
                    base[c] = 0.0
            src_df = base[list(required)].head(200)
            st.caption(f"🧠 Auto-built {len(src_df)} supplier scenarios from live PO receipts + cost.")
        except Exception as _e:
            st.warning(f"Could not auto-derive CVaR scenarios: {_e}")

    if src_df.empty:
        st.info("No supplier scenarios available. Upload a CSV with these columns or load PO receipts:")
        st.code(", ".join(sorted(required)))
        csv = st.file_uploader("Upload supplier scenario CSV", type=["csv"], key="cvar_csv")
        if csv:
            src_df = pd.read_csv(csv)
    if not src_df.empty:
        a1, a2 = st.columns(2)
        alpha = a1.slider("CVaR α (tail risk quantile)", 0.80, 0.999, 0.95, 0.005)
        sims  = a2.slider("Monte Carlo simulations per supplier", 500, 5000, 2000, 500)
        with st.spinner("Running CVaR simulation …"):
            out = supplier_cost_scenarios(src_df, n_sims=sims, alpha=alpha)
        if "error" not in out.columns:
            cvar_col = f"cvar_{int(alpha*100)}"
            if "expected_cost" in out.columns and cvar_col in out.columns:
                fig_cvar = px.scatter(
                    out, x="expected_cost", y=cvar_col,
                    color="pareto_efficient" if "pareto_efficient" in out.columns else None,
                    hover_data=[c for c in out.columns if c not in ["expected_cost",cvar_col]][:5],
                    title=f"CVaR{int(alpha*100)} vs Expected Cost — Pareto Frontier",
                    template="plotly",
                    labels={"expected_cost":"Expected Cost","cvar_col":f"CVaR{int(alpha*100)}"},
                    color_discrete_map={True:"#22c55e",False:"#ef4444"},
                )
                fig_cvar.update_layout(paper_bgcolor="#0f172a",
                                        height=450)
                st.plotly_chart(fig_cvar, use_container_width=True)
            st.dataframe(out, use_container_width=True, hide_index=True)
        else:
            st.error(out.iloc[0]["error"])

with tab6:
    st.subheader("🧠 Lead-Time Causal Attribution")
    ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
    render_dynamic_brain_insight("Lead-Time Causal Attribution", ctx)
    st.caption(
        "**How to read this:** the bar shows which factor most explains lead-time variation. "
        "A taller bar = bigger driver. If `supplier_key` dominates, your lead-time pain is "
        "vendor-specific (renegotiate or dual-source). If `part_key`/category dominates, the "
        "problem is the part design or sourcing market. Anything else points to process drift."
    )
    from src.brain.research.causal_lead_time import lead_time_attribution
    if not recv.empty and "lead_time_days" in recv.columns:
        with st.spinner("Running causal forest analysis …"):
            attr = lead_time_attribution(recv)
        if not attr.empty:
            st.caption(f"Method: `{attr.iloc[0]['method'] if 'method' in attr.columns else 'causal_forest'}`")
            feat_col = "feature" if "feature" in attr.columns else attr.columns[0]
            imp_col  = [c for c in attr.columns if "import" in c.lower() or "effect" in c.lower()]
            imp_col  = imp_col[0] if imp_col else attr.columns[-1]
            fig_attr = px.bar(attr.head(20).sort_values(imp_col, ascending=True),
                               x=imp_col, y=feat_col, orientation="h",
                               color=imp_col, color_continuous_scale="RdYlGn_r",
                               title="Lead-Time Causal Drivers (higher = more impact)",
                               template="plotly")
            fig_attr.update_layout(paper_bgcolor="#0f172a",
                                    height=400, coloraxis_showscale=False)
            st.plotly_chart(fig_attr, use_container_width=True)
            st.dataframe(attr, use_container_width=True, hide_index=True)
        else:
            st.info("Causal attribution returned no results — ensure lead_time_days and covariates exist.")
    else:
        st.info("`lead_time_days` not available — needs `promise_date` and `receipt_date`.")
