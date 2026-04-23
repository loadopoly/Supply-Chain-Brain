"""Page 1 — Supply Chain Brain: interactive network graph + cross-domain KPI command centre."""
from pathlib import Path
import sys
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.db_registry import bootstrap_default_connectors, read_sql
from src.brain.data_access import fetch_logical, query_df
from src.brain.global_filters import date_key_window, sql_and_date_key
from src.brain.graph_context import GraphContext, HAS_NX
from src.brain.findings_index import all_kinds, lookup_findings

# set_page_config handled by app.py st.navigation()
st.session_state["_page"] = "supply_chain_brain"
bootstrap_default_connectors()

# ── Header ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
.kpi-box{background:linear-gradient(135deg,#1e293b,#0f172a);border-radius:12px;
         padding:18px;text-align:center;border:1px solid #334155;}
.kpi-val{font-size:2rem;font-weight:700;color:#38bdf8;}
.kpi-lbl{font-size:.8rem;color:#94a3b8;margin-top:4px;}
</style>
""", unsafe_allow_html=True)

st.markdown("## 🧠 Supply Chain Brain")
st.caption("Procurement · Logistics · Supply Chain · Customer Service — multi-dimensional graph intelligence")

st.divider()

# ── Auto-load graph data ─────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def _get_sites():
    try:
        df = query_df("azure_sql", "SELECT DISTINCT business_unit_id FROM edap_dw_replica.dim_part WITH (NOLOCK) WHERE business_unit_id IS NOT NULL")
        if not df.empty:
            return [""] + sorted(df["business_unit_id"].astype(str).tolist())
    except Exception:
        pass
    return [""]

with st.expander("🔍 Refine graph limits & filters", expanded=False):
    col1, col2, col3 = st.columns(3)
    n_parts = col1.number_input("Parts", 50, 5000, 200, step=50, key="g_np")
    n_recv  = col2.number_input("PO Receipts", 100, 20000, 750, step=100, key="g_nr")
    n_so    = col3.number_input("SO Lines", 100, 20000, 750, step=100, key="g_nso")
    f1, f2, f3, f4 = st.columns(4)
    show_kinds = f1.multiselect("Show node kinds",
        ["part","supplier","customer"], default=["part","supplier","customer"], key="g_kinds")
    min_degree = f2.number_input("Min degree", 0, 100, 0, key="g_mindeg")
    max_nodes  = f3.number_input("Max nodes drawn", 50, 1500, 300, step=50, key="g_maxn")
    st.caption("Global Mfg Site filter controls this graph.")
    site_filter = st.session_state.get("g_site", "")
    rerun   = st.button("🔄 Build / rebuild graph", key="g_rerun")

from src.brain.label_resolver import enrich_labels

@st.cache_resource(ttl=600, show_spinner="Building supply chain knowledge graph …")
def _build_graph(np_: int, nr_: int, nso_: int, site: str, start_k: int, end_k: int, _bump: int = 0):
    w_parts = f"business_unit_id = '{site}'" if site else "1=1"
    w_recv  = f"business_unit_key IN (SELECT business_unit_key FROM edap_dw_replica.dim_business_unit WITH (NOLOCK) WHERE business_unit_id = '{site}')" if site else "1=1"
    w_so    = f"business_unit_key IN (SELECT business_unit_key FROM edap_dw_replica.dim_business_unit WITH (NOLOCK) WHERE business_unit_id = '{site}')" if site else "1=1"
    
    # Apply global timeline bounds
    w_recv += f" AND receipt_date_key BETWEEN {start_k} AND {end_k}"
    w_so   += f" AND order_date_key BETWEEN {start_k} AND {end_k}"

    parts = fetch_logical("azure_sql", "parts",             top=np_, where=w_parts, timeout_s=30)
    recv  = fetch_logical("azure_sql", "po_receipts",       top=nr_, where=w_recv, timeout_s=30)
    so    = fetch_logical("azure_sql", "sales_order_lines", top=nso_, where=w_so, timeout_s=30)

    # Enrich every frame with human-readable label columns (*_label)
    parts = enrich_labels(parts)
    recv  = enrich_labels(recv)
    so    = enrich_labels(so)

    g = GraphContext()
    diag = {
        "parts":              parts.attrs.get("_error"),
        "po_receipts":        recv.attrs.get("_error"),
        "sales_order_lines":  so.attrs.get("_error"),
    }

    # Resolve label column names — prefer enriched "*_label" columns
    _part_lbl_parts = next((c for c in ("part_key_label","part_number_label","part_description")
                            if c in parts.columns), None)
    _sup_lbl_recv   = next((c for c in ("supplier_key_label","supplier_name","vendor_name")
                            if c in recv.columns), None)
    _part_lbl_recv  = next((c for c in ("part_key_label","part_number_label") if c in recv.columns), None)
    _part_lbl_so    = next((c for c in ("part_key_label","part_number_label") if c in so.columns), None)
    _cust_lbl_so    = next((c for c in ("customer_key_label","customer_name","bill_to_customer_name")
                            if c in so.columns), None)

    if not parts.empty and "part_key" in parts.columns:
        g.add_parts(parts, id_col="part_key", label_col=_part_lbl_parts)
    if not recv.empty and {"supplier_key","part_key"}.issubset(recv.columns):
        g.add_edges(recv, "supplier", "supplier_key", "part", "part_key", "received",
                    src_label_col=_sup_lbl_recv, dst_label_col=_part_lbl_recv)
    if not so.empty and {"customer_key","part_key"}.issubset(so.columns):
        g.add_edges(so, "part", "part_key", "customer", "customer_key", "shipped",
                    src_label_col=_part_lbl_so, dst_label_col=_cust_lbl_so)
    return g, diag, len(parts), len(recv), len(so)

bump = st.session_state.get("_g_bump", 0)
if rerun:
    bump += 1
    st.session_state["_g_bump"] = bump
    st.session_state["_graph_requested"] = True
    st.cache_resource.clear()
    st.cache_data.clear()

graph_requested = st.session_state.get("_graph_requested", True)
g = GraphContext() if HAS_NX else None
diag = {}
n_p = n_r = n_s = 0

if graph_requested:
    _sk, _ek = date_key_window()
    g, diag, n_p, n_r, n_s = _build_graph(
        int(st.session_state.get("g_np", 200)),
        int(st.session_state.get("g_nr", 750)),
        int(st.session_state.get("g_nso", 750)),
        str(st.session_state.get("g_site", "")),
        _sk, 
        _ek,
        bump,
    )
else:
    st.info("Graph build is paused on first load to keep the app responsive. Open **Refine graph limits & filters** and click **Build / rebuild graph**.")

# Store actual graph results so DBI worker can produce data-specific insights.
st.session_state["dbi_graph_nodes"]  = g.g.number_of_nodes() if g and HAS_NX else 0
st.session_state["dbi_graph_edges"]  = g.g.number_of_edges() if g and HAS_NX else 0
st.session_state["dbi_actual_parts"] = n_p
st.session_state["dbi_actual_po"]    = n_r
st.session_state["dbi_actual_so"]    = n_s

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight('Supply Chain Brain', ctx)

# ── KPI strip ────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("🔩 Parts",       f"{n_p:,}")
k2.metric("📋 PO Receipts", f"{n_r:,}")
k3.metric("🛒 SO Lines",    f"{n_s:,}")
k4.metric("🕸 Graph Edges", f"{g.g.number_of_edges():,}")
k5.metric("🔗 Graph Nodes", f"{g.g.number_of_nodes():,}")

# ── Errors surface ───────────────────────────────────────────────────────────
bad = {k: v for k, v in diag.items() if v}
if bad:
    for tbl, err in bad.items():
        with st.expander(f"⚠️ {tbl} — live query failed (click to diagnose)", expanded=False):
            st.code(err, language="text")
            st.info("Update the table mapping in `config/brain.yaml` → tables section.")

st.divider()

# ── Interactive force-directed graph ─────────────────────────────────────────
import plotly.graph_objects as go

st.subheader("🕸 Interactive Supply Chain Knowledge Graph")
st.caption("Node size = degree centrality · Color = node type · Click nodes to drill down")

@st.cache_data(ttl=600, show_spinner="Computing graph layout …")
def _graph_layout(_bump: int = 0, kinds=("part","supplier","customer"),
                  min_deg: int = 0, max_n: int = 300):
    if not HAS_NX or g.g.number_of_nodes() == 0:
        return None
    import networkx as nx
    # Apply filters: kind whitelist + min-degree + cap
    keep = [n for n, d in g.g.nodes(data=True)
            if d.get("kind","node") in set(kinds)
            and g.g.degree(n) >= int(min_deg)]
    subg = g.g.subgraph(keep)
    if subg.number_of_nodes() > max_n:
        top_nodes = sorted(dict(subg.degree()).items(), key=lambda x: x[1], reverse=True)[:max_n]
        subg = subg.subgraph([n for n, _ in top_nodes])
    if subg.number_of_nodes() == 0:
        return None

    pos = nx.spring_layout(subg, k=2.5/max(1, subg.number_of_nodes()**0.5), seed=42)
    deg = dict(subg.degree())

    # Group nodes per kind so each gets its own trace (legend-grouped colors)
    COLOR_MAP = {"part": "#38bdf8", "supplier": "#f97316",
                 "customer": "#22c55e", "node": "#a855f7"}
    by_kind: dict[str, dict] = {}
    for nd, attrs in subg.nodes(data=True):
        kind = attrs.get("kind", "node")
        bucket = by_kind.setdefault(kind, {"x": [], "y": [], "text": [], "hover": [],
                                            "size": [], "ids": []})
        x, y = pos[nd]
        label = attrs.get("label", str(nd))
        bucket["x"].append(x); bucket["y"].append(y)
        bucket["text"].append(str(label)[:18])
        bucket["hover"].append(
            f"<b>{label}</b><br>Type: {kind}<br>Degree: {deg.get(nd,0)}<br>ID: {nd}")
        bucket["size"].append(6 + min(34, deg.get(nd, 1) * 3))
        bucket["ids"].append(nd)

    edge_x, edge_y = [], []
    for u, v in subg.edges():
        x0, y0 = pos[u]; x1, y1 = pos[v]
        edge_x += [x0, x1, None]; edge_y += [y0, y1, None]

    return edge_x, edge_y, by_kind, COLOR_MAP

layout_data = _graph_layout(bump,
    tuple(st.session_state.get("g_kinds",["part","supplier","customer"])),
    int(st.session_state.get("g_mindeg", 0)),
    int(st.session_state.get("g_maxn", 300))) if graph_requested and g is not None else None

if layout_data and HAS_NX:
    edge_x, edge_y, by_kind, COLOR_MAP = layout_data

    fig_g = go.Figure()
    fig_g.add_trace(go.Scatter(
        x=edge_x, y=edge_y, mode="lines",
        line=dict(width=0.5, color="#334155"), hoverinfo="none", showlegend=False))
    for kind, bucket in by_kind.items():
        fig_g.add_trace(go.Scatter(
            x=bucket["x"], y=bucket["y"], mode="markers",
            marker=dict(size=bucket["size"], color=COLOR_MAP.get(kind,"#a855f7"),
                        line=dict(width=1, color="#1e293b"), opacity=0.92),
            hovertemplate="%{hovertext}<extra></extra>",
            hovertext=bucket["hover"],
            customdata=[str(n) for n in bucket["ids"]],
            name=kind.title(), legendgroup=kind, showlegend=True))
        fig_g.add_trace(go.Scatter(
            x=bucket["x"], y=bucket["y"], mode="text",
            text=bucket["text"], textposition="top center",
            textfont=dict(size=8, color="#cbd5e1"),
            hoverinfo="skip",
            showlegend=False,
            legendgroup=kind,
        ))

    fig_g.update_layout(
        height=600,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        margin=dict(l=0, r=0, t=30, b=30),
        clickmode="event+select",
        hovermode="closest",
    )
    clicked = st.plotly_chart(fig_g, use_container_width=True, key="kg_graph",
                              on_select="rerun")

    # Drill-down on click
    if clicked and clicked.get("selection") and clicked["selection"].get("points"):
        pt = clicked["selection"]["points"][0]
        nid = pt.get("customdata")
        if nid:
            st.session_state["kg_selected_node"] = nid

    sel_node = st.session_state.get("kg_selected_node")
    if sel_node and HAS_NX:
        # Resolve string back to tuple node id
        node_match = None
        for n in g.g.nodes():
            if str(n) == str(sel_node) or (isinstance(n, tuple) and n[1] == str(sel_node)):
                node_match = n; break
        if node_match is None:
            st.info(f"Selected node `{sel_node}` not found in graph.")
        else:
            st.divider()
            st.subheader(f"🔍 Discovery: why is `{node_match}` connected?")
            exp = g.explain_node(node_match)
            d1, d2, d3 = st.columns(3)
            d1.metric("Total Degree", exp["degree"])
            d2.metric("In Degree",    exp["in_degree"] if exp["in_degree"] is not None else "—")
            d3.metric("Out Degree",   exp["out_degree"] if exp["out_degree"] is not None else "—")
            cA, cB = st.columns(2)
            with cA:
                st.caption("**Neighbor breakdown by kind**")
                if exp["neighbor_kinds"]:
                    nb_df = pd.DataFrame(list(exp["neighbor_kinds"].items()),
                                          columns=["kind","count"]).sort_values("count", ascending=False)
                    st.dataframe(nb_df, use_container_width=True, hide_index=True)
            with cB:
                st.caption("**Edge relations**")
                if exp["edge_kinds"]:
                    ek_df = pd.DataFrame(list(exp["edge_kinds"].items()),
                                          columns=["relation","count"]).sort_values("count", ascending=False)
                    st.dataframe(ek_df, use_container_width=True, hide_index=True)

            # Edge-level evidence table
            neighbors = list(g.g.successors(node_match)) + list(g.g.predecessors(node_match))
            rows_drill = []
            for nb in neighbors[:80]:
                edata = g.g.get_edge_data(node_match, nb) or g.g.get_edge_data(nb, node_match) or {}
                # MultiDiGraph returns {key: {attrs}}; flatten each
                if edata:
                    for k, attrs in edata.items():
                        rows_drill.append({
                            "neighbor": str(nb),
                            "kind": g.g.nodes[nb].get("kind","?"),
                            "relation": attrs.get("kind", str(k)),
                            "weight": attrs.get("weight", 1.0),
                            "neighbor_degree": g.g.degree(nb),
                        })
            if rows_drill:
                st.caption("**Connected nodes (top 80, sorted by degree)**")
                st.dataframe(pd.DataFrame(rows_drill).sort_values("neighbor_degree",ascending=False),
                             use_container_width=True, hide_index=True)
else:
    st.info("Graph will render once live data loads. Check connector status above.")

st.divider()

# ── Centrality leaderboard ───────────────────────────────────────────────────
st.subheader("🎯 Network Leverage Points — Top Centrality")
st.caption("Highest centrality = highest supply chain risk concentration / opportunity")

cdf = g.centrality(top_n=50) if graph_requested and g is not None else pd.DataFrame()
if not cdf.empty and "_error" not in cdf.columns:
    import plotly.express as px

    # node_kind already provided by centrality(); fall back via lookup if missing
    if "node_kind" not in cdf.columns:
        cdf["node_kind"] = cdf["node"].apply(
            lambda nd: g.g.nodes[nd].get("kind","node") if nd in g.g.nodes else "node")
    cdf["type"] = cdf["node_kind"]

    tab1, tab2 = st.tabs(["📊 Bar chart", "📋 Table"])
    with tab1:
        fig_cen = px.bar(
            cdf.head(30), x="centrality", y="node_id", orientation="h",
            color="type",
            color_discrete_map={"part":"#38bdf8","supplier":"#f97316","customer":"#22c55e"},
            title="Top 30 Leverage Nodes by Centrality",
            labels={"centrality":"Centrality Score","node_id":"Node"},
            template="plotly",
        )
        fig_cen.update_layout(height=500, yaxis={"categoryorder":"total ascending"})
        cent_click = st.plotly_chart(fig_cen, use_container_width=True,
                                     key="centrality_bar", on_select="rerun")
        if cent_click and cent_click.get("selection",{}).get("points"):
            pt = cent_click["selection"]["points"][0]
            if "y" in pt:
                st.session_state["kg_selected_node"] = pt["y"]
                st.rerun()
    with tab2:
        st.dataframe(cdf.drop(columns=["node"], errors="ignore"),
                     use_container_width=True, hide_index=True,
                     column_config={"centrality": st.column_config.ProgressColumn(
                         "Centrality", min_value=0, max_value=float(cdf["centrality"].max() or 1.0))})

st.divider()

# ── Cross-page findings index ─────────────────────────────────────────────────
st.subheader("📌 Cross-page Findings Index")

# Plain-language explanations for every finding kind the Brain pages emit.
_FINDING_EXPLANATIONS = {
    "part":         ("**Part-level finding** — flagged on **EOQ Deviation**. "
                      "Each row is a part whose on-hand+open position is far from the EOQ "
                      "optimum (high |z| = severely over- or under-stocked). `score` = "
                      "abs deviation z; `dollar_at_risk` = inventory $ exposure."),
    "cluster":      ("**Cluster-level finding** — flagged on **OTD Recursive**. "
                      "Each row is a recursive cluster of late shipments grouped by "
                      "supplier × part × root cause. `score` = cluster size."),
    "supplier":     ("**Supplier-level finding** — flagged on **Procurement 360 / Lead Time**. "
                      "Identifies suppliers with elevated lead-time variance, defect rate, "
                      "or CVaR tail risk."),
    "lane":         ("**Lane-level finding** — flagged on **Freight Portfolio**. "
                      "Each row is an OD pair (origin → destination) with high cost or risk."),
    "node":         ("**Network node finding** — flagged on **Multi-Echelon / Brain Network**. "
                      "Each row is a graph node (part, supplier, customer) ranked by "
                      "centrality (= leverage)."),
    "vendor":       ("**Vendor finding** — flagged on **LLM Vendor Management**. "
                      "Highlights consolidation candidates derived from NLP product-similarity."),
    "default":      ("Each row is one decision-grade observation produced by the Brain. "
                      "`score` is severity; `payload` carries kind-specific context."),
}

kinds = all_kinds()
if not kinds:
    st.info("No findings pinned yet. Navigate to EOQ Deviation, OTD, or Procurement 360 to generate findings.")
else:
    col_k, col_lim = st.columns([3, 1])
    pick_kind = col_k.selectbox("Finding kind", kinds, key="fi_kind")
    lim = col_lim.number_input("Max rows", 10, 500, 100, key="fi_lim")
    # Plain-language explainer for the selected kind.
    st.info(_FINDING_EXPLANATIONS.get(str(pick_kind).lower(),
                                       _FINDING_EXPLANATIONS["default"]))
    rows = lookup_findings(kind=pick_kind, limit=int(lim))
    if rows:
        fi_df = pd.DataFrame([{
            "page": r["page"], "key": r["key"],
            "score": round(float(r["score"]), 3), "when": r["created_at"], **r["payload"],
        } for r in rows])
        import plotly.express as px
        if "score" in fi_df.columns and len(fi_df) > 1:
            fig_fi = px.histogram(fi_df, x="score", color="page", nbins=30,
                                  title="Finding Score Distribution",
                                  template="plotly",
                                  color_discrete_sequence=px.colors.qualitative.Vivid)
            fig_fi.update_layout(paper_bgcolor="#0f172a", height=250)
            st.plotly_chart(fig_fi, use_container_width=True)
        st.dataframe(fi_df, use_container_width=True, hide_index=True)

# ── Brain Expert TODO List (semantic value-weighting) ───────────────────────
st.divider()
st.markdown("## 🧭 Brain Expert TODO — Ranked by $/year Value")
st.caption(
    "The Brain converts every page''s telemetry into actionable, layperson-friendly tasks. "
    "Each row tells you **what to do**, **why it matters**, **who owns it**, and the "
    "estimated **annual $ value** of the action."
)
try:
    from src.brain.actions import actions_for_pipeline, actions_to_dataframe
    from src.brain.local_store import add_bookmark
    # Use a deterministic baseline telemetry — actions module supplies sensible
    # defaults for any missing stage. Live per-stage telemetry lives on page 1b.
    _tel = {}
    _acts = actions_for_pipeline(_tel)
    _adf = actions_to_dataframe(_acts).sort_values("value_per_year", ascending=False)
    st.dataframe(
        _adf, use_container_width=True, hide_index=True,
        column_config={
            "value_per_year": st.column_config.NumberColumn("Annual $ Value", format="$%.0f"),
            "confidence":     st.column_config.ProgressColumn("Confidence", min_value=0, max_value=1.0),
        },
    )
    if st.button("📌 Bookmark top action", key="brain_bm_top"):
        _t = _adf.iloc[0]
        add_bookmark(stage=str(_t["stage"]), title=str(_t["title"]),
                     detail=str(_t.get("do_this","")), priority=str(_t.get("severity","")),
                     value_score=float(_t.get("value_per_year",0) or 0))
        st.success("Bookmarked.")
except Exception as _e:
    st.info(f"Action queue unavailable: {_e}")
