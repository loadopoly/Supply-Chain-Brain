"""Page 1 — Supply Chain Brain: interactive network graph + cross-domain KPI command centre."""
from pathlib import Path
import sys
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight
from src.brain.operator_shell import render_operator_sidebar_fallback
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
render_operator_sidebar_fallback()

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

def _page_link(page: str, label: str, icon: str, fallback_url: str) -> None:
    try:
        st.page_link(page, label=label, icon=icon)
    except Exception:
        st.markdown(f"[{icon} {label}]({fallback_url})")

if st.session_state.get("operator_mode", True):
    _site_scope = st.session_state.get("g_site") or "All plants"
    st.markdown("### Plant Risk Control Room")
    _op1, _op2, _op3 = st.columns(3)
    with _op1:
        with st.container(border=True):
            st.markdown("**1 · DBI Next Move**")
            st.caption(f"Scope: {_site_scope}")
            st.markdown("Work the DBI action before exploring lower-priority charts.")
    with _op2:
        with st.container(border=True):
            st.markdown("**2 · Find The Owner**")
            st.caption("Click the largest supplier, part, or customer node.")
            _page_link("app.py", "Search exact record", "🔍", "/")
    with _op3:
        with st.container(border=True):
            st.markdown("**3 · Leave With A One-Pager**")
            st.caption("Use the default report after the top risk is known.")
            _page_link("pages/15_Report_Creator.py", "Create report", "📊", "/Report_Creator")

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

# ── KPI strip (with per-metric Brain insight) ────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    st.metric("🔩 Parts", f"{n_p:,}")
    with st.expander("🟢 Brain · Parts", expanded=False):
        st.markdown(
            f"**Population:** `{n_p:,}` part masters loaded.  \n"
            "This defines the available product scope for the entire search space."
        )

with k2:
    st.metric("📋 PO Receipts", f"{n_r:,}")
    with st.expander("🟢 Brain · POs", expanded=False):
        st.markdown(
            f"**Volume:** `{n_r:,}` receipts retrieved.  \n"
            "Supply-side relationships (Supplier -> Part) are built from these events."
        )

with k3:
    st.metric("🛒 SO Lines", f"{n_s:,}")
    with st.expander("🟢 Brain · SOs", expanded=False):
        st.markdown(
            f"**Demand:** `{n_s:,}` sales lines retrieved.  \n"
            "Demand-side relationships (Part -> Customer) are built from these events."
        )

with k4:
    n_edges = g.g.number_of_edges() if g and HAS_NX else 0
    st.metric("🕸 Graph Edges", f"{n_edges:,}")
    with st.expander("🟢 Brain · Edges", expanded=False):
        st.markdown(
            f"**Connectivity:** `{n_edges:,}` links identified.  \n"
            "High edge count indicates strong cross-domain overlap."
        )

with k5:
    n_nodes = g.g.number_of_nodes() if g and HAS_NX else 0
    st.metric("🟣 Total Nodes", f"{n_nodes:,}")
    with st.expander("🟢 Brain · Nodes", expanded=False):
        st.markdown(
            f"**Domain breadth:** `{n_nodes:,}` unique entities.  \n"
            "Counts Suppliers, Parts, and Customers linked in the graph."
        )

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
        import networkx as nx

        # ── Resolve node ──────────────────────────────────────────────────────
        node_match = None
        for n in g.g.nodes():
            if str(n) == str(sel_node) or (isinstance(n, tuple) and n[1] == str(sel_node)):
                node_match = n; break

        if node_match is None:
            st.info(f"Selected node `{sel_node}` not found in current graph slice.")
        else:
            node_attrs  = g.g.nodes[node_match]
            node_kind   = node_attrs.get("kind", "node")
            node_label  = node_attrs.get("label", str(node_match))
            entity_id   = node_match[1] if isinstance(node_match, tuple) else str(node_match)

            exp            = g.explain_node(node_match)
            neighbors_all  = list(g.g.successors(node_match)) + list(g.g.predecessors(node_match))
            neighbors_uniq = list(dict.fromkeys(neighbors_all))

            # Pre-build edge evidence rows (reused across tabs)
            rows_drill = []
            for _nb in neighbors_uniq[:120]:
                _edata = g.g.get_edge_data(node_match, _nb) or g.g.get_edge_data(_nb, node_match) or {}
                for _ek, _ea in _edata.items():
                    rows_drill.append({
                        "neighbor":        g.g.nodes[_nb].get("label",
                                               _nb[1] if isinstance(_nb, tuple) else str(_nb)),
                        "neighbor_id":     _nb[1] if isinstance(_nb, tuple) else str(_nb),
                        "kind":            g.g.nodes[_nb].get("kind", "?"),
                        "relation":        _ea.get("kind", str(_ek)),
                        "weight":          round(float(_ea.get("weight", 1.0)), 2),
                        "neighbor_degree": g.g.degree(_nb),
                    })

            # Degree rank among all graph nodes (O(N), instant)
            _all_degs = sorted(dict(g.g.degree()).values(), reverse=True)
            _self_deg = g.g.degree(node_match)
            _deg_rank = next((i+1 for i, d in enumerate(_all_degs) if d == _self_deg), "—")

            # ── Header strip ───────────────────────────────────────────────────
            st.divider()
            _ICONS = {"part": "🔩", "supplier": "🏭", "customer": "🛒"}
            _hdr_col, _close_col = st.columns([9, 1])
            _hdr_col.markdown(
                f"### {_ICONS.get(node_kind,'⚙️')} **{node_label}**  "
                f"<span style='color:#64748b;font-size:.85rem'>`{node_kind.upper()}` · ID `{entity_id}`</span>",
                unsafe_allow_html=True)
            if _close_col.button("✖ Deselect", key="kg_clear"):
                st.session_state.pop("kg_selected_node", None)
                st.rerun()

            # ── 5-metric ribbon ────────────────────────────────────────────────
            _m1, _m2, _m3, _m4, _m5 = st.columns(5)
            _m1.metric("Degree",       exp["degree"])
            _m2.metric("In",           exp["in_degree"]  if exp["in_degree"]  is not None else "—")
            _m3.metric("Out",          exp["out_degree"] if exp["out_degree"] is not None else "—")
            _m4.metric("Neighbours",   len(neighbors_uniq))
            _m5.metric("Degree Rank",  f"#{_deg_rank}")

            # ── Tabs ───────────────────────────────────────────────────────────
            _tab_net, _tab_rel, _tab_2hop, _tab_sql, _tab_find = st.tabs([
                "🕸 1-hop Network", "📊 Relationships", "🔄 2-hop Network",
                "🗃 Live Data",     "📌 Findings",
            ])

            # ── Tab 1: 1-hop neighbourhood ─────────────────────────────────────
            with _tab_net:
                _sub_nodes = [node_match] + neighbors_uniq[:60]
                _subg      = g.g.subgraph(_sub_nodes)
                if _subg.number_of_nodes() > 1:
                    _C = {"part":"#38bdf8","supplier":"#f97316",
                          "customer":"#22c55e","node":"#a855f7"}
                    _pos_sub = nx.spring_layout(
                        _subg, k=1.8/max(1, _subg.number_of_nodes()**0.4), seed=7)
                    _deg_sub = dict(_subg.degree())

                    # edge trace
                    _ex, _ey = [], []
                    for _u, _v in _subg.edges():
                        _x0,_y0=_pos_sub[_u]; _x1,_y1=_pos_sub[_v]
                        _ex+=[_x0,_x1,None]; _ey+=[_y0,_y1,None]

                    _fig_sub = go.Figure()
                    _fig_sub.add_trace(go.Scatter(
                        x=_ex, y=_ey, mode="lines",
                        line=dict(width=0.8, color="#475569"),
                        hoverinfo="none", showlegend=False))

                    # one Scatter trace per node kind — preserves per-point customdata
                    _sub_by_kind: dict = {}
                    for _nd, _at in _subg.nodes(data=True):
                        _kd = _at.get("kind","node")
                        _b  = _sub_by_kind.setdefault(_kd, {
                            "x":[],"y":[],"text":[],"hover":[],"sz":[],"bw":[],"bc":[],"ids":[]})
                        _xp, _yp = _pos_sub[_nd]
                        _lbl = _at.get("label", str(_nd))
                        _is_sel = _nd == node_match
                        _b["x"].append(_xp); _b["y"].append(_yp)
                        _b["text"].append(str(_lbl)[:16])
                        _b["hover"].append(
                            f"<b>{_lbl}</b><br>Type: {_kd}<br>"
                            f"Degree: {_deg_sub.get(_nd,0)}<br>"
                            f"ID: {_nd[1] if isinstance(_nd,tuple) else _nd}"
                            + ("<br>⭐ SELECTED" if _is_sel else ""))
                        _b["sz"].append(26 if _is_sel else 8+min(22, _deg_sub.get(_nd,1)*2))
                        _b["bw"].append(4 if _is_sel else 1)
                        _b["bc"].append("#facc15" if _is_sel else "#1e293b")
                        _b["ids"].append(_nd)

                    for _kd, _b in _sub_by_kind.items():
                        _fig_sub.add_trace(go.Scatter(
                            x=_b["x"], y=_b["y"], mode="markers+text",
                            marker=dict(size=_b["sz"], color=_C.get(_kd,"#a855f7"),
                                        line=dict(width=_b["bw"], color=_b["bc"])),
                            text=_b["text"], textposition="top center",
                            textfont=dict(size=9, color="#e2e8f0"),
                            hovertemplate="%{hovertext}<extra></extra>",
                            hovertext=_b["hover"],
                            customdata=[str(_n) for _n in _b["ids"]],
                            name=_kd.title(), showlegend=True))

                    _fig_sub.update_layout(
                        height=440, paper_bgcolor="#0f172a", plot_bgcolor="#0f172a",
                        xaxis=dict(showgrid=False,zeroline=False,showticklabels=False),
                        yaxis=dict(showgrid=False,zeroline=False,showticklabels=False),
                        margin=dict(l=0,r=0,t=35,b=10),
                        title=dict(
                            text=f"1-hop neighbourhood · {len(neighbors_uniq)} direct connections · click a node to pivot",
                            font=dict(color="#94a3b8", size=12)),
                        hovermode="closest", clickmode="event+select",
                    )
                    _sub_click = st.plotly_chart(_fig_sub, use_container_width=True,
                                                 key="kg_subgraph", on_select="rerun")
                    if _sub_click and _sub_click.get("selection",{}).get("points"):
                        _pt2 = _sub_click["selection"]["points"][0]
                        _nid2 = _pt2.get("customdata")
                        if _nid2 and _nid2 != str(node_match):
                            st.session_state["kg_selected_node"] = _nid2
                            st.rerun()
                    st.caption("🟡 thick border = selected node · click any neighbour to pivot the drill-down to it")
                else:
                    st.info("No neighbours in the current graph slice — increase the graph size in the filter panel.")

            # ── Tab 2: relationships ───────────────────────────────────────────
            with _tab_rel:
                _rc1, _rc2 = st.columns(2)
                with _rc1:
                    st.caption("**Neighbour breakdown by type**")
                    if exp["neighbor_kinds"]:
                        st.dataframe(
                            pd.DataFrame(list(exp["neighbor_kinds"].items()),
                                         columns=["kind","count"])
                              .sort_values("count", ascending=False),
                            use_container_width=True, hide_index=True)
                with _rc2:
                    st.caption("**Edge relation types**")
                    if exp["edge_kinds"]:
                        st.dataframe(
                            pd.DataFrame(list(exp["edge_kinds"].items()),
                                         columns=["relation","count"])
                              .sort_values("count", ascending=False),
                            use_container_width=True, hide_index=True)

                if rows_drill:
                    st.caption(
                        f"**All direct connections — {len(rows_drill)} edge rows "
                        f"(top 120 neighbours, sorted by degree)**")
                    st.dataframe(
                        pd.DataFrame(rows_drill).sort_values("neighbor_degree", ascending=False),
                        use_container_width=True, hide_index=True,
                        column_config={
                            "weight":          st.column_config.NumberColumn("Weight", format="%.2f"),
                            "neighbor_degree": st.column_config.NumberColumn("Degree"),
                        })

            # ── Tab 3: 2-hop network ───────────────────────────────────────────
            with _tab_2hop:
                _hop2 = set([node_match])
                for _nb in neighbors_uniq[:40]:
                    _hop2.add(_nb)
                    for _nb2 in (list(g.g.successors(_nb))[:10]
                                 + list(g.g.predecessors(_nb))[:10]):
                        _hop2.add(_nb2)
                _hop2_list = list(_hop2)[:150]
                _subg2     = g.g.subgraph(_hop2_list)

                if _subg2.number_of_nodes() > 2:
                    _pos2  = nx.spring_layout(
                        _subg2, k=2.2/max(1, _subg2.number_of_nodes()**0.4), seed=42)
                    _deg2  = dict(_subg2.degree())
                    _C2    = {"part":"#38bdf8","supplier":"#f97316",
                              "customer":"#22c55e","node":"#a855f7"}
                    _1hop_set = set(neighbors_uniq)

                    _ex2, _ey2 = [], []
                    for _u2, _v2 in _subg2.edges():
                        _x0,_y0=_pos2[_u2]; _x1,_y1=_pos2[_v2]
                        _ex2+=[_x0,_x1,None]; _ey2+=[_y0,_y1,None]

                    _fig_hop2 = go.Figure()
                    _fig_hop2.add_trace(go.Scatter(
                        x=_ex2, y=_ey2, mode="lines",
                        line=dict(width=0.4, color="#334155"),
                        hoverinfo="none", showlegend=False))

                    _rings: dict = {
                        "Selected": {"x":[],"y":[],"hover":[],"sz":[],"col":[],"ids":[],"op":[]},
                        "1-hop":    {"x":[],"y":[],"hover":[],"sz":[],"col":[],"ids":[],"op":[]},
                        "2-hop":    {"x":[],"y":[],"hover":[],"sz":[],"col":[],"ids":[],"op":[]},
                    }
                    for _nd2, _at2 in _subg2.nodes(data=True):
                        _kd2 = _at2.get("kind","node")
                        _lb2 = _at2.get("label", str(_nd2))
                        _x2, _y2 = _pos2[_nd2]
                        if _nd2 == node_match:
                            _ring, _sz2, _col2, _op2 = "Selected", 24, "#facc15", 1.0
                        elif _nd2 in _1hop_set:
                            _ring, _sz2, _col2, _op2 = "1-hop",    12, _C2.get(_kd2,"#a855f7"), 0.9
                        else:
                            _ring, _sz2, _col2, _op2 = "2-hop",     6, _C2.get(_kd2,"#a855f7"), 0.4
                        _r = _rings[_ring]
                        _r["x"].append(_x2); _r["y"].append(_y2)
                        _r["hover"].append(
                            f"<b>{_lb2}</b><br>Ring: {_ring}<br>"
                            f"Type: {_kd2}<br>Degree: {_deg2.get(_nd2,0)}")
                        _r["sz"].append(_sz2); _r["col"].append(_col2)
                        _r["op"].append(_op2); _r["ids"].append(_nd2)

                    for _rname, _rb in _rings.items():
                        if not _rb["x"]: continue
                        _fig_hop2.add_trace(go.Scatter(
                            x=_rb["x"], y=_rb["y"], mode="markers",
                            marker=dict(size=_rb["sz"], color=_rb["col"],
                                        opacity=_rb["op"],
                                        line=dict(width=1, color="#1e293b")),
                            hovertemplate="%{hovertext}<extra></extra>",
                            hovertext=_rb["hover"],
                            customdata=[str(_n) for _n in _rb["ids"]],
                            name=_rname, showlegend=True))

                    _fig_hop2.update_layout(
                        height=480, paper_bgcolor="#0f172a", plot_bgcolor="#0f172a",
                        xaxis=dict(showgrid=False,zeroline=False,showticklabels=False),
                        yaxis=dict(showgrid=False,zeroline=False,showticklabels=False),
                        margin=dict(l=0,r=0,t=35,b=10),
                        title=dict(
                            text=(f"2-hop network — {_subg2.number_of_nodes()} nodes · "
                                  "yellow=selected · blue=1-hop · grey=2-hop"),
                            font=dict(color="#94a3b8", size=12)),
                        hovermode="closest", clickmode="event+select",
                    )
                    _hop2_click = st.plotly_chart(_fig_hop2, use_container_width=True,
                                                  key="kg_hop2", on_select="rerun")
                    if _hop2_click and _hop2_click.get("selection",{}).get("points"):
                        _pt3 = _hop2_click["selection"]["points"][0]
                        _nid3 = _pt3.get("customdata")
                        if _nid3 and _nid3 != str(node_match):
                            st.session_state["kg_selected_node"] = _nid3
                            st.rerun()
                    st.caption("Click any node to pivot the drill-down to that entity.")
                else:
                    st.info("Not enough 2-hop connections with the current graph size — increase Parts/PO/SO limits.")

            # ── Tab 4: live SQL data ───────────────────────────────────────────
            with _tab_sql:
                try:
                    if node_kind == "part":
                        _wr = f"part_key = '{entity_id}'"
                        _rr = fetch_logical("azure_sql", "po_receipts",       top=30, where=_wr, timeout_s=25)
                        _rs = fetch_logical("azure_sql", "sales_order_lines", top=30, where=_wr, timeout_s=25)
                        if not _rr.empty and "_error" not in _rr.columns:
                            st.caption(f"📋 PO receipts — part `{entity_id}` ({len(_rr)} rows)")
                            st.dataframe(_rr, use_container_width=True, hide_index=True)
                        else:
                            st.info("No PO receipts found for this part key.")
                        if not _rs.empty and "_error" not in _rs.columns:
                            st.caption(f"🛒 SO lines — part `{entity_id}` ({len(_rs)} rows)")
                            st.dataframe(_rs, use_container_width=True, hide_index=True)
                        else:
                            st.info("No SO lines found for this part key.")

                    elif node_kind == "supplier":
                        _wr = f"supplier_key = '{entity_id}'"
                        _rr = fetch_logical("azure_sql", "po_receipts", top=50, where=_wr, timeout_s=25)
                        if not _rr.empty and "_error" not in _rr.columns:
                            st.caption(f"📋 PO receipts — supplier `{entity_id}` ({len(_rr)} rows)")
                            st.dataframe(_rr, use_container_width=True, hide_index=True)
                        else:
                            st.info("No receipts found for this supplier key.")

                    elif node_kind == "customer":
                        _wc = f"customer_key = '{entity_id}'"
                        _rc = fetch_logical("azure_sql", "sales_order_lines", top=50, where=_wc, timeout_s=25)
                        if not _rc.empty and "_error" not in _rc.columns:
                            st.caption(f"🛒 SO lines — customer `{entity_id}` ({len(_rc)} rows)")
                            st.dataframe(_rc, use_container_width=True, hide_index=True)
                        else:
                            st.info("No SO lines found for this customer key.")
                    else:
                        st.info("Live SQL queries are available for part, supplier, and customer nodes.")
                except Exception as _se:
                    st.warning(f"Live SQL query failed: {_se}")

            # ── Tab 5: cross-page findings ─────────────────────────────────────
            with _tab_find:
                from src.brain.findings_index import lookup_findings, record_finding
                _fi = lookup_findings(kind=node_kind, key=entity_id, limit=50)
                if _fi:
                    st.caption(f"**{len(_fi)} cross-page findings for this {node_kind}**")
                    _fdf = pd.DataFrame([{
                        "page":  _f["page"],
                        "score": round(float(_f["score"]), 3),
                        "when":  _f["created_at"],
                        **_f["payload"],
                    } for _f in _fi])
                    st.dataframe(_fdf, use_container_width=True, hide_index=True)
                else:
                    st.info(
                        f"No findings pinned for `{entity_id}` yet.  \n"
                        "Navigate to **EOQ Deviation**, **OTD**, or **Procurement 360** to generate findings.")
                if st.button(f"📌 Pin `{node_label}` to findings index",
                             key=f"kg_pin_{entity_id}"):
                    record_finding(
                        page="supply_chain_brain", kind=node_kind, key=entity_id,
                        score=float(exp["degree"]),
                        payload={"label": node_label, "degree": exp["degree"],
                                 "in_degree": exp["in_degree"],
                                 "out_degree": exp["out_degree"],
                                 "neighbours": len(neighbors_uniq)})
                    st.toast(f"Pinned {node_label}", icon="📌")
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

        # Score filter driven by histogram click (default = show all)
        _fi_score_min = st.session_state.get("fi_score_min", None)
        _fi_score_max = st.session_state.get("fi_score_max", None)

        if "score" in fi_df.columns and len(fi_df) > 1:
            fig_fi = px.histogram(fi_df, x="score", color="page", nbins=30,
                                  title="Finding Score Distribution — click a bar to filter the table",
                                  template="plotly",
                                  color_discrete_sequence=px.colors.qualitative.Vivid)
            fig_fi.update_layout(
                paper_bgcolor="#0f172a", plot_bgcolor="#162032", height=260,
                clickmode="event+select",
                xaxis=dict(title="Score"),
                yaxis=dict(title="Count"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            _hist_click = st.plotly_chart(fig_fi, use_container_width=True,
                                          key="fi_hist", on_select="rerun")
            if _hist_click and _hist_click.get("selection", {}).get("points"):
                _hpt = _hist_click["selection"]["points"][0]
                # histogram points carry x (bin centre) — derive ±half bin width
                _bin_x  = float(_hpt.get("x", 0))
                _bin_w  = float(fi_df["score"].max() - fi_df["score"].min()) / 30 if len(fi_df) > 1 else 1.0
                st.session_state["fi_score_min"] = _bin_x - _bin_w
                st.session_state["fi_score_max"] = _bin_x + _bin_w
                st.rerun()

        # Apply histogram filter
        _display_df = fi_df.copy()
        if _fi_score_min is not None and _fi_score_max is not None:
            _mask = (_display_df["score"] >= _fi_score_min) & (_display_df["score"] <= _fi_score_max)
            _display_df = _display_df[_mask]
            _fc1, _fc2 = st.columns([6, 1])
            _fc1.caption(
                f"🔍 Filtered to score **{_fi_score_min:.3f} – {_fi_score_max:.3f}** "
                f"· {len(_display_df)} of {len(fi_df)} findings")
            if _fc2.button("✖ Clear filter", key="fi_clear_filter"):
                st.session_state.pop("fi_score_min", None)
                st.session_state.pop("fi_score_max", None)
                st.rerun()

        st.caption(
            "**Click a row** to open the full relational drill-down for that entity in the graph above.")
        _fi_sel = st.dataframe(
            _display_df, use_container_width=True, hide_index=True,
            key="fi_table", on_select="rerun", selection_mode="single-row",
            column_config={
                "score": st.column_config.ProgressColumn(
                    "Score",
                    min_value=0,
                    max_value=float(fi_df["score"].max() or 1.0),
                    format="%.3f",
                ),
            },
        )
        _sel_rows = getattr(getattr(_fi_sel, "selection", None), "rows", []) or []
        if _sel_rows:
            _sel_finding = _display_df.iloc[_sel_rows[0]]
            _sel_key     = str(_sel_finding.get("key", ""))
            if _sel_key:
                st.session_state["kg_selected_node"] = _sel_key
                # Scroll hint — the drill-down panel renders below the graph above
                st.info(
                    f"🔗 Navigating to **{pick_kind}** `{_sel_key}` — "
                    "scroll up to the graph drill-down panel.")
                st.rerun()

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
