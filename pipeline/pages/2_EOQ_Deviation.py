"""Page 2 — EOQ centroidal deviation with Bayesian-Poisson posteriors and adaptive re-ranking."""
from pathlib import Path
import sys
import streamlit as st
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.db_registry import bootstrap_default_connectors, read_sql
from src.brain.data_access import fetch_logical, query_df
from src.brain.global_filters import date_key_window, get_global_window
from src.brain.dynamic_insight import render_dynamic_brain_insight
from src.brain.operator_shell import render_operator_sidebar_fallback
from src.brain.eoq import EOQInputs, deviation_table, LinUCBRanker
from src.brain.findings_index import record_findings_bulk, record_finding
from src.brain.label_resolver import enrich_labels, get_part_labels

# set_page_config handled by app.py st.navigation()
st.session_state["_page"] = "eoq_deviation"
render_operator_sidebar_fallback()
bootstrap_default_connectors()

import plotly.express as px
import plotly.graph_objects as go

st.markdown("## 📦 EOQ — Centroidal Deviation Analysis")
st.caption("Q\\* from lead-time-derived demand · Bayesian-Poisson posterior · LinUCB adaptive re-ranking")

# Early DBI card — renders BEFORE any SQL call so Playwright finds [data-testid="dbi-card"]
# even when Azure SQL is offline and the page falls back to demo data.
_early_eoq_ctx = {k: v for k, v in st.session_state.items()
                  if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight("EOQ Deviation Overview", _early_eoq_ctx)

@st.cache_data(ttl=1800, show_spinner="Resolving columns …")
def _resolve_eoq_columns() -> dict:
    """Hit INFORMATION_SCHEMA once per 30 min and cache all column lookups."""
    try:
        from src.brain.col_resolver import discover_table_columns, resolve
    except ImportError:
        from src.brain.col_resolver import discover_table_columns, resolve  # noqa

    def _col(logical, schema, table, fallback):
        cols = discover_table_columns("azure_sql", schema, table)
        return resolve(cols, logical) or fallback

    sol_part  = _col("part_key",    "edap_dw_replica", "fact_sales_order_line",      "part_key")
    sol_qty   = _col("quantity",    "edap_dw_replica", "fact_sales_order_line",      None)
    inv_part  = _col("part_key",    "edap_dw_replica", "fact_inventory_on_hand",     "part_key")
    inv_qty   = _col("on_hand_qty", "edap_dw_replica", "fact_inventory_on_hand",     None)
    ooo_part  = _col("part_key",    "edap_dw_replica", "fact_inventory_open_orders", "part_key")
    ooo_qty   = _col("open_qty",    "edap_dw_replica", "fact_inventory_open_orders", None)
    cost_part = _col("part_key",    "stg_replica",     "fact_part_cost",             "part_key")
    cost_col  = _col("unit_cost",   "stg_replica",     "fact_part_cost",             None)
    sol_date  = _col("order_date_key", "edap_dw_replica", "fact_sales_order_line",   "order_date_key")

    dp_cols = discover_table_columns("azure_sql", "edap_dw_replica", "dim_part")
    dp_key  = resolve(dp_cols, "part_key")        or "part_key"
    dp_desc = resolve(dp_cols, "part_description") or "part_description"

    return dict(
        sol_part=sol_part, sol_qty=sol_qty, sol_date=sol_date,
        inv_part=inv_part, inv_qty=inv_qty,
        ooo_part=ooo_part, ooo_qty=ooo_qty,
        cost_part=cost_part, cost_col=cost_col,
        dp_key=dp_key, dp_desc=dp_desc,
    )


def _build_default_sql() -> str:
    """Build EOQ SQL using cached column resolution."""
    # Use cached column resolution (30-min TTL)
    _c = _resolve_eoq_columns()
    sol_part  = _c["sol_part"]; sol_qty  = _c["sol_qty"]; sol_date = _c["sol_date"]
    inv_part  = _c["inv_part"]; inv_qty  = _c["inv_qty"]
    ooo_part  = _c["ooo_part"]; ooo_qty  = _c["ooo_qty"]
    cost_part = _c["cost_part"]; cost_col = _c["cost_col"]
    dp_key    = _c["dp_key"];   dp_desc  = _c["dp_desc"]

    sol_qty_expr = f"SUM([{sol_qty}])" if sol_qty else "COUNT(*)"
    inv_qty_expr = f"SUM([{inv_qty}])" if inv_qty else "CAST(0 AS FLOAT)"
    ooo_qty_expr = f"SUM([{ooo_qty}])" if ooo_qty else "CAST(0 AS FLOAT)"
    cost_expr    = f"AVG([{cost_col}])" if cost_col else "CAST(0 AS FLOAT)"

    _sk, _ek = date_key_window()
    _sd, _ed = get_global_window()
    window_days = max(1, (_ed - _sd).days)

    return f"""
SELECT TOP 2000
    p.[{dp_key}]                                        AS part_id,
    COALESCE(CAST(p.[{dp_desc}] AS NVARCHAR(500)), '')  AS part_description,
    COALESCE(s.shipped_qty, 0)                          AS demand_qty,
    {window_days}                                       AS periods,
    COALESCE(oh.on_hand_qty, 0)                         AS on_hand,
    COALESCE(oo.open_qty, 0)                            AS open_qty,
    COALESCE(c.unit_cost, 0)                            AS unit_cost
FROM [edap_dw_replica].[dim_part]                   p WITH (NOLOCK)
LEFT JOIN (SELECT [{sol_part}], {sol_qty_expr} AS shipped_qty
           FROM [edap_dw_replica].[fact_sales_order_line] WITH (NOLOCK)
           WHERE [{sol_date}] BETWEEN {_sk} AND {_ek}
           GROUP BY [{sol_part}]) s ON s.[{sol_part}] = p.[{dp_key}]
LEFT JOIN (SELECT [{inv_part}], {inv_qty_expr} AS on_hand_qty
           FROM [edap_dw_replica].[fact_inventory_on_hand] WITH (NOLOCK)
           GROUP BY [{inv_part}]) oh ON oh.[{inv_part}] = p.[{dp_key}]
LEFT JOIN (SELECT [{ooo_part}], {ooo_qty_expr} AS open_qty
           FROM [edap_dw_replica].[fact_inventory_open_orders] WITH (NOLOCK)
           GROUP BY [{ooo_part}]) oo ON oo.[{ooo_part}] = p.[{dp_key}]
LEFT JOIN (SELECT [{cost_part}], {cost_expr} AS unit_cost
           FROM [stg_replica].[fact_part_cost] WITH (NOLOCK)
           GROUP BY [{cost_part}]) c ON c.[{cost_part}] = p.[{dp_key}]
OPTION (RECOMPILE, MAXDOP 4)
"""


def _get_default_sql() -> str:
    """Return cached EOQ SQL — builds once per session to avoid repeated INFORMATION_SCHEMA hits."""
    # Since the SQL now relies on dynamic Global Timeline, it must rebuild whenever dates change.
    # The cache should be parameterized or disabled. We'll simply build it live.
    return _build_default_sql()


DEFAULT_SQL = _get_default_sql()

with st.expander("🔍 Refine assumptions / SQL override", expanded=False):
    c1, c2, c3 = st.columns(3)
    ordering_cost = c1.number_input("Ordering cost $/PO", 1.0, 1000.0, 75.0, key="eoq_oc")
    holding_rate  = c2.number_input("Holding rate /yr",   0.01, 1.0,  0.22,  key="eoq_hr")
    top_n         = c3.number_input("Show top N",         25, 5000,   250,   step=25, key="eoq_topn")
    custom_sql = st.text_area("Optional override SQL", height=80, key="eoq_sql")
    st.caption("💡 **SQL Requirements:** Must yield `part_id`, `demand_qty`, `periods`, `on_hand`, `open_qty`, `unit_cost`")

sql_to_run = st.session_state.get("eoq_sql","").strip() or DEFAULT_SQL

def _load(sql: str, site: str):
    from src.brain.demo_data import auto_load
    if site:
        sql = f"SELECT * FROM ({sql}) AS subq WHERE business_unit_key IN (SELECT business_unit_key FROM edap_dw_replica.dim_business_unit WITH (NOLOCK) WHERE business_unit_id = '{site}') OR business_unit_id = '{site}'"
    return auto_load(sql=sql, connector="azure_sql", timeout_s=300)

site = st.session_state.get("g_site", "")
result = _load(sql_to_run, site)

if not result.ok:
    st.warning("⚠️ **Demo mode** — Azure SQL offline. Showing synthetic EOQ data for UI preview.")
    _rng = np.random.default_rng(42)
    _n = 50
    df = pd.DataFrame({
        "part_id":          [f"DEMO-{i:04d}" for i in range(_n)],
        "part_description": [f"Demo Part {i}" for i in range(_n)],
        "demand_qty":       _rng.integers(50, 2000, _n).astype(float),
        "periods":          _rng.integers(6, 24, _n).astype(float),
        "on_hand":          _rng.integers(0, 500, _n).astype(float),
        "open_qty":         _rng.integers(0, 200, _n).astype(float),
        "unit_cost":        _rng.uniform(5.0, 500.0, _n),
    })
else:
    df = result.df

inp = EOQInputs(
    part_id_col="part_id", demand_col="demand_qty", periods_col="periods",
    on_hand_col="on_hand", open_qty_col="open_qty", unit_cost_col="unit_cost",
    periods_per_year=365.0
)
eoq_result = deviation_table(df, inp,
    ordering_cost=float(st.session_state.get("eoq_oc", 75.0)),
    holding_rate=float(st.session_state.get("eoq_hr", 0.22)))

# Enrich part_id column with human-readable label
_part_labels = get_part_labels()
eoq_result["part_label"] = (
    eoq_result["part_id"].astype(str).map(
        lambda k: _part_labels.get(k, k)
    )
)

# ── NLP-deduced part categorization (persists to local SQLite) ──────────────
try:
    from src.brain.nlp_categorize import categorize_parts
    _cat_src = df[["part_id", "part_description"]].drop_duplicates() \
        if "part_description" in df.columns else df[["part_id"]].drop_duplicates()
    _cat_src = _cat_src.rename(columns={"part_id": "part_key"})
    _cat = categorize_parts(_cat_src, key_col="part_key",
                             desc_cols=("part_description",))
    if not _cat.empty and "nlp_category" in _cat.columns:
        _cat = _cat[["part_key", "nlp_category", "nlp_confidence"]] \
            .rename(columns={"part_key": "part_id"})
        eoq_result = eoq_result.merge(_cat, on="part_id", how="left")
    if "nlp_category" not in eoq_result.columns:
        eoq_result["nlp_category"] = "Uncategorized"
except Exception as _ce:
    eoq_result["nlp_category"] = "Uncategorized"
    st.caption(f"NLP categorization unavailable: {_ce}")

# Recompute centroidal deviation *within each NLP category* so peers compare
# against true semantic peers (Steel vs Steel, Wiring vs Wiring) instead of all parts.
if "nlp_category" in eoq_result.columns and "abs_dev_z" in eoq_result.columns:
    grp = eoq_result.groupby("nlp_category")["abs_dev_z"]
    _mu = grp.transform("mean")
    _sd = grp.transform("std").replace(0, np.nan)
    eoq_result["category_dev_z"] = ((eoq_result["abs_dev_z"] - _mu) / _sd).fillna(0)

top = eoq_result.head(int(st.session_state.get("eoq_topn", 250)))
record_findings_bulk("eoq_deviation", "part",
    [{"key": r.part_id, "score": float(r.abs_dev_z),
      "payload": {"eoq": float(r.eoq), "qoh": float(r.qty_on_hand_plus_open),
                  "dollar_at_risk": float(r.dollar_at_risk)}}
     for r in top.itertuples()])

# ── KPI strip ────────────────────────────────────────────────────────────────
st.markdown(f"🟢 **Live** · {len(result.df):,} parts loaded")
# ── KPI strip (with per-metric Brain insight) ────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)

# Calculate some dynamic labels for the DBI
mean_z = float(eoq_result['abs_dev_z'].mean(skipna=True) or 0)
risk_usd = float(eoq_result['dollar_at_risk'].sum(skipna=True))
crit_count = int((eoq_result['abs_dev_z']>3).sum())
over_units = int(eoq_result['overstock_units'].sum(skipna=True))

with k1:
    st.metric("Parts Evaluated", f"{len(eoq_result):,}")
    with st.expander("🟢 Brain · Count", expanded=False):
        st.markdown(f"**Total scope:** `{len(eoq_result):,}` parts analyzed using current filters.")

with k2:
    st.metric("Mean |z| deviation", f"{mean_z:.2f}σ")
    _z_state = "🔴" if mean_z > 2 else "🟡" if mean_z > 1 else "🟢"
    with st.expander(f"{_z_state} Brain · Variance", expanded=mean_z > 1.5):
        st.markdown(
            f"**Average Deviation:** `{mean_z:.2f}σ`.  \n"
            "This measures how far on average your inventory levels sit from the theoretical "
            "Bayesian-Poisson EOQ centroid. Values above 2.0σ signal systemic misalignment."
        )

with k3:
    st.metric("Σ $ At Risk", f"${risk_usd:,.0f}")
    _risk_state = "🔴" if risk_usd > 1000000 else "🟡" if risk_usd > 100000 else "🟢"
    with st.expander(f"{_risk_state} Brain · Exposure", expanded=risk_usd > 500000):
        st.markdown(
            f"**Financial Exposure:** `${risk_usd:,.0f}`.  \n"
            "Calculated as the carrying value of inventory held in excess of the EOQ safety bound."
        )

with k4:
    st.metric("Critical (|z|>3σ)", f"{crit_count}")
    _crit_state = "🔴" if crit_count > 0 else "🟢"
    with st.expander(f"{_crit_state} Brain · Outliers", expanded=crit_count > 0):
        st.markdown(
            f"**Statistical Outliers:** `{crit_count}` parts.  \n"
            "These records deviate by more than 3 standard deviations — "
            "highly unlikely to be random noise. Prioritize these for manual review."
        )

with k5:
    st.metric("Overstock units", f"{over_units:,}")
    with st.expander("🟢 Brain · Surplus", expanded=False):
        st.markdown(
            f"**Volume Surplus:** `{over_units:,}` units.  \n"
            "The physical quantity count exceeding optimal stock parameters."
        )

st.divider()

# ── Main visualizations ──────────────────────────────────────────────────────
tab1, tab2, tab_h, tab3, tab4 = st.tabs(
    ["🎯 Deviation Scatter", "📊 Distribution", "🔥 Outlier Heatmap",
     "💰 Dollar Risk Pareto", "📋 Ranked Table"])

with tab1:
    st.subheader("EOQ Centroidal Deviation — Scatter Plot")
    ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
    render_dynamic_brain_insight("EOQ Centroidal Deviation  Scatter Plot", ctx)

    st.caption("X = EOQ optimal quantity · Y = deviation z-score · Size = $ at risk · Color = severity")
    plot_df = top.copy()
    plot_df["severity"] = pd.cut(plot_df["abs_dev_z"].fillna(0),
        bins=[-np.inf, 1, 2, 3, np.inf], labels=["Normal","Moderate","High","Critical"])
    color_map = {"Normal":"#22c55e","Moderate":"#eab308","High":"#f97316","Critical":"#ef4444"}

    fig_scatter = px.scatter(
        plot_df.fillna(0), x="eoq", y="dev_z",
        color="severity", color_discrete_map=color_map,
        size="dollar_at_risk", size_max=40,
        hover_name="part_label",
        hover_data={"eoq":":.0f","dev_z":":.2f","dollar_at_risk":"$,.0f",
                    "qty_on_hand_plus_open":":.0f","demand_hat_annual":":.0f","severity":False},
        title="EOQ Deviation by Part — Centroidal Bayesian-Poisson Analysis",
        labels={"eoq":"EOQ Quantity","dev_z":"Z-Score Deviation"},
        template="plotly",
    )
    fig_scatter.add_hline(y=3,  line_dash="dash", line_color="#ef4444",
                          annotation_text="Critical +3σ", annotation_position="left")
    fig_scatter.add_hline(y=-3, line_dash="dash", line_color="#ef4444",
                          annotation_text="Critical −3σ", annotation_position="left")
    fig_scatter.add_hline(y=0,  line_dash="dot",  line_color="#64748b")
    fig_scatter.update_layout(height=550)

    scatter_click = st.plotly_chart(fig_scatter, use_container_width=True,
                                    key="eoq_scatter", on_select="rerun")

    # drill-down
    if scatter_click and scatter_click.get("selection",{}).get("points"):
        pt = scatter_click["selection"]["points"][0]
        hname = pt.get("hovertext") or pt.get("customdata", [None])[0]
        if hname:
            part_row = top[top["part_id"].astype(str) == str(hname)]
            if not part_row.empty:
                r = part_row.iloc[0]
                st.divider()
                st.subheader(f"🔍 Part Detail: `{hname}`")
                ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
                render_dynamic_brain_insight("f Part Detail hname", ctx)

                d1,d2,d3,d4 = st.columns(4)
                d1.metric("EOQ",           f"{r.get('eoq',0):,.0f}")
                d2.metric("On-Hand+Open",  f"{r.get('qty_on_hand_plus_open',0):,.0f}")
                d3.metric("Annual Demand", f"{r.get('demand_hat_annual',0):,.0f}")
                d4.metric("$ At Risk",     f"${r.get('dollar_at_risk',0):,.0f}")

                # z-score gauge
                z = float(r.get("dev_z", 0))
                fig_gauge = go.Figure(go.Indicator(
                    mode="gauge+number+delta",
                    value=z,
                    delta={"reference": 0, "valueformat": ".2f"},
                    title={"text": "Z-Score Deviation"},
                    gauge={"axis": {"range": [-6, 6]},
                           "bar": {"color": "#38bdf8"},
                           "steps": [
                               {"range": [-6,-3],"color":"#7f1d1d"},
                               {"range": [-3,-1],"color":"#78350f"},
                               {"range": [-1, 1],"color":"#14532d"},
                               {"range": [1, 3], "color":"#78350f"},
                               {"range": [3, 6], "color":"#7f1d1d"},
                           ],
                           "threshold":{"line":{"color":"white","width":2},"value":z}},
                ))
                fig_gauge.update_layout(height=250)
                st.plotly_chart(fig_gauge, use_container_width=True)

with tab2:
    st.subheader("Z-Score Distribution")
    ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
    render_dynamic_brain_insight("Z-Score Distribution", ctx)
    fig_hist = px.histogram(
        top.fillna(0), x="abs_dev_z", nbins=50, color_discrete_sequence=["#38bdf8"],
        title="Distribution of |z| Deviations from EOQ",
        labels={"abs_dev_z":"|z| Score","count":"Parts"},
        template="plotly",
    )
    fig_hist.add_vline(x=2, line_dash="dash", line_color="#eab308", annotation_text="2σ")
    fig_hist.add_vline(x=3, line_dash="dash", line_color="#ef4444", annotation_text="3σ")
    fig_hist.update_layout(height=400)
    st.plotly_chart(fig_hist, use_container_width=True)

    fig_box = px.box(
        top.fillna(0), x="severity" if "severity" in top.columns else None,
        y="dollar_at_risk",
        color_discrete_sequence=["#f97316"],
        title="$ At Risk by Severity Band",
        template="plotly",
    )
    fig_box.update_layout(height=350)
    st.plotly_chart(fig_box, use_container_width=True)

with tab_h:
    st.subheader("🔥 Outlier Decision Heatmap")
    ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
    render_dynamic_brain_insight("Outlier Decision Heatmap", ctx)

    st.caption("Each row = a part bucket of |z|. Columns aggregate $ at risk + counts so outlier zones jump out.")
    h = top.copy().fillna(0)
    h["z_band"] = pd.cut(h["abs_dev_z"],
                          bins=[-0.01, 0.5, 1, 2, 3, 5, 1e9],
                          labels=["<0.5σ","0.5-1σ","1-2σ","2-3σ","3-5σ","≥5σ"])
    h["dollar_band"] = pd.cut(h["dollar_at_risk"],
                               bins=[-0.01, 100, 1_000, 10_000, 100_000, 1e12],
                               labels=["<$100","$100-1k","$1-10k","$10-100k","≥$100k"])
    heat_pivot = (h.pivot_table(index="z_band", columns="dollar_band",
                                values="part_id", aggfunc="count", fill_value=0)
                   .reindex(index=["<0.5σ","0.5-1σ","1-2σ","2-3σ","3-5σ","≥5σ"]))
    fig_heat = px.imshow(
        heat_pivot, color_continuous_scale="OrRd",
        title="Part Counts by |z| × $ Risk — top-right cells = highest priority",
        labels=dict(color="Parts", x="$ at Risk Band", y="|z| Band"),
        template="plotly", text_auto=True, aspect="auto",
    )
    fig_heat.update_layout(height=420)
    st.plotly_chart(fig_heat, use_container_width=True)

    # Decision quadrant: parts plotted as |z| vs $ at risk, with action zones
    st.markdown("##### Action quadrant (decision aid)")
    quad = h.copy()
    quad["action"] = np.where((quad["abs_dev_z"]>=2) & (quad["dollar_at_risk"]>=10_000), "🔴 Act now",
                     np.where((quad["abs_dev_z"]>=2),                                    "🟠 Investigate",
                     np.where((quad["dollar_at_risk"]>=10_000),                          "🟡 Watch",
                                                                                          "🟢 OK")))
    fig_quad = px.scatter(
        quad, x="abs_dev_z", y="dollar_at_risk", color="action",
        color_discrete_map={"🔴 Act now":"#ef4444","🟠 Investigate":"#f97316",
                             "🟡 Watch":"#eab308","🟢 OK":"#22c55e"},
        hover_name="part_label", log_y=True,
        title="Outlier Decision Quadrant — log $ vs |z|",
        template="plotly", labels={"abs_dev_z":"|z|","dollar_at_risk":"$ at Risk (log)"},
    )
    fig_quad.add_vline(x=2, line_dash="dash", line_color="#94a3b8")
    fig_quad.add_hline(y=10_000, line_dash="dash", line_color="#94a3b8")
    fig_quad.update_layout(paper_bgcolor="#0f172a", height=420)
    st.plotly_chart(fig_quad, use_container_width=True)
    st.dataframe(quad["action"].value_counts().rename_axis("action").reset_index(name="parts"),
                 use_container_width=True, hide_index=True)

with tab3:
    st.subheader("💰 Dollar-at-Risk Pareto")
    ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
    render_dynamic_brain_insight("Dollar-at-Risk Pareto", ctx)
    pareto = top.nlargest(50, "dollar_at_risk").fillna(0).copy()
    pareto["cumulative_pct"] = pareto["dollar_at_risk"].cumsum() / pareto["dollar_at_risk"].sum() * 100

    fig_pareto = go.Figure()
    fig_pareto.add_bar(x=pareto["part_label"].astype(str), y=pareto["dollar_at_risk"],
                       name="$ At Risk", marker_color="#f97316")
    fig_pareto.add_trace(go.Scatter(
        x=pareto["part_label"].astype(str), y=pareto["cumulative_pct"],
        name="Cumulative %", yaxis="y2",
        line=dict(color="#38bdf8", width=2), mode="lines+markers",
    ))
    fig_pareto.update_layout(
        title="Top-50 Parts by Dollar Risk (Pareto)",
        yaxis=dict(title="$ At Risk"),
        yaxis2=dict(title="Cumulative %", overlaying="y", side="right",
                    range=[0,110], ticksuffix="%"),
        xaxis=dict(tickangle=-45),
        height=500, template="plotly",
    )
    pareto_click = st.plotly_chart(fig_pareto, use_container_width=True,
                                   key="eoq_pareto", on_select="rerun")
    if pareto_click and pareto_click.get("selection",{}).get("points"):
        pt = pareto_click["selection"]["points"][0]
        pid = str(pt.get("x",""))
        if pid:
            st.session_state["eoq_drilldown_part"] = pid

    drilldown_pid = st.session_state.get("eoq_drilldown_part")
    if drilldown_pid:
        prow = top[top["part_id"].astype(str) == drilldown_pid]
        if not prow.empty:
            st.dataframe(prow.T.rename(columns={prow.index[0]:"Value"}),
                         use_container_width=True)

with tab4:
    st.subheader("📋 Ranked Deviations — Worst First")
    ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
    render_dynamic_brain_insight("Ranked Deviations  Worst First", ctx)

    st.caption("Rows auto-sorted by |z|; resolving items triggers LinUCB re-rank below.")

    disp = top[["part_label","part_id","abs_dev_z","dev_z","eoq","qty_on_hand_plus_open",
                "demand_hat_annual","overstock_units","understock_units","dollar_at_risk"]].copy()
    st.dataframe(
        disp.fillna(0),
        use_container_width=True, hide_index=True,
        column_config={
            "part_label":     st.column_config.TextColumn("Part"),
            "part_id":        st.column_config.TextColumn("Part ID"),
            "abs_dev_z":      st.column_config.NumberColumn("|z|", format="%.2f"),
            "dollar_at_risk": st.column_config.NumberColumn("$ At Risk", format="$%.0f"),
            "eoq":            st.column_config.NumberColumn("EOQ", format="%.0f"),
        },
    )

# ── LinUCB re-ranking ─────────────────────────────────────────────────────────
st.divider()
st.subheader("🎯 Adaptive Re-Ranking · LinUCB")
st.caption("Log resolutions to reshape ranking toward highest-return opportunities.")

ranker = st.session_state.setdefault("_eoq_ranker", LinUCBRanker(dim=4, alpha=1.0))
with st.form("linucb_update"):
    c1, c2, c3 = st.columns(3)
    pid    = c1.text_input("Resolved part_id")
    rec    = c2.number_input("Realized $ recovery", 0.0, 1e7, 0.0, step=100.0)
    bucket = c3.selectbox("Outcome", ["fixed","noop","deferred"])
    submitted = st.form_submit_button("📊 Log & Re-rank")

if submitted and pid:
    row = top[top["part_id"].astype(str) == str(pid)]
    if row.empty:
        st.warning("Part not in current top-N.")
    else:
        x = row[["abs_dev_z","dollar_at_risk","demand_hat_annual",
                 "qty_on_hand_plus_open"]].iloc[0].fillna(0).to_numpy(dtype=float)
        ranker.update(str(pid), x, float(rec))
        record_finding("eoq_deviation","resolution", str(pid), score=float(rec),
                       payload={"outcome": bucket,"recovery": float(rec)})
        rer = ranker.rerank(top, ["abs_dev_z","dollar_at_risk",
                                  "demand_hat_annual","qty_on_hand_plus_open"])
        st.success(f"✅ Logged. New rank for top 20:")
        fig_rer = px.bar(rer.head(20), x="part_label", y="abs_dev_z",
                         color="dollar_at_risk", color_continuous_scale="Oranges",
                         title="Re-ranked Top-20 (post-resolution)",
                         template="plotly",
                         labels={"abs_dev_z":"|z|","part_id":"Part"})
        fig_rer.update_layout(paper_bgcolor="#0f172a", height=350)
        st.plotly_chart(fig_rer, use_container_width=True)
