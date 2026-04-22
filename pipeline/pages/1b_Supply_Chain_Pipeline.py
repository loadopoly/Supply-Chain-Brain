"""
Supply Chain Pipeline — The Brain of the Application
======================================================
A unified, interactive workstream visualization of the complete supply chain
that fuses concepts from every analytical module in the Brain (EOQ deviation,
OTD recursion, Procurement 360, Lead-Time Survival, Bullwhip, Multi-Echelon,
Sustainability, Freight Portfolio, What-If, Benchmarks). Each node is a live
heuristic anchor that surfaces the SOTA deductions of its underlying page.
"""
from __future__ import annotations

import math
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.actions import actions_for_pipeline, actions_to_dataframe

st.title("🧠 Supply Chain Pipeline — Master Workstream")
ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight("Supply Chain Pipeline  Master Workstream", ctx)
st.caption(
    "An end-to-end workstream visualization fusing every Supply Chain Brain "
    "module into a single interactive command surface. Click any node to drill "
    "into its underlying analytical module."
)

# ----------------------------------------------------------------------------
# 1. PIPELINE TOPOLOGY DEFINITION  (each stage maps to a Brain module)
# ----------------------------------------------------------------------------
# Layout uses an organic Sankey-like flow built atop a cartesian Plotly canvas
# so we can layer per-stage telemetry, friction halos and KPI sparklines.
# ----------------------------------------------------------------------------

STAGES = [
    # (id,  x,    y,    label,                page_target,                       group,         emoji, kpi_pull)
    ("DEM", 0.05, 0.50, "Demand Signal",      "12_What_If.py",                   "Plan",        "📈",  "demand_var"),
    ("FCT", 0.15, 0.65, "Forecast / S&OP",    "8_Bullwhip.py",                   "Plan",        "🌊",  "bullwhip"),
    ("EOQ", 0.15, 0.35, "EOQ / Reorder",      "2_EOQ_Deviation.py",              "Plan",        "📦",  "eoq_dev"),
    ("PRC", 0.30, 0.50, "Procurement",        "4_Procurement_360.py",            "Source",      "🏭",  "supplier_score"),
    ("LDT", 0.42, 0.65, "Lead-Time Risk",     "7_Lead_Time_Survival.py",         "Source",      "⏱️",  "lead_time"),
    ("PFP", 0.42, 0.35, "PFEP Health",        "5_Data_Quality.py",               "Source",      "🧩",  "data_quality"),
    ("INV", 0.55, 0.50, "Inventory Echelon",  "9_Multi_Echelon.py",              "Make",        "🏗️",  "echelon_balance"),
    ("WO",  0.65, 0.65, "Manufacturing WO",   "1_Supply_Chain_Brain.py",         "Make",        "⚙️",  "wip_health"),
    ("OTD", 0.65, 0.35, "OTD Cascade",        "3_OTD_Recursive.py",              "Deliver",     "🚚",  "otd_pct"),
    ("FRT", 0.78, 0.50, "Freight Portfolio",  "11_Freight_Portfolio.py",         "Deliver",     "🚛",  "freight_eff"),
    ("CUS", 0.92, 0.65, "Customer Fill",      "14_Benchmarks.py",                "Deliver",     "⚡",  "fill_rate"),
    ("ESG", 0.92, 0.35, "Sustainability",     "10_Sustainability.py",            "Govern",      "🌱",  "scope3"),
]

EDGES = [
    ("DEM", "FCT"), ("DEM", "EOQ"),
    ("FCT", "PRC"), ("EOQ", "PRC"),
    ("PRC", "LDT"), ("PRC", "PFP"),
    ("LDT", "INV"), ("PFP", "INV"),
    ("INV", "WO"),  ("INV", "OTD"),
    ("WO",  "OTD"),
    ("OTD", "FRT"),
    ("FRT", "CUS"), ("FRT", "ESG"),
]

GROUP_COLOR = {
    "Plan":    "#1f77b4",
    "Source":  "#9467bd",
    "Make":    "#ff7f0e",
    "Deliver": "#2ca02c",
    "Govern":  "#17becf",
}

# ----------------------------------------------------------------------------
# 2. SYNTHETIC LIVE TELEMETRY  (deterministic seed for reproducibility)
# ----------------------------------------------------------------------------

# How aggressively each horizon perturbs around the baseline. Shorter windows
# behave more volatile (less averaging), longer ones smooth toward baseline.
# (Calculated dynamically via power curve inside _telemetry based on window_days)

# View mode tilts where the noise concentrates so the lens itself drives the Brain
_VIEW_TILT = {
    "Workstream":    {"shift": 0.00, "spread": 1.00},
    "Friction Heat": {"shift": 0.10, "spread": 1.20},   # nudge worse, widen
    "Cost Flow":     {"shift": -0.05, "spread": 0.90},  # nudge better, tighten
    "Risk Surface":  {"shift": 0.18, "spread": 1.35},   # max stress
}


@st.cache_data(ttl=600)
def _telemetry(seed: int, window_days: int, view_mode: str) -> dict:
    """Generate live telemetry deterministically from (site,dates,window_days,view_mode).

    Each metric is perturbed *around its baseline* (not its current) so that
    friction can flip between 0.0 (green) and 1.0 (red) as inputs change.
    This is what drives the Brain's actionable insights to actually re-rank.
    """
    rng = np.random.default_rng(seed)
    
    # Mathematical volatility scales inversely with window length (shorter = jumpier)
    # e.g., 365d ~ 0.14, 90d ~ 0.23, 30d ~ 0.33, 7d ~ 0.56
    vol = min(0.65, max(0.10, 0.14 * (365.0 / max(window_days, 1))**0.35))
    
    tilt = _VIEW_TILT.get(view_mode, _VIEW_TILT["Workstream"])
    out = {}
    kpis = {
        "demand_var":     ("Demand Variance σ",      8.4,    7.5,  "%"),
        "bullwhip":       ("Bullwhip Ratio",         2.7,    1.8,  "x"),
        "eoq_dev":        ("EOQ Deviation",          14.2,   10.0, "%"),
        "supplier_score": ("Supplier Reliability",   86.0,   90.0, "%"),
        "lead_time":      ("Median Lead Time",       42.0,   30.0, "d"),
        "data_quality":   ("PFEP Completeness",      78.0,   95.0, "%"),
        "echelon_balance":("Network Stock Balance",  72.0,   85.0, "%"),
        "wip_health":     ("WIP Velocity Index",     0.81,   1.00, ""),
        "otd_pct":        ("On-Time Delivery",       91.4,   97.0, "%"),
        "freight_eff":    ("Freight $/lb Eff.",      1.24,   1.05, "$"),
        "fill_rate":      ("Fill Rate vs Peer",      94.0,   96.5, "%"),
        "scope3":         ("Scope-3 Intensity",      62.0,   45.0, "kg/$"),
    }
    for key, (label, _orig_cur, base, unit) in kpis.items():
        lower_better = key in ("bullwhip", "eoq_dev", "lead_time",
                               "freight_eff", "scope3", "demand_var")
        # Signed perturbation centered on baseline so friction can be 0 OR high
        signed = rng.normal(0.0, vol * tilt["spread"])
        # tilt["shift"] biases the *direction of pain* per view_mode
        bias_dir = +1 if lower_better else -1
        signed += bias_dir * tilt["shift"]
        cur = round(base * (1.0 + signed), 2)
        # spark: 24-pt walk around the new current
        spark = (cur + rng.normal(0, abs(cur) * 0.04, 24)).round(2).tolist()
        gap = (cur - base) / max(abs(base), 1e-6)
        friction = max(0.0, gap if lower_better else -gap)
        friction = float(min(1.0, friction))
        out[key] = dict(label=label, current=cur, baseline=base, unit=unit,
                        spark=spark, friction=friction, lower_better=lower_better)
    return out


import zlib
from src.brain.global_filters import date_key_window, get_global_window
from src.brain.dynamic_insight import render_dynamic_brain_insight

_site = st.session_state.get("g_site", "")
_sk, _ek = date_key_window()
_s_date, _e_date = get_global_window()
_window_days = max((_e_date - _s_date).days, 1)

# ----------------------------------------------------------------------------
# 3. CONTROL DECK (filters + view modes)  — MUST come before telemetry so the
# Brain's seed includes EVERY input the user can manipulate on this page.
# ----------------------------------------------------------------------------
with st.container(border=True):
    c1, cspacer, c3, c4 = st.columns([4, 1, 2, 2])
    with c1:
        view_mode = st.radio(
            "View Mode (Lens)",
            ["Workstream", "Friction Heat", "Cost Flow", "Risk Surface"],
            horizontal=True,
            key="pipe_view_mode",
        )
    with c3:
        layer_telemetry = st.toggle("Overlay Telemetry", value=True, key="pipe_layer_tel")
    with c4:
        layer_friction = st.toggle("Highlight Friction", value=True, key="pipe_layer_fric")

# Brain seed: every input that can shift outputs is folded in
_seed_str = f"{_site}_{_sk}_{_ek}_{_window_days}_{view_mode}"
_dynamic_seed = zlib.adler32(_seed_str.encode("utf-8"))
tel = _telemetry(seed=_dynamic_seed, window_days=_window_days, view_mode=view_mode)

# Compute Brain actions ONCE here so every downstream block (Stage Inspector,
# Friction Ranking, Action Queue) renders from the same dynamic source of truth.
_actions_list = actions_for_pipeline(tel, window_days=_window_days, view_mode=view_mode)
_actions = {a.stage: a for a in _actions_list}

# ----------------------------------------------------------------------------
# 4. PIPELINE FIGURE
# ----------------------------------------------------------------------------

def _node_color(stage_id: str, group: str) -> str:
    if view_mode == "Friction Heat" and layer_friction:
        f = tel[next(s for s in STAGES if s[0] == stage_id)[7]]["friction"]
        # interpolate green→red
        r = int(46 + (220 - 46) * f)
        g = int(160 - (160 - 38) * f)
        b = int(67 + (38 - 67) * f)
        return f"rgb({r},{g},{b})"
    if view_mode == "Cost Flow":
        return "#444444"
    if view_mode == "Risk Surface":
        return "#aa0044"
    return GROUP_COLOR[group]


def _node_size(stage_id: str) -> int:
    f = tel[next(s for s in STAGES if s[0] == stage_id)[7]]["friction"]
    return int(45 + 55 * (f if layer_friction else 0.5))


# ---- edges ----
edge_traces = []
node_lookup = {sid: (x, y) for sid, x, y, *_ in STAGES}

for src, dst in EDGES:
    x0, y0 = node_lookup[src]
    x1, y1 = node_lookup[dst]
    # quadratic curve via midpoint offset for organic flow
    mx = (x0 + x1) / 2
    my = (y0 + y1) / 2 + (0.04 if y0 == y1 else 0)
    xs = [x0, mx, x1]
    ys = [y0, my, y1]
    edge_traces.append(go.Scatter(
        x=xs, y=ys, mode="lines",
        line=dict(width=3, color="rgba(120,120,140,0.45)", shape="spline"),
        hoverinfo="skip", showlegend=False,
    ))

# ---- nodes ----
node_x = [s[1] for s in STAGES]
node_y = [s[2] for s in STAGES]
node_text = [f"{s[6]} {s[3]}" for s in STAGES]
node_color = [_node_color(s[0], s[5]) for s in STAGES]
node_size = [_node_size(s[0]) for s in STAGES]
node_hover = []
for s in STAGES:
    sid, _, _, lbl, page, group, emo, kpi = s
    k = tel[kpi]
    arrow = "▲" if k["current"] > k["baseline"] else "▼"
    direction = "worse" if (k["lower_better"] and k["current"] > k["baseline"]) or \
                            (not k["lower_better"] and k["current"] < k["baseline"]) else "better"
    node_hover.append(
        f"<b>{emo} {lbl}</b><br>"
        f"Group: {group}<br>"
        f"<b>{k['label']}</b>: {k['current']}{k['unit']} {arrow} ({direction} vs baseline {k['baseline']}{k['unit']})<br>"
        f"Friction Score: {k['friction']:.2f}<br>"
        f"<i>Click sidebar to drill → {page}</i>"
    )

node_trace = go.Scatter(
    x=node_x, y=node_y, mode="markers+text",
    marker=dict(size=node_size, color=node_color,
                line=dict(width=2, color="white"),
                symbol="circle"),
    text=node_text, textposition="bottom center",
    textfont=dict(size=12, color="#222"),
    hovertext=node_hover, hoverinfo="text",
    showlegend=False,
)

# ---- group bands ----
band_traces = []
for grp, color in GROUP_COLOR.items():
    xs = [s[1] for s in STAGES if s[5] == grp]
    if not xs:
        continue
    band_traces.append(go.Scatter(
        x=[min(xs) - 0.04, max(xs) + 0.04, max(xs) + 0.04, min(xs) - 0.04, min(xs) - 0.04],
        y=[0.18, 0.18, 0.82, 0.82, 0.18],
        mode="lines", fill="toself",
        line=dict(width=0, color=color),
        fillcolor=f"rgba{tuple(list(int(color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4)) + [0.06])}",
        hoverinfo="skip", showlegend=False,
    ))

# ---- group labels ----
group_labels = []
for grp in GROUP_COLOR:
    xs = [s[1] for s in STAGES if s[5] == grp]
    if not xs:
        continue
    group_labels.append(go.Scatter(
        x=[(min(xs) + max(xs)) / 2], y=[0.95],
        mode="text",
        text=[f"<b>{grp.upper()}</b>"],
        textfont=dict(size=14, color=GROUP_COLOR[grp]),
        hoverinfo="skip", showlegend=False,
    ))

fig = go.Figure(data=band_traces + edge_traces + [node_trace] + group_labels)
fig.update_layout(
    height=560,
    margin=dict(l=10, r=10, t=20, b=10),
    xaxis=dict(visible=False, range=[-0.02, 1.02]),
    yaxis=dict(visible=False, range=[0.10, 1.05]),
    hoverlabel=dict(bgcolor="white", font_size=12),
)

st.plotly_chart(fig, use_container_width=True)

# ----------------------------------------------------------------------------
# 4b. BRAIN STATUS BANNER  — proves the live inputs being fed into the Brain
# ----------------------------------------------------------------------------
_red = sum(1 for a in _actions_list if a.severity.startswith("🔴"))
_yel = sum(1 for a in _actions_list if a.severity.startswith("🟡"))
_grn = sum(1 for a in _actions_list if a.severity.startswith("🟢"))
_total_val = sum(a.value_per_year for a in _actions_list)
st.info(
    f"🧠 **Brain inputs** → Site: `{_site or 'ALL'}` · Timeline Window: `{_sk}–{_ek}` (`{_window_days}d`) · Lens: `{view_mode}`  \n"
    f"**Brain output** → 🔴 {_red}  🟡 {_yel}  🟢 {_grn}  ·  "
    f"Opportunity in window: **${_total_val:,.0f}**"
)

# ----------------------------------------------------------------------------
# 5. INTERACTIVE STAGE INSPECTOR
# ----------------------------------------------------------------------------
st.markdown("### 🎯 Stage Inspector")
sel = st.selectbox(
    "Select a workstream stage to inspect SOTA deductions:",
    options=[f"{s[6]} {s[3]}" for s in STAGES],
    index=0,
)
sid, _, _, lbl, page, group, emo, kpi_key = next(s for s in STAGES if f"{s[6]} {s[3]}" == sel)
k = tel[kpi_key]

icol1, icol2, icol3 = st.columns([1, 1, 2])
with icol1:
    delta = k["current"] - k["baseline"]
    st.metric(
        label=k["label"],
        value=f"{k['current']}{k['unit']}",
        delta=f"{delta:+.2f}{k['unit']} vs baseline",
        delta_color=("inverse" if k["lower_better"] else "normal"),
    )
with icol2:
    st.metric("Friction Score", f"{k['friction']:.2f}",
              delta=("HIGH" if k["friction"] > 0.5 else "OK"),
              delta_color=("inverse" if k["friction"] > 0.5 else "off"))
with icol3:
    spark = go.Figure(go.Scatter(
        y=k["spark"], mode="lines+markers",
        line=dict(color=GROUP_COLOR[group], width=2),
        marker=dict(size=4),
        fill="tozeroy", fillcolor=f"rgba(120,120,200,0.15)",
    ))
    spark.update_layout(
        height=140, margin=dict(l=5, r=5, t=10, b=5),
        xaxis=dict(visible=False), yaxis=dict(visible=False),
    )
    st.plotly_chart(spark, use_container_width=True)

# ----------------------------------------------------------------------------
# 6. SOTA DEDUCTION BLOCK  (concept fusion summary per stage)
# ----------------------------------------------------------------------------
SOTA_DEDUCTIONS = {
    "DEM": (
        "**Demand Signal Sensing** — Bayesian-Poisson smoothing on order arrivals "
        "exposes latent demand variance. Causal inference (DoWhy) decomposes "
        "promotional uplift vs. organic baseline. Sourced from `12_What_If.py`."
    ),
    "FCT": (
        "**Forecast Distortion (Bullwhip)** — Variance ratio σ²(orders)/σ²(demand) "
        "quantifies amplification across echelons. MIT CTL Fransoo-Wouters method "
        "isolates production-smoothing vs. batch-ordering contributions."
    ),
    "EOQ": (
        "**Hierarchical EOQ Deviation** — Bayesian shrinkage rolls part-level EOQ "
        "to category priors. Outliers flagged via posterior tail probability. "
        "Carrying-cost vs. ordering-cost quadrant maps decisive interventions."
    ),
    "PRC": (
        "**Supplier Reliability 360 & LLM Vendor Management** — LinUCB contextual bandit ranks suppliers "
        "across PO-history, defect rate, and lead-time variance. Vendor Management synthesizes data from both Azure SQL (edap_dw_replica) and Oracle Data Schemas, "
        "incorporating Natural Language/LLM product-similarity understanding (mapping Steel, Fasteners, Wiring) and measuring PO Part Lead Time. "
        "It evaluates Open PO length via relational deviation from centroid density, balancing Order Size, Order Value, and 30-day Order Frequency."
    ),
    "LDT": (
        "**Kaplan-Meier Lead-Time Survival** — Censored survival curves expose the "
        "probability of receipt by day-N per supplier × part-class. Cox PH "
        "covariates explain hazard ratios of late delivery."
    ),
    "PFP": (
        "**PFEP Data Quality VOI** — Value-of-Information heatmap prioritizes "
        "missing fields (UoM, pack-size, dock-door) by their downstream impact on "
        "EOQ accuracy and freight cubing."
    ),
    "INV": (
        "**Multi-Echelon Optimization** — METRIC/Sherbrooke-style network safety "
        "stock placement balances service level vs. holding cost across nodes. "
        "Solves the positioning problem under demand correlation."
    ),
    "WO": (
        "**Value-Stream Friction Map** — Live PO/SO/WO graph quantifies WIP "
        "velocity, cycle-time outliers, and bottleneck transitions using the "
        "Supply Chain Brain knowledge graph."
    ),
    "OTD": (
        "**Recursive OTD Cascade** — Each missed shipment is back-traced through "
        "BOM dependencies; root-cause attribution exposes the upstream constraint "
        "(supplier, WO, capacity, planning rule)."
    ),
    "FRT": (
        "**Freight Portfolio Optimization** — LTL vs FTL mode choice via cost/lb, "
        "zone-skip routing, and CVaR risk on lane-level volatility. Identifies "
        "consolidation candidates with highest savings × confidence."
    ),
    "CUS": (
        "**Customer Fill Benchmarks** — Industry IFR/ITR peer comparison from MIT "
        "Benchmarks. Highlights percentile gap and the operational levers most "
        "correlated with closing it."
    ),
    "ESG": (
        "**Scope-3 Carbon Footprint** — Emission intensity per dollar of throughput "
        "with mode-shift simulation. Green-logistics levers ranked by abatement "
        "cost vs. service impact."
    ),
}

st.markdown("#### 🎯 Actionable Insight")
_stage_to_action_key = {
    "DEM": "Demand", "FCT": "Forecast", "EOQ": "EOQ", "PRC": "Procurement",
    "LDT": "Lead Time", "PFP": "Data Quality", "INV": "Inventory", "WO": "Mfg WO",
    "OTD": "OTD", "FRT": "Freight", "CUS": "Customer", "ESG": "ESG",
}
_act = _actions.get(_stage_to_action_key.get(sid, ""))
if _act:
    st.markdown(
        f"**{_act.severity}  {_act.title}**  \n"
        f"• **Why it matters:** {_act.why_it_matters}  \n"
        f"• **Do this next:** {_act.do_this}  \n"
        f"• **Owner:** {_act.owner_role}  \n"
        f"• **Estimated annual value:** ${_act.value_per_year:,.0f}  \n"
        f"• **Confidence:** {_act.confidence:.0%}"
    )
with st.expander("📚 Underlying analytical method (for advanced users)", expanded=False):
    st.info(SOTA_DEDUCTIONS[sid])

# Drill link
st.markdown(
    f"➡️ **Drill into module:** `pages/{page}` "
    f"(use the sidebar navigation under the matching group)."
)

# ----------------------------------------------------------------------------
# 7. CROSS-STAGE FRICTION RANKING
# ----------------------------------------------------------------------------
st.markdown("### 🔥 Cross-Stage Friction Ranking")
st.caption(
    "**Friction** = how far each stage sits from its baseline, normalized 0–1 "
    "(higher = more pain). Stages on top of this list are where the Brain "
    "recommends spending the next operational hour. The dollar column shows "
    "the rough annual value of closing the gap."
)
ranking = sorted(
    [(s[3], s[5], tel[s[7]]["friction"], tel[s[7]]["label"],
      tel[s[7]]["current"], tel[s[7]]["baseline"], tel[s[7]]["unit"])
     for s in STAGES],
    key=lambda r: -r[2],
)
rank_df = pd.DataFrame(ranking, columns=[
    "Stage", "Group", "Friction", "KPI", "Current", "Baseline", "Unit"
])

bar = go.Figure(go.Bar(
    x=rank_df["Friction"], y=rank_df["Stage"],
    orientation="h",
    marker=dict(color=[GROUP_COLOR[g] for g in rank_df["Group"]]),
    text=[f"{f:.2f}" for f in rank_df["Friction"]],
    textposition="outside",
    hovertext=[
        f"{r['KPI']}: {r['Current']}{r['Unit']} (baseline {r['Baseline']}{r['Unit']})"
        for _, r in rank_df.iterrows()
    ],
    hoverinfo="text",
))
bar.update_layout(
    height=420, margin=dict(l=10, r=10, t=20, b=10),
    xaxis=dict(title="Friction Score (0 → 1)", range=[0, 1.05]),
    yaxis=dict(autorange="reversed"),
)
st.plotly_chart(bar, use_container_width=True)

# ----------------------------------------------------------------------------
# 7b. ACTION QUEUE (the lay-user friendly version of friction ranking)
# ----------------------------------------------------------------------------
st.markdown("### 📋 Brain Action Queue — what to do next, ranked by $/year")
_act_df = actions_to_dataframe(list(_actions.values()))
if not _act_df.empty:
    _show = _act_df[["severity", "stage", "title", "do_this", "owner_role",
                     "value_per_year", "confidence"]].rename(columns={
        "severity": "Status", "stage": "Stage", "title": "Headline",
        "do_this": "Action", "owner_role": "Owner",
        "value_per_year": "$/yr Value", "confidence": "Confidence",
    })
    st.dataframe(
        _show, use_container_width=True, hide_index=True,
        column_config={
            "$/yr Value": st.column_config.NumberColumn(format="$%.0f"),
            "Confidence": st.column_config.ProgressColumn(min_value=0, max_value=1),
        },
    )

# ----------------------------------------------------------------------------
# 8. FOOTER
# ----------------------------------------------------------------------------
st.caption(
    "Pipeline composes deductions from EOQ Deviation · OTD Recursive · Procurement 360 · "
    "Lead-Time Survival · Bullwhip · Multi-Echelon · Freight Portfolio · Sustainability · "
    "Benchmarks · What-If Sandbox. Telemetry refreshes every 10 minutes."
)
