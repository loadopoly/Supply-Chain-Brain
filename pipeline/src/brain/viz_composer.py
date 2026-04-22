"""
Viz Composer — turn a MissionResult into a dict of Plotly figures keyed
by viz-intent (kpi_trend, pareto, heatmap_matrix, network, sankey_flow,
cohort_survival). The composer never invents data; it only renders what
the orchestrator already collected.

Each figure ships with a short caption (`fig._caption`, monkey-patched)
that the deck builders pull as the slide title supplement.
"""
from __future__ import annotations

from typing import Any
import math

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _annotate(fig: go.Figure, caption: str) -> go.Figure:
    setattr(fig, "_caption", caption)
    fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), template="plotly_white")
    return fig


def _findings_for(findings: list[dict], kind: str) -> list[dict]:
    return [f for f in findings if f.get("kind") == kind]


def _outcome_metrics(result, scope_tag: str) -> dict[str, Any]:
    for o in getattr(result, "outcomes", []):
        if o.scope_tag == scope_tag and o.ok:
            return o.metrics or {}
    return {}


# ---------------------------------------------------------------------------
# Individual viz builders
# ---------------------------------------------------------------------------
def _kpi_trend(result) -> go.Figure | None:
    """Bar chart of every numeric KPI in the snapshot."""
    snap = getattr(result, "kpi_snapshot", {}) or {}
    if not snap:
        return None
    items = [(k, float(v)) for k, v in snap.items()
             if isinstance(v, (int, float)) and math.isfinite(float(v))]
    if not items:
        return None
    items.sort(key=lambda x: abs(x[1]), reverse=True)
    items = items[:12]
    labels = [k for k, _ in items]
    values = [v for _, v in items]
    fig = go.Figure(go.Bar(x=values, y=labels, orientation="h",
                           marker_color="#164E87"))
    fig.update_layout(title="Mission KPI snapshot", height=380,
                      yaxis=dict(autorange="reversed"))
    return _annotate(fig, "Current value of each KPI tracked by this mission.")


def _pareto(result) -> go.Figure | None:
    """Pareto of the top-scoring findings across kinds."""
    findings = list(getattr(result, "findings", []) or [])
    if not findings:
        return None
    rows = sorted(
        ({"label": f.get("key", ""), "kind": f.get("kind", ""),
          "score": float(f.get("score") or 0)} for f in findings),
        key=lambda r: r["score"], reverse=True,
    )[:25]
    if not rows:
        return None
    df = pd.DataFrame(rows)
    fig = px.bar(df, x="label", y="score", color="kind",
                 title="Top findings by score (Pareto)")
    fig.update_layout(xaxis_tickangle=-40, height=420)
    return _annotate(fig, "The 25 highest-scoring findings produced by the mission's analyzers.")


def _heatmap_matrix(result) -> go.Figure | None:
    """Heatmap of finding score by (kind × top-key)."""
    findings = list(getattr(result, "findings", []) or [])
    if not findings:
        return None
    df = pd.DataFrame([{"kind": f.get("kind", ""), "key": f.get("key", ""),
                        "score": float(f.get("score") or 0)} for f in findings])
    if df.empty:
        return None
    # Limit to top 15 keys per kind so the matrix stays readable.
    top = (df.sort_values("score", ascending=False)
             .groupby("kind").head(15))
    pivot = top.pivot_table(index="key", columns="kind", values="score",
                            aggfunc="max", fill_value=0)
    if pivot.shape[0] < 2 or pivot.shape[1] < 1:
        return None
    fig = go.Figure(go.Heatmap(z=pivot.values, x=list(pivot.columns),
                               y=list(pivot.index), colorscale="Blues",
                               colorbar=dict(title="score")))
    fig.update_layout(title="Finding intensity by kind × key",
                      height=max(360, 18 * pivot.shape[0]))
    return _annotate(fig, "Where the hottest signals concentrate across analyzer outputs.")


def _network(result) -> go.Figure | None:
    """Tiny scope-tag → analyzer bipartite layout (always-on diagnostic)."""
    outcomes = list(getattr(result, "outcomes", []) or [])
    if not outcomes:
        return None
    tags = sorted({o.scope_tag for o in outcomes})
    analyzers = [o.analyzer for o in outcomes]
    # Place tags on left, analyzers on right.
    xs, ys, texts, colors = [], [], [], []
    pos: dict[str, tuple[float, float]] = {}
    for i, t in enumerate(tags):
        x, y = 0.0, 1.0 - (i + 0.5) / max(1, len(tags))
        pos[("tag", t)] = (x, y)
        xs.append(x); ys.append(y); texts.append(t); colors.append("#164E87")
    for i, o in enumerate(outcomes):
        x, y = 1.0, 1.0 - (i + 0.5) / max(1, len(outcomes))
        pos[("ana", o.analyzer)] = (x, y)
        xs.append(x); ys.append(y); texts.append(o.analyzer)
        colors.append("#1F763C" if o.ok else "#A61B1B")
    edge_x: list[float] = []; edge_y: list[float] = []
    for o in outcomes:
        if ("tag", o.scope_tag) in pos and ("ana", o.analyzer) in pos:
            x0, y0 = pos[("tag", o.scope_tag)]
            x1, y1 = pos[("ana", o.analyzer)]
            edge_x += [x0, x1, None]; edge_y += [y0, y1, None]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=edge_x, y=edge_y, mode="lines",
                             line=dict(color="#6B7280", width=1),
                             hoverinfo="skip", showlegend=False))
    fig.add_trace(go.Scatter(x=xs, y=ys, mode="markers+text", text=texts,
                             textposition="middle right",
                             marker=dict(size=14, color=colors),
                             showlegend=False))
    fig.update_layout(title="Mission scope → analyzer dispatch",
                      xaxis=dict(visible=False, range=[-0.2, 1.6]),
                      yaxis=dict(visible=False, range=[-0.05, 1.05]),
                      height=max(320, 32 * max(len(tags), len(outcomes))))
    return _annotate(fig, "Which analyzers fired for each scope tag (green=ok, red=failed).")


def _sankey_flow(result) -> go.Figure | None:
    """Sankey: scope_tag → analyzer → finding-count buckets."""
    outcomes = [o for o in getattr(result, "outcomes", []) if o.ok]
    if not outcomes:
        return None
    tags = sorted({o.scope_tag for o in outcomes})
    analyzers = sorted({o.analyzer for o in outcomes})
    nodes = tags + analyzers
    idx = {n: i for i, n in enumerate(nodes)}
    src, tgt, val = [], [], []
    for o in outcomes:
        n = max(1, int(o.n_findings or 0))
        src.append(idx[o.scope_tag]); tgt.append(idx[o.analyzer]); val.append(n)
    fig = go.Figure(go.Sankey(
        node=dict(label=nodes, pad=12, thickness=14,
                  color=["#164E87"] * len(tags) + ["#1F763C"] * len(analyzers)),
        link=dict(source=src, target=tgt, value=val, color="#cfd8e3"),
    ))
    fig.update_layout(title="Findings flow: scope → analyzer", height=360)
    return _annotate(fig, "How many findings each analyzer contributed under each scope.")


def _cohort_survival(result) -> go.Figure | None:
    """Use lead_time metrics to draw a synthetic survival curve when present."""
    metrics = _outcome_metrics(result, "lead_time")
    if not metrics or "lead_time_p90" not in metrics:
        return None
    median = float(metrics.get("lead_time_median") or 0)
    p90 = float(metrics.get("lead_time_p90") or 0)
    if p90 <= 0:
        return None
    # Fit a crude exponential survival S(t) = exp(-t/median) for visualization.
    import numpy as np
    t = np.linspace(0, p90 * 1.4, 60)
    s = np.exp(-t / max(1.0, median))
    fig = go.Figure(go.Scatter(x=t, y=s, mode="lines", line=dict(color="#164E87")))
    fig.add_vline(x=median, line_dash="dot", annotation_text=f"median={median:.0f}d")
    fig.add_vline(x=p90, line_dash="dot", annotation_text=f"p90={p90:.0f}d")
    fig.update_layout(title="Lead-time survival (exponential proxy)",
                      xaxis_title="days", yaxis_title="P(receipt later than t)",
                      height=340)
    return _annotate(fig, "Synthetic survival curve from observed median/p90 lead time.")


# ---------------------------------------------------------------------------
# Orchestrator entry point
# ---------------------------------------------------------------------------
_BUILDERS = {
    "kpi_trend":         _kpi_trend,
    "pareto":            _pareto,
    "heatmap_matrix":    _heatmap_matrix,
    "network":           _network,
    "sankey_flow":       _sankey_flow,
    "cohort_survival":   _cohort_survival,
}


def compose(result) -> dict[str, go.Figure]:
    """Return {viz_intent → Figure} for every viz that found data."""
    out: dict[str, go.Figure] = {}
    for intent, builder in _BUILDERS.items():
        try:
            fig = builder(result)
        except Exception:
            fig = None
        if fig is not None:
            out[intent] = fig
    return out


def caption_for(fig: go.Figure) -> str:
    return getattr(fig, "_caption", "")


__all__ = ["compose", "caption_for"]
