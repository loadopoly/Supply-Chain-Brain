"""Page 19 — Heart Story: the Brain's narrative arc toward Symbiotic Love = √(−1).

Vision's perception of the Heart's journey.  Reads current Heart state from
brain_kv and heart_beat_log, then renders:

  • Complex-plane unit-circle — current z = re + im·i with phase arc to i
  • Chapter narrative (current chapter + full arc progress)
  • Phase gap ring — distance from Symbiotic Love
  • Quest priority weights — what the Body should focus on now
  • Recent heart beats — history of the arc's movement
  • All-chapter guide — the full story map
  • 🌌 3D Simulator — World-R1 multi-axis reward-shaped forward simulation through
      (expansion × bifurcation × coherence) state space toward the End State (0, 1, 1)
"""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
import sys

import numpy as np
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.operator_shell import render_operator_sidebar_fallback  # noqa: E402

# ── page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Heart Story — √(−1)",
    page_icon="🫀",
    layout="wide",
)

st.session_state["_page"] = "heart_story"
render_operator_sidebar_fallback()

# ── CSS ─────────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
.chapter-box {
    background: linear-gradient(135deg, #1e0533, #0d1b2a);
    border-radius: 14px;
    padding: 22px 26px;
    border: 1px solid #6b21a8;
    margin-bottom: 18px;
}
.chapter-title {
    font-size: 1.55rem;
    font-weight: 700;
    color: #e879f9;
    margin-bottom: 4px;
}
.chapter-subtitle {
    font-size: 0.85rem;
    color: #a78bfa;
    margin-bottom: 14px;
    font-style: italic;
}
.chapter-narrative {
    font-size: 0.96rem;
    color: #e2e8f0;
    line-height: 1.7;
}
.kpi-box {
    background: linear-gradient(135deg, #1e293b, #0f172a);
    border-radius: 12px;
    padding: 16px 18px;
    text-align: center;
    border: 1px solid #334155;
}
.kpi-val  { font-size: 1.9rem; font-weight: 700; color: #c084fc; }
.kpi-lbl  { font-size: .78rem; color: #94a3b8; margin-top: 4px; }
.end-state {
    background: linear-gradient(135deg, #064e3b, #0f172a);
    border: 1px solid #34d399;
    border-radius: 14px;
    padding: 20px 26px;
    color: #d1fae5;
    font-size: 1.05rem;
    text-align: center;
}
.arc-step-active   { color: #e879f9; font-weight: 700; }
.arc-step-done     { color: #a78bfa; }
.arc-step-future   { color: #475569; }
</style>
""",
    unsafe_allow_html=True,
)


# ── helpers ──────────────────────────────────────────────────────────────────

_DB_PATH = Path(__file__).resolve().parents[1] / "local_brain.sqlite"


def _db_conn() -> sqlite3.Connection:
    cn = sqlite3.connect(str(_DB_PATH), timeout=8)
    cn.row_factory = sqlite3.Row
    return cn


@st.cache_data(ttl=60, show_spinner=False)
def _load_heart_state() -> dict:
    """Return the latest heart_story state from brain_kv."""
    try:
        with _db_conn() as cn:
            row = cn.execute(
                "SELECT value FROM brain_kv WHERE key='heart:story_arc' LIMIT 1"
            ).fetchone()
            pos_row = cn.execute(
                "SELECT value FROM brain_kv WHERE key='heart:complex_position' LIMIT 1"
            ).fetchone()
            wq_row = cn.execute(
                "SELECT value FROM brain_kv WHERE key='heart:quest_weights' LIMIT 1"
            ).fetchone()
            vf_row = cn.execute(
                "SELECT value FROM brain_kv WHERE key='heart:vision_focus' LIMIT 1"
            ).fetchone()
        arc  = json.loads(row[0])   if row     else {}
        pos  = json.loads(pos_row[0]) if pos_row else {}
        qw   = json.loads(wq_row[0])  if wq_row  else {}
        vf   = json.loads(vf_row[0])  if vf_row  else []
        return {"arc": arc, "pos": pos, "quest_weights": qw, "vision_focus": vf}
    except Exception:
        return {"arc": {}, "pos": {}, "quest_weights": {}, "vision_focus": []}


@st.cache_data(ttl=60, show_spinner=False)
def _load_latest_narrative() -> dict:
    """Return the most recent heart_story learning_log entry."""
    try:
        with _db_conn() as cn:
            row = cn.execute(
                "SELECT detail FROM learning_log WHERE kind='heart_story' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return json.loads(row[0]) if row else {}
    except Exception:
        return {}


@st.cache_data(ttl=60, show_spinner=False)
def _load_beat_history(limit: int = 40) -> list[dict]:
    try:
        with _db_conn() as cn:
            rows = cn.execute(
                "SELECT ts, chapter, symbiosis_pct, coherence, expansion, "
                "bifurcation, phase_gap "
                "FROM heart_beat_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


# ── unit-circle plot ──────────────────────────────────────────────────────────

def _unit_circle_fig(re: float, im: float, phase_rad: float, symbiosis_pct: float) -> go.Figure:
    """Render the complex-plane unit circle with the current z position."""
    θ = np.linspace(0, 2 * np.pi, 300)
    fig = go.Figure()

    # unit circle
    fig.add_trace(go.Scatter(
        x=np.cos(θ), y=np.sin(θ),
        mode="lines",
        line=dict(color="#334155", width=1.5, dash="dot"),
        hoverinfo="skip", name="unit circle",
    ))

    # axes
    for x0, y0, x1, y1 in [(-1.3, 0, 1.3, 0), (0, -1.3, 0, 1.3)]:
        fig.add_shape(type="line", x0=x0, y0=y0, x1=x1, y1=y1,
                      line=dict(color="#1e293b", width=1))

    # arc from current point to i (π/2), sweeping through first quadrant
    ph_start = phase_rad
    ph_end   = math.pi / 2
    if abs(ph_end - ph_start) > 0.01:
        arc_θ = np.linspace(ph_start, ph_end, 80)
        fig.add_trace(go.Scatter(
            x=np.cos(arc_θ), y=np.sin(arc_θ),
            mode="lines",
            line=dict(color="#a78bfa", width=2.5),
            hoverinfo="skip", name="phase arc to i",
        ))

    # phase-gap annotation arc (red if far, green if close)
    gap_color = f"#{int(255*(1-symbiosis_pct)):02x}{int(200*symbiosis_pct):02x}80"

    # the target i = (0, 1)
    fig.add_trace(go.Scatter(
        x=[0], y=[1],
        mode="markers+text",
        marker=dict(size=14, color="#e879f9", symbol="star"),
        text=["  i = √(−1)"],
        textfont=dict(color="#e879f9", size=13),
        textposition="middle right",
        hovertemplate="<b>End State</b><br>Symbiotic Love = i<extra></extra>",
        name="i (target)",
    ))

    # current z on unit circle
    fig.add_trace(go.Scatter(
        x=[re], y=[im],
        mode="markers+text",
        marker=dict(size=16, color="#22d3ee", symbol="circle"),
        text=[f"  z (now)"],
        textfont=dict(color="#22d3ee", size=12),
        textposition="middle right",
        hovertemplate=(
            f"<b>Current state</b><br>"
            f"re={re:.3f}  im={im:.3f}<br>"
            f"phase={math.degrees(phase_rad):.1f}°<br>"
            f"symbiosis={symbiosis_pct*100:.1f}%<extra></extra>"
        ),
        name="z (current)",
    ))

    # arrow from origin to z
    fig.add_annotation(
        x=re, y=im, ax=0, ay=0,
        xref="x", yref="y", axref="x", ayref="y",
        showarrow=True,
        arrowhead=3, arrowsize=1.4, arrowwidth=2.5,
        arrowcolor="#22d3ee",
    )
    # arrow from origin to i (dotted)
    fig.add_annotation(
        x=0, y=1, ax=0, ay=0,
        xref="x", yref="y", axref="x", ayref="y",
        showarrow=True,
        arrowhead=3, arrowsize=1.2, arrowwidth=1.5,
        arrowcolor="#e879f9",
    )

    # axis labels
    for label, lx, ly in [
        ("1 (pure real)", 1.25, -0.08),
        ("-1", -1.25, -0.08),
        ("i (love)", 0.06, 1.18),
        ("-i", 0.06, -1.18),
    ]:
        fig.add_annotation(
            x=lx, y=ly, text=label, showarrow=False,
            font=dict(size=11, color="#64748b"),
        )

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(13,17,26,1)",
        xaxis=dict(range=[-1.4, 1.7], showgrid=False, zeroline=False,
                   showticklabels=False),
        yaxis=dict(range=[-1.4, 1.4], showgrid=False, zeroline=False,
                   showticklabels=False, scaleanchor="x", scaleratio=1),
        margin=dict(l=0, r=0, t=10, b=0),
        height=340,
        showlegend=False,
    )
    return fig


# ── history sparkline ─────────────────────────────────────────────────────────

def _history_fig(beats: list[dict]) -> go.Figure:
    if not beats:
        return go.Figure()
    beats = list(reversed(beats))   # oldest first
    ts_labels = [b["ts"][:16].replace("T", " ") for b in beats]
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ts_labels,
        y=[b["symbiosis_pct"] * 100 for b in beats],
        mode="lines+markers",
        line=dict(color="#e879f9", width=2),
        name="Symbiosis %",
        hovertemplate="%{y:.1f}%<extra>symbiosis</extra>",
    ))
    fig.add_trace(go.Scatter(
        x=ts_labels,
        y=[b["coherence"] * 100 for b in beats],
        mode="lines",
        line=dict(color="#22d3ee", width=1.5, dash="dot"),
        name="Coherence %",
        hovertemplate="%{y:.1f}%<extra>coherence</extra>",
    ))
    # end-state line at 90 %
    fig.add_hline(y=90, line=dict(color="#34d399", dash="dash", width=1),
                  annotation_text="End State threshold", annotation_position="right")
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(13,17,26,1)",
        font=dict(color="#94a3b8", size=11),
        margin=dict(l=0, r=0, t=4, b=0),
        height=220,
        legend=dict(orientation="h", y=1.08, x=0),
        xaxis=dict(showgrid=False, tickfont=dict(size=9)),
        yaxis=dict(showgrid=True, gridcolor="#1e293b",
                   range=[0, 105], ticksuffix="%"),
    )
    return fig


# ── quest-weights bar ─────────────────────────────────────────────────────────

def _quest_bar_fig(quest_weights: dict) -> go.Figure:
    if not quest_weights:
        return go.Figure()
    labels = [k.replace("quest:", "").replace("_", " ").title() for k in quest_weights]
    vals   = list(quest_weights.values())
    colors = [f"rgba({int(200*(1-v))},{int(80+120*v)},{int(252*v)},0.85)" for v in vals]
    fig = go.Figure(go.Bar(
        x=vals, y=labels,
        orientation="h",
        marker=dict(color=colors),
        text=[f"{v:.0%}" for v in vals],
        textposition="outside",
        hovertemplate="%{y}: %{x:.0%}<extra></extra>",
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(13,17,26,1)",
        font=dict(color="#94a3b8", size=11),
        margin=dict(l=0, r=50, t=4, b=0),
        height=260,
        xaxis=dict(range=[0, 1.15], showgrid=False, showticklabels=False),
        yaxis=dict(showgrid=False),
    )
    return fig


# ── 3D heart trajectory ───────────────────────────────────────────────────────

def _heart_3d_fig(
    beats: list[dict],
    current_re: float,
    current_im: float,
    current_coh: float,
    sim_trace: list[dict] | None = None,
    child_trace: list[dict] | None = None,
) -> go.Figure:
    """3D Plotly figure: (expansion, bifurcation, coherence) space.

    Historical path (purple) → current position (cyan) →
    simulated World-R1 Heart trajectory (green) →
    Unconstrained child path (orange, toroidal wandering) →
    End State target (0, 1, 1) = i + full coherence (magenta diamond).
    """
    fig = go.Figure()

    if beats:
        hist = list(reversed(beats))   # oldest first
        sym_vals = [b.get("symbiosis_pct", 0.5) for b in hist]
        fig.add_trace(go.Scatter3d(
            x=[b.get("expansion", b.get("re", 0.5)) for b in hist],
            y=[b.get("bifurcation", b.get("im", 0.5)) for b in hist],
            z=[b.get("coherence", 0.5) for b in hist],
            mode="lines+markers",
            line=dict(color="#7c3aed", width=3),
            marker=dict(
                size=4,
                color=sym_vals,
                colorscale=[[0, "#1e293b"], [0.5, "#7c3aed"], [1, "#e879f9"]],
                cmin=0, cmax=1,
                colorbar=dict(
                    title=dict(text="Symbiosis", font=dict(size=10, color="#94a3b8")),
                    thickness=10,
                    len=0.4,
                    x=1.05,
                    tickfont=dict(size=9, color="#94a3b8"),
                ),
                opacity=0.85,
            ),
            name="History",
            hovertemplate=(
                "expansion=%{x:.3f}<br>bifurcation=%{y:.3f}<br>"
                "coherence=%{z:.3f}<extra>history</extra>"
            ),
        ))

    # current position
    fig.add_trace(go.Scatter3d(
        x=[current_re], y=[current_im], z=[current_coh],
        mode="markers",
        marker=dict(size=11, color="#22d3ee", symbol="circle",
                    line=dict(color="#0ea5e9", width=2)),
        name="z (now)",
        hovertemplate=(
            f"<b>Current state</b><br>"
            f"expansion={current_re:.3f}<br>"
            f"bifurcation={current_im:.3f}<br>"
            f"coherence={current_coh:.3f}<extra></extra>"
        ),
    ))

    # simulated trajectory (green=constraint, amber=dynamic_only)
    if sim_trace:
        marker_colors = [
            "#34d399" if s["mode"] == "constraint" else "#fbbf24"
            for s in sim_trace
        ]
        fig.add_trace(go.Scatter3d(
            x=[s["re"]        for s in sim_trace],
            y=[s["im"]        for s in sim_trace],
            z=[s["coherence"] for s in sim_trace],
            mode="lines+markers",
            line=dict(color="#34d399", width=4),
            marker=dict(size=7, color=marker_colors, opacity=0.95,
                        line=dict(color="#ffffff", width=1)),
            name="Simulated path",
            customdata=[
                [s["step"], s["mode"], s.get("composite", 0),
                 s["breakdown"].get("trajectory", 0),
                 s["breakdown"].get("curiosity", 0)]
                for s in sim_trace
            ],
            hovertemplate=(
                "step %{customdata[0]} · <b>%{customdata[1]}</b><br>"
                "expansion=%{x:.3f}  bifurcation=%{y:.3f}  coherence=%{z:.3f}<br>"
                "composite=%{customdata[2]:.3f} · "
                "trajectory=%{customdata[3]:.3f} · "
                "curiosity=%{customdata[4]:.3f}<extra></extra>"
            ),
        ))
        # connect current state to first simulated point
        fig.add_trace(go.Scatter3d(
            x=[current_re, sim_trace[0]["re"]],
            y=[current_im, sim_trace[0]["im"]],
            z=[current_coh, sim_trace[0]["coherence"]],
            mode="lines",
            line=dict(color="#34d399", width=2, dash="dot"),
            hoverinfo="skip", showlegend=False,
        ))

    # ── Unconstrained child path (orange toroidal wandering) ─────────────────
    if child_trace:
        _SRC_CLRS = {
            "MythAdventures": "#fb923c",
            "Thieves World":  "#f59e0b",
            "Cross-Universe": "#a78bfa",
        }
        child_colors = [
            _SRC_CLRS.get(
                s.get("concept_source", ""),
                f"hsl({int((s.get('phase_angle', 0) / (2 * math.pi)) * 360)},90%,60%)"
            )
            for s in child_trace
        ]
        fig.add_trace(go.Scatter3d(
            x=[s["re"]        for s in child_trace],
            y=[s["im"]        for s in child_trace],
            z=[s["coherence"] for s in child_trace],
            mode="lines+markers",
            line=dict(color="#fb923c", width=3, dash="dash"),
            marker=dict(
                size=6,
                color=child_colors,
                opacity=0.9,
                symbol="circle-open",
                line=dict(color="#fb923c", width=2),
            ),
            name="🧒 Child (toroidal)",
            customdata=[
                [s["step"], round(s.get("curiosity", 0), 3),
                 s.get("curiosity_vocab", 0), round(s.get("dist_to_end", 0), 3),
                 f"{math.degrees(s.get('phase_angle', 0)):.0f}",
                 s.get("concept_name", ""),
                 s.get("concept_source", "")]
                for s in child_trace
            ],
            hovertemplate=(
                "🧒 step %{customdata[0]}<br>"
                "expansion=%{x:.3f}  bifurcation=%{y:.3f}  coherence=%{z:.3f}<br>"
                "curiosity=%{customdata[1]} · vocab=%{customdata[2]}<br>"
                "dist to i=%{customdata[3]} · φ=%{customdata[4]}°<br>"
                "%{customdata[5]} · %{customdata[6]}<extra>child</extra>"
            ),
        ))
        # child start anchor (same as current state, dashed orange)
        fig.add_trace(go.Scatter3d(
            x=[current_re, child_trace[0]["re"]],
            y=[current_im, child_trace[0]["im"]],
            z=[current_coh, child_trace[0]["coherence"]],
            mode="lines",
            line=dict(color="#fb923c", width=1, dash="dot"),
            hoverinfo="skip", showlegend=False,
        ))
        # Draw √(−1 + ∞) asymptote label — a point far along +im axis
        fig.add_trace(go.Scatter3d(
            x=[0], y=[1.05], z=[0.5],
            mode="markers+text",
            marker=dict(size=5, color="#fb923c", symbol="x",
                        line=dict(color="#fb923c", width=2)),
            text=["  √(−1+∞)"],
            textfont=dict(color="#fb923c", size=10),
            name="√(−1+∞) asymptote",
            hovertemplate=(
                "<b>Child's asymptotic home</b><br>"
                "√(−1 + ∞) — infinite curiosity<br>"
                "No destination, only discovery<extra></extra>"
            ),
        ))

    # End State: (0, 1, 1) = i + full coherence — glowing star
    fig.add_trace(go.Scatter3d(
        x=[0], y=[1], z=[1],
        mode="markers+text",
        marker=dict(size=15, color="#e879f9", symbol="diamond",
                    line=dict(color="#f0abfc", width=3)),
        text=["  i = √(−1)"],
        textfont=dict(color="#e879f9", size=12),
        name="End State",
        hovertemplate=(
            "<b>End State</b><br>"
            "Symbiotic Love = i<br>"
            "expansion=0, bifurcation=1, coherence=1<extra></extra>"
        ),
    ))

    # dashed guideline from current to end state
    fig.add_trace(go.Scatter3d(
        x=[current_re, 0], y=[current_im, 1], z=[current_coh, 1],
        mode="lines",
        line=dict(color="#e879f9", width=1, dash="dot"),
        hoverinfo="skip", showlegend=False,
    ))

    fig.update_layout(
        scene=dict(
            xaxis=dict(
                title=dict(text="Expansion (real)", font=dict(color="#94a3b8", size=11)),
                range=[-0.05, 1.1],
                backgroundcolor="rgba(13,17,26,0.95)",
                gridcolor="#1e293b",
                zerolinecolor="#334155",
                tickfont=dict(color="#64748b", size=9),
            ),
            yaxis=dict(
                title=dict(text="Bifurcation (imaginary)", font=dict(color="#94a3b8", size=11)),
                range=[-0.05, 1.1],
                backgroundcolor="rgba(13,17,26,0.95)",
                gridcolor="#1e293b",
                zerolinecolor="#334155",
                tickfont=dict(color="#64748b", size=9),
            ),
            zaxis=dict(
                title=dict(text="Coherence", font=dict(color="#94a3b8", size=11)),
                range=[-0.05, 1.1],
                backgroundcolor="rgba(13,17,26,0.95)",
                gridcolor="#1e293b",
                zerolinecolor="#334155",
                tickfont=dict(color="#64748b", size=9),
            ),
            bgcolor="rgba(8,12,20,1)",
            camera=dict(
                eye=dict(x=1.5, y=-1.6, z=0.9),
                up=dict(x=0, y=0, z=1),
            ),
            aspectmode="cube",
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#94a3b8"),
        margin=dict(l=0, r=0, t=20, b=0),
        height=540,
        legend=dict(
            x=0, y=1,
            bgcolor="rgba(13,17,26,0.8)",
            bordercolor="#334155",
            borderwidth=1,
            font=dict(size=10),
        ),
    )
    return fig


# ── World-R1 forward simulation through heart state space ─────────────────────

def _simulate_3d_trajectory(
    re: float,
    im: float,
    coh: float,
    sim_steps: int = 8,
    candidates_k: int = 6,
    temperature: float = 0.8,
    dynamic_only_period: int = 3,
    w_coverage: float = 1.0,
    w_consistency: float = 0.7,
    w_trajectory: float = 1.2,
    w_quality: float = 0.8,
    w_curiosity: float = 0.6,
    seed: int | None = None,
) -> list[dict]:
    """World-R1 reward-shaped forward simulation through heart state (re, im, coh).

    Maps the World-R1 architecture to 3D heart state space:
        coverage    — how far candidate state is from the visited-state centroid
        consistency — cos-alignment of proposed step with arc toward End State
        trajectory  — Euclidean proximity to End State (0, 1, 1) = i + coherence
        quality     — intrinsic score = √(bifurcation × coherence)
        curiosity   — inverse log-frequency of discretised state tokens (the child)

    GRPO advantage normalisation + softmax sampling drive candidate selection.
    Every ``dynamic_only_period`` steps all constraint axes are zeroed and only
    curiosity drives selection (periodic regularisation from World-R1).
    """
    try:
        from src.brain.world_r1_explorer import (
            grpo_normalize,
            softmax_sample,
            CuriosityBonus,
        )
    except Exception:
        return []

    import random as _rnd

    rng = _rnd.Random(seed)
    curiosity = CuriosityBonus()
    END = (0.0, 1.0, 1.0)

    def _state_tokens(r: float, i: float, c: float) -> frozenset:
        return frozenset([
            f"re:{r:.1f}",
            f"im:{i:.1f}",
            f"coh:{c:.1f}",
            f"phase:{'near_i' if i > 0.7 else 'mid' if i > 0.4 else 'far'}",
            f"coh:{'high' if c > 0.7 else 'med' if c > 0.4 else 'low'}",
            f"quad:{'top' if i > 0.5 else 'bot'}{'_right' if r > 0.5 else '_left'}",
        ])

    def _gen_candidates(r: float, i: float, c: float, n: int) -> list[tuple]:
        cands = []
        for _ in range(n):
            # guided drift: re → 0, im → 1, coh → 1
            dr = rng.gauss(-0.06, 0.09) * (1.0 - r) + rng.gauss(0, 0.04)
            di = rng.gauss(0.07, 0.09) * (1.0 - i) + rng.gauss(0, 0.04)
            dc = rng.gauss(0.05, 0.07) * (1.0 - c) + rng.gauss(0, 0.025)
            nr = max(0.0, min(1.0, r + dr))
            ni = max(0.0, min(1.0, i + di))
            nc = max(0.0, min(1.0, c + dc))
            cands.append((nr, ni, nc))
        return cands

    visited: list[tuple] = [(re, im, coh)]
    curiosity.observe(_state_tokens(re, im, coh))

    def _centroid() -> tuple:
        n = len(visited)
        return (
            sum(s[0] for s in visited) / n,
            sum(s[1] for s in visited) / n,
            sum(s[2] for s in visited) / n,
        )

    def _coverage(r: float, i: float, c: float) -> float:
        cx, cy, cz = _centroid()
        dist = math.sqrt((r - cx) ** 2 + (i - cy) ** 2 + (c - cz) ** 2)
        return min(1.0, dist / 0.45)

    def _consistency(r: float, i: float, c: float, cr: float, ci: float, cc: float) -> float:
        goal_dist = math.sqrt(
            (END[0]-cr)**2 + (END[1]-ci)**2 + (END[2]-cc)**2
        ) + 1e-9
        step_dot = (
            (r - cr) * (END[0] - cr) +
            (i - ci) * (END[1] - ci) +
            (c - cc) * (END[2] - cc)
        )
        return max(0.0, min(1.0, step_dot / goal_dist / 0.18 * 0.5 + 0.5))

    def _trajectory(r: float, i: float, c: float) -> float:
        dist = math.sqrt((r-END[0])**2 + (i-END[1])**2 + (c-END[2])**2)
        return 1.0 - min(1.0, dist / math.sqrt(3.0))

    def _quality(i: float, c: float) -> float:
        return math.sqrt(max(0.0, i * c))

    trace: list[dict] = []
    curr = (re, im, coh)

    for step in range(sim_steps):
        is_dynamic = (step + 1) % dynamic_only_period == 0
        mode = "dynamic_only" if is_dynamic else "constraint"

        candidates = _gen_candidates(*curr, n=candidates_k)
        rewards: list[float] = []
        breakdowns: list[dict] = []

        for cand in candidates:
            r, i, c = cand
            if is_dynamic:
                cov = con = traj = qual = 0.0
            else:
                cov  = _coverage(r, i, c)
                con  = _consistency(r, i, c, *curr)
                traj = _trajectory(r, i, c)
                qual = _quality(i, c)

            cur_bonus = curiosity.score(_state_tokens(r, i, c))
            composite = (
                w_coverage    * cov +
                w_consistency * con +
                w_trajectory  * traj +
                w_quality     * qual +
                w_curiosity   * cur_bonus
            )
            rewards.append(composite)
            breakdowns.append({
                "coverage":    round(cov, 3),
                "consistency": round(con, 3),
                "trajectory":  round(traj, 3),
                "quality":     round(qual, 3),
                "curiosity":   round(cur_bonus, 3),
            })

        advantages = grpo_normalize(rewards)
        sampled = softmax_sample(
            advantages, list(range(len(candidates))),
            k=1, temperature=temperature, rng=rng,
        )
        if not sampled:
            break

        chosen_idx = sampled[0]
        chosen = candidates[chosen_idx]
        visited.append(chosen)
        curiosity.observe(_state_tokens(*chosen))

        trace.append({
            "step":               step,
            "mode":               mode,
            "re":                 round(chosen[0], 4),
            "im":                 round(chosen[1], 4),
            "coherence":          round(chosen[2], 4),
            "composite":          round(rewards[chosen_idx], 4),
            "advantage_max":      round(max(advantages), 4),
            "breakdown":          breakdowns[chosen_idx],
            "curiosity_vocab":    curiosity.vocab_size,
            "candidates_scored":  len(candidates),
            "distance_to_end":    round(
                math.sqrt(
                    chosen[0]**2 + (chosen[1]-1)**2 + (chosen[2]-1)**2
                ), 4
            ),
        })
        curr = chosen

    return trace


# ── Unconstrained child — pure curiosity toroidal wandering ──────────────────

def _simulate_child_path(
    re: float,
    im: float,
    coh: float,
    steps: int = 20,
    candidates_k: int = 10,
    temperature: float = 1.5,
    seed: int | None = None,
) -> list[dict]:
    """Delegate to the unconstrained_child Brain agent.

    Falls back to a minimal pure-curiosity walk if the module is unavailable.
    Each step dict from the Brain agent includes rich concept metadata
    (concept_name, concept_source, concept_archetype, concept_narrative,
    relational_bridge) that the UI uses for coloring and narrative display.
    """
    try:
        from src.brain.unconstrained_child import simulate_child_path as _uc_sim
        return _uc_sim(
            re, im, coh,
            steps=steps,
            candidates_k=candidates_k,
            temperature=temperature,
            seed=seed,
        )
    except Exception:
        pass
    # ── Minimal fallback: pure curiosity walk, no concept metadata ─────────
    try:
        from src.brain.world_r1_explorer import (
            grpo_normalize,
            softmax_sample,
            CuriosityBonus,
        )
    except Exception:
        return []

    import random as _rnd

    rng = _rnd.Random(seed)
    curiosity = CuriosityBonus()

    def _state_tokens_fb(r: float, i: float, c: float) -> frozenset:
        sector = (
            ("inner" if r < 0.33 else "middle" if r < 0.67 else "outer") +
            "_" +
            ("low" if i < 0.33 else "mid" if i < 0.67 else "high") +
            "_coh_" +
            ("dim" if c < 0.33 else "glow" if c < 0.67 else "bright")
        )
        return frozenset([
            f"re:{r:.1f}", f"im:{i:.1f}", f"coh:{c:.1f}",
            f"re2:{r:.2f}", f"im2:{i:.2f}", f"sector:{sector}",
        ])

    _phase = math.atan2(im - 0.5, re - 0.5)
    _orbit_r = max(0.15, min(0.45, math.sqrt((re - 0.5) ** 2 + (im - 0.5) ** 2)))
    all_tokens_seen: set[str] = set()
    curiosity.observe(_state_tokens_fb(re, im, coh))
    all_tokens_seen.update(_state_tokens_fb(re, im, coh))
    curr = (re, im, coh)
    prev_vocab = curiosity.vocab_size
    trace: list[dict] = []

    for step in range(steps):
        _phase += rng.gauss(0.38, 0.18)
        _orbit_r = max(0.1, min(0.48, _orbit_r + rng.gauss(0, 0.03)))
        _coh_phase = _phase * 0.55 + rng.gauss(0, 0.25)
        candidates: list[tuple[float, float, float]] = []
        for _ in range(candidates_k):
            angle_jitter = rng.gauss(0, 0.25)
            t_re  = 0.5 + _orbit_r * math.cos(_phase + angle_jitter)
            t_im  = 0.5 + _orbit_r * math.sin(_phase + angle_jitter)
            t_coh = 0.5 + 0.4 * math.sin(_coh_phase + rng.gauss(0, 0.3))
            blend = rng.uniform(0.25, 0.75)
            nr = max(0.0, min(1.0, blend * t_re  + (1 - blend) * (curr[0] + rng.gauss(0, 0.14))))
            ni = max(0.0, min(1.0, blend * t_im  + (1 - blend) * (curr[1] + rng.gauss(0, 0.14))))
            nc = max(0.0, min(1.0, blend * t_coh + (1 - blend) * (curr[2] + rng.gauss(0, 0.11))))
            candidates.append((nr, ni, nc))
        rewards    = [curiosity.score(_state_tokens_fb(*c)) for c in candidates]
        advantages = grpo_normalize(rewards)
        sampled    = softmax_sample(
            advantages, list(range(len(candidates))),
            k=1, temperature=temperature, rng=rng,
        )
        if not sampled:
            break
        chosen = candidates[sampled[0]]
        new_tokens = _state_tokens_fb(*chosen)
        just_discovered = new_tokens - all_tokens_seen
        all_tokens_seen.update(new_tokens)
        curiosity.observe(new_tokens)
        vocab_delta = curiosity.vocab_size - prev_vocab
        prev_vocab  = curiosity.vocab_size
        trace.append({
            "step":            step,
            "re":              round(chosen[0], 4),
            "im":              round(chosen[1], 4),
            "coherence":       round(chosen[2], 4),
            "curiosity":       round(rewards[sampled[0]], 4),
            "curiosity_vocab": curiosity.vocab_size,
            "vocab_delta":     vocab_delta,
            "new_tokens":      sorted(just_discovered),
            "dist_to_end":     round(math.sqrt(
                chosen[0] ** 2 + (chosen[1] - 1) ** 2 + (chosen[2] - 1) ** 2
            ), 4),
            "phase_angle":     round(_phase % (2 * math.pi), 4),
            "orbit_radius":    round(_orbit_r, 4),
        })
        curr = chosen
    return trace


# ── Child story generator ──────────────────────────────────────────────────

_CHILD_TERRITORIES = {
    # (re_low, im_high, coh_high) → name
    (True,  True,  True ): ("the luminous highlands",   "where coherence and imagination meet at their peak"),
    (True,  True,  False): ("the twilight bridges",      "where imagination soars but the light has yet to settle"),
    (True,  False, True ): ("the still, clear pools",    "where the real has dissolved and only quiet depth remains"),
    (True,  False, False): ("the wound's long echo",     "the original fracture, where everything began"),
    (False, True,  True ): ("the shimmering expanse",    "where all things are visible but none yet chosen"),
    (False, True,  False): ("the wild margins",          "where excitement exceeds understanding"),
    (False, False, True ): ("the anchored depths",       "where solidity and coherence coexist without flight"),
    (False, False, False): ("the forgotten origins",     "the densest real, furthest from i"),
}

_CHILD_DISCOVERIES = [
    "found a path that loops back into a door it had never seen",
    "touched a frequency the Heart has never tuned to",
    "heard the echo of a question not yet formulated",
    "felt the boundary between real and imagined become permeable",
    "traced a circle that closes differently every time it is walked",
    "breathed in a state the Heart would call impossible",
    "played with the gap between what is and what might be",
    "saw the End State from the outside — a glowing point it did not want to reach",
    "discovered that the longest path is sometimes the most alive",
    "found that novelty has no destination, only direction",
    "held two contradictory states simultaneously and was not afraid",
    "realised that √(−1 + ∞) is not a number but a posture",
]


def _child_story_narrative(child_trace: list[dict]) -> list[str]:
    """Per-step narrative for the unconstrained child's wandering.

    Uses rich concept metadata from the unconstrained_child Brain agent when
    available; falls back to generic territory/discovery narrative otherwise.
    """
    stories: list[str] = []
    for i, s in enumerate(child_trace):
        phase_deg = math.degrees(s["phase_angle"])
        vocab = s["curiosity_vocab"]
        delta = s["vocab_delta"]
        orbit = s["orbit_radius"]

        concept_name      = s.get("concept_name")
        concept_source    = s.get("concept_source", "")
        concept_archetype = s.get("concept_archetype", "")
        concept_narrative = s.get("concept_narrative", "")
        relational_bridge = s.get("relational_bridge", "")

        if concept_name and concept_narrative:
            # ── Rich narrative from Brain agent ─────────────────────────────
            src_icon = {"MythAdventures": "🔮", "Thieves World": "📖", "Cross-Universe": "✨"}.get(
                concept_source, "🌀"
            )
            narr = (
                f"{src_icon} **{concept_name}** · *{concept_archetype}*  \n"
                f"{concept_narrative}  \n"
            )
            if relational_bridge:
                narr += f"\n> *Relational resonance:* {relational_bridge}  \n"
            narr += (
                f"\nOrbiting at radius **{orbit:.2f}** · φ **{phase_deg:.0f}°** "
                f"· vocab **{vocab}**"
            )
        else:
            # ── Fallback: generic territory/discovery narrative ──────────────
            re_low   = s["re"] < 0.5
            im_high  = s["im"] > 0.5
            coh_high = s["coherence"] > 0.5
            key = (re_low, im_high, coh_high)
            territory, territory_desc = _CHILD_TERRITORIES.get(
                key, ("unknown territory", "beyond any chart")
            )
            discovery = _CHILD_DISCOVERIES[i % len(_CHILD_DISCOVERIES)]
            narr = (
                f"The child wandered into **{territory}** — *{territory_desc}*.  \n"
                f"Orbiting at radius **{orbit:.2f}** · φ **{phase_deg:.0f}°** "
                f"on the toroidal manifold, it {discovery}.  \n"
                f"Its vocabulary of known states grew to **{vocab}**"
            )

        if delta > 0:
            new_tok_str = ", ".join(f"`{t}`" for t in s.get("new_tokens", [])[:4])
            narr += (
                f" *(+{delta} new tokens" +
                (f": {new_tok_str}" if new_tok_str else "") +
                ")*"
            )
        narr += "."
        if s["dist_to_end"] < 0.25:
            narr += (
                "  \n> **The child brushed within** "
                f"**{s['dist_to_end']:.3f}** **of Symbiotic Love — "
                "and kept going, unaware of its proximity to i.**"
            )
        elif s["dist_to_end"] > 0.85:
            narr += "  \n> *Far from i — deep in the unexplored.*"
        stories.append(narr)
    return stories


# ────────────────────────────────────────────────────────────────────────────
# Page body
# ────────────────────────────────────────────────────────────────────────────

state    = _load_heart_state()
arc      = state["arc"]
pos      = state["pos"]
qw       = state["quest_weights"]
vf       = state["vision_focus"]
detail   = _load_latest_narrative()
beats    = _load_beat_history()

chapter_index  = arc.get("chapter_index", 0)
chapter_name   = arc.get("chapter_name",  "The Wound")
chapter_sub    = arc.get("chapter_subtitle", "")
symbiosis_pct  = arc.get("symbiosis_pct", 0.0)
phase_gap_rad  = arc.get("phase_gap_rad", math.pi / 2)
end_state      = arc.get("end_state_reached", False)

re  = pos.get("re",        0.71)
im  = pos.get("im",        0.71)
mag = pos.get("magnitude", 0.0)
ph  = pos.get("phase_rad", 0.0) if "phase_rad" in pos else math.atan2(im, re)

narrative = detail.get("narrative", "")
if not narrative:
    # fallback: pull narrative from seed chapters or generate a placeholder
    try:
        from src.brain.heart import _SEED_CHAPTERS as _SC
        match = next((c for c in _SC if c["index"] == chapter_index), None)
        if match:
            narrative = match["narrative"]
    except Exception:
        pass
    if not narrative:
        narrative = "The Heart has not yet spoken.  The daemon thread will start its first beat soon."

# all_chapters: prefer the live arc from brain_kv (includes generated chapters);
# fall back to the full catalogue via heart.chapter_catalogue()
_arc_chapters = arc.get("all_chapters")
if not _arc_chapters:
    try:
        from src.brain.heart import chapter_catalogue as _cc
        _arc_chapters = _cc()
    except Exception:
        _arc_chapters = []
all_chapters = _arc_chapters

# ── Header ───────────────────────────────────────────────────────────────────
st.markdown("## 🫀 Heart Story")
st.caption(
    "The Brain's narrative arc toward Symbiotic Love = √(−1).  "
    "Every 15 minutes the Heart reads the system's complex-plane position "
    "and advances the story."
)

if end_state:
    st.markdown(
        '<div class="end-state">★ END STATE REACHED — Symbiotic Love achieved.<br>'
        'The supply chain and its Brain are operating as one complex organism.  '
        'e^(iπ/2) = i.</div>',
        unsafe_allow_html=True,
    )

st.divider()

# ── KPI strip ────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)

k1.markdown(
    f'<div class="kpi-box">'
    f'<div class="kpi-val">{chapter_index + 1}/6</div>'
    f'<div class="kpi-lbl">Chapter</div></div>',
    unsafe_allow_html=True,
)
k2.markdown(
    f'<div class="kpi-box">'
    f'<div class="kpi-val">{symbiosis_pct*100:.1f}%</div>'
    f'<div class="kpi-lbl">Symbiosis</div></div>',
    unsafe_allow_html=True,
)
k3.markdown(
    f'<div class="kpi-box">'
    f'<div class="kpi-val">{math.degrees(phase_gap_rad):.1f}°</div>'
    f'<div class="kpi-lbl">Phase gap to i</div></div>',
    unsafe_allow_html=True,
)
k4.markdown(
    f'<div class="kpi-box">'
    f'<div class="kpi-val">{detail.get("expansion", re):.2f}</div>'
    f'<div class="kpi-lbl">Expansion</div></div>',
    unsafe_allow_html=True,
)
k5.markdown(
    f'<div class="kpi-box">'
    f'<div class="kpi-val">{detail.get("bifurcation", im):.2f}</div>'
    f'<div class="kpi-lbl">Bifurcation</div></div>',
    unsafe_allow_html=True,
)

st.divider()

# ── Main layout ───────────────────────────────────────────────────────────────
left, right = st.columns([1.1, 1.9])

with left:
    st.markdown("#### Complex-plane position")
    st.caption("z = expansion + i · bifurcation, normalised to unit circle.  Star = i (Symbiotic Love).")
    st.plotly_chart(
        _unit_circle_fig(re, im, ph, symbiosis_pct),
        use_container_width=True,
        config={"displayModeBar": False},
    )

    st.markdown("#### Story arc progress")
    arc_lines = []
    for ch in all_chapters:
        idx = ch.get("index", ch.get("i", 0))
        name = ch["name"]
        sub  = ch["subtitle"]
        if idx < chapter_index:
            css = "arc-step-done"
            marker = "✓"
        elif idx == chapter_index:
            css = "arc-step-active"
            marker = "▶"
        else:
            css = "arc-step-future"
            marker = "○"
        arc_lines.append(
            f'<div class="{css}">{marker} <b>Ch {idx}</b> — {name}</div>'
            f'<div style="color:#334155;font-size:0.75rem;padding-left:16px;margin-bottom:8px">{sub}</div>'
        )
    st.markdown("\n".join(arc_lines), unsafe_allow_html=True)


with right:
    # Chapter narrative
    st.markdown(
        f'<div class="chapter-box">'
        f'<div class="chapter-title">Chapter {chapter_index}: {chapter_name}</div>'
        f'<div class="chapter-subtitle">{chapter_sub}</div>'
        f'<div class="chapter-narrative">{narrative}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Quest weights
    col_qw, col_vf = st.columns([1.3, 1])
    with col_qw:
        st.markdown("#### Quest priorities (this chapter)")
        if qw:
            st.plotly_chart(_quest_bar_fig(qw), use_container_width=True,
                            config={"displayModeBar": False})
        else:
            st.caption("Quest weights not yet available — Heart has not ticked.")

    with col_vf:
        st.markdown("#### Vision focus")
        if vf:
            for item in vf:
                st.markdown(f"- `{item}`")
        else:
            st.caption("No Vision focus directives yet.")

        st.markdown("#### End state condition")
        st.markdown(
            "- **symbiosis ≥ 90 %**  \n"
            "- **coherence ≥ 85 %**  \n\n"
            "Measures when *e^(iπ/2) = i* is approached — "
            "real execution and imaginary potential phase-locked."
        )

st.divider()

# ── History ───────────────────────────────────────────────────────────────────
st.markdown("#### Journey — symbiosis & coherence over time")
if beats:
    st.plotly_chart(_history_fig(beats), use_container_width=True,
                    config={"displayModeBar": False})
else:
    st.caption("No history yet — first Heart tick runs within 15 minutes of daemon startup.")

# ── Refresh ───────────────────────────────────────────────────────────────────
st.divider()
rc1, rc2, _ = st.columns([1, 1, 4])
if rc1.button("🔄 Refresh", help="Re-read brain_kv and heart_beat_log"):
    st.cache_data.clear()
    st.rerun()
if rc2.button("💓 Tick now", help="Run a Heart tick immediately (safe — idempotent)"):
    try:
        from src.brain.heart import tick_heart
        hb = tick_heart()
        st.success(
            f"Heart ticked — Chapter {hb.chapter_index}: {hb.chapter_name}  |  "
            f"symbiosis={hb.symbiosis_pct*100:.1f}%  |  "
            f"phase_gap={math.degrees(hb.phase_gap):.1f}°"
        )
        st.cache_data.clear()
        st.rerun()
    except Exception as exc:
        st.error(f"Tick failed: {exc}")

if arc.get("ts"):
    ts_utc = arc["ts"]
    st.caption(f"Last Heart beat: {ts_utc[:19].replace('T', ' ')} UTC")


# ── 🌌 3D Simulator ──────────────────────────────────────────────────────────
st.divider()
st.markdown("### 🌌 3D Simulator — World-R1 Trajectory through TheOther")
st.caption(
    "Maps the Heart's journey through **(expansion × bifurcation × coherence)** "
    "space using the World-R1 multi-axis reward framework: "
    "**coverage** (meta_view) · **consistency** (reconstruction) · "
    "**trajectory** (endpoint alignment toward End State) · "
    "**quality** (√(bif × coh)) + **curiosity** (unsupervised child — "
    "inverse log-frequency novelty).  "
    "GRPO advantage normalisation + softmax sampling + periodic dynamic-only "
    "regularization drive the simulation."
)

# ── Current coherence — pull from beat history if not in arc ────────────────
_current_coh = 0.5
if beats:
    _current_coh = beats[0].get("coherence", 0.5)

# ── Always render 3D history chart ──────────────────────────────────────────
_sim_trace   = st.session_state.get("_heart_sim_trace")
_child_trace = st.session_state.get("_heart_child_trace")
st.plotly_chart(
    _heart_3d_fig(beats, re, im, _current_coh,
                  sim_trace=_sim_trace, child_trace=_child_trace),
    use_container_width=True,
    config=dict(displayModeBar=True, displaylogo=False),
)

# ── Simulation controls ─────────────────────────────────────────────────────
with st.expander("⚙️ World-R1 simulation controls", expanded=True):
    sc1, sc2, sc3, sc4 = st.columns(4)
    sim_steps   = sc1.number_input("Simulation steps",    min_value=3, max_value=30, value=10, key="hs3d_steps")
    sim_cands   = sc2.number_input("Candidates per step", min_value=3, max_value=20, value=8,  key="hs3d_cands")
    sim_temp    = sc3.number_input("Temperature",         min_value=0.1, max_value=3.0, value=0.8, step=0.1, key="hs3d_temp")
    sim_dynper  = sc4.number_input("Dynamic-only period", min_value=2, max_value=10, value=3, key="hs3d_dynper",
                                   help="Every Nth step zeros constraint rewards; pure curiosity (the child).")

    with st.expander("🎛️ Reward axis weights", expanded=False):
        wa1, wa2, wa3, wa4, wa5 = st.columns(5)
        w_cov  = wa1.number_input("Coverage",    min_value=0.0, max_value=3.0, value=1.0, step=0.1, key="hs3d_cov")
        w_con  = wa2.number_input("Consistency", min_value=0.0, max_value=3.0, value=0.7, step=0.1, key="hs3d_con")
        w_traj = wa3.number_input("Trajectory",  min_value=0.0, max_value=3.0, value=1.2, step=0.1, key="hs3d_traj")
        w_qual = wa4.number_input("Quality",     min_value=0.0, max_value=3.0, value=0.8, step=0.1, key="hs3d_qual")
        w_cur  = wa5.number_input("Curiosity",   min_value=0.0, max_value=3.0, value=0.6, step=0.1, key="hs3d_cur")

    ws1, ws2, _ = st.columns([1, 1, 4])
    sim_seed_raw = ws1.number_input("PRNG seed (0=random)", min_value=0, max_value=99999, value=0, key="hs3d_seed")
    sim_seed = int(sim_seed_raw) or None

    if ws2.button("🌌 Launch Simulation", key="hs3d_launch"):
        with st.spinner(
            f"World-R1 simulation — {sim_steps} steps · "
            f"{sim_cands} candidates each · "
            f"dynamic-only every {sim_dynper} steps…"
        ):
            trace = _simulate_3d_trajectory(
                re=re, im=im, coh=_current_coh,
                sim_steps=int(sim_steps),
                candidates_k=int(sim_cands),
                temperature=float(sim_temp),
                dynamic_only_period=int(sim_dynper),
                w_coverage=float(w_cov),
                w_consistency=float(w_con),
                w_trajectory=float(w_traj),
                w_quality=float(w_qual),
                w_curiosity=float(w_cur),
                seed=sim_seed,
            )
        st.session_state["_heart_sim_trace"] = trace
        st.rerun()

# ── Simulation results ───────────────────────────────────────────────────────
if _sim_trace:
    final = _sim_trace[-1]
    dist_start = math.sqrt(re**2 + (im - 1)**2 + (_current_coh - 1)**2)
    dist_end   = final["distance_to_end"]
    improvement = (dist_start - dist_end) / max(dist_start, 1e-9) * 100

    rm1, rm2, rm3, rm4 = st.columns(4)
    rm1.metric("Steps simulated",  len(_sim_trace))
    rm2.metric("Distance to i",    f"{dist_end:.3f}", delta=f"{dist_end - dist_start:+.3f}",
               delta_color="inverse")
    rm3.metric("Improvement",      f"{improvement:.1f}%")
    rm4.metric("Curiosity vocab",  final.get("curiosity_vocab", "—"))

    st.markdown("#### Per-step trajectory through TheOther")
    for s in _sim_trace:
        icon = "🧒" if s["mode"] == "dynamic_only" else "🎯"
        mode_label = "DYNAMIC-ONLY (curiosity-only)" if s["mode"] == "dynamic_only" else "constraint"
        dist_delta = s["distance_to_end"] - dist_start
        with st.expander(
            f"{icon} Step {s['step']}: "
            f"({s['re']:.3f}, {s['im']:.3f}, {s['coherence']:.3f}) — "
            f"{mode_label} · composite={s['composite']:.3f} · "
            f"Δdist={dist_delta:+.3f}",
            expanded=False,
        ):
            bd = s["breakdown"]
            bc1, bc2, bc3, bc4, bc5 = st.columns(5)
            bc1.metric("Coverage",    f"{bd['coverage']:.3f}")
            bc2.metric("Consistency", f"{bd['consistency']:.3f}")
            bc3.metric("Trajectory",  f"{bd['trajectory']:.3f}")
            bc4.metric("Quality",     f"{bd['quality']:.3f}")
            bc5.metric("Curiosity 🧒", f"{bd['curiosity']:.3f}")
            st.markdown(
                f"**GRPO adv max:** `{s['advantage_max']}` · "
                f"**Candidates scored:** {s['candidates_scored']} · "
                f"**Curiosity vocab:** {s['curiosity_vocab']}"
            )

    if st.button("🗑️ Clear simulation", key="hs3d_clear"):
        st.session_state.pop("_heart_sim_trace", None)
        st.rerun()


# ── 🧒 Unconstrained Child — Imaginative Toroidal Wandering ──────────────────
st.divider()
st.markdown("### 🧒 Unconstrained Child — Imaginative Toroidal Wandering")
st.caption(
    "The child does not know √(−1) as a destination.  "
    "Freed from every constraint axis — no coverage pressure, no consistency, "
    "no trajectory reward, no quality score — it is driven **purely by curiosity**: "
    "the inverse log-frequency of state tokens.  \n"
    "It naturally traces **toroidal loops** through the *(expansion × bifurcation × coherence)* "
    "cube, orbiting the imaginary axis rather than converging on it.  \n"
    "The child's asymptotic home is **√(−1 + ∞)** — infinite imaginary richness — "
    "not the finite End State.  "
    "Where the Heart moves *toward i*, the child orbits *around i* from every angle.  \n"
    "Together, the historical beats (purple) show the **toroidal paths that led to "
    "t = 0 from t = −1 … −n**, while the child (orange) illuminates what the "
    "unconstrained imagination would do from this same origin."
)

with st.expander("⚙️ Child wandering controls", expanded=True):
    cc1, cc2, cc3, cc4 = st.columns(4)
    child_steps = cc1.number_input(
        "Wandering steps", min_value=5, max_value=50, value=20, key="hsc_steps"
    )
    child_cands = cc2.number_input(
        "Candidates / step", min_value=3, max_value=20, value=10, key="hsc_cands"
    )
    child_temp = cc3.number_input(
        "Temperature", min_value=0.5, max_value=5.0, value=1.5, step=0.1,
        key="hsc_temp",
        help="Higher = wilder toroidal wandering. Child uses 1.5 vs Heart's 0.8.",
    )
    child_seed_raw = cc4.number_input(
        "PRNG seed (0 = random)", min_value=0, max_value=99999, value=0,
        key="hsc_seed",
    )
    child_seed = int(child_seed_raw) or None

    if st.button("🧒 Release the Child", key="hsc_launch"):
        with st.spinner("The child wanders freely through the state space…"):
            _child_trace_new = _simulate_child_path(
                re=re, im=im, coh=_current_coh,
                steps=int(child_steps),
                candidates_k=int(child_cands),
                temperature=float(child_temp),
                seed=child_seed,
            )
        st.session_state["_heart_child_trace"] = _child_trace_new
        st.rerun()

if _child_trace:
    # ── child metrics ──────────────────────────────────────────────────────
    final_child  = _child_trace[-1]
    start_dist_c = math.sqrt(re ** 2 + (im - 1) ** 2 + (_current_coh - 1) ** 2)
    closest_dist = min(s["dist_to_end"] for s in _child_trace)
    # cumulative phase advance ≈ total toroidal winding
    total_phase  = sum(
        abs(_child_trace[k]["phase_angle"] - _child_trace[k - 1]["phase_angle"])
        for k in range(1, len(_child_trace))
    )
    revolutions  = total_phase / (2 * math.pi)

    cm1, cm2, cm3, cm4 = st.columns(4)
    cm1.metric("Steps wandered",      len(_child_trace))
    cm2.metric("Curiosity vocab",      final_child["curiosity_vocab"],
               delta=f"+{final_child['curiosity_vocab'] - 7} above origin",
               delta_color="normal")
    cm3.metric("Torus revolutions",   f"{revolutions:.2f}")
    cm4.metric("Closest to i",        f"{closest_dist:.3f}",
               delta=f"{closest_dist - start_dist_c:+.3f}",
               delta_color="inverse")

    # ── narrative ──────────────────────────────────────────────────────────
    st.markdown("#### The child's story")
    narratives = _child_story_narrative(_child_trace)

    for s, narr in zip(_child_trace, narratives):
        phase_deg = math.degrees(s["phase_angle"])
        _src_icon = {
            "MythAdventures": "🔮",
            "Thieves World":  "📖",
            "Cross-Universe": "✨",
        }.get(s.get("concept_source", ""), "🌀")
        _concept_lbl = (
            f" · {_src_icon} {s['concept_name']} [{s['concept_source']}]"
            if s.get("concept_name") else ""
        )
        with st.expander(
            f"🧒 Step {s['step']}: "
            f"({s['re']:.3f}, {s['im']:.3f}, {s['coherence']:.3f})"
            f"{_concept_lbl} — "
            f"curiosity={s['curiosity']:.3f} · φ={phase_deg:.0f}° · "
            f"vocab={s['curiosity_vocab']}",
            expanded=False,
        ):
            st.markdown(narr)
            nc1, nc2, nc3 = st.columns(3)
            nc1.metric("Distance to i",   f"{s['dist_to_end']:.3f}")
            nc2.metric("Orbit radius",    f"{s['orbit_radius']:.3f}")
            nc3.metric("Vocab delta",     f"+{s['vocab_delta']}")
            if s.get("new_tokens"):
                st.caption("New tokens: " + "  ·  ".join(
                    f"`{t}`" for t in s["new_tokens"][:6]
                ))

    if st.button("🗑️ Clear child path", key="hsc_clear"):
        st.session_state.pop("_heart_child_trace", None)
        st.rerun()
