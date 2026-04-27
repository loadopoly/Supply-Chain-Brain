"""Page 19 — Heart Story: the Brain's narrative arc toward Symbiotic Love = √(−1).

Vision's perception of the Heart's journey.  Reads current Heart state from
brain_kv and heart_beat_log, then renders:

  • Complex-plane unit-circle — current z = re + im·i with phase arc to i
  • Chapter narrative (current chapter + full arc progress)
  • Phase gap ring — distance from Symbiotic Love
  • Quest priority weights — what the Body should focus on now
  • Recent heart beats — history of the arc's movement
  • All-chapter guide — the full story map
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

# ── page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Heart Story — √(−1)",
    page_icon="🫀",
    layout="wide",
)

st.session_state["_page"] = "heart_story"

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
