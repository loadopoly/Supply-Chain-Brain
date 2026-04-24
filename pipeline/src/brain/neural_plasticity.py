"""Neural plasticity — the rewiring agent.

As the corpus grows, the senses (Vision, Touch, Smell, Body, Brain) need
to expand and adapt their capabilities.  This module measures knowledge
state across all five senses and computes per-sense capability *dials*
that the senses then read at runtime, so each sweep operates with
parameters appropriate to the current scale of the corpus.

Architecture
------------
::

  ┌────────────────────┐     ┌──────────────────────┐
  │  measure_knowledge │ ──► │ compute_capability_  │
  │  _state()          │     │ targets()            │
  │                    │     │                      │
  │ • entity_count     │     │ Sigmoid maps from    │
  │ • edge_count       │     │ growth → dial value  │
  │ • learning_count   │     │  (one per sense)     │
  │ • doc_chunk_count  │     │                      │
  │ • smell_readings   │     │                      │
  └────────────────────┘     └──────────┬───────────┘
                                        │
                                        ▼
                          ┌─────────────────────────────┐
                          │ plasticity_step() — ADAM-   │
                          │ smoothed update toward      │
                          │ targets, persisted to       │
                          │ brain_kv.neural_plasticity_ │
                          │ state                       │
                          └──────────────┬──────────────┘
                                         │
        ┌──────────────┬─────────────────┼─────────────────┬──────────────┐
        ▼              ▼                 ▼                 ▼              ▼
     Vision         Touch              Smell             Body          Brain
   (knowledge_  (brain_body_       (sense_of_smell  (cadence,      (round
    corpus)      signals)            sensitivity)    directive cap) intervals)

Each sense calls :func:`get_dial` to read its current capability values
with sensible defaults if plasticity hasn't run yet.
"""

from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from .local_store import db_path as _local_db_path


_PLASTICITY_LOCK   = threading.Lock()
_LAST_REWIRE_TS    = 0.0
_REWIRE_MIN_SEC    = 30.0   # rate-limit between rewires
_PLASTICITY_KEY    = "neural_plasticity_state"

# ADAM smoothing on dials so they don't snap on every observation.
_PL_BETA1 = 0.9
_PL_BETA2 = 0.999
_PL_LR    = 0.20
_PL_EPS   = 1e-6


# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------
@contextmanager
def _conn():
    cn = sqlite3.connect(_local_db_path())
    cn.row_factory = sqlite3.Row
    try:
        yield cn
        cn.commit()
    finally:
        cn.close()


def _kv_read(cn, key: str) -> dict:
    cn.execute(
        "CREATE TABLE IF NOT EXISTS brain_kv("
        "key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)"
    )
    try:
        row = cn.execute(
            "SELECT value FROM brain_kv WHERE key=?", (key,)
        ).fetchone()
        if row and row[0]:
            return json.loads(row[0])
    except (sqlite3.OperationalError, json.JSONDecodeError):
        pass
    return {}


def _kv_write(cn, key: str, val: dict) -> None:
    cn.execute(
        "CREATE TABLE IF NOT EXISTS brain_kv("
        "key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)"
    )
    cn.execute(
        "INSERT OR REPLACE INTO brain_kv(key, value, updated_at) VALUES(?,?,?)",
        (key, json.dumps(val, default=str),
         datetime.now(timezone.utc).isoformat()),
    )


def _safe_count(cn, sql: str) -> int:
    try:
        row = cn.execute(sql).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except sqlite3.OperationalError:
        return 0


# ---------------------------------------------------------------------------
# Default dials — what the senses see if plasticity has never run
# ---------------------------------------------------------------------------
# Each dial is a value in a sense-specific natural unit (count, seconds,
# pressure threshold, learning rate, ...).  These defaults match the
# hardcoded values that lived in each sense before plasticity existed.
_DEFAULT_DIALS: dict[str, dict[str, float]] = {
    "vision": {
        "pressure_threshold":   0.30,   # _PRESSURE_THRESHOLD in knowledge_corpus
        "force_threshold":      0.30,   # _PRESSURE_FORCE in toroidal scheduler
        "dw_batch_size":      500.0,    # entities per DW outreach call
        "ocw_batch_size":      50.0,    # course pages per OCW outreach
        "min_seconds":         60.0,    # min_seconds_between_rounds
        "blade_period_dw":      3.0,    # major period for dw blade
        "blade_period_ocw":     3.0,    # major period for ocw blade
    },
    "touch": {
        "max_directives":      25.0,    # max_directives_per_round
        "learning_rate":        0.30,   # _TOUCH_LR
        "resolved_grad":       -0.50,   # _RESOLVED_GRAD magnitude (signed)
        "min_seconds":         30.0,    # min_seconds_between_rounds
    },
    "smell": {
        "sensitivity":          1.00,   # multiplier on Dirichlet evidence
        "burst_priority":       0.50,   # weight of burst-class on attention
        "tau_jitter":           0.02,   # Gaussian jitter on carrier mass
    },
    "body": {
        "cadence_seconds":     60.0,    # how often body_directives surface
        "value_per_year_mult":  1.00,   # economic scaling on directives
        "owner_role_breadth":   3.0,    # how many distinct owner_roles to span
    },
    "brain": {
        "round_min_seconds":   60.0,    # global Vision round floor
        "graph_centrality_top": 50.0,   # how many high-centrality parts to track
        "synaptic_decay":       0.05,   # passive pressure decay per quiet round
    },
}

# Normalising scales — what counts as "lots of" each metric.  When the
# observed metric == its scale, the sigmoid hits 0.5 (mid-range).  Tuned
# from typical SCB corpus sizes (Apr 2026: ~30k entities, ~20k edges).
_KNOWLEDGE_SCALES: dict[str, float] = {
    "entities":      30_000.0,
    "edges":         20_000.0,
    "learnings":     10_000.0,
    "doc_chunks":     2_000.0,
    "smell_readings": 1_000.0,
    "directives":       200.0,
    "rounds":         1_000.0,
}


def _sigmoid(x: float) -> float:
    """Numerically stable sigmoid in (0, 1)."""
    if x >= 0.0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _saturating(metric: float, scale: float) -> float:
    """Map a non-negative metric to [0, 1] via metric / (metric + scale).

    Smoother than a hard sigmoid for raw counts.  At metric==0 → 0,
    at metric==scale → 0.5, asymptotes toward 1.0 as metric ≫ scale.
    """
    if metric <= 0:
        return 0.0
    return float(metric) / (float(metric) + float(scale))


# ---------------------------------------------------------------------------
# Knowledge measurement
# ---------------------------------------------------------------------------
def measure_knowledge_state() -> dict[str, float]:
    """Snapshot every metric the plasticity agent uses to decide dial moves.

    Reads from the SQLite brain.  Tables that don't exist yet return 0,
    so this is safe to call on a fresh database.
    """
    with _conn() as cn:
        return {
            "entities":      _safe_count(cn, "SELECT COUNT(*) FROM corpus_entity"),
            "edges":         _safe_count(cn, "SELECT COUNT(*) FROM corpus_edge"),
            "learnings":     _safe_count(cn, "SELECT COUNT(*) FROM learning_log"),
            "doc_chunks":    _safe_count(
                cn, "SELECT COUNT(*) FROM doc_chunk"),
            "smell_readings": _safe_count(
                cn, "SELECT COUNT(*) FROM sense_of_smell"),
            "directives":    _safe_count(
                cn, "SELECT COUNT(*) FROM body_directives"),
            "rounds":        _safe_count(
                cn, "SELECT COUNT(*) FROM corpus_round_log"),
            "ts":            time.time(),
        }


# ---------------------------------------------------------------------------
# Capability targets — knowledge → desired dial values
# ---------------------------------------------------------------------------
# Each dial has a well-defined direction:
#   * Some grow with knowledge (capacity grows with corpus size)
#   * Some shrink with knowledge (cadence speeds up, thresholds relax)
#   * Some are bounded between defaults and a stretch maximum
def compute_capability_targets(state: dict[str, float]) -> dict[str, dict[str, float]]:
    """Produce target dial values for every sense from the knowledge state.

    Linear interpolation in the *saturating* knowledge axis means as the
    corpus grows toward each metric's scale, the dial moves toward its
    "expanded" value; once knowledge ≫ scale the dial saturates.
    """
    s_ent  = _saturating(state.get("entities", 0),      _KNOWLEDGE_SCALES["entities"])
    s_edg  = _saturating(state.get("edges", 0),         _KNOWLEDGE_SCALES["edges"])
    s_lrn  = _saturating(state.get("learnings", 0),     _KNOWLEDGE_SCALES["learnings"])
    s_doc  = _saturating(state.get("doc_chunks", 0),    _KNOWLEDGE_SCALES["doc_chunks"])
    s_sml  = _saturating(state.get("smell_readings",0), _KNOWLEDGE_SCALES["smell_readings"])
    s_dir  = _saturating(state.get("directives", 0),    _KNOWLEDGE_SCALES["directives"])
    s_rnd  = _saturating(state.get("rounds", 0),        _KNOWLEDGE_SCALES["rounds"])

    # Composite "corpus richness" — used by senses that don't have a
    # natural single-axis driver (Brain, Body cadence).
    richness = (s_ent + s_edg + s_lrn) / 3.0

    # Recursive strengthening: a productive recent chain (n-1 actionable
    # potential) lifts the effective richness so the n+1 dial targets
    # stretch further toward their maxima. Capped so a single hot streak
    # cannot pin every dial at saturation.
    try:
        from .recursive_strengthening import get_actionable_potential as _ap
        potential = float(_ap(0.0))
        # Lerp halfway from current richness to 1.0, weighted by potential.
        richness = richness + (1.0 - richness) * (0.5 * potential)
    except Exception:
        pass

    def lerp(lo: float, hi: float, t: float) -> float:
        return lo + (hi - lo) * max(0.0, min(1.0, t))

    return {
        "vision": {
            # Threshold relaxes as corpus grows (we trust pressure more once
            # the model has seen plenty of data, so it's easier to force
            # outreach blades on weaker signals).
            "pressure_threshold":   lerp(0.30, 0.18, s_ent),
            "force_threshold":      lerp(0.30, 0.18, s_ent),
            # Batch sizes grow with corpus — bigger DW sweeps when we have
            # the graph to absorb them.
            "dw_batch_size":        lerp(500.0, 2000.0, s_edg),
            "ocw_batch_size":       lerp(50.0,  200.0,  s_lrn),
            # Cadence speeds up once we're learning fast.
            "min_seconds":          lerp(60.0,  20.0,   richness),
            # Blade periods shorten so the propeller spins faster on a
            # mature corpus that can chew through more outreach per round.
            "blade_period_dw":      lerp(3.0,   2.0,    s_ent),
            "blade_period_ocw":     lerp(3.0,   2.0,    s_doc),
        },
        "touch": {
            # Directive cap grows with directive history — once Touch has
            # demonstrated it can act on many signals, give it more to act on.
            "max_directives":       lerp(25.0,  100.0,  s_dir),
            # Anneal the learning rate as corpus stabilises (more data →
            # smaller corrections per round, classic ML tactic).
            "learning_rate":        lerp(0.30,  0.10,   s_rnd),
            # Resolved gradient stays stable; only nudge slightly stronger
            # when we have rich directive history (Touch can be more
            # decisive about marking signals resolved).
            "resolved_grad":        lerp(-0.50, -0.65,  s_dir),
            "min_seconds":          lerp(30.0,  10.0,   richness),
        },
        "smell": {
            # Olfactory sensitivity grows with smell history — the more
            # readings recorded, the more confident the Dirichlet posterior.
            "sensitivity":          lerp(1.0,   2.5,    s_sml),
            # Burst-class priority relaxes as corpus matures (we don't need
            # to chase every burst once we have a stable substrate).
            "burst_priority":       lerp(0.50,  0.30,   richness),
            # Carrier jitter shrinks with experience (less random noise).
            "tau_jitter":           lerp(0.02,  0.005,  s_rnd),
        },
        "body": {
            # Body cadence speeds up with knowledge.
            "cadence_seconds":      lerp(60.0,  15.0,   richness),
            # Economic scaling grows with directive count (more confidence
            # in dollar attribution once we have a track record).
            "value_per_year_mult":  lerp(1.0,   1.5,    s_dir),
            "owner_role_breadth":   lerp(3.0,   8.0,    s_lrn),
        },
        "brain": {
            "round_min_seconds":    lerp(60.0,  20.0,   richness),
            "graph_centrality_top": lerp(50.0,  300.0,  s_ent),
            "synaptic_decay":       lerp(0.05,  0.02,   s_rnd),
        },
    }


# ---------------------------------------------------------------------------
# ADAM-smoothed dial update
# ---------------------------------------------------------------------------
def _smooth_dial(cur: float, target: float, opt_state: dict) -> float:
    """One ADAM step on a single dial.

    Mutates opt_state {m, v, t} in place and returns the new dial value.
    Operates in *delta* space so dials with very different magnitudes
    (0.1 learning rate vs 2000 batch size) all converge at similar pace.
    """
    grad   = float(target) - float(cur)
    m_prev = float(opt_state.get("m", 0.0))
    v_prev = float(opt_state.get("v", 0.0))
    t      = int(opt_state.get("t", 0)) + 1

    m = _PL_BETA1 * m_prev + (1.0 - _PL_BETA1) * grad
    v = _PL_BETA2 * v_prev + (1.0 - _PL_BETA2) * (grad * grad)
    m_hat = m / (1.0 - _PL_BETA1 ** t)
    v_hat = v / (1.0 - _PL_BETA2 ** t)

    # Step is proportional to the magnitude scale of cur+target so that a
    # batch_size dial moves by ~hundreds while a learning_rate dial moves
    # by ~hundredths.  We use abs(target) + abs(cur) to set the scale.
    scale = max(1e-3, 0.5 * (abs(target) + abs(cur)))
    # Temporal-spatiality rhythm scales the rADAM lr — coherent senses
    # accelerate plasticity, the synaptic wash damps it during gradient bursts.
    try:
        from .temporal_spatiality import get_rhythm_factor as _rf
        lr = _PL_LR * float(_rf("lr_factor", 1.0))
    except Exception:
        lr = _PL_LR
    step  = lr * scale * m_hat / ((v_hat ** 0.5) + _PL_EPS)

    opt_state["m"] = m
    opt_state["v"] = v
    opt_state["t"] = t

    return float(cur) + step


# ---------------------------------------------------------------------------
# Rewire driver — the agent
# ---------------------------------------------------------------------------
def rewire_round(*, force: bool = False) -> dict[str, Any]:
    """Run one rewiring pass: measure → target → ADAM-update → persist.

    Rate-limited by ``_REWIRE_MIN_SEC`` unless ``force`` is True.
    Safe to call from inside any other round driver.

    Returns a summary dict with the previous and new dials per sense,
    plus the knowledge state that drove the move.
    """
    global _LAST_REWIRE_TS

    with _PLASTICITY_LOCK:
        now = time.monotonic()
        if not force and (now - _LAST_REWIRE_TS) < _REWIRE_MIN_SEC:
            return {"skipped": True, "reason": "rate-limited"}
        _LAST_REWIRE_TS = now

    knowledge = measure_knowledge_state()
    targets   = compute_capability_targets(knowledge)

    with _conn() as cn:
        prev = _kv_read(cn, _PLASTICITY_KEY) or {}
        prev_dials   = prev.get("dials")     or _DEFAULT_DIALS
        prev_optstate = prev.get("optstate") or {}

        new_dials:    dict[str, dict[str, float]] = {}
        new_optstate: dict[str, dict[str, dict]]  = {}

        for sense, sense_targets in targets.items():
            new_dials[sense]    = {}
            new_optstate[sense] = {}
            for dial_name, target_val in sense_targets.items():
                cur_val = float(
                    (prev_dials.get(sense) or {}).get(
                        dial_name,
                        _DEFAULT_DIALS[sense][dial_name],
                    )
                )
                opt_st = dict(
                    (prev_optstate.get(sense) or {}).get(dial_name) or {}
                )
                new_val = _smooth_dial(cur_val, float(target_val), opt_st)
                new_dials[sense][dial_name]    = round(new_val, 6)
                new_optstate[sense][dial_name] = opt_st

        full_state = {
            "dials":     new_dials,
            "targets":   targets,
            "optstate":  new_optstate,
            "knowledge": knowledge,
            "ran_at":    datetime.now(timezone.utc).isoformat(),
        }
        _kv_write(cn, _PLASTICITY_KEY, full_state)

    return {
        "skipped":   False,
        "ran_at":    full_state["ran_at"],
        "knowledge": knowledge,
        "targets":   targets,
        "dials":     new_dials,
        "prev_dials": prev_dials,
    }


# ---------------------------------------------------------------------------
# Public read accessors — what each sense calls every cycle
# ---------------------------------------------------------------------------
def get_dial(sense: str, name: str, default: float | None = None) -> float:
    """Read one dial.  Falls back to the module's default if plasticity
    hasn't been run yet, or to ``default`` if neither exists.
    """
    with _conn() as cn:
        st = _kv_read(cn, _PLASTICITY_KEY) or {}
    dials = ((st.get("dials") or {}).get(sense) or {})
    if name in dials:
        return float(dials[name])
    fallback = (_DEFAULT_DIALS.get(sense) or {}).get(name)
    if fallback is not None:
        return float(fallback)
    if default is not None:
        return float(default)
    return 0.0


def get_sense_dials(sense: str) -> dict[str, float]:
    """Read all dials for one sense as a dict, with defaults filled in."""
    with _conn() as cn:
        st = _kv_read(cn, _PLASTICITY_KEY) or {}
    out = dict(_DEFAULT_DIALS.get(sense) or {})
    out.update((st.get("dials") or {}).get(sense) or {})
    return out


def get_all_dials() -> dict[str, dict[str, float]]:
    """Read every sense's dials as a nested dict, with defaults."""
    return {sense: get_sense_dials(sense) for sense in _DEFAULT_DIALS}


def get_plasticity_state() -> dict:
    """Diagnostic accessor — full state including ADAM optstate and the
    knowledge measurement that produced the most recent dial set."""
    with _conn() as cn:
        return _kv_read(cn, _PLASTICITY_KEY) or {}


__all__ = [
    "measure_knowledge_state",
    "compute_capability_targets",
    "rewire_round",
    "get_dial",
    "get_sense_dials",
    "get_all_dials",
    "get_plasticity_state",
]
