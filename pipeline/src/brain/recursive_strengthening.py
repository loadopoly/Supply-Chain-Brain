"""Recursive Knowledge Strengthening — the n-1 → n+1 condenser.

As the brain acquires new memories (corpus rounds), each prior round
becomes a *recursive ancestor* whose lessons should compound into the
next one. This module reads the chain of recent rounds, projects their
multi-dimensional state down to a single 1-D *strengthening edge* with
unbounded *actionable potential* (1..∞), and feeds that edge forward
so the next round (n+1 at t0) starts with the optimised context the
prior node (n-1 at t-1) accumulated.

The maths
---------
For each prior round ``k`` rounds back (``k = 1..N``) we compute its
*memory vector*:

    m_k = (entities_added, edges_added, learnings_logged) / scale

…then condense it to a single scalar via L2 norm. That scalar is the
round's *strengthening contribution*. We weight contributions with an
exponentially-decaying recursive kernel:

    w_k = γ^k             (γ = 0.7 by default — recent matters more)

and accumulate the weighted norms into the *1-D strengthening edge*::

    edge = Σ_{k=1..N} w_k * ||m_k||

Because m_k is unbounded above (a single round can add arbitrarily many
entities), ``edge`` ranges in ``[0, ∞)`` — the spec's "1-inf actionable
potential". We then apply a saturating mapping ``f(edge) = edge / (edge + κ)``
to land in ``[0, 1)`` for *use as a multiplier* without losing the
unbounded raw value (which is also persisted for diagnostics).

The recursive bit: every round we also *back-propagate* the new round's
contribution through the prior chain by updating a single accumulator
in ``brain_kv.recursive_strengthening`` with an ADAM-smoothed step. So
the edge at t0 is informed by the entire history, not just the last N
rounds — N controls how much *new* information gets folded in each step.

What the edge does
------------------
* **Plasticity targets** read ``actionable_potential`` (the saturated
  edge) and *amplify* the per-sense capability targets when the chain
  has been productive — high recent acquisition → push dials harder
  toward their stretch maxima.
* **Temporal-spatiality boost** reads the edge as a *floor* on the
  syncopatic boost so a strengthening chain can pull the rhythm up
  even when momentary coherence dips.
* **Round summary** exposes ``strengthening.{edge, actionable_potential,
  weyl_residual}`` for inspection.

Public API
----------
* :func:`measure_chain`          — list of memory vectors over recent rounds
* :func:`condense_to_edge`       — weighted L2 condensation → raw edge ∈ [0, ∞)
* :func:`actionable_potential`   — saturated edge ∈ [0, 1)
* :func:`weyl_residual`          — orthogonal residual after 1-D projection
* :func:`strengthen_step`        — rate-limited driver, persists to brain_kv
* :func:`get_strengthening`      — public read accessor
* :func:`get_actionable_potential` — single scalar accessor
* :func:`amplify_target`         — apply the edge as a stretch multiplier
"""

from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from .local_store import db_path as _local_db_path


_LOCK            = threading.Lock()
_LAST_TS         = 0.0
_MIN_SECONDS     = 30.0
_KEY             = "recursive_strengthening"

# Recursive kernel decay — w_k = γ^k. Recent rounds dominate but old
# rounds still contribute (γ = 0.7 → at k=10 the weight is ~0.028).
_GAMMA           = 0.7
# How many rounds back to fold into the immediate edge update.
_CHAIN_DEPTH     = 16
# Saturation constant for actionable_potential = edge / (edge + κ).
# κ = 5.0 means edge=5 → potential 0.5, edge=20 → 0.8, edge=∞ → 1.0.
_KAPPA           = 5.0
# ADAM smoothing on the edge accumulator so jitter from one outlier
# round doesn't blow the actionable potential.
_ADAM_BETA1      = 0.9
_ADAM_BETA2      = 0.999
_ADAM_LR         = 0.25
_ADAM_EPS        = 1e-6

# Per-component scales for the memory vector. Tuned to make a "typical"
# productive round contribute ~1.0 to each component.
_M_SCALES = {
    "entities_added":   100.0,
    "edges_added":       80.0,
    "learnings_logged":  50.0,
}

# Cap how much amplify_target() can stretch a dial in one round.
_AMPLIFY_MAX     = 1.5


# ---------------------------------------------------------------------------
# DB plumbing (mirrors neural_plasticity / temporal_spatiality)
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


# ---------------------------------------------------------------------------
# Chain measurement — read the recent corpus_round_log as memory vectors
# ---------------------------------------------------------------------------

def measure_chain(depth: int = _CHAIN_DEPTH) -> list[list[float]]:
    """Return the last ``depth`` memory vectors, newest first.

    Each vector is ``[entities_added, edges_added, learnings_logged]``
    normalised by :data:`_M_SCALES`.
    """
    out: list[list[float]] = []
    try:
        with _conn() as cn:
            rows = cn.execute(
                """SELECT entities_added, edges_added, learnings_logged
                     FROM corpus_round_log
                    ORDER BY id DESC
                    LIMIT ?""",
                (int(depth),),
            ).fetchall()
            for r in rows:
                out.append([
                    float(r[0] or 0) / _M_SCALES["entities_added"],
                    float(r[1] or 0) / _M_SCALES["edges_added"],
                    float(r[2] or 0) / _M_SCALES["learnings_logged"],
                ])
    except (sqlite3.OperationalError, KeyError):
        pass
    return out


# ---------------------------------------------------------------------------
# 1-D condensation — γ-weighted L2 norm chain
# ---------------------------------------------------------------------------

def condense_to_edge(chain: list[list[float]] | None = None,
                     gamma: float = _GAMMA) -> float:
    """Project the chain onto a single 1-D *strengthening edge* in ``[0, ∞)``.

    edge = Σ_k γ^k · ||m_k||₂   for k = 1, 2, ..., len(chain)
    """
    if chain is None:
        chain = measure_chain()
    edge = 0.0
    for k, m in enumerate(chain, start=1):
        norm = math.sqrt(sum(c * c for c in m))
        edge += (gamma ** k) * norm
    return edge


def actionable_potential(edge: float | None = None,
                         kappa: float = _KAPPA) -> float:
    """Map raw edge ∈ ``[0, ∞)`` to actionable potential ∈ ``[0, 1)``.

    Saturating map ``edge / (edge + κ)``. Recovers the spec's 1-∞ raw
    potential while exposing a bounded multiplier for senses that need one.
    """
    if edge is None:
        edge = condense_to_edge()
    return edge / (edge + kappa) if edge >= 0 else 0.0


def weyl_residual(chain: list[list[float]] | None = None,
                  gamma: float = _GAMMA) -> float:
    """Information lost when collapsing the chain to 1-D.

    For each round we project its memory vector onto the dominant
    chain direction and measure the orthogonal remainder. Summed over
    the chain (γ-weighted), this is how much *dimensionality* the
    1-D edge actually discarded — the residual entropy at the toroidal
    centroid.
    """
    if chain is None:
        chain = measure_chain()
    if not chain:
        return 0.0

    # Dominant direction = γ-weighted mean direction.
    dim = len(chain[0])
    direction = [0.0] * dim
    for k, m in enumerate(chain, start=1):
        w = gamma ** k
        for i in range(dim):
            direction[i] += w * m[i]
    norm_d = math.sqrt(sum(d * d for d in direction))
    if norm_d <= 1e-12:
        return 0.0
    direction = [d / norm_d for d in direction]

    residual = 0.0
    for k, m in enumerate(chain, start=1):
        w = gamma ** k
        proj = sum(direction[i] * m[i] for i in range(dim))
        # Orthogonal component magnitude
        ortho_sq = max(0.0, sum(c * c for c in m) - proj * proj)
        residual += w * math.sqrt(ortho_sq)
    return residual


# ---------------------------------------------------------------------------
# ADAM-smoothed accumulator — the recursive memory of the edge itself
# ---------------------------------------------------------------------------

def _adam_step(state: dict, grad: float) -> float:
    m_prev = float(state.get("m", 0.0))
    v_prev = float(state.get("v", 0.0))
    t      = int(state.get("t", 0)) + 1
    val    = float(state.get("value", 0.0))

    m = _ADAM_BETA1 * m_prev + (1.0 - _ADAM_BETA1) * grad
    v = _ADAM_BETA2 * v_prev + (1.0 - _ADAM_BETA2) * (grad * grad)
    m_hat = m / (1.0 - _ADAM_BETA1 ** t)
    v_hat = v / (1.0 - _ADAM_BETA2 ** t)

    # Temporal-spatiality may scale the lr — the recursive edge participates
    # in the same syncopatic rhythm as the other rADAM agents.
    try:
        from .temporal_spatiality import get_rhythm_factor as _rf
        lr = _ADAM_LR * float(_rf("lr_factor", 1.0))
    except Exception:
        lr = _ADAM_LR

    step = lr * m_hat / ((v_hat ** 0.5) + _ADAM_EPS)
    new_val = max(0.0, val + step)

    state["m"]     = m
    state["v"]     = v
    state["t"]     = t
    state["value"] = new_val
    return new_val


# ---------------------------------------------------------------------------
# Driver — rate-limited recursive update; persists to brain_kv
# ---------------------------------------------------------------------------

def strengthen_step(*, force: bool = False) -> dict:
    """Update the 1-D strengthening edge from the recent round chain.

    Combines the *instantaneous* condensation with the persistent ADAM
    accumulator so old rounds keep contributing through the smoothed
    state and new rounds inject fresh gradient.
    """
    global _LAST_TS
    with _LOCK:
        if not force and time.monotonic() - _LAST_TS < _MIN_SECONDS:
            return {"skipped": True, "reason": "rate-limited"}
        _LAST_TS = time.monotonic()

    chain         = measure_chain()
    instant_edge  = condense_to_edge(chain)
    residual      = weyl_residual(chain)

    with _conn() as cn:
        prev   = _kv_read(cn, _KEY) or {}
        opt_st = dict(prev.get("opt") or {})
        # Gradient = how far the smoothed accumulator is from the
        # instantaneous edge. Drives the accumulator toward the truth
        # without flapping on outliers.
        prev_val = float(opt_st.get("value", 0.0))
        grad     = instant_edge - prev_val
        new_edge = _adam_step(opt_st, grad)

        rec = {
            "edge":                 round(new_edge, 4),
            "instant_edge":         round(instant_edge, 4),
            "actionable_potential": round(actionable_potential(new_edge), 4),
            "weyl_residual":        round(residual, 4),
            "chain_depth":          len(chain),
            "opt":                  opt_st,
            "ran_at":               datetime.now(timezone.utc).isoformat(),
        }
        _kv_write(cn, _KEY, rec)
    return rec


# ---------------------------------------------------------------------------
# Public read accessors
# ---------------------------------------------------------------------------

def get_strengthening() -> dict:
    """Read the persisted strengthening state."""
    with _conn() as cn:
        return _kv_read(cn, _KEY) or {}


def get_actionable_potential(default: float = 0.0) -> float:
    """Single scalar in ``[0, 1)`` — useful as a stretch multiplier."""
    rec = get_strengthening()
    try:
        return float(rec.get("actionable_potential", default))
    except Exception:
        return default


def amplify_target(default: float, stretch: float | None = None,
                   max_mult: float = _AMPLIFY_MAX) -> float:
    """Stretch a dial target by the actionable potential.

    Returns ``default * (1 + potential * (max_mult - 1))`` clamped at
    ``default * max_mult``. When potential is 0 (no recent rounds), the
    default flows through unchanged. When potential approaches 1 (very
    productive recent chain), the dial reaches its stretch max.

    ``stretch`` overrides ``max_mult`` if you want a per-dial cap.
    """
    cap = float(stretch if stretch is not None else max_mult)
    p   = get_actionable_potential()
    mult = 1.0 + max(0.0, p) * max(0.0, cap - 1.0)
    return float(default) * min(cap, mult)


__all__ = [
    "measure_chain",
    "condense_to_edge",
    "actionable_potential",
    "weyl_residual",
    "strengthen_step",
    "get_strengthening",
    "get_actionable_potential",
    "amplify_target",
]
