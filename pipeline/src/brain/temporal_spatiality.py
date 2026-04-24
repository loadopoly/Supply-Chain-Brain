"""Temporal-Spatiality — the cross-sense rhythm coordinator.

The five senses (Vision, Touch, Smell, Body, Brain) are individually
plastic and interconnected (Smell decay → Touch force → Vision reach).
This module is the *temporal-spatial* layer that coordinates them at the
rhythmic level: it measures the coherence of all five senses together,
projects their joint state onto a 1-D Weyl coordinate at the toroidal
centroid, and modulates the syncopatic rhythm of every rADAM optimizer
in the system so that high-coherence moments accelerate training while
high-gradient moments wash that acceleration back down (the synaptic
wash) to keep the loop stable.

Concepts mapped to code
-----------------------
* **Temporal-Spatiality** — coordinated motion across all five senses,
  measured as :func:`measure_coherence` (a single scalar in ``[0, 1]``).
* **rADAM Optimizer agents** — the per-dial ADAM optimizers in
  :mod:`neural_plasticity` and the per-signal-kind ADAM in
  :mod:`brain_body_signals` (Touch). Their rhythm is the *learning rate*
  and the *rate-limit floors* (``round_min_seconds``, ``cadence_seconds``).
* **Syncopatic rhythm** — when coherence is high, we *temporarily*
  raise rADAM lr and shorten cadences (boost). When it falls, we relax
  back toward steady-state.
* **Synaptic wash** — the periodicity modulator: when the relational
  gradient (Touch pressure-field magnitude × Smell decay) is steep,
  the wash *damps* the boost so the system never accelerates into an
  unstable region. Implemented as a sigmoid on the gradient norm.
* **1-D Weyl Structure at the toroidal centroid** — the joint sense
  state is projected onto a single scalar by taking the *circular mean*
  of the 7-D torus angles (``torus_touch.TORUS_DIMS = 7``) weighted by
  the per-sense coherence contributions, then unfolded onto ``[0, 2π]``.
  This is the condensed coordinate the orchestrator reads.

Public API
----------
* :func:`measure_coherence`     — joint coherence scalar over the senses
* :func:`relational_gradient`   — magnitude of the cross-sense gradient
* :func:`weyl_centroid`         — 1-D condensed coordinate at torus centre
* :func:`modulate`              — combine the above into a *rhythm* dict
* :func:`temporal_step`         — rate-limited driver, persists rhythm
* :func:`get_rhythm`            — public read accessor (senses use this)
* :func:`get_rhythm_factor`     — single multiplier ``(0.5 .. 1.5)``
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


_LOCK            = threading.Lock()
_LAST_TS         = 0.0
_MIN_SECONDS     = 15.0   # rate-limit between rhythm updates
_RHYTHM_KEY      = "temporal_spatiality_rhythm"

# Bounds on the syncopatic boost so the wash can never push the system
# into instability — even at perfect coherence the lr / cadence change
# is at most ±50%.
_BOOST_MIN       = 0.5
_BOOST_MAX       = 1.5
_BOOST_NEUTRAL   = 1.0

# Weights of each sense in the coherence score. Sum to 1.0 by design.
_SENSE_WEIGHTS = {
    "vision": 0.25,
    "touch":  0.25,
    "smell":  0.20,
    "body":   0.15,
    "brain":  0.15,
}


# ---------------------------------------------------------------------------
# DB helpers (mirrors neural_plasticity for consistency)
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


def _safe_scalar(cn, sql: str, default: float = 0.0) -> float:
    try:
        row = cn.execute(sql).fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except sqlite3.OperationalError:
        pass
    return default


# ---------------------------------------------------------------------------
# Sense state extraction
# ---------------------------------------------------------------------------

def _sense_signals() -> dict:
    """Read fresh per-sense health signals.

    Each signal lives in ``[0, 1]`` and represents how *active* that
    sense was in its most recent cycle. We use these as the components
    of the joint coherence vector.
    """
    out = {
        "vision": 0.0,
        "touch":  0.0,
        "smell":  0.0,
        "body":   0.0,
        "brain":  0.0,
    }

    with _conn() as cn:
        # Vision — recent corpus_round activity (entities discovered).
        v = _safe_scalar(
            cn,
            """SELECT MIN(1.0, COALESCE(SUM(entities_added), 0) / 50.0)
                 FROM corpus_round_log
                WHERE ran_at >= datetime('now', '-1 hour')""",
        )
        out["vision"] = max(0.0, min(1.0, v))

        # Touch — current pressure field magnitude (read from brain_kv).
        try:
            field = _kv_read(cn, "touch_pressure_field") or {}
            if field:
                pmax = max(float(p) for p in field.values())
                out["touch"] = max(0.0, min(1.0, pmax))
        except Exception:
            pass

        # Smell — recent reading carrier mass (high mass = fresh signal).
        m = _safe_scalar(
            cn,
            """SELECT carrier_mass FROM sense_of_smell
                ORDER BY id DESC LIMIT 1""",
            default=1.0,
        )
        # Map carrier mass into "active sense" — both very-high (fresh)
        # and very-low (decaying fast) count as active.
        out["smell"] = max(0.0, min(1.0, abs(m - 0.5) * 2.0)) if m else 0.0

        # Body — open-directive activity, capped at 25.
        b = _safe_scalar(
            cn,
            """SELECT MIN(1.0, CAST(COUNT(*) AS REAL) / 25.0)
                 FROM body_directives
                WHERE status IN ('open','ack','in_progress')""",
        )
        out["body"] = b

        # Brain — round cadence (more rounds in last hour → more active).
        n = _safe_scalar(
            cn,
            """SELECT MIN(1.0, COUNT(*) / 60.0)
                 FROM corpus_round_log
                WHERE ran_at >= datetime('now', '-1 hour')""",
        )
        out["brain"] = n

    return out


# ---------------------------------------------------------------------------
# Coherence — circular alignment of the per-sense vector
# ---------------------------------------------------------------------------

def measure_coherence(signals: dict | None = None) -> float:
    """Return joint coherence in ``[0, 1]``.

    Coherence is *high* when every sense is firing at a similar level
    (uniform activation, no silent senses). It is the weighted mean
    activity multiplied by ``(1 - normalised dispersion)`` so a perfect
    score requires both:

      * non-trivial activity across all five senses, and
      * low spread between them.

    A single saturated sense with the rest silent gives near-zero
    coherence (uneven). All five at ~0.8 gives a near-perfect score
    (the rhythmic moment the spec calls for).
    """
    if signals is None:
        signals = _sense_signals()

    senses = list(_SENSE_WEIGHTS.keys())
    activities = [float(signals.get(s, 0.0)) for s in senses]
    weights    = [_SENSE_WEIGHTS[s]          for s in senses]

    # Weighted mean activity.
    mean_a = sum(a * w for a, w in zip(activities, weights))
    if mean_a <= 0.0:
        return 0.0

    # Weighted dispersion around the mean, normalised to [0, 1] by
    # dividing by the maximum possible weighted spread (mean itself).
    var = sum(w * (a - mean_a) ** 2 for a, w in zip(activities, weights))
    std = math.sqrt(var)
    norm_disp = min(1.0, std / max(mean_a, 1e-9))

    coherence = mean_a * (1.0 - norm_disp)
    return max(0.0, min(1.0, coherence))


# ---------------------------------------------------------------------------
# Relational gradient — the synaptic wash damper
# ---------------------------------------------------------------------------

def relational_gradient(signals: dict | None = None) -> float:
    """Magnitude of the cross-sense relational gradient in ``[0, 1]``.

    Combines:
      * Touch pressure-field magnitude (how hard Body is pushing Vision).
      * Smell decay (relational distance of stale knowledge).
      * Spread between strongest and weakest sense (uneven activity).

    A high gradient means the senses are rapidly trying to re-align —
    this is the moment when accelerating the rADAM rhythm risks
    overshoot, so the synaptic wash will damp the boost.
    """
    if signals is None:
        signals = _sense_signals()

    # Touch pressure component
    touch = float(signals.get("touch", 0.0))

    # Smell decay component — 1.0 - mass. Read freshly (don't reuse the
    # bell-curved "smell" signal, which equals 0 at mid mass).
    decay = 0.0
    try:
        with _conn() as cn:
            mass = _safe_scalar(
                cn,
                """SELECT carrier_mass FROM sense_of_smell
                    ORDER BY id DESC LIMIT 1""",
                default=1.0,
            )
            decay = max(0.0, min(1.0, 1.0 - mass))
    except Exception:
        pass

    # Sense-spread component — how unbalanced is the system.
    activities = [float(signals.get(s, 0.0)) for s in _SENSE_WEIGHTS]
    spread = (max(activities) - min(activities)) if activities else 0.0

    # Mean of the three components.
    grad = (touch + decay + spread) / 3.0
    return max(0.0, min(1.0, grad))


# ---------------------------------------------------------------------------
# 1-D Weyl coordinate at the toroidal centroid
# ---------------------------------------------------------------------------

def weyl_centroid(signals: dict | None = None) -> float:
    """Project the joint sense state onto ``[0, 2π]`` at the torus centre.

    The 5 senses are mapped to evenly-spaced angles on the 7-D torus
    (the first 5 of the ``TORUS_DIMS = 7`` dimensions), each weighted
    by its activity level. The circular mean of those angles is the
    1-D Weyl coordinate — a single scalar that captures *where* the
    system's energy is concentrated on the torus.
    """
    if signals is None:
        signals = _sense_signals()

    try:
        from .torus_touch import TORUS_DIMS
    except Exception:
        TORUS_DIMS = 7

    senses = list(_SENSE_WEIGHTS.keys())
    n = len(senses)

    # Senses on the first n dimensions of the torus, evenly spaced
    # around their share of the angular range.
    sx = 0.0
    sy = 0.0
    for i, sense in enumerate(senses):
        # Map sense index → torus angle
        theta = 2.0 * math.pi * (i / TORUS_DIMS)
        a = float(signals.get(sense, 0.0))
        sx += a * math.cos(theta)
        sy += a * math.sin(theta)

    mean_angle = math.atan2(sy, sx)
    if mean_angle < 0.0:
        mean_angle += 2.0 * math.pi
    return mean_angle


# ---------------------------------------------------------------------------
# Rhythm modulation — coherence boosts, gradient washes back
# ---------------------------------------------------------------------------

def modulate(signals: dict | None = None) -> dict:
    """Compute the rhythm dict from the current sense state.

    Output keys:
      * ``coherence``       — joint coherence ``[0, 1]``
      * ``gradient``        — relational gradient ``[0, 1]``
      * ``weyl``            — 1-D Weyl coordinate at torus centroid
      * ``boost``           — rADAM lr / cadence multiplier ``[0.5, 1.5]``
                              ``boost = 1 + (coherence - gradient) * 0.5``
                              clamped to ``[_BOOST_MIN, _BOOST_MAX]``
      * ``period_factor``   — rate-limit floor multiplier ``= 1 / boost``
                              (high boost → shorter floors → faster rounds)
      * ``lr_factor``       — rADAM learning-rate multiplier ``= boost``
      * ``signals``         — the raw per-sense activity dict
    """
    if signals is None:
        signals = _sense_signals()

    coh  = measure_coherence(signals)
    grad = relational_gradient(signals)
    weyl = weyl_centroid(signals)

    # Syncopatic boost: high coherence raises the boost, the synaptic
    # wash (gradient) lowers it. Net effect is bounded.
    raw_boost = _BOOST_NEUTRAL + (coh - grad) * 0.5
    boost     = max(_BOOST_MIN, min(_BOOST_MAX, raw_boost))

    # period_factor multiplies floor seconds — boost > 1 → smaller floor.
    period_factor = 1.0 / boost
    # lr_factor multiplies ADAM learning rates — boost > 1 → faster lr.
    lr_factor     = boost

    return {
        "coherence":     round(coh, 4),
        "gradient":      round(grad, 4),
        "weyl":          round(weyl, 4),
        "boost":         round(boost, 4),
        "period_factor": round(period_factor, 4),
        "lr_factor":     round(lr_factor, 4),
        "signals":       {k: round(v, 4) for k, v in signals.items()},
    }


# ---------------------------------------------------------------------------
# Driver — rate-limited; persists into brain_kv
# ---------------------------------------------------------------------------

def temporal_step(*, force: bool = False) -> dict:
    """Compute and persist the current rhythm. Rate-limited."""
    global _LAST_TS
    with _LOCK:
        if not force and time.monotonic() - _LAST_TS < _MIN_SECONDS:
            return {"skipped": True, "reason": "rate-limited"}
        _LAST_TS = time.monotonic()

    rhythm = modulate()
    rhythm["ran_at"] = datetime.now(timezone.utc).isoformat()
    with _conn() as cn:
        _kv_write(cn, _RHYTHM_KEY, rhythm)
    return rhythm


# ---------------------------------------------------------------------------
# Public read accessors
# ---------------------------------------------------------------------------

def get_rhythm() -> dict:
    """Read the persisted rhythm dict."""
    with _conn() as cn:
        return _kv_read(cn, _RHYTHM_KEY) or {}


def get_rhythm_factor(name: str = "boost", default: float = 1.0) -> float:
    """Read a single rhythm scalar — used by senses to scale their lr / cadence.

    Common keys:
      * ``"boost"``         — the syncopatic multiplier (default neutral 1.0)
      * ``"period_factor"`` — multiply rate-limit floors by this
      * ``"lr_factor"``     — multiply ADAM learning rates by this
    """
    r = get_rhythm()
    try:
        return float(r.get(name, default))
    except Exception:
        return default


__all__ = [
    "measure_coherence",
    "relational_gradient",
    "weyl_centroid",
    "modulate",
    "temporal_step",
    "get_rhythm",
    "get_rhythm_factor",
]
