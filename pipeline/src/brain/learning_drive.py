"""Symbiotic Learning Drive — directional gradient bias from the Brain's own
learning health.

This module closes the internal feedback loop between the rADAM optimizer and
the rest of the Supply Chain Brain's learning subsystems:

    ┌──────────────────────────────────────────────────────────────────────┐
    │                  SYMBIOTIC INTERNAL LOOP                            │
    │                                                                      │
    │  corpus saturation ──────────► pivot_alpha  (ReLU tightness)        │
    │  self-train quality ─────────► heartbeat_kappa (energy oscillation) │
    │  learning stagnation ────────► noise_sigma  (Langevin exploration)  │
    │  difficulty × room ──────────► acquisition_drive (grad_imag boost)  │
    │                                                                      │
    │  rADAM drives Touch pressure → Vision outreach → corpus grows       │
    │  corpus grows → saturation changes → pivot_alpha adjusts → loop     │
    └──────────────────────────────────────────────────────────────────────┘

Math
----
Let:
    s = corpus saturation ∈ [0, 1]      (weighted entities/edges/learnings vs scale)
    q = self-train quality ∈ [0, 1]     (recent avg_validator normalised, 0 = random)
    v = learning velocity ∈ [0, 1]      (recent learning density via tanh)
    d = RDT task difficulty ∈ [0, 1]    (1 − mean convergence rate across tasks)

Derived rADAM knobs (all reduce to identity / zero when the DB is empty):

    pivot_alpha      = 1.0 − 0.60 · s               ∈ [0.40, 1.00]
    heartbeat_kappa  = 0.25 · (1 − q)               ∈ [0.00, 0.25]
    noise_sigma      = 0.15 · (1 − v) + 0.10 · d   ∈ [0.00, 0.25]
    acquisition_drive = 0.20 · (1−s) · d
                      + 0.10 · (1 − v)              ∈ [0.00, 0.30]

``acquisition_drive`` is injected by ``brain_body_signals._adam_step`` as an
additive boost on ``grad_imag``, combining with the existing torus-gap latent
gradient to form a composite imaginary gradient that pushes the optimizer
toward under-explored knowledge regions when learning is stagnant or difficult.

Public API
----------
    compute_drive() -> LearningDrive      # always fresh
    get_drive()     -> LearningDrive      # cached, recomputes every 60 s
"""
from __future__ import annotations

import math
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .local_store import db_path as _local_db_path


# ---------------------------------------------------------------------------
# Module-level cache
# ---------------------------------------------------------------------------
_DRIVE_LOCK     = threading.Lock()
_CACHED_DRIVE: "LearningDrive | None" = None
_LAST_COMPUTED: float = 0.0
_CACHE_TTL: float = 60.0   # maximum recompute frequency

# Normalising scales (match neural_plasticity._KNOWLEDGE_SCALES)
_SCALE_ENTITIES  = 30_000.0
_SCALE_EDGES     = 20_000.0
_SCALE_LEARNINGS = 10_000.0


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class LearningDrive:
    """Instantaneous snapshot of the Brain's learning health.

    Each field is a float in a bounded range.  All optimizer-knob fields
    reduce to their identity values when the Brain has no learning history
    (fresh DB), so the drive is safe to pass to rADAM from the very first
    optimizer call.
    """
    # ── Optimizer knobs (consumed directly by rADAM) ──────────────────────
    pivot_alpha:       float  # pivoted-ReLU slope        ∈ [0.40, 1.00]
    heartbeat_kappa:   float  # oscillation amplitude      ∈ [0.00, 0.25]
    noise_sigma:       float  # Langevin noise scale       ∈ [0.00, 0.25]
    acquisition_drive: float  # grad_imag additive bias    ∈ [0.00, 0.30]

    # ── Diagnostic signals (exposed for dashboards / tests) ───────────────
    corpus_saturation:  float  # composite s               ∈ [0, 1]
    self_train_quality: float  # q, normalised             ∈ [0, 1]
    learning_velocity:  float  # v                         ∈ [0, 1]
    rdt_difficulty:     float  # d                         ∈ [0, 1]

    computed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _saturating(x: float, scale: float) -> float:
    """x / (x + scale) — smooth [0, ∞) → [0, 1) saturation."""
    if x <= 0.0:
        return 0.0
    return float(x) / (float(x) + float(scale))


def _safe_count(cn: sqlite3.Connection, sql: str) -> int:
    try:
        row = cn.execute(sql).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0


def _identity_drive() -> LearningDrive:
    """Return a no-op drive: all optimizer knobs at identity / zero."""
    return LearningDrive(
        pivot_alpha=1.0,
        heartbeat_kappa=0.0,
        noise_sigma=0.0,
        acquisition_drive=0.0,
        corpus_saturation=0.0,
        self_train_quality=0.5,
        learning_velocity=0.0,
        rdt_difficulty=0.5,
    )


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------
def compute_drive() -> LearningDrive:
    """Compute a fresh LearningDrive from the current DB state.

    All reads are soft-failing.  Missing tables, locked DB, or any other
    exception returns ``_identity_drive()`` so rADAM always gets valid inputs.
    """
    try:
        cn = sqlite3.connect(_local_db_path(), timeout=2.0)
        cn.row_factory = sqlite3.Row
    except Exception:
        return _identity_drive()

    try:
        # ── 1. Corpus saturation s ∈ [0, 1] ──────────────────────────────
        entities  = _safe_count(cn, "SELECT COUNT(*) FROM corpus_entity")
        edges     = _safe_count(cn, "SELECT COUNT(*) FROM corpus_edge")
        learnings = _safe_count(cn, "SELECT COUNT(*) FROM learning_log")

        s_ent = _saturating(entities,  _SCALE_ENTITIES)
        s_edg = _saturating(edges,     _SCALE_EDGES)
        s_lrn = _saturating(learnings, _SCALE_LEARNINGS)
        s = s_ent * 0.4 + s_edg * 0.3 + s_lrn * 0.3

        # ── 2. Self-train quality q ∈ [0, 1] ─────────────────────────────
        # ``learning_log`` ingests ``llm_self_train_log`` via
        # ``_ingest_self_train`` with signal_strength = avg_validator.
        # avg_validator ∈ [0.5, 1.0] (0.5 = random baseline).
        # We normalise to [0, 1] so q=0 means random, q=1 means perfect.
        try:
            q_rows = cn.execute(
                """SELECT signal_strength FROM learning_log
                   WHERE kind = 'self_train' AND signal_strength IS NOT NULL
                   ORDER BY id DESC LIMIT 20"""
            ).fetchall()
            if q_rows:
                raw_q = sum(float(r["signal_strength"]) for r in q_rows) / len(q_rows)
                q = max(0.0, min(1.0, (raw_q - 0.5) / 0.5))
            else:
                q = 0.5   # no history → mid quality (moderate heartbeat)
        except Exception:
            q = 0.5

        # ── 3. Learning velocity v ∈ [0, 1] ──────────────────────────────
        # Derived from the density of recent signal in the learning_log.
        # ``tanh(count/10 × mean_signal)`` saturates naturally: 30 rows at
        # avg signal 0.5 → v ≈ 0.91; 5 rows at 0.2 → v ≈ 0.10.
        try:
            v_rows = cn.execute(
                """SELECT signal_strength FROM learning_log
                   ORDER BY id DESC LIMIT 30"""
            ).fetchall()
            if v_rows:
                sigs = [float(r["signal_strength"] or 0.3) for r in v_rows]
                v = math.tanh(len(sigs) / 10.0 * (sum(sigs) / len(sigs)))
            else:
                v = 0.0   # no learnings at all → stagnant
        except Exception:
            v = 0.0

        # ── 4. RDT difficulty d ∈ [0, 1] ─────────────────────────────────
        # 1 − mean convergence rate across all recurrent-depth tasks.
        # d=0: all tasks converge instantly (easy).
        # d=1: no tasks converge (highly difficult / uncertain).
        try:
            d_row = cn.execute(
                """SELECT AVG(CAST(converged AS REAL)) AS conv_rate
                   FROM recurrent_depth_log"""
            ).fetchone()
            if d_row and d_row["conv_rate"] is not None:
                d = max(0.0, min(1.0, 1.0 - float(d_row["conv_rate"])))
            else:
                d = 0.5   # no RDT history → assume moderate difficulty
        except Exception:
            d = 0.5

    except Exception:
        return _identity_drive()
    finally:
        try:
            cn.close()
        except Exception:
            pass

    # ── Derive optimizer knobs from (s, q, v, d) ──────────────────────────
    # pivot_alpha: tighten the pivot as the corpus saturates.
    #   Fresh corpus (s=0) → alpha=1.0 (identity).
    #   Saturated corpus (s=1) → alpha=0.40 (strong attenuation below pivot).
    pivot_alpha = max(0.40, 1.0 - 0.60 * s)

    # heartbeat_kappa: energise the oscillation when self-training is unreliable.
    #   High quality (q→1) → kappa=0.  Random quality (q=0) → kappa=0.25.
    heartbeat_kappa = max(0.0, min(0.25, 0.25 * (1.0 - q)))

    # noise_sigma: inject Langevin exploration when learning is stagnant or
    # tasks are difficult.
    noise_sigma = max(0.0, min(0.25, 0.15 * (1.0 - v) + 0.10 * d))

    # acquisition_drive: directional bias toward high-yield unexplored space.
    #   High when corpus has room (1-s) AND tasks are hard (d) AND velocity is low.
    acquisition_drive = max(0.0, min(0.30,
                                     0.20 * (1.0 - s) * d
                                     + 0.10 * (1.0 - v)))

    return LearningDrive(
        pivot_alpha=round(pivot_alpha,       4),
        heartbeat_kappa=round(heartbeat_kappa,   4),
        noise_sigma=round(noise_sigma,       4),
        acquisition_drive=round(acquisition_drive, 4),
        corpus_saturation=round(s,  4),
        self_train_quality=round(q, 4),
        learning_velocity=round(v,  4),
        rdt_difficulty=round(d,     4),
    )


# ---------------------------------------------------------------------------
# Cached public accessor
# ---------------------------------------------------------------------------
def get_drive() -> LearningDrive:
    """Return a cached LearningDrive, recomputing at most every 60 seconds.

    Thread-safe.  Never raises — falls back to identity drive on any error.
    """
    global _CACHED_DRIVE, _LAST_COMPUTED
    with _DRIVE_LOCK:
        now = time.monotonic()
        if _CACHED_DRIVE is None or (now - _LAST_COMPUTED) >= _CACHE_TTL:
            try:
                _CACHED_DRIVE = compute_drive()
            except Exception:
                if _CACHED_DRIVE is None:
                    _CACHED_DRIVE = _identity_drive()
            _LAST_COMPUTED = now
        return _CACHED_DRIVE


__all__ = ["LearningDrive", "compute_drive", "get_drive"]
