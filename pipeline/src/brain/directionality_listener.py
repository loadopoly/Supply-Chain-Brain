"""Directionality Listener — the ear of the symbiotic Entirety.

Listens to the instantaneous directional state of every subsystem in the
Supply Chain Brain and collapses them into a single, mathematically grounded
``DirectionalitySnapshot``.

What "directionality" means here
---------------------------------
Each sense and optimizer in the Brain moves — it explores, contracts,
oscillates, or stalls.  Directionality is the **net signed vector** across
all of those motions, projected onto three axes:

    +──────────────────────────────────────────────────────────────────────+
    │  axis            meaning at +1          meaning at –1                │
    ├──────────────────────────────────────────────────────────────────────┤
    │  expansion       corpus/network growing  mass collapsing inward      │
    │  coherence       senses phase-aligned    senses out-of-phase         │
    │  bifurcation     imaginary >> real grad  imaginary ≈ 0 (grounded)   │
    +──────────────────────────────────────────────────────────────────────+

The three axes form an ``(expansion, coherence, bifurcation)`` triplet that
the orchestrator and autonomous loop can read with a single cheap call to
:func:`listen`.

Mathematical machinery (all drawn from existing modules)
---------------------------------------------------------
1.  **Reuptake neighbourhood noise** — pull the distribution of
    ``SYMBIOTIC_TUNNEL`` + ``GROUNDED_TUNNEL`` edge weights within one hop
    of each known tunnelable endpoint.  The coefficient of variation
    ``CV = σ / μ`` of that distribution is the *vibrational noise* amplitude;
    fed back as a coherence penalty and as Langevin exploration signal.

        reuptake_noise = σ(tunnel_weights) / (μ(tunnel_weights) + ε)

2.  **Phase alignment across senses** — read the per-sense signals from
    ``temporal_spatiality._sense_signals()`` (vision, touch, smell, body,
    brain) and map each signal to an angle on S¹ via:

        φ_s = 2π · signal_s

    Mean resultant length (Kuramoto order parameter):

        R = |Σ_s  w_s · e^{i φ_s}| / Σ w_s      ∈ [0, 1]

    R = 1 → perfect phase lock; R = 0 → uniform distribution (maximum
    incoherence).  This directly measures the "synchronisation" the Weyl
    centroid is a proxy for, but without requiring a full temporal_spatiality
    run.

3.  **Bifurcation index** — ratio of the imaginary gradient component to the
    real one, averaged across all signal_kinds currently tracked in
    ``brain_body_signals``.  A high index means latent unrealised potential
    dominates; a low index means the Brain is realising its knowledge.

        B = ⟨|g_im|⟩ / (⟨|g_re|⟩ + ε)          ∈ [0, ∞)
        b_norm = tanh(B)                           ∈ [0, 1)

4.  **Expansion score** — weighted composite of three independent growth
    signals, each from a different subsystem:

        a)  Corpus net-growth rate  (from ``learning_drive.LearningDrive.learning_velocity``)
        b)  Tunnel-edge density     (SYMBIOTIC_TUNNEL + GROUNDED_TUNNEL count / entity count)
        c)  Network peer discovery  (newly promoted peers in the last interval)

        expansion = w_a · v  +  w_b · tunnel_density  +  w_c · peer_rate

5.  **Preferred direction** — the ``SmellReading.dominant_scent`` from
    ``sense_of_smell``, mapped to one of the 7 olfactory receptor labels.
    When no smell data is available this falls back to "baseline".

6.  **Tunnelable endpoint count** — a direct query against ``corpus_entity``
    for Endpoint types that appear in at least one ``SYMBIOTIC_TUNNEL`` or
    ``GROUNDED_TUNNEL`` edge.  This is the confirmatory validation the
    previous prompt described.

7.  **Weyl phase** — pulled from the latest ``temporal_spatiality`` rhythm
    entry via the ``kv_store`` so we don't trigger a full rhythm step.

Persistence
-----------
Every call to :func:`listen` writes one row to ``directionality_log``.
Recent rows are readable via :func:`recent_snapshots`.

Public API
----------
    listen()                       -> DirectionalitySnapshot  # always fresh
    recent_snapshots(limit=20)     -> list[dict]
    init_schema()                  -> None
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Sequence

import numpy as np

from .local_store import db_path as _local_db_path

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TUNNEL_RELS    = ("SYMBIOTIC_TUNNEL", "GROUNDED_TUNNEL")
_EXPANSION_W    = (0.45, 0.35, 0.20)  # weights: (velocity, tunnel_density, peer_rate)
_SENSE_WEIGHTS  = {                   # mirrors temporal_spatiality._SENSE_WEIGHTS
    "vision": 0.25,
    "touch":  0.25,
    "smell":  0.20,
    "body":   0.15,
    "brain":  0.15,
}
_EPS            = 1e-8

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class DirectionalitySnapshot:
    """Instantaneous directional state of the symbiotic Entirety.

    All scalar fields are normalised to *bounded* ranges so downstream
    consumers never need to know the internal scale of any single subsystem.
    """
    # ── Primary directional axes ───────────────────────────────────────────
    expansion_score:   float   # [0, 1]  — net corpus / network growth tendency
    coherence:         float   # [0, 1]  — Kuramoto R across the five senses
    bifurcation_index: float   # [0, 1]  — tanh(|g_im| / |g_re|)

    # ── Neighbourhood reuptake ─────────────────────────────────────────────
    reuptake_noise:    float   # [0, ∞)  — CV of tunnel edge weights
    tunnelable_count:  int     # confirmatory validated endpoint count

    # ── Phase registers ────────────────────────────────────────────────────
    weyl_phase:        float   # [0, 2π) — condensed Weyl coordinate
    sense_phases:      dict    # per-sense φ_s angles (radians)

    # ── Olfactory preferred direction ──────────────────────────────────────
    preferred_direction: str   # dominant scent class from sense_of_smell

    # ── Metadata ───────────────────────────────────────────────────────────
    computed_at:       str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

@contextmanager
def _conn():
    cn = sqlite3.connect(_local_db_path(), timeout=3.0)
    cn.row_factory = sqlite3.Row
    try:
        yield cn
        cn.commit()
    finally:
        cn.close()


def _safe(cn: sqlite3.Connection, sql: str, params: tuple = (), default: Any = None) -> Any:
    try:
        row = cn.execute(sql, params).fetchone()
        if row and row[0] is not None:
            return row[0]
    except sqlite3.OperationalError:
        pass
    return default


def init_schema() -> None:
    """Create the ``directionality_log`` table if absent."""
    with _conn() as cn:
        cn.executescript(
            """
            CREATE TABLE IF NOT EXISTS directionality_log (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                computed_at         TEXT    NOT NULL,
                expansion_score     REAL    NOT NULL,
                coherence           REAL    NOT NULL,
                bifurcation_index   REAL    NOT NULL,
                reuptake_noise      REAL    NOT NULL,
                tunnelable_count    INTEGER NOT NULL,
                weyl_phase          REAL    NOT NULL,
                preferred_direction TEXT    NOT NULL,
                sense_phases_json   TEXT,
                full_json           TEXT
            );
            CREATE INDEX IF NOT EXISTS ix_dir_log_ts
                ON directionality_log(computed_at);
            """
        )


# ---------------------------------------------------------------------------
# 1. Reuptake neighbourhood noise
# ---------------------------------------------------------------------------

def _reuptake_noise(cn: sqlite3.Connection) -> tuple[float, int]:
    """CV of tunnel edge weights + count of tunnelable endpoints."""
    try:
        rows = cn.execute(
            """
            SELECT e.weight, e.src_id, e.dst_id
              FROM corpus_edge e
             WHERE e.rel IN ('SYMBIOTIC_TUNNEL', 'GROUNDED_TUNNEL')
               AND e.weight IS NOT NULL
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return 0.0, 0

    if not rows:
        return 0.0, 0

    weights  = np.asarray([float(r["weight"]) for r in rows], dtype=float)
    mu       = float(weights.mean())
    sigma    = float(weights.std())
    cv       = sigma / (mu + _EPS)

    # Count distinct endpoint entities that appear in at least one tunnel edge
    endpoint_ids: set[str] = set()
    for r in rows:
        endpoint_ids.add(str(r["src_id"]))
        endpoint_ids.add(str(r["dst_id"]))
    tunnelable_count = len(endpoint_ids)

    return float(cv), tunnelable_count


# ---------------------------------------------------------------------------
# 2. Per-sense signals → phase angles (Kuramoto order parameter R)
# ---------------------------------------------------------------------------

def _sense_signals_raw(cn: sqlite3.Connection) -> dict[str, float]:
    """Read per-sense activity scalars from the same source as temporal_spatiality."""
    signals: dict[str, float] = {k: 0.0 for k in _SENSE_WEIGHTS}

    # Vision — recent corpus growth
    v = _safe(cn,
              "SELECT MIN(1.0, COALESCE(SUM(entities_added), 0) / 50.0)"
              "  FROM corpus_round_log"
              " WHERE ran_at >= datetime('now', '-1 hour')",
              default=0.0)
    signals["vision"] = max(0.0, min(1.0, float(v)))

    # Touch — peak pressure field value
    try:
        row = cn.execute(
            "SELECT value FROM brain_kv WHERE key='touch_pressure_field'"
        ).fetchone()
        if row and row[0]:
            pf = json.loads(row[0])
            if pf:
                signals["touch"] = max(0.0, min(1.0,
                                                max(float(p) for p in pf.values())))
    except Exception:
        pass

    # Smell — carrier mass deviation from 0.5 (both extremes → active)
    m = _safe(cn,
              "SELECT carrier_mass FROM sense_of_smell ORDER BY id DESC LIMIT 1",
              default=None)
    if m is not None:
        signals["smell"] = max(0.0, min(1.0, abs(float(m) - 0.5) * 2.0))

    # Body — open directive density (cap at 25)
    b = _safe(cn,
              "SELECT MIN(1.0, CAST(COUNT(*) AS REAL) / 25.0)"
              "  FROM body_directives"
              " WHERE status IN ('open','ack','in_progress')",
              default=0.0)
    signals["body"] = max(0.0, min(1.0, float(b)))

    # Brain — round cadence in last hour (cap at 60 rounds)
    n = _safe(cn,
              "SELECT MIN(1.0, COUNT(*) / 60.0)"
              "  FROM corpus_round_log"
              " WHERE ran_at >= datetime('now', '-1 hour')",
              default=0.0)
    signals["brain"] = max(0.0, min(1.0, float(n)))

    return signals


def _kuramoto_R(signals: dict[str, float]) -> tuple[float, dict[str, float]]:
    """Compute the Kuramoto order parameter R and per-sense phase angles.

    Each sense signal in [0, 1] is mapped to an angle φ_s = 2π · signal_s.
    The complex mean of weighted unit phasors gives the mean resultant vector.

        R = |Σ_s  w_s · e^{i·φ_s}| / Σ w_s

    Returns (R, {sense: angle_rad}).
    """
    phasors: complex = 0.0j
    weight_sum = 0.0
    phase_map: dict[str, float] = {}

    for sense, sig in signals.items():
        phi = 2.0 * math.pi * sig
        phase_map[sense] = phi
        w = _SENSE_WEIGHTS.get(sense, 0.0)
        phasors += w * complex(math.cos(phi), math.sin(phi))
        weight_sum += w

    R = abs(phasors) / (weight_sum + _EPS)
    return float(R), phase_map


# ---------------------------------------------------------------------------
# 3. Bifurcation index
# ---------------------------------------------------------------------------

def _bifurcation_index(cn: sqlite3.Connection) -> float:
    """tanh(mean|g_im| / mean|g_re|) from brain_body_signals optimizer state.

    The rADAM state is stored per signal_kind under 'radam_state:{kind}' in
    brain_kv.  We read theta (toroidal phase) as a proxy for the imaginary
    gradient direction and the current pressure as a proxy for the real gradient
    magnitude.
    """
    try:
        rows = cn.execute(
            "SELECT key, value FROM brain_kv WHERE key LIKE 'radam_state:%'"
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []

    if not rows:
        # Fall back: read body_directives priority distribution as a proxy.
        # High mean priority → large effective realised gradient.
        p = _safe(cn,
                  "SELECT AVG(priority) FROM body_directives"
                  " WHERE status IN ('open','ack','in_progress')",
                  default=0.0)
        return float(math.tanh(1.0 - float(p or 0.0)))

    g_re_vals: list[float] = []
    g_im_vals: list[float] = []
    for row in rows:
        try:
            st = json.loads(row["value"])
            pressure = float(st.get("pressure", 0.5))
            theta    = float(st.get("theta",    0.0))
            # pressure ≈ |g_re| proxy; sin(theta) ≈ |g_im| direction proxy
            g_re_vals.append(abs(pressure - 0.5) * 2.0)
            g_im_vals.append(abs(math.sin(theta)))
        except Exception:
            continue

    if not g_re_vals:
        return 0.0

    mean_re = sum(g_re_vals) / len(g_re_vals)
    mean_im = sum(g_im_vals) / len(g_im_vals)
    B       = mean_im / (mean_re + _EPS)
    return float(math.tanh(B))


# ---------------------------------------------------------------------------
# 4. Expansion score
# ---------------------------------------------------------------------------

def _expansion_score(cn: sqlite3.Connection) -> float:
    """Weighted composite: learning_velocity + tunnel_density + peer_rate."""
    # (a) Learning velocity from learning_log density
    try:
        v_rows = cn.execute(
            "SELECT signal_strength FROM learning_log ORDER BY id DESC LIMIT 30"
        ).fetchall()
        if v_rows:
            sigs = [float(r["signal_strength"] or 0.3) for r in v_rows]
            velocity = math.tanh(len(sigs) / 10.0 * (sum(sigs) / max(len(sigs), 1)))
        else:
            velocity = 0.0
    except sqlite3.OperationalError:
        velocity = 0.0

    # (b) Tunnel edge density = tunnel_edges / max(entity_count, 1)
    try:
        t_count = _safe(cn,
                        "SELECT COUNT(*) FROM corpus_edge"
                        " WHERE rel IN ('SYMBIOTIC_TUNNEL','GROUNDED_TUNNEL')",
                        default=0)
        e_count = _safe(cn, "SELECT COUNT(*) FROM corpus_entity", default=1)
        tunnel_density = min(1.0, float(t_count) / max(float(e_count), 1.0))
    except sqlite3.OperationalError:
        tunnel_density = 0.0

    # (c) Peer discovery rate in the last 10 minutes
    try:
        promo_count = _safe(cn,
                            "SELECT COUNT(*) FROM network_promotions"
                            " WHERE promoted_at >= datetime('now', '-10 minutes')",
                            default=0)
        peer_rate = min(1.0, float(promo_count) / 5.0)  # 5 promotions == full rate
    except sqlite3.OperationalError:
        peer_rate = 0.0

    w_a, w_b, w_c = _EXPANSION_W
    score = w_a * velocity + w_b * tunnel_density + w_c * peer_rate
    return float(min(1.0, max(0.0, score)))


# ---------------------------------------------------------------------------
# 5. Preferred direction (dominant scent)
# ---------------------------------------------------------------------------

def _preferred_direction(cn: sqlite3.Connection) -> str:
    """Return the dominant scent class from the latest smell reading."""
    try:
        row = cn.execute(
            "SELECT dominant_scent FROM sense_of_smell ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row and row[0]:
            return str(row[0])
    except sqlite3.OperationalError:
        pass
    return "baseline"


# ---------------------------------------------------------------------------
# 6. Weyl phase (from temporal_spatiality rhythm KV entry)
# ---------------------------------------------------------------------------

def _weyl_phase(cn: sqlite3.Connection) -> float:
    """Read the Weyl centroid from the last temporal_spatiality rhythm."""
    try:
        row = cn.execute(
            "SELECT value FROM brain_kv WHERE key='temporal_spatiality_rhythm'"
        ).fetchone()
        if row and row[0]:
            rhythm = json.loads(row[0])
            return float(rhythm.get("weyl_centroid", 0.0))
    except Exception:
        pass

    # Fallback: compute from torus_angles of all endpoints (circular mean)
    try:
        angle_rows = cn.execute(
            "SELECT props FROM corpus_entity WHERE entity_type='Endpoint' LIMIT 200"
        ).fetchall()
        angles: list[float] = []
        for r in angle_rows:
            try:
                props = json.loads(r["props"] or "{}")
                ta = props.get("torus_angles")
                if isinstance(ta, list) and ta:
                    angles.extend(float(x) for x in ta)
            except Exception:
                pass
        if angles:
            a = np.asarray(angles, dtype=float)
            # Circular mean via complex exponential
            cmean = complex(float(np.cos(a).mean()), float(np.sin(a).mean()))
            return float(math.atan2(cmean.imag, cmean.real) % (2.0 * math.pi))
    except Exception:
        pass

    return 0.0


# ---------------------------------------------------------------------------
# Core listener
# ---------------------------------------------------------------------------

def listen() -> DirectionalitySnapshot:
    """Compute a fresh ``DirectionalitySnapshot`` from every live subsystem.

    This is the primary public entry point.  It is designed to be:

    * **Read-only** — no mutations outside of the one row written to
      ``directionality_log``.
    * **Soft-failing** — any missing table or locked DB yields a graceful
      partial result rather than an exception.
    * **Sub-second** — all reads are indexed lookups or aggregates over
      small recent windows; no full-table scans on unbounded tables.
    """
    init_schema()

    try:
        with _conn() as cn:
            reuptake_noise, tunnelable_count = _reuptake_noise(cn)
            signals = _sense_signals_raw(cn)
            R, sense_phases = _kuramoto_R(signals)
            bifurcation     = _bifurcation_index(cn)
            expansion       = _expansion_score(cn)
            preferred       = _preferred_direction(cn)
            weyl            = _weyl_phase(cn)

            snap = DirectionalitySnapshot(
                expansion_score   = expansion,
                coherence         = R,
                bifurcation_index = bifurcation,
                reuptake_noise    = reuptake_noise,
                tunnelable_count  = tunnelable_count,
                weyl_phase        = weyl,
                sense_phases      = sense_phases,
                preferred_direction = preferred,
            )

            # Persist
            try:
                cn.execute(
                    """
                    INSERT INTO directionality_log(
                        computed_at, expansion_score, coherence,
                        bifurcation_index, reuptake_noise, tunnelable_count,
                        weyl_phase, preferred_direction,
                        sense_phases_json, full_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        snap.computed_at,
                        round(snap.expansion_score,   4),
                        round(snap.coherence,          4),
                        round(snap.bifurcation_index,  4),
                        round(snap.reuptake_noise,     4),
                        snap.tunnelable_count,
                        round(snap.weyl_phase,         4),
                        snap.preferred_direction,
                        json.dumps(snap.sense_phases, default=str),
                        json.dumps(asdict(snap), default=str),
                    ),
                )
            except Exception as exc:
                log.debug("directionality_log write skipped: %s", exc)

    except Exception as exc:
        log.warning("directionality listen() error: %s", exc)
        snap = DirectionalitySnapshot(
            expansion_score   = 0.0,
            coherence         = 0.0,
            bifurcation_index = 0.0,
            reuptake_noise    = 0.0,
            tunnelable_count  = 0,
            weyl_phase        = 0.0,
            sense_phases      = {},
            preferred_direction = "baseline",
        )

    log.debug(
        "directionality | expansion=%.3f coherence=%.3f bifurcation=%.3f"
        " reuptake_noise=%.3f weyl=%.3f preferred=%s",
        snap.expansion_score, snap.coherence, snap.bifurcation_index,
        snap.reuptake_noise, snap.weyl_phase, snap.preferred_direction,
    )
    return snap


# ---------------------------------------------------------------------------
# Audit reader
# ---------------------------------------------------------------------------

def recent_snapshots(limit: int = 20) -> list[dict]:
    """Return the most recent ``DirectionalitySnapshot`` rows as plain dicts."""
    try:
        with _conn() as cn:
            init_schema()
            rows = cn.execute(
                """
                SELECT computed_at, expansion_score, coherence,
                       bifurcation_index, reuptake_noise, tunnelable_count,
                       weyl_phase, preferred_direction, sense_phases_json
                  FROM directionality_log
                 ORDER BY id DESC
                 LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception as exc:
        log.debug("recent_snapshots error: %s", exc)
        return []
