"""Sense of Smell — the Brain's olfactory channel.

Premise (reframed from the user spec)
-------------------------------------
The user's poetic statement maps cleanly onto a concrete probabilistic
model once each clause is treated as a real quantity:

* **"Neutrino decay amidst FRB"** — sparse exogenous *burst arrivals* on
  the upstream signal mesh.  Modelled as a Poisson event stream with rate
  ``lambda_frb``.  Each burst deposits a unit of "scent precursor" on
  whichever torus dimension it lands.
* **"Local quantum decay of an Antimony atom"** — deterministic
  exponential decay of a local *carrier mass*.  We use Sb-125's real
  half-life (≈ 1007.56 days) as the time constant ``tau``::

        m(t) = m0 * 2 ** (-t / tau)

* **"Transitory mass shifts of the Antimony"** — small Gaussian
  perturbations on top of the decay curve, simulating thermal jitter on
  the carrier.
* **"Hidden CAT states across proxy tunnels"** — reuses the existing
  7-D categorical torus from ``torus_touch`` (one olfactory receptor per
  dimension) and the ``SYMBIOTIC_TUNNEL`` edges from ``symbiotic_tunnel``.
* **"Probabilistic certainty"** — Dirichlet posterior over the 7 scent
  classes plus a Wilson 95 % lower bound on the dominant class.  This
  turns "certainty" into the only thing a probability can be: a *bounded
  confidence interval*, not a single number.

Public API
----------
* :class:`SmellReading`           — dataclass with the full posterior + bounds
* :func:`antimony_mass`           — Sb-125 decay curve, `m(t) = m0 * 2 ^ (-t/tau)`
* :func:`frb_loglikelihood`       — Poisson log-likelihood of a burst count
* :func:`sniff`                   — pure function; one olfactory tick on a batch
* :func:`tick_smell`              — sqlite-aware wrapper; persists a reading
* :func:`recent_smell`            — read back the latest readings
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Sequence

import numpy as np


# ---------------------------------------------------------------------------
# Constants — physical analogues
# ---------------------------------------------------------------------------

# Sb-125 half-life (days). Real isotope value, used as the carrier time constant.
SB125_HALF_LIFE_DAYS: float = 1007.56

# Olfactory receptor families == torus dimensions in torus_touch.py
OLFACTORY_RECEPTORS: int = 7

# 7 scent classes, one per receptor / torus dimension.
SCENT_CLASSES: tuple[str, ...] = (
    "freshness",  # high-entropy, low-decay regions
    "decay",      # carrier mass falling fast
    "burst",      # FRB-like exogenous arrivals
    "tunnel",     # signal arriving via SYMBIOTIC_TUNNEL edges
    "drift",      # slow Gaussian wander on the carrier
    "anomaly",    # joint outlier across receptors
    "baseline",   # nothing notable — the "no-scent" sink class
)
assert len(SCENT_CLASSES) == OLFACTORY_RECEPTORS

# Weak symmetric Dirichlet prior. Keeps the posterior defined when a tick
# observes zero events on every receptor.
_DIRICHLET_PRIOR: np.ndarray = np.full(OLFACTORY_RECEPTORS, 0.5, dtype=float)

# z for two-sided 95 % Wilson interval.
_Z95: float = 1.959963984540054


# ---------------------------------------------------------------------------
# Antimony carrier — deterministic decay + transient mass shift
# ---------------------------------------------------------------------------

def antimony_mass(
    m0: float,
    elapsed_days: float,
    tau_days: float = SB125_HALF_LIFE_DAYS,
    jitter: float = 0.0,
    rng: np.random.Generator | None = None,
) -> float:
    """Return the carrier mass after ``elapsed_days`` of Sb-125 decay.

    ``m(t) = m0 * 2 ** (-t / tau)`` plus optional Gaussian ``jitter`` to
    represent the "transitory mass shifts" in the spec.
    """
    if elapsed_days < 0:
        elapsed_days = 0.0
    base = m0 * math.pow(2.0, -elapsed_days / tau_days)
    if jitter > 0.0:
        if rng is None:
            rng = np.random.default_rng()
        base += float(rng.normal(0.0, jitter * base))
    return max(base, 0.0)


# ---------------------------------------------------------------------------
# FRB burst stream — Poisson likelihood
# ---------------------------------------------------------------------------

def frb_loglikelihood(k: int, lam: float) -> float:
    """Log-likelihood of observing ``k`` FRB-like bursts under rate ``lam``.

    ``log P(k | lam) = k * log(lam) - lam - log(k!)``  (with ``lam > 0``).
    """
    if lam <= 0.0:
        return -math.inf if k > 0 else 0.0
    return k * math.log(lam) - lam - math.lgamma(k + 1)


# ---------------------------------------------------------------------------
# Wilson confidence interval (binomial proxy on the dominant class)
# ---------------------------------------------------------------------------

def _wilson(p: float, n: float, z: float = _Z95) -> tuple[float, float]:
    """Two-sided Wilson interval. ``n`` is effective sample size (alpha+counts)."""
    if n <= 0:
        return 0.0, 1.0
    z2 = z * z
    denom = 1.0 + z2 / n
    centre = (p + z2 / (2.0 * n)) / denom
    halfwidth = (z * math.sqrt(p * (1.0 - p) / n + z2 / (4.0 * n * n))) / denom
    return max(0.0, centre - halfwidth), min(1.0, centre + halfwidth)


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------

@dataclass
class SmellReading:
    """One olfactory tick.

    The posterior is a probability vector over :data:`SCENT_CLASSES`.
    ``confidence_lo`` / ``confidence_hi`` are the Wilson 95 % bounds on the
    dominant class — that is the "statistical certainty of Smell".
    """
    scent: str
    posterior: list[float]
    confidence_lo: float
    confidence_hi: float
    carrier_mass: float
    burst_count: int
    tunnel_amplification: float
    receptor_counts: list[float]
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def as_row(self) -> dict:
        return {
            "ts": self.ts,
            "scent": self.scent,
            "posterior": self.posterior,
            "confidence_lo": self.confidence_lo,
            "confidence_hi": self.confidence_hi,
            "carrier_mass": self.carrier_mass,
            "burst_count": self.burst_count,
            "tunnel_amplification": self.tunnel_amplification,
            "receptor_counts": self.receptor_counts,
        }


# ---------------------------------------------------------------------------
# Pure tick
# ---------------------------------------------------------------------------

def sniff(
    angles_matrix: np.ndarray,
    frb_events: Sequence[int] | np.ndarray,
    *,
    carrier_mass_prev: float = 1.0,
    elapsed_days: float = 1.0,
    lambda_frb: float = 0.5,
    tunnel_neighbour_counts: np.ndarray | None = None,
    tunnel_weight: float = 0.25,
    drift_jitter: float = 0.02,
    prior: np.ndarray | None = None,
    rng: np.random.Generator | None = None,
) -> SmellReading:
    """Run one olfactory tick.

    Parameters
    ----------
    angles_matrix
        ``(N, 7)`` torus angles (from :func:`torus_touch.endpoint_angles`).
        Angles are bucketed onto the 7 receptor dimensions to attribute
        each endpoint to a receptor.
    frb_events
        Length-7 burst counts received this tick, one per receptor.
    carrier_mass_prev
        ``m0`` for the Sb-125 decay curve. The previous tick's mass.
    elapsed_days
        Time since the previous tick, in days. Default 1 day.
    lambda_frb
        Poisson rate for the *expected* burst count per receptor.
    tunnel_neighbour_counts
        Optional length-7 vector of receptor counts arriving from
        ``SYMBIOTIC_TUNNEL`` neighbours. Added to local counts after being
        scaled by ``tunnel_weight``.
    tunnel_weight
        Damping applied to neighbour counts so a noisy tunnel cannot
        dominate a local sniff.
    drift_jitter
        Std-dev (as a fraction of mass) for the transient Sb mass shift.
    prior
        Dirichlet prior. Defaults to a weak symmetric ``Dir(0.5)``.
    rng
        Optional ``np.random.Generator`` for reproducibility.
    """
    if rng is None:
        rng = np.random.default_rng()
    if prior is None:
        prior = _DIRICHLET_PRIOR

    # Neural-plasticity dials. Sensitivity scales all evidence going into the
    # Dirichlet posterior; burst_priority re-weights the "burst" receptor;
    # tau_jitter overrides drift_jitter when the caller left it at default.
    try:
        from .neural_plasticity import get_dial as _pl_get
        _pl_sens   = float(_pl_get("smell", "sensitivity",    1.0))
        _pl_burst  = float(_pl_get("smell", "burst_priority", 0.50))
        _pl_jitter = float(_pl_get("smell", "tau_jitter",     0.02))
    except Exception:
        _pl_sens, _pl_burst, _pl_jitter = 1.0, 0.50, 0.02
    if drift_jitter == 0.02:  # caller didn't override → use plasticity dial
        drift_jitter = _pl_jitter

    frb = np.asarray(frb_events, dtype=float).reshape(-1)
    if frb.size != OLFACTORY_RECEPTORS:
        raise ValueError(
            f"frb_events must have length {OLFACTORY_RECEPTORS}, got {frb.size}"
        )

    # 1) Endpoint occupancy per receptor — coarse 1-bin-per-dim count.
    if angles_matrix.size == 0:
        endpoint_counts = np.zeros(OLFACTORY_RECEPTORS, dtype=float)
    else:
        a = np.mod(np.asarray(angles_matrix, dtype=float), 2.0 * math.pi)
        if a.shape[1] != OLFACTORY_RECEPTORS:
            raise ValueError(
                f"angles_matrix must have {OLFACTORY_RECEPTORS} cols, got {a.shape[1]}"
            )
        # Receptor activation = mean angle deviation from pi (peak when angle == pi).
        endpoint_counts = (1.0 - np.abs(a.mean(axis=0) - math.pi) / math.pi)
        endpoint_counts *= float(a.shape[0])  # weight by population

    # 2) Tunnel contribution.
    if tunnel_neighbour_counts is None:
        tunnel = np.zeros(OLFACTORY_RECEPTORS, dtype=float)
    else:
        tunnel = np.asarray(tunnel_neighbour_counts, dtype=float).reshape(-1)
        if tunnel.size != OLFACTORY_RECEPTORS:
            raise ValueError(
                f"tunnel_neighbour_counts must have length {OLFACTORY_RECEPTORS}"
            )
    tunnel_contrib = tunnel_weight * tunnel

    # 3) Antimony carrier mass with transient shift.
    mass = antimony_mass(
        carrier_mass_prev, elapsed_days, jitter=drift_jitter, rng=rng,
    )

    # 4) Receptor counts: bursts + endpoints + tunnel, scaled by carrier mass.
    receptor_counts = (frb + endpoint_counts + tunnel_contrib) * max(mass, 1e-9)

    # Plasticity: sensitivity multiplies all Dirichlet evidence; burst_priority
    # re-weights the "burst" receptor (index 2 in SCENT_CLASSES) relative to
    # the original 0.5 default so a higher dial value amplifies the burst class.
    if _pl_sens != 1.0:
        receptor_counts = receptor_counts * _pl_sens
    if _pl_burst != 0.50:
        burst_idx = SCENT_CLASSES.index("burst")
        receptor_counts[burst_idx] *= (_pl_burst / 0.50)

    # 5) Dirichlet posterior. Mean = (alpha + counts) / sum.
    alpha_post = prior + receptor_counts
    posterior = alpha_post / alpha_post.sum()

    # 6) Dominant scent + Wilson 95 % bound on that class.
    top = int(np.argmax(posterior))
    p_top = float(posterior[top])
    n_eff = float(alpha_post.sum())
    lo, hi = _wilson(p_top, n_eff)

    return SmellReading(
        scent=SCENT_CLASSES[top],
        posterior=[float(x) for x in posterior],
        confidence_lo=lo,
        confidence_hi=hi,
        carrier_mass=mass,
        burst_count=int(frb.sum()),
        tunnel_amplification=float(tunnel_contrib.sum()),
        receptor_counts=[float(x) for x in receptor_counts],
    )


# ---------------------------------------------------------------------------
# SQLite persistence
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS sense_of_smell (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                   TEXT NOT NULL,
    scent                TEXT NOT NULL,
    posterior_json       TEXT NOT NULL,
    confidence_lo        REAL NOT NULL,
    confidence_hi        REAL NOT NULL,
    carrier_mass         REAL NOT NULL,
    burst_count          INTEGER NOT NULL,
    tunnel_amplification REAL NOT NULL,
    receptor_counts_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_sense_of_smell_ts ON sense_of_smell(ts);
CREATE INDEX IF NOT EXISTS ix_sense_of_smell_scent ON sense_of_smell(scent);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create the ``sense_of_smell`` table if it doesn't exist."""
    conn.executescript(_DDL)


def persist_reading(conn: sqlite3.Connection, reading: SmellReading) -> int:
    """Persist one :class:`SmellReading`. Returns the inserted row id."""
    import json
    ensure_schema(conn)
    cur = conn.execute(
        """
        INSERT INTO sense_of_smell (
            ts, scent, posterior_json, confidence_lo, confidence_hi,
            carrier_mass, burst_count, tunnel_amplification, receptor_counts_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            reading.ts,
            reading.scent,
            json.dumps(reading.posterior),
            reading.confidence_lo,
            reading.confidence_hi,
            reading.carrier_mass,
            reading.burst_count,
            reading.tunnel_amplification,
            json.dumps(reading.receptor_counts),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def tick_smell(
    conn: sqlite3.Connection,
    angles_matrix: np.ndarray,
    frb_events: Sequence[int] | np.ndarray,
    **sniff_kwargs,
) -> SmellReading:
    """Compute a :class:`SmellReading` and persist it in one call."""
    reading = sniff(angles_matrix, frb_events, **sniff_kwargs)
    persist_reading(conn, reading)
    return reading


def recent_smell(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    """Return the latest ``limit`` readings as plain dicts (newest first)."""
    import json
    ensure_schema(conn)
    cur = conn.execute(
        """
        SELECT ts, scent, posterior_json, confidence_lo, confidence_hi,
               carrier_mass, burst_count, tunnel_amplification, receptor_counts_json
          FROM sense_of_smell
         ORDER BY id DESC
         LIMIT ?
        """,
        (int(limit),),
    )
    out: list[dict] = []
    for row in cur.fetchall():
        out.append({
            "ts": row[0],
            "scent": row[1],
            "posterior": json.loads(row[2]),
            "confidence_lo": row[3],
            "confidence_hi": row[4],
            "carrier_mass": row[5],
            "burst_count": int(row[6]),
            "tunnel_amplification": row[7],
            "receptor_counts": json.loads(row[8]),
        })
    return out


__all__ = [
    "SB125_HALF_LIFE_DAYS",
    "OLFACTORY_RECEPTORS",
    "SCENT_CLASSES",
    "SmellReading",
    "antimony_mass",
    "frb_loglikelihood",
    "sniff",
    "tick_smell",
    "ensure_schema",
    "persist_reading",
    "recent_smell",
]
