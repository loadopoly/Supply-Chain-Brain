"""Torus-Touch: continuous boundary pressure across a 7-D categorical manifold.

The Vision worker materialises ``Endpoint`` entities and the symbiotic
tunnel kernel mints discrete ``SYMBIOTIC_TUNNEL`` edges.  This module adds
the *continuous* leg of Touch the user requires:

    Touch must constantly push against the torus edge in order to
    expand informational gaps in multidimensional CAT states across
    n = 7 dimensions.

Mapping
-------
* **Manifold**            — :math:`T^7 = (S^1)^7`, one angular coordinate
  :math:`\\theta_i \\in [0, 2\\pi)` per dimension.
* **CAT states**          — Each dimension is discretised into ``B`` categorical
  bins; an endpoint's occupancy is the per-dim histogram of its angle.
* **Informational gap**   — Per-dim KL divergence from uniform; the manifold
  *gap field* :math:`G(\\theta)` is the sum across dimensions.  High gap = the
  CAT state is bunched, low gap = it has spread to the torus edge.
* **Torus edge**          — A torus has no Euclidean boundary, so "the edge"
  is the locus of maximal occupancy gradient — the wavefront where bunched
  mass meets empty cells.  Pushing against the edge means walking endpoints
  *up* the gap gradient until the manifold flattens.
* **Touch pressure**      — A constant outward force on every endpoint along
  :math:`\\nabla G(\\theta)`, wrapped mod :math:`2\\pi` so the topology stays a
  torus.  Run continuously by ``_touch_pressure_worker``.

Public API
----------
* :func:`tick_torus_pressure` — one pressure step over all Endpoint entities.
* :func:`endpoint_angles`     — read/synthesise the 7-D angle for an endpoint.
* :func:`gap_field_summary`   — current entropy / gap diagnostics.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

import numpy as np

# n = 7 dimensions, fixed by spec
TORUS_DIMS: int = 7
DEFAULT_BINS: int = 16


# ---------------------------------------------------------------------------
# Endpoint → 7-D angle
# ---------------------------------------------------------------------------

def _hash_to_unit(s: str, salt: int) -> float:
    h = hashlib.blake2b(f"{salt}:{s}".encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(h, "big") / float(1 << 64)


def _seed_angles(entity_id: str) -> np.ndarray:
    """Deterministic initial 7-D angle from the entity_id."""
    return np.array(
        [_hash_to_unit(entity_id, d) * 2.0 * math.pi for d in range(TORUS_DIMS)],
        dtype=float,
    )


def endpoint_angles(props: dict | None, entity_id: str) -> np.ndarray:
    """Read existing angles from props, falling back to a deterministic seed."""
    if props and isinstance(props.get("torus_angles"), list):
        a = np.asarray(props["torus_angles"], dtype=float)
        if a.size == TORUS_DIMS and np.all(np.isfinite(a)):
            return np.mod(a, 2.0 * math.pi)
    return _seed_angles(entity_id)


# ---------------------------------------------------------------------------
# CAT-state gap field
# ---------------------------------------------------------------------------

@dataclass
class CatGapField:
    """Per-dimension categorical occupancy of the torus.

    Maintains a ``(dims, bins)`` histogram normalised to a probability mass
    function per dim.  KL divergence vs uniform measures the informational
    gap — the quantity Touch is trying to *increase* by spreading mass into
    empty cells (i.e. push against the torus edge).
    """
    dims: int = TORUS_DIMS
    bins: int = DEFAULT_BINS
    smoothing: float = 1e-3   # Laplace prior so empty cells stay differentiable

    def histogram(self, angles_matrix: np.ndarray) -> np.ndarray:
        """``angles_matrix`` shape ``(N, dims)`` → ``(dims, bins)`` pmf."""
        h = np.full((self.dims, self.bins), self.smoothing, dtype=float)
        if angles_matrix.size == 0:
            return h / h.sum(axis=1, keepdims=True)
        idx = np.floor(
            (angles_matrix % (2.0 * math.pi)) / (2.0 * math.pi) * self.bins
        ).astype(int)
        idx = np.clip(idx, 0, self.bins - 1)
        for d in range(self.dims):
            counts = np.bincount(idx[:, d], minlength=self.bins).astype(float)
            h[d] += counts
        return h / h.sum(axis=1, keepdims=True)

    def kl_from_uniform(self, pmf: np.ndarray) -> np.ndarray:
        """Per-dim KL(p || uniform) — the "informational gap" we want OPEN."""
        u = 1.0 / self.bins
        return (pmf * np.log(pmf / u)).sum(axis=1)

    def gradient_at(self, angles: np.ndarray, pmf: np.ndarray) -> np.ndarray:
        """Outward force per dimension at ``angles``.

        We push *away* from over-populated bins (where pmf > uniform) toward
        the nearest under-populated cell — that is the wavefront pressing on
        the torus edge.  Implemented via finite difference of pmf around each
        endpoint's bin, sign-flipped so the step climbs the *gap* (entropy
        deficit) field rather than the density itself.
        """
        u = 1.0 / self.bins
        bin_w = 2.0 * math.pi / self.bins
        idx = np.floor((angles % (2.0 * math.pi)) / bin_w).astype(int)
        idx = np.clip(idx, 0, self.bins - 1)
        grad = np.zeros(self.dims, dtype=float)
        for d in range(self.dims):
            i = int(idx[d])
            left  = pmf[d, (i - 1) % self.bins]
            right = pmf[d, (i + 1) % self.bins]
            here  = pmf[d, i]
            # Push toward the lower-density neighbour, scaled by overflow
            overflow = max(here - u, 0.0)
            direction = -1.0 if left < right else 1.0
            grad[d] = direction * overflow
        return grad


# ---------------------------------------------------------------------------
# Pressure step
# ---------------------------------------------------------------------------

@dataclass
class TouchPressure:
    """Continuous outward force generator on the 7-D torus."""
    step: float = 0.12          # base radians per tick
    momentum: float = 0.7       # carry-over of last gradient (per endpoint)
    jitter: float = 0.01        # break degenerate symmetry on hashed seeds

    def apply(
        self,
        angles_matrix: np.ndarray,
        pmf: np.ndarray,
        field: CatGapField,
        prev_velocity: np.ndarray | None = None,
        rng: np.random.Generator | None = None,
        step_multipliers: np.ndarray | None = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Apply one pressure step.  *step_multipliers* (shape ``(N,)``) scales
        the radial step per-endpoint — grounded_tunneling writes amplification
        factors for nodes on active resistance paths to expand torsional bounds.
        """
        if rng is None:
            rng = np.random.default_rng()
        N = angles_matrix.shape[0]
        v = (
            prev_velocity
            if prev_velocity is not None and prev_velocity.shape == angles_matrix.shape
            else np.zeros_like(angles_matrix)
        )
        new_v = np.zeros_like(angles_matrix)
        new_a = np.zeros_like(angles_matrix)
        for n in range(N):
            g = field.gradient_at(angles_matrix[n], pmf)
            j = rng.normal(0.0, self.jitter, size=field.dims)
            scale = float(step_multipliers[n]) if step_multipliers is not None else 1.0
            nv = self.momentum * v[n] + self.step * scale * g + j
            new_v[n] = nv
            new_a[n] = (angles_matrix[n] + nv) % (2.0 * math.pi)
        return new_a, new_v


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------

def gap_field_summary(pmf: np.ndarray, field: CatGapField) -> dict:
    kl = field.kl_from_uniform(pmf)
    H_per_dim = -(pmf * np.log(pmf + 1e-12)).sum(axis=1)
    H_max = math.log(field.bins)
    return {
        "kl_total":    float(kl.sum()),
        "kl_per_dim":  [round(float(x), 4) for x in kl],
        "entropy_avg": float(H_per_dim.mean()),
        "entropy_max": float(H_max),
        "spread_pct":  round(100.0 * float(H_per_dim.mean() / H_max), 2),
    }


# ---------------------------------------------------------------------------
# Touch coupling on the manifold
# ---------------------------------------------------------------------------

def touch_couple_torus(angles_a: np.ndarray, angles_b: np.ndarray) -> float:
    """Manifold-aware Touch: shared dimensionality discounted by torus
    distance.

    Combines the existing exp/ln Touch identity with a wrap-aware angular
    proximity term.  Two endpoints sitting on opposite sides of the torus
    couple weakly even if their scalar weights are large.
    """
    delta = np.abs(angles_a - angles_b)
    delta = np.minimum(delta, 2.0 * math.pi - delta)        # wrap distance
    closeness = np.exp(-delta).mean()                       # in (0, 1]
    base = math.exp(math.log1p(closeness)) - 1.0            # exp(ln(1+x))-1
    return float(base * (1.0 + closeness))


# ---------------------------------------------------------------------------
# DB-driven tick
# ---------------------------------------------------------------------------

_VEL_KEY_PREFIX = "torus_vel:"   # per-endpoint velocity carryover (kv_store)


def _read_velocity(cn: sqlite3.Connection, eid: str) -> np.ndarray | None:
    try:
        row = cn.execute(
            "SELECT value FROM kv_store WHERE key=?",
            (f"{_VEL_KEY_PREFIX}{eid}",),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row or not row[0]:
        return None
    try:
        v = np.asarray(json.loads(row[0]), dtype=float)
        if v.size == TORUS_DIMS and np.all(np.isfinite(v)):
            return v
    except Exception:
        return None
    return None


def _write_velocity(cn: sqlite3.Connection, eid: str, v: np.ndarray) -> None:
    try:
        cn.execute(
            "INSERT INTO kv_store(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (f"{_VEL_KEY_PREFIX}{eid}", json.dumps(v.tolist())),
        )
    except sqlite3.OperationalError:
        pass


def _ensure_kv_store(cn: sqlite3.Connection) -> None:
    cn.execute(
        "CREATE TABLE IF NOT EXISTS kv_store("
        "key TEXT PRIMARY KEY, value TEXT)"
    )


def tick_torus_pressure(
    cn: sqlite3.Connection,
    *,
    bins: int = DEFAULT_BINS,
    step: float = 0.12,
) -> dict:
    """One continuous Touch tick: read endpoints, recompute pmf, push, write.

    Returns diagnostics consumed by the worker for KV logging.
    """
    _ensure_kv_store(cn)
    field = CatGapField(dims=TORUS_DIMS, bins=bins)
    rows = cn.execute(
        "SELECT entity_id, props_json FROM corpus_entity "
        "WHERE entity_type='Endpoint'"
    ).fetchall()
    if len(rows) < 2:
        return {"endpoints": len(rows), "moved": 0,
                "gap_before": 0.0, "gap_after": 0.0,
                "spread_before": 0.0, "spread_after": 0.0}

    eids: list[str] = []
    propslist: list[dict] = []
    for eid, props_json in rows:
        try:
            props = json.loads(props_json) if props_json else {}
        except Exception:
            props = {}
        eids.append(eid)
        propslist.append(props)

    angles = np.vstack(
        [endpoint_angles(propslist[i], eids[i]) for i in range(len(eids))]
    )
    velocity = np.vstack(
        [
            (_read_velocity(cn, e) if _read_velocity(cn, e) is not None
             else np.zeros(TORUS_DIMS))
            for e in eids
        ]
    )

    pmf_before = field.histogram(angles)
    diag_before = gap_field_summary(pmf_before, field)

    # Read per-endpoint torus amplification factors written by grounded_tunneling.
    # Nodes on active resistance paths receive AMPLIFY_FACTOR (default 1.8) so
    # their radial step is boosted, expanding the torsional manifold boundary.
    amplify = np.ones(len(eids), dtype=float)
    try:
        for i, eid in enumerate(eids):
            row = cn.execute(
                "SELECT value FROM kv_store WHERE key=?",
                (f"torus_amplify:{eid}",),
            ).fetchone()
            if row and row[0]:
                try:
                    amplify[i] = float(row[0])
                except (ValueError, TypeError):
                    pass
    except sqlite3.OperationalError:
        pass

    pressure = TouchPressure(step=step)
    new_angles, new_velocity = pressure.apply(
        angles, pmf_before, field, velocity, step_multipliers=amplify,
    )
    pmf_after = field.histogram(new_angles)
    diag_after = gap_field_summary(pmf_after, field)

    now_s = datetime.now().isoformat()
    moved = 0
    for i, eid in enumerate(eids):
        delta = float(np.linalg.norm((new_angles[i] - angles[i] + math.pi)
                                     % (2 * math.pi) - math.pi))
        if delta < 1e-4:
            continue
        propslist[i]["torus_angles"] = [round(float(x), 6) for x in new_angles[i]]
        propslist[i]["torus_gap"]    = round(diag_after["kl_total"], 4)
        propslist[i]["torus_tick"]   = now_s
        cn.execute(
            "UPDATE corpus_entity SET props_json=?, last_seen=? "
            "WHERE entity_id=? AND entity_type='Endpoint'",
            (json.dumps(propslist[i]), now_s, eid),
        )
        _write_velocity(cn, eid, new_velocity[i])
        moved += 1

    cn.commit()
    return {
        "endpoints":     len(eids),
        "moved":         moved,
        "gap_before":    diag_before["kl_total"],
        "gap_after":     diag_after["kl_total"],
        "spread_before": diag_before["spread_pct"],
        "spread_after":  diag_after["spread_pct"],
    }


__all__ = [
    "TORUS_DIMS",
    "CatGapField",
    "TouchPressure",
    "endpoint_angles",
    "gap_field_summary",
    "tick_torus_pressure",
    "touch_couple_torus",
]
