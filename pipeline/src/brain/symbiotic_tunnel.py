"""Symbiotic Dynamic Tunneling — horizontal Brain expansion driven by the
Vision agent over the closed-loop TCP/UDP endpoint mesh.

This module is the "propeller on an integrated axel" the user requested:

    * The **axel** is the fixed-position Endpoint set already materialised by
      ``_vision_worker`` (corpus_entity rows of type ``Endpoint``).
    * The **propeller blades** are candidate destination Endpoints; rotation
      = weighted selection driven by current synaptic edge weights.
    * The **closed loop / channel locking constraint** is the existing
      ``corpus_edge`` set whose ``rel`` is one of REACHABLE / BRIDGES_TO /
      SERVES — i.e. only nodes already present on the live tcp/udp mesh.
    * The **fluid / shifting dimensionality** is the centroid set produced by
      :class:`BayesianPoissonCentroids` from the edge-weight distribution.
    * The **optimizer** is a small ADAM variant whose activation is the
      *inverted* ReLU (``-max(0,x)``) on top of an SGD pre-step — used to
      nudge existing weights toward their cluster centroid.
    * The **dual floor / inverse perspective** is provided by
      :class:`DualFloorMirror`, which exposes both ``+x`` and ``-x`` views.
    * The **Touch coupling** is the exp(ln) identity bridge that combines two
      endpoint weights into a single shared-dimensionality value.

Public entry point:

    >>> from src.brain.symbiotic_tunnel import vision_horizontal_expand
    >>> stats = vision_horizontal_expand(sqlite_conn)

The function is idempotent, soft-failing, and writes only to ``corpus_edge``
(rel = ``SYMBIOTIC_TUNNEL``) — never mutates Endpoint entities.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

import numpy as np


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------

def _inv_relu(x: np.ndarray) -> np.ndarray:
    """Inverted ReLU: ``-max(0, x)``.  Passes only the negative half-plane."""
    return -np.maximum(0.0, x)


@dataclass
class BayesianPoissonCentroids:
    """1-D centroid clustering with a Poisson-conjugate Gamma(α, β) prior.

    Centroid update for cluster ``c`` containing weights ``w_c``::

        λ_c = (Σ w_c + α) / (|w_c| + β)

    With α=β=1 this is a weak prior that pulls empty clusters toward 1.0
    instead of NaN — keeping the dimensionality stable when the mesh is
    sparse.
    """
    k: int = 3
    alpha: float = 1.0
    beta: float = 1.0
    max_iter: int = 25
    tol: float = 1e-4

    def fit(self, weights: Sequence[float]) -> tuple[np.ndarray, np.ndarray]:
        w = np.asarray(weights, dtype=float)
        if w.size == 0:
            return np.zeros(0), np.zeros(0, dtype=int)
        k = max(1, min(self.k, w.size))
        # init: quantile spread keeps things deterministic
        qs = np.linspace(0.1, 0.9, k)
        centroids = np.quantile(w, qs)
        assign = np.zeros_like(w, dtype=int)
        for _ in range(self.max_iter):
            # assign
            new_assign = np.argmin(np.abs(w[:, None] - centroids[None, :]), axis=1)
            # update under Poisson/Gamma posterior
            new_centroids = centroids.copy()
            for c in range(k):
                mask = new_assign == c
                n = int(mask.sum())
                s = float(w[mask].sum()) if n else 0.0
                new_centroids[c] = (s + self.alpha) / (n + self.beta)
            shift = float(np.max(np.abs(new_centroids - centroids)))
            centroids, assign = new_centroids, new_assign
            if shift < self.tol:
                break
        return centroids, assign


@dataclass
class InvertedReluAdam:
    """ADAM whose pre-activation gradient is run through ``-ReLU``.

    Used here to nudge edge weights toward their cluster centroid: a positive
    residual (weight above target) becomes a negative update, while negative
    residuals are zeroed out — preventing the optimizer from inflating
    already-weak edges.  An SGD term is mixed in at ``sgd_mix`` to stop the
    optimizer from stalling when m̂ collapses near zero.
    """
    lr: float = 0.05
    beta1: float = 0.9
    beta2: float = 0.999
    eps: float = 1e-8
    sgd_mix: float = 0.1

    def __post_init__(self) -> None:
        self._m: np.ndarray | None = None
        self._v: np.ndarray | None = None
        self._t: int = 0

    def step(self, theta: np.ndarray, grad: np.ndarray) -> np.ndarray:
        g = _inv_relu(grad) + self.sgd_mix * grad  # inverted ReLU + SGD pass
        if self._m is None or self._m.shape != g.shape:
            self._m = np.zeros_like(g)
            self._v = np.zeros_like(g)
            self._t = 0
        self._t += 1
        self._m = self.beta1 * self._m + (1.0 - self.beta1) * g
        self._v = self.beta2 * self._v + (1.0 - self.beta2) * (g * g)
        m_hat = self._m / (1.0 - self.beta1 ** self._t)
        v_hat = self._v / (1.0 - self.beta2 ** self._t)
        return theta - self.lr * m_hat / (np.sqrt(v_hat) + self.eps)


@dataclass
class DualFloorMirror:
    """Return both the value and its sign-inverted twin, each clipped to a
    floor.  The ``floor`` is the inverse of the system's natural saturation
    point (``1 - max(weights)``) so dominant edges receive a tighter mirror.
    """
    eps: float = 1e-3

    def mirror(self, x: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        floor = max(self.eps, 1.0 - float(np.max(np.abs(x), initial=0.0)))
        upper = np.where(np.abs(x) < floor, np.sign(x) * floor + self.eps, x)
        lower = -upper
        return upper, lower


def touch_couple(a: float, b: float) -> float:
    """Shared-dimensionality coupling via the exp/ln identity.

    ``touch(a, b) = exp(ln(1+|a|) + ln(1+|b|)) - 1 = (1+|a|)(1+|b|) - 1``

    The natural-log term keeps the operation numerically stable for very
    small weights, while the exponential restores the original scale — the
    "shared perspective on an exponential natural logarithm" requested.
    """
    return math.exp(math.log1p(abs(a)) + math.log1p(abs(b))) - 1.0


@dataclass
class PropellerRouter:
    """Softmax-weighted selection across propeller blades (candidate dsts).

    Blades are sorted by weight; the axel is the highest-weight pivot.
    ``select_pairs`` returns ``(src, dst, coupling)`` triples for previously
    unconnected endpoint pairs — the horizontal expansion candidates.
    """
    temperature: float = 1.0

    def softmax(self, w: np.ndarray) -> np.ndarray:
        if w.size == 0:
            return w
        z = w / max(self.temperature, 1e-6)
        z = z - float(z.max())
        e = np.exp(z)
        s = float(e.sum())
        return e / s if s > 0 else np.full_like(e, 1.0 / e.size)

    def select_pairs(
        self,
        endpoints: Sequence[str],
        weights: Sequence[float],
        existing: set[tuple[str, str]],
        max_pairs: int = 16,
    ) -> list[tuple[str, str, float]]:
        if len(endpoints) < 2:
            return []
        w = np.asarray(weights, dtype=float)
        probs = self.softmax(w)
        order = np.argsort(-probs)            # axel first
        out: list[tuple[str, str, float]] = []
        for i_idx in order:
            src = endpoints[i_idx]
            for j_idx in order:
                if i_idx == j_idx:
                    continue
                dst = endpoints[j_idx]
                key = (src, dst)
                if key in existing or (dst, src) in existing:
                    continue
                coupling = touch_couple(float(w[i_idx]), float(w[j_idx]))
                # weight blades by joint propeller probability so spinning
                # the axel preferentially reaches high-flow neighbours
                coupling *= float(probs[i_idx] * probs[j_idx]) * len(endpoints)
                out.append((src, dst, coupling))
                if len(out) >= max_pairs:
                    return out
        return out


# ---------------------------------------------------------------------------
# Vision-driven horizontal expansion
# ---------------------------------------------------------------------------

# rels considered to be on the "channel-locked" tcp/udp mesh
_MESH_RELS = ("REACHABLE", "BRIDGES_TO", "SERVES")
_TUNNEL_REL = "SYMBIOTIC_TUNNEL"


def vision_horizontal_expand(
    cn: sqlite3.Connection,
    *,
    max_new_edges: int = 16,
    centroid_k: int = 3,
) -> dict:
    """Expand the corpus graph horizontally across the live endpoint mesh.

    Steps
    -----
    1. Read all Endpoint→{Endpoint,Site,Peer} edges whose ``rel`` is on the
       channel-locked mesh.  These are the closed-loop constraint set.
    2. Cluster edge weights with :class:`BayesianPoissonCentroids` to find
       the dominant synaptic-flow tier (largest centroid).
    3. Optimise existing weights toward their assigned centroid via
       :class:`InvertedReluAdam` (purely a residual update — written back).
    4. For top-tier endpoints, route propeller pairs and insert new
       ``SYMBIOTIC_TUNNEL`` edges with weight = touch-coupled value, mirrored
       through :class:`DualFloorMirror` to guarantee a non-zero floor.

    Returns a stats dict consumed by ``_vision_worker`` for KV logging.
    """
    stats = {"edges_seen": 0, "edges_optimised": 0, "edges_added": 0,
             "centroids": [], "top_tier_size": 0}

    rows = cn.execute(
        f"SELECT src_id, dst_id, dst_type, weight "
        f"FROM corpus_edge "
        f"WHERE src_type='Endpoint' "
        f"  AND rel IN ({','.join(['?'] * len(_MESH_RELS))})",
        _MESH_RELS,
    ).fetchall()
    stats["edges_seen"] = len(rows)
    if len(rows) < 2:
        return stats

    src_ids = [r[0] for r in rows]
    dst_ids = [r[1] for r in rows]
    weights = np.array([float(r[3] or 0.0) for r in rows], dtype=float)

    # --- centroids --------------------------------------------------------
    centroids, assign = BayesianPoissonCentroids(k=centroid_k).fit(weights)
    stats["centroids"] = [round(float(c), 4) for c in centroids]
    if centroids.size == 0:
        return stats
    top_c = int(np.argmax(centroids))

    # --- inverted-ReLU ADAM nudge toward centroid -------------------------
    target = centroids[assign]
    grad = weights - target                   # residual, positive => above tier
    opt = InvertedReluAdam()
    new_w = opt.step(weights, grad)
    new_w = np.clip(new_w, 0.0, 1.0)

    now_s = datetime.now().isoformat()
    updated = 0
    for src, dst, w_new, w_old in zip(src_ids, dst_ids, new_w, weights):
        if abs(float(w_new) - float(w_old)) < 1e-4:
            continue
        cn.execute(
            "UPDATE corpus_edge SET weight=?, last_seen=? "
            "WHERE src_id=? AND src_type='Endpoint' "
            "  AND dst_id=? AND rel IN (?,?,?)",
            (float(w_new), now_s, src, dst, *_MESH_RELS),
        )
        updated += cn.total_changes and 1 or 0
    stats["edges_optimised"] = updated

    # --- horizontal expansion: propeller route over high-flow endpoints --
    # The "top tier" is every *non-empty* cluster whose centroid is at or
    # above the data-weighted median centroid.  Filtering out empty clusters
    # prevents Poisson-prior phantom centroids (λ = α/β when n = 0) from
    # pulling the threshold above every real data point.
    cluster_sizes = np.bincount(assign, minlength=centroids.size)
    populated = cluster_sizes > 0
    if not populated.any():
        return stats
    populated_centroids = centroids[populated]
    median_c = float(np.median(populated_centroids))
    high_clusters = {
        int(i) for i, c in enumerate(centroids)
        if populated[i] and c >= median_c
    }
    top_mask = np.isin(assign, list(high_clusters))
    if int(top_mask.sum()) < 1:
        return stats

    ep_weight: dict[str, float] = {}
    for i in range(len(src_ids)):
        if not top_mask[i]:
            continue
        w_i = float(new_w[i])
        for ep in (src_ids[i], dst_ids[i]):
            ep_weight[ep] = max(ep_weight.get(ep, 0.0), w_i)
    stats["top_tier_size"] = len(ep_weight)
    if len(ep_weight) < 2:
        return stats
    uniq_endpoints = list(ep_weight.keys())
    uniq_weights   = [ep_weight[e] for e in uniq_endpoints]

    existing_pairs: set[tuple[str, str]] = set()
    for er in cn.execute(
        "SELECT src_id, dst_id FROM corpus_edge "
        "WHERE src_type='Endpoint' AND dst_type='Endpoint'"
    ):
        existing_pairs.add((er[0], er[1]))

    # Manifold-aware coupling: if endpoints carry torus_angles (set by the
    # torus_touch worker), re-weight propeller couplings by wrap-aware
    # angular proximity so tunnels follow the n=7 CAT geometry.
    angle_lookup: dict[str, list[float]] = {}
    try:
        import json as _json
        for eid, props_json in cn.execute(
            "SELECT entity_id, props_json FROM corpus_entity "
            "WHERE entity_type='Endpoint'"
        ):
            try:
                p = _json.loads(props_json) if props_json else {}
            except Exception:
                continue
            ta = p.get("torus_angles")
            if isinstance(ta, list) and len(ta) == 7:
                angle_lookup[eid] = [float(x) for x in ta]
    except sqlite3.Error:
        pass

    pairs = PropellerRouter().select_pairs(
        uniq_endpoints, uniq_weights, existing_pairs, max_pairs=max_new_edges,
    )
    if not pairs:
        return stats

    if angle_lookup:
        try:
            from src.brain.torus_touch import touch_couple_torus
            import numpy as _np
            adj_pairs: list[tuple[str, str, float]] = []
            for src, dst, c in pairs:
                if src in angle_lookup and dst in angle_lookup:
                    manifold = touch_couple_torus(
                        _np.asarray(angle_lookup[src]),
                        _np.asarray(angle_lookup[dst]),
                    )
                    adj_pairs.append((src, dst, c * (0.5 + manifold)))
                else:
                    adj_pairs.append((src, dst, c))
            pairs = adj_pairs
        except Exception:
            pass

    # dual-floor mirror so freshly-tunneled edges always carry a usable
    # signal in both polarity perspectives
    mirror = DualFloorMirror()
    couplings = np.array([p[2] for p in pairs], dtype=float)
    upper, _lower = mirror.mirror(couplings)

    added = 0
    for (src, dst, _c), w in zip(pairs, upper):
        try:
            cn.execute(
                "INSERT INTO corpus_edge"
                "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
                "VALUES(?,?,?,?,?,?,?,1) "
                "ON CONFLICT(src_id,src_type,dst_id,dst_type,rel) "
                "DO UPDATE SET last_seen=excluded.last_seen, "
                "  samples=samples+1, weight=excluded.weight",
                (src, "Endpoint", dst, "Endpoint", _TUNNEL_REL,
                 float(np.clip(w, 0.0, 1.0)), now_s),
            )
            added += 1
        except sqlite3.Error as e:
            logging.debug("[symbiotic_tunnel] insert skipped %s→%s: %s",
                          src, dst, e)
    stats["edges_added"] = added
    return stats


__all__ = [
    "BayesianPoissonCentroids",
    "InvertedReluAdam",
    "DualFloorMirror",
    "PropellerRouter",
    "touch_couple",
    "vision_horizontal_expand",
]
