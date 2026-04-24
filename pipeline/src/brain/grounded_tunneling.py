"""Grounded Tunneling — certainty-anchored expansory pathway collapser.

Mechanism
---------
Statistical certainty of known edge weights provides the grounding anchor::

    certainty(v) = mean_e[ samples_e * weight_e ]   for edges e incident on v

High-certainty Endpoints become **Ground nodes** — the known substrate from
which expansory pathways push outward toward uncertain (low-certainty,
high torus-gap) territory along the manifold boundary.

Lifecycle of a grounded tunnel path A → B → C → D
--------------------------------------------------
1. **Resistance phase** (``RESISTANCE_DURATION`` seconds):

   - A resistance record is written to ``kv_store`` under
     ``resist_rec:{path[0]}::{path[-1]}``.  While the record is live the path
     resists weight relaxation — the expansory edges hold their weight above
     the local Bayesian centroid floor rather than being dragged back by the
     inverted-ReLU ADAM nudge.
   - Every node on the path receives a ``torus_amplify:{eid}`` boost factor
     in ``kv_store``.  ``tick_torus_pressure`` reads this and multiplies the
     radial step, expanding the torsional manifold boundary further for those
     nodes — this is "expanding the torsional boundaries".

2. **Nodal collapse** (when the resistance record expires):

   - The intermediate tunnel edges "succumb to the grounding weights" — their
     weights relax back toward the local Bayesian centroid on the next
     ADAM pass.
   - A new permanent edge is minted from the original Ground node directly to
     the path terminus::

         rel    = GROUNDED_TUNNEL
         weight = touch_couple_torus(θ_ground, θ_terminus)
                  * exp(−path_length / τ)
                  * clamp(certainty_ground / 10, 0, 1)

   - This is the "new edge with nodal collapse": the Brain's knowledge of
     having traversed the path distils into a single direct structural bond
     between the grounded anchor and the newly-reached frontier node.
   - ``torus_amplify`` keys for intermediate nodes are removed; those nodes
     return to normal manifold pressure.

Net effect: the Brain maintains stable certainty anchors while continuously
probing uncertain frontiers, and each completed probe cycle crystallises the
acquired knowledge into a new permanent ``GROUNDED_TUNNEL`` graph edge.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from datetime import datetime, timedelta

import numpy as np


# ── Constants ─────────────────────────────────────────────────────────────────

RESISTANCE_DURATION: int = 300           # seconds a path resists weight decay
CERTAINTY_THRESHOLD_PCTILE: float = 0.75 # top quartile becomes a Ground node
MAX_PATH_LEN: int = 5                    # BFS depth cap for expansory pathway
AMPLIFY_FACTOR: float = 1.8              # torus step boost for path nodes
COLLAPSE_DECAY_TAU: float = 3.0          # path-length decay τ in collapse weight

_GROUNDED_REL = "GROUNDED_TUNNEL"
_TUNNEL_REL   = "SYMBIOTIC_TUNNEL"
_MESH_RELS    = ("REACHABLE", "BRIDGES_TO", "SERVES", _TUNNEL_REL)

_RESIST_KEY_PREFIX  = "resist_rec:"
_AMPLIFY_KEY_PREFIX = "torus_amplify:"


# ── Statistical certainty ─────────────────────────────────────────────────────

def compute_endpoint_certainty(cn: sqlite3.Connection) -> dict[str, float]:
    """Return a certainty score per Endpoint entity_id.

    ::

        certainty(v) = mean_e[ samples_e * weight_e ]   for edges e incident on v

    Mirrors the Bayesian Poisson posterior precision: more observations of a
    consistently high-weight edge → tighter distribution → high certainty.
    Certainty is the "known statistical certainty" used as the grounding anchor.
    """
    rows = cn.execute(
        "SELECT src_id, dst_id, weight, samples "
        "FROM corpus_edge "
        "WHERE src_type='Endpoint' "
        f"  AND rel IN ({','.join(['?'] * len(_MESH_RELS))})",
        _MESH_RELS,
    ).fetchall()

    accum: dict[str, list[float]] = {}
    for src, dst, weight, samples in rows:
        cert = float(samples or 1) * float(weight or 0.0)
        for eid in (src, dst):
            accum.setdefault(eid, []).append(cert)

    return {eid: float(np.mean(vals)) for eid, vals in accum.items()}


# ── Adjacency graph ───────────────────────────────────────────────────────────

def _load_adjacency(
    cn: sqlite3.Connection,
) -> dict[str, list[tuple[str, float]]]:
    """Return undirected ``{endpoint_id: [(neighbour_id, weight), …]}`` map."""
    adj: dict[str, list[tuple[str, float]]] = {}
    rows = cn.execute(
        "SELECT src_id, dst_id, weight FROM corpus_edge "
        "WHERE src_type='Endpoint' "
        f"  AND rel IN ({','.join(['?'] * len(_MESH_RELS))})",
        _MESH_RELS,
    ).fetchall()
    for src, dst, w in rows:
        fw = float(w or 0.0)
        adj.setdefault(src, []).append((dst, fw))
        adj.setdefault(dst, []).append((src, fw))
    return adj


# ── Expansory pathway BFS ─────────────────────────────────────────────────────

def find_expansory_pathway(
    ground_id: str,
    certainty: dict[str, float],
    adj: dict[str, list[tuple[str, float]]],
    torus_gap: dict[str, float],
    *,
    max_len: int = MAX_PATH_LEN,
) -> list[str] | None:
    """BFS from *ground_id* toward the most uncertain reachable frontier.

    At each hop selects the unvisited neighbour that minimises::

        score = certainty[n] − torus_gap[n]

    (lowest certainty + highest torus KL gap = most informational / unexplored
    territory).  Stops early if a candidate node is at least as certain as the
    origin — it would not represent a genuine expansory frontier.

    Returns ``[ground_id, hop1, …, terminus]`` (length ≥ 2) or ``None`` if no
    valid expansory path exists.
    """
    ground_cert = certainty.get(ground_id, 0.0)
    visited = {ground_id}
    path = [ground_id]

    for _ in range(max_len - 1):
        node = path[-1]
        candidates = [
            (nid, w)
            for nid, w in adj.get(node, [])
            if nid not in visited and nid in certainty
        ]
        if not candidates:
            break
        # Pick the neighbour with the lowest (certainty − torus_gap) score
        best_nid = min(
            candidates,
            key=lambda x: certainty.get(x[0], 0.0) - torus_gap.get(x[0], 0.0),
        )[0]
        # Stop if the frontier node is at least as certain as the ground —
        # we want to expand *toward* uncertainty, not toward certainty.
        if certainty.get(best_nid, 0.0) >= ground_cert:
            break
        visited.add(best_nid)
        path.append(best_nid)

    return path if len(path) >= 2 else None


# ── Resistance records ────────────────────────────────────────────────────────

def _resist_key(src: str, dst: str) -> str:
    """Canonical kv_store key for a resistance record covering src→dst."""
    return f"{_RESIST_KEY_PREFIX}{src}::{dst}"


def _amplify_key(eid: str) -> str:
    """kv_store key for the torus amplification factor of *eid*."""
    return f"{_AMPLIFY_KEY_PREFIX}{eid}"


def _write_resistance(
    cn: sqlite3.Connection,
    path: list[str],
    ground_id: str,
    duration_s: int = RESISTANCE_DURATION,
) -> None:
    """Write one resistance record (path[0]→path[-1]) plus per-node amplify keys."""
    expires_at = (datetime.now() + timedelta(seconds=duration_s)).isoformat()
    record = json.dumps({
        "expires_at": expires_at,
        "ground_id":  ground_id,
        "path":       path,
    })
    cn.execute(
        "INSERT INTO kv_store(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (_resist_key(path[0], path[-1]), record),
    )
    for eid in path:
        cn.execute(
            "INSERT INTO kv_store(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (_amplify_key(eid), str(AMPLIFY_FACTOR)),
        )


def _load_expired_resistances(
    cn: sqlite3.Connection,
) -> list[tuple[str, str, dict]]:
    """Return ``(src, terminus, record_dict)`` for each expired resistance record."""
    now_iso = datetime.now().isoformat()
    rows = cn.execute(
        "SELECT key, value FROM kv_store WHERE key LIKE ?",
        (f"{_RESIST_KEY_PREFIX}%",),
    ).fetchall()
    expired: list[tuple[str, str, dict]] = []
    for _key, val in rows:
        try:
            rec = json.loads(val)
        except Exception:
            continue
        if rec.get("expires_at", "z") >= now_iso:
            continue   # still active
        path = rec.get("path", [])
        if len(path) >= 2:
            expired.append((path[0], path[-1], rec))
    return expired


# ── Nodal collapse ────────────────────────────────────────────────────────────

def nodal_collapse(
    cn: sqlite3.Connection,
    src: str,
    terminus: str,
    record: dict,
    certainty: dict[str, float],
    angle_lookup: dict[str, list[float]],
) -> bool:
    """Collapse an expired resistance path into a new ``GROUNDED_TUNNEL`` edge.

    The weight of the new edge encodes the distilled knowledge of the traversal::

        weight = touch_couple_torus(θ_ground, θ_terminus)
                 * exp(−path_length / τ)
                 * clamp(certainty_ground / 10, 0, 1)

    Cleans up the resistance record and ``torus_amplify`` keys for intermediate
    path nodes.  Returns ``True`` if the edge was inserted successfully.
    """
    try:
        from src.brain.torus_touch import touch_couple_torus  # type: ignore[import]
        _have_torus = True
    except ImportError:
        _have_torus = False

    path = record.get("path", [src, terminus])
    ground_id = record.get("ground_id", src)
    path_len = max(1, len(path) - 1)

    # Torus-proximity component
    if _have_torus and ground_id in angle_lookup and terminus in angle_lookup:
        torus_w = touch_couple_torus(
            np.asarray(angle_lookup[ground_id]),
            np.asarray(angle_lookup[terminus]),
        )
    else:
        torus_w = 0.3   # fallback when angles not yet written

    ground_cert = float(np.clip(certainty.get(ground_id, 0.1) / 10.0, 0.0, 1.0))
    weight = float(np.clip(
        torus_w * math.exp(-path_len / COLLAPSE_DECAY_TAU) * ground_cert,
        0.05, 1.0,
    ))

    now_s = datetime.now().isoformat()
    try:
        cn.execute(
            "INSERT INTO corpus_edge"
            "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
            "VALUES(?,?,?,?,?,?,?,1) "
            "ON CONFLICT(src_id,src_type,dst_id,dst_type,rel) "
            "DO UPDATE SET last_seen=excluded.last_seen, "
            "  samples=samples+1, "
            "  weight=max(weight, excluded.weight)",
            (ground_id, "Endpoint", terminus, "Endpoint",
             _GROUNDED_REL, weight, now_s),
        )
    except sqlite3.Error as exc:
        logging.warning("[grounded_tunneling] collapse insert error: %s", exc)
        return False

    # Remove the resistance record for this path
    cn.execute(
        "DELETE FROM kv_store WHERE key=?",
        (_resist_key(path[0], path[-1]),),
    )
    # Remove torus amplification for intermediate nodes (keep source + terminus)
    for eid in path[1:-1]:
        cn.execute("DELETE FROM kv_store WHERE key=?", (_amplify_key(eid),))

    logging.info(
        "[grounded_tunneling] nodal collapse %s→%s GROUNDED_TUNNEL "
        "w=%.3f path_len=%d",
        ground_id, terminus, weight, path_len,
    )
    return True


# ── Main tick ─────────────────────────────────────────────────────────────────

def ground_and_expand(
    cn: sqlite3.Connection,
    *,
    max_new_paths: int = 4,
    resistance_duration: int = RESISTANCE_DURATION,
) -> dict:
    """One grounded-tunneling tick: open expansory paths and collapse expired ones.

    Steps
    -----
    1. Compute certainty scores for all Endpoints from incident edge statistics.
    2. Identify Ground nodes (top ``CERTAINTY_THRESHOLD_PCTILE`` percentile).
    3. For each Ground node, BFS toward the uncertain frontier and write a
       resistance record + ``torus_amplify`` boost keys.
    4. Detect expired resistance records and trigger nodal collapse →
       new permanent ``GROUNDED_TUNNEL`` edge.

    Returns a stats dict for ``_vision_worker`` KV heartbeat logging.
    """
    _ensure_kv_store(cn)
    stats: dict = {
        "ground_nodes": 0,
        "paths_opened": 0,
        "collapses":    0,
    }

    # ── 1. Certainty scores ────────────────────────────────────────────────
    certainty = compute_endpoint_certainty(cn)
    if len(certainty) < 2:
        return stats

    cert_vals = np.array(list(certainty.values()), dtype=float)
    threshold = float(np.quantile(cert_vals, CERTAINTY_THRESHOLD_PCTILE))
    ground_nodes = [eid for eid, c in certainty.items() if c >= threshold]
    stats["ground_nodes"] = len(ground_nodes)

    # ── 2. Torus gap + angle lookup (for collapse weight) ─────────────────
    torus_gap: dict[str, float] = {}
    angle_lookup: dict[str, list[float]] = {}
    try:
        for eid, props_json in cn.execute(
            "SELECT entity_id, props_json FROM corpus_entity "
            "WHERE entity_type='Endpoint'"
        ):
            try:
                p = json.loads(props_json) if props_json else {}
            except Exception:
                p = {}
            raw_gap = p.get("torus_gap", 0.0)
            torus_gap[eid] = float(raw_gap) if isinstance(raw_gap, (int, float)) else 0.0
            ta = p.get("torus_angles")
            if isinstance(ta, list) and len(ta) == 7:
                angle_lookup[eid] = [float(x) for x in ta]
    except sqlite3.Error:
        pass

    # ── 3. Track existing resistance keys ──────────────────────────────────
    existing_keys: set[str] = set()
    try:
        for (k,) in cn.execute(
            "SELECT key FROM kv_store WHERE key LIKE ?",
            (f"{_RESIST_KEY_PREFIX}%",),
        ):
            existing_keys.add(k)
    except sqlite3.Error:
        pass

    # ── 4. Open new expansory paths ────────────────────────────────────────
    adj = _load_adjacency(cn)
    paths_opened = 0
    for gid in ground_nodes:
        if paths_opened >= max_new_paths:
            break
        path = find_expansory_pathway(gid, certainty, adj, torus_gap)
        if not path:
            continue
        rk = _resist_key(path[0], path[-1])
        if rk in existing_keys:
            continue   # already open for this ground→terminus pair
        _write_resistance(cn, path, gid, duration_s=resistance_duration)
        paths_opened += 1
        existing_keys.add(rk)
    stats["paths_opened"] = paths_opened

    # ── 5. Nodal collapse: process expired resistance records ──────────────
    expired = _load_expired_resistances(cn)
    collapses = 0
    for src, terminus, rec in expired:
        if nodal_collapse(cn, src, terminus, rec, certainty, angle_lookup):
            collapses += 1
    stats["collapses"] = collapses

    return stats


def _ensure_kv_store(cn: sqlite3.Connection) -> None:
    cn.execute(
        "CREATE TABLE IF NOT EXISTS kv_store("
        "key TEXT PRIMARY KEY, value TEXT)"
    )


__all__ = [
    "compute_endpoint_certainty",
    "find_expansory_pathway",
    "nodal_collapse",
    "ground_and_expand",
    "RESISTANCE_DURATION",
    "CERTAINTY_THRESHOLD_PCTILE",
    "AMPLIFY_FACTOR",
    "COLLAPSE_DECAY_TAU",
    "_GROUNDED_REL",
    "_RESIST_KEY_PREFIX",
    "_AMPLIFY_KEY_PREFIX",
]
