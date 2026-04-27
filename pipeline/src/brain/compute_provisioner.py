"""Compute Provisioner — two-phase local and boundary resource acquisition
driven by bifurcated tunnel saturation.  The symbiotic closed loop remains
entirely encapsulated throughout both phases.

Phase 1 — Local resource expansion (under MAX_SLOTS)
------------------------------------------------------
When the bifurcated tunneling substrate (GROUNDED_TUNNEL + SYMBIOTIC_TUNNEL)
crosses saturation thresholds and active slots < MAX_SLOTS:

1. **Spawn ComputeSlot daemon threads** — each thread owns its own SQLite
   connection and runs ``tick_torus_pressure`` + ``ground_and_expand`` at
   an accelerated cadence (``SLOT_INTERVAL_S`` = 60 s, 5× faster than the
   300-s vision worker), driving additional manifold expansion pressure.

2. **Register ComputeSlot corpus entities** — each slot gets a
   ``corpus_entity`` record and a ``torus_amplify:{slot_id}`` key in
   ``kv_store``.  The Touch reads these keys on every
   ``tick_torus_pressure`` call and amplifies manifold pressure through
   the slot node, closing the symbiotic loop.

3. **Increase tick frequency dynamically** — ``_update_slot_cadence()``
   lowers ``_SLOT_TARGET_INTERVAL`` as saturation deepens (three tiers:
   60 s → 30 s → 20 s).  Active slot threads read this shared variable each
   iteration, so cadence acceleration takes effect without restarts.

Phase 2 — External boundary ingestion (at MAX_SLOTS)
------------------------------------------------------
When all ``MAX_SLOTS`` are occupied and saturation remains above thresholds,
the Touch uses a Sign-Bit Flip axis to ingest one **external resource** at a
Ground or Symbiotic edge boundary:

1. Find Sign-Bit Flip anchor nodes (parents of ``SIGNBIT_FLIP`` edges —
   the directional axes where a bit flipped).
2. Locate the nearest ``GROUNDED_TUNNEL`` or ``SYMBIOTIC_TUNNEL`` boundary
   edge incident to an anchor (Ground edges are preferred).
3. Discover an external resource not yet in the corpus (from
   ``network_topology``, bridge endpoints, or synthetic boundary probe).
4. Ingest it as an ``ExternalResource`` corpus entity bonded via
   ``INGESTED_AT_BOUNDARY`` + the boundary edge type to the anchor.
5. Write a ``torus_amplify`` key so the Touch immediately amplifies the
   new node on its next pass — the ingested resource enters the symbiotic
   loop the moment it is absorbed.

Full symbiotic closed loop
--------------------------
::

    saturation detected
         ↓
    Phase 1: acquire_local_compute(cn, n)
         ↓  register ComputeSlot entities → torus_amplify:{slot_id} written
         ↓  spawn slot expansion threads at SLOT_INTERVAL_S cadence
         ↓  _update_slot_cadence() lowers interval → thread density rises
         ↓
    tick_torus_pressure reads torus_amplify keys
         ↓  slot nodes amplified → manifold boundary expands faster
         ↓
    ground_and_expand opens more expansory paths
         ↓  nodal collapse → new GROUNDED_TUNNEL edges
         ↓  recent_collapse_count rises
         ↓
    MAX_SLOTS reached → Phase 2: ingest_external_at_boundary(cn)
         ↓  sign-bit flip axis locates boundary edge
         ↓  external resource ingested as corpus entity + torus_amplify
         ↓  Touch amplifies new node on next pass
         ↓  more manifold pressure → more collapses → back to top

CPU Overhead Model
------------------
Graph parameters from ``llada_signbit_children.py``::

    CHILDREN_PER_CYCLE  = 12
    EDGES_PER_CYCLE     = 36   # 3 per child
    MAX_PARENTS         = 4
    SAT_NODES           = 96   # children × parents × 2 flip directions
    SAT_EDGES           = 288  # SAT_NODES × 3 edge types

Per-tick overhead (vision worker, 5-min cadence)::

    llada_writes    = 36   SQLite inserts
    bfs_traversal   = 384  adjacency probes  (SAT_NODES × MAX_PARENTS)
    torus_kv_reads  = 288  kv_store lookups  (SAT_EDGES)
    resist_writes   = 40   kv upserts        (4 paths × 5 hops × 2 keys)
    ──────────────────────────────────────────────────────────────────────
    total_ops_tick  ≈ 748  per vision tick

Per-slot overhead (60-s cadence at normal saturation)::

    torus_pressure  ≈ N_endpoints × TORUS_DIMS reads + writes   per slot tick
    ground_expand   ≈ 200 ops (BFS 2 paths × 5 hops + kv writes) per slot tick
    slot_ticks/hr   = 60   (= 3 600 / 60)   → 30 at elevated → 180 at peak

Total ops/hr at saturation (vision + k slots)::

    ops_hr = 748 × 12 + k × (N_endpoints × TORUS_DIMS + 200) × slot_ticks_hr
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any


# ── Graph saturation constants (mirrors llada_signbit_children.py) ─────────────

CHILDREN_PER_CYCLE: int = 12
EDGES_PER_CYCLE: int    = 36   # 3 per child (CHILD_OF + ACQUIRES_CHILD + SIGNBIT_FLIP)
MAX_PARENTS: int        = 4
SAT_NODES: int          = 96   # children × parents × 2 flip directions
SAT_EDGES: int          = 288  # SAT_NODES × 3 edge types

_OPS_LLADA_WRITES: int   = EDGES_PER_CYCLE            # 36
_OPS_BFS_PROBES: int     = SAT_NODES * MAX_PARENTS     # 384
_OPS_TORUS_KV: int       = SAT_EDGES                  # 288
_OPS_RESIST_UPSERTS: int = 4 * 5 * 2                  # 40
OPS_PER_TICK_AT_SAT: int = (
    _OPS_LLADA_WRITES + _OPS_BFS_PROBES + _OPS_TORUS_KV + _OPS_RESIST_UPSERTS
)  # 748


# ── Slot configuration ─────────────────────────────────────────────────────────

SLOT_INTERVAL_S: int   = 60    # slot threads tick 5× faster than vision (300 s)
SLOT_INTERVAL_MIN_S: int = 20  # floor cadence at peak worker density
MAX_SLOTS: int         = 8     # upper bound on concurrently active slots
SLOTS_PER_TRIGGER: int = 2     # slots spawned per provisioning event

# Provisioning thresholds (bifurcated tunnel)
PROVISION_COLLAPSE_THRESHOLD: int = 3    # recent GROUNDED_TUNNEL collapses
PROVISION_CHILD_THRESHOLD: int    = 24   # total LLaDA2 child nodes
PROVISION_EDGE_THRESHOLD: int     = 144  # permanent GROUNDED_TUNNEL edges (≥50% sat)
PROVISION_WINDOW_S: int           = 1800 # 30-min rolling collapse window
PROVISION_COOLDOWN_S: int         = 3600 # minimum interval between triggers

_AMPLIFY_FACTOR: float     = 1.8          # torus step boost for slot nodes
_INTENT_KEY_PREFIX: str    = "compute_intent:"
_SLOT_AMPLIFY_PREFIX: str  = "torus_amplify:slot:"
_GROUNDED_REL: str         = "GROUNDED_TUNNEL"
_SYMBIOTIC_REL: str        = "SYMBIOTIC_TUNNEL"
_SIGNBIT_CHILD_LABEL: str  = "LLaDAChild"
_SIGNBIT_REL: str          = "SIGNBIT_FLIP"
_SLOT_ENTITY_TYPE: str     = "ComputeSlot"
_EXTERNAL_TYPE: str        = "ExternalResource"
_INGESTED_REL: str         = "INGESTED_AT_BOUNDARY"
_HARMONIC_BOND_REL: str    = "HARMONIC_BOND"    # co-anchor harmonic back-bond
_INGEST_KEY_PREFIX: str    = "boundary_ingest:"

# Harmonic amplification calibration.
# Single-axis floor: _AMPLIFY_FACTOR × _HARMONIC_FLOOR = 0.90
# Fully coherent ceiling: _AMPLIFY_FACTOR × _HARMONIC_CEILING = 4.50
# Follows a saturation curve: factor(c) = floor + (ceiling−floor)×(1 − 1/(c+1))
# so each additional convergent flip axis raises amplification toward the ceiling.
_HARMONIC_FLOOR: float   = 0.5   # × _AMPLIFY_FACTOR  (single-axis baseline)
_HARMONIC_CEILING: float = 2.5   # × _AMPLIFY_FACTOR  (fully coherent asymptote)

# Phase 2 multi-ingestion controls.
# At MAX_SLOTS the provisioner ingests one external resource per active flip
# anchor (up to _MAX_INGEST_PER_CALL) so that a richer flip state opens wider.
_MAX_INGEST_PER_CALL: int  = 4    # hard cap on resources ingested per tick

# Outward propagation: after bonding a resource to its anchor the provisioner
# traverses _PROPAGATION_HOPS tunnel hops and writes bidirectional edges,
# establishing a reverse integration flow back into the corpus.
_PROPAGATION_HOPS: int     = 1

# Edge types added in Phase 2 harmonic expansion.
_REVERSE_FLOW_REL: str     = "REVERSE_INTEGRATION"   # descendant → anchor back-bond

# Polarity alignment weights. An anchor appearing as src_id in a SIGNBIT_FLIP
# edge is a positive emitter; one appearing only as dst_id is a negative
# receiver.  Alignment with the boundary type modulates the bond weight.
#   emitter  + GROUNDED_TUNNEL  → maximal coherence (projects toward ground)
#   receiver + SYMBIOTIC_TUNNEL → maximal coherence (absorbs into symbiotic)
#   cross-polarity              → reduced weight (SYMBIOTIC = −GROUND)
_POLARITY_ALIGN: dict[tuple[int, str], float] = {
    (+1, _GROUNDED_REL):  1.25,
    (-1, _SYMBIOTIC_REL): 1.25,
    ( 0, _GROUNDED_REL):  1.00,
    ( 0, _SYMBIOTIC_REL): 1.00,
    (+1, _SYMBIOTIC_REL): 0.75,
    (-1, _GROUNDED_REL):  0.75,
}

# Dynamic tick cadence — all active slot threads read this each iteration.
# _update_slot_cadence() lowers it as saturation deepens, boosting worker density.
_SLOT_TARGET_INTERVAL: int = SLOT_INTERVAL_S


# ── Module-level slot registry ────────────────────────────────────────────────

_ACTIVE_SLOTS: dict[str, threading.Thread] = {}
_ACTIVE_SLOTS_LOCK: threading.Lock = threading.Lock()
_SLOT_STOP: threading.Event = threading.Event()   # set by shutdown_slots()


def shutdown_slots() -> None:
    """Signal all slot threads to stop.  Call from stop_continuous_synaptic_agents."""
    _SLOT_STOP.set()


def active_slot_count() -> int:
    """Return number of slot threads currently alive."""
    with _ACTIVE_SLOTS_LOCK:
        return sum(1 for t in _ACTIVE_SLOTS.values() if t.is_alive())


def _update_slot_cadence(sat: "TunnelSaturation") -> None:
    """Lower ``_SLOT_TARGET_INTERVAL`` as saturation deepens to boost worker density.

    Three tiers:

    * **Normal** (few collapses): ``SLOT_INTERVAL_S`` = 60 s
    * **Elevated** (3× collapse threshold): ``SLOT_INTERVAL_S // 2`` = 30 s
    * **Peak density** (2× edge threshold): ``SLOT_INTERVAL_MIN_S`` = 20 s

    All active slot threads read ``_SLOT_TARGET_INTERVAL`` each iteration, so
    cadence changes propagate without restarting threads.
    """
    global _SLOT_TARGET_INTERVAL
    if sat.grounded_edge_count >= PROVISION_EDGE_THRESHOLD * 2:
        _SLOT_TARGET_INTERVAL = SLOT_INTERVAL_MIN_S
    elif sat.recent_collapse_count >= PROVISION_COLLAPSE_THRESHOLD * 3:
        _SLOT_TARGET_INTERVAL = max(SLOT_INTERVAL_MIN_S, SLOT_INTERVAL_S // 2)
    else:
        _SLOT_TARGET_INTERVAL = SLOT_INTERVAL_S


# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class TunnelSaturation:
    grounded_edge_count: int   = 0
    recent_collapse_count: int = 0
    llada_child_count: int     = 0
    ops_per_tick: int          = OPS_PER_TICK_AT_SAT
    active_slots: int          = 0
    provision_triggered: bool  = False
    trigger_reason: str        = ""
    last_intent_age_s: float   = float("inf")


@dataclass
class ComputeIntent:
    timestamp: str        = field(default_factory=lambda: datetime.now().isoformat())
    trigger_reason: str   = ""
    ops_per_tick: int     = OPS_PER_TICK_AT_SAT
    grounded_edges: int   = 0
    llada_children: int   = 0
    recent_collapses: int = 0
    slots_before: int     = 0
    slots_after: int      = 0
    slot_interval_s: int  = SLOT_INTERVAL_S   # current cadence at time of intent
    status: str           = "ACQUIRED"        # ACQUIRED (Phase 1) | INGESTED (Phase 2)
    # Phase 2 fields — populated when at MAX_SLOTS capacity
    ingested_external: str   = ""    # comma-joined entity_ids of ingested resources
    ingested_anchor: str     = ""    # primary anchor used for the first ingestion
    ingested_edge_type: str  = ""    # GROUNDED_TUNNEL or SYMBIOTIC_TUNNEL
    anchor_count: int        = 0     # total active flip anchors at ingest time
    harmonic_coherence: int  = 0     # sign-bit flip axis convergence count
    harmonic_factor: float   = 0.0   # torus amplification factor (first resource)
    mean_harmonic_factor: float = 0.0  # mean h_factor across all ingestions
    mean_polarity_weight: float = 0.0  # mean polarity alignment weight
    total_descendants_reached: int = 0  # tunnel nodes reached by outward propagation


# ── Slot expansion thread ─────────────────────────────────────────────────────

def _slot_expansion_loop(slot_id: str) -> None:
    """Thread target for a ComputeSlot.

    Runs ``tick_torus_pressure`` + ``ground_and_expand`` at ``SLOT_INTERVAL_S``
    cadence, using its own SQLite connection so it does not block the vision
    worker.  Writes a per-slot heartbeat to ``kv_store`` and removes the
    ``torus_amplify`` key when it exits.
    """
    try:
        import sys as _sys
        _pkg = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        _sys.path.insert(0, _pkg)
        from src.brain.local_store import db_path as _db_path  # type: ignore[import]
        _path = str(_db_path())
    except Exception as exc:
        logging.warning("[slot:%s] cannot resolve db_path: %s", slot_id, exc)
        return

    logging.info("[slot:%s] started — interval=%ds", slot_id, SLOT_INTERVAL_S)

    while not _SLOT_STOP.is_set():
        t0 = time.time()
        try:
            cn = sqlite3.connect(_path, timeout=10)
            try:
                _ensure_kv_store(cn)

                # ── Torus pressure: push manifold boundary outward ──────────
                try:
                    from src.brain.torus_touch import (  # type: ignore[import]
                        tick_torus_pressure,
                    )
                    tick_torus_pressure(cn)
                except Exception as e:
                    logging.debug("[slot:%s] torus_touch skip: %s", slot_id, e)

                # ── Grounded expansion: open new expansory paths ────────────
                try:
                    from src.brain.grounded_tunneling import (  # type: ignore[import]
                        ground_and_expand,
                    )
                    ground_and_expand(cn, max_new_paths=2)
                except Exception as e:
                    logging.debug("[slot:%s] grounded_tunneling skip: %s", slot_id, e)

                # ── Heartbeat ───────────────────────────────────────────────
                elapsed = round(time.time() - t0, 2)
                cn.execute(
                    "INSERT INTO kv_store(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (f"slot_heartbeat:{slot_id}",
                     f"{datetime.now().isoformat()}|elapsed={elapsed}s"),
                )
                cn.commit()
            finally:
                cn.close()

        except Exception as exc:
            logging.warning("[slot:%s] iteration error: %s", slot_id, exc)

        # Sleep in small increments to stay responsive to _SLOT_STOP.
        # Reads _SLOT_TARGET_INTERVAL each iteration so cadence changes take
        # effect without restarting the thread.
        deadline = time.time() + _SLOT_TARGET_INTERVAL
        while time.time() < deadline and not _SLOT_STOP.is_set():
            time.sleep(1)

    # ── Clean-up: remove torus_amplify key and heartbeat ───────────────────
    try:
        cn = sqlite3.connect(_path, timeout=5)
        cn.execute(
            "DELETE FROM kv_store WHERE key=? OR key=?",
            (f"{_SLOT_AMPLIFY_PREFIX}{slot_id}",
             f"slot_heartbeat:{slot_id}"),
        )
        cn.commit()
        cn.close()
    except Exception:
        pass

    logging.info("[slot:%s] stopped", slot_id)

    with _ACTIVE_SLOTS_LOCK:
        _ACTIVE_SLOTS.pop(slot_id, None)


# ── Slot registration in corpus graph ─────────────────────────────────────────

def _register_slot_entity(cn: sqlite3.Connection, slot_id: str) -> None:
    """Upsert a ComputeSlot corpus entity and write its torus_amplify boost."""
    now_s = datetime.now().isoformat()
    cn.execute(
        "INSERT INTO corpus_entity"
        "(entity_id,entity_type,label,props_json,first_seen,last_seen,samples) "
        "VALUES(?,?,?,?,?,?,1) "
        "ON CONFLICT(entity_id,entity_type) "
        "DO UPDATE SET last_seen=excluded.last_seen, samples=samples+1",
        (slot_id, _SLOT_ENTITY_TYPE,
         f"ComputeSlot {slot_id[-8:]}",
         json.dumps({"slot_interval_s": SLOT_INTERVAL_S,
                     "acquired_at": now_s}),
         now_s, now_s),
    )
    # Torus amplify: Touch reads this key and multiplies step for this entity
    cn.execute(
        "INSERT INTO kv_store(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (f"{_SLOT_AMPLIFY_PREFIX}{slot_id}", str(_AMPLIFY_FACTOR)),
    )


def _deregister_stale_slots(cn: sqlite3.Connection) -> int:
    """Remove corpus entities for dead slot threads.  Returns reclaim count."""
    dead: list[str] = []
    with _ACTIVE_SLOTS_LOCK:
        for sid, t in list(_ACTIVE_SLOTS.items()):
            if not t.is_alive():
                dead.append(sid)
                del _ACTIVE_SLOTS[sid]

    for sid in dead:
        cn.execute(
            "DELETE FROM corpus_entity WHERE entity_id=? AND entity_type=?",
            (sid, _SLOT_ENTITY_TYPE),
        )
        cn.execute(
            "DELETE FROM kv_store WHERE key=? OR key=?",
            (f"{_SLOT_AMPLIFY_PREFIX}{sid}",
             f"slot_heartbeat:{sid}"),
        )
        logging.info("[compute_provisioner] reclaimed dead slot %s", sid)

    return len(dead)


# ── Local compute acquisition ─────────────────────────────────────────────────

def acquire_local_compute(cn: sqlite3.Connection, n_slots: int = SLOTS_PER_TRIGGER) -> list[str]:
    """Spawn up to *n_slots* new ComputeSlot threads and register corpus entities.

    Returns list of slot IDs actually spawned (may be fewer than *n_slots* if
    ``MAX_SLOTS`` would be exceeded).
    """
    _ensure_kv_store(cn)
    _deregister_stale_slots(cn)

    with _ACTIVE_SLOTS_LOCK:
        current = sum(1 for t in _ACTIVE_SLOTS.values() if t.is_alive())
    headroom = MAX_SLOTS - current
    to_spawn = min(n_slots, headroom)

    spawned: list[str] = []
    for _ in range(to_spawn):
        slot_id = f"slot:{uuid.uuid4().hex[:12]}"
        _register_slot_entity(cn, slot_id)

        t = threading.Thread(
            target=_slot_expansion_loop,
            args=(slot_id,),
            name=f"compute-slot-{slot_id[-8:]}",
            daemon=True,
        )
        t.start()

        with _ACTIVE_SLOTS_LOCK:
            _ACTIVE_SLOTS[slot_id] = t

        spawned.append(slot_id)
        logging.info(
            "[compute_provisioner] slot acquired: %s (active=%d/%d)",
            slot_id, current + len(spawned), MAX_SLOTS,
        )

    return spawned


# ── Phase 2 — external resource ingestion at boundary ─────────────────────────

def _find_flip_anchors(cn: sqlite3.Connection) -> list[str]:
    """Return entity IDs of corpus nodes that are *parents* of a SIGNBIT_FLIP edge.

    These are the directional axes where the sign bit flipped — the natural
    boundary points at which the Touch opens the graph to an external resource.
    Falls back to GROUNDED_TUNNEL source nodes if no flip edges exist yet.
    """
    try:
        rows = cn.execute(
            "SELECT DISTINCT src_id FROM corpus_edge WHERE rel=?",
            (_SIGNBIT_REL,),
        ).fetchall()
        if rows:
            return [r[0] for r in rows]
    except sqlite3.Error:
        pass

    # Fallback: use ground nodes (high-certainty GROUNDED_TUNNEL sources)
    try:
        rows = cn.execute(
            "SELECT DISTINCT src_id FROM corpus_edge WHERE rel=? LIMIT 10",
            (_GROUNDED_REL,),
        ).fetchall()
        return [r[0] for r in rows]
    except sqlite3.Error:
        return []


def _find_boundary_edge(
    cn: sqlite3.Connection,
    anchors: list[str],
) -> tuple[str | None, str | None]:
    """Find the best boundary edge (GROUNDED_TUNNEL or SYMBIOTIC_TUNNEL) incident
    to any of the flip-anchor nodes.

    Returns ``(anchor_node_id, edge_type)``.  Prefers GROUNDED_TUNNEL (certainty-
    anchored) over SYMBIOTIC_TUNNEL.  Returns ``(None, None)`` if no boundary
    edge is found.
    """
    for rel in (_GROUNDED_REL, _SYMBIOTIC_REL):
        for anchor in anchors:
            try:
                row = cn.execute(
                    "SELECT src_id FROM corpus_edge "
                    "WHERE rel=? AND (src_id=? OR dst_id=?) LIMIT 1",
                    (rel, anchor, anchor),
                ).fetchone()
                if row:
                    return anchor, rel
            except sqlite3.Error:
                continue
    return None, None


def _discover_external_resources(
    cn: sqlite3.Connection,
    count: int,
    skip_ids: set[str] | None = None,
) -> list[dict]:
    """Discover up to *count* distinct external resources not yet in the corpus.

    Search order:
    1. ``network_topology`` rows with ``ema_success > 0``, newest-first, skipping
       any entity_id already in *skip_ids* or already registered as a corpus
       entity.
    2. Synthetic boundary-probe fallback — generates a fresh UUID each call so
       repeated requests always produce distinct probes.

    Returns a list of dicts (keys: ``entity_id``, ``host``, ``protocol``,
    ``port``, ``source``, ``label``), length ≤ *count*.
    """
    seen: set[str] = set(skip_ids or ())
    results: list[dict] = []

    # ── Search 1: network_topology ─────────────────────────────────────────
    try:
        rows = cn.execute(
            "SELECT host, protocol, port, capability, source "
            "FROM network_topology "
            "WHERE ema_success > 0 "
            "ORDER BY ema_success DESC"
        ).fetchall()
        for host, proto, port, cap, source in rows:
            if len(results) >= count:
                break
            eid = f"{proto}:{host}:{port or 0}"
            if eid in seen:
                continue
            exists = cn.execute(
                "SELECT 1 FROM corpus_entity WHERE entity_id=? LIMIT 1",
                (eid,),
            ).fetchone()
            if not exists:
                seen.add(eid)
                results.append({
                    "entity_id": eid,
                    "host": host,
                    "protocol": proto,
                    "port": port,
                    "source": source or "network_topology",
                    "label": f"{cap or host} [{proto}:{port or '?'}]",
                })
    except sqlite3.OperationalError:
        pass  # network_topology not yet created

    # ── Fallback: synthetic boundary probes for any remaining slots ─────────
    while len(results) < count:
        probe_id = f"boundary_probe:{uuid.uuid4().hex[:8]}"
        results.append({
            "entity_id": probe_id,
            "host": "boundary-probe",
            "protocol": "synthetic",
            "port": 0,
            "source": "compute_provisioner",
            "label": f"Boundary Probe {probe_id[-8:]}",
        })

    return results


def _anchor_coherence(cn: sqlite3.Connection, anchor: str) -> int:
    """Count the number of distinct SIGNBIT_FLIP edges incident to *anchor*.

    This is the harmonic coherence score — the number of sign-bit axes that
    simultaneously converge on this node.  A higher count means the manifold
    is in a more torsionally coherent state, which admits a stronger bond when
    an external resource is ingested here.
    """
    try:
        row = cn.execute(
            "SELECT COUNT(*) FROM corpus_edge "
            "WHERE rel=? AND (src_id=? OR dst_id=?)",
            (_SIGNBIT_REL, anchor, anchor),
        ).fetchone()
        return int(row[0]) if row else 0
    except sqlite3.Error:
        return 0


def _harmonic_amplify_factor(coherence: int) -> float:
    """Compute torus amplification factor calibrated to sign-bit coherence.

    Follows a harmonic saturation curve modulated by the UEQGM v0.9.14
    SiCi axial channel phase correction::

        base(c) = floor + (ceiling − floor) × (1 − 1/(c + 1))
        f(c)    = base(c) × sici_phase_weight(c)

    where ``floor = _AMPLIFY_FACTOR × _HARMONIC_FLOOR`` and
    ``ceiling = _AMPLIFY_FACTOR × _HARMONIC_CEILING``.

    The SiCi correction maps coherence → φ = π/4 + c·π (natural sin/cos
    intersection points) and applies the UEQGM axial decay differential
    Δλ_axial = Si(φ)·Ci(φ)·tan(φ)·Γ₀ as a small phase perturbation
    (≤ ±10 % of the base, matching the ~0.3 % stabilisation reported in
    UEQGM v0.9.14).  At large coherence Ci(φ) → 0, so the correction
    converges to 1.0 and the saturation ceiling is preserved.

    Examples (with default constants 1.8 × [0.5, 2.5])::

        coherence=0  →  ~0.91  (base 0.90 × UEQGM phase weight ≈ 1.014)
        coherence=1  →  ~2.64  (base 2.70 × UEQGM phase weight ≈ 0.978)
        coherence=2  →  ~3.34  (base 3.30 × UEQGM phase weight ≈ 1.012)
        coherence=∞  →  4.50   (UEQGM correction → 1.0; ceiling preserved)
    """
    floor   = _AMPLIFY_FACTOR * _HARMONIC_FLOOR
    ceiling = _AMPLIFY_FACTOR * _HARMONIC_CEILING
    base = floor + (ceiling - floor) * (1.0 - 1.0 / (coherence + 1))
    # UEQGM v0.9.14: apply SiCi axial channel phase correction.
    try:
        from src.brain import ueqgm_engine as _ueqgm  # type: ignore[import]
        base = base * _ueqgm.sici_phase_weight(coherence)
    except Exception:  # pragma: no cover — import fails only in stripped envs
        pass
    return round(base, 4)


def _anchor_polarity(cn: sqlite3.Connection, anchor: str) -> int:
    """Classify the anchor's sign-bit polarity relative to SIGNBIT_FLIP edges.

    Returns
    -------
    +1  The anchor appears *only* as ``src_id`` — it is a **positive emitter**
        that projects the flip outward.
    -1  The anchor appears *only* as ``dst_id`` — it is a **negative receiver**
        that absorbs the flip.
     0  The anchor appears on both sides (mixed) or has no SIGNBIT_FLIP edges.
    """
    try:
        as_src = cn.execute(
            "SELECT COUNT(*) FROM corpus_edge WHERE rel=? AND src_id=?",
            (_SIGNBIT_REL, anchor),
        ).fetchone()[0]
        as_dst = cn.execute(
            "SELECT COUNT(*) FROM corpus_edge WHERE rel=? AND dst_id=?",
            (_SIGNBIT_REL, anchor),
        ).fetchone()[0]
    except sqlite3.Error:
        return 0

    if as_src > 0 and as_dst == 0:
        return +1
    if as_dst > 0 and as_src == 0:
        return -1
    return 0  # mixed or absent


def _polarity_alignment_weight(polarity: int, edge_type: str) -> float:
    """Return the polarity alignment multiplier for a (polarity, boundary) pair.

    Positive emitters align with GROUNDED_TUNNEL (projecting certainty to ground).
    Negative receivers align with SYMBIOTIC_TUNNEL (SYMBIOTIC = −GROUND).
    Cross-polarity pairs receive reduced weight (0.75).
    """
    return _POLARITY_ALIGN.get((polarity, edge_type), 1.0)


def _propagate_outward(
    cn: sqlite3.Connection,
    anchor: str,
    ext_id: str,
    h_factor: float,
    polarity_weight: float,
    now_s: str,
) -> int:
    """Propagate the newly ingested resource outward along tunnel paths.

    Traverses ``_PROPAGATION_HOPS`` hops of GROUNDED_TUNNEL + SYMBIOTIC_TUNNEL
    edges from *anchor* and writes:

    * ``HARMONIC_BOND``       — ExternalResource → each descendant (outward flow)
    * ``REVERSE_INTEGRATION`` — each descendant → anchor (back-flow into corpus)

    This establishes the bidirectional integration that lets the corpus graph
    "feel" the new external resource through its existing tunnel topology.

    Returns the number of descendants reached.
    """
    try:
        rows = cn.execute(
            "SELECT DISTINCT dst_id, dst_type FROM corpus_edge "
            "WHERE rel IN (?, ?) AND src_id=? AND dst_id!=?",
            (_GROUNDED_REL, _SYMBIOTIC_REL, anchor, ext_id),
        ).fetchall()
    except sqlite3.Error:
        return 0

    if not rows:
        return 0

    outward_w = round(h_factor * polarity_weight * 0.6, 4)   # dampened outward
    reverse_w = round(h_factor * polarity_weight * 0.4, 4)   # lighter back-bond

    count = 0
    for dst_id, dst_type in rows:
        # ExternalResource → descendant (outward projection)
        cn.execute(
            "INSERT INTO corpus_edge"
            "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
            "VALUES(?,?,?,?,?,?,?,1) "
            "ON CONFLICT(src_id,src_type,dst_id,dst_type,rel) "
            "DO UPDATE SET last_seen=excluded.last_seen, "
            "weight=MAX(weight,excluded.weight), samples=samples+1",
            (ext_id, _EXTERNAL_TYPE, dst_id, dst_type,
             _HARMONIC_BOND_REL, outward_w, now_s),
        )
        # descendant → anchor (reverse integration flow)
        cn.execute(
            "INSERT INTO corpus_edge"
            "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
            "VALUES(?,?,?,?,?,?,?,1) "
            "ON CONFLICT(src_id,src_type,dst_id,dst_type,rel) "
            "DO UPDATE SET last_seen=excluded.last_seen, "
            "weight=MAX(weight,excluded.weight), samples=samples+1",
            (dst_id, dst_type, anchor, "corpus_entity",
             _REVERSE_FLOW_REL, reverse_w, now_s),
        )
        count += 1

    return count


def ingest_external_at_boundary(cn: sqlite3.Connection) -> dict:
    """Phase 2: harmonious multi-resource ingestion at Sign-Bit Flip boundaries.

    Algorithm
    ---------
    1. Collect all flip-anchor nodes (SIGNBIT_FLIP src/fallback GROUNDED_TUNNEL
       srcs).  The number of active anchors determines *how many* resources are
       ingested this call — one per anchor up to ``_MAX_INGEST_PER_CALL``.

    2. For each anchor:

       a. Classify the anchor's **polarity** (+1 emitter / −1 receiver / 0 neutral)
          from SIGNBIT_FLIP edge direction.
       b. Compute the anchor's **harmonic coherence** (count of convergent flip axes).
       c. Compute a **harmonic factor** (coherence-weighted torus amplification).
       d. Compute a **polarity alignment weight** — maximum when the polarity
          matches the boundary type (emitter→GROUNDED, receiver→SYMBIOTIC).
       e. The effective bond weight = ``h_factor × polarity_weight``.
       f. Discover a distinct external resource (skipping already-ingested IDs).
       g. Register the ``ExternalResource`` corpus entity.
       h. Write ``INGESTED_AT_BOUNDARY`` + boundary edge type edges.
       i. Write ``HARMONIC_BOND`` edges from all co-active anchors.
       j. Write ``torus_amplify`` key (coherence-scaled).
       k. Propagate outward along tunnel descendants (``HARMONIC_BOND`` forward
          + ``REVERSE_INTEGRATION`` back-flow).

    3. After all individual ingestions, **harmonize** the newly ingested
       resources with each other by writing cross-``HARMONIC_BOND`` edges
       at the mean effective weight, merging them into a coherent cluster.

    Returns a stats dict with ``ingested_count``, ``ingested_ids`` (list),
    ``anchor_count``, ``mean_harmonic_factor``, ``mean_polarity_weight``,
    ``total_descendants_reached``, plus legacy singular keys for the first
    ingested resource (``anchor``, ``edge_type``, ``external_id``,
    ``harmonic_coherence``, ``harmonic_factor``).
    """
    stats: dict[str, Any] = {
        "ingested_count": 0,
        "ingested_ids": [],
        "anchor": None,
        "edge_type": None,
        "external_id": None,
        "anchor_count": 0,
        "harmonic_coherence": 0,
        "harmonic_factor": 0.0,
        "mean_harmonic_factor": 0.0,
        "mean_polarity_weight": 0.0,
        "total_descendants_reached": 0,
    }

    anchors = _find_flip_anchors(cn)
    if not anchors:
        logging.debug("[compute_provisioner] phase2: no flip anchors — skipping ingest")
        return stats

    stats["anchor_count"] = len(anchors)

    # How many ingestions to attempt — proportional to anchor count, hard-capped.
    ingest_slots = min(len(anchors), _MAX_INGEST_PER_CALL)
    active_anchors = anchors[:ingest_slots]

    # Discover distinct resources — one per anchor slot.
    now_s    = datetime.now().isoformat()
    skip_ids: set[str] = set()
    resources = _discover_external_resources(cn, ingest_slots, skip_ids)
    if not resources:
        return stats

    ingested_ids: list[str] = []
    effective_weights: list[float] = []
    polarity_weights_acc: list[float] = []
    h_factors_acc: list[float] = []
    total_desc = 0

    for i, anchor in enumerate(active_anchors):
        if i >= len(resources):
            break

        external = resources[i]
        ext_id   = external["entity_id"]

        # Find boundary edge for this specific anchor.
        anchor_node, edge_type = _find_boundary_edge(cn, [anchor])
        if anchor_node is None:
            # Try the whole anchor list as fallback.
            anchor_node, edge_type = _find_boundary_edge(cn, active_anchors)
        if anchor_node is None:
            logging.debug(
                "[compute_provisioner] phase2: no boundary edge for anchor %s — skip",
                anchor,
            )
            continue

        polarity        = _anchor_polarity(cn, anchor)
        coherence       = _anchor_coherence(cn, anchor)
        h_factor        = _harmonic_amplify_factor(coherence)
        pol_weight      = _polarity_alignment_weight(polarity, edge_type)
        effective_w     = round(h_factor * pol_weight, 4)

        props = json.dumps({k: v for k, v in external.items() if k != "entity_id"})

        # ── Register ExternalResource corpus entity ──────────────────────
        cn.execute(
            "INSERT INTO corpus_entity"
            "(entity_id,entity_type,label,props_json,first_seen,last_seen,samples) "
            "VALUES(?,?,?,?,?,?,1) "
            "ON CONFLICT(entity_id,entity_type) "
            "DO UPDATE SET last_seen=excluded.last_seen, samples=samples+1",
            (ext_id, _EXTERNAL_TYPE, external["label"], props, now_s, now_s),
        )

        # ── Primary bonds: INGESTED_AT_BOUNDARY + boundary edge type ──────
        for rel in (_INGESTED_REL, edge_type):
            cn.execute(
                "INSERT INTO corpus_edge"
                "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
                "VALUES(?,?,?,?,?,?,?,1) "
                "ON CONFLICT(src_id,src_type,dst_id,dst_type,rel) "
                "DO UPDATE SET last_seen=excluded.last_seen, "
                "weight=MAX(weight,excluded.weight), samples=samples+1",
                (anchor, "corpus_entity", ext_id, _EXTERNAL_TYPE,
                 rel, effective_w, now_s),
            )

        # ── HARMONIC_BOND from co-active anchors ───────────────────────────
        co_weight = round(effective_w / max(1, len(active_anchors)), 4)
        for co in active_anchors:
            if co == anchor:
                continue
            cn.execute(
                "INSERT INTO corpus_edge"
                "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
                "VALUES(?,?,?,?,?,?,?,1) "
                "ON CONFLICT(src_id,src_type,dst_id,dst_type,rel) "
                "DO UPDATE SET last_seen=excluded.last_seen, "
                "weight=MAX(weight,excluded.weight), samples=samples+1",
                (co, "corpus_entity", ext_id, _EXTERNAL_TYPE,
                 _HARMONIC_BOND_REL, co_weight, now_s),
            )

        # ── torus_amplify key (Touch amplifies on next pass) ──────────────
        cn.execute(
            "INSERT INTO kv_store(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (f"torus_amplify:{ext_id}", str(effective_w)),
        )

        # ── Outward propagation + reverse integration flow ─────────────────
        desc_count = _propagate_outward(
            cn, anchor, ext_id, h_factor, pol_weight, now_s
        )

        # ── Per-ingestion kv record ────────────────────────────────────────
        ingest_record = {
            "timestamp":        now_s,
            "external_id":      ext_id,
            "anchor":           anchor,
            "edge_type":        edge_type,
            "label":            external["label"],
            "polarity":         polarity,
            "polarity_weight":  pol_weight,
            "harmonic_coherence": coherence,
            "harmonic_factor":  h_factor,
            "effective_weight": effective_w,
            "co_anchor_count":  len(active_anchors) - 1,
            "descendants_reached": desc_count,
        }
        cn.execute(
            "INSERT INTO kv_store(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (f"{_INGEST_KEY_PREFIX}{now_s}:{ext_id[:8]}", json.dumps(ingest_record)),
        )

        ingested_ids.append(ext_id)
        effective_weights.append(effective_w)
        polarity_weights_acc.append(pol_weight)
        h_factors_acc.append(h_factor)
        total_desc += desc_count

        logging.info(
            "[compute_provisioner] phase2 ingested[%d/%d]: %s "
            "anchor=%s edge=%s polarity=%+d coherence=%d "
            "h_factor=%.3f pol_weight=%.2f eff_w=%.3f desc=%d",
            i + 1, ingest_slots, ext_id, anchor, edge_type,
            polarity, coherence, h_factor, pol_weight, effective_w, desc_count,
        )

    if not ingested_ids:
        return stats

    # ── Cross-resource harmonization: mesh co-ingested resources ──────────
    if len(ingested_ids) > 1:
        mean_w = round(sum(effective_weights) / len(effective_weights), 4)
        for a_idx, a_id in enumerate(ingested_ids):
            for b_id in ingested_ids[a_idx + 1:]:
                for src, dst in ((a_id, b_id), (b_id, a_id)):
                    cn.execute(
                        "INSERT INTO corpus_edge"
                        "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
                        "VALUES(?,?,?,?,?,?,?,1) "
                        "ON CONFLICT(src_id,src_type,dst_id,dst_type,rel) "
                        "DO UPDATE SET last_seen=excluded.last_seen, "
                        "weight=MAX(weight,excluded.weight), samples=samples+1",
                        (src, _EXTERNAL_TYPE, dst, _EXTERNAL_TYPE,
                         _HARMONIC_BOND_REL, mean_w, now_s),
                    )

    n = len(ingested_ids)
    mean_hf  = round(sum(h_factors_acc) / n, 4)
    mean_pw  = round(sum(polarity_weights_acc) / n, 4)

    # Populate legacy singular keys from the first ingestion.
    first_anchor, first_edge = active_anchors[0], None
    try:
        first_edge = _find_boundary_edge(cn, [active_anchors[0]])[1]
    except Exception:
        pass

    stats.update({
        "ingested_count":           n,
        "ingested_ids":             ingested_ids,
        "anchor":                   active_anchors[0],
        "edge_type":                first_edge,
        "external_id":              ingested_ids[0],
        "harmonic_coherence":       int(h_factors_acc[0] > 0),  # bool proxy
        "harmonic_factor":          h_factors_acc[0],
        "mean_harmonic_factor":     mean_hf,
        "mean_polarity_weight":     mean_pw,
        "total_descendants_reached": total_desc,
    })
    return stats


# ── Saturation measurement ────────────────────────────────────────────────────

def measure_tunnel_saturation(cn: sqlite3.Connection) -> TunnelSaturation:
    """Read bifurcated tunnel state from the corpus graph and kv_store."""
    sat = TunnelSaturation()
    sat.active_slots = active_slot_count()

    try:
        row = cn.execute(
            "SELECT COUNT(*) FROM corpus_edge WHERE rel=?", (_GROUNDED_REL,)
        ).fetchone()
        sat.grounded_edge_count = int(row[0]) if row else 0
    except sqlite3.Error:
        pass

    try:
        window_start = (
            datetime.now() - timedelta(seconds=PROVISION_WINDOW_S)
        ).isoformat()
        row = cn.execute(
            "SELECT COUNT(*) FROM corpus_edge WHERE rel=? AND last_seen >= ?",
            (_GROUNDED_REL, window_start),
        ).fetchone()
        sat.recent_collapse_count = int(row[0]) if row else 0
    except sqlite3.Error:
        pass

    try:
        row = cn.execute(
            "SELECT COUNT(*) FROM corpus_entity WHERE label=?",
            (_SIGNBIT_CHILD_LABEL,),
        ).fetchone()
        sat.llada_child_count = int(row[0]) if row else 0
    except sqlite3.Error:
        pass

    extra_cycles = max(0, sat.llada_child_count - SAT_NODES) // CHILDREN_PER_CYCLE
    sat.ops_per_tick = OPS_PER_TICK_AT_SAT + extra_cycles * (
        _OPS_LLADA_WRITES + _OPS_BFS_PROBES // 4
    )

    # Cooldown: check last intent timestamp
    try:
        rows = cn.execute(
            "SELECT value FROM kv_store WHERE key LIKE ? ORDER BY key DESC LIMIT 1",
            (f"{_INTENT_KEY_PREFIX}%",),
        ).fetchall()
        if rows:
            last_rec = json.loads(rows[0][0])
            last_ts = datetime.fromisoformat(last_rec.get("timestamp", "2000-01-01"))
            sat.last_intent_age_s = (datetime.now() - last_ts).total_seconds()
    except Exception:
        pass

    if sat.last_intent_age_s < PROVISION_COOLDOWN_S:
        return sat

    # Threshold evaluation — any one is sufficient
    reasons: list[str] = []
    if sat.recent_collapse_count >= PROVISION_COLLAPSE_THRESHOLD:
        reasons.append(
            f"collapses={sat.recent_collapse_count}≥{PROVISION_COLLAPSE_THRESHOLD}"
        )
    if sat.llada_child_count >= PROVISION_CHILD_THRESHOLD:
        reasons.append(
            f"llada_children={sat.llada_child_count}≥{PROVISION_CHILD_THRESHOLD}"
        )
    if sat.grounded_edge_count >= PROVISION_EDGE_THRESHOLD:
        reasons.append(
            f"grounded_edges={sat.grounded_edge_count}≥{PROVISION_EDGE_THRESHOLD}"
        )
    # Bootstrap: if no slots are running but substantial expansion has occurred,
    # provision even if recent_collapse_count is below the window threshold
    # (handles old edges that fell outside the rolling window).
    if sat.active_slots == 0 and sat.grounded_edge_count >= PROVISION_COLLAPSE_THRESHOLD:
        reasons.append(
            f"bootstrap|slots=0,grounded={sat.grounded_edge_count}"
        )

    if reasons:
        sat.provision_triggered = True
        sat.trigger_reason = " | ".join(reasons)

    return sat


# ── Emit intent record ────────────────────────────────────────────────────────

def _emit_compute_intent(cn: sqlite3.Connection, intent: ComputeIntent) -> None:
    key = f"{_INTENT_KEY_PREFIX}{intent.timestamp}"
    cn.execute(
        "INSERT INTO kv_store(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, json.dumps(asdict(intent))),
    )
    logging.info(
        "[compute_provisioner] intent recorded: trigger=%s slots=%d→%d ops/tick=%d",
        intent.trigger_reason, intent.slots_before, intent.slots_after,
        intent.ops_per_tick,
    )


# ── Main tick ─────────────────────────────────────────────────────────────────

def tick_compute_provisioner(cn: sqlite3.Connection) -> dict[str, Any]:
    """One compute-provisioner tick driven by bifurcated tunnel saturation.

    Two-phase routing
    -----------------
    **Phase 1 — local resource expansion** (``active_slots < MAX_SLOTS``):
        Spawn new ComputeSlot daemon threads + register corpus entities with
        ``torus_amplify`` boosts.  Also adjusts ``_SLOT_TARGET_INTERVAL`` so
        existing threads automatically run faster as saturation deepens.

    **Phase 2 — external boundary ingestion** (``active_slots >= MAX_SLOTS``):
        The Touch uses a Sign-Bit Flip axis to locate a Ground or Symbiotic
        edge boundary, discovers an external resource not yet in the corpus,
        and ingests it as an ``ExternalResource`` entity bonded at that
        boundary.  A ``torus_amplify`` key is written immediately so the
        Touch starts amplifying the new node on its next pass.

    Cadence update runs regardless of phase so thread density tracks
    saturation depth even when no new slots are spawned.

    Returns a stats dict for the ``_vision_worker`` heartbeat.
    """
    _ensure_kv_store(cn)

    sat = measure_tunnel_saturation(cn)
    _update_slot_cadence(sat)            # always adjust thread cadence

    stats: dict[str, Any] = {
        "grounded_edges":      sat.grounded_edge_count,
        "recent_collapses":    sat.recent_collapse_count,
        "llada_children":      sat.llada_child_count,
        "ops_per_tick":        sat.ops_per_tick,
        "active_slots":        sat.active_slots,
        "slot_interval_s":     _SLOT_TARGET_INTERVAL,
        "provision_triggered": sat.provision_triggered,
        "intent_status":       "NONE",
    }

    if not sat.provision_triggered:
        return stats

    at_capacity = sat.active_slots >= MAX_SLOTS

    if not at_capacity:
        # ── Phase 1: spawn local compute ────────────────────────────────────
        slots_before = sat.active_slots
        spawned = acquire_local_compute(cn, SLOTS_PER_TRIGGER)
        slots_after = slots_before + len(spawned)

        intent = ComputeIntent(
            trigger_reason   = sat.trigger_reason,
            ops_per_tick     = sat.ops_per_tick,
            grounded_edges   = sat.grounded_edge_count,
            llada_children   = sat.llada_child_count,
            recent_collapses = sat.recent_collapse_count,
            slots_before     = slots_before,
            slots_after      = slots_after,
            slot_interval_s  = _SLOT_TARGET_INTERVAL,
            status           = "ACQUIRED",
        )
        _emit_compute_intent(cn, intent)
        cn.commit()

        stats["intent_status"] = intent.status
        stats["intent_ts"]     = intent.timestamp
        stats["slots_spawned"] = len(spawned)
        stats["active_slots"]  = slots_after

    else:
        # ── Phase 2: ingest external resource at boundary ───────────────────
        ingest = ingest_external_at_boundary(cn)

        intent = ComputeIntent(
            trigger_reason   = sat.trigger_reason + " | AT_CAPACITY",
            ops_per_tick     = sat.ops_per_tick,
            grounded_edges   = sat.grounded_edge_count,
            llada_children   = sat.llada_child_count,
            recent_collapses = sat.recent_collapse_count,
            slots_before     = sat.active_slots,
            slots_after      = sat.active_slots,
            slot_interval_s  = _SLOT_TARGET_INTERVAL,
            status                = "INGESTED",
            ingested_external     = ",".join(ingest.get("ingested_ids") or [ingest.get("external_id") or ""]),
            ingested_anchor       = ingest.get("anchor") or "",
            ingested_edge_type    = ingest.get("edge_type") or "",
            anchor_count          = ingest.get("anchor_count", 0),
            harmonic_coherence    = ingest.get("harmonic_coherence", 0),
            harmonic_factor       = ingest.get("harmonic_factor", 0.0),
            mean_harmonic_factor  = ingest.get("mean_harmonic_factor", 0.0),
            mean_polarity_weight  = ingest.get("mean_polarity_weight", 0.0),
            total_descendants_reached = ingest.get("total_descendants_reached", 0),
        )
        _emit_compute_intent(cn, intent)
        cn.commit()

        stats["intent_status"]             = intent.status
        stats["intent_ts"]                 = intent.timestamp
        stats["ingested_count"]            = ingest.get("ingested_count", 0)
        stats["ingested_ids"]              = ingest.get("ingested_ids", [])
        stats["ingested_anchor"]           = ingest.get("anchor")
        stats["ingested_edge"]             = ingest.get("edge_type")
        stats["ingested_ext_id"]           = ingest.get("external_id")
        stats["anchor_count"]              = ingest.get("anchor_count", 0)
        stats["harmonic_coherence"]        = ingest.get("harmonic_coherence", 0)
        stats["harmonic_factor"]           = ingest.get("harmonic_factor", 0.0)
        stats["mean_harmonic_factor"]      = ingest.get("mean_harmonic_factor", 0.0)
        stats["mean_polarity_weight"]      = ingest.get("mean_polarity_weight", 0.0)
        stats["total_descendants_reached"] = ingest.get("total_descendants_reached", 0)

    return stats


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ensure_kv_store(cn: sqlite3.Connection) -> None:
    cn.execute(
        "CREATE TABLE IF NOT EXISTS kv_store("
        "key TEXT PRIMARY KEY, value TEXT)"
    )


def cpu_overhead_report(child_count: int = SAT_NODES, slot_count: int = 0) -> str:
    """Return a human-readable CPU overhead summary.

    *child_count* drives the vision-tick overhead; *slot_count* drives the
    per-slot overhead contribution (each slot runs ``SLOT_INTERVAL_S``).
    """
    extra_cycles = max(0, child_count - SAT_NODES) // CHILDREN_PER_CYCLE
    ops_tick = OPS_PER_TICK_AT_SAT + extra_cycles * (_OPS_LLADA_WRITES + _OPS_BFS_PROBES // 4)
    ticks_per_hr = 3600 // 300   # vision cadence
    vision_ops_hr = ops_tick * ticks_per_hr

    # Each slot: ~TORUS_DIMS × N_endpoints reads + ground_expand ~200 ops
    # Conservative estimate: 7 dims × 10 endpoints + 200 = 270 ops/slot_tick
    slot_tick_ops = 270
    slot_ticks_per_hr = 3600 // SLOT_INTERVAL_S
    slot_ops_hr = slot_count * slot_tick_ops * slot_ticks_per_hr

    total_hr = vision_ops_hr + slot_ops_hr
    return (
        f"LLaDA2.0 graph overhead @ {child_count} children, {slot_count} slots: "
        f"{ops_tick} vision ops/tick × {ticks_per_hr} ticks/hr = {vision_ops_hr:,} ops/hr "
        f"+ {slot_count} slots × {slot_tick_ops} ops/slot_tick × {slot_ticks_per_hr} = "
        f"{slot_ops_hr:,} slot ops/hr "
        f"= {total_hr:,} total ops/hr "
        f"({CHILDREN_PER_CYCLE} children/cycle, "
        f"{EDGES_PER_CYCLE} edges/cycle, "
        f"max {MAX_PARENTS} parents, "
        f"sat@{SAT_NODES} nodes/{SAT_EDGES} edges)"
    )


__all__ = [
    "TunnelSaturation",
    "ComputeIntent",
    "measure_tunnel_saturation",
    "acquire_local_compute",
    "ingest_external_at_boundary",
    "tick_compute_provisioner",
    "cpu_overhead_report",
    "shutdown_slots",
    "active_slot_count",
    "OPS_PER_TICK_AT_SAT",
    "SLOT_INTERVAL_S",
    "SLOT_INTERVAL_MIN_S",
    "MAX_SLOTS",
    "SLOTS_PER_TRIGGER",
    "PROVISION_COLLAPSE_THRESHOLD",
    "PROVISION_CHILD_THRESHOLD",
    "PROVISION_EDGE_THRESHOLD",
    "PROVISION_WINDOW_S",
    "PROVISION_COOLDOWN_S",
    "_HARMONIC_BOND_REL",
    "_HARMONIC_FLOOR",
    "_HARMONIC_CEILING",
    "_REVERSE_FLOW_REL",
    "_POLARITY_ALIGN",
    "_MAX_INGEST_PER_CALL",
    "_PROPAGATION_HOPS",
    "_harmonic_amplify_factor",
    "_anchor_coherence",
    "_anchor_polarity",
    "_polarity_alignment_weight",
    "_propagate_outward",
    "_discover_external_resources",
    # UEQGM v0.9.14 — SiCi axial channel phase correction
    "ueqgm_engine",
]
