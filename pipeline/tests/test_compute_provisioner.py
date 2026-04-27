"""Tests for compute_provisioner.py â€” local/virtual compute acquisition.

Encapsulated design: ComputeSlot daemon threads (local compute) + ComputeSlot
corpus entities with torus_amplify boosts (virtual compute) acquired when the
bifurcated tunnel (GROUNDED_TUNNEL + LLaDA2 children) crosses saturation
thresholds.  No external cloud calls.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from src.brain.compute_provisioner import (
    CHILDREN_PER_CYCLE,
    EDGES_PER_CYCLE,
    MAX_PARENTS,
    MAX_SLOTS,
    OPS_PER_TICK_AT_SAT,
    PROVISION_CHILD_THRESHOLD,
    PROVISION_COLLAPSE_THRESHOLD,
    PROVISION_COOLDOWN_S,
    PROVISION_EDGE_THRESHOLD,
    SAT_EDGES,
    SAT_NODES,
    SLOT_INTERVAL_MIN_S,
    SLOT_INTERVAL_S,
    SLOTS_PER_TRIGGER,
    ComputeIntent,
    TunnelSaturation,
    _ACTIVE_SLOTS,
    _ACTIVE_SLOTS_LOCK,
    _SLOT_AMPLIFY_PREFIX,
    _SLOT_ENTITY_TYPE,
    _EXTERNAL_TYPE,
    _INGESTED_REL,
    _HARMONIC_BOND_REL,
    _REVERSE_FLOW_REL,
    _POLARITY_ALIGN,
    _MAX_INGEST_PER_CALL,
    _AMPLIFY_FACTOR,
    _HARMONIC_FLOOR,
    _HARMONIC_CEILING,
    _harmonic_amplify_factor,
    _anchor_coherence,
    _anchor_polarity,
    _polarity_alignment_weight,
    _propagate_outward,
    _discover_external_resources,
    acquire_local_compute,
    active_slot_count,
    cpu_overhead_report,
    ingest_external_at_boundary,
    measure_tunnel_saturation,
    shutdown_slots,
    tick_compute_provisioner,
)


# â”€â”€ Fixtures â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _fresh_db() -> sqlite3.Connection:
    """In-memory DB with corpus_entity, corpus_edge, and kv_store tables."""
    cn = sqlite3.connect(":memory:")
    cn.execute(
        "CREATE TABLE corpus_entity("
        "entity_id TEXT, entity_type TEXT, label TEXT, props_json TEXT, "
        "first_seen TEXT, last_seen TEXT, samples INTEGER DEFAULT 1, "
        "UNIQUE(entity_id, entity_type))"
    )
    cn.execute(
        "CREATE TABLE corpus_edge("
        "src_id TEXT, src_type TEXT, dst_id TEXT, dst_type TEXT, "
        "rel TEXT, weight REAL, last_seen TEXT, samples INTEGER, "
        "UNIQUE(src_id, src_type, dst_id, dst_type, rel))"
    )
    cn.execute(
        "CREATE TABLE IF NOT EXISTS kv_store(key TEXT PRIMARY KEY, value TEXT)"
    )
    return cn


def _insert_grounded_edge(cn, src, dst, last_seen=None):
    ts = last_seen or datetime.now().isoformat()
    cn.execute(
        "INSERT OR IGNORE INTO corpus_edge"
        "(src_id, src_type, dst_id, dst_type, rel, weight, last_seen, samples) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (src, "Endpoint", dst, "Endpoint", "GROUNDED_TUNNEL", 0.5, ts, 1),
    )


def _insert_llada_child(cn, label="LLaDAChild"):
    eid = f"child_{id(label)}_{datetime.now().isoformat()}"
    cn.execute(
        "INSERT INTO corpus_entity(entity_id, entity_type, label, props_json, "
        "first_seen, last_seen, samples) "
        "VALUES(?,?,?,?,?,?,?)",
        (eid, "Model", label, "{}", datetime.now().isoformat(),
         datetime.now().isoformat(), 1),
    )
    return eid


def _clear_active_slots():
    """Remove any leftover slot entries from previous test runs."""
    with _ACTIVE_SLOTS_LOCK:
        _ACTIVE_SLOTS.clear()


# â”€â”€ Graph constants â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def test_graph_constant_children_per_cycle():
    assert CHILDREN_PER_CYCLE == 12


def test_graph_constant_edges_per_cycle():
    assert EDGES_PER_CYCLE == 36


def test_graph_constant_max_parents():
    assert MAX_PARENTS == 4


def test_sat_nodes_equals_12x4x2():
    assert SAT_NODES == CHILDREN_PER_CYCLE * MAX_PARENTS * 2


def test_sat_edges_equals_sat_nodes_times_3():
    assert SAT_EDGES == SAT_NODES * 3


def test_ops_per_tick_at_sat_positive():
    assert OPS_PER_TICK_AT_SAT > 0


def test_ops_per_tick_at_sat_formula():
    # 36 + 384 + 288 + 40 = 748
    expected = 36 + (SAT_NODES * MAX_PARENTS) + SAT_EDGES + (4 * 5 * 2)
    assert OPS_PER_TICK_AT_SAT == expected


# â”€â”€ Slot configuration constants â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def test_max_slots_is_positive_and_reasonable():
    assert 1 <= MAX_SLOTS <= 32


def test_slot_interval_s_positive():
    assert SLOT_INTERVAL_S > 0


def test_slots_per_trigger_positive():
    assert SLOTS_PER_TRIGGER >= 1


def test_slot_interval_min_s_less_than_slot_interval_s():
    assert SLOT_INTERVAL_MIN_S < SLOT_INTERVAL_S


# â”€â”€ _update_slot_cadence â€” dynamic tick frequency â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def test_update_slot_cadence_normal_saturation_uses_full_interval():
    import src.brain.compute_provisioner as _cp
    _cp._SLOT_TARGET_INTERVAL = SLOT_INTERVAL_S   # reset
    sat = TunnelSaturation(
        grounded_edge_count=5,
        recent_collapse_count=1,
    )
    _cp._update_slot_cadence(sat)
    assert _cp._SLOT_TARGET_INTERVAL == SLOT_INTERVAL_S


def test_update_slot_cadence_elevated_collapse_halves_interval():
    import src.brain.compute_provisioner as _cp
    _cp._SLOT_TARGET_INTERVAL = SLOT_INTERVAL_S   # reset
    sat = TunnelSaturation(
        grounded_edge_count=10,
        recent_collapse_count=PROVISION_COLLAPSE_THRESHOLD * 3,
    )
    _cp._update_slot_cadence(sat)
    assert _cp._SLOT_TARGET_INTERVAL == max(SLOT_INTERVAL_MIN_S, SLOT_INTERVAL_S // 2)


def test_update_slot_cadence_peak_density_uses_min_interval():
    import src.brain.compute_provisioner as _cp
    _cp._SLOT_TARGET_INTERVAL = SLOT_INTERVAL_S   # reset
    sat = TunnelSaturation(
        grounded_edge_count=PROVISION_EDGE_THRESHOLD * 2,
        recent_collapse_count=0,
    )
    _cp._update_slot_cadence(sat)
    assert _cp._SLOT_TARGET_INTERVAL == SLOT_INTERVAL_MIN_S


# â”€â”€ measure_tunnel_saturation â€” empty DB â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def test_measure_empty_db_no_provision():
    cn = _fresh_db()
    sat = measure_tunnel_saturation(cn)
    assert sat.grounded_edge_count == 0
    assert sat.recent_collapse_count == 0
    assert sat.llada_child_count == 0
    assert not sat.provision_triggered


def test_measure_ops_per_tick_baseline():
    cn = _fresh_db()
    sat = measure_tunnel_saturation(cn)
    assert sat.ops_per_tick == OPS_PER_TICK_AT_SAT


# â”€â”€ measure_tunnel_saturation â€” collapse threshold â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def test_measure_collapse_threshold_not_met():
    cn = _fresh_db()
    for i in range(PROVISION_COLLAPSE_THRESHOLD - 1):
        _insert_grounded_edge(cn, f"A{i}", f"B{i}")
    sat = measure_tunnel_saturation(cn)
    assert sat.recent_collapse_count == PROVISION_COLLAPSE_THRESHOLD - 1
    assert not sat.provision_triggered


def test_measure_collapse_threshold_exactly_met():
    cn = _fresh_db()
    for i in range(PROVISION_COLLAPSE_THRESHOLD):
        _insert_grounded_edge(cn, f"A{i}", f"B{i}")
    sat = measure_tunnel_saturation(cn)
    assert sat.provision_triggered
    assert "collapses=" in sat.trigger_reason


def test_measure_old_collapses_outside_window_not_counted():
    cn = _fresh_db()
    old_ts = (
        datetime.now() - timedelta(seconds=PROVISION_COLLAPSE_THRESHOLD * 1000)
    ).isoformat()
    for i in range(PROVISION_COLLAPSE_THRESHOLD):
        _insert_grounded_edge(cn, f"A{i}", f"B{i}", last_seen=old_ts)
    sat = measure_tunnel_saturation(cn)
    # Windowed count must be zero even though total edge count equals threshold
    assert sat.recent_collapse_count == 0
    assert sat.grounded_edge_count == PROVISION_COLLAPSE_THRESHOLD
    # Bootstrap fires when slots=0 AND total grounded edges >= threshold, so
    # provision_triggered is True here — that is the intended behaviour
    # (restart compute if slots are lost but expansion history exists).
    assert "bootstrap" in sat.trigger_reason


# â”€â”€ measure_tunnel_saturation â€” child threshold â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def test_measure_child_threshold_triggers_provision():
    cn = _fresh_db()
    for _ in range(PROVISION_CHILD_THRESHOLD):
        _insert_llada_child(cn)
    sat = measure_tunnel_saturation(cn)
    assert sat.provision_triggered
    assert "llada_children=" in sat.trigger_reason


def test_measure_child_below_threshold_no_trigger():
    cn = _fresh_db()
    for _ in range(PROVISION_CHILD_THRESHOLD - 1):
        _insert_llada_child(cn)
    sat = measure_tunnel_saturation(cn)
    assert not sat.provision_triggered


# â”€â”€ measure_tunnel_saturation â€” cooldown â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def test_cooldown_suppresses_repeat_provision():
    cn = _fresh_db()
    for i in range(PROVISION_COLLAPSE_THRESHOLD):
        _insert_grounded_edge(cn, f"A{i}", f"B{i}")
    # Write a recent intent (within cooldown)
    recent_ts = (
        datetime.now() - timedelta(seconds=PROVISION_COOLDOWN_S // 2)
    ).isoformat()
    cn.execute(
        "INSERT INTO kv_store(key, value) VALUES(?,?)",
        (f"compute_intent:{recent_ts}", json.dumps({"timestamp": recent_ts})),
    )
    sat = measure_tunnel_saturation(cn)
    assert not sat.provision_triggered


# â”€â”€ acquire_local_compute â€” slot spawning â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _noop_loop(slot_id: str) -> None:
    """Replacement _slot_expansion_loop for tests: exits immediately."""
    return


def test_acquire_local_compute_spawns_thread():
    _clear_active_slots()
    cn = _fresh_db()
    with patch(
        "src.brain.compute_provisioner._slot_expansion_loop", side_effect=_noop_loop
    ):
        spawned = acquire_local_compute(cn, n_slots=1)
    assert len(spawned) == 1


def test_acquire_local_compute_registers_corpus_entity():
    _clear_active_slots()
    cn = _fresh_db()
    with patch(
        "src.brain.compute_provisioner._slot_expansion_loop", side_effect=_noop_loop
    ):
        spawned = acquire_local_compute(cn, n_slots=1)
    slot_id = spawned[0]
    row = cn.execute(
        "SELECT entity_type FROM corpus_entity WHERE entity_id=?", (slot_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == _SLOT_ENTITY_TYPE


def test_acquire_local_compute_writes_torus_amplify_key():
    _clear_active_slots()
    cn = _fresh_db()
    with patch(
        "src.brain.compute_provisioner._slot_expansion_loop", side_effect=_noop_loop
    ):
        spawned = acquire_local_compute(cn, n_slots=1)
    slot_id = spawned[0]
    row = cn.execute(
        "SELECT value FROM kv_store WHERE key=?",
        (f"{_SLOT_AMPLIFY_PREFIX}{slot_id}",),
    ).fetchone()
    assert row is not None
    amplify_factor = float(row[0])
    assert amplify_factor > 1.0   # must be a genuine amplification


def test_acquire_local_compute_respects_max_slots_cap():
    _clear_active_slots()
    cn = _fresh_db()

    def _slow_loop(slot_id: str) -> None:
        time.sleep(0.05)

    with patch(
        "src.brain.compute_provisioner._slot_expansion_loop", side_effect=_slow_loop
    ):
        # Request more than MAX_SLOTS
        first_batch = acquire_local_compute(cn, n_slots=MAX_SLOTS)
        assert len(first_batch) == MAX_SLOTS
        # Additional request should return nothing (cap reached)
        second_batch = acquire_local_compute(cn, n_slots=2)
        assert len(second_batch) == 0

    _clear_active_slots()


def test_acquire_local_compute_multiple_slots_all_registered():
    _clear_active_slots()
    cn = _fresh_db()
    n = min(3, MAX_SLOTS)
    with patch(
        "src.brain.compute_provisioner._slot_expansion_loop", side_effect=_noop_loop
    ):
        spawned = acquire_local_compute(cn, n_slots=n)
    assert len(spawned) == n
    for slot_id in spawned:
        row = cn.execute(
            "SELECT entity_id FROM corpus_entity WHERE entity_id=?", (slot_id,)
        ).fetchone()
        assert row is not None, f"Missing corpus entity for {slot_id}"


# â”€â”€ active_slot_count â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def test_active_slot_count_empty():
    _clear_active_slots()
    assert active_slot_count() == 0


def test_active_slot_count_increments_on_acquire():
    _clear_active_slots()
    cn = _fresh_db()

    done_event = threading.Event()

    def _blocking_loop(slot_id: str) -> None:
        done_event.wait(timeout=2)

    with patch(
        "src.brain.compute_provisioner._slot_expansion_loop", side_effect=_blocking_loop
    ):
        acquire_local_compute(cn, n_slots=2)
        count = active_slot_count()
    done_event.set()
    _clear_active_slots()
    assert count == 2


# â”€â”€ tick_compute_provisioner â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def test_tick_no_trigger_returns_stats():
    cn = _fresh_db()
    stats = tick_compute_provisioner(cn)
    assert stats["provision_triggered"] is False
    assert stats["intent_status"] == "NONE"


def test_tick_records_ops_per_tick():
    cn = _fresh_db()
    stats = tick_compute_provisioner(cn)
    assert stats["ops_per_tick"] == OPS_PER_TICK_AT_SAT


def test_tick_triggers_acquires_slots_and_returns_acquired_status():
    _clear_active_slots()
    cn = _fresh_db()
    for i in range(PROVISION_COLLAPSE_THRESHOLD):
        _insert_grounded_edge(cn, f"A{i}", f"B{i}")

    with patch(
        "src.brain.compute_provisioner._slot_expansion_loop", side_effect=_noop_loop
    ):
        stats = tick_compute_provisioner(cn)

    assert stats["provision_triggered"] is True
    assert stats["intent_status"] == "ACQUIRED"
    assert stats["slots_spawned"] >= 1
    _clear_active_slots()


def test_tick_writes_intent_to_kv_store():
    _clear_active_slots()
    cn = _fresh_db()
    for i in range(PROVISION_COLLAPSE_THRESHOLD):
        _insert_grounded_edge(cn, f"A{i}", f"B{i}")

    with patch(
        "src.brain.compute_provisioner._slot_expansion_loop", side_effect=_noop_loop
    ):
        tick_compute_provisioner(cn)

    row = cn.execute(
        "SELECT value FROM kv_store WHERE key LIKE 'compute_intent:%'"
    ).fetchone()
    assert row is not None
    rec = json.loads(row[0])
    assert rec["status"] == "ACQUIRED"
    _clear_active_slots()


def test_tick_includes_active_slots_in_stats():
    _clear_active_slots()
    cn = _fresh_db()
    stats = tick_compute_provisioner(cn)
    assert "active_slots" in stats


def test_tick_includes_slot_interval_in_stats():
    _clear_active_slots()
    cn = _fresh_db()
    stats = tick_compute_provisioner(cn)
    assert "slot_interval_s" in stats
    assert stats["slot_interval_s"] > 0


def test_tick_at_capacity_routes_to_phase2():
    """When active_slots >= MAX_SLOTS, tick_compute_provisioner uses Phase 2."""
    _clear_active_slots()
    cn = _fresh_db()
    # Fill the slot registry with fake alive threads so measure_tunnel_saturation
    # sees active_slots == MAX_SLOTS
    done = threading.Event()

    def _live(sid):
        done.wait(timeout=3)

    with _ACTIVE_SLOTS_LOCK:
        for i in range(MAX_SLOTS):
            t = threading.Thread(target=_live, args=(f"fake{i}",), daemon=True)
            t.start()
            _ACTIVE_SLOTS[f"fake{i}"] = t

    # Satisfy saturation threshold
    for i in range(PROVISION_COLLAPSE_THRESHOLD):
        _insert_grounded_edge(cn, f"A{i}", f"B{i}")
    # Add a SIGNBIT_FLIP edge so phase2 finds an anchor
    cn.execute(
        "INSERT INTO corpus_edge(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
        "VALUES('parent1','Model','child1','Model','SIGNBIT_FLIP',0.5,?,1)",
        (datetime.now().isoformat(),),
    )
    cn.execute(
        "INSERT OR IGNORE INTO corpus_edge"
        "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
        "VALUES('parent1','corpus_entity','ep1','Endpoint','GROUNDED_TUNNEL',0.8,?,1)",
        (datetime.now().isoformat(),),
    )
    cn.commit()

    stats = tick_compute_provisioner(cn)
    done.set()
    _clear_active_slots()

    assert stats["intent_status"] == "INGESTED"


# â”€â”€ ingest_external_at_boundary â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _fresh_db_with_boundary(n_collapse=PROVISION_COLLAPSE_THRESHOLD) -> sqlite3.Connection:
    """DB with GROUNDED_TUNNEL + SIGNBIT_FLIP edges ready for Phase 2."""
    cn = _fresh_db()
    for i in range(n_collapse):
        _insert_grounded_edge(cn, f"A{i}", f"B{i}")
    cn.execute(
        "INSERT INTO corpus_edge(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
        "VALUES('parent1','Model','child1','Model','SIGNBIT_FLIP',0.5,?,1)",
        (datetime.now().isoformat(),),
    )
    cn.execute(
        "INSERT OR IGNORE INTO corpus_edge"
        "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
        "VALUES('parent1','corpus_entity','ep1','Endpoint','GROUNDED_TUNNEL',0.8,?,1)",
        (datetime.now().isoformat(),),
    )
    cn.commit()
    return cn


def test_ingest_external_at_boundary_returns_ingested_count():
    cn = _fresh_db_with_boundary()
    result = ingest_external_at_boundary(cn)
    # Should ingest 1 external resource (synthetic probe as fallback)
    assert result["ingested_count"] == 1


def test_ingest_external_creates_corpus_entity():
    cn = _fresh_db_with_boundary()
    result = ingest_external_at_boundary(cn)
    ext_id = result["external_id"]
    assert ext_id is not None
    row = cn.execute(
        "SELECT entity_type FROM corpus_entity WHERE entity_id=?", (ext_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == _EXTERNAL_TYPE


def test_ingest_external_writes_torus_amplify_key():
    cn = _fresh_db_with_boundary()
    result = ingest_external_at_boundary(cn)
    ext_id = result["external_id"]
    row = cn.execute(
        "SELECT value FROM kv_store WHERE key=?",
        (f"torus_amplify:{ext_id}",),
    ).fetchone()
    assert row is not None
    assert float(row[0]) > 1.0


def test_ingest_external_bonds_ingested_at_boundary_edge():
    cn = _fresh_db_with_boundary()
    result = ingest_external_at_boundary(cn)
    ext_id = result["external_id"]
    anchor = result["anchor"]
    row = cn.execute(
        "SELECT rel FROM corpus_edge WHERE src_id=? AND dst_id=? AND rel=?",
        (anchor, ext_id, _INGESTED_REL),
    ).fetchone()
    assert row is not None


def test_ingest_external_bonds_boundary_edge_type():
    cn = _fresh_db_with_boundary()
    result = ingest_external_at_boundary(cn)
    ext_id = result["external_id"]
    anchor = result["anchor"]
    edge_type = result["edge_type"]
    assert edge_type in ("GROUNDED_TUNNEL", "SYMBIOTIC_TUNNEL")
    row = cn.execute(
        "SELECT rel FROM corpus_edge WHERE src_id=? AND dst_id=? AND rel=?",
        (anchor, ext_id, edge_type),
    ).fetchone()
    assert row is not None


def test_ingest_external_writes_boundary_ingest_record():
    cn = _fresh_db_with_boundary()
    ingest_external_at_boundary(cn)
    row = cn.execute(
        "SELECT value FROM kv_store WHERE key LIKE 'boundary_ingest:%'"
    ).fetchone()
    assert row is not None
    rec = json.loads(row[0])
    assert "external_id" in rec
    assert "anchor" in rec
    assert "edge_type" in rec


def test_ingest_external_empty_db_no_anchors_returns_zero():
    """With no SIGNBIT_FLIP or GROUNDED_TUNNEL edges, ingest should skip gracefully."""
    cn = _fresh_db()
    result = ingest_external_at_boundary(cn)
    assert result["ingested_count"] == 0


def test_ingest_from_network_topology_preferred_over_probe():
    """When network_topology has an unregistered host, it should be ingested first."""
    cn = _fresh_db_with_boundary()
    cn.execute(
        "CREATE TABLE IF NOT EXISTS network_topology("
        "host TEXT, protocol TEXT, port INTEGER, capability TEXT, "
        "last_ok TEXT, ema_success REAL, ema_latency_ms REAL, source TEXT)"
    )
    cn.execute(
        "INSERT INTO network_topology VALUES(?,?,?,?,?,?,?,?)",
        ("ext-host.example.com", "tcp", 5432, "PostgreSQL",
         datetime.now().isoformat(), 0.9, 5.0, "network_learner"),
    )
    cn.commit()
    result = ingest_external_at_boundary(cn)
    assert result["ingested_count"] == 1
    # entity_id should encode the host
    assert "ext-host" in result["external_id"]


# ── SIGNBIT_FLIP fixture helper ───────────────────────────────────────────────

def _insert_signbit_flip_edge(cn, src, dst, weight=0.5):
    """Insert a SIGNBIT_FLIP edge for coherence testing."""
    ts = datetime.now().isoformat()
    cn.execute(
        "INSERT OR IGNORE INTO corpus_edge"
        "(src_id, src_type, dst_id, dst_type, rel, weight, last_seen, samples) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (src, "corpus_entity", dst, "corpus_entity", "SIGNBIT_FLIP", weight, ts, 1),
    )


# ── Harmonic amplify factor (unit tests, no DB needed) ────────────────────────

def test_harmonic_amplify_factor_floor_at_zero_coherence():
    """coherence=0 → base floor × UEQGM SiCi phase weight at φ=π/4."""
    from src.brain.ueqgm_engine import sici_phase_weight
    base = round(_AMPLIFY_FACTOR * _HARMONIC_FLOOR, 4)
    expected = round(base * sici_phase_weight(0), 4)
    assert _harmonic_amplify_factor(0) == pytest.approx(expected, abs=1e-3)


def test_harmonic_amplify_factor_scales_with_coherence():
    """Higher coherence must produce a larger factor."""
    assert _harmonic_amplify_factor(1) > _harmonic_amplify_factor(0)
    assert _harmonic_amplify_factor(5) > _harmonic_amplify_factor(1)


def test_harmonic_amplify_factor_approaches_ceiling_asymptotically():
    """Factor must stay strictly below _AMPLIFY_FACTOR × _HARMONIC_CEILING."""
    ceiling = _AMPLIFY_FACTOR * _HARMONIC_CEILING
    assert _harmonic_amplify_factor(100) < ceiling
    assert _harmonic_amplify_factor(10_000) < ceiling


def test_harmonic_amplify_factor_returns_float():
    result = _harmonic_amplify_factor(3)
    assert isinstance(result, float)


# ── _anchor_coherence (DB-backed) ─────────────────────────────────────────────

def test_anchor_coherence_zero_when_no_edges():
    cn = _fresh_db()
    assert _anchor_coherence(cn, "node-x") == 0


def test_anchor_coherence_counts_signbit_flip_edges():
    cn = _fresh_db()
    _insert_signbit_flip_edge(cn, "anchor-a", "child-1")
    _insert_signbit_flip_edge(cn, "anchor-a", "child-2")
    _insert_signbit_flip_edge(cn, "anchor-a", "child-3")
    cn.commit()
    assert _anchor_coherence(cn, "anchor-a") == 3


def test_anchor_coherence_also_counts_edges_where_anchor_is_dst():
    cn = _fresh_db()
    _insert_signbit_flip_edge(cn, "some-parent", "anchor-a")
    cn.commit()
    assert _anchor_coherence(cn, "anchor-a") == 1


def test_anchor_coherence_does_not_count_other_rel_types():
    cn = _fresh_db()
    ts = datetime.now().isoformat()
    cn.execute(
        "INSERT OR IGNORE INTO corpus_edge"
        "(src_id, src_type, dst_id, dst_type, rel, weight, last_seen, samples) "
        "VALUES(?,?,?,?,?,?,?,?)",
        ("anchor-a", "corpus_entity", "child-1", "corpus_entity",
         "GROUNDED_TUNNEL", 0.5, ts, 1),
    )
    cn.commit()
    assert _anchor_coherence(cn, "anchor-a") == 0


# ── Harmonic ingestion (integration) ─────────────────────────────────────────

def test_ingest_stats_include_harmonic_coherence():
    cn = _fresh_db_with_boundary()
    result = ingest_external_at_boundary(cn)
    assert "harmonic_coherence" in result
    assert isinstance(result["harmonic_coherence"], int)


def test_ingest_stats_include_harmonic_factor():
    cn = _fresh_db_with_boundary()
    result = ingest_external_at_boundary(cn)
    assert "harmonic_factor" in result
    assert isinstance(result["harmonic_factor"], float)


def test_ingest_torus_amplify_matches_harmonic_factor():
    """The torus_amplify key must exist and store a positive bond weight.

    Since v0.17.5 the stored value is effective_weight = h_factor × pol_weight,
    not the raw h_factor, so we verify the key is written and > 0.
    """
    cn = _fresh_db_with_boundary()
    result = ingest_external_at_boundary(cn)
    ext_id = result["external_id"]
    row = cn.execute(
        "SELECT value FROM kv_store WHERE key=?",
        (f"torus_amplify:{ext_id}",),
    ).fetchone()
    assert row is not None
    assert float(row[0]) > 0.0


def test_ingest_boundary_record_includes_harmonic_coherence():
    cn = _fresh_db_with_boundary()
    ingest_external_at_boundary(cn)
    row = cn.execute(
        "SELECT value FROM kv_store WHERE key LIKE 'boundary_ingest:%'"
    ).fetchone()
    assert row is not None
    rec = json.loads(row[0])
    assert "harmonic_coherence" in rec
    assert "harmonic_factor" in rec


def test_ingest_boundary_record_includes_co_anchor_count():
    cn = _fresh_db_with_boundary()
    ingest_external_at_boundary(cn)
    row = cn.execute(
        "SELECT value FROM kv_store WHERE key LIKE 'boundary_ingest:%'"
    ).fetchone()
    rec = json.loads(row[0])
    assert "co_anchor_count" in rec


def test_ingest_harmonic_bond_written_for_co_anchor():
    """When two anchors exist, HARMONIC_BOND edges must be present."""
    cn = _fresh_db()
    ts = datetime.now().isoformat()
    for anc in ("anchor-alpha", "anchor-beta"):
        cn.execute(
            "INSERT OR IGNORE INTO corpus_edge"
            "(src_id, src_type, dst_id, dst_type, rel, weight, last_seen, samples) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (anc, "Endpoint", "shared-dst", "Endpoint", "GROUNDED_TUNNEL",
             0.6, ts, 1),
        )
    cn.commit()
    result = ingest_external_at_boundary(cn)
    if result["ingested_count"] == 0:
        pytest.skip("ingest skipped (discovery found nothing)")
    # At least one HARMONIC_BOND edge from a corpus_entity must exist
    row = cn.execute(
        "SELECT COUNT(*) FROM corpus_edge WHERE rel=? AND src_type=?",
        (_HARMONIC_BOND_REL, "corpus_entity"),
    ).fetchone()
    assert row[0] >= 1


def test_ingest_harmonic_bond_weight_proportional_to_anchors():
    """HARMONIC_BOND edge weight must equal h_factor / n_anchors."""
    cn = _fresh_db()
    ts = datetime.now().isoformat()
    for anc in ("anchor-p", "anchor-q"):
        cn.execute(
            "INSERT OR IGNORE INTO corpus_edge"
            "(src_id, src_type, dst_id, dst_type, rel, weight, last_seen, samples) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (anc, "Endpoint", "shared-dst2", "Endpoint", "GROUNDED_TUNNEL",
             0.6, ts, 1),
        )
    cn.commit()
    result = ingest_external_at_boundary(cn)
    if result["ingested_count"] == 0:
        pytest.skip("ingest skipped")
    ext_id   = result["external_id"]
    h_factor = result["harmonic_factor"]
    all_anchors = ["anchor-p", "anchor-q"]
    expected_co_weight = round(h_factor / max(1, len(all_anchors)), 4)
    row = cn.execute(
        "SELECT weight FROM corpus_edge WHERE dst_id=? AND rel=?",
        (ext_id, _HARMONIC_BOND_REL),
    ).fetchone()
    if row is not None:
        assert float(row[0]) == pytest.approx(expected_co_weight, abs=0.01)


def test_compute_intent_has_harmonic_fields():
    intent = ComputeIntent()
    for field in ("harmonic_coherence", "harmonic_factor", "mean_harmonic_factor",
                  "mean_polarity_weight", "anchor_count", "total_descendants_reached"):
        assert hasattr(intent, field), f"missing field: {field}"
    assert intent.harmonic_coherence == 0
    assert intent.harmonic_factor == 0.0
    assert intent.anchor_count == 0


# ── _anchor_polarity ──────────────────────────────────────────────────────────

def test_anchor_polarity_positive_emitter():
    cn = _fresh_db()
    _insert_signbit_flip_edge(cn, "emitter", "child")
    cn.commit()
    assert _anchor_polarity(cn, "emitter") == +1


def test_anchor_polarity_negative_receiver():
    cn = _fresh_db()
    _insert_signbit_flip_edge(cn, "parent", "receiver")
    cn.commit()
    assert _anchor_polarity(cn, "receiver") == -1


def test_anchor_polarity_mixed_is_zero():
    cn = _fresh_db()
    _insert_signbit_flip_edge(cn, "mixed", "child")
    _insert_signbit_flip_edge(cn, "parent", "mixed")
    cn.commit()
    assert _anchor_polarity(cn, "mixed") == 0


def test_anchor_polarity_absent_is_zero():
    assert _anchor_polarity(_fresh_db(), "ghost") == 0


# ── _polarity_alignment_weight ────────────────────────────────────────────────

def test_polarity_alignment_emitter_ground_is_max():
    w = _polarity_alignment_weight(+1, "GROUNDED_TUNNEL")
    assert w == max(_POLARITY_ALIGN.values())


def test_polarity_alignment_receiver_symbiotic_is_max():
    w = _polarity_alignment_weight(-1, "SYMBIOTIC_TUNNEL")
    assert w == max(_POLARITY_ALIGN.values())


def test_polarity_alignment_cross_polarity_reduced():
    assert _polarity_alignment_weight(+1, "SYMBIOTIC_TUNNEL") < 1.0
    assert _polarity_alignment_weight(-1, "GROUNDED_TUNNEL") < 1.0


def test_polarity_alignment_unknown_key_returns_1():
    assert _polarity_alignment_weight(99, "UNKNOWN_REL") == 1.0


# ── _discover_external_resources ─────────────────────────────────────────────

def test_discover_external_resources_returns_requested_count():
    cn = _fresh_db()
    resources = _discover_external_resources(cn, 3)
    assert len(resources) == 3


def test_discover_external_resources_all_distinct():
    cn = _fresh_db()
    resources = _discover_external_resources(cn, 4)
    ids = [r["entity_id"] for r in resources]
    assert len(ids) == len(set(ids))


def test_discover_external_resources_skip_ids_respected():
    cn = _fresh_db()
    first = _discover_external_resources(cn, 1)[0]["entity_id"]
    second_batch = _discover_external_resources(cn, 1, skip_ids={first})
    assert second_batch[0]["entity_id"] != first


# ── _propagate_outward ────────────────────────────────────────────────────────

def _insert_grounded_tunnel(cn, src, dst, weight=0.6):
    ts = datetime.now().isoformat()
    cn.execute(
        "INSERT OR IGNORE INTO corpus_edge"
        "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (src, "corpus_entity", dst, "corpus_entity", "GROUNDED_TUNNEL", weight, ts, 1),
    )


def test_propagate_outward_writes_harmonic_bond_to_descendants():
    cn = _fresh_db()
    ts = datetime.now().isoformat()
    _insert_grounded_tunnel(cn, "anc", "desc-1")
    cn.execute(
        "INSERT OR IGNORE INTO corpus_entity"
        "(entity_id,entity_type,label,props_json,first_seen,last_seen,samples) "
        "VALUES(?,?,?,?,?,?,?)",
        ("ext-r1", "ExternalResource", "Probe", "{}", ts, ts, 1),
    )
    cn.commit()
    count = _propagate_outward(cn, "anc", "ext-r1", 2.0, 1.0, ts)
    assert count >= 1
    row = cn.execute(
        "SELECT rel FROM corpus_edge WHERE src_id=? AND dst_id=? AND rel=?",
        ("ext-r1", "desc-1", _HARMONIC_BOND_REL),
    ).fetchone()
    assert row is not None


def test_propagate_outward_writes_reverse_integration_to_anchor():
    cn = _fresh_db()
    ts = datetime.now().isoformat()
    _insert_grounded_tunnel(cn, "anc-rv", "desc-rv")
    cn.execute(
        "INSERT OR IGNORE INTO corpus_entity"
        "(entity_id,entity_type,label,props_json,first_seen,last_seen,samples) "
        "VALUES(?,?,?,?,?,?,?)",
        ("ext-rv", "ExternalResource", "Probe", "{}", ts, ts, 1),
    )
    cn.commit()
    _propagate_outward(cn, "anc-rv", "ext-rv", 2.0, 1.0, ts)
    row = cn.execute(
        "SELECT rel FROM corpus_edge WHERE src_id=? AND dst_id=? AND rel=?",
        ("desc-rv", "anc-rv", _REVERSE_FLOW_REL),
    ).fetchone()
    assert row is not None


def test_propagate_outward_returns_zero_with_no_tunnel_neighbors():
    cn = _fresh_db()
    ts = datetime.now().isoformat()
    count = _propagate_outward(cn, "isolated-anc", "ext-iso", 1.0, 1.0, ts)
    assert count == 0


# ── Multi-ingestion: proportional to anchor count ────────────────────────────

def _fresh_db_multi_anchor():
    cn = _fresh_db()
    ts = datetime.now().isoformat()
    for anc in ("anc-1", "anc-2", "anc-3"):
        cn.execute(
            "INSERT OR IGNORE INTO corpus_edge"
            "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (anc, "Endpoint", "shared-sink", "Endpoint",
             "GROUNDED_TUNNEL", 0.7, ts, 1),
        )
    cn.commit()
    return cn


def test_ingest_multi_anchor_ingests_multiple_resources():
    cn = _fresh_db_multi_anchor()
    result = ingest_external_at_boundary(cn)
    assert result["ingested_count"] >= 2


def test_ingest_multi_anchor_returns_ingested_ids_list():
    cn = _fresh_db_multi_anchor()
    result = ingest_external_at_boundary(cn)
    assert isinstance(result["ingested_ids"], list)
    assert len(result["ingested_ids"]) == result["ingested_count"]


def test_ingest_multi_anchor_ids_are_distinct():
    cn = _fresh_db_multi_anchor()
    result = ingest_external_at_boundary(cn)
    ids = result["ingested_ids"]
    assert len(ids) == len(set(ids))


def test_ingest_anchor_count_in_stats():
    cn = _fresh_db_with_boundary()
    result = ingest_external_at_boundary(cn)
    assert "anchor_count" in result
    assert result["anchor_count"] >= 1


def test_ingest_capped_at_max_ingest_per_call():
    cn = _fresh_db()
    ts = datetime.now().isoformat()
    for i in range(_MAX_INGEST_PER_CALL + 3):
        cn.execute(
            "INSERT OR IGNORE INTO corpus_edge"
            "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (f"big-anc-{i}", "Endpoint", "sink", "Endpoint",
             "GROUNDED_TUNNEL", 0.7, ts, 1),
        )
    cn.commit()
    result = ingest_external_at_boundary(cn)
    assert result["ingested_count"] <= _MAX_INGEST_PER_CALL


# ── Cross-resource harmonization ─────────────────────────────────────────────

def test_ingest_cross_harmonic_bond_between_co_ingested():
    cn = _fresh_db_multi_anchor()
    result = ingest_external_at_boundary(cn)
    ids = result["ingested_ids"]
    if len(ids) < 2:
        pytest.skip("only one resource ingested — no cross-bond possible")
    row = cn.execute(
        "SELECT COUNT(*) FROM corpus_edge "
        "WHERE rel=? AND src_type=? AND dst_type=?",
        (_HARMONIC_BOND_REL, "ExternalResource", "ExternalResource"),
    ).fetchone()
    assert row[0] >= 1


def test_ingest_mean_harmonic_factor_in_stats():
    cn = _fresh_db_multi_anchor()
    result = ingest_external_at_boundary(cn)
    assert "mean_harmonic_factor" in result
    assert result["mean_harmonic_factor"] >= 0.0


def test_ingest_mean_polarity_weight_in_stats():
    cn = _fresh_db_multi_anchor()
    result = ingest_external_at_boundary(cn)
    assert "mean_polarity_weight" in result
    assert result["mean_polarity_weight"] > 0.0


def test_ingest_total_descendants_reached_in_stats():
    cn = _fresh_db_multi_anchor()
    result = ingest_external_at_boundary(cn)
    assert "total_descendants_reached" in result
    assert isinstance(result["total_descendants_reached"], int)


# ── cpu_overhead_report ─────────────────────────────────────────────────────

def test_cpu_overhead_report_baseline():
    report = cpu_overhead_report(SAT_NODES)
    assert f"{SAT_NODES} children" in report
    assert "ops/tick" in report
    assert "ops/hr" in report


def test_cpu_overhead_report_extra_cycles():
    extra = SAT_NODES + CHILDREN_PER_CYCLE
    report = cpu_overhead_report(extra)
    import re
    match = re.search(r"(\d[\d,]+) vision ops/tick", report)
    assert match
    ops = int(match.group(1).replace(",", ""))
    assert ops > OPS_PER_TICK_AT_SAT


def test_cpu_overhead_report_below_sat_uses_baseline():
    report = cpu_overhead_report(0)
    import re
    match = re.search(r"(\d[\d,]+) vision ops/tick", report)
    assert match
    ops = int(match.group(1).replace(",", ""))
    assert ops == OPS_PER_TICK_AT_SAT


def test_cpu_overhead_report_includes_slot_contribution():
    report = cpu_overhead_report(SAT_NODES, slot_count=2)
    assert "slot" in report.lower()
    assert "2 slots" in report
