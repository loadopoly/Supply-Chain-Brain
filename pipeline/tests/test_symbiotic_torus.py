"""Unit tests for src.brain.symbiotic_tunnel and src.brain.torus_touch.

Self-contained: every test creates its own in-memory sqlite schema, no
LLM/network dependencies (the autouse stub_llm fixture in conftest.py
already neuters dispatch_parallel).
"""
from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime

import numpy as np
import pytest

from src.brain.symbiotic_tunnel import (
    BayesianPoissonCentroids,
    DualFloorMirror,
    InvertedReluAdam,
    PropellerRouter,
    touch_couple,
    vision_horizontal_expand,
)
from src.brain.torus_touch import (
    TORUS_DIMS,
    CatGapField,
    TouchPressure,
    endpoint_angles,
    gap_field_summary,
    tick_torus_pressure,
    touch_couple_torus,
)


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def _make_corpus_db() -> sqlite3.Connection:
    cn = sqlite3.connect(":memory:")
    cn.executescript(
        """
        CREATE TABLE corpus_entity(
            entity_id TEXT, entity_type TEXT, label TEXT, props_json TEXT,
            first_seen TEXT, last_seen TEXT, samples INTEGER DEFAULT 1,
            PRIMARY KEY(entity_id, entity_type)
        );
        CREATE TABLE corpus_edge(
            src_id TEXT, src_type TEXT, dst_id TEXT, dst_type TEXT,
            rel TEXT, weight REAL, last_seen TEXT, samples INTEGER DEFAULT 1,
            PRIMARY KEY(src_id, src_type, dst_id, dst_type, rel)
        );
        CREATE TABLE kv_store(key TEXT PRIMARY KEY, value TEXT);
        """
    )
    return cn


def _seed_endpoints(cn: sqlite3.Connection, names: list[str]) -> None:
    now = datetime.now().isoformat()
    for n in names:
        cn.execute(
            "INSERT INTO corpus_entity VALUES(?,?,?,?,?,?,1)",
            (f"bridge:{n}", "Endpoint", n, "{}", now, now),
        )
    cn.commit()


def _seed_mesh_edges(cn: sqlite3.Connection,
                     edges: list[tuple[str, str, float]]) -> None:
    now = datetime.now().isoformat()
    for s, d, w in edges:
        cn.execute(
            "INSERT INTO corpus_edge VALUES(?,?,?,?,?,?,?,1)",
            (s, "Endpoint", d, "Endpoint", "REACHABLE", w, now),
        )
    cn.commit()


# ===========================================================================
# symbiotic_tunnel — primitives
# ===========================================================================

class TestPrimitives:
    def test_touch_couple_identity(self):
        # exp(ln(1+a) + ln(1+b)) - 1 == (1+a)(1+b) - 1
        for a, b in [(0.0, 0.0), (0.3, 0.7), (1.0, 1.0), (0.001, 0.999)]:
            assert touch_couple(a, b) == pytest.approx((1 + a) * (1 + b) - 1, abs=1e-9)

    def test_touch_couple_monotonic(self):
        assert touch_couple(0.5, 0.5) < touch_couple(0.5, 0.6)
        assert touch_couple(0.0, 0.0) == 0.0

    def test_centroids_with_prior_handles_empty_input(self):
        c, a = BayesianPoissonCentroids(k=3).fit([])
        assert c.size == 0 and a.size == 0

    def test_centroids_k_clipped_to_data(self):
        c, _ = BayesianPoissonCentroids(k=5).fit([0.5])
        assert c.size == 1
        # with α=β=1 and one sample of 0.5: λ = (0.5+1)/(1+1) = 0.75
        assert c[0] == pytest.approx(0.75, abs=1e-6)

    def test_centroids_separate_clusters(self):
        c, a = BayesianPoissonCentroids(k=2).fit([0.1, 0.12, 0.9, 0.92])
        assert c.size == 2
        # one centroid should be in the low tier, one in the high tier
        # (Poisson prior pulls both toward 1, so use a generous gap test)
        sc = sorted(float(x) for x in c)
        assert sc[1] - sc[0] >= 0.3
        # assignments must split: not all 4 in the same cluster
        assert len(set(a.tolist())) == 2

    def test_inverted_relu_adam_zeroes_negative_gradient(self):
        opt = InvertedReluAdam(lr=0.1, sgd_mix=0.0)
        # pure negative gradient → -ReLU(g) = 0 → no update from inv_relu term
        theta = np.array([0.5, 0.5])
        new = opt.step(theta, np.array([-0.3, -0.5]))
        # with sgd_mix=0 the only gradient component is inv_relu(g)=0
        # so adam update is zero-ish (ε-bounded)
        assert np.allclose(new, theta, atol=1e-6)

    def test_inverted_relu_adam_steps_on_positive_gradient(self):
        opt = InvertedReluAdam(lr=0.1)
        theta = np.array([0.5, 0.5])
        new = opt.step(theta, np.array([0.3, 0.4]))
        # positive gradient → -ReLU = -g → theta - lr * (negative momentum direction)
        # net effect: theta moves *up* (because gradient is negated then subtracted)
        assert (new > theta).all()

    def test_dual_floor_mirror_signs_inverse(self):
        upper, lower = DualFloorMirror().mirror(np.array([0.2, -0.4, 0.0]))
        assert np.allclose(upper, -lower)

    def test_dual_floor_above_floor(self):
        # element below floor must be lifted to ±floor
        x = np.array([0.001])
        upper, _ = DualFloorMirror(eps=1e-3).mirror(x)
        assert abs(upper[0]) >= 1e-3

    def test_propeller_softmax_sums_to_one(self):
        p = PropellerRouter().softmax(np.array([0.1, 0.5, 0.9]))
        assert p.sum() == pytest.approx(1.0, abs=1e-9)

    def test_propeller_skips_existing_pairs(self):
        existing = {("a", "b"), ("b", "a")}
        out = PropellerRouter().select_pairs(
            ["a", "b", "c"], [0.9, 0.5, 0.1], existing, max_pairs=10,
        )
        for s, d, _ in out:
            assert (s, d) not in existing and (d, s) not in existing


# ===========================================================================
# symbiotic_tunnel — vision_horizontal_expand
# ===========================================================================

class TestHorizontalExpansion:
    def test_no_edges_returns_empty_stats(self):
        cn = _make_corpus_db()
        stats = vision_horizontal_expand(cn)
        assert stats["edges_seen"] == 0
        assert stats["edges_added"] == 0

    def test_expansion_adds_tunnel_edges_on_sparse_chain(self):
        cn = _make_corpus_db()
        names = ["a", "b", "c", "d", "e", "f", "g", "h", "i"]
        _seed_endpoints(cn, names)
        _seed_mesh_edges(cn, [
            (f"bridge:{names[i]}", f"bridge:{names[i+1]}",
             [0.90, 0.85, 0.20, 0.70, 0.15, 0.05, 0.95, 0.80][i])
            for i in range(len(names) - 1)
        ])
        stats = vision_horizontal_expand(cn)
        assert stats["edges_seen"] == 8
        assert stats["edges_added"] > 0
        rows = cn.execute(
            "SELECT COUNT(*) FROM corpus_edge WHERE rel='SYMBIOTIC_TUNNEL'"
        ).fetchone()[0]
        assert rows == stats["edges_added"]

    def test_existing_tunnel_edges_not_duplicated(self):
        cn = _make_corpus_db()
        _seed_endpoints(cn, ["a", "b", "c", "d", "e"])
        _seed_mesh_edges(cn, [
            ("bridge:a", "bridge:b", 0.9),
            ("bridge:b", "bridge:c", 0.85),
            ("bridge:c", "bridge:d", 0.8),
            ("bridge:d", "bridge:e", 0.75),
        ])
        s1 = vision_horizontal_expand(cn)
        # second pass should add fewer (or zero) — pairs are already there
        s2 = vision_horizontal_expand(cn)
        assert s2["edges_added"] <= s1["edges_added"]

    def test_centroid_count_clipped_to_input_size(self):
        cn = _make_corpus_db()
        _seed_endpoints(cn, ["a", "b"])
        _seed_mesh_edges(cn, [("bridge:a", "bridge:b", 0.5)])
        stats = vision_horizontal_expand(cn)
        # 1 edge → centroids array has at most 1 entry, no crash
        assert len(stats["centroids"]) <= 1


# ===========================================================================
# torus_touch — manifold geometry
# ===========================================================================

class TestTorusGeometry:
    def test_dims_is_seven(self):
        assert TORUS_DIMS == 7

    def test_endpoint_angles_deterministic(self):
        a = endpoint_angles(None, "bridge:foo")
        b = endpoint_angles(None, "bridge:foo")
        assert np.allclose(a, b)
        assert a.size == TORUS_DIMS
        assert (a >= 0).all() and (a < 2 * math.pi).all()

    def test_endpoint_angles_reads_from_props(self):
        target = [0.1] * TORUS_DIMS
        a = endpoint_angles({"torus_angles": target}, "bridge:foo")
        assert np.allclose(a, target)

    def test_endpoint_angles_falls_back_on_bad_props(self):
        a = endpoint_angles({"torus_angles": [1, 2, 3]}, "bridge:foo")  # wrong size
        assert a.size == TORUS_DIMS

    def test_pmf_rows_sum_to_one(self):
        field = CatGapField(dims=TORUS_DIMS, bins=8)
        m = np.random.default_rng(0).uniform(0, 2 * math.pi, size=(20, TORUS_DIMS))
        pmf = field.histogram(m)
        for d in range(TORUS_DIMS):
            assert pmf[d].sum() == pytest.approx(1.0, abs=1e-9)

    def test_kl_uniform_is_zero(self):
        field = CatGapField(dims=TORUS_DIMS, bins=8)
        pmf = np.full((TORUS_DIMS, 8), 1.0 / 8)
        kl = field.kl_from_uniform(pmf)
        assert np.allclose(kl, 0.0, atol=1e-12)

    def test_pressure_wraps_modulo_2pi(self):
        field = CatGapField(dims=TORUS_DIMS, bins=8)
        angles = np.full((1, TORUS_DIMS), 2 * math.pi - 0.01)
        pmf = field.histogram(angles)
        new_a, _ = TouchPressure(step=1.0, jitter=0.0).apply(angles, pmf, field)
        assert ((new_a >= 0) & (new_a < 2 * math.pi)).all()

    def test_couple_torus_near_far(self):
        zero = np.zeros(TORUS_DIMS)
        anti = np.full(TORUS_DIMS, math.pi)
        near = touch_couple_torus(zero, zero)
        far = touch_couple_torus(zero, anti)
        assert near > far
        assert far < 0.1            # antipodal must collapse
        assert near > 1.0           # co-located endpoints must amplify

    def test_couple_torus_wraps(self):
        a = np.zeros(TORUS_DIMS)
        b = np.full(TORUS_DIMS, 2 * math.pi - 0.001)   # should be ≈ 0 distance
        c = np.full(TORUS_DIMS, 0.001)
        assert touch_couple_torus(a, b) == pytest.approx(touch_couple_torus(a, c),
                                                          rel=1e-3)


# ===========================================================================
# torus_touch — tick_torus_pressure DB integration
# ===========================================================================

class TestTorusTick:
    def test_tick_returns_zero_with_one_endpoint(self):
        cn = _make_corpus_db()
        _seed_endpoints(cn, ["a"])
        d = tick_torus_pressure(cn)
        assert d["endpoints"] == 1
        assert d["moved"] == 0

    def test_tick_persists_torus_angles(self):
        cn = _make_corpus_db()
        _seed_endpoints(cn, list("abcdefgh"))
        tick_torus_pressure(cn)
        rows = cn.execute(
            "SELECT props_json FROM corpus_entity WHERE entity_type='Endpoint'"
        ).fetchall()
        assert len(rows) == 8
        for (props_json,) in rows:
            p = json.loads(props_json)
            assert "torus_angles" in p
            assert len(p["torus_angles"]) == TORUS_DIMS
            assert "torus_gap" in p
            assert "torus_tick" in p

    def test_tick_increases_spread_over_time(self):
        cn = _make_corpus_db()
        _seed_endpoints(cn, [chr(ord("a") + i) for i in range(12)])
        spreads = []
        for _ in range(8):
            d = tick_torus_pressure(cn, step=0.25)
            spreads.append(d["spread_after"])
        # Trend-up is the contract; allow non-monotone tick-to-tick noise.
        assert max(spreads) > spreads[0]

    def test_tick_writes_velocity_kv(self):
        cn = _make_corpus_db()
        _seed_endpoints(cn, list("abcd"))
        tick_torus_pressure(cn)
        keys = [r[0] for r in cn.execute(
            "SELECT key FROM kv_store WHERE key LIKE 'torus_vel:%'"
        ).fetchall()]
        assert len(keys) > 0


# ===========================================================================
# Cross-module: tunnels respect torus geometry when angles present
# ===========================================================================

class TestTunnelManifoldCoupling:
    def test_tunnel_weight_amplified_for_close_angles(self):
        cn = _make_corpus_db()
        names = ["a", "b", "c", "d", "e", "f"]
        _seed_endpoints(cn, names)
        _seed_mesh_edges(cn, [
            (f"bridge:{names[i]}", f"bridge:{names[i+1]}", 0.9)
            for i in range(len(names) - 1)
        ])
        # Pre-place all endpoints at the same torus angle → manifold near-coupling
        same = [0.5] * TORUS_DIMS
        for n in names:
            cn.execute(
                "UPDATE corpus_entity SET props_json=? "
                "WHERE entity_id=? AND entity_type='Endpoint'",
                (json.dumps({"torus_angles": same}), f"bridge:{n}"),
            )
        cn.commit()
        stats = vision_horizontal_expand(cn)
        assert stats["edges_added"] > 0


# ===========================================================================
# grounded_tunneling — certainty-anchored expansory pathway collapser
# ===========================================================================

class TestGroundedTunneling:
    """Tests for src.brain.grounded_tunneling — statistical grounding mechanism."""

    def test_certainty_higher_for_well_sampled_high_weight_edge(self):
        from src.brain.grounded_tunneling import compute_endpoint_certainty
        cn = _make_corpus_db()
        _seed_endpoints(cn, ["a", "b", "c"])
        now = datetime.now().isoformat()
        # high-certainty: many samples + high weight
        cn.execute(
            "INSERT INTO corpus_edge VALUES(?,?,?,?,?,?,?,?)",
            ("bridge:a", "Endpoint", "bridge:b", "Endpoint",
             "REACHABLE", 0.9, now, 20),
        )
        # low-certainty: 1 sample + low weight
        cn.execute(
            "INSERT INTO corpus_edge VALUES(?,?,?,?,?,?,?,?)",
            ("bridge:b", "Endpoint", "bridge:c", "Endpoint",
             "REACHABLE", 0.1, now, 1),
        )
        cn.commit()
        c = compute_endpoint_certainty(cn)
        # bridge:a is only in the high-cert edge; bridge:c only in low-cert
        assert c["bridge:a"] > c["bridge:c"]

    def test_find_expansory_pathway_prefers_uncertain_high_gap(self):
        from src.brain.grounded_tunneling import find_expansory_pathway
        certainty = {"A": 9.0, "B": 2.0, "C": 5.0}
        adj = {"A": [("B", 0.8), ("C", 0.7)]}
        torus_gap = {"A": 0.0, "B": 1.5, "C": 0.2}
        path = find_expansory_pathway("A", certainty, adj, torus_gap)
        assert path is not None
        assert path[0] == "A"
        assert path[1] == "B"   # B: lower certainty AND higher torus gap

    def test_find_expansory_pathway_stops_when_neighbour_more_certain(self):
        from src.brain.grounded_tunneling import find_expansory_pathway
        certainty = {"A": 5.0, "B": 9.0}  # B MORE certain than A
        adj = {"A": [("B", 0.5)]}
        path = find_expansory_pathway("A", certainty, adj, {})
        assert path is None   # no expansory frontier

    def test_find_expansory_pathway_returns_none_when_isolated(self):
        from src.brain.grounded_tunneling import find_expansory_pathway
        certainty = {"A": 9.0}
        path = find_expansory_pathway("A", certainty, {}, {})
        assert path is None

    def test_resistance_record_written_with_all_amplify_keys(self):
        from src.brain.grounded_tunneling import (
            _write_resistance, _resist_key, _amplify_key, _ensure_kv_store,
        )
        cn = _make_corpus_db()
        _ensure_kv_store(cn)
        path = ["A", "B", "C"]
        _write_resistance(cn, path, "A", duration_s=300)
        cn.commit()
        # resistance record
        row = cn.execute(
            "SELECT value FROM kv_store WHERE key=?",
            (_resist_key("A", "C"),),
        ).fetchone()
        assert row is not None
        rec = json.loads(row[0])
        assert rec["ground_id"] == "A"
        assert rec["path"] == path
        # amplify key for every node on the path
        for eid in path:
            arow = cn.execute(
                "SELECT value FROM kv_store WHERE key=?",
                (_amplify_key(eid),),
            ).fetchone()
            assert arow is not None
            assert float(arow[0]) > 1.0

    def test_nodal_collapse_inserts_grounded_tunnel_edge(self):
        from src.brain.grounded_tunneling import nodal_collapse
        cn = _make_corpus_db()
        _seed_endpoints(cn, ["ground", "mid", "term"])
        certainty = {
            "bridge:ground": 8.0,
            "bridge:mid":    2.0,
            "bridge:term":   1.0,
        }
        record = {
            "ground_id": "bridge:ground",
            "path": ["bridge:ground", "bridge:mid", "bridge:term"],
        }
        ok = nodal_collapse(
            cn, "bridge:ground", "bridge:term", record, certainty, {},
        )
        assert ok is True
        row = cn.execute(
            "SELECT rel, weight FROM corpus_edge "
            "WHERE src_id='bridge:ground' AND dst_id='bridge:term'"
        ).fetchone()
        assert row is not None
        assert row[0] == "GROUNDED_TUNNEL"
        assert 0.0 < row[1] <= 1.0

    def test_nodal_collapse_removes_resistance_record(self):
        from src.brain.grounded_tunneling import (
            nodal_collapse, _write_resistance, _resist_key, _ensure_kv_store,
        )
        cn = _make_corpus_db()
        _seed_endpoints(cn, ["g", "t"])
        _ensure_kv_store(cn)
        path = ["bridge:g", "bridge:t"]
        _write_resistance(cn, path, "bridge:g", duration_s=300)
        cn.commit()
        record = {"ground_id": "bridge:g", "path": path}
        nodal_collapse(cn, "bridge:g", "bridge:t", record, {"bridge:g": 5.0}, {})
        cn.commit()
        row = cn.execute(
            "SELECT key FROM kv_store WHERE key=?",
            (_resist_key("bridge:g", "bridge:t"),),
        ).fetchone()
        assert row is None   # resistance record cleaned up

    def test_ground_and_expand_opens_and_collapses(self):
        """Full tick: open paths then force-expire and collapse."""
        from src.brain.grounded_tunneling import (
            ground_and_expand, _resist_key, _ensure_kv_store,
        )
        cn = _make_corpus_db()
        names = ["a", "b", "c", "d"]
        _seed_endpoints(cn, names)
        now = datetime.now().isoformat()
        # a→b: high certainty anchor
        cn.execute(
            "INSERT INTO corpus_edge VALUES(?,?,?,?,?,?,?,?)",
            ("bridge:a", "Endpoint", "bridge:b", "Endpoint",
             "REACHABLE", 0.95, now, 25),
        )
        # b→c→d: low certainty frontier
        cn.execute(
            "INSERT INTO corpus_edge VALUES(?,?,?,?,?,?,?,?)",
            ("bridge:b", "Endpoint", "bridge:c", "Endpoint",
             "REACHABLE", 0.08, now, 1),
        )
        cn.execute(
            "INSERT INTO corpus_edge VALUES(?,?,?,?,?,?,?,?)",
            ("bridge:c", "Endpoint", "bridge:d", "Endpoint",
             "REACHABLE", 0.05, now, 1),
        )
        cn.commit()

        s1 = ground_and_expand(cn)
        assert s1["ground_nodes"] >= 1

        # Force-expire a resistance record to test collapse
        from datetime import timedelta
        past = (datetime.now() - timedelta(seconds=10)).isoformat()
        expired_path = ["bridge:a", "bridge:b", "bridge:c"]
        cn.execute(
            "INSERT INTO kv_store(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (
                _resist_key("bridge:a", "bridge:c"),
                json.dumps({
                    "expires_at": past,
                    "ground_id":  "bridge:a",
                    "path":       expired_path,
                }),
            ),
        )
        cn.commit()

        s2 = ground_and_expand(cn)
        assert s2["collapses"] >= 1
        row = cn.execute(
            "SELECT rel FROM corpus_edge "
            "WHERE src_id='bridge:a' AND dst_id='bridge:c' "
            "  AND rel='GROUNDED_TUNNEL'"
        ).fetchone()
        assert row is not None

    def test_torus_amplify_read_by_tick_without_error(self):
        """tick_torus_pressure reads torus_amplify keys from kv_store silently."""
        cn = _make_corpus_db()
        _seed_endpoints(cn, list("abcdefgh"))
        cn.execute(
            "INSERT INTO kv_store(key,value) VALUES('torus_amplify:bridge:a','1.8')"
        )
        cn.commit()
        d = tick_torus_pressure(cn)
        assert d["endpoints"] == 8
        assert d["moved"] >= 0   # no exception raised
