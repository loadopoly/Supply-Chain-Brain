from __future__ import annotations

import json
import sqlite3
from datetime import datetime

import pytest

from src.brain.llada_signbit_children import (
    _CHILD_TYPE,
    _REL_CHILD_OF,
    acquire_llada_signbit_children,
    bit_signature,
    candidate_parents,
    flip_delta,
    seed_model_entities_from_llm_weights,
    sign_bits_from_axes,
)


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


def _seed_entity(cn: sqlite3.Connection, entity_id: str, entity_type: str = "Quest") -> None:
    now = datetime.now().isoformat()
    cn.execute(
        "INSERT INTO corpus_entity VALUES(?,?,?,?,?,?,1)",
        (entity_id, entity_type, entity_id, "{}", now, now),
    )
    cn.commit()


def _seed_llm_weights(cn: sqlite3.Connection) -> None:
    cn.executescript(
        """
        CREATE TABLE llm_weights(
            task TEXT,
            model_id TEXT,
            weight REAL,
            ema_success REAL,
            ema_latency REAL,
            n_obs INTEGER
        );
        """
    )
    cn.execute(
        "INSERT INTO llm_weights VALUES(?,?,?,?,?,?)",
        ("cross_dataset_review", "llada-2.0-local", 0.42, 0.55, 1200.0, 12),
    )
    cn.execute(
        "INSERT INTO llm_weights VALUES(?,?,?,?,?,?)",
        ("narrative_summary", "llada-2.0-local", 0.50, 0.60, 900.0, 9),
    )
    cn.commit()


def test_sign_bits_centered_at_half():
    bits = sign_bits_from_axes({"expansion_score": 0.7, "coherence": 0.49, "bifurcation_index": 0.5})
    assert bits == {"expansion": 1, "coherence": -1, "bifurcation": 1}
    assert bit_signature(bits) == "exp+_coh-_bif+"


def test_flip_delta_bootstraps_missing_previous_bits():
    current = {"expansion": 1, "coherence": -1, "bifurcation": 1}
    flips = flip_delta({}, current)
    assert set(flips) == {"expansion", "coherence", "bifurcation"}
    assert flips["coherence"] == (0, -1)


def test_flip_delta_detects_only_changed_axes():
    previous = {"expansion": 1, "coherence": -1, "bifurcation": 1}
    current = {"expansion": -1, "coherence": -1, "bifurcation": 1}
    assert flip_delta(previous, current) == {"expansion": (1, -1)}


def test_seed_model_entities_from_llm_weights():
    cn = _make_corpus_db()
    _seed_llm_weights(cn)
    added = seed_model_entities_from_llm_weights(cn)
    assert added == 1
    row = cn.execute(
        "SELECT entity_type, props_json FROM corpus_entity WHERE entity_id='llada-2.0-local'"
    ).fetchone()
    assert row is not None
    assert row[0] == "Model"
    props = json.loads(row[1])
    assert props["llada_version"] == "2.0"
    assert props["tasks"] == 2


def test_candidate_parents_prefers_seeded_model():
    cn = _make_corpus_db()
    _seed_llm_weights(cn)
    seed_model_entities_from_llm_weights(cn)
    _seed_entity(cn, "quest:baseline", "Quest")
    parents = candidate_parents(cn, max_parents=2)
    assert parents
    assert parents[0].entity_type == "Model"
    assert parents[0].entity_id == "llada-2.0-local"


def test_acquire_children_on_first_signbit_frame():
    cn = _make_corpus_db()
    _seed_llm_weights(cn)
    stats = acquire_llada_signbit_children(
        cn,
        axes={"expansion_score": 0.8, "coherence": 0.3, "bifurcation_index": 0.7},
        max_children=6,
        max_parents=2,
    )
    assert set(stats["flips"]) == {"expansion", "coherence", "bifurcation"}
    assert stats["child_nodes_added"] > 0
    child_count = cn.execute(
        "SELECT COUNT(*) FROM corpus_entity WHERE entity_type=?",
        (_CHILD_TYPE,),
    ).fetchone()[0]
    assert child_count == stats["child_nodes_added"]
    edge_count = cn.execute(
        "SELECT COUNT(*) FROM corpus_edge WHERE src_type=? AND rel=?",
        (_CHILD_TYPE, _REL_CHILD_OF),
    ).fetchone()[0]
    assert edge_count == child_count


def test_second_same_frame_is_idempotent():
    cn = _make_corpus_db()
    _seed_llm_weights(cn)
    axes = {"expansion_score": 0.8, "coherence": 0.3, "bifurcation_index": 0.7}
    s1 = acquire_llada_signbit_children(cn, axes=axes)
    s2 = acquire_llada_signbit_children(cn, axes=axes)
    assert s1["child_nodes_added"] > 0
    assert s2["flips"] == {}
    assert s2["child_nodes_added"] == 0


def test_crossing_axis_creates_new_child_signature():
    cn = _make_corpus_db()
    _seed_llm_weights(cn)
    acquire_llada_signbit_children(
        cn,
        axes={"expansion_score": 0.8, "coherence": 0.3, "bifurcation_index": 0.7},
    )
    before = cn.execute(
        "SELECT COUNT(*) FROM corpus_entity WHERE entity_type=?",
        (_CHILD_TYPE,),
    ).fetchone()[0]
    stats = acquire_llada_signbit_children(
        cn,
        axes={"expansion_score": 0.2, "coherence": 0.3, "bifurcation_index": 0.7},
    )
    after = cn.execute(
        "SELECT COUNT(*) FROM corpus_entity WHERE entity_type=?",
        (_CHILD_TYPE,),
    ).fetchone()[0]
    assert stats["flips"] == {"expansion": [1, -1]}
    assert after > before


def test_child_props_record_flip_axis_and_bits():
    cn = _make_corpus_db()
    _seed_llm_weights(cn)
    acquire_llada_signbit_children(
        cn,
        axes={"expansion_score": 0.8, "coherence": 0.3, "bifurcation_index": 0.7},
    )
    row = cn.execute(
        "SELECT props_json FROM corpus_entity WHERE entity_type=? LIMIT 1",
        (_CHILD_TYPE,),
    ).fetchone()
    assert row is not None
    props = json.loads(row[0])
    assert props["llada_version"] == "2.0"
    assert props["flip_axis"] in {"expansion", "coherence", "bifurcation"}
    assert props["sign_bits"] == {"expansion": 1, "coherence": -1, "bifurcation": 1}


def test_acquire_uses_generic_parent_when_no_model_exists():
    cn = _make_corpus_db()
    _seed_entity(cn, "quest:root", "Quest")
    stats = acquire_llada_signbit_children(
        cn,
        axes={"expansion_score": 0.1, "coherence": 0.9, "bifurcation_index": 0.1},
        max_children=3,
    )
    assert stats["parents_seen"] == 1
    assert stats["child_nodes_added"] == 3


@pytest.mark.parametrize(
    "axes,expected",
    [
        ({"expansion_score": 1.2, "coherence": -1.0, "bifurcation_index": 0.49}, {"expansion": 1, "coherence": -1, "bifurcation": -1}),
        ({"expansion": 0.0, "coherence": 1.0, "bifurcation": 0.5}, {"expansion": -1, "coherence": 1, "bifurcation": 1}),
    ],
)
def test_sign_bits_clip_axis_inputs(axes, expected):
    assert sign_bits_from_axes(axes) == expected