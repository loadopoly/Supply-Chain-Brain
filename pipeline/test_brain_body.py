"""Tests for the Brain → Body bridge.

Verifies:
  1. surface_effective_signals emits directives across multiple sources.
  2. Re-running on unchanged inputs deduplicates (fingerprint-based).
  3. Directives are sorted by priority and capped by max_directives_per_round.
  4. record_feedback writes to body_feedback AND updates body_directives.status.
  5. The knowledge_corpus._ingest_body_feedback round picks up feedback,
     adding a learning_log row + a Body→Target EXECUTED_ edge.
"""
from __future__ import annotations

import sqlite3
import sys
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

TMP_DB = ROOT / "test_local_brain_body.sqlite"
if TMP_DB.exists():
    TMP_DB.unlink()

from src.brain import local_store  # noqa: E402
local_store._DB_PATH = TMP_DB
local_store.db_path = lambda: TMP_DB  # type: ignore

from src.brain import brain_body_signals as bb  # noqa: E402
from src.brain import knowledge_corpus as kc    # noqa: E402


def _conn():
    return sqlite3.connect(TMP_DB)


def banner(msg: str) -> None:
    print(f"\n{'=' * 72}\n{msg}\n{'=' * 72}")


def seed_signals() -> None:
    """Plant rows that should trigger every generator."""
    bb.init_schema()
    kc.init_schema()
    with closing(_conn()) as cn:
        cn.executescript(
            """
            CREATE TABLE IF NOT EXISTS llm_dispatch_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ran_at TEXT, model_id TEXT, task TEXT, validator REAL);
            CREATE TABLE IF NOT EXISTS network_topology(
                host TEXT NOT NULL, protocol TEXT NOT NULL, port INTEGER,
                capability TEXT, first_seen TEXT, last_seen TEXT,
                last_ok INTEGER, samples INTEGER, successes INTEGER,
                ema_latency_ms REAL, ema_success REAL, source TEXT,
                PRIMARY KEY(host, protocol, port));
            CREATE TABLE IF NOT EXISTS llm_self_train_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ran_at TEXT, task TEXT, samples INTEGER, matched INTEGER,
                avg_validator REAL, drift_capped INTEGER,
                diversity_dampened INTEGER, notes TEXT);

            -- 5 weak dispatches → low_dispatch_quality should fire
            INSERT INTO llm_dispatch_log(ran_at, model_id, task, validator) VALUES
              (datetime('now','-1 hour'),'weakmdl','vendor_consolidation',0.20),
              (datetime('now','-1 hour'),'weakmdl','vendor_consolidation',0.30),
              (datetime('now','-1 hour'),'weakmdl','vendor_consolidation',0.25),
              (datetime('now','-1 hour'),'weakmdl','vendor_consolidation',0.35),
              (datetime('now','-1 hour'),'weakmdl','vendor_consolidation',0.18);

            -- A dead peer
            INSERT INTO network_topology(host, protocol, port, capability,
                first_seen, last_seen, last_ok, samples, successes,
                ema_latency_ms, ema_success, source) VALUES
              ('dead-peer', 'tcp', 8000, 'compute peer',
               datetime('now','-7 day'), datetime('now','-1 hour'),
               0, 30, 4, 1500.0, 0.10, 'compute_peers');

            -- Self-train round with drift caps fired
            INSERT INTO llm_self_train_log(ran_at, task, samples, matched,
                avg_validator, drift_capped, diversity_dampened, notes)
              VALUES (datetime('now','-30 minute'),'vendor_consolidation',
                      20, 18, 0.85, 3, 1, 'strong signal but clamped');

            -- High-centrality part with 4 corpus edges
            INSERT INTO corpus_entity(entity_id, entity_type, label, props_json,
                first_seen, last_seen, samples)
              VALUES ('PRT-HOT', 'Part', 'PRT-HOT', '{}',
                      datetime('now'), datetime('now'), 10);
            INSERT INTO corpus_edge(src_id, src_type, dst_id, dst_type, rel,
                weight, last_seen, samples) VALUES
              ('PRT-HOT','Part','SUP-A','Supplier','SUPPLIED_BY',1.0,datetime('now'),1),
              ('PRT-HOT','Part','SUP-B','Supplier','SUPPLIED_BY',1.0,datetime('now'),1),
              ('PRT-HOT','Part','SITE-X','Site','STOCKED_AT',1.0,datetime('now'),1),
              ('PRT-HOT','Part','Bearing','Category','CLASSIFIED_AS',1.0,datetime('now'),1);

            -- An UNCLASSIFIED part (no CLASSIFIED_AS edge)
            INSERT INTO corpus_entity(entity_id, entity_type, label, props_json,
                first_seen, last_seen, samples)
              VALUES ('PRT-NEW', 'Part', 'PRT-NEW', '{}',
                      datetime('now'), datetime('now'), 1);
            """
        )
    print("  seeded dispatch + network + self_train + corpus rows")


# ---------------------------------------------------------------------------
def test_surface_emits_multi_source() -> None:
    banner("1) surface_effective_signals emits directives across sources")
    bb._LAST_SURFACE_TS = 0.0  # type: ignore[attr-defined]

    real_cfg = bb._cfg
    def patched() -> dict:
        c = dict(real_cfg() or {})
        c["enabled"] = True
        c["min_seconds_between_rounds"] = 0.0
        c["max_directives_per_round"] = 25
        return c
    bb._cfg = patched  # type: ignore[assignment]

    out = bb.surface_effective_signals()
    print(f"  result: {out}")
    assert out["directives_emitted"] >= 4, "expected ≥4 from 4+ generators"

    open_d = bb.list_open_directives(limit=50)
    sources = {d["source"] for d in open_d}
    print(f"  sources surfaced: {sorted(sources)}")
    assert {"dispatch", "network", "self_train", "corpus"} <= sources


# ---------------------------------------------------------------------------
def test_dedupe_on_second_run() -> None:
    banner("2) Second run with unchanged inputs deduplicates")
    bb._LAST_SURFACE_TS = 0.0  # type: ignore[attr-defined]
    out = bb.surface_effective_signals()
    print(f"  second-run: emitted={out['directives_emitted']} deduped={out['directives_deduped']}")
    assert out["directives_emitted"] == 0
    assert out["directives_deduped"] >= 4


# ---------------------------------------------------------------------------
def test_priority_sort_and_cap() -> None:
    banner("3) Directives sorted by priority desc; cap honored")
    bb._LAST_SURFACE_TS = 0.0  # type: ignore[attr-defined]

    real_cfg = bb._cfg
    def patched() -> dict:
        c = dict(real_cfg() or {})
        c["enabled"] = True
        c["min_seconds_between_rounds"] = 0.0
        c["max_directives_per_round"] = 2   # tight cap
        return c
    bb._cfg = patched  # type: ignore[assignment]

    # Wipe directives so the cap is observable on a fresh insert path.
    with closing(_conn()) as cn:
        cn.execute("DELETE FROM body_directives")
        cn.commit()

    out = bb.surface_effective_signals()
    print(f"  emitted under cap=2: {out['directives_emitted']}")
    assert out["directives_emitted"] <= 2

    rows = bb.list_open_directives(limit=10)
    priorities = [r["priority"] for r in rows]
    print(f"  priorities (desc?): {[round(p,2) for p in priorities]}")
    assert priorities == sorted(priorities, reverse=True)


# ---------------------------------------------------------------------------
def test_record_feedback_updates_status() -> None:
    banner("4) record_feedback writes body_feedback + updates directive.status")
    rows = bb.list_open_directives(limit=10)
    assert rows, "need at least one open directive to ack"
    target_id = rows[0]["id"]
    target_entity = rows[0].get("target_entity")
    res = bb.record_feedback(target_id, "in_progress",
                             outcome="Picked it up; ETA tomorrow.",
                             executed_by="agard@astec")
    print(f"  feedback: {res}")
    with closing(_conn()) as cn:
        st = cn.execute(
            "SELECT status FROM body_directives WHERE id=?", (target_id,)
        ).fetchone()[0]
        n_fb = cn.execute(
            "SELECT COUNT(*) FROM body_feedback WHERE directive_id=?",
            (target_id,),
        ).fetchone()[0]
    print(f"  directive.status={st}, body_feedback rows={n_fb}")
    assert st == "in_progress"
    assert n_fb == 1
    return target_id, target_entity


# ---------------------------------------------------------------------------
def test_loop_closes_via_corpus_ingest() -> None:
    banner("5) knowledge_corpus picks up body_feedback (loop closes)")
    kc._LAST_REFRESH_TS = 0.0  # type: ignore[attr-defined]

    real_cfg = kc._cfg
    def patched() -> dict:
        c = dict(real_cfg() or {})
        c["enabled"] = True
        c["min_seconds_between_rounds"] = 0.0
        return c
    kc._cfg = patched  # type: ignore[assignment]

    out = kc.refresh_corpus_round()
    print(f"  corpus round: {out}")
    body_learnings = kc.recent_learnings(limit=10, kind="body_feedback")
    print(f"  body_feedback learnings: {len(body_learnings)}")
    assert len(body_learnings) >= 1

    with closing(_conn()) as cn:
        body_node = cn.execute(
            "SELECT COUNT(*) FROM corpus_entity WHERE entity_type='Body'"
        ).fetchone()[0]
        body_edges = cn.execute(
            """SELECT COUNT(*) FROM corpus_edge
                WHERE src_type='Body' AND rel LIKE 'EXECUTED_%'"""
        ).fetchone()[0]
    print(f"  Body entities={body_node}, EXECUTED_* edges={body_edges}")
    assert body_node >= 1
    assert body_edges >= 1


# ---------------------------------------------------------------------------
def main() -> int:
    seed_signals()
    test_surface_emits_multi_source()
    test_dedupe_on_second_run()
    test_priority_sort_and_cap()
    test_record_feedback_updates_status()
    test_loop_closes_via_corpus_ingest()
    banner("ALL BRAIN-BODY GUARD-RAILS VERIFIED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
