"""Tests for the knowledge corpus & recent-learnings log."""
from __future__ import annotations

import sqlite3
import sys
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

TMP_DB = ROOT / "test_local_brain_corpus.sqlite"
if TMP_DB.exists():
    TMP_DB.unlink()

from src.brain import local_store  # noqa: E402
local_store._DB_PATH = TMP_DB
local_store.db_path = lambda: TMP_DB  # type: ignore

from src.brain import knowledge_corpus as kc  # noqa: E402


def _conn():
    return sqlite3.connect(TMP_DB)


def banner(msg: str) -> None:
    bar = "=" * 72
    print(f"\n{bar}\n{msg}\n{bar}")


def seed_sources() -> None:
    """Plant rows in every source stream the corpus reads from."""
    with closing(_conn()) as cn:
        cn.executescript(
            """
            CREATE TABLE IF NOT EXISTS llm_self_train_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ran_at TEXT, task TEXT, samples INTEGER, matched INTEGER,
                avg_validator REAL, drift_capped INTEGER,
                diversity_dampened INTEGER, notes TEXT);
            CREATE TABLE IF NOT EXISTS llm_dispatch_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ran_at TEXT, model_id TEXT, task TEXT, validator REAL);
            CREATE TABLE IF NOT EXISTS llm_weights(
                model_id TEXT, task TEXT, weight REAL, bias REAL,
                PRIMARY KEY(model_id, task));
            CREATE TABLE IF NOT EXISTS network_observations(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT, source TEXT, protocol TEXT, host TEXT,
                port INTEGER, capability TEXT, latency_ms REAL,
                ok INTEGER, error TEXT);
            CREATE TABLE IF NOT EXISTS network_promotions(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                promoted_at TEXT, target TEXT, host TEXT, reason TEXT);
            CREATE TABLE IF NOT EXISTS part_category(
                part_key TEXT PRIMARY KEY, category TEXT);
            CREATE TABLE IF NOT EXISTS otd_ownership(
                po_number TEXT PRIMARY KEY, owner TEXT);

            INSERT INTO llm_self_train_log(ran_at, task, samples, matched,
                avg_validator, drift_capped, diversity_dampened, notes)
              VALUES('2026-04-22T00:00:00+00:00','vendor_consolidation',
                     12, 9, 0.78, 0, 1, 'ok');

            INSERT INTO llm_dispatch_log(ran_at, model_id, task, validator) VALUES
              ('2026-04-22T00:00:01+00:00','gemma-4',  'vendor_consolidation', 0.82),
              ('2026-04-22T00:00:02+00:00','qwen-3.5', 'vendor_consolidation', 0.71),
              ('2026-04-22T00:00:03+00:00','gemma-4',  'otd_classify',         0.65),
              ('2026-04-22T00:00:04+00:00','deepseek-v3','what_if',            0.55);

            INSERT INTO llm_weights(model_id, task, weight, bias) VALUES
              ('gemma-4',    'vendor_consolidation', 1.40, 0.0),
              ('qwen-3.5',   'vendor_consolidation', 1.10, 0.0),
              ('deepseek-v3','what_if',              1.05, 0.0);

            INSERT INTO network_observations(observed_at, source, protocol,
                host, port, capability, latency_ms, ok, error) VALUES
              ('2026-04-22T00:00:01+00:00','connections.yaml','sqlserver',
               'edap-replica-cms-sqlserver.database.windows.net', 1433, 'azure_sql', 23.5, 1, NULL),
              ('2026-04-22T00:00:02+00:00','smb_mapping','smb',
               'crp-fs03', 445, 'public', 8.1, 1, NULL),
              ('2026-04-22T00:00:03+00:00','mx_lookup','smtp',
               'astecindustries-com.mail.protection.outlook.com', 25, 'MX', 41.0, 1, NULL),
              ('2026-04-22T00:00:04+00:00','compute_peers','tcp',
               'roadd-5wd1nh3', 8000, 'compute peer', 4.2, 1, NULL);

            INSERT INTO network_promotions(promoted_at, target, host, reason)
              VALUES('2026-04-22T00:00:05+00:00','compute_grid',
                     'roadd-5wd1nh3','ema_success>=0.7');

            INSERT INTO part_category(part_key, category) VALUES
              ('PRT-001','Bearing'),
              ('PRT-002','Hydraulic'),
              ('PRT-003','Bearing'),
              ('PRT-004','Fastener');

            INSERT INTO otd_ownership(po_number, owner) VALUES
              ('PO-1001','Buyer A'),
              ('PO-1002','Supplier B'),
              ('PO-1003','Buyer A');
            """
        )
    print("  seeded all 7 source streams")


# ---------------------------------------------------------------------------
def test_round_ingests_all_streams() -> None:
    banner("1) refresh_corpus_round ingests every source stream")
    kc._LAST_REFRESH_TS = 0.0  # type: ignore[attr-defined]

    real_cfg = kc._cfg
    def patched() -> dict:
        c = dict(real_cfg() or {})
        c["enabled"] = True
        c["min_seconds_between_rounds"] = 0.0
        return c
    kc._cfg = patched  # type: ignore[assignment]

    out = kc.refresh_corpus_round()
    print(f"  result: {out}")
    assert out.get("entities_added", 0) > 0
    assert out.get("edges_added", 0) > 0
    assert out.get("learnings_logged", 0) >= 1


# ---------------------------------------------------------------------------
def test_corpus_summary_covers_expected_types() -> None:
    banner("2) corpus_summary covers all expected entity types & edge rels")
    s = kc.corpus_summary()
    print(f"  entities_by_type: {s['entities_by_type']}")
    print(f"  edges_by_rel:     {s['edges_by_rel']}")
    expected_entities = {"Task", "Model", "Endpoint", "Protocol", "Peer",
                         "Part", "Category", "PO", "Owner"}
    actual = set(s["entities_by_type"].keys())
    missing = expected_entities - actual
    assert not missing, f"missing entity types: {missing}"
    expected_rels = {"ANSWERS", "WEIGHTED_FOR", "USES", "CLASSIFIED_AS", "OWNS"}
    missing_r = expected_rels - set(s["edges_by_rel"].keys())
    assert not missing_r, f"missing edge rels: {missing_r}"


# ---------------------------------------------------------------------------
def test_recent_learnings_filter_and_order() -> None:
    banner("3) recent_learnings is reverse-chronological and filterable")
    all_l = kc.recent_learnings(limit=20)
    print(f"  total recent: {len(all_l)}; kinds: {sorted({l['kind'] for l in all_l})}")
    assert len(all_l) >= 2
    ids = [l["id"] for l in all_l]
    assert ids == sorted(ids, reverse=True), "must be reverse-chronological"

    self_only = kc.recent_learnings(limit=20, kind="self_train")
    promo_only = kc.recent_learnings(limit=20, kind="promotion")
    print(f"  self_train rows: {len(self_only)}, promotion rows: {len(promo_only)}")
    assert len(self_only) >= 1
    assert len(promo_only) >= 1
    assert all(l["kind"] == "self_train" for l in self_only)


# ---------------------------------------------------------------------------
def test_incremental_cursor_avoids_duplicates() -> None:
    banner("4) Cursors make subsequent rounds incremental (no duplicate edges)")
    with closing(_conn()) as cn:
        before = cn.execute("SELECT COUNT(*) FROM corpus_edge").fetchone()[0]

    # Run again with no new source rows
    kc._LAST_REFRESH_TS = 0.0  # type: ignore[attr-defined]
    out = kc.refresh_corpus_round()
    print(f"  second round: added entities={out['entities_added']}, edges={out['edges_added']}")

    with closing(_conn()) as cn:
        after = cn.execute("SELECT COUNT(*) FROM corpus_edge").fetchone()[0]
    print(f"  edge count: {before} -> {after}")
    # No new edges should be added since no new source rows arrived.
    # WEIGHTED_FOR and CLASSIFIED_AS get re-touched (full-table snapshots),
    # but they shouldn't add NEW rows.
    assert out["edges_added"] == 0


# ---------------------------------------------------------------------------
def test_materialize_into_graph_projects_nodes_and_edges() -> None:
    banner("5) materialize_into_graph projects corpus into NetworkX backend")
    res = kc.materialize_into_graph(max_entities=500, max_edges=2000)
    print(f"  result: {res}")
    assert res.get("ok") is True
    assert res.get("nodes_projected", 0) > 0
    assert res.get("edges_projected", 0) > 0


# ---------------------------------------------------------------------------
def main() -> int:
    seed_sources()
    test_round_ingests_all_streams()
    test_corpus_summary_covers_expected_types()
    test_recent_learnings_filter_and_order()
    test_incremental_cursor_avoids_duplicates()
    test_materialize_into_graph_projects_nodes_and_edges()
    banner("ALL KNOWLEDGE-CORPUS GUARD-RAILS VERIFIED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
