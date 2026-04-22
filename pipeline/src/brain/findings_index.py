"""
Findings index — SQLite-backed cross-page index of interesting items so any
page can drill THROUGH to context surfaced by another (req 2c, 2d).

Schema (kept tiny on purpose):
  findings(id INTEGER PK, page TEXT, kind TEXT, key TEXT, score REAL,
           payload_json TEXT, created_at TEXT)
"""
from __future__ import annotations
from pathlib import Path
import json
import sqlite3
import time
from typing import Iterable

DB_PATH = Path(__file__).resolve().parents[2] / "findings_index.db"


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.execute("""
    CREATE TABLE IF NOT EXISTS findings (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        page        TEXT NOT NULL,
        kind        TEXT NOT NULL,
        key         TEXT NOT NULL,
        score       REAL,
        payload_json TEXT,
        created_at  TEXT NOT NULL
    )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS ix_findings_kind_key   ON findings(kind, key)")
    c.execute("CREATE INDEX IF NOT EXISTS ix_findings_page       ON findings(page)")
    c.execute("CREATE INDEX IF NOT EXISTS ix_findings_page_kind  ON findings(page, kind)")
    c.execute("CREATE INDEX IF NOT EXISTS ix_findings_created_at ON findings(created_at DESC)")
    c.execute("""
    CREATE TABLE IF NOT EXISTS decision_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        page        TEXT NOT NULL,
        action      TEXT NOT NULL,
        target_kind TEXT,
        target_key  TEXT,
        inputs_json TEXT,
        model       TEXT,
        confidence  REAL,
        user_id     TEXT,
        created_at  TEXT NOT NULL
    )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS ix_decision_target     ON decision_log(target_kind, target_key)")
    c.execute("CREATE INDEX IF NOT EXISTS ix_decision_created_at ON decision_log(created_at DESC)")
    # Quest engine — Missions are user-initiated instances of a Brain Quest.
    # mission_events captures status changes, refresh runs, and progress signals
    # so the deck/one-pager footer can show "refreshed YYYY-MM-DD · progress X%"
    # without losing history.
    c.execute("""
    CREATE TABLE IF NOT EXISTS missions (
        id                  TEXT PRIMARY KEY,
        quest_id            TEXT NOT NULL,
        site                TEXT NOT NULL,
        user_query          TEXT NOT NULL,
        parsed_intent_json  TEXT NOT NULL,
        scope_tags_json     TEXT NOT NULL,
        target_entity_kind  TEXT NOT NULL,
        target_entity_key   TEXT NOT NULL,
        horizon_days        INTEGER NOT NULL DEFAULT 90,
        status              TEXT NOT NULL DEFAULT 'open',
        progress_pct        REAL NOT NULL DEFAULT 0.0,
        artifact_paths_json TEXT NOT NULL DEFAULT '{}',
        created_at          TEXT NOT NULL,
        last_refreshed_at   TEXT
    )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS ix_missions_status ON missions(status)")
    c.execute("CREATE INDEX IF NOT EXISTS ix_missions_site   ON missions(site)")
    c.execute("CREATE INDEX IF NOT EXISTS ix_missions_quest  ON missions(quest_id)")
    c.execute("""
    CREATE TABLE IF NOT EXISTS mission_events (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        mission_id  TEXT NOT NULL,
        kind        TEXT NOT NULL,
        payload_json TEXT,
        created_at  TEXT NOT NULL
    )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS ix_mission_events_mid ON mission_events(mission_id)")
    return c


def log_decision(page: str, action: str, *, target_kind: str | None = None,
                 target_key: str | None = None, inputs: dict | None = None,
                 model: str | None = None, confidence: float | None = None,
                 user_id: str | None = None) -> int:
    """Phase 3.2 — record provenance for every automated recommendation."""
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO decision_log(page, action, target_kind, target_key, inputs_json, "
            "model, confidence, user_id, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (page, action, target_kind, str(target_key) if target_key is not None else None,
             json.dumps(inputs or {}, default=str), model, confidence, user_id,
             time.strftime("%Y-%m-%dT%H:%M:%S")),
        )
        return int(cur.lastrowid)


def lookup_decisions(target_kind: str | None = None, target_key: str | None = None,
                     limit: int = 50) -> list[dict]:
    with _conn() as c:
        sql = ("SELECT id, page, action, target_kind, target_key, inputs_json, "
               "model, confidence, user_id, created_at FROM decision_log WHERE 1=1")
        params: list = []
        if target_kind:
            sql += " AND target_kind=?"; params.append(target_kind)
        if target_key:
            sql += " AND target_key=?"; params.append(str(target_key))
        sql += " ORDER BY id DESC LIMIT ?"; params.append(limit)
        rows = c.execute(sql, params).fetchall()
    return [{
        "id": r[0], "page": r[1], "action": r[2], "target_kind": r[3],
        "target_key": r[4], "inputs": json.loads(r[5] or "{}"),
        "model": r[6], "confidence": r[7], "user_id": r[8], "created_at": r[9],
    } for r in rows]


def record_finding(page: str, kind: str, key: str,
                   score: float | None = None, payload: dict | None = None) -> int:
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO findings(page, kind, key, score, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (page, kind, str(key), score,
             json.dumps(payload or {}, default=str),
             time.strftime("%Y-%m-%dT%H:%M:%S")),
        )
        return int(cur.lastrowid)


def record_findings_bulk(page: str, kind: str, items: Iterable[dict]) -> int:
    """`items` iterable of {key, score?, payload?}."""
    n = 0
    with _conn() as c:
        for it in items:
            c.execute(
                "INSERT INTO findings(page, kind, key, score, payload_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (page, kind, str(it["key"]),
                 it.get("score"),
                 json.dumps(it.get("payload", {}), default=str),
                 time.strftime("%Y-%m-%dT%H:%M:%S")),
            )
            n += 1
    return n


def lookup_findings(kind: str, key: str | None = None, limit: int = 50) -> list[dict]:
    with _conn() as c:
        sql = ("SELECT id, page, kind, key, score, payload_json, created_at "
               "FROM findings WHERE kind=?")
        params: tuple = (kind,)
        if key is not None:
            sql += " AND key=?"
            params = (kind, str(key))
        sql += " ORDER BY id DESC LIMIT ?"
        params = params + (limit,)
        rows = c.execute(sql, params).fetchall()
    return [{
        "id": r[0], "page": r[1], "kind": r[2], "key": r[3], "score": r[4],
        "payload": json.loads(r[5] or "{}"), "created_at": r[6],
    } for r in rows]


def all_kinds() -> list[str]:
    with _conn() as c:
        return [r[0] for r in c.execute("SELECT DISTINCT kind FROM findings").fetchall()]


def clear(kind: str | None = None) -> int:
    with _conn() as c:
        if kind:
            cur = c.execute("DELETE FROM findings WHERE kind=?", (kind,))
        else:
            cur = c.execute("DELETE FROM findings")
        return cur.rowcount


def prune(retention_days: int = 90) -> tuple[int, int]:
    """Delete findings and decision_log entries older than retention_days.

    Returns (findings_deleted, decisions_deleted).
    Call from a scheduled job or the Benchmarks page to keep the DB compact.
    """
    cutoff = time.strftime(
        "%Y-%m-%dT%H:%M:%S",
        time.gmtime(time.time() - retention_days * 86400),
    )
    with _conn() as c:
        f_cur = c.execute("DELETE FROM findings WHERE created_at < ?", (cutoff,))
        d_cur = c.execute("DELETE FROM decision_log WHERE created_at < ?", (cutoff,))
    return f_cur.rowcount, d_cur.rowcount
