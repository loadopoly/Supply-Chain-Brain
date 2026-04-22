"""
Mission CRUD — thin wrapper over the `missions` / `mission_events` tables in
findings_index.db. Keeps the Quest Console UI and the orchestrator out of
SQL details.

Findings produced during a Mission run are tagged with `mission_id` in the
existing `findings` table by passing it through `payload["mission_id"]`,
which lets a refresh diff progress without touching the original schema.
"""
from __future__ import annotations

from dataclasses import asdict
import json
import time
from typing import Any

from .findings_index import _conn
from .quests import Mission, new_mission_id


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _row_to_mission(row: tuple) -> Mission:
    (mid, qid, site, query, parsed_json, tags_json, ek, ekey, horizon,
     status, progress, artifacts_json, created, refreshed) = row
    return Mission(
        id=mid,
        quest_id=qid,
        site=site,
        user_query=query,
        parsed_intent=json.loads(parsed_json or "{}"),
        scope_tags=json.loads(tags_json or "[]"),
        target_entity_kind=ek,
        target_entity_key=ekey,
        horizon_days=int(horizon),
        status=status,
        progress_pct=float(progress),
        artifact_paths=json.loads(artifacts_json or "{}"),
        created_at=created or "",
        last_refreshed_at=refreshed or "",
    )


_COLS = ("id, quest_id, site, user_query, parsed_intent_json, scope_tags_json, "
         "target_entity_kind, target_entity_key, horizon_days, status, "
         "progress_pct, artifact_paths_json, created_at, last_refreshed_at")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def create_mission(*, quest_id: str, site: str, user_query: str,
                   parsed_intent: dict, scope_tags: list[str],
                   target_entity_kind: str, target_entity_key: str,
                   horizon_days: int = 90) -> Mission:
    m = Mission(
        id=new_mission_id(),
        quest_id=quest_id,
        site=site,
        user_query=user_query,
        parsed_intent=parsed_intent,
        scope_tags=list(scope_tags),
        target_entity_kind=target_entity_kind,
        target_entity_key=str(target_entity_key or ""),
        horizon_days=int(horizon_days),
        status="open",
        progress_pct=0.0,
        created_at=_now(),
    )
    with _conn() as c:
        c.execute(
            "INSERT INTO missions(" + _COLS + ") "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (m.id, m.quest_id, m.site, m.user_query,
             json.dumps(m.parsed_intent, default=str),
             json.dumps(m.scope_tags),
             m.target_entity_kind, m.target_entity_key, m.horizon_days,
             m.status, m.progress_pct,
             json.dumps(m.artifact_paths),
             m.created_at, m.last_refreshed_at or None),
        )
    record_event(m.id, "created", {"site": m.site, "scope_tags": m.scope_tags})
    return m


def get_mission(mission_id: str) -> Mission | None:
    with _conn() as c:
        row = c.execute(
            "SELECT " + _COLS + " FROM missions WHERE id=?", (mission_id,)
        ).fetchone()
    return _row_to_mission(row) if row else None


def list_missions(*, status: str | None = None, site: str | None = None,
                  limit: int = 100) -> list[Mission]:
    sql = "SELECT " + _COLS + " FROM missions WHERE 1=1"
    params: list[Any] = []
    if status:
        sql += " AND status=?"; params.append(status)
    if site:
        sql += " AND site=?"; params.append(site)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(int(limit))
    with _conn() as c:
        rows = c.execute(sql, params).fetchall()
    return [_row_to_mission(r) for r in rows]


def list_open(limit: int = 50) -> list[Mission]:
    with _conn() as c:
        rows = c.execute(
            "SELECT " + _COLS + " FROM missions "
            "WHERE status IN ('open','running','refreshed') "
            "ORDER BY created_at DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    return [_row_to_mission(r) for r in rows]


def update_status(mission_id: str, status: str) -> None:
    with _conn() as c:
        c.execute("UPDATE missions SET status=? WHERE id=?", (status, mission_id))
    record_event(mission_id, "status_changed", {"status": status})


def update_progress(mission_id: str, progress_pct: float,
                    note: str | None = None) -> None:
    p = max(0.0, min(100.0, float(progress_pct)))
    with _conn() as c:
        c.execute(
            "UPDATE missions SET progress_pct=?, last_refreshed_at=? WHERE id=?",
            (p, _now(), mission_id),
        )
    record_event(mission_id, "progress", {"progress_pct": p, "note": note})


def attach_artifact(mission_id: str, name: str, path: str) -> None:
    m = get_mission(mission_id)
    if not m:
        return
    m.artifact_paths[name] = path
    with _conn() as c:
        c.execute(
            "UPDATE missions SET artifact_paths_json=? WHERE id=?",
            (json.dumps(m.artifact_paths), mission_id),
        )
    record_event(mission_id, "artifact_attached", {"name": name, "path": path})


def mark_refreshed(mission_id: str) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE missions SET last_refreshed_at=?, status=CASE "
            "WHEN status='running' THEN 'refreshed' ELSE status END WHERE id=?",
            (_now(), mission_id),
        )
    record_event(mission_id, "refreshed", {})


def record_event(mission_id: str, kind: str, payload: dict | None = None) -> int:
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO mission_events(mission_id, kind, payload_json, created_at) "
            "VALUES (?, ?, ?, ?)",
            (mission_id, kind, json.dumps(payload or {}, default=str), _now()),
        )
        return int(cur.lastrowid)


def list_events(mission_id: str, limit: int = 50) -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT id, kind, payload_json, created_at FROM mission_events "
            "WHERE mission_id=? ORDER BY id DESC LIMIT ?",
            (mission_id, int(limit)),
        ).fetchall()
    return [{
        "id": r[0], "kind": r[1],
        "payload": json.loads(r[2] or "{}"), "created_at": r[3],
    } for r in rows]


def delete_mission(mission_id: str) -> None:
    """Used by tests; safe to leave on the public surface."""
    with _conn() as c:
        c.execute("DELETE FROM mission_events WHERE mission_id=?", (mission_id,))
        c.execute("DELETE FROM missions WHERE id=?", (mission_id,))


__all__ = [
    "create_mission", "get_mission", "list_missions", "list_open",
    "update_status", "update_progress", "attach_artifact", "mark_refreshed",
    "record_event", "list_events", "delete_mission",
]
