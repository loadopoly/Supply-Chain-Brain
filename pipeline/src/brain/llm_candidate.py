"""LLM Candidate Trial System — probationary evaluation before ensemble promotion.

New models declared under ``llms.candidates.models`` in config/brain.yaml enter
a scored probationary trial.  On every ``dispatch_parallel`` call the candidates
are fired alongside the main ensemble but their responses are NOT included in
the authoritative answer.  After ``dispatches_required`` observations:

  * ema_success >= promote_threshold  →  promoted into the live ``llm_registry``
    SQLite table; brain.llm_router.available_models() discovers them immediately.
  * ema_success <= reject_threshold   →  marked rejected and excluded from
    future candidate dispatches.
  * otherwise                         →  trial continues.

Every promotion / rejection is appended to ``pipeline/docs/LLM_CANDIDATE_AUDIT.md``.

SQLite table: ``llm_candidate_trials``
    model_id    TEXT PRIMARY KEY
    dispatches  INTEGER DEFAULT 0
    ema_success REAL    DEFAULT 0.5   (EMA-smoothed call-success rate)
    ema_latency REAL    DEFAULT 0.0   (EMA-smoothed latency in ms)
    status      TEXT    DEFAULT 'trial'  -- trial | promoted | rejected
    started_at  TIMESTAMP
    decided_at  TIMESTAMP

Public API::

    get_active_candidates()                   -> list[dict]
    tick_candidate(model_id, ok, latency_ms)  -> None
    evaluate_candidates()                     -> list[dict]   # decisions this call
    candidate_stats()                         -> list[dict]
"""
from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import load_config
from .local_store import db_path

logger = logging.getLogger(__name__)

# Audit log written alongside the existing LLM_SCOUT_AUDIT.md
_AUDIT_LOG: Path = (
    Path(__file__).resolve().parents[3] / "docs" / "LLM_CANDIDATE_AUDIT.md"
)

_TRIALS_DDL = """
CREATE TABLE IF NOT EXISTS llm_candidate_trials (
    model_id    TEXT NOT NULL PRIMARY KEY,
    dispatches  INTEGER NOT NULL DEFAULT 0,
    ema_success REAL    NOT NULL DEFAULT 0.5,
    ema_latency REAL    NOT NULL DEFAULT 0.0,
    status      TEXT    NOT NULL DEFAULT 'trial',
    started_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    decided_at  TIMESTAMP
);
"""

# Shared with llm_scout / llm_router — create if absent so candidates can be
# promoted even on a fresh install before the scout has ever run.
_REGISTRY_DDL = """
CREATE TABLE IF NOT EXISTS llm_registry (
    id            TEXT PRIMARY KEY,
    vendor        TEXT,
    payload_json  TEXT NOT NULL,
    source        TEXT,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    promoted      INTEGER   DEFAULT 0,
    last_seen_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


@contextmanager
def _conn():
    cn = sqlite3.connect(db_path())
    cn.row_factory = sqlite3.Row
    try:
        cn.executescript(_TRIALS_DDL + _REGISTRY_DDL)
        yield cn
        cn.commit()
    finally:
        cn.close()


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _candidates_cfg(cfg: dict | None = None) -> dict:
    cfg = cfg or load_config()
    return (cfg.get("llms") or {}).get("candidates") or {}


def _trial_params(cfg: dict | None = None) -> dict:
    return _candidates_cfg(cfg).get("trial") or {}


def _candidate_specs(cfg: dict | None = None) -> list[dict]:
    return _candidates_cfg(cfg).get("models") or []


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_active_candidates(cfg: dict | None = None) -> list[dict]:
    """Return model spec dicts (same schema as registry entries) for models
    whose trial status is 'trial'.  Seeds DB rows for first-seen candidates."""
    specs = _candidate_specs(cfg)
    if not specs:
        return []
    with _conn() as cn:
        known = {r["model_id"] for r in
                 cn.execute("SELECT model_id FROM llm_candidate_trials").fetchall()}
        for spec in specs:
            mid = spec["id"]
            if mid not in known:
                cn.execute(
                    "INSERT OR IGNORE INTO llm_candidate_trials (model_id, status) "
                    "VALUES (?, 'trial')",
                    (mid,),
                )
        active = {r["model_id"] for r in cn.execute(
            "SELECT model_id FROM llm_candidate_trials WHERE status='trial'"
        ).fetchall()}
    return [s for s in specs if s["id"] in active]


def tick_candidate(model_id: str, success: bool, latency_ms: int,
                   cfg: dict | None = None,
                   key_miss: bool = False) -> None:
    """Record one dispatch result for a trial candidate (EMA update).

    Args:
        model_id:   Candidate model id.
        success:    Whether the call returned a usable response.
        latency_ms: Wall-clock call time in milliseconds.
        cfg:        Optional pre-loaded config (avoids re-reading brain.yaml).
        key_miss:   When True the call was skipped entirely because the API key
                    was absent or in backoff.  EMA is NOT updated — a missing
                    key is not a model quality signal.  Only the raw dispatch
                    counter is left unchanged so the trial clock doesn't advance
                    on non-events.
    """
    if key_miss:
        logger.debug(
            "llm_candidate: skipping tick for %s (key_miss=True — no credential signal)",
            model_id,
        )
        return
    alpha = float(_trial_params(cfg).get("ema_alpha", 0.10))
    with _conn() as cn:
        row = cn.execute(
            "SELECT dispatches, ema_success, ema_latency, status "
            "FROM llm_candidate_trials WHERE model_id = ?",
            (model_id,),
        ).fetchone()
        if row is None or row["status"] != "trial":
            return
        n = row["dispatches"] + 1
        ema_s = (1 - alpha) * float(row["ema_success"]) + alpha * (1.0 if success else 0.0)
        ema_l = (1 - alpha) * float(row["ema_latency"]) + alpha * float(latency_ms)
        cn.execute(
            "UPDATE llm_candidate_trials "
            "SET dispatches=?, ema_success=?, ema_latency=? "
            "WHERE model_id=?",
            (n, ema_s, ema_l, model_id),
        )


def evaluate_candidates(cfg: dict | None = None) -> list[dict]:
    """Check all active trials against thresholds; auto-promote or auto-reject.

    Returns a list of decision dicts (one per model that crossed a threshold).
    """
    cfg = cfg or load_config()
    params = _trial_params(cfg)
    required = int(params.get("dispatches_required", 50))
    promote_thr = float(params.get("promote_threshold", 0.72))
    reject_thr = float(params.get("reject_threshold", 0.45))

    decisions: list[dict] = []
    with _conn() as cn:
        rows = cn.execute(
            "SELECT model_id, dispatches, ema_success, ema_latency "
            "FROM llm_candidate_trials WHERE status='trial'"
        ).fetchall()
        for row in rows:
            mid = row["model_id"]
            n = row["dispatches"]
            ema_s = float(row["ema_success"])
            ema_l = float(row["ema_latency"])
            if n < required:
                continue
            if ema_s >= promote_thr:
                verdict = "promoted"
            elif ema_s <= reject_thr:
                verdict = "rejected"
            else:
                continue
            now = datetime.now(timezone.utc).isoformat()
            cn.execute(
                "UPDATE llm_candidate_trials SET status=?, decided_at=? "
                "WHERE model_id=?",
                (verdict, now, mid),
            )
            if verdict == "promoted":
                _write_to_registry(mid, cn, cfg)
            dec: dict[str, Any] = {
                "model_id": mid,
                "decision": verdict,
                "dispatches": n,
                "ema_success": round(ema_s, 4),
                "ema_latency_ms": round(ema_l, 1),
                "decided_at": now,
            }
            decisions.append(dec)

    for dec in decisions:
        _audit_log(dec)
        logger.info(
            "llm_candidate: %s → %s  (n=%d  ema_success=%.3f)",
            dec["model_id"], dec["decision"],
            dec["dispatches"], dec["ema_success"],
        )
    return decisions


def candidate_stats() -> list[dict]:
    """Return the current trial state for every known candidate."""
    with _conn() as cn:
        rows = cn.execute(
            "SELECT model_id, dispatches, ema_success, ema_latency, "
            "status, started_at, decided_at "
            "FROM llm_candidate_trials"
        ).fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _write_to_registry(model_id: str, cn: sqlite3.Connection,
                       cfg: dict) -> None:
    """Insert / upsert the promoted model into llm_registry with promoted=1.

    brain.llm_router.available_models() already reads this table, so the
    model becomes eligible for the main ensemble on the very next dispatch.
    """
    specs = _candidate_specs(cfg)
    spec = next((s for s in specs if s["id"] == model_id), None)
    if spec is None:
        logger.warning(
            "llm_candidate: promoted model %s not found in brain.yaml candidates",
            model_id,
        )
        return
    now = datetime.now(timezone.utc).isoformat()
    cn.execute(
        """
        INSERT INTO llm_registry
            (id, vendor, payload_json, source, discovered_at, promoted, last_seen_at)
        VALUES (?, ?, ?, 'candidate_trial', ?, 1, ?)
        ON CONFLICT(id) DO UPDATE SET
            promoted=1,
            payload_json=excluded.payload_json,
            last_seen_at=excluded.last_seen_at
        """,
        (
            model_id,
            str(spec.get("vendor", "")),
            json.dumps(spec),
            now,
            now,
        ),
    )


def _audit_log(dec: dict) -> None:
    """Append one markdown row to LLM_CANDIDATE_AUDIT.md."""
    try:
        _AUDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
        header_needed = not _AUDIT_LOG.exists()
        with open(_AUDIT_LOG, "a", encoding="utf-8") as fh:
            if header_needed:
                fh.write(
                    "# LLM Candidate Trial Audit Log\n\n"
                    "| decided_at | model_id | decision | dispatches | ema_success |\n"
                    "|---|---|---|---|---|\n"
                )
            fh.write(
                f"| {dec['decided_at'][:19]} | `{dec['model_id']}` "
                f"| **{dec['decision']}** "
                f"| {dec['dispatches']} | {dec['ema_success']:.4f} |\n"
            )
    except Exception as exc:  # pragma: no cover
        logger.debug("llm_candidate: audit log write failed: %s", exc)


__all__ = [
    "get_active_candidates",
    "tick_candidate",
    "evaluate_candidates",
    "candidate_stats",
]
