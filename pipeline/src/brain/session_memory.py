"""session_memory.py — Brain-native session recall module.

Wraps the ``session-recall`` CLI (installed via ``pip install auto-memory``)
as Python functions that slot into the Brain's learning loop.  Every call to
:func:`orient_agent` produces a structured snapshot of the developer's recent
Copilot sessions and writes each session summary as a ``session_recall`` entry
in ``learning_log`` so the Brain accumulates a durable audit trail of what the
human and AI have been working on together.

If ``session-recall`` is unavailable (database not found, CLI not installed, or
any other error) every function degrades gracefully — callers receive empty
structures and no exception propagates into the agent loop.
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _run_recall(*args: str, timeout: int = 15) -> list[dict] | dict | None:
    """Run ``session-recall <args>`` and parse JSON output.

    Returns the parsed JSON value (list or dict) or ``None`` on any failure.
    """
    cmd = ["session-recall", *args]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None
        return json.loads(result.stdout)
    except FileNotFoundError:
        log.debug("session-recall not installed; skipping session recall.")
        return None
    except subprocess.TimeoutExpired:
        log.debug("session-recall timed out; skipping.")
        return None
    except Exception as exc:
        log.debug(f"session-recall error: {exc}")
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_recent_context(
    file_limit: int = 10,
    session_limit: int = 5,
) -> dict[str, Any]:
    """Return a dict with recent Copilot session context.

    Keys
    ----
    ``files``
        List of recently touched file paths (strings).
    ``sessions``
        List of recent session summary dicts as returned by
        ``session-recall list --json``.
    ``health``
        ``"ok"`` if at least one file or session was retrieved, else
        ``"unavailable"``.
    """
    files_raw    = _run_recall("files",  "--json", "--limit", str(file_limit))
    sessions_raw = _run_recall("list",   "--json", "--limit", str(session_limit))

    files    = files_raw    if isinstance(files_raw,    list) else []
    sessions = sessions_raw if isinstance(sessions_raw, list) else []

    return {
        "files":    files,
        "sessions": sessions,
        "health":   "ok" if (files or sessions) else "unavailable",
    }


def orient_agent(
    file_limit: int = 10,
    session_limit: int = 5,
) -> dict[str, Any]:
    """Orient the Brain from recent session context and persist to corpus.

    Calls :func:`get_recent_context`, then writes each retrieved session
    as a ``session_recall`` entry in ``learning_log`` so the Brain treats
    recent developer work as first-class signals.  Previously persisted
    sessions are not re-written (title de-dup by session id/timestamp).

    Returns the same structure as :func:`get_recent_context` augmented with
    ``learnings_written`` (int).
    """
    ctx = get_recent_context(file_limit=file_limit, session_limit=session_limit)
    if ctx["health"] == "unavailable":
        log.debug("session_memory.orient_agent: no session data available.")
        ctx["learnings_written"] = 0
        return ctx

    written = 0
    try:
        # Lazy import so this module can be loaded even if corpus isn't ready.
        import sys as _sys
        _pipeline_root = str(Path(__file__).resolve().parents[2])
        if _pipeline_root not in _sys.path:
            _sys.path.insert(0, _pipeline_root)

        import sqlite3
        from src.brain.local_store import db_path as _db_path

        db = _db_path()
        with sqlite3.connect(db) as cn:
            cn.row_factory = sqlite3.Row
            # Ensure learning_log table exists (created by knowledge_corpus.init_schema)
            try:
                cn.execute(
                    """CREATE TABLE IF NOT EXISTS learning_log (
                       id             INTEGER PRIMARY KEY AUTOINCREMENT,
                       logged_at      TEXT    NOT NULL,
                       kind           TEXT    NOT NULL,
                       title          TEXT    NOT NULL,
                       detail         TEXT,
                       signal_strength REAL,
                       source_table   TEXT,
                       source_row_id  INTEGER
                    )"""
                )
                cn.commit()
            except Exception:
                pass

            for sess in ctx["sessions"]:
                if not isinstance(sess, dict):
                    continue
                # Use session id or timestamp as unique key for de-dup
                sess_id = sess.get("id") or sess.get("session_id") or sess.get("timestamp", "")
                title   = f"[session-recall] {sess_id}"

                # Skip if already logged
                existing = cn.execute(
                    "SELECT id FROM learning_log WHERE kind='session_recall' AND title=?",
                    (title,),
                ).fetchone()
                if existing:
                    continue

                now = datetime.now(timezone.utc).isoformat()
                detail_payload = {
                    "session": sess,
                    "recent_files": ctx["files"][:5],
                }
                cn.execute(
                    """INSERT INTO learning_log(logged_at, kind, title, detail,
                              signal_strength)
                       VALUES(?,?,?,?,?)""",
                    (
                        now,
                        "session_recall",
                        title,
                        json.dumps(detail_payload, default=str),
                        0.5,
                    ),
                )
                written += 1

            cn.commit()

    except Exception as exc:
        log.warning(f"session_memory: failed to write learnings: {exc}")

    ctx["learnings_written"] = written
    log.info(
        f"session_memory.orient_agent: "
        f"files={len(ctx['files'])}, sessions={len(ctx['sessions'])}, "
        f"learnings_written={written}"
    )
    return ctx
