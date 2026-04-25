"""Resumption Manager — learning continuity across hard shutdowns.

Responsibilities
----------------
1. **Alive stamping**: the agent calls ``stamp_alive()`` every ~60 s so the
   DB always holds a fresh "machine is up" epoch.

2. **Downtime detection**: on every startup ``detect_downtime()`` computes the
   gap between the last alive stamp and *now*.  Any gap > 5 minutes is treated
   as unplanned downtime and logged to ``logs/downtime_log.json``.

3. **Cloud queue ingestion**: while this machine was offline, GitHub Actions
   cloud runners continued learning and committed new entries to
   ``cloud_learning_queue.jsonl`` in the repo root.  ``ingest_cloud_queue()``
   reads that file, inserts new learning_log rows, and advances a cursor so
   entries are never double-imported.

4. **Catch-up burst**: ``schedule_catchup_burst()`` writes a transient
   brain_kv key that instructs the corpus round and synaptic workers to run
   at elevated batch sizes for one cycle to recoup lost learning time.

5. **Graceful shutdown stamp**: call ``stamp_graceful_shutdown()`` before a
   clean exit so the next startup can distinguish planned pause from crash.

Migration path
--------------
If the local machine goes down hard:

    Layer 1 — OneDrive sync
        The entire ``VS Code/pipeline/`` folder (including local_brain.sqlite)
        is inside an OneDrive-managed directory.  A replacement machine that
        signs into the same Microsoft 365 account receives the full DB
        automatically once OneDrive syncs (~minutes).  The bootstrap script
        (``bootstrap_new_machine.ps1``) then creates the venv, installs deps,
        and re-registers the watchdog Scheduled Task.

    Layer 2 — GitHub Actions cloud queue
        A scheduled GH Actions workflow runs every 4 hours on GitHub-hosted
        runners, performs OCW / ML-research ingest against a cached temp DB,
        and appends new learning events to ``cloud_learning_queue.jsonl`` which
        is committed back to ``main``.  This file is pulled and ingested here
        on the first post-downtime startup.

    Layer 3 — Full corpus rebuild
        If both the machine and OneDrive state are unrecoverable, setting all
        ``corpus_cursor`` values to 0 and running ``refresh_corpus_round()``
        will fully re-derive the knowledge graph from public sources (OCW,
        arxiv, missions, SCB Grok export).  Brain_kv state (plasticity dials,
        rADAM optimizer, etc.) is lost but all learnable content is recovered.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .local_store import db_path as _local_db_path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_PIPELINE_ROOT = Path(__file__).resolve().parents[2]
_LOGS_DIR      = _PIPELINE_ROOT / "logs"
_DOWNTIME_LOG  = _LOGS_DIR / "downtime_log.json"
_CLOUD_QUEUE   = _PIPELINE_ROOT / "cloud_learning_queue.jsonl"

# brain_kv key names
_KEY_LAST_ALIVE     = "resumption:last_alive"
_KEY_GRACEFUL_STOP  = "resumption:graceful_shutdown"
_KEY_CATCHUP_BURST  = "resumption:catchup_burst"
_KEY_CLOUD_CURSOR   = "resumption:cloud_queue_cursor"

# Downtime threshold: gaps shorter than this (seconds) are noise (sleep,
# screensaver) and not treated as real downtime.
_DOWNTIME_MIN_S = 300  # 5 minutes


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@dataclass
class DowntimeReport:
    gap_seconds: float        = 0.0
    is_downtime: bool         = False
    was_graceful: bool        = False
    last_alive_iso: str       = ""
    resumed_at_iso: str       = ""
    cloud_entries_ingested: int = 0
    message: str              = ""


def stamp_alive(cn: sqlite3.Connection | None = None) -> None:
    """Write the current epoch to brain_kv.  Call every ~60 s from the agent loop."""
    _kv_set(_open_if_none(cn), _KEY_LAST_ALIVE, str(int(time.time())))


def stamp_graceful_shutdown(cn: sqlite3.Connection | None = None) -> None:
    """Mark that the next gap in alive stamps was intentional."""
    _kv_set(_open_if_none(cn), _KEY_GRACEFUL_STOP, str(int(time.time())))


def detect_downtime(cn: sqlite3.Connection | None = None) -> DowntimeReport:
    """Called once at startup.  Returns a DowntimeReport and side-effects:
    - logs the downtime window to ``logs/downtime_log.json``
    - ingests ``cloud_learning_queue.jsonl``
    - writes a catchup burst key if downtime was significant
    """
    _cn = _open_if_none(cn)
    now = int(time.time())
    now_iso = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()

    last_alive   = _kv_int(_cn, _KEY_LAST_ALIVE)
    graceful_at  = _kv_int(_cn, _KEY_GRACEFUL_STOP)

    report = DowntimeReport(resumed_at_iso=now_iso)

    if last_alive == 0:
        # First ever run — no downtime to record
        report.message = "first_run"
        stamp_alive(_cn)
        return report

    gap = now - last_alive
    was_graceful = (graceful_at > 0 and abs(graceful_at - last_alive) < 120)

    report.gap_seconds  = float(gap)
    report.last_alive_iso = datetime.fromtimestamp(last_alive, tz=timezone.utc).isoformat()
    report.was_graceful = was_graceful
    report.is_downtime  = (gap > _DOWNTIME_MIN_S)

    if report.is_downtime:
        _record_downtime_window(last_alive, now, was_graceful)
        gap_min = round(gap / 60, 1)
        kind = "graceful" if was_graceful else "HARD_CRASH"
        logging.warning(
            f"resumption_manager: {kind} downtime detected — {gap_min} min gap "
            f"(last_alive={report.last_alive_iso})"
        )
        # Ingest any cloud-queued learnings accumulated while we were offline
        try:
            ingested = ingest_cloud_queue(_cn)
            report.cloud_entries_ingested = ingested
            if ingested:
                logging.info(f"resumption_manager: ingested {ingested} cloud queue entries")
        except Exception as exc:
            logging.error(f"resumption_manager: cloud queue ingest failed: {exc}")

        # Schedule a catch-up burst proportional to downtime
        schedule_catchup_burst(_cn, gap)
        report.message = f"{kind}: {gap_min} min; cloud_ingested={report.cloud_entries_ingested}"
    else:
        report.message = "normal_startup"

    # Clear graceful flag and stamp alive
    _kv_set(_cn, _KEY_GRACEFUL_STOP, "0")
    stamp_alive(_cn)
    _cn.commit()
    return report


def ingest_cloud_queue(cn: sqlite3.Connection | None = None) -> int:
    """Import learning events from cloud_learning_queue.jsonl into learning_log.

    Returns the number of new entries ingested.  Uses a line-number cursor
    stored in brain_kv so entries are never double-imported.
    """
    if not _CLOUD_QUEUE.exists():
        return 0

    _cn = _open_if_none(cn)
    cursor_line = _kv_int(_cn, _KEY_CLOUD_CURSOR)  # last line already ingested (1-based)

    lines = _CLOUD_QUEUE.read_text(encoding="utf-8").splitlines()
    new_lines = lines[cursor_line:]  # 0-indexed: skip already-processed
    if not new_lines:
        return 0

    ingested = 0
    for raw_line in new_lines:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            entry = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        # Insert into learning_log
        try:
            _cn.execute(
                "INSERT OR IGNORE INTO learning_log"
                "(logged_at, kind, title, detail, signal_strength, source_table, source_row_id)"
                " VALUES (?,?,?,?,?,?,?)",
                (
                    entry.get("logged_at", datetime.now(tz=timezone.utc).isoformat()),
                    entry.get("kind",   "cloud_learning"),
                    entry.get("title",  ""),
                    json.dumps(entry.get("detail", {})),
                    float(entry.get("signal", 0.5)),
                    "cloud_queue",
                    entry.get("cloud_run_id", ""),
                ),
            )
            ingested += 1
        except Exception as exc:
            logging.debug(f"resumption_manager: cloud entry insert failed: {exc}")

    # Advance cursor
    _kv_set(_cn, _KEY_CLOUD_CURSOR, str(cursor_line + len(new_lines)))
    _cn.commit()
    return ingested


def schedule_catchup_burst(cn: sqlite3.Connection | None = None,
                           downtime_seconds: float = 3600) -> None:
    """Write a transient brain_kv key that signals corpus/synaptic workers
    to run at elevated batch sizes for the next cycle.

    The burst multiplier scales with downtime:
      ≤ 1 h  → 1.5×
      ≤ 6 h  → 2.0×
      ≤ 24 h → 3.0×
       > 24 h → 4.0×  (full rebuild cadence)
    """
    if downtime_seconds <= 3600:
        multiplier = 1.5
    elif downtime_seconds <= 21600:
        multiplier = 2.0
    elif downtime_seconds <= 86400:
        multiplier = 3.0
    else:
        multiplier = 4.0

    payload = {
        "multiplier":       multiplier,
        "downtime_seconds": int(downtime_seconds),
        "issued_at":        datetime.now(tz=timezone.utc).isoformat(),
        "expires_after":    1,   # consumed after 1 corpus round
    }
    _cn = _open_if_none(cn)
    _kv_set(_cn, _KEY_CATCHUP_BURST, json.dumps(payload))
    _cn.commit()
    logging.info(
        f"resumption_manager: catchup burst scheduled "
        f"({multiplier}× for {round(downtime_seconds/3600,1)} h downtime)"
    )


def consume_catchup_burst(cn: sqlite3.Connection | None = None) -> float:
    """Read and consume the catchup burst multiplier (returns 1.0 if none active).

    Call once at the start of each corpus round.  After reading, the key is
    cleared so the burst only applies for a single round.
    """
    _cn = _open_if_none(cn)
    raw = _kv_str(_cn, _KEY_CATCHUP_BURST)
    if not raw:
        return 1.0
    try:
        payload = json.loads(raw)
        multiplier = float(payload.get("multiplier", 1.0))
        expires = int(payload.get("expires_after", 1))
        if expires <= 1:
            _kv_set(_cn, _KEY_CATCHUP_BURST, "")
        else:
            payload["expires_after"] = expires - 1
            _kv_set(_cn, _KEY_CATCHUP_BURST, json.dumps(payload))
        _cn.commit()
        return multiplier
    except Exception:
        return 1.0


def run_resumption_check(cn: sqlite3.Connection | None = None) -> DowntimeReport:
    """Orchestrate all startup checks: detect downtime, pull latest cloud
    queue, ingest it, schedule burst.  Call once at agent startup.

    Also performs a best-effort ``git pull`` so cloud_learning_queue.jsonl
    is current before ingestion.  Safe to call even on the very first run.
    """
    _cn = _open_if_none(cn)
    # Pull latest so cloud_learning_queue.jsonl is up to date
    git_pull_latest()
    report = detect_downtime(_cn)
    _cn.commit()
    return report


def git_pull_latest(repo_root: Path | None = None) -> bool:
    """Pull latest commits from origin so cloud_learning_queue.jsonl is current.

    Returns True on success.  Safe to call on startup even if git is absent —
    failures are logged and swallowed.
    """
    root = repo_root or _PIPELINE_ROOT.parent
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "pull", "--ff-only", "origin", "main"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            logging.info(f"resumption_manager: git pull OK — {result.stdout.strip()[:100]}")
            return True
        else:
            logging.warning(f"resumption_manager: git pull failed — {result.stderr.strip()[:200]}")
            return False
    except Exception as exc:
        logging.warning(f"resumption_manager: git pull error: {exc}")
        return False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _open_if_none(cn: sqlite3.Connection | None) -> sqlite3.Connection:
    if cn is not None:
        return cn
    _cn = sqlite3.connect(str(_local_db_path()), timeout=15)
    _cn.row_factory = sqlite3.Row
    return _cn


def _kv_set(cn: sqlite3.Connection, key: str, value: str) -> None:
    cn.execute(
        "CREATE TABLE IF NOT EXISTS brain_kv("
        "key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)"
    )
    cn.execute(
        "INSERT OR REPLACE INTO brain_kv(key, value, updated_at) VALUES(?,?,?)",
        (key, value, datetime.now(tz=timezone.utc).isoformat()),
    )


def _kv_str(cn: sqlite3.Connection, key: str) -> str:
    try:
        row = cn.execute("SELECT value FROM brain_kv WHERE key=?", (key,)).fetchone()
        return (row[0] or "") if row else ""
    except Exception:
        return ""


def _kv_int(cn: sqlite3.Connection, key: str) -> int:
    val = _kv_str(cn, key)
    try:
        return int(float(val)) if val else 0
    except (ValueError, TypeError):
        return 0


def _record_downtime_window(start_epoch: int, end_epoch: int, graceful: bool) -> None:
    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    windows: list[dict] = []
    if _DOWNTIME_LOG.exists():
        try:
            data = json.loads(_DOWNTIME_LOG.read_text(encoding="utf-8"))
            windows = data.get("windows", [])
        except Exception:
            windows = []

    windows.append({
        "start":     start_epoch,
        "end":       end_epoch,
        "seconds":   end_epoch - start_epoch,
        "graceful":  graceful,
        "start_iso": datetime.fromtimestamp(start_epoch, tz=timezone.utc).isoformat(),
        "end_iso":   datetime.fromtimestamp(end_epoch,   tz=timezone.utc).isoformat(),
    })
    # Keep last 1000 windows
    windows = windows[-1000:]
    _DOWNTIME_LOG.write_text(
        json.dumps({"windows": windows}, indent=2), encoding="utf-8"
    )
