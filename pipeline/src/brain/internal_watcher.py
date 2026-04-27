"""Internal watcher agent for autonomous_agent.py.

This replaces the assumption that Windows Scheduled Tasks are required for
learning continuity. The watcher is a Python supervisor that belongs to the
Brain itself: it launches the learning loop as a child process, records its own
heartbeat/status, restarts the child on exit, records downtime windows, and
keeps the resumption heartbeat fresh while the child is alive.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .local_store import db_path


PIPELINE_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PIPELINE_ROOT / "logs"
WATCHER_LOG = LOG_DIR / "internal_agent_watcher.log"
WATCHER_HEARTBEAT = LOG_DIR / "internal_agent_watcher_heartbeat.txt"
WATCHER_STATUS = LOG_DIR / "internal_agent_watcher_status.json"
DOWNTIME_LOG = LOG_DIR / "downtime_log.json"

CHILD_ENV = "SCB_INTERNAL_WATCHER_CHILD"
DISABLE_ENV = "SCB_DISABLE_INTERNAL_WATCHER"
CHILD_ARG = "--agent-child"
NO_WATCH_ARG = "--no-internal-watcher"

_KEY_HEARTBEAT = "internal_watcher:heartbeat"
_KEY_STATE = "internal_watcher:state"


def should_supervise(argv: list[str] | None = None) -> bool:
    """Return True when this process should become the internal watcher."""
    args = list(sys.argv[1:] if argv is None else argv)
    if os.environ.get(CHILD_ENV) == "1":
        return False
    if os.environ.get(DISABLE_ENV) == "1":
        return False
    if CHILD_ARG in args or NO_WATCH_ARG in args:
        return False
    return True


def run_supervisor(
    *,
    python_exe: str | None = None,
    agent_script: Path | None = None,
    heartbeat_interval_s: int = 30,
    restart_delay_s: int = 15,
) -> int:
    """Run the internal watcher loop until interrupted.

    The watched child receives ``SCB_INTERNAL_WATCHER_CHILD=1`` plus
    ``--agent-child`` so it runs the learning loop instead of recursively
    becoming another watcher.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    _configure_logging()

    python = python_exe or sys.executable or "python"
    script = agent_script or (PIPELINE_ROOT / "autonomous_agent.py")
    if not script.exists():
        _log("FATAL: autonomous_agent.py not found at %s", script)
        return 1

    _log("Internal watcher online. Agent=%s Python=%s", script, python)
    _write_state(agent_state="starting", restarts=0, agent_pid=None)

    restarts = 0
    down_since: float | None = None
    stopping = False
    child: subprocess.Popen[Any] | None = None

    def _stop(signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True
        _log("Internal watcher received signal %s; stopping child.", signum)
        if child is not None and child.poll() is None:
            try:
                child.terminate()
            except Exception:
                pass

    for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if sig is not None:
            try:
                signal.signal(sig, _stop)
            except Exception:
                pass

    while not stopping:
        env = os.environ.copy()
        env[CHILD_ENV] = "1"
        env["SCB_INTERNAL_WATCHER_PID"] = str(os.getpid())
        cmd = [python, str(script), CHILD_ARG]

        try:
            child = subprocess.Popen(cmd, cwd=str(PIPELINE_ROOT), env=env)
        except Exception as exc:
            now = time.time()
            down_since = down_since or now
            restarts += 1
            _log("Failed to start child: %s", exc)
            _write_state(
                agent_state="start_failed",
                restarts=restarts,
                agent_pid=None,
                last_error=str(exc),
            )
            time.sleep(restart_delay_s)
            continue

        _log("Agent child started. PID=%s", child.pid)
        if down_since is not None:
            _record_downtime_window(down_since, time.time(), "internal_watcher")
            down_since = None

        _write_state(agent_state="running", restarts=restarts, agent_pid=child.pid)

        while not stopping:
            try:
                exit_code = child.wait(timeout=heartbeat_interval_s)
                break
            except subprocess.TimeoutExpired:
                _write_heartbeat(child.pid, restarts)
                _stamp_child_alive()
                _write_state(agent_state="running", restarts=restarts, agent_pid=child.pid)

        if stopping:
            break

        restarts += 1
        down_since = time.time()
        _log("Agent child exited with code=%s. Restarting in %ss.", exit_code, restart_delay_s)
        _write_state(
            agent_state="restarting",
            restarts=restarts,
            agent_pid=None,
            last_exit_code=exit_code,
        )
        time.sleep(restart_delay_s)

    if down_since is not None:
        _record_downtime_window(down_since, time.time(), "internal_watcher_stop")
    _write_state(agent_state="stopped", restarts=restarts, agent_pid=None)
    _log("Internal watcher stopped.")
    return 0


def _configure_logging() -> None:
    logging.basicConfig(
        filename=str(WATCHER_LOG),
        level=logging.INFO,
        format="%(asctime)s - [INTERNAL WATCHER] - %(message)s",
        force=True,
    )


def _log(message: str, *args: Any) -> None:
    logging.info(message, *args)
    text = message % args if args else message
    print(f"{_now_iso()} [INTERNAL WATCHER] {text}", flush=True)


def _write_heartbeat(agent_pid: int | None, restarts: int) -> None:
    epoch = int(time.time())
    WATCHER_HEARTBEAT.write_text(str(epoch), encoding="utf-8")
    _kv_set(_KEY_HEARTBEAT, str(epoch))
    _kv_set(
        _KEY_STATE,
        json.dumps(
            {
                "watcher_pid": os.getpid(),
                "agent_pid": agent_pid,
                "restarts": restarts,
                "updated_at": _now_iso(),
            },
            sort_keys=True,
        ),
    )


def _write_state(
    *,
    agent_state: str,
    restarts: int,
    agent_pid: int | None,
    last_exit_code: int | None = None,
    last_error: str | None = None,
) -> None:
    payload = {
        "agent_pid": agent_pid,
        "agent_state": agent_state,
        "last_error": last_error,
        "last_exit_code": last_exit_code,
        "restarts": restarts,
        "updated_at": _now_iso(),
        "watcher_pid": os.getpid(),
    }
    WATCHER_STATUS.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    _kv_set(_KEY_STATE, json.dumps(payload, sort_keys=True))


def _stamp_child_alive() -> None:
    try:
        from .resumption_manager import stamp_alive

        with sqlite3.connect(str(db_path()), timeout=5) as cn:
            stamp_alive(cn)
            cn.commit()
    except Exception as exc:
        logging.debug("internal_watcher: stamp_alive failed: %s", exc)


def _record_downtime_window(start_epoch: float, end_epoch: float, source: str) -> None:
    seconds = int(end_epoch - start_epoch)
    if seconds <= 60:
        return

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    windows: list[dict[str, Any]] = []
    if DOWNTIME_LOG.exists():
        try:
            raw = json.loads(DOWNTIME_LOG.read_text(encoding="utf-8") or "{}")
            windows = list(raw.get("windows") or [])
        except Exception:
            windows = []

    windows.append(
        {
            "end": int(end_epoch),
            "end_iso": datetime.fromtimestamp(end_epoch, timezone.utc).isoformat(),
            "seconds": seconds,
            "source": source,
            "start": int(start_epoch),
            "start_iso": datetime.fromtimestamp(start_epoch, timezone.utc).isoformat(),
        }
    )
    if len(windows) > 500:
        windows = windows[-500:]
    DOWNTIME_LOG.write_text(json.dumps({"windows": windows}, indent=2), encoding="utf-8")

    try:
        from .resumption_manager import schedule_catchup_burst

        with sqlite3.connect(str(db_path()), timeout=5) as cn:
            schedule_catchup_burst(cn, float(seconds))
            cn.commit()
    except Exception as exc:
        logging.debug("internal_watcher: catchup burst schedule failed: %s", exc)


def _kv_set(key: str, value: str) -> None:
    try:
        with sqlite3.connect(str(db_path()), timeout=5) as cn:
            cn.execute(
                "CREATE TABLE IF NOT EXISTS brain_kv ("
                "key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            cn.execute(
                "INSERT OR REPLACE INTO brain_kv(key, value, updated_at) VALUES(?,?,?)",
                (key, value, _now_iso()),
            )
            cn.commit()
    except Exception as exc:
        logging.debug("internal_watcher: kv_set(%s) failed: %s", key, exc)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    raise SystemExit(run_supervisor())
