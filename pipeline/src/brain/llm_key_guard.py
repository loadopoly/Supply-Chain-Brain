"""LLM Key Guard — credential availability tracking, retry scheduling, and
dimensionality redirection.

When a model's ``endpoint_env`` variable is absent or returning auth errors the
KeyGuard applies exponential backoff so the ensemble is not hammered with doomed
calls.  Between retries it redirects the task to the next highest-scoring model
whose credentials ARE live ("dimensionality redirection").  The rest of the Brain
keeps answering — candidate EMA stats are only updated when an actual network
call occurs, so key-absent periods do not pollute trial scores.

Backoff schedule (per env-var):
    fail_count → cooldown
    0 (first miss)  →  60 s
    1               → 120 s
    2               → 240 s
    …
    ≥5              → 3 600 s (1 hour cap)

On every ``check_key`` call the env-var is re-read live from ``os.environ`` so
keys set at runtime (e.g. by a ``.env`` loader or user action in the UI) are
picked up automatically on the next post-backoff attempt.

Persistent state — SQLite table ``llm_key_state`` in ``local_brain.sqlite``:
    env_var       TEXT  PRIMARY KEY
    available     INTEGER   1 = last check found a live key; 0 = missing/failed
    fail_count    INTEGER   consecutive auth-class failures (drives backoff)
    next_retry_at REAL      Unix timestamp; 0.0 = no active cooldown
    last_checked  REAL      Unix timestamp of last os.environ probe

Public API::

    check_key(env_var)                     -> bool          proceed now?
    record_miss(env_var)                   -> float         seconds to next retry
    record_success(env_var)                -> None
    record_auth_error(env_var)             -> float         seconds to next retry
    redirect_to_best(task, exclude_envs)   -> Decision | None
    key_status()                           -> list[dict]    dashboard view
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from .local_store import db_path

if TYPE_CHECKING:
    from .llm_router import Decision

logger = logging.getLogger(__name__)

_BACKOFF_FLOOR_S: float = 60.0    # first backoff after a miss
_BACKOFF_CAP_S:   float = 3600.0  # ceiling (1 hour)

_DDL = """
CREATE TABLE IF NOT EXISTS llm_key_state (
    env_var       TEXT    NOT NULL PRIMARY KEY,
    available     INTEGER NOT NULL DEFAULT 0,
    fail_count    INTEGER NOT NULL DEFAULT 0,
    next_retry_at REAL    NOT NULL DEFAULT 0.0,
    last_checked  REAL    NOT NULL DEFAULT 0.0
);
"""


@contextmanager
def _conn():
    cn = sqlite3.connect(db_path())
    cn.row_factory = sqlite3.Row
    try:
        cn.executescript(_DDL)
        yield cn
        cn.commit()
    finally:
        cn.close()


# ---------------------------------------------------------------------------
# Backoff helpers
# ---------------------------------------------------------------------------

def _cooldown_s(fail_count: int) -> float:
    """Exponential cooldown capped at _BACKOFF_CAP_S."""
    return min(_BACKOFF_CAP_S, _BACKOFF_FLOOR_S * (2 ** max(0, fail_count - 1)))


def _upsert(cn: sqlite3.Connection, env_var: str, **fields: Any) -> None:
    col_names = ", ".join(fields)
    placeholders = ", ".join(["?"] * len(fields))
    updates = ", ".join(f"{k}=excluded.{k}" for k in fields)
    cn.execute(
        f"INSERT INTO llm_key_state(env_var, {col_names}) VALUES(?, {placeholders}) "
        f"ON CONFLICT(env_var) DO UPDATE SET {updates}",
        (env_var, *fields.values()),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def check_key(env_var: str) -> bool:
    """Return True if the key is present in the environment AND not in backoff.

    Side-effects:
    - If in backoff: returns False immediately without touching the env.
    - If not in backoff: probes ``os.environ`` live.
      - Key found   → calls record_success, returns True.
      - Key missing → calls record_miss, returns False.
    """
    if not env_var:
        return False
    now = time.time()
    with _conn() as cn:
        row = cn.execute(
            "SELECT fail_count, next_retry_at FROM llm_key_state WHERE env_var=?",
            (env_var,),
        ).fetchone()
    if row is not None and float(row["next_retry_at"]) > now:
        secs = float(row["next_retry_at"]) - now
        logger.debug("key_guard: %s in backoff for %.0fs", env_var, secs)
        return False

    if os.environ.get(env_var):
        record_success(env_var)
        return True

    record_miss(env_var)
    return False


def record_miss(env_var: str) -> float:
    """Record a key-missing event; schedule the next retry.  Returns cooldown seconds."""
    now = time.time()
    with _conn() as cn:
        row = cn.execute(
            "SELECT fail_count FROM llm_key_state WHERE env_var=?", (env_var,)
        ).fetchone()
        fc = (int(row["fail_count"]) + 1) if row else 1
        cd = _cooldown_s(fc)
        _upsert(cn, env_var, available=0, fail_count=fc,
                next_retry_at=now + cd, last_checked=now)
    logger.info(
        "key_guard: %s not set — next check in %.0fs (fail_count=%d)",
        env_var, cd, fc,
    )
    return cd


def record_auth_error(env_var: str) -> float:
    """Record an authentication failure (401/403) for a present-but-invalid key."""
    now = time.time()
    with _conn() as cn:
        row = cn.execute(
            "SELECT fail_count FROM llm_key_state WHERE env_var=?", (env_var,)
        ).fetchone()
        fc = (int(row["fail_count"]) + 1) if row else 1
        cd = _cooldown_s(fc)
        _upsert(cn, env_var, available=0, fail_count=fc,
                next_retry_at=now + cd, last_checked=now)
    logger.warning(
        "key_guard: %s returned auth error — backoff %.0fs (fail_count=%d)",
        env_var, cd, fc,
    )
    return cd


def record_success(env_var: str) -> None:
    """Reset backoff state after a successful call using this key."""
    now = time.time()
    with _conn() as cn:
        _upsert(cn, env_var, available=1, fail_count=0,
                next_retry_at=0.0, last_checked=now)


def redirect_to_best(task: str,
                     exclude_envs: set[str] | None = None) -> "Decision | None":
    """Return the highest-scoring model for ``task`` whose key IS live.

    Excludes models whose ``endpoint_env`` is in ``exclude_envs`` OR whose key
    is currently in backoff.  This is the 'dimensionality redirection' step:
    when a preferred model is credential-locked, the Brain routes through the
    next viable dimension rather than blocking.

    Returns None when no live candidate is found (caller should fall back to
    the main ensemble's offline / compute-grid path).
    """
    from .llm_router import rank_llms  # deferred — avoids circular imports
    exclude_envs = exclude_envs or set()
    for d in rank_llms(task):
        if not d.passed_filters:
            continue
        env = d.endpoint_env or ""
        if env in exclude_envs:
            continue
        if env and not check_key(env):
            continue
        # No env_var set means the model uses the registered OR caller (free-tier).
        return d
    return None


def key_status() -> list[dict]:
    """Return the full key-state table (for dashboards / the Brain UI)."""
    with _conn() as cn:
        rows = cn.execute(
            "SELECT env_var, available, fail_count, next_retry_at, last_checked "
            "FROM llm_key_state ORDER BY env_var"
        ).fetchall()
    now = time.time()
    out = []
    for r in rows:
        nra = float(r["next_retry_at"])
        out.append({
            "env_var":      r["env_var"],
            "available":    bool(r["available"]),
            "fail_count":   r["fail_count"],
            "in_backoff":   nra > now,
            "retry_in_s":   max(0.0, nra - now),
            "last_checked": r["last_checked"],
        })
    return out


__all__ = [
    "check_key",
    "record_miss",
    "record_success",
    "record_auth_error",
    "redirect_to_best",
    "key_status",
]
