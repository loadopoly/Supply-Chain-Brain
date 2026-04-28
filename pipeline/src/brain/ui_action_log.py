"""ui_action_log.py — Lightweight Brain-native User-action telemetry.

Records every page visit, data query, and explicit decision the User makes
inside the Supply Chain Brain UI into the ``learning_log`` table of
``local_brain.sqlite``.  The autonomous agent's Body generators read these
rows back to drive the Brain toward *realizable, User-anchored goals* rather
than purely autonomous ERP-signal goals.

Design rules
------------
- **No Streamlit imports** — this module is imported by both the Streamlit
  app (UI thread) and the autonomous agent (background thread). Streamlit
  must never be imported outside a page script.
- **Fire-and-forget writes** — every public function swallows exceptions so
  a logging failure never crashes a UI interaction.
- **Read-path is also cheap** — ``recent_focus()`` returns the last N rows
  and is called once per Body surface cycle, not on every page render.

Schema (appends to existing ``learning_log`` rows in local_brain.sqlite)
------------------------------------------------------------------------
kind = 'ui_visit'    — page navigation events
kind = 'ui_query'    — data query executions (rows returned, elapsed ms)
kind = 'ui_decision' — explicit user decisions (assign owner, close mission …)

The ``detail`` column holds a JSON blob with event-specific fields.
``signal_strength`` is set to 0.0 for ui_* rows (these are observations,
not quality/confidence scores) so the directionality listener ignores them
when computing learning velocity.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Page → scope-tag mapping (closed vocabulary — mirrors SCOPE_TAGS in quests.py)
# ---------------------------------------------------------------------------
PAGE_SCOPE_MAP: dict[str, str] = {
    "EOQ Deviation":         "inventory_sizing",
    "OTD Recursive":         "fulfillment",
    "Procurement 360":       "sourcing",
    "Data Quality":          "data_quality",
    "Lead-Time Survival":    "lead_time",
    "Bullwhip Effect":       "demand_distortion",
    "Multi-Echelon":         "network_position",
    "Cycle Count Accuracy":  "cycle_count",
    # Non-analytical pages — no scope tag assigned
    "Overview & Graph":      "",
    "Supply Chain Pipeline": "",
    "Query Console":         "",
    "Schema Discovery":      "",
    "Connectors":            "",
    "What-If Sandbox":       "",
    "Decision Log":          "",
    "Benchmarks":            "",
    "Report Creator":        "",
    "Document Analysis":     "",
    "ML Research Hub":       "",
    "Freight Portfolio":     "",
    "Sustainability":        "",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _db_path() -> str:
    """Return path to local_brain.sqlite without importing local_store
    (avoids circular import with knowledge_corpus)."""
    try:
        from src.brain.local_store import db_path
        return str(db_path())
    except Exception:
        # Fallback: resolve relative to this file's location
        from pathlib import Path
        return str(Path(__file__).resolve().parents[2] / "local_brain.sqlite")


@contextmanager
def _conn():
    cn = sqlite3.connect(_db_path(), timeout=5)
    cn.row_factory = sqlite3.Row
    try:
        yield cn
        cn.commit()
    finally:
        cn.close()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write(kind: str, title: str, detail: dict, signal: float = 0.0) -> None:
    """Append one row to learning_log. Silent on failure."""
    try:
        with _conn() as cn:
            cn.execute(
                """INSERT INTO learning_log
                       (logged_at, kind, title, detail, signal_strength)
                   VALUES (?, ?, ?, ?, ?)""",
                (_now(), kind, title, json.dumps(detail, default=str), float(signal)),
            )
    except Exception as exc:
        log.debug(f"ui_action_log._write failed ({kind}): {exc}")


# ---------------------------------------------------------------------------
# Public logging API
# ---------------------------------------------------------------------------

def log_page_visit(page_name: str, site_filter: str = "", extra: dict | None = None) -> None:
    """Record that the User navigated to *page_name*.

    Parameters
    ----------
    page_name:
        The Streamlit page title (``pg.title`` in app.py).
    site_filter:
        The currently selected global site (``st.session_state['g_site']``).
    extra:
        Any additional key-value context (e.g., active filters).
    """
    scope_tag = PAGE_SCOPE_MAP.get(page_name, "")
    detail: dict[str, Any] = {
        "page": page_name,
        "scope_tag": scope_tag,
        "site_filter": site_filter or "",
    }
    if extra:
        detail.update(extra)
    # Signal reflects analytical depth: scoped pages > nav pages; site filter adds focus.
    sig = (0.30 if scope_tag else 0.10) + (0.10 if site_filter else 0.0)
    _write(
        kind="ui_visit",
        title=f"Page visit: {page_name}" + (f" [{site_filter}]" if site_filter else ""),
        detail=detail,
        signal=sig,
    )


def log_query_run(
    page_name: str,
    rows_returned: int,
    site: str = "",
    elapsed_s: float | None = None,
    query_label: str = "",
) -> None:
    """Record that a data query was executed on *page_name*.

    Parameters
    ----------
    page_name:
        The page that triggered the query.
    rows_returned:
        Number of result rows (0 = empty result, worth flagging).
    site:
        Active site filter at time of query.
    elapsed_s:
        Query wall-clock time in seconds.
    query_label:
        Short human-readable description of the query (no SQL text, no PII).
    """
    scope_tag = PAGE_SCOPE_MAP.get(page_name, "")
    detail: dict[str, Any] = {
        "page": page_name,
        "scope_tag": scope_tag,
        "site_filter": site or "",
        "rows_returned": int(rows_returned),
        "empty_result": rows_returned == 0,
    }
    if elapsed_s is not None:
        detail["elapsed_ms"] = round(elapsed_s * 1000)
    if query_label:
        detail["query_label"] = str(query_label)[:200]
    # Signal: scoped pages carry base 0.25; filtered results add focus; empty result is a
    # meaningful gap signal (+0.08) so Body generators notice data-quality issues.
    import math as _math
    _scope_tag = PAGE_SCOPE_MAP.get(page_name, "")
    sig = (0.25 if _scope_tag else 0.12)
    if site:
        sig += 0.10
    if rows_returned == 0:
        sig += 0.08
    elif rows_returned > 0:
        sig += min(0.15, 0.05 * _math.log1p(rows_returned / 10.0))
    sig = round(min(sig, 0.60), 4)
    _write(
        kind="ui_query",
        title=f"Query on {page_name}: {rows_returned} rows" + (f" ({query_label})" if query_label else ""),
        detail=detail,
        signal=sig,
    )


def log_decision(
    action_type: str,
    entity: str = "",
    detail: dict | None = None,
) -> None:
    """Record an explicit User decision inside the UI.

    Call this whenever the User makes a change that the Brain should learn
    from: assigning an OTD owner, creating/closing a Mission, bookmarking a
    finding, acknowledging a directive.

    Parameters
    ----------
    action_type:
        Short verb-noun label: ``"otd_owner_assigned"``,
        ``"mission_created"``, ``"directive_acked"``, etc.
    entity:
        The primary entity affected (``"part:ABC-123"``,
        ``"mission:m_abc123"``, etc.).
    detail:
        Arbitrary structured context. Keep it small — no DataFrames.
    """
    d: dict[str, Any] = {"action_type": action_type}
    if entity:
        d["entity"] = str(entity)
    if detail:
        d.update(detail)
    # Explicit user decisions are the highest-quality UI signal — always 0.72.
    _write(
        kind="ui_decision",
        title=f"User decision: {action_type}" + (f" → {entity}" if entity else ""),
        detail=d,
        signal=0.72,
    )


# ---------------------------------------------------------------------------
# Read-back API (used by Body generators)
# ---------------------------------------------------------------------------

def recent_focus(hours: int = 48, limit: int = 200) -> list[dict]:
    """Return recent ui_* learning_log rows as plain dicts, newest first.

    Returns an empty list on any database error so callers don't need
    exception handling.
    """
    try:
        with _conn() as cn:
            rows = cn.execute(
                """SELECT id, logged_at, kind, title, detail
                   FROM learning_log
                   WHERE kind IN ('ui_visit', 'ui_query', 'ui_decision')
                     AND logged_at >= datetime('now', ?)
                   ORDER BY id DESC LIMIT ?""",
                (f"-{int(hours)} hour", int(limit)),
            ).fetchall()
        result = []
        for r in rows:
            try:
                d = json.loads(r["detail"] or "{}")
            except Exception:
                d = {}
            result.append({
                "id": r["id"],
                "logged_at": r["logged_at"],
                "kind": r["kind"],
                "title": r["title"],
                **d,
            })
        return result
    except Exception as exc:
        log.debug(f"ui_action_log.recent_focus: {exc}")
        return []


def visit_scope_counts(hours: int = 48) -> dict[str, int]:
    """Return {scope_tag: visit_count} for analytical pages in the last *hours* h.

    Only includes pages that map to a SCOPE_TAG (skips nav/utility pages).
    """
    events = recent_focus(hours=hours, limit=500)
    counts: dict[str, int] = {}
    for ev in events:
        if ev.get("kind") != "ui_visit":
            continue
        scope = ev.get("scope_tag", "")
        if scope:
            counts[scope] = counts.get(scope, 0) + 1
    return counts
