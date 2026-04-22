"""LLM Scout — periodic internet review for newly released open-weight models.

Runs on a configurable cadence (default weekly per `llms.scout.interval_hours`)
and polls public catalogs for newly released open-weight LLMs:
    * HuggingFace        /api/models   (filter=text-generation, sort=likes7d)
    * OpenRouter         /api/v1/models (open-weight flag)
    * lmarena            leaderboard (Elo)
    * artificialanalysis model index

Each candidate is normalized into the same schema as `config/brain.yaml ->
llms.registry`, scored against `accept_rules`, and persisted to
`local_brain.sqlite.llm_registry`. Newcomers passing all rules are
auto-promoted (`promoted = 1`) so `brain.llm_router` will consider them on the
next call. Every sweep also writes a markdown audit row to
`pipeline/docs/LLM_SCOUT_AUDIT.md` so the decisioning is fully traceable.

Network calls degrade gracefully — if a source is unreachable, that source is
skipped and noted in the audit; the router keeps using the YAML seed.

Public API:
    refresh_llm_registry(force: bool = False) -> ScoutReport
    schedule_in_background()  # daemon thread; safe no-op if disabled
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from . import load_config
from .local_store import db_path


_HEARTBEAT_KEY = "llm_scout.last_run"
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
@contextmanager
def _conn():
    cn = sqlite3.connect(db_path())
    try:
        cn.executescript(
            """
            CREATE TABLE IF NOT EXISTS llm_registry (
                id TEXT PRIMARY KEY,
                vendor TEXT,
                payload_json TEXT NOT NULL,
                source TEXT,
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                promoted INTEGER DEFAULT 0,
                last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS brain_kv (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        yield cn
        cn.commit()
    finally:
        cn.close()


def _kv_get(key: str) -> str | None:
    with _conn() as cn:
        cur = cn.execute("SELECT value FROM brain_kv WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row else None


def _kv_set(key: str, value: str) -> None:
    with _conn() as cn:
        cn.execute(
            "INSERT INTO brain_kv(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
            "updated_at=CURRENT_TIMESTAMP",
            (key, value),
        )


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------
@dataclass
class ScoutReport:
    started_at: str
    finished_at: str
    sources_polled: list[str] = field(default_factory=list)
    sources_failed: list[dict[str, str]] = field(default_factory=list)
    candidates_seen: int = 0
    promoted_ids: list[str] = field(default_factory=list)
    rejected_ids: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__


# ---------------------------------------------------------------------------
# Source adapters — each returns a list[dict] in our normalized schema
# ---------------------------------------------------------------------------
def _http_get(url: str, params: dict | None = None, timeout: int = 15) -> Any:
    """Tiny requests wrapper. Imported lazily so the Brain runs without it."""
    import requests
    headers = {"User-Agent": "scbrain-llm-scout/1.0"}
    r = requests.get(url, params=params or {}, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _from_huggingface(src: dict) -> list[dict[str, Any]]:
    data = _http_get(src["url"], src.get("query"))
    out: list[dict[str, Any]] = []
    for m in data or []:
        mid = m.get("modelId") or m.get("id")
        if not mid:
            continue
        out.append({
            "id": mid.lower(),
            "vendor": (mid.split("/")[0] if "/" in mid else "unknown").lower(),
            "params_b": _safe_int(m.get("config", {}).get("num_parameters", 0)) // 1_000_000_000 or 0,
            "ctx_window": _safe_int(m.get("config", {}).get("max_position_embeddings", 0)),
            "license": (m.get("cardData") or {}).get("license", "unknown"),
            "released": (m.get("createdAt") or "")[:10],
            "likes_7d": _safe_int(m.get("likes", 0)),
            "downloads_30d": _safe_int(m.get("downloads", 0)),
            "tags": m.get("tags") or [],
            "_source": "huggingface",
        })
    return out


def _from_openrouter(src: dict) -> list[dict[str, Any]]:
    data = _http_get(src["url"]).get("data", [])
    out = []
    for m in data:
        mid = m.get("id") or ""
        if not mid:
            continue
        pricing = m.get("pricing", {}) or {}
        out.append({
            "id": mid.lower(),
            "vendor": (mid.split("/")[0] if "/" in mid else "unknown").lower(),
            "ctx_window": _safe_int(m.get("context_length", 0)),
            "license": "see-vendor",
            "cost_per_mtok_in": float(pricing.get("prompt", 0)) * 1_000_000,
            "cost_per_mtok_out": float(pricing.get("completion", 0)) * 1_000_000,
            "tags": ["openrouter"],
            "_source": "openrouter",
        })
    return out


def _from_lmarena(src: dict) -> list[dict[str, Any]]:
    data = _http_get(src["url"])
    rows = data.get("leaderboard") or data or []
    out = []
    for r in rows:
        mid = (r.get("model") or r.get("name") or "").lower()
        if not mid:
            continue
        out.append({
            "id": mid,
            "vendor": (mid.split("/")[0] if "/" in mid else "unknown"),
            "arena_elo": _safe_int(r.get("rating") or r.get("elo") or 0),
            "tags": ["lmarena"],
            "_source": "lmarena",
        })
    return out


def _from_artificial_analysis(src: dict) -> list[dict[str, Any]]:
    data = _http_get(src["url"])
    rows = data.get("models") or data or []
    out = []
    for r in rows:
        mid = (r.get("model_id") or r.get("name") or "").lower()
        if not mid:
            continue
        out.append({
            "id": mid,
            "vendor": (r.get("creator") or "unknown").lower(),
            "ctx_window": _safe_int(r.get("context_window", 0)),
            "median_latency_ms": _safe_int(r.get("median_latency_ms", 0)),
            "cost_per_mtok_in": float(r.get("price_input", 0) or 0),
            "cost_per_mtok_out": float(r.get("price_output", 0) or 0),
            "tags": r.get("tags") or [],
            "_source": "artificial_analysis",
        })
    return out


_ADAPTERS = {
    "huggingface":          _from_huggingface,
    "openrouter":           _from_openrouter,
    "lmarena":              _from_lmarena,
    "artificial_analysis":  _from_artificial_analysis,
}


def _safe_int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Acceptance & persistence
# ---------------------------------------------------------------------------
def _accept(candidate: dict, rules: dict) -> tuple[bool, str]:
    if rules.get("open_weights_only", True):
        lic = str(candidate.get("license", "")).lower()
        bad = {x.lower() for x in rules.get("exclude_licenses", [])}
        if lic in bad or lic in {"proprietary", "noncommercial", "research-only"}:
            return False, f"license '{lic}' is not open"
    # Free-tier gate: zero cost in *and* out, OR explicit ':free' suffix on id.
    if rules.get("free_tier_only", True):
        cin = float(candidate.get("cost_per_mtok_in", 0) or 0)
        cout = float(candidate.get("cost_per_mtok_out", 0) or 0)
        is_free_suffix = str(candidate.get("id", "")).endswith(":free")
        if (cin > 0 or cout > 0) and not is_free_suffix:
            return False, f"non-zero pricing in={cin} out={cout}"
        if rules.get("require_free_suffix", False) and not is_free_suffix \
                and (cin > 0 or cout > 0):
            return False, "missing :free suffix"
    # Vendor blocklist (used to keep proprietary OpenRouter passthroughs out)
    blocked = {v.lower() for v in rules.get("vendor_blocklist") or []}
    vendor = str(candidate.get("vendor", "")).lower()
    if vendor in blocked:
        return False, f"vendor '{vendor}' on blocklist"
    if (likes := candidate.get("likes_7d")) is not None:
        if int(likes) < int(rules.get("min_likes_7d", 0) or 0):
            return False, f"likes_7d {likes} below floor"
    if (elo := candidate.get("arena_elo")):
        if int(elo) < int(rules.get("min_arena_elo", 0) or 0):
            return False, f"arena_elo {elo} below floor"
    if (released := candidate.get("released")):
        try:
            cutoff = rules.get("min_release_date")
            if cutoff and datetime.fromisoformat(str(released)[:10]) < \
                    datetime.fromisoformat(str(cutoff)):
                return False, f"released {released} before cutoff {cutoff}"
        except Exception:
            pass
    return True, "accepted"


def _persist(candidate: dict, source: str, promoted: bool) -> None:
    payload = json.dumps({k: v for k, v in candidate.items() if not k.startswith("_")})
    with _conn() as cn:
        cn.execute(
            "INSERT INTO llm_registry(id, vendor, payload_json, source, promoted) "
            "VALUES(?, ?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "  payload_json=excluded.payload_json, "
            "  source=excluded.source, "
            "  promoted=MAX(llm_registry.promoted, excluded.promoted), "
            "  last_seen_at=CURRENT_TIMESTAMP",
            (candidate["id"], candidate.get("vendor", ""), payload, source, int(promoted)),
        )


def _append_audit(report: ScoutReport, audit_path: str) -> None:
    p = _PROJECT_ROOT.parent / audit_path if not Path(audit_path).is_absolute() else Path(audit_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists():
        p.write_text("# LLM Scout Audit Log\n\n", encoding="utf-8")
    line = (
        f"\n## {report.finished_at}\n"
        f"- sources polled: {', '.join(report.sources_polled) or 'none'}\n"
        f"- sources failed: {report.sources_failed or 'none'}\n"
        f"- candidates seen: {report.candidates_seen}\n"
        f"- promoted: {report.promoted_ids or 'none'}\n"
        f"- rejected: {[r['id'] for r in report.rejected_ids][:10]}"
        f"{' …' if len(report.rejected_ids) > 10 else ''}\n"
    )
    with p.open("a", encoding="utf-8") as fh:
        fh.write(line)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def refresh_llm_registry(force: bool = False) -> ScoutReport:
    """Poll all configured sources and update the persisted registry."""
    cfg = (load_config().get("llms") or {}).get("scout") or {}
    started = datetime.now(timezone.utc).isoformat()
    report = ScoutReport(started_at=started, finished_at=started)

    if not cfg.get("enabled", False) and not force:
        report.finished_at = datetime.now(timezone.utc).isoformat()
        return report

    if not force:
        last = _kv_get(_HEARTBEAT_KEY)
        if last:
            try:
                last_dt = datetime.fromisoformat(last)
                interval = timedelta(hours=int(cfg.get("interval_hours", 168)))
                if datetime.now(timezone.utc) - last_dt < interval:
                    report.finished_at = datetime.now(timezone.utc).isoformat()
                    return report
            except Exception:
                pass

    rules = cfg.get("accept_rules") or {}
    for src in cfg.get("sources") or []:
        kind = src.get("kind")
        adapter = _ADAPTERS.get(kind)
        if adapter is None:
            report.sources_failed.append({"kind": str(kind), "error": "no adapter"})
            continue
        try:
            candidates = adapter(src)
        except Exception as e:  # network / parsing failure — keep going
            report.sources_failed.append({"kind": kind, "error": str(e)[:200]})
            continue
        report.sources_polled.append(kind)
        for c in candidates:
            report.candidates_seen += 1
            ok, why = _accept(c, rules)
            _persist(c, source=kind, promoted=ok)
            if ok:
                report.promoted_ids.append(c["id"])
            else:
                report.rejected_ids.append({"id": c["id"], "reason": why})

    report.finished_at = datetime.now(timezone.utc).isoformat()
    _kv_set(_HEARTBEAT_KEY, report.finished_at)
    audit_path = cfg.get("audit_log") or "pipeline/docs/LLM_SCOUT_AUDIT.md"
    try:
        _append_audit(report, audit_path)
    except Exception:
        pass
    return report


# ---------------------------------------------------------------------------
# Background scheduling — opt-in daemon thread
# ---------------------------------------------------------------------------
_SCHEDULER_LOCK = threading.Lock()
_SCHEDULER_STARTED = False


def schedule_in_background() -> bool:
    """Start a daemon thread that calls `refresh_llm_registry` on the configured
    cadence. Safe to call multiple times — only the first invocation wins."""
    global _SCHEDULER_STARTED
    cfg = (load_config().get("llms") or {}).get("scout") or {}
    if not cfg.get("enabled", False):
        return False
    with _SCHEDULER_LOCK:
        if _SCHEDULER_STARTED:
            return False
        _SCHEDULER_STARTED = True

    def _loop() -> None:
        interval_s = max(3600, int(cfg.get("interval_hours", 168)) * 3600)
        # First run immediately so the registry is warm; then on cadence.
        while True:
            try:
                refresh_llm_registry()
            except Exception:
                pass
            time.sleep(interval_s)

    threading.Thread(target=_loop, name="llm-scout", daemon=True).start()
    return True


__all__ = ["refresh_llm_registry", "schedule_in_background", "ScoutReport"]
