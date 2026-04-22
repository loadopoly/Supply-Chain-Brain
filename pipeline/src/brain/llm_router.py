"""LLM Router — decisive open-weight model selection for the Brain.

The Brain orchestrates 12 supply-chain stages, each with a different cognitive
profile (long-context cross-dataset review vs. cheap row-level classification
vs. heavy reasoning for OTD root-cause). This router scores every registered
open-weight model against the requested task profile and returns the single
best pick with a transparent rationale.

Inputs come from `config/brain.yaml -> llms` and are merged with newcomers
discovered by `brain.llm_scout` (persisted in `local_brain.sqlite.llm_registry`).

Public API:
    select_llm(task: str, *, overrides: dict | None = None) -> Decision
    rank_llms(task: str) -> list[Decision]
    available_models() -> list[dict]

A Decision carries the model id, score, hard-filter pass/fail, the per-axis
contributions, and the chosen inference endpoint pulled from the env var named
in the registry (`endpoint_env`). Callers never hard-code a model id.
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any

from . import load_config
from .local_store import db_path


# ---------------------------------------------------------------------------
# Persisted registry (newcomers from llm_scout land here)
# ---------------------------------------------------------------------------
_REGISTRY_DDL = """
CREATE TABLE IF NOT EXISTS llm_registry (
    id              TEXT PRIMARY KEY,
    vendor          TEXT,
    payload_json    TEXT NOT NULL,
    source          TEXT,
    discovered_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    promoted        INTEGER DEFAULT 0,
    last_seen_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS llm_decision_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task            TEXT NOT NULL,
    chosen_model    TEXT NOT NULL,
    score           REAL,
    rationale_json  TEXT,
    decided_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


@contextmanager
def _conn():
    cn = sqlite3.connect(db_path())
    try:
        cn.executescript(_REGISTRY_DDL)
        yield cn
        cn.commit()
    finally:
        cn.close()


# ---------------------------------------------------------------------------
# Decision object
# ---------------------------------------------------------------------------
@dataclass
class Decision:
    model_id: str
    vendor: str
    score: float
    passed_filters: bool
    endpoint_env: str
    endpoint: str | None
    contributions: dict[str, float] = field(default_factory=dict)
    penalties: dict[str, float] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Registry assembly: yaml seed + sqlite newcomers, merged by id
# ---------------------------------------------------------------------------
def _load_yaml_block() -> dict[str, Any]:
    return (load_config().get("llms") or {})


def _load_persisted() -> list[dict[str, Any]]:
    import json
    rows: list[dict[str, Any]] = []
    with _conn() as cn:
        cur = cn.execute(
            "SELECT id, payload_json FROM llm_registry WHERE promoted = 1"
        )
        for mid, payload in cur.fetchall():
            try:
                obj = json.loads(payload)
                obj["id"] = mid
                rows.append(obj)
            except Exception:
                continue
    return rows


def available_models() -> list[dict[str, Any]]:
    """Return merged registry: YAML seed + scout-promoted newcomers (id-unique)."""
    block = _load_yaml_block()
    seed = list(block.get("registry") or [])
    persisted = _load_persisted()
    merged: dict[str, dict] = {m["id"]: m for m in seed}
    for m in persisted:
        merged.setdefault(m["id"], m)  # YAML always wins on collision
    return list(merged.values())


def task_profile(task: str) -> dict[str, Any]:
    profiles = (_load_yaml_block().get("task_profiles") or {})
    return profiles.get(task) or profiles.get("default") or {
        "weights": {"reasoning": 1.0},
        "min_ctx": 0,
        "lambda_cost": 0.2,
        "lambda_latency": 0.1,
    }


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def _normalize(value: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (value - lo) / (hi - lo)))


def _score_model(model: dict[str, Any], profile: dict[str, Any],
                 cost_range: tuple[float, float],
                 latency_range: tuple[float, float]) -> Decision:
    weights: dict[str, float] = profile.get("weights") or {}
    caps: dict[str, float] = model.get("capabilities") or {}

    contributions: dict[str, float] = {}
    base = 0.0
    for axis, w in weights.items():
        c = float(caps.get(axis, 0.0)) * float(w)
        contributions[axis] = round(c, 4)
        base += c

    cost_in = float(model.get("cost_per_mtok_in", 0.0))
    cost_out = float(model.get("cost_per_mtok_out", 0.0))
    blended_cost = (cost_in + 3.0 * cost_out) / 4.0   # output-weighted
    norm_cost = _normalize(blended_cost, *cost_range)
    norm_lat = _normalize(float(model.get("median_latency_ms", 0.0)), *latency_range)

    pen_cost = float(profile.get("lambda_cost", 0.0)) * norm_cost
    pen_lat = float(profile.get("lambda_latency", 0.0)) * norm_lat
    score = base - pen_cost - pen_lat

    notes: list[str] = []
    passed = True
    min_ctx = int(profile.get("min_ctx", 0) or 0)
    if int(model.get("ctx_window", 0)) < min_ctx:
        passed = False
        notes.append(f"ctx_window {model.get('ctx_window')} < required {min_ctx}")
    max_lat = profile.get("max_latency_ms")
    if max_lat is not None and float(model.get("median_latency_ms", 0)) > float(max_lat):
        passed = False
        notes.append(f"latency {model.get('median_latency_ms')}ms > cap {max_lat}ms")

    env_name = str(model.get("endpoint_env") or "")
    return Decision(
        model_id=model["id"],
        vendor=str(model.get("vendor", "")),
        score=round(score, 4),
        passed_filters=passed,
        endpoint_env=env_name,
        endpoint=os.environ.get(env_name) if env_name else None,
        contributions=contributions,
        penalties={"cost": round(pen_cost, 4), "latency": round(pen_lat, 4)},
        notes=notes,
    )


def rank_llms(task: str) -> list[Decision]:
    """Score every registered model for the given task; descending by score."""
    models = available_models()
    if not models:
        return []
    profile = task_profile(task)
    costs = [(float(m.get("cost_per_mtok_in", 0)) +
              3.0 * float(m.get("cost_per_mtok_out", 0))) / 4.0 for m in models]
    lats = [float(m.get("median_latency_ms", 0)) for m in models]
    cost_range = (min(costs), max(costs) or 1.0)
    lat_range = (min(lats), max(lats) or 1.0)
    decisions = [_score_model(m, profile, cost_range, lat_range) for m in models]
    decisions.sort(key=lambda d: (d.passed_filters, d.score), reverse=True)
    return decisions


def select_llm(task: str, *, overrides: dict[str, Any] | None = None,
               log: bool = True) -> Decision:
    """Decisively pick the best model for `task`. Always returns a Decision."""
    import json
    ranked = rank_llms(task)
    if overrides and overrides.get("force_model"):
        forced = overrides["force_model"]
        for d in ranked:
            if d.model_id == forced:
                d.notes.append(f"forced via overrides[force_model]={forced}")
                _maybe_log(d, task, log)
                return d
    eligible = [d for d in ranked if d.passed_filters]
    chosen = eligible[0] if eligible else (ranked[0] if ranked else _fallback())
    if not eligible and ranked:
        chosen.notes.append("no model passed hard filters; returning best-effort")
    _maybe_log(chosen, task, log)
    return chosen


def _fallback() -> Decision:
    return Decision(
        model_id="<no-registry>", vendor="", score=0.0, passed_filters=False,
        endpoint_env="", endpoint=None,
        notes=["llm registry is empty — populate config/brain.yaml -> llms.registry"],
    )


def _maybe_log(decision: Decision, task: str, log: bool) -> None:
    if not log or decision.model_id == "<no-registry>":
        return
    import json
    with _conn() as cn:
        cn.execute(
            "INSERT INTO llm_decision_log(task, chosen_model, score, rationale_json) "
            "VALUES (?, ?, ?, ?)",
            (task, decision.model_id, decision.score, json.dumps(decision.as_dict())),
        )


# ---------------------------------------------------------------------------
# Convenience for callers that just want the endpoint string
# ---------------------------------------------------------------------------
def endpoint_for(task: str) -> tuple[str, str | None]:
    d = select_llm(task)
    return d.model_id, d.endpoint


__all__ = [
    "Decision",
    "available_models",
    "task_profile",
    "rank_llms",
    "select_llm",
    "endpoint_for",
]
