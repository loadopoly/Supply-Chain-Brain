"""
Mission Runner — single entry point that wires together every Brain piece
for the Quest Console:

    intent_parser  →  mission_store.create
    orchestrator   →  MissionResult (findings + KPI snapshot + progress)
    schema_synth   →  EntitySchema for the target entity
    viz_composer   →  dict[str, plotly.Figure]
    one_pager      →  Executive 1-pager PPTX (overwrite in place)
    implementation →  Implementation Plan PPTX (overwrite in place)
    mission_store  →  attach_artifact + mark_refreshed

Public API:
    launch(user_query, site, *, horizon_days=90) -> Mission
    refresh(mission_id) -> dict
    refresh_open_missions(*, max_concurrent=1) -> list[dict]
"""
from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any

from . import (
    intent_parser, mission_store, orchestrator,
    schema_synthesizer, viz_composer, quests,
)

log = logging.getLogger("brain.mission_runner")

_PIPELINE_ROOT = Path(__file__).resolve().parents[2]
_ARTIFACT_ROOT = _PIPELINE_ROOT / "snapshots" / "missions"

# Per-mission lock so manual refresh and the autonomous loop can't collide.
_MISSION_LOCKS: dict[str, threading.Lock] = {}
_REGISTRY_LOCK = threading.Lock()


def _lock_for(mission_id: str) -> threading.Lock:
    with _REGISTRY_LOCK:
        if mission_id not in _MISSION_LOCKS:
            _MISSION_LOCKS[mission_id] = threading.Lock()
        return _MISSION_LOCKS[mission_id]


def _quest_for_scope(scope_tags: list[str]) -> str:
    for tag in scope_tags or []:
        qid = quests.SCOPE_TAG_TO_QUEST.get(tag)
        if qid:
            return qid
    return quests.ROOT_QUEST_ID


_TAG_OWNER = {
    "inventory_sizing":  "Planner",
    "fulfillment":       "Planner",
    "sourcing":          "Buyer",
    "data_quality":      "Quality",
    "lead_time":         "Buyer",
    "demand_distortion": "Planner",
    "network_position":  "Ops",
    "cycle_count":       "Ops",
}


def _owner_for_tags(scope_tags) -> str:
    for t in scope_tags or []:
        if t in _TAG_OWNER:
            return _TAG_OWNER[t]
    return "Anyone"


# ---------------------------------------------------------------------------
# Launch a brand-new mission
# ---------------------------------------------------------------------------
def launch(user_query: str, site: str, *, horizon_days: int = 90,
           executed_by: str | None = None) -> dict:
    """Parse intent → create mission → run first refresh → return mission dict."""
    user_query = (user_query or "").strip()
    if not user_query:
        raise ValueError("user_query must be a non-empty string")
    site = (site or "").strip() or "ALL"
    parsed = intent_parser.parse(user_query, site_default=site)
    parsed_dict = {
        "scope_tags": list(parsed.scope_tags),
        "target_entity_kind": parsed.target_entity_kind,
        "target_entity_key": parsed.target_entity_key,
        "owner_role": _owner_for_tags(parsed.scope_tags),
        "parser_source": parsed.parser_source,
        "executed_by": executed_by or "User",
        "kpis_to_move": list(getattr(parsed, "kpis_to_move", []) or []),
        "success_criteria": getattr(parsed, "success_criteria", "") or "",
    }

    quest_id = _quest_for_scope(list(parsed.scope_tags))
    mission = mission_store.create_mission(
        quest_id=quest_id,
        site=site,
        user_query=user_query,
        parsed_intent=parsed_dict,
        scope_tags=list(parsed.scope_tags),
        target_entity_kind=parsed.target_entity_kind,
        target_entity_key=parsed.target_entity_key or site,
        horizon_days=horizon_days,
    )
    mid = mission.id
    mission_store.record_event(mid, "launched",
                               {"parser_source": parsed.parser_source})

    # Run the first refresh inline so artifacts exist immediately.
    try:
        refresh(mid)
    except Exception as e:
        log.exception("first refresh failed for %s: %s", mid, e)
        mission_store.record_event(mid, "refresh_failed", {"error": str(e)})

    refreshed = mission_store.get_mission(mid)
    return refreshed if refreshed else mission


# ---------------------------------------------------------------------------
# Refresh an existing mission (living-document update)
# ---------------------------------------------------------------------------
def refresh(mission_id: str) -> dict:
    lock = _lock_for(mission_id)
    if not lock.acquire(blocking=False):
        return {"ok": False, "skipped": True, "reason": "locked"}
    try:
        mission_obj = mission_store.get_mission(mission_id)
        if not mission_obj:
            return {"ok": False, "error": f"unknown mission {mission_id}"}

        result = orchestrator.BrainOrchestrator().run(mission_obj, refresh=True)
        schema = schema_synthesizer.synthesize(
            target_entity_kind=mission_obj.target_entity_kind,
            target_entity_key=mission_obj.target_entity_key,
        )
        viz = viz_composer.compose(result)

        # Lazy-import deck modules — keep brain Streamlit-/pptx-free at import
        from ..deck import one_pager, implementation_plan

        out_dir = _ARTIFACT_ROOT / mission_id
        out_dir.mkdir(parents=True, exist_ok=True)
        one_pager_path = out_dir / "one_pager.pptx"
        plan_path = out_dir / "implementation_plan.pptx"

        artifacts: dict[str, str] = {}
        try:
            one_pager.render_one_pager(mission_obj, result, viz, schema,
                                       one_pager_path)
            mission_store.attach_artifact(mission_id, "one_pager",
                                          str(one_pager_path))
            artifacts["one_pager"] = str(one_pager_path)
        except Exception as e:
            log.warning("one_pager render failed: %s", e)
            mission_store.record_event(mission_id, "render_error",
                                       {"artifact": "one_pager", "error": str(e)})

        try:
            implementation_plan.render_implementation_plan(
                mission_obj, result, viz, schema, plan_path)
            mission_store.attach_artifact(mission_id, "implementation_plan",
                                          str(plan_path))
            artifacts["implementation_plan"] = str(plan_path)
        except Exception as e:
            log.warning("implementation_plan render failed: %s", e)
            mission_store.record_event(mission_id, "render_error",
                                       {"artifact": "implementation_plan",
                                        "error": str(e)})

        # Update progress + last_refreshed_at
        try:
            mission_store.update_progress(mission_id,
                                          float(result.progress_pct or 0.0),
                                          note="auto-refresh")
        except Exception:
            pass
        try:
            mission_store.mark_refreshed(mission_id)
        except Exception:
            pass

        return {
            "ok": True,
            "mission_id": mission_id,
            "progress_pct": float(getattr(result, "progress_pct", 0.0) or 0.0),
            "n_findings": len(getattr(result, "findings", []) or []),
            "n_outcomes": len(getattr(result, "outcomes", []) or []),
            "artifacts": artifacts,
            "elapsed_ms": getattr(result, "elapsed_ms", None),
        }
    finally:
        lock.release()


def refresh_open_missions(*, max_concurrent: int = 1, limit: int = 25) -> list[dict]:
    """Called by the autonomous agent loop. Sequential by default to avoid
    DB lock contention; max_concurrent reserved for future tuning."""
    out: list[dict] = []
    try:
        open_missions = mission_store.list_open(limit=limit)
    except Exception as e:
        log.warning("list_open failed: %s", e)
        return out
    for m in open_missions:
        mid = getattr(m, "id", None)
        if not mid:
            continue
        try:
            out.append(refresh(mid))
        except Exception as e:
            log.exception("refresh failed for %s: %s", mid, e)
            out.append({"ok": False, "mission_id": mid, "error": str(e)})
    return out


__all__ = ["launch", "refresh", "refresh_open_missions"]
