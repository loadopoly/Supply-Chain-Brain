"""
Quests — first-class taxonomy of what the Brain is trying to accomplish.

A Quest is a long-running optimization goal (e.g. "Optimize Supply Chains")
with sub-quests that map onto existing Brain analyzer modules. A Mission is
one user-initiated, site-scoped instance of a Quest: a single NL query
becomes a Mission whose findings, artifacts, and progress are tracked over
time so deliverables can be refreshed in place ("living documents").

The taxonomy here is deliberately small and derived from the analyzers we
already have under brain/. Adding a sub-quest is a one-line registry edit;
binding it to an analyzer is a one-line edit in orchestrator.MODULE_REGISTRY.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any
import time
import uuid


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass
class Quest:
    id: str
    name: str
    description: str
    parent_id: str | None = None
    kpis: list[str] = field(default_factory=list)
    owner: str = "supply_chain"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


# Closed vocabulary of scope tags. Every parsed intent must reduce to a
# subset of these, which keeps the orchestrator deterministic. Extend by
# adding a row here AND a binding in orchestrator.MODULE_REGISTRY.
SCOPE_TAGS = (
    "inventory_sizing",       # EOQ deviation, overstock/understock dollars
    "fulfillment",            # OTD recursive root cause
    "sourcing",               # Procurement 360, supplier scorecards
    "data_quality",           # value-of-information, missing fields
    "lead_time",              # Lead-time survival
    "demand_distortion",      # Bullwhip
    "network_position",       # Multi-echelon safety stock placement
    "cycle_count",            # Cycle count accuracy / ABC classification
)


@dataclass
class Mission:
    id: str
    quest_id: str
    site: str
    user_query: str
    parsed_intent: dict[str, Any] = field(default_factory=dict)
    scope_tags: list[str] = field(default_factory=list)
    target_entity_kind: str = "site"   # site | warehouse | supplier | customer | part_family | process
    target_entity_key: str = ""
    horizon_days: int = 90
    status: str = "open"               # open | running | refreshed | closed | failed
    progress_pct: float = 0.0
    created_at: str = ""
    last_refreshed_at: str = ""
    artifact_paths: dict[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def new_mission_id() -> str:
    """Stable, sortable, filesystem-safe mission id."""
    return f"m_{int(time.time())}_{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Seed registry — the first quest, with sub-quests bound to existing modules
# ---------------------------------------------------------------------------
ROOT_QUEST_ID = "quest:optimize_supply_chains"

_REGISTRY: dict[str, Quest] = {
    ROOT_QUEST_ID: Quest(
        id=ROOT_QUEST_ID,
        name="Optimize Supply Chains",
        description=(
            "Continuously reduce stockouts, late shipments, working-capital lock, "
            "and supplier risk across every site. Every Mission inherits this goal."
        ),
        kpis=["otd_pct", "ifr_pct", "cycle_count_accuracy_pct", "dollars_at_risk"],
    ),
    "quest:inventory_sizing": Quest(
        id="quest:inventory_sizing",
        name="Right-size inventory",
        description="Drive Q_observed toward EOQ; release working capital on overstock; cover understock.",
        parent_id=ROOT_QUEST_ID,
        kpis=["dollars_at_risk", "overstock_units", "understock_units"],
    ),
    "quest:fulfillment": Quest(
        id="quest:fulfillment",
        name="Hit OTD/IFR",
        description="Reduce miss rate and days-late tail; classify failures as systemic vs operational.",
        parent_id=ROOT_QUEST_ID,
        kpis=["otd_pct", "ifr_pct", "days_late_p90"],
    ),
    "quest:sourcing": Quest(
        id="quest:sourcing",
        name="Source reliably",
        description="Consolidate vendors, raise scorecards, surface single-source risk.",
        parent_id=ROOT_QUEST_ID,
        kpis=["supplier_otd_pct", "single_source_count"],
    ),
    "quest:data_quality": Quest(
        id="quest:data_quality",
        name="Trust the data",
        description="Close PFEP gaps, fix mapping errors, raise value-of-information across master data.",
        parent_id=ROOT_QUEST_ID,
        kpis=["pfep_match_rate", "missing_field_pct"],
    ),
    "quest:lead_time": Quest(
        id="quest:lead_time",
        name="Compress lead time",
        description="Lower median + p90 supplier lead time; reduce variance.",
        parent_id=ROOT_QUEST_ID,
        kpis=["lead_time_median", "lead_time_p90"],
    ),
    "quest:demand_distortion": Quest(
        id="quest:demand_distortion",
        name="Damp the bullwhip",
        description="Reduce variance amplification across echelons.",
        parent_id=ROOT_QUEST_ID,
        kpis=["bullwhip_ratio"],
    ),
    "quest:network_position": Quest(
        id="quest:network_position",
        name="Position safety stock",
        description="Place safety stock at the right echelon; reduce total holding for same service level.",
        parent_id=ROOT_QUEST_ID,
        kpis=["network_safety_stock_dollars", "service_level_pct"],
    ),
    "quest:cycle_count": Quest(
        id="quest:cycle_count",
        name="Lock cycle-count accuracy",
        description="Raise cycle-count accuracy by ABC class; tighten cadence on high-velocity items.",
        parent_id=ROOT_QUEST_ID,
        kpis=["cycle_count_accuracy_pct", "abc_a_completion_pct"],
    ),
}


# Map closed-vocabulary scope tags → quest ids so a Mission's tags reveal
# which sub-quests it advances.
SCOPE_TAG_TO_QUEST: dict[str, str] = {
    "inventory_sizing":   "quest:inventory_sizing",
    "fulfillment":        "quest:fulfillment",
    "sourcing":           "quest:sourcing",
    "data_quality":       "quest:data_quality",
    "lead_time":          "quest:lead_time",
    "demand_distortion":  "quest:demand_distortion",
    "network_position":   "quest:network_position",
    "cycle_count":        "quest:cycle_count",
}


def get_quest(quest_id: str) -> Quest | None:
    return _REGISTRY.get(quest_id)


def list_quests(parent_id: str | None = ...) -> list[Quest]:
    """List quests. Pass parent_id=None for roots; omit for all."""
    if parent_id is ...:
        return list(_REGISTRY.values())
    return [q for q in _REGISTRY.values() if q.parent_id == parent_id]


def quests_for_scope_tags(tags: list[str]) -> list[Quest]:
    out: list[Quest] = []
    seen: set[str] = set()
    for t in tags:
        qid = SCOPE_TAG_TO_QUEST.get(t)
        if qid and qid not in seen and qid in _REGISTRY:
            out.append(_REGISTRY[qid])
            seen.add(qid)
    return out
