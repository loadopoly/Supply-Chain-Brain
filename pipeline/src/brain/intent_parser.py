"""
NL Intent Parser — turns a free-form user query into a strict ParsedIntent.

The parser uses the existing brain LLM ensemble (llm_router + llm_ensemble)
with a closed-vocabulary JSON schema; if the ensemble returns malformed
JSON it gets one fix-up retry. If the ensemble is unreachable (offline
smoke test, no models registered, network down), the parser falls back to
a deterministic keyword classifier so the Quest Console keeps working.

The closed vocabulary is the single source of truth for what the
orchestrator can act on; unmatched intents are still recorded as a
mission_event so the vocabulary can be grown empirically.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any
import json
import re

from .quests import SCOPE_TAGS, ROOT_QUEST_ID


_VALID_ENTITY_KINDS = (
    "site", "warehouse", "supplier", "customer", "part_family", "process",
)


# ---------------------------------------------------------------------------
# Output dataclass
# ---------------------------------------------------------------------------
@dataclass
class ParsedIntent:
    site: str
    user_query: str
    scope_tags: list[str]
    target_entity_kind: str
    target_entity_key: str
    horizon_days: int = 90
    kpis_to_move: list[str] = field(default_factory=list)
    success_criteria: str = ""
    quest_id: str = ROOT_QUEST_ID
    parser_source: str = "llm"   # "llm" | "fallback_keyword" | "fixed_up"
    raw_llm_answer: dict | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Keyword fallback — robust deterministic classifier when the ensemble is
# unavailable. Maps phrases → scope tags via the same closed vocabulary.
# ---------------------------------------------------------------------------
_KEYWORDS: dict[str, tuple[str, ...]] = {
    "inventory_sizing": ("inventory", "stock", "eoq", "overstock", "understock",
                         "working capital", "right-size", "rightsize",
                         "warehouse", "restructur", "slot", "locator", "bin"),
    "fulfillment":      ("otd", "on-time", "on time", "late", "miss",
                         "ship", "fulfill", "delivery", "ifr", "fill rate"),
    "sourcing":         ("supplier", "vendor", "procure", "sourcing",
                         "consolidat", "single-source", "single source"),
    "data_quality":     ("data quality", "missing", "pfep", "master data",
                         "value of information", "voi"),
    "lead_time":        ("lead time", "leadtime", "lead-time"),
    "demand_distortion":("bullwhip", "demand variance", "amplificat",
                         "forecast distort"),
    "network_position": ("multi-echelon", "multi echelon", "echelon",
                         "safety stock", "network position", "node placement"),
    "cycle_count":      ("cycle count", "cycle-count", "abc class", "abc-class",
                         "count accuracy"),
}


def _keyword_scope_tags(text: str) -> list[str]:
    t = text.lower()
    hits: list[str] = []
    for tag, words in _KEYWORDS.items():
        if any(w in t for w in words):
            hits.append(tag)
    # Default to fulfillment + inventory_sizing — the two most common asks —
    # so a vague Mission still gets useful work done.
    return hits or ["fulfillment", "inventory_sizing"]


_SITE_HINTS = (
    "jerome", "burlington", "chattanooga", "eugene", "yankton",
    "mequon", "sterling", "thornbury", "albuquerque",
)


def _guess_site(text: str, default: str) -> str:
    t = text.lower()
    for s in _SITE_HINTS:
        if s in t:
            return s.capitalize()
    return default or "ALL"


def _guess_entity_kind(text: str) -> tuple[str, str]:
    t = text.lower()
    if "supplier" in t or "vendor" in t:
        return "supplier", ""
    if "customer" in t:
        return "customer", ""
    if "part family" in t or "part-family" in t or "commodity" in t:
        return "part_family", ""
    if "warehouse" in t or "locator" in t or "bin" in t:
        return "warehouse", ""
    if "process" in t or "workflow" in t:
        return "process", ""
    return "site", ""


def _fallback_parse(user_query: str, site_default: str) -> ParsedIntent:
    tags = _keyword_scope_tags(user_query)
    site = _guess_site(user_query, site_default)
    ek, ekey = _guess_entity_kind(user_query)
    if ek == "site":
        ekey = site
    return ParsedIntent(
        site=site,
        user_query=user_query,
        scope_tags=tags,
        target_entity_kind=ek,
        target_entity_key=ekey,
        horizon_days=_guess_horizon(user_query),
        kpis_to_move=[],
        success_criteria="",
        parser_source="fallback_keyword",
    )


def _guess_horizon(text: str) -> int:
    m = re.search(r"(\d{1,3})\s*(?:day|d\b)", text.lower())
    if m:
        return max(7, min(365, int(m.group(1))))
    if "quarter" in text.lower():
        return 90
    if "year" in text.lower():
        return 365
    return 90


# ---------------------------------------------------------------------------
# JSON validation — strict enough to keep the orchestrator deterministic
# ---------------------------------------------------------------------------
def _coerce(answer: Any, user_query: str, site_default: str) -> ParsedIntent | None:
    """Validate the LLM JSON and clip to closed vocabulary. Return None on fail."""
    if isinstance(answer, str):
        # Pull the first {...} block out of any prose around it.
        m = re.search(r"\{.*\}", answer, re.S)
        if not m:
            return None
        try:
            answer = json.loads(m.group(0))
        except Exception:
            return None
    if not isinstance(answer, dict):
        return None
    tags_raw = answer.get("scope_tags") or []
    if not isinstance(tags_raw, list):
        tags_raw = []
    tags = [t for t in tags_raw if isinstance(t, str) and t in SCOPE_TAGS]
    if not tags:
        return None
    ek = str(answer.get("target_entity_kind") or "site")
    if ek not in _VALID_ENTITY_KINDS:
        ek = "site"
    site = str(answer.get("site") or site_default or "ALL").strip() or "ALL"
    ekey = str(answer.get("target_entity_key") or "").strip()
    if ek == "site" and not ekey:
        ekey = site
    horizon = answer.get("horizon_days")
    try:
        horizon = max(7, min(365, int(horizon))) if horizon is not None else 90
    except Exception:
        horizon = 90
    kpis = answer.get("kpis_to_move") or []
    if not isinstance(kpis, list):
        kpis = []
    kpis = [str(k) for k in kpis][:6]
    return ParsedIntent(
        site=site,
        user_query=user_query,
        scope_tags=tags,
        target_entity_kind=ek,
        target_entity_key=ekey,
        horizon_days=horizon,
        kpis_to_move=kpis,
        success_criteria=str(answer.get("success_criteria") or "")[:400],
        parser_source="llm",
        raw_llm_answer=answer,
    )


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------
_SYSTEM = (
    "You are the supply-chain Brain's intent parser. Read the user's "
    "free-form request and emit ONLY a single JSON object with this shape:\n"
    "{\n"
    '  "site": "<site name or ALL>",\n'
    '  "scope_tags": [<one or more from the closed vocabulary below>],\n'
    '  "target_entity_kind": "<one of: site, warehouse, supplier, customer, part_family, process>",\n'
    '  "target_entity_key": "<entity identifier or empty string>",\n'
    '  "horizon_days": <integer 7..365>,\n'
    '  "kpis_to_move": ["<short kpi labels>"],\n'
    '  "success_criteria": "<one sentence>"\n'
    "}\n"
    "Closed vocabulary for scope_tags (use only these): "
    + ", ".join(SCOPE_TAGS)
    + ". Pick every tag that genuinely applies. Do not invent new tags. "
    "Return JSON only — no prose."
)


def _build_payload(user_query: str, site_default: str) -> dict:
    return {
        "kind": "json",
        "system": _SYSTEM,
        "text": (f"USER QUERY:\n{user_query}\n\n"
                 f"DEFAULT SITE: {site_default or 'ALL'}\n"
                 "Emit the JSON now."),
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def parse(user_query: str, site_default: str = "ALL", *,
          task: str = "cross_dataset_review") -> ParsedIntent:
    """Parse a free-form Quest Console query into a ParsedIntent.

    Always returns something usable. The `parser_source` field lets the
    UI tell the user whether the LLM ensemble or the keyword fallback
    produced the answer.
    """
    user_query = (user_query or "").strip()[:2000]
    if not user_query:
        return _fallback_parse("", site_default)

    payload = _build_payload(user_query, site_default)
    answer: Any = None
    try:
        from .llm_ensemble import dispatch_parallel
        result = dispatch_parallel(task, payload)
        answer = getattr(result, "answer", result)
        if isinstance(answer, dict) and "value" in answer and "ensemble_confidence" in answer:
            answer = answer["value"]
    except Exception:
        answer = None

    parsed = _coerce(answer, user_query, site_default) if answer is not None else None
    if parsed is not None:
        return parsed

    # One-shot fix-up: tell the ensemble its prior answer was malformed.
    if answer is not None:
        try:
            from .llm_ensemble import dispatch_parallel
            fix_payload = {
                "kind": "json",
                "system": _SYSTEM,
                "text": (f"USER QUERY:\n{user_query}\n\nDEFAULT SITE: "
                         f"{site_default or 'ALL'}\n\n"
                         "Your previous answer did not match the schema. "
                         "Emit ONLY the JSON object with the fields above, "
                         "using only the allowed scope_tags."),
            }
            result = dispatch_parallel(task, fix_payload)
            answer2 = getattr(result, "answer", result)
            if isinstance(answer2, dict) and "value" in answer2:
                answer2 = answer2["value"]
            parsed = _coerce(answer2, user_query, site_default)
            if parsed is not None:
                parsed.parser_source = "fixed_up"
                return parsed
        except Exception:
            pass

    return _fallback_parse(user_query, site_default)


__all__ = ["ParsedIntent", "parse"]
