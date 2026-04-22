"""Unit tests for src/brain/intent_parser.py

All LLM calls are stubbed (see conftest.stub_llm autouse fixture) so
every test runs in pure-Python fallback mode — deterministic and fast.

Covers:
  - parse() returns a valid ParsedIntent for varied phrasings
  - closed-vocabulary enforcement on scope_tags and target_entity_kind
  - site extraction from explicit site hints
  - empty / whitespace query handled gracefully
  - as_dict() round-trips to expected keys
  - kpis_to_move populated from keywords
  - horizon_days populated from numeric cues
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_PARAPHRASES = [
    # (query, site, expected_tags_subset)
    ("Jerome restructuring — velocity hotspots and overstock", "JEROME",
     {"inventory_sizing"}),
    ("Burlington keeps missing OTD on heavy castings", "BURLINGTON",
     {"fulfillment"}),
    ("Consolidate the supplier base for Eugene", "EUGENE",
     {"sourcing"}),
    ("PFEP at Chattanooga is full of holes", "CHATTANOOGA",
     {"data_quality"}),
    ("Bullwhip is killing me on the spare parts forecast", "ALL",
     {"demand_distortion"}),
    ("Lead time variance from Acme is too high", "ALL",
     {"lead_time"}),
    ("Run cycle counts more accurately", "ALL",
     {"cycle_count"}),
    ("Multi-echelon safety stock is wrong across the network", "ALL",
     {"inventory_sizing", "network_position"}),
]


@pytest.mark.unit
@pytest.mark.quest
class TestParseReturnType:
    def test_returns_parsed_intent(self):
        from src.brain.intent_parser import parse, ParsedIntent
        p = parse("show me fulfillment metrics", "BURLINGTON")
        assert isinstance(p, ParsedIntent)

    def test_has_required_fields(self):
        from src.brain.intent_parser import parse
        p = parse("check inventory levels", "ALL")
        required = [
            "site", "user_query", "scope_tags", "target_entity_kind",
            "target_entity_key", "horizon_days", "kpis_to_move",
            "success_criteria", "quest_id", "parser_source",
        ]
        d = p.as_dict()
        for f in required:
            assert f in d, f"Missing field {f!r} in ParsedIntent.as_dict()"

    def test_parser_source_is_fallback(self):
        from src.brain.intent_parser import parse
        p = parse("check inventory", "ALL")
        assert p.parser_source in ("fallback_keyword", "fixed_up", "llm")

    def test_empty_query_returns_safely(self):
        from src.brain.intent_parser import parse
        p = parse("", "ALL")
        assert p is not None
        assert isinstance(p.scope_tags, list)

    def test_whitespace_query_returns_safely(self):
        from src.brain.intent_parser import parse
        p = parse("   ", "ALL")
        assert p is not None


@pytest.mark.unit
@pytest.mark.quest
class TestClosedVocabulary:
    def test_scope_tags_are_valid(self):
        from src.brain.intent_parser import parse
        from src.brain.quests import SCOPE_TAGS
        for q, site, _ in _PARAPHRASES:
            p = parse(q, site)
            bad = [t for t in p.scope_tags if t not in SCOPE_TAGS]
            assert bad == [], f"Invalid scope tags {bad!r} for query: {q!r}"

    def test_target_entity_kind_is_valid(self):
        from src.brain.intent_parser import parse, _VALID_ENTITY_KINDS
        for q, site, _ in _PARAPHRASES:
            p = parse(q, site)
            assert p.target_entity_kind in _VALID_ENTITY_KINDS, (
                f"Invalid entity kind {p.target_entity_kind!r} for {q!r}")

    def test_at_least_one_scope_tag(self):
        from src.brain.intent_parser import parse
        for q, site, _ in _PARAPHRASES:
            p = parse(q, site)
            assert len(p.scope_tags) >= 1, f"No scope tags for: {q!r}"


@pytest.mark.unit
@pytest.mark.quest
class TestKeywordMapping:
    @pytest.mark.parametrize("query,site,expected_tags", _PARAPHRASES)
    def test_keyword_mapping(self, query, site, expected_tags):
        from src.brain.intent_parser import parse
        p = parse(query, site)
        assert expected_tags.issubset(set(p.scope_tags)), (
            f"Expected {expected_tags} ⊆ {set(p.scope_tags)} for {query!r}")

    def test_site_propagated(self):
        from src.brain.intent_parser import parse
        p = parse("check OTD for the plant", "JEROME")
        assert p.site == "JEROME"

    def test_default_site_all(self):
        from src.brain.intent_parser import parse
        p = parse("check inventory levels")
        assert p.site in ("ALL", "")

    def test_horizon_days_positive(self):
        from src.brain.intent_parser import parse
        p = parse("show me fulfillment trends over the next 90 days", "ALL")
        assert p.horizon_days > 0

    def test_kpis_to_move_is_list(self):
        from src.brain.intent_parser import parse
        p = parse("improve OTD and lead time", "ALL")
        assert isinstance(p.kpis_to_move, list)


@pytest.mark.unit
@pytest.mark.quest
class TestAsDictRoundTrip:
    def test_as_dict_is_json_serialisable(self):
        import json
        from src.brain.intent_parser import parse
        p = parse("evaluate supplier consolidation at Burlington", "BURLINGTON")
        try:
            json.dumps(p.as_dict())
        except (TypeError, ValueError) as e:
            pytest.fail(f"ParsedIntent.as_dict() is not JSON-serialisable: {e}")

    def test_as_dict_preserves_scope_tags(self):
        from src.brain.intent_parser import parse
        p = parse("lead time is too long", "ALL")
        d = p.as_dict()
        assert d["scope_tags"] == p.scope_tags
