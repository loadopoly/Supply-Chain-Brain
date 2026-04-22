"""Unit tests for src/brain/quests.py

Covers:
  - SCOPE_TAGS closed vocabulary (8 items, known strings)
  - ROOT_QUEST_ID exists and is registered
  - get_quest() returns a Quest dataclass with expected fields
  - list_quests(parent_id) returns children
  - quests_for_scope_tags() maps every tag to a quest
  - new_mission_id() produces unique, timestamped IDs
  - SCOPE_TAG_TO_QUEST covers all 8 scope tags
"""
from __future__ import annotations

import time
import pytest


@pytest.mark.unit
@pytest.mark.quest
class TestScopeTags:
    def test_count(self):
        from src.brain.quests import SCOPE_TAGS
        assert len(SCOPE_TAGS) == 8

    def test_known_members(self):
        from src.brain.quests import SCOPE_TAGS
        expected = {
            "fulfillment", "lead_time", "sourcing", "inventory_sizing",
            "network_position", "demand_distortion", "cycle_count", "data_quality",
        }
        assert set(SCOPE_TAGS) == expected

    def test_no_duplicates(self):
        from src.brain.quests import SCOPE_TAGS
        assert len(SCOPE_TAGS) == len(set(SCOPE_TAGS))

    def test_all_strings(self):
        from src.brain.quests import SCOPE_TAGS
        assert all(isinstance(t, str) for t in SCOPE_TAGS)


@pytest.mark.unit
@pytest.mark.quest
class TestQuestRegistry:
    def test_root_exists(self):
        from src.brain.quests import ROOT_QUEST_ID, get_quest
        q = get_quest(ROOT_QUEST_ID)
        assert q is not None
        assert q.id == ROOT_QUEST_ID

    def test_root_has_name(self):
        from src.brain.quests import ROOT_QUEST_ID, get_quest
        q = get_quest(ROOT_QUEST_ID)
        assert isinstance(q.name, str) and len(q.name) > 3

    def test_root_has_children(self):
        from src.brain.quests import ROOT_QUEST_ID, list_quests
        children = list_quests(ROOT_QUEST_ID)
        assert len(children) >= 8

    def test_unknown_quest_returns_none(self):
        from src.brain.quests import get_quest
        assert get_quest("quest:does_not_exist") is None

    def test_scope_tag_to_quest_covers_all_tags(self):
        from src.brain.quests import SCOPE_TAGS, SCOPE_TAG_TO_QUEST
        missing = [t for t in SCOPE_TAGS if t not in SCOPE_TAG_TO_QUEST]
        assert missing == [], f"Tags without a quest mapping: {missing}"

    def test_quests_for_scope_tags_single(self):
        from src.brain.quests import quests_for_scope_tags
        qs = quests_for_scope_tags(["fulfillment"])
        assert len(qs) >= 1

    def test_quests_for_scope_tags_multi(self):
        from src.brain.quests import quests_for_scope_tags
        qs = quests_for_scope_tags(["fulfillment", "lead_time", "sourcing"])
        assert len(qs) >= 2

    def test_quests_for_empty_tags(self):
        from src.brain.quests import quests_for_scope_tags
        qs = quests_for_scope_tags([])
        assert isinstance(qs, list)

    def test_quests_for_unknown_tag(self):
        from src.brain.quests import quests_for_scope_tags
        qs = quests_for_scope_tags(["not_a_real_tag"])
        assert isinstance(qs, list)


@pytest.mark.unit
@pytest.mark.quest
class TestMissionId:
    def test_format(self):
        from src.brain.quests import new_mission_id
        mid = new_mission_id()
        assert mid.startswith("m_")
        parts = mid.split("_")
        assert len(parts) == 3
        assert parts[1].isdigit()
        assert len(parts[2]) == 8

    def test_uniqueness(self):
        from src.brain.quests import new_mission_id
        ids = {new_mission_id() for _ in range(50)}
        assert len(ids) == 50

    def test_monotone_timestamps(self):
        from src.brain.quests import new_mission_id
        a = new_mission_id()
        time.sleep(0.01)
        b = new_mission_id()
        ts_a = int(a.split("_")[1])
        ts_b = int(b.split("_")[1])
        assert ts_b >= ts_a
