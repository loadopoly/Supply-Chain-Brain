"""Integration tests for src/brain/mission_store.py

Uses the real findings_index.db (via mission_store's default path) but
cleans up every mission it creates through the mission_factory fixture.

Covers:
  - create_mission → Mission dataclass with expected fields
  - get_mission round-trip
  - list_open includes newly created missions
  - update_progress clamps to [0, 100]
  - attach_artifact records path and event
  - update_status changes status field
  - record_event / list_events append-only log
  - delete_mission removes from list_open
  - mark_refreshed updates last_refreshed_at
"""
from __future__ import annotations

import pytest
from datetime import datetime


@pytest.mark.integration
@pytest.mark.quest
class TestCreateAndRetrieve:
    def test_create_returns_mission(self, mission_factory):
        from src.brain.mission_store import Mission
        m = mission_factory()
        assert isinstance(m, Mission)

    def test_create_assigns_id(self, mission_factory):
        m = mission_factory()
        assert m.id and m.id.startswith("m_")

    def test_get_mission_round_trip(self, mission_factory):
        from src.brain import mission_store
        m = mission_factory(site="RRT", user_query="get-mission smoke")
        retrieved = mission_store.get_mission(m.id)
        assert retrieved is not None
        assert retrieved.id == m.id
        assert retrieved.site == "RRT"
        assert retrieved.user_query == "get-mission smoke"

    def test_unknown_id_returns_none(self):
        from src.brain import mission_store
        assert mission_store.get_mission("m_does_not_exist") is None

    def test_list_open_includes_new(self, mission_factory):
        from src.brain import mission_store
        m = mission_factory()
        opens = mission_store.list_open(limit=200)
        ids = [getattr(o, "id", None) for o in opens]
        assert m.id in ids

    def test_list_open_excludes_closed(self, mission_factory):
        from src.brain import mission_store
        m = mission_factory()
        mission_store.update_status(m.id, "done")
        opens = mission_store.list_open(limit=200)
        ids = [getattr(o, "id", None) for o in opens]
        assert m.id not in ids


@pytest.mark.integration
@pytest.mark.quest
class TestProgress:
    def test_update_progress_normal(self, mission_factory):
        from src.brain import mission_store
        m = mission_factory()
        mission_store.update_progress(m.id, 55.0)
        got = mission_store.get_mission(m.id)
        assert abs(got.progress_pct - 55.0) < 0.01

    def test_update_progress_clamps_above_100(self, mission_factory):
        from src.brain import mission_store
        m = mission_factory()
        mission_store.update_progress(m.id, 150.0)
        got = mission_store.get_mission(m.id)
        assert got.progress_pct <= 100.0

    def test_update_progress_clamps_below_0(self, mission_factory):
        from src.brain import mission_store
        m = mission_factory()
        mission_store.update_progress(m.id, -10.0)
        got = mission_store.get_mission(m.id)
        assert got.progress_pct >= 0.0

    def test_progress_creates_event(self, mission_factory):
        from src.brain import mission_store
        m = mission_factory()
        mission_store.update_progress(m.id, 33.0, note="first checkpoint")
        events = mission_store.list_events(m.id)
        kinds = [e["kind"] for e in events]
        assert "progress" in kinds


@pytest.mark.integration
@pytest.mark.quest
class TestArtifacts:
    def test_attach_artifact_records(self, mission_factory):
        from src.brain import mission_store
        m = mission_factory()
        mission_store.attach_artifact(m.id, "one_pager", "/tmp/x.pptx")
        events = mission_store.list_events(m.id)
        kinds = [e["kind"] for e in events]
        assert "artifact_attached" in kinds

    def test_attach_artifact_in_mission(self, mission_factory):
        from src.brain import mission_store
        import json
        m = mission_factory()
        mission_store.attach_artifact(m.id, "impl_plan", "/tmp/plan.pptx")
        got = mission_store.get_mission(m.id)
        paths = got.artifact_paths or {}
        assert "impl_plan" in paths


@pytest.mark.integration
@pytest.mark.quest
class TestStatusAndEvents:
    def test_status_change_recorded(self, mission_factory):
        from src.brain import mission_store
        m = mission_factory()
        mission_store.update_status(m.id, "in_progress")
        events = mission_store.list_events(m.id)
        kinds = [e["kind"] for e in events]
        assert "status_changed" in kinds

    def test_created_event_exists(self, mission_factory):
        from src.brain import mission_store
        m = mission_factory()
        events = mission_store.list_events(m.id)
        kinds = [e["kind"] for e in events]
        assert "created" in kinds

    def test_record_custom_event(self, mission_factory):
        from src.brain import mission_store
        m = mission_factory()
        mission_store.record_event(m.id, "custom_ping", {"source": "test"})
        events = mission_store.list_events(m.id)
        kinds = [e["kind"] for e in events]
        assert "custom_ping" in kinds

    def test_list_events_limit(self, mission_factory):
        from src.brain import mission_store
        m = mission_factory()
        for i in range(10):
            mission_store.record_event(m.id, "tick", {"i": i})
        events = mission_store.list_events(m.id, limit=5)
        assert len(events) <= 5

    def test_mark_refreshed(self, mission_factory):
        from src.brain import mission_store
        m = mission_factory()
        mission_store.mark_refreshed(m.id)
        got = mission_store.get_mission(m.id)
        assert got.last_refreshed_at is not None


@pytest.mark.integration
@pytest.mark.quest
class TestDelete:
    def test_delete_removes_from_list_open(self):
        """Use mission_store directly (not fixture) to test delete path."""
        from src.brain import mission_store, quests
        m = mission_store.create_mission(
            quest_id=quests.ROOT_QUEST_ID,
            site="DEL_TEST",
            user_query="delete smoke",
            parsed_intent={},
            scope_tags=["data_quality"],
            target_entity_kind="site",
            target_entity_key="DEL_TEST",
            horizon_days=30,
        )
        mid = m.id
        mission_store.delete_mission(mid)
        opens = mission_store.list_open(limit=200)
        ids = [getattr(o, "id", None) for o in opens]
        assert mid not in ids

    def test_get_after_delete_returns_none(self):
        from src.brain import mission_store, quests
        m = mission_store.create_mission(
            quest_id=quests.ROOT_QUEST_ID,
            site="DEL2",
            user_query="delete check",
            parsed_intent={},
            scope_tags=["fulfillment"],
            target_entity_kind="site",
            target_entity_key="DEL2",
            horizon_days=30,
        )
        mid = m.id
        mission_store.delete_mission(mid)
        assert mission_store.get_mission(mid) is None
