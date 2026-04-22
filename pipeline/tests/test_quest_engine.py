"""Unit + integration tests for the Quest engine pipeline:
  schema_synthesizer, viz_composer, orchestrator, mission_runner.

All LLM calls stubbed by conftest. DB-touching tests use the shared
findings_index.db with mission_factory cleanup.
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# schema_synthesizer
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.quest
class TestSchemaSynthesizer:
    @pytest.mark.parametrize("kind", [
        "site", "supplier", "part_family", "buyer", "warehouse", "customer"
    ])
    def test_synthesize_known_kinds(self, kind):
        from src.brain.schema_synthesizer import synthesize, EntitySchema
        s = synthesize(kind, "TEST")
        assert isinstance(s, EntitySchema)
        assert s.target_entity_kind == kind

    def test_synthesize_returns_tables(self):
        from src.brain.schema_synthesizer import synthesize
        s = synthesize("site", "ALL")
        assert isinstance(s.tables, list)

    def test_synthesize_has_mermaid_or_none(self):
        from src.brain.schema_synthesizer import synthesize
        s = synthesize("supplier", "S001")
        assert s.mermaid is None or isinstance(s.mermaid, str)

    def test_synthesize_relationships_list(self):
        from src.brain.schema_synthesizer import synthesize
        s = synthesize("part_family", "FASTENERS")
        assert isinstance(s.relationships, list)

    def test_synthesize_unknown_kind_graceful(self):
        """Should not raise even for unknown entity kinds."""
        from src.brain.schema_synthesizer import synthesize
        s = synthesize("unknown_entity", "X")
        assert s is not None

    def test_synthesize_empty_key(self):
        from src.brain.schema_synthesizer import synthesize
        s = synthesize("buyer", "")
        assert s is not None


# ---------------------------------------------------------------------------
# viz_composer
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.quest
class TestVizComposer:
    def test_compose_returns_dict(self, synth_result):
        from src.brain.viz_composer import compose
        viz = compose(synth_result)
        assert isinstance(viz, dict)

    def test_compose_has_kpi_trend(self, synth_result):
        from src.brain.viz_composer import compose
        viz = compose(synth_result)
        assert "kpi_trend" in viz

    def test_compose_figures_count(self, synth_result):
        from src.brain.viz_composer import compose
        viz = compose(synth_result)
        assert len(viz) >= 1

    def test_compose_all_expected_keys(self, synth_result):
        from src.brain.viz_composer import compose
        viz = compose(synth_result)
        expected = {"kpi_trend", "pareto", "heatmap_matrix",
                    "network", "sankey_flow", "cohort_survival"}
        assert expected == set(viz.keys())

    def test_caption_for_figure(self, synth_result):
        from src.brain.viz_composer import compose, caption_for
        viz = compose(synth_result)
        cap = caption_for(viz["kpi_trend"])
        assert isinstance(cap, str)

    def test_compose_empty_result(self):
        """viz_composer must not crash on an empty MissionResult."""
        from src.brain.viz_composer import compose
        from src.brain.orchestrator import MissionResult
        empty = MissionResult(
            mission_id="EMPTY", site="X", scope_tags=[],
            outcomes=[], findings=[],
            kpi_snapshot={}, progress_pct=0.0, elapsed_ms=0, refresh=False,
        )
        viz = compose(empty)
        assert isinstance(viz, dict)


# ---------------------------------------------------------------------------
# orchestrator
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.quest
class TestOrchestrator:
    def test_run_returns_mission_result(self, mission_factory):
        from src.brain.orchestrator import BrainOrchestrator, MissionResult
        m = mission_factory(scope_tags=["data_quality"])
        result = BrainOrchestrator().run(m, refresh=True)
        assert isinstance(result, MissionResult)

    def test_result_has_progress_pct(self, mission_factory):
        from src.brain.orchestrator import BrainOrchestrator
        m = mission_factory(scope_tags=["data_quality"])
        result = BrainOrchestrator().run(m)
        assert isinstance(result.progress_pct, float)
        assert 0.0 <= result.progress_pct <= 100.0

    def test_outcomes_is_list(self, mission_factory):
        from src.brain.orchestrator import BrainOrchestrator
        m = mission_factory(scope_tags=["data_quality"])
        result = BrainOrchestrator().run(m)
        assert isinstance(result.outcomes, list)

    def test_findings_is_list(self, mission_factory):
        from src.brain.orchestrator import BrainOrchestrator
        m = mission_factory(scope_tags=["data_quality"])
        result = BrainOrchestrator().run(m)
        assert isinstance(result.findings, list)

    def test_elapsed_ms_positive(self, mission_factory):
        from src.brain.orchestrator import BrainOrchestrator
        m = mission_factory(scope_tags=["data_quality"])
        result = BrainOrchestrator().run(m)
        assert result.elapsed_ms >= 0

    def test_as_dict_serialisable(self, mission_factory):
        import json
        from src.brain.orchestrator import BrainOrchestrator
        m = mission_factory(scope_tags=["data_quality"])
        result = BrainOrchestrator().run(m)
        try:
            json.dumps(result.as_dict())
        except (TypeError, ValueError) as e:
            pytest.fail(f"MissionResult.as_dict() not JSON-serialisable: {e}")


# ---------------------------------------------------------------------------
# mission_runner
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.quest
@pytest.mark.slow
class TestMissionRunner:
    def test_launch_returns_mission(self):
        from src.brain import mission_runner, mission_store
        m = mission_runner.launch(
            user_query="fulfillment investigation smoke",
            site="SMOKE",
            horizon_days=30,
        )
        try:
            assert m is not None
            assert m.id.startswith("m_")
        finally:
            mission_store.delete_mission(m.id)

    def test_launch_creates_artifacts_entry(self):
        from src.brain import mission_runner, mission_store
        m = mission_runner.launch(
            user_query="data quality audit",
            site="TEST",
            horizon_days=30,
        )
        try:
            assert isinstance(m.artifact_paths, dict)
            assert len(m.artifact_paths) >= 1
        finally:
            mission_store.delete_mission(m.id)

    def test_refresh_returns_dict(self):
        from src.brain import mission_runner, mission_store
        m = mission_runner.launch(
            user_query="inventory sizing review",
            site="ALL",
            horizon_days=60,
        )
        try:
            r = mission_runner.refresh(m.id)
            assert isinstance(r, dict)
        finally:
            mission_store.delete_mission(m.id)

    def test_refresh_unknown_mission(self):
        from src.brain import mission_runner
        r = mission_runner.refresh("m_does_not_exist_0000_00000000")
        assert isinstance(r, dict)
        # Should not raise; should report error/skipped
        assert "error" in r or "skipped" in r or r.get("ok") is False

    def test_refresh_open_missions_list(self):
        from src.brain import mission_runner
        results = mission_runner.refresh_open_missions(max_concurrent=1, limit=5)
        assert isinstance(results, list)
    def test_launch_rejects_empty_query(self):
        from src.brain import mission_runner
        import pytest
        with pytest.raises(ValueError, match="non-empty"):
            mission_runner.launch("", site="ALL")