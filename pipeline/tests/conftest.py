"""pytest conftest — shared fixtures for the Supply Chain Brain test suite.

All fixtures are local-only (no live DB, no Azure SQL, no LLM endpoints).

Key fixtures
------------
tmp_db          — fresh temporary SQLite path (auto-cleaned per test)
stub_llm        — monkeypatches llm_ensemble.dispatch_parallel to raise so
                  intent_parser falls through to its keyword fallback
mission_factory — callable that creates a disposable Mission in the
                  shared findings_index.db (cleans up after the test)
synth_result    — a pre-built MissionResult for viz/deck tests
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path
from typing import Generator

import pytest

# ---------------------------------------------------------------------------
# Path bootstrap: make `from src.brain import ...` work everywhere
# ---------------------------------------------------------------------------
_PIPELINE_ROOT = Path(__file__).resolve().parent.parent  # pipeline/
sys.path.insert(0, str(_PIPELINE_ROOT / "src"))
sys.path.insert(0, str(_PIPELINE_ROOT))


# ---------------------------------------------------------------------------
# LLM stub — prevents real HTTP calls during tests
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def stub_llm(monkeypatch):
    """Replace dispatch_parallel with a stub that always raises.

    intent_parser catches the exception and falls through to its deterministic
    keyword fallback, which is the contract we actually want to test.
    """
    try:
        from src.brain import llm_ensemble  # type: ignore
        monkeypatch.setattr(llm_ensemble, "dispatch_parallel",
                            lambda *a, **k: (_ for _ in ()).throw(
                                RuntimeError("LLM stubbed in tests")))
    except Exception:
        pass
    yield


# ---------------------------------------------------------------------------
# Temporary SQLite path
# ---------------------------------------------------------------------------
@pytest.fixture
def tmp_db(tmp_path) -> Path:
    """Return a fresh temporary SQLite file path (auto-cleaned)."""
    return tmp_path / "test_brain.sqlite"


# ---------------------------------------------------------------------------
# Mission factory — creates a real Mission and deletes it on teardown
# ---------------------------------------------------------------------------
@pytest.fixture
def mission_factory():
    """Return a callable that creates a Mission and auto-cleans up after the test."""
    from src.brain import mission_store, quests

    created: list[str] = []

    def _make(
        *,
        site: str = "TEST",
        user_query: str = "fixture mission",
        scope_tags: list[str] | None = None,
        target_entity_kind: str = "site",
        target_entity_key: str = "TEST",
        horizon_days: int = 30,
    ):
        m = mission_store.create_mission(
            quest_id=quests.ROOT_QUEST_ID,
            site=site,
            user_query=user_query,
            parsed_intent={"fixture": True},
            scope_tags=scope_tags or ["data_quality"],
            target_entity_kind=target_entity_kind,
            target_entity_key=target_entity_key,
            horizon_days=horizon_days,
        )
        created.append(m.id)
        return m

    yield _make

    # teardown
    for mid in created:
        try:
            mission_store.delete_mission(mid)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Synthetic MissionResult
# ---------------------------------------------------------------------------
@pytest.fixture
def synth_result():
    """Return a fully-populated MissionResult built from synthetic data."""
    from src.brain import orchestrator

    outcomes = [
        orchestrator.AnalyzerOutcome(
            scope_tag="fulfillment", analyzer="otd",
            ok=True, elapsed_ms=12, n_findings=5,
            metrics={"otd_pct": 84.3, "late_dollars_at_risk": 18750.0},
        ),
        orchestrator.AnalyzerOutcome(
            scope_tag="lead_time", analyzer="po_receipts",
            ok=True, elapsed_ms=8, n_findings=3,
            metrics={"lead_time_median": 22, "lead_time_p90": 48},
        ),
        orchestrator.AnalyzerOutcome(
            scope_tag="data_quality", analyzer="imputation",
            ok=True, elapsed_ms=5, n_findings=1,
            metrics={"null_pct": 0.08},
        ),
    ]
    findings = [
        {
            "page": "fixture",
            "kind": f"kind_{i % 4}",
            "key": f"KEY_{i:04d}",
            "score": max(0.05, 1.0 - i * 0.04),
            "payload": {"mission_id": "FIXTURE"},
        }
        for i in range(25)
    ]
    return orchestrator.MissionResult(
        mission_id="FIXTURE",
        site="TEST",
        scope_tags=["fulfillment", "lead_time", "data_quality"],
        outcomes=outcomes,
        findings=findings,
        kpi_snapshot={
            "fulfillment.otd_pct": 84.3,
            "lead_time.lead_time_median": 22,
            "data_quality.null_pct": 0.08,
        },
        progress_pct=41.0,
        elapsed_ms=25,
        refresh=True,
    )
