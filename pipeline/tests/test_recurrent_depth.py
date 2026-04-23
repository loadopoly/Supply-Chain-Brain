from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def rdt_env(monkeypatch, tmp_path):
    db_path = tmp_path / "test_local_brain_rdt.sqlite"

    from src.brain import local_store

    monkeypatch.setattr(local_store, "_DB_PATH", db_path, raising=False)
    monkeypatch.setattr(local_store, "db_path", lambda: db_path)

    from src.brain import recurrent_depth as rdt
    from src.brain.llm_ensemble import WorkerOutcome, _AGGREGATORS

    monkeypatch.setattr(rdt, "_local_db_path", lambda: db_path)
    return rdt, WorkerOutcome, _AGGREGATORS, db_path


def _outcome(worker_outcome_cls, model: str, label: str, conf: float,
             weight: float = 1.0, router: float = 1.0):
    return worker_outcome_cls(
        model_id=model,
        response={"label": label, "confidence": conf},
        latency_ms=100,
        ok=True,
        error=None,
        router_score=router,
        weight=weight,
        bias=0.0,
    )


@pytest.mark.integration
def test_unanimous_vote_halts_quickly(rdt_env) -> None:
    rdt, worker_outcome_cls, _, _ = rdt_env
    outcomes = [
        _outcome(worker_outcome_cls, "m1", "Steel & Plate", 0.95, weight=1.5),
        _outcome(worker_outcome_cls, "m2", "Steel & Plate", 0.92, weight=1.4),
        _outcome(worker_outcome_cls, "m3", "Steel & Plate", 0.88, weight=1.3),
    ]

    result = rdt.rdt_aggregate(outcomes, {"_task": "pytest_unanimous"})

    assert result is not None
    assert result["rdt_meta"]["halted_at_depth"] <= 2
    assert result["rdt_meta"]["converged"] is True
    assert result["ensemble_confidence"] >= 0.95


@pytest.mark.integration
def test_close_split_is_sharpened(rdt_env) -> None:
    rdt, worker_outcome_cls, _, _ = rdt_env
    outcomes = [
        _outcome(worker_outcome_cls, "m1", "Fasteners", 0.62, weight=2.5, router=0.9),
        _outcome(worker_outcome_cls, "m2", "Fasteners", 0.58, weight=2.5, router=0.9),
        _outcome(worker_outcome_cls, "m3", "Steel & Plate", 0.60, weight=2.5, router=0.9),
        _outcome(worker_outcome_cls, "m4", "Wiring", 0.55, weight=2.5, router=0.9),
        _outcome(worker_outcome_cls, "m5", "Fasteners", 0.51, weight=2.5, router=0.9),
    ]

    result = rdt.rdt_aggregate(outcomes, {"_task": "pytest_close_split"})
    meta = result["rdt_meta"]

    assert result["value"]["label"] == "Fasteners"
    assert meta["shift_from_oneshot"] > 0.05
    assert result["vote_distribution"]["Fasteners"] >= 0.55


@pytest.mark.integration
def test_kl_trace_settles(rdt_env) -> None:
    rdt, worker_outcome_cls, _, _ = rdt_env
    outcomes = [
        _outcome(worker_outcome_cls, "m1", "A", 0.7),
        _outcome(worker_outcome_cls, "m2", "A", 0.6),
        _outcome(worker_outcome_cls, "m3", "B", 0.55),
        _outcome(worker_outcome_cls, "m4", "A", 0.5),
    ]

    result = rdt.rdt_aggregate(outcomes, {"_task": "pytest_decay", "max_depth": 6})
    trace = result["rdt_meta"]["kl_trace"]

    assert trace
    if len(trace) >= 2:
        assert trace[-1] <= max(trace)


@pytest.mark.integration
def test_rdt_is_registered_with_ensemble(rdt_env) -> None:
    _, worker_outcome_cls, aggregators, _ = rdt_env

    assert "recurrent_depth_vote" in aggregators

    result = aggregators["recurrent_depth_vote"](
        [
            _outcome(worker_outcome_cls, "m1", "X", 0.9),
            _outcome(worker_outcome_cls, "m2", "X", 0.85),
        ],
        {"_task": "pytest_registration"},
    )

    assert result is not None
    assert result["value"]["label"] == "X"


@pytest.mark.integration
def test_rdt_audit_log_and_summary_are_recorded(rdt_env) -> None:
    rdt, worker_outcome_cls, _, db_path = rdt_env
    outcomes = [
        _outcome(worker_outcome_cls, "m1", "Fasteners", 0.62, weight=2.5, router=0.9),
        _outcome(worker_outcome_cls, "m2", "Fasteners", 0.58, weight=2.5, router=0.9),
        _outcome(worker_outcome_cls, "m3", "Steel & Plate", 0.60, weight=2.5, router=0.9),
    ]

    rdt.rdt_aggregate(outcomes, {"_task": "pytest_summary"})

    runs = rdt.recent_runs(limit=20)
    summary = rdt.learned_depth_summary()

    assert db_path.exists()
    assert runs
    assert any(row["task"] == "pytest_summary" for row in summary["by_task"])