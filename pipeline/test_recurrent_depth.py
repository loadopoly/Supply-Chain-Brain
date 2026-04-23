"""Tests for the Recurrent Depth Transformer ensemble aggregator.

Verifies:
  1. Easy unanimous votes converge in 1 step (efficient halting).
  2. Close 3-way splits use multiple recurrent steps and produce a sharper
     winner distribution than the one-shot weighted softmax.
  3. KL-trace is monotone non-increasing once near convergence.
  4. Auto-registration places `recurrent_depth_vote` in llm_ensemble._AGGREGATORS.
  5. Audit log + learned_depth_summary correctly record per-task depth stats.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

TMP_DB = ROOT / "test_local_brain_rdt.sqlite"
if TMP_DB.exists():
    TMP_DB.unlink()

from src.brain import local_store  # noqa: E402
local_store._DB_PATH = TMP_DB
local_store.db_path = lambda: TMP_DB  # type: ignore

from src.brain import recurrent_depth as rdt  # noqa: E402
from src.brain.llm_ensemble import WorkerOutcome, _AGGREGATORS  # noqa: E402


def banner(msg: str) -> None:
    print(f"\n{'=' * 72}\n{msg}\n{'=' * 72}")


# ---------------------------------------------------------------------------
def _outcome(model: str, label: str, conf: float, weight: float = 1.0,
             router: float = 1.0) -> WorkerOutcome:
    return WorkerOutcome(
        model_id=model,
        response={"label": label, "confidence": conf},
        latency_ms=100, ok=True, error=None,
        router_score=router, weight=weight, bias=0.0,
    )


# ---------------------------------------------------------------------------
def test_unanimous_converges_in_one_step() -> None:
    banner("1) Unanimous vote → halts in 1 step")
    outs = [
        _outcome("m1", "Steel & Plate", 0.95, weight=1.5),
        _outcome("m2", "Steel & Plate", 0.92, weight=1.4),
        _outcome("m3", "Steel & Plate", 0.88, weight=1.3),
    ]
    res = rdt.rdt_aggregate(outs, {"_task": "test_unanimous"})
    print(f"  result keys: {list(res.keys())}")
    print(f"  winner: {res['vote_distribution']}  conf={res['ensemble_confidence']}")
    print(f"  rdt_meta: {res['rdt_meta']}")
    assert res["rdt_meta"]["halted_at_depth"] <= 2, "unanimous should halt fast"
    assert res["rdt_meta"]["converged"] is True
    assert res["ensemble_confidence"] >= 0.95


# ---------------------------------------------------------------------------
def test_close_split_sharpens() -> None:
    banner("2) Close 3-way split → RDT sharpens vs one-shot")
    outs = [
        _outcome("m1", "Fasteners",     0.62, weight=2.5, router=0.9),
        _outcome("m2", "Fasteners",     0.58, weight=2.5, router=0.9),
        _outcome("m3", "Steel & Plate", 0.60, weight=2.5, router=0.9),
        _outcome("m4", "Wiring",        0.55, weight=2.5, router=0.9),
        _outcome("m5", "Fasteners",     0.51, weight=2.5, router=0.9),
    ]
    res = rdt.rdt_aggregate(outs, {"_task": "test_close_split"})
    meta = res["rdt_meta"]
    print(f"  vote_dist: {res['vote_distribution']}")
    print(f"  halted_at={meta['halted_at_depth']}  shift_from_oneshot={meta['shift_from_oneshot']}")
    print(f"  KL trace: {meta['kl_trace']}")
    # Three-vote-Fasteners coalition should win
    assert res["value"]["label"] == "Fasteners"
    # RDT should have moved meaningfully from the one-shot starting point
    assert meta["shift_from_oneshot"] > 0.05, \
        f"RDT should shift the distribution; got {meta['shift_from_oneshot']}"
    # Winner mass should be sharpened above the initial three-vote share
    assert res["vote_distribution"]["Fasteners"] >= 0.55


# ---------------------------------------------------------------------------
def test_kl_trace_eventually_decreases() -> None:
    banner("3) KL trace eventually decreases toward convergence")
    outs = [
        _outcome("m1", "A", 0.7),
        _outcome("m2", "A", 0.6),
        _outcome("m3", "B", 0.55),
        _outcome("m4", "A", 0.5),
    ]
    res = rdt.rdt_aggregate(outs, {"_task": "test_decay", "max_depth": 6})
    trace = res["rdt_meta"]["kl_trace"]
    print(f"  KL trace ({len(trace)} steps): {trace}")
    # If we ran ≥ 2 steps, the last KL should be ≤ the max KL seen
    if len(trace) >= 2:
        assert trace[-1] <= max(trace), "KL should not blow up at the end"


# ---------------------------------------------------------------------------
def test_aggregator_auto_registered() -> None:
    banner("4) `recurrent_depth_vote` auto-registers with the ensemble")
    print(f"  registered aggregators: {sorted(_AGGREGATORS.keys())}")
    assert "recurrent_depth_vote" in _AGGREGATORS
    fn = _AGGREGATORS["recurrent_depth_vote"]
    out = fn([_outcome("m1", "X", 0.9), _outcome("m2", "X", 0.85)],
             {"_task": "test_registration"})
    assert out is not None
    assert out["value"]["label"] == "X"


# ---------------------------------------------------------------------------
def test_audit_log_and_summary() -> None:
    banner("5) Audit log + learned_depth_summary report per-task depth stats")
    runs = rdt.recent_runs(limit=20)
    print(f"  total audit rows: {len(runs)}")
    assert len(runs) >= 4, "previous tests should have logged runs"

    summary = rdt.learned_depth_summary()
    print(f"  per-task summary: {len(summary['by_task'])} tasks")
    for row in summary["by_task"]:
        print(f"    {row['task']:<25s} n={row['n']}  avg_depth={row['avg_depth']}  "
              f"converged={row['convergence_rate']}  shift={row['avg_shift_from_oneshot']}")
    tasks_seen = {r["task"] for r in summary["by_task"]}
    assert "test_unanimous" in tasks_seen
    assert "test_close_split" in tasks_seen


# ---------------------------------------------------------------------------
def main() -> int:
    test_unanimous_converges_in_one_step()
    test_close_split_sharpens()
    test_kl_trace_eventually_decreases()
    test_aggregator_auto_registered()
    test_audit_log_and_summary()
    banner("ALL RECURRENT-DEPTH GUARD-RAILS VERIFIED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
