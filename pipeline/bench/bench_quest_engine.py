"""Benchmarks for the Brain Quest Engine.

Run from pipeline/:
    .venv\Scripts\python.exe -m bench.bench_quest_engine [--rows N] [--repeats R]

Output: bench/results/bench_quest_engine-YYYYMMDD-HHMMSS.csv
        bench/results/latest_quest.csv   (always overwritten)
"""
from __future__ import annotations

import argparse
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Callable

import pandas as pd

# ── path bootstrap ──────────────────────────────────────────────────────────
_PIPELINE_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PIPELINE_ROOT / "src"))
sys.path.insert(0, str(_PIPELINE_ROOT))

# Stub the LLM ensemble so intent_parser uses its keyword fallback throughout
# (no network, no timeouts, deterministic output)
try:
    from src.brain import llm_ensemble  # type: ignore
    llm_ensemble.dispatch_parallel = lambda *a, **k: (_ for _ in ()).throw(
        RuntimeError("LLM stubbed in benchmark"))
except Exception:
    pass


# ── benchmark harness ────────────────────────────────────────────────────────
def time_it(fn: Callable, repeats: int = 3) -> float:
    """Return median wall time (seconds) over `repeats` runs after 1 warm-up."""
    fn()  # warm-up
    samples = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t0)
    return median(samples)


# ── synthetic data helpers ───────────────────────────────────────────────────
_PARAPHRASES = [
    ("fulfillment", "Burlington keeps missing OTD on heavy castings", "BURLINGTON"),
    ("lead_time", "Lead time variance from Acme keeps spiking", "ALL"),
    ("sourcing", "Consolidate the supplier base for Eugene", "EUGENE"),
    ("inventory_sizing", "Jerome overstock and velocity hotspots", "JEROME"),
    ("data_quality", "PFEP at Chattanooga is full of holes", "CHATTANOOGA"),
    ("demand_distortion", "Bullwhip is killing me on spare parts forecast", "ALL"),
    ("cycle_count", "Run cycle counts more accurately", "ALL"),
    ("network_position", "Multi-echelon safety stock placement across the network", "ALL"),
]

_ALL_ENTITY_KINDS = ["site", "supplier", "part_family", "buyer", "warehouse", "customer"]


def _synth_findings(n: int) -> list[dict]:
    import numpy as np
    rng = np.random.default_rng(42)
    return [
        {
            "page": f"page_{i % 10}",
            "kind": f"kind_{i % 4}",
            "key": f"KEY_{i:06d}",
            "score": float(rng.uniform(0.1, 1.0)),
            "payload": {"mission_id": "BENCH", "v": int(rng.integers(0, 100))},
        }
        for i in range(n)
    ]


def _make_mission_result(n_findings: int):
    from src.brain.orchestrator import MissionResult, AnalyzerOutcome
    outcomes = [
        AnalyzerOutcome(scope_tag="fulfillment", analyzer="otd",
                        ok=True, elapsed_ms=10, n_findings=n_findings // 2),
        AnalyzerOutcome(scope_tag="data_quality", analyzer="imputation",
                        ok=True, elapsed_ms=5, n_findings=n_findings // 2),
    ]
    return MissionResult(
        mission_id="BENCH",
        site="ALL",
        scope_tags=["fulfillment", "data_quality"],
        outcomes=outcomes,
        findings=_synth_findings(n_findings),
        kpi_snapshot={"fulfillment.otd_pct": 82.5, "data_quality.null_pct": 0.07},
        progress_pct=50.0,
        elapsed_ms=15,
        refresh=True,
    )


# ── benchmarks ───────────────────────────────────────────────────────────────
def bench_intent_parser_single(repeats: int) -> float:
    """Single query parse — keyword fallback path."""
    from src.brain.intent_parser import parse
    return time_it(lambda: parse("Burlington keeps missing OTD on heavy castings",
                                 "BURLINGTON"), repeats)


def bench_intent_parser_all_paraphrases(repeats: int) -> float:
    """Parse all 8 paraphrases in sequence — measures keyword mapping coverage."""
    from src.brain.intent_parser import parse
    return time_it(
        lambda: [parse(q, s) for _, q, s in _PARAPHRASES],
        repeats,
    )


def bench_intent_parser_bulk(n: int, repeats: int) -> float:
    """Parse the same query N times — baseline throughput."""
    from src.brain.intent_parser import parse
    return time_it(
        lambda: [parse("check inventory levels at Jerome", "JEROME") for _ in range(n)],
        repeats,
    )


def bench_mission_store_create(n: int, repeats: int) -> tuple[float, list[str]]:
    """Create N missions; return timing and list of IDs for cleanup."""
    from src.brain import mission_store, quests

    ids: list[str] = []

    def _run():
        nonlocal ids
        ids = []
        for i in range(n):
            m = mission_store.create_mission(
                quest_id=quests.ROOT_QUEST_ID,
                site="BENCH",
                user_query=f"bench mission {i}",
                parsed_intent={},
                scope_tags=["data_quality"],
                target_entity_kind="site",
                target_entity_key="BENCH",
                horizon_days=30,
            )
            ids.append(m.id)

    elapsed = time_it(_run, repeats)
    return elapsed, ids


def bench_mission_store_list_open(repeats: int) -> float:
    from src.brain import mission_store
    return time_it(lambda: mission_store.list_open(limit=50), repeats)


def bench_mission_store_update_progress(ids: list[str], repeats: int) -> float:
    from src.brain import mission_store
    if not ids:
        return 0.0
    return time_it(
        lambda: [mission_store.update_progress(mid, 55.0) for mid in ids],
        repeats,
    )


def bench_schema_synthesizer_all_kinds(repeats: int) -> float:
    from src.brain.schema_synthesizer import synthesize
    return time_it(
        lambda: [synthesize(k, "BENCH") for k in _ALL_ENTITY_KINDS],
        repeats,
    )


def bench_viz_composer(n_findings: int, repeats: int) -> float:
    from src.brain.viz_composer import compose
    result = _make_mission_result(n_findings)
    return time_it(lambda: compose(result), repeats)


# ── run_benchmarks (callable API used by tests and main()) ───────────────────
def run_benchmarks(
    rows: int = 100,
    repeats: int = 3,
    results_dir: "Path | None" = None,
    emit_stdout: bool = True,
) -> None:
    """Run all Quest Engine benchmarks and write CSVs to *results_dir*.

    Parameters
    ----------
    rows:
        N for bulk / CRUD benchmarks.
    repeats:
        How many timing repeats per benchmark.
    results_dir:
        Directory to write ``bench_quest_engine-*.csv`` and ``latest_quest.csv``.
        Defaults to ``<bench>/results/`` when *None*.
    emit_stdout:
        If *False*, suppresses all print output (useful in test mode).
    """
    N = rows
    R = repeats

    py   = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    plat = platform.platform()
    when = datetime.now(timezone.utc).isoformat(timespec="seconds")

    brows: list[dict] = []

    def _out(msg: str) -> None:
        if emit_stdout:
            print(msg)

    def bench(name: str, scenario: str, elapsed_s: float, n: int) -> None:
        tps = round(n / elapsed_s, 1) if elapsed_s > 0 and n > 0 else None
        brows.append({
            "benchmark": name, "scenario": scenario,
            "elapsed_s": round(elapsed_s, 4),
            "items_per_s": tps,
            "n": n, "repeats": R, "python": py, "platform": plat, "ts": when,
        })
        tps_str = f"{tps:>9.0f} /s" if tps else "           "
        _out(f"  {name:<45} {scenario:<20} {elapsed_s*1000:>9.1f} ms  {tps_str}")

    _out(f"\n{'─'*100}")
    _out(f"  Brain Quest Engine Benchmarks   rows={N}  repeats={R}  python={py}")
    _out(f"{'─'*100}")

    # intent_parser ─────────────────────────────────────────────────────────
    _out("\n  [intent_parser]")
    e = bench_intent_parser_single(R)
    bench("intent_parser.parse", "single_query", e, 1)

    e = bench_intent_parser_all_paraphrases(R)
    bench("intent_parser.parse", "8_paraphrases", e, 8)

    e = bench_intent_parser_bulk(N, R)
    bench("intent_parser.parse", f"bulk_{N}", e, N)

    # mission_store ─────────────────────────────────────────────────────────
    _out("\n  [mission_store]")
    e_create, ids = bench_mission_store_create(N, R)
    bench("mission_store.create_mission", f"create_{N}", e_create, N)

    e = bench_mission_store_list_open(R)
    bench("mission_store.list_open", "limit_50", e, 1)

    e = bench_mission_store_update_progress(ids, R)
    bench("mission_store.update_progress", f"update_{N}", e, N)

    # cleanup created missions
    try:
        from src.brain import mission_store
        for mid in ids:
            mission_store.delete_mission(mid)
    except Exception:
        pass

    # schema_synthesizer ────────────────────────────────────────────────────
    _out("\n  [schema_synthesizer]")
    e = bench_schema_synthesizer_all_kinds(R)
    bench("schema_synthesizer.synthesize", "6_entity_kinds", e, 6)

    # viz_composer ──────────────────────────────────────────────────────────
    _out("\n  [viz_composer]")
    for n_f in [100, 1_000, 5_000]:
        e = bench_viz_composer(n_f, R)
        bench("viz_composer.compose", f"{n_f}_findings", e, n_f)

    # results ────────────────────────────────────────────────────────────────
    if results_dir is None:
        results_dir = Path(__file__).parent / "results"
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    stamp    = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_path = results_dir / f"bench_quest_engine-{stamp}.csv"
    latest   = results_dir / "latest_quest.csv"

    df = pd.DataFrame(brows)
    df.to_csv(out_path, index=False)
    df.to_csv(latest,   index=False)

    _out(f"\n  Results → {out_path.name}  (and latest_quest.csv)")
    _out(f"{'─'*100}\n")


# ── CLI entry point ───────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Bench Quest Engine")
    ap.add_argument("--rows",    type=int, default=100,
                    help="N for bulk/CRUD benchmarks (default 100)")
    ap.add_argument("--repeats", type=int, default=3,
                    help="Timing repeats per benchmark (default 3)")
    args = ap.parse_args()
    run_benchmarks(rows=args.rows, repeats=args.repeats)


if __name__ == "__main__":
    main()
