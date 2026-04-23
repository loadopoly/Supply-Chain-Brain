from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest


@pytest.mark.bench
@pytest.mark.slow
@pytest.mark.integration
def test_bench_quest_engine_emits_structured_results(tmp_path) -> None:
    results_dir = tmp_path / "bench_results"
    pipeline_root = Path(__file__).resolve().parent.parent

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from pathlib import Path; "
                "from bench import bench_quest_engine; "
                f"bench_quest_engine.run_benchmarks(rows=8, repeats=1, results_dir=Path(r'{results_dir}'), emit_stdout=False)"
            ),
        ],
        cwd=pipeline_root,
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )

    assert result.returncode == 0

    latest = results_dir / "latest_quest.csv"
    timestamped = list(results_dir.glob("bench_quest_engine-*.csv"))
    assert latest.exists()
    assert timestamped

    df = pd.read_csv(latest)
    assert not df.empty
    assert {"benchmark", "scenario", "elapsed_s", "n", "repeats"} <= set(df.columns)

    expected = {
        ("intent_parser.parse", "single_query"),
        ("intent_parser.parse", "8_paraphrases"),
        ("mission_store.create_mission", "create_8"),
        ("schema_synthesizer.synthesize", "6_entity_kinds"),
        ("viz_composer.compose", "100_findings"),
    }
    actual = {(row.benchmark, row.scenario) for row in df.itertuples()}
    assert expected <= actual
    assert (df["elapsed_s"] >= 0).all()
    assert (df["elapsed_s"] > 0).any()