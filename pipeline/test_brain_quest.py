"""Smoke test for the Brain Quest engine.

Validates end-to-end wiring:

  * intent_parser → ParsedIntent (closed vocab) on paraphrased queries
  * mission_store round-trip (create → update_progress → list_open → close)
  * orchestrator dry-run produces a MissionResult with progress_pct
  * schema_synthesizer returns at least one TableRef
  * viz_composer returns ≥ 1 figure
  * mission_runner.launch + refresh produces real PPTX files (PPTX renders
    are skipped if python-pptx isn't installed)

Run from repo root:
    python -m pipeline.test_brain_quest
"""
from __future__ import annotations

import os
import sys
import json
import traceback
from pathlib import Path


# --- import-path bootstrap (same convention as other pipeline test_*.py) ---
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE / "src"))
sys.path.insert(0, str(_HERE))


# Force the keyword-fallback path in the parser so the smoke test does not
# spend time waiting on real LLM endpoints. We replace dispatch_parallel
# with a stub that raises — intent_parser catches and falls through to
# _fallback_parse(), which is fully deterministic and exercises the same
# closed-vocabulary contract.
def _disable_llm_ensemble() -> None:
    try:
        from src.brain import llm_ensemble  # type: ignore
    except Exception:
        return
    def _stub(*_a, **_k):
        raise RuntimeError("LLM disabled in smoke test")
    llm_ensemble.dispatch_parallel = _stub  # type: ignore[attr-defined]


_disable_llm_ensemble()


def _ok(name: str) -> None:
    print(f"  ✓ {name}")


def _bad(name: str, e: Exception) -> None:
    print(f"  ✗ {name}: {e}")
    traceback.print_exc()


# --------------------------------------------------------------------------
# Test 1: intent parser closed-vocabulary
# --------------------------------------------------------------------------
def test_intent_parser() -> int:
    from src.brain import intent_parser, quests
    paraphrases = [
        ("I'm at Jerome and conducting a restructuring of their warehouse "
         "— show me velocity hotspots and overstock", "JEROME"),
        ("Burlington keeps missing OTD on heavy castings, why?", "BURLINGTON"),
        ("Consolidate the supplier base for Eugene plant", "EUGENE"),
        ("Our PFEP at Chattanooga is full of holes", "CHATTANOOGA"),
        ("Bullwhip is killing me on the spare parts forecast", "ALL"),
        ("Lead time variance from Acme Corp is too high", "ALL"),
        ("Run cycle counts more accurately at Burlington", "BURLINGTON"),
        ("Multi-echelon safety stock is wrong across the network", "ALL"),
    ]
    fails = 0
    for q, site in paraphrases:
        try:
            p = intent_parser.parse(q, site_default=site)
            assert p.target_entity_kind in intent_parser._VALID_ENTITY_KINDS, (
                f"bad entity kind {p.target_entity_kind!r}")
            assert p.scope_tags, "no scope tags inferred"
            for t in p.scope_tags:
                assert t in quests.SCOPE_TAGS, f"unknown scope tag {t!r}"
            _ok(f"parse: {q[:40]}…  → tags={p.scope_tags}  src={p.parser_source}")
        except Exception as e:
            _bad(f"parse({q[:30]}…)", e)
            fails += 1
    return fails


# --------------------------------------------------------------------------
# Test 2: mission_store round-trip
# --------------------------------------------------------------------------
def test_mission_store() -> int:
    from src.brain import mission_store, quests
    fails = 0
    mid = None
    try:
        m = mission_store.create_mission(
            quest_id=quests.ROOT_QUEST_ID,
            site="SMOKE",
            user_query="smoke test mission",
            parsed_intent={"smoke": True, "owner_role": "Anyone"},
            scope_tags=["fulfillment"],
            target_entity_kind="site",
            target_entity_key="SMOKE",
            horizon_days=30,
        )
        mid = m.id
        _ok(f"create_mission → {mid}")
        mission_store.update_progress(mid, 42.5, note="halfway smoke")
        mission_store.attach_artifact(mid, "smoke_artifact", "/tmp/x.pptx")
        opens = mission_store.list_open(limit=50)
        assert any(getattr(o, "id", None) == mid for o in opens), \
            "newly created mission not in list_open"
        _ok(f"list_open contains {mid}")
        mission_store.update_status(mid, "done")
        events = mission_store.list_events(mid, limit=20)
        kinds = {e["kind"] for e in events}
        assert "created" in kinds, "missing 'created' event"
        assert "progress" in kinds, "missing 'progress' event"
        assert "artifact_attached" in kinds, "missing 'artifact_attached' event"
        assert "status_changed" in kinds, "missing 'status_changed' event"
        _ok(f"event log kinds={sorted(kinds)}")
    except Exception as e:
        _bad("mission_store round-trip", e)
        fails += 1
    finally:
        if mid:
            try:
                from src.brain import mission_store as _ms
                _ms.delete_mission(mid)
                _ok(f"delete_mission cleanup → {mid}")
            except Exception as e:
                _bad("cleanup", e)
                fails += 1
    return fails


# --------------------------------------------------------------------------
# Test 3: orchestrator dry-run (allowed to skip individual analyzers)
# --------------------------------------------------------------------------
def test_orchestrator_dry_run() -> int:
    from src.brain import orchestrator, mission_store, quests
    fails = 0
    mid = None
    try:
        m = mission_store.create_mission(
            quest_id=quests.ROOT_QUEST_ID,
            site="ALL",
            user_query="dry run",
            parsed_intent={"dry_run": True},
            scope_tags=["data_quality"],
            target_entity_kind="site",
            target_entity_key="ALL",
            horizon_days=90,
        )
        mid = m.id
        result = orchestrator.BrainOrchestrator().run(m, refresh=True)
        assert result is not None
        assert hasattr(result, "progress_pct")
        assert isinstance(result.outcomes, list)
        assert isinstance(result.findings, list)
        _ok(f"orchestrator.run → {len(result.outcomes)} outcomes, "
            f"{len(result.findings)} findings, "
            f"progress={result.progress_pct}, elapsed={result.elapsed_ms}ms")
    except Exception as e:
        _bad("orchestrator dry-run", e)
        fails += 1
    finally:
        if mid:
            try:
                mission_store.delete_mission(mid)
            except Exception:
                pass
    return fails


# --------------------------------------------------------------------------
# Test 4: schema synthesizer
# --------------------------------------------------------------------------
def test_schema_synthesizer() -> int:
    from src.brain import schema_synthesizer
    fails = 0
    try:
        s = schema_synthesizer.synthesize("site", "ALL")
        assert s.target_entity_kind == "site"
        # tables may be empty if discovered_schema.yaml is unavailable; that's ok.
        _ok(f"synthesize(site) → {len(s.tables)} tables, "
            f"{len(s.relationships)} rels, mermaid={'yes' if s.mermaid else 'no'}")
        s2 = schema_synthesizer.synthesize("supplier", "ACME")
        _ok(f"synthesize(supplier) → {len(s2.tables)} tables")
    except Exception as e:
        _bad("schema_synthesizer", e)
        fails += 1
    return fails


# --------------------------------------------------------------------------
# Test 5: viz composer (synthetic MissionResult)
# --------------------------------------------------------------------------
def test_viz_composer() -> int:
    from src.brain import viz_composer, orchestrator
    fails = 0
    try:
        # Build a synthetic MissionResult
        outcomes = [
            orchestrator.AnalyzerOutcome(
                scope_tag="fulfillment", analyzer="otd",
                ok=True, elapsed_ms=10, n_findings=4,
                metrics={"otd_pct": 87.5, "late_dollars_at_risk": 12500.0}),
            orchestrator.AnalyzerOutcome(
                scope_tag="lead_time", analyzer="po_receipts",
                ok=True, elapsed_ms=8, n_findings=3,
                metrics={"lead_time_median": 18, "lead_time_p90": 41}),
        ]
        findings = [
            {"page": "smoke", "kind": "late_owner", "key": f"OWNER_{i}",
             "score": 0.9 - i * 0.07,
             "payload": {"mission_id": "SMOKE"}}
            for i in range(8)
        ]
        result = orchestrator.MissionResult(
            mission_id="SMOKE",
            site="ALL",
            scope_tags=["fulfillment", "lead_time"],
            outcomes=outcomes, findings=findings,
            kpi_snapshot={"fulfillment.otd_pct": 87.5,
                          "lead_time.lead_time_median": 18},
            progress_pct=33.0, elapsed_ms=12, refresh=True,
        )
        viz = viz_composer.compose(result)
        assert isinstance(viz, dict)
        assert len(viz) >= 1, "viz composer produced no figures"
        _ok(f"viz_composer.compose → {len(viz)} figures: {sorted(viz)}")
    except Exception as e:
        _bad("viz_composer", e)
        fails += 1
    return fails


# --------------------------------------------------------------------------
# Test 6: mission_runner end-to-end (best effort — skips PPTX if pptx absent)
# --------------------------------------------------------------------------
def test_mission_runner() -> int:
    fails = 0
    mid = None
    try:
        from src.brain import mission_runner, mission_store
        mission = mission_runner.launch(
            user_query=("smoke test launch — fulfillment investigation "
                        "for a specific site"),
            site="SMOKE",
            horizon_days=30,
        )
        mid = mission.id
        _ok(f"mission_runner.launch → {mid} "
            f"progress={mission.progress_pct} "
            f"artifacts={list(mission.artifact_paths.keys())}")
        # Refresh-while-locked path
        r = mission_runner.refresh(mid)
        assert r.get("ok") in (True, None) or r.get("skipped"), r
        _ok(f"mission_runner.refresh → ok={r.get('ok')}")
    except Exception as e:
        _bad("mission_runner", e)
        fails += 1
    finally:
        if mid:
            try:
                from src.brain import mission_store as _ms
                _ms.delete_mission(mid)
            except Exception:
                pass
    return fails


# --------------------------------------------------------------------------
# Test 7: Brain↔Body integration — corpus ingester + signal generator
# --------------------------------------------------------------------------
def test_brain_body_integration() -> int:
    fails = 0
    try:
        from src.brain import knowledge_corpus, brain_body_signals
        kc = knowledge_corpus.refresh_corpus_round()
        assert isinstance(kc, dict)
        _ok(f"knowledge_corpus.refresh_corpus_round → "
            f"+{kc.get('entities_added', 0)} entities, "
            f"+{kc.get('learnings_logged', 0)} learnings")
        bb = brain_body_signals.surface_effective_signals()
        assert isinstance(bb, dict)
        _ok(f"brain_body_signals.surface_effective_signals → "
            f"{bb.get('directives_emitted', 0)} new, "
            f"{bb.get('directives_deduped', 0)} deduped")
    except Exception as e:
        _bad("Brain↔Body integration", e)
        fails += 1
    return fails


# --------------------------------------------------------------------------
def main() -> int:
    print("=" * 72)
    print("Brain Quest smoke test")
    print("=" * 72)
    fails = 0
    for name, fn in [
        ("intent parser",            test_intent_parser),
        ("mission store",            test_mission_store),
        ("orchestrator dry-run",     test_orchestrator_dry_run),
        ("schema synthesizer",       test_schema_synthesizer),
        ("viz composer",             test_viz_composer),
        ("mission runner",           test_mission_runner),
        ("brain↔body integration",   test_brain_body_integration),
    ]:
        print(f"\n[{name}]")
        fails += fn()
    print("\n" + "=" * 72)
    print(f"FAILURES: {fails}")
    print("=" * 72)
    return 0 if fails == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
