from __future__ import annotations



__version__ = "0.14.2"

__release__ = (

    "Test + Benchmark Infrastructure, Hardening, Docs. "
    "88-test pytest suite (tests/conftest.py + 5 test modules) covering "
    "Quest taxonomy, intent_parser keyword fallback, mission_store CRUD, "
    "orchestrator, viz_composer, schema_synthesizer, deck builders with "
    "kaleido-absent fallback. bench_quest_engine.py benchmarks intent_parser / "
    "mission_store / schema_synthesizer / viz_composer. Hardened: orchestrator "
    "_CURRENT_MISSION in finally block; intent_parser 2 000-char truncation; "
    "mission_runner launch() rejects empty query. README Quest Engine section added."

)

__build_date__ = "2026-04-22"



PHASES = {

    "0.1.0": "Phase 1 - core (EOQ Bayesian-Poisson, OTD recursive, Procurement 360, Data Quality, Connectors)",

    "0.2.0": "Phase 2 - depth (graph backends, LinUCB ranker, hierarchical OTD index)",

    "0.3.0": "Phase 3 - MIT CTL research (hierarchical EOQ, causal lead-time, survival, bullwhip, multi-echelon, sustainability, freight portfolio, CVaR risk)",

    "0.4.0": "Phase 4 - platform (what-if sandbox, HMAC cross-app webhooks, nightly fact builder, session auth)",

    "0.4.1": "Self-driving pages, fixed MPA sidebar slugs, schema-browser diagnostics on live failures",

    "0.4.2": "All 14 pages Plotly rewrite complete; page_header/drilldown_table retired; DQ VOI heatmaps; Connectors modernized",

    "0.4.3": "Bug-fix wave: VOI Timestamp/datetime handling, graph node-kind propagation + discovery panel, EOQ outlier heatmap+quadrant, Procurement 360 supplier/part name lookups, Benchmarks rows_per_s merge collision, sidebar filter wiring",

    "0.4.4": "Pages 7-12 SQL queries rewritten against base replica tables; eliminates dependency on missing vw_* views",

    "0.4.5": "YYYYMMDD integer-date fix all research pages; session_state SQL cache kills HYT00 timeout cascade; graph node labels enriched; timeout 120 s globally; connection health-check ping",

    "0.4.6": "Unified Database Explorer with auto-parsed schema reviews from DATA_DICTIONARY and EDAP_DASHBOARD_TABLES",

    "0.5.0": "Value Stream Living Map: End-to-end integration of PO, SO, WO flows. Formulaic friction bottlenecks. Production Plant and Value Stream filtering.",

    "0.6.0": "Global Application Filter, unified st.session_state routing, and AI PowerPoint Creator.",

    "0.7.0": "Enterprise Network Autonomous Agent. Native Exchange/SMB discovery.",

    "0.7.1": "Ask the Data cross-dataset report generation. Fixed global connection uninitialized bug. Fixed SQL column queries across analytical pages.",

    "0.8.0": "Massive UX/Actionable overhaul: SQLite local store, NLP-part categorization, Global date windows, semantic action-TODO engine, OTD local-owner tracking, and cross-page metric fixes.",

    "0.8.1": "Brain Pipeline Actionability Sync: The master pipeline outputs, action lists, and ROI evaluations now fully react to Control Deck combinations (Workstream lens, Horizon).",

    "0.13.0": "Brain → Body Bridge. Efferent nervous system: brain_body_signals distills every effective signal into prioritized, role-targeted Directives. Five generators ship out of the box. Loop closure via body_feedback → corpus → next round.",

    "0.14.0": "Brain-Driven Quest Engine. NL → ParsedIntent → Mission → analyzers + schema synthesis + viz composer → 2 living PPTX artifacts (Executive 1-Pager portrait + Implementation Plan landscape) overwriting in place. Wired into autonomous_agent Step 3g; agent_uplink streams MISSION/<id>/ to laptop body.",

    "0.14.1": "UI Stability & Full-Suite Playwright Benchmarks. Fixed duplicate URL collision, missing import, out-of-order import. Playwright smoke-test: 20/20 pages PASS.",

    "0.14.2": "Test + Benchmark Infrastructure, Hardening, Docs. 88-test pytest suite, bench_quest_engine.py, orchestrator hardening, intent_parser 2k truncation, mission_runner empty-query guard, README Quest Engine section.",

}

