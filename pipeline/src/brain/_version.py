from __future__ import annotations



__version__ = "0.14.6"

__release__ = (

    "Continuous Multi-Agent Synaptic Extension (rebased on 0.14.5 OTD fixes). "
    "autonomous_agent.py: (1) Four daemon worker threads now run continuously "
    "underneath the main 1-4hr cycle: _synaptic_builder_worker (10min, 24h "
    "window), _lookahead_worker (15min, rotating 7d/30d/90d dispersed historical "
    "windows), _dispersed_sweeper_worker (20min, one-connector-per-tick rotation), "
    "_convergence_worker (30min, refresh_corpus_round + materialize_into_graph). "
    "(2) rag_knowledge_deepdive() parameterised with window_label, window_hours, "
    "window_offset_hours, max_iterations, max_entities, explored_kv_key so each "
    "worker explores its own temporal slice with its own persisted explored-pair "
    "set. Existing Step 3e.5 call site preserved by defaults. "
    "(3) start_continuous_synaptic_agents() wired into autonomous_loop() startup; "
    "stop_continuous_synaptic_agents() with threading.Event cooperative shutdown. "
    "All workers use SQLite check_same_thread=False, jittered cadences (±60-120s) "
    "to desynchronise organically, and write per-worker last-run heartbeats to "
    "brain_kv (synapse_*_last) so operators can observe synaptic activity. "
    "Result: synapses are pre-built ahead of the slower main cycle's reads "
    "instead of being constructed only episodically. 100/100 pytest passing."

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

    "0.14.3": "SOTA RAG Deepdive + Dynamic Connector Discovery + 100/100 Tests. rag_knowledge_deepdive() Step 3e.5: structural-hole bridging, TF-IDF cosine, data-grounded co-occurrence, 8-iter convergence. sweep_all_data_sources() Step 3a wired + Section 4 dynamic SQL connector auto-discovery. bench_quest_engine.run_benchmarks() callable API. 100/100 pytest.",
    "0.14.4": "Continuous Multi-Agent Synaptic Extension. Four daemon worker threads (synaptic-builder/10min/24h, lookahead/15min/rotating-7d-30d-90d, dispersed-sweeper/20min/connector-rotation, convergence/30min) run continuously underneath the main cycle, building synapses on relationally dispersed temporal windows so they're ready before the next agent traverses that aspect. rag_knowledge_deepdive() parameterised with window_label/window_hours/window_offset_hours/explored_kv_key for per-worker temporal targeting.",

    "0.14.5": "OTD Recursive Hardening & Training Loop Fix. Daily Review worklists, offline fallback clustering, strict TF trending windows, and seed_otd_direct offline ground-truth seeding.",
    "0.14.6": "Continuous Multi-Agent Synaptic Extension (rebased on 0.14.5). Four daemon worker threads run continuously underneath the main cycle: synaptic-builder (10min, 24h), lookahead (15min, rotating 7d/30d/90d), dispersed-sweeper (20min, connector rotation), convergence (30min). rag_knowledge_deepdive() parameterised for per-worker temporal slicing. start/stop_continuous_synaptic_agents() with threading.Event cooperative shutdown. 100/100 pytest.",

}

