from __future__ import annotations

__version__ = "0.19.0"
__release__ = (
    "Session-Store Cloud Sync + Symbiotic Loop + Citation-Chain Acquirer + Internal Watcher. "
    "Session history flows through every node in the distributed fabric: each autonomous agent "
    "pushes its own session-store blob to Azure Blob Storage and pulls all peer blobs on a "
    "slow cadence inside the network_observer loop. Offline peer absorption now also absorbs "
    "the peer's full session history. citation_chain_acquirer.py follows bibliography chains "
    "recursively via Semantic Scholar + OpenAlex to deepen the research frontier. "
    "internal_watcher.py replaces Windows Scheduled Task dependency with a Python-native "
    "supervisor that launches, monitors, and restarts the learning child process. "
    "Cloud-learning workflow refactored to DB high-water mark for deterministic queue capture. "
    "Free-tier OpenRouter model pool expanded and verified live."
)
    "nodes toward quest:type5_sc; pulses goal-alignment signal each cycle. "
    "resumption_manager.py: detects downtime gap on startup, classifies crash vs graceful, "
    "git-pulls latest cloud_learning_queue.jsonl, ingests queued cloud events, schedules "
    "1.5x-4x catchup burst proportional to downtime; stamp_alive every 5 min; "
    "stamp_graceful_shutdown on clean exit. "
    ".github/workflows/cloud_learning.yml: GH Actions every 4 h runs OCW + ML-research "
    "ingestors against cached cloud_brain.sqlite and commits events to cloud_learning_queue.jsonl. "
    "agent_watcher.ps1: watchdog restarts autonomous_agent.py on crash, records downtime_log.json. "
    "install_agent_watcher.ps1: SCBLearningAgent Scheduled Task (AtStartup+AtLogOn, 9999 restarts). "
    "bootstrap_new_machine.ps1: one-script full new-machine setup from OneDrive sync. "
    "autonomous_agent.py: run_resumption_check before loop, stamp_alive in sleep, "
    "stamp_graceful_shutdown on exit, start_network_observer alongside other daemons."
)
__build_date__ = "2026-04-24"
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
    "0.14.9": "OCW semantic bridge + synaptic worker protection + network vision worker. Workers moved to synaptic_workers.py. Sweeper treats network errors as soft skips. _vision_worker probes bridge_rdp + piggyback topology every 5 min and materialises Endpoint entities into corpus graph.",
    "0.15.0": "4-ERP xlsx pipeline (Epicor/Oracle/SyteLine/AX) + Brain page fixes + EOQ query optimisation.",
    "0.16.0": "Symbiotic Dynamic Tunneling + Torus-Touch. symbiotic_tunnel.vision_horizontal_expand mints SYMBIOTIC_TUNNEL edges from closed-loop tcp/udp mesh using Bayesian-Poisson centroids, inverted-ReLU ADAM, dual-floor mirror, and propeller routing. torus_touch runs continuously (30 s) pushing every Endpoint along the n=7 categorical gap gradient on the toroidal manifold, so tunnel weights follow manifold geometry. 29 unit tests in tests/test_symbiotic_torus.py.",
    "0.17.0": "UEQGM + AI Knowledge Expansion Research Tracks. _EXTENDED_RESEARCH_TOPICS (47 queries, 8 clusters) added to ml_research.py from the active Grok 3 thread: quantum dynamics, biohybrid computing, moiré superlattices, astrophysical timing, AI knowledge graph self-reference, ensemble LLM/RAG, archival AI training, organic data structures. Extended sweep runs before SC loop so foundational physics/AI context precedes supply chain systems engineering acquisition each cycle.",
    "0.17.1": "Grounded Tunneling. grounded_tunneling.py: certainty-anchored expansory pathway collapser. Ground nodes (top-quartile Bayesian certainty) open BFS paths toward uncertain (low-certainty, high-torus-gap) frontiers with RESISTANCE_DURATION weight immunity + torus_amplify torsional boost. Expired paths undergo nodal collapse \u2192 new permanent GROUNDED_TUNNEL edge. torus_touch.TouchPressure gains step_multipliers for per-endpoint amplification. _vision_worker Step 5 wires ground_and_expand. 8 new unit tests (TestGroundedTunneling), 38 total, all green. Oracle schema map expanded (Demand Priority Rules, B2B Trading Partners, ECN Tracking, General Ledger Journals, Supply Network maintenance). oracle_schema_mapper.py ADF task panel hardened with computed-style section-header detection and fallback link scraping. Corpus diagnostic (_cohesion_report.py), bilateral Vision<->Touch test harness (_test_bilateral.py), ADAM optimiser unit test (_test_adam.py), Oracle Fusion intersection map (build_intersection_map.py) added. brain_body_signals.py gains 2 quest-comprehension generators (_gen_fallback_parse_warning, _gen_scope_underpowered); knowledge_corpus.py gains SCB Grok conversation ingestor (_ingest_scb_docs) and extended vision scan for SCB docs directory.",
    "0.18.0": "DeepSeek V4 Candidate Trial System. llm_candidate.py: scored probationary EMA trial system (\u03b1=0.10, promote\u22650.72, reject\u22640.45 after 50 dispatches); promotes to llm_registry with no YAML edit required. brain.yaml: llms.candidates block with DeepSeek V4 Pro (1.6T/49B MoE, 1M ctx) and DeepSeek V4 Flash (284B/13B MoE). llm_ensemble.py: _try_dispatch_candidates() sidecar + 10th-dispatch evaluation trigger.",
    "0.18.1": "Key guard + dimensionality redirection. Ensemble key guard prevents hash collisions on concurrent dispatch; dimensionality redirection routes embedding mismatches to compatible corpus slots without blocking.",
    "0.18.2": "rADAM + Directional Intelligence + Systemic Refinement. radam_optimizer.py: rADAM with toroidal phase coupling \u2014 5 extensions over vanilla Adam (complex bifurcated gradient, pivoted-ReLU, heart-beat momentum modulation, Langevin incoherence noise, T^2 toroidal pressure projection). directionality_listener.py: (expansion, coherence, bifurcation) triplet. learning_drive.py: corpus saturation \u2192 pivot_alpha, self-train quality \u2192 heartbeat_kappa, learning velocity \u2192 noise_sigma, difficulty\u00d7room \u2192 acquisition_drive. systemic_refinement_agent.py: 5-phase daemon (SENSE/DIAGNOSE/RANK/EXECUTE/LEARN), 10 SC refinement strategies, adaptive cadence 20 min\u20132 h.",
    "0.18.3": "Autonomous Failsafe + Network Observer. network_observer.py: latent daemon; peer liveness monitoring + corpus cursor absorption; singularity velocity toward quest:type5_sc. resumption_manager.py: startup downtime detection, cloud queue ingestion, catchup burst. cloud_learning.yml: GH Actions cloud learning every 4 h. agent_watcher.ps1 + install_agent_watcher.ps1: local watchdog + Scheduled Task. bootstrap_new_machine.ps1: one-script new-machine setup from OneDrive sync.",
}
