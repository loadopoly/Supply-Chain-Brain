from __future__ import annotations

__version__ = "0.19.8"
__release__ = (
    "Directed Toroidal Knowledge Expansion. Every paper discovered by the ML "
    "research sweep (arXiv, OpenAlex, CrossRef — 56 SC topics + 45 Grok extended "
    "topics: UEQGM, biohybrid quantum, astrophysics, AI knowledge graphs) now "
    "donates its full bibliography inline: referenced_works is captured in the same "
    "OpenAlex API call and written directly into citation_chain_state, so the "
    "citation-chain expander follows the entire reference tree in its next cycle "
    "without a round-trip through the corpus. CITES edges are also materialised "
    "into the knowledge graph, making stub MLPaper nodes for each cited work so "
    "supply-chain, quantum-systems, and cross-domain papers become mutually "
    "reachable via citation paths. seed_from_learning_log now also extracts "
    "OpenAlex IDs (oa: prefix) from papers previously stored with openalex_id or "
    "with an OA W-ID in the arxiv_id field, closing the dead-end loop for papers "
    "without arXiv/DOI identifiers."
)
__build_date__ = "2026-04-29"

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
    "0.17.1": "Grounded Tunneling. grounded_tunneling.py: certainty-anchored expansory pathway collapser. Ground nodes (top-quartile Bayesian certainty) open BFS paths toward uncertain (low-certainty, high-torus-gap) frontiers with RESISTANCE_DURATION weight immunity + torus_amplify torsional boost. Expired paths undergo nodal collapse → new permanent GROUNDED_TUNNEL edge. torus_touch.TouchPressure gains step_multipliers for per-endpoint amplification. _vision_worker Step 5 wires ground_and_expand. 8 new unit tests (TestGroundedTunneling), 38 total, all green. Oracle schema map expanded (Demand Priority Rules, B2B Trading Partners, ECN Tracking, General Ledger Journals, Supply Network maintenance). oracle_schema_mapper.py ADF task panel hardened with computed-style section-header detection and fallback link scraping. Corpus diagnostic (_cohesion_report.py), bilateral Vision<->Touch test harness (_test_bilateral.py), ADAM optimiser unit test (_test_adam.py), Oracle Fusion intersection map (build_intersection_map.py) added. brain_body_signals.py gains 2 quest-comprehension generators (_gen_fallback_parse_warning, _gen_scope_underpowered); knowledge_corpus.py gains SCB Grok conversation ingestor (_ingest_scb_docs) and extended vision scan for SCB docs directory.",
    "0.19.6": "Operator UI stabilization: Global Filters persist across app-shell and direct-page navigation, sidebar fallback is run-local, Plant cache is disk-first, DBI copy is page-specific and Brain-first, Bullwhip DBI uses live ratio context, Heart Story is navigable and Plotly-compatible, WIP Aging Review is in Platform nav.",
}
