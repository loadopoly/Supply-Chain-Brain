# Changelog

All notable changes to **Supply Chain Brain** are documented here. Versions
follow [Semantic Versioning](https://semver.org). The single source of
truth for the version number is `src/brain/_version.py`.

## 0.17.0 — UEQGM + AI Knowledge Expansion Research Tracks (2026-04-24)

### Added
- **`src/brain/ml_research.py`** — `_EXTENDED_RESEARCH_TOPICS` list (47 arXiv/
  OpenAlex queries across 8 discipline clusters), derived from the user's active
  Grok 3 research thread ("Introduction to Grok 3 and Capabilities", 553
  responses):
  - **Quantum Dynamics & Wavefunction Models** — UEQGM observer model, Floquet
    systems, loop quantum gravity, holographic entropy, dissipative Kerr
    resonators, parity-time symmetry photonics, quantum fluctuations EFT
  - **Quantum Computing Architectures** — superconducting qubit/resonator
    coupling, niobium cavity QED, Weyl semimetal circuits, Bayesian quantum
    state tomography, surface-code error correction, ST-GCN
  - **Topological & Condensed Matter Physics** — moiré superlattices, skyrmion
    plasmonics, Weyl node 1-D lattice duality, levitated optomechanics backaction
    suppression
  - **Biohybrid & Biological Quantum Systems** — biohybrid QC vesicle transport,
    cryptochrome quantum coherence, axonal presynapse nanodisk lipid membranes
  - **Astrophysical & Cosmological Timing** — FRB cosmological timing, muonic
    decay precision, gravitational wave memory (BNS), millisecond pulsar timing,
    Hubble constant local distance, neutrino superradiance BEC, parity-violating
    dispersion
  - **AI Knowledge Graph & Self-Referential Systems** — knowledge graph AI
    introspection, recursive LLM feedback, centroidal ontology construction,
    meta-learning, ensemble LLM/RAG, archival AI training quality, RDF graph
    databases, document intelligence OCR→KG
  - **Advanced ML Architectures (UEQGM-adjacent)** — spatio-temporal Bayesian
    graph physics, neural ODEs, physics-informed NNs, quantum ML variational
    circuits, geometric deep learning equivariance
  - **Organic & Topological Data Structures** — quipu/torsion computation,
    persistent homology, fractal self-similar encoding
- **`_EXTENDED_TOPICS_PER_CYCLE = 5`** and **`_EXTENDED_PAPERS_PER_TOPIC = 8`**
  constants; cursor persisted in `brain_kv` under key `extended_topic_cursor`
- Extended sweep positioned **before** the SC per-topic loop so foundational
  physics/AI context is already in the corpus when supply chain systems
  engineering topics are processed each cycle

## 0.16.0 — Symbiotic Dynamic Tunneling + Torus-Touch (T^7) (2026-04-24)

### Added
- **`src/brain/symbiotic_tunnel.py`** — discrete horizontal-expansion kernel
  for the corpus graph:
  - `BayesianPoissonCentroids` — 1-D Poisson/Gamma(α,β) conjugate clustering;
    empty clusters are pulled toward `α/β = 1.0` instead of NaN
  - `InvertedReluAdam` — ADAM whose pre-activation gradient is `−ReLU(g) +
    sgd_mix · g`, used to nudge edge weights toward their assigned centroid
  - `DualFloorMirror` — returns `(+x, −x)` clipped to `1 − max(|w|)` so
    freshly minted edges always carry usable signal in both polarities
  - `PropellerRouter` — softmax over weights → axel + blade selection,
    skips existing pairs, joint-probability coupling
  - `touch_couple(a, b) = exp(ln(1+|a|)+ln(1+|b|)) − 1` — exp/ln identity
    coupling (numerically stable at small weights)
  - `vision_horizontal_expand(cn)` — orchestrates the above against
    `corpus_edge` rows whose `rel ∈ {REACHABLE, BRIDGES_TO, SERVES}` and
    inserts new `SYMBIOTIC_TUNNEL` edges
- **`src/brain/torus_touch.py`** — continuous boundary-pressure agent on
  `T^7 = (S^1)^7`:
  - `CatGapField` — per-dim categorical PMF (default 16 bins/dim) with
    Laplace smoothing; KL-from-uniform measures the informational gap
  - `TouchPressure` — momentum + step + jitter, wrapped mod 2π each tick
  - `tick_torus_pressure(cn)` — reads every `Endpoint`, builds the gap
    field, walks each endpoint up `∇G`, persists `torus_angles`,
    `torus_gap`, and per-endpoint velocity in `kv_store`
  - `touch_couple_torus(θ_a, θ_b)` — wrap-aware angular Touch
  - `endpoint_angles()`, `gap_field_summary()` helpers
- **`src/brain/synaptic_workers.py`** — registered `_torus_touch_worker` as a
  30-second daemon thread alongside the existing five workers; added
  `synapse_torus_last` heartbeat (`endpoints | moved | gap | spread%`) and
  `_vision_worker` Step 4 calls `vision_horizontal_expand` after each
  bridge/network probe pass
- **`tests/test_symbiotic_torus.py`** — 29 unit tests covering primitives,
  horizontal expansion, manifold geometry, DB-driven ticks, and cross-module
  manifold-aware coupling

### Closed-loop architecture
```
torus_touch (30 s)            vision_horizontal_expand (5 min)
─────────────────             ────────────────────────────────
read Endpoints                read Endpoints + corpus_edge
build CAT pmf                 cluster weights via Bayesian/Poisson centroids
∇G gap field                  propeller route over top-tier
push θ_i along ∇G ──► writes  ─► touch_couple_torus(θ_a, θ_b) ◄── consumes T^7
torus_angles into             write SYMBIOTIC_TUNNEL edges weighted by
corpus_entity.props_json      manifold proximity, not just scalar weight
```

### Test Results (2026-04-24)
```
tests/test_symbiotic_torus.py ......................... 29/29 PASS
  TestPrimitives                  11/11
  TestHorizontalExpansion          4/4
  TestTorusGeometry                9/9
  TestTorusTick                    4/4
  TestTunnelManifoldCoupling       1/1
```

---

## 1.4.1 — DBI Playwright Suite · LLM timeout · Procurement 360 expanders (2026-04-23)

### Added
- **`tests/playwright/test_dbi_tooltip.py`** — 19-page Playwright E2E suite for the Dynamic Brain Insight (DBI) widget:
  - `_wait_for_server_stable()`: waits up to 60 s for Streamlit `stAppViewContainer` before running tests; prevents false failures from slow cold starts
  - `_check_popover` `src` retry loop: 4 × 1.6 s attempts to locate "Insight source" text; re-locates the trigger button on each retry to survive `@st.fragment(run_every=2)` DOM rebuilds (stale-reference fix)
  - `_check_help_tooltips` stExpander ancestor walk: 8-level DOM traversal to correctly classify metrics inside `st.expander` blocks

### Fixed
- **`tests/playwright/test_dbi_tooltip.py`**: `_check_popover` trigger locator replaced lambda pattern with `.filter(has_text=…)` to avoid stale closures
- **`tests/playwright/test_dbi_tooltip.py`**: `passed` property `expanders_ok` guard — pages with zero metrics now pass without requiring expanders
- **`tests/playwright/test_dbi_tooltip.py`**: `wait_for_function` timeout increased to 20 000 ms; `wait_for_selector` for dbi-card to 25 000 ms
- **`src/brain/llm_caller_openrouter.py`**: LLM per-model `timeout` reduced 40 s → 7 s; worst-case with 2-model fallback = 15 s < 20 s test window
- **`pages/4_Procurement_360.py`**: Restructured all 7 KPI metrics into inline `st.expander` blocks (5 in main KPI strip + 2 in obsolescence tab) so DBI expander check returns `expanders=7/7`

### Test Results (run 2026-04-23, fresh server PID 26756)
```
11/19 PASS
  PASS: Query Console, Schema Discovery, Supply Chain Brain (5/5 expanders),
        Supply Chain Pipeline (2/2), Connectors, Lead-Time Survival (4/4),
        Multi-Echelon (4/4), Sustainability (4/4), What-If, Decision Log (4/4),
        Benchmarks (5/5)
  FAIL (Azure SQL offline — expected): EOQ Deviation, OTD Recursive, Bullwhip Effect
  FAIL (stale DOM, re-locate fix applied): Procurement 360, Report Creator
  FAIL (LLM timeout >20 s): Data Quality, Freight Portfolio, Cycle Count Accuracy
```

### Infrastructure Notes
- Kill orphaned `chrome-headless-shell` processes before each run: `Get-Process -Name "chrome-headless-shell" | Stop-Process -Force`
- Restart Streamlit server between test runs to prevent memory bloat (276 MB → 1 GB after 5+ runs)

---

## 0.15.0 — 4-ERP xlsx Pipeline · Brain Page Fixes · EOQ Optimisation (2026-04-23)

### Added
- **`src/extract/xlsx_extractor.py`** — OneDrive-based live data pipeline for all four ERP systems without requiring SQL credentials:
  - 16 registered aliases across Epicor 9, Oracle Fusion, SyteLine (Parsons), and Microsoft Dynamics AX (Eugene Airport Rd)
  - Canonical column names (`part_number`, `warehouse_code`, `frozen_qty`, `count_qty`, `abc_class`, etc.) normalised across all ERPs
  - `fetch(alias)`, `fetch_all_cc_data()`, `fetch_all_abc_data()`, `available_aliases()` public API
  - Path override via `ONEDRIVE_ROOT` env var
  - Real row counts verified: Epicor CCMerger 14,562 · Oracle on-hand 130 · SyteLine item count 44 · AX CC journal 65
- **`src/connections/ax.py`** — Microsoft Dynamics AX connector for Eugene Airport Rd (AX 2012, `MicrosoftDynamicsAX` database), following the same pattern as `epicor.py` and `syteline.py`
- **`data_access.py`**: `fetch_xlsx_source(alias)` and `fetch_xlsx_all_cc()` wired into the Brain’s session-cached data layer
- **`brain.yaml`**: `xlsx_sources:` section mapping all 16 sheet aliases; AX staging table entries added
- **`test_connector_assumptions.py` Group 8**: 11 live xlsx tests against real OneDrive files — all pass (61 PASS / 0 FAIL / 10 WARN)

### Fixed
- **`1_Supply_Chain_Brain.py`**: `_build_graph()` switched from `@st.cache_data` to `@st.cache_resource` — `GraphContext` (NetworkX graph) is not pickle-serialisable so `cache_data` raised `UnserializableReturnValueError`
- **`1_Supply_Chain_Brain.py`**: Connector status bar removed from the Brain page; it now lives exclusively in the Connectors page
- **`6_Connectors.py`**: Status summary row added above the expanders; shows 🟢 green for connectors with an active handle, 🟡 yellow for unconfigured ones
- **`connections.yaml`**: SyteLine Parsons database corrected from `PFI_App` → `PFI_SLMiscApps_DB`; `schema: cycle_count` added
- **`connections.yaml`**: `ax_airport_rd` block added (`MicrosoftDynamicsAX`, `ActiveDirectoryIntegrated`)
- **`ax.py`**: Removed broken `from . import load_connections_config, DPAPIVault` import; replaced with `yaml.safe_load` + `from . import secrets as _secrets` matching the epicor.py pattern

### Improved
- **`2_EOQ_Deviation.py`**: Column schema resolution cached via `@st.cache_data(ttl=1800)` — eliminates ~5 `INFORMATION_SCHEMA` round-trips per page load
- **EOQ query**: `TOP 5000` → `TOP 2000`; `OPTION (RECOMPILE, MAXDOP 4)` added for better query plan; timeout raised from 120 s → 300 s
- **`db_registry.py`**: AX connector registered; SyteLine description updated to reflect correct database name
- **`mappings.yaml`**: Verified 28 entries (9 Epicor · 5 SyteLine · 14 Azure/Oracle)

### Test Results
```
PASS: 61  WARN: 10 (expected — servers not configured)  FAIL: 0
All .py files outside .venv compile clean
```


## 0.14.9 — Network Vision Worker + OCW Semantic Bridge + Synaptic Worker Protection (2026-04-23)

### Added
- **`_vision_worker` — Network Vision** (`src/brain/synaptic_workers.py`)  
  Fifth synaptic thread (interval 5 min) that gives the Brain eyes over its own
  compute/network topology:
  - `bridge_rdp.probe_all()` — TCP-probes every declared bridge target (RDP,
    SQL-server, VSCode tunnel) and records live/down status.
  - `network_learner.observe_network_round()` — full endpoint observation round
    across connections.yaml, brain.yaml, SMB mappings, compute peers, and seeds.
  - Materialises observations as `Endpoint` corpus entities with `REACHABLE` /
    `UNREACHABLE` edges to linked `Site` entities, `SERVES` edges to `Peer`
    entities, and `BRIDGES_TO` edges when a piggyback RDP route is alive.
  - All network errors treated as soft skips (no backoff accumulation).

- **`_ingest_bridge_observations`** (`src/brain/knowledge_corpus.py`)  
  Corpus refresh now promotes every `network_topology` row and every
  `bridge_rdp` target into the corpus graph on each 30-min convergence cycle —
  so network vision is persistent across restarts, not just in-memory.

- **OCW → Task/Quest semantic bridges** (`temp_correct_bridge.py`, run once)  
  All 13 `AcademicTopic` entities now have `INFORMS` edges to the two `Quest`
  entities and curated `Task` entities (`abc_classify`, `otd_classify`,
  `vendor_consolidation`, etc.).  35 SC-relevant `OCWCourse` entities now have
  `INFORMS` edges to `Task` and `Quest` hubs, enabling the RAG deepdive to
  find structural holes that cross the academic/operational divide.

### Changed
- **Synaptic workers protected from autonomous-agent rewrites**  
  All 5 synaptic worker functions moved to `src/brain/synaptic_workers.py`.
  `autonomous_agent.py` imports them via a try/except fallback stub so
  autonomous LLM rewrites of `autonomous_agent.py` cannot strip the workers.

- **Sweeper treats network errors as soft skips**  
  `_dispersed_sweeper_worker` now uses `_is_network_error(exc)` to detect
  host-DOWN / timeout conditions and sets `ok=True` — preventing exponential
  backoff from accumulating when `desktop-sql` (172.16.4.76) is unreachable.

- **`synaptic_agents_status()`** updated to include `synapse-vision` heartbeat
  with 300 s expected interval.



### Added
- **Global Timeline Windows (`global_filters.py`)** — Start/End date lookbacks now reliably filter dashboards on the SQL side using YYYYMMDD integer `date_key` constructs (`CAST(receipt_date_key AS bigint) BETWEEN {sk} AND {ek}`).
- **Local Persistence (`local_store.py`)** — Added a local SQLite database (`local_brain.sqlite`) for storing state independent of the Azure Replica. Support added for action bookmarks, NLP part categories, and manual OTD workflow comments/owners.
- **NLP Semantic Categorization (`nlp_categorize.py`)** — Parts are now bucketed into taxonomic categories (e.g. Steel, Fasteners, Wiring, Hydraulics) dynamically using a scikit-learn TF-IDF / cosine_similarity model falling back to heuristic keyword-matching.
- **Action Evaluation Engine (`actions.py`)** — Academic outputs are converted into layperson tasks via a deterministic Friction-to-Action semantic mapping that computes Annual Impact ($ / yr), Prioritization, Confidence metrics, and Action Owners.
- **Brain Expert TODO List** — `1_Supply_Chain_Brain.py` now leverages `actions_for_pipeline` to load a unified list of pipeline tasks sorted by monetary value per year.
- **Intercompany Inventory Transfer Scan** — `4_Procurement_360.py` now cross-references obsolete list parts with global network-wide `on_hand` metrics to locate viable transfer sites.
- **Executive ESG ROI Panel** — `10_Sustainability.py` now includes a net-present 5-Year ROI evaluation per abatement lever (mode-shifts, LTL/FTL).
- **Interactive Daily Plant Review** — `3_OTD_Recursive.py` integrates directly into the local SQLite store allowing analysts to review rows "Opened Yesterday", claim assignment, and drop updates manually via Streamlit's `data_editor`.

### Fixed
- **Multi-Echelon Decimal/Float TypeErrors** — Enforced complete `float` casting during safety-stock calculations preventing decimal schema type collisions.
- **Bullwhip Query Timeouts** — All 3 primary CTEs (`demand`, `mfg`, `supplier`) dynamically bound to the global 365-day timeline window by default, resolving arbitrary lockups on `fact_po_receipt`.
- **Goldfish Lane Exclusions** — Repaired Freight Portfolio SQL logic matching correct unit-price schemas. OD (Origin→Destination) pairs now display via normalized `get_supplier_labels` mappings.
- **Cross-Page Findings Mapping** — Refined Report Creator and Overview UI explanations for index labels (part, cluster, supplier, lane, node, vendor).

## 0.7.1 — Ask the Data & Cross-Dataset Reports

### Fixed
- Stabilized Oracle connection pooling across Streamlit app states during extensive cross-dataset AI reporting.

## 0.6.0 — Global Filter & Deck Creator

### Added
- **Global Application Filter**: Implemented a global "Mfg Site" dropdown in pp.py that syncs state across all dashboards via st.session_state["g_site"]. Removed hardcoded/local filters from individual pages to streamline unified navigation across the entire toolkit.
- **PowerPoint & Reports Manager**: Added 15_Report_Creator.py to auto-generate PowerPoint Cross-Dataset performance reviews directly from the UI.
- **Presentation Template Auto-Scrubber**: Allow users to upload .pptx (corporate slide masters, slide decks) which the pipeline visually "scrubs" empty using python-pptx, securely retaining localized styles/fonts without carrying over extraneous information, creating logic hooks to populate new reviews natively.

## 0.5.0 — Value Stream Living Map

### Added
- **Value Stream Pipeline**: Replaced generic graph on \Pages/1_Supply_Chain_Brain.py\ with an interactive Value Stream Map.
- **Formulaic Friction Points**: Added integrated bottleneck algorithms based on MIT SCALE principles, calculating friction dynamically using \due_date_key\ tracking for POs/WOs, and \promised_ship_day_key\ for SOs.
- **Enhanced Topology Filtering**: Added specific MIT Design Lab UI filters for Production Plant (Business Unit) and Value Stream (Part Types), pushing filter complexity upstream and using non-linear marker scaling.
- **Function & Schema Intersection Guide** (`docs/REPO_FUNCTION_AND_SCHEMA_GUIDE.md`) — end-to-end reference mapping every brain module, MIT CTL research module, Streamlit page and `src/deck/` PPTX builder to the underlying replica tables/columns. Documents the four confirmed schema gaps (`failure_reason`, `fact_cycle_count`, point-in-time inventory, ABC part codes on `dim_part`) that surface as empty/Unknown slides in the agent-generated PowerPoint.
- **ABC Inventory Catalog "D" Candidates Fallback**: Updated the `src/deck/live.py` SQL generation to strictly respect the existing `ABC Inventory Catalog` codes (which are locked at the beginning of the year). The live query now intelligently identifies D-Code candidates by outputting "D" only when the existing classification is null *and* there is active `quantity_on_hand` present.

## 0.4.6 — Unified Database Explorer

### Added
- **Unified Database Explorer** (`pages/0_Schema_Discovery.py`) — a dynamic dropdown interface that queries all registered database connectors on the platform (Azure SQL, Oracle Fusion) to let users independently browse any schema, subject area, and table.
- **Automated Schema Reviews** — schema UI dynamically parses contextual notes, table grains, definitions, and usage dependencies from `DATA_DICTIONARY.md` and `EDAP_DASHBOARD_TABLES.md` directly into the app view when inspecting a table.

---

## 0.4.5 — YYYYMMDD date fix · session-cache SQL · graph label enrichment

### Fixed

- **YYYYMMDD integer-date conversion** — all MIT CTL research pages (7–11) and
  the EOQ page now convert fact-table integer date keys with
  `TRY_CONVERT(date, CONVERT(varchar(8), CAST([col] AS bigint)), 112)`.
  The previous `TRY_CONVERT(date, [col])` silently returned NULL for integer
  inputs, producing zero-row results on every page.

- **HYT00 query-timeout cascade eliminated** — every `_build_xxx_sql()`
  function was being called at module load time on each Streamlit rerun,
  firing 2–5 `INFORMATION_SCHEMA` discovery queries before the actual data
  query. All SQL builders are now lazily evaluated and cached in
  `st.session_state` (keys: `_eoq_default_sql`, `_lt_sql`, `_bw_sql`,
  `_me_sql`, `_sus_sql`, `_port_sql`). SQL is built at most once per
  browser session.

- **`9_Multi_Echelon.py` — orphaned code after `return`** — two unreachable
  `st.text_area` / `st.file_uploader` lines were left floating after the
  `return` statement inside `_get_me_sql()`; removed. Reference to undefined
  `default_sql` replaced with `_get_me_sql()` call.

- **`10_Sustainability.py` / `11_Freight_Portfolio.py`** — same YYYYMMDD date
  fix applied; absolute fallback SQL added; `_load()` / `_port()` timeout
  raised to 120 s.

- **Graph node labels** (`1_Supply_Chain_Brain.py` + `graph_context.py`) —
  nodes were labelled with raw integer keys (e.g. `221273`) instead of human
  names. Fixed by:
  - `graph_context.add_parts()` now accepts `label_col=` parameter.
  - `graph_context.add_suppliers()` writes `label=` from `name_col`.
  - `graph_context.add_edges()` accepts `src_label_col=` / `dst_label_col=`
    and upgrades implicit node labels from raw key → human name whenever
    a richer label is available.
  - `_build_graph()` in page 1 now calls `enrich_labels()` on all three
    DataFrames and passes resolved `*_label` column names into the graph
    builder.

### Changed

- **Default query timeout raised to 120 s** across `db_registry.read_sql()`,
  `data_access.query_df()`, and `demo_data.auto_load()` (was 30 s).
- **`db_registry._healthy_conn()`** — connection handle is now validated with
  a `SELECT 1` ping before use; stale handles are discarded and reconnected
  automatically without requiring a new MFA prompt.
- **`WITH (NOLOCK)` + `OPTION (MAXDOP 4)`** added to all fact-table reads in
  pages 2, 7, 8, 10, 11 to reduce lock contention and cap parallel workers.

---

## 0.4.4 — Replica-table rewire (vw_* view elimination)

### Fixed
- Pages 7–12 SQL queries rewritten against base replica tables
  (`fact_po_receipt`, `fact_sales_order_line`, `fact_inventory_on_hand`,
  `fact_inventory_open_mfg_orders`). Removed dependency on non-existent
  `vw_*` views that caused immediate connection errors on every page load.

---

## 0.4.3 — Bug-fix wave

### Fixed
- VOI Timestamp / datetime columns converted to `int64` epoch before LightGBM
  fit — eliminates `TypeError: float() argument must be … Timestamp`.
- Graph node-kind propagation restored; discovery panel explains why high-degree
  nodes (e.g. `('part','221273')`) are central.
- EOQ outlier heatmap + quadrant chart added alongside the ranked table.
- Procurement 360 supplier/part fields resolved to human-readable names via
  `label_resolver.enrich_labels()`.
- Benchmarks `rows_per_s` merge collision fixed (suffixed columns deduplicated).
- Sidebar node-type filters now correctly hide/show graph nodes.

---

## 0.4.2 — Full Plotly rewrite

### Changed
- All 14 pages converted from Altair/Vega to Plotly Express for consistent
  drill-down and cross-filter behaviour.
- `page_header` / `drilldown_table` helper retired; each page manages its own
  `st.plotly_chart(use_container_width=True)` layout.
- Data Quality VOI section now renders heatmaps for missing-value impact.
- Connectors page modernised with live ping status and edit-in-place YAML.

---

## 0.4.1 — Self-driving live pages

### Changed
- **Every sidebar page now auto-loads from the live database on first paint.**
  No more "click Run / Compute / Build" gates — pages 1, 2, 3, 5, 7, 8, 9, 10,
  11, 12 all execute their default Azure-SQL-replica queries inside an
  `@st.cache_data(ttl=600)` loader the moment the page is opened, and the user
  refines via collapsed expanders rather than primary buttons.
- All data is pulled **only** from the registered SQL connectors (Azure SQL
  replica + Oracle Fusion). Synthetic data is reserved for `bench_brain` and
  is never used in the UI.

### Added
- `src/brain/demo_data.py` (now a live-only loader) — `auto_load(sql, connector)`
  + `render_diagnostics()` that, when a live query fails, shows the SQL, the
  error, and an inline **schema browser** (INFORMATION_SCHEMA tables → columns
  → 25-row sample) so the user can see the real shape and fix
  `config/brain.yaml` mappings without leaving the page.
- `first_existing_table(connector, candidates)` helper for pages that want to
  probe several physical mappings before failing.

### Fixed
- Sidebar `_safe_page_link` markdown fallback was emitting `/11_Freight_Portfolio`
  style URLs which don't match Streamlit MPA's actual `/Freight_Portfolio`
  routing — every fallback link redirected to the EDAP query console root.
  The leading `\d+_` prefix is now stripped from the slug, so the markdown
  fallback works correctly when `st.page_link` itself isn't available.

## 0.4.0 — Phase 4 (platform)

### Added
- `bench/bench_brain.py` — synthetic-data benchmark suite with 18 timings
  covering EOQ, hierarchical EB shrinkage, OTD cleaning, missingness +
  mass-impute, bullwhip, KM/per-group lead-time, GLEC emissions, lane
  volatility & portfolio mix, CVaR Pareto, multi-echelon safety stock,
  graph centrality (degree + eigenvector), and findings-index round-trip.
- `pages/14_Benchmarks.py` — in-app dashboard for the latest run.
- `bench/results/latest.csv` and timestamped historical runs.
- `requirements.pinned.txt` — version-bounded reference set validated
  together on Python 3.14 / Windows.
- `docs/ARCHITECTURE.md`, `docs/RESEARCH.md`, `docs/CONFIG.md`,
  `docs/RUNBOOK.md` — full operational + reference documentation.
- `src/brain/_version.py` — single source of truth for `__version__`.
- App sidebar now shows `Brain v{__version__}`.

### Fixed
- `brain.graph_backend.NetworkXBackend` was importing a non-existent
  `SCGraph` symbol from `graph_context`; rewritten to wrap an
  `nx.MultiDiGraph` directly so all 25 brain modules import cleanly.
- `bench_brain.py` deprecation warnings (`datetime.utcnow`, `'d'` unit)
  cleared.

## 0.3.0 — Phase 3 (MIT CTL research suite)

### Added
- `src/brain/research/`:
  - `hierarchical_eoq.py` — empirical-Bayes shrinkage on Poisson rates.
  - `causal_lead_time.py` — `econml` causal forest with permutation-importance
    fallback.
  - `lead_time_survival.py` — KM + Cox PH via `lifelines`, empirical-quantile
    fallback.
  - `bullwhip.py` — Lee/Padmanabhan/Whang variance ratio + heatmap frame.
  - `multi_echelon.py` — Graves-Willems guaranteed-service safety stock.
  - `sustainability.py` — GLEC / ISO 14083 Scope-3 freight emissions.
  - `freight_portfolio.py` — CV-thresholded contract/spot/mini-bid mix
    + goldfish-memory rejection score.
  - `risk_design.py` — Monte-Carlo CVaR + Pareto frontier on supplier
    scenarios.
- `pages/7_Lead_Time_Survival.py`, `8_Bullwhip.py`, `9_Multi_Echelon.py`,
  `10_Sustainability.py`, `11_Freight_Portfolio.py`.
- `ips_freight.ghost_lane_survival()` — gradient-boosted survival on
  contract-vs-actual volume (logistic fallback if `scikit-survival` not
  installed).
- `procurement_360` extended with **CVaR Pareto frontier** + **causal-forest
  lead-time attribution**.
- `drilldown.CITATIONS` — every research page renders a citation footer
  back to its originating MIT CTL lab.

## 0.2.0 — Phase 2 (depth)

### Added
- `src/brain/graph_backend.py` — pluggable graph backend behind one API
  (NetworkX default; Neo4j and Cosmos Gremlin opt-in).
- LinUCB contextual-bandit ranker so the EOQ table self-reshapes after
  each user resolution.
- OTD recursive page now indexes every cluster path into the findings
  index so other pages can drill through.

## 0.1.0 — Phase 1 (core)

### Added
- `src/brain/` package skeleton: `db_registry`, `data_access`,
  `schema_introspect`, `cleaning`, `eoq`, `otd_recursive`,
  `graph_context`, `imputation`, `ips_freight`, `findings_index`,
  `drilldown`.
- Six Streamlit pages: 🧠 Brain · 📦 EOQ Deviation · 🚚 OTD Recursive
  · 🏭 Procurement 360 · 🧩 Data Quality · 🔌 Connectors.
- Drill-down + cross-page findings index baked into `app.py`.
- `config/brain.yaml` — single source of truth for connectors, column
  mappings, and analytics defaults.
