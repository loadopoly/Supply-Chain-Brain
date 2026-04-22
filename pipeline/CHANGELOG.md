# Changelog

All notable changes to **Supply Chain Brain** are documented here. Versions
follow [Semantic Versioning](https://semver.org). The single source of
truth for the version number is `src/brain/_version.py`.

## 0.11.0 — Network Expansion Learner (Cross-Protocol)

### Added
- **`src/brain/network_learner.py`** — the Brain now learns from every
  connection it already touches across **all protocols**: Azure SQL,
  Oracle Fusion (HTTPS), SMB shares, SMTP/MX, declared external apps,
  cross-app webhook subscribers, OneDrive bridge_state heartbeats,
  compute-grid peers, and operator seeds.
- New SQLite tables in `local_brain.sqlite`:
  - `network_observations` — append-only audit of every probe (host,
    protocol, port, capability, latency, ok, error, source).
  - `network_topology` — rolling per-(host, protocol, port) stats
    (samples, successes, EMA latency, EMA success-rate, first/last seen).
  - `network_promotions` — audit of every host promoted into the
    compute-grid seed pool.
- **Wired into `autonomous_agent` as Step 3d**, alongside Step 3c
  self-training. Standalone scheduler available via
  `network_learner.schedule_in_background()`.
- Verified peers (EMA success ≥ 0.70 on tcp:8000) are promoted into
  `SCBRAIN_GRID_EXTRA_SEEDS` so the next `compute_grid` discovery round
  can dispatch jobs to them — closing the loop from observation to
  network expansion.
- 5-check test suite in `pipeline/test_network_learner.py`. All passing.

### Guard Rails (so observation never compromises fluidity or safety)
- **Passive-by-default**. Only TCP-connect (or DNS resolve when no port
  is known) on endpoints already declared in `connections.yaml`,
  `brain.yaml`, `bridge_state/compute_peers/`, mapped SMB drives, MX
  records, or operator seeds. No credentials are ever transmitted.
- **`active_probe: false`** by default — arbitrary CIDR scanning is
  opt-in only.
- **`probe_timeout_s: 1.5`** + **`max_probes_per_round: 64`** +
  **`min_seconds_between_rounds: 30`** prevent any single round from
  saturating the network or the workstation.
- **Routing-only effect** — observations adjust compute-grid seeds and
  topology stats. They never touch `llm_weights`, router scores, or
  ensemble dispatch math, so multi-echeloned reasoning fluidity is
  preserved (parallel design to the 0.10.0 self-training guard rails).
- **EMA smoothing (`ema_alpha: 0.30`)** keeps a single down-blip from
  evicting a known-good peer.
- **Full audit** — every probe and every promotion is queryable from
  the Decision Log page via the new tables.

## 0.10.0 — Bounded Self-Training (Pipeline-Grounded W&B Updates)

### Added
- **`src/brain/llm_self_train.py`** — the Brain now uses its own data
  pipeline as a *soft validator* to refine per-(model, task) weights.
  Mines recent dispatches from `llm_dispatch_log` whose live validator
  was NULL, scores each contributor's response against a whitelisted
  ground-truth table (e.g. `part_category`, `otd_ownership`) and applies
  bounded SGD updates via the existing `update_weights` path.
- **`pipeline/test_self_train.py`** — 5-check guard-rail suite (mining,
  whitelist isolation, drift cap, diversity guard, exploration reserve).
  All passing.
- **Wired into autonomous_agent as Step 3c** — runs once per cycle
  alongside the LLM scout. Standalone scheduler also available via
  `llm_self_train.schedule_in_background()`.
- New SQLite table `llm_self_train_log` (samples, matches, drift-capped
  count, dampened/floored model lists, notes) for full provenance.

### Guard Rails (so the Brain doesn't lose fluidity)
- **Per-task whitelist** in `brain.yaml → llms.self_train.tasks`. Only
  tasks with a declared `(table, key_column, value_column)` ever receive
  pipeline-grounded updates. Generative / reasoning / what-if /
  multi-echelon tasks keep pure router-prior dynamics.
- **`lr_scale` (0.5)** — self-training validator is dampened toward 0.5
  by half the live learning rate.
- **`drift_cap` (0.5)** — post-round weight is clamped within ±0.5 of
  the pre-round snapshot. One round can never catastrophically rewrite
  the ensemble.
- **`min_weight_floor` (0.10)** — re-floored after each round so no
  model is ever fully suppressed.
- **`max_share_per_task` (0.50) + `dampen_factor` (0.85)** — any model
  exceeding 50% of total weight mass on a task is dampened back toward
  parity. Keeps the ensemble plural.
- **`exploration_reserve` (0.15)** — 15% of all `dispatch_parallel`
  calls bypass the learned weights entirely and use the pure router
  prior, keeping newcomer scout-discovered models and underdogs
  permanently reachable.

## 0.9.0 — Distributed Multi-LLM Brain & Shared Compute Grid

### Added
- **Free-tier LLM registry & router (`src/brain/llm_router.py`)** with
  capability-weighted scoring (Gemma 4, GLM-5.1, Qwen3.5-397B-A17B,
  DeepSeek-V3.2, Kimi-K2.5, MiniMax-M2.7, MiMo-V2-Flash; cost = 0).
- **Periodic LLM scout (`src/brain/llm_scout.py`)** — HuggingFace,
  OpenRouter, lmarena, artificial-analysis adapters. Free-tier +
  vendor-blocklist gated. Wired as Step 3b in `autonomous_agent`.
- **Parallel multi-LLM ensemble (`src/brain/llm_ensemble.py`)** —
  `dispatch_parallel` fans the top-K eligible models out concurrently
  with online learning of per-(model, task) weights & biases (SGD + L2 +
  EMA telemetry) persisted in `llm_weights`.
- **Shared compute grid (`src/brain/compute_grid.py`)** — domain
  workstations contribute CPU/GPU through the existing piggyback fabric
  (OneDrive-synced `bridge_state/compute_peers/`, `compute_*.trigger`
  wake-up files, port 8000, HMAC over body via `SCBRAIN_GRID_SECRET`).
- **`bridge_watcher.ps1` extension** — `Ensure-ComputeNode` auto-spawns
  `serve_compute_node()` on any `compute_*.trigger`.
- **Multi-vendor GPU detection** — NVIDIA via `nvidia-smi` plus AMD/Intel
  via Win32_VideoController WMI.
- **Idempotent local-node bootstrap** (`ensure_local_node_running()`)
  and self-host detection so single-host installs work zero-config.
- **Fast-fail dead peers** — pre-`submit_job` 1 s TCP probe + 30 s
  negative cache (was 15 s+ failures).
- **Test suite (`pipeline/test_compute_grid.py`)** — 6 end-to-end checks.

## 0.8.2 — OTD Recursive Timeline Cascade
- **OTD Recursive Dashboard** correctly filters Live Replica pulling to respect the exact `global_window` date range selected.

## 0.8.1 — Brain Pipeline Dynamic Reactivity

### Added
- **Dynamic Brain Telemetry** — The Pipeline view now calculates its deterministic `friction` index dynamically around the baseline (using a signed normal distribution). It now accurately swings from 0.0 (🟢 OK) to 1.0 (🔴 Act Now) depending on the selected Business Unit, Time Window, and Decision Horizon.
- **Contextual Lens Weighting (`actions.py`)** — Brain suggestions now automatically scale annualized monetary returns based on the chosen `Decision Horizon` (e.g. 7d = 20% of YoY value; 365d = 120%). View modes (Cost Flow vs Risk Surface) tilt the algorithm to amplify either dollar values or pain-friction indexes respectfully.
- **Action Queue Consistency** — Synchronized the Cross-Stage Friction Ranking bar chart and the end-user Action Queue table to parse directly from the master `actions_for_pipeline()` method instead of executing asynchronous duplicate calls.

## 0.8.0 — Architectural Overhaul & Actionable Intelligence

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
