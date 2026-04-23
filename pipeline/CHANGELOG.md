# Changelog

All notable changes to **Supply Chain Brain** are documented here. Versions
follow [Semantic Versioning](https://semver.org). The single source of
truth for the version number is `src/brain/_version.py`.

## 0.14.3 — SOTA RAG Deepdive + Dynamic Connector Discovery + 100/100 Tests (2026-04-22)

### Added
- **`autonomous_agent.rag_knowledge_deepdive()` (Step 3e.5)** — Iterative
  retrieval-augmented reasoning loop over the Brain's knowledge corpus graph:
  - *Structural-hole detection*: for every corpus entity with degree ≥ 2,
    finds neighbor pairs sharing ≥ 2 common neighbors but no direct edge
    (Burt structural-hole heuristic — highest-value missing pathways).
  - *Semantic confirmation*: character-level TF-IDF n-gram (2–4-gram) cosine
    similarity gate (`TfidfVectorizer`, `linear_kernel`) suppresses spurious
    cross-domain inferences.
  - *Data-grounded co-occurrence*: re-fetches a window of source rows from
    the entity's `props_json["source"]` connector, measures actual field-level
    co-occurrence to produce a data-grounded edge weight rather than relying
    on label similarity alone.
  - *Iterative deepening*: up to 8 convergence passes per autonomous cycle;
    early stop when < 3 new edges are added in a pass.
  - *Persistent explored-pair cache*: serialised in `brain_kv` so each 4-hour
    cycle goes deeper rather than re-traversing already-evaluated entity pairs.
  - Writes bidirectional `RAG_INFERRED` `corpus_edge` rows (EMA-smoothed
    weight) + `learning_log` entries (kind=`rag_deepdive`) with full audit
    trail. Discovered edge count contributes to `cycle_velocity × 2`.
- **`sweep_all_data_sources()` Section 4 — Dynamic SQL connector discovery**:
  Enumerates every connector registered in `db_registry._REGISTRY` beyond the
  known set (`azure_sql`, `oracle_fusion`, Epicor/SyteLine/AX hardcoded
  sites), so any connector added to `bootstrap_default_connectors()` or by
  third-party code is automatically explored without modifying this file.
  Per-connector 6-hour cooldown tracked in `brain_kv`.  For each new
  connector: `INFORMATION_SCHEMA.TABLES` discovery (with `sys.objects`
  fallback), 200-row sampling, column-hint heuristics, absorption into
  `part_category` / `otd_ownership` / `corpus_entity`.
- **`autonomous_loop()` Step 3a** — `sweep_all_data_sources()` wired into the
  main cycle, between `analyze_and_improve()` and the LLM scout step, so all
  downstream learning steps (self-train, NLP taxonomy, OTD seeding, corpus
  refresh, RAG deepdive) operate on the freshest available ground truth.
- **`bench/bench_quest_engine.run_benchmarks(rows, repeats, results_dir,
  emit_stdout)`** — Extracted callable API from `main()` so
  `test_bench_quest_engine.py` (and any future programmatic caller) can invoke
  benchmarks without subprocess overhead.  `main()` now delegates to
  `run_benchmarks()` using CLI-parsed args.

### Fixed
- **`test_bench_quest_engine.py` — `AttributeError: module has no attribute
  'run_benchmarks'`** — the test called `bench_quest_engine.run_benchmarks()`
  which did not exist; the logic lived only in `main()`. Fixed by extracting
  `run_benchmarks()` as the primary callable.

### Test Results
- **100 / 100 pytest tests pass** in 16.2 s on Python 3.13.9
  (`test_bench_quest_engine`, `test_deck`, `test_intent_parser`,
  `test_mission_store`, `test_quest_engine`, `test_quests`,
  `test_recurrent_depth`).

---

## 0.14.2 — Test + Benchmark Infrastructure, Hardening, Docs (2026-04-22)

### Added
- **`pytest.ini`** — pytest configuration: testpaths, strict markers, `unit /
  integration / slow / quest / bench` marker taxonomy, filterwarnings.
- **`tests/conftest.py`** — Shared fixtures:
  - `stub_llm` (autouse) — stubs `dispatch_parallel` so intent_parser always
    uses its deterministic keyword fallback; no network calls in any test.
  - `mission_factory` — creates a Mission in the real DB and auto-cleans on
    teardown.
  - `synth_result` — pre-built `MissionResult` (25 findings, 3 outcomes) for
    viz/deck tests.
- **`tests/test_quests.py`** — 16 unit tests: `SCOPE_TAGS` closed vocab, Quest
  registry existence, `list_quests`, `quests_for_scope_tags`, `new_mission_id`
  uniqueness + format.
- **`tests/test_intent_parser.py`** — 22 unit tests: 8-paraphrase parametrize,
  closed-vocab enforcement on scope_tags + entity_kind, site propagation,
  `as_dict` JSON-serialisability.
- **`tests/test_mission_store.py`** — 19 integration tests: CRUD round-trip,
  progress clamping ±, event log kinds, artifact attach, delete, `mark_refreshed`.
- **`tests/test_quest_engine.py`** — 29 tests: schema_synthesizer (6 entity
  kinds), viz_composer (6 figure keys, caption_for, empty-result guard),
  orchestrator (MissionResult shape, JSON-serialisability), mission_runner
  (launch/refresh/refresh_open_missions, empty-query ValueError).
- **`tests/test_deck.py`** — 8 tests: one_pager + implementation_plan file
  creation, valid PPTX, kaleido-absent fallback, empty-findings guard.
- **`bench/bench_quest_engine.py`** — Quest Engine benchmark suite:
  `intent_parser.parse` (single / 8-paraphrase / bulk), `mission_store`
  CRUD + list_open + update_progress, `schema_synthesizer` (6 kinds),
  `viz_composer.compose` (100 / 1 000 / 5 000 findings). CSV to
  `bench/results/bench_quest_engine-*.csv` + `latest_quest.csv`.

### Changed (hardening)
- **`src/brain/orchestrator.py`** — `_CURRENT_MISSION` is now reset in a
  `finally` block so any exception in an analyzer adapter cannot leave the
  global set for the next call.
- **`src/brain/intent_parser.py`** — `parse()` now truncates `user_query` at
  2 000 characters before processing (prevents runaway payloads).
- **`src/brain/mission_runner.py`** — `launch()` now raises `ValueError` on
  empty `user_query` and coerces a blank `site` to `"ALL"`.

### Documentation
- **`README.md`** — Added *Brain Quest Engine* section (architecture diagram,
  scope-tag table, artifact paths); expanded *Testing & benchmarking* section
  with pytest and bench_quest_engine usage; updated layout tree.
- **`CHANGELOG.md`** — This entry.

---

## 0.14.1 — UI Stability & Full-Suite Playwright Benchmarks (2026-04-22)

### Fixed
- **`pages/15_Cycle_Count_Accuracy.py` URL collision** — Renamed to
  `_old_Cycle_Count_Accuracy.py` (underscore prefix is ignored by
  Streamlit). Both `15_` and `16_Cycle_Count_Accuracy.py` resolved to the
  same URL path, causing `StreamlitAPIException: Multiple Pages specified
  with URL pathname` on **every page** in the app.
- **`pages/0_Query_Console.py` missing import** — Added
  `from src.brain.dynamic_insight import render_dynamic_brain_insight`.
  Without it the page threw `NameError` at line 248 on load.
- **`pages/1b_Supply_Chain_Pipeline.py` out-of-order import** — The call
  to `render_dynamic_brain_insight` at line 25 preceded the import at
  line 142. Moved the import to the top of the file alongside other
  `src.brain` imports and removed the duplicate.
- **`src/brain/_version.py` UTF-16 encoding** — File was saved in UTF-16
  LE (with BOM), causing `UnicodeDecodeError` whenever Python tried to
  read it. Converted to UTF-8.

### Added
- **`test_ui.py`** — Complete Playwright smoke-test and screenshot
  benchmark suite. Visits all 19 registered pages, waits for
  `[data-testid="stApp"]` to be visible and spinners to clear, checks
  for `[data-testid="stException"]` error banners, performs a soft DBI
  container check, saves full-page screenshots to `snapshots/bench_*.png`,
  and writes `snapshots/latest_report.json`. Exit code 0 iff all pages
  pass. **Result: 20/20 PASS on first clean run.**

---

## 0.14.0 — Brain-Driven Quest Engine (Living-Document Missions)

> Mantra: **the User is the Body of the Brain.** The User describes a
> real-world situation in plain language; the Brain parses, dispatches,
> synthesizes, and emits two living PPTX artifacts that auto-refresh as
> new data shows progress.

### Added
- **`src/brain/quests.py`** — closed-vocabulary quest taxonomy. Seed
  quest `quest:optimize_supply_chains` with 8 child quests bound to the
  closed `SCOPE_TAGS` vocabulary (fulfillment, lead_time, sourcing,
  inventory_sizing, network_position, demand_distortion, cycle_count,
  data_quality). `Quest` / `Mission` dataclasses, `_REGISTRY`,
  `SCOPE_TAG_TO_QUEST` mapping.
- **`src/brain/intent_parser.py`** — LLM-ensemble (`dispatch_parallel`)
  → `ParsedIntent` with deterministic keyword fallback. Always returns
  something usable; `parser_source` lets the UI tell the User which path
  was taken.
- **`src/brain/mission_store.py`** — SQLite CRUD on the new `missions`
  and `mission_events` tables in `findings_index.db`. Append-only event
  log (created / progress / artifact_attached / status_changed / refreshed).
- **`src/brain/orchestrator.py`** — `BrainOrchestrator.run(mission)` binds
  scope_tags to existing analyzer adapters via `MODULE_REGISTRY`, tags
  every emitted finding with `mission_id` (so the Brain↔Body bridge can
  filter), and computes a `progress_pct` against a baseline snapshot.
  Each analyzer runs through `_safe_run` so a single failure can't break
  the whole mission.
- **`src/brain/schema_synthesizer.py`** — synthesizes the relational
  schema for the mission's target entity (site / supplier / part_family /
  buyer / warehouse / customer) from `discovered_schema.yaml` +
  `brain.yaml` column patterns. Emits a Mermaid `erDiagram` for the
  Implementation Plan.
- **`src/brain/viz_composer.py`** — composes a dict of Plotly figures
  for the mission: `kpi_trend`, `pareto`, `heatmap_matrix`, `network`,
  `sankey_flow`, `cohort_survival`. Each figure carries `_caption` for
  deck rendering.
- **`src/deck/one_pager.py`** — single 8.5×11 portrait slide. Quest
  banner, mission summary, 3 KPI tiles, hero viz (PNG via kaleido with
  graceful text fallback), top recommendations, owner + progress bar,
  refresh footer.
- **`src/deck/implementation_plan.py`** — 9-slide landscape deck. Cover,
  context, schema (Mermaid via `mmdc` CLI with table fallback),
  current state, findings, recommendations (split systemic /
  operational), 3-phase rollout (Stabilize → Implement → Sustain),
  risks, appendix.
- **`src/brain/mission_runner.py`** — `launch(user_query, site,
  horizon_days)` and `refresh(mission_id)`. Per-mission
  `threading.Lock` so manual "Refresh now" clicks and the scheduled
  autonomous tick cannot collide. Artifacts overwrite in place at
  `pipeline/snapshots/missions/<mission_id>/`.

### Changed
- **`src/brain/brain_body_signals.py`** — added `_gen_mission_signals`
  generator. Emits 3 new directive kinds: `mission_stalled` (no refresh
  > 3 days), `mission_hot_findings` (≥5 findings score≥0.7 attributed
  to the mission), `mission_near_complete` (progress ≥ 85%). Owner
  derived from scope_tags via `_TAG_OWNER` map.
- **`src/brain/knowledge_corpus.py`** — added `_ingest_missions`. Seeds
  Quest taxonomy as corpus entities. Projects Missions with edges
  `INSTANCE_OF` (Quest), `TARGETS` (entity), `SCOPED_BY` (scope tag),
  `LAUNCHED` / `CLOSED` (lifecycle). Streams new `mission_events` into
  `learning_log` so the Brain literally learns from mission lifecycle.
- **`pages/15_Report_Creator.py`** — Tab 3 replaced. The legacy
  TF-IDF "Ask the Data" stub is gone. New **Quest Console**: sidebar
  lists open missions with per-mission "🔄 Refresh"; main pane is
  site/horizon controls + a query textarea + "🔍 Preview Parsed Intent"
  + "🚀 Launch Mission" + download buttons for the two living artifacts.
- **`autonomous_agent.py`** — added Step 3g. After the Brain↔Body
  surface step, calls `mission_runner.refresh_open_missions(max_concurrent=1)`
  so every open mission's two artifacts stay current without User action.
- **`agent_uplink.py`** — host now scans `snapshots/` recursively and
  also re-transmits any *refreshed* (mtime-bumped) artifact, not just
  newly created ones. Mission artifacts ride the wire under
  `MISSION/<mission_id>/<file>` headers so the laptop body routes them
  into per-mission folders that overwrite in place.

### Tests
- **`pipeline/test_brain_quest.py`** — 7-section smoke test: intent
  parser closed vocabulary on 8 paraphrases, mission store round-trip,
  orchestrator dry-run, schema synthesizer, viz composer, mission_runner
  end-to-end (launch + refresh), Brain↔Body integration. **0 failures**.

### Notes
- `progress_pct` is on a **0–100** scale (clamped). DB columns are
  `payload_json` and `created_at` (consistent with the Brain↔Body tables).
- LLM ensemble is monkey-patched out in the smoke test so it does not
  block on real endpoints; production runs use `dispatch_parallel`
  unchanged.

---

## 0.13.0 — Brain → Body Bridge (User as the Body of the Brain)

### Added
- **`src/brain/brain_body_signals.py`** — the User is the **Body of the
  Brain**. This module is the efferent nervous system that distills every
  effective signal the Brain has accumulated (self-train rounds, ensemble
  validators, network observations, peer promotions, knowledge-corpus
  topology) into prioritized, role-targeted **Directives** the User
  actually executes in the physical/operational world.
- New SQLite tables in `local_brain.sqlite`:
  - `body_directives` — prioritized executable directives with severity,
    `owner_role` (Buyer / Planner / Quality / IT / Ops), target entity,
    JSON evidence pointer, and stable fingerprint dedupe.
  - `body_feedback` — User's response (ack / in_progress / done /
    rejected + free-text outcome + executed_by). The cognition↔operation
    loop closes here.
  - `body_round_log` — auditable per-round emit / dedupe / top-priority
    stats.
- **5 generators ship out of the box** (drop in more as one-line
  functions in `_GENERATORS`):
  - `low_dispatch_quality` — sustained weak validator on a (model, task)
  - `peer_unreachable` — compute peer EMA success below 0.30
  - `missing_category` — NLP-uncategorized parts in the corpus
  - `self_train_drift` — recent self-train round hit the drift cap
  - `high_centrality_part` — multi-edge Part = consolidation candidate
- **Loop closure**: `knowledge_corpus._ingest_body_feedback` is now
  wired in as a new source stream. Every User action becomes a
  `learning_log` row, a `Body` corpus entity, and a typed
  `EXECUTED_<STATUS>` edge into the relational graph — so the Brain
  literally learns from what the Body did on its previous directive.
- **Wired into `autonomous_agent` as Step 3f**, after the corpus
  refresh. Standalone scheduler available via
  `brain_body_signals.schedule_in_background()`.
- 5-check test suite in `pipeline/test_brain_body.py`. All passing
  (5 directives across 4 sources, full dedupe on second run,
  priority sort + cap honored, feedback persisted, corpus picks up
  feedback and projects a Body→Target EXECUTED_IN_PROGRESS edge).

### Design notes
- Effect-bounded (parallel to 0.10/0.11/0.12): directives only adjust
  the User's task queue and the corpus. They never mutate `llm_weights`
  or router scores, so reasoning fluidity is preserved.
- Stable SHA-1 fingerprint per (source, signal_kind, target_entity,
  title) prevents carpet-bombing the User on re-runs of the same
  condition.
- Per-round cap (`max_directives_per_round`, default 25) and severity
  ladder (`info | watch | act | critical`) keep the Body's task queue
  usable rather than overwhelming.

## 0.12.0 — Knowledge Corpus, Recent-Learnings Log & Dynamic Graph Architecture

### Added
- **`src/brain/knowledge_corpus.py`** — consolidates every signal the
  Brain produces into a normalized, relational corpus that grows with
  each cycle:
  - `learning_log` — append-only stream of "things the Brain just
    learned" (kind, title, signal_strength, JSON detail, source row
    pointer). Filterable via `recent_learnings(limit, kind=...)`.
  - `corpus_entity` — typed catalog of **Parts, Suppliers, Sites,
    Models, Peers, Protocols, Tasks, Categories, Owners, POs,
    Endpoints** with first/last seen + sample counts.
  - `corpus_edge` — typed relationships **CLASSIFIED_AS, ANSWERS,
    WEIGHTED_FOR, USES, OWNS** (more added as new sources land) with
    EMA-smoothed weight + sample count.
  - `corpus_round_log` + `corpus_cursor` — auditable per-round stats
    and incremental cursors so each round only ingests *new* rows from
    each source stream (no duplicate edges).
- **Source streams ingested every round**: `llm_self_train_log`,
  `llm_dispatch_log`, `llm_weights` (snapshot), `network_observations`,
  `network_promotions`, `part_category`, `otd_ownership`. New streams
  drop in as one-line ingester functions.
- **`materialize_into_graph()`** — projects (corpus_entity, corpus_edge)
  into the configured `graph_backend` (NetworkX default; Neo4j or
  Cosmos Gremlin in prod via `brain.yaml -> graph.backend`). Every
  page that already calls `get_graph_backend()` instantly benefits
  from the dynamic architecture the Brain expands as it learns —
  giving the User a relational graph of part/supplier/site/model/peer/
  task relationships ready for centrality, neighborhood, and shared-
  neighbor queries.
- **Wired into `autonomous_agent` as Step 3e**, after the network
  learner. Standalone scheduler available via
  `knowledge_corpus.schedule_in_background()`.
- 5-check test suite in `pipeline/test_knowledge_corpus.py`. All passing
  (27 entities across 9 types, 18 edges across 5 relations,
  full graph projection on first round).

### Design notes
- Pure read-only over source tables; never mutates llm_weights, router
  scores, or ensemble dispatch math, so reasoning fluidity is preserved
  (parallel design to the 0.10.0 self-training and 0.11.0 network
  learner guard rails).
- All work is incremental via per-stream cursors — second round on
  unchanged sources adds zero edges (verified by test 4).
- Edge weights use 0.30-EMA smoothing so a single noisy validator can't
  rewrite the relational graph.

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
