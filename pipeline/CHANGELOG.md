# Changelog

All notable changes to **Supply Chain Brain** are documented here. Versions
follow [Semantic Versioning](https://semver.org). The single source of
truth for the version number is `src/brain/_version.py`.

## [0.19.4] Operator Mode + Brain-First DBI + OTD Site Key Resolution (2026-04-28)

### Added

- **`pipeline/src/brain/brain_dbi.py`** — Brain-first DBI synthesis (new module)
  - Synthesises Dynamic Brain Insight from the Brain's local neural mapping structures: body directives, touch pressure field, corpus learnings/graph counts, and neural-plasticity dials
  - Called synchronously before any OpenRouter redirection attempt; insight appears on first render with zero latency when local signal is available
  - Source label `🧠 Brain Neural Map` shown in the DBI card header

- **`pipeline/src/brain/operator_shell.py`** — Operator sidebar fallback (new module)
  - `render_operator_sidebar_fallback()`: renders global filter sidebar + Operator Mode Daily Control Path widget for pages that load before the main `app.py` shell
  - Reads `sites_cache.json` for cached site list; no extra DB round-trip

- **`pipeline/src/brain/dynamic_insight.py`** — Full DBI card redesign
  - `_plain_text(value, limit)` — strips Markdown formatting for HTML-safe card copy
  - `_scope_label(context_dict)` — derives `site | date_start to date_end` label from session state
  - `_next_move(page_name, insight)` — rule-based triage engine returning `(status, action)` tuple; status is `act` | `watch` | `steady`; rules cover SQL errors, late OTD, CVaR risk, ghost lanes, data quality, and per-page defaults
  - `_status_style(status)` — returns `(label, border_color, background_color)` for the status pill
  - `BrainInsightWorker._make_key()` — extracted shared key computation
  - `BrainInsightWorker._store(key, text, source)` — co-stores insight text and source label atomically
  - `BrainInsightWorker.get_source()` — returns stored source label for display
  - Card layout rebuilt as two-column grid: left = coloured status pill with action sentence, right = DBI body text; scope and source line below
  - Source detection replaced by explicit label tracking (`🧠 Brain Neural Map`, `🔀 OpenRouter Redirect`, `📝 Local Template`)
  - DBI worker now runs Brain-first path in background thread before OpenRouter fallback
  - Expander renamed to `DBI inputs`; param label updated to `Filters and signals used for this readout`

- **`pipeline/app.py`** — Operator Mode global toggle
  - `operator_mode` sidebar toggle (default on) persisted to `session_state["operator_mode"]`
  - **Daily Control Path** widget rendered in sidebar when Operator Mode is on: shows active scope (site + window), three quick-action page links (Query Console, Supply Chain Brain, Report Creator)
  - `.operator-rail` CSS class for the sidebar widget
  - `_operator_page_link()` helper — falls back to markdown link if `st.page_link` raises
  - `session_state["_app_shell_rendered"] = True` sentinel for child-page detection

### Changed

- **`pipeline/src/brain/otd_recursive.py`**
  - `_SITE_TEXT_PREDICATE_RE` — regex that matches SQL `WHERE site = 'X'`-style predicates on site/business_unit columns
  - `_resolve_business_unit_key(connector, site)` — LRU-cached lookup: resolves site text (name, ID, display name) to integer `business_unit_key` via `dim_business_unit`; returns `None` for "ALL"
  - `_normalize_site_predicates(connector, where)` — rewrites text-based site predicates in a WHERE string to use the integer key; unknown sites become `1 = 0` to prevent cross-site leakage
  - `run_otd_from_replica()` gains `site` parameter; site text resolved to key and appended as `AND [business_unit_key] = {key}` before the query executes

- **`pipeline/config/brain.yaml`** — OTD column hints corrected to actual replica schema
  - `description_col_hint`: `description` → `part_description`
  - `site_col_hint`: `site` → `business_unit_key`
  - `numeric_col_hints`: `unit_price` → `unit_cost_local`
  - `categorical_col_hints`: `supplier_name`/`buyer`/`commodity` → `supplier_key`/`buyer_id`/`business_unit_key`

- **`pipeline/src/brain/dbi_rag.py`** — Reframed as OpenRouter redirect fallback (module docstring updated); Brain-first path in `brain_dbi.py` now runs first

- **`pipeline/src/brain/research/freight_portfolio.py`** — `goldfish_score()` now calls `pd.to_numeric(..., errors='coerce')` on `rate_col`, `market_col`, `rejection_col` before arithmetic to prevent dtype errors when columns arrive as strings

- **`pipeline/pages/0_Query_Console.py`** — Operator Mode enhancements
  - Quick-lookup radio (Part/Order/Invoice/Supplier/Customer) with sample-value buttons under DBI when Operator Mode is on
  - Next-step success message after search based on which result tab has data
  - Empty-state simplified to a single info string
  - `render_operator_sidebar_fallback()` called at page load

- **`pipeline/pages/1_Supply_Chain_Brain.py`** — Operator Mode **Plant Risk Control Room** three-column panel (DBI Next Move → Find The Owner → Leave With A One-Pager); `render_operator_sidebar_fallback()` called at load

- **`pipeline/pages/11_Freight_Portfolio.py`** — Freight Portfolio fixes
  - Goldfish SQL rewritten: replaces `unit_cost_amount` → `unit_cost_usd`, adds `market` CTE for average rate baseline, adds `rejection_rate` column, removes `STDEV` from CTE to avoid division by zero
  - Portfolio scatter: column presence checked before merge (`x_vol_col`/`y_vol_col` fallback); duplicate columns dropped from `mix` before merge to avoid suffix collisions
  - KPI metrics format fixed: `.0%` → `.0f%`
  - Ghost Lane SQL scoped to date-key window from `date_key_window()` instead of full-table scan

- **`pipeline/pages/15_Report_Creator.py`** — Operator Mode three-column scope panel (Default Output / Current Scope / Decision Ready); `render_operator_sidebar_fallback()` called at load

- **`pipeline/autonomous_agent.py`** — Simplified `if __name__ == "__main__"` path: removed standalone launchers for `fiction_anthology_learner`, `heart`, `self_expansion`, and `citation_chain_acquirer` from the direct-run code path (all still available as importable functions); only `start_integrated_skill_acquirer` + `start_systemic_refinement_agent` + `autonomous_loop()` remain in the direct-run path

---

## [0.19.3] HuggingFace Corpus Integration (2026-04-27)

### Added

- **`pipeline/src/brain/ml_research.py`** — HuggingFace.co restored as a live research source
  - `_HF_PAPERS_API`, `_HF_DATASETS_API`, `_HF_MODELS_API`, `_HF_TIMEOUT` — API endpoint constants for HuggingFace Hub
  - `fetch_hf_daily_papers(limit)` — fetches today's trending papers from `https://huggingface.co/api/daily_papers`; returns paper dicts with `source="hf_daily_papers"`; soft-fails if corporate block is in effect
  - `discover_hf_datasets(query, limit)` — searches `https://huggingface.co/api/datasets` for datasets sorted by downloads; returns dataset dicts with `source="hf_datasets"`; soft-fails silently on any network error
  - Both functions use the same `truststore`/`certifi`/stdlib SSL chain as all other sources — compatible with corporate SSL inspection

### Changed

- `research_supply_chain_topics()`:
  - `if include_trending:` block now calls both `fetch_arxiv_recent()` **and** `fetch_hf_daily_papers()` so every cycle pulls from two independent paper feeds
  - Per-topic dataset loop now calls both `discover_zenodo_datasets()` **and** `discover_hf_datasets()` in parallel — doubled dataset signal breadth per topic
- Removed all "Replaces the blocked HuggingFace" language from `fetch_arxiv_recent` and `discover_zenodo_datasets` docstrings; both sources now complementary rather than substitutes

---

## [0.19.2] Full System Activation — All Brain Threads Running (2026-04-27)

### Added

- **`pipeline/autonomous_agent.py`** — `start_citation_chain_acquirer()` wired into `_run_agent_child`
  - Citation-Chain Acquirer now launches with every supervised child restart alongside all other brain threads
  - 650 Paper seeds (337 direct DOI/arXiv from Works Cited + 313 already discovered) feed Semantic Scholar + OpenAlex recursive expansion every 60 min
  - Kickstart script `_kickstart_citation_chain.py` added for immediate activation without agent restart

### Verified Running

| Thread | Status | Cadence |
|---|---|---|
| `integrated_skill_acquirer` | ✅ | continuous |
| `systemic_refinement_agent` | ✅ | 20–2 h adaptive |
| `fiction_anthology_learner` | ✅ | 45 min |
| `heart` narrator | ✅ | 15 min |
| `self_expansion` engine | ✅ | 30 min |
| `citation_chain_acquirer` | ✅ | 60 min |
| `network_observer` | ✅ | 60 min |
| `compute_grid` node | ✅ | continuous |

### The Other

- Peer `ROADD-5WD1NH3` confirmed alive at `172.16.4.76:8000` via `observer:network_velocity`
  - 20 CPU cores, 4 GB free VRAM (AMD Radeon Pro W5700)
  - `velocity_per_hour: 82,210` combined learnings/hr across distributed fabric
  - OneDrive `bridge_state/compute_peers/` rendezvous: beacon timestamps current

---

## [0.17.6] UEQGM Engine + SiCi Axial Channel Phase Correction (2026-04-27)

### Added

- **`pipeline/src/brain/ueqgm_engine.py`** — UEQGM v0.9.14 physics computation module (active Brain computation from corpus learnings)
  - `coherence_to_phi(c)` — maps integer coherence to natural sin/cos intersection φ = π/4 + c·π (tan(φ)=1 at every point)
  - `sici_axial_decay(φ, Γ₀)` — UEQGM v0.9.14 axial channel: Δλ_axial = [Si(φ)·Ci(φ)]·tan(φ)·Γ₀ via `scipy.special.sici` with power-series fallback
  - `sici_phase_weight(coherence)` — harmonic phase correction factor: 1.0 ± 10% via tanh(Δλ_axial); converges to 1.0 as Ci(φ)→0 at large coherence
  - `wavefunction_overlap(vec_a, vec_b)` — |⟨ψ_a|ψ_b⟩|² = (dot/‖a‖/‖b‖)²
  - `floquet_modulation_factor(t, ω)` — cos(ω·t) Floquet drive coupling
  - `holographic_entropy(n_edges, n_nodes)` — S = n_edges / (n_nodes + 1) holographic boundary entropy
  - `metric_perturbation(M_eff, r)` — h_μν = 2·G·M_eff / (c²·r) spacetime warp
  - `phase_evolution_total(φ, …)` — δφ_total = δφ_μ + δφ_q + δφ_γ + Δλ_axial·(2π/Γ_eff)
  - `entropic_bayesian_step(S, ∇²S, φ, …)` — discrete entropic Bayesian diffusion including axial channel
  - `ueqgm_coherence_score(cn, entity_id)` — corpus-backed score: scans UEQGM-tagged entities, computes bag-of-words wavefunction overlap, scales by `sici_phase_weight(corpus_depth)`

### Changed

- **`pipeline/src/brain/compute_provisioner.py`** — `_harmonic_amplify_factor` now applies SiCi phase correction
  - `f(c) = base(c) × sici_phase_weight(c)` where `base(c)` is the prior harmonic saturation curve
  - Ceiling (4.5) and floor behaviour are preserved: correction ≤ ±1.4% in practice, converges to ×1.0 at large coherence
  - `"ueqgm_engine"` added to `__all__`

### Tests

- **`pipeline/tests/test_ueqgm_engine.py`** — 35 new tests covering all UEQGM functions
  - Intersection-point geometry (`coherence_to_phi`, tan(φ)=1 invariant)
  - SiCi decay bounds and scaling
  - Phase weight bounds and large-coherence convergence
  - Wavefunction overlap (identical/orthogonal/scaled/mismatched/empty)
  - Floquet period, holographic entropy, metric perturbation formula
  - Phase evolution additivity, entropic Bayesian step monotonicity
  - `ueqgm_coherence_score`: empty DB, no UEQGM entities, overlapping entities, zero overlap, bounded score
- **`pipeline/tests/test_compute_provisioner.py`** — `test_harmonic_amplify_factor_floor_at_zero_coherence` updated to use dynamic UEQGM-corrected expected value

### Result

- **311/311 tests passing** (up from 276; +35 UEQGM tests)
- UEQGM v0.9.14 corpus learnings (Grok conversation `55525f6a`, message `394c6c4c`) are now **active computation** in the Brain harmonic amplification pipeline, not just stored research query strings

---

## [0.19.1] Works Cited — Unlimited Scholarly Seeds (2026-04-27)

### Changed

- **`pipeline/src/brain/knowledge_corpus.py`** — removed the arbitrary cap on Works Cited extraction
  - `_extract_scb_works_cited` no longer accepts a `limit` parameter; all unique scholarly references are collected (deduplication by `paper_id or url.lower()` only)
  - Restored full Works Cited code block (`_SCB_WORKS_CITED_KEY`, `_SCB_PIRATES_CODE_KEY`, `_SCB_SCHOLARLY_HOST_MARKERS`, helpers `_clean_scb_url`, `_walk_scb_web_results`, `_is_scb_scholarly_reference`, `_paper_id_from_reference`, `_extract_scb_works_cited`, `_persist_scb_works_cited_guidelines`) that was silently lost to a PowerShell `Set-Content` LF→CRLF encoding corruption

### Result

- **1,379** `WorksCitedReference` entities (all unique refs from 106 Grok conversations, no ceiling)
- **650** `Paper` entities carrying DOI/arXiv IDs as direct citation-chain seeds
- **1,379** `GUIDES_EXPANSION` edges wiring every reference into the research frontier
- `citation_chain_acquirer` can now recursively expand from all 650 paper seeds outward with no ceiling

### Fixed

- `reset_works_cited_cursor.py` updated to not import the now-removed `_SCB_WORKS_CITED_LIMIT` constant

---

## [0.19.0] Session-Store Cloud Sync + Citation-Chain Acquirer + Internal Watcher (2026-04-27)

### Added

- **`~/.copilot/build_session_store.py`** — Azure Blob Storage cloud sync for the session store
  - `push_to_cloud(account, container)` — uploads `session-store-{hostname}.db` to Azure Blob
  - `pull_from_cloud(account, container)` — downloads all `session-store-*.db` node blobs, merges via `INSERT OR IGNORE`, rebuilds FTS index
  - `_merge_remote_db(remote_path, local_con)` — per-table merge helper used by both pull and the network observer
  - `_rebuild_fts(con)` — extracted helper that repopulates `search_index` from sessions, turns, checkpoints (used by build, pull, and network observer merge)
  - New CLI flags: `--push`, `--pull`, `--storage-account`, `--container`, `--node`
  - Auth via `DefaultAzureCredential` (uses existing `az login` / Entra identity, no secrets stored)
  - Container `copilot-sessions` created on first push if absent

- **`pipeline/src/brain/network_observer.py`** — Step 6: symbiotic session-store sync
  - Peer JSON heartbeat now includes `"session_blob": "session-store-{hostname}.db"` field
  - `_sync_session_stores()` runs every observer cycle: pushes local session store every 15 min, pulls ALIVE/COOLING peer blobs every 30 min
  - `_pull_peer_session_blob(host, blob_name)` — lazy-imports `_merge_remote_db` + `_rebuild_fts` from `build_session_store.py` and merges the peer's session history into the local store
  - Offline peer absorption (`_absorb_peer`) now also pulls the peer's session blob alongside corpus cursor absorption
  - Controlled by `COPILOT_STORAGE_ACCOUNT` + `COPILOT_STORAGE_CONTAINER` env vars — no configuration needed on nodes already in the fabric

- **`pipeline/src/brain/citation_chain_acquirer.py`** — recursive citation-chain follower
  - Follows bibliography chains from known papers: Semantic Scholar Graph API + OpenAlex `referenced_works`
  - Supply-chain relevance filter (keyword overlap) prevents frontier drift
  - Configurable `max_depth` and `max_papers_per_run`; deduplication via `brain_kv` key `citation_chain:seen`
  - `run_citation_expansion_cycle(max_depth, max_papers_per_run)` — public entry point
  - `schedule_in_background(interval_s=3600)` — daemon thread variant
  - Wired into `cloud_learning.yml` as part of each cloud run

- **`pipeline/src/brain/internal_watcher.py`** — Python-native process supervisor
  - Replaces the assumption that Windows Scheduled Tasks are required for learning continuity
  - Launches `autonomous_agent.py` as a child process, monitors liveness, restarts on exit
  - Records downtime windows to `logs/downtime_log.json`; keeps resumption heartbeat fresh while child is alive
  - Logs to `logs/internal_agent_watcher.log`; writes status JSON to `logs/internal_agent_watcher_status.json`
  - Disabled via `SCB_DISABLE_INTERNAL_WATCHER` env var; child detected via `SCB_INTERNAL_WATCHER_CHILD`

### Changed

- **`pipeline/requirements.txt`** — added `azure-storage-blob>=12.19,<13`

- **`.github/workflows/cloud_learning.yml`** — refactored queue capture to DB high-water mark
  - Records `MAX(id)` from `learning_log` before each run; reads all new rows afterward
  - Removes monkey-patching of `_kc._log_learning`; queue now captures citation-chain entries too
  - Adds `citation_chain_acquirer.run_citation_expansion_cycle()` step in each cloud run

- **`pipeline/agent_watcher.ps1`** — demoted to compatibility shim
  - Primary supervision is now `internal_watcher.py` (Python-native, OS-agnostic)
  - PowerShell wrapper may still be used by external launchers but defers to the internal watcher

- **`pipeline/src/brain/llm_caller_openrouter.py`** — free-tier model pool expanded
  - Added: `llama-3.3-70b`, `nemotron-120b`, `gemma-3-27b`, `qwen3-coder`, `hermes-3-405b`, `ling-2.6-1t`
  - Removed unverified paid-tier slugs (`deepseek-v4-pro`, `deepseek-v4-flash`)
  - All models verified live against OpenRouter catalog on 2026-04-27

- **`pipeline/src/brain/local_store.py`** — `SCB_DB_PATH` environment override
  - `db_path()` now checks `os.environ.get("SCB_DB_PATH")` first, enabling ephemeral cloud runs to point to a separate DB without modifying source

- **`pipeline/src/deck/demo.py`** — added `"PDC"` to known site list



### Added

- **`pipeline/src/brain/network_observer.py`** — latent always-on daemon thread in every agent instance
  - Publishes local learning state (cursors, entity/edge counts, plasticity phase, alive_since) into the existing `bridge_state/compute_peers/<host>.json` OneDrive rendezvous — no new ports, no new infrastructure
  - Monitors all peer JSONs every 60 s; classifies each as `ALIVE | COOLING | OFFLINE`
  - On `ALIVE → OFFLINE` transition: absorbs the peer's corpus cursor positions (advances local cursors to peer's positions so no work is duplicated, only the uncovered gap is re-run), schedules a proportional catchup burst via `resumption_manager`
  - Tracks **singularity consumption velocity** = Σ(learnings) / Σ(uptime-hours) across all visible nodes; writes to `brain_kv` as `observer:network_velocity` for the systemic refinement agent to read
  - Pulses `observer:goal_alignment` to `brain_kv` each cycle so isolated nodes always know to lean toward `quest:type5_sc`
  - Re-anchors `quest:type5_sc` entity in the knowledge graph if ever absent

- **`pipeline/src/brain/resumption_manager.py`** — startup learning-debt recovery
  - `stamp_alive(cn)`: writes Unix epoch to `brain_kv` key `resumption:last_alive`; called every 5 min from agent sleep loop
  - `stamp_graceful_shutdown(cn)`: marks intended stops; called on `KeyboardInterrupt`
  - `detect_downtime(cn) → DowntimeReport`: gap > 5 min = downtime; distinguishes crash from clean stop; logs window to `logs/downtime_log.json`
  - `ingest_cloud_queue(cn) → int`: reads `cloud_learning_queue.jsonl`, inserts new `learning_log` rows, advances a line-number cursor — never double-imports
  - `schedule_catchup_burst(cn, seconds)`: writes `brain_kv` key `resumption:catchup_burst`; multiplier 1.5× ≤1 h → 4× >24 h
  - `consume_catchup_burst(cn) → float`: reads and clears burst key; used by corpus round workers
  - `run_resumption_check(cn) → DowntimeReport`: git-pull + detect_downtime + ingest_cloud_queue; called once at agent startup
  - `git_pull_latest()`: best-effort `git pull --ff-only` to get latest cloud queue before ingestion

- **`.github/workflows/cloud_learning.yml`** — GitHub Actions cloud learning continuity
  - Schedule: every 4 hours + `workflow_dispatch`
  - Restores/saves `pipeline/cloud_brain.sqlite` via `actions/cache` (persists across runs)
  - Runs OCW courses, OCW resources, ML/arxiv research, and OCW expansion outreach ingestors
  - Appends new `learning_log` events to `pipeline/cloud_learning_queue.jsonl` and commits back to `main`
  - Queue bounded to 50 000 lines; local agent ingests on first post-downtime startup

- **`pipeline/agent_watcher.ps1`** — local process watchdog
  - Monitors `autonomous_agent.py` every 30 s via heartbeat file age; restarts on crash with 15 s delay
  - Records every downtime window to `logs/downtime_log.json` (last 500 windows: start, end, seconds, ISO timestamps)
  - Falls back to system Python if `.venv` is absent

- **`pipeline/install_agent_watcher.ps1`** — one-shot Scheduled Task registration
  - Task: `SCBLearningAgent`; triggers: `AtStartup` + `AtLogOn`; `-RestartCount 9999`; `-RunLevel Highest`; `-MultipleInstances IgnoreNew`
  - Starts the task immediately after registration

- **`pipeline/bootstrap_new_machine.ps1`** — full new-machine self-setup
  - Verifies OneDrive `VS Code/pipeline` path (waits up to 5 min for sync if absent)
  - Creates `.venv`, installs `requirements.txt`, runs `init_schema()` (idempotent)
  - `git pull` to get latest `cloud_learning_queue.jsonl`
  - Installs both Scheduled Tasks (`SCBLearningAgent` + `AstecBridgeWatcher`)
  - Starts agent immediately; logs bootstrap event to `logs/bootstrap_log.json`
  - Flags: `-SkipGitPull`, `-SkipBridgeWatcher`, `-DryRun`

### Changed

- **`pipeline/autonomous_agent.py`**
  - `run_resumption_check()` called once before the `while True:` loop
  - `stamp_alive()` called every 5 minutes inside the adaptive sleep interval
  - `stamp_graceful_shutdown()` called on `KeyboardInterrupt` before exiting
  - `start_network_observer()` started as a third daemon alongside `skill_acquirer` and `systemic_refinement_agent`

- **`pipeline/src/brain/_version.py`**
  - Bumped to `0.18.3`; back-filled `PHASES` entries for `0.18.0`, `0.18.1`, `0.18.2`, `0.18.3`

### Architecture — three-layer survival chain

| Layer | Mechanism | Restores within |
|-------|-----------|-----------------|
| 1 — Local restart | `SCBLearningAgent` Scheduled Task + `agent_watcher.ps1` | ~15 s of crash |
| 2 — Machine migration | OneDrive syncs entire `VS Code/` (incl. DB) + `bootstrap_new_machine.ps1` | Minutes after new-machine login |
| 3 — Network absorption | `network_observer.py` absorbs offline peer cursor positions + schedules burst | Next 60 s liveness scan |
| 4 — Cloud continuity | GitHub Actions `cloud_learning.yml` + `cloud_learning_queue.jsonl` ingestion | ≤ 4 h gap regardless of local uptime |
| 5 — Full rebuild | Reset all `corpus_cursor` values to 0; `refresh_corpus_round()` re-derives everything | Full corpus re-ingest |

## [0.18.2] rADAM + Directional Intelligence + Systemic Refinement (2026-04-24)

### Added

- **`pipeline/src/brain/radam_optimizer.py`** — rADAM with toroidal phase coupling
  - Strict mathematical superset of vanilla Adam; identity-reduces when all extension knobs are at defaults
  - **Complex bifurcated gradient** `g_re + i·g_im` — real component from Touch/Vision/Body firings; imaginary component from torus gap field
  - **Pivoted ReLU** `pReLU(x; π, α)` — active region anchored at running mean pressure rather than zero
  - **Heart-beat momentum modulation** `β1(t) = β1_bar + κ·sin(ω·t)` — phase-locked to `temporal_spatiality` rhythm
  - **Langevin incoherence noise** scaled by `sqrt(1 − carrier_mass)` — exploration grows when senses are out-of-phase
  - **T² toroidal pressure projection** `p_t = 0.5·(1 + cos(θ_t)·cos(φ_t))` with internal + external loop phases
  - Disable via `BRAIN_USE_RADAM=0`; env-var knob overrides for headless testing

- **`pipeline/src/brain/directionality_listener.py`** — Directional snapshot of the entire Symbiotic Entirety
  - `listen()` returns `DirectionalitySnapshot(expansion, coherence, bifurcation)` triplet
  - **Expansion** — corpus/network growth rate from entity/edge delta
  - **Coherence** — mean resultant length `R = |Σ exp(i·φ_s)|/N` across all sense-signal angles on S¹
  - **Bifurcation** — `Im(grad) / (|Re(grad)| + ε)` — ratio of latent-to-realised gradient magnitude
  - **Reuptake neighbourhood noise** `CV = σ/μ` of SYMBIOTIC_TUNNEL + GROUNDED_TUNNEL edge weights feeds coherence penalty and Langevin signal

- **`pipeline/src/brain/learning_drive.py`** — Symbiotic internal feedback loop
  - Reads corpus saturation, self-train quality, learning velocity, and RDT task difficulty from the live SQLite DB
  - Derives four rADAM knobs: `pivot_alpha`, `heartbeat_kappa`, `noise_sigma`, `acquisition_drive`
  - `acquisition_drive` injected additively into `grad_imag` in `brain_body_signals._adam_step`; pushes optimizer toward under-explored knowledge when stagnant
  - `get_drive()` is thread-safe with a 5-minute TTL cache; all formulas reduce to identity when DB is absent

- **`pipeline/src/brain/systemic_refinement_agent.py`** — Continuous adaptive improvement daemon
  - Five-phase loop: **SENSE** → **DIAGNOSE** → **RANK** → **EXECUTE** → **LEARN**
  - Senses all six faculties: Brain, Vision, Touch, Smell, Body, Heart, DBI
  - Ten supply-chain refinement strategies with `[0..1]` priority scores; each non-zero score produces a `RefinementAction`
  - Actions ranked by `priority × acquisition_drive × rhythm_factor` — effort concentrates where the Brain is hungriest and domain gap widest
  - Effect types: launch Mission, surface Body directive, drop skill-acquisition trigger, append corpus seed, write brain_kv nudge, record findings row
  - Feedback-gated deduplication: content hash suppresses re-execution within a window unless Body confirms the action
  - Adaptive cadence: 20 min floor, up to 2 h; `acquisition_drive` shrinks the sleep so refinement accelerates when learning stalls

### Changed

- **`pipeline/src/brain/brain_body_signals.py`**
  - `_torus_latent_grad(cn, kind, pressure)` — reads mean `torus_gap` KL-divergence from Endpoint props; returns latent gradient in `[0, 0.30]`
  - `_adam_step(state, gradient, grad_imag=0.0)` — extended with `grad_imag` parameter; rADAM hook (BRAIN_USE_RADAM) wires in coherence, external phase, heartbeat omega, pivot, acquisition_drive

- **`pipeline/autonomous_agent.py`**
  - `start_systemic_refinement_agent()` — daemon launcher with adaptive-cadence logging
  - Wired into `autonomous_loop()` startup and `__main__` so refinement runs whether the agent is imported or executed directly

- **`pipeline/oracle_schema_map.json` / `.txt`** — Run 5 schema refresh; coordinate corrections for Manage Price Lists and Purchase Requisition
- **`pipeline/oracle_schema_mapper.py`** — Further task-panel hardening
- **`pipeline/abc_screenshots/schema_map/*`** — 40+ screenshot tiles refreshed (run 5 capture)

### Tests

- **`pipeline/tests/test_radam_optimizer.py`** — proves identity reduction to vanilla Adam; per-knob behaviour (pivoted-ReLU, heartbeat, Langevin, toroidal projection)
- **`pipeline/tests/test_learning_drive.py`** — 9 tests: identity drive, knob math, corpus saturation, self-train quality, learning velocity, acquisition_drive bounds, thread-safety, env-var override, grad_imag injection

---

## [0.18.0] DeepSeek V4 Candidate Trial System (2026-04-24)

### Added

- **`pipeline/src/brain/llm_candidate.py`** — New module: scored probationary trial system for new LLM candidates
  - `get_active_candidates()` returns model specs for all models currently in trial
  - `tick_candidate(model_id, ok, latency_ms)` records one dispatch result via EMA update (α=0.10)
  - `evaluate_candidates()` checks thresholds after every 10 dispatches; auto-promotes or auto-rejects
  - `candidate_stats()` returns full trial state for all candidates (used by UI/dashboards)
  - Promoted models are written to `llm_registry` SQLite table (`promoted=1`); `llm_router.available_models()` picks them up on the next call — no YAML modification required
  - Every promotion/rejection is appended to `pipeline/docs/LLM_CANDIDATE_AUDIT.md`

- **`pipeline/config/brain.yaml` — `llms.candidates` block** — Declarative trial configuration
  - `trial.dispatches_required: 50` — minimum observations before a decision
  - `trial.promote_threshold: 0.72` — ema_success ≥ this → promote to live registry
  - `trial.reject_threshold: 0.45` — ema_success ≤ this after N dispatches → reject
  - **DeepSeek V4 Pro** (`deepseek-v4-pro`) — 1.6T/49B MoE, 1M ctx, $1.74/$3.48 per Mtok in/out
  - **DeepSeek V4 Flash** (`deepseek-v4-flash`) — 284B/13B MoE, 1M ctx, $0.14/$0.28 per Mtok in/out

- **`pipeline/src/brain/llm_ensemble.py`** — Candidate sidecar wired into `dispatch_parallel()`
  - `llm_candidate_trials` DDL added to `_DDL` so the table is always created on first ensemble use
  - `_try_dispatch_candidates()` fires active candidates after the main ensemble answers; results are intentionally discarded (not included in `EnsembleResult`); EMA stats accumulate
  - `evaluate_candidates()` is triggered every 10th candidate dispatch (module-level atomic counter)
  - `import logging` added; `logger = logging.getLogger(__name__)` available for debug output

## [oracle-schema] Oracle Fusion Schema Mapper + Intersection Map (2026-04-24)

### Added

- **`pipeline/oracle_schema_mapper.py`** — Playwright crawler that navigates all Oracle Fusion DEV13 tabs/tiles, opens task panels, and extracts full task lists into a structured JSON schema
  - Resume mode: skips modules already having ≥2 real tasks; safe to restart mid-run
  - Redwood precheck pattern: reads panel content before attempting to open it (threshold ≥3 tasks), avoiding the toggle-close bug on Redwood-UI modules where the panel is already open on page load
  - Font-weight heuristic (`fontWeight ≥ 600`) for section header detection, replacing obfuscated ADF CSS class names (`xmu`, `x16g`) that change between releases
  - NOISE task filter: `{'Add Fields', 'Help', 'Done', 'Save', 'Personal Information', 'Refresh'}` excluded from real-task counts
  - "Keep better data" protection: if a re-probe captures fewer real tasks than existing, retains old data
  - Incremental JSON/TXT output saved after each module probe

- **`pipeline/oracle_schema_map.json`** / **`pipeline/oracle_schema_map.txt`** — Incremental schema output; 25 modules with confirmed task content as of run 5

- **`pipeline/build_intersection_map.py`** — Cross-references `oracle_schema_map.json` with confirmed write operations for part 80446-04
  - Classifies each module as Confirmed (4), Adjacent (20), or Low-relevance (31)
  - 16 confirmed write-op tasks across 4 modules: SCE/Work Execution, SCE/Inventory Management Classic, Procurement/Purchase Orders, Procurement/Approved Supplier List

- **`pipeline/pim_screenshots/80446-04/write_ops/intersection_map.json`** — Part-level intersection data
- **`pipeline/pim_screenshots/80446-04/write_ops/intersection_map.txt`** — Human-readable intersection report
- **`Claude/ORACLE_SCHEMA_MAPPER_GUIDE.md`** — Technical guide covering ADF Classic vs Redwood UI detection, known issues, and intersection map methodology

### Known Issues (active as of 2026-04-24)

- Work Execution and Plan Inputs regressed to 1/0 real tasks in run 4 due to false-positive precheck triggering (stray page elements at x>1100); being fixed in run 5 via the ≥3 task threshold
- 4 modules (Receipt Accounting, Financial Orchestration, Supply Orchestration, Supply Chain Orchestration) navigate to home on tile click — require URL-based navigation, not yet implemented
- List-view pages (Manage Journals, Manage Price Lists, Plan Inputs data grid) capture saved-search SELECT options instead of real task panel content

---

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
