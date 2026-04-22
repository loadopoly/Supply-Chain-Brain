# Architecture

```
            ┌───────────────────────────────────────────────────────────┐
            │                      Streamlit UI                          │
            │   app.py  +  pages/1..14_*.py     (no business logic)      │
            └────┬───────────────────────────────────────────────┬──────┘
                 │ uses                                            │ uses
        ┌────────▼─────────┐                            ┌─────────▼─────────┐
        │ src.connections  │                            │      src.brain     │
        │  azure_sql       │                            │ analytics + cross  │
        │  oracle_fusion   │                            │ app + integration  │
        └────────┬─────────┘                            └─┬────────────────┬─┘
                 │                                        │                │
                 │ pyodbc / msal                         │  pandas / np / │
                 │                                        │  sklearn / nx  │
        ┌────────▼─────────┐    ┌──────────────────────┐ │                │
        │ Azure SQL replica│    │  Oracle Fusion DEV13 │ │                │
        │ edap-replica-cms │    │  REST + SAML         │ │                │
        └──────────────────┘    └──────────────────────┘ │                │
                                                         │                │
                                  ┌──────────────────────▼──────┐         │
                                  │ findings_index.db (sqlite)  │         │
                                  │  · findings   · decision_log│         │
                                  └─────────────────────────────┘         │
                                                                          │
                                  ┌───────────────────────────────────────▼────┐
                                  │        graph_backend (one API)              │
                                  │  NetworkX (default) │ Neo4j │ Cosmos Gremlin│
                                  └────────────────────────────────────────────┘
```

## Layers

### 1. Connections (`src/connections/`)

Concrete drivers. **No business logic.** They expose `get_connection()`
helpers used by `src.brain.db_registry`. Adding a new driver = adding a new
class here and a one-line entry in `bootstrap_default_connectors()`.

### 2. Brain (`src/brain/`)

Pure-Python analytics + integration. **Never imports Streamlit** so each
module is unit-testable and re-usable from CLI / batch jobs.

| Module               | Role                                                                           |
|----------------------|--------------------------------------------------------------------------------|
| `db_registry`        | `Connector` dataclass + factory + `bootstrap_default_connectors()`             |
| `data_access`        | `query_df()` and `fetch_table()` — defensive wrappers around `pd.read_sql`     |
| `llm_router`         | Free-tier LLM registry + capability-weighted model scoring & selection         |
| `llm_scout`          | Periodic internet scanner (HuggingFace/OpenRouter/lmarena) for new open LLMs   |
| `llm_ensemble`       | Parallel multi-LLM dispatch with online W&B learning (SGD + EMA telemetry)     |
| `llm_self_train`     | Bounded pipeline-grounded W&B updates with diversity & drift caps             |
| `compute_grid`       | Shared compute grid over piggyback fabric (peer discovery, TCP dispatch)       |
| `schema_introspect`  | Pattern-matches physical column names to logical roles (`lead_time`, `otd`, …) |
| `cleaning`           | One cleaning pipeline used by every page (trim / coerce / winsorize)           |
| `eoq`                | EOQ + Bayesian-Poisson centroidal deviation + LinUCB bandit                     |
| `otd_recursive`      | Recursive cluster of OTD signal — replica-wired port of user's algorithm        |
| `imputation`         | Missingness profile + VOI booster + MissForest mass-impute                      |
| `graph_context`      | NetworkX MultiDiGraph with `add_parts/add_suppliers/centrality/shared_*`        |
| `graph_backend`      | Same API across NetworkX / Neo4j / Cosmos Gremlin                              |
| `ips_freight`        | HTTP client for IPS Freight Platform + `ghost_lane_survival()`                  |
| `cross_app`          | HMAC-signed outbound webhooks + `verify()` for inbound                          |
| `whatif`             | snapshot → mutate → replay → diff sandbox                                       |
| `analytics_fact`     | Nightly denormalized fact-table builder (parquet → CSV fallback)                |
| `findings_index`     | SQLite store: `findings` (cross-page memory) + `decision_log` (provenance)      |
| `auth`               | Session identity picker (opt-in via `brain.yaml → auth.enabled`)                |
| `drilldown`          | Streamlit-side selection helpers + `CITATIONS` map + `page_header()`            |
| `_version`           | `__version__` source of truth                                                   |
| `research/*`         | MIT-CTL-grounded modules (see RESEARCH.md)                                      |

### 3. UI (`pages/`)

Each Streamlit page is a thin shell:

```python
1. bootstrap_default_connectors()             # ensure connectors are registered
2. df = data_access.query_df("azure_sql", sql)
3. results = brain.<some_module>.<some_fn>(df, ...)
4. drilldown.drilldown_table(results, key=...)# row-selection + cross-page links
5. drilldown.cite("freight_lab", ...)         # research provenance
```

## Distributed Multi-LLM Brain (Shared Compute Grid)

The system delegates NLP, categorization, and action evaluations to a purely zero-cost, open-weights distributed model ensemble. 

### Why It's Robust (Fault Tolerance & Consensus)
- **Infrastructure Fallbacks**: The `compute_grid` module reuses the existing OneDrive piggyback fabric (`wifi_ip.txt` rendezvous logic) to advertise workstation CPU/GPU capacity (NVIDIA, AMD, Intel). It uses a fast TCP pre-probe (< 1 second) and a negative cache to instantly skip offline peers. If the grid is unavailable, requests automatically fall back to the local device with zero TCP overhead, ensuring the pipeline never stalls.
- **Parallel Multi-LLM Consensus**: To mitigate hallucinations, API rate limits, or slow responses from any single model, `llm_ensemble.dispatch_parallel()` fans out the identical prompt to the top $K$ eligible zero-cost models concurrently.
- **Aggregators**: The parallel responses are evaluated by configurable aggregators (e.g., `weighted_softmax_vote` or `json_merge`) which enforce a consensus, effectively neutralizing bad outputs from any outlier model.

### Why It's Learning (Online Weight Updates)
The model ensemble actively learns which LLMs are best at specific tasks (e.g., `vendor_consolidation` vs. `fast_classify`).
- **Stochastic Gradient Descent (SGD)**: Using an online SGD-style update, models that produce a correct or highly-rated answer (scored by a `validator` function) receive a positive bump to their routing `weight` and `bias` stored in `local_brain.sqlite` (`llm_weights` table). Failed responses trigger a downward nudge.
- **L2 Regularization**: A background decay mechanism applies L2 regularization to prevent any single model's influence from exploding to infinity, retaining a competitive, diverse ensemble.
- **EMA Telemetry Tracking**: Even without an explicit correctness validator, the ensemble continuously tracks Exponential Moving Averages (EMA) for success rate (`ema_success`) and response latency (`ema_latency`). The router uses this telemetry to automatically deprioritize models that begin timing out or producing malformed JSON.

As the `llm_scout` continuously discovers hundreds of new open-source models online, this learning loop automatically weeds out the underperformers and promotes the fastest, most capable models for the specific demands of the Supply Chain Brain.

### Bounded Self-Training (Pipeline-Grounded Refinement)

The Brain *also* uses its own supply-chain data pipeline as a soft validator to refine ensemble weights — but only within explicit boundaries that preserve fluidity for multi-echeloned reasoning and dynamic interpretations on tasks the pipeline has no ground truth for.

- **Whitelist Isolation (`llms.self_train.tasks`)**: only tasks with a declared `(table, key_column, value_column)` reference (currently `vendor_consolidation` → `part_category`, `otd_classify` → `otd_ownership`) ever receive pipeline-driven updates. Generative, narrative, what-if, and multi-echelon reasoning tasks keep pure router-prior dynamics — the data pipeline cannot leak into them.
- **Bounded SGD**: `llm_self_train.self_train_round()` mines recent `llm_dispatch_log` rows whose live validator was NULL, scores each contributor's response against the whitelisted ground-truth column, and applies updates with a halved learning rate (`lr_scale=0.5`) and a per-round weight delta clamp (`drift_cap=0.5`). One mining round can never catastrophically rewrite the ensemble.
- **Diversity Guard**: after each round, any model whose share of total weight mass on a task exceeds `max_share_per_task` (0.50) is dampened back toward parity by `dampen_factor` (0.85). Any model below `min_weight_floor` (0.10) is re-floored. The ensemble stays plural by construction.
- **Exploration Reserve**: `dispatch_parallel` bypasses the learned weights entirely on `exploration_reserve` (0.15) of ALL dispatches and uses the pure router prior instead. This keeps newcomer models discovered by the scout and underdogs the validator can't easily judge permanently reachable, preserving the ensemble's ability to surface unexpected interpretations.
- **Audit Trail**: every round writes a row to `local_brain.sqlite.llm_self_train_log` recording matched samples, drift-capped count, dampened/floored model lists, and notes for full provenance.

The net effect: the Brain learns from what it provably knows (its own data) without forgetting how to reason about what it doesn't.

## Drill-down mechanics

`drilldown.drilldown_table(df, key=...)` returns the user-selected rows.
The selected rows are passed into `record_finding()` → SQLite. Every other
page calls `lookup_findings(kind=...)` on render to pull the cross-page
context. This is what enables:

- Click an OTD cluster → jump to Procurement 360 filtered to that
  supplier.
- Click an EOQ deviation → jump to Lead-Time Survival for that part's
  supplier-lane.
- Click any node in the Brain graph → context panel opens with all
  recorded findings for that node.

`record_finding()` is extremely cheap (one SQLite INSERT). The benchmark
suite measures bulk-write at ~40 ms / 2k rows.

## Decision provenance

`findings_index.log_decision(page, action, ...)` writes to the
`decision_log` table every time the application offers a recommendation
or the user records an action. The **Decision Log** page renders this in
chronological order. This realizes the Digital SC Lab's "trust" pillar:
every automated recommendation is auditable end-to-end.

## Pluggable graph backend

`graph_backend.get_graph_backend()` reads `brain.yaml → graph.backend`
and returns one of:

- **`networkx`** — in-process MultiDiGraph (default; zero infra).
- **`neo4j`**    — bolt+auth, optional package `neo4j`.
- **`cosmos_gremlin`** — Azure Cosmos DB Gremlin, optional `gremlinpython`.

Pages **never** import these libraries directly; they only call methods on
`GraphBackend`.

## What-if sandbox

`whatif.create_snapshot(name, mutations)` clones the relevant slices of the
analytics fact table, applies the mutations (e.g., consolidate suppliers,
shift lead times, double demand), then re-runs the KPI suite and stores the
diff against baseline in `findings_index`. Surfaced via the **What-If**
page with a side-by-side baseline-vs-scenario diff.

## Cross-app integration

`cross_app.emit(event, body)` HMAC-SHA256-signs a payload using
`secret_env` per subscriber and POSTs to each subscriber URL with header
`X-SCBrain-Signature`. Inbound webhooks should call `cross_app.verify()`.

The IPS Freight Platform is the canonical first peer; its dashboard URL
is `https://ips-freight-api.onrender.com/dashboard`.

## Value Stream Mapping (Added in 0.5.0)
The app integrates MIT SCALE value stream bottleneck detection. 
The core data access (src.brain.data_access) integrates POs, WOs, and SOs into a cohesive Force-Directed graph via NetworkX (with Neo4j/Gremlin support planned). Value streams identify friction bottlenecks using:
1. due_date_key delays on Purchase & Work Orders.
2. promised_ship_day_key delays on Sales Orders.
Centrality scoring exposes nodes scaling with high supply chain friction.

