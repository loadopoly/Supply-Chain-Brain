# Supply Chain Brain

> **Streamlit-native, modular Supply Chain "brain" for procurement, logistics,
> supply chain, and customer service** — built on top of the existing
> `EDAP Query Console` and grounded in research from the **MIT Center for
> Transportation & Logistics** (CTL).

The application gives a single analyst surface to:

- **Search** any value across Azure SQL replica + Oracle Fusion (Phase-0 console).
- **Drill down** from any cell or graph node into context, history, related
  parts/suppliers/POs/orders/customers, and *decisions* logged from other pages.
- **Find missing data** that would most improve the next decision
  (Value-of-Information from XGBoost / LightGBM / CatBoost + MissForest).
- **Optimize EOQ** with a Bayesian-Poisson centroidal-deviation engine that
  re-ranks itself after every resolution via a contextual bandit.
- **Recursively cluster OTD** — port of the user's own algorithm, wired off
  Excel and onto the Replica DB with shared cleaning across the entire app.
- **Procurement 360** — multi-dimensional graph + CVaR Pareto + causal-forest
  attribution surfaces *Lead Time, DIO, Planned Obsolescence, Vendor
  Engagement, Shared Vendors, Shared Parts*.
- **Run MIT-CTL-grounded research** — bullwhip diagnostic, Cox/KM lead-time
  survival, Graves–Willems multi-echelon safety stock, Scope-3 emissions,
  smart freight portfolio (contract / spot / mini-bid).
- **What-if sandbox** — clone state, mutate, replay every KPI, diff baseline.
- **Cross-app bus** — HMAC-signed webhooks to/from
  [IPS Freight Platform](https://ips-freight-api.onrender.com/dashboard)
  and other peer apps.
- **Add a new database** = add 6 lines to `config/brain.yaml`.

| | |
|---|---|
| **Version** | see `src/brain/_version.py` |
| **Python**  | 3.10+ (validated on 3.14)   |
| **Stack**   | Streamlit · pandas · NumPy · SciPy · scikit-learn · NetworkX · plotly |
| **Optional**| XGBoost / LightGBM / CatBoost · lifelines · scikit-survival · econml · neo4j · gremlinpython |

---

## Quickstart

```bash
# 1) create venv (Windows shown; macOS/Linux is identical with python3 -m venv .venv)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2) install
pip install -r requirements.txt
# (optional richer analytics)
pip install xgboost lightgbm lifelines scikit-survival

# 3) launch
streamlit run app.py
# → http://localhost:8501

# 4) optional: render the Cross-Dataset Supply-Chain Review deck from the attached databases
python pipeline.py deck --out snapshots/cross_dataset_review_live.pptx --json snapshots/cross_dataset_review_live.json

# 5) optional: render the same deck from synthetic data
python pipeline.py deck --demo --out snapshots/cross_dataset_review_demo.pptx --json snapshots/cross_dataset_review_demo.json

# 6) optional: run the analytics benchmark suite
python -m bench.bench_brain --rows 20000
```

> **Already running on 8501?** A second instance can be launched on
> any other port: `streamlit run app.py --server.port 8502`.
> See **`docs/RUNBOOK.md`** for the full operational runbook.

---

## Pages

| # | Page | Lab grounding |
|---|---|---|
| 0 | 🔬 Schema Discovery     — live INFORMATION_SCHEMA browser + column explorer   | —                |
| 1 | 🧠 Supply Chain Brain  — overview + interactive graph (human-readable labels) | CAVE             |
| 2 | 📦 EOQ Deviation        — Bayesian-Poisson centroid + LinUCB bandit           | Deep Knowledge   |
| 3 | 🚚 OTD Recursive        — recursive cluster of OTD signal                     | Intelligent Logistics |
| 4 | 🏭 Procurement 360      — graph leverage + CVaR Pareto + causal forest        | SC Design + Deep Knowledge |
| 5 | 🧩 Data Quality         — VOI ranker + MissForest mass-impute                 | Digital SC       |
| 6 | 🔌 Connectors           — register databases / external apps                  | —                |
| 7 | ⏱️ Lead-Time Survival   — Kaplan-Meier + Cox PH                               | Intelligent Logistics |
| 8 | 🌊 Bullwhip             — Lee/Padmanabhan/Whang variance ratio                 | Intelligent Logistics |
| 9 | 🏗️ Multi-Echelon        — Graves-Willems guaranteed-service                   | Intelligent Logistics + SC Design |
| 10| 🌱 Sustainability       — GLEC Scope-3 freight emissions                      | Sustainable      |
| 11| 🚚 Freight Portfolio    — contract/spot/mini-bid + goldfish + ghost-lane       | FreightLab       |
| 12| 🧪 What-If              — snapshot · mutate · replay · diff                   | SC Design + CAVE |
| 13| 📒 Decision Log         — provenance for every recommendation                  | Digital SC       |
| 14| ⚡ Benchmarks           — performance dashboard for the analytics core         | —                |
| 15| 🗺️ Quest Console        — Brain-Driven mission launcher (NL query → living PPTX) | SC Design      |

Every research-derived view carries an in-page citation footer back to its
originating MIT CTL lab.

---

## Brain Quest Engine (v0.14)

The **Quest Console** (page 15) turns a free-form supply-chain question into a
*living mission* — a persistent record that the autonomous agent refreshes on
every cycle.

```
User NL query
    ↓  intent_parser.parse()          keyword fallback when LLM is offline
ParsedIntent (site, scope_tags, …)
    ↓  mission_store.create_mission() SQLite row in findings_index.db
Mission (id, status=open, …)
    ↓  orchestrator.BrainOrchestrator.run()
MissionResult (findings, kpi_snapshot, progress_pct, …)
    ├─ viz_composer.compose()         → 6 Plotly figures
    ├─ schema_synthesizer.synthesize() → EntitySchema + Mermaid ER
    ├─ one_pager.render_one_pager()   → snapshots/missions/<id>/one_pager.pptx
    └─ implementation_plan.render_implementation_plan() → snapshots/missions/<id>/implementation_plan.pptx
```

### Quest taxonomy

Eight closed-vocabulary **scope tags** route every query to the right analyzer:

| Scope tag          | Owner    | Analyzer              |
|--------------------|----------|-----------------------|
| `fulfillment`      | Planner  | OTD recursive cluster |
| `inventory_sizing` | Planner  | EOQ deviation         |
| `sourcing`         | Buyer    | Procurement 360       |
| `lead_time`        | Buyer    | PO receipts survival  |
| `data_quality`     | Quality  | VOI / imputation      |
| `demand_distortion`| Planner  | Bullwhip ratio        |
| `network_position` | Ops      | Multi-echelon SSS     |
| `cycle_count`      | Ops      | ABC / cycle count     |

### Artifacts

Both PPTX artifacts are regenerated on every refresh and stored at:

```
pipeline/snapshots/missions/<mission_id>/
    one_pager.pptx          8.5×11 portrait  — executive 1-pager
    implementation_plan.pptx  16:9 landscape — 9-slide implementation deck
```

The autonomous agent (Step 3g) automatically refreshes all open missions on
every heartbeat cycle.

---

## Layout

```
pipeline/
├─ app.py                     # main console + sidebar nav
├─ pages/                     # auto-discovered Streamlit pages 1–14
├─ src/
│  ├─ connections/            # azure_sql + oracle_fusion drivers
│  ├─ deck/                   # CrossDataset_Agent_Process_Spec deck pipeline + PPTX renderer
│  └─ brain/                  # analytics + integration core (no Streamlit imports)
│     ├─ research/            # MIT-CTL-grounded modules
│     ├─ _version.py          # __version__ source of truth
│     ├─ db_registry.py       # pluggable connector contract (health-check ping, 120 s timeout)
│     ├─ data_access.py       # safe read-SQL → DataFrame
│     ├─ demo_data.py         # live-only auto-loader + inline schema diagnostics
│     ├─ schema_introspect.py # pattern-match physical → logical columns
│     ├─ col_resolver.py      # INFORMATION_SCHEMA discovery; 14 semantic role patterns
│     ├─ label_resolver.py    # human-readable dim-table label enrichment (enrich_labels)
│     ├─ cleaning.py          # one cleaning pipeline shared across pages
│     ├─ eoq.py               # EOQ + Bayesian-Poisson + LinUCB ranker
│     ├─ otd_recursive.py     # recursive OTD clustering (replica-wired)
│     ├─ graph_context.py     # NetworkX MultiDiGraph + leverage helpers (label-enriched)
│     ├─ graph_backend.py     # NetworkX | Neo4j | Cosmos Gremlin behind one API
│     ├─ imputation.py        # missingness + VOI + MissForest
│     ├─ ips_freight.py       # IPS Freight Platform connector
│     ├─ findings_index.py    # SQLite cross-page memory + decision log
│     ├─ drilldown.py         # row-selection + citations + page header
│     ├─ whatif.py            # snapshot / mutate / replay
│     ├─ cross_app.py         # HMAC outbound webhooks + verify
│     ├─ analytics_fact.py    # nightly fact-table builder
│     └─ auth.py              # session identity (opt-in)
├─ bench/
│  ├─ bench_brain.py          # synthetic-data benchmark suite (analytics core)
│  ├─ bench_quest_engine.py   # Quest Engine benchmark suite
│  └─ results/                # CSV history + latest*.csv
├─ tests/
│  ├─ conftest.py             # shared fixtures (stub_llm, mission_factory, synth_result)
│  ├─ test_quests.py          # Quest taxonomy unit tests
│  ├─ test_intent_parser.py   # Intent parser unit tests (keyword fallback)
│  ├─ test_mission_store.py   # Mission store integration tests
│  ├─ test_quest_engine.py    # Orchestrator + viz + mission_runner tests
│  └─ test_deck.py            # Deck builder unit tests (PPTX + kaleido fallback)
├─ pytest.ini                 # pytest configuration
├─ config/brain.yaml          # single source of truth for tables/columns/defaults
├─ docs/
│  ├─ ARCHITECTURE.md
│  ├─ RESEARCH.md             # MIT CTL labs → modules + math
│  ├─ CONFIG.md
│  └─ RUNBOOK.md
├─ requirements.txt
├─ requirements.pinned.txt    # version-bounded, validated together
└─ CHANGELOG.md
```

---

## Add a new database

```yaml
# config/brain.yaml
connectors:
  my_new_warehouse:
    kind: azure_sql            # or "oracle_fusion" | "http_api"
    server: mywh.database.windows.net
    database: prod
    auth: ActiveDirectoryInteractive
```

Restart Streamlit — the connector appears in the **Connectors** page and any
SQL-driven module can use it via `data_access.query_df("my_new_warehouse", sql)`.

## Cross-app sharing with IPS Freight

```yaml
cross_app:
  subscribers:
    - name: ips_freight
      url: https://ips-freight-api.onrender.com/webhook
      secret_env: IPS_FREIGHT_SHARED_SECRET
      events: [eoq.deviation, otd.cluster, freight.ghost_lane]
```

Outbound payloads are HMAC-SHA256-signed; inbound use `cross_app.verify()`.

---

## Testing & benchmarking

### pytest suite

```bash
# All non-slow tests (~12 s):
.venv\Scripts\python.exe -m pytest tests\ -m "not slow" -v

# Full suite including mission_runner end-to-end (~45 s):
.venv\Scripts\python.exe -m pytest tests\ -v

# Single module:
.venv\Scripts\python.exe -m pytest tests\test_quests.py -v
```

**Test structure:**

| File | Marks | Tests |
|---|---|---|
| `tests/test_quests.py` | `unit quest` | Quest taxonomy, scope tags, IDs |
| `tests/test_intent_parser.py` | `unit quest` | Keyword fallback, closed-vocab enforcement |
| `tests/test_mission_store.py` | `integration quest` | CRUD, progress clamping, events, cleanup |
| `tests/test_quest_engine.py` | `unit/integration quest` | Schema synthesizer, viz composer, orchestrator, mission_runner |
| `tests/test_deck.py` | `unit quest` | One-pager + impl-plan creation, kaleido fallback |

### Benchmarks

**Analytics core** (EOQ, OTD, bullwhip, graph, …):
```bash
.venv\Scripts\python.exe -m bench.bench_brain --rows 20000 --repeats 3
```

**Quest Engine** (intent_parser, mission_store CRUD, schema_synthesizer, viz_composer):
```bash
.venv\Scripts\python.exe -m bench.bench_quest_engine --rows 100 --repeats 3
```

Both write CSV results to `bench/results/` (timestamped + `latest*.csv`).
Open the **⚡ Benchmarks** page in the app to visualise the latest run.

On the dev box (Windows · Python 3.14 · pandas 3.0 · sklearn 1.8):
- Core bench 18 benchmarks ≈ **5 s** at 20k rows
- Quest Engine bench ≈ **3 s** at 100 rows

---

## Documentation

- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** — module map + data flow
  + drill-down mechanics.
- **[RESEARCH.md](docs/RESEARCH.md)** — MIT CTL lab → module mapping with
  math + citations.
- **[CONFIG.md](docs/CONFIG.md)** — every `brain.yaml` key explained.
- **[RUNBOOK.md](docs/RUNBOOK.md)** — install / run / troubleshoot.
- **[CHANGELOG.md](CHANGELOG.md)** — phase-by-phase deliverable log.
