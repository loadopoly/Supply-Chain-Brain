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

Every research-derived view carries an in-page citation footer back to its
originating MIT CTL lab.

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
│  ├─ bench_brain.py          # synthetic-data benchmark suite
│  └─ results/                # CSV history + latest.csv
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

```bash
python -m bench.bench_brain --rows 20000 --repeats 3
```

Deck smoke test:

```bash
python pipeline.py deck --demo
```

Live deck render:

```bash
python pipeline.py deck
```

Then open the **⚡ Benchmarks** page in the app to see the latest run.
On the dev box (Windows · Python 3.14 · pandas 3.0 · sklearn 1.8) the full
18-benchmark suite runs in **≈ 5 seconds** end-to-end at 20k rows.

---

## Documentation

- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** — module map + data flow
  + drill-down mechanics.
- **[RESEARCH.md](docs/RESEARCH.md)** — MIT CTL lab → module mapping with
  math + citations.
- **[CONFIG.md](docs/CONFIG.md)** — every `brain.yaml` key explained.
- **[RUNBOOK.md](docs/RUNBOOK.md)** — install / run / troubleshoot.
- **[CHANGELOG.md](CHANGELOG.md)** — phase-by-phase deliverable log.
