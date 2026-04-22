# Supply Chain Brain — Function & Schema Intersection Guide

> **Purpose:** A single, authoritative companion document that traces every
> function, every schema, and every data dependency from raw replica tables →
> brain modules → MIT CTL research modules → Streamlit pages → PowerPoint deck output.
>
> **Use this doc when:**
> - A page or slide is empty / shows "Unknown" / shows ~0%
> - You add a new column to the replica and need to know which downstream consumers light up
> - You are debugging the PPTX render pipeline (`pipeline.py deck …`)
> - An agent (or human) misattributes a missing value to a code bug instead of a schema gap
>
> **Last updated:** 2026-04-21 · Scope: `pipeline/` package (v0.4.6+)

---

## 1. Confirmed Schema Gaps (Root Cause of Recent PPTX Issues)

The four issues raised against the recent agent-generated PowerPoint are **not
code bugs** — they are **upstream schema gaps** in the Azure SQL replica. The
deck-builder is correctly degrading to safe defaults. Each row below maps the
visible symptom to the actual missing data and the consumers that go dark.

| # | Symptom in PPTX | Real Root Cause | Where the gap lives | Fallback in code | Slides / pages affected |
|---|-----------------|-----------------|---------------------|------------------|-------------------------|
| 1 | "Late-line root cause: **Unknown / not captured**" | `failure_reason` column missing on `fact_sales_order_line` | `edap_dw_replica.fact_sales_order_line` (Oracle source: `DOO_LINES_ALL.LATE_REASON_CODE`) — flagged as a gap in `docs/DATA_DICTIONARY.md` | `src/deck/live.py` returns the literal string `"Unknown / not captured"` when null/absent | Slide: *OTD Failure Pareto*; Page 3 (`OTD_Recursive`); Phase 3a (`src/deck/findings.py`) |
| 2 | Cycle-count slides empty (no rows) | No `fact_cycle_count` table in the replica (Oracle source: `INV_CYCLE_COUNT_ENTRIES`) | Discovered dynamically at runtime in `src/deck/live.py` (`discover_cycle_count_table`); not in `config/schema_cache.json` | When discovery returns nothing, ITR / accuracy frames are returned empty | Slides: *Cycle-Count Cadence*, *ITR Accuracy*; Phase 5b (`findings.py`) |
| 3 | IFR ≈ 0 % across the board | No point-in-time inventory snapshots — only the **current** state of `fact_inventory_on_hand` exists | `edap_dw_replica.fact_inventory_on_hand` is overwritten daily; no history table | `src/deck/live.py` computes IFR using *current* on-hand against *historical* orders → almost always 0 % | Slides: *Inventory Fill Rate Trend*, *IFR by ABC*; Page 2 (`EOQ_Deviation`), Phase 3b |
| 4 | ABC analysis missing / part class blank | `sales_part_code` and `inventory_part_code` not populated on `dim_part` | `edap_dw_replica.dim_part` (eDap should publish; currently NULL) | Falls back to Oracle `ABC Inventory Catalog` category if reachable, else NULL | Slides: *ABC Cycle-Count Cadence*, *Centrality by Part Class*; Pages 1, 2, 3 |

### What this means for the agent that built the PPTX
The agent **was not wrong** to render those slides — it correctly emitted the
canonical safe defaults defined in `src/deck/schemas.py`. The fix is **not**
in the deck builder; it is in the upstream replica ETL.

---

## 2. Layered Architecture — End-to-End Data Flow

```
                ┌─────────────────────────────────────────┐
                │ ORACLE FUSION (DEV13) · BIP / FSCM REST │
                └────────────────┬────────────────────────┘
                                 │  (nightly + on-demand)
                ┌────────────────▼────────────────────────┐
                │ AZURE SQL REPLICA · edap-replica-cms    │
                │   schemas: edap_dw_replica · stg_replica│
                └────────────────┬────────────────────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
┌───────▼────────┐      ┌────────▼────────┐      ┌────────▼─────────┐
│ src/brain/     │      │ src/brain/      │      │ src/discovery/   │
│ data_access    │      │ research/*      │      │ schema_introspect│
│ col_resolver   │      │ (MIT CTL)       │      │ (live caching)   │
└───────┬────────┘      └────────┬────────┘      └────────┬─────────┘
        │                        │                        │
        └─────────┬──────────────┴───────┬────────────────┘
                  │                      │
        ┌─────────▼────────┐    ┌────────▼─────────┐
        │ pages/*.py       │    │ src/deck/        │
        │ (Streamlit UI)   │    │ live · findings  │
        └─────────┬────────┘    │ builder · schemas│
                  │             └────────┬─────────┘
                  │                      │
                  └────────────┬─────────┘
                               │
                    ┌──────────▼────────────┐
                    │ findings_index (SQL)  │
                    │ snapshots/*.json/.pptx│
                    └───────────────────────┘
```

---

## 3. Module-by-Module Reference

### 3.1 Connection layer — `src/connections/`

| File | Purpose | Schema touchpoints |
|------|---------|--------------------|
| `azure_sql.py` | pyodbc connection, `list_schemas`, `list_tables`, `list_columns` | `INFORMATION_SCHEMA.SCHEMATA / TABLES / COLUMNS` |
| `oracle_fusion.py` | Playwright SSO + BIP REST + FSCM REST | OTBI subject areas, BIP catalog |

Reads `config/connections.yaml`. Both connectors auto-register through
`src/brain/db_registry.bootstrap_default_connectors()`.

### 3.2 Brain core — `src/brain/`

| Module | Reads from | Writes to | Notes |
|--------|------------|-----------|-------|
| `db_registry.py` | — | in-memory `_REGISTRY` | Pluggable Connector pattern; `read_sql(name, sql)` is the canonical query API |
| `auth.py` | env / config | session token | MFA / OAuth helpers |
| `col_resolver.py` | `INFORMATION_SCHEMA` (cached to `config/schema_cache.json`) | cache JSON | Resolves semantic role → physical column (e.g. `part_key` → actual name) |
| `schema_introspect.py` | INFORMATION_SCHEMA | — | Lower-level introspection |
| `data_access.py` | Any registered SQL | DataFrame | Type-coerces, applies 120 s timeout |
| `cleaning.py` | DataFrame | DataFrame | Standard cleaning (whitespace, NaN harmonisation) |
| `imputation.py` | DataFrame | DataFrame | Missingness profile, mass-impute, VOI scoring (Page 5) |
| `analytics_fact.py` | `fact_po_receipt`, `fact_inventory_on_hand`, `fact_sales_order_line` | denormalised fact `fact_supply_chain_brain` | Lead-time mean/std, on-hand avg, annual demand |
| `eoq.py` | DataFrame (from `analytics_fact`) | EOQ, Bayesian-Poisson, LinUCB rerank | Pure math; no DB |
| `otd_recursive.py` | `fact_sales_order_line` | hierarchical cluster path per row | TF-IDF + K-means (see also `recursive_otd_categorization_rebuilt.py`) |
| `graph_context.py` | parts / receipts / sales | `nx.MultiDiGraph` | Adds nodes & edges with `*_label` enrichment (v0.4.5 fix) |
| `graph_backend.py` | — | NetworkX / Neo4j / Cosmos | Backend abstraction |
| `label_resolver.py` | `dim_part`, `dim_supplier`, `dim_customer` | enriched DF with `*_label` columns | Used by every page that displays a key |
| `findings_index.py` | SQLite `findings.db` | `findings.db` | Cross-page drill-down store |
| `drilldown.py` | `findings.db` | UI helpers | Citation footer + drill expanders |
| `cross_app.py` | (Spec Phase 4) | — | **NOT YET IMPLEMENTED** — referenced by deck spec |
| `whatif.py` | findings snapshots | scenario snapshots | Clone / mutate / diff for Page 12 |
| `demo_data.py` | live SQL only | DataFrame | `auto_load(sql, connector)` + `render_diagnostics()` schema browser fallback |
| `ips_freight.py` | po_receipt + cost | freight survival | Logistic fallback if `scikit-survival` absent |

### 3.3 MIT CTL research — `src/brain/research/`

| Module | Reads from | Computes | Page |
|--------|------------|----------|------|
| `hierarchical_eoq.py` | demand by part | Empirical-Bayes shrinkage on Poisson rates | 2 |
| `causal_lead_time.py` | po_receipt + dim_supplier | EconML causal forest (perm-importance fallback) | 4 |
| `lead_time_survival.py` | po_receipt | Kaplan-Meier + Cox PH (lifelines) | 7 |
| `bullwhip.py` | sales / mfg / receipt | Lee-Padmanabhan-Whang variance ratio + heatmap | 8 |
| `multi_echelon.py` | WIP + inventory + part hierarchy | Graves-Willems guaranteed-service safety stock | 9 |
| `sustainability.py` | po_receipt + cost + supplier | GLEC / ISO 14083 Scope-3 freight CO₂ | 10 |
| `freight_portfolio.py` | po_receipt + cost | CV-thresholded contract / spot / mini-bid mix; goldfish rejection | 11 |
| `risk_design.py` | scenarios | Monte-Carlo CVaR + Pareto frontier | 4 (panel) |

### 3.4 ETL plumbing — `src/extract / load / transform / sync / discovery`

| File | Role |
|------|------|
| `extract/extractor.py` | Pull from Oracle / source systems |
| `transform/transformer.py` | Apply rules in `config/mappings.yaml` |
| `load/loader.py` | Upsert into Azure SQL replica |
| `sync/reconciler.py` | Diff Azure ↔ Oracle and reconcile |
| `discovery/schema_discovery.py` | Enumerate **all** schemas/tables on both sources; writes `discovered_schema.yaml` |

### 3.5 Deck pipeline — `src/deck/`  ← THIS IS WHERE PPTX IS BUILT

| File | Role |
|------|------|
| `schemas.py` | Canonical Pydantic-style contracts for every finding category. **Defines the safe defaults** ("Unknown / not captured", empty frames, NULL fillers) |
| `live.py` | Live SQL discovery + KPI computation. Contains `discover_cycle_count_table()`, `compute_ifr()`, `late_line_reasons()`. **All four schema gaps land here.** |
| `demo.py` | Synthetic findings used when `--demo` flag is passed (or live SQL fails) |
| `findings.py` | Phase 3–6 finding rules: OTD failure Pareto, IFR trends, ITR cadence, PFEP centrality |
| `realizations.py` | Wraps each finding into a slide-ready realisation object |
| `pathways.py` | Cross-finding causal pathways (Phase 5/6) |
| `windows.py` | Time-window selection (rolling 12 mo, fiscal qtr, etc.) |
| `erp_translation.py` | Maps replica column names ↔ ERP friendly labels for slide titles |
| `constants.py` | Slide titles, deck order, branding |
| `builder.py` | **`render_pptx(findings, out_path)`** — uses `python-pptx` to assemble 14 (single-site) or 16 (portfolio) slides |

### 3.6 Streamlit pages — `pages/`

| Page | Primary modules | Critical columns |
|------|-----------------|------------------|
| `0_Query_Console.py` | `db_registry`, `findings_index` | Hardcoded `AZURE_SEARCH_TABLES` (now augmented by Schema Discovery) |
| `0_Schema_Discovery.py` | `db_registry`, `col_resolver` + DATA_DICTIONARY parser | Any (live INFORMATION_SCHEMA) |
| `1_Supply_Chain_Brain.py` | `graph_context`, `label_resolver` | `part_key`, `supplier_key`, `customer_key` |
| `2_EOQ_Deviation.py` | `eoq`, `analytics_fact` | `quantity`, `unit_cost`, `lead_time_days` |
| `3_OTD_Recursive.py` | `otd_recursive`, `cleaning` | `description`, `promise_date`, `ship_date`, **`failure_reason` (gap)** |
| `4_Procurement_360.py` | `graph_context`, `causal_lead_time`, `risk_design` | supplier + part + receipts |
| `5_Data_Quality.py` | `imputation`, `col_resolver` | logical-table picker |
| `6_Connectors.py` | `db_registry`, `auth` | live status pings |
| `7_Lead_Time_Survival.py` | `research.lead_time_survival` | `receipt_date`, `promise_date` |
| `8_Bullwhip.py` | `research.bullwhip` | sales / mfg / po qty by week |
| `9_Multi_Echelon.py` | `research.multi_echelon` | WIP, on-hand by stage |
| `10_Sustainability.py` | `research.sustainability` | freight km + mode |
| `11_Freight_Portfolio.py` | `research.freight_portfolio` | rate, lane, mode |
| `12_What_If.py` | `whatif`, `eoq`, `findings_index` | snapshot diffs |
| `13_Decision_Log.py` | `findings_index` | findings.db |
| `14_Benchmarks.py` | `bench/bench_brain` results | `bench/results/latest.csv` |

---

## 4. Schema Catalogue (with consumer cross-references)

Only the columns most commonly bound to multiple consumers are listed. The
full set lives in `config/schema_cache.json` and is documented row-by-row in
`docs/DATA_DICTIONARY.md`.

### 4.1 `edap_dw_replica.fact_sales_order_line`
*Grain: one row per shipped or open SO line · Oracle source: `DOO_LINES_ALL`*

| Column | Used by |
|--------|---------|
| `sales_order_number` | Page 0 search, Page 3 grouping |
| `part_key` | label_resolver, graph_context, eoq, otd_recursive |
| `customer_key` | graph_context, sustainability |
| `order_date_key` (YYYYMMDD int) | bullwhip, IFR window, OTD trend |
| `promise_date_key` / `ship_date_key` | OTD calculation, lead-time survival |
| `quantity` | eoq demand, bullwhip variance |
| **`failure_reason`** | **MISSING — see §1** |

### 4.2 `edap_dw_replica.fact_inventory_on_hand`
*Grain: site × part × snapshot date · current overwritten daily*

| Column | Used by |
|--------|---------|
| `part_key`, `business_unit_key` | label_resolver, multi_echelon |
| `on_hand_qty` | EOQ, IFR, multi-echelon |
| `snapshot_day_key` | **only "today" exists — historical IFR breaks (§1)** |

### 4.3 `edap_dw_replica.fact_inventory_open_orders` / `…open_mfg_orders`
Used by multi-echelon (`research.multi_echelon`) and Procurement 360.

### 4.4 `edap_dw_replica.fact_po_receipt`
*Grain: one row per receipt event · Oracle: `RCV_TRANSACTIONS`*

Critical for **lead-time survival, bullwhip, sustainability, freight portfolio.**
Date keys are YYYYMMDD ints — convert with
`TRY_CONVERT(date, CONVERT(varchar(8), CAST([col] AS bigint)), 112)`
(v0.4.5 fix; failure to do this returns NULL silently).

### 4.5 `edap_dw_replica.dim_part`
| Column | Status |
|--------|--------|
| `part_number`, `part_description` | OK |
| `commodity`, `buyer` | OK |
| **`sales_part_code`**, **`inventory_part_code`** | **MISSING — ABC analysis blocked (§1.4)** |

### 4.6 `edap_dw_replica.dim_supplier`, `dim_customer`, `dim_po_contract`
Standard dimension tables; resolved by `label_resolver`.

### 4.7 `stg_replica.fact_part_cost`
Standard / frozen unit cost per item × site × cost type. Feeds EOQ, sustainability, freight.

### 4.8 Optional / runtime-discovered tables
- `fact_cycle_count` — discovered by `src/deck/live.py::discover_cycle_count_table`. **Not currently in replica.**
- `fact_supply_chain_brain` — denormalised nightly fact built by `analytics_fact.build()`.

---

## 5. PowerPoint Render — Step-by-Step

```bash
python pipeline.py deck \
    --site SITE_CODE \           # optional; omit for portfolio (16 slides)
    --out  snapshots/cross_dataset_review_live.pptx \
    --json snapshots/cross_dataset_review_live.json \
    [--demo]                     # bypass live SQL, use src/deck/demo.py
```

### Sequence

1. **CLI dispatch** → `pipeline.py` resolves `deck` subcommand.
2. **Connector bootstrap** → `db_registry.bootstrap_default_connectors()`.
3. **Live discovery** → `src/deck/live.py`:
   - Calls `col_resolver` to translate logical → physical column names.
   - Runs `discover_cycle_count_table()` (returns empty if §1.2 holds).
   - Computes IFR via `compute_ifr()` (§1.3 makes this ≈ 0 %).
   - Aggregates late-line reasons via `late_line_reasons()` (§1.1 → "Unknown / not captured").
4. **Findings rules** → `src/deck/findings.py` runs Phase 3–6 rules over the live frames.
5. **Realisation** → `realizations.py` shapes each finding into a slide payload.
6. **JSON dump** → payload written to `--json` path (used by What-If page and audit).
7. **`render_pptx(findings, out_path)`** in `builder.py` builds the deck slide-by-slide.

### Why the four issues persist

Every slide template assumes the **safe defaults** declared in `schemas.py`.
When the upstream column / table is missing, the slide still renders, but
contains the literal default ("Unknown / not captured", empty frame, 0 %).
**No code change in `src/deck/` will fix this** — the fix is in the replica ETL
(see §6).

---

## 6. Closing the Gaps — Action Plan

| # | Gap | Action | Owner | Touches |
|---|-----|--------|-------|---------|
| 1 | `failure_reason` | Add column to `fact_sales_order_line`; map from `DOO_LINES_ALL.LATE_REASON_CODE` in `src/transform/transformer.py`; add to `config/mappings.yaml`; refresh `schema_cache.json`. | EDAP ETL | Page 3, Phase 3a, slide *OTD Failure Pareto* |
| 2 | `fact_cycle_count` | Confirm Oracle source `INV_CYCLE_COUNT_ENTRIES`; build `fact_cycle_count` in `analytics_fact.py`; register in `mappings.yaml`. | EDAP ETL | Slides *Cycle-Count Cadence*, *ITR Accuracy*, Phase 5b |
| 3 | Point-in-time inventory | Add nightly snapshot of `fact_inventory_on_hand` keyed by `snapshot_day_key`; modify `compute_ifr()` to join orders against snapshot ≤ `order_date_key`. | EDAP ETL + `src/deck/live.py` | Page 2, IFR slides, Phase 3b |
| 4 | ABC columns on `dim_part` | Either (a) sync `sales_part_code` / `inventory_part_code` from eDap publication or (b) compute Pareto ABC inside `analytics_fact.py` and write back to `dim_part`. | EDAP / Brain | Pages 1–3, ABC slides, Phase 5c |

After any of these are landed:

```powershell
# 1. Refresh column cache
python -c "from src.brain.col_resolver import discover_all_key_tables; discover_all_key_tables('azure_sql')"

# 2. Re-run benchmark suite
python -m bench.bench_brain --rows 20000 --repeats 3

# 3. Regenerate the deck
python pipeline.py deck --out snapshots/cross_dataset_review_live.pptx \
                        --json snapshots/cross_dataset_review_live.json
```

---

## 7. Quick Diagnostic Checklist (when a slide goes blank)

1. **Open `0_Schema_Discovery`** in the app → pick the connector → search for the column the slide expects.
   - Found? → real bug, file a ticket against the consumer module.
   - Not found? → **schema gap**; consult §1 / §6 above.
2. **Inspect the JSON sidecar** (`snapshots/cross_dataset_review_live.json`) — look for `"_warnings"` and `"_fallback_reason"` keys emitted by `src/deck/live.py`.
3. **Check `bench/results/latest.csv`** — if a research module is timing out, IFR / OTD frames may be truncated.
4. **Run with `--demo`** — if the slide renders correctly with synthetic data, the pipeline is healthy and the issue is purely upstream data.

---

## 8. Cross-References

- `docs/DATA_DICTIONARY.md` — column-level dictionary (the source of truth this guide cites).
- `docs/EDAP_DASHBOARD_TABLES.md` — Power BI dashboard ↔ replica table mapping.
- `docs/ARCHITECTURE.md` — layered architecture overview.
- `docs/CONFIG.md` — every key in `config/brain.yaml` and `config/mappings.yaml`.
- `docs/RUNBOOK.md` — install, start, health-check, common-issue table.
- `CHANGELOG.md` — version-by-version changes (current: 0.4.6).
- `CrossDataset_Agent_Process_Spec.md` (repo root) — Phase 1–6 spec the deck builder implements.
