# Cross-Dataset Supply-Chain Review — Process Spec

**Purpose.** This document is an implementation guide for an agent that performs the same end-to-end analysis used to produce the Cross-Dataset Supply-Chain Review deck. The agent reads from a connected database, applies a fixed analytical pipeline, and outputs higher-level realizations about supply-chain performance — across one plant or across many.

**Origin.** The all-plant deck and the Burlington deck were both produced from the same logical pipeline. This spec generalizes that pipeline so it can run on any plant, any ERP, any time window, with no manual scripting.

**Anchor policy.** AST-INV-PRO-0001. **Reproducibility seed.** `9` for every stochastic step.

---

## 1. Agent Intent — what this thing actually does

The agent answers six questions in order. Every output traces back to one of these:

1. **Where are we** on OTD, Item Fill Rate, and Cycle-Count Accuracy right now, against goal?
2. **What changed** versus the prior 14-day window?
3. **Why are we missing** — what is the *signature* of failure (reason / customer / supplier)?
4. **What does the join across datasets reveal** that any single dataset can't?
5. **Which interventions are systemic** (ERP-level) and which are **operational** (plant-floor)?
6. **In what sequence** should we apply them, and who owns each?

Anything the agent produces — slide, dashboard tile, JSON payload — must be an answer to one of these. If a finding doesn't map to a question, it's noise.

---

## 2. Data Contract — what the agent needs

The agent expects four logical datasets. Each has a canonical schema below. The agent must support pulling these from any of three ERPs (Epicor 9, Oracle Fusion, Syteline) — see the ERP translation layer in §3.

### 2.1 OTD (On-Time Delivery) — sales-order line history

| Canonical field | Type | Required | Notes |
|---|---|---|---|
| `Site` | string | yes | Plant identifier |
| `Order Date` | date | yes | SO entry date |
| `Ship Date` | date | yes | Actual ship date — primary KPI window anchor |
| `SO No` / `Line No` | string | yes | Composite line key |
| `Part` | string | yes | Item number (normalized: trim + uppercase) |
| `Qty` / `Available Qty` / `On Hand Qty` | number | yes | Quantities at ship time |
| `OTD Miss (Late)` | bool/0-1 | yes | 1 = late, 0 = on-time |
| `Days Late` | number | yes if late | For survival/fat-tail analysis |
| `Customer` / `Customer No` | string | yes | For centrality |
| `Supplier Name` | string | optional | Drop-ship lines |
| `Part Pur/Fab` | enum | yes | `purchased` / `fabricated` |
| `Failure Reason` | string | yes | The most important field — see VoI rule §6 |
| `Promised Date` / `Adjusted Promise Date` | date | yes | OTD baseline; per EDAP, use Adjusted Promise Date |

### 2.2 IFR (Item Fill Rate) — order-line snapshot at order time

| Canonical field | Type | Required | Notes |
|---|---|---|---|
| `Site` | string | yes | |
| `Order Date` | date | yes | KPI window anchor |
| `Part` | string | yes | |
| `SO Qty` | number | yes | What was ordered |
| `Available Qty` | number | yes | At order time |
| `On Hand Qty` | number | yes | At order time — the **stockout vs allocation-gap** discriminator |
| `Hit Miss` | bool/0-1 | yes | 1 = filled, 0 = missed |
| `Part Fab/Pur` | enum | yes | |
| `Supplier Name` | string | yes if purchased | |
| `Failure` | string | yes | Often sparse — drives VoI gap flag |
| `Customer Name` | string | yes | |

### 2.3 ITR (Inventory Transactions) — Oracle Fusion or equivalent

| Canonical field | Type | Required | Notes |
|---|---|---|---|
| `Transaction Date` | date | yes | |
| `Transaction Type` | enum | yes | Filter to `Cycle Count Adjustment` for CC analysis |
| `Item Name` | string | yes | Normalize to match `Part` from OTD/IFR |
| `Quantity` / `Net Dollar` | number | yes | Variance magnitude |
| `Subinventory` | string | yes | Sub-level distribution |
| `Transaction Reason Code` | string | **yes (high VoI)** | Population % is a primary finding |
| `Created By` / `Last Updated By` | string | optional | Audit trail |

### 2.4 PFEP (Plan For Every Part) — planning master

| Canonical field | Type | Required | Notes |
|---|---|---|---|
| `Item Name` | string | yes | The join key to OTD/IFR/ITR |
| `Item Status` | enum | yes | Filter to "Active" before any health calc |
| `Make or Buy` | enum | yes | |
| `Supplier` / `Buyer Name` | string | yes | Buyer Name = ownership routing |
| `Cost` | number | yes | |
| `Total Usage` / `Usage Value` | number | yes | Demand signal |
| `Safety Stock` | number | yes | Critical population field |
| `Minimum Quantity` / `Maximum Quantity` | number | yes | |
| `Processing Lead Time` | number | yes | |
| `ABC Inventory Catalog` | string | yes | Drives CC cadence under AST-INV-PRO-0001 |
| `Item Cycle Count Enabled` | bool | yes | |
| `Inventory Planning Method` / `Safety Stock Planning Method` | enum | yes | |

---

## 3. ERP Translation Layer

The agent must alias source-ERP columns to canonical names before any analysis. This is the single most important separation-of-concerns boundary in the agent — every downstream function reads canonical fields only.

### 3.1 Cycle-count column translation

| Canonical | Epicor 9 | Oracle Fusion | Syteline |
|---|---|---|---|
| Frozen QOH | `TotFrozenQOH` | `FrozenOnHand` (count_events) | `cc_trn.qty_on_hand_before` |
| Counted QOH | `TotCountQOH` | `CountedQty` | `cc_trn.qty_counted` |
| Discrepancy reason | `CDRCode` | `DiscrepancyReasonCode` | `reason_code` (app: `CYC`) |
| ABC class | `ABCCode` | `ABC_Class` (custom) | `item_abc_code` |
| Complete date | `CompleteDate` | `CountCompletedDate` | `post_date` + `post_status IN (1,2)` |
| Cycle / due date | `CycleDate` | `CountDueDate` | `cc_trn.due_date` |
| Warehouse | `WarehouseCode` | `SubInventoryCode` | `whse` |

### 3.2 Plant ↔ ERP map (extend as new sites onboard)

| Site | Site string in OTD/IFR | ERP | Cycle-count source |
|---|---|---|---|
| Jerome Ave (Chattanooga) | `Chattanooga - Jerome Avenue` | Epicor 9 | `PartCount` / `CCMerger` (wh `S1`, `E1`) |
| Manufacturers Rd | `Chattanooga - Manufacturers Road` | Epicor 9 | `PartCount` |
| Wilson Rd | `Chattanooga - Wilson Road` | Epicor 9 | `PartCount` |
| Airport Rd (Eugene) | `Eugene - Airport Road` | Oracle Fusion | Oracle `CountEvents` |
| PDC | `Prairie du Chien` | Oracle Fusion | Oracle `CountEvents` |
| St. Cloud | `St Cloud` | Oracle Fusion | Oracle `CountEvents` |
| Burlington | `Burlington` | Oracle Fusion | Oracle `CountEvents` (REXCON instance) |
| Blair | `Blair` | Oracle Fusion | Oracle `CountEvents` |
| Parsons | `Parsons` | Syteline (SQL) | `PFI_App.dbo.cc_trn` |
| St. Bruno | `St Bruno` | Oracle Fusion | Oracle `CountEvents` |

### 3.3 Common gotchas

- **`CHAR(160)` non-breaking spaces** in Oracle Fusion item-number exports require `SUBSTITUTE(TRIM(x), CHAR(160), "")` before normalization. Apply at ingest, not at join.
- **Epicor `PartTran` ↔ `PartWhse` join** is many-to-many — use a Crystal subquery or aggregate before join.
- **Syteline `post_status`**: `1 = posted`, `2 = posted-and-archived`. Either counts as "complete."
- **OTD weekend buckets**: zero-line days will calc as 100% — exclude weekends and holidays from daily aggregations.

---

## 4. Window Convention

The agent uses a single, fixed temporal framework for everything. This is non-negotiable — every comparison in the deck assumes these brackets:

- **T** = the day the analysis runs (Monday anchor preferred for review cadence)
- **`T-28 → T-15`** = "Prior 14d" (baseline for delta calculation)
- **`T-14 → T-1`** = "Past 14d" (headline KPI window)
- **`T → T+14`** = "Future 14d" (forecast horizon)
- **`T-90 → T-1`** = 90-day baseline for site-level scorecards

Charts:
- OTD & IFR → daily line over Past 14d, weekly line over 90d for trend slides
- Cycle Count → stacked daily column over all 14 days (zero-height days are the story)

---

## 5. Pipeline — eight phases

The agent runs these phases in strict order. Outputs from each phase feed the next.

### Phase 1 — Ingest & Clean

```
inputs:  raw exports from each ERP (or live DB queries)
outputs: 4 typed dataframes with canonical schemas
```

Required cleaning steps:
1. Drop Tableau/Power-BI footer rows (`Site` starts with `Applied filters` or `Total`)
2. Coerce all dates with `errors='coerce'`, drop rows with null `Transaction Date` or `Ship Date` / `Order Date`
3. Coerce all numerics with `errors='coerce'` (Net Dollar, Quantity, Unit Cost, all PFEP fields)
4. Filter PFEP to `Item Status = '20 - Active'` for any "health" calculation (full set for join purposes)
5. Normalize part keys: `df['_pn'] = df[part_col].astype(str).str.strip().str.upper()`
6. Apply CHAR(160) substitution wherever item numbers come from Oracle Fusion exports
7. For PFEP: drop duplicates on `_pn` keeping first — used as a lookup index downstream

### Phase 2 — KPI Computation

For each site (or `ALL`):

```
otd_pct(window) = 1 - sum(OTD Miss (Late)) / count(lines)  on Ship Date in window
ifr_pct(window) = sum(Hit Miss) / count(lines)             on Order Date in window
cc_pct(window)  = 1 - count(nonzero variance tags) / count(posted tags)
                  where nonzero = |QtyVar| > 0.001 OR |ValVar| > 0.01
```

For each KPI, compute and store:
- 14-day value
- Prior 14-day value (delta = past − prior, in pp)
- 90-day baseline value
- Goal (95.0% for OTD, 95.0% for IFR, ≥95% accuracy for CC)
- n (line count in window)

### Phase 3 — Failure Decomposition

#### 3a. OTD failure-reason ranking

Group `OTD Miss (Late) == 1` lines by `Failure Reason`. Output top-N reasons globally and per-site. Flag the dominant reason per site and tag the operational class:
- `WH failed to ship` → **EXECUTION** class (WMS / pick-wave)
- `Missing other item on same SO` → **KITTING** class
- `Manufactured not ready, *` → **SCHEDULING** class
- `No purchased part, *` → **SUPPLY** class

#### 3b. IFR miss decomposition (the critical one)

For each miss line, classify into exactly one of three buckets:

```
allocation_gap = (On Hand Qty > 0)  AND (Available Qty < SO Qty)
hard_stockout  = (On Hand Qty <= 0)
covered_miss   = (On Hand Qty > 0)  AND (Available Qty >= SO Qty)
```

The ratio of these three is the most diagnostically valuable per-site signature in the entire pipeline. High allocation-gap → reservation/pegging issue (Oracle hygiene). High hard-stockout → safety-stock problem (PFEP fix). Covered-miss should be near zero — if not, investigate data quality.

#### 3c. Cycle-count cadence + variance-$

```
active_days_in_window = count(distinct day where any CC adjustment posted)
cadence_compliance    = active_days_in_window / business_days_in_window
absolute_variance_$   = sum(|Net Dollar|)
net_variance_$        = sum(Net Dollar)  -- positive bias = found-stock
repeat_offender_pct   = sum(tx for items with ≥2 adjustments) / total tx
```

#### 3d. Days-Late distribution (fat-tail detection)

Compute median, p75, p90, p99, max of `Days Late` on miss lines. Flag fat-tail when `p99 > 30 days` — triggers the survival-curve addendum and pre-stage recommendations.

### Phase 4 — Cross-Dataset Intersection (the differentiator)

This is what no single dataset can do. The agent computes these joins in order:

#### 4a. PFEP match audit on miss parts

Left-join IFR miss lines → PFEP master on normalized part. Compute and report:
- PFEP match rate (percentage of miss lines with a matching PFEP record)
- For matched lines: % with `Safety Stock = 0 or null`, % with `ABC class = null`, % with `Lead Time = 0`

If match rate < 90%, that itself is a finding — it means the canonical part identifier isn't actually canonical across systems. Flag and stop the recoverable-stockout calc until resolved.

#### 4b. Recoverable stockout calculation

```
purchased_stockouts   = IFR misses where Make/Buy = Buy AND On Hand = 0
recoverable_stockouts = purchased_stockouts where PFEP.Safety Stock IN (0, null)
recoverable_pct       = recoverable_stockouts / purchased_stockouts
```

This single number is the most important finding the agent produces. It quantifies the lift available from a one-time PFEP backfill.

#### 4c. Triple intersection

```
triple = (parts in CC variance YTD) ∩ (parts in IFR miss 90d) ∩ (parts in OTD late 90d)
```

Three outcomes are meaningful:
- **Non-zero triple count** → highest-leverage fix list (one part, three KPI lifts)
- **Zero triple count** → datasets pick up *different* failure modes; interventions run in parallel
- **Pairwise doubles** (CC∩IFR, CC∩OTD, OTD∩IFR) — always report, even when triple is zero

For each pair, output the top-N parts with: PFEP safety stock, CC adjustment count, CC abs $, IFR miss count, OTD late count.

### Phase 5 — Centrality & Leverage

Treat each universe as a bipartite graph and rank nodes by degree (or eigenvector centrality if scaled):

#### 5a. Customer centrality (OTD)

```
graph: customer ↔ late_line
rank:  customers by count of late lines in window
flag:  any customer with ≥ 5% of total lates is a "concentrated relationship"
```

#### 5b. Vendor centrality (IFR + OTD combined)

```
graph: supplier ↔ {miss_line, late_line}  [purchased lines only]
rank:  suppliers by ifr_miss + otd_late combined count
flag:  top-5 = the shared-leverage intervention list
```

The combined ranking is more valuable than either single-dataset ranking — it captures suppliers who hurt at order time AND at ship time.

#### 5c. Part centrality (variance × demand)

```
graph: part ↔ variance_event (from ITR)
weight: |Net Dollar| × usage_frequency
rank:   parts by weighted centrality
flag:   ABC = A items in top-15 weighted-variance list = process root cause
```

### Phase 6 — Higher-Level Realizations (the synthesis layer)

This is where the agent moves from numbers to findings. Apply each rule below to the outputs of Phases 2-5. Each fired rule produces a structured finding.

#### Realization rules

| Rule ID | Trigger | Realization output |
|---|---|---|
| R1: Site-signature divergence | Top failure reason differs across ≥3 sites | "Same KPI, N different problems — interventions must be site-specific, not portfolio-wide" |
| R2: Allocation-gap concentration | Any site has alloc-gap % > 30 of misses | "Reservation logic at {site} needs Oracle hygiene fix, not inventory fix" |
| R3: Recoverable stockout cluster | recoverable_pct > 70% on ≥10 stockouts | "{N} of {M} purchased stockouts are PFEP-preventable. One-time data fix = compounding ROI" |
| R4: Cadence collapse | cc_active_days / business_days < 0.6 over rolling 60d | "Cycle-count program at {site} has stopped — accuracy KPIs are directional only" |
| R5: VoI gap (Reason Code) | CC reason code populated < 20% on adjustments | "{X}% of variance is un-attributable. Root-cause targeting is impossible until field is enforced" |
| R6: VoI gap (IFR Failure) | IFR Failure field populated < 20% on miss lines | "Same — IFR misses can't be themed without the field. Mandate capture before next review cycle" |
| R7: PFEP forecasting floor | Active items with (Safety Stock + ABC + Lead Time all populated) < 20% | "MRP is running on an empty master. PFEP remediation is the prerequisite to any operational fix" |
| R8: Fat-tail lateness | p99(Days Late) > 30 days | "Pre-stage the top-20 fat-tail parts. Cheaper than chasing median cycle-time" |
| R9: Concentrated customer | Single customer ≥ 30% of lates over 90d | "Relationship review with {customer} — kit complexity / order shape conversation" |
| R10: Vendor cluster | Top-5 suppliers ≥ 40% of combined misses+lates | "Supplier scorecard program with focused vendor cohort, not full base" |

### Phase 7 — Pathway Synthesis

Realizations from Phase 6 are mapped to two pathway classes. **The classification is binary and the agent must apply it deterministically:**

#### Systemic pathways (ERP-level configuration)

A finding maps to systemic when remediation requires changing an Oracle/Epicor/Syteline setting, batch job, validation rule, or master-data attribute. The fix replicates across all future cycles automatically once applied.

Default systemic fix set (extend as new realizations emerge):
1. **Populate ABC Inventory Catalog** — triggered by R7 when ABC null > 50%
2. **Switch Safety Stock Planning Method** — triggered by R3
3. **Transaction Reason Code enforcement** — triggered by R5
4. **Allocation reconciliation batch job** — triggered by R2
5. **Assign Buyer / Planner codes** — triggered by R10 (need ownership routing)
6. **Item Cycle Count Enabled audit** — triggered by R4

#### Operational pathways (plant-floor process)

A finding maps to operational when remediation requires a process change a site can implement without touching ERP config. The fix is per-site, per-shift, per-team.

Default operational fix templates:
- "WH failed to ship" → pick-wave rebalance + same-SO completion gate
- "Missing other item on same SO" → kit-complete check at pick release
- "Manufactured not ready" → fab-schedule sync + early-warning flag
- Concentrated customer → relationship review + order-shape conversation
- Concentrated vendor → scorecard + expedite protocol

For each pathway, the agent emits: **(evidence, mechanism, downstream lift, owner, expected sequence position)**.

### Phase 8 — Output Generation

The agent produces three artifacts from the same underlying findings JSON:

#### 8a. JSON payload (the source of truth)

Structured output with sections:
- `scope` (datasets in scope, dates, site filter)
- `kpis` (with deltas and goals)
- `by_site` (per-site breakdowns, only when scope > 1 site)
- `failure_signatures` (Phase 3a/3b/3c outputs)
- `intersections` (Phase 4 outputs, including the triple/double tables)
- `centrality` (Phase 5 outputs — customers, vendors, parts)
- `realizations` (fired Phase 6 rules with evidence)
- `pathways_systemic` (with sequencing position)
- `pathways_operational` (per-site, with owner)
- `roadmap` (T+30, T+60, T+90, T+180 phase contents)
- `governance` (review cadence + KPI targets)

#### 8b. Slide deck (16-slide template for portfolio view, 14-slide for single site)

The deck is a *renderer* over the JSON. Slide order is fixed and deliberate:

1. Cover with scope summary
2. Exec summary — 3 KPI cards + 4 lenses + 3 realizations
3. Data architecture — 2×2 grid of datasets joined through PART-No.
4. OTD scorecard / trend (per-site if portfolio, weekly if single)
5. OTD failure signature
6. OTD customer centrality + days-late tail
7. IFR scorecard + miss decomposition (the allocation-vs-stockout chart is non-negotiable)
8. IFR allocation-gap thesis (only when alloc-gap pct > 20% somewhere)
9. Vendor centrality
10. Cycle-count cadence + variance subinventory
11. PFEP parameter health
12. Cross-dataset intersection findings
13. Systemic Oracle pathways (5 fixes)
14. Operational pathways (per-site action cards)
15. 30-60-90-180 roadmap
16. Governance + closing one-liner

#### 8c. Dashboard tiles (for live reviews)

Same JSON, rendered as a set of widgets keyed by review cadence (weekly, bi-weekly, monthly). The PlantReview runbook §6 describes the 60-minute conversation flow.

---

## 6. Reproducibility Rules

- **Seed = 9** for everything stochastic: EWMA bootstrap CIs, Thompson sampling in any LinUCB ranking, train/test splits for feature importance.
- Anyone re-running the same pipeline on the same data must get bit-identical CI bounds.
- All intermediate dataframes are typed at ingest (Phase 1) and never re-coerced downstream.
- Every chart's underlying data goes into the JSON payload — no chart can show a number that isn't traceable.

---

## 7. When the Agent Should Flag (escalation rules)

The agent should not just report. It should escalate when these conditions hit:

| Condition | Escalate to | Cadence |
|---|---|---|
| Site OTD or IFR drops > 5 pp below 90-day baseline in 14d window | Site Lead | Within day of detection |
| New realization fires that has never fired for that site before | Site Lead + Planner | Next review |
| Triple-intersection part count goes from 0 → ≥3 | Senior Engineer (SC) | Immediate |
| PFEP match rate on miss parts drops below 80% | IT Governance | Immediate (data integrity) |
| CC reason code population improves above 50% from baseline | Whole team (positive escalation) | Next review |
| Recoverable-stockout count exceeds prior period by ≥20% | Planner + Buyer | Immediate |

---

## 8. What Makes This Different From a Reporting Tool

The agent is not a dashboard generator. The distinguishing capability is the synthesis layer (Phase 6 realizations + Phase 7 pathway classification). A reporting tool stops at "OTD is 88.1%." This agent goes to:

> "OTD is 88.1%. Manufacturers Rd drives 61% of lateness. Of Man Rd's 14-day misses, 54.7% are 'WH failed to ship' and 42% of all 90-day misses are allocation-gap (not stockout). The systemic fix is allocation reconciliation in Oracle (Systemic #4). The operational fix is pick-wave rebalancing (Operational, owner = Site Lead + WMS lead). The two run in parallel; expect 4-6 weeks to see lift."

That second paragraph is the value. Everything in this spec exists to make it producible without manual analysis.

---

## 9. Extending the Agent

Three places the agent should be designed to extend:

1. **New ERP** → add a translation block to §3.1, no other code changes.
2. **New realization rule** → add a row to the §6 table with trigger + output template; the synthesis layer picks it up automatically.
3. **New pathway** → add to §7's systemic or operational list, link to triggering realization rule.

Don't extend by adding new dataset categories without a clear contract — the four-dataset structure (OTD, IFR, ITR, PFEP) is what makes the cross-dataset joins meaningful. Adding a fifth dataset means revisiting the architecture slide and the join logic in Phase 4.

---

## 10. Reference Implementation Pointers

The Python pipeline that produced the source decks is structured into four scripts that map 1:1 onto Phases 1-4 / 5-7 / 8a / 8b. For the agent implementation:

- Replace `pd.read_excel(...)` calls with parameterized DB queries against the canonical schemas in §2
- Keep the cleaning (Phase 1) as a pre-query view layer in the database where possible — let Postgres / Snowflake do the date coercion and the CHAR(160) substitution at view-definition time
- Phase 2-5 is pure pandas / numpy — no external dependencies beyond `numpy`, `pandas`, `networkx` (for actual centrality calculations rather than the simplified count-based version used here)
- Phase 6 (realizations) is best implemented as a rules engine — each rule is a function `(payload) -> Optional[Finding]`
- Phase 7 (pathway classification) is a deterministic lookup on the realization output
- Phase 8a (JSON) is the API; 8b (deck) and 8c (dashboard) are renderers — they should not contain analytical logic

---

*Spec version 1.0 · 2026-04-20 · Adam Gard / IPS Logistics & Inventory Control · Astec Industries*
*Anchor policy: AST-INV-PRO-0001 · Reproducibility seed: 9*
