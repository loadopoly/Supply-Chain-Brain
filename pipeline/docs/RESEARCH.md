# Research grounding — MIT Center for Transportation & Logistics

Every research-derived module in this codebase ties back to one of the
**MIT CTL** research labs. The application surfaces a citation footer on
each research page so analysts can see the provenance.

| Lab | Module(s) | Page |
|---|---|---|
| Deep Knowledge Lab for Supply Chain & Logistics | `eoq.py` (Bayesian-Poisson + LinUCB), `research/hierarchical_eoq.py`, `research/causal_lead_time.py` | 2, 4 |
| Digital Supply Chain Transformation Lab        | `imputation.py`, `findings_index.log_decision`                                                       | 5, 13 |
| Supply Chain Design Lab                         | `graph_context.py`, `research/risk_design.py` (CVaR Pareto), `research/multi_echelon.py`             | 1, 4, 9 |
| FreightLab                                      | `ips_freight.ghost_lane_survival`, `research/freight_portfolio.py` (CV-thresholded mix + goldfish)   | 11 |
| Intelligent Logistics Systems Lab               | `research/lead_time_survival.py`, `research/bullwhip.py`, `research/multi_echelon.py`                | 7, 8, 9 |
| Sustainable Supply Chain Lab                    | `research/sustainability.py`                                                                          | 10 |
| Computational Analytics, Visualization & Education (CAVE) | `drilldown.py` patterns + page UX                                                          | every page |

Citations are stored in `src/brain/drilldown.py::CITATIONS` and rendered
via `drilldown.cite("freight_lab", "intelligent_logistics", ...)` at the
bottom of each research page.

---

## Math

### EOQ Bayesian-Poisson centroidal deviation (req 1a–1c)

Classic EOQ:

    Q* = sqrt( 2 · D · S / (h · c) )

with `D` = annual demand units, `S` = ordering cost / order, `h` = holding
rate /year, `c` = unit cost.

Bayesian per-period demand rate λ:

    λ ~ Gamma(α₀, β₀)
    posterior:  α = α₀ + Σy ,   β = β₀ + n     →   E[λ|y] = α/β
    annual D̂ = (α/β) · periods_per_year

Centroidal deviation (z-score):

    var(D̂) = α/β² · periods_per_year²
    var(Q*) ≈ (dQ*/dD)² · var(D̂)         where  dQ*/dD = sqrt(S / (2·D·h·c))
    z       = (Q_observed − Q*) / sqrt(var(Q*))

Items are sorted by `|z|` then by dollar-at-risk so the highest-leverage
deviations float to the top.

### LinUCB self-reshape (req 1d)

Disjoint LinUCB. Each part is one arm with feature vector `xₚ` (commodity
one-hot, supplier, unit-cost bin, on-hand bucket, prior-OTD bucket).
After the user resolves a part with realized $-recovery `r`:

    Aₐ ← Aₐ + xxᵀ
    bₐ ← bₐ + r·x
    score(p) = θᵀx + α · sqrt(xᵀ A⁻¹ x)       with  θ = A⁻¹ b

`brain.eoq.LinUCBRanker` re-ranks the deviation table after every action.

### Hierarchical EOQ shrinkage (Deep Knowledge Lab)

Empirical-Bayes pooling within a `commodity` (or supplier) group:

    yᵢ = log(rateᵢ + 1) ,   σᵢ² = 1 / (countᵢ + 1)
    μ  = Σᵢ wᵢyᵢ / Σᵢ wᵢ                with wᵢ = 1/σᵢ²
    τ² = max(0, Var(y) − mean(σ²))
    ŷᵢ = (τ²·yᵢ + σᵢ²·μ) / (τ² + σᵢ²)
    rate_shrunkᵢ = exp(ŷᵢ) − 1

Slow movers borrow strength from neighbors → fewer over-reactions to
sparse demand. Surfaced as `rate_shrunk`, `shrink_weight`, `group_mean`.

### Causal forest on lead time (Deep Knowledge Lab)

Treatment = supplier feature in question (region / mode / lane / prior
OTD bucket). Outcome = realized lead time. Falls back to permutation
importance from a `RandomForestRegressor` if `econml` is unavailable.

### OTD recursive clustering (Intelligent Logistics)

Port of the user's own algorithm:

1. Featurize each receipt: TF-IDF on textual fields + numeric `days_late`.
2. KMeans with `find_optimal_k` (silhouette).
3. For every cluster with `n ≥ min_n` and entropy above threshold,
   recurse one level deeper.
4. Every cluster path is logged into `findings_index` so other pages can
   drill *through* into the offending receipts.

The original notebook expected an Excel input. We wired it to
`data_access.query_df("azure_sql", "<otd query>")` and run the same
`brain.cleaning.standard_clean` pipeline used by every page so signals
are consistent across the application.

### Lead-time survival (Intelligent Logistics)

Kaplan-Meier non-parametric survival → `median, p90, p95` per
supplier+lane group. Cox PH (lifelines) optionally fits hazard ratios
for supplier features. Falls back to empirical quantiles if `lifelines`
isn't installed.

### Bullwhip ratio (Intelligent Logistics)

Lee, Padmanabhan, Whang (1997):

    bullwhip_ratio = Var(orders) / Var(demand)

Surfaced per echelon as a heatmap. Ratios `> 1` indicate amplification.

### Multi-echelon safety stock — Graves & Willems guaranteed-service (Intelligent Logistics + SC Design)

For each stage `i`:

    NRTᵢ = max(0, SIᵢ + Tᵢ − Sᵢ)
    safety_stockᵢ = z(α) · σᵢ · sqrt(NRTᵢ)

with `Sᵢ` = downstream service time, `SIᵢ` = inbound service time,
`Tᵢ` = processing time, `σᵢ` = per-day demand σ, `z(α)` = inverse normal
at service level α.

### CVaR Pareto on supplier scenarios (SC Design)

For each supplier we Monte-Carlo simulate `n_sims` scenarios:

    demand   ~ Gamma(4, AnnualDemand/4)
    leadTime ~ Normal(μ_LT, σ_LT)        clipped ≥ 1
    disrupt  ~ Bernoulli(p_disrupt)
    cost     = unitCost · demand · (1 + 0.005·LT) · (1.35 if disrupt else 1)

Then:

    CVaR_α(cost) = E[cost | cost ≥ VaR_α(cost)]

Pareto-efficient on `(expected_cost, CVaR_α)` is highlighted.

### Scope-3 freight emissions — GLEC / ISO 14083 (Sustainable)

    co2e_kg = distance_km · payload_t · factor_g(mode) / 1000

Default factors per tonne-km (g CO2e):

    truck_ftl: 62 ,  truck_ltl: 95 ,  rail: 22 ,  ocean: 8 ,  air: 602

Override in `brain.yaml → emissions.factors`. Supplier sustainability
score is a 0–100 scaling of `(g CO2e / tkm)` with 5–95 percentile
winsorization.

### Smart freight portfolio + goldfish memory + ghost-lane survival (FreightLab)

- **Lane volatility** = CV per lane × period.
- **Portfolio mix**: contract / mini-bid / spot weights are calibrated
  via two sigmoids on CV: contract heavy below `CV ≈ 0.35`, spot above
  `CV ≈ 0.85`, mini-bid in the middle.
- **Goldfish memory**: rejection probability decays exponentially in
  rate-vs-market gap → suggested rate is `1.02 · market_rate`.
- **Ghost-lane survival**: gradient-boosted survival
  (`sksurv.ensemble.GradientBoostingSurvivalAnalysis`) on
  contract-vs-actual-volume; flags lanes with high inactivation
  probability so they can be renegotiated. Logistic-regression fallback
  keeps the page useful without `scikit-survival` installed.

---

## Citations (live URLs)

- Deep Knowledge Lab — <https://ctl.mit.edu/research/deep-knowledge-lab-supply-chain-and-logistics>
- Digital Supply Chain Transformation Lab — <https://ctl.mit.edu/research/digital-supply-chain-transformation-lab>
- Supply Chain Design Lab — <https://ctl.mit.edu/research/supply-chain-design-lab>
- FreightLab — <https://ctl.mit.edu/research/freightlab>
- Intelligent Logistics Systems Lab — <https://ctl.mit.edu/research/intelligent>
- Sustainable Supply Chain Lab — <https://ctl.mit.edu/research/sustainable-supply-chain-lab>
- CAVE Lab — <https://ctl.mit.edu/research/cave>
