# Supply Chain Pipeline - The Brain of the Application

## Master Overview
The **Supply Chain Pipeline** is the core "Brain" serving as a unified, interactive workstream visualization of the complete supply chain. It fuses concepts from every analytical module into a single interactive command surface with 12 heuristics stages ranging from Demand / Forecasting to Delivery and Sustainability.

## SOTA Deductions by Stage

### Demand Signal Sensing (DEM)
Bayesian-Poisson smoothing on order arrivals exposes latent demand variance. Causal inference (DoWhy) decomposes promotional uplift vs. organic baseline.

### Forecast Distortion (Bullwhip) (FCT)
Variance ratio σ²(orders)/σ²(demand) quantifies amplification across echelons. MIT CTL Fransoo-Wouters method isolates production-smoothing vs. batch-ordering contributions.

### Hierarchical EOQ Deviation (EOQ)
Bayesian shrinkage rolls part-level EOQ to category priors. Outliers flagged via posterior tail probability. Carrying-cost vs. ordering-cost quadrant maps decisive interventions.

### Supplier Reliability 360 & LLM Vendor Management (PRC)
LinUCB contextual bandit ranks suppliers across PO-history, defect rate, and lead-time variance. The **Vendor Management Model** specifically addresses the following components natively within the application:
0. **Cross-Dataset Schemas:** Synthesizes and aggregates data across both Azure SQL (edap_dw_replica) and Oracle Data Schemas.
1. **Ordering Velocity:** Assesses 30-day Order Frequency dynamics tied to demand.
2. **Product Information & LLM Understanding:** Utilizes Natural Language/LLM techniques to understand product similarities (e.g. Steel, Fasteners, Wiring) and matching similar vendor profiles to expand the capability of vendor consolidations.
3. **PO Part Lead Time:** Measured at the distinct Part and Product Category intersection.
4. **Volume & Density Deviations:** Open PO length is modeled by its relational deviation from a centroid density derived from three physical dimensions: Order Size, Order Value, and 30-day Order Frequency.

### Kaplan-Meier Lead-Time Survival (LDT)
Censored survival curves expose the probability of receipt by day-N per supplier × part-class. Cox PH covariates explain hazard ratios of late delivery.

### PFEP Data Quality VOI (PFP)
Value-of-Information heatmap prioritizes missing fields (UoM, pack-size, dock-door) by their downstream impact on EOQ accuracy and freight cubing.

### Multi-Echelon Optimization (INV)
METRIC/Sherbrooke-style network safety stock placement balances service level vs. holding cost across nodes. Solves the positioning problem under demand correlation.

### Value-Stream Friction Map (WO)
Live PO/SO/WO graph quantifies WIP velocity, cycle-time outliers, and bottleneck transitions using the Supply Chain Brain knowledge graph.

### Recursive OTD Cascade (OTD)
Each missed shipment is back-traced through BOM dependencies; root-cause attribution exposes the upstream constraint (supplier, WO, capacity, planning rule).

### Freight Portfolio Optimization (FRT)
LTL vs FTL mode choice via cost/lb, zone-skip routing, and CVaR risk on lane-level volatility. Identifies consolidation candidates with highest savings × confidence.

### Customer Fill Benchmarks (CUS)
Industry IFR/ITR peer comparison from MIT Benchmarks. Highlights percentile gap and the operational levers most correlated with closing it.

### Scope-3 Carbon Footprint (ESG)
Emission intensity per dollar of throughput with mode-shift simulation. Green-logistics levers ranked by abatement cost vs. service impact.
