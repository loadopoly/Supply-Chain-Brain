"""
Pathway classification (spec §7).

Binary, deterministic. A Phase 6 realization maps to:
  • systemic — ERP-level configuration change (Oracle/Epicor/Syteline);
               fix replicates across all future cycles automatically.
  • operational — plant-floor process change; per-site, per-shift.

Emit (evidence, mechanism, downstream_lift, owner, sequence) for each pathway.
"""
from __future__ import annotations

SYSTEMIC = {
    "R7": {
        "name": "Populate ABC Inventory Catalog",
        "mechanism": "Run ABC classification job against active items; write-back to PFEP master.",
        "downstream_lift": "Drives CC cadence under AST-INV-PRO-0001; unlocks safety-stock math.",
        "owner": "IT Governance + Planner",
        "sequence": 1,
    },
    "R3": {
        "name": "Switch Safety Stock Planning Method",
        "mechanism": "Set non-zero Safety Stock + assign Safety Stock Planning Method for recoverable parts.",
        "downstream_lift": "Converts recoverable stockouts to on-time fills — compounding ROI per spec §4.4b.",
        "owner": "Planner + Buyer",
        "sequence": 2,
    },
    "R5": {
        "name": "Transaction Reason Code enforcement",
        "mechanism": "Required-field validation on cycle-count adjustment transactions.",
        "downstream_lift": "Variance becomes attributable; root-cause targeting becomes possible.",
        "owner": "IT Governance",
        "sequence": 3,
    },
    "R2": {
        "name": "Allocation reconciliation batch job",
        "mechanism": "Nightly job reconciling allocations vs. on-hand; clears orphaned reservations.",
        "downstream_lift": "Collapses allocation-gap bucket — IFR lift without inventory investment.",
        "owner": "IT Governance + Planner",
        "sequence": 4,
    },
    "R10": {
        "name": "Assign Buyer / Planner codes",
        "mechanism": "Backfill Buyer Name / Planner on unattributed active items.",
        "downstream_lift": "Ownership routing — vendor scorecard program can run deterministically.",
        "owner": "IT Governance + Procurement",
        "sequence": 5,
    },
    "R4": {
        "name": "Item Cycle Count Enabled audit",
        "mechanism": "Audit & toggle Item Cycle Count Enabled flag against ABC-driven cadence.",
        "downstream_lift": "Restores the cycle-count program; CC accuracy becomes meaningful again.",
        "owner": "Site Lead + IT Governance",
        "sequence": 6,
    },
}

# Operational — keyed by fingerprint rather than rule id because multiple
# rules can spawn the same operational action.
OPERATIONAL_TEMPLATES = {
    "WH failed to ship": {
        "name": "Pick-wave rebalance + same-SO completion gate",
        "mechanism": "WMS configuration: reshape pick waves by SO-completion, not by zone.",
        "downstream_lift": "Direct OTD lift on EXECUTION-class misses.",
        "owner": "Site Lead + WMS Lead",
    },
    "Missing other item on same SO": {
        "name": "Kit-complete check at pick release",
        "mechanism": "Block pick release until all lines on an SO are allocable.",
        "downstream_lift": "Eliminates KITTING-class lateness.",
        "owner": "Site Lead + WMS Lead",
    },
    "Manufactured not ready": {
        "name": "Fab-schedule sync + early-warning flag",
        "mechanism": "Fab-to-ship daily sync meeting; raise flag at T-2 if fab slipping.",
        "downstream_lift": "SCHEDULING-class misses caught before ship-day.",
        "owner": "Site Lead + Fab Lead",
    },
    "Concentrated customer": {
        "name": "Customer relationship review + order-shape conversation",
        "mechanism": "Monthly review; reshape order cadence / kit complexity.",
        "downstream_lift": "Reduces single-customer concentration risk.",
        "owner": "Commercial + Site Lead",
    },
    "Concentrated vendor": {
        "name": "Supplier scorecard + expedite protocol",
        "mechanism": "Monthly scorecard on top-5 combined miss+late suppliers; expedite playbook for outliers.",
        "downstream_lift": "Converts vendor-driven misses into on-time fills.",
        "owner": "Procurement + Planner",
    },
    "Fat-tail": {
        "name": "Pre-stage top-20 fat-tail parts",
        "mechanism": "Standing safety-stock on parts with p99 Days Late > 30.",
        "downstream_lift": "Collapses the fat tail; cheaper than chasing median cycle-time.",
        "owner": "Planner + Site Lead",
    },
}


def classify(realizations: list[dict]) -> dict:
    """Return {'systemic': [...], 'operational': [...]} sorted by sequence
    (systemic) and by trigger rule-id (operational)."""
    systemic: list[dict] = []
    operational: list[dict] = []

    fired_ids = {r["rule_id"] for r in realizations}

    for rid, template in SYSTEMIC.items():
        if rid in fired_ids:
            systemic.append({**template, "rule_id": rid})

    # Operational mapping — walk fired realizations and their evidence
    for r in realizations:
        rid = r["rid"] if "rid" in r else r.get("rule_id")
        if rid == "R9":
            operational.append({**OPERATIONAL_TEMPLATES["Concentrated customer"], "rule_id": rid})
        if rid == "R10":
            operational.append({**OPERATIONAL_TEMPLATES["Concentrated vendor"], "rule_id": rid})
        if rid == "R8":
            operational.append({**OPERATIONAL_TEMPLATES["Fat-tail"], "rule_id": rid})
    # Operational from failure-reason signatures — caller-agnostic: always
    # emit the three EXECUTION/KITTING/SCHEDULING templates so the deck has
    # per-site action cards even if no rule fired for them.
    for key in ("WH failed to ship", "Missing other item on same SO", "Manufactured not ready"):
        operational.append({**OPERATIONAL_TEMPLATES[key], "rule_id": None, "trigger_reason": key})

    systemic.sort(key=lambda x: x["sequence"])
    return {"systemic": systemic, "operational": operational}
