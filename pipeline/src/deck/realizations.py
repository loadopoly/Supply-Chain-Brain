"""
Realization rules (spec §6, R1-R10).

Each rule is a pure function `(context) -> Finding | None`. The rule set is
additive: extend by appending to `RULES`. Every fired finding carries the
rule id, the realization sentence with fields substituted, and the evidence
that tripped it — so Phase 7 can classify deterministically and the slide
renderer can cite numbers.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Callable, Optional

import pandas as pd


@dataclass
class Finding:
    rule_id: str
    realization: str
    evidence: dict

    def to_dict(self) -> dict:
        return asdict(self)


RuleFn = Callable[[dict], Optional[Finding]]


def _r1_site_signature_divergence(ctx: dict) -> Optional[Finding]:
    # Trigger: top failure reason differs across ≥3 sites.
    per_site_top = {}
    by_site = ctx.get("by_site", {})
    # The per-site kpi block doesn't carry failure reasons; owner supplies them
    # via ctx['per_site_top_reasons'] when available — otherwise skip gracefully.
    reasons = ctx.get("per_site_top_reasons", {})
    if len(reasons) < 3:
        return None
    distinct = len({v for v in reasons.values() if v})
    if distinct < 3:
        return None
    return Finding(
        rule_id="R1",
        realization=(f"Same KPI, {distinct} different problems across {len(reasons)} sites — "
                     "interventions must be site-specific, not portfolio-wide."),
        evidence={"per_site_top_reason": reasons},
    )


def _r2_allocation_gap_concentration(ctx: dict) -> Optional[Finding]:
    ifr = ctx["failure_signatures"]["ifr"]
    pct = ifr.get("allocation_gap_pct")
    if pct is None or pct <= 30:
        return None
    return Finding(
        rule_id="R2",
        realization=(f"Reservation logic needs Oracle hygiene fix, not inventory fix "
                     f"(allocation-gap = {pct}% of misses)."),
        evidence={"allocation_gap_pct": pct, "miss_n": ifr["total_miss"]},
    )


def _r3_recoverable_stockout_cluster(ctx: dict) -> Optional[Finding]:
    rec = ctx["intersections"]["recoverable"]
    if rec.get("blocked_by_match_rate"):
        return None
    pct = rec.get("recoverable_pct") or 0
    n = rec.get("recoverable") or 0
    denom = rec.get("purchased_stockouts") or 0
    if pct <= 70 or denom < 10:
        return None
    return Finding(
        rule_id="R3",
        realization=(f"{n} of {denom} purchased stockouts are PFEP-preventable. "
                     "One-time data fix = compounding ROI."),
        evidence=rec,
    )


def _r4_cadence_collapse(ctx: dict) -> Optional[Finding]:
    cc = ctx["failure_signatures"]["cc"]
    bd = cc.get("business_days") or 0
    ad = cc.get("active_days") or 0
    if bd == 0:
        return None
    ratio = ad / bd
    if ratio >= 0.6:
        return None
    return Finding(
        rule_id="R4",
        realization=(f"Cycle-count program has stopped (active days {ad}/{bd} = "
                     f"{round(100*ratio,1)}%) — accuracy KPIs are directional only."),
        evidence={"active_days": ad, "business_days": bd, "ratio": round(ratio, 3)},
    )


def _r5_voi_gap_reason_code(ctx: dict) -> Optional[Finding]:
    cc = ctx["failure_signatures"]["cc"]
    pop = cc.get("reason_code_populated_pct")
    if pop is None or pop >= 20:
        return None
    return Finding(
        rule_id="R5",
        realization=(f"{round(100 - pop,1)}% of variance is un-attributable "
                     f"(Reason Code populated on {pop}% of adjustments). "
                     "Root-cause targeting is impossible until the field is enforced."),
        evidence={"reason_code_populated_pct": pop},
    )


def _r6_voi_gap_ifr_failure(ctx: dict) -> Optional[Finding]:
    # IFR Failure population lives on the raw dataframe; ctx['ifr_failure_pop_pct']
    # is injected by the findings builder.
    pop = ctx.get("ifr_failure_pop_pct")
    if pop is None or pop >= 20:
        return None
    return Finding(
        rule_id="R6",
        realization=(f"IFR misses can't be themed without the field (populated {pop}%). "
                     "Mandate capture before next review cycle."),
        evidence={"ifr_failure_pop_pct": pop},
    )


def _r7_pfep_forecasting_floor(ctx: dict) -> Optional[Finding]:
    pfep: pd.DataFrame = ctx["pfep"]
    if pfep is None or pfep.empty:
        return None
    ss = pd.to_numeric(pfep["Safety Stock"], errors="coerce")
    abc = pfep["ABC Inventory Catalog"]
    lt = pd.to_numeric(pfep["Processing Lead Time"], errors="coerce")
    populated = (ss.notna() & (ss >= 0)) & abc.notna() & (lt.notna() & (lt > 0))
    rate = float(populated.mean()) if len(pfep) else 0.0
    if rate >= 0.20:
        return None
    return Finding(
        rule_id="R7",
        realization=(f"MRP is running on an empty master — only {round(100*rate,1)}% of "
                     "active items have Safety Stock + ABC + Lead Time all populated. "
                     "PFEP remediation is the prerequisite to any operational fix."),
        evidence={"all_three_populated_pct": round(100 * rate, 1),
                  "active_items": int(len(pfep))},
    )


def _r8_fat_tail(ctx: dict) -> Optional[Finding]:
    dl = ctx["failure_signatures"]["days_late"]
    if not dl.get("fat_tail"):
        return None
    return Finding(
        rule_id="R8",
        realization=(f"Pre-stage the top-20 fat-tail parts (p99 Days Late = "
                     f"{round(dl['p99'],1)}). Cheaper than chasing median cycle-time."),
        evidence=dl,
    )


def _r9_concentrated_customer(ctx: dict) -> Optional[Finding]:
    cust = ctx["centrality"]["customers"]
    if not cust:
        return None
    top = max(cust, key=lambda c: c["pct"])
    if top["pct"] < 30:
        return None
    return Finding(
        rule_id="R9",
        realization=(f"Relationship review with {top['customer']} — kit complexity / "
                     f"order-shape conversation ({top['pct']}% of lates)."),
        evidence=top,
    )


def _r10_vendor_cluster(ctx: dict) -> Optional[Finding]:
    vend = ctx["centrality"]["vendors"]
    if len(vend) < 5:
        return None
    top5 = sum(v["combined"] for v in vend[:5])
    total = max(top5, 1)  # without a global denominator we use top5 itself
    total_all = ctx.get("vendor_total") or total
    share = top5 / max(total_all, 1)
    if share < 0.40:
        return None
    return Finding(
        rule_id="R10",
        realization=("Supplier scorecard program with focused vendor cohort, not full base "
                     f"(top-5 suppliers = {round(100*share,1)}% of combined misses+lates)."),
        evidence={"top5_share_pct": round(100 * share, 1),
                  "top5": vend[:5]},
    )


RULES: list[RuleFn] = [
    _r1_site_signature_divergence,
    _r2_allocation_gap_concentration,
    _r3_recoverable_stockout_cluster,
    _r4_cadence_collapse,
    _r5_voi_gap_reason_code,
    _r6_voi_gap_ifr_failure,
    _r7_pfep_forecasting_floor,
    _r8_fat_tail,
    _r9_concentrated_customer,
    _r10_vendor_cluster,
]


def evaluate_rules(**ctx: Any) -> list[dict]:
    # Caller passes kpis/by_site/failure_signatures/intersections/centrality/pfep/itr/win.
    # Precompute IFR Failure population rate from ITR? No — that's on IFR; we
    # approximate by sentinel if the builder didn't inject it.
    ctx.setdefault("ifr_failure_pop_pct", None)
    findings: list[Finding] = []
    for rule in RULES:
        try:
            f = rule(ctx)
        except Exception as e:  # pragma: no cover — rules must never crash the build
            f = Finding(rule_id=getattr(rule, "__name__", "?"),
                        realization=f"rule error: {e}", evidence={})
        if f is not None:
            findings.append(f)
    return [f.to_dict() for f in findings]
