"""
Phase 1-5 compute + Phase 8a findings JSON (spec §5, §8a).

build_findings(otd, ifr, itr, pfep, site="ALL", anchor=None) -> dict

The returned dict is the canonical payload every downstream artifact reads —
it is the source of truth. Keep it deterministic (seed=9, see spec §6) so
re-runs on identical data are bit-identical.
"""
from __future__ import annotations
from collections import Counter
from datetime import date
from typing import Iterable

import numpy as np
import pandas as pd

from .constants import SEED, ANCHOR_POLICY
from .erp_translation import erp_for_site, normalize_part
from .realizations import evaluate_rules
from .pathways import classify
from .schemas import validate
from .windows import Windows, business_days, default_anchor, mask_between

OTD_GOAL = 95.0
IFR_GOAL = 95.0
CC_GOAL = 95.0
MATCH_RATE_FLOOR = 0.90
VARIANCE_QTY_EPS = 1e-3
VARIANCE_VAL_EPS = 1e-2


# ---------------------------------------------------------------------------
# Phase 1 — ingest + clean (caller owns extraction; this enforces contract)
# ---------------------------------------------------------------------------

def _coerce(df: pd.DataFrame, dataset: str) -> pd.DataFrame:
    from .schemas import DATASETS
    dtypes = DATASETS[dataset]["dtypes"]
    out = df.copy()
    for col, kind in dtypes.items():
        if col not in out.columns:
            continue
        if kind == "datetime":
            out[col] = pd.to_datetime(out[col], errors="coerce")
        elif kind == "float":
            out[col] = pd.to_numeric(out[col], errors="coerce")
        elif kind == "bool01":
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype(int).clip(0, 1)
    # Drop Tableau/PBI footer rows — caller hint §5 Phase 1
    site_col = "Site" if "Site" in out.columns else None
    if site_col:
        out = out[~out[site_col].astype(str).str.startswith(("Applied filters", "Total"), na=False)]
    # Normalize part keys
    for pn_col in ("Part", "Item Name"):
        if pn_col in out.columns:
            out["_pn"] = normalize_part(out[pn_col])
            break
    return out


def _scope(df: pd.DataFrame, site: str) -> pd.DataFrame:
    if site == "ALL" or "Site" not in df.columns:
        return df
    exact = df[df["Site"] == site]
    # Fall back to full df when no exact match — live loader pre-filters by resolved site code
    return exact if not exact.empty else df


# ---------------------------------------------------------------------------
# Phase 2 — KPI computation
# ---------------------------------------------------------------------------

def _otd_pct(otd: pd.DataFrame, lo: date, hi: date) -> tuple[float, int]:
    m = mask_between(otd["Ship Date"], lo, hi)
    lines = int(m.sum())
    if lines == 0:
        return (float("nan"), 0)
    late = int(otd.loc[m, "OTD Miss (Late)"].sum())
    return (100.0 * (1 - late / lines), lines)


def _ifr_pct(ifr: pd.DataFrame, lo: date, hi: date) -> tuple[float, int]:
    m = mask_between(ifr["Order Date"], lo, hi)
    lines = int(m.sum())
    if lines == 0:
        return (float("nan"), 0)
    hits = int(ifr.loc[m, "Hit Miss"].sum())
    return (100.0 * hits / lines, lines)


def _cc_pct(itr: pd.DataFrame, lo: date, hi: date) -> tuple[float, int]:
    m = mask_between(itr["Transaction Date"], lo, hi) & (
        itr["Transaction Type"].astype(str).str.contains("Cycle Count", case=False, na=False)
    )
    sub = itr.loc[m]
    n = int(sub.shape[0])
    if n == 0:
        return (float("nan"), 0)
    nonzero = ((sub["Quantity"].abs() > VARIANCE_QTY_EPS) |
               (sub["Net Dollar"].abs() > VARIANCE_VAL_EPS))
    return (100.0 * (1 - nonzero.sum() / n), n)


def _kpi_block(fn, lo14, hi14, loPR, hiPR, lo90, hi90, goal):
    past, n_past = fn(lo14, hi14)
    prior, _ = fn(loPR, hiPR)
    base90, _ = fn(lo90, hi90)
    delta = None if (np.isnan(past) or np.isnan(prior)) else round(past - prior, 2)
    return {
        "value": None if np.isnan(past) else round(past, 2),
        "prior": None if np.isnan(prior) else round(prior, 2),
        "delta_pp": delta,
        "baseline_90d": None if np.isnan(base90) else round(base90, 2),
        "goal": goal,
        "n": n_past,
    }


# ---------------------------------------------------------------------------
# Phase 3 — failure decomposition
# ---------------------------------------------------------------------------

_OTD_REASON_CLASS = [
    ("WH failed to ship",             "EXECUTION"),
    ("Missing other item on same SO", "KITTING"),
    ("Manufactured not ready",        "SCHEDULING"),
    ("No purchased part",             "SUPPLY"),
]


def _classify_otd_reason(reason: str) -> str:
    if not isinstance(reason, str):
        return "OTHER"
    for prefix, cls in _OTD_REASON_CLASS:
        if reason.lower().startswith(prefix.lower()):
            return cls
    return "OTHER"


def _otd_failure_signature(otd: pd.DataFrame, lo: date, hi: date, top_n: int = 6):
    late = otd[mask_between(otd["Ship Date"], lo, hi) & (otd["OTD Miss (Late)"] == 1)]
    counts = late["Failure Reason"].fillna("(blank)").value_counts().head(top_n)
    total = int(late.shape[0]) or 1
    ranked = [
        {"reason": r, "count": int(n), "pct": round(100.0 * n / total, 1),
         "class": _classify_otd_reason(r)}
        for r, n in counts.items()
    ]
    return {"total_late_lines": int(late.shape[0]), "top_reasons": ranked}


def _ifr_decomposition(ifr: pd.DataFrame, lo: date, hi: date):
    m = mask_between(ifr["Order Date"], lo, hi) & (ifr["Hit Miss"] == 0)
    miss = ifr.loc[m].copy()
    n = int(miss.shape[0])
    if n == 0:
        return {"total_miss": 0, "allocation_gap": 0, "hard_stockout": 0, "covered_miss": 0,
                "allocation_gap_pct": None, "hard_stockout_pct": None, "covered_miss_pct": None}
    oh = miss["On Hand Qty"].fillna(0)
    av = miss["Available Qty"].fillna(0)
    so = miss["SO Qty"].fillna(0)
    hard = int((oh <= 0).sum())
    alloc = int(((oh > 0) & (av < so)).sum())
    covered = int(((oh > 0) & (av >= so)).sum())
    return {
        "total_miss": n,
        "allocation_gap": alloc, "allocation_gap_pct": round(100.0 * alloc / n, 1),
        "hard_stockout": hard,    "hard_stockout_pct":   round(100.0 * hard / n, 1),
        "covered_miss": covered,  "covered_miss_pct":    round(100.0 * covered / n, 1),
    }


def _cc_cadence_and_variance(itr: pd.DataFrame, lo: date, hi: date):
    m = mask_between(itr["Transaction Date"], lo, hi) & (
        itr["Transaction Type"].astype(str).str.contains("Cycle Count", case=False, na=False)
    )
    sub = itr.loc[m].copy()
    if sub.empty:
        return {"active_days": 0, "business_days": business_days(lo, hi),
                "cadence_compliance_pct": 0.0, "absolute_variance_$": 0.0,
                "net_variance_$": 0.0, "repeat_offender_pct": 0.0,
                "reason_code_populated_pct": 0.0}
    sub["_day"] = pd.to_datetime(sub["Transaction Date"]).dt.date
    active = sub["_day"].nunique()
    bd = max(business_days(lo, hi), 1)
    per_item = sub.groupby("Item Name").size()
    repeat_items = per_item[per_item >= 2].index
    repeat_tx = int(sub["Item Name"].isin(repeat_items).sum())
    rc = sub["Transaction Reason Code"]
    rc_pop = float((rc.notna() & (rc.astype(str).str.strip() != "")).mean())
    return {
        "active_days": int(active),
        "business_days": bd,
        "cadence_compliance_pct": round(100.0 * active / bd, 1),
        "absolute_variance_$": round(float(sub["Net Dollar"].abs().sum()), 2),
        "net_variance_$": round(float(sub["Net Dollar"].sum()), 2),
        "repeat_offender_pct": round(100.0 * repeat_tx / max(sub.shape[0], 1), 1),
        "reason_code_populated_pct": round(100.0 * rc_pop, 1),
    }


def _days_late_tail(otd: pd.DataFrame, lo: date, hi: date):
    m = mask_between(otd["Ship Date"], lo, hi) & (otd["OTD Miss (Late)"] == 1)
    dl = pd.to_numeric(otd.loc[m, "Days Late"], errors="coerce").dropna()
    if dl.empty:
        return {"n": 0, "median": None, "p75": None, "p90": None, "p99": None, "max": None,
                "fat_tail": False}
    q = dl.quantile([0.5, 0.75, 0.9, 0.99])
    p99 = float(q.loc[0.99])
    return {
        "n": int(dl.size),
        "median": float(q.loc[0.5]),
        "p75":    float(q.loc[0.75]),
        "p90":    float(q.loc[0.9]),
        "p99":    p99,
        "max":    float(dl.max()),
        "fat_tail": bool(p99 > 30),
    }


# ---------------------------------------------------------------------------
# Phase 4 — cross-dataset intersections
# ---------------------------------------------------------------------------

def _pfep_match_audit(ifr: pd.DataFrame, pfep: pd.DataFrame, lo: date, hi: date):
    m = mask_between(ifr["Order Date"], lo, hi) & (ifr["Hit Miss"] == 0)
    miss = ifr.loc[m].copy()
    if miss.empty or "_pn" not in miss.columns or "_pn" not in pfep.columns:
        return {"miss_n": int(miss.shape[0]), "match_rate": None, "matched": 0,
                "ss_missing_pct": None, "abc_missing_pct": None, "lt_zero_pct": None}
    m_pfep = pfep.drop_duplicates("_pn", keep="first")
    joined = miss.merge(m_pfep, how="left", on="_pn", suffixes=("", "_pfep"))
    matched_mask = joined["Item Name"].notna() if "Item Name" in joined.columns else joined["Safety Stock"].notna()
    matched = joined[matched_mask]
    n_miss = int(miss.shape[0])
    out = {
        "miss_n": n_miss,
        "matched": int(matched.shape[0]),
        "match_rate": round(matched.shape[0] / max(n_miss, 1), 3),
    }
    if not matched.empty:
        ss = pd.to_numeric(matched["Safety Stock"], errors="coerce")
        abc = matched["ABC Inventory Catalog"]
        lt = pd.to_numeric(matched["Processing Lead Time"], errors="coerce")
        out.update({
            "ss_missing_pct":  round(100.0 * float(((ss.isna()) | (ss == 0)).mean()), 1),
            "abc_missing_pct": round(100.0 * float(abc.isna().mean()), 1),
            "lt_zero_pct":     round(100.0 * float(((lt.isna()) | (lt == 0)).mean()), 1),
        })
    else:
        out.update({"ss_missing_pct": None, "abc_missing_pct": None, "lt_zero_pct": None})
    return out


def _recoverable_stockouts(ifr: pd.DataFrame, pfep: pd.DataFrame, lo: date, hi: date,
                           pfep_match_rate: float | None) -> dict:
    if pfep_match_rate is not None and pfep_match_rate < MATCH_RATE_FLOOR:
        return {"blocked_by_match_rate": True, "match_rate": pfep_match_rate,
                "purchased_stockouts": None, "recoverable": None, "recoverable_pct": None}
    m = mask_between(ifr["Order Date"], lo, hi) & (ifr["Hit Miss"] == 0)
    miss = ifr.loc[m].copy()
    purch = miss[miss["Part Fab/Pur"].astype(str).str.lower().str.startswith(("p", "buy"))]
    stockouts = purch[purch["On Hand Qty"].fillna(0) <= 0]
    m_pfep = pfep.drop_duplicates("_pn", keep="first")[["_pn", "Safety Stock", "Make or Buy"]]
    joined = stockouts.merge(m_pfep, on="_pn", how="left")
    ss = pd.to_numeric(joined["Safety Stock"], errors="coerce")
    recoverable = int(((ss.isna()) | (ss == 0)).sum())
    n = int(stockouts.shape[0])
    return {
        "blocked_by_match_rate": False,
        "purchased_stockouts": n,
        "recoverable": recoverable,
        "recoverable_pct": round(100.0 * recoverable / n, 1) if n else 0.0,
    }


def _triple_intersection(otd: pd.DataFrame, ifr: pd.DataFrame, itr: pd.DataFrame,
                         lo90: date, hi90: date):
    cc_parts = set(itr.loc[
        mask_between(itr["Transaction Date"], lo90, hi90)
        & itr["Transaction Type"].astype(str).str.contains("Cycle Count", case=False, na=False),
        "_pn"
    ].dropna())
    ifr_parts = set(ifr.loc[mask_between(ifr["Order Date"], lo90, hi90) & (ifr["Hit Miss"] == 0), "_pn"].dropna())
    otd_parts = set(otd.loc[mask_between(otd["Ship Date"], lo90, hi90) & (otd["OTD Miss (Late)"] == 1), "_pn"].dropna())
    triple = cc_parts & ifr_parts & otd_parts
    return {
        "cc_only": len(cc_parts - ifr_parts - otd_parts),
        "ifr_only": len(ifr_parts - cc_parts - otd_parts),
        "otd_only": len(otd_parts - cc_parts - ifr_parts),
        "cc_and_ifr": len((cc_parts & ifr_parts) - otd_parts),
        "cc_and_otd": len((cc_parts & otd_parts) - ifr_parts),
        "otd_and_ifr": len((otd_parts & ifr_parts) - cc_parts),
        "triple": sorted(triple)[:25],
        "triple_count": len(triple),
    }


# ---------------------------------------------------------------------------
# Phase 5 — centrality
# ---------------------------------------------------------------------------

def _centrality(otd: pd.DataFrame, ifr: pd.DataFrame, itr: pd.DataFrame, lo: date, hi: date):
    # 5a customer
    late = otd[mask_between(otd["Ship Date"], lo, hi) & (otd["OTD Miss (Late)"] == 1)]
    cust = late["Customer"].fillna("(unknown)").value_counts().head(10)
    total = int(late.shape[0]) or 1
    customers = [{"customer": c, "late_lines": int(n), "pct": round(100.0 * n / total, 1),
                  "concentrated": bool(n / total >= 0.05)} for c, n in cust.items()]
    # 5b vendor — purchased lines only
    purch_otd = late[late["Part Pur/Fab"].astype(str).str.lower().str.startswith(("p", "buy"))]
    miss_ifr = ifr[mask_between(ifr["Order Date"], lo, hi) & (ifr["Hit Miss"] == 0)]
    otd_vendor = purch_otd["Supplier Name"].fillna("(unknown)").value_counts() if "Supplier Name" in purch_otd else pd.Series(dtype=int)
    ifr_vendor = miss_ifr["Supplier Name"].fillna("(unknown)").value_counts() if "Supplier Name" in miss_ifr else pd.Series(dtype=int)
    combined = (otd_vendor.add(ifr_vendor, fill_value=0)).astype(int).sort_values(ascending=False).head(5)
    vendors = [{"supplier": s, "combined": int(n),
                "otd_late": int(otd_vendor.get(s, 0)), "ifr_miss": int(ifr_vendor.get(s, 0))}
               for s, n in combined.items()]
    # 5c part — weighted variance
    m = mask_between(itr["Transaction Date"], lo, hi) & itr["Transaction Type"].astype(str).str.contains("Cycle Count", case=False, na=False)
    sub = itr.loc[m]
    if sub.empty:
        parts = []
    else:
        g = sub.groupby("Item Name").agg(adj_count=("Net Dollar", "size"),
                                         abs_var=("Net Dollar", lambda x: x.abs().sum()))
        g["weighted"] = g["abs_var"] * g["adj_count"]
        parts = (g.sort_values("weighted", ascending=False).head(15)
                  .reset_index().to_dict("records"))
        for p in parts:
            for k in ("abs_var", "weighted"):
                p[k] = round(float(p[k]), 2)
            p["adj_count"] = int(p["adj_count"])
    return {"customers": customers, "vendors": vendors, "parts": parts}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_findings(otd_df: pd.DataFrame, ifr_df: pd.DataFrame,
                   itr_df: pd.DataFrame, pfep_df: pd.DataFrame,
                   site: str = "ALL", anchor: date | None = None) -> dict:
    """Run Phases 1-7 and return a JSON-ready dict (Phase 8a)."""
    np.random.seed(SEED)

    for name, df in [("OTD", otd_df), ("IFR", ifr_df), ("ITR", itr_df), ("PFEP", pfep_df)]:
        missing = validate(df, name)
        if missing:
            raise ValueError(f"{name} is missing required fields: {missing}")

    otd  = _scope(_coerce(otd_df,  "OTD"),  site)
    ifr  = _scope(_coerce(ifr_df,  "IFR"),  site)
    itr  = _coerce(itr_df,  "ITR")   # ITR has no Site column
    pfep = _coerce(pfep_df[pfep_df["Item Status"].astype(str).str.contains("Active", na=False)].copy(), "PFEP")

    T = anchor or default_anchor()
    win = Windows(T)
    (lo14, hi14) = win.past_14d
    (loPR, hiPR) = win.prior_14d
    (lo90, hi90) = win.baseline_90d

    kpis = {
        "otd": _kpi_block(lambda a, b: _otd_pct(otd, a, b),
                          lo14, hi14, loPR, hiPR, lo90, hi90, OTD_GOAL),
        "ifr": _kpi_block(lambda a, b: _ifr_pct(ifr, a, b),
                          lo14, hi14, loPR, hiPR, lo90, hi90, IFR_GOAL),
        "cc":  _kpi_block(lambda a, b: _cc_pct(itr, a, b),
                          lo14, hi14, loPR, hiPR, lo90, hi90, CC_GOAL),
    }

    by_site = {}
    if site == "ALL" and "Site" in otd_df.columns:
        for s in sorted(set(otd_df["Site"].dropna()) - {"Total", ""}):
            try:
                by_site[s] = build_findings(
                    otd_df[otd_df["Site"] == s],
                    ifr_df[ifr_df["Site"] == s] if "Site" in ifr_df.columns else ifr_df,
                    itr_df, pfep_df, site=s, anchor=T,
                )["kpis"]
            except Exception as e:  # pragma: no cover — keep portfolio robust
                by_site[s] = {"error": str(e)}

    failure_signatures = {
        "otd": _otd_failure_signature(otd, lo14, hi14),
        "ifr": _ifr_decomposition(ifr, lo14, hi14),
        "cc":  _cc_cadence_and_variance(itr, lo14, hi14),
        "days_late": _days_late_tail(otd, lo14, hi14),
    }

    pfep_audit = _pfep_match_audit(ifr, pfep, lo90, hi90)
    recov = _recoverable_stockouts(ifr, pfep, lo90, hi90, pfep_audit.get("match_rate"))
    triple = _triple_intersection(otd, ifr, itr, lo90, hi90)
    intersections = {"pfep_match": pfep_audit, "recoverable": recov, "triple": triple}

    centrality = _centrality(otd, ifr, itr, lo14, hi14)

    realizations = evaluate_rules(
        kpis=kpis,
        by_site=by_site,
        failure_signatures=failure_signatures,
        intersections=intersections,
        centrality=centrality,
        pfep=pfep,
        itr=itr,
        win=win,
    )
    pathways = classify(realizations)

    return {
        "scope": {
            "site": site,
            "erp": erp_for_site(site) if site != "ALL" else "mixed",
            "datasets": ["OTD", "IFR", "ITR", "PFEP"],
            "anchor_policy": ANCHOR_POLICY,
            "seed": SEED,
            "windows": win.as_dict(),
        },
        "kpis": kpis,
        "by_site": by_site,
        "failure_signatures": failure_signatures,
        "intersections": intersections,
        "centrality": centrality,
        "realizations": realizations,
        "pathways_systemic": pathways["systemic"],
        "pathways_operational": pathways["operational"],
        "roadmap": _default_roadmap(),
        "governance": _default_governance(),
    }


def _default_roadmap() -> dict:
    """Spec §8a roadmap section — filled with spec defaults; renderer may
    customize based on realizations. Kept static here so the payload is
    always complete even with sparse data."""
    return {
        "T+30": ["Close PFEP data gaps on active Buy items (Safety Stock + ABC + LT)",
                 "Enforce Transaction Reason Code on cycle-count adjustments",
                 "Stand up weekly OTD / IFR / CC scorecard with §4 window convention"],
        "T+60": ["Allocation reconciliation batch in Oracle (systemic #4)",
                 "Pick-wave rebalance pilot at highest-lateness site",
                 "Vendor scorecard for top-5 combined OTD+IFR suppliers"],
        "T+90": ["PFEP health review #2 — measure match-rate lift on miss parts",
                 "Extend cycle-count cadence audit to every site",
                 "Pre-stage top-20 fat-tail parts if R8 fired"],
        "T+180":["Kit-complete check at pick release across all sites",
                 "Quarterly customer-relationship review with concentrated accounts",
                 "Re-baseline KPI goals using 90d rolling performance"],
    }


def _default_governance() -> dict:
    return {
        "cadence": "Weekly KPI review; bi-weekly realization review; monthly pathway progress.",
        "targets": {"OTD": OTD_GOAL, "IFR": IFR_GOAL, "CC": CC_GOAL},
        "review_runbook": "PlantReview §6 (60-minute conversation flow).",
    }
