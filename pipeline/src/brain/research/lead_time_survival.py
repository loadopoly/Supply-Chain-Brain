"""Survival analysis on PO receipts → risk-adjusted lead time.

Phase 3.5 — MIT Intelligent Logistics Systems Lab. Kaplan-Meier for the marginal
distribution and Cox PH for supplier-/lane-conditional hazard. Falls back to
empirical quantiles when ``lifelines`` is not installed so the dashboard always
shows *something*.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Sequence
import numpy as np
import pandas as pd

try:
    from lifelines import KaplanMeierFitter, CoxPHFitter  # type: ignore
    HAS_LIFELINES = True
except Exception:
    HAS_LIFELINES = False


@dataclass
class LeadTimeRisk:
    median: float
    p90: float
    p95: float
    n: int
    method: str


def km_lead_time(durations: pd.Series, events: pd.Series | None = None) -> LeadTimeRisk:
    d = pd.to_numeric(durations, errors="coerce").dropna()
    if d.empty:
        return LeadTimeRisk(np.nan, np.nan, np.nan, 0, "empty")
    e = (pd.to_numeric(events, errors="coerce").fillna(1).astype(int)
         if events is not None else pd.Series(1, index=d.index))
    if HAS_LIFELINES:
        try:
            km = KaplanMeierFitter().fit(d, event_observed=e)
            sf = km.survival_function_.iloc[:, 0]
            def pq(p):
                target = 1 - p
                below = sf[sf <= target]
                return float(below.index[0]) if len(below) else float(d.max())
            return LeadTimeRisk(pq(0.5), pq(0.9), pq(0.95), len(d), "KaplanMeier")
        except Exception:
            pass
    return LeadTimeRisk(float(d.median()), float(d.quantile(0.9)), float(d.quantile(0.95)),
                        len(d), "EmpiricalQuantile")


def per_group_lead_time(
    df: pd.DataFrame, group_cols: Sequence[str], duration_col: str,
    event_col: str | None = None, min_n: int = 5,
) -> pd.DataFrame:
    if duration_col not in df.columns:
        return pd.DataFrame()
    rows = []
    for keys, sub in df.groupby(list(group_cols)):
        if len(sub) < min_n:
            continue
        r = km_lead_time(sub[duration_col], sub[event_col] if event_col else None)
        rec = dict(zip(group_cols, keys if isinstance(keys, tuple) else (keys,)))
        rec.update({"median_lt": r.median, "p90_lt": r.p90, "p95_lt": r.p95,
                    "n": r.n, "method": r.method})
        rows.append(rec)
    return (pd.DataFrame(rows).sort_values("p95_lt", ascending=False).reset_index(drop=True)
            if rows else pd.DataFrame())


def cox_lead_time(
    df: pd.DataFrame, duration_col: str, event_col: str | None,
    covariates: Sequence[str],
) -> pd.DataFrame:
    if not HAS_LIFELINES or duration_col not in df.columns:
        return pd.DataFrame(columns=["covariate", "hazard_ratio", "p", "method"])
    cov = [c for c in covariates if c in df.columns]
    work = df[[duration_col] + cov].copy()
    if event_col and event_col in df.columns:
        work["_event"] = df[event_col].fillna(1).astype(int)
    else:
        work["_event"] = 1
    work = pd.get_dummies(work, columns=[c for c in cov if work[c].dtype == "object"],
                          drop_first=True).dropna()
    if work.empty:
        return pd.DataFrame(columns=["covariate", "hazard_ratio", "p", "method"])
    try:
        cph = CoxPHFitter(penalizer=0.01).fit(work, duration_col=duration_col, event_col="_event")
        s = cph.summary[["exp(coef)", "p"]].reset_index().rename(
            columns={"covariate": "covariate", "exp(coef)": "hazard_ratio"})
        s["method"] = "CoxPH"
        return s.sort_values("p")
    except Exception:
        return pd.DataFrame(columns=["covariate", "hazard_ratio", "p", "method"])
