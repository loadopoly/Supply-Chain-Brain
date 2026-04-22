"""Causal forest on lead-time variability.

Phase 3.1 — MIT Deep Knowledge Lab. Attribution: *why* is supplier-X's lead time
high, controlling for region, mode, lane, and prior OTD bucket? Uses EconML's
CausalForestDML when available, falls back to a permutation-importance gradient
boost so the page degrades rather than crashes.
"""
from __future__ import annotations
from typing import Sequence
import numpy as np
import pandas as pd

try:
    from econml.dml import CausalForestDML  # type: ignore
    HAS_ECONML = True
except Exception:
    HAS_ECONML = False

from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.inspection import permutation_importance


def lead_time_attribution(
    df: pd.DataFrame,
    *,
    outcome: str = "lead_time_days",
    treatment: str = "supplier_key",
    confounders: Sequence[str] = ("region", "mode", "lane", "prior_otd_bucket"),
) -> pd.DataFrame:
    """Return a per-feature attribution table sorted by |effect| descending."""
    cols = [c for c in [outcome, treatment, *confounders] if c in df.columns]
    if outcome not in df.columns or len(cols) < 2:
        return pd.DataFrame(columns=["feature", "effect", "method"])
    work = df[cols].dropna()
    if work.empty:
        return pd.DataFrame(columns=["feature", "effect", "method"])

    y = work[outcome].astype(float).values
    X = pd.get_dummies(work.drop(columns=[outcome]), drop_first=True)

    if HAS_ECONML and treatment in work.columns and work[treatment].nunique() > 1:
        try:
            T = pd.factorize(work[treatment])[0].astype(float)
            est = CausalForestDML(
                model_t=RandomForestRegressor(n_estimators=120, n_jobs=-1, random_state=0),
                model_y=RandomForestRegressor(n_estimators=120, n_jobs=-1, random_state=0),
                discrete_treatment=False, random_state=0,
            )
            est.fit(y, T, X=X.values)
            te = est.effect(X.values)
            out = (pd.DataFrame({"feature": X.columns,
                                 "effect": np.abs(np.corrcoef(X.values.T, te)[:-1, -1])})
                   .assign(method="CausalForestDML"))
            return out.sort_values("effect", ascending=False).reset_index(drop=True)
        except Exception:
            pass

    gbr = GradientBoostingRegressor(random_state=0).fit(X, y)
    pi = permutation_importance(gbr, X, y, n_repeats=8, random_state=0, n_jobs=-1)
    return (pd.DataFrame({"feature": X.columns, "effect": pi.importances_mean,
                          "method": "PermutationImportance(GBR)"})
            .sort_values("effect", ascending=False).reset_index(drop=True))
