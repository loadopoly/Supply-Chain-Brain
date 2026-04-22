"""Hierarchical empirical-Bayes EOQ pooling.

Phase 3.1 — MIT Deep Knowledge Lab pattern. Slow-moving parts borrow strength
from neighboring parts within the same commodity / supplier through a
James-Stein-style shrinkage of Poisson rates toward the group mean.

Math (closed-form normal-normal hierarchical model on log-rate):

    yᵢ = log(λ̂ᵢ + 1)            sample log-rate for part i
    σ²ᵢ = 1 / (n̂ᵢ + 1)            sampling variance proxy
    μ̂  = Σᵢ wᵢ yᵢ / Σᵢ wᵢ        group prior mean (precision-weighted)
    τ²  = max(0, Var(yᵢ) - mean(σ²ᵢ))  between-part variance
    ŷᵢ  = (τ² yᵢ + σ²ᵢ μ̂) / (τ² + σ²ᵢ)

Returns shrunken rates and the shrinkage weight per part so the UI can show
how much a part's recommendation depends on its neighbors.
"""
from __future__ import annotations
import numpy as np
import pandas as pd


def shrink_rates(
    df: pd.DataFrame,
    *,
    rate_col: str = "demand_hat_annual",
    count_col: str = "obs_count",
    group_col: str = "commodity",
) -> pd.DataFrame:
    """Empirical-Bayes shrink ``rate_col`` toward the per-group mean.

    Returns the input frame with three new columns:
    ``rate_shrunk``, ``shrink_weight`` (∈ [0,1], 1 == fully pooled), ``group_mean``.
    """
    if df.empty or rate_col not in df.columns:
        return df.assign(rate_shrunk=np.nan, shrink_weight=np.nan, group_mean=np.nan)

    out = df.copy()
    if group_col not in out.columns:
        out[group_col] = "_global"
    out[count_col] = out.get(count_col, pd.Series(1, index=out.index)).fillna(1).clip(lower=1)
    y = np.log(out[rate_col].astype(float).clip(lower=0) + 1.0)
    sig2 = 1.0 / (out[count_col].astype(float) + 1.0)

    pieces = []
    for grp, idx in out.groupby(group_col).groups.items():
        yi = y.loc[idx]; si = sig2.loc[idx]
        w = 1.0 / si
        mu = float(np.sum(w * yi) / np.sum(w))
        tau2 = max(0.0, float(yi.var(ddof=0) - si.mean()))
        denom = (tau2 + si)
        weight = si / denom            # 0 == no shrinkage, 1 == fully pooled
        y_hat = (tau2 * yi + si * mu) / denom
        pieces.append(pd.DataFrame({
            "rate_shrunk": np.expm1(y_hat),
            "shrink_weight": weight,
            "group_mean": np.expm1(mu),
        }, index=idx))
    sh = pd.concat(pieces).reindex(out.index)
    return pd.concat([out, sh], axis=1)
