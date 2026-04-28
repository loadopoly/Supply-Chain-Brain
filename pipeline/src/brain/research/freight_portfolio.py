"""Smart freight procurement portfolio + goldfish-memory contract pricing.

Phase 3.4 — MIT FreightLab patterns.

* **Goldfish-memory** — reduce contract rejection probability by aligning
  contract rate with the carrier's recent-period market expectation. We score
  each shipper-carrier pair on the rate-vs-reliability gap (carrier's rejection
  rate at the offered rate vs. its rejection at market mean rate).
* **Smart portfolio** — per-lane mix of contract / mini-bid / spot, optimized
  on volume volatility. High-volatility lanes prefer spot; stable lanes prefer
  contract; medium volatility prefers mini-bid.
"""
from __future__ import annotations
import numpy as np
import pandas as pd


def lane_volatility(df: pd.DataFrame, *, lane_col: str = "lane_id",
                    qty_col: str = "load_count", period_col: str = "period") -> pd.DataFrame:
    if not {lane_col, qty_col, period_col}.issubset(df.columns):
        return pd.DataFrame()
    g = df.groupby([lane_col, period_col])[qty_col].sum().unstack(fill_value=0)
    cv = (g.std(axis=1) / g.mean(axis=1).replace(0, pd.NA)).fillna(0)
    mean = g.mean(axis=1)
    return pd.DataFrame({"lane_id": cv.index, "mean_loads": mean.values,
                        "cv": cv.values}).reset_index(drop=True)


def portfolio_mix(volatility: pd.DataFrame) -> pd.DataFrame:
    """CV thresholds calibrated against FreightLab published recommendations."""
    if volatility.empty:
        return volatility
    out = volatility.copy()
    cv = out["cv"].clip(lower=0)
    contract_w = (1 / (1 + np.exp(8 * (cv - 0.35)))).clip(0, 1)
    spot_w = (1 / (1 + np.exp(-8 * (cv - 0.85)))).clip(0, 1)
    minibid_w = (1 - contract_w - spot_w).clip(lower=0)
    total = (contract_w + spot_w + minibid_w).replace(0, 1)
    out["contract_pct"] = (100 * contract_w / total).round(1)
    out["minibid_pct"]  = (100 * minibid_w  / total).round(1)
    out["spot_pct"]     = (100 * spot_w     / total).round(1)
    out["recommended"] = out[["contract_pct", "minibid_pct", "spot_pct"]].idxmax(axis=1)
    return out.sort_values("mean_loads", ascending=False).reset_index(drop=True)


def goldfish_score(df: pd.DataFrame, *, rate_col: str = "rate", market_col: str = "market_rate",
                  rejection_col: str = "rejection_rate") -> pd.DataFrame:
    """Higher score == more likely to be rejected; recommend rate adjustment."""
    if not {rate_col, market_col, rejection_col}.issubset(df.columns):
        return pd.DataFrame()
    out = df.copy()
    for col in (rate_col, market_col, rejection_col):
      out[col] = pd.to_numeric(out[col], errors="coerce")
    out["rate_gap_pct"] = 100 * (out[rate_col] - out[market_col]) / out[market_col].replace(0, pd.NA)
    out["rejection_score"] = (out[rejection_col].fillna(0) * np.exp(-out["rate_gap_pct"].fillna(0) / 10))
    out["suggested_rate"] = out[market_col] * 1.02
    return out.sort_values("rejection_score", ascending=False).reset_index(drop=True)
