"""
EOQ + Bayesian-Poisson centroidal deviation + LinUCB contextual-bandit
re-ranking (req 1a-1d).

Math
----
Classic EOQ:     Q* = sqrt(2 * D * S / (h * c))
  D = annual demand units, S = ordering cost / order, h = holding rate /year,
  c = unit cost.

Bayesian-Poisson rate (per-period demand):
  Prior  : Gamma(alpha0, beta0)
  Evidence: sum of observations y over n periods → posterior Gamma(alpha0+y, beta0+n)
  Posterior mean rate = (alpha0 + y) / (beta0 + n)
  Annual D_hat = posterior_rate * periods_per_year
  Posterior variance = (alpha0 + y) / (beta0 + n)**2

Centroidal deviation:
  z = (Q_observed - Q*) / sqrt(EOQ-sensitivity-variance)
  We propagate posterior demand variance through the EOQ derivative
  dQ*/dD = sqrt(S / (2 * D * h * c)), so var(Q*) ≈ (dQ*/dD)^2 * var(D).
  Then z is standardized, |z| ranks the worst offenders first.

LinUCB re-ranking (req 1d):
  Treat each part as an arm with feature vector x_p (commodity 1-hot, supplier,
  unit-cost bin, on-hand bucket, prior-OTD bucket). After the user resolves a
  part, log reward = realized $ recovery and update A_a += x x^T, b_a += r x.
  Next ranking score = expected_reward + alpha * sqrt(x^T A^-1 x).
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import numpy as np
import pandas as pd

from . import load_config


# ---------------------------------------------------------------------------
# EOQ + Bayesian deviation
# ---------------------------------------------------------------------------
def eoq(demand_annual: pd.Series, ordering_cost: float,
        holding_rate: float, unit_cost: pd.Series) -> pd.Series:
    d = np.array(pd.to_numeric(demand_annual, errors="coerce").fillna(0), dtype=float)
    c = np.array(pd.to_numeric(unit_cost,     errors="coerce").fillna(0), dtype=float)
    denom = holding_rate * c
    q = np.where((d > 0) & (denom > 0), np.sqrt(2 * d * ordering_cost / np.where(denom > 0, denom, 1.0)), np.nan)
    return pd.Series(q, index=demand_annual.index, name="eoq")


def bayes_poisson_rate(observed_units: pd.Series, periods: pd.Series,
                       alpha0: float, beta0: float) -> tuple[pd.Series, pd.Series]:
    """Returns (posterior_mean, posterior_var) of per-period Poisson rate."""
    y = np.array(pd.to_numeric(observed_units, errors="coerce").fillna(0), dtype=float)
    n = np.array(pd.to_numeric(periods,        errors="coerce").fillna(0), dtype=float)
    a_post = alpha0 + y
    b_post = beta0  + n
    mean = pd.Series(a_post / b_post, index=observed_units.index)
    var  = pd.Series(a_post / (b_post ** 2), index=observed_units.index)
    return mean, var


def centroidal_deviation(q_observed: pd.Series, q_star: pd.Series,
                         demand_var: pd.Series,
                         ordering_cost: float, holding_rate: float,
                         unit_cost: pd.Series) -> pd.Series:
    """Standardized z-score of Q_observed vs EOQ, propagating demand variance."""
    # Force everything to plain float64 — kills pandas NA / StringDtype that
    # would raise "boolean value of NA is ambiguous" inside np.where.
    c      = np.array(pd.to_numeric(unit_cost,   errors="coerce").fillna(0), dtype=float)
    d_safe = np.array(pd.to_numeric(q_star,      errors="coerce").fillna(0), dtype=float)
    dv     = np.array(pd.to_numeric(demand_var,  errors="coerce").fillna(0), dtype=float)
    q_obs  = np.array(pd.to_numeric(q_observed,  errors="coerce").fillna(0), dtype=float)
    qs     = np.array(pd.to_numeric(q_star,      errors="coerce").fillna(0), dtype=float)

    denom       = 2.0 * d_safe * holding_rate * c
    sensitivity = np.where(denom > 0, np.sqrt(ordering_cost / np.where(denom > 0, denom, 1.0)), np.nan)
    var_q       = (sensitivity ** 2) * dv
    sd_q        = np.where(var_q > 0, np.sqrt(np.where(var_q > 0, var_q, 1.0)), np.nan)
    z           = (q_obs - qs) / np.where(np.isfinite(sd_q) & (sd_q > 0), sd_q, np.nan)
    return pd.Series(z, index=q_observed.index, name="dev_z")


# ---------------------------------------------------------------------------
# End-to-end deviation table
# ---------------------------------------------------------------------------
@dataclass
class EOQInputs:
    part_id_col:   str
    demand_col:    str   # observed demand units (sum over `periods_col`)
    periods_col:   str   # number of demand periods observed
    on_hand_col:   str
    open_qty_col:  str
    unit_cost_col: str
    periods_per_year: float = 12.0


def deviation_table(df: pd.DataFrame, inp: EOQInputs,
                    ordering_cost: Optional[float] = None,
                    holding_rate: Optional[float] = None) -> pd.DataFrame:
    cfg = load_config().get("eoq", {})
    S = ordering_cost if ordering_cost is not None else cfg.get("ordering_cost_default", 75.0)
    h = holding_rate if holding_rate is not None else cfg.get("holding_rate_default", 0.22)
    a0 = cfg.get("bayes_prior_alpha", 2.0)
    b0 = cfg.get("bayes_prior_beta", 1.0)

    rate, var = bayes_poisson_rate(df[inp.demand_col], df[inp.periods_col], a0, b0)
    d_hat = rate * inp.periods_per_year
    d_var_annual = var * (inp.periods_per_year ** 2)

    q_star = eoq(d_hat, S, h, df[inp.unit_cost_col])
    q_obs = pd.to_numeric(df[inp.on_hand_col], errors="coerce").fillna(0) \
          + pd.to_numeric(df[inp.open_qty_col], errors="coerce").fillna(0)

    z = centroidal_deviation(q_obs, q_star, d_var_annual, S, h, df[inp.unit_cost_col])

    abs_z = z.abs()
    overstock = (q_obs - q_star).clip(lower=0)
    understock = (q_star - q_obs).clip(lower=0)
    dollar_at_risk = (overstock * pd.to_numeric(df[inp.unit_cost_col], errors="coerce")).fillna(0)

    out = pd.DataFrame({
        "part_id":           df[inp.part_id_col].values,
        "demand_hat_annual": d_hat.values,
        "eoq":               q_star.values,
        "qty_on_hand_plus_open": q_obs.values,
        "dev_z":             z.values,
        "abs_dev_z":         abs_z.values,
        "overstock_units":   overstock.values,
        "understock_units":  understock.values,
        "dollar_at_risk":    dollar_at_risk.values,
    })
    return out.sort_values(["abs_dev_z", "dollar_at_risk"], ascending=[False, False]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# LinUCB contextual-bandit re-ranking (req 1d)
# ---------------------------------------------------------------------------
class LinUCBRanker:
    """
    Disjoint LinUCB. One arm = one part. After the user 'resolves' a part,
    we observe a reward (e.g., realized $ recovery) and update its arm.
    Re-rank uses ucb = theta^T x + alpha * sqrt(x^T A^-1 x).
    """

    def __init__(self, dim: int, alpha: float = 1.0):
        self.dim = dim
        self.alpha = alpha
        self.A: dict[str, np.ndarray] = {}
        self.b: dict[str, np.ndarray] = {}

    def _ensure(self, arm: str) -> None:
        if arm not in self.A:
            self.A[arm] = np.eye(self.dim)
            self.b[arm] = np.zeros(self.dim)

    def update(self, arm: str, x: np.ndarray, reward: float) -> None:
        self._ensure(arm)
        x = x.reshape(-1)
        self.A[arm] += np.outer(x, x)
        self.b[arm] += reward * x

    def ucb(self, arm: str, x: np.ndarray) -> float:
        self._ensure(arm)
        x = x.reshape(-1)
        A_inv = np.linalg.inv(self.A[arm])
        theta = A_inv @ self.b[arm]
        mu = float(theta @ x)
        bonus = self.alpha * float(np.sqrt(x @ A_inv @ x))
        return mu + bonus

    def rerank(self, candidates: pd.DataFrame, feature_cols: list[str],
               id_col: str = "part_id") -> pd.DataFrame:
        if candidates.empty:
            return candidates
        scored = candidates.copy()
        feats = scored[feature_cols].fillna(0.0).to_numpy(dtype=float)
        scored["_ucb"] = [
            self.ucb(str(scored[id_col].iloc[i]), feats[i]) for i in range(len(scored))
        ]
        return scored.sort_values("_ucb", ascending=False).reset_index(drop=True)
