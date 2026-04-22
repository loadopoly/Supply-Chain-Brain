"""CVaR risk-aware supplier design.

Phase 3.3 — MIT Supply Chain Design Lab. Scenario-based stochastic supplier
selection: for each part we sample joint demand × lead-time × disruption draws
and compute, per supplier, expected cost vs. tail-cost (CVaR_α). Output is a
Pareto frontier the analyst can drill into.
"""
from __future__ import annotations
from typing import Sequence
import numpy as np
import pandas as pd


def cvar(values: np.ndarray, alpha: float = 0.95) -> float:
    if values.size == 0:
        return float("nan")
    vsort = np.sort(values)
    k = max(1, int(np.ceil((1 - alpha) * vsort.size)))
    return float(vsort[-k:].mean())


def supplier_cost_scenarios(
    suppliers: pd.DataFrame, *, n_sims: int = 2000, alpha: float = 0.95, seed: int = 0,
) -> pd.DataFrame:
    """``suppliers`` columns required: ``supplier_key``, ``unit_cost``,
    ``lead_time_mean``, ``lead_time_std``, ``disruption_prob``, ``annual_demand``.
    Returns expected cost, CVaR, and a ``pareto_efficient`` flag."""
    needed = {"supplier_key", "unit_cost", "lead_time_mean", "lead_time_std",
              "disruption_prob", "annual_demand"}
    if not needed.issubset(suppliers.columns):
        return pd.DataFrame({"error": [f"missing: {sorted(needed - set(suppliers.columns))}"]})

    rng = np.random.default_rng(seed)
    rows = []
    for _, r in suppliers.iterrows():
        demand = rng.gamma(shape=4.0, scale=r["annual_demand"] / 4.0, size=n_sims)
        lt = rng.normal(r["lead_time_mean"], max(r["lead_time_std"], 1e-6), size=n_sims).clip(min=1)
        disrupt = rng.random(n_sims) < float(r["disruption_prob"])
        # cost = base unit cost * demand + lead-time penalty + disruption surcharge
        cost = r["unit_cost"] * demand * (1 + 0.005 * lt) * np.where(disrupt, 1.35, 1.0)
        rows.append({
            "supplier_key": r["supplier_key"],
            "expected_cost": float(cost.mean()),
            f"cvar_{int(alpha*100)}": cvar(cost, alpha=alpha),
            "p95_cost": float(np.quantile(cost, 0.95)),
            "disruption_prob": r["disruption_prob"],
        })
    out = pd.DataFrame(rows)
    cvar_col = f"cvar_{int(alpha*100)}"
    out["pareto_efficient"] = _pareto_mask(out[["expected_cost", cvar_col]].values)
    return out.sort_values(["pareto_efficient", "expected_cost"],
                          ascending=[False, True]).reset_index(drop=True)


def _pareto_mask(points: np.ndarray) -> np.ndarray:
    """Min-min Pareto frontier mask."""
    n = points.shape[0]
    eff = np.ones(n, dtype=bool)
    for i in range(n):
        if not eff[i]:
            continue
        dominated = np.all(points <= points[i], axis=1) & np.any(points < points[i], axis=1)
        eff[dominated] = False
    return eff
