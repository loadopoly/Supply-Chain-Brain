"""Graves–Willems guaranteed-service multi-echelon safety stock.

Phase 3.5 — MIT Intelligent Logistics Systems Lab pattern. Each stage i has a
processing time Tᵢ, a service time to its downstream Sᵢ, and an inbound
service time SIᵢ. Net replenishment time at stage i is

    NRTᵢ = SIᵢ + Tᵢ - Sᵢ                    (clipped at 0)

Guaranteed-service safety stock at stage i for service level α is

    SSᵢ = z(α) · σᵢ · √NRTᵢ

This is a single-pass approximation (no graph optimization yet) but is the
right interface for the optimizer to slot into in Phase 4.
"""
from __future__ import annotations
import math
import numpy as np
import pandas as pd
from scipy.stats import norm  # type: ignore


def safety_stock_per_stage(
    stages: pd.DataFrame, *, service_level: float = 0.95,
) -> pd.DataFrame:
    """Required columns: ``stage_id``, ``T_i`` (processing time, days),
    ``S_i`` (downstream service time), ``SI_i`` (inbound service time),
    ``sigma_i`` (per-day demand standard deviation at stage)."""
    needed = {"stage_id", "T_i", "S_i", "SI_i", "sigma_i"}
    if not needed.issubset(stages.columns):
        missing = needed - set(stages.columns)
        return pd.DataFrame({"error": [f"missing columns: {sorted(missing)}"]})
    z = float(norm.ppf(service_level))
    out = stages.copy()
    # Cast all numeric inputs to float to avoid Decimal+float TypeError when
    # source is SQL Server NUMERIC/DECIMAL (returned as decimal.Decimal).
    for col in ("T_i", "S_i", "SI_i", "sigma_i"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype(float)
    out["NRT_i"] = (out["SI_i"] + out["T_i"] - out["S_i"]).clip(lower=0)
    out["safety_stock"] = z * out["sigma_i"] * np.sqrt(out["NRT_i"])
    out["service_level"] = service_level
    return out.sort_values("safety_stock", ascending=False).reset_index(drop=True)


def total_holding_cost(stages: pd.DataFrame, holding_rate: float = 0.22) -> float:
    if "safety_stock" not in stages.columns or "unit_cost" not in stages.columns:
        return float("nan")
    ss = pd.to_numeric(stages["safety_stock"], errors="coerce").astype(float)
    uc = pd.to_numeric(stages["unit_cost"], errors="coerce").astype(float)
    return float((ss * uc * float(holding_rate)).sum())
