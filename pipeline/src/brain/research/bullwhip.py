"""Bullwhip diagnostic — variance amplification per echelon.

Phase 3.5 — MIT Intelligent Logistics Systems Lab. Lee/Padmanabhan/Whang (1997)
defines bullwhip ratio at echelon e as Var(orders_e) / Var(demand_signal). A
ratio > 1 means amplification (the bullwhip).
"""
from __future__ import annotations
from typing import Mapping
import numpy as np
import pandas as pd


def bullwhip_ratio(demand: pd.Series, orders: pd.Series) -> float:
    d = pd.to_numeric(demand, errors="coerce").dropna()
    o = pd.to_numeric(orders, errors="coerce").dropna()
    if len(d) < 4 or len(o) < 4 or d.var() <= 0:
        return float("nan")
    return float(o.var() / d.var())


def bullwhip_per_echelon(series_by_echelon: Mapping[str, pd.Series],
                        demand_signal: pd.Series) -> pd.DataFrame:
    rows = []
    for ech, s in series_by_echelon.items():
        rows.append({
            "echelon": ech,
            "bullwhip_ratio": bullwhip_ratio(demand_signal, s),
            "var": float(pd.to_numeric(s, errors="coerce").var()),
            "n": int(s.dropna().shape[0]),
        })
    return (pd.DataFrame(rows)
            .sort_values("bullwhip_ratio", ascending=False, na_position="last")
            .reset_index(drop=True))


def bullwhip_heatmap_frame(df: pd.DataFrame, *, time_col: str, qty_col: str,
                          demand_col: str, echelon_col: str) -> pd.DataFrame:
    """Long-form frame ready for plotly imshow / heatmap."""
    needed = {time_col, qty_col, demand_col, echelon_col}
    if not needed.issubset(df.columns):
        return pd.DataFrame()
    df = df.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df["_period"] = df[time_col].dt.to_period("M").astype(str)
    rows = []
    demand_by_period = df.groupby("_period")[demand_col].sum()
    for (ech, period), sub in df.groupby([echelon_col, "_period"]):
        rows.append({echelon_col: ech, "period": period,
                    "ratio": bullwhip_ratio(
                        demand_by_period, pd.Series([sub[qty_col].sum()] * len(demand_by_period)))})
    return pd.DataFrame(rows)
