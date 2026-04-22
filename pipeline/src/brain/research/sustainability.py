"""Scope-3 freight emissions (GLEC / ISO 14083).

Phase 3.6 — MIT Sustainable Supply Chain Lab. Per-shipment CO₂e estimate from
distance × payload × mode-specific emission factor, plus a supplier-rolled
sustainability score that the procurement multi-dim graph can ingest as a
fourth axis (cost / service / risk / sustainability).

Default emission factors are from the 2023 GLEC Framework summary (g CO₂e per
tonne-km, well-to-wheel). They are deliberately editable in
``config/brain.yaml`` under ``emissions.factors`` so the analyst can swap to a
GHG-protocol-conformant local set without code changes.
"""
from __future__ import annotations
import pandas as pd

try:
    from .. import load_config
except Exception:                                                 # pragma: no cover
    load_config = lambda: {}                                      # type: ignore


_DEFAULT_FACTORS_G_PER_TKM = {
    "truck_ftl":  62.0,
    "truck_ltl":  103.0,
    "rail":       22.0,
    "ocean":      8.0,
    "air":        602.0,
    "intermodal": 35.0,
    "barge":      30.0,
}


def emission_factors() -> dict[str, float]:
    cfg = (load_config() or {}).get("emissions", {}).get("factors") or {}
    out = dict(_DEFAULT_FACTORS_G_PER_TKM)
    out.update({k.lower(): float(v) for k, v in cfg.items()})
    return out


def shipment_emissions(
    df: pd.DataFrame, *, mode_col: str = "mode", distance_km_col: str = "distance_km",
    payload_t_col: str = "payload_t",
) -> pd.DataFrame:
    if df.empty:
        return df.assign(co2e_kg=0.0)
    factors = emission_factors()
    mode = df[mode_col].astype(str).str.lower().str.strip() if mode_col in df.columns else "truck_ftl"
    dist = pd.to_numeric(df.get(distance_km_col, 0), errors="coerce").fillna(0)
    pay = pd.to_numeric(df.get(payload_t_col, 0), errors="coerce").fillna(0)
    factor_g = (mode.map(factors).fillna(factors["truck_ftl"])
                if hasattr(mode, "map") else factors["truck_ftl"])
    out = df.copy()
    out["co2e_kg"] = (dist * pay * factor_g) / 1000.0
    return out


def supplier_sustainability_score(
    shipments: pd.DataFrame, *, supplier_col: str = "supplier_key",
) -> pd.DataFrame:
    """0-100 score; higher == greener (lower CO₂e per tonne-km)."""
    if "co2e_kg" not in shipments.columns or supplier_col not in shipments.columns:
        return pd.DataFrame()
    g = shipments.groupby(supplier_col).agg(
        co2e_total=("co2e_kg", "sum"),
        tkm_total=("payload_t", lambda s: float((s * shipments.loc[s.index, "distance_km"]).sum())),
        shipments=("co2e_kg", "size"),
    ).reset_index()
    g["intensity_g_per_tkm"] = (g["co2e_total"] * 1000.0 / g["tkm_total"].replace(0, pd.NA))
    if g["intensity_g_per_tkm"].notna().any():
        lo, hi = g["intensity_g_per_tkm"].quantile([0.05, 0.95])
        rng = max(hi - lo, 1e-9)
        g["sustainability_score"] = (100 * (1 - ((g["intensity_g_per_tkm"] - lo) / rng))).clip(0, 100)
    else:
        g["sustainability_score"] = pd.NA
    return g.sort_values("sustainability_score", ascending=False, na_position="last").reset_index(drop=True)
