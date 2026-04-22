"""
IPS Freight Platform external connector (req 5).

Hits https://ips-freight-api.onrender.com — the user's existing freight
dashboard. We expose generic `get_json(path)` plus a stub for the FreightLab
ghost-lane detector to demonstrate the cross-app analytics pattern.
"""
from __future__ import annotations
import os
from typing import Optional
import pandas as pd
import requests

from . import load_config


def _config() -> dict:
    return (load_config().get("external_apps", {}) or {}).get("ips_freight", {})


def is_enabled() -> bool:
    return bool(_config().get("enabled", False))


def base_url() -> str:
    return _config().get("base_url", "https://ips-freight-api.onrender.com").rstrip("/")


def _headers() -> dict:
    env_key = _config().get("auth_env", "IPS_FREIGHT_TOKEN")
    token = os.environ.get(env_key)
    h = {"Accept": "application/json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def get_json(path: str, params: dict | None = None) -> dict | list | None:
    if not is_enabled():
        return None
    url = f"{base_url()}/{path.lstrip('/')}"
    try:
        r = requests.get(url, headers=_headers(), params=params,
                         timeout=int(_config().get("timeout_s", 20)))
        if r.status_code == 200 and r.headers.get("content-type", "").startswith("application/json"):
            return r.json()
        return {"_status": r.status_code, "_text": r.text[:500]}
    except Exception as exc:
        return {"_error": str(exc)}


def health() -> dict:
    """Quick reachability probe shown on the Connectors page."""
    if not is_enabled():
        return {"enabled": False}
    try:
        r = requests.get(base_url(), timeout=8)
        return {"enabled": True, "url": base_url(), "status": r.status_code}
    except Exception as exc:
        return {"enabled": True, "url": base_url(), "error": str(exc)}


# ---------------------------------------------------------------------------
# FreightLab ghost-lane detector (Phase 3 stub — demonstrates the pattern)
# ---------------------------------------------------------------------------
def ghost_lane_candidates(po_receipts: pd.DataFrame,
                          contract_lanes: Optional[pd.DataFrame] = None,
                          inactive_days: int = 90) -> pd.DataFrame:
    """
    MIT FreightLab finding: up to 70% of contracted lanes go unused.
    Heuristic V0: a lane is a ghost candidate if it has a contract but no
    receipts in the last `inactive_days`. If contract data is unavailable
    (IPS Freight not reachable yet), fall back to inferring lanes from PO
    receipts and flagging the longest-dormant.
    """
    if po_receipts is None or po_receipts.empty:
        return pd.DataFrame()

    df = po_receipts.copy()
    date_col = next((c for c in df.columns if "receipt" in c.lower() and "date" in c.lower()),
                    next((c for c in df.columns if "date" in c.lower()), None))
    if not date_col:
        return pd.DataFrame()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    lane_cols = [c for c in df.columns if c in ("origin", "destination", "supplier_key", "lane_id")]
    if not lane_cols:
        lane_cols = [c for c in df.columns if "supplier" in c.lower()][:1]
    if not lane_cols:
        return pd.DataFrame()

    grp = df.groupby(lane_cols, dropna=False)[date_col].max().reset_index(name="last_receipt")
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=inactive_days)

    if contract_lanes is not None and not contract_lanes.empty:
        merged = contract_lanes.merge(grp, on=lane_cols, how="left")
        merged["dormant"] = merged["last_receipt"].isna() | (merged["last_receipt"] < cutoff)
        return merged.sort_values("dormant", ascending=False).reset_index(drop=True)

    grp["dormant"] = grp["last_receipt"] < cutoff
    return grp.sort_values("last_receipt").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Phase 3.4 — gradient-boosted survival on contract-vs-actual volume
# ---------------------------------------------------------------------------
def ghost_lane_survival(contract_lanes: pd.DataFrame, po_receipts: pd.DataFrame,
                        *, horizon_days: int = 30) -> pd.DataFrame:
    """Return per-lane probability of inactivation within ``horizon_days``.

    Tries scikit-survival's GradientBoostingSurvivalAnalysis when installed;
    otherwise falls back to a logistic regression on (days_since_receipt,
    historical_load_count, contract_volume). In all cases the dataframe is
    sorted by ``inactivation_prob`` descending so the worst offenders sit at
    the top of the dashboard.
    """
    if contract_lanes is None or contract_lanes.empty:
        return pd.DataFrame()
    df = contract_lanes.copy()
    if po_receipts is not None and not po_receipts.empty:
        date_col = next((c for c in po_receipts.columns
                        if "receipt" in c.lower() and "date" in c.lower()), None)
        lane_cols = [c for c in po_receipts.columns
                    if c in ("origin", "destination", "lane_id", "supplier_key")
                    and c in df.columns]
        if date_col and lane_cols:
            po_receipts = po_receipts.copy()
            po_receipts[date_col] = pd.to_datetime(po_receipts[date_col], errors="coerce")
            agg = (po_receipts.groupby(lane_cols)
                   .agg(last_receipt=(date_col, "max"),
                        load_count=(date_col, "count")).reset_index())
            df = df.merge(agg, on=lane_cols, how="left")
    df["last_receipt"] = pd.to_datetime(df.get("last_receipt"), errors="coerce")
    df["days_since"] = (pd.Timestamp.now().normalize() - df["last_receipt"]).dt.days.fillna(9999)
    df["load_count"] = df.get("load_count", 0).fillna(0)
    contract_vol = pd.to_numeric(df.get("contract_volume", df.get("contracted_loads", 0)),
                                errors="coerce").fillna(0)

    try:                                                          # sksurv branch
        from sksurv.ensemble import GradientBoostingSurvivalAnalysis  # type: ignore
        from sksurv.util import Surv  # type: ignore
        import numpy as np
        observed = (df["days_since"] > horizon_days).values.astype(bool)
        time_to = df["days_since"].clip(lower=1).values.astype(float)
        X = df[["days_since", "load_count"]].assign(contract_vol=contract_vol).values
        y = Surv.from_arrays(event=observed, time=time_to)
        m = GradientBoostingSurvivalAnalysis(n_estimators=120).fit(X, y)
        risk = m.predict(X)
        df["inactivation_prob"] = (risk - risk.min()) / max(risk.max() - risk.min(), 1e-9)
        df["model"] = "GradientBoostingSurvival"
    except Exception:
        import numpy as np
        # Logistic-shaped fallback: longer dormancy + lower load_count → higher prob
        z = (df["days_since"] / max(horizon_days, 1)) - (df["load_count"] / 10) - (contract_vol / 100)
        df["inactivation_prob"] = (1 / (1 + np.exp(-z))).clip(0, 1)
        df["model"] = "LogisticFallback"

    df["horizon_days"] = horizon_days
    return df.sort_values("inactivation_prob", ascending=False).reset_index(drop=True)
