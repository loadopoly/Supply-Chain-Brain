"""
Forward-facing data interface (Digital SC Transformation Lab pattern).

Two stages:
  1. `missingness_profile(df)`     — per-column missing % + dtype + sample
  2. `value_of_information(df, target)` — GBT importance ranking; "fixing
     this column moves the target by ~ X" (ranked)
  3. `mass_impute(df)` — MissForest-style Random-Forest imputation; falls
     back to a per-column median/mode imputer when missingpy is absent.

Optional libraries (graceful degradation):
  xgboost, lightgbm, catboost, missingpy
"""
from __future__ import annotations
from typing import Optional, Sequence
import warnings
import numpy as np
import pandas as pd

# ---- optional imports -------------------------------------------------------
def _try_import(name: str):
    try:
        return __import__(name)
    except Exception:
        return None

xgb = _try_import("xgboost")
lgb = _try_import("lightgbm")
cb  = _try_import("catboost")
missingpy = _try_import("missingpy")


# ---------------------------------------------------------------------------
def missingness_profile(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    n = len(df)
    rows = []
    for c in df.columns:
        s = df[c]
        miss = int(s.isna().sum())
        rows.append({
            "column":      c,
            "dtype":       str(s.dtype),
            "missing":     miss,
            "missing_pct": round(100.0 * miss / n, 2),
            "n_unique":    int(s.nunique(dropna=True)),
            "sample":      ", ".join(map(str, s.dropna().head(3).tolist())),
        })
    return (pd.DataFrame(rows)
              .sort_values("missing_pct", ascending=False)
              .reset_index(drop=True))


# ---------------------------------------------------------------------------
def _pick_booster():
    """Return (name, fit_predict_callable) for whichever GBT is installed."""
    if lgb is not None:
        def go(X, y):
            m = lgb.LGBMRegressor(n_estimators=200, max_depth=6, verbosity=-1)
            m.fit(X, y); return m, m.feature_importances_
        return "lightgbm", go
    if xgb is not None:
        def go(X, y):
            m = xgb.XGBRegressor(n_estimators=200, max_depth=6,
                                 verbosity=0, tree_method="hist")
            m.fit(X, y); return m, m.feature_importances_
        return "xgboost", go
    if cb is not None:
        def go(X, y):
            m = cb.CatBoostRegressor(iterations=200, depth=6, verbose=False)
            m.fit(X, y); return m, m.feature_importances_
        return "catboost", go
    # sklearn fallback
    from sklearn.ensemble import GradientBoostingRegressor
    def go(X, y):
        m = GradientBoostingRegressor(n_estimators=200, max_depth=4)
        m.fit(X, y); return m, m.feature_importances_
    return "sklearn-gbm", go


def value_of_information(df: pd.DataFrame, target_col: str,
                         features: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """
    Train a GBT on (features → target) and report SHAP-like global importance.
    Columns with both high missingness AND high importance are the highest-
    leverage cells the user should fix first.
    """
    if target_col not in df.columns:
        raise KeyError(f"target_col '{target_col}' not in DataFrame")
    feats = list(features) if features else [c for c in df.columns if c != target_col]
    X = df[feats].copy()

    # Convert datetime columns to numeric (epoch nanoseconds) — GBT cannot ingest Timestamps.
    dt_cols = X.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()
    for c in dt_cols:
        X[c] = pd.to_datetime(X[c], errors="coerce").astype("int64", errors="ignore")
    # Detect object columns that look like dates
    for c in X.select_dtypes(include=["object"]).columns:
        try:
            parsed = pd.to_datetime(X[c], errors="coerce")
            if parsed.notna().sum() > 0.5 * len(parsed):
                X[c] = parsed.astype("int64", errors="ignore")
        except Exception:
            pass

    # one-hot encode remaining object columns; numeric as-is
    X = pd.get_dummies(X, drop_first=False, dummy_na=True)
    # Force everything to numeric — drop columns that still aren't
    X = X.apply(pd.to_numeric, errors="coerce")
    X = X.dropna(axis=1, how="all").fillna(0)

    y = pd.to_numeric(df[target_col], errors="coerce")
    keep = y.notna()
    X, y = X[keep], y[keep]
    if len(y) < 30 or X.shape[1] == 0:
        return pd.DataFrame(columns=["column", "importance", "missing_pct", "voi"])

    name, go = _pick_booster()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            model, imp = go(X.values.astype(float), y.values.astype(float))
        except Exception as exc:
            return pd.DataFrame([{"column": "_error", "importance": 0.0,
                                  "missing_pct": 0.0, "voi": 0.0,
                                  "booster": f"{name} failed: {exc}"}])

    # Aggregate one-hot importance back to original columns
    agg: dict[str, float] = {}
    for col, w in zip(X.columns, imp):
        base = col.split("_")[0] if col not in df.columns else col
        # better: longest matching original column
        match = next((f for f in feats if col.startswith(f)), col)
        agg[match] = agg.get(match, 0.0) + float(w)

    miss = missingness_profile(df).set_index("column")["missing_pct"]
    out = pd.DataFrame({"column": list(agg.keys()),
                        "importance": list(agg.values())})
    out["missing_pct"] = out["column"].map(miss).fillna(0.0)
    out["voi"] = out["importance"] * (out["missing_pct"] / 100.0)
    out["booster"] = name
    return out.sort_values("voi", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
def mass_impute(df: pd.DataFrame, max_iter: int = 5) -> pd.DataFrame:
    """MissForest if missingpy is installed; else IterativeImputer + mode."""
    if df is None or df.empty:
        return df
    out = df.copy()
    # Convert datetime cols to numeric epoch so imputers don't choke
    dt_cols = out.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()
    dt_meta = {}
    for c in dt_cols:
        dt_meta[c] = out[c].dtype
        out[c] = pd.to_datetime(out[c], errors="coerce").astype("int64", errors="ignore")
    if missingpy is not None:
        try:
            mf = missingpy.MissForest(max_iter=max_iter)
            arr = mf.fit_transform(out.values)
            res = pd.DataFrame(arr, columns=out.columns, index=out.index)
            for c, dt in dt_meta.items():
                res[c] = pd.to_datetime(res[c], errors="coerce")
            return res
        except Exception:
            pass
    # fallback: numeric → IterativeImputer; categorical → mode
    from sklearn.experimental import enable_iterative_imputer  # noqa
    from sklearn.impute import IterativeImputer
    num_cols = out.select_dtypes(include=["number"]).columns.tolist()
    if num_cols:
        out[num_cols] = IterativeImputer(max_iter=max_iter, random_state=42)\
            .fit_transform(out[num_cols])
    for c in out.columns.difference(num_cols):
        if out[c].isna().any():
            mode = out[c].mode(dropna=True)
            if len(mode):
                out[c] = out[c].fillna(mode.iloc[0])
    for c, dt in dt_meta.items():
        out[c] = pd.to_datetime(out[c], errors="coerce")
    return out
