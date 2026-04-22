"""
Consistent data-cleaning step shared by every page (req 2).
"""
from __future__ import annotations
import re
import pandas as pd
import numpy as np


_WS_RE = re.compile(r"\s+")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        _WS_RE.sub("_", str(c).strip()).lower().replace("-", "_")
        for c in df.columns
    ]
    return df


def trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype("string").str.strip()
        df[c] = df[c].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    return df


def coerce_dates(df: pd.DataFrame, hints: tuple[str, ...] = ("date", "_dt")) -> pd.DataFrame:
    for c in df.columns:
        lc = c.lower()
        if not any(h in lc for h in hints):
            continue
        # Integer columns: detect YYYYMMDD pattern (20000101–20991231 range)
        if pd.api.types.is_integer_dtype(df[c]):
            sample = df[c].dropna()
            if not sample.empty:
                minv, maxv = int(sample.min()), int(sample.max())
                if 19700101 <= minv <= 20991231 and 19700101 <= maxv <= 20991231:
                    try:
                        df[c] = pd.to_datetime(df[c].astype(str), format="%Y%m%d", errors="coerce")
                    except Exception:
                        pass
                    continue
            # Otherwise skip integer date-key columns (leave as-is)
            continue
        # Object / float columns: standard parse
        if df[c].dtype == "object" or pd.api.types.is_float_dtype(df[c]):
            try:
                df[c] = pd.to_datetime(df[c], errors="coerce", utc=False)
            except Exception:
                pass
    return df


def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def standard_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Canonical pipeline applied across the brain. Idempotent."""
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    df = df.copy()   # single copy here; all sub-functions mutate in-place
    normalize_columns(df)
    trim_strings(df)
    coerce_dates(df)
    return df


def winsorize(s: pd.Series, lower_q: float = 0.01, upper_q: float = 0.99) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    if s.dropna().empty:
        return s
    lo, hi = s.quantile([lower_q, upper_q])
    return s.clip(lower=lo, upper=hi)


def safe_div(num, den, default=np.nan):
    num = pd.to_numeric(num, errors="coerce")
    den = pd.to_numeric(den, errors="coerce")
    out = np.where((den == 0) | den.isna() | num.isna(), default, num / den)
    return out
