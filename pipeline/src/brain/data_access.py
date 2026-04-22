"""
Data access helpers — thin layer over db_registry returning CLEANED DataFrames.
"""
from __future__ import annotations
import hashlib
import time
from typing import Optional
import pandas as pd

from . import load_config
from .db_registry import read_sql
from .cleaning import standard_clean

# Cross-page session cache TTL in seconds (matches @st.cache_data ttl on most pages)
_SESSION_CACHE_TTL = 600

try:
    import streamlit as st
    _HAS_ST = True
except Exception:
    _HAS_ST = False


def _session_get(key: str) -> pd.DataFrame | None:
    """Retrieve a cached DataFrame from Streamlit session_state if fresh."""
    if not _HAS_ST:
        return None
    entry = st.session_state.get(key)
    if entry and (time.monotonic() - entry["ts"]) < _SESSION_CACHE_TTL:
        return entry["df"]
    return None


def _session_set(key: str, df: pd.DataFrame) -> None:
    """Store a DataFrame in Streamlit session_state with a timestamp."""
    if _HAS_ST:
        st.session_state[key] = {"df": df, "ts": time.monotonic()}


def fetch_table(connector: str, qualified: str,
                top: Optional[int] = None,
                where: Optional[str] = None,
                params: list | None = None,
                order_by: Optional[str] = None,
                timeout_s: int = 120) -> pd.DataFrame:
    schema, table = qualified.split(".")[0], qualified.split(".")[-1]
    top_clause   = f"TOP {int(top)} " if top else ""
    where_clause = f"WHERE {where}" if where else ""
    order_clause = f"ORDER BY {order_by}" if order_by else ""
    sql = f"SELECT {top_clause}* FROM [{schema}].[{table}] WITH (NOLOCK) {where_clause} {order_clause}".strip()

    # Cross-page session cache key
    cache_key = "_da_" + hashlib.md5(f"{connector}|{sql}|{params}".encode()).hexdigest()
    cached = _session_get(cache_key)
    if cached is not None:
        return cached

    df = standard_clean(read_sql(connector, sql, params or [], timeout_s=timeout_s))
    _session_set(cache_key, df)
    return df


def fetch_logical(connector: str, logical_name: str,
                  top: Optional[int] = None,
                  where: Optional[str] = None,
                  params: list | None = None,
                  order_by: Optional[str] = None,
                  timeout_s: int = 120) -> pd.DataFrame:
    """Resolve a logical table name from brain.yaml, then fetch + clean."""
    tables = load_config().get("tables", {})
    qualified = tables.get(logical_name)
    if not qualified:
        df = pd.DataFrame()
        df.attrs["_error"] = f"logical table '{logical_name}' not mapped in brain.yaml"
        return df
    return fetch_table(connector, qualified, top=top, where=where,
                       params=params, order_by=order_by, timeout_s=timeout_s)


def query_df(connector: str, sql: str, params: list | None = None,
             timeout_s: int = 120) -> pd.DataFrame:
    df = read_sql(connector, sql, params, timeout_s=timeout_s)
    return standard_clean(df)
