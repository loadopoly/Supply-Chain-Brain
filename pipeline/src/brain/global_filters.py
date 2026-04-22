"""Global cross-page filters (date range, site) shared across the Brain.

Reads/writes Streamlit session_state under standardized keys:
    g_site         – manufacturing business unit
    g_date_start   – datetime.date  (inclusive)
    g_date_end     – datetime.date  (inclusive)

Helpers convert these into SQL WHERE fragments compatible with edap_dw_replica
date_keys (yyyymmdd integers) and with python pandas datetime columns.
"""
from __future__ import annotations
from datetime import date, timedelta
from typing import Optional, Tuple
import pandas as pd
import streamlit as st


DEFAULT_LOOKBACK_DAYS = 365


def get_global_window() -> Tuple[date, date]:
    """Return (start, end) dates from session_state, with sensible defaults."""
    end = st.session_state.get("g_date_end") or date.today()
    start = st.session_state.get("g_date_start") or (end - timedelta(days=DEFAULT_LOOKBACK_DAYS))
    return start, end


def date_key_window() -> Tuple[int, int]:
    """Return (start_key, end_key) as yyyymmdd integers."""
    s, e = get_global_window()
    return int(s.strftime("%Y%m%d")), int(e.strftime("%Y%m%d"))


def sql_where_date_key(col: str, alias: str | None = None) -> str:
    """Build SQL WHERE fragment for an integer yyyymmdd column."""
    sk, ek = date_key_window()
    pfx = f"{alias}." if alias else ""
    return f"{pfx}[{col}] BETWEEN {sk} AND {ek}"


def sql_and_date_key(col: str, alias: str | None = None) -> str:
    """Returns ' AND <col> BETWEEN ...' suitable for appending to existing WHERE."""
    return " AND " + sql_where_date_key(col, alias)


def filter_df_by_date(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Filter a pandas DataFrame by the global date window on `date_col`."""
    if df is None or df.empty or date_col not in df.columns:
        return df
    s, e = get_global_window()
    ts = pd.to_datetime(df[date_col], errors="coerce")
    mask = (ts.dt.date >= s) & (ts.dt.date <= e)
    return df[mask].copy()


def render_global_filter_sidebar():
    """Render the standardized Start/End Date pickers in the sidebar."""
    with st.sidebar:
        st.markdown("### 📅 Global Timeline")
        end_default = st.session_state.get("g_date_end", date.today())
        start_default = st.session_state.get(
            "g_date_start", end_default - timedelta(days=DEFAULT_LOOKBACK_DAYS)
        )
        c1, c2 = st.columns(2)
        with c1:
            sd = st.date_input("Start", value=start_default, key="g_date_start_widget")
        with c2:
            ed = st.date_input("End", value=end_default, key="g_date_end_widget")
        if st.session_state.get("g_date_start") != sd or st.session_state.get("g_date_end") != ed:
            st.session_state["g_date_start"] = sd
            st.session_state["g_date_end"] = ed
            for k in list(st.session_state.keys()):
                if k.endswith('_sql') or k == 'otd_where' or k == 'bw_sql' or k == 'eoq_sql':
                    del st.session_state[k]
            st.cache_data.clear()
        st.caption(f"Window: **{(ed - sd).days} d**")
