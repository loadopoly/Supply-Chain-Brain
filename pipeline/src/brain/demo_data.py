"""Live data auto-bootstrap helpers.

Every page calls ``auto_load(sql, connector)`` so it renders on first paint
**without** any user input. Data is **always** pulled from the registered
connectors (Azure SQL replica, Oracle Fusion). When a query fails (e.g. the
table mapping in ``brain.yaml`` doesn't match the live schema), the page shows
the SQL, the live error, and an inline schema browser so the user can see the
real tables/columns and fix the mapping — never falls back to synthetic data.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Iterable
import pandas as pd

from .data_access import query_df
from .db_registry import get as get_connector, read_sql


def _quote_ident(name: str) -> str:
    if not name:
        raise ValueError("SQL identifier cannot be empty")
    return f"[{name.replace(']', ']]')}]"


@dataclass
class LoadResult:
    df: pd.DataFrame
    source: str            # "live" | "error" | "empty"
    error: Optional[str]
    sql: Optional[str]
    connector: str

    @property
    def ok(self) -> bool:
        return self.source == "live" and not self.df.empty


def auto_load(*, sql: str, connector: str = "azure_sql",
              timeout_s: int = 120) -> LoadResult:
    """Run ``sql`` on ``connector``. Always live — no synthetic fallback."""
    try:
        df = query_df(connector, sql, timeout_s=timeout_s)
        err = df.attrs.get("_error") if hasattr(df, "attrs") else None
        if err:
            return LoadResult(df=pd.DataFrame(), source="error", error=err,
                              sql=sql, connector=connector)
        if df.empty:
            return LoadResult(df=df, source="empty", error=None,
                              sql=sql, connector=connector)
        return LoadResult(df=df, source="live", error=None,
                          sql=sql, connector=connector)
    except Exception as exc:
        return LoadResult(df=pd.DataFrame(), source="error", error=str(exc),
                          sql=sql, connector=connector)


def render_diagnostics(result: LoadResult, *, st_module) -> None:
    """When a live load fails, show the SQL, the error, and an inline schema
    browser so the user can find the real table/column names and update
    ``config/brain.yaml``."""
    st = st_module
    if result.source == "live":
        st.success(f"🟢 Live · {result.connector} · {len(result.df):,} rows", icon=None)
        return
    if result.source == "empty":
        st.warning(f"🟡 Live query on **{result.connector}** returned 0 rows. "
                   "Widen the WHERE clause or verify the table mapping in `config/brain.yaml`.")
    else:
        st.error(f"🔴 Live query failed on **{result.connector}** — see SQL + error below.")
        if result.error:
            st.code(result.error, language="text")
    with st.expander("SQL that was executed", expanded=False):
        st.code(result.sql or "(none)", language="sql")
    _render_schema_browser(result.connector, st_module=st)


def _render_schema_browser(connector: str, *, st_module) -> None:
    st = st_module
    with st.expander("🔎 Browse live schema (so you can fix the mapping)", expanded=True):
        try:
            schemas = read_sql(connector,
                "SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES ORDER BY 1")
            if schemas.empty:
                st.caption("No schemas returned (connector may be Oracle Fusion REST — use the Connectors page).")
                return
            sch = st.selectbox("Schema", schemas.iloc[:, 0].tolist(), key=f"_sb_sch_{connector}")
            tables = read_sql(connector,
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME",
                [sch])
            if tables.empty:
                st.caption("No tables in that schema."); return
            tbl = st.selectbox("Table", tables.iloc[:, 0].tolist(), key=f"_sb_tbl_{connector}_{sch}")
            cols = read_sql(connector,
                "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                "ORDER BY ORDINAL_POSITION",
                [sch, tbl])
            st.dataframe(cols, use_container_width=True, hide_index=True)
            sample = read_sql(
                connector,
                f"SELECT TOP 25 * FROM {_quote_ident(sch)}.{_quote_ident(tbl)}",
            )
            st.caption(f"Sample rows from `{sch}.{tbl}`")
            st.dataframe(sample, use_container_width=True, hide_index=True)
        except Exception as exc:
            st.caption(f"Schema browser unavailable: {exc}")


def first_existing_table(connector: str, candidates: Iterable[str]) -> Optional[str]:
    """Return the first ``schema.table`` from ``candidates`` that actually
    exists on the connector. Lets pages probe several mappings without
    bringing the page down."""
    for qualified in candidates:
        if "." not in qualified:
            continue
        sch, tbl = qualified.split(".", 1)
        chk = read_sql(connector,
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            [sch, tbl])
        if not chk.empty:
            return qualified
    return None

