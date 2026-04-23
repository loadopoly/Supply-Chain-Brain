"""
Extractor — pulls DataFrames from Azure SQL, Oracle Fusion, Epicor 9, or SyteLine.
Supports full and incremental (watermark-based) extraction.
"""

import pandas as pd
import pyodbc
from typing import Optional
from src.connections.oracle_fusion import OracleFusionSession


def extract_azure_table(
    conn: pyodbc.Connection,
    schema: str,
    table: str,
    columns: Optional[list[str]] = None,
    where: Optional[str] = None,
    watermark_col: Optional[str] = None,
    watermark_val=None,
    timeout_s: int = 300,
) -> pd.DataFrame:
    """
    Pull a table from Azure SQL into a DataFrame.
    If watermark_col + watermark_val supplied, only rows newer than watermark are fetched.
    timeout_s: query timeout in seconds (default 5 min for ETL workloads).
    """
    col_list = ", ".join(f"[{c}]" for c in columns) if columns else "*"
    query = f"SELECT {col_list} FROM [{schema}].[{table}]"

    conditions = []
    if where:
        conditions.append(f"({where})")
    if watermark_col and watermark_val is not None:
        conditions.append(f"[{watermark_col}] > ?")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    params = [watermark_val] if (watermark_col and watermark_val is not None) else []
    print(f"[Extract Azure] {schema}.{table} ...")
    conn.timeout = timeout_s
    df = pd.read_sql(query, conn, params=params)
    print(f"[Extract Azure] {len(df)} rows.")
    return df


def extract_oracle_table(
    session: OracleFusionSession,
    sql: str,
    max_rows: int = 50_000,
) -> pd.DataFrame:
    """
    Pull data from Oracle Fusion via BIP REST API using arbitrary SQL.
    The SQL must be valid against Oracle's BIP/OTBI reporting model.
    """
    print(f"[Extract Oracle] Running query ...")
    df = session.execute_sql(sql, max_rows=max_rows)
    print(f"[Extract Oracle] {len(df)} rows.")
    return df


def extract_epicor_table(
    conn: pyodbc.Connection,
    table: str,
    site_key: str,
    columns: Optional[list[str]] = None,
    where: Optional[str] = None,
    watermark_col: Optional[str] = None,
    watermark_val=None,
    schema: str = "dbo",
    timeout_s: int = 300,
) -> pd.DataFrame:
    """
    Pull a table from an Epicor 9 SQL Server database into a DataFrame.

    Epicor 9 uses SQL Server with a standard dbo schema.  The site_key is
    used only for logging; the caller passes an already-open connection from
    ``src.connections.epicor.get_connection(site_key)``.

    Args:
        conn:           Open pyodbc connection (from epicor.get_connection).
        table:          Physical Epicor table name (e.g. ``PartCount``, ``RcvDtl``).
        site_key:       Site identifier for log messages (e.g. ``jerome_ave``).
        columns:        Specific columns to select; None = all (*).
        where:          Optional WHERE clause fragment (no WHERE keyword).
        watermark_col:  Column for incremental extraction.
        watermark_val:  Lower-bound value for watermark (exclusive >).
        schema:         SQL Server schema (default ``dbo``).
        timeout_s:      Query timeout in seconds.

    Returns:
        DataFrame with physical column names.  Apply
        ``transformer.apply_mapping()`` with the relevant mappings.yaml block
        to convert to canonical names before passing to Brain analytics.
    """
    col_list = ", ".join(f"[{c}]" for c in columns) if columns else "*"
    query = f"SELECT {col_list} FROM [{schema}].[{table}]"

    conditions = []
    if where:
        conditions.append(f"({where})")
    if watermark_col and watermark_val is not None:
        conditions.append(f"[{watermark_col}] > ?")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    params = [watermark_val] if (watermark_col and watermark_val is not None) else []
    print(f"[Extract Epicor {site_key}] {schema}.{table} ...")
    conn.timeout = timeout_s
    df = pd.read_sql(query, conn, params=params)
    print(f"[Extract Epicor {site_key}] {len(df)} rows.")
    return df


def extract_syteline_table(
    conn: pyodbc.Connection,
    table: str,
    site_key: str,
    columns: Optional[list[str]] = None,
    where: Optional[str] = None,
    watermark_col: Optional[str] = None,
    watermark_val=None,
    schema: str = "dbo",
    timeout_s: int = 300,
) -> pd.DataFrame:
    """
    Pull a table from a SyteLine (CloudSuite Industrial) SQL Server database.

    SyteLine uses SQL Server with company-specific databases and a dbo schema.
    Interface is identical to ``extract_epicor_table``; kept separate for
    log clarity and future schema divergence.

    Args:
        conn:           Open pyodbc connection (from syteline.get_connection).
        table:          Physical SyteLine table name (e.g. ``cc_trn``, ``item``).
        site_key:       Site identifier for log messages (e.g. ``st_cloud``).
        columns:        Specific columns to select; None = all (*).
        where:          Optional WHERE clause fragment (no WHERE keyword).
        watermark_col:  Column for incremental extraction.
        watermark_val:  Lower-bound value for watermark (exclusive >).
        schema:         SQL Server schema (default ``dbo``).
        timeout_s:      Query timeout in seconds.

    Returns:
        DataFrame with physical column names.  Apply
        ``transformer.apply_mapping()`` with the relevant mappings.yaml block
        to convert to canonical names before passing to Brain analytics.
    """
    col_list = ", ".join(f"[{c}]" for c in columns) if columns else "*"
    query = f"SELECT {col_list} FROM [{schema}].[{table}]"

    conditions = []
    if where:
        conditions.append(f"({where})")
    if watermark_col and watermark_val is not None:
        conditions.append(f"[{watermark_col}] > ?")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    params = [watermark_val] if (watermark_col and watermark_val is not None) else []
    print(f"[Extract SyteLine {site_key}] {schema}.{table} ...")
    conn.timeout = timeout_s
    df = pd.read_sql(query, conn, params=params)
    print(f"[Extract SyteLine {site_key}] {len(df)} rows.")
    return df

