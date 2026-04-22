"""
Extractor — pulls DataFrames from Azure SQL or Oracle Fusion.
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
