"""
Loader — upserts DataFrames into Azure SQL staging tables using T-SQL MERGE.
Creates the target table automatically if it does not exist.
"""

import pandas as pd
import pyodbc
from typing import Optional


def _quote_ident(name: str) -> str:
    if not name:
        raise ValueError("SQL identifier cannot be empty")
    return f"[{name.replace(']', ']]')}]"


def _py_type_to_sql(dtype) -> str:
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME2"
    if pd.api.types.is_bool_dtype(dtype):
        return "BIT"
    return "NVARCHAR(MAX)"


def ensure_table(conn: pyodbc.Connection, schema: str, table: str, df: pd.DataFrame):
    """Create staging table if it doesn't exist, matching DataFrame schema."""
    qualified_table = f"{_quote_ident(schema)}.{_quote_ident(table)}"
    cols_ddl = ",\n    ".join(
        f"[{col}] {_py_type_to_sql(df[col].dtype)}" for col in df.columns
    )
    # Escape single quotes for T-SQL string literals
    safe_schema = schema.replace("'", "''")
    safe_table = table.replace("'", "''")
    ddl = f"""
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{safe_schema}' AND TABLE_NAME = '{safe_table}'
    )
    BEGIN
        CREATE TABLE {qualified_table} (
            {cols_ddl}
        )
    END
    """
    conn.execute(ddl)
    conn.commit()


def upsert(
    conn: pyodbc.Connection,
    df: pd.DataFrame,
    schema: str,
    table: str,
    key_columns: list[str],
    batch_size: int = 1000,
):
    """
    Upsert df rows into [{schema}].[{table}] using T-SQL MERGE on key_columns.
    Rows are written in batches to a temp table, then merged.
    """
    if df.empty:
        print(f"[Load] No rows to upsert into {schema}.{table}.")
        return

    ensure_table(conn, schema, table, df)
    qualified_table = f"{_quote_ident(schema)}.{_quote_ident(table)}"

    staging = f"#stg_{table}"
    cols_ddl = ",\n    ".join(
        f"[{col}] {_py_type_to_sql(df[col].dtype)}" for col in df.columns
    )
    conn.execute(f"CREATE TABLE {staging} ({cols_ddl})")

    # Batch insert into staging
    placeholders = ", ".join("?" for _ in df.columns)
    col_names = ", ".join(f"[{c}]" for c in df.columns)
    insert_sql = f"INSERT INTO {staging} ({col_names}) VALUES ({placeholders})"

    cursor = conn.cursor()
    rows = [tuple(row) for row in df.itertuples(index=False)]
    for i in range(0, len(rows), batch_size):
        cursor.executemany(insert_sql, rows[i : i + batch_size])
    conn.commit()

    # MERGE from staging into target
    key_match = " AND ".join(f"t.[{k}] = s.[{k}]" for k in key_columns)
    non_key_cols = [c for c in df.columns if c not in key_columns]
    insert_cols = ", ".join(f"[{c}]" for c in df.columns)
    insert_vals = ", ".join(f"s.[{c}]" for c in df.columns)

    if non_key_cols:
        update_set = ", ".join(f"t.[{c}] = s.[{c}]" for c in non_key_cols)
        merge_sql = f"""
        MERGE {qualified_table} AS t
        USING {staging} AS s ON {key_match}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_cols}) VALUES ({insert_vals});
        """
    else:
        # All columns are key columns — INSERT only, no UPDATE needed
        merge_sql = f"""
        MERGE {qualified_table} AS t
        USING {staging} AS s ON {key_match}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_cols}) VALUES ({insert_vals});
        """
    conn.execute(merge_sql)
    conn.execute(f"DROP TABLE {staging}")
    conn.commit()
    print(f"[Load] Upserted {len(df)} rows into {schema}.{table}.")
