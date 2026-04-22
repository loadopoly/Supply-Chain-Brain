"""
Reconciler — compares records between Oracle Fusion and Azure SQL
on shared key columns and reports NEW / UPDATED / DELETED / IN_SYNC status.
"""

import pandas as pd
from typing import Optional
from pathlib import Path


STATUS_NEW = "NEW_IN_ORACLE"
STATUS_DELETED = "DELETED_IN_ORACLE"
STATUS_UPDATED = "UPDATED"
STATUS_IN_SYNC = "IN_SYNC"


def reconcile(
    oracle_df: pd.DataFrame,
    azure_df: pd.DataFrame,
    key_columns: list[str],
    compare_columns: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Compare two DataFrames on key_columns.
    Returns a DataFrame with an added '__status__' column.

    compare_columns: subset of columns to check for updates (default: all non-key cols).
    """
    oracle_df = oracle_df.copy()
    azure_df = azure_df.copy()

    oracle_df["__src__"] = "oracle"
    azure_df["__src__"] = "azure"

    merged = oracle_df.merge(azure_df, on=key_columns, how="outer", suffixes=("_oracle", "_azure"))

    if compare_columns is None:
        # Detect shared non-key columns
        oracle_cols = set(oracle_df.columns) - {"__src__"} - set(key_columns)
        azure_cols = set(azure_df.columns) - {"__src__"} - set(key_columns)
        compare_columns = list(oracle_cols & azure_cols)

    statuses = []
    for _, row in merged.iterrows():
        src_oracle = row.get("__src___oracle")
        src_azure = row.get("__src___azure")

        if pd.isna(src_azure):
            statuses.append(STATUS_NEW)
        elif pd.isna(src_oracle):
            statuses.append(STATUS_DELETED)
        else:
            changed = False
            for col in compare_columns:
                o_val = row.get(f"{col}_oracle")
                a_val = row.get(f"{col}_azure")
                if str(o_val) != str(a_val):
                    changed = True
                    break
            statuses.append(STATUS_UPDATED if changed else STATUS_IN_SYNC)

    merged["__status__"] = statuses
    merged = merged.drop(columns=["__src___oracle", "__src___azure"], errors="ignore")
    return merged


def print_summary(reconciled: pd.DataFrame):
    counts = reconciled["__status__"].value_counts()
    print("\n[Reconciliation Summary]")
    for status, count in counts.items():
        print(f"  {status}: {count}")
    print()


def export_report(reconciled: pd.DataFrame, path: str = "reconciliation_report.xlsx"):
    reconciled.to_excel(path, index=False)
    print(f"[Reconcile] Report written to {path}")
