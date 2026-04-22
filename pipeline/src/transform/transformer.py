"""
Transformer — applies column mappings, type coercion, and data cleaning.
Driven by config/mappings.yaml.
"""

import pandas as pd
import yaml
from pathlib import Path


def _load_mappings() -> list[dict]:
    path = Path(__file__).parent.parent.parent / "config" / "mappings.yaml"
    with open(path) as f:
        return yaml.safe_load(f).get("mappings", [])


def apply_mapping(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """
    Rename columns per mapping definition and drop unmapped columns.
    mapping["columns"] = { "ORACLE_COL": "azure_col", ... }
    """
    col_map = mapping.get("columns", {})
    if not col_map:
        return df

    # Keep only mapped columns that actually exist
    existing = {k: v for k, v in col_map.items() if k in df.columns}
    df = df[list(existing.keys())].rename(columns=existing)
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    General cleaning:
    - Strip leading/trailing whitespace from string columns
    - Coerce obvious date columns
    - Drop fully-duplicate rows
    """
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # Try to parse columns whose names suggest dates
    date_hints = ["date", "time", "created", "updated", "modified"]
    for col in df.columns:
        if any(hint in col.lower() for hint in date_hints):
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass

    df = df.drop_duplicates()
    return df


def transform(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Apply mapping then clean."""
    df = apply_mapping(df, mapping)
    df = clean(df)
    return df
