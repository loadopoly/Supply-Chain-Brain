"""
Schema introspection: pattern-match likely columns across schemas/tables so
that pages keep working when columns are renamed or new databases are added.
"""
from __future__ import annotations
from typing import Iterable, Optional
import pandas as pd

from . import load_config
from .db_registry import read_sql


def list_columns(connector: str, schema: str, table: str) -> pd.DataFrame:
    sql = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    """
    return read_sql(connector, sql, [schema, table])


def find_column(columns: Iterable[str], patterns: Iterable[str]) -> Optional[str]:
    """Return the first column whose lower-cased name contains any pattern."""
    cols = list(columns)
    lowered = {c: c.lower() for c in cols}
    for p in patterns:
        p_lc = p.lower()
        for c, lc in lowered.items():
            if p_lc == lc or p_lc in lc:
                return c
    return None


def resolve_columns(columns: Iterable[str],
                    needs: Iterable[str] | None = None) -> dict[str, Optional[str]]:
    """For each logical name, return the actual column name from `columns` or None."""
    cfg = load_config().get("column_patterns", {})
    needs = list(needs) if needs is not None else list(cfg.keys())
    return {name: find_column(columns, cfg.get(name, [name])) for name in needs}


def split_qualified(name: str) -> tuple[str, str]:
    """`'edap_dw_replica.dim_part'` -> `('edap_dw_replica', 'dim_part')`."""
    parts = name.replace("[", "").replace("]", "").split(".")
    if len(parts) == 1:
        return "dbo", parts[0]
    return parts[-2], parts[-1]
