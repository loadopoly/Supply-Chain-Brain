"""Nightly fact-table builder (Phase 4).

Run as ``python -m pipeline.src.brain.analytics_fact`` (or wire into the
existing `pipeline.py sync` job). Materializes a denormalized
``fact_supply_chain_brain`` from the replica + writes back as a CSV snapshot
under ``pipeline/snapshots/`` so pages can fall back to it when the replica is
unavailable.
"""
from __future__ import annotations
import sys
import time
from pathlib import Path
import pandas as pd

from . import load_config
from .db_registry import bootstrap_default_connectors, read_sql

SNAP_DIR = Path(__file__).resolve().parents[2] / "snapshots"
SNAP_DIR.mkdir(exist_ok=True)


# Column names use EDAP replica schema (verified against schema_cache.json).
# date_key columns are YYYYMMDD integers; TRY_CONVERT handles both int and DATE types.
# receipt_date_key / due_date_key  → fact_po_receipt
# quantity_on_hand                   → fact_inventory_on_hand
# shipped_quantity                   → fact_sales_order_line
_FACT_SQL = """
WITH lt AS (
  SELECT supplier_key, part_key,
         AVG(DATEDIFF(day,
               TRY_CONVERT(date, TRY_CONVERT(varchar(8), due_date_key)),
               TRY_CONVERT(date, TRY_CONVERT(varchar(8), receipt_date_key))) * 1.0) AS lt_mean,
         STDEV(DATEDIFF(day,
               TRY_CONVERT(date, TRY_CONVERT(varchar(8), due_date_key)),
               TRY_CONVERT(date, TRY_CONVERT(varchar(8), receipt_date_key))) * 1.0) AS lt_std,
         COUNT(*) AS n_receipts,
         MAX(TRY_CONVERT(date, TRY_CONVERT(varchar(8), receipt_date_key))) AS last_receipt
  FROM [edap_dw_replica].[fact_po_receipt]
  WHERE receipt_date_key IS NOT NULL AND due_date_key IS NOT NULL
  GROUP BY supplier_key, part_key
),
oh AS (
  SELECT part_key, AVG(quantity_on_hand * 1.0) AS avg_on_hand
  FROM [edap_dw_replica].[fact_inventory_on_hand]
  GROUP BY part_key
),
de AS (
  SELECT part_key, SUM(shipped_quantity) AS annual_demand
  FROM [edap_dw_replica].[fact_sales_order_line]
  WHERE due_date_key >= CONVERT(int, CONVERT(varchar(8), DATEADD(year, -1, GETDATE()), 112))
  GROUP BY part_key
)
SELECT lt.supplier_key, lt.part_key, lt.lt_mean, lt.lt_std, lt.n_receipts,
       lt.last_receipt, oh.avg_on_hand, de.annual_demand
FROM lt LEFT JOIN oh ON oh.part_key = lt.part_key
        LEFT JOIN de ON de.part_key = lt.part_key
"""


def build(connector_name: str = "azure_sql", sql: str | None = None) -> Path:
    bootstrap_default_connectors()
    df = read_sql(connector_name, sql or _FACT_SQL)
    if df.attrs.get("_error"):
        raise RuntimeError(df.attrs["_error"])
    out = SNAP_DIR / f"fact_supply_chain_brain_{time.strftime('%Y%m%d')}.parquet"
    try:
        df.to_parquet(out)
    except Exception:
        out = out.with_suffix(".csv")
        df.to_csv(out, index=False)
    print(f"wrote {len(df):,} rows → {out}")
    return out


def latest_snapshot() -> pd.DataFrame:
    snaps = sorted(SNAP_DIR.glob("fact_supply_chain_brain_*"))
    if not snaps:
        return pd.DataFrame()
    p = snaps[-1]
    return pd.read_parquet(p) if p.suffix == ".parquet" else pd.read_csv(p)


if __name__ == "__main__":                                       # pragma: no cover
    cn = sys.argv[1] if len(sys.argv) > 1 else "azure_sql"
    build(cn)
