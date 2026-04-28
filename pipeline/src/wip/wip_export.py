"""Excel export — produces a workbook that mirrors Francesco's WIP Aging
Review file (MAKE / BUY Wilson Rd WIP Aging review).

Sheets emitted:
    Instructions          — abbreviated copy of the Lion's instructions list
    WIP Analysis          — main output (KPI header + 12 raw + 12 modified buckets)
    Transaction report    — passthrough of cleaned source transactions
    PIVOT Transaction1    — item × DAYS PAST (Sum of Modified Qty for Aging)
    Manage WO Report      — passthrough Materials Shortage
    MOWR PIVOT            — component × supply_type → Sum(Pending Issue)
    WO History            — passthrough of work order history
    Demand                — passthrough of demand
    Pivot Demand          — item → Sum(Order Quantity)
    Backoffice            — Transaction Type → Modifier reference
"""
from __future__ import annotations

import datetime as _dt
import io
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd

from .wip_aging import (
    AGING_BUCKETS,
    BACKOFFICE_TRANSFER_MODIFIERS,
    WIP_ID_COLUMNS,
    WIPSourceFrames,
    _clean_transactions,
)

INSTRUCTIONS = [
    (1, "Take WIP inventory listing in the format shown in WIP Analysis (Cols A:F)."),
    (2, "Open the WIP Aging Review workbook for the side (MAKE or BUY)."),
    (3, "Copy the data from Cols A:F from the WIP Aging Review."),
    (4, "Run Inventory Transaction Report since 2025-01-01 for the Wilson Rd Org, "
        "transaction type 'Work in Process Material Issue'. Paste in tab "
        "Transaction Report cols A:V. Extend col W formulas down."),
    (5, "Refresh PIVOT Transaction1 (and Pivot Transaction2 when present)."),
    (6, "Extend down formulas G:T."),
    (7, "Copy the calculated values back into the WIP Aging Review."),
    (8, "Append new Manage Work Orders rows for the days since the last run."),
    (9, "Append new Materials Shortage rows; refresh MOWR PIVOT."),
    (10, "Refresh Pivot Demand from the appended Demand from Supply Plan rows."),
]


def _wip_analysis_with_header(df: pd.DataFrame, kpis: Mapping[str, float | int]) -> pd.DataFrame:
    """Insert a 2-row KPI header above the WIP Analysis table.

    Row 0: KPI labels packed across cells (matching Lion's manual header).
    Row 1: KPI values.
    Row 2: blank spacer.
    Row 3: column headers (handled by `to_excel(header=True, startrow=3)`).
    """
    return df  # actual KPI overlay is written cell-by-cell below


def export_workbook(
    wip_analysis: pd.DataFrame,
    sources: WIPSourceFrames,
    kpis: Mapping[str, float | int],
    *,
    side: str = "MAKE",
    as_of: _dt.date | None = None,
    plant: str = "Wilson Rd",
) -> bytes:
    """Render the entire workbook into an in-memory .xlsx and return its bytes."""
    as_of = as_of or _dt.date.today()
    buf = io.BytesIO()

    with pd.ExcelWriter(buf, engine="openpyxl") as xl:
        # --- Instructions ---
        pd.DataFrame(INSTRUCTIONS, columns=["Step", "Action"]).to_excel(
            xl, sheet_name="Instructions", index=False
        )

        # --- WIP Analysis (with KPI overlay rows) ---
        wip_analysis.to_excel(xl, sheet_name="WIP Analysis", index=False, startrow=3)
        ws = xl.sheets["WIP Analysis"]
        ws.cell(row=1, column=1, value=f"{side} {plant} WIP Aging Review — as of {as_of.isoformat()}")
        col_idx = 1
        for label, value in kpis.items():
            ws.cell(row=2, column=col_idx, value=label)
            ws.cell(row=3, column=col_idx, value=value)
            col_idx += 2

        # --- Transaction report (cleaned with Modifier columns) ---
        txn = _clean_transactions(sources.transactions, as_of=as_of)
        txn.to_excel(xl, sheet_name="Transaction report", index=False)

        # --- PIVOT Transaction1 ---
        if not txn.empty and "Item Name" in txn.columns and "DAYS PAST" in txn.columns:
            piv = txn.pivot_table(
                index="Item Name", columns="DAYS PAST",
                values="Modified Qty for Aging", aggfunc="sum", fill_value=0.0,
            ).reset_index()
        else:
            piv = pd.DataFrame()
        piv.to_excel(xl, sheet_name="PIVOT Transaction1", index=False)

        # --- Manage WO Report (Materials Shortage) ---
        sources.wo_materials_shortage.to_excel(
            xl, sheet_name="Manage WO Report", index=False
        )

        # --- MOWR PIVOT ---
        mos = sources.wo_materials_shortage
        if not mos.empty and {"Component", "Supply Type", "Pending Issue"}.issubset(mos.columns):
            mowr = mos.assign(
                _p=pd.to_numeric(mos["Pending Issue"], errors="coerce").fillna(0.0)
            ).pivot_table(
                index="Component", columns="Supply Type",
                values="_p", aggfunc="sum", fill_value=0.0,
            ).reset_index()
        else:
            mowr = pd.DataFrame()
        mowr.to_excel(xl, sheet_name="MOWR PIVOT", index=False)

        # --- WO History ---
        sources.wo_history.to_excel(xl, sheet_name="WO History", index=False)

        # --- Demand ---
        sources.demand.to_excel(xl, sheet_name="Demand", index=False)

        # --- Pivot Demand ---
        dem = sources.demand
        if not dem.empty and {"Item Name", "Order Quantity"}.issubset(dem.columns):
            pdem = dem.assign(
                _q=pd.to_numeric(dem["Order Quantity"], errors="coerce").fillna(0.0)
            ).groupby("Item Name")["_q"].sum().reset_index()
            pdem.columns = ["Item Name", "Total"]
        else:
            pdem = pd.DataFrame(columns=["Item Name", "Total"])
        pdem.to_excel(xl, sheet_name="Pivot Demand", index=False)

        # --- Backoffice (Transaction Type → Modifier) ---
        bo = pd.DataFrame(
            [
                {"Transaction type": k,
                 "Group": "Transfer" if v == 1 else "WIP",
                 "Modifier": v}
                for k, v in BACKOFFICE_TRANSFER_MODIFIERS.items()
            ]
        )
        bo.to_excel(xl, sheet_name="Backoffice", index=False)

    return buf.getvalue()


def export_to_path(
    out_path: str | Path,
    wip_analysis: pd.DataFrame,
    sources: WIPSourceFrames,
    kpis: Mapping[str, float | int],
    *,
    side: str = "MAKE",
    as_of: _dt.date | None = None,
    plant: str = "Wilson Rd",
) -> Path:
    """Convenience: write the workbook to disk and return the Path."""
    blob = export_workbook(wip_analysis, sources, kpis, side=side, as_of=as_of, plant=plant)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(blob)
    return out
