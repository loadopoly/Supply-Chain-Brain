"""WIP Aging engine — programmatic recreation of Francesco Bernacchia's manual
Excel workflow shown in the 2026-04-28 WIP Review session.

The Lion's pipeline (per the recording transcript) is:

    Inputs (5 Oracle Fusion BIP reports)
        1. Astec Inventory On Hand with Cost Detail by Locator   → seeds A:G
        2. Astec Inventory Transaction Report                     → 'Transaction report'
        3. Demand from Supply Plan                                → 'Demand'
        4. Manage Work Orders Reports — Materials Shortage        → 'Manage WO Report'
        5. Manage Work Orders Reports — Work Orders               → 'WO History'

    Pivot tables (rebuilt programmatically)
        PIVOT Transaction1   item × DAYS PAST → Sum(Modified Qty for Aging)
        MOWR PIVOT           component × supply_type → Sum(pending_issue)
        Pivot Demand         item → Sum(order_qty)

    WIP Analysis (output)
        Cols A:G  identification + cost + on-hand + extended cost
        Cols H:S  aging buckets 30..360 days — RAW Quantity
        Cols T:AE aging buckets 30..360 days — Modified Qty (Backoffice cleaned)
        Cols AF:AH  pending issue, demand, write-off flag
        KPIs: TOTAL extended cost, WRITE OFF POTENTIAL, CLEANED UP, COMPLETED IN LAST

The "Modified Qty for Aging" cleaning rule comes from the Backoffice tab in
the source workbook: any transaction whose type lands in BACKOFFICE_TRANSFER_MODIFIERS
is NOT real consumption — it gets a modifier of 1 (transfer) so its absolute
qty is netted out of aging totals. Real WIP issues use modifier 0 (kept as-is).
"""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Optional

import numpy as np
import pandas as pd


# Aging cut-offs the Lion uses on the WIP Analysis tab (cols H..S / T..AE).
AGING_BUCKETS: tuple[int, ...] = (30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 360)


# Backoffice tab — Transaction Type → Modifier (1 = transfer, will be netted).
# Mirrors the 'Backoffice' sheet inside Lion's BUY/MAKE WIP Aging Review files.
BACKOFFICE_TRANSFER_MODIFIERS: dict[str, int] = {
    "Account Alias Issue": 1,
    "Account Alias Receipt": 1,
    "Cycle Count Adjustment": 1,
    "Direct Organization Transfer": 1,
    "Intransit Receipt": 1,
    "Intransit Shipment": 1,
    "Miscellaneous Issue": 1,
    "Miscellaneous Receipt": 1,
    "Subinventory Transfer": 1,
    "Staging Transfer": 1,
    # WIP material moves stay un-modified (modifier 0)
    "Work in Process Material Issue": 0,
    "Work in Process Material Return": 0,
    "Work in Process Negative Component Issue": 0,
    "Work in Process Component Return": 0,
    "Work in Process Product Completion": 0,
}


# Column order the Lion uses on WIP Analysis (matches the .xlsx exactly).
WIP_ID_COLUMNS = [
    "Item Name",
    "Item Description",
    "Item Cost",
    "Buy Make",
    "Supply type",
    "Sum of Quantity Onhand",
    "Sum of Extended Cost",
]


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

@dataclass
class WIPSourceFrames:
    """Container for the 5 source dataframes feeding the WIP Aging engine."""

    inventory_onhand: pd.DataFrame      # from 'Astec Inventory On Hand with Cost Detail by Locator'
    transactions: pd.DataFrame          # from 'Astec Inventory Transaction Report'
    demand: pd.DataFrame                # from 'Demand from Supply Plan'
    wo_materials_shortage: pd.DataFrame # from 'Manage Work Orders Reports — Materials Shortage'
    wo_history: pd.DataFrame            # from 'Manage Work Orders Reports — Work Orders'


def _read_excel_smart(src, sheet=None, header_row: int | None = None) -> pd.DataFrame:
    """Read an Excel sheet, hunting for the real header row when needed.

    Lion's source reports prepend a banner ('Astec Inventory ... Report', etc.)
    before the column headers. We auto-detect the header row by searching for
    one of the well-known column tokens.
    """
    raw = pd.read_excel(src, sheet_name=sheet or 0, header=None)
    if header_row is None:
        markers = {
            "Item Name", "Transaction ID", "Work Order", "Planning Serial No",
            "Component", "Inventory Item Description",
        }
        for i in range(min(20, len(raw))):
            row_vals = {str(v).strip() for v in raw.iloc[i].tolist() if pd.notna(v)}
            if row_vals & markers:
                header_row = i
                break
        if header_row is None:
            header_row = 0
    df = pd.read_excel(src, sheet_name=sheet or 0, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]
    return df.dropna(how="all")


def load_source(
    inventory_onhand: str | Path | pd.DataFrame,
    transactions: str | Path | pd.DataFrame,
    demand: str | Path | pd.DataFrame,
    wo_materials_shortage: str | Path | pd.DataFrame,
    wo_history: str | Path | pd.DataFrame,
) -> WIPSourceFrames:
    """Load all five source reports from either DataFrame, path, or file-like.

    Each argument can be:
      - a pandas.DataFrame already loaded
      - a path / file-like to a workbook (the first sheet is read)
    """

    def _coerce(x) -> pd.DataFrame:
        if isinstance(x, pd.DataFrame):
            return x.copy()
        return _read_excel_smart(x)

    return WIPSourceFrames(
        inventory_onhand=_coerce(inventory_onhand),
        transactions=_coerce(transactions),
        demand=_coerce(demand),
        wo_materials_shortage=_coerce(wo_materials_shortage),
        wo_history=_coerce(wo_history),
    )


# ---------------------------------------------------------------------------
# Cleaning helpers
# ---------------------------------------------------------------------------

def _clean_transactions(
    txn: pd.DataFrame,
    as_of: _dt.date,
) -> pd.DataFrame:
    """Add `Qty clean`, `Days from transaction`, `DAYS PAST`, `Modified Qty for Aging`.

    Mirrors the Excel formulas the Lion extends down columns Y..AB on the
    Transaction report sheet.
    """
    df = txn.copy()
    if "Quantity" not in df.columns:
        return df  # nothing to do

    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0.0)
    df["Qty clean"] = df["Quantity"].abs()

    if "Transaction Date" in df.columns:
        td = pd.to_datetime(df["Transaction Date"], errors="coerce")
        df["Date value"] = td
        df["Days from transaction"] = (pd.Timestamp(as_of) - td).dt.days
    else:
        df["Date value"] = pd.NaT
        df["Days from transaction"] = np.nan

    df["DAYS PAST"] = df["Days from transaction"]

    ttype = df.get("Transaction Type", pd.Series(index=df.index, dtype=object)).astype(str)
    modifier = ttype.map(BACKOFFICE_TRANSFER_MODIFIERS).fillna(0).astype(int)
    # When modifier == 1 the txn is a transfer / adjustment → exclude from aging
    df["Modified Qty for Aging"] = df["Qty clean"].where(modifier == 0, 0.0)
    return df


def _aging_pivot(txn_clean: pd.DataFrame) -> pd.DataFrame:
    """Build PIVOT Transaction1 — item × days_past → sum(Modified Qty for Aging).

    Returns a DataFrame indexed by `Item Name` with one column per integer day.
    """
    if "Modified Qty for Aging" not in txn_clean.columns:
        return pd.DataFrame()
    keep = txn_clean[["Item Name", "DAYS PAST", "Modified Qty for Aging"]].dropna(
        subset=["Item Name", "DAYS PAST"]
    )
    if keep.empty:
        return pd.DataFrame()
    keep["DAYS PAST"] = keep["DAYS PAST"].astype(int)
    pivot = keep.pivot_table(
        index="Item Name",
        columns="DAYS PAST",
        values="Modified Qty for Aging",
        aggfunc="sum",
        fill_value=0.0,
    )
    return pivot


def _bucket_columns(pivot: pd.DataFrame, raw_pivot: pd.DataFrame) -> pd.DataFrame:
    """For each item, cumulative-sum aging quantities into the 12 standard
    buckets >= bucket-days. Produces both the RAW (Quantity) and the
    Modified (cleaned) bucket sets."""
    items = sorted(set(pivot.index) | set(raw_pivot.index))
    out = pd.DataFrame(index=items)
    for label, source in (("Mod", pivot), ("Raw", raw_pivot)):
        if source.empty:
            for b in AGING_BUCKETS:
                out[f"{label}_{b}"] = 0.0
            continue
        days = sorted(int(c) for c in source.columns)
        cum = source.reindex(items, fill_value=0.0)
        for b in AGING_BUCKETS:
            cols_ge = [d for d in days if d >= b]
            out[f"{label}_{b}"] = cum[cols_ge].sum(axis=1) if cols_ge else 0.0
    return out


# ---------------------------------------------------------------------------
# Main API
# ---------------------------------------------------------------------------

def build_wip_analysis(
    sources: WIPSourceFrames,
    *,
    side: str = "MAKE",
    as_of: _dt.date | None = None,
    write_off_threshold_days: int = 360,
) -> pd.DataFrame:
    """Reproduce the WIP Analysis tab.

    Parameters
    ----------
    sources : WIPSourceFrames
    side    : "MAKE" or "BUY" — filters the inventory list (Lion runs two files).
    as_of   : reference date for aging math; defaults to today.
    write_off_threshold_days : items with non-zero aged qty beyond this day
        cut-off are flagged for write-off potential.
    """
    as_of = as_of or _dt.date.today()
    side = side.upper().strip()
    if side not in ("MAKE", "BUY", "ALL"):
        raise ValueError("side must be MAKE, BUY, or ALL")

    inv = sources.inventory_onhand.copy()
    inv.columns = [str(c).strip() for c in inv.columns]

    # Map the messy on-hand report → the canonical 7 ID columns.
    name_map = {
        "Item Name": ["Item Name", "Item", "Item Number"],
        "Item Description": ["Item Description", "Inventory Item Description", "Description"],
        "Item Cost": ["Item Cost", "Unit Cost", "Cost"],
        "Buy Make": ["Buy Make", "Buy/Make", "Make or Buy", "Make/Buy"],
        "Supply type": ["Supply Type", "WIP Supply Type", "Supply type"],
        "Sum of Quantity Onhand": ["Quantity Onhand", "On Hand", "Qty On Hand", "On-Hand"],
        "Sum of Extended Cost": ["Extended Cost", "Onhand Value", "Net Dollar"],
    }
    canonical = {}
    for dest, candidates in name_map.items():
        for c in candidates:
            if c in inv.columns:
                canonical[dest] = inv[c]
                break
        if dest not in canonical:
            canonical[dest] = pd.Series([np.nan] * len(inv))
    base = pd.DataFrame(canonical)

    # Recompute extended cost when missing.
    qty = pd.to_numeric(base["Sum of Quantity Onhand"], errors="coerce").fillna(0.0)
    cost = pd.to_numeric(base["Item Cost"], errors="coerce").fillna(0.0)
    ext = pd.to_numeric(base["Sum of Extended Cost"], errors="coerce")
    base["Sum of Quantity Onhand"] = qty
    base["Item Cost"] = cost
    base["Sum of Extended Cost"] = ext.fillna(qty * cost)

    if side != "ALL":
        bm = base["Buy Make"].astype(str).str.upper()
        base = base[bm.str.startswith(side[0])]  # 'M' or 'B'

    # Aggregate per item (Lion pivots collapse multi-locator on-hand).
    agg = (
        base.groupby(["Item Name", "Item Description", "Buy Make", "Supply type"], dropna=False)
        .agg(
            **{
                "Item Cost": ("Item Cost", "max"),
                "Sum of Quantity Onhand": ("Sum of Quantity Onhand", "sum"),
                "Sum of Extended Cost": ("Sum of Extended Cost", "sum"),
            }
        )
        .reset_index()
    )

    # Aging buckets from transactions
    txn_clean = _clean_transactions(sources.transactions, as_of=as_of)
    if "Item Name" in txn_clean.columns:
        raw_pivot = txn_clean.pivot_table(
            index="Item Name", columns="DAYS PAST", values="Qty clean", aggfunc="sum", fill_value=0.0,
        ) if "Qty clean" in txn_clean.columns else pd.DataFrame()
        mod_pivot = _aging_pivot(txn_clean)
        buckets = _bucket_columns(mod_pivot, raw_pivot)
    else:
        buckets = pd.DataFrame()

    out = agg.merge(buckets, how="left", left_on="Item Name", right_index=True)
    for b in AGING_BUCKETS:
        for tag in ("Raw", "Mod"):
            col = f"{tag}_{b}"
            if col not in out.columns:
                out[col] = 0.0
            out[col] = out[col].fillna(0.0)

    # Pending issue from Materials Shortage report (MOWR PIVOT)
    mos = sources.wo_materials_shortage
    if not mos.empty and "Component" in mos.columns and "Pending Issue" in mos.columns:
        pend = (
            mos.assign(_p=pd.to_numeric(mos["Pending Issue"], errors="coerce").fillna(0.0))
            .groupby("Component")["_p"].sum().rename("Pending Issue")
        )
        out = out.merge(pend, how="left", left_on="Item Name", right_index=True)
    else:
        out["Pending Issue"] = 0.0
    out["Pending Issue"] = out["Pending Issue"].fillna(0.0)

    # Demand pivot
    dem = sources.demand
    if not dem.empty and "Item Name" in dem.columns and "Order Quantity" in dem.columns:
        d = (
            dem.assign(_q=pd.to_numeric(dem["Order Quantity"], errors="coerce").fillna(0.0))
            .groupby("Item Name")["_q"].sum().rename("Total Demand")
        )
        out = out.merge(d, how="left", left_on="Item Name", right_index=True)
    else:
        out["Total Demand"] = 0.0
    out["Total Demand"] = out["Total Demand"].fillna(0.0)

    # Write-off flag — aged qty beyond threshold AND no live demand
    aged_far_col = f"Mod_{write_off_threshold_days}"
    out["Write-Off Flag"] = (
        (out[aged_far_col] > 0) & (out["Total Demand"] <= 0)
    ).astype(int)
    out["Write-Off $"] = np.where(
        out["Write-Off Flag"] == 1, out["Sum of Extended Cost"], 0.0
    )

    # Reorder to match Lion's WIP Analysis tab exactly (A..AH-ish).
    final_cols = WIP_ID_COLUMNS.copy()
    final_cols += [f"Raw_{b}" for b in AGING_BUCKETS]
    final_cols += [f"Mod_{b}" for b in AGING_BUCKETS]
    final_cols += ["Pending Issue", "Total Demand", "Write-Off Flag", "Write-Off $"]
    for c in final_cols:
        if c not in out.columns:
            out[c] = 0.0
    out = out[final_cols].sort_values("Sum of Extended Cost", ascending=False).reset_index(drop=True)
    return out


def compute_kpis(
    wip_analysis: pd.DataFrame,
    *,
    completed_window_days: int = 30,
    sources: WIPSourceFrames | None = None,
    as_of: _dt.date | None = None,
) -> dict:
    """KPI strip mirroring Lion's header cells (TOTAL / WRITE OFF / CLEANED UP / COMPLETED IN LAST)."""
    as_of = as_of or _dt.date.today()
    total = float(wip_analysis["Sum of Extended Cost"].sum())
    write_off = float(wip_analysis["Write-Off $"].sum())
    aged_qty_far = float(wip_analysis["Mod_360"].sum())

    completed = 0
    if sources is not None and not sources.wo_history.empty:
        wo = sources.wo_history
        if "Actual Completion " in wo.columns or "Actual Completion" in wo.columns:
            col = "Actual Completion " if "Actual Completion " in wo.columns else "Actual Completion"
            ts = pd.to_datetime(wo[col], errors="coerce")
            cutoff = pd.Timestamp(as_of) - pd.Timedelta(days=completed_window_days)
            completed = int((ts >= cutoff).sum())

    cleaned_up = float((wip_analysis["Raw_30"] - wip_analysis["Mod_30"]).sum())

    return {
        "TOTAL": total,
        "WRITE OFF POTENTIAL": write_off,
        "CLEANED UP": cleaned_up,
        f"COMPLETED IN LAST {completed_window_days}d": completed,
        "AGED > 360d (qty)": aged_qty_far,
        "ITEM COUNT": int(len(wip_analysis)),
    }
