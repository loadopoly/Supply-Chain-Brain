"""
xlsx_extractor.py  —  OneDrive-file-based data pipeline.

Reads the compiled Epicor/Oracle/SyteLine/AX cycle-count exports that live in
OneDrive and returns canonical DataFrames immediately usable by Brain analytics.

This is the LIVE working pipeline while direct SQL Server / Oracle connections
are being provisioned.  The canonical column names here match exactly what the
SQL extractors will produce once servers are configured.

Primary source:
  CycleConsolidated.xlsx  (Data Review folder)
  — one tab per ERP with raw source columns

Secondary sources (per-site):
  ABCSQL Warehouse Data Review.xlsx  (Microsoft Teams Chat Files)
  2025 Cycle Count Master_Jerome.xlsx (Cycle Count Review)
  Raw data from CC Tables.xlsx        (Cycle Count Review / Parsons SyteLine)

Canonical column name convention
---------------------------------
All output DataFrames follow §3.1 of CrossDataset_Agent_Process_Spec.md:
  part_number, part_description, abc_class, warehouse_code, plant_code,
  frozen_qty, count_qty, discrepancy_reason, unit_cost, cycle_date_key,
  count_date_key, post_status, sequence_num, counted_by, quantity_on_hand,
  uom, inventory_value, adjustment_qty, net_variance_qty, abs_variance_qty,
  net_variance_value, abs_variance_value
"""
from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Optional

import pandas as pd

# Suppress openpyxl warnings about unsupported xlsx extensions
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ---------------------------------------------------------------------------
# Resolved file paths  (override via ONEDRIVE_ROOT env var if needed)
# ---------------------------------------------------------------------------
_ONEDRIVE = Path(
    os.environ.get("ONEDRIVE_ROOT",
                   r"C:\Users\agard\OneDrive - astecindustries.com")
)

CYCLE_CONSOLIDATED = _ONEDRIVE / "Data Review" / "CycleConsolidated.xlsx"
ABCSQL_TEAMS       = _ONEDRIVE / "Microsoft Teams Chat Files" / "ABCSQL Warehouse Data Review.xlsx"
RAW_CC_TABLES      = _ONEDRIVE / "Cycle Count Review" / "Raw data from CC Tables.xlsx"
JEROME_MASTER_2025 = _ONEDRIVE / "Cycle Count Review" / "2025 Cycle Count Master_Jerome.xlsx"


def _load_sheet(path: Path, sheet: str) -> pd.DataFrame:
    """Load one sheet, return empty DataFrame on error with _error attr set."""
    if not path.exists():
        df = pd.DataFrame()
        df.attrs["_error"] = f"File not found: {path}"
        return df
    try:
        return pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
    except Exception as exc:
        df = pd.DataFrame()
        df.attrs["_error"] = str(exc)
        return df


def _rename(df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
    """Rename columns that exist; silently skip missing ones."""
    present = {k: v for k, v in col_map.items() if k in df.columns}
    return df.rename(columns=present)


# ===========================================================================
# EPICOR  (Epicor 9 — Jerome Ave, Manufacturers Rd, Wilson Rd)
# ===========================================================================

def extract_epicor_abcsql(path: Path = ABCSQL_TEAMS) -> pd.DataFrame:
    """
    ABCSQL_Epicor — item master with ABC classification and on-hand qty.

    Source columns: PartNum, PartDescription, SystemAbc, classID, OnHandQty
    Output (canonical):
      part_number, part_description, abc_class, class_id, quantity_on_hand
    """
    df = _load_sheet(CYCLE_CONSOLIDATED, "ABCSQL_Epicor")
    if df.empty:
        # Fall back to standalone ABCSQL file
        df = _load_sheet(path, "Sheet1")
        if df.empty:
            return df
        # Standalone file has different layout — take first column as part_number
        col_map = {df.columns[0]: "part_number"}
        df = _rename(df, col_map)
        return df

    col_map = {
        "PartNum":          "part_number",
        "PartDescription":  "part_description",
        "SystemAbc":        "abc_class",
        "classID":          "class_id",
        "OnHandQty":        "quantity_on_hand",
    }
    df = _rename(df, col_map)
    df["erp"] = "epicor"
    return df


def extract_epicor_ccmerger(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """
    CCMerger_Epicor — cycle-count header records (one row per part per cycle).

    This is the primary Epicor CC source matching spec §3.1 exactly.
    Source table in Epicor 9: dbo.CCMerger (query via PartCount join).

    Key canonical columns produced:
      part_number, warehouse_code, plant_code, cc_year, cc_month,
      frozen_qty (TotFrozenQOH), count_qty (TotCountQOH),
      discrepancy_reason (CDRCode), abc_class (ABCCode),
      cycle_date_key (CycleDate), count_date_key (CompleteDate),
      post_status (PostStatus), sequence_num (CycleSeq),
      frozen_value, count_value, company
    """
    df = _load_sheet(source, "CCMerger_Epicor")
    if df.empty:
        return df

    col_map = {
        "Company":          "company",
        "WarehouseCode":    "warehouse_code",
        "Plant":            "plant_code",
        "CCYear":           "cc_year",
        "CCMonth":          "cc_month",
        "PartNum":          "part_number",
        "TotFrozenQOH":     "frozen_qty",       # §3.1 Frozen QOH
        "TotFrozenVal":     "frozen_value",
        "TotCountQOH":      "count_qty",         # §3.1 Counted QOH
        "TotCountVal":      "count_value",
        "CDRCode":          "discrepancy_reason",# §3.1 Discrepancy reason
        "ABCCode":          "abc_class",         # §3.1 ABC class
        "CycleDate":        "cycle_date_key",    # §3.1 Cycle / due date
        "CompleteDate":     "count_date_key",    # §3.1 Complete date
        "PostStatus":       "post_status",
        "CycleSeq":         "sequence_num",
        "BaseUOM":          "uom",
        "AllocationVariance": "allocation_variance",
        "QtyAdjTolerance":  "qty_adj_tolerance",
    }
    df = _rename(df, col_map)
    # Derive net variance from frozen→count
    if "frozen_qty" in df.columns and "count_qty" in df.columns:
        df["net_variance_qty"] = df["count_qty"] - df["frozen_qty"]
        df["abs_variance_qty"] = df["net_variance_qty"].abs()
    df["erp"] = "epicor"
    df["site"] = "jerome_ave"   # CycleConsolidated Jerome data; override when multi-site
    return df


def extract_epicor_cycle_analysis(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """
    CycleCount_Epicor — aggregated count-needs analysis per part.

    Columns: part_number, abc_class, quantity_on_hand, q1..q4, h1, h2, ytd,
             needs_count_now, reason, multi_count, ytd_need_cnt
    """
    df = _load_sheet(source, "CycleCount_Epicor")
    if df.empty:
        return df
    col_map = {
        "PartNum":          "part_number",
        "Classification":   "abc_class",
        "QOH":              "quantity_on_hand",
        "Q1": "q1", "Q2": "q2", "Q3": "q3", "Q4": "q4",
        "H1": "h1", "H2": "h2", "YTD": "ytd",
        "Needs_Count_Now":  "needs_count_now",
        "Reason":           "count_reason",
        "Multi-Count":      "multi_count",
        "YTD_Need_Cnt":     "ytd_need_cnt",
    }
    df = _rename(df, col_map)
    df["erp"] = "epicor"
    return df


def extract_epicor_count_data(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """
    Data_Epicor — individual count transaction records.

    Columns: part_number, sequence_num, audit_flag, abc_class,
             count_date_key, month, quarter, pre_ioh, post_ioh,
             net_variance_value, abs_variance_value, net_variance_qty,
             abs_variance_qty, plant_code
    """
    df = _load_sheet(source, "Data_Epicor")
    if df.empty:
        return df
    col_map = {
        "PartNum":          "part_number",
        "UniqueID":         "sequence_num",
        "Audit":            "audit_flag",
        "Classification":   "abc_class",
        "CountDate":        "count_date_key",
        "Month":            "month",
        "Qtr":              "quarter",
        "Pre_IOH":          "frozen_qty",
        "Post_IOH":         "count_qty",
        "Pre_$":            "frozen_value",
        "Post_$":           "count_value",
        "Net Variance":     "net_variance_value",
        "AbsVariance":      "abs_variance_value",
        "NetVarQOH":        "net_variance_qty",
        "AbsVarQOH":        "abs_variance_qty",
        "Plant":            "plant_code",
    }
    df = _rename(df, col_map)
    df["erp"] = "epicor"
    return df


# ===========================================================================
# ORACLE FUSION  (Airport Rd Eugene, PDC, St. Cloud, Burlington, Blair, St. Bruno,
#                 Manufacturers Rd — per §3.2)
# ===========================================================================

def extract_oracle_on_hand(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """
    AstecOnHandDetails_Oracle — on-hand inventory snapshot per sub-inventory.

    Canonical: part_number, part_description, make_or_buy, warehouse_code,
               locator, quantity_on_hand, uom, unit_cost, supplier,
               last_po_receipt_date, last_wip_issue_date, planner_code,
               abc_class, item_type
    """
    df = _load_sheet(source, "AstecOnHandDetails_Oracle")
    if df.empty:
        return df
    col_map = {
        "Item Name":              "part_number",
        " Item Description":      "part_description",
        "Buy Make":               "make_or_buy",
        "Subinventory Code":      "warehouse_code",    # §3.1 Warehouse
        "Locator":                "locator",
        "Quantity Onhand":        "quantity_on_hand",
        "Uom Code":               "uom",
        "Item Cost":              "unit_cost",
        "Extended Cost":          "inventory_value",
        "Planner Code":           "planner_code",
        "Item Class":             "abc_class",          # §3.1 ABC class
        " Item Type Name":        "item_type",
        "Supplier":               "supplier_name",
        "Last Po Receipt Date":   "last_po_receipt_date",
        "Last WIP Issue Date":    "last_wip_issue_date",
        "Le Entity":              "legal_entity",
        "Organization Name":      "org_name",
    }
    df = _rename(df, col_map)
    df["erp"] = "oracle"
    return df


def extract_oracle_cc_metrics(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """
    AstecCycleCountMetrics_Oracle — cycle count sequence records.

    This is the primary Oracle CC source.  Matches spec §3.1 Oracle column names.

    Canonical: sequence_num, part_number, part_description, abc_class,
               unit_cost, cycle_date_key, count_date_key, warehouse_code,
               location_code, frozen_qty, count_qty, adjustment_qty,
               abs_adjustment_qty, frozen_value, count_value,
               abs_variance_value, accuracy_pct, post_status, recounted_flag,
               counted_by
    """
    df = _load_sheet(source, "AstecCycleCountMetrics_Oracle")
    if df.empty:
        return df
    col_map = {
        "Count Sequence Number":         "sequence_num",
        "Item":                          "part_number",
        "Desc":                          "part_description",
        "ABC":                           "abc_class",           # §3.1 ABC class
        "Item Cost":                     "unit_cost",
        "Count Date":                    "count_date_key",      # §3.1 Complete date
        "Reviewed Date":                 "reviewed_date",
        "Sub-inventory":                 "warehouse_code",      # §3.1 Warehouse (SubInventoryCode)
        "Location":                      "location_code",
        "Location Qty":                  "frozen_qty",          # §3.1 Frozen QOH
        "Count Qty":                     "count_qty",           # §3.1 Counted QOH
        "Adjusted Qty":                  "adjustment_qty",
        "Absolute Qty":                  "abs_adjustment_qty",
        "Location Dollars":              "frozen_value",
        "Count Dollars":                 "count_value",
        "Adjusted Ext Dollars":          "net_variance_value",
        "Absolute Dollars Adjusted":     "abs_variance_value",
        "Absolute Accuracy of Dollars":  "accuracy_pct",
        "Count Sequence Status":         "post_status",         # §3.1 Complete date gating
        "Recounted":                     "recounted_flag",
    }
    df = _rename(df, col_map)
    # Derive variance
    if "frozen_qty" in df.columns and "count_qty" in df.columns:
        for col in ("frozen_qty", "count_qty"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["net_variance_qty"] = df["count_qty"] - df["frozen_qty"]
        df["abs_variance_qty"] = df["net_variance_qty"].abs()
    df["erp"] = "oracle"
    return df


def extract_oracle_cycle_analysis(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """CycleCount_Oracle — same shape as CycleCount_Epicor, Oracle sourced."""
    df = _load_sheet(source, "CycleCount_Oracle")
    if df.empty:
        return df
    col_map = {
        "PartNum": "part_number", "Classification": "abc_class",
        "QOH": "quantity_on_hand", "Needs_Count_Now": "needs_count_now",
        "Reason": "count_reason", "YTD_Need_Cnt": "ytd_need_cnt",
    }
    df = _rename(df, col_map)
    df["erp"] = "oracle"
    return df


def extract_oracle_count_data(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """Data_Oracle — individual Oracle count transactions."""
    df = _load_sheet(source, "Data_Oracle")
    if df.empty:
        return df
    col_map = {
        "PartNum": "part_number", "UniqueID": "sequence_num",
        "Audit": "audit_flag", "Classification": "abc_class",
        "CountDate": "count_date_key", "Month": "month", "Qtr": "quarter",
        "Pre_IOH": "frozen_qty", "Post_IOH": "count_qty",
        "Pre_$": "frozen_value", "Post_$": "count_value",
        "Net Variance": "net_variance_value", "AbsVariance": "abs_variance_value",
        "NetVarQOH": "net_variance_qty", "AbsVarQOH": "abs_variance_qty",
        "Plant": "plant_code",
    }
    df = _rename(df, col_map)
    df["erp"] = "oracle"
    return df


# ===========================================================================
# SYTELINE  (Parsons — PFI_SLMiscApps_DB.cycle_count.item_count)
# ===========================================================================

def extract_syteline_item_abc(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """
    item_ABC_Code_Syteline — SyteLine item master with ABC codes and on-hand.

    Source table: PFI_SLMiscApps_DB (schema varies by site).
    Columns: item, description, u_m, abc_code, p_m_t_code (P=purchase/M=mfg),
             whse, cycle_freq, last_cycle, QuantityOnHand, unit_cost,
             InventoryDollars, DateToCount

    Canonical: part_number, part_description, uom, abc_class, make_or_buy,
               warehouse_code, cycle_freq, last_cycle_date, quantity_on_hand,
               unit_cost, inventory_value, cycle_date_key
    """
    df = _load_sheet(source, "item_ABC_Code_Syteline")
    if df.empty:
        # Also try Raw data from CC Tables (Data2 sheet = same schema)
        df = _load_sheet(RAW_CC_TABLES, "Data2")
        if df.empty:
            return df

    col_map = {
        "item":             "part_number",
        "description":      "part_description",
        "u_m":              "uom",
        "abc_code":         "abc_class",
        "p_m_t_code":       "make_or_buy",
        "whse":             "warehouse_code",
        "cycle_freq":       "cycle_freq",
        "last_cycle":       "last_cycle_date",
        "QuantityOnHand":   "quantity_on_hand",
        "unit_cost":        "unit_cost",
        "InventoryDollars": "inventory_value",
        "DateToCount":      "cycle_date_key",
    }
    df = _rename(df, col_map)
    df["erp"] = "syteline"
    df["site"] = "parsons"
    return df


def extract_syteline_item_count(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """
    Item_Count_Syteline — SyteLine cycle count transaction records.

    Source table: PFI_SLMiscApps_DB.cycle_count.item_count
    Columns: Id, ItemId, Location, Count, CountDate, OnHandInErpAtTimeOfCount,
             Lot, UnitCostAtTimeOfCount, UserName

    Canonical: sequence_num, part_number, location_code, count_qty,
               count_date_key, frozen_qty, lot_number, unit_cost, counted_by
    """
    df = _load_sheet(source, "Item_Count_Syteline")
    if df.empty:
        return df
    col_map = {
        "Id":                        "sequence_num",
        "ItemId":                    "part_number",
        "Location":                  "location_code",
        "Count":                     "count_qty",
        "CountDate":                 "count_date_key",
        "OnHandInErpAtTimeOfCount":  "frozen_qty",
        "Lot":                       "lot_number",
        "UnitCostAtTimeOfCount":     "unit_cost",
        "UserName":                  "counted_by",
    }
    df = _rename(df, col_map)
    if "frozen_qty" in df.columns and "count_qty" in df.columns:
        df["net_variance_qty"] = pd.to_numeric(df["count_qty"], errors="coerce") \
                               - pd.to_numeric(df["frozen_qty"], errors="coerce")
        df["abs_variance_qty"] = df["net_variance_qty"].abs()
    df["erp"] = "syteline"
    df["site"] = "parsons"
    return df


def extract_syteline_cycle_analysis(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """CycleCount_Syteline — aggregated count-needs per part."""
    df = _load_sheet(source, "CycleCount_Syteline")
    if df.empty:
        return df
    col_map = {
        "PartNum": "part_number", "Classification": "abc_class",
        "QOH": "quantity_on_hand", "Needs_Count_Now": "needs_count_now",
        "Reason": "count_reason", "YTD_Need_Cnt": "ytd_need_cnt",
    }
    df = _rename(df, col_map)
    df["erp"] = "syteline"
    df["site"] = "parsons"
    return df


def extract_syteline_count_data(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """Data_Syteline — individual SyteLine count transactions."""
    df = _load_sheet(source, "Data_Syteline")
    if df.empty:
        return df
    col_map = {
        "PartNum": "part_number", "UniqueID": "sequence_num",
        "Audit": "audit_flag", "Classification": "abc_class",
        "CountDate": "count_date_key", "Month": "month", "Qtr": "quarter",
        "Pre_IOH": "frozen_qty", "Post_IOH": "count_qty",
        "Pre_$": "frozen_value", "Post_$": "count_value",
        "Net Variance": "net_variance_value", "AbsVariance": "abs_variance_value",
        "NetVarQOH": "net_variance_qty", "AbsVarQOH": "abs_variance_qty",
        "Plant": "plant_code",
    }
    df = _rename(df, col_map)
    df["erp"] = "syteline"
    df["site"] = "parsons"
    return df


# ===========================================================================
# MICROSOFT DYNAMICS AX  (Eugene Airport Rd — site "airport_rd")
# ===========================================================================

def extract_ax_item_abc(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """
    Assigned Codes EUG_AX — AX item master with ABC count groups.

    Source: Dynamics AX counting groups / item master
    Columns: Item number, Product name, Warehouse, On-hand, Counting group

    Canonical: part_number, part_description, warehouse_code,
               quantity_on_hand, count_group
    """
    df = _load_sheet(source, "Assigned Codes EUG_AX")
    if df.empty:
        return df
    col_map = {
        "Item number":   "part_number",
        "Product name":  "part_description",
        "Warehouse":     "warehouse_code",
        "On-hand":       "quantity_on_hand",
        "Counting group":"count_group",
    }
    df = _rename(df, col_map)
    # Map count_group to ABC class heuristic (WH1 C → C, etc.)
    if "count_group" in df.columns:
        df["abc_class"] = df["count_group"].str.extract(r"\b([ABC])\b").iloc[:, 0]
    df["erp"] = "ax"
    df["site"] = "airport_rd"
    return df


def extract_ax_cc_journal(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """
    MONTH_AX — Dynamics AX counting journal lines for Eugene Airport Rd.

    Source: AX counting journal (INV_COUNT / IIJ* journals)
    Columns: Item number, Configuration, Site, Warehouse, Location, Journal,
             Counting date, Created date and time, Created by, On-hand,
             Quantity counted, OK, Cost price

    Canonical: part_number, configuration, site_code, warehouse_code,
               location_code, journal_id, cycle_date_key, created_datetime,
               counted_by, frozen_qty, count_qty, approved_flag, unit_cost
    """
    df = _load_sheet(source, "MONTH_AX")
    if df.empty:
        return df
    col_map = {
        "Item number":            "part_number",
        "Configuration":          "configuration",
        "Site":                   "site_code",
        "Warehouse":              "warehouse_code",
        "Location":               "location_code",
        "Journal":                "journal_id",
        "Counting date":          "cycle_date_key",
        "Created date and time":  "created_datetime",
        "Created by":             "counted_by",
        "On-hand":                "frozen_qty",
        "Quantity counted":       "count_qty",
        "OK":                     "approved_flag",
        "Cost price":             "unit_cost",
    }
    df = _rename(df, col_map)
    if "frozen_qty" in df.columns and "count_qty" in df.columns:
        df["net_variance_qty"] = pd.to_numeric(df["count_qty"], errors="coerce") \
                               - pd.to_numeric(df["frozen_qty"], errors="coerce")
        df["abs_variance_qty"] = df["net_variance_qty"].abs()
    df["erp"] = "ax"
    df["site"] = "airport_rd"
    return df


def extract_ax_cycle_analysis(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """CycleCount_AX — aggregated count-needs per part (AX)."""
    df = _load_sheet(source, "CycleCount_AX")
    if df.empty:
        return df
    col_map = {
        "PartNum": "part_number", "Classification": "abc_class",
        "QOH": "quantity_on_hand", "Needs_Count_Now": "needs_count_now",
        "Reason": "count_reason", "YTD_Need_Cnt": "ytd_need_cnt",
    }
    df = _rename(df, col_map)
    df["erp"] = "ax"
    df["site"] = "airport_rd"
    return df


def extract_ax_count_data(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """Data_AX — individual AX count transactions."""
    df = _load_sheet(source, "Data_AX")
    if df.empty:
        return df
    col_map = {
        "PartNum": "part_number", "UniqueID": "sequence_num",
        "Audit": "audit_flag", "Classification": "abc_class",
        "CountDate": "count_date_key", "Month": "month", "Qtr": "quarter",
        "Pre_IOH": "frozen_qty", "Post_IOH": "count_qty",
        "Pre_$": "frozen_value", "Post_$": "count_value",
        "Net Variance": "net_variance_value", "AbsVariance": "abs_variance_value",
        "NetVarQOH": "net_variance_qty", "AbsVarQOH": "abs_variance_qty",
        "Plant": "plant_code",
    }
    df = _rename(df, col_map)
    df["erp"] = "ax"
    df["site"] = "airport_rd"
    return df


# ===========================================================================
# Combined / cross-ERP helpers
# ===========================================================================

# Logical sheet alias → extractor function mapping
# Keys match the xlsx_sources section in brain.yaml
_EXTRACTOR_MAP = {
    # Epicor
    "epicor_abcsql":           extract_epicor_abcsql,
    "epicor_ccmerger":         extract_epicor_ccmerger,
    "epicor_cycle_analysis":   extract_epicor_cycle_analysis,
    "epicor_count_data":       extract_epicor_count_data,
    # Oracle
    "oracle_on_hand":          extract_oracle_on_hand,
    "oracle_cc_metrics":       extract_oracle_cc_metrics,
    "oracle_cycle_analysis":   extract_oracle_cycle_analysis,
    "oracle_count_data":       extract_oracle_count_data,
    # SyteLine
    "syteline_item_abc":       extract_syteline_item_abc,
    "syteline_item_count":     extract_syteline_item_count,
    "syteline_cycle_analysis": extract_syteline_cycle_analysis,
    "syteline_count_data":     extract_syteline_count_data,
    # AX
    "ax_item_abc":             extract_ax_item_abc,
    "ax_cc_journal":           extract_ax_cc_journal,
    "ax_cycle_analysis":       extract_ax_cycle_analysis,
    "ax_count_data":           extract_ax_count_data,
}


def fetch(alias: str, source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """
    Fetch a logical dataset by alias name.

    Example::

        from src.extract.xlsx_extractor import fetch
        df = fetch("epicor_ccmerger")
        df = fetch("oracle_cc_metrics")
        df = fetch("syteline_item_abc")
        df = fetch("ax_cc_journal")

    Returns a canonical DataFrame or an empty DataFrame with ``_error`` attr.
    """
    fn = _EXTRACTOR_MAP.get(alias)
    if fn is None:
        df = pd.DataFrame()
        df.attrs["_error"] = (
            f"Unknown xlsx alias '{alias}'. "
            f"Valid: {sorted(_EXTRACTOR_MAP)}"
        )
        return df
    try:
        return fn(source)
    except Exception as exc:
        df = pd.DataFrame()
        df.attrs["_error"] = f"{type(exc).__name__}: {exc}"
        return df


def fetch_all_cc_data(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """
    Concatenate all ERP cycle-count DATA sheets into one canonical DataFrame.

    Returns a single DataFrame with erp and site columns for cross-ERP analysis.
    Useful for the Brain's bullwhip, OTD, and multi-echelon analytics.
    """
    frames = [
        extract_epicor_count_data(source),
        extract_oracle_count_data(source),
        extract_syteline_count_data(source),
        extract_ax_count_data(source),
    ]
    valid = [f for f in frames if not f.empty]
    if not valid:
        df = pd.DataFrame()
        df.attrs["_error"] = "No count data found in any ERP sheet"
        return df
    return pd.concat(valid, ignore_index=True)


def fetch_all_abc_data(source: Path = CYCLE_CONSOLIDATED) -> pd.DataFrame:
    """
    Concatenate item master / ABC classification from all ERPs.

    Returns: part_number, abc_class, quantity_on_hand, unit_cost, erp, site
    """
    frames = [
        extract_epicor_abcsql(source),
        extract_oracle_on_hand(source),
        extract_syteline_item_abc(source),
        extract_ax_item_abc(source),
    ]
    valid = [f for f in frames if not f.empty]
    if not valid:
        return pd.DataFrame()
    return pd.concat(valid, ignore_index=True)


def available_aliases() -> list[str]:
    """Return all registered xlsx source aliases."""
    return sorted(_EXTRACTOR_MAP)
