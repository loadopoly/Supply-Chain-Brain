"""
Canonical dataset contracts (spec §2).

Each dataset lists:
  required   — fields that must be present before the pipeline will run
  optional   — fields consumed if present
  dtypes     — coercion hints used by the ingest step (Phase 1)

Downstream code reads canonical field names only. Use the ERP translation
layer (erp_translation.py) to alias source columns to these names before
calling any findings function.
"""
from __future__ import annotations

# §2.1 OTD — sales-order line history
OTD = {
    "required": [
        "Site",
        "Order Date",
        "Ship Date",
        "SO No",
        "Line No",
        "Part",
        "Qty",
        "OTD Miss (Late)",
        "Customer",
        "Part Pur/Fab",
        "Failure Reason",
        "Promised Date",
    ],
    "optional": [
        "Adjusted Promise Date",  # EDAP prefers this as OTD baseline
        "Available Qty",
        "On Hand Qty",
        "Days Late",
        "Customer No",
        "Supplier Name",
    ],
    "dtypes": {
        "Order Date": "datetime",
        "Ship Date": "datetime",
        "Promised Date": "datetime",
        "Adjusted Promise Date": "datetime",
        "Qty": "float",
        "Available Qty": "float",
        "On Hand Qty": "float",
        "Days Late": "float",
        "OTD Miss (Late)": "bool01",
    },
}

# §2.2 IFR — order-line snapshot at order time
IFR = {
    "required": [
        "Site",
        "Order Date",
        "Part",
        "SO Qty",
        "Available Qty",
        "On Hand Qty",
        "Hit Miss",
        "Part Fab/Pur",
        "Failure",
        "Customer Name",
    ],
    "optional": ["Supplier Name"],
    "dtypes": {
        "Order Date": "datetime",
        "SO Qty": "float",
        "Available Qty": "float",
        "On Hand Qty": "float",
        "Hit Miss": "bool01",
    },
}

# §2.3 ITR — inventory transactions
ITR = {
    "required": [
        "Transaction Date",
        "Transaction Type",
        "Item Name",
        "Quantity",
        "Net Dollar",
        "Subinventory",
        "Transaction Reason Code",
    ],
    "optional": ["Created By", "Last Updated By"],
    "dtypes": {
        "Transaction Date": "datetime",
        "Quantity": "float",
        "Net Dollar": "float",
    },
}

# §2.4 PFEP — plan for every part (master)
PFEP = {
    "required": [
        "Item Name",
        "Item Status",
        "Make or Buy",
        "Supplier",
        "Buyer Name",
        "Cost",
        "Total Usage",
        "Usage Value",
        "Safety Stock",
        "Minimum Quantity",
        "Maximum Quantity",
        "Processing Lead Time",
        "ABC Inventory Catalog",
        "Item Cycle Count Enabled",
        "Inventory Planning Method",
        "Safety Stock Planning Method",
    ],
    "optional": [],
    "dtypes": {
        "Cost": "float",
        "Total Usage": "float",
        "Usage Value": "float",
        "Safety Stock": "float",
        "Minimum Quantity": "float",
        "Maximum Quantity": "float",
        "Processing Lead Time": "float",
        "Item Cycle Count Enabled": "bool01",
    },
}

DATASETS = {"OTD": OTD, "IFR": IFR, "ITR": ITR, "PFEP": PFEP}


def required_fields(dataset: str) -> list[str]:
    return DATASETS[dataset]["required"]


def validate(df, dataset: str) -> list[str]:
    """Return list of missing required fields, or [] if contract is met."""
    return [f for f in required_fields(dataset) if f not in df.columns]
