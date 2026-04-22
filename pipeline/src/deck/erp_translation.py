"""
ERP translation layer (spec §3).

Aliases source-ERP columns to canonical names from schemas.py. Apply at
ingest — every downstream function reads canonical fields only.
"""
from __future__ import annotations
import pandas as pd

# §3.1 cycle-count column translation
CC_TRANSLATION = {
    "epicor9": {
        "Frozen QOH": "TotFrozenQOH",
        "Counted QOH": "TotCountQOH",
        "Discrepancy Reason": "CDRCode",
        "ABC Class": "ABCCode",
        "Complete Date": "CompleteDate",
        "Cycle / Due Date": "CycleDate",
        "Warehouse": "WarehouseCode",
    },
    "oracle_fusion": {
        "Frozen QOH": "FrozenOnHand",
        "Counted QOH": "CountedQty",
        "Discrepancy Reason": "DiscrepancyReasonCode",
        "ABC Class": "ABC_Class",
        "Complete Date": "CountCompletedDate",
        "Cycle / Due Date": "CountDueDate",
        "Warehouse": "SubInventoryCode",
    },
    "syteline": {
        "Frozen QOH": "qty_on_hand_before",
        "Counted QOH": "qty_counted",
        "Discrepancy Reason": "reason_code",
        "ABC Class": "item_abc_code",
        "Complete Date": "post_date",
        "Cycle / Due Date": "due_date",
        "Warehouse": "whse",
    },
}

# §3.2 plant → ERP map
SITE_ERP_MAP = {
    "Chattanooga - Jerome Avenue":     ("epicor9",       "PartCount / CCMerger (wh S1, E1)"),
    "Chattanooga - Manufacturers Road":("epicor9",       "PartCount"),
    "Chattanooga - Wilson Road":       ("epicor9",       "PartCount"),
    "Eugene - Airport Road":           ("oracle_fusion", "Oracle CountEvents"),
    "Prairie du Chien":                ("oracle_fusion", "Oracle CountEvents"),
    "St Cloud":                        ("oracle_fusion", "Oracle CountEvents"),
    "Burlington":                      ("oracle_fusion", "Oracle CountEvents (REXCON)"),
    "Blair":                           ("oracle_fusion", "Oracle CountEvents"),
    "Parsons":                         ("syteline",      "PFI_App.dbo.cc_trn"),
    "St Bruno":                        ("oracle_fusion", "Oracle CountEvents"),
}


def erp_for_site(site: str) -> str | None:
    entry = SITE_ERP_MAP.get(site)
    return entry[0] if entry else None


def normalize_part(series: pd.Series) -> pd.Series:
    """
    §3.3 gotcha: Oracle Fusion item-number exports contain CHAR(160)
    non-breaking spaces. Apply this at ingest, not at join.
    """
    return (
        series.astype(str)
        .str.replace("\xa0", "", regex=False)
        .str.strip()
        .str.upper()
    )


def alias_columns(df: pd.DataFrame, erp: str, mapping_kind: str = "cc") -> pd.DataFrame:
    """Rename source columns → canonical names. `mapping_kind` is reserved
    for future contracts (otd/ifr/itr); only `cc` is defined today."""
    if mapping_kind != "cc":
        raise NotImplementedError(mapping_kind)
    src = CC_TRANSLATION.get(erp)
    if not src:
        return df
    # src: {canonical: source_col} — invert and rename what's present
    inv = {v: k for k, v in src.items() if v in df.columns}
    return df.rename(columns=inv)
