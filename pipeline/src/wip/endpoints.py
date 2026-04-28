"""Oracle Fusion BI Publisher / OTBI endpoint catalog for the WIP Aging Review.

Each source report is the same one Brenda runs and emails to Francesco. We bind
each to a Fusion REST report path AND an equivalent BIP SQL template so the
Brain can pull the data directly without manual export.

Plant default = Wilson Road = `3125_US_WIL_MFG`. Override per call.

These paths mirror the report names visible in the recording / shared workbook
filenames (see docs/Lions Lectures/WIP Review/ADAM/*.xlsx).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class FusionReport:
    """Metadata for one Fusion BIP report used in the WIP Aging Review."""

    key: str
    title: str
    bip_report_path: str           # /~user/.../<Report>.xdo  (BIP catalog path)
    description: str
    sheet_in_workbook: str         # native sheet name when manually exported
    sql: str = ""                  # optional BIP SQL fallback (executed via execute_sql)
    parameters: dict = field(default_factory=dict)


# Default plant / org for the Wilson Road manufacturing book the Lion analyzes.
DEFAULT_INVENTORY_ORG = "3125_US_WIL_MFG"
DEFAULT_LEGAL_ENTITY = "HEATEC, INC."
DEFAULT_COST_BOOK = "3125_US_CHA-WIL_PRI-BOOK"
DEFAULT_TRANSACTION_FROM = "2025-01-01"


SOURCE_REPORTS: dict[str, FusionReport] = {
    # 1. WIP / Inventory On-Hand by Locator with cost ----------------------------
    "inventory_onhand": FusionReport(
        key="inventory_onhand",
        title="Astec Inventory On Hand with Cost Detail by Locator",
        bip_report_path="/Custom/Astec/Inventory/Astec Inventory On Hand with Cost Detail by Locator Report.xdo",
        description=(
            "Per-locator on-hand inventory snapshot with item cost and extended "
            "cost. Filtered to WIP subinventory to seed the WIP Analysis tab."
        ),
        sheet_in_workbook="Sheet1",
        sql=(
            "SELECT item_name, item_description, item_cost, buy_make, "
            "supply_type, subinventory, locator, quantity_onhand, "
            "(item_cost * quantity_onhand) AS extended_cost "
            "FROM ASTEC_INVENTORY_ONHAND_V "
            "WHERE legal_entity = :legal_entity "
            "  AND inventory_organization_name = :inventory_org "
            "  AND subinventory IN ('PROD','WIP','HTRPIPING','JIGJOBFAB','OPTIONS')"
        ),
        parameters={
            "legal_entity": DEFAULT_LEGAL_ENTITY,
            "inventory_org": DEFAULT_INVENTORY_ORG,
        },
    ),

    # 2. Inventory transaction history (WIP material issues) --------------------
    "inventory_transactions": FusionReport(
        key="inventory_transactions",
        title="Astec Inventory Transaction Report",
        bip_report_path="/Custom/Astec/Inventory/Astec Inventory Transaction Report.xdo",
        description=(
            "Every inventory transaction since the prior baseline. The Lion "
            "filters Transaction Type = 'Work in Process Material Issue' and "
            "appends new dates onto an ever-growing time series. Day-aging is "
            "computed against (today - Transaction Date)."
        ),
        sheet_in_workbook="Sheet1",
        sql=(
            "SELECT business_unit, legal_entity, cost_book, "
            "inventory_organization_name, transaction_id, transaction_date, "
            "transaction_time, transaction_reason_code, "
            "transaction_reason_code_description, transaction_type, "
            "reference_number, source_reference, subinventory, "
            "locator_inventory_location, created_by, item_name, "
            "item_description, quantity, unit_cost, transaction_uom_code, "
            "secondary_uom, net_dollar, wip_supply_type, last_updated_by "
            "FROM ASTEC_INVENTORY_TRANSACTION_V "
            "WHERE inventory_organization_name = :inventory_org "
            "  AND transaction_date >= TO_DATE(:from_date,'YYYY-MM-DD') "
            "  AND transaction_type IN ('Work in Process Material Issue', "
            "                           'Work in Process Negative Component Issue', "
            "                           'Work in Process Component Return')"
        ),
        parameters={
            "inventory_org": DEFAULT_INVENTORY_ORG,
            "from_date": DEFAULT_TRANSACTION_FROM,
        },
    ),

    # 3. Demand from Supply Plan (planning output) ------------------------------
    "demand_supply_plan": FusionReport(
        key="demand_supply_plan",
        title="Demand from Supply Plan",
        bip_report_path="/Custom/Astec/Planning/Demand from Supply Plan.xdo",
        description=(
            "Item-level demand pivot from the active Supply Plan. Used to "
            "judge whether aged WIP still has live demand backing it."
        ),
        sheet_in_workbook="Sheet1",
        sql=(
            "SELECT item_name, inventory_item_description, make_or_buy, "
            "       order_quantity, order_type "
            "FROM ASTEC_DEMAND_FROM_SUPPLY_PLAN_V "
            "WHERE inventory_organization_name = :inventory_org"
        ),
        parameters={"inventory_org": DEFAULT_INVENTORY_ORG},
    ),

    # 4. Manage Work Orders Reports — Materials Shortage ------------------------
    "wo_materials_shortage": FusionReport(
        key="wo_materials_shortage",
        title="Manage Work Orders Reports — Materials Shortage",
        bip_report_path="/Custom/Astec/Manufacturing/Manage Work Orders Reports - Materials Shortage.xdo",
        description=(
            "Component-level shortage view per work order: pending issue, on "
            "hand, available, supply locator. Drives the MOWR PIVOT tab."
        ),
        sheet_in_workbook="Sheet1",
        sql=(
            "SELECT planning_serial_no, sales_order, pick_slip_number, "
            "       component, description, work_center_code, supply_type, "
            "       buy_make, qty_required, issued, confirmed_pick, open_pick, "
            "       remaining_allocated, on_hand, pick_released, "
            "       available_to_transact, available_to_reserve, pending_issue, "
            "       supply_sub, locator, supply_locator_on_hand, "
            "       work_order_number, work_order_description, oper_seq_no, "
            "       assembly_item_number, assembly_item_description, item_type, "
            "       material_seq, wo_status, firm "
            "FROM ASTEC_WO_MATERIAL_SHORTAGE_V "
            "WHERE inventory_organization_name = :inventory_org"
        ),
        parameters={"inventory_org": DEFAULT_INVENTORY_ORG},
    ),

    # 5. Manage Work Orders Reports — Work Orders -------------------------------
    "wo_history": FusionReport(
        key="wo_history",
        title="Manage Work Orders Reports — Work Orders",
        bip_report_path="/Custom/Astec/Manufacturing/Manage Work Orders Reports - Work Orders.xdo",
        description=(
            "Header-level work order list: status, dates, qty, assembly. The "
            "Lion appends incremental creations to the WO History tab."
        ),
        sheet_in_workbook="Sheet1",
        sql=(
            "SELECT work_order_number AS work_order, work_order_description, "
            "       planning_serial_number, color_dff, assembly_item_number, "
            "       assembly_item_description, item_type, qty, uom, wd, status, "
            "       creation_date, cancel_date, released_date, start_date, "
            "       planned_completion, actual_completion, comp, locator, firm, "
            "       source_header_reference, sales_order_dff "
            "FROM ASTEC_WORK_ORDERS_V "
            "WHERE inventory_organization_name = :inventory_org "
            "  AND creation_date >= TO_DATE(:from_date,'YYYY-MM-DD')"
        ),
        parameters={
            "inventory_org": DEFAULT_INVENTORY_ORG,
            "from_date": DEFAULT_TRANSACTION_FROM,
        },
    ),
}


def get_report(key: str) -> FusionReport:
    """Lookup helper for the page UI."""
    if key not in SOURCE_REPORTS:
        raise KeyError(f"Unknown WIP Aging source report key: {key!r}")
    return SOURCE_REPORTS[key]


def list_report_summary() -> list[dict]:
    """Compact dict list (for st.dataframe rendering on the page)."""
    return [
        {
            "Key": r.key,
            "Report": r.title,
            "BIP Path": r.bip_report_path,
            "Sheet": r.sheet_in_workbook,
            "Description": r.description,
        }
        for r in SOURCE_REPORTS.values()
    ]
