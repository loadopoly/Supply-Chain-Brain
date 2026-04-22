"""
col_resolver — discovers real DB column names and resolves them by semantic role.

Pattern coverage spans Oracle EBS, Oracle Fusion, SAP, D365, and generic ETL naming.
Results are cached to config/schema_cache.json so subsequent loads are instant.
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Optional

_CACHE_FILE = Path(__file__).resolve().parents[2] / "config" / "schema_cache.json"

# ------------------------------------------------------------------
# Semantic-role → candidate column name patterns (lowercase)
# Listed most-specific first so exact matches win over substrings.
# ------------------------------------------------------------------
PATTERNS: dict[str, list[str]] = {
    # --- part / item ---
    "part_key": [
        "part_key", "item_key", "inventory_item_key",
        "part_id", "item_id", "inventory_item_id",
        "part_number", "item_number", "item_no",
    ],
    "part_description": [
        "part_description", "item_description", "description",
        "item_desc", "part_desc", "item_name",
    ],
    # --- supplier / vendor ---
    "supplier_key": [
        "supplier_key", "vendor_key",
        "supplier_id", "vendor_id",
        "supplier_number", "vendor_number",
        "supplier_code", "vendor_code",
    ],
    "supplier_name": [
        "supplier_name", "vendor_name",
        # EDAP raw fallback (pre-MDM cleanse)
        "pre_standardization_supplier_name",
        "supplier_description", "vendor_description",
        "name", "description",
    ],
    # --- quantities ---
    "quantity": [
        # Oracle Fusion
        "ordered_quantity", "order_quantity",
        "shipped_quantity", "ship_quantity",
        "fulfilled_quantity", "fulfillment_quantity",
        "quantity_received", "qty_received",
        "quantity_ordered", "qty_ordered",
        # EDAP replica exact names
        "received_qty", "sales_order_quantity",
        # generic
        "order_qty", "ordered_qty", "shipped_qty",
        "qty", "quantity",
        # EBS / D365
        "oe_ordered_quantity", "oe_shipped_quantity",
        "order_line_qty", "line_quantity",
    ],
    "on_hand_qty": [
        # EDAP replica exact name first
        "quantity_on_hand",
        "on_hand_quantity", "on_hand_qty",
        "qty_on_hand", "on_hand", "qoh",
        "current_quantity", "available_qty",
    ],
    "open_qty": [
        # EDAP replica exact names first
        "quantity_not_received", "demand_order_part_quantity",
        "open_quantity", "open_order_quantity",
        "open_qty", "open_order_qty",
        "on_order_qty", "on_order_quantity",
        "pending_qty", "pending_quantity",
        "qty_not_received",
    ],
    # Dollar value of open/unreceived PO lines (fact_inventory_open_orders)
    "open_amount": [
        "amount_not_received_local", "amount_not_received_usd",
        "open_amount_local", "open_amount_usd",
        "open_po_amount", "open_po_value",
        "unreceived_amount",
    ],
    # --- cost ---
    "unit_cost": [
        "unit_cost", "item_cost",
        "standard_cost", "avg_cost", "average_cost",
        "unit_price", "list_price",
        "cost_per_unit", "item_standard_cost",
    ],
    # --- dates ---
    "promise_date": [
        "promised_date", "promise_date",
        "need_by_date", "need_by",
        "requested_date", "request_date",
        "schedule_date", "scheduled_date",
        "required_date", "req_date",
        "due_date",
    ],
    "receipt_date": [
        "receipt_date", "received_date",
        "actual_receipt_date", "actual_received_date",
        "delivery_date", "delivered_date",
        "completion_date", "completed_date",
        "shipment_date",
    ],
    "order_date": [
        "order_date", "ordered_date",
        "po_date", "purchase_date",
        "creation_date", "created_date",
        "creation_datetime", "created_datetime",
        "transaction_date",
    ],
    "due_date": [
        "due_date", "need_by_date",
        "required_date", "required_by_date",
        "scheduled_date", "delivery_due_date",
    ],
    "ship_date": [
        "ship_date", "shipped_date",
        "actual_ship_date", "shipment_date",
        "departure_date",
    ],
    # --- lead time ---
    "lead_time_days": [
        # EDAP replica exact name
        "order_lead_time",
        "lead_time_days", "lead_time",
        "po_lead_time", "planned_lead_time",
        "planning_lead_time", "vendor_lead_time",
        "days_lead_time",
    ],
    # --- OTD / delivery ---
    "days_late": [
        "days_late", "days_past_due",
        "lateness_days", "overdue_days",
        "delivery_days_late",
    ],
    # --- site / org ---
    "site": [
        "site", "plant", "facility",
        "organization_code", "org_code",
        "inv_org", "inv_organization_id",
        "business_unit",
    ],
    # --- commodity / category ---
    "commodity": [
        "commodity", "commodity_code",
        "category", "category_code",
        "item_category", "purchasing_category",
    ],
    "buyer": [
        "buyer", "buyer_name", "buyer_code",
        "planner", "planner_name",
        "agent", "purchasing_agent",
    ],
}


# ------------------------------------------------------------------
# Cache helpers
# ------------------------------------------------------------------
def _load_cache() -> dict:
    try:
        if _CACHE_FILE.exists():
            return json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_cache(cache: dict) -> None:
    try:
        _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")
    except Exception:
        pass


# ------------------------------------------------------------------
# Discovery
# ------------------------------------------------------------------
def discover_table_columns(
    connector: str,
    schema: str,
    table: str,
    force: bool = False,
) -> list[str]:
    """
    Return the real column names (original case) for [schema].[table].
    Results are cached in config/schema_cache.json.
    Falls back to SELECT TOP 0 if INFORMATION_SCHEMA is unavailable.
    """
    cache = _load_cache()
    cache_key = f"{connector}|{schema}|{table}"
    if not force and cache_key in cache:
        return cache[cache_key]

    try:
        from .db_registry import read_sql

        # Primary: INFORMATION_SCHEMA
        sql = """
        SELECT COLUMN_NAME
        FROM   INFORMATION_SCHEMA.COLUMNS
        WHERE  TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        df = read_sql(connector, sql, [schema, table])
        if not df.attrs.get("_error") and not df.empty:
            col0 = df.columns[0]
            cols = df[col0].tolist()
            cache[cache_key] = cols
            _save_cache(cache)
            return cols

        # Fallback: SELECT TOP 0
        df2 = read_sql(connector, f"SELECT TOP 0 * FROM [{schema}].[{table}]")
        if not df2.attrs.get("_error"):
            cols = list(df2.columns)
            cache[cache_key] = cols
            _save_cache(cache)
            return cols
    except Exception:
        pass

    return []


def discover_all_key_tables(connector: str = "azure_sql") -> dict[str, list[str]]:
    """Discover columns for all tables referenced in brain.yaml. Returns {key: [cols]}."""
    table_map = {
        "parts":            ("edap_dw_replica", "dim_part"),
        "suppliers":        ("edap_dw_replica", "dim_supplier"),
        "po_receipts":      ("edap_dw_replica", "fact_po_receipt"),
        "sales_order_lines":("edap_dw_replica", "fact_sales_order_line"),
        "on_hand":          ("edap_dw_replica", "fact_inventory_on_hand"),
        "open_purchase":    ("edap_dw_replica", "fact_inventory_open_orders"),
        "open_mfg":         ("edap_dw_replica", "fact_inventory_open_mfg_orders"),
        "part_cost":        ("stg_replica",     "fact_part_cost"),
        "po_contract_part": ("edap_dw_replica", "fact_po_contract_part"),
        "ap_invoice_lines": ("edap_dw_replica", "fact_ap_invoice_lines"),
    }
    results = {}
    for logical, (schema, table) in table_map.items():
        cols = discover_table_columns(connector, schema, table)
        results[logical] = cols
    return results


# ------------------------------------------------------------------
# Resolution
# ------------------------------------------------------------------
def resolve(
    columns: list[str],
    semantic: str,
    extra_patterns: list[str] | None = None,
) -> Optional[str]:
    """
    Find the best-matching column for a semantic role from a list of actual column names.
    After standard_clean, columns are already lowercase. We search both cases.
    """
    if not columns:
        return None
    candidates = list(extra_patterns or []) + PATTERNS.get(semantic, [semantic])
    # Build lowercase lookup preserving original
    lower_map: dict[str, str] = {}
    for c in columns:
        lower_map[c.lower()] = c  # last one wins if dupe (shouldn't happen)

    for pat in candidates:
        pat_lc = pat.lower()
        # 1. exact lowercase match
        if pat_lc in lower_map:
            return lower_map[pat_lc]
        # 2. pat is a substring of a column
        for lc, orig in lower_map.items():
            if pat_lc in lc:
                return orig
        # 3. column is a substring of pat (e.g. "qty" matches "ordered_qty")
        for lc, orig in lower_map.items():
            if lc in pat_lc and len(lc) >= 3:
                return orig
    return None


def resolve_from_table(
    connector: str,
    schema: str,
    table: str,
    semantic: str,
    extra_patterns: list[str] | None = None,
) -> Optional[str]:
    """Convenience: discover + resolve in one call."""
    cols = discover_table_columns(connector, schema, table)
    return resolve(cols, semantic, extra_patterns)


def get_col(
    connector: str,
    schema: str,
    table: str,
    semantic: str,
    fallback: str | None = None,
) -> str:
    """Return resolved column name or fallback. Safe for SQL injection (only alphanumeric + _)."""
    found = resolve_from_table(connector, schema, table, semantic)
    result = found or fallback
    if result:
        # Sanitize: allow only word chars (guards against injection via schema_cache)
        import re
        safe = re.sub(r"[^\w]", "", result)
        return safe
    return fallback or semantic  # worst-case: return semantic name as-is
