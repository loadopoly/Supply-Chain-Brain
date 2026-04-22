"""
Live-data loader for the Cross-Dataset deck.

Builds the four canonical datasets from the configured Azure SQL replica and,
when available, discovers a cycle-count transaction source for the ITR feed.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

from src.connections import azure_sql

from .schemas import ITR as ITR_SCHEMA
from .windows import Windows, default_anchor


@dataclass
class LiveDeckData:
    otd: pd.DataFrame
    ifr: pd.DataFrame
    itr: pd.DataFrame
    pfep: pd.DataFrame
    warnings: list[str]


class _AzureMeta:
    def __init__(self, conn):
        self.conn = conn
        self._cols: dict[tuple[str, str], dict[str, str]] = {}
        self._tables: list[tuple[str, str]] | None = None

    def columns(self, schema: str, table: str) -> dict[str, str]:
        key = (schema, table)
        if key not in self._cols:
            rows = azure_sql.list_columns(self.conn, schema, table)
            self._cols[key] = {row["column"]: row["type"] for row in rows}
        return self._cols[key]

    def has_table(self, schema: str, table: str) -> bool:
        return bool(self.columns(schema, table))

    def has_col(self, schema: str, table: str, col: str) -> bool:
        return col in self.columns(schema, table)

    def pick(self, schema: str, table: str, *candidates: str) -> str | None:
        cols = self.columns(schema, table)
        for candidate in candidates:
            if candidate in cols:
                return candidate
        return None

    def sql_type(self, schema: str, table: str, col: str) -> str | None:
        return self.columns(schema, table).get(col)

    def list_tables(self) -> list[tuple[str, str]]:
        if self._tables is None:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                ORDER BY TABLE_SCHEMA, TABLE_NAME
                """
            )
            self._tables = [(row[0], row[1]) for row in cur.fetchall()]
            cur.close()
        return self._tables


def _resolve_site_filter(meta: _AzureMeta, conn, site: str, warnings: list[str]) -> set[str] | None:
    """Map a friendly site name to the set of values we'll see in the [Site] column.

    Returns None if no filter should be applied (site == 'ALL'), or an empty set
    if the user supplied a value but no match was found (caller will warn).
    """
    if site in (None, "", "ALL"):
        return None
    candidates: set[str] = set()
    if meta.has_col("edap_dw_replica", "dim_part", "business_unit_id"):
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT CAST([business_unit_id] AS nvarchar(120)) "
                "FROM [edap_dw_replica].[dim_part] "
                "WHERE [business_unit_id] IS NOT NULL"
            )
            candidates.update(row[0] for row in cur.fetchall() if row[0] is not None)
            cur.close()
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"Could not enumerate dim_part.business_unit_id: {exc}")

    site_low = site.lower()
    matches = {c for c in candidates if c and c.lower() == site_low}
    if not matches:
        # Fuzzy: try short tokens from the friendly name (e.g. "Manufacturers", "Mfr", "CHM")
        tokens = [t for t in site.replace("-", " ").split() if len(t) >= 3]
        for c in candidates:
            cl = (c or "").lower()
            if any(t.lower() in cl for t in tokens):
                matches.add(c)
    # Last-resort manual mapping for known Astec sites that don't surface text codes
    if not matches:
        manual = {
            "chattanooga - manufacturers road": ["CHM", "MFR", "MFRS", "MFRSRD", "MFG", "MR"],
            "chattanooga - jerome avenue":      ["JERO", "JER"],
            "chattanooga - wilson road":        ["WILS", "WIL"],
        }
        seeds = manual.get(site_low, [])
        for c in candidates:
            cu = (c or "").upper()
            if any(s in cu for s in seeds):
                matches.add(c)
    if matches:
        warnings.append(f"Site '{site}' resolved to business_unit_id values: {sorted(matches)}")
    return matches


def load_live_datasets(site: str = "ALL", anchor: date | None = None) -> LiveDeckData:
    anchor_date = anchor or default_anchor()
    win = Windows(anchor_date)
    start_90, end_90 = win.baseline_90d
    start_365 = anchor_date - timedelta(days=365)

    conn = azure_sql.get_connection()
    warnings: list[str] = []
    try:
        meta = _AzureMeta(conn)
        site_values = _resolve_site_filter(meta, conn, site, warnings)
        otd = _load_otd(conn, meta, start_90, end_90, warnings)
        ifr = _load_ifr(conn, meta, start_90, end_90, warnings)
        pfep = _load_pfep(conn, meta, start_365, end_90, warnings)
        itr = _load_itr(conn, meta, start_90, end_90, warnings)
    finally:
        conn.close()

    if site_values is not None:
        if not site_values:
            warnings.append(f"Site filter '{site}' did not match any business_unit_id; deck will be empty.")
            otd = otd.iloc[0:0].copy()
            ifr = ifr.iloc[0:0].copy()
            pfep = pfep.iloc[0:0].copy() if "Site" in pfep.columns else pfep
        else:
            otd = otd[otd["Site"].isin(site_values)].copy()
            ifr = ifr[ifr["Site"].isin(site_values)].copy()
            if "Site" in pfep.columns:
                pfep = pfep[pfep["Site"].isin(site_values) | pfep["Site"].isna()].copy()
            if otd.empty and ifr.empty:
                warnings.append(f"Site filter '{site}' matched no live OTD or IFR rows after projection.")

    return LiveDeckData(otd=otd, ifr=ifr, itr=itr, pfep=pfep, warnings=warnings)


def _date_expr(meta: _AzureMeta, schema: str, table: str, alias: str, col: str | None) -> str:
    if not col:
        return "CAST(NULL AS date)"
    dtype = (meta.sql_type(schema, table, col) or "").lower()
    if "date" in dtype or "time" in dtype:
        return f"TRY_CONVERT(date, {alias}.[{col}])"
    # Guard negative sentinels (e.g. -1 used in some ERP date-key columns to mean "no date")
    return (
        f"TRY_CONVERT(date, CONVERT(varchar(8), "
        f"CASE WHEN {alias}.[{col}] > 0 THEN CAST({alias}.[{col}] AS bigint) ELSE NULL END), 112)"
    )


def _nvarchar(expr: str, length: int = 200) -> str:
    return f"CAST({expr} AS nvarchar({length}))"


def _float(expr: str) -> str:
    return f"TRY_CONVERT(float, {expr})"


def _sql_text(value: str) -> str:
    return "N'" + value.replace("'", "''") + "'"


def _inventory_cte(meta: _AzureMeta) -> str:
    if not meta.has_table("edap_dw_replica", "fact_inventory_on_hand"):
        return ""
    inv_cols = meta.columns("edap_dw_replica", "fact_inventory_on_hand")
    qty = "quantity_on_hand"
    demand = "demand_order_part_quantity" if "demand_order_part_quantity" in inv_cols else None
    available = "available_qty" if "available_qty" in inv_cols else None
    available_expr = (
        _float(f"i.[{available}]")
        if available
        else (
            f"COALESCE({_float(f'i.[{qty}]')}, 0.0) - COALESCE({_float(f'i.[{demand}]')}, 0.0)"
            if demand
            else f"COALESCE({_float(f'i.[{qty}]')}, 0.0)"
        )
    )
    orderer = _date_expr(meta, "edap_dw_replica", "fact_inventory_on_hand", "i", "snapshot_day_key") + " DESC"
    if "aud_update_datetime" in inv_cols:
        orderer += ", i.[aud_update_datetime] DESC"
    return f"""
    , inv_latest AS (
        SELECT
            i.[business_unit_key],
            i.[part_key],
            {_float('i.[quantity_on_hand]')} AS quantity_on_hand,
            {available_expr} AS available_qty,
            {_float('i.[safety_stock_limit]') if 'safety_stock_limit' in inv_cols else 'CAST(NULL AS float)'} AS safety_stock_limit,
            {_float('i.[part_quantity_min]') if 'part_quantity_min' in inv_cols else 'CAST(NULL AS float)'} AS part_quantity_min,
            {_float('i.[part_quantity_max]') if 'part_quantity_max' in inv_cols else 'CAST(NULL AS float)'} AS part_quantity_max,
            {_float('i.[order_lead_time]') if 'order_lead_time' in inv_cols else 'CAST(NULL AS float)'} AS order_lead_time,
            {_float('i.[part_price_local]') if 'part_price_local' in inv_cols else 'CAST(NULL AS float)'} AS part_price_local,
            {_float('i.[part_price_usd]') if 'part_price_usd' in inv_cols else 'CAST(NULL AS float)'} AS part_price_usd,
            {('i.[last_supplier_key]') if 'last_supplier_key' in inv_cols else 'CAST(NULL AS bigint)'} AS last_supplier_key,
            {(_nvarchar('i.[pre_standardization_supplier_name]')) if 'pre_standardization_supplier_name' in inv_cols else 'CAST(NULL AS nvarchar(200))'} AS supplier_name_raw,
            ROW_NUMBER() OVER (
                PARTITION BY i.[business_unit_key], i.[part_key]
                ORDER BY {orderer}
            ) AS rn
        FROM [edap_dw_replica].[fact_inventory_on_hand] i
    )
    """


def _cost_cte(meta: _AzureMeta) -> str:
    if not meta.has_table("stg_replica", "fact_part_cost"):
        return ""
    cols = meta.columns("stg_replica", "fact_part_cost")
    orderer = (
        "CASE WHEN UPPER(CAST(c.[current_record_ind] AS nvarchar(10))) "
        "IN (N'1', N'Y', N'YES', N'TRUE', N'T') THEN 0 ELSE 1 END"
    )
    if "effective_date" in cols:
        orderer += ", c.[effective_date] DESC"
    return f"""
    , cost_current AS (
        SELECT
            c.[business_unit_key],
            c.[part_key],
            COALESCE(
                {_float('c.[total_cost_amount_usd]') if 'total_cost_amount_usd' in cols else 'CAST(NULL AS float)'},
                {_float('c.[cost_amount_local]') if 'cost_amount_local' in cols else 'CAST(NULL AS float)'}
            ) AS unit_cost,
            ROW_NUMBER() OVER (
                PARTITION BY c.[business_unit_key], c.[part_key]
                ORDER BY {orderer}
            ) AS rn
        FROM [stg_replica].[fact_part_cost] c
    )
    """


def _site_expr(meta: _AzureMeta, alias: str) -> str:
    if meta.has_table("edap_dw_replica", "dim_business_unit"):
        col = meta.pick(
            "edap_dw_replica",
            "dim_business_unit",
            "site_name",
            "business_unit_name",
            "plant_name",
            "name",
            "description",
        )
        if col:
            return _nvarchar(f"bu.[{col}]")
    if meta.has_col("edap_dw_replica", "dim_part", "business_unit_id"):
        return f"COALESCE({_nvarchar('dp.[business_unit_id]')}, {_nvarchar(f'{alias}.[business_unit_key]', 120)})"
    return _nvarchar(f"{alias}.[business_unit_key]", 120)


def _customer_name_expr(meta: _AzureMeta, alias: str) -> str:
    if meta.has_table("edap_dw_replica", "dim_customer"):
        col = meta.pick(
            "edap_dw_replica",
            "dim_customer",
            "customer_name",
            "name",
            "customer_display_name",
        )
        if col:
            return _nvarchar(f"dc.[{col}]")
    return _nvarchar(f"{alias}.[customer_key]")


def _customer_no_expr(meta: _AzureMeta, alias: str) -> str:
    if meta.has_table("edap_dw_replica", "dim_customer"):
        col = meta.pick(
            "edap_dw_replica",
            "dim_customer",
            "customer_number",
            "customer_no",
            "account_number",
        )
        if col:
            return _nvarchar(f"dc.[{col}]")
    return _nvarchar(f"{alias}.[customer_key]")


def _part_pur_fab_expr(meta: _AzureMeta, alias: str) -> str:
    if meta.has_col("edap_dw_replica", "dim_part", "fabricated_purchased"):
        return f"""
        CASE
            WHEN LOWER(LTRIM(RTRIM(COALESCE({alias}.[fabricated_purchased], '')))) LIKE 'fab%' THEN 'fabricated'
            WHEN LOWER(LTRIM(RTRIM(COALESCE({alias}.[fabricated_purchased], '')))) LIKE 'make%' THEN 'fabricated'
            WHEN LOWER(LTRIM(RTRIM(COALESCE({alias}.[fabricated_purchased], '')))) LIKE 'buy%' THEN 'purchased'
            WHEN LOWER(LTRIM(RTRIM(COALESCE({alias}.[fabricated_purchased], '')))) LIKE 'pur%' THEN 'purchased'
            ELSE LOWER(LTRIM(RTRIM(COALESCE({alias}.[fabricated_purchased], 'purchased'))))
        END
        """
    if meta.has_col("edap_dw_replica", "dim_part", "part_type"):
        return f"""
        CASE
            WHEN LOWER(COALESCE({alias}.[part_type], '')) IN ('make', 'manufactured', 'fabricated') THEN 'fabricated'
            ELSE 'purchased'
        END
        """
    return _sql_text("purchased")


def _supplier_expr(meta: _AzureMeta, part_expr: str) -> str:
    if meta.has_table("edap_dw_replica", "dim_supplier") and meta.has_col("edap_dw_replica", "dim_supplier", "supplier_name"):
        return f"COALESCE({_nvarchar('ds.[supplier_name]')}, inv.supplier_name_raw, CASE WHEN {part_expr} = 'fabricated' THEN N'ASTEC' ELSE NULL END)"
    return f"COALESCE(inv.supplier_name_raw, CASE WHEN {part_expr} = 'fabricated' THEN N'ASTEC' ELSE NULL END)"


def _active_status_expr(meta: _AzureMeta, alias: str) -> str:
    if meta.has_col("edap_dw_replica", "dim_part", "current_record_ind"):
        return (
            f"CASE WHEN UPPER(CAST({alias}.[current_record_ind] AS nvarchar(10))) "
            f"IN (N'1', N'Y', N'YES', N'TRUE', N'T') THEN N'20 - Active' ELSE N'Inactive' END"
        )
    if meta.has_col("edap_dw_replica", "dim_part", "expiry_date"):
        return f"CASE WHEN {alias}.[expiry_date] IS NULL OR {alias}.[expiry_date] >= CAST(GETDATE() AS date) THEN N'20 - Active' ELSE N'Inactive' END"
    return _sql_text("20 - Active")


def _abc_expr(meta: _AzureMeta, alias: str) -> str:
    col = meta.pick("edap_dw_replica", "dim_part", "inventory_part_code", "sales_part_code")
    return _nvarchar(f"{alias}.[{col}]") if col else "CAST(NULL AS nvarchar(100))"


def _load_otd(conn, meta: _AzureMeta, start_90: date, end_90: date, warnings: list[str]) -> pd.DataFrame:
    if not meta.has_table("edap_dw_replica", "fact_sales_order_line"):
        raise RuntimeError("Azure SQL table edap_dw_replica.fact_sales_order_line is required for live OTD.")
    if not meta.has_table("edap_dw_replica", "fact_inventory_on_hand"):
        raise RuntimeError("Azure SQL table edap_dw_replica.fact_inventory_on_hand is required for live OTD.")

    sales = "edap_dw_replica"
    table = "fact_sales_order_line"
    ship_date = _date_expr(meta, sales, table, "sol", meta.pick(sales, table, "ship_day_key"))
    order_date = _date_expr(meta, sales, table, "sol", meta.pick(sales, table, "order_date_key", "aud_create_datetime"))
    promised_date = _date_expr(meta, sales, table, "sol", meta.pick(sales, table, "promised_ship_day_key", "estimated_ship_date_key"))
    adjusted_date = _date_expr(meta, sales, table, "sol", meta.pick(sales, table, "adjusted_promise_date", "promised_ship_day_key", "estimated_ship_date_key"))
    part_pur_fab = _part_pur_fab_expr(meta, "dp")
    failure_expr = (
        _nvarchar("sol.[failure_reason]")
        if meta.has_col(sales, table, "failure_reason")
        else f"CASE WHEN {ship_date} > {adjusted_date} THEN N'Unknown / not captured' ELSE N'' END"
    )

    supplier_join_key = "COALESCE(inv.[last_supplier_key], sol.[last_supplier_key])" if meta.has_col(sales, table, "last_supplier_key") else "inv.[last_supplier_key]"
    customer_join = (
        "LEFT JOIN [edap_dw_replica].[dim_customer] dc ON dc.[customer_key] = sol.[customer_key]"
        if meta.has_table("edap_dw_replica", "dim_customer") and meta.has_col(sales, table, "customer_key") and meta.has_col("edap_dw_replica", "dim_customer", "customer_key")
        else ""
    )
    business_unit_join = (
        "LEFT JOIN [edap_dw_replica].[dim_business_unit] bu ON bu.[business_unit_key] = sol.[business_unit_key]"
        if meta.has_table("edap_dw_replica", "dim_business_unit") and meta.has_col("edap_dw_replica", "dim_business_unit", "business_unit_key")
        else ""
    )
    supplier_join = (
        f"LEFT JOIN [edap_dw_replica].[dim_supplier] ds ON ds.[supplier_key] = {supplier_join_key}"
        if meta.has_table("edap_dw_replica", "dim_supplier") and meta.has_col("edap_dw_replica", "dim_supplier", "supplier_key")
        else ""
    )

    sql = f"""
    WITH base AS (
        SELECT 1 AS keepalive
    )
    {_inventory_cte(meta)}
    SELECT
        {_site_expr(meta, 'sol')} AS [Site],
        {order_date} AS [Order Date],
        {ship_date} AS [Ship Date],
        {_nvarchar('sol.[sales_order_number]')} AS [SO No],
        {_nvarchar('sol.[sales_order_line]')} AS [Line No],
        COALESCE({_nvarchar('dp.[part_number]') if meta.has_col('edap_dw_replica', 'dim_part', 'part_number') else 'NULL'}, {_nvarchar('sol.[part_key]')}) AS [Part],
        COALESCE({_float('sol.[sales_order_quantity]')}, 0.0) AS [Qty],
        inv.available_qty AS [Available Qty],
        inv.quantity_on_hand AS [On Hand Qty],
        CASE WHEN {ship_date} > {adjusted_date} THEN 1 ELSE 0 END AS [OTD Miss (Late)],
        CASE WHEN {ship_date} IS NULL OR {adjusted_date} IS NULL THEN NULL ELSE DATEDIFF(day, {adjusted_date}, {ship_date}) END AS [Days Late],
        {_customer_name_expr(meta, 'sol')} AS [Customer],
        {_customer_no_expr(meta, 'sol')} AS [Customer No],
        {_supplier_expr(meta, part_pur_fab)} AS [Supplier Name],
        {part_pur_fab} AS [Part Pur/Fab],
        {failure_expr} AS [Failure Reason],
        {promised_date} AS [Promised Date],
        {adjusted_date} AS [Adjusted Promise Date]
    FROM [edap_dw_replica].[fact_sales_order_line] sol
    LEFT JOIN inv_latest inv
      ON inv.rn = 1
     AND inv.[business_unit_key] = sol.[business_unit_key]
     AND inv.[part_key] = sol.[part_key]
    LEFT JOIN [edap_dw_replica].[dim_part] dp
      ON dp.[part_key] = sol.[part_key]
    {supplier_join}
    {customer_join}
    {business_unit_join}
    WHERE {ship_date} BETWEEN ? AND ?
      AND {_float('sol.[sales_order_quantity]')} IS NOT NULL
    """
    df = pd.read_sql(sql, conn, params=[start_90, end_90])
    if not meta.has_col(sales, table, "failure_reason"):
        warnings.append("Live OTD source is missing failure_reason; deck uses 'Unknown / not captured' for late lines.")
    adj_col = meta.pick(sales, table, "adjusted_promise_date", "promised_ship_day_key", "estimated_ship_date_key")
    if adj_col == "estimated_ship_date_key":
        warnings.append("OTD using estimated_ship_date_key as promised-date baseline; "
                        "preferred column promised_ship_day_key was not found. Verify OTD% accuracy.")
    return df


def _load_ifr(conn, meta: _AzureMeta, start_90: date, end_90: date, warnings: list[str]) -> pd.DataFrame:
    if not meta.has_table("edap_dw_replica", "fact_sales_order_line"):
        raise RuntimeError("Azure SQL table edap_dw_replica.fact_sales_order_line is required for live IFR.")
    if not meta.has_table("edap_dw_replica", "fact_inventory_on_hand"):
        raise RuntimeError("Azure SQL table edap_dw_replica.fact_inventory_on_hand is required for live IFR.")

    sales = "edap_dw_replica"
    table = "fact_sales_order_line"
    order_date = _date_expr(meta, sales, table, "sol", meta.pick(sales, table, "order_date_key", "aud_create_datetime"))
    part_pur_fab = _part_pur_fab_expr(meta, "dp")
    failure_expr = _nvarchar("sol.[failure_reason]") if meta.has_col(sales, table, "failure_reason") else "N''"

    supplier_join_key = "COALESCE(inv.[last_supplier_key], sol.[last_supplier_key])" if meta.has_col(sales, table, "last_supplier_key") else "inv.[last_supplier_key]"
    customer_join = (
        "LEFT JOIN [edap_dw_replica].[dim_customer] dc ON dc.[customer_key] = sol.[customer_key]"
        if meta.has_table("edap_dw_replica", "dim_customer") and meta.has_col(sales, table, "customer_key") and meta.has_col("edap_dw_replica", "dim_customer", "customer_key")
        else ""
    )
    business_unit_join = (
        "LEFT JOIN [edap_dw_replica].[dim_business_unit] bu ON bu.[business_unit_key] = sol.[business_unit_key]"
        if meta.has_table("edap_dw_replica", "dim_business_unit") and meta.has_col("edap_dw_replica", "dim_business_unit", "business_unit_key")
        else ""
    )
    supplier_join = (
        f"LEFT JOIN [edap_dw_replica].[dim_supplier] ds ON ds.[supplier_key] = {supplier_join_key}"
        if meta.has_table("edap_dw_replica", "dim_supplier") and meta.has_col("edap_dw_replica", "dim_supplier", "supplier_key")
        else ""
    )

    sql = f"""
    WITH base AS (
        SELECT 1 AS keepalive
    )
    {_inventory_cte(meta)}
    SELECT
        {_site_expr(meta, 'sol')} AS [Site],
        {order_date} AS [Order Date],
        COALESCE({_nvarchar('dp.[part_number]') if meta.has_col('edap_dw_replica', 'dim_part', 'part_number') else 'NULL'}, {_nvarchar('sol.[part_key]')}) AS [Part],
        COALESCE({_float('sol.[sales_order_quantity]')}, 0.0) AS [SO Qty],
        COALESCE(inv.available_qty, 0.0) AS [Available Qty],
        COALESCE(inv.quantity_on_hand, 0.0) AS [On Hand Qty],
        CASE WHEN COALESCE(inv.available_qty, 0.0) >= COALESCE({_float('sol.[sales_order_quantity]')}, 0.0) THEN 1 ELSE 0 END AS [Hit Miss],
        {part_pur_fab} AS [Part Fab/Pur],
        {failure_expr} AS [Failure],
        {_customer_name_expr(meta, 'sol')} AS [Customer Name],
        {_supplier_expr(meta, part_pur_fab)} AS [Supplier Name]
    FROM [edap_dw_replica].[fact_sales_order_line] sol
    LEFT JOIN inv_latest inv
      ON inv.rn = 1
     AND inv.[business_unit_key] = sol.[business_unit_key]
     AND inv.[part_key] = sol.[part_key]
    LEFT JOIN [edap_dw_replica].[dim_part] dp
      ON dp.[part_key] = sol.[part_key]
    {supplier_join}
    {customer_join}
    {business_unit_join}
    WHERE {order_date} BETWEEN ? AND ?
      AND {_float('sol.[sales_order_quantity]')} IS NOT NULL
    """
    df = pd.read_sql(sql, conn, params=[start_90, end_90])
    if not meta.has_col(sales, table, "failure_reason"):
        warnings.append("Live IFR source is missing failure_reason; IFR miss theming will be sparse.")
    warnings.append(
        "IFR uses current fact_inventory_on_hand snapshot (not point-in-time); "
        "fill-rate will appear near 0% for historical order windows. "
        "Treat IFR as indicative of current stock coverage, not historical fill rate."
    )
    return df


def _load_pfep(conn, meta: _AzureMeta, start_365: date, end_90: date, warnings: list[str]) -> pd.DataFrame:
    if not meta.has_table("edap_dw_replica", "dim_part"):
        raise RuntimeError("Azure SQL table edap_dw_replica.dim_part is required for live PFEP.")
    if not meta.has_table("edap_dw_replica", "fact_inventory_on_hand"):
        raise RuntimeError("Azure SQL table edap_dw_replica.fact_inventory_on_hand is required for live PFEP.")

    sales = "edap_dw_replica"
    order_date = _date_expr(meta, sales, "fact_sales_order_line", "sol", meta.pick(sales, "fact_sales_order_line", "order_date_key", "aud_create_datetime"))
    usage_cte = f"""
    , usage_agg AS (
        SELECT
            sol.[business_unit_key],
            sol.[part_key],
            SUM(COALESCE({_float('sol.[sales_order_quantity]')}, 0.0)) AS total_usage
        FROM [edap_dw_replica].[fact_sales_order_line] sol
        WHERE {order_date} BETWEEN ? AND ?
        GROUP BY sol.[business_unit_key], sol.[part_key]
    )
    """
    abc_expr = _abc_expr(meta, "dp")
    part_pur_fab = _part_pur_fab_expr(meta, "dp")
    buyer_expr = _nvarchar("dp.[category_manager]") if meta.has_col("edap_dw_replica", "dim_part", "category_manager") else "CAST(NULL AS nvarchar(200))"
    cost_expr = (
        "COALESCE(cc.unit_cost, inv.part_price_usd, inv.part_price_local, 0.0)"
        if meta.has_table("stg_replica", "fact_part_cost")
        else "COALESCE(inv.part_price_usd, inv.part_price_local, 0.0)"
    )
    has_dp_bu_key = meta.has_col("edap_dw_replica", "dim_part", "business_unit_key")
    cost_join = (
        "LEFT JOIN cost_current cc ON cc.rn = 1 AND cc.[part_key] = dp.[part_key] AND "
        + ("cc.[business_unit_key] = dp.[business_unit_key]" if has_dp_bu_key else "1=1")
        if meta.has_table("stg_replica", "fact_part_cost")
        else ""
    )
    supplier_join = (
        "LEFT JOIN [edap_dw_replica].[dim_supplier] ds ON ds.[supplier_key] = inv.[last_supplier_key]"
        if meta.has_table("edap_dw_replica", "dim_supplier") and meta.has_col("edap_dw_replica", "dim_supplier", "supplier_key")
        else ""
    )
    inv_bu_clause = "inv.[business_unit_key] = dp.[business_unit_key]" if has_dp_bu_key else "1=1"
    usage_bu_clause = "u.[business_unit_key] = dp.[business_unit_key]" if has_dp_bu_key else "1=1"
    sql = f"""
    WITH base AS (
        SELECT 1 AS keepalive
    )
    {_inventory_cte(meta)}
    {_cost_cte(meta)}
    {usage_cte}
    SELECT
        COALESCE({_nvarchar('dp.[part_number]') if meta.has_col('edap_dw_replica', 'dim_part', 'part_number') else 'NULL'}, {_nvarchar('dp.[part_key]')}) AS [Item Name],
        {_active_status_expr(meta, 'dp')} AS [Item Status],
        {part_pur_fab} AS [Make or Buy],
        {_supplier_expr(meta, part_pur_fab)} AS [Supplier],
        {buyer_expr} AS [Buyer Name],
        {cost_expr} AS [Cost],
        COALESCE(u.total_usage, 0.0) AS [Total Usage],
        COALESCE(u.total_usage, 0.0) * {cost_expr} AS [Usage Value],
        COALESCE(inv.safety_stock_limit, 0.0) AS [Safety Stock],
        COALESCE(inv.part_quantity_min, 0.0) AS [Minimum Quantity],
        COALESCE(inv.part_quantity_max, 0.0) AS [Maximum Quantity],
        COALESCE(inv.order_lead_time, 0.0) AS [Processing Lead Time],
        COALESCE({abc_expr}, CASE WHEN COALESCE(inv.quantity_on_hand, 0.0) > 0.0 THEN 'D' ELSE NULL END) AS [ABC Inventory Catalog],
        CASE WHEN {abc_expr} IS NOT NULL OR COALESCE(inv.quantity_on_hand, 0.0) > 0.0 OR COALESCE(inv.safety_stock_limit, 0.0) > 0 THEN 1 ELSE 0 END AS [Item Cycle Count Enabled],
        CASE WHEN inv.part_quantity_min IS NOT NULL OR inv.part_quantity_max IS NOT NULL THEN N'Min/Max' ELSE NULL END AS [Inventory Planning Method],
        CASE WHEN inv.safety_stock_limit IS NOT NULL THEN N'Manual' ELSE NULL END AS [Safety Stock Planning Method],
        {(_nvarchar('dp.[business_unit_id]') if meta.has_col('edap_dw_replica', 'dim_part', 'business_unit_id') else 'CAST(NULL AS nvarchar(120))')} AS [Site]
    FROM [edap_dw_replica].[dim_part] dp
    LEFT JOIN inv_latest inv
      ON inv.rn = 1
     AND inv.[part_key] = dp.[part_key]
     AND ({inv_bu_clause})
    LEFT JOIN usage_agg u
      ON u.[part_key] = dp.[part_key]
     AND ({usage_bu_clause})
    {cost_join}
    {supplier_join}
    WHERE {_active_status_expr(meta, 'dp')} = N'20 - Active'
    """
    df = pd.read_sql(sql, conn, params=[start_365, end_90])
    if meta.pick("edap_dw_replica", "dim_part", "inventory_part_code", "sales_part_code") is None:
        warnings.append("Live PFEP source is missing inventory ABC columns; PFEP health findings will flag this as a data gap.")
    return df

def _itr_empty() -> pd.DataFrame:
    cols = ITR_SCHEMA["required"] + ITR_SCHEMA["optional"]
    return pd.DataFrame(columns=cols)


def _discover_itr_source(meta: _AzureMeta) -> tuple[str, str, dict[str, str | None]] | None:
    date_candidates = ["Transaction Date", "transaction_date", "CountCompletedDate", "CompleteDate", "post_date"]
    item_candidates = ["Item Name", "item_name", "part_number", "Part", "SEGMENT1", "inventory_item_id"]
    qty_candidates = ["Quantity", "quantity", "CountedQty", "TotCountQOH", "qty_counted"]
    net_candidates = ["Net Dollar", "net_dollar", "NetDollar", "value_var", "variance_dollar"]
    sub_candidates = ["Subinventory", "subinventory", "SubInventoryCode", "WarehouseCode", "whse"]
    reason_candidates = ["Transaction Reason Code", "reason_code", "DiscrepancyReasonCode", "CDRCode"]
    type_candidates = ["Transaction Type", "transaction_type", "TransactionType"]

    for schema, table in meta.list_tables():
        name = f"{schema}.{table}".lower()
        if not any(token in name for token in ("count", "cycle")):
            continue
        cols = meta.columns(schema, table)
        mapped = {
            "date": next((c for c in date_candidates if c in cols), None),
            "item": next((c for c in item_candidates if c in cols), None),
            "qty": next((c for c in qty_candidates if c in cols), None),
            "net": next((c for c in net_candidates if c in cols), None),
            "sub": next((c for c in sub_candidates if c in cols), None),
            "reason": next((c for c in reason_candidates if c in cols), None),
            "type": next((c for c in type_candidates if c in cols), None),
        }
        if mapped["date"] and mapped["item"] and mapped["qty"] and mapped["sub"] and mapped["reason"]:
            return schema, table, mapped
    return None


def _load_itr(conn, meta: _AzureMeta, start_90: date, end_90: date, warnings: list[str]) -> pd.DataFrame:
    source = _discover_itr_source(meta)
    if source is None:
        warnings.append("No cycle-count transaction table was discovered in the attached Azure SQL schemas; cycle-count slides will render with no live transactions.")
        return _itr_empty()

    schema, table, mapped = source
    date_expr = _date_expr(meta, schema, table, "itr", mapped["date"])
    type_expr = _nvarchar(f"itr.[{mapped['type']}]") if mapped["type"] else _sql_text("Cycle Count Adjustment")
    net_expr = _float(f"itr.[{mapped['net']}]") if mapped["net"] else "CAST(0.0 AS float)"
    created_by = meta.pick(schema, table, "Created By", "created_by", "createdby")
    updated_by = meta.pick(schema, table, "Last Updated By", "last_updated_by", "updated_by")
    sql = f"""
    SELECT
        {date_expr} AS [Transaction Date],
        {type_expr} AS [Transaction Type],
        {_nvarchar(f'itr.[{mapped["item"]}]')} AS [Item Name],
        {_float(f'itr.[{mapped["qty"]}]')} AS [Quantity],
        {net_expr} AS [Net Dollar],
        {_nvarchar(f'itr.[{mapped["sub"]}]')} AS [Subinventory],
        {_nvarchar(f'itr.[{mapped["reason"]}]')} AS [Transaction Reason Code],
        {_nvarchar(f'itr.[{created_by}]') if created_by else 'CAST(NULL AS nvarchar(200))'} AS [Created By],
        {_nvarchar(f'itr.[{updated_by}]') if updated_by else 'CAST(NULL AS nvarchar(200))'} AS [Last Updated By]
    FROM [{schema}].[{table}] itr
    WHERE {date_expr} BETWEEN ? AND ?
    """
    warnings.append(f"Cycle-count live source discovered dynamically from {schema}.{table}.")
    return pd.read_sql(sql, conn, params=[start_90, end_90])
