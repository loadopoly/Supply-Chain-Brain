"""Analytics logic for Cycle Count compliance. 

Replaces the legacy Power BI templates by converting DAX / Power Query
rules into native Pandas operations running directly against live Azure SQL
or Oracle configurations discovered via data pipelines.
"""
from __future__ import annotations

import pandas as pd
from datetime import date
from typing import Any
from src.brain.db_registry import read_sql
from src.deck.live import _AzureMeta, _discover_itr_source, _nvarchar, _date_expr, _float

def fetch_and_calculate_cycle_counts(conn, current_year: int | None = None) -> pd.DataFrame:
    """Builds a part-level cycle count compliance table replicating the legacy DAX measures."""
    meta = _AzureMeta(conn)
    current_year = current_year or date.today().year

    # 1. Use the data discovery already completed
    # This automatically finds live cycle count data whether configured as fact_cycle_count or another table.
    source = _discover_itr_source(meta)
    if not source:
        # If not deployed to Azure SQL, fallback mechanism could go here.
        # Returning an empty dataframe structure equivalent to the full pipeline output.
        return pd.DataFrame(columns=[
            "Part", "Description", "ABC", "Subinventory", "Year", 
            "Q1_Count", "Q2_Count", "Q3_Count", "Q4_Count", "Total_Counts",
            "Required_Counts", "Pass_YTD", "Q1_Pass", "Q2_Pass", "Q3_Pass", "Q4_Pass"
        ])

    schema, table, mapped = source
    date_expr = _date_expr(meta, schema, table, 'itr', mapped['date'])
    item_col = mapped['item']
    
    # Check if item column matches part_key or part_number
    join_col = "part_key" if "key" in item_col.lower() or "id" in item_col.lower() else "part_number"

    # Query live transactions for the requested year
    sql = f"""
    WITH counts AS (
        SELECT 
            {date_expr} AS [Count Date],
            {_nvarchar(f'itr.[{item_col}]')} AS [Item_Match],
            {_float(f'itr.[{mapped["qty"]}]')} AS [Count_Qty],
            {_float(f'itr.[{mapped["net"]}]') if mapped['net'] else '0.0'} AS [Variance_Dollar]
        FROM [{schema}].[{table}] itr
        WHERE YEAR({date_expr}) = ?
    )
    SELECT
        c.[Count Date] as count_date,
        c.Item_Match as part_number,
        c.Count_Qty as count_qty,
        c.Variance_Dollar as abs_dollar_var,
        dp.part_description as description,
        COALESCE(dpc.inventory_part_code, dpc.sales_part_code, CASE WHEN inv.quantity_on_hand > 0 THEN 'D' ELSE NULL END) as abc_code,
        CASE WHEN inv.part_quantity_min IS NOT NULL THEN 'Y' ELSE 'N' END as wh 
    FROM counts c
    LEFT JOIN [edap_dw_replica].[dim_part] dp ON dp.{join_col} = c.Item_Match   
    LEFT JOIN [edap_dw_replica].[dim_part_code] dpc ON dpc.part_key = dp.part_key AND dpc.current_record_ind = 1
    LEFT JOIN (
        SELECT part_key, quantity_on_hand, part_quantity_min, ROW_NUMBER() OVER(PARTITION BY part_key ORDER BY aud_update_datetime DESC) as rn 
        FROM [edap_dw_replica].[fact_inventory_on_hand]
    ) inv ON inv.rn = 1 AND inv.part_key = dp.part_key
    """
    df_raw = pd.read_sql(sql, conn, params=[current_year])

    if df_raw.empty:
        return pd.DataFrame(columns=[
            "Part", "Description", "ABC", "Subinventory", "Year", 
            "Q1_Count", "Q2_Count", "Q3_Count", "Q4_Count", "Total_Counts",
            "Required_Counts", "Pass_YTD", "Q1_Pass", "Q2_Pass", "Q3_Pass", "Q4_Pass"
        ])

    return _aggregate_and_evaluate_counts(df_raw, current_year)

def _aggregate_and_evaluate_counts(df_raw, current_year: int):
    """Core logic to aggregate raw transactions and apply standard ABC compliance rules."""
    if df_raw.empty:
        return __import__('pandas').DataFrame(columns=[
            "Part", "Description", "ABC", "Subinventory", "Year",
            "Q1_Count", "Q2_Count", "Q3_Count", "Q4_Count", "Total_Counts",     
            "Required_Counts", "Pass_YTD", "Q1_Pass", "Q2_Pass", "Q3_Pass", "Q4_Pass"
        ])

    df_raw["abc_code"] = df_raw["abc_code"].fillna("D").astype(str)
    df_raw["count_date"] = __import__('pandas').to_datetime(df_raw["count_date"])
    df_raw["Qtr"] = df_raw["count_date"].dt.quarter

    df_agg = df_raw.groupby(["part_number", "description", "abc_code"]).apply(  
        lambda g: __import__('pandas').Series({
            "Q1_Count": (g["Qtr"] == 1).sum(),
            "Q2_Count": (g["Qtr"] == 2).sum(),
            "Q3_Count": (g["Qtr"] == 3).sum(),
            "Q4_Count": (g["Qtr"] == 4).sum(),
            "Total_Counts": len(g),
            "Abs_Dollar_Var": g["abs_dollar_var"].sum() if "abs_dollar_var" in g else 0.0
        })
    ).reset_index()

    df_agg["ABC"] = df_agg["abc_code"].str.upper().str.strip()
    df_agg["ABC"] = df_agg["ABC"].apply(lambda x: x if x in ["A", "B", "C"] else "D")
    df_agg["Year"] = current_year

    def get_required(abc):
        if abc == "A": return 4
        elif abc == "B": return 2
        else: return 1

    def check_pass_ytd(row):
        abc = row["ABC"]
        if abc == "A": return 1 if (row["Q1_Count"] >= 1 and row["Q2_Count"] >= 1 and row["Q3_Count"] >= 1 and row["Q4_Count"] >= 1) else 0
        elif abc == "B": return 1 if ((row["Q1_Count"] + row["Q2_Count"] >= 1) and (row["Q3_Count"] + row["Q4_Count"] >= 1)) else 0
        else: return 1 if (row["Q1_Count"] + row["Q2_Count"] + row["Q3_Count"] + row["Q4_Count"] >= 1) else 0

    def check_q1(row):
        abc = row["ABC"]
        if abc == "A": return 1 if row["Q1_Count"] >= 1 else 0
        elif abc == "B": return 1 if (row["Q1_Count"] + row["Q2_Count"] >= 1) else 0
        else: return 1 if row["Total_Counts"] >= 1 else 0

    def check_q2(row):
        abc = row["ABC"]
        if abc == "A": return 1 if row["Q2_Count"] >= 1 else 0
        elif abc == "B": return 1 if (row["Q1_Count"] + row["Q2_Count"] >= 1) else 0
        else: return 1 if row["Total_Counts"] >= 1 else 0

    def check_q3(row):
        abc = row["ABC"]
        if abc == "A": return 1 if row["Q3_Count"] >= 1 else 0
        elif abc == "B": return 1 if (row["Q3_Count"] + row["Q4_Count"] >= 1) else 0
        else: return 1 if row["Total_Counts"] >= 1 else 0

    def check_q4(row):
        abc = row["ABC"]
        if abc == "A": return 1 if row["Q4_Count"] >= 1 else 0
        elif abc == "B": return 1 if (row["Q3_Count"] + row["Q4_Count"] >= 1) else 0
        else: return 1 if row["Total_Counts"] >= 1 else 0

    df_agg["Required_Counts"] = df_agg["ABC"].apply(get_required)
    df_agg["Pass_YTD"] = df_agg.apply(check_pass_ytd, axis=1)
    df_agg["Q1_Pass"] = df_agg.apply(check_q1, axis=1)
    df_agg["Q2_Pass"] = df_agg.apply(check_q2, axis=1)
    df_agg["Q3_Pass"] = df_agg.apply(check_q3, axis=1)
    df_agg["Q4_Pass"] = df_agg.apply(check_q4, axis=1)

    df_agg.rename(columns={"part_number": "Part", "description": "Description"}, inplace=True)
    return df_agg

def process_uploaded_cycle_counts(df_upload, current_year: int):
    cols = [c.lower().strip() for c in df_upload.columns]
    date_col = next((c for c in df_upload.columns if "date" in c.lower()), None)
    part_col = next((c for c in df_upload.columns if "part" in c.lower() or "item" in c.lower()), None)
    qty_col = next((c for c in df_upload.columns if "qty" in c.lower() or "quant" in c.lower() or "count" in c.lower() and "qty" in c.lower()), None)
    desc_col = next((c for c in df_upload.columns if "desc" in c.lower()), None)
    abc_col = next((c for c in df_upload.columns if "abc" in c.lower() or "class" in c.lower()), None)

    if not date_col or not part_col:
        raise ValueError(f"Uploaded data missing required 'Date' or 'Part' column. Found columns: {list(df_upload.columns)}")

    df_raw = __import__('pandas').DataFrame()
    df_raw["count_date"] = __import__('pandas').to_datetime(df_upload[date_col], errors="coerce")
    df_raw["part_number"] = df_upload[part_col].astype(str)
    df_raw["count_qty"] = __import__('pandas').to_numeric(df_upload[qty_col] if qty_col else 1, errors="coerce").fillna(1)
    df_raw["description"] = df_upload[desc_col].astype(str) if desc_col else "Uploaded Part"
    df_raw["abc_code"] = df_upload[abc_col].astype(str) if abc_col else "D"
    df_raw["abs_dollar_var"] = 0.0

    df_raw = df_raw.dropna(subset=["count_date", "part_number"])
    return _aggregate_and_evaluate_counts(df_raw, current_year)

