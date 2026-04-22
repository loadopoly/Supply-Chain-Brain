import re
with open('pipeline/src/brain/cycle_count.py', 'r', encoding='utf-8') as f:
    text = f.read()

replacement = '''    return _aggregate_and_evaluate_counts(df_raw, current_year)

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
'''

text = re.sub(r'    df_raw\["abc_code"\].*return df_agg\n', replacement, text, flags=re.DOTALL)
with open('pipeline/src/brain/cycle_count.py', 'w', encoding='utf-8') as f:
    f.write(text)