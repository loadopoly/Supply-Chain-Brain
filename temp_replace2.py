import sys, re

path = "pipeline/autonomous_agent.py"
with open(path, "r", encoding="utf-8") as f:
    text = f.read()

match = re.search(r"def seed_otd_direct\(\) -> int:.*?(?=def |\Z)", text, re.DOTALL)
if match:
    new_func = """def seed_otd_direct() -> int:
    \"\"\"Lightweight periodic OTD pull.\"\"\"
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.brain.otd_recursive import run_otd_from_replica
        from src.brain.local_store import upsert_otd_owner
        import pandas as pd
        from pathlib import Path
        import logging

        n = 0
        bundle_path = Path(__file__).parent / "docs" / "OTD file.xlsx"
        if bundle_path.exists():
            xf = pd.ExcelFile(bundle_path)
            for sn in ["Missed Yesterday", "Shipping today", "Opened Yesterday"]:
                if sn in xf.sheet_names:
                    w = xf.parse(sn).dropna(how="all")
                    for c in ["Owner", "SO No", "Part", "Site", "Reason why failed", "Review Comment"]:
                        if c not in w.columns: w[c] = ""
                    for _, row in w.iterrows():
                        if str(row.get("SO No", "")).strip() == "": continue
                        rk = f'{str(row["SO No"]).strip()}_{str(row["Part"]).strip()}_{str(row["Site"]).strip()}'
                        ow = str(row["Owner"]).strip()
                        cm = str(row.get("Reason why failed", str(row.get("Review Comment", "")))).strip()
                        if ow and ow.lower() not in ("nan", "none", "") and rk != "__":
                            try:
                                upsert_otd_owner(rk, owner=ow, owner_comment=cm)
                                n += 1
                            except Exception:
                                pass
            if n > 0:
                logging.info(f"OTD seeding: wrote {n} ownership rows from bundle -> otd_classify ground truth.")
                return n

        # Fallback
        work, _ = run_otd_from_replica(connector="azure_sql", where=None, limit=1000)
        if work is None or work.empty:
            return 0

        cols_lower = {c.lower(): c for c in work.columns}
        owner_col = next((cols_lower[h] for h in ["buyer", "planner", "owner", "assigned_to"] if h in cols_lower), None)
        key_col = next((cols_lower[h] for h in ["po_number", "receipt_id", "so_no"] if h in cols_lower), None)

        if not owner_col:
            return 0
        
        for idx, row in work.iterrows():
            rk = str(row[key_col]).strip() if key_col else f"otd_{idx}"
            ow = str(row[owner_col]).strip()
            if ow and ow.lower() not in ("nan", "none", "") and rk:
                try:
                    upsert_otd_owner(rk, owner=ow)
                    n += 1
                except Exception:
                    pass
        if n > 0:
            logging.info(f"OTD seeding: wrote {n} ownership rows from replica -> otd_classify ground truth.")
        return n
    except Exception as e:
        logging.warning(f"OTD direct seeding failed: {e}")
        return 0

"""
    text = text[:match.start()] + new_func + text[match.end():]
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print("Replaced successfully")
else:
    print("Not found")
