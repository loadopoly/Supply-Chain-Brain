import sys, re
path = "pipeline/autonomous_agent.py"
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

old_block = """def seed_otd_direct() -> int:
    \"\"\"Lightweight periodic OTD pull to keep otd_ownership seeded between missions.

    Fulfillment missions populate otd_ownership when they process, but mission
    cycles may not include a fulfillment mission every cycle. This step ensures
    the otd_classify self-train task always has fresh ground truth by pulling a
    1 000-row slice of ERP OTD data every cycle independent of the mission queue.
    Returns the number of ownership rows upserted.
    \"\"\"
    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.brain.otd_recursive import run_otd_from_replica
        from src.brain.local_store import upsert_otd_owner

        work, _ = run_otd_from_replica(connector="azure_sql", where=None, limit=1000)
        if work is None or work.empty:
            logging.info("OTD seeding: no source rows available from replica.")
            return 0

        _OWNER_HINTS = [
            "buyer", "planner", "responsible_party", "purchaser",
            "assigned_to", "owner", "buyer_name", "planner_name",
        ]
        _KEY_HINTS = [
            "po_number", "po_num", "purchase_order", "receipt_id",
            "po_line_id", "receipt_number", "po_id",
        ]
        cols_lower = {c.lower(): c for c in work.columns}
        owner_col = next((cols_lower[h] for h in _OWNER_HINTS if h in cols_lower), None)
        key_col   = next((cols_lower[h] for h in _KEY_HINTS   if h in cols_lower), None)

        if not owner_col:
            logging.info("OTD seeding: no buyer/planner column found in replica \u2014 skipping.")
            return 0

        n = 0
        for idx, row in work.iterrows():
            rk = str(row[key_col]).strip() if key_col else f"otd_{idx}"
            ow = str(row[owner_col]).strip()
            if ow and ow.lower() not in ("nan", "none", "") and rk:
                try:
                    upsert_otd_owner(rk, owner=ow)
                    n += 1
                except Exception:
                    pass

        logging.info(
            f"OTD seeding: wrote {n} ownership rows "
            f"\u2192 otd_classify ground truth ready for next self-train."
        )
        return n
    except Exception as e:
        logging.warning(f"OTD direct seeding failed: {e}")
        return 0"""

new_block = """def seed_otd_direct() -> int:
    \"\"\"Lightweight periodic OTD pull to keep otd_ownership seeded between missions.

    Fulfillment missions populate otd_ownership when they process, but mission
    cycles may not include a fulfillment mission every cycle. This step ensures
    the otd_classify self-train task always has fresh ground truth by pulling a
    slice of ERP OTD data every cycle independent of the mission queue.
    Returns the number of ownership rows upserted.
    \"\"\"
    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.brain.otd_recursive import run_otd_from_replica
        from src.brain.local_store import upsert_otd_owner
        import pandas as pd
        from pathlib import Path

        n = 0
        # 1. Try local bundled excel first for training ground truth (matches Daily Review dashboard)
        bundle_path = Path(__file__).parent / "docs" / "OTD file.xlsx"
        if bundle_path.exists():
            xf = pd.ExcelFile(bundle_path)
            _WORKLISTS = ["Missed Yesterday", "Shipping today", "Opened Yesterday"]
            for sn in _WORKLISTS:
                if sn in xf.sheet_names:
                    w = xf.parse(sn).dropna(how="all")
                    # Needs "Owner" and ID columns
                    for c in ["Owner", "SO No", "Part", "Site", "Reason why failed", "Review Comment"]:
                        if c not in w.columns: w[c] = ""
                    for _, row in w.iterrows():
                        rk = f"{str(row['SO No']).strip()}_{str(row['Part']).strip()}_{str(row['Site']).strip()}"
                        ow = str(row["Owner"]).strip()
                        cm = str(row.get("Reason why failed", str(row.get("Review Comment", "")))).strip()
                        if ow and ow.lower() not in ("nan", "none", "") and rk != "__":
                            try:
                                upsert_otd_owner(rk, owner=ow, owner_comment=cm)
                                n += 1
                            except Exception:
                                pass

        if n > 0:
            logging.info(
                f"OTD seeding: wrote {n} ownership rows from bundle "
                f"\u2192 otd_classify ground truth ready for next self-train."
            )
            return n

        # 2. Fallback to replica
        work, _ = run_otd_from_replica(connector="azure_sql", where=None, limit=1000)
        if work is None or work.empty:
            logging.info("OTD seeding: no source rows available from replica.")
            return 0

        _OWNER_HINTS = ["buyer", "planner", "responsible_party", "purchaser", "assigned_to", "owner", "buyer_name", "planner_name"]
        _KEY_HINTS = ["po_number", "po_num", "purchase_order", "receipt_id", "po_line_id", "receipt_number", "po_id", "so_no"]
        cols_lower = {c.lower(): c for c in work.columns}
        owner_col = next((cols_lower[h] for h in _OWNER_HINTS if h in cols_lower), None)
        key_col   = next((cols_lower[h] for h in _KEY_HINTS   if h in cols_lower), None)

        if not owner_col:
            logging.info("OTD seeding: no buyer/planner column found in replica \u2014 skipping.")
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

        logging.info(
            f"OTD seeding: wrote {n} ownership rows from replica "
            f"\u2192 otd_classify ground truth ready for next self-train."
        )
        return n
    except Exception as e:
        logging.warning(f"OTD direct seeding failed: {e}")
        return 0"""

if old_block in content:
    content = content.replace(old_block, new_block)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print("Success: seed_otd_direct block replaced")
else:
    print("Error: Old block not found!")
