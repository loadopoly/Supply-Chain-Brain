"""
Update ABC Codes in Oracle Fusion PIM for Burlington (3165_US_BUR_MFG).

Source file: Burlington_Strat_Change_04212026.xlsx
Reads the Excel file, connects to Oracle Fusion DEV13 via SSO,
and PATCHes the ABC class for each item in the specified organization.

Usage:
    python update_abc_codes_burlington.py [--dry-run]
"""
import sys
import os
import json
import argparse
import openpyxl
from pathlib import Path

# Add pipeline/src to path so we can import the existing connector
sys.path.insert(0, str(Path(__file__).parent))
from src.connections.oracle_fusion import OracleFusionSession

EXCEL_PATH = (
    r"C:\Users\agard\OneDrive - astecindustries.com"
    r"\Cycle Count Review\Claude Changes\Burlington_Strat_Change_04212026.xlsx"
)

ORG_CODE = "3165_US_BUR_MFG"
FSCM_BASE = "/fscmRestApi/resources/11.13.18.05"


def load_changes() -> list[dict]:
    wb = openpyxl.load_workbook(EXCEL_PATH)
    ws = wb.active
    rows = list(ws.iter_rows(min_row=2, values_only=True))
    changes = []
    for row in rows:
        org, item, subinv, locator, desc, qty, old_abc, new_abc = row
        if not item or not new_abc:
            continue
        changes.append({
            "item": str(item).strip(),
            "subinv": subinv,
            "locator": locator,
            "description": desc,
            "qty": qty,
            "old_abc": str(old_abc).strip() if old_abc else "",
            "new_abc": str(new_abc).strip(),
        })
    return changes


def get_item_org_id(session: OracleFusionSession, item_number: str) -> tuple[str | None, str | None]:
    """Return (ItemId, ItemOrganizationsId) for the item in ORG_CODE, or (None, None)."""
    host = session.host

    # Step 1: find the item by ItemNumber
    url = f"{host}{FSCM_BASE}/items"
    params = {
        "q": f"ItemNumber=eq.'{item_number}'",
        "fields": "ItemId,ItemNumber",
        "limit": 1,
    }
    resp = session.session.get(url, params=params, timeout=session.cfg["request_timeout"])
    if not resp.ok:
        print(f"  [WARN] items query failed for {item_number}: HTTP {resp.status_code}")
        return None, None

    data = resp.json()
    items = data.get("items", [])
    if not items:
        print(f"  [WARN] Item not found in Oracle: {item_number}")
        return None, None

    item_id = items[0].get("ItemId")

    # Step 2: find the org-specific record
    url2 = f"{host}{FSCM_BASE}/items/{item_id}/child/itemsForOrg"
    params2 = {
        "q": f"OrganizationCode=eq.'{ORG_CODE}'",
        "fields": "ItemOrganizationsId,OrganizationCode,ABCClass",
        "limit": 1,
    }
    resp2 = session.session.get(url2, params=params2, timeout=session.cfg["request_timeout"])
    if not resp2.ok:
        print(f"  [WARN] itemsForOrg query failed for {item_number}: HTTP {resp2.status_code}")
        return item_id, None

    data2 = resp2.json()
    org_items = data2.get("items", [])
    if not org_items:
        print(f"  [WARN] Item {item_number} not found in org {ORG_CODE}")
        return item_id, None

    org_id = org_items[0].get("ItemOrganizationsId")
    current_abc = org_items[0].get("ABCClass", "?")
    print(f"  Found: ItemId={item_id}, OrgId={org_id}, current ABCClass={current_abc}")
    return item_id, org_id


def update_abc(session: OracleFusionSession, item_id: str, org_id: str, new_abc: str) -> bool:
    """PATCH the ABCClass for the item-org record. Returns True on success."""
    host = session.host
    url = f"{host}{FSCM_BASE}/items/{item_id}/child/itemsForOrg/{org_id}"
    payload = {"ABCClass": new_abc}
    resp = session.session.patch(url, json=payload, timeout=session.cfg["request_timeout"])
    if resp.ok:
        return True
    print(f"  [ERROR] PATCH failed: HTTP {resp.status_code} — {resp.text[:300]}")
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Query only, do not PATCH")
    args = parser.parse_args()

    changes = load_changes()
    print(f"Loaded {len(changes)} items from Excel.\n")

    # Load vault credentials for FSCM REST API (OWSM requires Basic/Bearer auth)
    from src.connections.secrets import get_credentials
    creds = get_credentials("oracle_fusion")
    if not creds or not creds.get("user") or not creds.get("password"):
        print("ERROR: No oracle_fusion credentials in vault. Run:")
        print("  python -m src.connections.secrets set oracle_fusion --user <user> --password <pass>")
        sys.exit(1)

    import requests
    fscm_session = requests.Session()
    fscm_session.auth = (creds["user"], creds["password"])
    fscm_session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})

    # Wrap in a minimal object so we can reuse the helper functions below
    class _FscmSession:
        def __init__(self):
            self.host = "https://fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com"
            self.session = fscm_session
            self.cfg = {"request_timeout": 120}

    session = _FscmSession()
    print(f"[FSCM] Using Basic auth as {creds['user']}")
    print()

    results = {"success": [], "failed": [], "not_found": []}

    for ch in changes:
        item = ch["item"]
        new_abc = ch["new_abc"]
        old_abc = ch["old_abc"]
        print(f"Processing: {item}  ({old_abc} -> {new_abc})")

        item_id, org_id = get_item_org_id(session, item)
        if not item_id or not org_id:
            results["not_found"].append(item)
            continue

        if args.dry_run:
            print(f"  [DRY-RUN] Would PATCH ABCClass={new_abc}")
            results["success"].append(item)
            continue

        ok = update_abc(session, item_id, org_id, new_abc)
        if ok:
            print(f"  OK — ABCClass updated to {new_abc}")
            results["success"].append(item)
        else:
            results["failed"].append(item)

    print("\n--- Summary ---")
    print(f"  Updated : {len(results['success'])}")
    print(f"  Failed  : {len(results['failed'])}")
    print(f"  Not found: {len(results['not_found'])}")

    if results["failed"]:
        print(f"\nFailed items: {results['failed']}")
    if results["not_found"]:
        print(f"Not found   : {results['not_found']}")


if __name__ == "__main__":
    main()
