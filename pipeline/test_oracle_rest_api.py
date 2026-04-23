"""
Probe Oracle Fusion REST API for item category assignments via authenticated Playwright browser.
The browser fetch() inherits the SSO session — no separate auth needed.
"""
import json, sys
from pathlib import Path
from playwright.sync_api import sync_playwright

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HOST          = "https://fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com"
SESSION_FILE  = Path(__file__).parent / "oracle_session.json"
ITEM_NUMBER   = "02040RIP4-SPRAY"
ORG_CODE      = "3165_US_BUR_MFG"


def _load_cookies(context):
    with open(SESSION_FILE) as f:
        raw = json.load(f)
    pw = []
    for c in raw:
        ck = {"name": c["name"], "value": c["value"],
              "domain": c.get("domain", "").lstrip("."),
              "path": c.get("path", "/"),
              "secure": c.get("secure", True),
              "httpOnly": c.get("httpOnly", False)}
        if c.get("expirationDate"):
            ck["expires"] = c["expirationDate"]
        pw.append(ck)
    context.add_cookies(pw)


def probe(page):
    print(f"\n=== Navigating to Oracle Fusion home ===")
    page.goto(HOST + "/fscmUI/faces/FuseWelcome", wait_until="networkidle", timeout=30000)
    url = page.url
    print(f"  Landed on: {url[:80]}")
    if "Sign In" in page.title() or "login" in url.lower():
        print("  ERROR: SSO session expired — re-run login flow first")
        return

    print("\n=== XSRF Token ===")
    xsrf = page.evaluate("""
        async () => {
            const r = await fetch('/fscmRestApi/anticsrf',
                {method:'GET', headers:{'Accept':'application/json'}});
            const j = await r.json();
            return {status: r.status, xsrf: j.xsrftoken || j.antiCSRFToken || JSON.stringify(j)};
        }
    """)
    print(f"  {xsrf}")
    token = xsrf.get("xsrf", "")

    headers = {
        "Accept": "application/json",
        "REST-Framework-Version": "6",
        "X-ORACLE-CSF-TOKEN": token,
    }

    print("\n=== Find item via REST ===")
    find_result = page.evaluate("""
        async ([item, org, hdrs]) => {
            const url = `/fscmRestApi/resources/11.13.18.05/items?q=ItemNumber='${item}' AND OrganizationCode='${org}'&fields=ItemId,ItemNumber,OrganizationId,OrganizationCode&limit=5&onlyData=true`;
            const r = await fetch(url, {headers: hdrs});
            const text = await r.text();
            return {status: r.status, body: text.slice(0, 1000)};
        }
    """, [ITEM_NUMBER, ORG_CODE, headers])
    print(f"  Status: {find_result['status']}")
    print(f"  Body:   {find_result['body']}")

    # Try alternate endpoint name
    print("\n=== Try alternate resource name: productItems ===")
    alt = page.evaluate("""
        async ([item, org, hdrs]) => {
            const url = `/fscmRestApi/resources/11.13.18.05/productItems?q=ItemNumber='${item}' AND OrganizationCode='${org}'&fields=ItemId,ItemNumber&limit=2&onlyData=true`;
            const r = await fetch(url, {headers: hdrs});
            const text = await r.text();
            return {status: r.status, body: text.slice(0, 400)};
        }
    """, [ITEM_NUMBER, ORG_CODE, headers])
    print(f"  Status: {alt['status']}, Body: {alt['body'][:200]}")

    # Try the items API without org filter to see if auth works at all
    print("\n=== items resource (no filter, limit 1) ===")
    bare = page.evaluate("""
        async (hdrs) => {
            const url = `/fscmRestApi/resources/11.13.18.05/items?limit=1&onlyData=true&fields=ItemId,ItemNumber`;
            const r = await fetch(url, {headers: hdrs});
            const text = await r.text();
            return {status: r.status, body: text.slice(0, 500)};
        }
    """, headers)
    print(f"  Status: {bare['status']}, Body: {bare['body'][:300]}")

    # Try discovering available resources
    print("\n=== REST catalog (available resources) ===")
    cat = page.evaluate("""
        async (hdrs) => {
            const r = await fetch('/fscmRestApi/resources', {headers: hdrs});
            const text = await r.text();
            return {status: r.status, body: text.slice(0, 800)};
        }
    """, headers)
    print(f"  Status: {cat['status']}, Body: {cat['body'][:600]}")


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=100)
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        _load_cookies(context)
        page = context.new_page()
        try:
            probe(page)
        finally:
            input("\nPress Enter to close browser...")
            browser.close()


if __name__ == "__main__":
    main()
