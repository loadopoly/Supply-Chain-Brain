"""
Oracle Fusion Session Capture
===============================
Opens a browser window for manual login, then saves cookies to oracle_session.json.
Run this whenever the session expires and the mapper shows "Sign In".

Usage:
    python capture_session.py

1. The script opens Chrome pointing at the Oracle Fusion login page.
2. Log in manually (SSO, MFA, etc.).
3. Once the home page (FuseWelcome) is visible, press ENTER in this terminal.
4. Cookies are saved to oracle_session.json and the browser closes.
"""
import json, sys
from pathlib import Path
from playwright.sync_api import sync_playwright

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HOST = "https://fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com"
SESSION_FILE = Path(__file__).parent / "oracle_session.json"


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=0)
        ctx = browser.new_context(viewport={"width": 1600, "height": 900})
        page = ctx.new_page()

        print(f"Opening Oracle Fusion login page...")
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome",
                  wait_until="domcontentloaded", timeout=60_000)

        print(f"\nCurrent page: {page.title()}")
        print("\n>>> Log in manually in the browser window.")
        print(">>> Once you see the Oracle Fusion home page, press ENTER here.")
        input()

        print(f"Page after login: {page.title()}")

        # Grab all cookies from the context
        cookies = ctx.cookies()
        with open(SESSION_FILE, 'w', encoding='utf-8') as f:
            json.dump(cookies, f, indent=2)

        print(f"\nSaved {len(cookies)} cookies to: {SESSION_FILE}")
        print("Session capture complete. You can now run oracle_schema_mapper.py.")
        browser.close()


if __name__ == "__main__":
    main()
