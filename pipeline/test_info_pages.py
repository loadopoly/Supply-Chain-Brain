"""
Playwright screenshot capture for information-gathering pages.
Saves full-page screenshots to snapshots/info_<page>.png
"""
from __future__ import annotations
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import os

BASE_URL = "http://localhost:8501"
OUT_DIR = "snapshots"
os.makedirs(OUT_DIR, exist_ok=True)

PAGES = [
    ("Supply Chain Brain",   f"{BASE_URL}/Supply_Chain_Brain"),
    ("Supply Chain Pipeline",f"{BASE_URL}/b_Supply_Chain_Pipeline"),
    ("EOQ Deviation",        f"{BASE_URL}/EOQ_Deviation"),
    ("OTD Recursive",        f"{BASE_URL}/OTD_Recursive"),
    ("Procurement 360",      f"{BASE_URL}/Procurement_360"),
    ("Data Quality",         f"{BASE_URL}/Data_Quality"),
    ("Lead-Time Survival",   f"{BASE_URL}/Lead_Time_Survival"),
    ("Bullwhip Effect",      f"{BASE_URL}/Bullwhip"),
    ("Multi-Echelon",        f"{BASE_URL}/Multi_Echelon"),
    ("Sustainability",       f"{BASE_URL}/Sustainability"),
    ("Freight Portfolio",    f"{BASE_URL}/Freight_Portfolio"),
    ("Decision Log",         f"{BASE_URL}/Decision_Log"),
]

SHELL_TIMEOUT  = 60_000
SETTLE_MS      = 4_000   # wait longer so charts/tables fully render


def wait_and_snap(page, label: str, url: str):
    safe = label.replace(" ", "_").replace("/", "_")
    out  = f"{OUT_DIR}/info_{safe}.png"

    print(f"  → {label}")
    page.goto(url, wait_until="domcontentloaded")
    page.wait_for_selector('[data-testid="stApp"]', state="visible", timeout=SHELL_TIMEOUT)
    try:
        page.wait_for_selector('[data-testid="stSpinner"]', state="hidden", timeout=8_000)
    except PWTimeout:
        pass
    # Give charts / dataframes / DBI fragment time to render
    page.wait_for_timeout(SETTLE_MS)

    # Check for error banners
    errors = []
    for el in page.locator('[data-testid="stException"]').all():
        try:
            errors.append(el.inner_text(timeout=1_000)[:200])
        except Exception:
            pass

    page.screenshot(path=out, full_page=True)
    status = "❌ ERROR" if errors else "✅ PASS"
    print(f"     {status}  →  {out}")
    if errors:
        for e in errors:
            print(f"     ERROR: {e}")
    return out, not bool(errors)


def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1400, "height": 900})
        page = ctx.new_page()

        results = []
        for label, url in PAGES:
            path, ok = wait_and_snap(page, label, url)
            results.append((label, path, ok))

        browser.close()

    print("\n" + "="*60)
    passed = sum(1 for _, _, ok in results if ok)
    print(f"  {passed}/{len(results)} pages captured without errors")
    print("="*60)
    for label, path, ok in results:
        mark = "✅" if ok else "❌"
        print(f"  {mark}  {label:30s}  {path}")


if __name__ == "__main__":
    run()
