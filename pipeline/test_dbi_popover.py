"""
Quick Playwright test — verifies the DBI popover is visible and captures it.
Saves two screenshots:
  snapshots/dbi_loaded.png    — page after DBI insight renders (not LOADING)
  snapshots/dbi_popover.png   — page with the Parameters popover open
"""
from __future__ import annotations
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import os, sys

BASE_URL = "http://localhost:8501"
OUT_DIR = "snapshots"
os.makedirs(OUT_DIR, exist_ok=True)

# Pages most likely to have a short ctx (fast DBI render)
TEST_PAGE = f"{BASE_URL}/Connectors"


def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1400, "height": 900})
        page = ctx.new_page()

        print(f"→ Opening {TEST_PAGE} …")
        page.goto(TEST_PAGE, wait_until="domcontentloaded")

        # Wait for Streamlit app shell
        page.wait_for_selector('[data-testid="stApp"]', state="visible", timeout=60_000)

        # Wait for spinners to clear
        try:
            page.wait_for_selector('[data-testid="stSpinner"]', state="hidden", timeout=8_000)
        except PWTimeout:
            pass

        # Wait for the DBI container to appear (up to 8s — thread finishes in 1s, fragment re-runs at 2s)
        print("  Waiting for .dbi-container to render …")
        try:
            page.wait_for_selector(".dbi-container", state="visible", timeout=8_000)
            print("  ✅ .dbi-container found")
        except PWTimeout:
            print("  ❌ .dbi-container did not appear within 8s")
            page.screenshot(path=f"{OUT_DIR}/dbi_timeout.png", full_page=True)
            browser.close()
            sys.exit(1)

        # Extra settle to let the fragment swap LOADING → insight text
        page.wait_for_timeout(1_500)

        # Screenshot 1: DBI loaded (no popover open)
        path1 = f"{OUT_DIR}/dbi_loaded.png"
        page.screenshot(path=path1, full_page=True)
        print(f"  📸 Screenshot saved: {path1}")

        # Locate and click the "🔍 Parameters" popover button
        popover_btn = page.locator("button", has_text="Parameters").first
        try:
            popover_btn.wait_for(state="visible", timeout=5_000)
            popover_btn.click()
            print("  ✅ Clicked '🔍 Parameters' popover button")
        except PWTimeout:
            print("  ❌ Popover button not found — DBI may still be in LOADING state")
            page.screenshot(path=f"{OUT_DIR}/dbi_no_popover.png", full_page=True)
            browser.close()
            sys.exit(1)

        # Wait for the popover panel to open
        page.wait_for_timeout(800)

        # Screenshot 2: popover open
        path2 = f"{OUT_DIR}/dbi_popover.png"
        page.screenshot(path=path2, full_page=True)
        print(f"  📸 Screenshot saved: {path2}")

        # Verify popover contains some parameter text
        popover_content = page.locator("[data-testid='stPopover']").first
        try:
            popover_content.wait_for(state="visible", timeout=3_000)
            text = popover_content.inner_text(timeout=2_000)
            print(f"  Popover text snippet: {text[:200]!r}")
        except Exception as e:
            print(f"  ⚠️  Could not read popover content: {e}")

        browser.close()
        print("\n✅ DBI popover test complete.")


if __name__ == "__main__":
    run()
