"""
Supply Chain Brain — Playwright UI smoke-test & screenshot benchmark.

Usage:
    cd pipeline
    python test_ui.py

Requirements:
    playwright install chromium
    pip install playwright

What it does:
    1. Opens the running Streamlit app at localhost:8501
    2. Discovers every page from the sidebar navigation
    3. Navigates to each page, waits for it to settle
    4. Checks for Streamlit error banners
    5. Saves a full-page screenshot to snapshots/bench_<page>.png
    6. Prints a pass/fail summary
"""
from __future__ import annotations
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import os, sys, re, json, datetime

BASE_URL = "http://localhost:8501"
SCREENSHOT_DIR = "snapshots"
# How long to wait for the Streamlit app shell to appear (ms)
SHELL_TIMEOUT = 60_000
# How long to wait for the app content to settle after navigation (ms)
SETTLE_TIMEOUT = 20_000
# Small extra pause (ms) after spinners clear so fragments finish rendering
FRAGMENT_SETTLE_MS = 2_500


def _safe_filename(text: str) -> str:
    return re.sub(r"[^\w\-]+", "_", text).strip("_")[:80]


def wait_for_streamlit(page, timeout: int = SETTLE_TIMEOUT) -> None:
    """Wait until Streamlit has finished loading content on the current page."""
    # Wait for the app root container to be present and visible
    page.wait_for_selector('[data-testid="stApp"]', state="visible", timeout=SHELL_TIMEOUT)
    # If there is a spinner / skeleton, wait for it to clear (best-effort)
    try:
        page.wait_for_selector('[data-testid="stSpinner"]', state="hidden", timeout=8_000)
    except PWTimeout:
        pass
    # Allow dynamic fragments (DBI) time to render
    page.wait_for_timeout(FRAGMENT_SETTLE_MS)


def get_error_banners(page) -> list[str]:
    """Return any Streamlit exception/error banners shown on the page."""
    banners: list[str] = []
    for el in page.locator('[data-testid="stException"]').all():
        try:
            banners.append(el.inner_text(timeout=2000)[:400])
        except Exception:
            pass
    return banners


def discover_nav_links(page) -> list[tuple[str, str]]:
    """
    Return the canonical list of all pages registered in app.py.
    Sidebar discovery is used to verify/augment but the master list is
    always the authoritative source so every page is tested even when
    sidebar groups are collapsed.
    """
    # Authoritative list derived from app.py st.navigation() registration.
    # URL paths are Streamlit's convention: strip leading digits/underscores,
    # keep the rest; "1b_..." → "b_...", etc.
    master: list[tuple[str, str]] = [
        ("Query Console",          "/Query_Console"),
        ("Schema Discovery",       "/Schema_Discovery"),
        ("Overview & Graph",       "/Supply_Chain_Brain"),
        ("Supply Chain Pipeline",  "/b_Supply_Chain_Pipeline"),
        ("EOQ Deviation",          "/EOQ_Deviation"),
        ("OTD Recursive",          "/OTD_Recursive"),
        ("Procurement 360",        "/Procurement_360"),
        ("Data Quality",           "/Data_Quality"),
        ("Connectors",             "/Connectors"),
        ("Lead-Time Survival",     "/Lead_Time_Survival"),
        ("Bullwhip Effect",        "/Bullwhip"),
        ("Multi-Echelon",          "/Multi_Echelon"),
        ("Sustainability",         "/Sustainability"),
        ("Freight Portfolio",      "/Freight_Portfolio"),
        ("What-If Sandbox",        "/What_If"),
        ("Decision Log",           "/Decision_Log"),
        ("Benchmarks",             "/Benchmarks"),
        ("Report Creator",         "/Report_Creator"),
        ("Cycle Count Accuracy",   "/Cycle_Count_Accuracy"),
    ]
    links = [(label, BASE_URL + path) for label, path in master]

    # Try sidebar discovery to detect any NEW pages not in master list
    extra: list[tuple[str, str]] = []
    nav = page.locator('[data-testid="stSidebarNav"]')
    try:
        nav.wait_for(state="visible", timeout=8_000)
        known_urls = {u for _, u in links}
        for a in nav.locator("a").all():
            try:
                href = a.get_attribute("href", timeout=2000) or ""
                label = a.inner_text(timeout=2000).strip()
                if href and label:
                    url = BASE_URL + href if href.startswith("/") else href
                    if url not in known_urls:
                        extra.append((label, url))
            except Exception:
                pass
    except PWTimeout:
        pass

    if extra:
        print(f"  [INFO] {len(extra)} extra page(s) found in sidebar (not in master list):")
        for lbl, url in extra:
            print(f"         + {lbl}  ({url})")
        links.extend(extra)

    return links


def run_tests() -> bool:
    os.makedirs(SCREENSHOT_DIR, exist_ok=True)
    results: list[dict] = []

    print(f"\n{'='*64}")
    print(f"  Supply Chain Brain — UI Smoke Test")
    print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*64}\n")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--disable-web-security"])
        ctx = browser.new_context(
            viewport={"width": 1600, "height": 900},
            # Ignore HTTPS errors if any redirects happen
            ignore_https_errors=True,
        )
        # Silence unnecessary console noise
        page = ctx.new_page()
        page.on("console", lambda msg: None)  # suppress console logs

        # ── Land on root ─────────────────────────────────────────────────────
        print(f"[1/N] Connecting to {BASE_URL} ...")
        try:
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=SHELL_TIMEOUT)
            wait_for_streamlit(page)
        except Exception as ex:
            print(f"\n❌  FATAL: Cannot reach {BASE_URL}\n    {ex}\n")
            browser.close()
            return False

        # ── Discover pages ────────────────────────────────────────────────────
        links = discover_nav_links(page)
        print(f"  Found {len(links)} pages to test\n")

        # ── Test each page ────────────────────────────────────────────────────
        for idx, (label, url) in enumerate(links, start=1):
            print(f"[{idx}/{len(links)}] {label}")
            print(f"         URL: {url}")
            rec = {"label": label, "url": url, "status": "PASS", "errors": []}

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=SHELL_TIMEOUT)
                wait_for_streamlit(page)

                # Check for DBI insight block (soft check — doesn't fail the test)
                dbi_found = page.locator(".dbi-container").count() > 0
                if dbi_found:
                    print(f"         DBI: ✅ container found")
                else:
                    print(f"         DBI: ⚠️  container not visible yet (fragment still loading)")

                # Hard check: error banners
                errors = get_error_banners(page)
                if errors:
                    rec["status"] = "FAIL"
                    rec["errors"] = errors
                    for e in errors:
                        print(f"         ⚠️  ERROR: {e[:120]}")
                else:
                    print(f"         Status: ✅ PASS")

                # Screenshot
                fname = _safe_filename(label)
                screenshot_path = os.path.join(SCREENSHOT_DIR, f"bench_{fname}.png")
                page.screenshot(path=screenshot_path, full_page=True)
                print(f"         Screenshot: {screenshot_path}")

            except PWTimeout as ex:
                rec["status"] = "TIMEOUT"
                rec["errors"] = [str(ex)[:200]]
                print(f"         ❌ TIMEOUT: {str(ex)[:120]}")
            except Exception as ex:
                rec["status"] = "ERROR"
                rec["errors"] = [str(ex)[:200]]
                print(f"         ❌ ERROR: {str(ex)[:120]}")

            results.append(rec)
            print()

        browser.close()

    # ── Summary ───────────────────────────────────────────────────────────────
    passed  = [r for r in results if r["status"] == "PASS"]
    failed  = [r for r in results if r["status"] != "PASS"]

    print(f"\n{'='*64}")
    print(f"  RESULTS: {len(passed)}/{len(results)} pages passed")
    print(f"{'='*64}")
    for r in results:
        mark = "✅" if r["status"] == "PASS" else "❌"
        print(f"  {mark} {r['status']:8s}  {r['label']}")
        for e in r["errors"]:
            print(f"            {e[:100]}")

    # Save JSON benchmark report
    report = {
        "timestamp": datetime.datetime.now().isoformat(),
        "base_url": BASE_URL,
        "total": len(results),
        "passed": len(passed),
        "failed": len(failed),
        "pages": results,
    }
    report_path = os.path.join(SCREENSHOT_DIR, "latest_report.json")
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    print(f"\n  Report saved: {report_path}")
    print(f"  Screenshots:  {SCREENSHOT_DIR}/\n")

    return len(failed) == 0


if __name__ == "__main__":
    ok = run_tests()
    sys.exit(0 if ok else 1)
