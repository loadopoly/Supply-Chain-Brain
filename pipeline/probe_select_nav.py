"""
Focused probe: interact with the Counts SELECT tab in Inventory Management
and explore the Navigator hamburger.
"""
import json, sys
from pathlib import Path
from playwright.sync_api import sync_playwright

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HOST = "https://fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com"
SESSION_FILE = Path(__file__).parent / "oracle_session.json"
SCREENSHOTS = Path(__file__).parent / "abc_screenshots"
SCREENSHOTS.mkdir(exist_ok=True)
ABC_KEYWORDS = ['abc', 'categor assign', 'item categor', 'classif', 'abc analysis', 'abc class', 'assign']


def load_cookies(context):
    with open(SESSION_FILE) as f:
        cookies = json.load(f)
    pw = []
    for c in cookies:
        ck = {"name": c["name"], "value": c["value"],
              "domain": c.get("domain", "").lstrip("."),
              "path": c.get("path", "/"), "secure": c.get("secure", True),
              "httpOnly": c.get("httpOnly", False)}
        if c.get("expires") and c["expires"] > 0:
            ck["expires"] = int(c["expires"])
        pw.append(ck)
    context.add_cookies(pw)


def dump_right_panel(page):
    """Dump all unique visible elements in the right task panel (x > 1100)."""
    els = page.evaluate("""
        () => {
            const results = [];
            const seen = new Set();
            for (const el of document.querySelectorAll('li, a, div, span')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim().replace(/\\s+/g, ' ');
                if (!t || t.length < 3 || t.length > 100 || seen.has(t)) continue;
                if (el.childElementCount > 2) continue;
                const r = el.getBoundingClientRect();
                if (r.x < 1100) continue;
                seen.add(t);
                results.push({tag: el.tagName, text: t,
                              cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)});
            }
            return results;
        }
    """)
    for e in els:
        lo = e['text'].lower()
        star = "***" if any(k in lo for k in ABC_KEYWORDS) else "   "
        print(f"  {star} [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> {e['text']}")
    return els


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=80)
        ctx = browser.new_context(viewport={"width": 1600, "height": 900})
        page = ctx.new_page()
        load_cookies(ctx)

        # Navigate to Inventory Management (Classic)
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="networkidle", timeout=120_000)
        page.wait_for_timeout(2000)

        page.locator("a:has-text('Supply Chain Execution')").first.click(timeout=10_000)
        page.wait_for_timeout(3000)

        # Find + click the tile
        tile_result = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('a, button')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim().replace(/\\s+/g, ' ');
                    if (!t.includes('Inventory Management') || !t.includes('Classic')) continue;
                    const r = el.getBoundingClientRect();
                    if (r.y < 290) continue;
                    return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                }
                return {cx: 1107, cy: 483};
            }
        """)
        page.mouse.click(tile_result["cx"], tile_result["cy"])
        page.wait_for_timeout(5000)
        page.wait_for_load_state("networkidle", timeout=20_000)
        print(f"Inventory Management loaded: {page.title()[:60]}")

        # Open task panel
        for sel in ["[title='Tasks']", "[aria-label='Tasks']"]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=2000):
                    el.click()
                    page.wait_for_timeout(2000)
                    print(f"Task panel opened via: {sel}")
                    break
            except Exception:
                pass

        # Cycle through ALL SELECT options via mouse click + keyboard
        # Start at option 0 (Inventory), use ArrowDown to cycle
        # Reset to option 0 first
        page.mouse.click(1288, 107)
        page.wait_for_timeout(300)
        # Press Home to go to first option
        page.keyboard.press("Home")
        page.keyboard.press("Enter")
        page.wait_for_timeout(1000)

        for i, option_name in enumerate(['Inventory', 'Counts', 'Shipments', 'Picks', 'Receipts']):
            if i == 0:
                # Already at Inventory, just dump
                pass
            else:
                page.mouse.click(1288, 107)
                page.wait_for_timeout(300)
                page.keyboard.press("Home")
                page.wait_for_timeout(100)
                for _ in range(i):
                    page.keyboard.press("ArrowDown")
                    page.wait_for_timeout(100)
                page.keyboard.press("Enter")
                page.wait_for_timeout(2000)

            page.screenshot(path=str(SCREENSHOTS / f"tab_{option_name.lower()}.png"))
            print(f"\n=== {option_name} tab ===")
            els = page.evaluate("""
                () => {
                    const results = [];
                    const seen = new Set();
                    for (const el of document.querySelectorAll('li, div')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim().replace(/\\s+/g, ' ');
                        if (!t || t.length < 3 || t.length > 100 || seen.has(t)) continue;
                        if (el.childElementCount > 2) continue;
                        const r = el.getBoundingClientRect();
                        if (r.x < 1100) continue;
                        seen.add(t);
                        results.push({tag: el.tagName, text: t,
                                      cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)});
                    }
                    return results;
                }
            """)
            for e in els:
                lo = e['text'].lower()
                star = "***" if any(k in lo for k in ABC_KEYWORDS) else "   "
                print(f"  {star} [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> {e['text']}")

        # === Return to SCE and scroll to see ALL tiles ===
        print("\n=== SCE SpringBoard - ALL tiles (with scrolling) ===")
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="networkidle", timeout=60_000)
        page.wait_for_timeout(1000)
        page.locator("a:has-text('Supply Chain Execution')").first.click(timeout=10_000)
        page.wait_for_timeout(3000)

        # Scroll down to see all tiles
        all_tiles = []
        seen_tiles = set()
        for scroll_y in [0, 300, 600, 900, 1200]:
            page.evaluate(f"window.scrollTo(0, {scroll_y})")
            page.wait_for_timeout(500)
            batch = page.evaluate("""
                () => {
                    const results = [];
                    const seen = new Set();
                    for (const el of document.querySelectorAll('a')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim().replace(/\\s+/g, ' ');
                        if (!t || t.length < 3 || t.length > 80) continue;
                        const r = el.getBoundingClientRect();
                        if (r.y < 200 || r.height < 15) continue;
                        results.push({text: t, cx: Math.round(r.x+r.width/2),
                                      cy: Math.round(r.y+r.height/2)});
                    }
                    return results;
                }
            """)
            for t in batch:
                if t['text'] not in seen_tiles:
                    seen_tiles.add(t['text'])
                    all_tiles.append(t)

        for t in sorted(all_tiles, key=lambda x: x['cy']):
            lo = t['text'].lower()
            star = "***" if ('abc' in lo or 'classif' in lo or
                              'cycle' in lo or 'count' in lo) else "   "
            print(f"  {star} [{t['cx']:4d},{t['cy']:4d}] {t['text']}")

        # === Try Navigator hamburger to find ALL tasks ===
        print("\n=== Navigator (hamburger) task search ===")
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="networkidle", timeout=60_000)
        page.wait_for_timeout(1000)
        # Click Navigator hamburger
        page.mouse.click(24, 57)
        page.wait_for_timeout(2000)
        page.screenshot(path=str(SCREENSHOTS / "navigator_open.png"))

        # Look for ABC-related items in the navigator
        nav_items = page.evaluate("""
            () => {
                const results = [];
                const seen = new Set();
                for (const el of document.querySelectorAll('a, li, span, div')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim().replace(/\\s+/g, ' ');
                    if (!t || t.length < 3 || t.length > 100 || seen.has(t)) continue;
                    if (el.childElementCount > 2) continue;
                    const r = el.getBoundingClientRect();
                    if (r.x > 500) continue;  // left navigator area
                    seen.add(t);
                    results.push({tag: el.tagName, text: t,
                                  cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)});
                }
                return results;
            }
        """)
        print(f"Navigator items ({len(nav_items)}):")
        for e in nav_items:
            lo = e['text'].lower()
            star = "***" if any(k in lo for k in ABC_KEYWORDS) else "   "
            print(f"  {star} [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> {e['text']}")

        browser.close()

        # Navigate to Inventory Management (Classic)
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="networkidle", timeout=120_000)
        page.wait_for_timeout(2000)

        page.locator("a:has-text('Supply Chain Execution')").first.click(timeout=10_000)
        page.wait_for_timeout(3000)

        # Find + click the tile
        tile_result = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('a, button')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim().replace(/\\s+/g, ' ');
                    if (!t.includes('Inventory Management') || !t.includes('Classic')) continue;
                    const r = el.getBoundingClientRect();
                    if (r.y < 290) continue;
                    return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                }
                return {cx: 1107, cy: 483};
            }
        """)
        page.mouse.click(tile_result["cx"], tile_result["cy"])
        page.wait_for_timeout(5000)
        page.wait_for_load_state("networkidle", timeout=20_000)
        print(f"Inventory Management loaded: {page.title()[:60]}")

        # Open task panel
        for sel in ["[title='Tasks']", "[aria-label='Tasks']"]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=2000):
                    el.click()
                    page.wait_for_timeout(2000)
                    print(f"Task panel opened via: {sel}")
                    break
            except Exception:
                pass

        # === Approach 1: Native click + keyboard on the SELECT ===
        print("\n=== Approach 1: SELECT via mouse click + keyboard ===")
        try:
            # Click on the SELECT to open it
            page.mouse.click(1288, 107)
            page.wait_for_timeout(800)
            page.screenshot(path=str(SCREENSHOTS / "select_opened.png"))

            # Press arrow down to navigate to "Counts" (option 2)
            page.keyboard.press("ArrowDown")
            page.wait_for_timeout(500)
            page.keyboard.press("Enter")
            page.wait_for_timeout(3000)
            page.screenshot(path=str(SCREENSHOTS / "select_counts.png"))
            print("After selecting Counts:")
            dump_right_panel(page)

            # Try Shipments
            page.mouse.click(1288, 107)
            page.wait_for_timeout(500)
            page.keyboard.press("ArrowDown")
            page.keyboard.press("ArrowDown")
            page.keyboard.press("Enter")
            page.wait_for_timeout(2000)
            page.screenshot(path=str(SCREENSHOTS / "select_shipments.png"))
            print("\nAfter selecting Shipments:")
            dump_right_panel(page)
        except Exception as ex:
            print(f"SELECT approach error: {ex}")

        # === Approach 2: Look at ALL tiles on SCE SpringBoard ===
        print("\n=== Approach 2: Return to SCE and enumerate ALL tiles ===")
        try:
            page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="networkidle", timeout=60_000)
            page.wait_for_timeout(1000)
            page.locator("a:has-text('Supply Chain Execution')").first.click(timeout=10_000)
            page.wait_for_timeout(3000)
            page.screenshot(path=str(SCREENSHOTS / "sce_springboard.png"))

            # Dump ALL tile A elements
            tiles = page.evaluate("""
                () => {
                    const results = [];
                    const seen = new Set();
                    for (const el of document.querySelectorAll('a')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim().replace(/\\s+/g, ' ');
                        if (!t || t.length < 3 || t.length > 80 || seen.has(t)) continue;
                        const r = el.getBoundingClientRect();
                        if (r.y < 290 || r.height < 20) continue;
                        seen.add(t);
                        results.push({text: t, cx: Math.round(r.x+r.width/2),
                                      cy: Math.round(r.y+r.height/2)});
                    }
                    return results;
                }
            """)
            print(f"SCE tiles ({len(tiles)}):")
            for t in tiles:
                lo = t['text'].lower()
                star = "***" if ('abc' in lo or 'classif' in lo or 'assign' in lo or
                                  'count' in lo or 'cycle' in lo) else "   "
                print(f"  {star} [{t['cx']:4d},{t['cy']:4d}] {t['text']}")
        except Exception as ex:
            print(f"SCE tiles error: {ex}")

        # === Approach 3: Try direct URL probes for ABC tasks ===
        print("\n=== Approach 3: Direct URL probes ===")
        abc_urls = [
            f"{HOST}/fscmUI/faces/FuseWelcome?fnd=%3B2%3Bfnd%3BManageAbcAnalysisMain",
            f"{HOST}/fscmUI/faces/FuseWelcome?fnd=%3B2%3Bfnd%3BManageAbcAssignmentMain",
            f"{HOST}/fscmUI/faces/FuseWelcome?fnd=%3B2%3Bfnd%3BAbcAnalysisMain",
            f"{HOST}/fscmUI/faces/FuseWelcome?fnd=%3B2%3Bfnd%3BManageAbcClassifications",
        ]
        for url in abc_urls:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=15_000)
                page.wait_for_timeout(2000)
                title = page.title()
                pg_url = page.url
                print(f"  URL probe: {title[:60]} | {pg_url[:80]}")
                if 'abc' in title.lower() or 'classif' in title.lower() or 'assign' in title.lower():
                    print(f"  *** ABC TASK FOUND! ***")
                    page.screenshot(path=str(SCREENSHOTS / "abc_task_found.png"))
            except Exception as ex:
                print(f"  Probe error: {ex}")

        browser.close()


if __name__ == "__main__":
    main()
