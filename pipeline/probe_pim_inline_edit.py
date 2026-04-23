"""
Test if existing ABC category assignment can be edited inline in PIM.
We don't need Add Row/Delete - just click the existing value and change it.
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

# Burlington item to test - first item in the change list
TEST_ITEM = "02040RIP4-SPRAY"
NEW_ABC = "B"   # Just testing - not actually changing yet


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


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=80)
        ctx = browser.new_context(viewport={"width": 1600, "height": 900})
        page = ctx.new_page()
        load_cookies(ctx)

        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="networkidle", timeout=120_000)
        page.wait_for_timeout(2000)

        # Navigate to Product Management
        page.locator("text=Product Management").first.click(timeout=10_000)
        page.wait_for_timeout(1500)

        # Click Product Information Management tile
        pim_result = page.evaluate("""
            () => {
                const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
                let node;
                while ((node = walker.nextNode())) {
                    if (node.textContent.trim() === 'Product Information Management') {
                        let el = node.parentElement;
                        for (let i = 0; i < 10 && el; i++) {
                            if (el.tagName === 'A' || el.tagName === 'BUTTON' ||
                                el.getAttribute('role') === 'button') {
                                const r = el.getBoundingClientRect();
                                return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                            }
                            el = el.parentElement;
                        }
                    }
                }
                return null;
            }
        """)
        if pim_result:
            page.mouse.click(pim_result['cx'], pim_result['cy'])
        else:
            print("PIM tile not found, trying text locator")
            page.locator("text=Product Information Management").first.click()
        page.wait_for_timeout(5000)
        page.wait_for_load_state("networkidle", timeout=30_000)
        print(f"Page: {page.title()[:60]}")

        # Search for the item
        print(f"\nSearching for item: {TEST_ITEM}")
        # Find and fill the item search box
        for sel in ["input[placeholder*='tem']", "input[id*='search']", "input[aria-label*='tem']"]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=2000):
                    el.click()
                    el.fill(TEST_ITEM)
                    page.keyboard.press("Enter")
                    page.wait_for_timeout(3000)
                    print(f"Search done via: {sel}")
                    break
            except Exception:
                pass

        page.screenshot(path=str(SCREENSHOTS / "pim_search.png"))

        # Click on the item in search results
        try:
            page.locator(f"text={TEST_ITEM}").first.click(timeout=10_000)
            page.wait_for_timeout(3000)
            print(f"Clicked on item {TEST_ITEM}")
        except Exception as ex:
            print(f"Item click error: {ex}")

        page.screenshot(path=str(SCREENSHOTS / "pim_item_open.png"))
        print(f"Item page: {page.title()[:60]}")

        # Click Categories tab
        for sel in ["a:has-text('Categories')", "[title='Categories']", "span:has-text('Categories')"]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=3000):
                    el.click()
                    page.wait_for_timeout(3000)
                    print(f"Categories tab clicked via: {sel}")
                    break
            except Exception:
                pass

        page.screenshot(path=str(SCREENSHOTS / "pim_categories_tab.png"))

        # Dump the categories table
        print("\n--- Categories tab content ---")
        cats_info = page.evaluate("""
            () => {
                const results = [];
                const seen = new Set();
                for (const el of document.querySelectorAll('td, th, span, div')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim().replace(/\\s+/g, ' ');
                    if (!t || t.length < 2 || t.length > 80 || seen.has(t)) continue;
                    if (el.childElementCount > 2) continue;
                    const r = el.getBoundingClientRect();
                    if (r.y < 100) continue;
                    const lo = t.toLowerCase();
                    if (!lo.includes('abc') && !lo.includes('classif') && !lo.includes('categor') &&
                        !lo.includes('class') && !lo.includes('add') && !lo.includes('delete') &&
                        !lo.includes('edit') && !lo.includes('save')) continue;
                    seen.add(t);
                    results.push({
                        tag: el.tagName,
                        text: t,
                        id: el.id || '',
                        cls: (el.className || '').toString().slice(0, 50),
                        cx: Math.round(r.x + r.width/2),
                        cy: Math.round(r.y + r.height/2),
                        disabled: el.hasAttribute('disabled') ||
                                  (el.className || '').toString().includes('Disabled')
                    });
                }
                return results;
            }
        """)
        for e in cats_info:
            dis = " [DISABLED]" if e['disabled'] else ""
            print(f"  [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> {e['text']}{dis} [{e['cls'][:30]}]")

        # Try to find and click the ABC category VALUE cell specifically
        print("\n--- Looking for ABC Classification row to edit inline ---")
        abc_result = page.evaluate("""
            () => {
                // Find cells in the categories table that have ABC-related content
                const cells = [];
                for (const el of document.querySelectorAll('td, span, div')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim().replace(/\\s+/g, ' ');
                    if (!t || t.length > 80) continue;
                    const lo = t.toLowerCase();
                    if (!lo.includes('abc') && !lo.includes('inventory') &&
                        !lo.includes(' a') && !lo.includes(' b') && !lo.includes(' c')) continue;
                    const r = el.getBoundingClientRect();
                    if (r.y < 100) continue;
                    cells.push({
                        tag: el.tagName,
                        text: t,
                        cls: (el.className || '').toString().slice(0, 60),
                        cx: Math.round(r.x + r.width/2),
                        cy: Math.round(r.y + r.height/2),
                        children: el.childElementCount,
                        clickable: el.tagName === 'A' || el.tagName === 'BUTTON' ||
                                   el.getAttribute('role') === 'button' ||
                                   (el.onclick !== null)
                    });
                }
                return cells;
            }
        """)
        for e in abc_result:
            print(f"  [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> clickable={e['clickable']} children={e['children']} '{e['text']}' [{e['cls'][:40]}]")

        # Take a full screenshot to examine
        page.screenshot(path=str(SCREENSHOTS / "pim_abc_row_inspect.png"))

        # Try double-clicking on potential ABC value cells
        if abc_result:
            print("\n--- Trying to double-click ABC category cells to enable inline edit ---")
            for e in abc_result[:5]:  # Try first 5 candidates
                print(f"  Double-clicking [{e['cx']},{e['cy']}]: {e['text']}")
                page.mouse.dblclick(e['cx'], e['cy'])
                page.wait_for_timeout(1000)

                # Check if any input appeared
                inputs = page.evaluate("""
                    () => {
                        const results = [];
                        for (const el of document.querySelectorAll('input, select')) {
                            if (!el.offsetParent) continue;
                            const r = el.getBoundingClientRect();
                            if (r.y < 100) continue;
                            results.push({tag: el.tagName, type: el.type || '',
                                          value: el.value || '', cx: Math.round(r.x+r.width/2),
                                          cy: Math.round(r.y+r.height/2)});
                        }
                        return results;
                    }
                """)
                if inputs:
                    print(f"    Inputs appeared after dblclick: {inputs}")
                    page.screenshot(path=str(SCREENSHOTS / f"pim_inline_edit_{e['cy']}.png"))
                    break
                else:
                    print(f"    No inputs appeared")

        browser.close()


if __name__ == "__main__":
    main()
