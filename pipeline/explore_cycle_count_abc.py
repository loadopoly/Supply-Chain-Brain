"""
Explore and update Cycle Count ABC Classifications for Burlington items.
Navigates to Inventory Management (Classic) > Counts > Manage Cycle Counts
for org 3165_US_BUR_MFG and updates ABC classes per the Burlington spreadsheet.
"""
import json, sys, time
from pathlib import Path
import pandas as pd
from playwright.sync_api import sync_playwright

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HOST = "https://fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com"
SESSION_FILE = Path(__file__).parent / "oracle_session.json"
SS_DIR = Path(__file__).parent / "abc_screenshots" / "burlington"
SS_DIR.mkdir(parents=True, exist_ok=True)

# Burlington items to update: {item_number: new_class}
BURLINGTON_UPDATES = {
    "02040RIP4-SPRAY": "D",
    "114-15721-01": "P",
    "212-00011-04": "D",
    "22459YXF-SPRAY": "D",
    "298-00114-93": "D",
    "30001167": "D",
    "30003263": "D",
    "30004360": "D",
    "398-02077-73": "D",
    "398-11000-19": "D",
    "398-11000-22": "D",
    "398-14000-21": "D",
    "398-20000-23": "D",
    "398-20000-25": "D",
    "398-20000-37": "D",
    "398-20000-39": "D",
    "399-20442-36": "P",
    "60-66593-01": "D",
}

def load_session(context):
    with open(SESSION_FILE) as f:
        cookies = json.load(f)
    pw = []
    for c in cookies:
        ck = {"name": c["name"], "value": c["value"],
              "domain": c.get("domain", "").lstrip("."),
              "path": c.get("path", "/"),
              "secure": c.get("secure", True),
              "httpOnly": c.get("httpOnly", False)}
        if c.get("expires") and c["expires"] > 0:
            ck["expires"] = int(c["expires"])
        pw.append(ck)
    context.add_cookies(pw)


def ss(page, name):
    try:
        page.screenshot(path=str(SS_DIR / f"{name}.png"))
        print(f"  [screenshot] {name}.png")
    except Exception:
        pass


def navigate_to_inventory_mgmt_counts(page):
    """Navigate to Inventory Management (Classic) and click Manage Cycle Counts."""
    print("\nNavigating to Oracle Fusion home...")
    page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(2000)
    print(f"Home: {page.title()[:60]}")

    # Click Supply Chain Execution tab using Playwright locator (same as mapper)
    print("Clicking Supply Chain Execution tab...")
    clicked = False
    for attempt in ["a:has-text('Supply Chain Execution')", "[role='tab']:has-text('Supply Chain Execution')"]:
        try:
            loc = page.locator(attempt).first
            if loc.is_visible(timeout=4000):
                loc.click()
                page.wait_for_timeout(2500)
                clicked = True
                break
        except Exception:
            pass

    if not clicked:
        # JS fallback
        res = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('a,div,span')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim().replace(/\\s+/g, ' ');
                    if (t !== 'Supply Chain Execution') continue;
                    const r = el.getBoundingClientRect();
                    if (r.y > 280 || r.y < 40) continue;
                    el.click();
                    return true;
                }
                return false;
            }
        """)
        page.wait_for_timeout(2500)
        if not res:
            print("ERROR: Could not click Supply Chain Execution tab")
            return False

    ss(page, "01_sce_tab")

    # Click Show More
    for _ in range(3):
        found = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('a,button,span,div')) {
                    if (!el.offsetParent) continue;
                    if (el.textContent.trim() === 'Show More') { el.click(); return true; }
                }
                return false;
            }
        """)
        if found:
            page.wait_for_timeout(1500)
        else:
            break

    ss(page, "01b_show_more")

    # Use same navigate_to_module_by_text approach as oracle_schema_mapper.py
    print("Clicking Inventory Management (Classic)...")
    result = page.evaluate("""
        ([txt]) => {
            for (const el of document.querySelectorAll('a')) {
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (t !== txt) continue;
                const r = el.getBoundingClientRect();
                if (r.y < 150 || r.height < 10) continue;
                const style = window.getComputedStyle(el);
                if (style.visibility === 'hidden' || style.display === 'none'
                    || parseFloat(style.opacity) < 0.1) continue;
                let parent = el.parentElement, hidden = false;
                for (let i = 0; i < 6 && parent; i++) {
                    const ps = window.getComputedStyle(parent);
                    if (ps.visibility === 'hidden' || ps.display === 'none') { hidden = true; break; }
                    parent = parent.parentElement;
                }
                if (hidden) continue;
                el.scrollIntoView({block:'center', behavior:'instant'});
                const r2 = el.getBoundingClientRect();
                return {cx: Math.round(r2.x+r2.width/2), cy: Math.round(r2.y+r2.height/2)};
            }
            return null;
        }
    """, ["Inventory Management (Classic)"])

    if result:
        page.mouse.click(result['cx'], result['cy'])
        page.wait_for_timeout(5000)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=15_000)
        except Exception:
            pass
        page.wait_for_timeout(2000)
    else:
        print("ERROR: Could not find Inventory Management (Classic) tile")
        return False

    print(f"Module: {page.title()[:70]}")
    ss(page, "02_inv_mgmt")

    # Open task panel
    print("Opening task panel...")
    for sel in ["[title='Tasks']", "[aria-label='Tasks']", "button[title='Tasks']"]:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=5000):
                el.click()
                page.wait_for_timeout(2500)
                break
        except Exception:
            pass

    ss(page, "03_task_panel")

    # Select Counts tab in task panel
    print("Switching to Counts task panel tab...")
    # Find and click the SELECT element for Counts
    select_result = page.evaluate("""
        () => {
            for (const sel of document.querySelectorAll('select')) {
                if (!sel.offsetParent) continue;
                const r = sel.getBoundingClientRect();
                if (r.x < 900) continue;
                const opts = Array.from(sel.options).map(o => o.text.trim());
                const countsIdx = opts.indexOf('Counts');
                return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2), opts, countsIdx};
            }
            return null;
        }
    """)

    if select_result and select_result['countsIdx'] >= 0:
        print(f"  Task panel options: {select_result['opts']}, Counts at index {select_result['countsIdx']}")
        cx, cy = select_result['cx'], select_result['cy']
        page.mouse.click(cx, cy)
        page.wait_for_timeout(300)
        page.keyboard.press("Home")
        page.wait_for_timeout(100)
        for _ in range(select_result['countsIdx']):
            page.keyboard.press("ArrowDown")
            page.wait_for_timeout(80)
        page.keyboard.press("Enter")
        page.wait_for_timeout(2000)
        ss(page, "04_counts_tab")
    else:
        print(f"  Select result: {select_result}")

    # Click Manage Cycle Counts
    print("Looking for 'Manage Cycle Counts' task...")
    task_clicked = page.evaluate("""
        () => {
            for (const el of document.querySelectorAll('a,li,span')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim();
                if (t !== 'Manage Cycle Counts') continue;
                const r = el.getBoundingClientRect();
                if (r.x < 900) continue;
                el.click();
                return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
            }
            return null;
        }
    """)

    if task_clicked:
        print(f"  Clicked Manage Cycle Counts at {task_clicked}")
        page.wait_for_timeout(5000)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=20_000)
        except Exception:
            pass
        page.wait_for_timeout(2000)
        ss(page, "05_manage_cycle_counts")
        print(f"  Page title: {page.title()[:70]}")
        return True
    else:
        print("  Could not find Manage Cycle Counts task link")
        ss(page, "05_no_manage_cc")
        return False


def explore_cycle_counts(page):
    """Explore what cycle counts exist, especially for Burlington org."""
    print("\n=== Exploring Cycle Counts page ===")
    print(f"Current URL: {page.url[:100]}")
    print(f"Current title: {page.title()[:70]}")

    # Dump visible page content
    content = page.evaluate("""
        () => {
            const results = [];
            for (const el of document.querySelectorAll('input,select,button,a,th,td,span,div')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (!t || t.length < 3 || t.length > 100) continue;
                if (el.childElementCount > 2) continue;
                const r = el.getBoundingClientRect();
                if (r.width < 10 || r.height < 5) continue;
                const tag = el.tagName;
                const val = el.value || '';
                results.push({tag, text: t.slice(0,80), x: Math.round(r.x), y: Math.round(r.y), val: val.slice(0,30)});
            }
            return results.slice(0, 100);
        }
    """)

    print("Visible elements:")
    for el in content[:50]:
        if el['y'] > 50 and el['y'] < 800:
            print(f"  [{el['tag']}] ({el['x']},{el['y']}) {el['text'][:60]}" +
                  (f" = {el['val']}" if el['val'] else ""))

    ss(page, "06_cycle_counts_content")

    # Look for organization selector/filter
    print("\nLooking for org selector...")
    org_inputs = page.evaluate("""
        () => {
            const results = [];
            for (const el of document.querySelectorAll('input')) {
                if (!el.offsetParent) continue;
                const t = (el.getAttribute('title') || el.getAttribute('placeholder') ||
                           el.getAttribute('aria-label') || '').toLowerCase();
                if (t.includes('org') || t.includes('organization') || t.includes('inventory')) {
                    const r = el.getBoundingClientRect();
                    results.push({cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2),
                                  title: el.getAttribute('title'), placeholder: el.getAttribute('placeholder')});
                }
            }
            return results;
        }
    """)
    print(f"  Org input fields: {org_inputs}")


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=60)
        ctx = browser.new_context(viewport={"width": 1600, "height": 900})
        page = ctx.new_page()

        load_session(ctx)
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(2000)
        print(f"Session: {page.title()[:60]}")

        ok = navigate_to_inventory_mgmt_counts(page)
        if ok:
            explore_cycle_counts(page)

        print("\nDone. Screenshots saved to:", SS_DIR)
        page.wait_for_timeout(5000)
        browser.close()


if __name__ == "__main__":
    main()
