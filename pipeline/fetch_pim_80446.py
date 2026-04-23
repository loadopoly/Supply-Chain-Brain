"""
fetch_pim_80446.py - Fetch all PIM data for part number 80446-04 from Oracle Fusion DEV13
"""
import json, sys, time
from pathlib import Path
from playwright.sync_api import sync_playwright

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HOST         = "https://fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com"
SESSION_FILE = Path(__file__).parent / "oracle_session.json"
SS_DIR       = Path(__file__).parent / "pim_screenshots" / "80446-04"
SS_DIR.mkdir(parents=True, exist_ok=True)
PART_NUMBER  = "80446-04"

def ss(page, name):
    try:
        path = SS_DIR / f"{name}.png"
        page.screenshot(path=str(path))
        print(f"  [ss] {name}.png")
    except Exception as e:
        print(f"  [ss error] {name}: {e}")

def load_session(context):
    with open(SESSION_FILE) as f:
        cookies = json.load(f)
    pw = []
    for c in cookies:
        ck = {"name": c["name"], "value": c["value"],
              "domain": c.get("domain","").lstrip("."),
              "path": c.get("path","/"),
              "secure": c.get("secure", True),
              "httpOnly": c.get("httpOnly", False)}
        if c.get("expires") and c["expires"] > 0:
            ck["expires"] = int(c["expires"])
        pw.append(ck)
    context.add_cookies(pw)

def mouse_click_text(page, text, exact=True, tag_filter="a,button,span,div,li"):
    """Find element by text and mouse.click it. Returns coords or None."""
    coords = page.evaluate(f"""
        ([txt, exact, tags]) => {{
            for (const el of document.querySelectorAll(tags)) {{
                if (!el.offsetParent) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (exact ? t === txt : t.includes(txt)) {{
                    const r = el.getBoundingClientRect();
                    if (r.width < 2 || r.height < 2) continue;
                    el.scrollIntoView({{block:'center', behavior:'instant'}});
                    const r2 = el.getBoundingClientRect();
                    return {{cx: Math.round(r2.x+r2.width/2), cy: Math.round(r2.y+r2.height/2)}};
                }}
            }}
            return null;
        }}
    """, [text, exact, tag_filter])
    if coords:
        page.mouse.click(coords['cx'], coords['cy'])
    return coords

def navigate_to_pim(page):
    """Navigate to Product Management > Product Information Management > Manage Items."""
    print("1. Go to FuseWelcome...")
    page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)
    print(f"   Title: {page.title()[:60]}")
    ss(page, "01_welcome")

    # Click Product Management tab
    print("2. Click Product Management tab...")
    clicked = False
    for attempt in ["a:has-text('Product Management')",
                    "[role='tab']:has-text('Product Management')",
                    "span:has-text('Product Management')"]:
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
        res = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('a,div,span,button')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim().replace(/\\s+/g,' ');
                    if (t !== 'Product Management') continue;
                    const r = el.getBoundingClientRect();
                    if (r.y < 40 || r.y > 120) continue;
                    el.scrollIntoView({block:'center', behavior:'instant'});
                    const r2 = el.getBoundingClientRect();
                    return {cx: Math.round(r2.x+r2.width/2), cy: Math.round(r2.y+r2.height/2)};
                }
                return false;
            }
        """)
        if res:
            page.mouse.click(res['cx'], res['cy'])
            page.wait_for_timeout(2500)
            clicked = True

    ss(page, "02_pm_tab")
    if not clicked:
        print("   ERROR: Could not click Product Management tab")
        return False

    # Show More
    print("3. Show More...")
    for _ in range(3):
        found = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('a,button,span')) {
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
    ss(page, "03_show_more")

    # Click Product Information Management tile
    print("4. Click Product Information Management tile...")
    tiles_to_try = [
        "Product Information Management",
        "Items",
        "Product Hub",
        "Product Master",
    ]
    tile_clicked = False
    for tile in tiles_to_try:
        res = page.evaluate("""
            ([txt]) => {
                for (const el of document.querySelectorAll('a,button,div')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim().replace(/\\s+/g,' ');
                    if (t !== txt) continue;
                    const r = el.getBoundingClientRect();
                    if (r.y < 150 || r.height < 10) continue;
                    const style = window.getComputedStyle(el);
                    if (style.visibility === 'hidden' || style.display === 'none') continue;
                    el.scrollIntoView({block:'center', behavior:'instant'});
                    const r2 = el.getBoundingClientRect();
                    return {cx: Math.round(r2.x+r2.width/2), cy: Math.round(r2.y+r2.height/2), text: t};
                }
                return null;
            }
        """, [tile])
        if res:
            print(f"   Found tile: '{res['text']}' at ({res['cx']},{res['cy']})")
            page.mouse.click(res['cx'], res['cy'])
            page.wait_for_timeout(5000)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=15000)
            except Exception:
                pass
            page.wait_for_timeout(2000)
            ss(page, "04_pim_tile")
            tile_clicked = True
            break

    if not tile_clicked:
        # Dump all visible tiles for diagnosis
        tiles = page.evaluate("""
            () => {
                const res = [];
                for (const el of document.querySelectorAll('a')) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    if (r.y < 150 || r.height < 10) continue;
                    const t = el.textContent.trim().replace(/\\s+/g,' ');
                    if (t.length > 2 && t.length < 80) res.push(t);
                }
                return [...new Set(res)].slice(0, 30);
            }
        """)
        print(f"   Visible tiles: {tiles}")
        ss(page, "04_no_pim_tile")
        return False

    # Task panel - look for Manage Items task
    print("5. Open task panel and find Manage Items...")
    page.wait_for_timeout(1000)

    # Dump task panel items
    task_items = page.evaluate("""
        () => {
            const res = [];
            for (const el of document.querySelectorAll('a,li,span')) {
                if (!el.offsetParent) continue;
                const r = el.getBoundingClientRect();
                if (r.x < 900) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (t.length > 2 && t.length < 80) res.push({text: t, cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)});
            }
            return res;
        }
    """)
    print(f"   Task panel items: {[i['text'] for i in task_items[:15]]}")

    # Try common task names for PIM
    manage_tasks = ["Manage Items", "Items", "Manage Products",
                    "Search Items", "Product Hub", "Item Management"]
    for task_name in manage_tasks:
        res = page.evaluate("""
            ([taskName]) => {
                for (const el of document.querySelectorAll('a,li,span,button')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (t !== taskName) continue;
                    const r = el.getBoundingClientRect();
                    return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                }
                return null;
            }
        """, [task_name])
        if res:
            print(f"   Clicking task: '{task_name}'")
            page.mouse.click(res['cx'], res['cy'])
            page.wait_for_timeout(4000)
            ss(page, "05_manage_items")
            return True

    ss(page, "05_no_task_found")
    print("   Could not find a Manage Items task — trying direct URL...")
    # Try direct navigation to PIM items search
    page.goto(f"{HOST}/fscmUI/faces/AtkStartPageDeepLink?deepLinkTarget=PIMItemSearch",
              wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(3000)
    ss(page, "05b_direct_url")
    return True


def search_for_part(page, part_num):
    """Search for the part number and click the result."""
    print(f"\n6. Searching for part number: {part_num}...")
    ss(page, "06_before_search")
    print(f"   Page title: {page.title()[:70]}")

    # Dump visible inputs for diagnosis
    inputs = page.evaluate("""
        () => {
            const res = [];
            for (const el of document.querySelectorAll('input')) {
                if (!el.offsetParent) continue;
                const r = el.getBoundingClientRect();
                res.push({
                    placeholder: el.getAttribute('placeholder') || '',
                    title: el.getAttribute('title') || '',
                    type: el.getAttribute('type') || 'text',
                    cx: Math.round(r.x+r.width/2),
                    cy: Math.round(r.y+r.height/2)
                });
            }
            return res;
        }
    """)
    print(f"   Visible inputs: {inputs[:8]}")

    # Try to find Item/Part Number search field
    search_coords = page.evaluate("""
        ([partNum]) => {
            const hints = ['item', 'part', 'number', 'search', 'keyword', 'name'];
            for (const el of document.querySelectorAll('input')) {
                if (!el.offsetParent) continue;
                const ph = (el.getAttribute('placeholder') || '').toLowerCase();
                const ti = (el.getAttribute('title') || '').toLowerCase();
                const id = (el.getAttribute('id') || '').toLowerCase();
                const label = ph + ti + id;
                if (hints.some(h => label.includes(h))) {
                    const r = el.getBoundingClientRect();
                    return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2), hint: label.slice(0,40)};
                }
            }
            // Fallback: first visible input that's not hidden
            for (const el of document.querySelectorAll('input[type="text"],input:not([type])')) {
                if (!el.offsetParent) continue;
                const r = el.getBoundingClientRect();
                if (r.width < 50) continue;
                return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2), hint: 'first-text-input'};
            }
            return null;
        }
    """, [part_num])

    if search_coords:
        print(f"   Found search field ({search_coords['hint']}) at ({search_coords['cx']},{search_coords['cy']})")
        page.mouse.click(search_coords['cx'], search_coords['cy'])
        page.wait_for_timeout(300)
        page.keyboard.press("Control+A")
        page.keyboard.type(part_num, delay=40)
        page.wait_for_timeout(500)

        # Try pressing Enter or clicking Search button
        page.keyboard.press("Enter")
        page.wait_for_timeout(3000)
        ss(page, "07_after_search")
    else:
        print("   No search field found - taking screenshot for diagnosis")
        ss(page, "07_no_search_field")

    # Look for search/query button
    for btn_text in ["Search", "Go", "Find", "Query"]:
        res = page.evaluate(f"""
            () => {{
                for (const btn of document.querySelectorAll('button,input[type="submit"],a')) {{
                    if (!btn.offsetParent) continue;
                    const t = btn.textContent.trim() || btn.getAttribute('value') || '';
                    if (t === '{btn_text}') {{
                        const r = btn.getBoundingClientRect();
                        return {{cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)}};
                    }}
                }}
                return null;
            }}
        """)
        if res:
            print(f"   Clicking '{btn_text}' button")
            page.mouse.click(res['cx'], res['cy'])
            page.wait_for_timeout(3000)
            ss(page, "07b_search_clicked")
            break

    # Click first result matching the part number
    print(f"\n7. Looking for result: {part_num}...")
    for attempt in range(3):
        result = page.evaluate("""
            ([pn]) => {
                for (const el of document.querySelectorAll('a,td,span,div')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (!t.includes(pn)) continue;
                    const r = el.getBoundingClientRect();
                    if (r.width < 30 || r.height < 10) continue;
                    return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2), text: t.slice(0,80)};
                }
                return null;
            }
        """, [part_num])

        if result:
            print(f"   Clicking result: '{result['text']}' at ({result['cx']},{result['cy']})")
            page.mouse.click(result['cx'], result['cy'])
            page.wait_for_timeout(4000)
            ss(page, "08_item_detail")
            return True
        else:
            page.wait_for_timeout(1500)

    ss(page, "08_no_result")
    print(f"   Part number '{part_num}' not found in results")
    return False


def extract_pim_data(page, part_num):
    """Extract all PIM fields from the item detail page."""
    print("\n8. Extracting PIM data from item detail page...")
    ss(page, "09_extracting_pim")
    print(f"   Page title: {page.title()[:80]}")
    print(f"   URL: {page.url[:100]}")

    # Extract all label-value pairs visible on the page
    data = page.evaluate("""
        () => {
            const fields = {};

            // Pattern 1: label/value pairs in definition lists
            for (const dl of document.querySelectorAll('dl')) {
                const dts = dl.querySelectorAll('dt');
                const dds = dl.querySelectorAll('dd');
                for (let i = 0; i < dts.length; i++) {
                    const label = dts[i]?.textContent.trim().replace(/\\s+/g,' ');
                    const value = dds[i]?.textContent.trim().replace(/\\s+/g,' ');
                    if (label && label.length < 60) fields[label] = value || '';
                }
            }

            // Pattern 2: Oracle ADF label/field pairs (span[class*="Label"] + span[class*="Content"])
            for (const el of document.querySelectorAll('[class*="Label"],[class*="label"]')) {
                if (!el.offsetParent) continue;
                const label = el.textContent.trim().replace(/\\s+/g,' ');
                if (!label || label.length > 80) continue;
                const parent = el.parentElement;
                if (!parent) continue;
                // Find sibling or adjacent value field
                const sibling = el.nextElementSibling;
                if (sibling) {
                    const val = sibling.textContent.trim().replace(/\\s+/g,' ');
                    if (val && val.length < 200) fields[label] = val;
                }
            }

            // Pattern 3: tr > td pairs (table-based forms)
            for (const row of document.querySelectorAll('tr')) {
                const cells = row.querySelectorAll('td');
                if (cells.length >= 2) {
                    const label = cells[0]?.textContent.trim().replace(/\\s+/g,' ');
                    const value = cells[1]?.textContent.trim().replace(/\\s+/g,' ');
                    if (label && label.length < 80 && value && !label.startsWith('(')) {
                        fields[label] = value;
                    }
                }
            }

            // Pattern 4: visible inputs with labels
            for (const input of document.querySelectorAll('input,select,textarea')) {
                if (!input.offsetParent) continue;
                const id = input.getAttribute('id');
                let label = '';
                if (id) {
                    const lbl = document.querySelector(`label[for="${id}"]`);
                    if (lbl) label = lbl.textContent.trim();
                }
                if (!label) {
                    // Look for preceding label in same container
                    const parent = input.parentElement;
                    if (parent) {
                        const lbl = parent.querySelector('label,span[class*="Label"]');
                        if (lbl) label = lbl.textContent.trim();
                    }
                }
                const value = input.value || input.textContent?.trim() || '';
                if (label && label.length < 80 && value) {
                    fields[label.replace(/\\s+/g,' ')] = value;
                }
            }

            // Also grab h1/h2/h3 for section context
            const headings = [];
            for (const h of document.querySelectorAll('h1,h2,h3,h4')) {
                if (!h.offsetParent) continue;
                const t = h.textContent.trim();
                if (t.length > 0 && t.length < 100) headings.push({tag: h.tagName, text: t});
            }

            return {fields, headings};
        }
    """)

    pim = data.get('fields', {})
    headings = data.get('headings', [])

    print(f"\n   Page headings: {headings}")
    print(f"\n   PIM Fields found ({len(pim)}):")
    for label, value in pim.items():
        print(f"     {label}: {value}")

    return pim, headings


def navigate_pim_sections(page):
    """Navigate through item tabs/sections to collect all PIM data."""
    print("\n9. Navigating through item sections...")
    all_data = {}

    # Find all tabs on the page
    tabs = page.evaluate("""
        () => {
            const tabs = [];
            for (const el of document.querySelectorAll('[role="tab"],a.tab,li.tab,button.tab')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (t.length > 0 && t.length < 60) {
                    const r = el.getBoundingClientRect();
                    tabs.push({text: t, cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)});
                }
            }
            return tabs;
        }
    """)
    print(f"   Available tabs: {[t['text'] for t in tabs]}")

    # Visit each tab
    pim_tabs = ["General", "Specifications", "Units of Measure", "Categories",
                "Associations", "Attachments", "Manufacturers",
                "Item Relationships", "Trade Items", "Structures",
                "Packing", "Safety", "Compliance"]

    for tab in tabs:
        if any(p_tab.lower() in tab['text'].lower() for p_tab in pim_tabs):
            print(f"   Clicking tab: '{tab['text']}'...")
            page.mouse.click(tab['cx'], tab['cy'])
            page.wait_for_timeout(2500)
            ss(page, f"tab_{tab['text'].replace(' ','_')}")

            # Extract from this tab
            tab_data = page.evaluate("""
                () => {
                    const fields = {};
                    for (const row of document.querySelectorAll('tr')) {
                        const cells = row.querySelectorAll('td');
                        if (cells.length >= 2) {
                            const label = cells[0]?.textContent.trim().replace(/\\s+/g,' ');
                            const value = cells[1]?.textContent.trim().replace(/\\s+/g,' ');
                            if (label && label.length < 80 && value) fields[label] = value;
                        }
                    }
                    for (const input of document.querySelectorAll('input,select')) {
                        if (!input.offsetParent) continue;
                        const id = input.getAttribute('id');
                        let label = '';
                        if (id) {
                            const lbl = document.querySelector(`label[for="${id}"]`);
                            if (lbl) label = lbl.textContent.trim();
                        }
                        const value = input.value || '';
                        if (label && value) fields[label] = value;
                    }
                    return fields;
                }
            """)

            all_data[tab['text']] = tab_data
            for k, v in tab_data.items():
                print(f"     [{tab['text']}] {k}: {v}")

    return all_data


def write_report(part_num, pim_data, headings, tab_data):
    """Write a PIM report file."""
    report_path = SS_DIR / "pim_report.json"
    full_data = {
        "part_number": part_num,
        "headings": headings,
        "primary_fields": pim_data,
        "tab_sections": tab_data,
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(full_data, f, indent=2, ensure_ascii=False)
    print(f"\n   [report] pim_report.json written")
    return report_path


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()

        try:
            print(f"Loading session cookies from {SESSION_FILE}...")
            load_session(context)

            ok = navigate_to_pim(page)
            if not ok:
                print("Navigation to PIM failed — check screenshots in pim_screenshots/80446-04/")
                return

            found = search_for_part(page, PART_NUMBER)
            pim_data, headings = extract_pim_data(page, PART_NUMBER)
            tab_data = navigate_pim_sections(page)

            write_report(PART_NUMBER, pim_data, headings, tab_data)

            print("\n=== FINAL PIM SUMMARY ===")
            print(f"Part Number: {PART_NUMBER}")
            print(f"Part found: {found}")
            print(f"Primary fields captured: {len(pim_data)}")
            print(f"Section tabs captured: {len(tab_data)}")
            print(f"Screenshots saved in: {SS_DIR}")

        except Exception as e:
            print(f"\nERROR: {e}")
            ss(page, "error")
            raise
        finally:
            browser.close()

if __name__ == "__main__":
    main()
