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


def navigate_to_inventory_mgmt(page):
    """Navigate to Inventory Management (Classic) tile."""
    print("\nNavigating to Oracle Fusion home...")
    page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(2000)
    print(f"Home: {page.title()[:60]}")

    # Click Supply Chain Execution tab
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

    # Navigate to Inventory Management (Classic) tile
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
        print(f"Module: {page.title()[:70]}")
        ss(page, "02_inv_mgmt")
        return True
    else:
        print("ERROR: Could not find Inventory Management (Classic) tile")
        return False


def get_task_panel_select(page):
    """Return info about the right-side task panel SELECT, or None if not visible."""
    return page.evaluate("""
        () => {
            for (const sel of document.querySelectorAll('select')) {
                if (!sel.offsetParent) continue;
                const r = sel.getBoundingClientRect();
                if (r.x < 900) continue;
                const opts = Array.from(sel.options).map(o => o.text.trim());
                return {
                    cx: Math.round(r.x + r.width/2),
                    cy: Math.round(r.y + r.height/2),
                    opts,
                    currentIdx: sel.selectedIndex,
                };
            }
            return null;
        }
    """)


def ensure_task_panel_open(page):
    """
    Ensure the Redwood task panel is open. The panel is already open after
    tile navigation — only click the toggle if it's NOT open to avoid closing it.
    """
    info = get_task_panel_select(page)
    if info:
        print(f"  Task panel already open. Options: {info['opts']}")
        return True

    print("  Task panel not open — looking for toggle icon...")
    ss(page, "tp_before_open")

    # Dump all buttons/links in the right-edge icon area to find the toggle
    icons = page.evaluate("""
        () => {
            const results = [];
            for (const el of document.querySelectorAll('button,a,[role="button"],span')) {
                if (!el.offsetParent) continue;
                const r = el.getBoundingClientRect();
                if (r.x < 1000 || r.width > 80) continue;
                if (r.y < 150 || r.y > 600) continue;
                results.push({
                    tag: el.tagName,
                    title: el.getAttribute('title') || '',
                    aria: el.getAttribute('aria-label') || '',
                    cx: Math.round(r.x + r.width/2),
                    cy: Math.round(r.y + r.height/2),
                });
            }
            return results;
        }
    """)
    print(f"  Right-side icons: {icons}")

    # Try attribute-based selectors first
    for attr_val in ["Tasks", "Show Tasks", "task", "list"]:
        for selector in [
            f"[title='{attr_val}']", f"[aria-label='{attr_val}']",
            f"button[title*='{attr_val}']", f"[aria-label*='{attr_val}']",
        ]:
            try:
                loc = page.locator(selector).first
                if loc.is_visible(timeout=1500):
                    loc.click()
                    page.wait_for_timeout(1500)
                    info = get_task_panel_select(page)
                    if info:
                        print(f"  Panel opened via selector: {selector}")
                        return True
            except Exception:
                pass

    # Positional fallback: click the first right-edge icon (list/tasks icon)
    for icon in icons:
        page.mouse.click(icon['cx'], icon['cy'])
        page.wait_for_timeout(1500)
        info = get_task_panel_select(page)
        if info:
            print(f"  Panel opened via positional click at ({icon['cx']},{icon['cy']})")
            return True
        # clicked but didn't open panel — try next icon

    print("  ERROR: Could not open task panel")
    ss(page, "tp_failed_open")
    return False


def select_counts_tab(page):
    """Change the task panel SELECT to 'Counts'. Returns True on success."""
    info = get_task_panel_select(page)
    if not info:
        print("  Task panel SELECT not found")
        return False

    print(f"  Task panel SELECT options: {info['opts']}, current: {info['opts'][info['currentIdx']] if info['opts'] else '?'}")

    counts_idx = None
    for i, opt in enumerate(info['opts']):
        if opt.lower() == 'counts':
            counts_idx = i
            break

    if counts_idx is None:
        print(f"  'Counts' option not found in: {info['opts']}")
        return False

    if info['currentIdx'] == counts_idx:
        print("  Already on Counts tab")
        return True

    # Click the SELECT then navigate to Counts
    cx, cy = info['cx'], info['cy']
    page.mouse.click(cx, cy)
    page.wait_for_timeout(300)
    page.keyboard.press("Home")
    page.wait_for_timeout(100)
    for _ in range(counts_idx):
        page.keyboard.press("ArrowDown")
        page.wait_for_timeout(80)
    page.keyboard.press("Enter")
    page.wait_for_timeout(2000)
    ss(page, "04_counts_tab")

    # Verify
    info2 = get_task_panel_select(page)
    if info2:
        current = info2['opts'][info2['currentIdx']] if info2['opts'] else '?'
        print(f"  After selection: {current}")
        return current.lower() == 'counts'

    return False


def click_manage_cycle_counts(page):
    """Click 'Manage Cycle Counts' in the task panel. Returns True on success."""
    # Get coordinates only — do NOT use el.click() in JS as Oracle ADF ignores it.
    # We must use page.mouse.click() so the full browser event chain fires.
    result = page.evaluate("""
        () => {
            for (const el of document.querySelectorAll('a,li,span,button')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim();
                if (t !== 'Manage Cycle Counts') continue;
                const r = el.getBoundingClientRect();
                return {cx: Math.round(r.x + r.width/2), cy: Math.round(r.y + r.height/2)};
            }
            return null;
        }
    """)

    if not result:
        print("  ERROR: 'Manage Cycle Counts' task link not found")
        visible_links = page.evaluate("""
            () => {
                const items = [];
                for (const el of document.querySelectorAll('a,li')) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    if (r.x < 900) continue;
                    const t = el.textContent.trim().replace(/\\s+/g,' ');
                    if (t.length > 3 && t.length < 80) items.push(t);
                }
                return items;
            }
        """)
        print(f"  Visible right-panel items: {visible_links}")
        ss(page, "05_no_manage_cc")
        return False

    print(f"  Found Manage Cycle Counts at {result} — using mouse.click()")
    page.mouse.click(result['cx'], result['cy'])
    page.wait_for_timeout(3000)

    # Oracle ADF may not change the URL. Wait for page content to change:
    # the Manage Cycle Counts page has an Organization search field.
    for _ in range(10):
        found = page.evaluate("""
            () => {
                // Look for Manage Cycle Counts page indicators:
                // a search/filter area with Organization or Cycle Count Name fields,
                // or a page title like "Manage Cycle Counts"
                for (const el of document.querySelectorAll('h1,h2,title')) {
                    if (el.textContent.includes('Manage Cycle Counts')) return 'h_title';
                }
                for (const el of document.querySelectorAll('input')) {
                    if (!el.offsetParent) continue;
                    const t = (el.getAttribute('title') || el.getAttribute('placeholder') || '').toLowerCase();
                    if (t.includes('cycle count')) return 'input_' + t;
                }
                return null;
            }
        """)
        if found:
            print(f"  Page changed: {found}")
            break
        page.wait_for_timeout(1000)

    try:
        page.wait_for_load_state("domcontentloaded", timeout=15_000)
    except Exception:
        pass
    page.wait_for_timeout(1000)
    ss(page, "05_manage_cycle_counts")
    print(f"  URL: {page.url[:100]}")
    print(f"  Title: {page.title()[:70]}")
    return True


def handle_org_dialog(page, org_code="3165_US_BUR_MFG"):
    """If the Select Organization dialog is visible, fill in org and click OK."""
    # Check for dialog
    dialog = page.evaluate("""
        () => {
            for (const el of document.querySelectorAll('input')) {
                if (!el.offsetParent) continue;
                const r = el.getBoundingClientRect();
                // Dialog input is centered on screen
                if (r.x < 500 || r.x > 900) continue;
                if (r.y < 350 || r.y > 500) continue;
                return {cx: Math.round(r.x + r.width/2), cy: Math.round(r.y + r.height/2)};
            }
            return null;
        }
    """)

    if not dialog:
        print("  No org dialog detected")
        return False

    print(f"  Org dialog found at {dialog} — entering '{org_code}'")
    # Click the input field and type
    page.mouse.click(dialog['cx'], dialog['cy'])
    page.wait_for_timeout(300)
    page.keyboard.type(org_code, delay=50)
    page.wait_for_timeout(1500)

    # Take screenshot to see autocomplete
    ss(page, "07_org_typing")

    # Check for autocomplete dropdown suggestions
    suggestion = page.evaluate("""
        ([org]) => {
            // Oracle LOV autocomplete shows suggestions in a popup list
            for (const el of document.querySelectorAll('li,td,tr,div')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (t.includes(org) || t.toLowerCase().includes('burlington') || t.includes('BUR')) {
                    const r = el.getBoundingClientRect();
                    if (r.width < 50 || r.height < 10) continue;
                    if (r.y < 400 || r.y > 700) continue;
                    return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2), text: t.slice(0,60)};
                }
            }
            return null;
        }
    """, [org_code])

    if suggestion:
        print(f"  Autocomplete suggestion: {suggestion}")
        page.mouse.click(suggestion['cx'], suggestion['cy'])
        page.wait_for_timeout(500)
    else:
        # No suggestion visible — press Tab to trigger LOV search or accept
        print("  No suggestion visible — pressing Tab to trigger autocomplete")
        page.keyboard.press("Tab")
        page.wait_for_timeout(1500)
        ss(page, "07b_org_after_tab")

        # Check for suggestions again
        suggestion = page.evaluate("""
            ([org]) => {
                for (const el of document.querySelectorAll('li,td,tr,div')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim().replace(/\\s+/g,' ');
                    if (t.includes(org) || t.includes('BUR_MFG')) {
                        const r = el.getBoundingClientRect();
                        if (r.width < 50) continue;
                        return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2), text: t.slice(0,60)};
                    }
                }
                return null;
            }
        """, [org_code])
        if suggestion:
            print(f"  Suggestion after Tab: {suggestion}")
            page.mouse.click(suggestion['cx'], suggestion['cy'])
            page.wait_for_timeout(500)

    ss(page, "07c_org_filled")

    # Click OK button
    ok_clicked = page.evaluate("""
        () => {
            for (const el of document.querySelectorAll('button')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim();
                if (t === 'OK' || t === 'Ok') {
                    const r = el.getBoundingClientRect();
                    return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                }
            }
            return null;
        }
    """)

    if ok_clicked:
        print(f"  Clicking OK at {ok_clicked}")
        page.mouse.click(ok_clicked['cx'], ok_clicked['cy'])
        page.wait_for_timeout(4000)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=15_000)
        except Exception:
            pass
        page.wait_for_timeout(2000)
        ss(page, "08_after_org_select")
        print(f"  After OK — title: {page.title()[:70]}")
        return True

    print("  ERROR: Could not find OK button")
    return False


def open_cycle_count(page, count_name):
    """Click on a cycle count link to open its detail. Returns True on success."""
    result = page.evaluate("""
        ([name]) => {
            for (const el of document.querySelectorAll('a')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim();
                if (t !== name) continue;
                const r = el.getBoundingClientRect();
                return {cx: Math.round(r.x + r.width/2), cy: Math.round(r.y + r.height/2)};
            }
            return null;
        }
    """, [count_name])

    if not result:
        print(f"  Cycle count link '{count_name}' not found")
        return False

    print(f"  Opening '{count_name}' at {result}")
    page.mouse.click(result['cx'], result['cy'])
    page.wait_for_timeout(5000)
    try:
        page.wait_for_load_state("domcontentloaded", timeout=15_000)
    except Exception:
        pass
    page.wait_for_timeout(2000)
    ss(page, f"cc_{count_name.replace(' ', '_')[:20]}")
    print(f"  Title: {page.title()[:70]}")
    return True


def explore_cycle_count_detail(page, count_name):
    """Explore the detail page of an opened cycle count."""
    print(f"\n=== Exploring cycle count: {count_name} ===")
    print(f"URL: {page.url[:100]}")
    print(f"Title: {page.title()[:70]}")

    content = page.evaluate("""
        () => {
            const results = [];
            for (const el of document.querySelectorAll('input,select,button,a,label,th,td,h1,h2,h3,span,li')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (!t || t.length < 2 || t.length > 120) continue;
                if (el.childElementCount > 3) continue;
                const r = el.getBoundingClientRect();
                if (r.width < 5 || r.height < 5) continue;
                if (r.y < 50 || r.y > 850) continue;
                const tag = el.tagName;
                const val = el.value || '';
                const title = el.getAttribute('title') || '';
                results.push({
                    tag, y: Math.round(r.y), x: Math.round(r.x),
                    text: t.slice(0,80), val: val.slice(0,40), title: title.slice(0,40)
                });
            }
            return results;
        }
    """)

    print("Visible elements:")
    for el in content:
        if el['y'] > 50 and el['y'] < 850:
            line = f"  [{el['tag']}] ({el['x']},{el['y']}) {el['text'][:60]}"
            if el['val']:
                line += f" = {el['val']}"
            if el['title']:
                line += f" [title={el['title']}]"
            print(line)


def explore_cycle_counts_page(page):
    """Explore the Manage Cycle Counts search results page."""
    print("\n=== Exploring Manage Cycle Counts page ===")
    print(f"URL: {page.url[:100]}")
    print(f"Title: {page.title()[:70]}")

    content = page.evaluate("""
        () => {
            const results = [];
            for (const el of document.querySelectorAll('input,select,button,a,label,th,td,h1,h2,h3,span')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (!t || t.length < 3 || t.length > 120) continue;
                if (el.childElementCount > 2) continue;
                const r = el.getBoundingClientRect();
                if (r.width < 10 || r.height < 5) continue;
                if (r.y < 50 || r.y > 850) continue;
                const tag = el.tagName;
                const val = el.value || '';
                const title = el.getAttribute('title') || '';
                const ph = el.getAttribute('placeholder') || '';
                results.push({
                    tag, y: Math.round(r.y), x: Math.round(r.x),
                    text: t.slice(0,80), val: val.slice(0,40),
                    title: title.slice(0,40), ph: ph.slice(0,40)
                });
            }
            return results;
        }
    """)

    # Just print cycle count names visible in the results table
    print("Cycle counts visible:")
    for el in content:
        if el['tag'] == 'A' and el['x'] < 200 and el['y'] > 300:
            print(f"  {el['text']}")

    ss(page, "06_cycle_counts_page")


def dump_all_inputs(page, label=""):
    """Debug helper: dump ALL visible inputs regardless of type or position."""
    result = page.evaluate("""
        () => {
            const out = {iframes: [], inputs: []};
            for (const f of document.querySelectorAll('iframe')) {
                const r = f.getBoundingClientRect();
                if (r.width > 0 && r.height > 0)
                    out.iframes.push({id: f.id||'', src: (f.src||'').slice(0,60), x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height)});
            }
            for (const inp of document.querySelectorAll('input,textarea')) {
                const t = (inp.type||'').toLowerCase();
                if (['hidden','submit','button','image','file','reset'].includes(t)) continue;
                const r = inp.getBoundingClientRect();
                out.inputs.push({
                    type: t || '(none)',
                    id: (inp.id||'').slice(0,35),
                    name: (inp.name||'').slice(0,35),
                    title: (inp.getAttribute('title')||'').slice(0,35),
                    cls: (inp.className||'').slice(0,50),
                    x: Math.round(r.x), y: Math.round(r.y),
                    w: Math.round(r.width), h: Math.round(r.height),
                    visible: r.width > 0 && r.height > 0,
                    hasOffsetParent: !!inp.offsetParent,
                });
            }
            return out;
        }
    """)
    tag = f"[{label}] " if label else ""
    print(f"  {tag}iframes: {result['iframes']}")
    print(f"  {tag}inputs ({len(result['inputs'])}):")
    for inp in result['inputs']:
        print(f"    ({inp['x']},{inp['y']}) {inp['w']}x{inp['h']} vis={inp['visible']} op={inp['hasOffsetParent']} type={inp['type']} id={inp['id'][:28]} title={inp['title'][:28]}")
    return result['inputs']


def find_item_search_fields(page):
    """
    Find the Item search input(s) in step 5.
    Uses broad detection (no offsetParent requirement, any type).
    Returns list of {cx,cy,section} dicts.
    """
    fields = page.evaluate("""
        () => {
            const fields = [];
            const skip_types = ['hidden','submit','button','image','file','reset','checkbox','radio'];
            for (const inp of document.querySelectorAll('input,textarea')) {
                const t = (inp.type||'').toLowerCase();
                if (skip_types.includes(t)) continue;
                const r = inp.getBoundingClientRect();
                // Must have visible size
                if (r.width < 10 || r.height < 10) continue;
                // Must be in content area (below wizard header)
                if (r.y < 300 || r.y > 900) continue;
                // Must be on visible x range
                if (r.x < 0 || r.x > 800) continue;

                // Find nearest category heading by walking up DOM
                let parent = inp.parentElement;
                let section = '';
                for (let i = 0; i < 25 && parent; i++) {
                    // Look for h2/h3 INSIDE this ancestor (sibling section header)
                    const h = parent.querySelector('h2,h3,h4');
                    if (h) {
                        const hr = h.getBoundingClientRect();
                        if (hr.width > 0) { section = h.textContent.trim().slice(0,20); break; }
                    }
                    parent = parent.parentElement;
                }

                fields.push({
                    cx: Math.round(r.x + r.width/2),
                    cy: Math.round(r.y + r.height/2),
                    x: Math.round(r.x), y: Math.round(r.y),
                    section,
                    id: (inp.id||'').slice(0,30),
                    type: t,
                });
            }
            return fields;
        }
    """)
    return fields


def search_items_in_step5(page, cc_name, target_items):
    """
    Navigate to step 5 and find target items via:
    1. Item search input field (fixed detection)
    2. Scrolling through visible item rows as fallback
    Reports which category/class each item is found in.
    """
    print(f"\n=== Item search in {cc_name} step 5 ===")

    # Navigate to step 5 (click Next 4 times)
    for step_i in range(4):
        btn = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('button')) {
                    if (!el.offsetParent) continue;
                    if (el.textContent.trim() === 'Next') {
                        const r = el.getBoundingClientRect();
                        return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                    }
                }
                return null;
            }
        """)
        if not btn:
            print(f"  No Next button at step {step_i+1}")
            break
        page.mouse.click(btn['cx'], btn['cy'])
        page.wait_for_timeout(3000)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=10_000)
        except Exception:
            pass
        page.wait_for_timeout(1000)

    heading = page.evaluate("""
        () => { for (const h of document.querySelectorAll('h1')) if (h.textContent.trim()) return h.textContent.trim(); return ''; }
    """)
    print(f"  At: {heading}")
    ss(page, f"srch_{cc_name[:15].replace(' ','_')}")

    # DEBUG: dump all inputs to understand Oracle ADF DOM
    dump_all_inputs(page, "step5")

    # Find item search fields with broad detection
    search_fields = find_item_search_fields(page)
    print(f"  Item search fields found: {len(search_fields)}")
    for f in search_fields:
        print(f"    ({f['x']},{f['y']}) section='{f['section']}' id={f['id']}")

    # Strategy 1: Scroll through all category sections (A, B, C, ...) and search each
    # For each visible category, search using the Item column filter
    print("\n  Strategy 1: Scrolling through category sections...")
    page.evaluate("() => window.scrollTo(0, 300)")  # Position for visibility
    page.wait_for_timeout(500)

    for scroll_attempt in range(15):  # Try scrolling through multiple sections
        # Find all item column filters (x > 200) visible at this scroll position
        current_filters = page.evaluate("""
            () => {
                const filters = [];
                for (const inp of document.querySelectorAll('input')) {
                    const t = (inp.type || '').toLowerCase();
                    if (['hidden','submit','button'].includes(t)) continue;
                    const r = inp.getBoundingClientRect();
                    if (r.width < 10 || r.height < 10) continue;
                    if (r.x < 200 || r.x > 350 || r.y < 400 || r.y > 500) continue;
                    let sec = '';
                    let parent = inp.parentElement;
                    for (let i = 0; i < 20 && parent; i++) {
                        const h = parent.querySelector('h2,h3,h4');
                        if (h && h.getBoundingClientRect().width > 0) {
                            sec = h.textContent.trim().slice(0,5); break;
                        }
                        parent = parent.parentElement;
                    }
                    filters.push({x: Math.round(r.x), y: Math.round(r.y), cx: Math.round(r.x + r.width/2), cy: Math.round(r.y + r.height/2), sec});
                }
                return filters.length > 0 ? filters[0] : null;
            }
        """)

        if not current_filters:
            if scroll_attempt > 3:
                break
            page.evaluate("() => window.scrollBy(0, 300)")
            page.wait_for_timeout(800)
            continue

        cat_section = current_filters.get('sec', '?')
        print(f"    Scroll {scroll_attempt}: Found category {cat_section} at y={current_filters['y']}")

        # Search for each item in this category's filter
        for item_num in target_items:
            if item_num in found_results:
                continue

            field = current_filters
            page.mouse.click(field['cx'], field['cy'])
            page.wait_for_timeout(200)
            page.keyboard.press("Control+a")
            page.keyboard.type(item_num, delay=30)
            page.wait_for_timeout(200)
            page.keyboard.press("Enter")
            page.wait_for_timeout(2500)

            # Check if item appears in table rows within this category
            item_found = page.evaluate("""
                ([item, cat]) => {
                    for (const el of document.querySelectorAll('a,span,td')) {
                        if (!el.offsetParent) continue;
                        if (el.textContent.trim() !== item) continue;
                        const r = el.getBoundingClientRect();
                        if (r.y < 440 || r.y > 700) continue;  // within table rows
                        let parent = el.parentElement;
                        for (let i = 0; i < 20 && parent; i++) {
                            const h = parent.querySelector('h2,h3,h4');
                            if (h && h.getBoundingClientRect().width > 0)
                                return {item, cls: h.textContent.trim().slice(0,10)};
                            parent = parent.parentElement;
                        }
                        return {item, cls: cat};
                    }
                    return null;
                }
            """, [item_num, cat_section])

            if item_found:
                print(f"      FOUND: {item_num} in {item_found['cls']}")
                found_results[item_num] = item_found['cls']

            # Clear filter
            page.mouse.click(field['cx'], field['cy'])
            page.keyboard.press("Control+a")
            page.keyboard.press("Delete")
            page.keyboard.press("Enter")
            page.wait_for_timeout(800)

        # Scroll to next category section
        page.evaluate("() => window.scrollBy(0, 300)")
        page.wait_for_timeout(800)

    # Strategy 2: Scan visible item links by scrolling (checks exact match)
    items_to_scan = [i for i in target_items if i not in found_results]
    if items_to_scan:
        print(f"\n  Scanning visible rows for: {items_to_scan}")
        page.evaluate("() => window.scrollTo(0, 0)")
        page.wait_for_timeout(500)
        prev_scroll = -1
        for _ in range(30):
            matches = page.evaluate("""
                (items_array) => {
                    const targets = items_array;
                    const result = [];
                    for (const el of document.querySelectorAll('a,span,td')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim();
                        if (!targets.includes(t)) continue;
                        const r = el.getBoundingClientRect();
                        if (r.y < 200 || r.y > 900) continue;
                        // Walk up for class heading
                        let parent = el.parentElement;
                        let cls = '';
                        for (let i = 0; i < 25 && parent; i++) {
                            const h = parent.querySelector('h2,h3,h4');
                            if (h && h.getBoundingClientRect().width > 0) {
                                cls = h.textContent.trim().slice(0,10); break;
                            }
                            parent = parent.parentElement;
                        }
                        result.push({item: t, cls, x: Math.round(r.x), y: Math.round(r.y)});
                    }
                    return result;
                }
            """, items_to_scan)
            for m in matches:
                if m['item'] not in found_results:
                    print(f"  FOUND (scroll): {m['item']} class='{m['cls']}' at ({m['x']},{m['y']})")
                    found_results[m['item']] = m['cls'] or 'found'
                    items_to_scan = [i for i in items_to_scan if i != m['item']]

            if not items_to_scan:
                break
            curr = page.evaluate("() => window.scrollY")
            if curr == prev_scroll:
                break
            prev_scroll = curr
            page.evaluate("() => window.scrollBy(0, 400)")
            page.wait_for_timeout(400)

        for item_num in items_to_scan:
            print(f"  NOT FOUND: {item_num}")

    print(f"\n  Results: {found_results}")
    ss(page, f"srch_results_{cc_name[:12].replace(' ','_')}")
    return found_results


def cancel_wizard(page):
    """Click Cancel/X to close the wizard and return to cycle counts list."""
    # Try Cancel button (text-based)
    result = page.evaluate("""
        () => {
            for (const el of document.querySelectorAll('button,a')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim();
                if (t === 'Cancel') {
                    const r = el.getBoundingClientRect();
                    return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                }
            }
            return null;
        }
    """)
    if result:
        print(f"  Cancelling wizard at {result}")
        page.mouse.click(result['cx'], result['cy'])
        page.wait_for_timeout(2000)

        # Handle confirmation dialog (Yes / OK) — must use mouse.click, not el.click()
        confirm = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('button')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (t === 'Yes' || t === 'OK' || t === 'Ok') {
                        const r = el.getBoundingClientRect();
                        return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2), label: t};
                    }
                }
                return null;
            }
        """)
        if confirm:
            print(f"  Confirming cancel ({confirm['label']}) at {confirm}")
            page.mouse.click(confirm['cx'], confirm['cy'])
            page.wait_for_timeout(3000)
    else:
        print("  No Cancel button found")

    # Wait for cycle counts list to be visible again
    for _ in range(10):
        found_list = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('a')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (t.includes('BUR') || t.includes('STEEL')) return true;
                }
                return false;
            }
        """)
        if found_list:
            break
        page.wait_for_timeout(1000)

    ss(page, "after_cancel")


def navigate_to_step5_and_search(page, cc_name, items_dict):
    """Navigate to step 5 of a cycle count wizard and search for target items.
    Returns dict of {item: found_class} for items found."""
    print(f"\n=== Step 5 search in: {cc_name} ===")

    # Click Next 4 times to reach step 5
    for step in range(1, 5):
        next_btn = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('button')) {
                    if (!el.offsetParent) continue;
                    if (el.textContent.trim() === 'Next') {
                        const r = el.getBoundingClientRect();
                        return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                    }
                }
                return null;
            }
        """)
        if not next_btn:
            print(f"  No Next button at step {step}")
            break
        page.mouse.click(next_btn['cx'], next_btn['cy'])
        page.wait_for_timeout(3000)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=10_000)
        except Exception:
            pass
        page.wait_for_timeout(1000)

    # We should now be on step 5
    heading = page.evaluate("""
        () => {
            for (const h of document.querySelectorAll('h1')) {
                const t = h.textContent.trim();
                if (t) return t;
            }
            return null;
        }
    """)
    print(f"  Current heading: {heading}")
    ss(page, f"step5_{cc_name.replace(' ', '_')[:15]}")

    # Dump the item categories (classes) visible
    categories = page.evaluate("""
        () => {
            const results = [];
            for (const el of document.querySelectorAll('td,span,th')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim();
                if (!t || t.length < 1 || t.length > 60) continue;
                const r = el.getBoundingClientRect();
                if (r.y < 230 || r.y > 550) continue;
                if (r.x > 600) continue;
                results.push({tag: el.tagName, text: t, x: Math.round(r.x), y: Math.round(r.y)});
            }
            return results;
        }
    """)
    print("  Visible categories/classes:")
    for c in categories[:30]:
        print(f"    [{c['tag']}] ({c['x']},{c['y']}) {c['text']}")

    # Search for each target item
    found = {}
    target_items = list(items_dict.keys())[:5]  # check first 5 to identify the right count

    for item_num in target_items:
        # Find the item search field and type the item number
        found_item = page.evaluate("""
            ([item]) => {
                // Look for all visible links with matching text
                for (const el of document.querySelectorAll('a,span')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (t === item) {
                        const r = el.getBoundingClientRect();
                        // Find parent section heading to get the class
                        let parent = el.parentElement;
                        for (let i = 0; i < 20 && parent; i++) {
                            const h = parent.querySelector('h2,h3');
                            if (h) return {item, cls: h.textContent.trim().slice(0,5)};
                            parent = parent.parentElement;
                        }
                        return {item, cls: 'found-no-class'};
                    }
                }
                return null;
            }
        """, [item_num])

        if found_item:
            print(f"  FOUND: {item_num} in class {found_item['cls']}")
            found[item_num] = found_item['cls']
        else:
            print(f"  Not visible: {item_num}")

    return found


def navigate_wizard_steps(page, target_step=6):
    """
    Click Next through the Oracle ADF wizard until we reach the target step number.
    Dumps page content at the target step.
    """
    current_step = 1
    print(f"\n  Navigating wizard from step {current_step} to step {target_step}...")

    while current_step < target_step:
        # Click Next button
        next_result = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('button')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (t === 'Next') {
                        const r = el.getBoundingClientRect();
                        return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                    }
                }
                return null;
            }
        """)

        if not next_result:
            print(f"  No Next button found at step {current_step}")
            break

        page.mouse.click(next_result['cx'], next_result['cy'])
        page.wait_for_timeout(3000)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=10_000)
        except Exception:
            pass
        page.wait_for_timeout(1000)
        current_step += 1

        # Check current wizard step
        step_info = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('h1')) {
                    const t = el.textContent.trim();
                    if (t) return t;
                }
                return null;
            }
        """)
        print(f"  Step {current_step}: {step_info}")
        ss(page, f"wizard_step{current_step}")

        # Check for error messages
        errors = page.evaluate("""
            () => {
                const msgs = [];
                for (const el of document.querySelectorAll('.af_messages,.AFErrorMsg,[class*="error"],[class*="Error"]')) {
                    if (el.offsetParent && el.textContent.trim()) {
                        msgs.push(el.textContent.trim().slice(0,100));
                    }
                }
                return msgs;
            }
        """)
        if errors:
            print(f"  Errors: {errors}")
            break

    # At target step — dump visible content
    print(f"\n=== Step {target_step} content ===")
    content = page.evaluate("""
        () => {
            const results = [];
            for (const el of document.querySelectorAll('input,select,button,a,th,td,h1,h2,span')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (!t || t.length < 2 || t.length > 100) continue;
                if (el.childElementCount > 3) continue;
                const r = el.getBoundingClientRect();
                if (r.width < 5 || r.height < 5) continue;
                if (r.y < 50 || r.y > 850) continue;
                results.push({
                    tag: el.tagName,
                    y: Math.round(r.y), x: Math.round(r.x),
                    text: t.slice(0,80),
                    val: (el.value || '').slice(0,40),
                    title: (el.getAttribute('title') || '').slice(0,40),
                });
            }
            return results;
        }
    """)
    for el in content:
        line = f"  [{el['tag']}] ({el['x']},{el['y']}) {el['text'][:60]}"
        if el['val']:
            line += f" = {el['val']}"
        if el['title']:
            line += f" [title={el['title']}]"
        print(line)

    ss(page, f"wizard_step{target_step}_detail")


def list_cycle_counts(page):
    """Get all cycle count names visible on the Manage Cycle Counts page."""
    NAV_LABELS = {'Help', 'Actions', 'View', 'Search', 'Save', 'Cancel', 'Reset', 'Add', 'Delete'}
    counts = page.evaluate("""
        () => {
            const names = [];
            for (const el of document.querySelectorAll('a')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim();
                if (!t || t.length < 3 || t.length > 60) continue;
                const r = el.getBoundingClientRect();
                // Cycle count links are in the left portion of the table
                if (r.x < 10 || r.x > 400 || r.y < 200 || r.y > 800) continue;
                names.push(t);
            }
            return [...new Set(names)];
        }
    """)
    # Filter out navigation labels
    return [c for c in counts if c not in NAV_LABELS]


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=60)
        ctx = browser.new_context(viewport={"width": 1600, "height": 900})
        page = ctx.new_page()

        load_session(ctx)
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(2000)
        print(f"Session: {page.title()[:60]}")

        if not navigate_to_inventory_mgmt(page):
            print("ABORT: Could not navigate to Inventory Management")
            browser.close()
            return

        if not ensure_task_panel_open(page):
            print("ABORT: Could not open task panel")
            browser.close()
            return

        if not select_counts_tab(page):
            print("ABORT: Could not select Counts tab")
            browser.close()
            return

        if not click_manage_cycle_counts(page):
            print("ABORT: Could not click Manage Cycle Counts")
            browser.close()
            return

        handle_org_dialog(page, org_code="3165_US_BUR_MFG")
        explore_cycle_counts_page(page)

        # Get the full list of cycle counts
        all_counts = list_cycle_counts(page)
        print(f"\nAll Burlington cycle counts: {all_counts}")

        # Sample items from our update list (pick a few to probe each count)
        sample_items = ["298-00114-93", "30001167", "399-20442-36", "60-66593-01",
                        "114-15721-01", "02040RIP4-SPRAY", "398-02077-73"]

        # Probe each Burlington cycle count
        all_found = {}  # {item: (count_name, class)}

        for cc_name in all_counts:
            if not any(kw in cc_name.upper() for kw in ["BUR", "BURLINGTON"]):
                continue
            remaining = [i for i in sample_items if i not in all_found]
            if not remaining:
                break

            print(f"\n{'='*60}")
            print(f"Probing: {cc_name}")
            cancel_wizard(page)
            if not open_cycle_count(page, cc_name):
                continue

            found = search_items_in_step5(page, cc_name, remaining)
            for item, cls in found.items():
                all_found[item] = (cc_name, cls)

        print("\n" + "="*60)
        print("SUMMARY - Items found in cycle counts:")
        for item, (cc, cls) in all_found.items():
            new_cls = BURLINGTON_UPDATES.get(item, '?')
            print(f"  {item}: currently class '{cls}' in '{cc}' → update to '{new_cls}'")
        not_found = [i for i in sample_items if i not in all_found]
        if not_found:
            print(f"  Not found in any count: {not_found}")

        print("\nDone. Screenshots saved to:", SS_DIR)
        page.wait_for_timeout(8000)
        browser.close()


if __name__ == "__main__":
    main()
