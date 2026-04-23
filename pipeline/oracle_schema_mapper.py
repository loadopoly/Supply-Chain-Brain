"""
Oracle Fusion Interactive Schema Mapper
========================================
Iteratively navigates every top-nav tab → tile → task panel section,
building a complete map of accessible tasks and actions.

Output: oracle_schema_map.json + oracle_schema_map.txt (human-readable)
"""
import json, sys, time
from pathlib import Path
from playwright.sync_api import sync_playwright

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HOST = "https://fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com"
SESSION_FILE = Path(__file__).parent / "oracle_session.json"
SCREENSHOTS = Path(__file__).parent / "abc_screenshots" / "schema_map"
OUTPUT_JSON = Path(__file__).parent / "oracle_schema_map.json"
OUTPUT_TXT  = Path(__file__).parent / "oracle_schema_map.txt"
SCREENSHOTS.mkdir(parents=True, exist_ok=True)

# Top-nav tabs to enumerate — skip "Me" (user profile widget, no module tiles)
TOP_NAV_TABS = [
    "Sales", "Service", "Order Management",
    "Supply Chain Execution", "Supply Chain Planning",
    "Product Management", "General Accounting", "Procurement",
    "Tools", "Others",
]

# ── helpers ──────────────────────────────────────────────────────────────────

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


def ss(page, name: str):
    path = SCREENSHOTS / f"{name}.png"
    try:
        page.screenshot(path=str(path))
    except Exception:
        pass


DUMP_VISIBLE_JS = """
() => {
    const seen = new Set();
    const results = [];
    for (const el of document.querySelectorAll('a,li,button,span,div,td')) {
        if (!el.offsetParent) continue;
        const t = el.textContent.trim().replace(/\\s+/g,' ');
        if (!t || t.length < 2 || t.length > 100) continue;
        if (el.childElementCount > 3) continue;
        const r = el.getBoundingClientRect();
        if (r.height < 8 || r.width < 8) continue;
        const key = t + '|' + el.tagName;
        if (seen.has(key)) continue;
        seen.add(key);
        results.push({
            tag: el.tagName,
            text: t.slice(0,80),
            cx: Math.round(r.x + r.width/2),
            cy: Math.round(r.y + r.height/2),
            href: el.tagName==='A' ? (el.href||'').slice(0,120) : '',
            disabled: el.hasAttribute('disabled') ||
                      (el.className||'').toString().includes('Disabled')
        });
    }
    return results;
}
"""


def dump_page(page, min_x=0, min_y=0):
    """Return list of unique visible elements filtered by position."""
    raw = page.evaluate(DUMP_VISIBLE_JS)
    return [e for e in raw if e['cx'] >= min_x and e['cy'] >= min_y]


def get_tiles(page):
    """Enumerate SpringBoard tiles (A elements in content area y > 290)."""
    return page.evaluate("""
        () => {
            const seen = new Set();
            const results = [];
            for (const el of document.querySelectorAll('a')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (!t || t.length < 3 || t.length > 80 || seen.has(t)) continue;
                const r = el.getBoundingClientRect();
                if (r.y < 290 || r.height < 15) continue;
                seen.add(t);
                results.push({text: t,
                              cx: Math.round(r.x+r.width/2),
                              cy: Math.round(r.y+r.height/2),
                              href: (el.href||'').slice(0,120)});
            }
            return results;
        }
    """)


def get_all_tiles_with_scroll(page):
    """Enumerate ALL SpringBoard tiles, scrolling to capture off-viewport ones."""
    all_tiles = {}
    for scroll_y in [0, 400, 800, 1200, 1600, 2000]:
        page.evaluate(f"window.scrollTo(0, {scroll_y})")
        page.wait_for_timeout(400)
        batch = page.evaluate("""
            () => {
                const results = [];
                const seen = new Set();
                for (const el of document.querySelectorAll('a')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim().replace(/\\s+/g,' ');
                    if (!t || t.length < 3 || t.length > 80 || seen.has(t)) continue;
                    const r = el.getBoundingClientRect();
                    if (r.y < 200 || r.height < 15) continue;
                    seen.add(t);
                    results.push({text: t, href: (el.href||'').slice(0,120)});
                }
                return results;
            }
        """)
        for t in batch:
            if t['text'] not in all_tiles:
                all_tiles[t['text']] = t
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(300)
    return list(all_tiles.values())


def get_quick_actions(page):
    """Enumerate left-side Quick Actions links."""
    return page.evaluate("""
        () => {
            const seen = new Set();
            const results = [];
            for (const el of document.querySelectorAll('a,span,div')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (!t || t.length < 3 || t.length > 80 || seen.has(t)) continue;
                if (el.childElementCount > 1) continue;
                const r = el.getBoundingClientRect();
                if (r.x > 460 || r.y < 290) continue;
                seen.add(t);
                results.push({text: t,
                              cx: Math.round(r.x+r.width/2),
                              cy: Math.round(r.y+r.height/2)});
            }
            return results;
        }
    """)


def click_show_more(page):
    """Click 'Show More' links to expand tiles/actions (max 3 clicks)."""
    for _ in range(3):
        found = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('a,button,span,div')) {
                    if (!el.offsetParent) continue;
                    if (el.textContent.trim() === 'Show More') {
                        el.click();
                        return true;
                    }
                }
                return false;
            }
        """)
        if found:
            page.wait_for_timeout(1500)
        else:
            break


def open_task_panel(page):
    """Open the right-side task panel if not already open."""
    # Extra wait for ADF Classic to fully render after module navigation
    page.wait_for_timeout(2000)
    for sel in ["[title='Tasks']", "[aria-label='Tasks']", "button[title='Tasks']",
                "a[title='Tasks']", "[role='button'][title='Tasks']"]:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=5000):
                el.click()
                page.wait_for_timeout(2500)
                return True
        except Exception:
            pass
    # Fallback: JS click on any Tasks button in the toolbar area
    found = page.evaluate("""
        () => {
            for (const el of document.querySelectorAll('a,button,span,div')) {
                if (!el.offsetParent) continue;
                const title = el.getAttribute('title') || '';
                const label = el.getAttribute('aria-label') || '';
                if (title === 'Tasks' || label === 'Tasks') {
                    const r = el.getBoundingClientRect();
                    if (r.y < 200) {
                        el.click();
                        return true;
                    }
                }
            }
            return false;
        }
    """)
    if found:
        page.wait_for_timeout(2500)
        return True
    return False


def get_task_panel_select(page):
    """Return (cx, cy, options) for the task panel SELECT dropdown, or (None, None, [])."""
    result = page.evaluate("""
        () => {
            for (const sel of document.querySelectorAll('select')) {
                if (!sel.offsetParent) continue;
                const r = sel.getBoundingClientRect();
                if (r.x < 900) continue;
                const opts = Array.from(sel.options).map(o => o.text.trim());
                return {
                    cx: Math.round(r.x + r.width/2),
                    cy: Math.round(r.y + r.height/2),
                    options: opts
                };
            }
            return null;
        }
    """)
    if result:
        return result['cx'], result['cy'], result['options']
    return None, None, []


def get_task_panel_select_options(page):
    """Return list of option labels in the task panel SELECT dropdown."""
    _, _, options = get_task_panel_select(page)
    return options


def select_task_tab(page, option_index: int):
    """
    Switch the task panel to the given option index using native
    mouse-click + keyboard (the only method that fires ADF's event handler).
    Uses dynamically detected SELECT position.
    """
    cx, cy, _ = get_task_panel_select(page)
    if cx is None:
        cx, cy = 1288, 107  # fallback to hardcoded
    page.mouse.click(cx, cy)
    page.wait_for_timeout(300)
    page.keyboard.press("Home")
    page.wait_for_timeout(100)
    for _ in range(option_index):
        page.keyboard.press("ArrowDown")
        page.wait_for_timeout(80)
    page.keyboard.press("Enter")
    page.wait_for_timeout(2000)


def get_task_panel_content(page):
    """Return section headers and task links currently visible in the task panel."""
    return page.evaluate("""
        () => {
            const sections = [];
            let current_section = {header: 'Unknown', tasks: []};

            const els = Array.from(document.querySelectorAll('div,li,a'))
                .filter(el => {
                    if (!el.offsetParent) return false;
                    const r = el.getBoundingClientRect();
                    return r.x > 1100 && r.y > 100 && r.height > 8;
                })
                .sort((a,b) => a.getBoundingClientRect().y - b.getBoundingClientRect().y);

            const seen = new Set();
            for (const el of els) {
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (!t || t.length < 3 || t.length > 100 || seen.has(t)) continue;
                if (el.childElementCount > 2) continue;
                seen.add(t);

                const r = el.getBoundingClientRect();
                const cls = (el.className||'').toString();

                // Section header: DIV with class containing 'xmu' or special section classes
                if (el.tagName === 'DIV' && (cls.includes('xmu') || cls.includes('x16g'))) {
                    if (current_section.tasks.length > 0 || sections.length > 0) {
                        sections.push(current_section);
                    }
                    current_section = {header: t, tasks: []};
                } else if (el.tagName === 'LI') {
                    current_section.tasks.push({
                        text: t,
                        cx: Math.round(r.x+r.width/2),
                        cy: Math.round(r.y+r.height/2)
                    });
                }
            }
            if (current_section.tasks.length > 0) {
                sections.push(current_section);
            }
            return sections;
        }
    """)


def navigate_to_module_by_text(page, tile_text: str) -> bool:
    """
    Navigate into a module tile using text-based locator with auto-scroll.
    This correctly handles tiles that are below the viewport fold.
    """
    try:
        # Use exact text match on A elements in the content area (y > 200)
        loc = page.locator("a").filter(has_text=tile_text).first
        loc.scroll_into_view_if_needed(timeout=6000)
        page.wait_for_timeout(600)
        loc.click(timeout=8000)
        page.wait_for_timeout(5000)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=15_000)
        except Exception:
            pass
        page.wait_for_timeout(2000)
        return True
    except Exception as ex:
        print(f"    Tile '{tile_text}' nav error (locator): {ex}")
        # Fallback: JS scroll-into-view + click
        try:
            result = page.evaluate("""
                ([txt]) => {
                    for (const el of document.querySelectorAll('a')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim().replace(/\\s+/g,' ');
                        if (t !== txt) continue;
                        const r = el.getBoundingClientRect();
                        if (r.y < 200) continue;
                        el.scrollIntoView({block:'center'});
                        return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                    }
                    return null;
                }
            """, [tile_text])
            if result:
                page.wait_for_timeout(500)
                page.mouse.click(result['cx'], result['cy'])
                page.wait_for_timeout(5000)
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=15_000)
                except Exception:
                    pass
                page.wait_for_timeout(2000)
                return True
        except Exception as ex2:
            print(f"    Tile '{tile_text}' nav error (JS fallback): {ex2}")
        return False


def go_home(page):
    """Return to the Oracle Fusion home page."""
    try:
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome",
                  wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(2000)
    except Exception:
        pass


# ── main mapper ──────────────────────────────────────────────────────────────

def map_module(page, tab_name: str, tile: dict, schema: dict):
    """
    Navigate into a module tile, enumerate its task panel sections,
    and record everything into schema.
    """
    tile_text = tile['text']
    slug = tile_text.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_')
    print(f"\n    [MODULE] {tile_text}")

    ok = navigate_to_module_by_text(page, tile_text)
    if not ok:
        print(f"    Navigation failed — skipping")
        schema[tab_name]["modules"][tile_text] = {"error": "navigation failed"}
        go_home(page)
        return

    module_title = page.title()
    print(f"    Title: {module_title[:70]}")
    ss(page, f"{tab_name[:8]}_{slug[:20]}_overview")

    module_entry = {
        "title": module_title,
        "tile": tile_text,
        "task_sections": {},
        "raw_visible": []
    }

    # Dump raw visible elements for archival
    visible = dump_page(page, min_x=900, min_y=100)
    module_entry["raw_visible"] = [e['text'] for e in visible if len(e['text']) < 60]

    task_opened = open_task_panel(page)
    if not task_opened:
        print(f"    (no task panel found)")
        schema[tab_name]["modules"][tile_text] = module_entry
        go_home(page)
        return

    # Get SELECT options for this module
    options = get_task_panel_select_options(page)
    print(f"    Task panel options: {options}")
    module_entry["task_panel_options"] = options

    if not options:
        # No SELECT — dump whatever is in the task panel
        sections = get_task_panel_content(page)
        module_entry["task_sections"]["default"] = sections
        ss(page, f"{tab_name[:8]}_{slug[:20]}_tasks")
        for sec in sections:
            star = "***" if any(k in sec['header'].lower() for k in
                                ['abc','classif','categor','strateg','assign']) else "   "
            print(f"    {star} Section: {sec['header']} ({len(sec['tasks'])} tasks)")
            for t in sec['tasks']:
                lo = t['text'].lower()
                s2 = "***" if any(k in lo for k in
                                  ['abc','classif','categor','strateg','assign']) else "   "
                print(f"        {s2} {t['text']}")
    else:
        # Cycle through each SELECT option
        for i, opt_name in enumerate(options):
            select_task_tab(page, i)
            page.wait_for_timeout(500)
            sections = get_task_panel_content(page)
            module_entry["task_sections"][opt_name] = sections
            ss(page, f"{tab_name[:8]}_{slug[:20]}_tab{i}_{opt_name[:10]}")

            print(f"    [{opt_name}]")
            for sec in sections:
                star = "***" if any(k in sec['header'].lower() for k in
                                    ['abc','classif','categor','strateg','assign']) else "   "
                print(f"      {star} Section: {sec['header']} ({len(sec['tasks'])} tasks)")
                for t in sec['tasks']:
                    lo = t['text'].lower()
                    s2 = "***" if any(k in lo for k in
                                      ['abc','classif','categor','strateg','assign']) else "   "
                    print(f"          {s2} {t['text']}")

    schema[tab_name]["modules"][tile_text] = module_entry
    go_home(page)


def map_tab(page, tab_name: str, schema: dict):
    """Navigate to a top-nav tab, enumerate all tiles + quick actions."""
    print(f"\n{'='*60}")
    print(f"TAB: {tab_name}")
    print(f"{'='*60}")

    # Click the tab — try exact A locator, then DIV
    clicked = False
    for attempt in [f"a:has-text('{tab_name}')", f"[role='tab']:has-text('{tab_name}')"]:
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
        # Fallback: JS text match on nav area
        res = page.evaluate("""
            ([txt]) => {
                for (const el of document.querySelectorAll('a,div,span')) {
                    if (!el.offsetParent) continue;
                    if (el.textContent.trim() !== txt) continue;
                    const r = el.getBoundingClientRect();
                    if (r.y > 280 || r.y < 220) continue;
                    el.click();
                    return true;
                }
                return false;
            }
        """, [tab_name])
        page.wait_for_timeout(2500)
        if not res:
            print(f"  Tab '{tab_name}' not found — skipping")
            schema[tab_name] = {"error": "tab not found", "modules": {}}
            return

    ss(page, f"tab_{tab_name[:15].replace(' ','_')}_overview")
    click_show_more(page)
    page.wait_for_timeout(1000)
    ss(page, f"tab_{tab_name[:15].replace(' ','_')}_expanded")

    # Use scroll-based enumeration to capture ALL tiles (including off-viewport)
    tiles_all = get_all_tiles_with_scroll(page)
    quick_actions = get_quick_actions(page)

    # Filter out nav links (y < 290 tiles already excluded inside get_all_tiles_with_scroll)
    # Additional filter: exclude very short strings and known nav items
    tiles_all = [t for t in tiles_all if len(t['text']) >= 3 and len(t['text']) <= 80]

    print(f"  Tiles ({len(tiles_all)}): {[t['text'] for t in tiles_all]}")
    print(f"  Quick Actions ({len(quick_actions)}): {[q['text'] for q in quick_actions[:8]]}")

    schema[tab_name] = {
        "tiles": [t['text'] for t in tiles_all],
        "quick_actions": [q['text'] for q in quick_actions],
        "modules": {}
    }

    # Navigate into each tile using text-based navigation
    for tile in tiles_all:
        try:
            map_module(page, tab_name, tile, schema)
            # Return to this tab after module exploration
            returned = False
            for attempt in [f"a:has-text('{tab_name}')", f"[role='tab']:has-text('{tab_name}')"]:
                try:
                    page.locator(attempt).first.click(timeout=6000)
                    page.wait_for_timeout(2000)
                    click_show_more(page)
                    page.wait_for_timeout(500)
                    returned = True
                    break
                except Exception:
                    pass
            if not returned:
                go_home(page)
                try:
                    page.locator(f"a:has-text('{tab_name}')").first.click(timeout=6000)
                    page.wait_for_timeout(2000)
                    click_show_more(page)
                    page.wait_for_timeout(500)
                except Exception:
                    pass
        except Exception as ex:
            print(f"    Error mapping {tile['text']}: {ex}")
            schema[tab_name]["modules"][tile['text']] = {"error": str(ex)}
            go_home(page)


def write_output(schema: dict):
    """Write JSON and human-readable text output."""
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)
    print(f"\nJSON saved: {OUTPUT_JSON}")

    lines = ["Oracle Fusion Interactive Schema Map", "=" * 60, ""]
    for tab, tab_data in schema.items():
        lines.append(f"\n{'='*60}")
        lines.append(f"TAB: {tab}")
        lines.append(f"{'='*60}")
        if "error" in tab_data:
            lines.append(f"  ERROR: {tab_data['error']}")
            continue
        tiles_list = tab_data.get('tiles', [])
        qa_list = tab_data.get('quick_actions', [])
        lines.append(f"  Tiles: {', '.join(tiles_list)}")
        lines.append(f"  Quick Actions: {', '.join(qa_list[:10])}")

        for tile_name, mod in tab_data.get("modules", {}).items():
            lines.append(f"\n  MODULE: {tile_name}")
            if "error" in mod:
                lines.append(f"    ERROR: {mod['error']}")
                continue
            lines.append(f"    Title: {mod.get('title','')}")
            opts = mod.get('task_panel_options', [])
            if opts:
                lines.append(f"    Task Panel Tabs: {', '.join(opts)}")
            for tab_opt, sections in mod.get('task_sections', {}).items():
                lines.append(f"    [{tab_opt}]")
                for sec in sections:
                    abc_flag = " *** ABC ***" if any(k in sec['header'].lower() for k in
                                                      ['abc','classif','categor','strateg']) else ""
                    lines.append(f"      Section: {sec['header']}{abc_flag}")
                    for t in sec['tasks']:
                        lo = t['text'].lower()
                        flag = " *** ABC ***" if any(k in lo for k in
                                                      ['abc','classif','categor','strateg']) else ""
                        lines.append(f"        - {t['text']}{flag}")

    with open(OUTPUT_TXT, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f"TXT  saved: {OUTPUT_TXT}")


def main():
    schema = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=60)
        ctx = browser.new_context(viewport={"width": 1600, "height": 900})
        page = ctx.new_page()
        load_cookies(ctx)

        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome",
                  wait_until="domcontentloaded", timeout=120_000)
        page.wait_for_timeout(3000)
        print(f"Home loaded: {page.title()}")

        for tab_name in TOP_NAV_TABS:
            try:
                map_tab(page, tab_name, schema)
                # Always return home before next tab
                go_home(page)
                write_output(schema)  # incremental saves
            except Exception as ex:
                print(f"\nFATAL error on tab {tab_name}: {ex}")
                schema.setdefault(tab_name, {})["fatal"] = str(ex)
                go_home(page)

        browser.close()

    write_output(schema)
    print("\nSchema mapping complete.")


if __name__ == "__main__":
    main()
