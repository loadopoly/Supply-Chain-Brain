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

# Top-nav tabs to enumerate (left → right order)
TOP_NAV_TABS = [
    "Me", "Sales", "Service", "Order Management",
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
    """Click 'Show More' links to expand all tiles/actions."""
    found = True
    while found:
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


def open_task_panel(page):
    """Open the right-side task panel if not already open."""
    for sel in ["[title='Tasks']", "[aria-label='Tasks']"]:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=2000):
                el.click()
                page.wait_for_timeout(1800)
                return True
        except Exception:
            pass
    return False


def get_task_panel_select_options(page):
    """Return list of option labels in the task panel SELECT dropdown."""
    return page.evaluate("""
        () => {
            for (const sel of document.querySelectorAll('select')) {
                if (!sel.offsetParent) continue;
                const r = sel.getBoundingClientRect();
                if (r.x < 900) continue;
                return Array.from(sel.options).map(o => o.text.trim());
            }
            return [];
        }
    """)


def select_task_tab(page, option_index: int):
    """
    Switch the task panel to the given option index using native
    mouse-click + keyboard (the only method that fires ADF's event handler).
    """
    page.mouse.click(1288, 107)
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


def navigate_to_module(page, tile_cx, tile_cy):
    """Click a module tile and wait for it to load."""
    page.mouse.click(tile_cx, tile_cy)
    page.wait_for_timeout(4000)
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass
    page.wait_for_timeout(1000)


def go_home(page):
    """Return to the Oracle Fusion home page."""
    page.goto(f"{HOST}/fscmUI/faces/FuseWelcome",
              wait_until="networkidle", timeout=60_000)
    page.wait_for_timeout(1500)


# ── main mapper ──────────────────────────────────────────────────────────────

def map_module(page, tab_name: str, tile: dict, schema: dict):
    """
    Navigate into a module tile, enumerate its task panel sections,
    and record everything into schema.
    """
    tile_text = tile['text']
    slug = tile_text.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_')
    print(f"\n    [MODULE] {tile_text}")

    navigate_to_module(page, tile['cx'], tile['cy'])
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

    # Click the tab
    try:
        page.locator(f"a:has-text('{tab_name}')").first.click(timeout=8000)
        page.wait_for_timeout(3000)
    except Exception as ex:
        print(f"  Tab click failed: {ex}")
        schema[tab_name] = {"error": str(ex), "modules": {}}
        return

    ss(page, f"tab_{tab_name[:15].replace(' ','_')}_overview")

    # Expand Show More to see all tiles and quick actions
    click_show_more(page)
    ss(page, f"tab_{tab_name[:15].replace(' ','_')}_expanded")

    tiles = get_tiles(page)
    quick_actions = get_quick_actions(page)

    print(f"  Tiles ({len(tiles)}): {[t['text'] for t in tiles]}")
    print(f"  Quick Actions ({len(quick_actions)}): {[q['text'] for q in quick_actions]}")

    schema[tab_name] = {
        "tiles": [t['text'] for t in tiles],
        "quick_actions": [q['text'] for q in quick_actions],
        "modules": {}
    }

    # Navigate into each tile and map its task panel
    for tile in tiles:
        try:
            map_module(page, tab_name, tile, schema)
            # Return to this tab after module exploration
            try:
                page.locator(f"a:has-text('{tab_name}')").first.click(timeout=8000)
                page.wait_for_timeout(2000)
                click_show_more(page)
            except Exception:
                go_home(page)
                try:
                    page.locator(f"a:has-text('{tab_name}')").first.click(timeout=8000)
                    page.wait_for_timeout(2000)
                    click_show_more(page)
                except Exception:
                    pass
            # Re-fetch tiles after navigation (positions may have changed)
            tiles_fresh = get_tiles(page)
            # Find this tile again by text
            tile_match = next((t for t in tiles_fresh if t['text'] == tile['text']), None)
            if tile_match:
                tile['cx'] = tile_match['cx']
                tile['cy'] = tile_match['cy']
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
                  wait_until="networkidle", timeout=120_000)
        page.wait_for_timeout(2000)
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
