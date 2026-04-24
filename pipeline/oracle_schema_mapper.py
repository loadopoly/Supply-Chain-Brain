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


# ── session management ────────────────────────────────────────────────────────

def _get_vault_creds() -> dict | None:
    """Load oracle_fusion credentials from the DPAPI vault."""
    try:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).parent))
        from src.connections.secrets import get_credentials
        return get_credentials("oracle_fusion")
    except Exception as ex:
        print(f"[auth] Vault read failed: {ex}")
        return None


def _sso_login(page, context) -> bool:
    """
    Perform Microsoft Entra SSO login using vault credentials.
    Auto-fills email + password, then waits for FuseWelcome.
    Returns True on success.
    """
    creds = _get_vault_creds()
    ss_dir = Path(__file__).parent / "abc_screenshots"
    ss_dir.mkdir(exist_ok=True)

    if creds and creds.get("user") and creds.get("password"):
        print(f"[auth] Auto-filling credentials for {creds['user']} ...")

        try:
            page.wait_for_selector("button, input[type='submit']", timeout=10000)
            page.screenshot(path=str(ss_dir / "auth_01_login_page.png"))

            # Step 1: Click "Company Single Sign-On" — this is the corporate SSO path
            sso_clicked = False
            for sel in ["button:has-text('Company Single Sign-On')",
                        "a:has-text('Company Single Sign-On')",
                        "button:has-text('Single Sign')",
                        "a:has-text('Single Sign')"]:
                try:
                    el = page.locator(sel).first
                    if el.is_visible(timeout=2000):
                        el.click()
                        sso_clicked = True
                        print(f"[auth] Clicked SSO button via: {sel}")
                        page.wait_for_timeout(3000)
                        break
                except Exception:
                    pass

            page.screenshot(path=str(ss_dir / "auth_02_after_sso_click.png"))
            inputs_after = page.evaluate("""
                () => Array.from(document.querySelectorAll('input')).map(el => ({
                    type: el.type, name: el.name, id: el.id,
                    placeholder: el.placeholder, visible: !!el.offsetParent
                }))
            """)
            print(f"[auth] Inputs after SSO click: {inputs_after}")

            # Step 2: Fill corporate email (Microsoft Entra two-step flow)
            for sel in ["input[type='email']", "input[name='loginfmt']",
                        "input[name='username']", "input[type='text']"]:
                try:
                    el = page.locator(sel).first
                    if el.is_visible(timeout=4000):
                        el.fill(creds["user"])
                        print(f"[auth] Filled email via: {sel}")
                        break
                except Exception:
                    pass

            # Click Next
            for sel in ["input[type='submit']", "button[type='submit']",
                        "button:has-text('Next')", "button:has-text('Sign in')"]:
                try:
                    el = page.locator(sel).first
                    if el.is_visible(timeout=2000):
                        el.click()
                        print(f"[auth] Clicked Next via: {sel}")
                        break
                except Exception:
                    pass
            page.wait_for_timeout(3000)
            page.screenshot(path=str(ss_dir / "auth_03_after_email.png"))

            # Step 3: Fill password
            try:
                page.wait_for_selector("input[type='password']", timeout=10000)
                page.fill("input[type='password']", creds["password"])
                print("[auth] Filled password")
                for sel in ["input[type='submit']", "button[type='submit']",
                            "button:has-text('Sign in')", "button:has-text('Next')"]:
                    try:
                        el = page.locator(sel).first
                        if el.is_visible(timeout=2000):
                            el.click()
                            print(f"[auth] Clicked Sign in via: {sel}")
                            break
                    except Exception:
                        pass
                page.wait_for_timeout(3000)
                page.screenshot(path=str(ss_dir / "auth_04_after_password.png"))
            except Exception as ex:
                print(f"[auth] Password step error: {ex}")

            # Step 4: "Stay signed in?" prompt
            try:
                for sel in ["button:has-text('Yes')", "input[type='submit']",
                            "button[type='submit']", "button:has-text('No')"]:
                    try:
                        el = page.locator(sel).first
                        if el.is_visible(timeout=4000):
                            el.click()
                            print(f"[auth] Dismissed post-login prompt via: {sel}")
                            break
                    except Exception:
                        pass
            except Exception:
                pass

        except Exception as ex:
            print(f"[auth] Login fill error: {ex}")
            page.screenshot(path=str(ss_dir / "auth_error.png"))

    else:
        print("[auth] No vault credentials — manual login required in browser window.")

    # Wait up to 3 minutes for FuseWelcome (covers MFA wait time)
    print("[auth] Waiting for FuseWelcome URL (up to 3 min, complete any MFA now) ...")
    try:
        page.wait_for_url(
            lambda url: "FuseWelcome" in url or "homePage" in url,
            timeout=180_000,
        )
    except Exception:
        try:
            page.wait_for_load_state("networkidle", timeout=60_000)
        except Exception:
            pass

    page.screenshot(path=str(ss_dir / "auth_05_final.png"))
    title = page.title()
    print(f"[auth] Final page: {title[:60]} | {page.url[:80]}")

    if "Sign In" in title:
        print(f"[auth] Login failed. Screenshots saved to {ss_dir}")
        return False

    # Save refreshed cookies
    cookies = context.cookies()
    with open(SESSION_FILE, "w") as f:
        json.dump(cookies, f, indent=2)
    print(f"[auth] Session saved ({len(cookies)} cookies) → {SESSION_FILE}")
    return True


def ensure_session(page, context) -> bool:
    """
    Load cached cookies and verify the session is live.
    Re-authenticates via SSO if expired.
    Returns True when the home page is loaded and ready.
    """
    # Load cached cookies if available
    if SESSION_FILE.exists():
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
        print(f"[auth] Loaded {len(pw)} cached cookies from {SESSION_FILE}")

    # Navigate to home — detect whether session is valid
    page.goto(f"{HOST}/fscmUI/faces/FuseWelcome",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(2000)
    title = page.title()
    print(f"[auth] Page title after cookie load: {title}")

    if "Sign In" in title:
        print("[auth] Session expired — starting SSO login ...")
        return _sso_login(page, context)

    print(f"[auth] Session valid: {title[:60]}")
    return True

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
    """Enumerate SpringBoard tiles (A elements in content area y > 290, truly visible)."""
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
                // Check computed visibility — filters out visibility:hidden sidebar items
                const style = window.getComputedStyle(el);
                if (style.visibility === 'hidden' || style.display === 'none'
                    || parseFloat(style.opacity) < 0.1) continue;
                // Check parent chain for visibility:hidden
                let parent = el.parentElement;
                let parentHidden = false;
                for (let i = 0; i < 6 && parent; i++) {
                    const ps = window.getComputedStyle(parent);
                    if (ps.visibility === 'hidden' || ps.display === 'none') {
                        parentHidden = true; break;
                    }
                    parent = parent.parentElement;
                }
                if (parentHidden) continue;
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
    """Return section headers and task links currently visible in the task panel.

    ADF Classic task panels use obfuscated CSS class names that change between releases,
    so we detect section headers via computed font-weight / structural heuristics:
    - bold DIV with no <a> children → section header
    - LI element or A element at x>1100 → task link
    Also falls back to scraping all <a> links in the right panel when no LI structure found.
    """
    return page.evaluate("""
        () => {
            const PANEL_X = 1100;
            const sections = [];
            let current_section = {header: 'Unknown', tasks: []};

            const els = Array.from(document.querySelectorAll('div,li,a,span'))
                .filter(el => {
                    if (!el.offsetParent) return false;
                    const r = el.getBoundingClientRect();
                    return r.x > PANEL_X && r.y > 75 && r.height > 8;
                })
                .sort((a,b) => a.getBoundingClientRect().y - b.getBoundingClientRect().y);

            const seen = new Set();
            for (const el of els) {
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (!t || t.length < 3 || t.length > 100 || seen.has(t)) continue;
                if (el.childElementCount > 3) continue;
                seen.add(t);

                const r = el.getBoundingClientRect();
                const tag = el.tagName;

                // Section header detection: bold DIV/SPAN with no anchor children,
                // or ADF-specific classes (xmu, x16g), or role="heading"
                const cls = (el.className||'').toString();
                const fw = window.getComputedStyle(el).fontWeight;
                const bold = parseInt(fw) >= 600;
                const hasAnchorChild = !!el.querySelector('a');
                const isHeading = el.getAttribute('role') === 'heading';
                const isAdfHeader = cls.includes('xmu') || cls.includes('x16g')
                                 || cls.includes('Header') || cls.includes('header');

                const isSection = tag === 'DIV' && (bold || isAdfHeader || isHeading)
                                  && !hasAnchorChild && r.height < 35;

                if (isSection) {
                    if (current_section.tasks.length > 0 || sections.length > 0) {
                        sections.push(current_section);
                    }
                    current_section = {header: t, tasks: []};
                } else if (tag === 'LI' || tag === 'A') {
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
    Navigate into a module tile. Uses CSS visibility traversal to find the
    truly visible tile, skipping hidden duplicates from other tabs' SpringBoards.
    """
    try:
        result = page.evaluate("""
            ([txt]) => {
                for (const el of document.querySelectorAll('a')) {
                    const t = el.textContent.trim().replace(/\\s+/g,' ');
                    if (t !== txt) continue;
                    const r = el.getBoundingClientRect();
                    if (r.y < 200 || r.height < 10) continue;
                    // CSS visibility check on element and parent chain (mirrors get_tiles)
                    const style = window.getComputedStyle(el);
                    if (style.visibility === 'hidden' || style.display === 'none'
                        || parseFloat(style.opacity) < 0.1) continue;
                    let parent = el.parentElement;
                    let parentHidden = false;
                    for (let i = 0; i < 6 && parent; i++) {
                        const ps = window.getComputedStyle(parent);
                        if (ps.visibility === 'hidden' || ps.display === 'none') {
                            parentHidden = true; break;
                        }
                        parent = parent.parentElement;
                    }
                    if (parentHidden) continue;
                    // Scroll into view and return refreshed coordinates
                    el.scrollIntoView({block:'center', behavior:'instant'});
                    const r2 = el.getBoundingClientRect();
                    return {cx: Math.round(r2.x+r2.width/2), cy: Math.round(r2.y+r2.height/2)};
                }
                return null;
            }
        """, [tile_text])

        if result:
            page.wait_for_timeout(400)
            page.mouse.click(result['cx'], result['cy'])
            page.wait_for_timeout(5000)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=15_000)
            except Exception:
                pass
            page.wait_for_timeout(2000)
            return True
        else:
            print(f"    Tile '{tile_text}' not found in visible DOM")
    except Exception as ex:
        print(f"    Tile '{tile_text}' nav error: {ex}")
    return False


def return_to_tab(page, tab_name: str) -> bool:
    """Navigate back to a specific top-nav tab and re-expand Show More."""
    # Try going home first for a clean state
    go_home(page)
    for attempt in [f"a:has-text('{tab_name}')", f"[role='tab']:has-text('{tab_name}')"]:
        try:
            loc = page.locator(attempt).first
            if loc.is_visible(timeout=5000):
                loc.click()
                page.wait_for_timeout(2500)
                click_show_more(page)
                page.wait_for_timeout(500)
                return True
        except Exception:
            pass
    # JS fallback
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
    if res:
        page.wait_for_timeout(2500)
        click_show_more(page)
        page.wait_for_timeout(500)
        return True
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

def _module_has_content(schema: dict, tab_name: str, tile_text: str) -> bool:
    """Return True if this module already has meaningful task section content mapped.
    Requires at least 2 tasks that are not trivial UI controls ('Add Fields', 'Help',
    'Done', 'Save', 'Personal Information').
    """
    NOISE = {'Add Fields', 'Help', 'Done', 'Save', 'Personal Information', 'Refresh'}
    mod = schema.get(tab_name, {}).get("modules", {}).get(tile_text)
    if not mod:
        return False
    ts = mod.get("task_sections", {})
    real_tasks = [
        t["text"]
        for secs in ts.values()
        for sec in secs
        for t in sec.get("tasks", [])
        if t["text"] not in NOISE
    ]
    return len(real_tasks) >= 2


def map_module(page, tab_name: str, tile: dict, schema: dict):
    """
    Navigate into a module tile, enumerate its task panel sections,
    and record everything into schema.
    Skips modules that already have task section content (resume mode).
    """
    tile_text = tile['text']
    slug = tile_text.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_')

    if _module_has_content(schema, tab_name, tile_text):
        print(f"\n    [SKIP] {tile_text} — already mapped")
        return

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

    # Precheck: Redwood modules render the task panel already open on page load.
    # Read content BEFORE attempting to click the Tasks icon — clicking it would
    # toggle the panel closed if it's already open.
    precheck = get_task_panel_content(page)
    if precheck:
        print(f"    (task panel already open — Redwood layout)")
        task_opened = True
    else:
        task_opened = open_task_panel(page)
        if not task_opened:
            # Final fallback: re-read in case panel opened without returning True
            precheck = get_task_panel_content(page)
            if not precheck:
                print(f"    (no task panel found)")
                schema[tab_name]["modules"][tile_text] = module_entry
                go_home(page)
                return
            print(f"    (task panel opened via fallback)")
            task_opened = True

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
    """Navigate to a top-nav tab, enumerate its tiles, then map each from home."""
    print(f"\n{'='*60}")
    print(f"TAB: {tab_name}")
    print(f"{'='*60}")

    # Go home first to get a clean state
    go_home(page)

    # Click the tab
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

    # Enumerate tiles BEFORE Show More (tab-specific)
    tiles_before = get_tiles(page)
    quick_actions = get_quick_actions(page)

    # Click Show More and re-enumerate
    click_show_more(page)
    page.wait_for_timeout(800)
    tiles_after = get_tiles(page)

    # Merge unique tiles
    tab_name_set = set(TOP_NAV_TABS) | {"Show More"}
    seen_texts = set()
    tiles_all = []
    for t in tiles_before + tiles_after:
        if t['text'] not in seen_texts and t['text'] not in tab_name_set:
            if 3 <= len(t['text']) <= 80:
                tiles_all.append(t)
                seen_texts.add(t['text'])

    ss(page, f"tab_{tab_name[:15].replace(' ','_')}_expanded")
    print(f"  Tiles ({len(tiles_all)}): {[t['text'] for t in tiles_all]}")
    print(f"  Quick Actions: {[q['text'] for q in quick_actions[:8]]}")

    schema[tab_name] = {
        "tiles": [t['text'] for t in tiles_all],
        "quick_actions": [q['text'] for q in quick_actions],
        "modules": {}
    }

    # Navigate each tile while staying in the tab context
    for tile in tiles_all:
        try:
            map_module(page, tab_name, tile, schema)
            # Return to this tab and re-expand Show More for the next tile
            if not return_to_tab(page, tab_name):
                print(f"  Could not return to tab '{tab_name}' — stopping this tab")
                break
        except Exception as ex:
            print(f"    Error mapping {tile['text']}: {ex}")
            schema[tab_name]["modules"][tile['text']] = {"error": str(ex)}
            return_to_tab(page, tab_name)


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
    # Resume mode: load existing schema so already-mapped modules are skipped
    if OUTPUT_JSON.exists():
        with open(OUTPUT_JSON, encoding='utf-8') as f:
            schema = json.load(f)
        mapped = sum(
            1 for tab in schema.values()
            for mod in tab.get("modules", {}).values()
            if any(s for s in mod.get("task_sections", {}).values())
        )
        print(f"[resume] Loaded existing schema ({mapped} modules already mapped)")
    else:
        schema = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=60)
        ctx = browser.new_context(viewport={"width": 1600, "height": 900})
        page = ctx.new_page()

        ok = ensure_session(page, ctx)
        if not ok:
            print("Authentication failed — aborting.")
            browser.close()
            return

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
