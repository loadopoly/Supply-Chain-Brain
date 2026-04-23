"""
Update ABC Codes in Oracle Fusion PIM via Playwright UI automation.

ABC Class in Oracle Fusion is NOT an attribute — it is a Category assignment in the
"ABC Inventory Catalog" on the Categories tab of the item detail page.

Per-item workflow:
  Manage Items search -> click item link in BUR row ->
  item opens in Edit mode -> Categories tab ->
  select ABC Inventory row -> delete row -> add new row with target category code ->
  Save and Close -> breadcrumb back to Manage Items

Usage:
    python update_abc_ui_burlington.py [--dry-run] [--explore] [--item ITEM_NUM]
"""
import json
import sys
import argparse
import openpyxl
from pathlib import Path

# Ensure Unicode characters (e.g., → in Oracle cell text) don't crash on Windows cp1252
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

EXCEL_PATH = (
    r"C:\Users\agard\OneDrive - astecindustries.com"
    r"\Cycle Count Review\Claude Changes\Burlington_Strat_Change_04212026.xlsx"
)
HOST = "https://fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com"
ORG_CODE = "3165_US_BUR_MFG"
SESSION_FILE = Path(__file__).parent / "oracle_session.json"
SCREENSHOTS = Path(__file__).parent / "abc_screenshots"


def load_changes() -> list[dict]:
    import shutil, tempfile
    src = Path(EXCEL_PATH)
    try:
        tmp = Path(tempfile.mktemp(suffix=".xlsx"))
        shutil.copy2(src, tmp)
        read_path = tmp
    except Exception:
        read_path = src
    wb = openpyxl.load_workbook(str(read_path), read_only=True, data_only=True)
    ws = wb.active
    changes = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        org, item, subinv, locator, desc, qty, old_abc, new_abc = row
        if not item or not new_abc:
            continue
        changes.append({
            "item": str(item).strip(),
            "old_abc": str(old_abc).strip() if old_abc else "",
            "new_abc": str(new_abc).strip(),
            "desc": desc,
        })
    wb.close()
    try:
        read_path.unlink(missing_ok=True)
    except Exception:
        pass
    return changes


def _load_cookies(context) -> bool:
    if not SESSION_FILE.exists():
        return False
    with open(SESSION_FILE) as f:
        cookies = json.load(f)
    pw_cookies = []
    for c in cookies:
        cookie = {
            "name": c["name"],
            "value": c["value"],
            "domain": c.get("domain", "").lstrip("."),
            "path": c.get("path", "/"),
            "secure": c.get("secure", True),
            "httpOnly": c.get("httpOnly", False),
        }
        if c.get("expires") and c["expires"] > 0:
            cookie["expires"] = int(c["expires"])
        pw_cookies.append(cookie)
    try:
        context.add_cookies(pw_cookies)
        return True
    except Exception as e:
        print(f"[WARN] Cookie load error: {e}")
        return False


def _ss(page, name: str):
    SCREENSHOTS.mkdir(exist_ok=True)
    path = str(SCREENSHOTS / f"{name}.png")
    page.screenshot(path=path)
    return path


def run_ui_updates(changes: list[dict], dry_run: bool, explore: bool, single_item: str | None):
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    results = {"success": [], "failed": [], "skipped": []}

    if single_item:
        changes = [c for c in changes if c["item"] == single_item]
        if not changes:
            print(f"[ERROR] Item '{single_item}' not found in Excel.")
            return results

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=150)
        context = browser.new_context(viewport={"width": 1600, "height": 900})
        page = context.new_page()

        if _load_cookies(context):
            print("[UI] Loaded cached SSO cookies.")

        print("[UI] Opening Oracle Fusion...")
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="networkidle", timeout=120_000)

        current = page.url
        if "microsoftonline" in current or "login" in current.lower():
            print("[UI] Session expired — please complete SSO login in the browser.")
            page.wait_for_url(lambda url: HOST in url, timeout=180_000)
            cookies = context.cookies()
            with open(SESSION_FILE, "w") as f:
                json.dump(cookies, f, indent=2)
            print("[UI] Session refreshed and cached.")

        # Auto-dismiss only auth challenges (not beforeunload confirms which would close the page)
        def _handle_dialog(dialog):
            if dialog.type in ("beforeunload", "confirm"):
                dialog.accept()   # accept = stay on page for beforeunload
            else:
                dialog.dismiss()
        page.on("dialog", _handle_dialog)

        def dismiss_dialogs():
            for sel in [
                "button:has-text('OK')", "button:has-text('Close')",
                "[title='Close']",
            ]:
                try:
                    btn = page.locator(sel).first
                    if btn.is_visible(timeout=800):
                        btn.click()
                        page.wait_for_timeout(400)
                except Exception:
                    pass

        def dismiss_oracle_reauth():
            """Dismiss Oracle ADF's HTML re-authentication popup (session timeout modal)."""
            try:
                # Oracle re-auth popup has a Cancel button and a Sign in header
                cancel = page.locator("button:has-text('Cancel')").first
                if cancel.is_visible(timeout=600):
                    print("[UI] Oracle re-auth popup detected — dismissing...")
                    cancel.click()
                    page.wait_for_timeout(800)
                    return True
            except Exception:
                pass
            return False

        dismiss_dialogs()
        page.wait_for_timeout(1500)

        _navigate_to_manage_items(page)
        _ss(page, "00_manage_items")
        print("[UI] On Manage Items page.")

        if dry_run:
            print(f"\n[DRY-RUN] Would update {len(changes)} items:")
            for ch in changes:
                print(f"  {ch['item']:30s}  {ch['old_abc']} -> {ch['new_abc']}")
            browser.close()
            return results

        for i, ch in enumerate(changes):
            item = ch["item"]
            new_abc = ch["new_abc"]
            old_abc = ch["old_abc"]
            print(f"\n[{i+1}/{len(changes)}] {item}  ({old_abc} -> {new_abc})")

            # Dismiss Oracle re-auth popup if session timed out between items
            dismiss_oracle_reauth()

            try:
                ok = _update_one_item(page, item, new_abc, explore=explore)
                if ok:
                    print(f"  OK")
                    results["success"].append(item)
                else:
                    print(f"  FAILED")
                    results["failed"].append(item)
            except Exception as e:
                print(f"  ERROR: {e}")
                _ss(page, f"error_{item.replace('/', '_')}")
                results["failed"].append(item)

            if explore:
                print("[EXPLORE] Stopping after first item.")
                break

        browser.close()

    print("\n--- Summary ---")
    print(f"  Updated  : {len(results['success'])}")
    print(f"  Failed   : {len(results['failed'])}")
    if results["failed"]:
        print(f"  Failed items: {results['failed']}")
    return results


def _navigate_to_manage_items(page):
    from playwright.sync_api import TimeoutError as PWTimeout

    print("[UI] Clicking Product Management tab...")
    try:
        page.locator("text=Product Management").first.click(timeout=10_000)
        page.wait_for_timeout(1500)
    except Exception as e:
        print(f"[UI] Product Management click failed: {e}")

    print("[UI] Clicking Product Information Management tile...")
    try:
        page.evaluate("""
            () => {
                const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
                let node;
                while ((node = walker.nextNode())) {
                    if (node.textContent.trim() === 'Product Information Management') {
                        let el = node.parentElement;
                        for (let i = 0; i < 5; i++) {
                            if (el && (el.tagName === 'A' || el.tagName === 'BUTTON' ||
                                el.getAttribute('role') === 'link' ||
                                el.getAttribute('role') === 'button')) {
                                el.click(); return;
                            }
                            if (el) el = el.parentElement;
                        }
                        node.parentElement.click();
                        return;
                    }
                }
            }
        """)
        page.wait_for_timeout(4000)

        try:
            page.wait_for_selector("a:has-text('Manage Items')", timeout=15_000)
        except Exception:
            print("[UI] Tasks panel collapsed, clicking tasks icon...")
            try:
                page.locator("[title='Tasks'], [aria-label='Tasks']").first.click(timeout=3000)
                page.wait_for_timeout(1500)
            except Exception:
                page.mouse.click(1421, 232)
                page.wait_for_timeout(1500)
            page.wait_for_selector("a:has-text('Manage Items')", timeout=15_000)

        print("[UI] PIM dashboard loaded.")
    except Exception as e:
        print(f"[UI] PIM navigation error: {e}")

    print("[UI] Clicking Manage Items...")
    try:
        page.locator("a:has-text('Manage Items')").first.click(timeout=10_000)
        page.wait_for_selector("text=Advanced Search", timeout=30_000)
        page.wait_for_timeout(2000)
    except Exception as e:
        print(f"[UI] Manage Items click failed: {e}")


def _update_one_item(page, item_number: str, new_abc: str, explore: bool = False) -> bool:
    """Full update flow: search -> click BUR item link -> update ABC category -> save -> back."""
    slug = item_number.replace("/", "_").replace(" ", "_")

    if not _search_item(page, item_number, slug):
        return False

    if not _click_bur_item_link(page, item_number, slug):
        return False

    success = _edit_item_abc_category(page, item_number, new_abc, slug, explore=explore)

    _return_to_manage_items(page, slug)
    return success


def _search_item(page, item_number: str, slug: str) -> bool:
    from playwright.sync_api import TimeoutError as PWTimeout

    print(f"  Searching for {item_number}...")
    try:
        item_input = page.locator(
            "input[aria-label=' Item'], input[aria-label='Item']"
        ).first
        item_input.click(click_count=3, timeout=5000)
        item_input.fill(item_number)
        page.keyboard.press("Tab")
        page.wait_for_timeout(300)

        page.locator("button:has-text('Search')").first.click(timeout=5000)
        page.wait_for_load_state("networkidle", timeout=20_000)
        page.wait_for_timeout(2000)
        _ss(page, f"search_{slug}")
        return True
    except PWTimeout as e:
        print(f"  Search timeout: {e}")
        _ss(page, f"search_err_{slug}")
        return False
    except Exception as e:
        print(f"  Search error: {e}")
        return False


def _click_bur_item_link(page, item_number: str, slug: str) -> bool:
    """Click the item number hyperlink in the BUR org row."""
    from playwright.sync_api import TimeoutError as PWTimeout

    # Strip parenthetical suffix: "60-66593-01 (ELEC/M.02.06C4)" -> "60-66593-01"
    base_item = item_number.split("(")[0].strip()

    try:
        page.wait_for_selector("text=BUR", timeout=10_000)
    except PWTimeout:
        print(f"  No BUR row visible after search.")
        _ss(page, f"no_bur_{slug}")
        return False

    result = page.evaluate("""
        ([base_item]) => {
            const rows = Array.from(document.querySelectorAll('tr'));
            for (const row of rows) {
                const rt = row.textContent;
                if (!rt.includes('BUR_MFG') && !rt.includes('3165_US_BUR')) continue;
                const rect = row.getBoundingClientRect();
                if (rect.height < 5 || rect.height > 200 || rect.width < 100) continue;

                const links = Array.from(row.querySelectorAll('a'));
                for (const link of links) {
                    const t = link.textContent.trim();
                    if (t === base_item || t === base_item.toUpperCase() || t.startsWith(base_item)) {
                        if (link.offsetParent !== null) {
                            const lr = link.getBoundingClientRect();
                            link.click();
                            return {found: true, text: t, x: Math.round(lr.x), y: Math.round(lr.y)};
                        }
                    }
                }
                // No exact link match — log what links exist in BUR row
                const allLinks = links.map(a => a.textContent.trim()).slice(0, 8);
                return {found: false, burRowText: rt.substring(0, 80), links: allLinks};
            }
            return {found: false, noBurRow: true};
        }
    """, [base_item])

    if not result or not result.get("found"):
        print(f"  Item link not found in BUR row. Debug: {result}")
        _ss(page, f"no_item_link_{slug}")
        return False

    print(f"  Clicked item link '{result.get('text')}' at ({result.get('x')}, {result.get('y')})")

    try:
        page.wait_for_load_state("networkidle", timeout=30_000)
        page.wait_for_timeout(2000)
    except Exception as e:
        print(f"  Load wait (continuing): {e}")

    return True


def _edit_item_abc_category(page, item_number: str, new_abc: str, slug: str, explore: bool = False) -> bool:
    """
    On the item detail page: the ABC assignment is a category in the ABC Inventory Catalog
    on the Categories tab. Change it from the current value to new_abc.

    Expected page state: item opened from BUR row link.  May be in view or edit mode.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    _ss(page, f"item_detail_{slug}")

    # ---- Enter edit mode if not already ----
    # Oracle ADF document.title always shows the parent page name, not "Edit Item".
    # Check body text and any visible element with text "Save" instead.
    in_edit = page.evaluate("""
        () => {
            if (document.body.innerText.includes('Edit Item:')) return true;
            const all = document.querySelectorAll('*');
            for (const el of all) {
                if (el.offsetParent !== null && el.childElementCount === 0 &&
                        el.textContent.trim() === 'Save') return true;
            }
            return false;
        }
    """)

    if not in_edit:
        print(f"  Clicking Edit to enter edit mode...")
        clicked = _js_click_visible(page, "Edit", exact=True)
        if not clicked:
            try:
                page.locator(
                    "button[title='Edit'], [aria-label='Edit'], button[id*='edit']:not([id*='Cancel'])"
                ).first.click(timeout=3000)
                clicked = True
            except Exception:
                pass
        if clicked:
            page.wait_for_timeout(2000)
            _ss(page, f"edit_mode_{slug}")
        else:
            print(f"  Warning: could not find Edit button — continuing anyway")

    # ---- Explore mode: dump the page and return ----
    if explore:
        info = page.evaluate("""
            () => ({
                url: window.location.href,
                title: document.title,
                tabs: Array.from(document.querySelectorAll('[role="tab"], a[href*="#"], .oj-tab'))
                    .filter(e => e.offsetParent !== null)
                    .map(e => e.textContent.trim()).filter(t => t),
                buttons: Array.from(document.querySelectorAll('button'))
                    .filter(e => e.offsetParent !== null)
                    .map(e => ({t: e.textContent.trim(), id: e.id, title: e.title}))
                    .filter(b => b.t.length > 0 && b.t.length < 60).slice(0, 30),
                text: document.body.innerText.substring(0, 2000)
            })
        """)
        print(f"  [EXPLORE] URL: {info.get('url','')[:120]}")
        print(f"  [EXPLORE] Title: {info.get('title','')}")
        print(f"  [EXPLORE] Tabs: {info.get('tabs',[])}")
        print(f"  [EXPLORE] Buttons: {info.get('buttons',[])[:25]}")
        print(f"  [EXPLORE] Text:\n{info.get('text','')[:1500]}")
        return False

    # ---- Navigate to Categories tab ----
    if not _click_tab(page, "Categories"):
        print(f"  Categories tab not found.")
        _ss(page, f"no_categories_tab_{slug}")
        return False

    page.wait_for_timeout(1500)
    _ss(page, f"categories_{slug}")

    # ---- Find and update the ABC Inventory Catalog row ----
    return _change_abc_category_row(page, new_abc, slug, item_number=item_number)


def _click_tab(page, tab_name: str) -> bool:
    """Click a tab by its exact text using Playwright mouse click."""
    coords = page.evaluate("""
        ([name]) => {
            const candidates = document.querySelectorAll(
                '[role="tab"], a, button, li, div, span'
            );
            for (let i = candidates.length - 1; i >= 0; i--) {
                const el = candidates[i];
                if (el.textContent.trim() === name && el.offsetParent !== null) {
                    const r = el.getBoundingClientRect();
                    if (r.height > 0 && r.width > 0) {
                        return {x: Math.round(r.x + r.width/2), y: Math.round(r.y + r.height/2)};
                    }
                }
            }
            return null;
        }
    """, [tab_name])
    if coords:
        page.mouse.click(coords["x"], coords["y"])
        return True
    # Fallback: try Playwright locator
    try:
        loc = page.locator(f"text='{tab_name}'").first
        if loc.is_visible(timeout=2000):
            loc.click()
            return True
    except Exception:
        pass
    return False


def _change_abc_category_row(page, new_abc: str, slug: str, item_number: str = "") -> bool:
    """
    On the Categories tab:
    Strategy REST: Oracle Fusion REST API via browser fetch() (preferred — bypasses UI restrictions)
    1. Try to select the ABC row via radio/checkbox input in the selection column
    2. If selected, Delete via toolbar Delete button
    3. Add Row via toolbar '+' button
    4. Fill new row's Catalog and Category fields
    """

    def _get_delete_state():
        """Check if the Delete toolbar button is enabled. Checks both img title and anchor title."""
        return page.evaluate("""
            () => {
                // Check anchors/buttons in toolbar area (cy 200-700)
                for (const el of document.querySelectorAll('a, button')) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    const cy = r.y + r.height/2;
                    if (cy < 200 || cy > 700) continue;
                    if (r.width < 1 || r.height < 1) continue;
                    const t = (el.title + ' ' + (el.getAttribute('aria-label')||'')).toLowerCase();
                    let isDelete = t.includes('delete');
                    if (!isDelete) {
                        // Check img children
                        for (const img of el.querySelectorAll('img')) {
                            const it = (img.title+' '+img.alt+' '+(img.getAttribute('aria-label')||'')).toLowerCase();
                            if (it.includes('delete')) { isDelete = true; break; }
                        }
                    }
                    if (!isDelete) continue;
                    const d = el.getAttribute('aria-disabled');
                    return d === 'true' ? 'disabled' : 'enabled';
                }
                return 'not_found';
            }
        """)

    def _get_toolbar_icon(title_text):
        """Return {cx, cy, disabled} for a visible toolbar button matching title_text.
        Checks anchor/button title AND img title/alt."""
        return page.evaluate("""
            ([ttl]) => {
                const ttlLower = ttl.toLowerCase();
                for (const el of document.querySelectorAll('a, button')) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    const cy = r.y + r.height/2;
                    if (cy < 200 || cy > 700) continue;
                    if (r.width < 1 || r.height < 1) continue;
                    // Check element's own title/aria-label
                    const t = (el.title + ' ' + (el.getAttribute('aria-label')||'')).toLowerCase();
                    if (t.includes(ttlLower))
                        return {cx: Math.round(r.x+r.width/2), cy: Math.round(cy),
                                disabled: el.getAttribute('aria-disabled'), src: 'el'};
                    // Check img children
                    for (const img of el.querySelectorAll('img')) {
                        const it = (img.title+' '+img.alt+' '+(img.getAttribute('aria-label')||'')).toLowerCase();
                        if (it.includes(ttlLower))
                            return {cx: Math.round(r.x+r.width/2), cy: Math.round(cy),
                                    disabled: el.getAttribute('aria-disabled'), src: 'img'};
                    }
                }
                return null;
            }
        """, [title_text])

    def _open_table_actions_menu():
        """Click the table-toolbar Actions button (cy 200-700, not item-level at cy ~149)."""
        coords = page.evaluate("""
            () => {
                const els = Array.from(document.querySelectorAll('a, button, span, div'));
                for (let i = els.length - 1; i >= 0; i--) {
                    const el = els[i];
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (t !== 'Actions' && t !== 'Actions ') continue;
                    const r = el.getBoundingClientRect();
                    const cy = r.y + r.height / 2;
                    if (cy > 200 && cy < 700 && r.height > 0)
                        return {cx: Math.round(r.x + r.width/2), cy: Math.round(cy)};
                }
                return null;
            }
        """)
        if coords:
            page.mouse.click(coords["cx"], coords["cy"])
            page.wait_for_timeout(800)
            return True
        return False

    def _click_menu_item(labels):
        """Click the first visible, ENABLED MENU ITEM matching any of labels (role=menuitem preferred)."""
        for label in labels:
            coords = page.evaluate("""
                ([lbl]) => {
                    // Prefer elements with menuitem role or inside a popup/menu container
                    const priority = document.querySelectorAll(
                        '[role="menuitem"], [role="option"], td.af_commandMenuItem, ' +
                        'li.af_commandMenuItem, .af_menuBar_item, .af_menu_popup td'
                    );
                    for (const el of priority) {
                        if (!el.offsetParent) continue;
                        if (el.textContent.trim() !== lbl) continue;
                        // Skip disabled items
                        if (el.getAttribute('aria-disabled') === 'true') continue;
                        if (el.classList.contains('p_AFDisabled')) continue;
                        const r = el.getBoundingClientRect();
                        if (r.height > 4)
                            return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2),
                                    source: 'role', disabled: false};
                    }
                    // Dump what we found (including disabled) for diagnostics
                    const found = [];
                    for (const el of document.querySelectorAll('[role="menuitem"], [role="option"], td.af_commandMenuItem')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim();
                        if (!t) continue;
                        const r = el.getBoundingClientRect();
                        found.push({t: t.slice(0,25), disabled: el.getAttribute('aria-disabled'),
                                    cls: el.className.slice(0,30)});
                    }
                    // Fallback: any visible element in dropdown-y-range (not in main table area)
                    const all = Array.from(document.querySelectorAll('a, li, span, td'));
                    for (let i = all.length-1; i >= 0; i--) {
                        const el = all[i];
                        if (!el.offsetParent) continue;
                        if (el.textContent.trim() !== lbl) continue;
                        if (el.getAttribute('aria-disabled') === 'true') continue;
                        if (el.classList.contains('p_AFDisabled')) continue;
                        const r = el.getBoundingClientRect();
                        if (r.height > 4 && r.height < 40)
                            return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2),
                                    source: 'fallback', menuItems: found};
                    }
                    return {found: false, menuItems: found};
                }
            """, [label])
            if coords and coords.get("cx"):
                print(f"  Clicking menu item '{label}' at ({coords['cx']}, {coords['cy']}) [{coords.get('source','')}]...")
                if coords.get("menuItems"):
                    print(f"    Menu items visible: {coords['menuItems']}")
                page.mouse.click(coords["cx"], coords["cy"])
                page.wait_for_timeout(1000)
                return True
            elif coords and coords.get("menuItems"):
                print(f"  Menu item '{label}' not clickable. Menu items: {coords['menuItems']}")
        return False

    def _abc_row_exists():
        return page.evaluate("""
            () => {
                for (const row of document.querySelectorAll('tr')) {
                    const rt = row.textContent;
                    if (!rt.includes('ABC Invent') && !rt.toLowerCase().includes('abc inv')) continue;
                    const rect = row.getBoundingClientRect();
                    if (rect.height > 4 && rect.width > 100) return true;
                }
                return false;
            }
        """)

    # ---- 1. Find the ABC Inventory row ----
    # First: filter the Catalogs dropdown to "ABC Inventory" — this may enable Add/Delete
    print(f"  Filtering table to 'ABC Inventory' catalog...")
    catalog_filter_result = page.evaluate("""
        () => {
            // Find the Catalogs SELECT dropdown in the toolbar
            for (const sel of document.querySelectorAll('select')) {
                if (!sel.offsetParent) continue;
                // Check if it has an ABC Inventory option
                const opts = Array.from(sel.options).map(o => ({val: o.value, text: o.text.trim()}));
                const abc_opt = opts.find(o => o.text.toLowerCase().includes('abc') || o.text.toLowerCase().includes('abc inv'));
                if (abc_opt) {
                    sel.value = abc_opt.val;
                    sel.dispatchEvent(new Event('change', {bubbles: true}));
                    return {found: true, selected: abc_opt.text, opts: opts.slice(0,10)};
                }
            }
            // Fallback: dump all selects
            const allSels = Array.from(document.querySelectorAll('select')).filter(s => s.offsetParent).map(s => {
                const r = s.getBoundingClientRect();
                return {id: s.id, opts: Array.from(s.options).map(o => o.text.trim()).slice(0,8),
                        cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
            });
            return {found: false, selects: allSels};
        }
    """)
    print(f"  Catalog filter: {catalog_filter_result}")
    if catalog_filter_result.get("found"):
        page.wait_for_timeout(1200)
        _ss(page, f"catalog_filtered_{slug}")

    row_info = page.evaluate("""
        () => {
            const rows = Array.from(document.querySelectorAll('tr'));
            for (const row of rows) {
                const rt = row.textContent;
                if (!rt.includes('ABC Invent') && !rt.toLowerCase().includes('abc inv')) continue;
                const rect = row.getBoundingClientRect();
                if (rect.height < 4 || rect.height > 150 || rect.width < 100) continue;
                const cells = Array.from(row.querySelectorAll('td'))
                    .map(c => {
                        const r = c.getBoundingClientRect();
                        return {text: c.textContent.trim().substring(0, 50),
                            cx: Math.round(r.x + r.width/2), cy: Math.round(r.y + r.height/2),
                            w: Math.round(r.width), h: Math.round(r.height)};
                    })
                    .filter(c => c.w > 0 && c.h > 0);
                return {found: true, rowY: Math.round(rect.y + rect.height/2),
                    rowText: rt.trim().substring(0, 120), cells: cells};
            }
            return {found: false};
        }
    """)

    if not row_info.get("found"):
        print(f"  ABC Inventory Catalog row not found on Categories tab.")
        _ss(page, f"no_abc_row_{slug}")
        return False

    row_y = row_info["rowY"]
    cells = row_info.get("cells", [])
    print(f"  Found ABC row at y={row_y}: {row_info.get('rowText','')[:80]}")

    # data cells: skip narrow indicator (w<20) AND wide container td (w>500)
    data_cells = [c for c in cells if 20 < c["w"] < 500]
    print(f"  data_cells: {[(c['cx'], c['w'], c['text'][:15]) for c in data_cells]}")

    # Dump toolbar buttons for diagnostics (include aria-disabled)
    toolbar_btns = page.evaluate("""
        () => Array.from(document.querySelectorAll('a, button')).filter(el => {
            if (!el.offsetParent) return false;
            const r = el.getBoundingClientRect();
            const cy = r.y + r.height/2;
            return cy > 200 && cy < 700 && r.width > 4 && r.height > 4;
        }).map(el => {
            const r = el.getBoundingClientRect();
            const imgs = Array.from(el.querySelectorAll('img')).map(i =>
                (i.title+'/'+i.alt).substring(0,20));
            return {tag: el.tagName, cx: Math.round(r.x+r.width/2),
                    cy: Math.round(r.y+r.height/2),
                    t: el.title.substring(0,25), aria: (el.getAttribute('aria-label')||'').substring(0,20),
                    txt: el.textContent.trim().substring(0,20), imgs: imgs,
                    disabled: el.getAttribute('aria-disabled')};
        }).filter(b => b.t || b.aria || b.imgs.length || b.txt).slice(0, 30)
    """)
    print(f"  Toolbar/buttons: {toolbar_btns}")

    # Extract Add Row and Delete button positions from the dump
    def _img_matches(imgs, keyword):
        kw = keyword.lower()
        return any(kw in img.lower() for img in imgs)

    tb_add = next((b for b in toolbar_btns if _img_matches(b.get("imgs", []), "add row")), None)
    tb_del = next((b for b in toolbar_btns if _img_matches(b.get("imgs", []), "delete")), None)
    print(f"  Toolbar Add Row: {tb_add}")
    print(f"  Toolbar Delete:  {tb_del}")

    deleted_ok = False

    # ---- Diagnostic: dump ABC row DOM attributes to understand why Delete is disabled ----
    row_dom = page.evaluate("""
        ([rowY]) => {
            for (const row of document.querySelectorAll('tr')) {
                if (!row.textContent.includes('ABC Invent')) continue;
                const rect = row.getBoundingClientRect();
                if (Math.abs(rect.y+rect.height/2 - rowY) > 30) continue;
                const tds = Array.from(row.querySelectorAll('td')).map(td => ({
                    cls: td.className.slice(0,40),
                    attrs: Object.fromEntries(Array.from(td.attributes).map(a => [a.name, a.value.slice(0,30)])),
                    w: Math.round(td.getBoundingClientRect().width)
                })).slice(0, 5);
                return {
                    id: row.id, cls: row.className.slice(0,60),
                    allAttrs: Object.fromEntries(Array.from(row.attributes).map(a => [a.name, a.value.slice(0,40)])),
                    tds: tds
                };
            }
            return null;
        }
    """, [row_y])
    print(f"  ABC row DOM: {row_dom}")

    # ---- Hover over the ABC row to trigger hover-state buttons ----
    if data_cells:
        page.mouse.move(data_cells[0]["cx"], row_y)
        page.wait_for_timeout(600)
        hover_btns = page.evaluate("""
            ([rowY]) => {
                const results = [];
                for (const el of document.querySelectorAll('a, button, span[onclick], img[onclick]')) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    const ey = r.y + r.height/2;
                    if (Math.abs(ey - rowY) > 40) continue;
                    if (r.width < 4 || r.height < 4) continue;
                    results.push({tag: el.tagName, cx: Math.round(r.x+r.width/2), cy: Math.round(ey),
                                  t: el.title||'', txt: el.textContent.trim().slice(0,20),
                                  aria: (el.getAttribute('aria-label')||'').slice(0,20),
                                  cls: el.className.slice(0,30)});
                }
                return results;
            }
        """, [row_y])
        print(f"  Hover buttons at row y={row_y}: {hover_btns}")

    # ---- Real Playwright click at exact selection indicator (cx=49) ----
    print(f"  Strategy 0: Real mouse click on selection indicator (49, {row_y})...")
    page.mouse.click(49, row_y)
    page.wait_for_timeout(700)
    # Check row state after real click
    row_after = page.evaluate("""
        ([rowY]) => {
            for (const row of document.querySelectorAll('tr')) {
                if (!row.textContent.includes('ABC Invent')) continue;
                const rect = row.getBoundingClientRect();
                if (Math.abs(rect.y+rect.height/2 - rowY) > 30) continue;
                return {cls: row.className.slice(0,60),
                        allAttrs: Object.fromEntries(Array.from(row.attributes).map(a => [a.name, a.value.slice(0,40)]))};
            }
            return null;
        }
    """, [row_y])
    print(f"  Row state after real click at (49, {row_y}): {row_after}")
    del_state0 = _get_delete_state()
    print(f"  Delete state after selection indicator click: {del_state0}")
    if del_state0 == "enabled":
        del_icon = _get_toolbar_icon("Delete")
        if del_icon and del_icon.get("disabled") != "true":
            page.mouse.click(del_icon["cx"], del_icon["cy"])
            page.wait_for_timeout(1000)
            deleted_ok = not _abc_row_exists()
            print(f"  After Delete (post-indicator-click): row removed = {deleted_ok}")

    # ---- Strategy 0b: Real Playwright click on Category cell — check for LOV input ----
    # If the cell is inline-editable (LOV field), we can change the value directly without
    # delete+add. Oracle ADF responds to real mouse events differently than JS element.click().
    if not deleted_ok:
        for cell_idx, field_label in [(0, "Category"), (3, "Category Code")]:
            if cell_idx >= len(data_cells):
                continue
            cell = data_cells[cell_idx]
            print(f"  Strategy 0b: Real mouse click on {field_label} cell ({cell['cx']}, {row_y})...")
            page.mouse.click(cell["cx"], row_y)
            page.wait_for_timeout(800)

            lov_input = page.evaluate("""
                ([cx, cy]) => {
                    for (const el of document.querySelectorAll('input:not([type=hidden])')) {
                        if (!el.offsetParent) continue;
                        const r = el.getBoundingClientRect();
                        if (Math.abs(r.y+r.height/2-cy) < 35 && Math.abs(r.x+r.width/2-cx) < 300)
                            return {found: true, cx: Math.round(r.x+r.width/2),
                                    cy: Math.round(r.y+r.height/2),
                                    val: el.value||'', id: el.id.slice(-40)};
                    }
                    return {found: false};
                }
            """, [cell["cx"], row_y])
            print(f"    LOV input after {field_label} click: {lov_input}")

            if lov_input.get("found"):
                print(f"  Inline edit IS possible on {field_label} — changing to '{new_abc}'...")
                page.mouse.click(lov_input["cx"], lov_input["cy"])
                page.wait_for_timeout(200)
                page.keyboard.press("Control+a")
                page.keyboard.type(new_abc, delay=80)
                page.wait_for_timeout(1400)
                sugg = page.evaluate("""
                    ([val]) => {
                        for (const sel of ['li[role="option"]','div[role="option"]',
                                           'tr[data-afr-suggestrow]','.af_popup li',
                                           '[id*="lov"] li','li']) {
                            for (const c of document.querySelectorAll(sel)) {
                                if (!c.offsetParent) continue;
                                const t = c.textContent.trim();
                                const r = c.getBoundingClientRect();
                                if (r.height > 4 && (t.startsWith(val) || t.includes(val)))
                                    return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2), text:t.slice(0,40)};
                            }
                        }
                        return null;
                    }
                """, [new_abc])
                if sugg:
                    print(f"    Clicking suggestion '{sugg['text']}'...")
                    page.mouse.click(sugg["cx"], sugg["cy"])
                    page.wait_for_timeout(600)
                else:
                    page.keyboard.press("Tab")
                    print(f"    No suggestion — pressed Tab")
                _ss(page, f"inline_edit_done_{slug}")
                return _save_item(page, slug)

            # Also check if Delete became enabled after this real click
            del_state_0b = _get_delete_state()
            if del_state_0b == "enabled":
                del_icon = _get_toolbar_icon("Delete")
                if del_icon and del_icon.get("disabled") != "true":
                    page.mouse.click(del_icon["cx"], del_icon["cy"])
                    page.wait_for_timeout(1000)
                    deleted_ok = not _abc_row_exists()
                    print(f"  Delete enabled after {field_label} click: row removed = {deleted_ok}")
                    if deleted_ok:
                        break

    # ---- Strategy 0c: Right-click for context menu Delete ----
    if not deleted_ok and data_cells:
        print(f"  Strategy 0c: Right-click on ABC row for context menu...")
        page.mouse.click(data_cells[0]["cx"], row_y, button="right")
        page.wait_for_timeout(800)
        _ss(page, f"context_menu_{slug}")
        ctx_clicked = _click_menu_item(["Delete", "Delete Row", "Remove"])
        if ctx_clicked:
            page.wait_for_timeout(800)
            deleted_ok = not _abc_row_exists()
            print(f"  After right-click Delete: row removed = {deleted_ok}")
        if not ctx_clicked or not deleted_ok:
            page.keyboard.press("Escape")
            page.wait_for_timeout(300)

    # ---- Strategy 0d: Double-click on Category / Category Code cell ----
    # Oracle ADF often uses double-click to enter cell edit mode (inline LOV edit)
    if not deleted_ok and data_cells:
        for cell_idx, field_label in [(0, "Category"), (3, "Category Code")]:
            if cell_idx >= len(data_cells):
                continue
            cell = data_cells[cell_idx]
            print(f"  Strategy 0d: Double-click on {field_label} cell ({cell['cx']}, {row_y})...")
            page.mouse.dblclick(cell["cx"], row_y)
            page.wait_for_timeout(900)

            # Broad scan — any input or LOV that appeared anywhere on the page
            lov_input2 = page.evaluate("""
                ([cx, cy]) => {
                    // Scan all inputs for any that appeared near the row
                    for (const el of document.querySelectorAll(
                        'input:not([type=hidden]):not([aria-label="Start Date"]):not([aria-label="End Date"])'
                    )) {
                        if (!el.offsetParent) continue;
                        const r = el.getBoundingClientRect();
                        const ey = r.y + r.height/2;
                        if (ey < 580) continue;  // skip toolbar/search inputs
                        if (r.width < 20) continue;
                        return {found: true, cx: Math.round(r.x+r.width/2), cy: Math.round(ey),
                                val: el.value||'', id: el.id.slice(-40), aria: el.getAttribute('aria-label')||''};
                    }
                    return {found: false};
                }
            """, [cell["cx"], row_y])
            print(f"    Input after double-click: {lov_input2}")

            if lov_input2.get("found"):
                print(f"  Double-click opened editable input! Changing to '{new_abc}'...")
                page.mouse.click(lov_input2["cx"], lov_input2["cy"])
                page.wait_for_timeout(200)
                page.keyboard.press("Control+a")
                page.keyboard.type(new_abc, delay=80)
                page.wait_for_timeout(1400)
                sugg = page.evaluate("""
                    ([val]) => {
                        for (const sel of ['li[role="option"]','div[role="option"]',
                                           'tr[data-afr-suggestrow]','.af_popup li',
                                           '[id*="lov"] li','li']) {
                            for (const c of document.querySelectorAll(sel)) {
                                if (!c.offsetParent) continue;
                                const t = c.textContent.trim();
                                const r = c.getBoundingClientRect();
                                if (r.height > 4 && (t.startsWith(val) || t.includes(val)))
                                    return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2), text:t.slice(0,40)};
                            }
                        }
                        return null;
                    }
                """, [new_abc])
                if sugg:
                    print(f"    Clicking suggestion '{sugg['text']}'...")
                    page.mouse.click(sugg["cx"], sugg["cy"])
                    page.wait_for_timeout(600)
                else:
                    page.keyboard.press("Tab")
                    print(f"    No suggestion — Tab")
                _ss(page, f"dblclick_edit_done_{slug}")
                return _save_item(page, slug)

            # Check if Delete became enabled
            del_st_0d = _get_delete_state()
            if del_st_0d == "enabled":
                del_icon = _get_toolbar_icon("Delete")
                if del_icon and del_icon.get("disabled") != "true":
                    page.mouse.click(del_icon["cx"], del_icon["cy"])
                    page.wait_for_timeout(1000)
                    deleted_ok = not _abc_row_exists()
                    if deleted_ok:
                        break

    # ---- Strategy 0e: F2 key after clicking Category cell ----
    if not deleted_ok and data_cells:
        print(f"  Strategy 0e: Click Category cell then press F2...")
        page.mouse.click(data_cells[0]["cx"], row_y)
        page.wait_for_timeout(400)
        page.keyboard.press("F2")
        page.wait_for_timeout(700)

        lov_input3 = page.evaluate("""
            ([cy]) => {
                for (const el of document.querySelectorAll(
                    'input:not([type=hidden]):not([aria-label="Start Date"]):not([aria-label="End Date"])'
                )) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    const ey = r.y + r.height/2;
                    if (ey < 580 || r.width < 20) continue;
                    return {found: true, cx: Math.round(r.x+r.width/2), cy: Math.round(ey),
                            val: el.value||'', id: el.id.slice(-40)};
                }
                return {found: false};
            }
        """, [row_y])
        print(f"  Input after F2: {lov_input3}")
        if lov_input3.get("found"):
            page.mouse.click(lov_input3["cx"], lov_input3["cy"])
            page.wait_for_timeout(200)
            page.keyboard.press("Control+a")
            page.keyboard.type(new_abc, delay=80)
            page.wait_for_timeout(1400)
            page.keyboard.press("Tab")
            _ss(page, f"f2_edit_done_{slug}")
            return _save_item(page, slug)

    # ---- Strategy 0f: Click Hierarchy icon (xko) — may open an edit/detail popup ----
    if not deleted_ok:
        xko_btn = page.evaluate("""
            ([rowY]) => {
                for (const el of document.querySelectorAll('a.xko')) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    if (Math.abs(r.y+r.height/2 - rowY) > 40) continue;
                    return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                }
                return null;
            }
        """, [row_y])
        if xko_btn:
            print(f"  Strategy 0f: Clicking Hierarchy/xko icon at ({xko_btn['cx']}, {xko_btn['cy']})...")
            page.mouse.click(xko_btn["cx"], xko_btn["cy"])
            page.wait_for_timeout(1200)
            _ss(page, f"xko_click_{slug}")
            # Check if a dialog appeared with edit options
            dialog_info = page.evaluate("""
                () => {
                    const dialogs = document.querySelectorAll('[role="dialog"], .af_popup, .af_dialog');
                    return Array.from(dialogs).filter(d => d.offsetParent).map(d => ({
                        role: d.getAttribute('role')||'',
                        cls: d.className.slice(0,40),
                        text: d.textContent.trim().slice(0,100)
                    }));
                }
            """)
            print(f"  Dialog after xko click: {dialog_info}")
            # If a dialog appeared, close it for now (we just needed to know it exists)
            if dialog_info:
                page.keyboard.press("Escape")
                page.wait_for_timeout(400)

    # ---- Strategy 0g: Set End Date to today, then check if Add Row becomes enabled ----
    # Hypothesis: Oracle requires the old assignment to have an End Date before allowing
    # a new one to be added. Setting End Date may enable Add Row.
    if not deleted_ok:
        import datetime
        today_str = datetime.date.today().strftime("%-m/%-d/%y") if not __import__('sys').platform.startswith('win') \
                    else datetime.date.today().strftime("%#m/%#d/%y")
        print(f"  Strategy 0g: Setting End Date on ABC row to {today_str} (may enable Add Row)...")
        # Click the End Date cell (rightmost data cell)
        end_date_cx = data_cells[-1]["cx"] if data_cells else 1438
        page.mouse.click(end_date_cx, row_y)
        page.wait_for_timeout(500)
        # Check if End Date input is now focused
        end_date_input = page.evaluate("""
            ([cx, cy]) => {
                for (const el of document.querySelectorAll('input[aria-label="End Date"]')) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    return {found: true, cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2),
                            val: el.value||''};
                }
                return {found: false};
            }
        """, [end_date_cx, row_y])
        print(f"  End Date input: {end_date_input}")

        if end_date_input.get("found"):
            page.mouse.click(end_date_input["cx"], end_date_input["cy"])
            page.wait_for_timeout(200)
            page.keyboard.press("Control+a")
            page.keyboard.type(today_str, delay=60)
            page.keyboard.press("Tab")
            page.wait_for_timeout(800)
            _ss(page, f"end_date_set_{slug}")

            # Re-check if Add Row is now enabled
            add_row_state = page.evaluate("""
                () => {
                    for (const el of document.querySelectorAll('a, button')) {
                        if (!el.offsetParent) continue;
                        for (const img of el.querySelectorAll('img')) {
                            const alt = (img.alt||img.title||'').toLowerCase();
                            if (alt.includes('add row'))
                                return {disabled: el.getAttribute('aria-disabled'), cls: el.className.slice(0,40)};
                        }
                    }
                    return null;
                }
            """)
            print(f"  Add Row state after setting End Date: {add_row_state}")

            if add_row_state and add_row_state.get("disabled") != "true":
                print(f"  Add Row IS NOW ENABLED after setting End Date!")
                # Re-fetch toolbar buttons and proceed to Add Row
                # (Fall through to the Add Row section below)
            else:
                # Also check if Delete is now enabled (might allow us to delete expired row)
                del_after_enddate = _get_delete_state()
                print(f"  Delete state after setting End Date: {del_after_enddate}")

    # ---- Strategy 1: Delete already enabled ----
    if tb_del and tb_del.get("disabled") != "true":
        print(f"  Toolbar Delete is ENABLED — clicking directly...")
        page.mouse.click(tb_del["cx"], tb_del["cy"])
        page.wait_for_timeout(1000)
        deleted_ok = not _abc_row_exists()
        print(f"  After direct Delete: row removed = {deleted_ok}")

    # ---- Strategy 2: Click row radio/checkbox selection input via JS ----
    if not deleted_ok:
        sel_result = page.evaluate("""
            ([rowY]) => {
                // Find input[type=radio/checkbox] near the ABC row y
                for (const inp of document.querySelectorAll('input[type="radio"], input[type="checkbox"]')) {
                    if (!inp.offsetParent) continue;
                    const r = inp.getBoundingClientRect();
                    const cy = r.y + r.height/2;
                    if (Math.abs(cy - rowY) < 30) {
                        inp.click();
                        return {found: true, type: inp.type, cx: Math.round(r.x+r.width/2), cy: Math.round(cy)};
                    }
                }
                // Fallback: click the narrow first td (selection indicator) of the row
                const rows = document.querySelectorAll('tr');
                for (const row of rows) {
                    if (!row.textContent.includes('ABC Invent')) continue;
                    const rect = row.getBoundingClientRect();
                    if (Math.abs(rect.y+rect.height/2 - rowY) > 30) continue;
                    const firstTd = row.querySelector('td');
                    if (firstTd) {
                        firstTd.click();
                        const fr = firstTd.getBoundingClientRect();
                        return {found: true, type: 'td', cx: Math.round(fr.x+fr.width/2), cy: Math.round(fr.y+fr.height/2)};
                    }
                }
                return {found: false};
            }
        """, [row_y])
        print(f"  Selection indicator click: {sel_result}")
        page.wait_for_timeout(700)
        del_state2 = _get_delete_state()
        print(f"  Delete state after selection click: {del_state2}")
        if del_state2 == "enabled":
            del_icon = _get_toolbar_icon("Delete")
            if del_icon and del_icon.get("disabled") != "true":
                page.mouse.click(del_icon["cx"], del_icon["cy"])
                page.wait_for_timeout(1000)
                deleted_ok = not _abc_row_exists()
                print(f"  After Delete (post-selection-click): row removed = {deleted_ok}")

    # ---- Strategy 3: Try inline edit of Category cell via JS element.click() ----
    if not deleted_ok:
        cat_click = page.evaluate("""
            ([rowY]) => {
                const rows = document.querySelectorAll('tr');
                for (const row of rows) {
                    if (!row.textContent.includes('ABC Invent')) continue;
                    const rect = row.getBoundingClientRect();
                    if (Math.abs(rect.y+rect.height/2 - rowY) > 30) continue;
                    // Find first data td (skip w<20 selection col and w>500 container)
                    const tds = Array.from(row.querySelectorAll('td'));
                    for (const td of tds) {
                        const r = td.getBoundingClientRect();
                        if (r.width > 20 && r.width < 500) {
                            td.click();
                            return {found: true, cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2),
                                    w: Math.round(r.width), text: td.textContent.trim().substring(0,20)};
                        }
                    }
                }
                return {found: false};
            }
        """, [row_y])
        print(f"  Category cell JS click: {cat_click}")
        page.wait_for_timeout(700)
        _ss(page, f"after_cat_click_{slug}")

        if cat_click.get("found"):
            cx_clicked = cat_click["cx"]
            cy_clicked = cat_click["cy"]

            # Check toolbar state immediately after the cell click
            # (row might now be selected, enabling Delete/Add Row)
            tb_after_click = page.evaluate("""
                () => {
                    const result = {};
                    for (const el of document.querySelectorAll('a, button')) {
                        if (!el.offsetParent) continue;
                        const r = el.getBoundingClientRect();
                        const cy = r.y + r.height/2;
                        if (cy < 550 || cy > 650) continue;
                        for (const img of el.querySelectorAll('img')) {
                            const alt = (img.alt || img.title || '').toLowerCase();
                            if (alt.includes('add row')) result.addRow = el.getAttribute('aria-disabled');
                            if (alt === 'delete') result.del = el.getAttribute('aria-disabled');
                        }
                    }
                    return result;
                }
            """)
            print(f"  Toolbar after cell click: {tb_after_click}")

            # If Delete is now enabled, delete the row
            if tb_after_click.get("del") != "true" and tb_after_click.get("del") is not None:
                tb_del2 = next((b for b in toolbar_btns if _img_matches(b.get("imgs",[]),"delete")), None)
                if tb_del2:
                    page.mouse.click(tb_del2["cx"], tb_del2["cy"])
                    page.wait_for_timeout(1000)
                    deleted_ok = not _abc_row_exists()
                    print(f"  After Delete (post cell-click): row removed = {deleted_ok}")

            # If Add Row is now enabled, skip straight to adding
            if not deleted_ok and tb_after_click.get("addRow") != "true" and tb_after_click.get("addRow") is not None:
                tb_add_now = next((b for b in toolbar_btns if _img_matches(b.get("imgs",[]),"add row")), None)
                if tb_add_now:
                    print(f"  Add Row enabled after cell click — USING IT DIRECTLY for delete+add flow")
                    # Will fall through to Add Row section below

            # Check for editable input near the clicked cell (inline edit approach)
            cat_input = page.evaluate("""
                ([cx, cy]) => {
                    const selectors = [
                        'input:not([type=hidden])',
                        '[class*="af_inputListOfValues"]',
                        '[class*="af_inputText"]',
                        'textarea'
                    ];
                    for (const sel of selectors) {
                        for (const el of document.querySelectorAll(sel)) {
                            if (!el.offsetParent) continue;
                            const r = el.getBoundingClientRect();
                            if (Math.abs(r.y+r.height/2 - cy) < 40 && Math.abs(r.x+r.width/2 - cx) < 300)
                                return {found: true, val: (el.value||el.textContent||'').substring(0,20),
                                        cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2),
                                        tag: el.tagName, id: el.id.slice(-40)};
                        }
                    }
                    return {found: false};
                }
            """, [cx_clicked, cy_clicked])
            print(f"  Input after Category cell JS click: {cat_input}")

            if cat_input.get("found"):
                print(f"  Category cell IS editable — direct inline edit to '{new_abc}'...")
                page.mouse.click(cat_input["cx"], cat_input["cy"])
                page.wait_for_timeout(200)
                page.keyboard.press("Control+a")
                page.keyboard.type(new_abc, delay=80)
                page.wait_for_timeout(1400)
                sugg = page.evaluate("""
                    ([val]) => {
                        for (const sel of ['li[role="option"]','div[role="option"]','tr[data-afr-suggestrow]',
                                           '.af_popup li','[id*="lov"] li','li']) {
                            for (const c of document.querySelectorAll(sel)) {
                                if (!c.offsetParent) continue;
                                const t = c.textContent.trim();
                                const r = c.getBoundingClientRect();
                                if (r.height > 4 && (t.startsWith(val) || t.includes(val)))
                                    return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2), text:t.slice(0,40)};
                            }
                        }
                        return null;
                    }
                """, [new_abc])
                if sugg:
                    print(f"    Clicking suggestion '{sugg['text']}'...")
                    page.mouse.click(sugg["cx"], sugg["cy"])
                    page.wait_for_timeout(600)
                else:
                    page.keyboard.press("Tab")
                _ss(page, f"inline_edit_done_{slug}")
                return _save_item(page, slug)

    # ---- Strategy 4: Escape × 2 → check Delete ----
    if not deleted_ok:
        page.keyboard.press("Escape")
        page.wait_for_timeout(400)
        page.keyboard.press("Escape")
        page.wait_for_timeout(600)
        del_state3 = _get_delete_state()
        print(f"  Delete state after 2× Escape: {del_state3}")
        if del_state3 == "enabled":
            del_icon = _get_toolbar_icon("Delete")
            if del_icon and del_icon.get("disabled") != "true":
                page.mouse.click(del_icon["cx"], del_icon["cy"])
                page.wait_for_timeout(1000)
                deleted_ok = not _abc_row_exists()

    # ---- Strategy 5: Actions > Delete ----
    if not deleted_ok:
        if _open_table_actions_menu():
            _ss(page, f"actions_menu_{slug}")
            _click_menu_item(["Delete", "Delete Row"])
            page.wait_for_timeout(800)
            deleted_ok = not _abc_row_exists()
            print(f"  After Actions > Delete: row removed = {deleted_ok}")
        if not deleted_ok:
            page.keyboard.press("Escape")
            page.wait_for_timeout(400)

    # Dismiss confirmation dialog if any
    for conf in ["Yes", "OK"]:
        try:
            if page.locator(f"button:has-text('{conf}')").first.is_visible(timeout=700):
                page.locator(f"button:has-text('{conf}')").first.click()
                page.wait_for_timeout(500)
                print(f"  Confirmed via '{conf}'")
                break
        except Exception:
            pass

    _ss(page, f"after_delete_{slug}")
    deleted_ok = not _abc_row_exists()
    print(f"  ABC row present after delete attempts: {not deleted_ok}")
    if not deleted_ok:
        print(f"  WARNING: delete failed — will attempt Add Row anyway.")

    # ---- Add Row ----
    # Re-fetch toolbar state (may have changed after delete attempts)
    toolbar_btns2 = page.evaluate("""
        () => Array.from(document.querySelectorAll('a, button')).filter(el => {
            if (!el.offsetParent) return false;
            const r = el.getBoundingClientRect();
            const cy = r.y + r.height/2;
            return cy > 200 && cy < 700 && r.width > 4 && r.height > 4;
        }).map(el => {
            const imgs = Array.from(el.querySelectorAll('img')).map(i =>
                (i.title+'/'+i.alt).substring(0,30));
            const r = el.getBoundingClientRect();
            return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2),
                    imgs: imgs, disabled: el.getAttribute('aria-disabled'),
                    t: el.title, txt: el.textContent.trim().substring(0,20)};
        })
    """)
    tb_add2 = next((b for b in toolbar_btns2 if _img_matches(b.get("imgs", []), "add row")), None)
    print(f"  Toolbar Add Row (post-delete): {tb_add2}")

    add_clicked = False
    if tb_add2 and tb_add2.get("disabled") != "true":
        print(f"  Clicking Add Row toolbar button at ({tb_add2['cx']}, {tb_add2['cy']})...")
        page.mouse.click(tb_add2["cx"], tb_add2["cy"])
        page.wait_for_timeout(1500)
        add_clicked = True
    else:
        # Open Actions menu and find "Add Row" dropdown item
        # Key: must find the DROPDOWN item (cy > toolbar cy + 15), not the toolbar button
        actions_cy = next((b["cy"] for b in toolbar_btns2 if b.get("txt") in ("Actions", "Actions ")), 605)
        print(f"  Actions button at cy={actions_cy} — restoring cell-focus then opening menu for Add Row...")
        # CRITICAL: restore cell-focus by clicking a row cell — Actions dropdown only shows
        # items when the table has a focused cell. Without this, only toolbar buttons appear.
        restore_focus_result = page.evaluate("""
            () => {
                // Click the first visible data row cell in the categories table
                for (const row of document.querySelectorAll('tr')) {
                    if (!row.offsetParent) continue;
                    const rect = row.getBoundingClientRect();
                    if (rect.height < 10 || rect.y < 500) continue;
                    const tds = Array.from(row.querySelectorAll('td'));
                    for (const td of tds) {
                        const r = td.getBoundingClientRect();
                        if (r.width > 20 && r.width < 500 && r.height > 5) {
                            td.click();
                            return {found: true, cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                        }
                    }
                }
                return {found: false};
            }
        """)
        print(f"  Cell-focus restore: {restore_focus_result}")
        page.wait_for_timeout(500)
        _open_table_actions_menu()
        # Dump all visible elements with 'Add Row' text after opening menu (diagnostic)
        add_row_candidates = page.evaluate("""
            () => {
                const results = [];
                for (const el of document.querySelectorAll('*')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (t !== 'Add Row' && !t.startsWith('Add Row') ) continue;
                    if (t.length > 20) continue;
                    const r = el.getBoundingClientRect();
                    if (r.height < 4) continue;
                    results.push({tag: el.tagName, role: el.getAttribute('role')||'',
                        class: el.className.substring(0,40), t: t,
                        cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)});
                }
                return results.slice(0, 20);
            }
        """)
        print(f"  'Add Row' elements visible: {add_row_candidates}")

        # Find the Add Row menu item in the DROPDOWN (try all positions)
        add_row_item = page.evaluate("""
            ([actionsCy]) => {
                // Priority: role=menuitem with text containing 'Add Row'
                for (const sel of ['[role="menuitem"]', '[role="option"]', 'td.af_commandMenuItem',
                                   'li.af_commandMenuItem', '.af_menu_popup *', '[id*="popup"] *']) {
                    for (const el of document.querySelectorAll(sel)) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim();
                        if (!t.includes('Add Row') || t.length > 25) continue;
                        const r = el.getBoundingClientRect();
                        if (r.height > 4) {
                            el.click();
                            return {found: true, cx: Math.round(r.x+r.width/2),
                                    cy: Math.round(r.y+r.height/2), sel: sel, t: t};
                        }
                    }
                }
                // Fallback: any visible 'Add Row' element that's NOT the toolbar button
                // (toolbar button at cx≈259, cy≈actionsCy)
                for (const el of document.querySelectorAll('a, li, td, span, div')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (t !== 'Add Row') continue;
                    const r = el.getBoundingClientRect();
                    const cy = r.y + r.height/2;
                    const cx = r.x + r.width/2;
                    // Skip the toolbar button (known position ~259, ~actionsCy)
                    if (Math.abs(cx - 259) < 20 && Math.abs(cy - actionsCy) < 20) continue;
                    if (r.height > 4) {
                        el.click();
                        return {found: true, cx: Math.round(cx), cy: Math.round(cy), sel: 'non-toolbar'};
                    }
                }
                return {found: false};
            }
        """, [actions_cy])
        print(f"  Add Row dropdown result: {add_row_item}")
        page.wait_for_timeout(1500)
        add_clicked = add_row_item.get("found", False)

    _ss(page, f"after_add_click_{slug}")

    # Dump all visible inputs in table area after Add Row (diagnostic)
    all_inputs = page.evaluate("""
        () => Array.from(document.querySelectorAll(
            'input:not([type=hidden]):not([type=checkbox]):not([type=radio])'
        )).filter(e => {
            if (!e.offsetParent) return false;
            const r = e.getBoundingClientRect();
            return r.height > 0 && (r.y + r.height/2) > 530 && (r.y + r.height/2) < 900;
        }).map(e => {
            const r = e.getBoundingClientRect();
            return {id: e.id.slice(-50), val: e.value.slice(0,20),
                    ph: e.placeholder||'', aria: (e.getAttribute('aria-label')||'').slice(0,30),
                    cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2), w: Math.round(r.width)};
        })
    """)
    print(f"  Inputs after Add Row: {all_inputs}")

    return _fill_new_row_by_columns(page, new_abc, slug, data_cells, all_inputs)


def _fill_new_inline_row(page, new_abc: str, slug: str, table_inputs: list) -> bool:
    """
    Fill the newly-added inline row in the Categories table.
    Oracle adds the row inline; Catalog and Category are LOV fields.
    Strategy:
      1. Find Catalog LOV field (NOT Start/End Date, NOT page search, NOT toolbar SELECT)
      2. Click it, type 'ABC Inventory', handle suggestion
      3. Find Category LOV field, click, type new_abc, handle suggestion
      4. Save the item
    """
    # Known non-row-field positions to exclude
    # Page search bar: cy≈58. Toolbar SELECTs: cy≈605. Start Date: cx≈1326. End Date: cx≈1437.
    def _is_row_field(inp):
        cy = inp.get("cy", 0)
        cx = inp.get("cx", 0)
        if cy < 580:       return False   # above table area
        if cy > 625 and inp.get("tag") == "SELECT": return False   # toolbar SELECTs only at cy=605
        if cx > 1280:      return False   # Start/End Date on far right
        if cx < 50:        return False   # left edge chrome
        return True

    row_fields = [i for i in table_inputs if _is_row_field(i)]
    print(f"  Row fields (Catalog/Category candidates): {row_fields}")

    if not row_fields:
        # Nothing found — new row might not have rendered yet or is a popup
        # Take screenshot and bail; manual fix needed
        print(f"  No new-row LOV fields found. Check after_add_click screenshot.")
        _ss(page, f"no_row_fields_{slug}")
        # Still try to save; the delete at least removed the old row
        return _save_item(page, slug)

    # First row field = Catalog LOV, second = Category LOV
    catalog_inp = row_fields[0] if len(row_fields) >= 1 else None
    category_inp = row_fields[1] if len(row_fields) >= 2 else None

    # ---- Set Catalog ----
    if catalog_inp:
        print(f"  Setting Catalog 'ABC Inventory' at ({catalog_inp['cx']}, {catalog_inp['cy']})...")
        page.mouse.click(catalog_inp["cx"], catalog_inp["cy"])
        page.wait_for_timeout(300)
        page.keyboard.press("Control+a")
        page.keyboard.type("ABC Inventory", delay=60)
        page.wait_for_timeout(1200)
        # Accept autocomplete: try Down+Enter, or just Enter
        suggestion_accepted = False
        for _ in range(3):
            sugg = page.evaluate("""
                () => {
                    const candidates = document.querySelectorAll('li, div[role="option"], tr[data-afr-suggestrow]');
                    for (const c of candidates) {
                        if (c.offsetParent && c.textContent.includes('ABC Inventory')) {
                            const r = c.getBoundingClientRect();
                            if (r.height > 0) return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2)};
                        }
                    }
                    return null;
                }
            """)
            if sugg:
                page.mouse.click(sugg["cx"], sugg["cy"])
                suggestion_accepted = True
                break
            page.keyboard.press("ArrowDown")
            page.wait_for_timeout(400)
        if not suggestion_accepted:
            page.keyboard.press("Tab")
            print(f"    No Catalog suggestion — pressed Tab")
        page.wait_for_timeout(600)
    else:
        print(f"  Catalog LOV field not found — skipping")

    # ---- Re-scan after catalog selection (Category LOV may activate) ----
    table_inputs2 = page.evaluate("""
        () => Array.from(document.querySelectorAll(
            'input:not([type="hidden"]):not([type="checkbox"]):not([type="radio"]), select'
        )).filter(e => {
            if (!e.offsetParent) return false;
            const r = e.getBoundingClientRect();
            return r.height > 0 && (r.y + r.height/2) > 580;
        }).map(e => {
            const r = e.getBoundingClientRect();
            return {tag:e.tagName, id:e.id.slice(-50),
                aria:e.getAttribute('aria-label')||'',
                ph:e.placeholder||'', val:e.value.slice(0,20),
                cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2)};
        })
    """)
    row_fields2 = [i for i in table_inputs2 if _is_row_field(i)]
    print(f"  Row fields after catalog set: {row_fields2}")

    # The Category field: find by id containing 'it2' or 'it3', or use second row field
    cat_field = None
    for f in row_fields2:
        if "it2" in f.get("id","") or "it3" in f.get("id",""):
            cat_field = f
            break
    if not cat_field and len(row_fields2) >= 2:
        cat_field = row_fields2[1]
    elif not cat_field and category_inp:
        cat_field = category_inp

    # ---- Set Category ----
    if cat_field:
        print(f"  Setting Category '{new_abc}' at ({cat_field['cx']}, {cat_field['cy']})...")
        page.mouse.click(cat_field["cx"], cat_field["cy"])
        page.wait_for_timeout(300)
        page.keyboard.press("Control+a")
        page.keyboard.type(new_abc, delay=80)
        page.wait_for_timeout(1200)
        sugg = page.evaluate("""
            ([code]) => {
                const candidates = document.querySelectorAll('li, div[role="option"], tr[data-afr-suggestrow]');
                for (const c of candidates) {
                    if (c.offsetParent) {
                        const r = c.getBoundingClientRect();
                        if (r.height > 0 && c.textContent.trim().startsWith(code))
                            return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2)};
                    }
                }
                return null;
            }
        """, [new_abc])
        if sugg:
            page.mouse.click(sugg["cx"], sugg["cy"])
            print(f"    Clicked suggestion for '{new_abc}'")
        else:
            page.keyboard.press("Tab")
            print(f"    No Category suggestion — pressed Tab")
        page.wait_for_timeout(600)
    else:
        print(f"  Category LOV field not found.")

    _ss(page, f"add_dialog_filled_{slug}")
    return _save_item(page, slug)


def _fill_new_row_by_columns(page, new_abc: str, slug: str, old_cells: list, scanned_inputs: list | None = None) -> bool:
    """
    Fill the newly-added inline row.
    1. If scanned_inputs were provided (from pre-scan after Add Row), use them to locate fields.
    2. Otherwise, find new row y via Start Date input.
    3. Click at column positions derived from old row's cell layout.
    """
    # Derive column positions from old row's data cells (skip narrow indicator and wide container)
    data_cells = [c for c in old_cells if 20 < c["w"] < 500]
    # data_cells layout: [0]=Category, [1]=Catalog, [2]=ControlledAt, [3]=CategoryCode, ...
    # We want:  Catalog = data_cells[1], Category = data_cells[0]
    # But Oracle may present Catalog field first in new row — try both orders

    # Find the new row (it has editable inputs — Start/End date are visible)
    new_row_info = page.evaluate("""
        () => {
            // New row will contain inputs for Start Date / End Date
            const inputs = Array.from(document.querySelectorAll(
                'input[aria-label="Start Date"], input[placeholder="m/d/yy"]'
            ));
            for (const inp of inputs) {
                if (!inp.offsetParent) continue;
                const r = inp.getBoundingClientRect();
                return {found: true, newRowY: Math.round(r.y + r.height/2)};
            }
            return {found: false};
        }
    """)

    if new_row_info.get("found"):
        new_row_y = new_row_info["newRowY"]
        print(f"  New row detected at cy={new_row_y}")
    else:
        # Fallback: assume new row appeared just above old row or at known position
        new_row_y = old_cells[0]["cy"] - 35 if old_cells else 640
        print(f"  New row cy not confirmed — using estimate {new_row_y}")

    # Column cx positions from old row data cells
    catalog_cx  = data_cells[1]["cx"] if len(data_cells) > 1 else 265
    cat_code_cx = data_cells[0]["cx"] if len(data_cells) > 0 else 87

    print(f"  New row positions — Catalog: ({catalog_cx}, {new_row_y}), Category: ({cat_code_cx}, {new_row_y})")

    def _click_and_type_lov(cx, cy, search_val, field_name):
        print(f"  Setting {field_name}='{search_val}' at ({cx}, {cy})...")
        page.mouse.click(cx, cy)
        page.wait_for_timeout(600)
        # Check if an input appeared
        inp_appeared = page.evaluate("""
            ([cx, cy]) => {
                const els = document.querySelectorAll('input:not([type=hidden])');
                for (const el of els) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    if (Math.abs(r.y + r.height/2 - cy) < 25 && Math.abs(r.x + r.width/2 - cx) < 200)
                        return {found: true, cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2),
                                val: el.value, id: el.id.slice(-40)};
                }
                return {found: false};
            }
        """, [cx, cy])
        print(f"    Input at click: {inp_appeared}")

        if inp_appeared.get("found"):
            # Click the confirmed input position, clear, type
            page.mouse.click(inp_appeared["cx"], inp_appeared["cy"])
            page.wait_for_timeout(200)
        page.keyboard.press("Control+a")
        page.keyboard.type(search_val, delay=70)
        page.wait_for_timeout(1400)

        # Accept suggestion
        for attempt in range(3):
            sugg = page.evaluate("""
                ([val]) => {
                    const selectors = [
                        'li[role="option"]', 'div[role="option"]',
                        'tr[data-afr-suggestrow]', 'li.af_selectItem',
                        '.af_popup li', '[id*="popup"] li', '[id*="lov"] li',
                        'li', 'td.af_selectItem'
                    ];
                    for (const sel of selectors) {
                        for (const c of document.querySelectorAll(sel)) {
                            if (!c.offsetParent) continue;
                            const t = c.textContent.trim();
                            const r = c.getBoundingClientRect();
                            if (r.height > 4 && (t.startsWith(val) || t.includes(val)))
                                return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2), text: t.slice(0,40)};
                        }
                    }
                    return null;
                }
            """, [search_val])
            if sugg:
                print(f"    Clicking suggestion '{sugg['text']}' at ({sugg['cx']},{sugg['cy']})...")
                page.mouse.click(sugg["cx"], sugg["cy"])
                page.wait_for_timeout(700)
                return True
            page.keyboard.press("ArrowDown")
            page.wait_for_timeout(400)
        print(f"    No suggestion found — pressing Tab")
        page.keyboard.press("Tab")
        page.wait_for_timeout(600)
        return False

    # ---- Set Catalog: "ABC Inventory" ----
    _click_and_type_lov(catalog_cx, new_row_y, "ABC Inventory", "Catalog")
    _ss(page, f"catalog_set_{slug}")

    # After catalog selection, re-derive new row y (row might shift)
    new_row_info2 = page.evaluate("""
        () => {
            const inputs = Array.from(document.querySelectorAll(
                'input[aria-label="Start Date"], input[placeholder="m/d/yy"]'
            ));
            for (const inp of inputs) {
                if (!inp.offsetParent) continue;
                const r = inp.getBoundingClientRect();
                return {found: true, newRowY: Math.round(r.y + r.height/2)};
            }
            return {found: false};
        }
    """)
    if new_row_info2.get("found"):
        new_row_y = new_row_info2["newRowY"]

    # ---- Set Category Code ----
    _click_and_type_lov(cat_code_cx, new_row_y, new_abc, "Category")
    _ss(page, f"add_dialog_filled_{slug}")

    return _save_item(page, slug)


def _click_actions_menu_item(page, labels: list) -> bool:
    """Open the Actions dropdown and click the first matching menu item."""
    clicked = page.evaluate("""
        ([labels]) => {
            const els = document.querySelectorAll('a, button, span, div');
            // find Actions button
            for (let i = els.length - 1; i >= 0; i--) {
                const el = els[i];
                if (el.offsetParent === null) continue;
                const t = el.textContent.trim();
                if (t === 'Actions' || t === 'Actions ') { el.click(); return 'opened'; }
            }
            return false;
        }
    """, [labels])
    if not clicked:
        return False
    page.wait_for_timeout(600)
    for label in labels:
        if page.evaluate("""
            ([lbl]) => {
                const els = document.querySelectorAll('a, li, span, div');
                for (let i = els.length - 1; i >= 0; i--) {
                    const el = els[i];
                    if (el.offsetParent === null) continue;
                    if (el.textContent.trim() === lbl) { el.click(); return true; }
                }
                return false;
            }
        """, [label]):
            print(f"  Clicked Actions > {label}")
            return True
    return False


def _click_table_action_button(page, row_y: int, title_keywords: list, text_options: list) -> bool:
    """
    Find a toolbar button near row_y whose title/aria-label contains one of title_keywords,
    or whose text is one of text_options. Click it and return True.
    """
    return page.evaluate("""
        ([row_y, title_kws, texts]) => {
            const btns = Array.from(document.querySelectorAll('button, a[role="button"]'));
            for (const btn of btns) {
                if (btn.offsetParent === null) continue;
                const r = btn.getBoundingClientRect();
                if (r.height < 4 || r.width < 4) continue;
                // Button must be within 350px vertically of the row
                if (Math.abs(r.y + r.height/2 - row_y) > 350) continue;

                const titleLower = (
                    btn.title + ' ' +
                    (btn.getAttribute('aria-label') || '') + ' ' +
                    (btn.getAttribute('data-afr-label') || '')
                ).toLowerCase();
                const btnText = btn.textContent.trim();

                const titleMatch = title_kws.some(kw => titleLower.includes(kw));
                const textMatch = texts.includes(btnText);

                if (titleMatch || textMatch) {
                    btn.click();
                    return true;
                }
            }
            return false;
        }
    """, [row_y, title_keywords, text_options])


def _fill_add_category_dialog(page, new_abc: str, slug: str) -> bool:
    """
    Fill the Add Category dialog:
    - Catalog = ABC Inventory Catalog
    - Category = new_abc code (D, P, etc.)
    Then click OK/Add.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    # Dump current visible inputs for debug
    inputs_info = page.evaluate("""
        () => Array.from(document.querySelectorAll(
            'input:not([type="hidden"]):not([type="checkbox"]), select'
        ))
        .filter(e => e.offsetParent !== null && e.getBoundingClientRect().height > 0)
        .map(e => ({
            tag: e.tagName, id: e.id,
            aria: e.getAttribute('aria-label') || '',
            label: e.getAttribute('placeholder') || '',
            value: e.value,
            cx: Math.round(e.getBoundingClientRect().x + e.getBoundingClientRect().width/2),
            cy: Math.round(e.getBoundingClientRect().y + e.getBoundingClientRect().height/2)
        }))
    """)
    print(f"  Dialog inputs: {inputs_info}")

    # ---- Set Catalog field ----
    catalog_done = False
    for inp in inputs_info:
        aria = (inp.get("aria", "") + " " + inp.get("label", "")).lower()
        if "catalog" in aria:
            _set_lov_field(page, inp["cx"], inp["cy"], "ABC Inventory", slug, field_name="Catalog")
            catalog_done = True
            break

    if not catalog_done:
        print(f"  Catalog field not found by aria-label — attempting LOV by position...")
        # If there are exactly 2 LOV fields (Catalog + Category), first is Catalog
        if len(inputs_info) >= 1:
            inp = inputs_info[0]
            _set_lov_field(page, inp["cx"], inp["cy"], "ABC Inventory", slug, field_name="Catalog")
            catalog_done = True

    page.wait_for_timeout(800)

    # Re-query inputs after catalog selection (Category LOV may now be enabled)
    inputs_info2 = page.evaluate("""
        () => Array.from(document.querySelectorAll(
            'input:not([type="hidden"]):not([type="checkbox"]), select'
        ))
        .filter(e => e.offsetParent !== null && e.getBoundingClientRect().height > 0)
        .map(e => ({
            tag: e.tagName, id: e.id,
            aria: e.getAttribute('aria-label') || '',
            label: e.getAttribute('placeholder') || '',
            value: e.value,
            cx: Math.round(e.getBoundingClientRect().x + e.getBoundingClientRect().width/2),
            cy: Math.round(e.getBoundingClientRect().y + e.getBoundingClientRect().height/2)
        }))
    """)
    print(f"  Dialog inputs after catalog set: {inputs_info2}")

    # ---- Set Category field ----
    category_done = False
    for inp in inputs_info2:
        aria = (inp.get("aria", "") + " " + inp.get("label", "")).lower()
        if "category" in aria and "catalog" not in aria:
            _set_lov_field(page, inp["cx"], inp["cy"], new_abc, slug, field_name="Category")
            category_done = True
            break

    if not category_done and len(inputs_info2) >= 2:
        # Assume second input is Category
        inp = inputs_info2[1]
        _set_lov_field(page, inp["cx"], inp["cy"], new_abc, slug, field_name="Category")
        category_done = True

    if not category_done:
        print(f"  Could not find Category field in add dialog.")
        _ss(page, f"add_dialog_fail_{slug}")
        _js_cancel_dialog(page)
        return False

    page.wait_for_timeout(800)
    _ss(page, f"add_dialog_filled_{slug}")

    # ---- Click OK / Add ----
    for btn_text in ["OK", "Add", "Apply", "Save"]:
        try:
            btn = page.locator(f"button:has-text('{btn_text}')").first
            if btn.is_visible(timeout=1500):
                btn.click()
                print(f"  Clicked '{btn_text}' to confirm add")
                page.wait_for_timeout(1500)
                _ss(page, f"after_add_{slug}")
                break
        except Exception:
            pass

    # ---- Now save the item ----
    print(f"  Saving item...")
    saved = False
    for btn_text in ["Save and Close", "Save"]:
        try:
            btn = page.locator(f"button:has-text('{btn_text}')").first
            if btn.is_visible(timeout=2000):
                btn.click()
                print(f"  Clicked '{btn_text}'")
                saved = True
                page.wait_for_timeout(3000)
                break
        except Exception:
            pass

    if not saved:
        # Try JS click for Save
        if _js_click_visible(page, "Save and Close", exact=True) or _js_click_visible(page, "Save", exact=True):
            saved = True
            page.wait_for_timeout(3000)

    if not saved:
        print(f"  Save button not found.")
        _ss(page, f"save_fail_{slug}")
        return False

    # Dismiss any confirmation
    page.wait_for_timeout(1000)
    for sel in ["button:has-text('OK')", "button:has-text('Close')"]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=1500):
                btn.click()
                page.wait_for_timeout(800)
        except Exception:
            pass

    _ss(page, f"saved_{slug}")
    print(f"  Saved.")
    return True


def _save_item(page, slug: str) -> bool:
    """Click Save (or Save and Close) on the item edit page, handle confirmations."""
    print(f"  Saving item...")
    saved = False
    for btn_text in ["Save and Close", "Save"]:
        try:
            btn = page.locator(f"button:has-text('{btn_text}')").first
            if btn.is_visible(timeout=2000):
                btn.click()
                print(f"  Clicked '{btn_text}'")
                saved = True
                page.wait_for_timeout(3000)
                break
        except Exception:
            pass

    if not saved:
        if _js_click_visible(page, "Save and Close", exact=True) or _js_click_visible(page, "Save", exact=True):
            saved = True
            page.wait_for_timeout(3000)

    if not saved:
        print(f"  Save button not found.")
        _ss(page, f"save_fail_{slug}")
        return False

    for sel in ["button:has-text('OK')", "button:has-text('Close')"]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=1500):
                btn.click()
                page.wait_for_timeout(800)
        except Exception:
            pass

    _ss(page, f"saved_{slug}")
    print(f"  Saved.")
    return True


def _set_lov_field(page, cx: int, cy: int, search_value: str, slug: str, field_name: str = "field"):
    """
    Click a LOV input field at (cx, cy), clear it, type the search value,
    wait for a dropdown suggestion, and select the first matching result.
    """
    print(f"  Setting {field_name} to '{search_value}' at ({cx}, {cy})...")
    page.mouse.click(cx, cy)
    page.wait_for_timeout(400)

    # Clear and type
    page.keyboard.press("Control+a")
    page.keyboard.type(search_value)
    page.wait_for_timeout(1200)

    # Look for a suggestion/dropdown to appear
    suggestion_clicked = page.evaluate("""
        ([search_value]) => {
            // Oracle ADF suggestion list: li or tr elements appearing after typing
            const candidates = document.querySelectorAll(
                'li[id*="lov"], tr[id*="lov"], li.af_selectItem, ' +
                'td.af_selectItem, [role="option"], [role="listitem"]'
            );
            for (const el of candidates) {
                const t = el.textContent.trim();
                if (el.offsetParent !== null && t.includes(search_value)) {
                    el.click();
                    return {clicked: true, text: t};
                }
            }
            // Fallback: any visible li/tr in a popup overlay whose text starts with search_value
            const overlayItems = document.querySelectorAll('.af_popup li, .af_popup tr, [id*="popup"] li');
            for (const el of overlayItems) {
                if (el.offsetParent !== null && el.textContent.trim().startsWith(search_value)) {
                    el.click();
                    return {clicked: true, fallback: true, text: el.textContent.trim()};
                }
            }
            return {clicked: false};
        }
    """, [search_value])

    if suggestion_clicked and suggestion_clicked.get("clicked"):
        print(f"    Selected suggestion: '{suggestion_clicked.get('text','')[:40]}'")
        page.wait_for_timeout(600)
    else:
        print(f"    No suggestion appeared — pressing Tab to confirm typed value")
        page.keyboard.press("Tab")
        page.wait_for_timeout(600)


def _return_to_manage_items(page, slug: str):
    """Navigate back to Manage Items search page."""
    from playwright.sync_api import TimeoutError as PWTimeout

    # Try breadcrumb
    try:
        page.locator("a:has-text('Manage Items')").first.click(timeout=3000)
        page.wait_for_selector("text=Advanced Search", timeout=15_000)
        page.wait_for_timeout(1500)
        print("  Returned via breadcrumb.")
        return
    except Exception:
        pass

    # Try browser back
    try:
        page.go_back()
        page.wait_for_load_state("networkidle", timeout=15_000)
        page.wait_for_timeout(1500)
        if page.locator("text=Advanced Search").count() > 0:
            print("  Returned via browser back.")
            return
    except Exception:
        pass

    # Full re-navigation
    print("  Re-navigating to Manage Items...")
    try:
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="networkidle", timeout=60_000)
        page.wait_for_timeout(1000)
        _navigate_to_manage_items(page)
    except Exception as e:
        print(f"  Re-navigation error: {e}")


def _js_click_visible(page, text: str, exact: bool = True) -> bool:
    """Click the last visible element matching text (avoids ADF hidden clones)."""
    return page.evaluate("""
        ([text, exact]) => {
            const els = document.querySelectorAll('button, a, td, span, div, li');
            for (let i = els.length - 1; i >= 0; i--) {
                const el = els[i];
                const t = el.textContent.trim();
                const match = exact ? t === text : t.includes(text);
                if (match && el.offsetParent !== null &&
                        el.getBoundingClientRect().height > 0) {
                    el.click();
                    return true;
                }
            }
            return false;
        }
    """, [text, exact])


def _js_cancel_dialog(page):
    """Click Cancel on any open dialog via JS."""
    page.evaluate("""
        () => {
            const btns = document.querySelectorAll('button');
            for (let i = btns.length - 1; i >= 0; i--) {
                const btn = btns[i];
                if (btn.textContent.trim() === 'Cancel' && btn.offsetParent !== null) {
                    btn.click(); return true;
                }
            }
            return false;
        }
    """)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--explore", action="store_true",
                        help="Open first item, dump page structure, stop before making changes")
    parser.add_argument("--explore-tasks", action="store_true",
                        help="Navigate to PIM dashboard and dump all task panel links, then exit")
    parser.add_argument("--explore-full", action="store_true",
                        help="Comprehensive Oracle Fusion ERP exploration — dump all modules, task panels, and direct ABC task URLs")
    parser.add_argument("--item", default=None)
    args = parser.parse_args()

    changes = load_changes()
    print(f"Loaded {len(changes)} items from Excel.\n")
    for ch in changes:
        print(f"  {ch['item']:30s}  {ch['old_abc']} -> {ch['new_abc']}")
    print()

    if args.explore_tasks:
        _explore_pim_tasks()
        return

    if args.explore_full:
        _explore_full()
        return

    run_ui_updates(
        changes,
        dry_run=args.dry_run,
        explore=args.explore,
        single_item=args.item,
    )


def _explore_pim_tasks():
    """Lightweight: navigate to Inventory Management Classic and dump its task panel."""
    _explore_full()


def _explore_full():
    """
    Comprehensive Oracle Fusion ERP exploration:
    1. Dump the full Navigator menu (all items, all groups)
    2. Probe task panels for key supply chain modules
    3. Search every module for ABC Analysis / Item Category Assignment tasks
    4. Try direct FSCM task URLs for ABC Assignment
    """
    import time
    from playwright.sync_api import sync_playwright

    ABC_KEYWORDS = ['abc', 'category assign', 'item categor', 'classif', 'abc analysis',
                    'abc class', 'strateg']

    def _dump_tasks_panel(page, label):
        """Ensure task panel open, expand all sections, return all task links."""
        # Try to open task panel via icon
        for sel in ["[title='Tasks']", "[aria-label='Tasks']",
                    "button[title='Tasks']", ".af_taskList", "[id*='task']"]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=1500):
                    el.click()
                    page.wait_for_timeout(1200)
                    break
            except Exception:
                pass

        # Expand all collapsed sections in right panel
        page.evaluate("""
            () => {
                for (const el of document.querySelectorAll(
                    '[class*="af_showDetailHeader"], [class*="p_AFCollapsed"], ' +
                    '.af_showDetailHeader_title, [id*="sdh"]'
                )) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    if (r.x < 900) continue;
                    try { el.click(); } catch(e) {}
                }
            }
        """)
        page.wait_for_timeout(800)

        links = page.evaluate("""
            () => {
                const seen = new Set();
                const results = [];
                for (const el of document.querySelectorAll('a, li, td, span')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (!t || t.length < 3 || t.length > 100) continue;
                    const r = el.getBoundingClientRect();
                    if (r.x < 900) continue;       // right panel only
                    if (r.height < 8) continue;
                    const key = t + '|' + el.tagName;
                    if (seen.has(key)) continue;
                    seen.add(key);
                    results.push({
                        tag: el.tagName,
                        text: t.slice(0, 70),
                        cx: Math.round(r.x + r.width/2),
                        cy: Math.round(r.y + r.height/2),
                        href: el.tagName === 'A' ? (el.href||'').slice(0,80) : ''
                    });
                }
                return results;
            }
        """)

        print(f"\n--- Task panel: {label} ({len(links)} items) ---")
        for lnk in links:
            lo = lnk['text'].lower()
            star = "***" if any(k in lo for k in ABC_KEYWORDS) else "   "
            href = f"  href={lnk['href']}" if lnk['href'] else ""
            print(f"  {star} [{lnk['cx']:4d},{lnk['cy']:4d}] <{lnk['tag']:4s}> {lnk['text']}{href}")
        return links

    def _dump_navigator(page):
        """Open the hamburger navigator and dump all visible menu entries with positions."""
        print("\n=== Opening Navigator ===")
        page.mouse.click(24, 57)
        page.wait_for_timeout(2500)

        # Full dump of navigator items (left 0-500px)
        items = page.evaluate("""
            () => {
                const seen = new Set();
                const results = [];
                for (const el of document.querySelectorAll('a, li, span, div')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (!t || t.length < 3 || t.length > 80) continue;
                    const r = el.getBoundingClientRect();
                    if (r.x > 500 || r.height < 8) continue;
                    if (seen.has(t)) continue;
                    seen.add(t);
                    results.push({
                        tag: el.tagName,
                        text: t.slice(0, 60),
                        cx: Math.round(r.x + r.width/2),
                        cy: Math.round(r.y + r.height/2)
                    });
                }
                return results;
            }
        """)
        print(f"Navigator items ({len(items)}):")
        for it in items:
            lo = it['text'].lower()
            star = "***" if any(k in lo for k in
                                ['inventory', 'supply', 'product', 'abc', 'manufact',
                                 'order', 'warehouse', 'cost', 'plan']) else "   "
            print(f"  {star} [{it['cx']:3d},{it['cy']:3d}] <{it['tag']:4s}> {it['text']}")
        return items

    def _navigate_to_module(page, nav_item_text, nav_cx, nav_cy):
        """Open navigator and click a module entry by position."""
        page.mouse.click(24, 57)
        page.wait_for_timeout(2000)
        # Try text click first, then coordinate click
        clicked = page.evaluate("""
            ([txt]) => {
                for (const el of document.querySelectorAll('a, li, span, div')) {
                    if (!el.offsetParent) continue;
                    if (el.textContent.trim() !== txt) continue;
                    const r = el.getBoundingClientRect();
                    if (r.x > 500) continue;
                    el.click();
                    return true;
                }
                return false;
            }
        """, [nav_item_text])
        if not clicked:
            page.mouse.click(nav_cx, nav_cy)
        page.wait_for_load_state("networkidle", timeout=30_000)
        page.wait_for_timeout(3000)
        title = page.evaluate("() => document.title")
        print(f"  Navigated → title: {title[:80]}")

    def _try_direct_url(page, path, label):
        """Navigate to a direct Oracle FSCM URL and dump the page."""
        url = f"{HOST}{path}"
        print(f"\n=== Direct URL: {label} ===")
        print(f"  {url}")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(3000)
            title = page.evaluate("() => document.title")
            body = page.evaluate("() => document.body.innerText.slice(0, 600)")
            print(f"  Title: {title}")
            print(f"  Body:\n{body}")
        except Exception as e:
            print(f"  Error: {e}")

    def _click_topnav_tab(page, tab_text):
        """Click a top nav bar tab — prefer A element, use Playwright locator."""
        try:
            # Playwright locator finds the most specific visible element
            loc = page.locator(f"a:has-text('{tab_text}')").first
            if loc.is_visible(timeout=3000):
                loc.click()
                page.wait_for_timeout(2500)
                return {"found": True, "method": "locator-A"}
        except Exception:
            pass
        # Fallback: JS click on A element specifically
        result = page.evaluate("""
            ([txt]) => {
                for (const el of document.querySelectorAll('a')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (t !== txt) continue;
                    const r = el.getBoundingClientRect();
                    if (r.height < 5 || r.height > 80) continue;
                    el.click();
                    return {found: true, cx: Math.round(r.x+r.width/2),
                            cy: Math.round(r.y+r.height/2), tag: el.tagName};
                }
                return {found: false};
            }
        """, [tab_text])
        page.wait_for_timeout(2500)
        return result

    def _dump_all_visible(page, label, min_x=0):
        """Dump all visible unique text elements on the page, marking ABC keywords."""
        els = page.evaluate("""
            ([minX]) => {
                const seen = new Set();
                const results = [];
                for (const el of document.querySelectorAll('a, button, td, li, span, div')) {
                    if (!el.offsetParent) continue;
                    // Only leaf-ish nodes (few children) to avoid giant containers
                    if (el.childElementCount > 3) continue;
                    const t = el.textContent.trim();
                    if (!t || t.length < 3 || t.length > 100) continue;
                    const r = el.getBoundingClientRect();
                    if (r.x < minX || r.height < 8 || r.width < 8) continue;
                    if (seen.has(t)) continue;
                    seen.add(t);
                    results.push({
                        tag: el.tagName, text: t.slice(0, 80),
                        cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2),
                        href: el.tagName === 'A' ? (el.href||'').slice(0,100) : ''
                    });
                }
                return results;
            }
        """, [min_x])
        print(f"\n--- {label} ({len(els)} elements) ---")
        for e in els:
            lo = e['text'].lower()
            star = "***" if any(k in lo for k in ABC_KEYWORDS) else "   "
            href = f"  -> {e['href']}" if e['href'] else ""
            print(f"  {star} [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> {e['text']}{href}")
        return els

    def _navigate_via_topnav(page, tab_text, sub_text=None):
        """Click a top-nav tab, optionally click a sub-item."""
        print(f"\n  Clicking top-nav: '{tab_text}'...")
        r = _click_topnav_tab(page, tab_text)
        print(f"  Click result: {r}")
        page.wait_for_load_state("networkidle", timeout=20_000)
        page.wait_for_timeout(2000)
        if sub_text:
            print(f"  Clicking sub-item: '{sub_text}'...")
            r2 = _click_topnav_tab(page, sub_text)
            print(f"  Sub-click: {r2}")
            page.wait_for_load_state("networkidle", timeout=20_000)
            page.wait_for_timeout(2000)
        title = page.evaluate("() => document.title")
        print(f"  Title: {title[:80]}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=80)
        context = browser.new_context(viewport={"width": 1600, "height": 900})
        page = context.new_page()

        if _load_cookies(context):
            print("[UI] Loaded cached SSO cookies.")

        print("[UI] Opening Oracle Fusion home...")
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="networkidle", timeout=120_000)
        page.wait_for_timeout(2000)

        # ---- 1. Dump top nav bar tabs ----
        print("\n=== TOP NAV BAR TABS ===")
        topnav = page.evaluate("""
            () => {
                const results = [];
                const seen = new Set();
                for (const el of document.querySelectorAll('a, td, li')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (!t || t.length > 60 || seen.has(t)) continue;
                    const r = el.getBoundingClientRect();
                    // Top nav is at y ≈ 220-300
                    if (r.y < 210 || r.y > 310) continue;
                    if (r.height < 10 || r.width < 10) continue;
                    seen.add(t);
                    results.push({tag: el.tagName, text: t,
                                  cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)});
                }
                return results;
            }
        """)
        for t in topnav:
            print(f"  [{t['cx']:4d},{t['cy']:4d}] <{t['tag']:4s}> {t['text']}")

        # ---- 2. Probe Supply Chain Execution (contains Inventory Mgmt) ----
        print("\n\n" + "="*60)
        print("MODULE: Supply Chain Execution")
        print("="*60)
        # Use proven Playwright locator approach (same as _navigate_to_manage_items)
        try:
            page.locator("text=Supply Chain Execution").first.click(timeout=10_000)
            page.wait_for_timeout(3000)
        except Exception as e:
            print(f"  Tab click error: {e}")
        title_after = page.evaluate("() => document.title")
        print(f"  Title after tab click: {title_after}")
        _ss(page, "explore_sce_tab")

        # Dump what tiles/links are now visible
        sce_tiles = page.evaluate("""
            () => {
                const seen = new Set();
                const results = [];
                for (const el of document.querySelectorAll('a, button, span, div')) {
                    if (!el.offsetParent) continue;
                    if (el.childElementCount > 2) continue;
                    const t = el.textContent.trim();
                    if (!t || t.length < 3 || t.length > 80 || seen.has(t)) continue;
                    const r = el.getBoundingClientRect();
                    // Content area: y > 290 (below nav), x varies
                    if (r.y < 290 || r.height < 8) continue;
                    seen.add(t);
                    results.push({tag: el.tagName, text: t.slice(0,70),
                                  cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2),
                                  href: el.tagName === 'A' ? (el.href||'').slice(0,100) : ''});
                }
                return results;
            }
        """)
        print(f"\n  SCE content area tiles/links ({len(sce_tiles)}):")
        for e in sce_tiles:
            lo = e['text'].lower()
            star = "***" if any(k in lo for k in ABC_KEYWORDS + ['inventory','supply chain exec']) else "   "
            print(f"  {star} [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> {e['text']}")

        # Try to find and click Inventory Management tile
        inv_clicked = page.evaluate("""
            () => {
                const targets = ['Inventory Management', 'Inventory Management (Classic)',
                                 'Manage Inventory', 'Inventory'];
                for (const txt of targets) {
                    for (const el of document.querySelectorAll('a, button, span, div, td')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim();
                        if (t !== txt && !t.startsWith(txt)) continue;
                        const r = el.getBoundingClientRect();
                        if (r.y < 290 || r.height < 8) continue;
                        el.click();
                        return {found: true, text: t, cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                    }
                }
                return {found: false};
            }
        """)
        print(f"\n  Inventory Management tile click: {inv_clicked}")
        if inv_clicked.get("found"):
            page.wait_for_load_state("networkidle", timeout=25_000)
            page.wait_for_timeout(3000)
            _ss(page, "explore_inv_mgmt")
            _dump_tasks_panel(page, "Inventory Management dashboard")
            _dump_all_visible(page, "Inventory Management full page")
        else:
            print("  No Inventory Management tile found — SCE tab may not have loaded tiles")
            _ss(page, "explore_sce_no_inv")

        # ---- 3. Try Navigator hamburger and read its sidebar ----
        print("\n\n" + "="*60)
        print("NAVIGATOR HAMBURGER SIDEBAR")
        print("="*60)
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="networkidle", timeout=60_000)
        page.wait_for_timeout(2000)
        # Click hamburger icon
        try:
            page.locator("[aria-label='Navigator'], button[title='Navigator'], #_FOpt1:_FNav1").first.click(timeout=3000)
        except Exception:
            page.mouse.click(24, 57)
        page.wait_for_timeout(2500)
        _ss(page, "explore_navigator_open")
        # Dump the FULL page after navigator opens — sidebar should be new content
        nav_sidebar = page.evaluate("""
            () => {
                const seen = new Set();
                const results = [];
                for (const el of document.querySelectorAll('a, li, span, div')) {
                    if (!el.offsetParent) continue;
                    if (el.childElementCount > 3) continue;
                    const t = el.textContent.trim();
                    if (!t || t.length < 3 || t.length > 80 || seen.has(t)) continue;
                    const r = el.getBoundingClientRect();
                    if (r.height < 8 || r.width < 8) continue;
                    seen.add(t);
                    results.push({tag: el.tagName, text: t.slice(0,70),
                                  cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2),
                                  href: el.tagName === 'A' ? (el.href||'').slice(0,100) : ''});
                }
                return results;
            }
        """)
        print(f"  All visible elements after navigator open ({len(nav_sidebar)}):")
        for e in nav_sidebar:
            lo = e['text'].lower()
            star = "***" if any(k in lo for k in ABC_KEYWORDS + ['inventory','supply chain','manage']) else "   "
            print(f"  {star} [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> {e['text']}")

        # Close navigator
        page.keyboard.press("Escape")
        page.wait_for_timeout(500)

        # ---- 4. Direct FSCM URL probes for ABC tasks ----
        DIRECT_URLS = [
            # Oracle Fusion 23B+ direct task paths for ABC
            ("/fscmUI/faces/FuseWelcome#abcAnalysis",
             "FuseWelcome #abcAnalysis hash"),
            ("/fscmUI/faces/ManageAbcAnalysisMain",
             "ManageAbcAnalysisMain direct"),
            ("/fscmUI/faces/ManageAbcClassesMain",
             "ManageAbcClassesMain direct"),
            ("/fscmUI/faces/ManageItemCategoryAssignmentMain",
             "ManageItemCategoryAssignment direct"),
            # Supply Chain Execution task list manager
            ("/fscmUI/faces/FuseTaskListManagerTop?fndGlobalItemNodeId=itemNode_inventory_management",
             "Inventory Management taskListManager"),
            ("/fscmUI/faces/FuseTaskListManagerTop?fndGlobalItemNodeId=itemNode_supply_chain_execution",
             "SCE taskListManager"),
            # FSCM Maintain Items: Category Assignment page
            ("/fscmUI/faces/FuseWelcome?_afrWindowMode=0&_adf.ctrl-state=1&fndGlobalItemNodeId=itemNode_manage_item_category_assignments",
             "Manage Item Category Assignments itemNode"),
            # BI Publisher REST
            ("/xmlpserver/rest/v1/reports",
             "BIP REST reports list"),
            ("/xmlpserver/rest/v1/folders",
             "BIP REST folders list"),
        ]
        print(f"\n\n{'='*60}")
        print("DIRECT URL PROBES")
        print("="*60)
        for path, label in DIRECT_URLS:
            _try_direct_url(page, path, label)

        # ---- 5. Global search for "ABC" ----
        print(f"\n\n{'='*60}")
        print("GLOBAL SEARCH: 'ABC Assignment'")
        print("="*60)
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(1500)
        try:
            # Oracle Fusion global search — try multiple possible selectors
            for sel in ["input[id*='search']:not([type='hidden'])",
                        "input[placeholder*='earch']",
                        "input[aria-label*='earch']",
                        "[title='Search'] input", "input.af_inputText_content"]:
                try:
                    el = page.locator(sel).first
                    if el.is_visible(timeout=1500):
                        el.click()
                        break
                except Exception:
                    pass
            else:
                # Try clicking the search bar area by coordinate
                page.mouse.click(480, 78)
            page.wait_for_timeout(500)
            page.keyboard.type("ABC Assignment", delay=80)
            page.wait_for_timeout(3000)
            _ss(page, "explore_search_abc")
            search_results = page.evaluate("""
                () => {
                    const results = [];
                    const seen = new Set();
                    for (const el of document.querySelectorAll('li, a, div, td, span')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim();
                        if (!t || t.length < 3 || t.length > 100 || seen.has(t)) continue;
                        if (!/abc|assign|categor/i.test(t)) continue;
                        seen.add(t);
                        const r = el.getBoundingClientRect();
                        results.push({tag: el.tagName, text: t.slice(0,80),
                                      cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2),
                                      href: el.tagName === 'A' ? (el.href||'').slice(0,100) : ''});
                    }
                    return results;
                }
            """)
            print(f"  Search results for 'ABC Assignment' ({len(search_results)}):")
            for r in search_results:
                print(f"  [{r['cx']:4d},{r['cy']:4d}] <{r['tag']:4s}> {r['text']}  {r['href']}")
        except Exception as e:
            print(f"  Global search error: {e}")

        print("\n\n=== EXPLORATION COMPLETE ===")
        browser.close()


def _navigate_to_pim_dashboard(page):
    """Navigate to PIM module and ensure the tasks panel is open."""
    from playwright.sync_api import TimeoutError as PWTimeout
    try:
        page.locator("text=Product Management").first.click(timeout=10_000)
        page.wait_for_timeout(1500)
    except Exception:
        pass
    page.evaluate("""
        () => {
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
            let node;
            while ((node = walker.nextNode())) {
                if (node.textContent.trim() === 'Product Information Management') {
                    let el = node.parentElement;
                    for (let i = 0; i < 5; i++) {
                        if (el && (el.tagName === 'A' || el.tagName === 'BUTTON' ||
                            el.getAttribute('role') === 'link')) { el.click(); return; }
                        if (el) el = el.parentElement;
                    }
                    node.parentElement.click(); return;
                }
            }
        }
    """)
    page.wait_for_timeout(4000)
    try:
        page.wait_for_selector("a:has-text('Manage Items')", timeout=15_000)
    except Exception:
        try:
            page.locator("[title='Tasks'], [aria-label='Tasks']").first.click(timeout=3000)
            page.wait_for_timeout(1500)
        except Exception:
            page.mouse.click(1421, 232)
            page.wait_for_timeout(1500)


if __name__ == "__main__":
    main()
