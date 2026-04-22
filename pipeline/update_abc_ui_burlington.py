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
import argparse
import openpyxl
from pathlib import Path

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
    return _change_abc_category_row(page, new_abc, slug)


def _click_tab(page, tab_name: str) -> bool:
    """Click a tab by its exact text."""
    return page.evaluate("""
        ([name]) => {
            const candidates = document.querySelectorAll(
                '[role="tab"], a, button, li, div, span'
            );
            // Reverse to prefer later (more visible) matches
            for (let i = candidates.length - 1; i >= 0; i--) {
                const el = candidates[i];
                if (el.textContent.trim() === name && el.offsetParent !== null) {
                    const r = el.getBoundingClientRect();
                    if (r.height > 0 && r.width > 0) {
                        el.click();
                        return true;
                    }
                }
            }
            return false;
        }
    """, [tab_name])


def _change_abc_category_row(page, new_abc: str, slug: str) -> bool:
    """
    On the Categories tab (in edit mode):
    Inline-edit the Category cell in the ABC Inventory Catalog row.
    Oracle ADF editable tables: clicking a cell activates inline edit LOV.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    # ---- Find row + identify the Category LOV link ----
    # Oracle ADF LOV cells: the displayed value is an <a> link (not the outer <td>).
    # Clicking the <a> activates the LOV editor; clicking the Hierarchy icon (also in
    # the same cell, further right) opens View Hierarchy instead.
    row_info = page.evaluate("""
        () => {
            const rows = Array.from(document.querySelectorAll('tr'));
            for (const row of rows) {
                const rt = row.textContent;
                if (!rt.includes('ABC Invent') && !rt.toLowerCase().includes('abc inv')) continue;
                const rect = row.getBoundingClientRect();
                if (rect.height < 4 || rect.height > 150 || rect.width < 100) continue;

                // Collect all visible children (td, a, span) in the row
                const kids = Array.from(row.querySelectorAll('td, a, span'))
                    .map(c => {
                        const r = c.getBoundingClientRect();
                        return {
                            tag: c.tagName,
                            text: c.textContent.trim().substring(0, 50),
                            cx: Math.round(r.x + r.width / 2),
                            cy: Math.round(r.y + r.height / 2),
                            x: Math.round(r.x),
                            w: Math.round(r.width), h: Math.round(r.height)
                        };
                    })
                    .filter(c => c.w > 0 && c.h > 0 && c.w < 600);

                // Find catalog x to identify columns left of it
                let catalogX = 999999;
                for (const c of kids) {
                    if (c.text.includes('ABC Invent') && c.w > 50) { catalogX = c.cx; break; }
                }

                // The Category LOV <a> link: short alpha text, left of catalog, smallest center-x
                let catLink = null;
                let catCX = 999999;
                for (const c of kids) {
                    const t = c.text.trim();
                    if (t.length >= 1 && t.length <= 5 && /^[A-Za-z]+$/.test(t) && c.cx < catalogX) {
                        if (c.cx < catCX) { catCX = c.cx; catLink = c; }
                    }
                }

                // Collect all <td> cells for fallback
                const cells = kids.filter(c => c.tag === 'TD').slice(0, 8);

                return {
                    found: true,
                    rowY: Math.round(rect.y + rect.height / 2),
                    rowText: rt.trim().substring(0, 120),
                    cells: cells,
                    catLink: catLink
                };
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
    cat_cell = row_info.get("catLink")
    print(f"  Found ABC row at y={row_y}: {row_info.get('rowText','')[:60]}")
    print(f"  Cells: {[(c['text'], c['cx'], c['cy']) for c in cells[:8]]}")
    print(f"  Category LOV link: {cat_cell}")

    if not cat_cell:
        # Fallback: use first td cell with short alpha text
        for c in cells:
            if 1 <= len(c["text"].strip()) <= 5 and c["text"].strip().isalpha():
                cat_cell = c
                break
    if not cat_cell and cells:
        cat_cell = cells[1] if len(cells) > 1 else cells[0]
    print(f"  Category target (final): {cat_cell}")

    # ---- Helper: find LOV input anywhere on page (not just near cell) ----
    def find_lov_input(near_cx, near_cy, radius=800):
        return page.evaluate("""
            ([cx, cy, radius]) => {
                const inputs = Array.from(document.querySelectorAll(
                    'input:not([type="hidden"]):not([type="checkbox"])'
                ));
                let best = null, bestDist = Infinity;
                for (const inp of inputs) {
                    if (inp.offsetParent === null) continue;
                    const r = inp.getBoundingClientRect();
                    if (r.height < 4 || r.width < 4) continue;
                    if (inp.readOnly) continue;
                    const dist = Math.hypot(r.x + r.width/2 - cx, r.y + r.height/2 - cy);
                    if (dist < radius && dist < bestDist) {
                        best = {
                            found: true,
                            cx: Math.round(r.x + r.width/2),
                            cy: Math.round(r.y + r.height/2),
                            id: inp.id,
                            aria: inp.getAttribute('aria-label') || '',
                            dist: Math.round(dist)
                        };
                        bestDist = dist;
                    }
                }
                return best || {found: false};
            }
        """, [near_cx, near_cy, radius])

    # ---- First: close any stray dialog (e.g. View Hierarchy from prior run) ----
    for close_sel in ["button:has-text('OK')", "button:has-text('Cancel')", "button:has-text('Close')", "[title='Close']"]:
        try:
            btn = page.locator(close_sel).first
            if btn.is_visible(timeout=600):
                btn.click()
                page.wait_for_timeout(400)
                break
        except Exception:
            pass

    # ---- Click the Category LOV <a> link directly ----
    print(f"  Clicking Category LOV link at ({cat_cell['cx']}, {cat_cell['cy']})...")
    page.mouse.click(cat_cell["cx"], cat_cell["cy"])
    page.wait_for_timeout(2000)
    _ss(page, f"cat_click1_{slug}")

    # DEBUG: dump all visible inputs and notable buttons on page
    page_scan = page.evaluate("""
        () => {
            const inputs = Array.from(document.querySelectorAll('input:not([type="hidden"])'))
                .filter(e => e.offsetParent !== null && e.getBoundingClientRect().height > 0)
                .map(e => {const r=e.getBoundingClientRect();
                    return {tag:'INPUT',type:e.type,id:e.id,aria:e.getAttribute('aria-label')||'',
                        val:e.value,ro:e.readOnly,cx:Math.round(r.x+r.width/2),cy:Math.round(r.y+r.height/2)};});
            const btns = Array.from(document.querySelectorAll('button,a[role="button"]'))
                .filter(e => e.offsetParent !== null && e.getBoundingClientRect().height > 0)
                .map(e => {const r=e.getBoundingClientRect();
                    return {tag:e.tagName,t:e.textContent.trim().substring(0,30),
                        title:e.title||'',aria:e.getAttribute('aria-label')||'',
                        cx:Math.round(r.x+r.width/2),cy:Math.round(r.y+r.height/2)};})
                .filter(b => b.cx > 30 && b.cy > 530 && b.cy < 700);
            return {inputs, btns};
        }
    """)
    print(f"  Page inputs: {page_scan.get('inputs', [])}")
    print(f"  Toolbar+row buttons: {page_scan.get('btns', [])}")

    lov_inp = find_lov_input(cat_cell["cx"], cat_cell["cy"])
    print(f"  LOV input after click: {lov_inp}")

    if not lov_inp.get("found"):
        # Try double-click (sometimes needed for ADF LOV cells)
        print(f"  Trying double-click...")
        page.mouse.dblclick(cat_cell["cx"], cat_cell["cy"])
        page.wait_for_timeout(800)
        lov_inp = find_lov_input(cat_cell["cx"], cat_cell["cy"])
        print(f"  LOV input after double-click: {lov_inp}")

    if not lov_inp.get("found"):
        # Try F2
        print(f"  Trying F2...")
        page.mouse.click(cat_cell["cx"], cat_cell["cy"])
        page.wait_for_timeout(300)
        page.keyboard.press("F2")
        page.wait_for_timeout(800)
        lov_inp = find_lov_input(cat_cell["cx"], cat_cell["cy"])
        print(f"  LOV input after F2: {lov_inp}")

    _ss(page, f"lov_state_{slug}")

    # ---- Type the new category code into the LOV ----
    if lov_inp.get("found") and not lov_inp.get("readonly"):
        _set_lov_field(page, lov_inp["cx"], lov_inp["cy"], new_abc, slug, field_name="Category")
    else:
        # Fallback: type directly at category cell position
        print(f"  No editable LOV found — typing directly at category cell position...")
        page.mouse.click(cat_cell["cx"], cat_cell["cy"])
        page.wait_for_timeout(400)
        page.keyboard.press("Control+a")
        page.keyboard.type(new_abc)
        page.wait_for_timeout(1500)
        # Handle any LOV suggestion
        page.evaluate("""
            ([val]) => {
                const sels = [
                    'li[id*="lov"]', 'tr[id*="lov"]', 'li.af_selectItem',
                    'td.af_selectItem', '[role="option"]', '[role="listitem"]',
                    '.af_popup li', '[id*="popup"] li'
                ];
                for (const sel of sels) {
                    for (const el of document.querySelectorAll(sel)) {
                        if (el.offsetParent !== null && el.textContent.trim().includes(val)) {
                            el.click(); return true;
                        }
                    }
                }
                return false;
            }
        """, [new_abc])
        page.wait_for_timeout(600)

    page.wait_for_timeout(800)
    _ss(page, f"abc_category_set_{slug}")

    # ---- Save the item ----
    print(f"  Saving item...")
    saved = False
    for btn_text in ["Save and Close", "Save"]:
        if _js_click_visible(page, btn_text, exact=True):
            print(f"  Clicked '{btn_text}'")
            saved = True
            page.wait_for_timeout(3000)
            break

    if not saved:
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
        print(f"  Save button not found.")
        _ss(page, f"save_fail_{slug}")
        return False

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
    return category_done


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
    parser.add_argument("--item", default=None)
    args = parser.parse_args()

    changes = load_changes()
    print(f"Loaded {len(changes)} items from Excel.\n")
    for ch in changes:
        print(f"  {ch['item']:30s}  {ch['old_abc']} -> {ch['new_abc']}")
    print()

    run_ui_updates(
        changes,
        dry_run=args.dry_run,
        explore=args.explore,
        single_item=args.item,
    )


if __name__ == "__main__":
    main()
