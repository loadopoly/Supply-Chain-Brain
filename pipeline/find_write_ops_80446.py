"""
find_write_ops_80446.py  v3
Probe three Oracle Fusion modules for write operations related to part 80446-04:
  1. Procurement  > Manage Agreements   (supplier pricing/agreements for the item)
  2. Work Execution > Manage Work Orders (WOs that consume this item as component)
  3. Inventory Mgmt (Classic) > Create Miscellaneous Transaction (adjustment)
"""
import json, sys
from pathlib import Path
from playwright.sync_api import sync_playwright

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HOST         = "https://fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com"
SESSION_FILE = Path(__file__).parent / "oracle_session.json"
SS_DIR       = Path(__file__).parent / "pim_screenshots" / "80446-04" / "write_ops"
SS_DIR.mkdir(parents=True, exist_ok=True)
PART = "80446-04"

# ─── helpers ────────────────────────────────────────────────────────────────

def ss(page, name):
    try:
        page.screenshot(path=str(SS_DIR / f"{name}.png"))
        print(f"  [ss] {name}.png")
    except Exception as e:
        print(f"  [ss err] {e}")

def load_session(context):
    with open(SESSION_FILE) as f:
        raw = json.load(f)
    pw = []
    for c in raw:
        ck = {"name": c["name"], "value": c["value"],
              "domain": c.get("domain","").lstrip("."),
              "path": c.get("path","/"),
              "secure": c.get("secure", True),
              "httpOnly": c.get("httpOnly", False)}
        if c.get("expires") and c["expires"] > 0:
            ck["expires"] = int(c["expires"])
        pw.append(ck)
    context.add_cookies(pw)

def is_logged_in(page):
    return "Sign In" not in page.title() and "sign in" not in page.title().lower()

def nav_home(page):
    page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(4000)
    if not is_logged_in(page):
        raise RuntimeError("Session expired — re-run capture_session.py")

def coords(page, text, exact=True, sel="a,button,li,div,span,td", x_max=99999, x_min=0, y_max=99999, y_min=0):
    return page.evaluate("""
        ([txt, exact, sel, xMax, xMin, yMax, yMin]) => {
            for (const el of document.querySelectorAll(sel)) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (exact ? t!==txt : !t.includes(txt)) continue;
                const r = el.getBoundingClientRect();
                if (r.width<2||r.height<2) continue;
                const cx = Math.round(r.x+r.width/2), cy = Math.round(r.y+r.height/2);
                if (cx<xMin||cx>xMax||cy<yMin||cy>yMax) continue;
                el.scrollIntoView({block:'center',behavior:'instant'});
                const r2 = el.getBoundingClientRect();
                return {cx:Math.round(r2.x+r2.width/2), cy:Math.round(r2.y+r2.height/2)};
            }
            return null;
        }
    """, [text, exact, sel, x_max, x_min, y_max, y_min])

def click(page, text, exact=True, sel="a,button,li,div,span", **kw):
    c = coords(page, text, exact, sel, **kw)
    if c:
        page.mouse.click(c['cx'], c['cy'])
    return c

def nav_tab(page, name):
    """Click a module nav tab (y<300)."""
    c = coords(page, name, y_max=300)
    if c:
        page.mouse.click(c['cx'], c['cy'])
        page.wait_for_timeout(2500)
    return bool(c)

def click_tile(page, names):
    for name in names:
        c = coords(page, name, sel="a,button", y_min=150)
        if c:
            print(f"   Tile '{name}' at ({c['cx']},{c['cy']})")
            page.mouse.click(c['cx'], c['cy'])
            page.wait_for_timeout(5000)
            try: page.wait_for_load_state("domcontentloaded", timeout=15000)
            except: pass
            page.wait_for_timeout(2000)
            return name
    return None

def show_more(page):
    for _ in range(3):
        c = coords(page, "Show More")
        if not c: break
        page.mouse.click(c['cx'], c['cy'])
        page.wait_for_timeout(1200)

def open_navigator(page, *path):
    """Open Navigator panel, click each step in path (x<700 = left panel)."""
    # Click Navigator icon (aria-label)
    ni = page.evaluate("""
        () => {
            for (const el of document.querySelectorAll('[aria-label*="Navigator"],[title*="Navigator"]')) {
                if (!el.offsetParent) continue;
                const r = el.getBoundingClientRect();
                return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2)};
            }
            return null;
        }
    """)
    if not ni:
        return False
    page.mouse.click(ni['cx'], ni['cy'])
    page.wait_for_timeout(1500)

    for label in path:
        c = coords(page, label, x_max=700)
        if not c:
            print(f"   [nav] '{label}' not found in panel")
            return False
        print(f"   [nav] '{label}' at ({c['cx']},{c['cy']})")
        page.mouse.click(c['cx'], c['cy'])
        page.wait_for_timeout(2000)

    page.wait_for_timeout(3000)
    try: page.wait_for_load_state("domcontentloaded", timeout=15000)
    except: pass
    page.wait_for_timeout(2000)
    return True

def dump_write_actions(page, label):
    keywords = [
        "Create","New","Edit","Update","Save","Submit","Approve","Reject","Cancel",
        "Delete","Add","Remove","Receive","Correct","Adjust","Post","Release",
        "Complete","Reopen","Return","Issue","Transfer","Reverse","Manage","Request",
    ]
    actions = page.evaluate("""
        (kw) => {
            const res=[]; const seen=new Set();
            for (const el of document.querySelectorAll('button,a,input[type="submit"]')) {
                if (!el.offsetParent) continue;
                const t=(el.textContent||el.value||el.getAttribute('title')||'').trim().replace(/\\s+/g,' ');
                if (!t||t.length>80||seen.has(t)) continue;
                if (kw.some(k=>t.toLowerCase().includes(k.toLowerCase()))) {
                    seen.add(t);
                    const r=el.getBoundingClientRect();
                    res.push({label:t, tag:el.tagName, cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2)});
                }
            }
            return res;
        }
    """, keywords)
    print(f"\n  [{label}] Write actions ({len(actions)}):")
    for a in actions:
        print(f"    [{a['tag']}] '{a['label']}' at ({a['cx']},{a['cy']})")
    return actions

def dump_fields(page):
    return page.evaluate("""
        () => {
            const f={};
            for (const row of document.querySelectorAll('tr')) {
                const cells=row.querySelectorAll('td');
                if (cells.length>=2) {
                    const l=cells[0]?.textContent.trim().replace(/\\s+/g,' ');
                    const v=cells[1]?.textContent.trim().replace(/\\s+/g,' ');
                    if (l&&l.length<100&&v&&v.length<300&&!l.match(/^\\d+$/)) f[l]=v;
                }
            }
            for (const inp of document.querySelectorAll('input,select')) {
                if (!inp.offsetParent) continue;
                const id=inp.getAttribute('id')||'';
                let lbl='';
                if (id) { const l=document.querySelector(`label[for="${id}"]`); if(l) lbl=l.textContent.trim(); }
                if (!lbl) { const p=inp.closest('td,li,div'); if(p){const ps=p.previousElementSibling; if(ps) lbl=ps.textContent.trim();} }
                if (lbl&&inp.value) f[lbl.replace(/\\s+/g,' ')]=inp.value;
            }
            return f;
        }
    """)

def click_right_rail_icon(page, label):
    """Click a right-rail icon button by aria-label or title.
    These Redwood icons use aria-label, not visible text."""
    c = page.evaluate("""
        ([lbl]) => {
            const terms = [lbl, lbl.toLowerCase()];
            for (const el of document.querySelectorAll('button,a,span,div')) {
                if (!el.offsetParent) continue;
                const al = (el.getAttribute('aria-label')||'').trim();
                const ti = (el.getAttribute('title')||'').trim();
                if (!terms.some(t => al.toLowerCase().includes(t) || ti.toLowerCase().includes(t))) continue;
                const r = el.getBoundingClientRect();
                if (r.x < 1400 || r.width < 5) continue;
                return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2), hint:al||ti};
            }
            return null;
        }
    """, [label])
    if c:
        print(f"   Right-rail '{label}' [{c['hint']}] at ({c['cx']},{c['cy']})")
        page.mouse.click(c['cx'], c['cy'])
        page.wait_for_timeout(2000)
        return True
    # Fallback: dump all right-rail icons and click first one matching label
    icons = page.evaluate("""
        () => {
            const res = [];
            for (const el of document.querySelectorAll('button,a,span')) {
                if (!el.offsetParent) continue;
                const r = el.getBoundingClientRect();
                if (r.x < 1400 || r.y < 100 || r.y > 500 || r.width < 5) continue;
                const al = el.getAttribute('aria-label')||el.getAttribute('title')||el.textContent.trim()||'';
                if (al.length > 0 && al.length < 60)
                    res.push({hint:al.slice(0,30), cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2)});
            }
            const seen=new Set(); return res.filter(r=>{if(seen.has(r.hint)) return false; seen.add(r.hint); return true;});
        }
    """)
    print(f"   Right-rail icons: {[i['hint'] for i in icons]}")
    for icon in icons:
        if label.lower() in icon['hint'].lower():
            print(f"   Fallback click: '{icon['hint']}' at ({icon['cx']},{icon['cy']})")
            page.mouse.click(icon['cx'], icon['cy'])
            page.wait_for_timeout(2000)
            return True
    return False


def lov_search(page, label_text, search_value, wait_ms=2000):
    """Find an input adjacent to a label, type search_value, trigger LOV."""
    c = page.evaluate("""
        ([lbl, val]) => {
            // Find label by text, then locate input in same row or nearby
            for (const el of document.querySelectorAll('label,span,td,div')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (t !== lbl) continue;
                const r = el.getBoundingClientRect();
                // Look for input in same row
                const row = el.closest('tr,div[role="row"]');
                if (row) {
                    const inp = row.querySelector('input');
                    if (inp && inp.offsetParent) {
                        const ri = inp.getBoundingClientRect();
                        return {cx:Math.round(ri.x+ri.width/2), cy:Math.round(ri.y+ri.height/2), found:'row'};
                    }
                }
                // Sibling input within 400px horizontally same y-band
                for (const inp of document.querySelectorAll('input')) {
                    if (!inp.offsetParent) continue;
                    const ri = inp.getBoundingClientRect();
                    if (Math.abs(ri.y - r.y) < 25 && ri.x > r.x && ri.x < r.x+400) {
                        return {cx:Math.round(ri.x+ri.width/2), cy:Math.round(ri.y+ri.height/2), found:'sibling'};
                    }
                }
            }
            return null;
        }
    """, [label_text, search_value])

    if not c:
        print(f"   LOV: label '{label_text}' input not found")
        return False

    print(f"   LOV: '{label_text}' input at ({c['cx']},{c['cy']}) [{c['found']}]")
    page.mouse.click(c['cx'], c['cy'])
    page.wait_for_timeout(300)
    page.keyboard.press("Control+A")
    page.keyboard.type(search_value, delay=40)
    page.wait_for_timeout(wait_ms)

    # Click LOV suggestion if it appears
    sug = page.evaluate("""
        ([val]) => {
            for (const el of document.querySelectorAll('li,td,div[role="option"]')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim();
                if (t.includes(val)) {
                    const r = el.getBoundingClientRect();
                    if (r.width>50 && r.y>100)
                        return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2), text:t.slice(0,60)};
                }
            }
            return null;
        }
    """, [search_value])
    if sug:
        print(f"   LOV: suggestion '{sug['text']}'")
        page.mouse.click(sug['cx'], sug['cy'])
        page.wait_for_timeout(800)
    else:
        page.keyboard.press("Tab")
        page.wait_for_timeout(500)

    return True

def search_button(page):
    """Click the form Search button — must be in content area (y>100), not global nav."""
    for t in ["Search", "Go", "Find", "Query"]:
        c = coords(page, t, sel="button,input[type='submit'],a", y_min=150)
        if c:
            print(f"   Clicking '{t}' at ({c['cx']},{c['cy']})")
            page.mouse.click(c['cx'], c['cy'])
            page.wait_for_timeout(4000)
            return True
    # Fallback: Enter key
    page.keyboard.press("Enter")
    page.wait_for_timeout(3000)
    return False

def click_result(page, part):
    """Click a result row containing the part number."""
    for _ in range(4):
        hit = page.evaluate("""
            ([p]) => {
                for (const el of document.querySelectorAll('a,td,span')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (t===p||t.startsWith(p)) {
                        const r = el.getBoundingClientRect();
                        if (r.width>20&&r.height>8&&r.y>100)
                            return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2), text:t.slice(0,80)};
                    }
                }
                return null;
            }
        """, [part])
        if hit:
            print(f"   Result: '{hit['text']}' at ({hit['cx']},{hit['cy']})")
            page.mouse.click(hit['cx'], hit['cy'])
            page.wait_for_timeout(4000)
            return True
        page.wait_for_timeout(1200)
    print(f"   No result row matching '{part}'")
    return False


# ─── MODULE 1: PROCUREMENT > Manage Agreements ─────────────────────────────

def probe_procurement(page, results):
    print("\n" + "="*70)
    print("MODULE 1: Procurement > Manage Agreements (for procured item)")
    print("="*70)
    nav_home(page)

    # Navigator: Procurement > Purchase Orders > (land on Overview)
    print("\n1. Navigator → Procurement → Purchase Orders...")
    open_navigator(page, "Procurement", "Purchase Orders")
    ss(page, "p01_po_overview")
    print(f"   Title: {page.title()[:70]}")

    # Open Tasks panel using aria-label icon, then click Manage Agreements
    print("\n2. Tasks panel → Manage Agreements...")
    page.wait_for_timeout(3000)  # wait for right-rail to render
    click_right_rail_icon(page, "Tasks")
    ss(page, "p02_tasks_panel")

    c = click(page, "Manage Agreements", sel="a,button,li,span")
    if c:
        page.wait_for_timeout(4000)
        try: page.wait_for_load_state("domcontentloaded", timeout=15000)
        except: pass
        page.wait_for_timeout(2000)
        print(f"   Manage Agreements clicked — Title: {page.title()[:70]}")
    else:
        print("   Manage Agreements link not found in Tasks panel")
    ss(page, "p03_manage_agreements")
    print(f"   Title: {page.title()[:70]}")

    # Search for part in the Item field
    print(f"\n3. Search agreements by Item = '{PART}'...")
    lov_ok = lov_search(page, "Item", PART, wait_ms=2500)
    if not lov_ok:
        # Fallback: first content-area input
        inp = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('input')) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    if (r.y>100&&r.width>80) return {cx:Math.round(r.x+r.width/2),cy:Math.round(r.y+r.height/2)};
                }
                return null;
            }
        """)
        if inp:
            page.mouse.click(inp['cx'], inp['cy'])
            page.keyboard.press("Control+A")
            page.keyboard.type(PART, delay=40)
    search_button(page)
    ss(page, "p04_agreements_results")

    write_actions = dump_write_actions(page, "Procurement > Agreements")
    fields = dump_fields(page)
    print(f"\n4. Fields ({len(fields)}):")
    for k, v in list(fields.items())[:12]:
        if v: print(f"   {k}: {v}")

    # Supplemental: Manage Approved Supplier List via Navigator
    print("\n5. Navigator → Procurement → Approved Supplier List...")
    nav_home(page)
    open_navigator(page, "Procurement", "Purchase Orders")
    page.wait_for_timeout(3000)
    click_right_rail_icon(page, "Tasks")
    ss(page, "p05_tasks2")
    c2 = click(page, "Manage Approved Supplier List Entries", sel="a,button,li,span")
    if c2:
        page.wait_for_timeout(4000)
        try: page.wait_for_load_state("domcontentloaded", timeout=15000)
        except: pass
        page.wait_for_timeout(2000)
    ss(page, "p05_asl")
    print(f"   Title: {page.title()[:70]}")

    lov_search(page, "Item", PART, wait_ms=2500)
    search_button(page)
    ss(page, "p06_asl_results")
    asl_actions = dump_write_actions(page, "Procurement > ASL")
    asl_fields = dump_fields(page)
    print(f"   ASL fields ({len(asl_fields)}):")
    for k, v in list(asl_fields.items())[:12]:
        if v: print(f"   {k}: {v}")

    results["procurement"] = {
        "module": "Procurement > Purchase Orders (Agreements & ASL)",
        "write_actions_agreements": [a['label'] for a in write_actions],
        "write_actions_asl": [a['label'] for a in asl_actions],
        "agreements_fields": {k:v for k,v in fields.items() if v and len(v)<200},
        "asl_fields": {k:v for k,v in asl_fields.items() if v and len(v)<200},
        "page_title": page.title(),
    }


# ─── MODULE 2: WORK EXECUTION > Manage Work Orders (Item LOV) ──────────────

def probe_work_execution(page, results):
    print("\n" + "="*70)
    print("MODULE 2: Work Execution > Manage Work Orders (Item = 80446-04)")
    print("="*70)
    nav_home(page)

    # SCE tab → restores previously open Work Orders page
    print("\n1. Supply Chain Execution tab...")
    nav_tab(page, "Supply Chain Execution")
    ss(page, "w01_sce")
    print(f"   Title: {page.title()[:70]}")

    # If not on Work Orders page yet, navigate
    if "Work Order" not in page.title():
        show_more(page)
        tile = click_tile(page, ["Work Execution", "Manufacturing Work Orders"])
        print(f"   Tile: '{tile}'")
        task = coords(page, "Manage Manufacturing Work Orders", x_min=900)
        if task:
            page.mouse.click(task['cx'], task['cy'])
            page.wait_for_timeout(4000)
        ss(page, "w01b_manage_wo")

    print(f"   On: {page.title()[:70]}")

    # Reset any existing search filters
    print("\n2. Reset search filters...")
    reset = coords(page, "Reset", sel="button,a")
    if reset:
        page.mouse.click(reset['cx'], reset['cy'])
        page.wait_for_timeout(2000)
    ss(page, "w02_reset")

    # Search by Item LOV (right side of form)
    print(f"\n3. Search Item LOV = '{PART}'...")
    item_ok = lov_search(page, "Item", PART, wait_ms=2500)
    if not item_ok:
        # Direct coordinate approach: Item field is right column of search form
        # From screenshot: Item label ~x:855, input ~x:975, y~255
        page.mouse.click(975, 255)
        page.wait_for_timeout(300)
        page.keyboard.press("Control+A")
        page.keyboard.type(PART, delay=40)
        page.wait_for_timeout(2000)
        sug = coords(page, PART, exact=False, sel="li,td,div")
        if sug:
            page.mouse.click(sug['cx'], sug['cy'])
        else:
            page.keyboard.press("Tab")
        page.wait_for_timeout(500)

    # Clear the Work Order number filter if pre-filled from previous run
    wo_field = page.evaluate("""
        () => {
            for (const inp of document.querySelectorAll('input')) {
                if (!inp.offsetParent) continue;
                if (inp.value && inp.value.includes('80446')) {
                    const id = inp.getAttribute('id')||'';
                    const lbl = id ? (document.querySelector(`label[for="${id}"]`)?.textContent||'') : '';
                    if (lbl.toLowerCase().includes('work order') || lbl==='') {
                        const r = inp.getBoundingClientRect();
                        return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2), val:inp.value, lbl};
                    }
                }
            }
            return null;
        }
    """)
    if wo_field:
        print(f"   Clearing WO filter '{wo_field['val']}' at ({wo_field['cx']},{wo_field['cy']})")
        page.mouse.click(wo_field['cx'], wo_field['cy'])
        page.keyboard.press("Control+A")
        page.keyboard.press("Delete")
        page.wait_for_timeout(300)

    # Clear Status filter (set to All)
    status = coords(page, "Status", sel="label,span,td")
    if status:
        # Click the status dropdown
        status_sel = page.evaluate("""
            ([sy]) => {
                for (const el of document.querySelectorAll('select')) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    if (Math.abs(r.y - sy) < 30) return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2)};
                }
                return null;
            }
        """, [status['cy']])
        if status_sel:
            page.mouse.click(status_sel['cx'], status_sel['cy'])
            page.wait_for_timeout(200)
            # Select "All" option (first option, press Home)
            page.keyboard.press("Home")
            page.wait_for_timeout(200)

    ss(page, "w03_item_entered")
    search_button(page)
    ss(page, "w04_results")
    print(f"   Results page: {page.title()[:70]}")

    # Count results
    result_count = page.evaluate("""
        () => {
            for (const el of document.querySelectorAll('span,div,td')) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim();
                const m = t.match(/^(\\d+)\\s*(result|row|record)/i) || t.match(/^Results:\\s*(\\d+)/i);
                if (m) return parseInt(m[1]);
            }
            // Count table rows (excluding header)
            const rows = document.querySelectorAll('tr[class*="row"],tbody tr');
            return rows.length;
        }
    """)
    print(f"   Result rows: {result_count}")

    write_actions = dump_write_actions(page, "Work Execution")
    fields = dump_fields(page)
    print(f"\n4. Fields ({len(fields)}):")
    for k, v in list(fields.items())[:15]:
        if v: print(f"   {k}: {v}")

    # Open first work order to see detail-level write ops
    print("\n5. Opening first work order result...")
    first_wo = page.evaluate("""
        () => {
            // Work order links are in the result table, typically y>480
            for (const el of document.querySelectorAll('a')) {
                if (!el.offsetParent) continue;
                const r = el.getBoundingClientRect();
                if (r.y < 480 || r.y > 700) continue;
                const t = el.textContent.trim();
                if (t.length > 2 && t.length < 50)
                    return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2), text:t.slice(0,50)};
            }
            return null;
        }
    """)
    detail_actions = []
    if first_wo:
        print(f"   Clicking: '{first_wo['text']}'")
        page.mouse.click(first_wo['cx'], first_wo['cy'])
        page.wait_for_timeout(4000)
        ss(page, "w05_wo_detail")
        print(f"   Detail: {page.title()[:70]}")
        detail_actions = dump_write_actions(page, "WO Detail")

    results["work_execution"] = {
        "module": "SCE > Work Execution > Manage Work Orders (Item=80446-04)",
        "result_count": result_count,
        "write_actions_list": [a['label'] for a in write_actions],
        "write_actions_detail": [a['label'] for a in detail_actions],
        "fields": {k:v for k,v in fields.items() if v and len(v)<200},
        "page_title": page.title(),
    }


# ─── MODULE 3: INVENTORY MANAGEMENT (CLASSIC) > Misc Transaction ───────────

def probe_inventory(page, results):
    print("\n" + "="*70)
    print("MODULE 3: Inventory Management (Classic) > Transactions")
    print("="*70)
    nav_home(page)

    # Use Navigator directly — "Inventory Management (Classic)" is in the panel
    print("\n1. Navigator → Inventory Management (Classic)...")
    nav_ok = open_navigator(page, "Supply Chain Execution", "Inventory Management (Classic)")
    ss(page, "i01_inv_nav")
    print(f"   Nav ok: {nav_ok} | Title: {page.title()[:70]}")

    if not nav_ok or "Inventory" not in page.title():
        # Fallback: SCE tab then tile
        print("   Fallback: SCE tab → Inventory Management (Classic) tile...")
        nav_home(page)
        nav_tab(page, "Supply Chain Execution")
        show_more(page)
        tile = click_tile(page, ["Inventory Management (Classic)", "Inventory Management"])
        print(f"   Tile: '{tile}' | Title: {page.title()[:70]}")
    ss(page, "i02_inv_home")
    print(f"   On: {page.title()[:70]}")

    # Task panel (select at x>900)
    task_panel = page.evaluate("""
        () => {
            for (const sel of document.querySelectorAll('select')) {
                if (!sel.offsetParent) continue;
                const r = sel.getBoundingClientRect();
                if (r.x < 900) continue;
                const opts = Array.from(sel.options).map(o=>({text:o.text.trim(), idx:o.index}));
                return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2), opts};
            }
            return null;
        }
    """)
    if task_panel:
        print(f"\n2. Task panel options: {[o['text'] for o in task_panel['opts']]}")
        # Select "Transactions" category
        for opt in task_panel['opts']:
            if 'transact' in opt['text'].lower() or 'item' in opt['text'].lower():
                page.mouse.click(task_panel['cx'], task_panel['cy'])
                page.wait_for_timeout(200)
                page.keyboard.press("Home")
                for _ in range(opt['idx']):
                    page.keyboard.press("ArrowDown")
                    page.wait_for_timeout(50)
                page.keyboard.press("Enter")
                page.wait_for_timeout(1500)
                print(f"   Selected: '{opt['text']}'")
                break
        ss(page, "i03_task_panel")

    # List all right-panel task links
    right_tasks = page.evaluate("""
        () => {
            const res=[];
            for (const el of document.querySelectorAll('a,li,span')) {
                if (!el.offsetParent) continue;
                const r = el.getBoundingClientRect();
                if (r.x < 900) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                if (t.length>2&&t.length<80) res.push({text:t, cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2)});
            }
            return res;
        }
    """)
    print(f"\n3. Right-panel tasks: {[t['text'] for t in right_tasks]}")

    # Try Manage Item Quantities first (read data then show write ops)
    opened_task = None
    for task_name in [
        "Manage Item Quantities",
        "View Item Quantities",
        "Manage Transactions",
        "Create Miscellaneous Transaction",
        "Create Movement Request",
    ]:
        c = coords(page, task_name, x_min=900)
        if c:
            print(f"\n4. Opening: '{task_name}'")
            page.mouse.click(c['cx'], c['cy'])
            page.wait_for_timeout(4000)
            opened_task = task_name
            break

    ss(page, "i04_task_opened")
    print(f"   Task opened: '{opened_task}' | Title: {page.title()[:70]}")

    # Handle org selection dialog
    org_dlg = page.evaluate("""
        () => {
            for (const inp of document.querySelectorAll('input')) {
                if (!inp.offsetParent) continue;
                const r = inp.getBoundingClientRect();
                if (r.x>400&&r.x<950&&r.y>300&&r.y<600)
                    return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2)};
            }
            return null;
        }
    """)
    if org_dlg:
        print(f"   Org dialog at ({org_dlg['cx']},{org_dlg['cy']}) — typing '3165'...")
        page.mouse.click(org_dlg['cx'], org_dlg['cy'])
        page.wait_for_timeout(300)
        page.keyboard.press("Control+A")
        page.keyboard.type("3165", delay=40)
        page.wait_for_timeout(1800)
        ss(page, "i04b_org")
        sug = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('li,td,div')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim();
                    if (t.includes('3165')||t.includes('BUR_MFG')||t.includes('Burlington')) {
                        const r = el.getBoundingClientRect();
                        if (r.width>50&&r.y>200)
                            return {cx:Math.round(r.x+r.width/2), cy:Math.round(r.y+r.height/2), text:t.slice(0,60)};
                    }
                }
                return null;
            }
        """)
        if sug:
            print(f"   Org: '{sug['text']}'")
            page.mouse.click(sug['cx'], sug['cy'])
        else:
            page.keyboard.press("Tab")
        page.wait_for_timeout(1500)
        ok = coords(page, "OK", sel="button")
        if ok:
            page.mouse.click(ok['cx'], ok['cy'])
        page.wait_for_timeout(3000)
        ss(page, "i04c_org_done")

    print(f"\n5. After org — Title: {page.title()[:70]}")
    ss(page, "i05_after_org")

    # Search item
    print(f"\n6. Search Item LOV = '{PART}'...")
    lov_search(page, "Item", PART, wait_ms=2000)
    search_button(page)
    ss(page, "i06_results")

    write_actions = dump_write_actions(page, "Inventory Mgmt")
    fields = dump_fields(page)
    print(f"\n7. Fields ({len(fields)}):")
    for k, v in list(fields.items())[:15]:
        if v: print(f"   {k}: {v}")

    results["inventory"] = {
        "module": f"SCE > Inventory Management (Classic) > {opened_task or 'Items'}",
        "write_actions": [a['label'] for a in write_actions],
        "fields": {k:v for k,v in fields.items() if v and len(v)<200},
        "page_title": page.title(),
    }


# ─── MAIN ───────────────────────────────────────────────────────────────────

def main():
    results = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = ctx.new_page()
        try:
            load_session(ctx)
            probe_procurement(page, results)
            probe_work_execution(page, results)
            probe_inventory(page, results)
        except Exception as e:
            print(f"\nERROR: {e}")
            ss(page, "error")
            import traceback; traceback.print_exc()
        finally:
            out = SS_DIR / "write_ops_report_v3.json"
            with open(out, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"\n[report] {out}")

            print("\n" + "="*70)
            print(f"WRITE OPERATIONS FOR PART {PART}")
            print("="*70)
            for key, data in results.items():
                print(f"\n  {data['module']}")
                for k, v in data.items():
                    if 'write_action' in k and v:
                        print(f"    {k}: {v}")
            browser.close()

if __name__ == "__main__":
    main()
