"""
fetch_pim_80446_v3.py - Targeted search using exact coordinates from screenshot
Item field: x~533, y~367  |  Search button: x~1253, y~423
"""
import json, sys
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
        page.screenshot(path=str(SS_DIR / f"{name}.png"))
        print(f"  [ss] {name}.png")
    except Exception as e:
        print(f"  [ss err] {e}")

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

def extract_all_fields(page):
    return page.evaluate("""
        () => {
            const fields = {};
            // tr/td pairs
            for (const row of document.querySelectorAll('tr')) {
                const cells = row.querySelectorAll('td');
                if (cells.length >= 2) {
                    const l = cells[0]?.textContent.trim().replace(/\\s+/g,' ');
                    const v = cells[1]?.textContent.trim().replace(/\\s+/g,' ');
                    if (l && l.length < 100 && v && v.length < 500 && !l.match(/^\\d+$/)) {
                        fields[l] = v;
                    }
                }
            }
            // Visible inputs with labels
            for (const inp of document.querySelectorAll('input,select')) {
                if (!inp.offsetParent) continue;
                const id = inp.getAttribute('id') || '';
                let label = '';
                if (id) {
                    const lbl = document.querySelector('label[for="' + id + '"]');
                    if (lbl) label = lbl.textContent.trim().replace(/\\s+/g,' ');
                }
                if (!label) {
                    const p = inp.closest('td,div,li');
                    if (p) {
                        const prev = p.previousElementSibling;
                        if (prev) label = prev.textContent.trim().replace(/\\s+/g,' ');
                    }
                }
                const val = inp.value || '';
                if (label && val && label.length < 80) fields[label] = val;
            }
            // span label + adjacent value
            for (const el of document.querySelectorAll('[class*="af_outputLabel"],[class*="Label"],[class*="label"]')) {
                if (!el.offsetParent) continue;
                const label = el.textContent.trim().replace(/\\s+/g,' ');
                if (!label || label.length > 80) continue;
                let sib = el.nextElementSibling || el.parentElement?.nextElementSibling;
                if (sib) {
                    const val = sib.textContent.trim().replace(/\\s+/g,' ');
                    if (val && val.length < 500 && val !== label) fields[label] = val;
                }
            }
            // headings
            const headings = [];
            for (const h of document.querySelectorAll('h1,h2,h3,h4')) {
                if (h.offsetParent) headings.push(h.textContent.trim());
            }
            return {fields, headings};
        }
    """)

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()

        try:
            load_session(context)

            # ── Navigate to Browse Commercial Items ───────────────────────────
            print("1. FuseWelcome → Product Management → Browse Commercial Items...")
            page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2500)

            # Click Product Management tab
            tab = page.evaluate("""
                () => {
                    for (const el of document.querySelectorAll('a,div,span,button,li')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim().replace(/\\s+/g,' ');
                        if (t !== 'Product Management') continue;
                        const r = el.getBoundingClientRect();
                        if (r.y > 300) continue;
                        return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                    }
                    return null;
                }
            """)
            if tab:
                page.mouse.click(tab['cx'], tab['cy'])
                page.wait_for_timeout(2500)

            # Click Browse Commercial Items
            tile = page.evaluate("""
                () => {
                    for (const el of document.querySelectorAll('a,button,div')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim().replace(/\\s+/g,' ');
                        if (t !== 'Browse Commercial Items') continue;
                        const r = el.getBoundingClientRect();
                        if (r.y < 150) continue;
                        el.scrollIntoView({block:'center', behavior:'instant'});
                        const r2 = el.getBoundingClientRect();
                        return {cx: Math.round(r2.x+r2.width/2), cy: Math.round(r2.y+r2.height/2)};
                    }
                    return null;
                }
            """)
            if tile:
                page.mouse.click(tile['cx'], tile['cy'])
                page.wait_for_timeout(5000)
                try: page.wait_for_load_state("domcontentloaded", timeout=15000)
                except: pass
                page.wait_for_timeout(2000)

            print(f"   Page: {page.title()[:70]}")
            ss(page, "v3_01_browse_items")

            # ── Type in Item field (x~533, y~367 from screenshot) ─────────────
            print(f"\n2. Type '{PART_NUMBER}' in Item search field...")

            # Find the Item input by its known position from the screenshot
            # Label "Item" is at ~x:375, y:367; input is at ~x:533, y:367
            item_input = page.evaluate("""
                () => {
                    // Look for input adjacent to a label "Item" or after it in a tr
                    for (const label of document.querySelectorAll('label,span,td')) {
                        if (!label.offsetParent) continue;
                        const t = label.textContent.trim();
                        if (t !== 'Item') continue;
                        const r = label.getBoundingClientRect();
                        // Find input in the same row / nearby
                        const row = label.closest('tr');
                        if (row) {
                            const inp = row.querySelector('input');
                            if (inp) {
                                const ri = inp.getBoundingClientRect();
                                return {cx: Math.round(ri.x+ri.width/2), cy: Math.round(ri.y+ri.height/2)};
                            }
                        }
                        // Fallback: find input near this label horizontally
                        for (const inp of document.querySelectorAll('input')) {
                            if (!inp.offsetParent) continue;
                            const ri = inp.getBoundingClientRect();
                            if (Math.abs(ri.y - r.y) < 20 && ri.x > r.x) {
                                return {cx: Math.round(ri.x+ri.width/2), cy: Math.round(ri.y+ri.height/2)};
                            }
                        }
                    }
                    return null;
                }
            """)

            if item_input:
                print(f"   Found Item input at ({item_input['cx']}, {item_input['cy']})")
                page.mouse.click(item_input['cx'], item_input['cy'])
            else:
                # Use screenshot coordinate directly
                print("   Using coordinate x=533, y=367 (from screenshot)")
                page.mouse.click(533, 367)

            page.wait_for_timeout(300)
            page.keyboard.press("Control+A")
            page.keyboard.type(PART_NUMBER, delay=40)
            page.wait_for_timeout(500)
            ss(page, "v3_02_typed")

            # ── Click Search button (x~1253, y~423 from screenshot) ───────────
            print("3. Click Search button...")
            search_btn = page.evaluate("""
                () => {
                    for (const btn of document.querySelectorAll('button,input[type="submit"]')) {
                        if (!btn.offsetParent) continue;
                        const t = btn.textContent.trim() || btn.getAttribute('value') || '';
                        if (t === 'Search') {
                            const r = btn.getBoundingClientRect();
                            return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
                        }
                    }
                    return null;
                }
            """)
            if search_btn:
                print(f"   Search button at ({search_btn['cx']}, {search_btn['cy']})")
                page.mouse.click(search_btn['cx'], search_btn['cy'])
            else:
                print("   Using coordinate x=1253, y=423 (from screenshot)")
                page.mouse.click(1253, 423)

            page.wait_for_timeout(4000)
            ss(page, "v3_03_results")
            print(f"   Results page: {page.title()[:70]}")

            # ── Click the result row for 80446-04 ─────────────────────────────
            print(f"\n4. Click result '{PART_NUMBER}'...")
            for attempt in range(5):
                result = page.evaluate("""
                    ([pn]) => {
                        for (const el of document.querySelectorAll('a,td,span,div')) {
                            if (!el.offsetParent) continue;
                            const t = el.textContent.trim();
                            if (t !== pn && !t.startsWith(pn)) continue;
                            const r = el.getBoundingClientRect();
                            if (r.width < 20 || r.height < 8) continue;
                            return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2), text: t.slice(0,80)};
                        }
                        return null;
                    }
                """, [PART_NUMBER])
                if result:
                    print(f"   Clicking: '{result['text']}' at ({result['cx']},{result['cy']})")
                    page.mouse.click(result['cx'], result['cy'])
                    page.wait_for_timeout(5000)
                    try: page.wait_for_load_state("domcontentloaded", timeout=15000)
                    except: pass
                    page.wait_for_timeout(2000)
                    ss(page, "v3_04_item_detail")
                    break
                page.wait_for_timeout(1500)
            else:
                print("   Item not found in results - capturing page state")
                ss(page, "v3_04_no_result")

            print(f"   Detail title: {page.title()[:70]}")
            print(f"   Detail URL:   {page.url[:120]}")

            # ── Extract all PIM fields from detail page ────────────────────────
            print("\n5. Extracting all PIM fields...")
            raw = extract_all_fields(page)
            fields  = raw.get('fields', {})
            headings = raw.get('headings', [])

            print(f"\n   Headings: {headings}")
            print(f"\n   === Primary PIM Fields ({len(fields)}) ===")
            for k, v in fields.items():
                if v and not v.startswith("Starts with"):
                    print(f"   {k}: {v}")

            # ── Walk all tabs ──────────────────────────────────────────────────
            print("\n6. Scanning tabs...")
            tabs = page.evaluate("""
                () => {
                    const res = [];
                    const seen = new Set();
                    for (const el of document.querySelectorAll(
                            '[role="tab"],a[class*="tab"],li[class*="tab"],a[id*="tab"],button[id*="tab"]')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim().replace(/\\s+/g,' ');
                        if (!t || t.length > 60 || seen.has(t)) continue;
                        seen.add(t);
                        const r = el.getBoundingClientRect();
                        res.push({text: t, cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)});
                    }
                    return res;
                }
            """)
            print(f"   Tabs found: {[t['text'] for t in tabs]}")

            all_tab_data = {}
            for tab in tabs:
                print(f"   Opening tab: '{tab['text']}'")
                page.mouse.click(tab['cx'], tab['cy'])
                page.wait_for_timeout(2500)
                ss(page, f"v3_tab_{tab['text'][:20].replace(' ','_')}")
                tab_raw = extract_all_fields(page)
                tab_fields = {k: v for k, v in tab_raw.get('fields',{}).items()
                              if v and len(v) < 300}
                all_tab_data[tab['text']] = tab_fields
                for k, v in tab_fields.items():
                    print(f"     [{tab['text']}] {k}: {v}")

            # ── Write final report ─────────────────────────────────────────────
            report = {
                "part_number": PART_NUMBER,
                "page_title": page.title(),
                "page_url": page.url,
                "headings": headings,
                "primary_fields": {k:v for k,v in fields.items() if v and not v.startswith("Starts with")},
                "tab_sections": all_tab_data,
            }
            report_path = SS_DIR / "pim_report_v3.json"
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False)

            print(f"\n[report] {report_path}")
            print(f"Primary fields: {len(report['primary_fields'])}")
            print(f"Tabs captured: {list(all_tab_data.keys())}")

        except Exception as e:
            print(f"\nERROR: {e}")
            ss(page, "v3_error")
            import traceback; traceback.print_exc()
        finally:
            browser.close()

if __name__ == "__main__":
    main()
