"""
fetch_pim_80446_v2.py - Fetch PIM data for 80446-04 using correct tile names from DEV13 screenshot
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

def click_by_text(page, text, exact=True, selectors="a,button,span,div,li"):
    coords = page.evaluate("""
        ([txt, exact, sel]) => {
            for (const el of document.querySelectorAll(sel)) {
                if (!el.offsetParent) continue;
                const t = el.textContent.trim().replace(/\\s+/g,' ');
                const matches = exact ? t === txt : t.includes(txt);
                if (!matches) continue;
                const r = el.getBoundingClientRect();
                if (r.width < 2 || r.height < 2) continue;
                el.scrollIntoView({block:'center', behavior:'instant'});
                const r2 = el.getBoundingClientRect();
                return {cx: Math.round(r2.x+r2.width/2), cy: Math.round(r2.y+r2.height/2)};
            }
            return null;
        }
    """, [text, exact, selectors])
    if coords:
        page.mouse.click(coords['cx'], coords['cy'])
    return coords

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()

        try:
            load_session(context)

            # ── Step 1: FuseWelcome ───────────────────────────────────────────
            print("1. FuseWelcome...")
            page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)
            ss(page, "01_welcome")

            # ── Step 2: Product Management tab ───────────────────────────────
            print("2. Click Product Management tab...")
            res = click_by_text(page, "Product Management", selectors="a,button,span,div")
            if not res:
                print("   ERROR: tab not found")
            page.wait_for_timeout(2500)
            ss(page, "02_pm_tab")

            # Show More / Show Less doesn't matter - we can see tiles already
            # ── Step 3: Click "Commercial Items" tile (PIM item search) ───────
            print("3. Click 'Commercial Items' tile...")
            # From the screenshot: Commercial Items, Browse Commercial Items are visible
            for tile in ["Commercial Items", "Browse Commercial Items", "Items",
                         "Item Catalogs", "Engineering Items"]:
                res = click_by_text(page, tile, selectors="a,button,div")
                if res:
                    print(f"   Clicked tile: '{tile}'")
                    page.wait_for_timeout(5000)
                    try:
                        page.wait_for_load_state("domcontentloaded", timeout=15000)
                    except Exception:
                        pass
                    page.wait_for_timeout(2000)
                    ss(page, "03_item_list")
                    break
            else:
                print("   No item tile found")
                ss(page, "03_no_tile")

            print(f"   Title: {page.title()[:70]}")
            print(f"   URL:   {page.url[:100]}")

            # ── Step 4: Search for 80446-04 ──────────────────────────────────
            print(f"\n4. Search for '{PART_NUMBER}'...")
            ss(page, "04_before_search")

            # Dump all visible inputs
            inputs = page.evaluate("""
                () => {
                    const res = [];
                    for (const el of document.querySelectorAll('input')) {
                        if (!el.offsetParent) continue;
                        const r = el.getBoundingClientRect();
                        res.push({
                            placeholder: el.getAttribute('placeholder') || '',
                            title: el.getAttribute('title') || '',
                            name: el.getAttribute('name') || '',
                            id: el.getAttribute('id') || '',
                            cx: Math.round(r.x+r.width/2),
                            cy: Math.round(r.y+r.height/2)
                        });
                    }
                    return res;
                }
            """)
            print(f"   Visible inputs: {inputs[:8]}")

            # Try to type in search
            search_input = page.evaluate("""
                () => {
                    const keywords = ['item', 'part', 'search', 'keyword', 'number', 'name', 'query'];
                    for (const el of document.querySelectorAll('input')) {
                        if (!el.offsetParent) continue;
                        const attrs = [
                            el.getAttribute('placeholder') || '',
                            el.getAttribute('title') || '',
                            el.getAttribute('name') || '',
                            el.getAttribute('id') || ''
                        ].join(' ').toLowerCase();
                        if (keywords.some(k => attrs.includes(k))) {
                            const r = el.getBoundingClientRect();
                            return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2), attrs};
                        }
                    }
                    // Fallback: first visible text input
                    for (const el of document.querySelectorAll('input[type="text"],input:not([type])')) {
                        if (!el.offsetParent) continue;
                        const r = el.getBoundingClientRect();
                        if (r.width > 50) return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2), attrs: 'fallback'};
                    }
                    return null;
                }
            """)

            if search_input:
                print(f"   Search field: attrs='{search_input['attrs']}' at ({search_input['cx']},{search_input['cy']})")
                page.mouse.click(search_input['cx'], search_input['cy'])
                page.wait_for_timeout(300)
                page.keyboard.press("Control+A")
                page.keyboard.type(PART_NUMBER, delay=40)
                page.wait_for_timeout(500)
                page.keyboard.press("Enter")
                page.wait_for_timeout(3000)
                ss(page, "05_after_search")
            else:
                print("   No search input found")
                ss(page, "05_no_input")

            # Also try clicking Search/Go button
            for btn_txt in ["Search", "Go", "Find"]:
                res2 = page.evaluate(f"""
                    () => {{
                        for (const btn of document.querySelectorAll('button,a,input[type="submit"]')) {{
                            if (!btn.offsetParent) continue;
                            const t = btn.textContent.trim() || btn.getAttribute('value') || '';
                            if (t === '{btn_txt}') {{
                                const r = btn.getBoundingClientRect();
                                return {{cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)}};
                            }}
                        }}
                        return null;
                    }}
                """)
                if res2:
                    print(f"   Clicking '{btn_txt}' button")
                    page.mouse.click(res2['cx'], res2['cy'])
                    page.wait_for_timeout(3000)
                    ss(page, "05b_search_btn")
                    break

            # ── Step 5: Click result ──────────────────────────────────────────
            print(f"\n5. Click result for '{PART_NUMBER}'...")
            ss(page, "06_results")

            for attempt in range(5):
                result = page.evaluate("""
                    ([pn]) => {
                        for (const el of document.querySelectorAll('a,td,span')) {
                            if (!el.offsetParent) continue;
                            const t = el.textContent.trim();
                            if (!t.includes(pn)) continue;
                            const r = el.getBoundingClientRect();
                            if (r.width < 20 || r.height < 8) continue;
                            return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2), text: t.slice(0,80)};
                        }
                        return null;
                    }
                """, [PART_NUMBER])

                if result:
                    print(f"   Clicking: '{result['text']}'")
                    page.mouse.click(result['cx'], result['cy'])
                    page.wait_for_timeout(4000)
                    ss(page, "07_item_detail")
                    break
                page.wait_for_timeout(1000)
            else:
                print(f"   Part not found in results - check 06_results.png")

            # ── Step 6: Extract ALL PIM fields ───────────────────────────────
            print(f"\n6. Extracting all PIM data from item detail...")
            ss(page, "08_detail_page")
            print(f"   Title: {page.title()[:70]}")

            pim_data = page.evaluate("""
                () => {
                    const fields = {};

                    // Table rows (most common Oracle ADF layout)
                    for (const row of document.querySelectorAll('tr')) {
                        const cells = row.querySelectorAll('td');
                        if (cells.length >= 2) {
                            const label = cells[0]?.textContent.trim().replace(/\\s+/g,' ');
                            const value = cells[1]?.textContent.trim().replace(/\\s+/g,' ');
                            if (label && label.length < 100 && value && value.length < 300) {
                                fields[label] = value;
                            }
                        }
                    }

                    // Input fields with labels
                    for (const inp of document.querySelectorAll('input,select,textarea')) {
                        if (!inp.offsetParent) continue;
                        const id = inp.getAttribute('id') || '';
                        let label = '';
                        if (id) {
                            const lbl = document.querySelector(`label[for="${id}"]`);
                            if (lbl) label = lbl.textContent.trim();
                        }
                        if (!label) {
                            const p = inp.closest('td,div,span');
                            if (p) {
                                const prevSib = p.previousElementSibling;
                                if (prevSib) label = prevSib.textContent.trim();
                            }
                        }
                        const value = inp.value || inp.textContent?.trim() || '';
                        if (label && value) fields[label.replace(/\\s+/g,' ')] = value;
                    }

                    // Label/value pattern: spans with class containing Label
                    for (const el of document.querySelectorAll('[class*="Label"],[class*="label"],[class*="field-label"]')) {
                        if (!el.offsetParent) continue;
                        const label = el.textContent.trim().replace(/\\s+/g,' ');
                        if (!label || label.length > 80) continue;
                        const sib = el.nextElementSibling || el.parentElement?.nextElementSibling;
                        if (sib) {
                            const val = sib.textContent.trim().replace(/\\s+/g,' ');
                            if (val && val.length < 300 && val !== label) fields[label] = val;
                        }
                    }

                    // Headings for context
                    const headings = [];
                    for (const h of document.querySelectorAll('h1,h2,h3,h4')) {
                        if (h.offsetParent) headings.push({tag: h.tagName, text: h.textContent.trim()});
                    }

                    return {fields, headings};
                }
            """)

            fields = pim_data.get('fields', {})
            headings = pim_data.get('headings', [])

            print(f"\n   Page headings: {headings}")
            print(f"\n   === PIM Fields ({len(fields)}) ===")
            for k, v in fields.items():
                if v:
                    print(f"   {k}: {v}")

            # ── Step 7: Navigate tabs ─────────────────────────────────────────
            print("\n7. Checking for tabs...")
            tabs = page.evaluate("""
                () => {
                    const res = [];
                    for (const el of document.querySelectorAll('[role="tab"],a[class*="tab"],li[class*="tab"]')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim().replace(/\\s+/g,' ');
                        if (t.length < 2 || t.length > 60) continue;
                        const r = el.getBoundingClientRect();
                        res.push({text: t, cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)});
                    }
                    return res;
                }
            """)
            print(f"   Tabs: {[t['text'] for t in tabs]}")

            all_tab_data = {}
            tab_keywords = ["General", "Specifications", "Units of Measure",
                           "Categories", "Associations", "Attachments",
                           "Manufacturers", "Safety", "Structures",
                           "Item Relationships", "Trade Items", "Packing"]

            for tab in tabs:
                if any(kw.lower() in tab['text'].lower() for kw in tab_keywords):
                    print(f"   Tab: '{tab['text']}'")
                    page.mouse.click(tab['cx'], tab['cy'])
                    page.wait_for_timeout(2500)
                    ss(page, f"tab_{tab['text'][:20].replace(' ','_')}")

                    tab_fields = page.evaluate("""
                        () => {
                            const f = {};
                            for (const row of document.querySelectorAll('tr')) {
                                const cells = row.querySelectorAll('td');
                                if (cells.length >= 2) {
                                    const l = cells[0]?.textContent.trim().replace(/\\s+/g,' ');
                                    const v = cells[1]?.textContent.trim().replace(/\\s+/g,' ');
                                    if (l && l.length < 100 && v) f[l] = v;
                                }
                            }
                            for (const inp of document.querySelectorAll('input,select')) {
                                if (!inp.offsetParent) continue;
                                const id = inp.getAttribute('id');
                                let lbl = '';
                                if (id) {
                                    const l = document.querySelector(`label[for="${id}"]`);
                                    if (l) lbl = l.textContent.trim();
                                }
                                if (lbl && inp.value) f[lbl] = inp.value;
                            }
                            return f;
                        }
                    """)

                    all_tab_data[tab['text']] = tab_fields
                    for k, v in tab_fields.items():
                        if v:
                            print(f"     [{tab['text']}] {k}: {v}")

            # ── Write report ──────────────────────────────────────────────────
            report = {
                "part_number": PART_NUMBER,
                "page_title": page.title(),
                "page_url": page.url,
                "headings": headings,
                "primary_fields": fields,
                "tab_sections": all_tab_data,
            }
            report_path = SS_DIR / "pim_report.json"
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            print(f"\n[report] Saved: {report_path}")
            print(f"\n=== DONE ===")
            print(f"Primary fields: {len(fields)}")
            print(f"Tab sections: {list(all_tab_data.keys())}")
            print(f"Screenshots: {SS_DIR}")

        except Exception as e:
            print(f"\nERROR: {e}")
            ss(page, "error")
            import traceback; traceback.print_exc()
        finally:
            browser.close()

if __name__ == "__main__":
    main()
