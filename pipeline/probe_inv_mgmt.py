"""
Navigate to Inventory Management (Classic) tile and dump the resulting dashboard.
"""
import json, sys
from pathlib import Path
from playwright.sync_api import sync_playwright

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HOST = "https://fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com"
SESSION_FILE = Path(__file__).parent / "oracle_session.json"
SCREENSHOTS = Path(__file__).parent / "abc_screenshots"
SCREENSHOTS.mkdir(exist_ok=True)


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


TILE_CLICK_JS = """
() => {
    const targets = Array.from(document.querySelectorAll('span, div, td, li, p'));
    for (const el of targets) {
        if (!el.offsetParent) continue;
        const t = el.textContent.trim().replace(/\\s+/g, ' ');
        if (t !== 'Inventory Management (Classic)') continue;
        const r = el.getBoundingClientRect();
        if (r.y < 290) continue;
        // Walk up to find A or button tile container
        let ancestor = el.parentElement;
        for (let i = 0; i < 12 && ancestor; i++) {
            const tag = ancestor.tagName;
            if (tag === 'A' || tag === 'BUTTON') {
                ancestor.click();
                const ar = ancestor.getBoundingClientRect();
                return {found: true, via: tag,
                        cx: Math.round(ar.x + ar.width/2),
                        cy: Math.round(ar.y + ar.height/2),
                        href: ancestor.href || ''};
            }
            // Also check role=button or onclick
            const role = ancestor.getAttribute('role') || '';
            const onclick = ancestor.getAttribute('onclick') || '';
            if (role === 'button' || role === 'link' || onclick) {
                ancestor.click();
                const ar = ancestor.getBoundingClientRect();
                return {found: true, via: 'role=' + role || 'onclick',
                        cx: Math.round(ar.x + ar.width/2),
                        cy: Math.round(ar.y + ar.height/2)};
            }
            ancestor = ancestor.parentElement;
        }
        // Fallback: click the text element itself
        el.click();
        return {found: true, via: 'self', cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)};
    }
    // If not found, dump what text is in content area (y > 290) for debug
    const debug = [];
    const seen = new Set();
    for (const el of document.querySelectorAll('span, div, a, button')) {
        if (!el.offsetParent) continue;
        const t = el.textContent.trim().replace(/\\s+/g, ' ');
        if (!t || t.length > 50 || seen.has(t)) continue;
        const r = el.getBoundingClientRect();
        if (r.y < 290 || r.height < 8) continue;
        if (t.toLowerCase().includes('inventory') || t.toLowerCase().includes('classic')) {
            seen.add(t);
            debug.push({tag: el.tagName, text: t, cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)});
        }
    }
    return {found: false, debug: debug};
}
"""

DUMP_JS = """
() => {
    const seen = new Set();
    const results = [];
    for (const el of document.querySelectorAll('a, button, li, td, span, div')) {
        if (!el.offsetParent) continue;
        if (el.childElementCount > 2) continue;
        const t = el.textContent.trim().replace(/\\s+/g, ' ');
        if (!t || t.length < 3 || t.length > 100 || seen.has(t)) continue;
        const r = el.getBoundingClientRect();
        if (r.height < 8 || r.width < 8) continue;
        seen.add(t);
        results.push({tag: el.tagName, text: t.slice(0, 70),
                      cx: Math.round(r.x + r.width/2),
                      cy: Math.round(r.y + r.height/2),
                      href: el.tagName === 'A' ? (el.href || '').slice(0, 100) : ''});
    }
    return results;
}
"""

ABC_KEYWORDS = ['abc', 'categor assign', 'item categor', 'classif', 'abc analysis', 'abc class']


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=80)
        ctx = browser.new_context(viewport={"width": 1600, "height": 900})
        page = ctx.new_page()
        load_cookies(ctx)

        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="networkidle", timeout=120_000)
        page.wait_for_timeout(2000)
        print("Home loaded.")

        # Step 1: Click Supply Chain Execution tab
        page.locator("a:has-text('Supply Chain Execution')").first.click(timeout=10_000)
        page.wait_for_timeout(3000)
        print("Supply Chain Execution tab clicked.")

        # Step 2: Set up a popup listener BEFORE clicking the tile
        new_page_holder = []
        ctx.on("page", lambda p: new_page_holder.append(p))

        # Click the tile via real mouse at the known A element position
        # The A element for the tile is at approx [1007, 415] in the screenshot
        # But we need to find it by text
        tile_result = page.evaluate("""
            () => {
                // Find the A element whose text content includes 'Inventory Management' and '(Classic)'
                for (const el of document.querySelectorAll('a, button')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim().replace(/\\s+/g, ' ');
                    if (!t.includes('Inventory Management') || !t.includes('Classic')) continue;
                    const r = el.getBoundingClientRect();
                    if (r.y < 290) continue;
                    return {cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2),
                            tag: el.tagName, text: t.slice(0,50), href: (el.href||'').slice(0,80)};
                }
                // Also try: just click at screen position of the known tile
                return {fallback: true};
            }
        """)
        print(f"Tile A/button search: {tile_result}")

        if tile_result.get("cx"):
            # Use real mouse click at the A element's position
            page.mouse.click(tile_result["cx"], tile_result["cy"])
        else:
            # Fallback: click at known tile center from screenshot ~[1007, 413]
            page.mouse.click(1007, 413)
        print("Tile clicked via mouse.")

        # Wait a bit for new window to open
        page.wait_for_timeout(4000)

        # Check if new page opened
        all_pages = ctx.pages
        print(f"Pages in context: {len(all_pages)}")
        for i, pg in enumerate(all_pages):
            print(f"  Page {i}: {pg.url[:100]}")

        # Work with whichever page has the Inventory Management content
        target_page = page
        for pg in all_pages:
            try:
                pg.wait_for_load_state("domcontentloaded", timeout=5000)
                title = pg.title()
                url = pg.url
                if "FuseWelcome" not in url or "inv" in url.lower() or "inv" in title.lower():
                    print(f"  -> Using page: {title[:60]} | {url[:80]}")
                    target_page = pg
                    break
            except Exception:
                pass

        target_page.wait_for_load_state("networkidle", timeout=30_000)
        target_page.wait_for_timeout(5000)
        target_page.screenshot(path=str(SCREENSHOTS / "probe_inv_target.png"))

        t_title = target_page.title()
        t_url = target_page.url
        print(f"\nTarget page title: {t_title}")
        print(f"Target page URL: {t_url[:150]}")

        # Open task panel
        for sel in ["[title='Tasks']", "[aria-label='Tasks']", "a:has-text('Tasks')"]:
            try:
                el = target_page.locator(sel).first
                if el.is_visible(timeout=2000):
                    el.click()
                    target_page.wait_for_timeout(1500)
                    print(f"Task panel opened via: {sel}")
                    break
            except Exception:
                pass

        # Expand collapsed sections
        target_page.evaluate("""
            () => {
                for (const el of document.querySelectorAll(
                    '[class*="af_showDetailHeader"], [class*="p_AFCollapsed"], [id*="sdh"]'
                )) {
                    if (el.offsetParent) { try { el.click(); } catch(e) {} }
                }
            }
        """)
        target_page.wait_for_timeout(1500)
        target_page.screenshot(path=str(SCREENSHOTS / "probe_inv_with_tasks.png"))

        # Inspect the task panel tab bar area specifically
        print("\n--- Task panel tab bar DOM inspection ---")
        tab_bar_info = target_page.evaluate("""
            () => {
                const results = [];
                for (const el of document.querySelectorAll('*')) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    if (r.y < 90 || r.y > 130) continue;
                    if (r.x < 1100) continue;
                    const t = el.textContent.trim().replace(/\\s+/g, ' ');
                    if (!t) continue;
                    results.push({
                        tag: el.tagName,
                        text: t.slice(0, 80),
                        id: el.id || '',
                        cls: (el.className || '').toString().slice(0, 60),
                        cx: Math.round(r.x + r.width/2),
                        cy: Math.round(r.y + r.height/2),
                        w: Math.round(r.width),
                        h: Math.round(r.height),
                        children: el.childElementCount,
                        href: el.tagName === 'A' ? (el.href || '') : ''
                    });
                }
                return results;
            }
        """)
        for e in tab_bar_info:
            print(f"  [{e['cx']:4d},{e['cy']:4d}] w={e['w']:3d} <{e['tag']:6s}> [{e['cls'][:40]}] '{e['text'][:60]}' {e['href'][:60]}")

        # Dump current task panel (Inventory tab)
        all_els = target_page.evaluate(DUMP_JS)
        print(f"\n=== Inventory tab tasks ({len(all_els)}) ===")
        for e in all_els:
            lo = e['text'].lower()
            star = "***" if any(k in lo for k in ABC_KEYWORDS) else "   "
            if e['cx'] > 1200:  # Only task panel area
                href = f" -> {e['href']}" if e['href'] else ""
                print(f"  {star} [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> {e['text']}{href}")

        # Get hrefs of task links and check for hidden LI elements
        print("\n--- Clicking 'Manage Item Quantities' to see URL it navigates to ---")
        # Click the first task to see how ADF navigation works
        target_page.locator('li').filter(has_text='Manage Item Quantities').first.click()
        target_page.wait_for_timeout(3000)
        nav_url = target_page.url
        nav_title = target_page.title()
        print(f"After click: {nav_title}")
        print(f"URL: {nav_url[:200]}")
        target_page.screenshot(path=str(SCREENSHOTS / "probe_inv_manage_items_click.png"))

        # Go back to Inventory Management overview
        target_page.go_back()
        target_page.wait_for_timeout(2000)

        # Try global search for "ABC" while in Inventory Management context
        print("\n--- Global search for ABC ---")
        try:
            # Click global search
            for sel in ["input[id*='pGlSrch']", "[title='Search'] input", "input[aria-label*='earch']",
                        "input[placeholder*='earch']", ".xpj input"]:
                try:
                    el = target_page.locator(sel).first
                    if el.is_visible(timeout=2000):
                        el.click()
                        target_page.wait_for_timeout(500)
                        el.fill("ABC Classification")
                        target_page.keyboard.press("Enter")
                        target_page.wait_for_timeout(3000)
                        print(f"Search done via: {sel}")
                        break
                except Exception:
                    pass
            target_page.screenshot(path=str(SCREENSHOTS / "probe_inv_search_abc.png"))

            # Dump search results
            search_els = target_page.evaluate("""
                () => {
                    const results = [];
                    const seen = new Set();
                    for (const el of document.querySelectorAll('a, li, span, div')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim().replace(/\\s+/g, ' ');
                        if (!t || t.length < 3 || t.length > 100 || seen.has(t)) continue;
                        if (el.childElementCount > 2) continue;
                        seen.add(t);
                        results.push({tag: el.tagName, text: t.slice(0, 80),
                                      cx: Math.round(el.getBoundingClientRect().x),
                                      cy: Math.round(el.getBoundingClientRect().y)});
                    }
                    return results.filter(e => {
                        const lo = e.text.toLowerCase();
                        return lo.includes('abc') || lo.includes('classif') || lo.includes('categor assign');
                    });
                }
            """)
            if search_els:
                print("ABC-related search results:")
                for e in search_els:
                    print(f"  [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> {e['text']}")
            else:
                print("No ABC results found in search")
        except Exception as ex:
            print(f"Search error: {ex}")

        # Try the Navigator hamburger from within Inventory Management page
        print("\n--- Navigator hamburger from Inventory Management ---")
        try:
            target_page.mouse.click(24, 57)
            target_page.wait_for_timeout(2000)
            target_page.screenshot(path=str(SCREENSHOTS / "probe_inv_navigator.png"))

            nav_els = target_page.evaluate("""
                () => {
                    const results = [];
                    const seen = new Set();
                    for (const el of document.querySelectorAll('a, li, span')) {
                        if (!el.offsetParent) continue;
                        const t = el.textContent.trim().replace(/\\s+/g, ' ');
                        if (!t || t.length < 3 || t.length > 80 || seen.has(t)) continue;
                        const r = el.getBoundingClientRect();
                        if (r.x > 400) continue;  // Left side only
                        seen.add(t);
                        results.push({tag: el.tagName, text: t,
                                      cx: Math.round(r.x + r.width/2),
                                      cy: Math.round(r.y + r.height/2)});
                    }
                    return results;
                }
            """)
            print(f"Navigator items ({len(nav_els)}):")
            for e in nav_els:
                lo = e['text'].lower()
                star = "***" if any(k in lo for k in ABC_KEYWORDS) else "   "
                print(f"  {star} [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> {e['text']}")
        except Exception as ex:
            print(f"Navigator error: {ex}")

        browser.close()
        task_hrefs = target_page.evaluate("""
            () => {
                const results = [];
                // Get ALL LI and their child A elements - ignore offsetParent to get hidden ones too
                for (const li of document.querySelectorAll('li')) {
                    const rect = li.getBoundingClientRect();
                    if (rect.x < 1100) continue;
                    const a = li.querySelector('a');
                    const t = li.textContent.trim().replace(/\\s+/g, ' ');
                    if (!t || t.length > 100) continue;
                    results.push({
                        text: t,
                        visible: !!li.offsetParent,
                        href: a ? (a.href || '').slice(0, 150) : '',
                        cy: Math.round(rect.y + rect.height/2)
                    });
                }
                return results;
            }
        """)
        for e in task_hrefs:
            vis = "V" if e['visible'] else "H"
            print(f"  {vis} [y={e['cy']:4d}] {e['text']}")
            if e['href']:
                print(f"         -> {e['href']}")

        # Now try to find any hidden section headers (DIVs with section titles)
        print("\n--- All section headers in task panel (visible + hidden) ---")
        sections = target_page.evaluate("""
            () => {
                const results = [];
                for (const el of document.querySelectorAll('div, span')) {
                    const rect = el.getBoundingClientRect();
                    if (rect.x < 1100 || rect.width < 100) continue;
                    if (el.childElementCount > 0) continue;  // leaf nodes only
                    const t = el.textContent.trim();
                    if (!t || t.length > 50 || t.length < 3) continue;
                    if (el.childElementCount > 0) continue;
                    // Check if it looks like a section header (not a task)
                    const cls = (el.className || '').toString();
                    if (!cls.includes('xmu') && !cls.includes('header') &&
                        !cls.includes('section') && !cls.includes('group')) continue;
                    results.push({
                        text: t,
                        visible: !!el.offsetParent,
                        cls: cls.slice(0, 40),
                        cy: Math.round(rect.y)
                    });
                }
                return results;
            }
        """)
        for s in sections:
            vis = "V" if s['visible'] else "H"
            print(f"  {vis} [y={s['cy']:4d}] '{s['text']}' [{s['cls']}]")

        browser.close()
        print("\n--- Scrolling task panel inner container ---")
        scroll_info = target_page.evaluate("""
            () => {
                // Find the task panel's scroll container (af_showDetailItem body)
                const containers = [];
                for (const el of document.querySelectorAll('*')) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    if (r.x < 1150 || r.width < 200) continue;
                    if (el.scrollHeight > el.clientHeight + 10) {
                        containers.push({
                            tag: el.tagName,
                            id: el.id || '',
                            cls: (el.className || '').toString().slice(0, 50),
                            scrollH: el.scrollHeight,
                            clientH: el.clientHeight,
                            cx: Math.round(r.x + r.width/2),
                            cy: Math.round(r.y + r.height/2)
                        });
                    }
                }
                return containers;
            }
        """)
        print("Scrollable containers in task panel area:")
        for c in scroll_info:
            print(f"  [{c['cx']:4d},{c['cy']:4d}] <{c['tag']:6s}> scrollH={c['scrollH']} clientH={c['clientH']} [{c['cls']}]")

        # Scroll each container down to reveal more content
        target_page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('*')) {
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    if (r.x < 1150 || r.width < 200) continue;
                    if (el.scrollHeight > el.clientHeight + 10) {
                        el.scrollTop = el.scrollHeight;
                    }
                }
            }
        """)
        target_page.wait_for_timeout(1000)
        target_page.screenshot(path=str(SCREENSHOTS / "probe_inv_panel_scrolled.png"))

        # Dump all LI/A in task panel after scrolling
        scrolled_tasks = target_page.evaluate("""
            () => {
                const results = [];
                const seen = new Set();
                for (const el of document.querySelectorAll('li, a, div')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim().replace(/\\s+/g, ' ');
                    if (!t || t.length < 3 || t.length > 100 || seen.has(t)) continue;
                    if (el.childElementCount > 2) continue;
                    const r = el.getBoundingClientRect();
                    if (r.x < 1150) continue;
                    seen.add(t);
                    results.push({tag: el.tagName, text: t, cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)});
                }
                return results;
            }
        """)
        print(f"\n=== After container scroll ({len(scrolled_tasks)} elements) ===")
        for e in scrolled_tasks:
            lo = e['text'].lower()
            star = "***" if any(k in lo for k in ABC_KEYWORDS) else "   "
            print(f"  {star} [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> {e['text']}")

        # Also dump the FULL task list from the DOM (even off-screen elements)
        print("\n=== FULL task list (including off-screen) ===")
        full_tasks = target_page.evaluate("""
            () => {
                const results = [];
                const seen = new Set();
                // Get ALL LI elements in the task panel regardless of visibility
                for (const el of document.querySelectorAll('li')) {
                    const t = el.textContent.trim().replace(/\\s+/g, ' ');
                    if (!t || t.length < 3 || t.length > 100 || seen.has(t)) continue;
                    const r = el.getBoundingClientRect();
                    if (r.x < 1100) continue;  // must be in right panel area
                    seen.add(t);
                    results.push({
                        text: t,
                        visible: !!el.offsetParent,
                        cx: Math.round(r.x + r.width/2),
                        cy: Math.round(r.y + r.height/2)
                    });
                }
                return results;
            }
        """)
        for e in full_tasks:
            lo = e['text'].lower()
            star = "***" if any(k in lo for k in ABC_KEYWORDS) else "   "
            vis = "V" if e['visible'] else "H"
            print(f"  {star} {vis} [{e['cx']:4d},{e['cy']:4d}] {e['text']}")
            print(f"\n--- Selecting task panel: {tab_name} ---")
            try:
                # Use Playwright's native select_option (triggers ADF event handlers)
                sel = target_page.locator('select').filter(has_text='Inventory').first
                sel.select_option(label=tab_name)
                target_page.wait_for_timeout(2000)
                target_page.screenshot(path=str(SCREENSHOTS / f"probe_inv_tab_{tab_name.lower()}.png"))

                # Dump all task panel elements (x > 1100)
                tab_els = target_page.evaluate("""
                    () => {
                        const results = [];
                        const seen = new Set();
                        for (const el of document.querySelectorAll('li, a, div, span')) {
                            if (!el.offsetParent) continue;
                            const t = el.textContent.trim().replace(/\\s+/g, ' ');
                            if (!t || t.length < 3 || t.length > 100 || seen.has(t)) continue;
                            if (el.childElementCount > 3) continue;
                            const r = el.getBoundingClientRect();
                            if (r.x < 1100) continue;
                            seen.add(t);
                            results.push({tag: el.tagName, text: t, cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)});
                        }
                        return results;
                    }
                """)
                for e in tab_els:
                    lo = e['text'].lower()
                    star = "***" if any(k in lo for k in ABC_KEYWORDS) else "   "
                    print(f"  {star} [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> {e['text']}")
            except Exception as ex:
                print(f"  Error: {ex}")

        # Also scroll down in the current view to find more tasks
        print("\n--- Scrolling task panel to find more sections ---")
        target_page.evaluate("""
            () => {
                // Find the task panel scrollable container and scroll it
                const panels = document.querySelectorAll('[class*="task"], [id*="task"], [class*="panel"]');
                for (const p of panels) {
                    if (p.scrollHeight > p.clientHeight) {
                        p.scrollTop = p.scrollHeight;
                    }
                }
                // Also try scrolling right side of page
                window.scrollTo(0, 0);
            }
        """)
        target_page.wait_for_timeout(1000)

        # Try keyboard scroll on task panel area
        target_page.mouse.move(1379, 400)
        for _ in range(10):
            target_page.mouse.wheel(0, 300)
            target_page.wait_for_timeout(200)
        target_page.wait_for_timeout(500)
        target_page.screenshot(path=str(SCREENSHOTS / "probe_inv_scrolled.png"))

        # Final dump after scroll
        scrolled_els = target_page.evaluate("""
            () => {
                const results = [];
                const seen = new Set();
                for (const el of document.querySelectorAll('li, a, div')) {
                    if (!el.offsetParent) continue;
                    const t = el.textContent.trim().replace(/\\s+/g, ' ');
                    if (!t || t.length < 3 || t.length > 100 || seen.has(t)) continue;
                    const r = el.getBoundingClientRect();
                    if (r.x < 1100) continue;
                    seen.add(t);
                    results.push({tag: el.tagName, text: t, cx: Math.round(r.x+r.width/2), cy: Math.round(r.y+r.height/2)});
                }
                return results;
            }
        """)
        print(f"\n=== After scroll ({len(scrolled_els)} elements in task panel area) ===")
        for e in scrolled_els:
            lo = e['text'].lower()
            star = "***" if any(k in lo for k in ABC_KEYWORDS) else "   "
            print(f"  {star} [{e['cx']:4d},{e['cy']:4d}] <{e['tag']:4s}> {e['text']}")

        browser.close()


if __name__ == "__main__":
    main()
