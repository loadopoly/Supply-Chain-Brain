"""Diagnostic: mirror the exact test JS to debug expander detection failure."""
import json
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1600, "height": 1000})
    page.set_default_timeout(15000)
    page.goto("http://localhost:8502/Supply_Chain_Brain", wait_until="domcontentloaded")
    page.wait_for_selector('[data-testid="stAppViewContainer"]', timeout=15000)
    # Wait for DBI card (like the test does)
    try:
        page.wait_for_selector('[data-testid="dbi-card"]', timeout=25000)
        print("DBI card found")
    except Exception:
        print("DBI card not found, continuing anyway")
    # Wait for metric
    try:
        page.wait_for_selector('[data-testid="stMetric"]', timeout=15000)
        print("stMetric found")
    except Exception:
        print("stMetric not found")

    # Mirror the exact test JS
    info = page.evaluate("""() => {
        const metrics = Array.from(document.querySelectorAll(
            '[data-testid="stMetric"]'
        )).filter(c => {
            const lbl = c.querySelector('[data-testid="stMetricLabel"]');
            return lbl && lbl.innerText.trim().length > 0;
        });
        const missing = [];
        let withHelp = 0;
        const debug = [];
        metrics.forEach(m => {
            let ancestor = m.parentElement;
            let hasExpander = false;
            let ancestorTestids = [];
            for (let d = 0; d < 8 && ancestor && ancestor.tagName !== 'BODY'; d++) {
                const tid = ancestor.getAttribute('data-testid') || '(none)';
                const found = !!ancestor.querySelector('[data-testid="stExpander"]');
                ancestorTestids.push({d, tid, found});
                if (found) {
                    hasExpander = true;
                    break;
                }
                ancestor = ancestor.parentElement;
            }
            const labelEl = m.querySelector('[data-testid="stMetricLabel"]');
            const label = labelEl ? labelEl.innerText.trim() : '(unknown)';
            debug.push({label, hasExpander, ancestorTestids});
            if (hasExpander) withHelp += 1;
            else missing.push(label);
        });
        return {total: metrics.length, withHelp, missing, debug};
    }""")
    print(json.dumps(info, indent=2))
    browser.close()
