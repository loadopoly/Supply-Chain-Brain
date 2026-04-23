"""Diagnostic: check expander detection on Supply Chain Brain."""
import json
import sys

sys.stdout.reconfigure(encoding="utf-8")

from playwright.sync_api import sync_playwright

JS = """() => {
    const metrics = Array.from(document.querySelectorAll('[data-testid="stMetric"]'))
        .filter(c => {
            const lbl = c.querySelector('[data-testid="stMetricLabel"]');
            return lbl && lbl.innerText.trim().length > 0;
        });
    const missing = [];
    let withHelp = 0;
    const debug = [];
    metrics.forEach(m => {
        let ancestor = m.parentElement;
        let hasExpander = false;
        const chain = [];
        for (let d = 0; d < 8 && ancestor && ancestor.tagName !== 'BODY'; d++) {
            const tid = ancestor.getAttribute('data-testid') || '(none)';
            const found = !!ancestor.querySelector('[data-testid="stExpander"]');
            chain.push(tid + (found ? '*FOUND*' : ''));
            if (found) { hasExpander = true; break; }
            ancestor = ancestor.parentElement;
        }
        const labelEl = m.querySelector('[data-testid="stMetricLabel"]');
        const label = labelEl ? labelEl.innerText.trim().replace(/[^\\x20-\\x7E]/g, '?') : '(unknown)';
        debug.push({label, hasExpander, chain});
        if (hasExpander) withHelp += 1;
        else missing.push(label);
    });
    return {total: metrics.length, withHelp,
            missing: missing.map(s => s.replace(/[^\\x20-\\x7E]/g, '?')), debug};
}"""

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1600, "height": 1000})
    page.set_default_timeout(20000)
    page.goto("http://localhost:8502/Supply_Chain_Brain", wait_until="domcontentloaded")
    page.wait_for_selector('[data-testid="stAppViewContainer"]', timeout=15000)
    try:
        page.wait_for_selector('[data-testid="dbi-card"]', timeout=25000)
        print("DBI card found", flush=True)
    except Exception:
        print("DBI card not found", flush=True)
    try:
        page.wait_for_selector('[data-testid="stMetric"]', timeout=15000)
        print("stMetric found", flush=True)
    except Exception:
        print("stMetric NOT found", flush=True)

    info = page.evaluate(JS)
    result = json.dumps(info, indent=2, ensure_ascii=True)
    print(result)
    # Also write to file
    with open("tests/playwright/_diag_out.json", "w", encoding="utf-8") as f:
        f.write(result)
    browser.close()
