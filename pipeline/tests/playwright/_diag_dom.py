"""Quick diagnostic: find the data-testid structure around stMetric on Supply Chain Brain."""
import json
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1600, "height": 1000})
    page.set_default_timeout(15000)
    page.goto("http://localhost:8502/Supply_Chain_Brain", wait_until="domcontentloaded")
    page.wait_for_selector('[data-testid="stAppViewContainer"]', timeout=15000)
    try:
        page.wait_for_selector('[data-testid="stMetric"]', timeout=15000)
    except Exception:
        print("No stMetric found in 15s")
    info = page.evaluate("""() => {
        const m = document.querySelector('[data-testid="stMetric"]');
        if (!m) return {metric: null, ancestors: []};
        const ancestors = [];
        let el = m.parentElement;
        for (let i = 0; i < 10 && el && el.tagName !== 'BODY'; i++) {
            const testid = el.getAttribute('data-testid') || '(none)';
            const childTestids = Array.from(el.querySelectorAll('[data-testid]'))
                .map(c => c.getAttribute('data-testid'))
                .slice(0, 30);
            ancestors.push({level: i, testid, childTestids});
            el = el.parentElement;
        }
        // Also: all testids in the whole page
        const allTestids = [...new Set(
            Array.from(document.querySelectorAll('[data-testid]'))
                .map(c => c.getAttribute('data-testid'))
        )];
        return {metric: m.getAttribute('data-testid'), ancestors, allTestids};
    }""")
    print(json.dumps(info, indent=2))
    browser.close()
