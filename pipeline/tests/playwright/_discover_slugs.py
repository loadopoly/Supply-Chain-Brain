"""Discover Streamlit nav URL slugs by scraping sidebar links."""
from playwright.sync_api import sync_playwright
import json

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    page = b.new_context().new_page()
    page.goto("http://localhost:8502/", wait_until="domcontentloaded", timeout=30000)
    page.wait_for_selector('[data-testid="stSidebar"]', timeout=20000)
    page.wait_for_timeout(2500)
    # Expand any collapsed nav sections
    page.evaluate("""() => {
        document.querySelectorAll('[data-testid="stSidebar"] details').forEach(d => d.open = true);
        document.querySelectorAll('[data-testid="stSidebar"] [aria-expanded="false"]').forEach(b => b.click());
    }""")
    page.wait_for_timeout(800)
    links = page.eval_on_selector_all(
        '[data-testid="stSidebar"] a',
        """els => els.map(e => ({
            text: (e.innerText || '').trim(),
            href: e.getAttribute('href') || ''
        })).filter(o => o.href.startsWith('/') || o.href.includes('localhost'))""",
    )
    print(json.dumps(links, indent=2))
    from pathlib import Path
    Path(__file__).parent.joinpath("_slugs.json").write_text(json.dumps(links, indent=2), encoding="utf-8")
    print(f"Wrote {len(links)} links to _slugs.json")
    b.close()
