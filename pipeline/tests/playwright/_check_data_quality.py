"""Focused single-page DBI + tooltip-coverage check (Data Quality)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import test_dbi_tooltip as t
from playwright.sync_api import sync_playwright

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(viewport={"width": 1600, "height": 1000})
    page = ctx.new_page()
    page.set_default_timeout(10000)
    rep = t._run_one(page, "/Data_Quality", "Data Quality", False)
    browser.close()

print("RESULT:")
print(f"  passed={rep.passed}")
print(f"  card_present={rep.card_present} popover={rep.popover_opens} src={rep.popover_shows_source}")
print(f"  metrics={rep.metrics_with_help}/{rep.metrics_total}  missing_help={rep.metrics_missing_help}")
print(f"  error={rep.error!r}")
sys.exit(0 if rep.passed else 1)
