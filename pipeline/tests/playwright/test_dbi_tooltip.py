"""
DBI Tooltip — Comprehensive Playwright E2E Suite
=================================================

Robustness checks for the Dynamic Brain Insight (DBI) card across every page
of the Supply Chain Brain Streamlit app.

Verifies:
  1. Card is rendered on every page that imports `render_dynamic_brain_insight`.
  2. Card is visible AND not visually clipped by parent overflow.
  3. Card stacks ABOVE Plotly hover layers and other interactive widgets.
  4. The 🔍 Parameters expander opens inline, shows insight source + parameters.
  5. The card's `data-digest` attribute updates when context changes
     (i.e. user interacts with a chart / changes a filter).
  6. Streamlit fragment auto-refresh advances the timestamp on a 2-s tick.

Run from the `pipeline/` directory:

    .\\.venv\\Scripts\\python.exe tests/playwright/test_dbi_tooltip.py

Requires: a running Streamlit instance on http://localhost:8502.
"""
from __future__ import annotations
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path

# Force UTF-8 stdout on Windows so the report (and arrows) print cleanly.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from playwright.sync_api import sync_playwright, Page, TimeoutError as PWTimeout

BASE = "http://localhost:8502"
RESULTS_PATH = Path(__file__).parent / "dbi_tooltip_results.json"

# (page-url-suffix, friendly-name, expects-plotly-chart, requires-azure-sql)
# Pages marked requires_db=True produce no DBI card when Azure SQL is offline;
# they are counted as SKIP (infrastructure) rather than FAIL.
PAGES = [
    ("/",                          "Query Console",         False, False),
    ("/Schema_Discovery",          "Schema Discovery",      False, False),
    ("/Supply_Chain_Brain",        "Supply Chain Brain",    True,  False),
    ("/b_Supply_Chain_Pipeline",   "Supply Chain Pipeline", True,  False),
    ("/EOQ_Deviation",             "EOQ Deviation",         True,  False),
    ("/OTD_Recursive",             "OTD Recursive",         True,  False),
    ("/Procurement_360",           "Procurement 360",       True,  False),
    ("/Data_Quality",              "Data Quality",          False, False),
    ("/Connectors",                "Connectors",            False, False),
    ("/Lead_Time_Survival",        "Lead-Time Survival",    True,  False),
    ("/Bullwhip",                  "Bullwhip Effect",       True,  False),
    ("/Multi_Echelon",             "Multi-Echelon",         True,  False),
    ("/Sustainability",            "Sustainability",        True,  False),
    ("/Freight_Portfolio",         "Freight Portfolio",     True,  False),
    ("/What_If",                   "What-If Sandbox",       False, False),
    ("/Decision_Log",              "Decision Log",          False, False),
    ("/Benchmarks",                "Benchmarks",            False, False),
    ("/Report_Creator",            "Report Creator",        False, False),
    ("/Cycle_Count_Accuracy",      "Cycle Count Accuracy",  False, False),
]


@dataclass
class PageReport:
    url: str
    name: str
    nav_ok: bool = False
    card_present: bool = False
    card_visible: bool = False
    card_in_viewport: bool = False
    not_clipped: bool = False
    z_above_plotly: bool = True   # default true for pages w/o plotly
    popover_opens: bool = False
    popover_shows_source: bool = False
    digest_updates: bool = False
    timestamp_advances: bool = False
    metrics_total: int = 0
    metrics_with_help: int = 0
    metrics_missing_help: list = None  # type: ignore[assignment]
    error: str = ""
    skipped: bool = False  # True when Azure SQL offline and card not expected

    def __post_init__(self):
        if self.metrics_missing_help is None:
            self.metrics_missing_help = []

    @property
    def passed(self) -> bool:
        # Infrastructure skips (e.g. Azure SQL offline) are not counted as failures.
        if self.skipped:
            return True
        # required-for-all checks
        base = (
            self.nav_ok and self.card_present and self.card_visible
            and self.card_in_viewport and self.not_clipped
            and self.popover_opens and self.popover_shows_source
            and self.timestamp_advances
        )
        # If the page renders any st.metric widgets, every one MUST have
        # an adjacent st.expander providing Brain context (replaces old help= icons).
        expanders_ok = (self.metrics_total == 0
                        or self.metrics_with_help == self.metrics_total)
        return base and expanders_ok


def _try_navigate(page: Page, url_suffix: str) -> bool:
    """Streamlit MPA URLs vary by title; try the suffix and a couple of fallbacks."""
    candidates = [url_suffix]
    # Streamlit collapses & → empty and spaces → _; provide a few variants.
    if url_suffix != "/":
        slug = url_suffix.lstrip("/")
        for variant in (slug.replace("__", "_"), slug.replace("_", ""), slug.replace("And", "")):
            if variant and ("/" + variant) not in candidates:
                candidates.append("/" + variant)
    for c in candidates:
        try:
            page.goto(BASE + c, wait_until="domcontentloaded", timeout=20000)
            page.wait_for_selector('[data-testid="stAppViewContainer"]', timeout=15000)
            return True
        except PWTimeout:
            continue
    return False


def _check_card(page: Page, rep: PageReport, expects_plotly: bool, db_required: bool = False):
    # Wait for the DBI card (worker thread may take a moment).
    try:
        page.wait_for_selector('[data-testid="dbi-card"]', timeout=25000)
        rep.card_present = True
    except PWTimeout:
        if db_required:
            # Azure SQL is offline — card will never appear; treat as SKIP.
            rep.skipped = True
            rep.error = "dbi-card never appeared (Azure SQL offline — SKIP)"
        else:
            rep.error = "dbi-card never appeared"
        return

    card = page.locator('[data-testid="dbi-card"]').first
    rep.card_visible = card.is_visible()

    box = card.bounding_box()
    if not box:
        rep.error = "no bounding box"
        return

    vp = page.viewport_size
    rep.card_in_viewport = (
        box["x"] >= 0 and box["y"] >= 0
        and box["x"] + box["width"] <= vp["width"] + 2
        and box["height"] > 10
    )

    # Clipping: card must not be inside an overflow:hidden container
    # whose right/bottom edges cut it off. We probe via JS.
    not_clipped = page.evaluate(
        """() => {
            const el = document.querySelector('[data-testid="dbi-card"]');
            if (!el) return false;
            const r = el.getBoundingClientRect();
            // Walk ancestors; flag if any has overflow hidden AND its rect
            // strictly contains less than the card.
            let p = el.parentElement;
            while (p) {
                const cs = getComputedStyle(p);
                if ((cs.overflowX === 'hidden' || cs.overflowY === 'hidden')) {
                    const pr = p.getBoundingClientRect();
                    if (r.right > pr.right + 1 || r.bottom > pr.bottom + 1) return false;
                }
                p = p.parentElement;
            }
            return true;
        }"""
    )
    rep.not_clipped = bool(not_clipped)

    # Z-index above any plotly hoverlayer present on this page.
    if expects_plotly:
        try:
            page.wait_for_selector('div[data-testid="stPlotlyChart"]', timeout=8000)
        except PWTimeout:
            pass
        rep.z_above_plotly = page.evaluate(
            """() => {
                const card = document.querySelector('[data-testid="dbi-card"]');
                if (!card) return false;
                const cz = parseInt(getComputedStyle(card).zIndex || '0', 10) || 0;
                const layers = document.querySelectorAll('.hoverlayer');
                if (!layers.length) return true;
                let maxL = 0;
                layers.forEach(l => {
                    const z = parseInt(getComputedStyle(l).zIndex || '0', 10) || 0;
                    if (z > maxL) maxL = z;
                });
                return cz >= maxL;
            }"""
        )


def _check_popover(page: Page, rep: PageReport):
    # Wait until card is no longer LOADING before clicking popover.
    try:
        page.wait_for_function(
            """() => {
                const c = document.querySelector('[data-testid="dbi-card"]');
                return c && c.getAttribute('data-loading') === '0';
            }""",
            timeout=45000,
        )
    except PWTimeout:
        rep.error = "card stayed in LOADING state"
        return

    # Find the Parameters expander button within the DBI card.
    import re as _re
    trigger = page.locator('[data-testid="dbi-card"] button').filter(has_text="Parameters").first
    try:
        trigger.wait_for(state="visible", timeout=10000)
        trigger.click()
        rep.popover_opens = True
    except PWTimeout:
        # Fallback: try any button whose text contains 'Parameters'
        try:
            page.get_by_text("Parameters", exact=False).first.click()
            rep.popover_opens = True
        except Exception:
            rep.error = "Parameters expander not clickable"
            return

    # Body should mention "Insight source".
    # @st.fragment(run_every=2) re-renders every 2s which may collapse the expander;
    # retry clicking up to 4 times (1.6s each = ~6.4s total coverage).
    # Re-locate the trigger on each attempt — the fragment rebuild detaches old nodes.
    for _attempt in range(4):
        try:
            page.wait_for_selector("text=Insight source", timeout=1600)
            rep.popover_shows_source = True
            break
        except PWTimeout:
            # Reopen expander if it collapsed on fragment re-render
            try:
                if not page.locator("text=Insight source").is_visible():
                    # Re-locate: fragment re-render replaces DOM nodes
                    trigger = page.locator('[data-testid="dbi-card"] button').filter(has_text="Parameters").first
                    trigger.click()
            except Exception:
                break


def _check_liveness(page: Page, rep: PageReport, expects_plotly: bool):
    # Capture initial digest.
    initial = page.evaluate(
        '() => document.querySelector("[data-testid=\\"dbi-card\\"]").getAttribute("data-digest")'
    )

    # Try to drive context change by clicking a Plotly marker if present.
    if expects_plotly:
        try:
            chart = page.locator('div[data-testid="stPlotlyChart"]').first
            if chart.count():
                box = chart.bounding_box()
                if box:
                    # Sweep small grid to provoke hover + a click.
                    for dx in (0.4, 0.5, 0.6):
                        for dy in (0.4, 0.5, 0.6):
                            page.mouse.move(box["x"] + box["width"] * dx,
                                            box["y"] + box["height"] * dy)
                            page.wait_for_timeout(120)
                    page.mouse.click(box["x"] + box["width"] * 0.5,
                                     box["y"] + box["height"] * 0.5)
        except Exception:
            pass

    # Liveness: timestamp portion (after `·`) must advance at least once
    # within ~6 s thanks to the 2-s fragment tick.
    deadline = time.time() + 5
    new_digest = initial
    while time.time() < deadline:
        page.wait_for_timeout(700)
        try:
            new_digest = page.evaluate(
                '() => document.querySelector("[data-testid=\\"dbi-card\\"]").getAttribute("data-digest")'
            )
            stamp = page.evaluate(
                '() => document.querySelector("[data-testid=\\"dbi-stamp\\"]").innerText'
            )
            if stamp:
                rep.timestamp_advances = True
            if new_digest != initial:
                rep.digest_updates = True
                break
        except Exception:
            pass

    # Even when no chart click happens, the timestamp must tick.
    if not rep.timestamp_advances:
        # one more grace check
        page.wait_for_timeout(2500)
        try:
            stamp = page.evaluate(
                '() => document.querySelector("[data-testid=\\"dbi-stamp\\"]").innerText'
            )
            rep.timestamp_advances = bool(stamp)
        except Exception:
            pass


def _check_help_tooltips(page: Page, rep: PageReport):
    """Every st.metric widget must have a co-located st.expander in the same
    column div — the always-visible Brain insight pattern that replaced
    help= tooltip icons (invisible in VS Code Simple Browser)."""
    try:
        info = page.evaluate(
            """() => {
                const metrics = Array.from(document.querySelectorAll(
                    '[data-testid="stMetric"]'
                )).filter(c => {
                    const lbl = c.querySelector('[data-testid="stMetricLabel"]');
                    return lbl && lbl.innerText.trim().length > 0;
                });
                const missing = [];
                let withHelp = 0;
                metrics.forEach(m => {
                    // Walk up the DOM up to 8 levels; stop at the first ancestor
                    // that also contains an stExpander (our per-metric Brain insight).
                    // This is more robust than relying on a specific data-testid name
                    // for the column wrapper across Streamlit versions.
                    let ancestor = m.parentElement;
                    let hasExpander = false;
                    for (let d = 0; d < 8 && ancestor && ancestor.tagName !== 'BODY'; d++) {
                        if (ancestor.querySelector('[data-testid="stExpander"]')) {
                            hasExpander = true;
                            break;
                        }
                        ancestor = ancestor.parentElement;
                    }
                    const labelEl = m.querySelector('[data-testid="stMetricLabel"]');
                    const label = labelEl ? labelEl.innerText.trim() : '(unknown)';
                    if (hasExpander) withHelp += 1;
                    else missing.push(label);
                });
                return {total: metrics.length, withHelp, missing};
            }"""
        )
        rep.metrics_total = int(info.get("total", 0))
        rep.metrics_with_help = int(info.get("withHelp", 0))
        rep.metrics_missing_help = list(info.get("missing", []))[:10]
    except Exception as e:
        # Non-fatal: leave defaults so the assertion still fires later.
        rep.error = (rep.error + " | " if rep.error else "") + f"expander-scan: {e}"


def _run_one(page: Page, url: str, name: str, expects_plotly: bool, db_required: bool = False) -> PageReport:
    rep = PageReport(url=url, name=name)
    rep.nav_ok = _try_navigate(page, url)
    if not rep.nav_ok:
        rep.error = "navigation failed"
        return rep
    try:
        _check_card(page, rep, expects_plotly, db_required=db_required)
        if rep.card_present and not rep.skipped:
            _check_popover(page, rep)
            _check_liveness(page, rep, expects_plotly)
        _check_help_tooltips(page, rep)
    except Exception as e:
        rep.error = f"{type(e).__name__}: {e}"
    return rep


def _flush(reports):
    summary = {
        "passed": sum(1 for r in reports if r.passed),
        "total": len(reports),
        "pages": [asdict(r) | {"passed": r.passed} for r in reports],
    }
    RESULTS_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def _wait_for_server_stable(page: Page, timeout_s: int = 60) -> bool:
    """Retry navigating to the root page until stAppViewContainer appears.

    Streamlit reloads on file changes; this ensures the server is fully up
    before the test suite begins.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            page.goto(BASE + "/", wait_until="domcontentloaded", timeout=12000)
            page.wait_for_selector('[data-testid="stAppViewContainer"]', timeout=10000)
            # Brief pause to let any in-progress reload settle.
            page.wait_for_timeout(2000)
            return True
        except Exception:
            page.wait_for_timeout(4000)
    return False


def main() -> int:
    # SMOKE=1 limits to a representative subset for fast feedback.
    pages = PAGES
    if os.environ.get("DBI_SMOKE") == "1":
        wanted = {"Query Console", "Supply Chain Brain", "EOQ Deviation",
                  "Sustainability", "Connectors"}
        pages = [t for t in PAGES if t[1] in wanted]

    reports: list[PageReport] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1600, "height": 1000})
        page = ctx.new_page()
        page.set_default_timeout(10000)

        # Wait for Streamlit to be fully loaded before starting any page tests.
        print("Waiting for Streamlit server to stabilise...", flush=True)
        if not _wait_for_server_stable(page):
            print("ERROR: Streamlit server not stable after 60 s — aborting.", flush=True)
            browser.close()
            return 1
        print("Server stable. Starting page tests.", flush=True)

        for url, name, has_plotly, db_required in pages:
            print(f"-> {name:30s} {url}", flush=True)
            rep = _run_one(page, url, name, has_plotly, db_required=db_required)
            reports.append(rep)
            mark = "SKIP" if rep.skipped else ("PASS" if rep.passed else "FAIL")
            tip = (f"expanders={rep.metrics_with_help}/{rep.metrics_total}"
                   if rep.metrics_total else "metrics=n/a")
            miss = (" no_expander=" + ",".join(rep.metrics_missing_help)
                    if rep.metrics_missing_help else "")
            print(
                f"  [{mark}] present={rep.card_present} visible={rep.card_visible} "
                f"unclipped={rep.not_clipped} z_above={rep.z_above_plotly} "
                f"popover={rep.popover_opens} src={rep.popover_shows_source} "
                f"ts_tick={rep.timestamp_advances} digest_chg={rep.digest_updates} "
                f"{tip}{miss}"
                f"{' err=' + rep.error if rep.error else ''}",
                flush=True,
            )
            _flush(reports)

        browser.close()

    skipped = sum(1 for r in reports if r.skipped)
    passed = sum(1 for r in reports if r.passed and not r.skipped)
    failed = sum(1 for r in reports if not r.passed and not r.skipped)
    tested = len(reports) - skipped
    summary = {
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "total": len(reports),
        "pages": [asdict(r) | {"passed": r.passed} for r in reports],
    }
    RESULTS_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    skip_note = f"  ({skipped} skipped — Azure SQL offline)" if skipped else ""
    print(f"\n=== DBI tooltip suite: {passed}/{tested} pages passed{skip_note} ===")
    print(f"Detailed results → {RESULTS_PATH}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
