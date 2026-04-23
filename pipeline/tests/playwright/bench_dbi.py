"""
DBI Performance Benchmark
=========================

Measures three dimensions of Dynamic Brain Insight (DBI) responsiveness:

  1. **load_to_card_ms** – how long after page load until the DBI card is rendered
  2. **fragment_tick_count_10s** – how many distinct timestamps the DBI stamp shows
     over a 10-second window (proves the @st.fragment(run_every=2) is firing)
  3. **popover_open_ms** – click-to-visible latency for the 🔍 Parameters popover

Writes results to `pipeline/bench/results/dbi-bench-YYYYMMDD-HHMMSS.csv`.

Usage (from `pipeline/`):
    .\\.venv\\Scripts\\python.exe tests/playwright/bench_dbi.py
"""
from __future__ import annotations
import csv
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from playwright.sync_api import sync_playwright, Page, TimeoutError as PWTimeout

BASE = "http://localhost:8502"

PAGES = [
    ("/",                      "Query Console"),
    ("/Supply_Chain_Brain",    "Supply Chain Brain"),
    ("/EOQ_Deviation",         "EOQ Deviation"),
    ("/Sustainability",        "Sustainability"),
    ("/Connectors",            "Connectors"),
]


def bench_one(page: Page, url: str, name: str) -> dict:
    rec = {"page": name, "url": url, "load_to_card_ms": -1,
           "fragment_tick_count_10s": -1, "popover_open_ms": -1, "error": ""}
    try:
        t0 = time.perf_counter()
        page.goto(BASE + url, wait_until="domcontentloaded", timeout=20000)
        page.wait_for_selector('[data-testid="dbi-card"]', timeout=20000)
        rec["load_to_card_ms"] = int((time.perf_counter() - t0) * 1000)

        # Sample stamps over 10 s
        seen = set()
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                s = page.evaluate(
                    '() => document.querySelector("[data-testid=\\"dbi-stamp\\"]").innerText'
                )
                if s:
                    seen.add(s.strip())
            except Exception:
                pass
            page.wait_for_timeout(400)
        rec["fragment_tick_count_10s"] = len(seen)

        # Wait for non-loading then time popover open
        try:
            page.wait_for_function(
                """() => {
                    const c = document.querySelector('[data-testid=\"dbi-card\"]');
                    return c && c.getAttribute('data-loading') === '0';
                }""",
                timeout=15000,
            )
            t1 = time.perf_counter()
            page.get_by_role("button", name="🔍 Parameters").first.click(timeout=8000)
            page.wait_for_selector("text=Insight source", timeout=5000)
            rec["popover_open_ms"] = int((time.perf_counter() - t1) * 1000)
            page.mouse.click(5, 5)
        except PWTimeout:
            rec["error"] = "popover-timeout"
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {e}"
    return rec


def main():
    out_dir = Path(__file__).parents[2] / "bench" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"dbi-bench-{datetime.now():%Y%m%d-%H%M%S}.csv"

    rows: list[dict] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1600, "height": 1000})
        page = ctx.new_page()
        page.set_default_timeout(15000)
        for url, name in PAGES:
            print(f"-> {name}", flush=True)
            rec = bench_one(page, url, name)
            print(f"   load_to_card={rec['load_to_card_ms']}ms "
                  f"ticks_10s={rec['fragment_tick_count_10s']} "
                  f"popover_open={rec['popover_open_ms']}ms "
                  f"err={rec['error']}", flush=True)
            rows.append(rec)
        browser.close()

    with out_file.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"\nWrote {out_file}")


if __name__ == "__main__":
    main()
