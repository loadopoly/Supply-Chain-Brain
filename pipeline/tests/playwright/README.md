# DBI Tooltip Robustness Suite

End-to-end Playwright validation that the **Dynamic Brain Insight (DBI)** card
on every page of the Supply Chain Brain Streamlit app is:

- **Present** in the DOM (selector: `[data-testid="dbi-card"]`)
- **Visible** and inside the viewport
- **Not clipped** by any ancestor with `overflow:hidden`
- **Stacked above** Plotly hover layers (`z-index >= .hoverlayer`)
- **Interactive** – the 🔍 Parameters popover opens and shows "Insight source"
- **Live** – the timestamp on the card ticks every 2 s thanks to
  `@st.fragment(run_every=2)`
- **Reactive** – the `data-digest` attribute changes when the user interacts
  with a Plotly chart (proves the hover-driven context bridge works)

## Files

| File | Purpose |
|------|---------|
| `test_dbi_tooltip.py` | Full 19-page assertion suite. Writes `dbi_tooltip_results.json` incrementally. |
| `bench_dbi.py`        | Performance benchmark (load-to-card, fragment ticks/10 s, popover open). Writes CSV under `pipeline/bench/results/`. |
| `_discover_slugs.py`  | Helper that scrapes the Streamlit sidebar to confirm URL slugs (writes `_slugs.json`). |
| `dbi_tooltip_results.json` | Latest test results (regenerated each run). |
| `dbi_run.log`         | Tee'd stdout of the most recent test run. |

## Prerequisites

```powershell
cd 'C:\Users\agard\OneDrive - astecindustries.com\VS Code\pipeline'
.\.venv\Scripts\python.exe -m pip install playwright
.\.venv\Scripts\python.exe -m playwright install chromium
```

## Run

Streamlit must be live on `http://localhost:8502`:

```powershell
# from pipeline/
$proc = Start-Process -FilePath ".\.venv\Scripts\python.exe" `
    -ArgumentList "-m","streamlit","run","app.py","--server.port=8502","--server.headless=true","--browser.gatherUsageStats=false" `
    -RedirectStandardOutput "logs/streamlit.log" `
    -RedirectStandardError  "logs/streamlit.err.log" `
    -PassThru -WindowStyle Hidden
$proc.Id | Out-File -Encoding ascii logs/streamlit.pid
```

### Smoke (5 pages, ~3 min)

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:DBI_SMOKE='1'
.\.venv\Scripts\python.exe tests/playwright/test_dbi_tooltip.py `
    | Tee-Object -FilePath tests/playwright/dbi_run.log
```

### Full (19 pages, ~15-25 min)

```powershell
Remove-Item Env:DBI_SMOKE -ErrorAction SilentlyContinue
$env:PYTHONIOENCODING='utf-8'
.\.venv\Scripts\python.exe tests/playwright/test_dbi_tooltip.py `
    | Tee-Object -FilePath tests/playwright/dbi_run.log
```

### Benchmark

```powershell
$env:PYTHONIOENCODING='utf-8'
.\.venv\Scripts\python.exe tests/playwright/bench_dbi.py
# results in pipeline/bench/results/dbi-bench-*.csv
```

## DBI hardening implemented in this work

1. **`pipeline/app.py` – global CSS**
   - `.dbi-container { position:relative !important; z-index:950 !important; }`
   - `div[data-testid="stPopover"] { z-index:951 !important; }`
   - `div[data-testid="stPopoverBody"], div[data-baseweb="popover"] { z-index:9999 !important; }`
   - `@keyframes dbiPulse` triggered when `data-dbi-updated="1"`
   - Existing rule preserved: `.hoverlayer { pointer-events: none; }`

2. **`pipeline/src/brain/dynamic_insight.py` – render block**
   - DOM exposes data attributes used by the test suite:
     - `data-testid="dbi-card"`, `data-testid="dbi-stamp"`, `data-testid="dbi-body"`
     - `data-page`, `data-digest` (md5 of insight + dbi_/g_/kg_ session keys),
       `data-dbi-updated`, `data-loading`
   - `role="status" aria-live="polite"` for AT/screen readers
   - Visible timestamp + digest snippet so humans can confirm liveness

## Interpreting `dbi_tooltip_results.json`

```json
{
  "passed": <int>,
  "total":  <int>,
  "pages": [
    {
      "name": "Query Console", "url": "/",
      "nav_ok": true, "card_present": true, "card_visible": true,
      "card_in_viewport": true, "not_clipped": true,
      "z_above_plotly": true, "popover_opens": true,
      "popover_shows_source": true,
      "timestamp_advances": true, "digest_updates": false,
      "passed": true, "error": ""
    }
  ]
}
```

`digest_updates=false` is acceptable on pages without a Plotly chart (Query
Console, Connectors, Decision Log, etc.) — there is nothing for the test to
hover/click to drive a new digest. The pass/fail flag does **not** require it.
