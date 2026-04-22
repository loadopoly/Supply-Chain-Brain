# Operational runbook

## 1. First-time install (Windows)

```powershell
cd "$env:USERPROFILE\OneDrive - astecindustries.com\VS Code\pipeline"

# 1.1 create venv (the repo's `.venv` is git-ignored)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 1.2 install pinned deps
pip install -r requirements.pinned.txt
# (or `pip install -r requirements.txt` for the looser bounds)

# 1.3 (optional) richer analytics
pip install xgboost lightgbm lifelines scikit-survival missingpy

# 1.4 sanity-check imports
python -c "import brain, brain.research.bullwhip, brain.graph_backend; print('ok')"
```

## 2. Start the app

```powershell
# default port 8501; switch ports if it's already in use
streamlit run app.py
# or
streamlit run app.py --server.port 8502
```

The app is served on `http://localhost:8501/`. The first hit triggers
`bootstrap_default_connectors()` which initialises Azure SQL +
Oracle Fusion + IPS Freight connectors. Connection failures show as
non-fatal warnings on the **Connectors** page.

## 3. Health check

```powershell
Invoke-WebRequest http://127.0.0.1:8501/_stcore/health
# expect: 200 ok
```

## 4. Run the benchmark suite

```powershell
python -m bench.bench_brain --rows 20000 --repeats 3
```

Then open the **⚡ Benchmarks** page. The full 18-benchmark suite finishes
in **≈ 5 seconds** at 20k rows on the dev box.

## 5. Common issues

| Symptom | Likely cause | Fix |
|---|---|---|
| Autonomous agent dies silently | Long `time.sleep()` blocks caused silent process hanging. | A heartbeat mechanism writes to `logs/agent_heartbeat.txt`. Run `python test_agent_health.py` to verify the agent isn't hung. |
| `Port 8501 is not available` | A previous Streamlit process is still bound. | `streamlit run app.py --server.port 8502` or kill the prior PID. |
| `pyodbc not installed` on connect | Missing Visual C++ runtime / wrong wheel | `pip install pyodbc==5.*` after installing the MS C++ Build Tools. |
| `azure-identity` SSO loop | Stale token cache | Delete `%USERPROFILE%\.azure\msal_token_cache.bin` and retry. |
| EOQ page shows "missing column" | Replica column name differs from defaults | Edit `config/brain.yaml → columns:` and add the actual physical name. |
| Empty graph on Brain page | No connector reachable yet | Open **Connectors** page and confirm the green badge for at least one source. |
| KM survival slow | Large group cardinality | The `lead_time.per_group_km` benchmark caps at ~230 ms / 20k rows; use `min_n` to filter sparse groups. |

## 6. Production hardening (when promoting beyond a single workstation)

1. Replace `findings_index.db` SQLite with Postgres / Azure SQL — only
   the `_conn()` helper in `findings_index.py` needs to change.
2. Switch graph backend to Neo4j or Cosmos Gremlin in `brain.yaml → graph`.
3. Provide real secrets via env vars referenced as `*_env: …` in
   `brain.yaml`. Nothing in this repo embeds a secret.
4. Run `python -m brain.analytics_fact build azure_sql` nightly to
   refresh the denormalized `fact_supply_chain_brain` snapshot.
5. Front the Streamlit process with a reverse proxy (nginx / Caddy) and
   enable Streamlit `--server.enableCORS=false` + AAD-protected ingress.

## 7. Stopping the app

```powershell
Get-Process | Where-Object { $_.ProcessName -eq 'python' -and $_.MainWindowTitle -match 'streamlit' } |
  Stop-Process -Id { $_.Id }
# or simply Ctrl-C in the terminal where you launched it.
```
