# Continuous Synaptic Agents — Operator Runbook

> **Module:** `pipeline/autonomous_agent.py`
> **Introduced:** v0.14.6 · **Hardened:** v0.14.7

The Brain runs four lightweight **daemon worker threads** continuously
underneath its main 1-4 hr autonomous cycle. Each worker is responsible
for a different relationally-dispersed temporal slice of the knowledge
corpus, so synapses (`corpus_edge` rows) are pre-built ahead of where any
downstream reader (Quest engine, Brain pages, Body directives) traverses
the graph.

---

## 1. Architecture

```
┌─────────────────────── autonomous_loop()  (main, 1-4 hr) ──────────────────────┐
│  Step 0..6 (VPN, tests, docs, sweep, RAG 3e.5, missions, email, commit)        │
│                                                                                │
│  └── start_continuous_synaptic_agents()  (called once at startup)              │
│         │                                                                      │
│         ├── daemon: synapse-builder       (10 min, 24h window)                 │
│         ├── daemon: synapse-lookahead     (15 min, rotating 7d / 30d / 90d)    │
│         ├── daemon: synapse-sweeper       (20 min, one connector per tick)     │
│         └── daemon: synapse-convergence   (30 min, refresh + materialise)      │
└────────────────────────────────────────────────────────────────────────────────┘
```

All four workers share:

| Concern              | Implementation                                                     |
|----------------------|--------------------------------------------------------------------|
| Threading            | `threading.Thread(daemon=True)`                                    |
| Shutdown             | `threading.Event` `_SYNAPTIC_STOP`, polled via `_wait_or_stop()`   |
| SQLite safety        | `sqlite3.connect(..., check_same_thread=False)`                    |
| Desync               | Initial random startup delay + ±60-120 s jitter per tick           |
| Heartbeat            | `_kv_write("synapse_<name>_last", "<iso>|<summary>")` every tick   |
| Failure backoff      | `_next_sleep_with_backoff()` — 1×, 2×, 4×, 8× cap                  |
| Failure marker       | `_kv_write("synapse_<name>_failures", ...)` on each failure        |

---

## 2. Worker reference

### 2.1 `synapse-builder` — near-present

| Field            | Value                                          |
|------------------|------------------------------------------------|
| Cadence          | 600 s (10 min) ± 60 s                          |
| Window           | last 24 h                                      |
| RAG iterations   | 4                                              |
| RAG entity cap   | 800                                            |
| Heartbeat key    | `synapse_builder_last`                         |
| Explored-pair KV | `rag_explored_pairs_builder`                   |

Catches the high-traffic part of the graph so missions launched against
fresh data find ready bridges.

### 2.2 `synapse-lookahead` — forward-look

| Field            | Value                                                 |
|------------------|-------------------------------------------------------|
| Cadence          | 900 s (15 min) ± 90 s                                 |
| Window           | rotates: 7 d / 30 d / 90 d                            |
| Offsets          | 24 h / 7 d / 30 d (so windows don't overlap heavily)  |
| RAG iterations   | 6                                                     |
| RAG entity cap   | 1500                                                  |
| Heartbeat keys   | `synapse_lookahead_{7d,30d,90d}_last`                 |
| Explored-pair KV | `rag_explored_pairs_lookahead_{7d,30d,90d}`           |

Each window has its own persisted explored-pair set so progress on one
slice doesn't reset another's. The 90-day slice deliberately lags the
others so deep-history bridges are ready before the convergence pass
projects them.

### 2.3 `synapse-sweeper` — continuous data freshness

| Field         | Value                                                 |
|---------------|-------------------------------------------------------|
| Cadence       | 1200 s (20 min) ± 120 s                               |
| Strategy      | One SQL connector per tick, round-robin               |
| Probe         | INFORMATION_SCHEMA TOP-5 sanity check                 |
| Heartbeat key | `synapse_sweeper_<connector_name>` (per connector)    |

Newly-registered connectors automatically join the rotation. When zero
connectors are registered the worker idles at base cadence (no failure
charged).

### 2.4 `synapse-convergence` — consolidate

| Field         | Value                                                                  |
|---------------|------------------------------------------------------------------------|
| Cadence       | 1800 s (30 min) ± 120 s                                                |
| Calls         | `refresh_corpus_round()` then `materialize_into_graph()`               |
| Heartbeat key | `synapse_convergence_last`                                             |

Projects everything the other three workers wrote into the read-side
graph backend. Without this step, the synaptic builders are writing
into a graph nothing reads from.

---

## 3. Backoff behaviour

When a worker iteration raises, `_next_sleep_with_backoff()`:

1. Increments `_SYNAPTIC_FAILURES[<name>]`.
2. Computes `mult = min(2 ** consecutive_failures, 8)`.
3. Returns `base_interval × mult + jitter`.
4. Persists `synapse_<name>_failures = "<iso>|consecutive=N|next_mult=Mx"`
   to `brain_kv` for ops visibility.

On the next successful iteration:

- The counter is reset to 0.
- The `synapse_<name>_failures` marker is **not** wiped (keep the
  forensic trail) — but `synaptic_agents_status()` reads the live
  in-memory counter so its `consecutive_failures` field reflects truth.

Worst-case sleep examples:

| Worker             | Base   | After 3 fails | Capped (8×) |
|--------------------|--------|---------------|-------------|
| synapse-builder    | 10 min | 80 min        | 80 min      |
| synapse-lookahead  | 15 min | 120 min       | 120 min     |
| synapse-sweeper    | 20 min | 160 min       | 160 min     |
| synapse-convergence| 30 min | 240 min       | 240 min     |

A permanently broken dependency therefore still gets retried roughly
hourly to several-hourly — never abandoned.

---

## 4. Health check

```python
from autonomous_agent import synaptic_agents_status
import json
print(json.dumps(synaptic_agents_status(), indent=2, default=str))
```

Sample output:

```json
{
  "started":      true,
  "started_at":   "2026-04-23T08:14:02.114",
  "thread_count": 4,
  "shutdown_set": false,
  "workers": [
    {
      "name":               "synapse-builder",
      "kv_key":             "synapse_builder_last",
      "expected_every":     600,
      "last_iso":           "2026-04-23T08:24:11.842",
      "age_seconds":        189,
      "summary":            "edges=12|paths=37",
      "consecutive_failures": 0,
      "verdict":            "ok"
    },
    ...
  ]
}
```

### Verdict semantics

| Verdict       | Meaning                                                          |
|---------------|------------------------------------------------------------------|
| `ok`          | Heartbeat younger than 4× expected interval. Healthy.            |
| `stale`       | Heartbeat older than 4× expected interval. Critical — investigate.|
| `never_ran`   | No heartbeat key written yet (worker hasn't completed first tick).|

A `stale` verdict means the daemon thread is alive but its iterations
are silently dying (caught exceptions still write a log line; an
uncaught crash silently kills the worker). Check `autonomous_agent.log`
for `[synapse:<name>]` entries around the `last_iso` timestamp.

---

## 5. Common ops scenarios

**Worker shows `verdict=stale`**
1. Tail `autonomous_agent.log` for `[synapse:<name>]` entries.
2. Check `_kv_read("synapse_<name>_failures")` for the last failure
   marker (the `consecutive=N` count tells you whether it's stuck or
   intermittent).
3. If the counter is high but log shows recent `iteration failed`
   warnings: a dependency (corpus DB, connector, downstream module) is
   broken. Fix the dependency; backoff will heal automatically.
4. If logs show no recent `[synapse:<name>]` entries at all: the worker
   thread has died from an uncaught exception. Restart the agent.

**Need to restart the workers without restarting the whole agent**
```python
import autonomous_agent as a
a.stop_continuous_synaptic_agents(timeout=10)
a.start_continuous_synaptic_agents()
```
The hardened `stop()` clears `_SYNAPTIC_THREADS`, `_SYNAPTIC_FAILURES`,
and the started flag, so re-start is clean.

**Sweeper isn't probing a new connector**
The sweeper rebuilds its connector list every tick from
`db_registry.list_connectors()`. Confirm the connector is registered
there; the sweeper will pick it up within one cycle (up to ~22 min).

---

## 6. Source-of-truth references

| Concern             | Symbol / file                                              |
|---------------------|------------------------------------------------------------|
| Worker functions    | `_synaptic_builder_worker`, `_lookahead_worker`,           |
|                     | `_dispersed_sweeper_worker`, `_convergence_worker`         |
| Orchestrator        | `start_continuous_synaptic_agents`, `stop_continuous_synaptic_agents` |
| Health snapshot     | `synaptic_agents_status`                                   |
| Backoff helper      | `_next_sleep_with_backoff`                                 |
| Shutdown event      | `_SYNAPTIC_STOP` (module-level `threading.Event`)          |
| Thread registry     | `_SYNAPTIC_THREADS`                                        |
| Failure counters    | `_SYNAPTIC_FAILURES`                                       |
| RAG core            | `rag_knowledge_deepdive(window_label=..., window_hours=..., ...)` |

All in `pipeline/autonomous_agent.py`.
