"""Continuous multi-agent synaptic extension workers.

This module is intentionally separated from autonomous_agent.py so that the
autonomous agent's periodic "optimize-and-commit" cycle cannot accidentally
strip the worker threads by rewriting its own source file.  All synaptic
substrate lives here; autonomous_agent.py only does:

    from src.brain.synaptic_workers import (
        start_continuous_synaptic_agents,
        stop_continuous_synaptic_agents,
        synaptic_agents_status,
    )

Architecture
------------
Four lightweight daemon threads run continuously beneath the main 1-4 h loop:

  1. _synaptic_builder_worker   — RAG deepdive, last 24 h window (~10 min)
  2. _lookahead_worker          — RAG deepdive, 7d/30d/90d rotating (~15 min)
  3. _dispersed_sweeper_worker  — SQL-connector health probe rotation (~20 min)
  4. _convergence_worker        — corpus refresh + graph materialise (~30 min)

All workers:
  • share a single threading.Event shutdown flag (_SYNAPTIC_STOP)
  • record heartbeats + summaries in brain_kv (synapse_<name>_last)
  • use exponential back-off on consecutive failures (cap 8×, reset on success)
  • treat network-level errors (host DOWN, timeout) as soft skips — not failures
"""

from __future__ import annotations

import logging
import os
import random
import sqlite3
import sys
import threading
import time
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# brain_kv helpers (self-contained; no circular import with autonomous_agent.py)
# ─────────────────────────────────────────────────────────────────────────────

def _sw_kv_read(key: str, default: str | None = None) -> str | None:
    """Read a scalar from brain_kv."""
    try:
        _base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sys.path.insert(0, _base)
        from src.brain.local_store import db_path  # type: ignore[import]
        cn = sqlite3.connect(str(db_path()))
        row = cn.execute("SELECT value FROM brain_kv WHERE key=?", (key,)).fetchone()
        cn.close()
        return row[0] if row else default
    except Exception:
        return default


def _sw_kv_write(key: str, value: str) -> None:
    """Upsert a scalar into brain_kv."""
    try:
        _base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sys.path.insert(0, _base)
        from src.brain.local_store import db_path  # type: ignore[import]
        cn = sqlite3.connect(str(db_path()))
        cn.execute(
            "INSERT INTO brain_kv(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        cn.commit()
        cn.close()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Shared state
# ─────────────────────────────────────────────────────────────────────────────

_SYNAPTIC_STOP: threading.Event = threading.Event()
_SYNAPTIC_THREADS: list[threading.Thread] = []
_SYNAPTIC_STARTED: bool = False

# Per-worker consecutive-failure counters (name → int).
# Reset to 0 on any successful iteration; incremented on exception.
# Multiplier caps at 8× so a permanently-broken connector still retries ~hourly.
_SYNAPTIC_FAILURES: dict[str, int] = {}
_SYNAPTIC_BACKOFF_MAX_MULT = 8

# Connection-error keywords that indicate a transient network outage rather
# than a code bug.  Sweeper failures matching these are treated as soft skips
# (ok=True) so backoff does not accumulate during host-down periods.
_NETWORK_ERROR_KEYWORDS = (
    "connection", "timeout", "network", "refused",
    "unreachable", "timed out", "server down", "could not connect",
    "login failed", "tcp provider", "communication link",
    "no route", "winrm", "winerror 10061", "winerror 10060",
)


# ─────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ─────────────────────────────────────────────────────────────────────────────

def _wait_or_stop(seconds: int) -> bool:
    """Sleep ``seconds`` but wake immediately on shutdown.

    Returns ``True`` if shutdown was requested (caller should exit).
    """
    return _SYNAPTIC_STOP.wait(timeout=max(1, int(seconds)))


def _next_sleep_with_backoff(name: str, base_interval_s: int,
                              jitter_s: int, last_ok: bool) -> int:
    """Return next sleep duration with exponential back-off on failure."""
    if last_ok:
        _SYNAPTIC_FAILURES[name] = 0
        mult = 1
    else:
        prev = _SYNAPTIC_FAILURES.get(name, 0)
        cur = prev + 1
        _SYNAPTIC_FAILURES[name] = cur
        mult = min(2 ** cur, _SYNAPTIC_BACKOFF_MAX_MULT)
        try:
            _sw_kv_write(
                f"synapse_{name}_failures",
                f"{datetime.now().isoformat()}|consecutive={cur}|next_mult={mult}x",
            )
        except Exception:
            pass
    return base_interval_s * mult + random.randint(-jitter_s, jitter_s)


def _is_network_error(exc: Exception) -> bool:
    """Return True if *exc* looks like a transient connectivity failure."""
    msg = str(exc).lower()
    return any(kw in msg for kw in _NETWORK_ERROR_KEYWORDS)


# ─────────────────────────────────────────────────────────────────────────────
# Status / health-check
# ─────────────────────────────────────────────────────────────────────────────

def synaptic_agents_status() -> dict:
    """Return a health snapshot of all synaptic workers.

    Reads heartbeat keys from brain_kv and reports name, last timestamp,
    age, summary, failure count, and verdict (ok / stale / never_ran).
    ``stale`` = age > 4× expected interval (worker is alive but iterations die).
    """
    workers_meta = [
        ("synapse_builder_last",       "synapse-builder",      600),
        ("synapse_lookahead_7d_last",  "synapse-lookahead-7d",  900),
        ("synapse_lookahead_30d_last", "synapse-lookahead-30d", 900),
        ("synapse_lookahead_90d_last", "synapse-lookahead-90d", 900),
        ("synapse_convergence_last",   "synapse-convergence",  1800),
        ("synapse_vision_last",        "synapse-vision",        300),
        ("synapse_torus_last",         "synapse-torus",          30),
    ]
    out: dict = {
        "started":      _SYNAPTIC_STARTED,
        "started_at":   _sw_kv_read("synapse_agents_started", "never"),
        "thread_count": sum(1 for t in _SYNAPTIC_THREADS if t.is_alive()),
        "shutdown_set": _SYNAPTIC_STOP.is_set(),
        "workers":      [],
    }
    now = datetime.now()
    for kv_key, friendly, interval_s in workers_meta:
        raw = _sw_kv_read(kv_key, "")
        ts_iso = raw.split("|", 1)[0] if raw else ""
        summary = raw.split("|", 1)[1] if "|" in raw else ""
        try:
            last_ts = datetime.fromisoformat(ts_iso) if ts_iso else None
        except Exception:
            last_ts = None
        if last_ts is None:
            verdict, age_s = "never_ran", None
        else:
            age_s = int((now - last_ts).total_seconds())
            verdict = "ok" if age_s < 4 * interval_s else "stale"
        short = friendly.replace("synapse-", "").split("-")[0]
        out["workers"].append({
            "name":                 friendly,
            "kv_key":               kv_key,
            "expected_every":       interval_s,
            "last_iso":             ts_iso or None,
            "age_seconds":          age_s,
            "summary":              summary,
            "consecutive_failures": _SYNAPTIC_FAILURES.get(short, 0),
            "verdict":              verdict,
        })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Worker #1 — synaptic builder (near-present 24 h window)
# ─────────────────────────────────────────────────────────────────────────────

def _synaptic_builder_worker() -> None:
    """RAG deepdive on the last 24 h of corpus activity, every ~10 min.

    Keeps the high-traffic part of the graph saturated with synapses so
    missions launched against fresh data find ready bridges.
    """
    INTERVAL_S = 600
    JITTER_S   = 60
    NAME       = "builder"

    logging.info("[synapse:builder] started — interval=10min window=24h")
    if _wait_or_stop(random.randint(5, 30)):
        return

    while not _SYNAPTIC_STOP.is_set():
        t0 = time.time()
        ok = False
        try:
            # Lazy import avoids circular dependency with autonomous_agent.py
            _pkg = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            sys.path.insert(0, _pkg)
            from autonomous_agent import rag_knowledge_deepdive  # type: ignore[import]
            r = rag_knowledge_deepdive(
                window_label="builder_24h",
                window_hours=24,
                window_offset_hours=0,
                max_iterations=4,
                max_entities=800,
                explored_kv_key="rag_explored_pairs_builder",
            )
            _sw_kv_write(
                "synapse_builder_last",
                f"{datetime.now().isoformat()}|edges={r.get('edges_discovered', 0)}"
                f"|paths={r.get('pathways_explored', 0)}",
            )
            elapsed = round(time.time() - t0, 1)
            logging.info(
                f"[synapse:builder] edges={r.get('edges_discovered', 0)} "
                f"paths={r.get('pathways_explored', 0)} elapsed={elapsed}s"
            )
            ok = True
        except Exception as e:
            logging.warning(f"[synapse:builder] iteration failed: {e}")
        sleep_s = _next_sleep_with_backoff(NAME, INTERVAL_S, JITTER_S, ok)
        if _wait_or_stop(sleep_s):
            return


# ─────────────────────────────────────────────────────────────────────────────
# Worker #2 — lookahead (dispersed 7d / 30d / 90d windows)
# ─────────────────────────────────────────────────────────────────────────────

def _lookahead_worker() -> None:
    """RAG deepdive on rotating historical windows, every ~15 min.

    Rotates 7d → 30d → 90d windows in sequence so each has its own persisted
    explored-pair set.  Pre-warms deep-history bridges ahead of where the main
    loop reads them.
    """
    INTERVAL_S = 900
    JITTER_S   = 90
    NAME       = "lookahead"

    WINDOWS = [
        ("lookahead_7d",  7  * 24,  24,       "rag_explored_pairs_lookahead_7d"),
        ("lookahead_30d", 30 * 24,  7  * 24,  "rag_explored_pairs_lookahead_30d"),
        ("lookahead_90d", 90 * 24,  30 * 24,  "rag_explored_pairs_lookahead_90d"),
    ]
    rotation = 0

    logging.info("[synapse:lookahead] started — interval=15min windows=7d/30d/90d rotating")
    if _wait_or_stop(random.randint(60, 180)):
        return

    while not _SYNAPTIC_STOP.is_set():
        label, hrs, offset, kvkey = WINDOWS[rotation % len(WINDOWS)]
        rotation += 1
        t0 = time.time()
        ok = False
        try:
            _pkg = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            sys.path.insert(0, _pkg)
            from autonomous_agent import rag_knowledge_deepdive  # type: ignore[import]
            r = rag_knowledge_deepdive(
                window_label=label,
                window_hours=hrs,
                window_offset_hours=offset,
                max_iterations=6,
                max_entities=1500,
                explored_kv_key=kvkey,
            )
            _sw_kv_write(
                f"synapse_{label}_last",
                f"{datetime.now().isoformat()}|edges={r.get('edges_discovered', 0)}"
                f"|paths={r.get('pathways_explored', 0)}",
            )
            elapsed = round(time.time() - t0, 1)
            logging.info(
                f"[synapse:lookahead/{label}] "
                f"edges={r.get('edges_discovered', 0)} "
                f"paths={r.get('pathways_explored', 0)} elapsed={elapsed}s"
            )
            ok = True
        except Exception as e:
            logging.warning(f"[synapse:lookahead/{label}] failed: {e}")
        sleep_s = _next_sleep_with_backoff(NAME, INTERVAL_S, JITTER_S, ok)
        if _wait_or_stop(sleep_s):
            return


# ─────────────────────────────────────────────────────────────────────────────
# Worker #3 — dispersed sweeper (SQL connector health probe, ~20 min)
# ─────────────────────────────────────────────────────────────────────────────

def _dispersed_sweeper_worker() -> None:
    """Probe one SQL connector per tick, rotating through the registry.

    FIX vs. original: network/connection errors (host DOWN, timeout) are
    treated as soft skips (ok=True) so backoff does not accumulate during
    periods when a host is simply offline.  Only code-level exceptions
    (unexpected SQL errors, import failures, etc.) trigger backoff.
    """
    INTERVAL_S = 1200
    JITTER_S   = 120
    NAME       = "sweeper"

    logging.info("[synapse:sweeper] started — interval=20min mode=connector-rotation")
    if _wait_or_stop(random.randint(120, 240)):
        return

    rotation = 0
    while not _SYNAPTIC_STOP.is_set():
        t0 = time.time()
        ok = False
        target = "(none)"
        try:
            _pkg = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            sys.path.insert(0, _pkg)
            from src.brain.db_registry import list_connectors, read_sql  # type: ignore[import]

            sql_connectors = [c.name for c in list_connectors() if c.kind == "sql"]
            if not sql_connectors:
                logging.info("[synapse:sweeper] no SQL connectors registered yet.")
                ok = True  # nothing to do is not a failure
                if _wait_or_stop(INTERVAL_S):
                    return
                continue

            target = sql_connectors[rotation % len(sql_connectors)]
            rotation += 1

            df = read_sql(
                target,
                "SELECT TOP 5 TABLE_SCHEMA, TABLE_NAME, "
                "(SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c "
                "  WHERE c.TABLE_NAME=t.TABLE_NAME) AS n_cols "
                "FROM INFORMATION_SCHEMA.TABLES t "
                "WHERE TABLE_TYPE='BASE TABLE' "
                "ORDER BY TABLE_NAME",
            )
            n_rows = 0 if df is None or df.empty else len(df)
            _sw_kv_write(
                f"synapse_sweeper_{target}",
                f"{datetime.now().isoformat()}|probed_rows={n_rows}",
            )
            elapsed = round(time.time() - t0, 1)
            logging.info(
                f"[synapse:sweeper/{target}] probe rows={n_rows} elapsed={elapsed}s"
            )
            ok = True

        except Exception as e:
            if _is_network_error(e):
                # Host is unreachable / timed out — transient, not a code bug.
                # Log at INFO level (not WARNING) and continue without backoff.
                logging.info(
                    f"[synapse:sweeper/{target}] host unreachable (soft skip, "
                    f"no backoff): {e}"
                )
                ok = True  # network outage does NOT count as a failure
            else:
                logging.warning(f"[synapse:sweeper] iteration failed: {e}")

        sleep_s = _next_sleep_with_backoff(NAME, INTERVAL_S, JITTER_S, ok)
        if _wait_or_stop(sleep_s):
            return


# ─────────────────────────────────────────────────────────────────────────────
# Worker #4 — convergence (corpus refresh + graph materialise, ~30 min)
# ─────────────────────────────────────────────────────────────────────────────

def _convergence_worker() -> None:
    """Consolidate corpus writes and project into the graph backend, ~30 min.

    Makes the edges built by workers 1-3 visible to downstream readers
    (Quest engine, Brain pages, Body directives) without waiting for the
    main 1-4 hour cycle.
    """
    INTERVAL_S = 1800
    JITTER_S   = 120
    NAME       = "convergence"

    logging.info("[synapse:convergence] started — interval=30min")
    if _wait_or_stop(random.randint(180, 360)):
        return

    while not _SYNAPTIC_STOP.is_set():
        t0 = time.time()
        ok = False
        try:
            _pkg = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            sys.path.insert(0, _pkg)
            from src.brain.knowledge_corpus import (  # type: ignore[import]
                refresh_corpus_round, materialize_into_graph,
            )
            kc = refresh_corpus_round() or {}
            mg = materialize_into_graph() or {}
            _sw_kv_write(
                "synapse_convergence_last",
                f"{datetime.now().isoformat()}"
                f"|+ents={kc.get('entities_added', 0)}"
                f"|+edges={kc.get('edges_added', 0)}"
                f"|nodes={mg.get('nodes_projected', 0)}",
            )
            elapsed = round(time.time() - t0, 1)
            logging.info(
                f"[synapse:convergence] "
                f"+{kc.get('entities_added', 0)} entities, "
                f"+{kc.get('edges_added', 0)} edges, "
                f"projected {mg.get('nodes_projected', 0)}n/"
                f"{mg.get('edges_projected', 0)}e elapsed={elapsed}s"
            )
            ok = True
        except Exception as e:
            logging.warning(f"[synapse:convergence] iteration failed: {e}")

        sleep_s = _next_sleep_with_backoff(NAME, INTERVAL_S, JITTER_S, ok)
        if _wait_or_stop(sleep_s):
            return


# ─────────────────────────────────────────────────────────────────────────────
# Worker #5 — vision (bridge + network topology → corpus graph, ~5 min)
# ─────────────────────────────────────────────────────────────────────────────

def _vision_worker() -> None:
    """Give the Brain eyes — observe bridge/network topology and write it into
    the corpus graph so the RAG deepdive can reason across compute boundaries.

    Every ~5 minutes this worker:
    1. bridge_rdp.probe_all()          — TCP-probe every declared bridge target
    2. network_learner.observe_network_round()  — probe all known endpoints
    3. Upsert ``Endpoint`` corpus entities with ``REACHABLE`` / ``UNREACHABLE``
       edges to linked ``Site`` / ``Peer`` entities.
    4. When a piggyback route is active, upsert a ``BRIDGES_TO`` edge between
       the laptop Endpoint and the desktop Endpoint (weight = ema_success).

    The resulting entities become first-class corpus nodes the same way Parts
    and Suppliers are.  The RAG deepdive can then discover structural holes
    between e.g. a Supplier entity and the SQL server Endpoint that serves its
    purchase-order data — giving the Brain situational awareness of its own
    data-access topology.

    Network errors (hosts DOWN, VPN unreachable) are treated as soft skips so
    backoff does not accumulate while the corporate network is unavailable.
    """
    import json as _json

    INTERVAL_S = 300    # 5-minute heartbeat
    JITTER_S   = 30
    NAME       = "vision"

    logging.info("[synapse:vision] started — interval=5min (bridge+network vision)")
    if _wait_or_stop(random.randint(10, 45)):
        return

    while not _SYNAPTIC_STOP.is_set():
        t0 = time.time()
        ok = False
        live = 0
        down = 0
        bridge_count = 0
        topo_count = 0
        try:
            _pkg = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            sys.path.insert(0, _pkg)

            from src.brain.local_store import db_path as _db_path  # type: ignore[import]

            # ── Step 1: bridge_rdp topology ───────────────────────────────
            bridge_results: dict = {}
            bridge_targets: list = []
            try:
                import bridge_rdp  # type: ignore[import]
                bridge_targets = bridge_rdp.list_targets()
                bridge_results = bridge_rdp.probe_all()
                bridge_count = len(bridge_results)
            except Exception as e:
                if _is_network_error(e):
                    logging.info(f"[synapse:vision] bridge probe soft-skip: {e}")
                else:
                    logging.warning(f"[synapse:vision] bridge_rdp.probe_all error: {e}")

            # ── Step 2: network_learner full observation round ─────────────
            obs: dict = {}
            try:
                from src.brain.network_learner import (  # type: ignore[import]
                    observe_network_round, init_schema,
                )
                init_schema()
                obs = observe_network_round()
                live = obs.get("live", 0)
                down = obs.get("down", 0)
            except Exception as e:
                if _is_network_error(e):
                    logging.info(f"[synapse:vision] net observation soft-skip: {e}")
                else:
                    logging.warning(f"[synapse:vision] observe_network_round error: {e}")

            # ── Step 3: materialise observations into corpus graph ─────────
            now_s = datetime.now().isoformat()
            cn = sqlite3.connect(str(_db_path()))
            try:
                def _upsert_ep(eid, label, props):
                    cn.execute(
                        "INSERT INTO corpus_entity"
                        "(entity_id,entity_type,label,props_json,first_seen,last_seen,samples) "
                        "VALUES(?,?,?,?,?,?,1) "
                        "ON CONFLICT(entity_id,entity_type) "
                        "DO UPDATE SET last_seen=excluded.last_seen, "
                        "samples=samples+1",
                        (eid, "Endpoint", label,
                         _json.dumps(props), now_s, now_s),
                    )

                def _upsert_edge(s_id, s_type, d_id, d_type, rel, weight):
                    cn.execute(
                        "INSERT INTO corpus_edge"
                        "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
                        "VALUES(?,?,?,?,?,?,?,1) "
                        "ON CONFLICT(src_id,src_type,dst_id,dst_type,rel) "
                        "DO UPDATE SET last_seen=excluded.last_seen, "
                        "samples=samples+1, weight=excluded.weight",
                        (s_id, s_type, d_id, d_type, rel, weight, now_s),
                    )

                # Bridge target entities + edges to Site
                for target in bridge_targets:
                    tname  = target.get("name") or ""
                    host   = target.get("target_host") or ""
                    port   = target.get("target_port") or 3389
                    proto  = target.get("protocol") or "rdp"
                    ep_id  = f"bridge:{tname}"
                    alive  = bridge_results.get(tname)   # True/False/None

                    if not host:
                        continue

                    _upsert_ep(ep_id, f"{tname} ({host}:{port})",
                               {"host": host, "port": port, "protocol": proto,
                                "bridge_name": tname})

                    # Link to existing Site entity if one matches this host
                    site_row = cn.execute(
                        "SELECT entity_id FROM corpus_entity "
                        "WHERE entity_type='Site' "
                        "AND (entity_id=? OR props_json LIKE ?)",
                        (host, f"%{host}%"),
                    ).fetchone()
                    if site_row:
                        rel    = "REACHABLE" if alive else "UNREACHABLE"
                        weight = 0.9 if alive else 0.1
                        _upsert_edge(ep_id, "Endpoint",
                                     site_row[0], "Site", rel, weight)

                    # Laptop Endpoint → Desktop Endpoint BRIDGES_TO edge
                    # when alive (piggyback route is live)
                    if alive and proto in ("rdp", "tcp"):
                        laptop_id = "bridge:laptop-relay"
                        _upsert_ep(laptop_id, "Laptop RDP Relay",
                                   {"role": "relay", "protocol": "rdp"})
                        _upsert_edge(laptop_id, "Endpoint",
                                     ep_id, "Endpoint",
                                     "BRIDGES_TO", 0.85)

                # Network topology rows → Endpoint entities
                try:
                    topo_rows = cn.execute(
                        "SELECT host, protocol, port, capability, "
                        "last_ok, ema_success, ema_latency_ms, source "
                        "FROM network_topology"
                    ).fetchall()
                    for (host, proto, port, cap, last_ok,
                         ema_ok, ema_lat, source) in topo_rows:
                        eid   = f"{proto}:{host}:{port or 0}"
                        label = f"{cap or host} [{proto}:{port or '?'}]"
                        _upsert_ep(eid, label,
                                   {"host": host, "port": port,
                                    "protocol": proto,
                                    "ema_success": ema_ok,
                                    "ema_latency_ms": ema_lat,
                                    "source": source})
                        topo_count += 1

                        # If the compute peer is reachable, add SERVES edge
                        # to the Peer entity if one exists
                        if ema_ok and float(ema_ok) >= 0.5:
                            peer_row = cn.execute(
                                "SELECT entity_id FROM corpus_entity "
                                "WHERE entity_type='Peer' "
                                "AND (entity_id=? OR props_json LIKE ?)",
                                (host, f"%{host}%"),
                            ).fetchone()
                            if peer_row:
                                _upsert_edge(eid, "Endpoint",
                                             peer_row[0], "Peer",
                                             "SERVES", float(ema_ok))

                except sqlite3.OperationalError:
                    pass  # network_topology not yet created

                # ── Step 4: symbiotic tunneling — horizontal Brain expansion ─
                # Closed-loop tcp/udp mesh constraint → Bayesian/Poisson
                # centroids of synaptic weights → inverted-ReLU ADAM nudge →
                # propeller routing of new SYMBIOTIC_TUNNEL edges.
                tunnel_stats: dict = {}
                try:
                    from src.brain.symbiotic_tunnel import (  # type: ignore[import]
                        vision_horizontal_expand,
                    )
                    tunnel_stats = vision_horizontal_expand(cn)
                except Exception as e:
                    if _is_network_error(e):
                        logging.info(
                            f"[synapse:vision] symbiotic tunnel soft-skip: {e}"
                        )
                    else:
                        logging.warning(
                            f"[synapse:vision] symbiotic_tunnel error: {e}"
                        )

                # ── Step 5: grounded tunneling — certainty-anchored collapser ─
                # Ground nodes (top certainty quartile) open expansory BFS
                # paths toward uncertain frontiers with temporary weight
                # resistance + torus amplification.  Expired paths collapse
                # into permanent GROUNDED_TUNNEL edges (nodal collapse).
                ground_stats: dict = {}
                try:
                    from src.brain.grounded_tunneling import (  # type: ignore[import]
                        ground_and_expand,
                    )
                    ground_stats = ground_and_expand(cn)
                except Exception as e:
                    if _is_network_error(e):
                        logging.info(
                            f"[synapse:vision] grounded_tunneling soft-skip: {e}"
                        )
                    else:
                        logging.warning(
                            f"[synapse:vision] grounded_tunneling error: {e}"
                        )

                cn.commit()
            finally:
                cn.close()

            elapsed = round(time.time() - t0, 1)
            _sw_kv_write(
                "synapse_vision_last",
                f"{datetime.now().isoformat()}"
                f"|live={live}|down={down}"
                f"|bridge={bridge_count}|topo={topo_count}"
                f"|tunnel_added={tunnel_stats.get('edges_added', 0)}"
                f"|tunnel_opt={tunnel_stats.get('edges_optimised', 0)}"
                f"|ground_nodes={ground_stats.get('ground_nodes', 0)}"
                f"|ground_paths={ground_stats.get('paths_opened', 0)}"
                f"|collapses={ground_stats.get('collapses', 0)}"
                f"|elapsed={elapsed}s",
            )
            logging.info(
                f"[synapse:vision] live={live} down={down} "
                f"bridge={bridge_count} topo={topo_count} "
                f"tunnel+={tunnel_stats.get('edges_added', 0)} "
                f"tunnel~={tunnel_stats.get('edges_optimised', 0)} "
                f"ground_nodes={ground_stats.get('ground_nodes', 0)} "
                f"paths={ground_stats.get('paths_opened', 0)} "
                f"collapses={ground_stats.get('collapses', 0)} "
                f"elapsed={elapsed}s"
            )
            ok = True

        except Exception as e:
            if _is_network_error(e):
                logging.info(f"[synapse:vision] network error (soft skip): {e}")
                ok = True
            else:
                logging.warning(f"[synapse:vision] iteration failed: {e}")

        sleep_s = _next_sleep_with_backoff(NAME, INTERVAL_S, JITTER_S, ok)
        if _wait_or_stop(sleep_s):
            return


# ─────────────────────────────────────────────────────────────────────────────
# Worker #6 — torus Touch (continuous boundary pressure on T^7, ~30s)
# ─────────────────────────────────────────────────────────────────────────────

def _torus_touch_worker() -> None:
    """Constantly push Touch against the torus edge.

    Every ~30 seconds this worker runs ``tick_torus_pressure`` which reads
    every ``Endpoint`` corpus entity, places it on the 7-D torus :math:`T^7`,
    measures the categorical occupancy gap field, and steps each endpoint
    one tick along the gap gradient (mod :math:`2\\pi`).

    The result is a *continuous* outward pressure that expands informational
    gaps in the multidimensional CAT state — the discrete tunneling pass in
    ``_vision_worker`` then fires every 5 minutes against the spread-out
    manifold, finding pairs that the static graph never could.

    Soft-fails on any DB error and continues; never holds the connection.
    """
    INTERVAL_S = 30
    JITTER_S   = 5
    NAME       = "torus"

    logging.info("[synapse:torus] started — interval=30s (T^7 boundary pressure)")
    if _wait_or_stop(random.randint(2, 8)):
        return

    while not _SYNAPTIC_STOP.is_set():
        t0 = time.time()
        ok = False
        diag: dict = {}
        try:
            _pkg = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            sys.path.insert(0, _pkg)
            from src.brain.local_store import db_path as _db_path  # type: ignore[import]
            from src.brain.torus_touch import tick_torus_pressure  # type: ignore[import]

            cn = sqlite3.connect(str(_db_path()))
            try:
                diag = tick_torus_pressure(cn)
            finally:
                cn.close()

            elapsed = round(time.time() - t0, 2)
            _sw_kv_write(
                "synapse_torus_last",
                f"{datetime.now().isoformat()}"
                f"|endpoints={diag.get('endpoints', 0)}"
                f"|moved={diag.get('moved', 0)}"
                f"|gap={round(diag.get('gap_after', 0.0), 4)}"
                f"|spread={diag.get('spread_after', 0.0)}%"
                f"|elapsed={elapsed}s",
            )
            ok = True
        except Exception as e:
            if _is_network_error(e):
                logging.info(f"[synapse:torus] soft-skip: {e}")
                ok = True
            else:
                logging.warning(f"[synapse:torus] tick failed: {e}")

        sleep_s = _next_sleep_with_backoff(NAME, INTERVAL_S, JITTER_S, ok)
        if _wait_or_stop(sleep_s):
            return


# ─────────────────────────────────────────────────────────────────────────────
# Lifecycle management
# ─────────────────────────────────────────────────────────────────────────────

def start_continuous_synaptic_agents() -> None:
    """Start all four synaptic workers as daemon threads.

    Idempotent — safe to call multiple times.  The main loop calls this once
    at startup, then proceeds with its own coarser cycle.  Workers run
    continuously underneath, building synapses ahead of where the main loop
    reads them.
    """
    global _SYNAPTIC_STARTED
    if _SYNAPTIC_STARTED:
        logging.info("Continuous synaptic agents already running.")
        return

    _SYNAPTIC_STOP.clear()
    workers = [
        ("synapse-builder",     _synaptic_builder_worker),
        ("synapse-lookahead",   _lookahead_worker),
        ("synapse-sweeper",     _dispersed_sweeper_worker),
        ("synapse-convergence", _convergence_worker),
        ("synapse-vision",      _vision_worker),
        ("synapse-torus",       _torus_touch_worker),
    ]
    for name, fn in workers:
        t = threading.Thread(target=fn, name=name, daemon=True)
        t.start()
        _SYNAPTIC_THREADS.append(t)

    _SYNAPTIC_STARTED = True
    _sw_kv_write("synapse_agents_started", datetime.now().isoformat())
    logging.info(
        f"Continuous synaptic agents started: "
        f"{', '.join(n for n, _ in workers)} "
        f"({len(workers)} threads, all daemon)."
    )


def stop_continuous_synaptic_agents(timeout: float = 5.0) -> None:
    """Signal all workers to exit and wait briefly for them to stop.

    Resets all state so a subsequent :func:`start_continuous_synaptic_agents`
    call starts a clean cohort.
    """
    global _SYNAPTIC_STARTED
    if not _SYNAPTIC_STARTED:
        return
    _SYNAPTIC_STOP.set()
    for t in _SYNAPTIC_THREADS:
        try:
            t.join(timeout=timeout)
        except Exception:
            pass
    _SYNAPTIC_THREADS.clear()
    _SYNAPTIC_FAILURES.clear()
    _SYNAPTIC_STARTED = False
    try:
        _sw_kv_write("synapse_agents_stopped", datetime.now().isoformat())
    except Exception:
        pass
    logging.info("Continuous synaptic agents stopped.")
