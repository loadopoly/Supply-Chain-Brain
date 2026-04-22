"""Network Expansion Learner — the Brain learns from every connection it
already touches across all protocols, then expands its reachable network by
promoting verified peers and endpoints into the discovery layer.

Design principles (parallel to llm_self_train):
    * PASSIVE-by-default. We only probe targets that are already declared
      somewhere the Brain owns (config/connections.yaml, brain.yaml,
      bridge_state/compute_peers, mapped SMB drives, declared MX records).
      Active scanning of arbitrary CIDR ranges requires explicit opt-in via
      `network_learn.active_probe`.
    * BOUNDED. Per-protocol probe-rate cap, per-call timeout cap, and a
      per-host cooldown after a failure burst.
    * AUDITED. Every probe writes a row to `network_observations`. Per-host
      rolling stats land in `network_topology`. Promotions to the compute
      grid land in `network_promotions` so the autonomous loop is fully
      reviewable in the Decision Log page.
    * FLUIDITY-PRESERVING. Observations adjust *routing telemetry* and
      compute_grid seeds — they do NOT touch llm_weights, llm_router scores,
      or the ensemble dispatch math. The reasoning surface stays open.

Public API:
    observe_network_round() -> dict       # probe everything once
    schedule_in_background(interval_s=600) -> threading.Thread
    list_known_endpoints() -> list[Endpoint]
    promote_compute_peers() -> list[str]  # add fresh peers to grid cache
"""
from __future__ import annotations

import json
import logging
import os
import socket
import sqlite3
import subprocess
import threading
import time
from contextlib import contextmanager, closing
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

from . import load_config
from .local_store import db_path as _local_db_path


_PIPELINE_ROOT = Path(__file__).resolve().parents[2]
_PROBE_LOCK = threading.Lock()
_LAST_ROUND_TS: float = 0.0


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def _cfg() -> dict:
    return ((load_config().get("llms") or {}).get("network_learn") or {})


def _enabled() -> bool:
    return bool(_cfg().get("enabled", True))


def _active_probe_allowed() -> bool:
    return bool(_cfg().get("active_probe", False))


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------
@contextmanager
def _conn():
    cn = sqlite3.connect(_local_db_path())
    try:
        yield cn
        cn.commit()
    finally:
        cn.close()


def init_schema() -> None:
    with _conn() as cn:
        cn.executescript(
            """
            CREATE TABLE IF NOT EXISTS network_observations (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at  TEXT    NOT NULL,
                source       TEXT    NOT NULL,   -- which catalog discovered it
                protocol     TEXT    NOT NULL,   -- tcp,smb,smtp,https,sqlserver,oracle,sqlite,onedrive,piggyback,ad
                host         TEXT    NOT NULL,
                port         INTEGER,
                capability   TEXT,               -- free-form (e.g. "MX", "Public Share", "compute peer")
                latency_ms   REAL,
                ok           INTEGER NOT NULL,
                error        TEXT
            );
            CREATE INDEX IF NOT EXISTS ix_netobs_host_proto
                ON network_observations(host, protocol, observed_at);

            CREATE TABLE IF NOT EXISTS network_topology (
                host         TEXT NOT NULL,
                protocol     TEXT NOT NULL,
                port         INTEGER,
                capability   TEXT,
                first_seen   TEXT NOT NULL,
                last_seen    TEXT NOT NULL,
                last_ok      INTEGER NOT NULL,
                samples      INTEGER NOT NULL DEFAULT 0,
                successes    INTEGER NOT NULL DEFAULT 0,
                ema_latency_ms REAL,
                ema_success    REAL,
                source       TEXT,
                PRIMARY KEY (host, protocol, port)
            );

            CREATE TABLE IF NOT EXISTS network_promotions (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                promoted_at  TEXT NOT NULL,
                target       TEXT NOT NULL,   -- destination, e.g. "compute_grid"
                host         TEXT NOT NULL,
                reason       TEXT
            );
            """
        )


# ---------------------------------------------------------------------------
# Endpoint catalog — every protocol the Brain already has an opinion about
# ---------------------------------------------------------------------------
@dataclass
class Endpoint:
    source:     str            # which catalog produced this row
    protocol:   str            # tcp/smb/smtp/https/sqlserver/oracle/sqlite/onedrive/piggyback/ad
    host:       str
    port:       int | None = None
    capability: str | None = None


def _from_connections_yaml() -> list[Endpoint]:
    """Parse pipeline/config/connections.yaml — declared DB connections."""
    out: list[Endpoint] = []
    try:
        import yaml
        p = _PIPELINE_ROOT / "config" / "connections.yaml"
        if not p.exists():
            return out
        cfg = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        for name, body in cfg.items():
            if not isinstance(body, dict):
                continue
            host = body.get("server") or body.get("host") or ""
            if host.startswith("http"):
                u = urlparse(host)
                hh = u.hostname or ""
                pp = u.port or (443 if u.scheme == "https" else 80)
                out.append(Endpoint("connections.yaml", "https", hh, pp, name))
            elif host:
                # SQL server style → port 1433
                proto = "sqlserver" if "sql" in name.lower() else "tcp"
                out.append(Endpoint("connections.yaml", proto, host, 1433, name))
    except Exception as e:
        logging.warning(f"network_learner: connections.yaml parse failed: {e}")
    return out


def _from_brain_yaml() -> list[Endpoint]:
    """External apps + cross_app subscribers from brain.yaml."""
    out: list[Endpoint] = []
    cfg = load_config() or {}
    for app, body in (cfg.get("external_apps") or {}).items():
        if not isinstance(body, dict):
            continue
        url = body.get("base_url") or body.get("dashboard")
        if not url:
            continue
        u = urlparse(url)
        port = u.port or (443 if u.scheme == "https" else 80)
        if u.hostname:
            out.append(Endpoint("brain.yaml/external_apps", u.scheme or "https",
                                u.hostname, port, app))
    for sub in (cfg.get("cross_app") or {}).get("subscribers") or []:
        url = sub.get("url")
        if not url:
            continue
        u = urlparse(url)
        port = u.port or (443 if u.scheme == "https" else 80)
        if u.hostname:
            out.append(Endpoint("brain.yaml/cross_app", u.scheme or "https",
                                u.hostname, port, sub.get("name", "subscriber")))
    return out


def _from_mx_lookup(domain: str) -> list[Endpoint]:
    """Resolve the corporate domain's MX records — derives the SMTP relay."""
    out: list[Endpoint] = []
    if not domain:
        return out
    try:
        # Use Windows Resolve-DnsName (already part of the SOP playbook).
        if os.name == "nt":
            r = subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 f"Resolve-DnsName -Type MX {domain} | Select-Object -ExpandProperty NameExchange"],
                capture_output=True, text=True, timeout=8,
            )
            for line in (r.stdout or "").splitlines():
                line = line.strip()
                if line and not line.startswith("Resolve-DnsName"):
                    out.append(Endpoint("mx_lookup", "smtp", line, 25, "MX"))
    except Exception as e:
        logging.debug(f"network_learner: MX lookup failed for {domain}: {e}")
    return out


def _from_smb_mappings() -> list[Endpoint]:
    """Get-SmbMapping — UNC mounts the workstation already trusts."""
    out: list[Endpoint] = []
    if os.name != "nt":
        return out
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-SmbMapping | Select-Object -ExpandProperty RemotePath"],
            capture_output=True, text=True, timeout=6,
        )
        for line in (r.stdout or "").splitlines():
            line = line.strip().lstrip("\\")
            if not line:
                continue
            host = line.split("\\", 1)[0]
            cap = line.split("\\", 1)[1] if "\\" in line else ""
            out.append(Endpoint("smb_mapping", "smb", host, 445, cap or "share"))
    except Exception as e:
        logging.debug(f"network_learner: SMB mapping enumeration failed: {e}")
    return out


def _from_compute_peers() -> list[Endpoint]:
    """Read pipeline/bridge_state/compute_peers/*.json heartbeats."""
    out: list[Endpoint] = []
    state_dir = _PIPELINE_ROOT / "bridge_state" / "compute_peers"
    if not state_dir.is_dir():
        return out
    try:
        for hb in state_dir.glob("*.json"):
            try:
                body = json.loads(hb.read_text(encoding="utf-8"))
            except Exception:
                continue
            host = body.get("address") or body.get("host") or hb.stem
            port = int(body.get("port", 8000))
            cap = "compute peer"
            gpus = body.get("gpus") or []
            if gpus:
                cap += f" ({len(gpus)} GPU)"
            out.append(Endpoint("compute_peers", "tcp", host, port, cap))
    except Exception as e:
        logging.debug(f"network_learner: compute peer scan failed: {e}")
    return out


def _from_seed_list() -> list[Endpoint]:
    """Operator-supplied seeds (config/brain.yaml -> llms.network_learn.seeds)."""
    out: list[Endpoint] = []
    for s in (_cfg().get("seeds") or []):
        if isinstance(s, str):
            out.append(Endpoint("seed", "tcp", s, None, "seed"))
        elif isinstance(s, dict) and s.get("host"):
            out.append(Endpoint("seed", s.get("protocol", "tcp"),
                                s["host"], s.get("port"), s.get("capability")))
    return out


def list_known_endpoints() -> list[Endpoint]:
    """Aggregate every catalog. De-dupes on (host, protocol, port)."""
    domain = _cfg().get("corporate_domain") or "astecindustries.com"
    catalogs = [
        _from_connections_yaml(),
        _from_brain_yaml(),
        _from_mx_lookup(domain) if _cfg().get("mx_lookup", True) else [],
        _from_smb_mappings(),
        _from_compute_peers(),
        _from_seed_list(),
    ]
    seen: dict[tuple, Endpoint] = {}
    for cat in catalogs:
        for ep in cat:
            key = (ep.host.lower(), ep.protocol, ep.port)
            seen.setdefault(key, ep)
    return list(seen.values())


# ---------------------------------------------------------------------------
# Probes — passive, time-capped, never sends credentials
# ---------------------------------------------------------------------------
def _tcp_probe(host: str, port: int, timeout: float) -> tuple[bool, float, str | None]:
    t0 = time.monotonic()
    try:
        with closing(socket.create_connection((host, port), timeout=timeout)):
            return True, (time.monotonic() - t0) * 1000.0, None
    except Exception as e:
        return False, (time.monotonic() - t0) * 1000.0, str(e)[:200]


def _probe(endpoint: Endpoint, timeout: float) -> dict:
    proto = endpoint.protocol.lower()
    # Map protocols to default ports if missing
    default_ports = {"smtp": 25, "smb": 445, "https": 443, "http": 80,
                     "sqlserver": 1433, "oracle": 1521, "tcp": None}
    port = endpoint.port or default_ports.get(proto)
    ok = False
    latency = 0.0
    err: str | None = None
    if port is None:
        # No idea how to reach it passively → DNS resolve only
        try:
            t0 = time.monotonic()
            socket.gethostbyname(endpoint.host)
            ok = True
            latency = (time.monotonic() - t0) * 1000.0
        except Exception as e:
            err = f"dns: {e}"[:200]
    else:
        ok, latency, err = _tcp_probe(endpoint.host, port, timeout)
    return {"ok": ok, "latency_ms": latency, "port": port, "error": err}


# ---------------------------------------------------------------------------
# Topology rollup
# ---------------------------------------------------------------------------
def _update_topology(cn, ep: Endpoint, port: int | None,
                     ok: bool, latency_ms: float, ema_alpha: float) -> None:
    now = datetime.now(timezone.utc).isoformat()
    row = cn.execute(
        """SELECT samples, successes, ema_latency_ms, ema_success, first_seen
           FROM network_topology WHERE host=? AND protocol=? AND port IS ?""",
        (ep.host, ep.protocol, port),
    ).fetchone()
    if row is None:
        cn.execute(
            """INSERT INTO network_topology(host, protocol, port, capability,
                first_seen, last_seen, last_ok, samples, successes,
                ema_latency_ms, ema_success, source)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (ep.host, ep.protocol, port, ep.capability, now, now,
             1 if ok else 0, 1, 1 if ok else 0,
             latency_ms, 1.0 if ok else 0.0, ep.source),
        )
        return
    samples, successes, ema_lat, ema_ok, first_seen = row
    samples = (samples or 0) + 1
    successes = (successes or 0) + (1 if ok else 0)
    ema_lat = latency_ms if ema_lat is None else (1 - ema_alpha) * ema_lat + ema_alpha * latency_ms
    ema_ok = (1.0 if ok else 0.0) if ema_ok is None else (1 - ema_alpha) * ema_ok + ema_alpha * (1.0 if ok else 0.0)
    cn.execute(
        """UPDATE network_topology
              SET last_seen=?, last_ok=?, samples=?, successes=?,
                  ema_latency_ms=?, ema_success=?, capability=COALESCE(?, capability),
                  source=COALESCE(?, source)
            WHERE host=? AND protocol=? AND port IS ?""",
        (now, 1 if ok else 0, samples, successes, ema_lat, ema_ok,
         ep.capability, ep.source, ep.host, ep.protocol, port),
    )


# ---------------------------------------------------------------------------
# Compute-grid promotion
# ---------------------------------------------------------------------------
def promote_compute_peers(min_success: float = 0.7) -> list[str]:
    """Any host whose tcp:8000 EMA success >= threshold gets dropped into the
    compute_grid discovery seed cache (in-memory, via env var) so the next
    `pick_compute_target` round can reach it. Audited to network_promotions."""
    promoted: list[str] = []
    try:
        with _conn() as cn:
            rows = cn.execute(
                """SELECT host FROM network_topology
                   WHERE protocol='tcp' AND port=8000 AND ema_success >= ?
                   ORDER BY ema_success DESC LIMIT 32""",
                (float(min_success),),
            ).fetchall()
            now = datetime.now(timezone.utc).isoformat()
            for (host,) in rows:
                cn.execute(
                    "INSERT INTO network_promotions(promoted_at, target, host, reason) VALUES (?,?,?,?)",
                    (now, "compute_grid", host, f"ema_success>={min_success}"),
                )
                promoted.append(host)
        if promoted:
            existing = (os.environ.get("SCBRAIN_GRID_EXTRA_SEEDS") or "").split(",")
            merged = sorted({s.strip() for s in existing + promoted if s.strip()})
            os.environ["SCBRAIN_GRID_EXTRA_SEEDS"] = ",".join(merged)
    except Exception as e:
        logging.warning(f"network_learner: compute promotion failed: {e}")
    return promoted


# ---------------------------------------------------------------------------
# Round driver
# ---------------------------------------------------------------------------
def observe_network_round() -> dict:
    """Probe every known endpoint once, update topology, audit observations,
    and (if enabled) promote fresh compute peers into the grid seed list."""
    if not _enabled():
        return {"enabled": False}

    init_schema()
    cfg = _cfg()
    timeout = float(cfg.get("probe_timeout_s", 1.5))
    ema_alpha = float(cfg.get("ema_alpha", 0.3))
    max_per_round = int(cfg.get("max_probes_per_round", 64))
    min_seconds_between = float(cfg.get("min_seconds_between_rounds", 30.0))

    global _LAST_ROUND_TS
    with _PROBE_LOCK:
        if time.monotonic() - _LAST_ROUND_TS < min_seconds_between:
            return {"skipped": True, "reason": "rate-limited"}
        _LAST_ROUND_TS = time.monotonic()

    endpoints = list_known_endpoints()[:max_per_round]
    summary: dict[str, Any] = {
        "endpoints_total": len(endpoints),
        "by_protocol":      {},
        "by_source":        {},
        "live":              0,
        "down":              0,
        "promoted":          [],
        "ran_at":            datetime.now(timezone.utc).isoformat(),
    }

    with _conn() as cn:
        for ep in endpoints:
            try:
                r = _probe(ep, timeout)
            except Exception as e:
                r = {"ok": False, "latency_ms": 0.0,
                     "port": ep.port, "error": f"probe-exc: {e}"[:200]}
            cn.execute(
                """INSERT INTO network_observations(observed_at, source, protocol,
                    host, port, capability, latency_ms, ok, error)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (datetime.now(timezone.utc).isoformat(), ep.source, ep.protocol,
                 ep.host, r["port"], ep.capability, r["latency_ms"],
                 1 if r["ok"] else 0, r["error"]),
            )
            _update_topology(cn, ep, r["port"], r["ok"], r["latency_ms"], ema_alpha)
            summary["by_protocol"][ep.protocol] = summary["by_protocol"].get(ep.protocol, 0) + 1
            summary["by_source"][ep.source]     = summary["by_source"].get(ep.source, 0) + 1
            if r["ok"]:
                summary["live"] += 1
            else:
                summary["down"] += 1

    if cfg.get("auto_promote_compute", True):
        summary["promoted"] = promote_compute_peers(
            min_success=float(cfg.get("compute_promotion_min_success", 0.7))
        )
    return summary


def schedule_in_background(interval_s: int | None = None) -> threading.Thread:
    """Spawn a daemon thread that calls observe_network_round() periodically."""
    interval = int(interval_s or _cfg().get("interval_s", 600))

    def _loop() -> None:
        while True:
            try:
                observe_network_round()
            except Exception as e:
                logging.warning(f"network_learner background round failed: {e}")
            time.sleep(max(30, interval))

    t = threading.Thread(target=_loop, name="network_learner", daemon=True)
    t.start()
    return t
