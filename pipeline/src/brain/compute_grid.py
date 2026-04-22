"""Shared Compute Grid — peer dispatch over the existing piggyback fabric.

The Brain reuses the same primitives that already glue the laptop/desktop
bridge together (`piggyback_router.py`, `bridge_watcher.ps1`, `agent_uplink.py`,
`trigger_bridge.py`):

    * Activation       — drop a `compute_*.trigger` file into the OneDrive-
                         synced `pipeline/bridge_triggers/` folder. Every
                         domain workstation already runs the bridge_watcher
                         scheduled task and will pick it up within ~5 s.
    * Rendezvous       — peer compute_nodes publish their capacity JSON to
                         `pipeline/bridge_state/compute_peers/<host>.json`.
                         OneDrive sync is the transport — no new ports.
    * Job channel      — raw TCP on port 8000 (already mapped by
                         bridge_watcher.ps1 as "LLM/API"). Wire format is the
                         same header-line + 8-byte BE size + payload bytes
                         used by `agent_uplink.py`, with HMAC over the body
                         using `SCBRAIN_GRID_SECRET`.

Public API:
    pick_compute_target(job_hint: dict | None = None) -> ComputeTarget
    submit_job(target, payload, *, timeout_s=None) -> dict
    discover_peers(force: bool = False) -> list[Peer]
    publish_local_capacity() -> Peer

Falls back to `local()` (in-process execution) whenever:
    * config/brain.yaml -> llms.compute_grid.enabled is false, OR
    * no peer heartbeat is fresh, OR
    * `fallback.local_only_if_no_peers` resolves true.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import socket
import struct
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from . import load_config


_PIPELINE_ROOT = Path(__file__).resolve().parents[2]
_DISCOVERY_LOCK = threading.Lock()
_DISCOVERY_CACHE: tuple[float, list["Peer"]] = (0.0, [])

# Idempotent local-node guard so any module that does I/O can lazily make
# sure THIS workstation is itself reachable on the grid.
_LOCAL_NODE_LOCK = threading.Lock()
_LOCAL_NODE_STARTED = False
_LOCAL_NODE_THREAD: threading.Thread | None = None

# Negative cache for unreachable peers so we don't pay multi-second TCP
# timeouts on every fanout round. address -> (cooldown_until_epoch, reason)
_PEER_DOWN_LOCK = threading.Lock()
_PEER_DOWN: dict[str, tuple[float, str]] = {}


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------
def _cfg() -> dict:
    return ((load_config().get("llms") or {}).get("compute_grid") or {})


def _resolve(rel_or_abs: str) -> Path:
    p = Path(rel_or_abs)
    if not p.is_absolute():
        p = _PIPELINE_ROOT.parent / rel_or_abs
    return p


def _state_dir() -> Path:
    d = _resolve(_cfg().get("rendezvous", {}).get(
        "state_dir", "pipeline/bridge_state/compute_peers"))
    d.mkdir(parents=True, exist_ok=True)
    return d


def _trigger_dir() -> Path:
    d = _resolve(_cfg().get("rendezvous", {}).get(
        "trigger_dir", "pipeline/bridge_triggers"))
    d.mkdir(parents=True, exist_ok=True)
    return d


def _secret() -> bytes:
    name = (_cfg().get("auth") or {}).get("shared_secret_env", "SCBRAIN_GRID_SECRET")
    return (os.environ.get(name) or "scbrain-dev").encode("utf-8")


# ---------------------------------------------------------------------------
# Capacity probing (CPU/GPU/RAM)
# ---------------------------------------------------------------------------
def _probe_local_capacity() -> dict[str, Any]:
    cap: dict[str, Any] = {
        "host":            socket.gethostname(),
        "ts":              datetime.now(timezone.utc).isoformat(),
        "cpu_count":       os.cpu_count() or 1,
        "cpu_load_1m":     0.0,
        "free_ram_gb":     0.0,
        "gpus":            [],
        "free_vram_gb":    0.0,
        "port":            int(_cfg().get("listen_port", 8000)),
    }
    try:
        import psutil
        cap["cpu_load_1m"] = float(psutil.cpu_percent(interval=0.1)) / 100.0
        cap["free_ram_gb"] = round(psutil.virtual_memory().available / (1024 ** 3), 2)
    except Exception:
        try:
            cap["cpu_load_1m"] = (os.getloadavg()[0] / max(cap["cpu_count"], 1)) \
                if hasattr(os, "getloadavg") else 0.0
        except Exception:
            pass
    # GPU probe — try NVIDIA first, then AMD/Intel via WMI on Windows.
    cap["gpus"].extend(_probe_nvidia_gpus())
    if not cap["gpus"]:
        cap["gpus"].extend(_probe_wmi_gpus())
    cap["free_vram_gb"] = round(
        sum(g.get("free_mb", 0) for g in cap["gpus"]) / 1024.0, 2)
    return cap


def _probe_nvidia_gpus() -> list[dict[str, Any]]:
    """NVIDIA via nvidia-smi. Empty list on non-NVIDIA / CPU-only hosts."""
    gpus: list[dict[str, Any]] = []
    try:
        import subprocess
        out = subprocess.run(
            ["nvidia-smi",
             "--query-gpu=name,memory.free,memory.total,utilization.gpu",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=2,
        )
        if out.returncode == 0:
            for line in out.stdout.strip().splitlines():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 4:
                    gpus.append({
                        "vendor":   "nvidia",
                        "name":     parts[0],
                        "free_mb":  int(parts[1]),
                        "total_mb": int(parts[2]),
                        "util_pct": int(parts[3]),
                    })
    except Exception:
        pass
    return gpus


def _probe_wmi_gpus() -> list[dict[str, Any]]:
    """AMD / Intel / discrete GPUs via Windows WMI. AdapterRAM is a DWORD
    capped at 4 GB, so for 8 GB+ cards we ALSO query the registry-derived
    `HardwareInformation.qwMemorySize` from `Win32_VideoController` via
    PowerShell. Returns empty list on non-Windows or on failure.
    """
    if os.name != "nt":
        return []
    try:
        import subprocess
        ps = (
            "$ErrorActionPreference='SilentlyContinue';"
            "Get-CimInstance Win32_VideoController | ForEach-Object {"
            "  $name = $_.Name;"
            "  $ram  = [int64]$_.AdapterRAM;"
            "  $qw   = (Get-ItemProperty -Path ($_.PNPDeviceID -replace '^','HKLM:\\SYSTEM\\CurrentControlSet\\Enum\\') -Name 'HardwareInformation.qwMemorySize' -EA SilentlyContinue).'HardwareInformation.qwMemorySize';"
            "  if ($qw) { $ram = [int64]$qw };"
            "  '{0}|{1}' -f $name, $ram"
            "}"
        )
        out = subprocess.run(
            ["powershell.exe", "-NoProfile", "-Command", ps],
            capture_output=True, text=True, timeout=4,
        )
        gpus: list[dict[str, Any]] = []
        if out.returncode != 0:
            return gpus
        for line in out.stdout.strip().splitlines():
            line = line.strip()
            if not line or "|" not in line:
                continue
            name, ram_s = [x.strip() for x in line.split("|", 1)]
            # Skip software / generic adapters that aren't useful for compute.
            if not name:
                continue
            low = name.lower()
            if any(skip in low for skip in (
                    "basic display", "microsoft basic", "remote display",
                    "meta virtual", "virtual display")):
                continue
            try:
                ram_b = int(ram_s)
            except Exception:
                ram_b = 0
            total_mb = max(0, ram_b // (1024 * 1024))
            vendor = "unknown"
            if "nvidia" in low:    vendor = "nvidia"
            elif "amd" in low or "radeon" in low: vendor = "amd"
            elif "intel" in low:   vendor = "intel"
            gpus.append({
                "vendor":   vendor,
                "name":     name,
                "free_mb":  total_mb,    # WMI gives total only; assume free=total.
                "total_mb": total_mb,
                "util_pct": 0,
            })
        return gpus
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Peer model
# ---------------------------------------------------------------------------
@dataclass
class Peer:
    host:        str
    address:     str | None
    port:        int
    cpu_count:   int
    cpu_load_1m: float
    free_ram_gb: float
    gpus:        list[dict[str, Any]] = field(default_factory=list)
    free_vram_gb: float = 0.0
    last_seen:   str = ""
    is_local:    bool = False

    def has_gpu(self) -> bool:
        return bool(self.gpus) and self.free_vram_gb > 0


@dataclass
class ComputeTarget:
    peer: Peer
    reason: str
    fallback: bool = False


# ---------------------------------------------------------------------------
# Heartbeat publication (call from compute_node daemon on every workstation)
# ---------------------------------------------------------------------------
def publish_local_capacity(*, ensure_listener: bool = True) -> Peer:
    """Write the local host's capacity into the OneDrive-synced state dir
    AND make sure the local compute_node listener is running so peers can
    actually reach this workstation. Other workstations see the JSON within
    seconds via OneDrive sync — same trick `bridge_watcher.ps1` uses for
    the wifi_ip.txt rendezvous."""
    if ensure_listener:
        # Lazy idempotent listener bootstrap. No-op if already running.
        try:
            ensure_local_node_running()
        except Exception:
            pass
    cap = _probe_local_capacity()
    p = _state_dir() / f"{cap['host']}.json"
    payload = {
        **cap,
        "address":  _best_local_ip(),
        "is_local": False,
    }
    p.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return _peer_from_dict(payload)


def ensure_local_node_running() -> bool:
    """Start the compute-node listener as a daemon thread if it's not
    already running in this process. Returns True if (now) running, False
    if startup failed. Safe to call repeatedly."""
    global _LOCAL_NODE_STARTED, _LOCAL_NODE_THREAD
    with _LOCAL_NODE_LOCK:
        if _LOCAL_NODE_STARTED and _LOCAL_NODE_THREAD and _LOCAL_NODE_THREAD.is_alive():
            return True
        port = int(_cfg().get("listen_port", 8000))
        # If port is already bound (e.g. another process is the node) treat
        # that as success — peers can still reach the box.
        if _port_in_use("127.0.0.1", port):
            _LOCAL_NODE_STARTED = True
            return True
        try:
            t = threading.Thread(target=serve_compute_node,
                                 name="grid-node-auto", daemon=True)
            t.start()
            _LOCAL_NODE_THREAD = t
            _LOCAL_NODE_STARTED = True
            # Give the listener a moment to bind before we declare success.
            for _ in range(20):
                if _port_in_use("127.0.0.1", port):
                    return True
                time.sleep(0.05)
            return _port_in_use("127.0.0.1", port)
        except Exception:
            return False


def _port_in_use(host: str, port: int, timeout: float = 0.15) -> bool:
    """Fast TCP connect probe (~150ms cap). True iff something is listening."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def _best_local_ip() -> str | None:
    """Pick the most useful interface IP (Wi-Fi/LAN, not loopback)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))   # no packet sent — just resolves the iface
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return None


def _peer_from_dict(d: dict) -> Peer:
    return Peer(
        host=d.get("host", "unknown"),
        address=d.get("address"),
        port=int(d.get("port", 8000)),
        cpu_count=int(d.get("cpu_count", 1)),
        cpu_load_1m=float(d.get("cpu_load_1m", 0.0)),
        free_ram_gb=float(d.get("free_ram_gb", 0.0)),
        gpus=list(d.get("gpus") or []),
        free_vram_gb=float(d.get("free_vram_gb", 0.0)),
        last_seen=str(d.get("ts", "")),
        is_local=bool(d.get("is_local", False)),
    )


# ---------------------------------------------------------------------------
# Discovery — read OneDrive-synced state dir + optional AD + seed list.
# Activation drops a `compute_*.trigger` so dormant peers wake up.
# ---------------------------------------------------------------------------
def discover_peers(force: bool = False) -> list[Peer]:
    global _DISCOVERY_CACHE
    cfg = _cfg()
    ttl = float(cfg.get("discovery", {}).get("cache_ttl_s", 300))
    now = time.time()
    with _DISCOVERY_LOCK:
        ts, cached = _DISCOVERY_CACHE
        if not force and cached and (now - ts) < ttl:
            return cached

    stale_after = float(cfg.get("rendezvous", {}).get("stale_after_s", 120))
    cutoff = datetime.now(timezone.utc).timestamp() - stale_after

    peers: dict[str, Peer] = {}
    self_host = socket.gethostname().lower()
    # 1) Piggyback rendezvous — read every JSON heartbeat that's still fresh
    for fp in _state_dir().glob("*.json"):
        try:
            d = json.loads(fp.read_text(encoding="utf-8"))
            if not d.get("ts"):
                continue
            try:
                ts_dt = datetime.fromisoformat(d["ts"]).timestamp()
            except Exception:
                ts_dt = fp.stat().st_mtime
            if ts_dt < cutoff:
                continue
            # Self-host detection: if the heartbeat we're reading is OUR own
            # JSON, mark it is_local so submit_job short-circuits to the
            # in-process executor instead of paying TCP overhead.
            if str(d.get("host", "")).lower() == self_host:
                d["is_local"] = True
                d["address"] = "127.0.0.1"
            p = _peer_from_dict(d)
            peers[p.host.lower()] = p
        except Exception:
            continue

    # 2) Drop activation triggers so dormant peers wake their compute_node.
    #    bridge_watcher.ps1 already polls this folder every 5s on every host.
    if "piggyback" in (cfg.get("discovery", {}).get("methods") or []):
        try:
            tf = _trigger_dir() / f"compute_{int(time.time())}.trigger"
            tf.write_text(
                json.dumps({"action": "compute_heartbeat",
                            "requested_by": socket.gethostname(),
                            "port": int(cfg.get("listen_port", 8000))}),
                encoding="utf-8",
            )
        except Exception:
            pass

    # 3) Optional explicit seeds (host or ip)
    for seed in cfg.get("discovery", {}).get("seeds") or []:
        if seed and seed.lower() not in peers:
            peers[seed.lower()] = Peer(
                host=seed, address=seed, port=int(cfg.get("listen_port", 8000)),
                cpu_count=0, cpu_load_1m=0.0, free_ram_gb=0.0,
            )

    # 4) Optional AD scrape (PowerShell Get-ADComputer). Best-effort.
    if "ad" in (cfg.get("discovery", {}).get("methods") or []):
        for host in _ad_hosts(cfg):
            if host.lower() not in peers:
                peers[host.lower()] = Peer(
                    host=host, address=host,
                    port=int(cfg.get("listen_port", 8000)),
                    cpu_count=0, cpu_load_1m=0.0, free_ram_gb=0.0,
                )

    out = list(peers.values())
    _DISCOVERY_CACHE = (now, out)
    return out


def _ad_hosts(cfg: dict) -> list[str]:
    try:
        import subprocess
        flt = cfg.get("discovery", {}).get(
            "ad_filter", 'OperatingSystem -like "Windows*"')
        n = int(cfg.get("discovery", {}).get("ad_max_hosts", 64))
        ps = ("Get-ADComputer -Filter '" + flt + "'"
              " | Select-Object -ExpandProperty DNSHostName"
              f" | Select-Object -First {n}")
        out = subprocess.run(
            ["powershell.exe", "-NoProfile", "-Command", ps],
            capture_output=True, text=True, timeout=8,
        )
        if out.returncode == 0:
            return [ln.strip() for ln in out.stdout.splitlines() if ln.strip()]
    except Exception:
        pass
    return []


# ---------------------------------------------------------------------------
# Selection & local fallback
# ---------------------------------------------------------------------------
def local() -> Peer:
    cap = _probe_local_capacity()
    cap["address"] = "127.0.0.1"
    cap["is_local"] = True
    return _peer_from_dict(cap)


def pick_compute_target(job_hint: dict | None = None) -> ComputeTarget:
    """Choose the best peer for a job. Honors selection caps and the
    `fallback.always_include_local` safety net."""
    cfg = _cfg()
    if not cfg.get("enabled", True):
        return ComputeTarget(peer=local(), reason="grid disabled", fallback=True)

    fb = cfg.get("fallback") or {}
    if fb.get("local_only_if_no_peers", False):
        return ComputeTarget(peer=local(), reason="local-only mode", fallback=True)

    sel = cfg.get("selection") or {}
    max_cpu = float(sel.get("max_cpu_load", 0.80))
    min_ram = float(sel.get("min_free_ram_gb", 4))
    min_vram = float(sel.get("min_free_vram_gb", 2))
    prefer_gpu = bool(sel.get("prefer_gpu", True))
    needs_gpu = bool((job_hint or {}).get("needs_gpu", False))

    candidates: list[Peer] = []
    for p in discover_peers():
        if p.cpu_load_1m > max_cpu:
            continue
        if p.free_ram_gb < min_ram and p.free_ram_gb > 0:
            continue
        if needs_gpu and (not p.has_gpu() or p.free_vram_gb < min_vram):
            continue
        candidates.append(p)

    def _score(p: Peer) -> float:
        gpu_bonus = (p.free_vram_gb if (prefer_gpu or needs_gpu) else 0.0)
        return (p.free_ram_gb + gpu_bonus * 4.0
                - p.cpu_load_1m * 16.0
                + p.cpu_count * 0.25)

    candidates.sort(key=_score, reverse=True)
    if candidates:
        return ComputeTarget(peer=candidates[0],
                             reason="best peer by free CPU/GPU/RAM")
    if fb.get("always_include_local", True):
        return ComputeTarget(peer=local(),
                             reason="no eligible peer — local fallback",
                             fallback=True)
    raise RuntimeError("compute_grid: no peer available and local fallback is disabled")


# ---------------------------------------------------------------------------
# Job channel — same wire format as pipeline/agent_uplink.py
# Header line:  b"[GRID_JOB]:<job_id>\n"
# Body:         8-byte BE length + JSON payload
# Auth:         HMAC-SHA256 of body, hex-encoded, on header line:
#               b"[GRID_JOB]:<job_id>:<hmac>\n"
# Response:     8-byte BE length + JSON payload
# ---------------------------------------------------------------------------
def submit_job(target: ComputeTarget, payload: dict,
               *, timeout_s: float | None = None) -> dict:
    """Send a job to `target` and return the JSON response. Raises on socket
    failure so the ensemble can choose to retry on the local fallback."""
    if target.peer.is_local or target.peer.address in (None, "", "127.0.0.1"):
        return _execute_locally(payload)

    addr = target.peer.address
    port = int(target.peer.port or _cfg().get("listen_port", 8000))
    cfg = _cfg()
    connect_timeout = float(cfg.get("connect_timeout_s", 1.0))
    timeout = float(timeout_s or cfg.get("request_timeout_s", 8.0))

    # Negative cache: skip peers we just confirmed as down.
    cooldown = float(cfg.get("down_cooldown_s", 30.0))
    key = f"{addr}:{port}"
    with _PEER_DOWN_LOCK:
        entry = _PEER_DOWN.get(key)
        if entry and entry[0] > time.time():
            raise RuntimeError(
                f"compute_grid: peer {key} is in down-cache ({entry[1]})")

    # Fast TCP pre-probe — fail in <connect_timeout instead of stalling.
    if not _port_in_use(addr, port, timeout=connect_timeout):
        with _PEER_DOWN_LOCK:
            _PEER_DOWN[key] = (time.time() + cooldown, "port closed")
        raise RuntimeError(
            f"compute_grid: peer {key} not listening (probe failed)")

    job_id = hashlib.sha1(
        f"{addr}:{port}:{time.time_ns()}".encode("utf-8")).hexdigest()[:12]
    body = json.dumps(payload, default=str).encode("utf-8")
    sig = hmac.new(_secret(), body, hashlib.sha256).hexdigest()
    header = f"[GRID_JOB]:{job_id}:{sig}\n".encode("utf-8")

    try:
        with socket.create_connection((addr, port), timeout=timeout) as s:
            s.settimeout(timeout)
            s.sendall(header)
            s.sendall(struct.pack(">Q", len(body)))
            s.sendall(body)
            s.shutdown(socket.SHUT_WR)
            size_bytes = _recv_exact(s, 8)
            (n,) = struct.unpack(">Q", size_bytes)
            data = _recv_exact(s, n)
            # Clear any stale down-cache entry on a successful round-trip.
            with _PEER_DOWN_LOCK:
                _PEER_DOWN.pop(key, None)
            return json.loads(data.decode("utf-8"))
    except Exception as e:
        with _PEER_DOWN_LOCK:
            _PEER_DOWN[key] = (time.time() + cooldown, str(e)[:80])
        raise RuntimeError(
            f"compute_grid: peer {addr}:{port} unreachable ({e})") from e


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(4096, n - len(buf)))
        if not chunk:
            raise ConnectionError("peer closed connection mid-message")
        buf += chunk
    return bytes(buf)


# ---------------------------------------------------------------------------
# Local execution shim — used both for the local fallback and (on the
# remote side) by the compute_node daemon when it processes an incoming job.
# Currently delegates straight to the offline ensemble caller; production
# code will replace this with the real model invocation routine.
# ---------------------------------------------------------------------------
def _execute_locally(payload: dict) -> dict:
    from .llm_router import select_llm
    from .llm_ensemble import _offline_caller   # safe internal use
    task = payload.get("task") or "default"
    body = payload.get("body")
    decision = select_llm(task, log=False)
    response = _offline_caller(decision, body, _cfg())
    return {
        "host":     socket.gethostname(),
        "model":    decision.model_id,
        "score":    decision.score,
        "response": response,
        "executed": "local",
    }


# ---------------------------------------------------------------------------
# Compute-node daemon — runs on every workstation; serves grid jobs on
# port 8000 (already exposed by bridge_watcher.ps1) and re-publishes
# capacity on a heartbeat cadence.
# ---------------------------------------------------------------------------
def serve_compute_node() -> None:
    """Long-running listener. Intended to be started by the autonomous_agent
    or a Scheduled Task on every workstation."""
    cfg = _cfg()
    port = int(cfg.get("listen_port", 8000))
    interval = float(cfg.get("rendezvous", {}).get("heartbeat_secs", 30))

    threading.Thread(target=_heartbeat_loop, args=(interval,),
                     name="grid-heartbeat", daemon=True).start()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        srv.bind(("0.0.0.0", port))
    except OSError as e:
        # Port already bound (likely another grid-node instance). Don't kill
        # the autonomous_agent — just exit this thread; the heartbeat loop
        # is still publishing capacity.
        srv.close()
        try:
            import logging
            logging.warning(f"compute_grid: listener could not bind :{port} ({e}); "
                            "another node likely owns the port.")
        except Exception:
            pass
        return
    srv.listen(16)
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=_handle_grid_conn, args=(conn, addr),
                         name=f"grid-job-{addr[0]}", daemon=True).start()


def _heartbeat_loop(interval: float) -> None:
    while True:
        try:
            publish_local_capacity()
        except Exception:
            pass
        time.sleep(max(5.0, interval))


def _handle_grid_conn(conn: socket.socket, addr) -> None:
    try:
        header = bytearray()
        while not header.endswith(b"\n"):
            ch = conn.recv(1)
            if not ch:
                return
            header += ch
        line = header.decode("utf-8", errors="ignore").strip()
        if not line.startswith("[GRID_JOB]:"):
            return
        parts = line.split(":")
        if len(parts) < 3:
            return
        sig = parts[-1]
        size = struct.unpack(">Q", _recv_exact(conn, 8))[0]
        body = _recv_exact(conn, size)
        expected = hmac.new(_secret(), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            resp = json.dumps({"error": "bad signature"}).encode("utf-8")
        else:
            try:
                result = _execute_locally(json.loads(body.decode("utf-8")))
                resp = json.dumps(result, default=str).encode("utf-8")
            except Exception as e:
                resp = json.dumps({"error": str(e)}).encode("utf-8")
        conn.sendall(struct.pack(">Q", len(resp)))
        conn.sendall(resp)
    finally:
        conn.close()


__all__ = [
    "Peer",
    "ComputeTarget",
    "discover_peers",
    "publish_local_capacity",
    "ensure_local_node_running",
    "pick_compute_target",
    "submit_job",
    "serve_compute_node",
    "local",
]
