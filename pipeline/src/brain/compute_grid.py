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
import logging
import os
import socket
import struct
import subprocess
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
# Dev Tunnel forward state — one local TCP forward per tunnel_id
# tunnel_id -> (local_port, proc)
# ---------------------------------------------------------------------------
_DT_FORWARD_LOCK = threading.Lock()
_DT_FORWARDS: dict[str, tuple[int, "subprocess.Popen | None"]] = {}
_DT_EXE: Path | None = None
_DT_EXE_LOCK = threading.Lock()
_DT_EXE_URL = "https://aka.ms/TunnelsCliDownload/win-x64"
_DT_FORWARD_START_TIMEOUT = 8.0   # seconds to wait for local port to open
_DT_FORWARD_BASE_PORT   = 18000   # start of ephemeral local port range


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
    # Dev Tunnel transport — "tcp" (default) or "devtunnel"
    transport:   str = "tcp"
    tunnel_id:   str = ""   # e.g. "scbrain-hideout.use2"

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
        transport=str(d.get("transport", "tcp")),
        tunnel_id=str(d.get("tunnel_id", "")),
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
            # Dev Tunnel peers are exempt from the stale cutoff — the TCP
            # probe (via the local dt-forward) is the real liveness check.
            is_tunnel = str(d.get("transport", "tcp")) == "devtunnel"
            if not is_tunnel:
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

    # 3b) Dev Tunnel seeds — external nodes reachable only via VS Code Dev Tunnel.
    #     These peers are immune to the stale-cutoff: the TCP probe (connect
    #     attempt) is the real liveness check once the forward is established.
    for seed in cfg.get("discovery", {}).get("devtunnel_seeds") or []:
        tid   = str(seed.get("tunnel_id", "")).strip()
        hname = str(seed.get("host",      tid)).strip()
        if not tid or not hname:
            continue
        key = hname.lower()
        if key in peers:
            # Heartbeat already loaded from state_dir (Hideout has OneDrive)
            peers[key].transport = "devtunnel"
            peers[key].tunnel_id = tid
        else:
            peers[key] = Peer(
                host=hname, address=None,
                port=int(cfg.get("listen_port", 8000)),
                cpu_count=0, cpu_load_1m=0.0, free_ram_gb=0.0,
                transport="devtunnel", tunnel_id=tid,
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
    if target.peer.is_local or target.peer.address in (None, "", "127.0.0.1") \
            and target.peer.transport != "devtunnel":
        return _execute_locally(payload)

    cfg = _cfg()
    connect_timeout = float(cfg.get("connect_timeout_s", 1.0))
    timeout = float(timeout_s or cfg.get("request_timeout_s", 8.0))

    # Dev Tunnel path — create or reuse a local TCP forward via dt connect
    if target.peer.transport == "devtunnel" and target.peer.tunnel_id:
        fwd = _ensure_devtunnel_forward(target.peer.tunnel_id)
        if fwd is None:
            raise RuntimeError(
                f"compute_grid: devtunnel forward for "
                f"{target.peer.tunnel_id} is unavailable")
        addr, port = fwd
    else:
        addr = target.peer.address
        port = int(target.peer.port or cfg.get("listen_port", 8000))

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
# Dev Tunnel helpers — download dt.exe once, manage local port forwards
# ---------------------------------------------------------------------------

def _ensure_dt_exe() -> "Path | None":
    """Return the path to dt.exe, downloading it if necessary.
    Returns None if the download fails or this isn't Windows."""
    global _DT_EXE
    if _DT_EXE and _DT_EXE.exists():
        return _DT_EXE
    with _DT_EXE_LOCK:
        if _DT_EXE and _DT_EXE.exists():
            return _DT_EXE
        if os.name != "nt":
            return None
        dest = _PIPELINE_ROOT / "bridge_state" / ".dt" / "dt.exe"
        if dest.exists():
            _DT_EXE = dest
            return _DT_EXE
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            import urllib.request
            logging.info("compute_grid: downloading dt.exe from %s", _DT_EXE_URL)
            urllib.request.urlretrieve(_DT_EXE_URL, dest)
            logging.info("compute_grid: dt.exe downloaded → %s", dest)
            _DT_EXE = dest
            return _DT_EXE
        except Exception as exc:
            logging.warning("compute_grid: dt.exe download failed: %s", exc)
            return None


def _free_port(base: int = _DT_FORWARD_BASE_PORT) -> int:
    """Find a free TCP port starting at base."""
    for candidate in range(base, base + 100):
        try:
            with socket.socket() as s:
                s.bind(("127.0.0.1", candidate))
                return candidate
        except OSError:
            continue
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _ensure_devtunnel_forward(tunnel_id: str) -> "tuple[str, int] | None":
    """Ensure a local TCP forward to *tunnel_id*:8000 is running via dt.exe.

    Returns (\"127.0.0.1\", local_port) on success, None if dt.exe is
    unavailable or the forward can't be established within the startup
    timeout.  Reuses an existing forward if the process is still alive and
    the port is open.
    """
    with _DT_FORWARD_LOCK:
        entry = _DT_FORWARDS.get(tunnel_id)
        if entry:
            local_port, proc = entry
            alive = proc is not None and proc.poll() is None
            if alive and _port_in_use("127.0.0.1", local_port, timeout=0.3):
                return ("127.0.0.1", local_port)
            # Stale — clean up and restart
            if proc is not None:
                try:
                    proc.terminate()
                except Exception:
                    pass
            _DT_FORWARDS.pop(tunnel_id, None)

        dt = _ensure_dt_exe()
        if dt is None:
            logging.warning(
                "compute_grid: dt.exe unavailable — cannot forward %s", tunnel_id)
            return None

        local_port = _free_port()
        cmd = [str(dt), "connect", f"{tunnel_id}:8000",
               "--local-port", str(local_port)]
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=(subprocess.CREATE_NO_WINDOW
                               if hasattr(subprocess, "CREATE_NO_WINDOW") else 0),
            )
        except Exception as exc:
            logging.warning(
                "compute_grid: failed to start dt connect for %s: %s", tunnel_id, exc)
            return None

        # Wait for the local port to open (dt connect takes a moment)
        deadline = time.time() + _DT_FORWARD_START_TIMEOUT
        while time.time() < deadline:
            if _port_in_use("127.0.0.1", local_port, timeout=0.3):
                _DT_FORWARDS[tunnel_id] = (local_port, proc)
                logging.info(
                    "compute_grid: devtunnel forward %s → 127.0.0.1:%d",
                    tunnel_id, local_port)
                return ("127.0.0.1", local_port)
            if proc.poll() is not None:
                logging.warning(
                    "compute_grid: dt connect exited early for %s "
                    "(exit %s) — not logged in?",
                    tunnel_id, proc.returncode)
                return None
            time.sleep(0.25)

        # Timed out — kill the orphaned process
        try:
            proc.terminate()
        except Exception:
            pass
        logging.warning(
            "compute_grid: devtunnel forward %s did not open "
            "within %.1fs", tunnel_id, _DT_FORWARD_START_TIMEOUT)
        return None


# ---------------------------------------------------------------------------
# Local execution shim — used both for the local fallback and (on the
# remote side) by the compute_node daemon when it processes an incoming job.
# Currently delegates straight to the offline ensemble caller; production
# code will replace this with the real model invocation routine.
# ---------------------------------------------------------------------------
def _execute_locally(payload: dict) -> dict:
    task = payload.get("task") or "default"
    # ── Brain-side compute tasks ──────────────────────────────────────────
    # When "The Other" receives one of these, it runs the corresponding
    # Brain primitive against ITS local copy of the corpus and returns the
    # result.  The originator commits / consumes the payload — keeping
    # writes single-host while distributing the CPU.
    if task == "self_expansion_compute":
        try:
            from .self_expansion import _compute_inferred_payload
            inner = _compute_inferred_payload()
        except Exception as e:
            inner = {"error": f"self_expansion_compute failed: {e}"}
        return {
            "host":     socket.gethostname(),
            "task":     task,
            "response": inner,
            "executed": "local",
        }
    if task == "self_expansion_infer_slice":
        try:
            from .self_expansion import _infer_from_slice
            inner = _infer_from_slice(payload.get("slice") or {})
        except Exception as e:
            inner = {"error": f"self_expansion_infer_slice failed: {e}"}
        return {
            "host":     socket.gethostname(),
            "task":     task,
            "response": inner,
            "executed": "local",
        }
    if task == "self_expansion_edge_commit":
        # Failsafe dispersal: the originating host ships its freshly committed
        # edges so this peer can persist them.  The peer is now a warm backup:
        # if the originator goes down the corpus is not lost.  Single-writer
        # per machine is preserved — each node writes to its own DB.
        committed = 0
        errors = 0
        try:
            import sqlite3 as _sql
            from datetime import datetime, timezone as _tz
            from .local_store import db_path as _db_path
            edge_list = payload.get("edges") or []
            cn = _sql.connect(str(_db_path()), timeout=20, check_same_thread=False)
            now = datetime.now(_tz.utc).isoformat()
            for e in edge_list:
                try:
                    cn.execute(
                        """
                        INSERT INTO corpus_edge
                            (src_id, src_type, dst_id, dst_type, rel,
                             weight, last_seen, samples)
                        VALUES (?,?,?,?,?,?,?,1)
                        ON CONFLICT(src_id, src_type, dst_id, dst_type, rel) DO UPDATE SET
                            weight    = (weight * samples + excluded.weight) / (samples + 1),
                            samples   = samples + 1,
                            last_seen = excluded.last_seen
                        """,
                        (
                            str(e.get("src_id",  "")),
                            str(e.get("src_type","unknown")),
                            str(e.get("dst_id",  "")),
                            str(e.get("dst_type","unknown")),
                            str(e.get("rel",     "INFORMS")),
                            float(e.get("confidence", 0.0)),
                            now,
                        ),
                    )
                    committed += 1
                except Exception:
                    errors += 1
            cn.commit()
            cn.close()
        except Exception as ex:
            errors += 1
            import logging as _log
            _log.debug("compute_grid: edge_commit error: %s", ex)
        inner = {"committed": committed, "errors": errors,
                 "schema": "self_expansion.edge_commit_result/v1"}
        return {
            "host":     socket.gethostname(),
            "task":     task,
            "response": inner,
            "executed": "local",
        }
    # ── Default: LLM ensemble dispatch (existing behaviour) ───────────────
    from .llm_router import select_llm
    from .llm_ensemble import _offline_caller   # safe internal use
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
