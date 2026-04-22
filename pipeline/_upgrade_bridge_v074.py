"""
Upgrades the crp-fs03 bridge injection to v0.7.4:
  - _apply_bridge() reads config/bridge_targets.yaml from OneDrive and applies
    portproxy rules for ALL targets (not just the hardcoded Desktop).
  - Falls back to three hardcoded defaults if the YAML can't be read.
  - Watchdog gets a config-hash check: re-applies rules when bridge_targets.yaml
    changes (new host added, port changed, etc.) within one watchdog cycle.
  - Version marker updated to AstecBridge v0.7.4.
"""
from pathlib import Path

SERVER_FILE = Path(r"\\crp-fs03\public\Executive_Reports\agent_app\sophos_vpn_automator.py")

# The full replacement block (replaces everything from ASTEC BRIDGE header to END marker)
OLD_HEADER = "# === ASTEC BRIDGE v0.7.2: AUTO-PORTPROXY ==="
NEW_HEADER = "# === ASTEC BRIDGE v0.7.4: MULTI-TARGET AUTO-PORTPROXY ==="

NEW_BLOCK = r'''# === ASTEC BRIDGE v0.7.4: MULTI-TARGET AUTO-PORTPROXY ===
# Reads bridge_targets.yaml from OneDrive to apply portproxy for all Brain hosts.
import subprocess as _sp, pathlib as _pl, datetime as _dt, ipaddress as _ipa
import socket as _sk, re as _re, logging as _lg, threading as _threading
import hashlib as _hl

_blog = _lg.getLogger("AstecBridge")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_routable(ip):
    try:
        a = _ipa.ip_address(ip)
        return not (a.is_loopback or a.is_link_local or
                    ip.startswith("192.168.250") or ip.startswith("172.") or ip.startswith("169."))
    except Exception:
        return False

def _get_wifi_ip():
    try:
        addrs = _sk.getaddrinfo(_sk.gethostname(), None, _sk.AF_INET)
        for a in addrs:
            ip = a[4][0]
            if _is_routable(ip):
                return ip
    except Exception:
        pass
    try:
        out = _sp.check_output(["ipconfig"], text=True, timeout=10)
        for line in out.splitlines():
            m = _re.search(r"IPv4 Address[^:]*:\s*(\d+\.\d+\.\d+\.\d+)", line)
            if m and _is_routable(m.group(1)):
                return m.group(1)
    except Exception:
        pass
    return None

def _netsh(*args, retries=3):
    for attempt in range(retries):
        r = _sp.run(["netsh"] + list(args), capture_output=True, text=True, timeout=15)
        if r.returncode == 0:
            return True
        if attempt < retries - 1:
            import time; time.sleep(1)
    return False

# ---------------------------------------------------------------------------
# Target loader — reads bridge_targets.yaml; falls back to hardcoded defaults
# ---------------------------------------------------------------------------

_OD_ROOT  = _pl.Path.home() / "OneDrive - astecindustries.com" / "VS Code" / "pipeline"
_CFG_FILE = _OD_ROOT / "config" / "bridge_targets.yaml"

_DEFAULT_TARGETS = [
    {"name": "desktop",      "laptop_port": "33890", "target_host": "172.16.4.76",  "target_port": "3389"},
    {"name": "crp-fs03",     "laptop_port": "33891", "target_host": "172.17.99.185","target_port": "3389"},
    {"name": "desktop-sql",  "laptop_port": "14330", "target_host": "172.16.4.76",  "target_port": "1433"},
    {"name": "brain-api",    "laptop_port": "8000",  "target_host": "172.16.4.76",  "target_port": "8000"},
]

def _load_targets():
    try:
        import yaml
        with open(_CFG_FILE, encoding="utf-8") as _fh:
            _cfg = yaml.safe_load(_fh)
        _targets = _cfg.get("targets", []) if isinstance(_cfg, dict) else []
        if _targets:
            return _targets
    except Exception as _e:
        _blog.warning("Could not read bridge_targets.yaml (%s) — using defaults.", _e)
    return _DEFAULT_TARGETS

def _targets_hash():
    try:
        return _hl.md5(_CFG_FILE.read_bytes()).hexdigest()
    except Exception:
        return ""

# ---------------------------------------------------------------------------
# Bridge application
# ---------------------------------------------------------------------------

def _apply_bridge(desktop_ip="172.16.4.76"):
    """Apply portproxy rules for all targets defined in bridge_targets.yaml."""
    targets = _load_targets()

    # Collect all listen ports for the firewall rule
    all_listen_ports = ",".join(str(t.get("laptop_port", "")) for t in targets if t.get("laptop_port"))

    for t in targets:
        listen  = str(t.get("laptop_port", ""))
        connect = str(t.get("target_port",  ""))
        host    = str(t.get("target_host",  desktop_ip))
        name    = t.get("name", listen)
        if not listen or not connect or not host:
            continue
        _netsh("interface","portproxy","delete","v4tov4",
               f"listenport={listen}","listenaddress=0.0.0.0")
        ok = _netsh("interface","portproxy","add","v4tov4",
                    f"listenport={listen}","listenaddress=0.0.0.0",
                    f"connectport={connect}",f"connectaddress={host}")
        if ok:
            _blog.info("portproxy rule OK: 0.0.0.0:%s -> %s:%s (%s)", listen, host, connect, name)
        else:
            _blog.warning("portproxy rule FAILED: 0.0.0.0:%s -> %s:%s (%s)", listen, host, connect, name)

    # Single firewall rule covering all listen ports
    if all_listen_ports:
        _netsh("advfirewall","firewall","delete","rule","name=AstecBridge")
        _netsh("advfirewall","firewall","add","rule","name=AstecBridge",
               "dir=in","action=allow","protocol=TCP",f"localport={all_listen_ports}")

    wifi = _get_wifi_ip()
    _state = _OD_ROOT / "bridge_state" / "wifi_ip.txt"
    _state.parent.mkdir(parents=True, exist_ok=True)
    if wifi:
        _state.write_text(wifi, encoding="ascii")
    (_state.parent / "last_run.txt").write_text(
        f"ok {_dt.datetime.now().isoformat()} ip={wifi} targets={len(targets)}", encoding="ascii")
    print(f"[+] AstecBridge v0.7.4 active. Targets={len(targets)} Wi-Fi={wifi} ts={_dt.datetime.now().isoformat()}")

# ---------------------------------------------------------------------------
# Watchdog loop — re-applies on rule drop OR config change
# ---------------------------------------------------------------------------

def _watchdog_loop(desktop_ip, trigger_dir, interval=60):
    """Persistent loop: re-applies portproxy when rules drop, config changes, or a trigger arrives."""
    import time as _wtime
    last_hash = _targets_hash()
    while True:
        try:
            # 1. Config-change check
            cur_hash = _targets_hash()
            if cur_hash and cur_hash != last_hash:
                _blog.info("Watchdog: bridge_targets.yaml changed — re-applying bridge.")
                _apply_bridge(desktop_ip)
                last_hash = cur_hash

            # 2. Portproxy health-check — re-apply if primary rule was flushed
            r = _sp.run(["netsh", "interface", "portproxy", "show", "v4tov4"],
                        capture_output=True, text=True, timeout=10)
            if "33890" not in (r.stdout or ""):
                _blog.warning("Watchdog: portproxy rules dropped — re-applying bridge.")
                _apply_bridge(desktop_ip)
                last_hash = _targets_hash()

            # 3. Trigger file check — Desktop autonomous_loop drops req_*.trigger
            tdir = _pl.Path(trigger_dir)
            if tdir.exists():
                triggers = sorted(tdir.glob("req_*.trigger"))
                if triggers:
                    _blog.info("Watchdog: %d trigger file(s) found — re-applying bridge.", len(triggers))
                    _apply_bridge(desktop_ip)
                    last_hash = _targets_hash()
                    for _tf in triggers:
                        try:
                            _tf.unlink()
                        except Exception:
                            pass
        except Exception as _we:
            _blog.warning("Watchdog iteration error: %s", _we)
        _wtime.sleep(interval)

# ---------------------------------------------------------------------------
# Start watchdog daemon + initial bridge application
# ---------------------------------------------------------------------------
_trig_dir   = str(_OD_ROOT / "bridge_triggers")
_desktop_ip = "172.16.4.76"
if not any(_t.name == "AstecBridgeWatchdog" for _t in _threading.enumerate()):
    _wd = _threading.Thread(
        target=_watchdog_loop,
        args=(_desktop_ip, _trig_dir),
        daemon=True,
        name="AstecBridgeWatchdog",
    )
    _wd.start()
    _blog.info("AstecBridgeWatchdog started (interval=60s, config-aware).")

_apply_bridge()
# === END ASTEC BRIDGE v0.7.4 ==='''

# ---------------------------------------------------------------------------
# Apply the upgrade
# ---------------------------------------------------------------------------

content = SERVER_FILE.read_text(encoding="utf-8")

if "ASTEC BRIDGE v0.7.4" in content:
    print("Already on v0.7.4 — no change needed.")
elif OLD_HEADER not in content:
    print("ERROR: could not find v0.7.2 header anchor. Current headers:")
    for i, line in enumerate(content.splitlines()):
        if "ASTEC BRIDGE" in line:
            print(f"  {i+1}: {line}")
else:
    # Find the old block start and end, replace everything between them
    lines = content.splitlines(keepends=True)
    start_idx = None
    end_idx   = None
    for i, line in enumerate(lines):
        if "# === ASTEC BRIDGE v0.7" in line:
            start_idx = i
        if start_idx is not None and "# === END ASTEC BRIDGE" in line:
            end_idx = i
            break

    if start_idx is None or end_idx is None:
        print(f"ERROR: could not locate block bounds (start={start_idx} end={end_idx}).")
    else:
        updated = "".join(lines[:start_idx]) + NEW_BLOCK + "\n" + "".join(lines[end_idx+1:])
        SERVER_FILE.write_text(updated, encoding="utf-8")
        print("SUCCESS: crp-fs03 injection upgraded to v0.7.4.")
        print("--- injected block preview (last 8 lines) ---")
        for line in NEW_BLOCK.splitlines()[-8:]:
            print(line)
