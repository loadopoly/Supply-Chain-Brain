"""
trigger_bridge.py — Agent-callable bridge activator.  v0.7.2

Primary pathway: reads the Wi-Fi IP written by sophos_vpn_automator.py
(injected bridge block) which runs on the laptop each VPN session.

Secondary pathway (legacy watcher): drops a trigger file into the
OneDrive-synced folder, waits for a bridge_watcher response.

Returns the laptop Wi-Fi IP so the Gaming PC knows where to connect.
"""
import os
import re
import time
import sys
import socket
import logging
from pathlib import Path

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="[bridge] %(message)s")

PIPELINE = Path(__file__).parent
TRIGGER_DIR = PIPELINE / "bridge_triggers"
STATE_DIR   = PIPELINE / "bridge_state"
WIFI_FILE   = STATE_DIR / "wifi_ip.txt"
LAST_RUN    = STATE_DIR / "last_run.txt"
DESKTOP_IP_DEFAULT = "172.16.4.76"
LAPTOP_VPN_IP      = "192.168.250.200"  # Fixed VPN-assigned IP for laptop

_IPV4_RE = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")


def _validate_ip(ip: str) -> bool:
    """Reject obviously wrong IPs before trusting them."""
    if not ip or not _IPV4_RE.match(ip):
        return False
    parts = [int(x) for x in ip.split(".")]
    return all(0 <= p <= 255 for p in parts) and not ip.startswith("127.")


def read_current_wifi_ip() -> str | None:
    """Return the IP written by the last successful VPN automator run, if fresh (<8 h)."""
    if not WIFI_FILE.exists():
        return None
    age = time.time() - WIFI_FILE.stat().st_mtime
    if age > 28800:   # 8 hours — stale after full work day
        log.warning("Cached Wi-Fi IP is stale (%.0f h). Will re-trigger.", age / 3600)
        return None
    ip = WIFI_FILE.read_text(encoding="ascii").strip()
    return ip if _validate_ip(ip) else None


def fire(desktop_ip: str = DESKTOP_IP_DEFAULT, timeout_s: int = 60) -> str | None:
    # Validate caller-supplied desktop IP
    if not _validate_ip(desktop_ip):
        log.error("Invalid desktop_ip supplied: %r — aborting.", desktop_ip)
        return None

    # Fast path: VPN automator already wrote a fresh IP
    cached = read_current_wifi_ip()
    if cached:
        log.info("Bridge already active. Laptop Wi-Fi IP: %s", cached)
        log.info("Gaming PC -> %s:33890 (RDP)  %s:14330 (SQL)  %s:8000 (API)", cached, cached, cached)
        return cached

    # Slow path: drop trigger for legacy watcher
    TRIGGER_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    prev_run = LAST_RUN.read_text(encoding="ascii").strip() if LAST_RUN.exists() else ""
    trigger  = TRIGGER_DIR / f"req_{int(time.time())}.trigger"
    trigger.write_text(desktop_ip, encoding="ascii")
    log.info("Trigger dropped: %s (desktop=%s)", trigger.name, desktop_ip)

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if LAST_RUN.exists() and LAST_RUN.read_text(encoding="ascii").strip() != prev_run:
            ip = WIFI_FILE.read_text(encoding="ascii").strip() if WIFI_FILE.exists() else None
            if ip and _validate_ip(ip):
                log.info("Bridge confirmed. Laptop Wi-Fi IP: %s", ip)
                log.info("Gaming PC -> %s:33890 (RDP)  %s:14330 (SQL)  %s:8000 (API)", ip, ip, ip)
                return ip
            log.warning("Watcher responded but IP is invalid: %r", ip)
            return None
        time.sleep(2)

    log.warning("Timed out after %ds. Laptop watcher may not be installed.", timeout_s)
    return None


def probe_bridge(laptop_ip: str = LAPTOP_VPN_IP, port: int = 33890,
                 timeout: float = 3.0) -> bool:
    """
    TCP handshake probe — returns True if the laptop's portproxy is
    accepting connections on the given port (bridge is alive).
    A bare connect+close is harmless: portproxy sees a short-lived
    peer without RDP data and tears down cleanly.

    Uses only format validation (no loopback exclusion) so the function
    can be exercised in unit tests against 127.0.0.1.
    """
    if not laptop_ip or not _IPV4_RE.match(laptop_ip):
        return False
    try:
        s = socket.create_connection((laptop_ip, port), timeout=timeout)
        s.close()
        return True
    except OSError:
        return False


def ensure_alive(
    desktop_ip: str = DESKTOP_IP_DEFAULT,
    laptop_vpn_ip: str = LAPTOP_VPN_IP,
    timeout_s: int = 90,
) -> str | None:
    """
    Guarantee the bridge is up and return a reachable laptop endpoint.

    Flow:
      1. TCP probe the laptop's fixed VPN IP — if alive, return cached
         wifi_ip.txt (Gaming PC path) or VPN IP as fallback.
      2. If dead, drop a req_*.trigger file and let the on-laptop
         AstecBridgeWatchdog respond (started by v0.7.3 injection).
         Polls until last_run.txt changes or timeout.
    """
    if not _validate_ip(laptop_vpn_ip):
        log.error("Invalid laptop_vpn_ip: %r", laptop_vpn_ip)
        return None

    # --- Step 1: Fast TCP probe (Desktop → laptop VPN IP) ---
    if probe_bridge(laptop_vpn_ip):
        log.info("Bridge alive (TCP probe OK on %s:33890).", laptop_vpn_ip)
        cached = read_current_wifi_ip()
        if cached:
            return cached
        # Bridge is up but wifi_ip.txt is stale — watchdog will refresh it;
        # return the VPN IP as a usable endpoint in the meantime.
        log.info("wifi_ip.txt stale but bridge is live; using VPN IP %s.", laptop_vpn_ip)
        return laptop_vpn_ip

    # --- Step 2: Bridge not responding — trigger watchdog recovery ---
    log.info("TCP probe failed on %s:33890 — requesting bridge recovery.", laptop_vpn_ip)
    return fire(desktop_ip=desktop_ip, timeout_s=timeout_s)


if __name__ == "__main__":
    desktop = sys.argv[1] if len(sys.argv) > 1 else DESKTOP_IP_DEFAULT
    result = ensure_alive(desktop_ip=desktop)
    sys.exit(0 if result else 1)
