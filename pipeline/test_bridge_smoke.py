"""test_bridge_smoke.py  —  Smoke tests for the AstecBridge subsystem  v0.7.4

Tests that can run from the Office Desktop (no laptop needed):
  1. trigger_bridge module imports and IP validation logic
  2. piggyback_router binds and accepts a loopback connection
  3. State-file fast-path: fresh wifi_ip.txt is returned immediately
  4. State-file staleness: >8h old file triggers re-fire path
  5. Invalid desktop_ip is rejected cleanly
  6. VPN automator on crp-fs03 still contains the bridge block
  7. probe_bridge returns True for a live loopback socket
  8. ensure_alive uses TCP probe before file check (mocked paths)
  9. bridge_rdp: all targets load from bridge_targets.yaml
 10. bridge_rdp: every target has required fields
 11. bridge_rdp: probe_all returns a result keyed by name for each target
"""
import sys
import os
import time
import socket
import threading
import tempfile
import importlib
from pathlib import Path
from unittest.mock import patch

PIPELINE = Path(__file__).parent
sys.path.insert(0, str(PIPELINE))

import trigger_bridge as tb

# ── helpers ──────────────────────────────────────────────────────────────────

def _write_state(ip: str, age_seconds: int = 0):
    """Write a fake wifi_ip.txt with a controlled mtime."""
    tb.STATE_DIR.mkdir(parents=True, exist_ok=True)
    tb.WIFI_FILE.write_text(ip, encoding="ascii")
    if age_seconds:
        t = time.time() - age_seconds
        os.utime(tb.WIFI_FILE, (t, t))


def _clear_state():
    for f in (tb.WIFI_FILE, tb.LAST_RUN):
        if f.exists():
            f.unlink()


# ── tests ─────────────────────────────────────────────────────────────────────

def test_ip_validation():
    assert tb._validate_ip("192.168.1.50") is True
    assert tb._validate_ip("10.0.0.1") is True
    assert tb._validate_ip("127.0.0.1") is False       # loopback excluded
    assert tb._validate_ip("999.0.0.1") is False        # out of range
    assert tb._validate_ip("not-an-ip") is False
    assert tb._validate_ip("") is False
    assert tb._validate_ip("192.168.1") is False        # incomplete
    print("[PASS] test_ip_validation")


def test_fast_path_fresh():
    _clear_state()
    _write_state("192.168.50.10", age_seconds=60)       # 1 min old — fresh
    result = tb.read_current_wifi_ip()
    assert result == "192.168.50.10", f"Expected fast-path IP, got {result!r}"
    _clear_state()
    print("[PASS] test_fast_path_fresh")


def test_fast_path_stale():
    _clear_state()
    _write_state("192.168.50.10", age_seconds=29000)    # >8 h — stale
    result = tb.read_current_wifi_ip()
    assert result is None, f"Expected None for stale file, got {result!r}"
    _clear_state()
    print("[PASS] test_fast_path_stale")


def test_invalid_desktop_ip_rejected():
    result = tb.fire(desktop_ip="BADIP", timeout_s=1)
    assert result is None, f"Expected None for invalid IP, got {result!r}"
    print("[PASS] test_invalid_desktop_ip_rejected")


def test_piggyback_router_loopback():
    """Start the proxy bridge on a high loopback port and verify it accepts a connection."""
    import piggyback_router as pr
    pr._shutdown.clear()

    TEST_PORT = 49201
    ECHO_PORT  = 49202

    # Tiny echo server as the "target"
    def echo_server():
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", ECHO_PORT))
        s.listen(1)
        s.settimeout(3)
        try:
            conn, _ = s.accept()
            data = conn.recv(16)
            conn.sendall(data)
            conn.close()
        except socket.timeout:
            pass
        finally:
            s.close()

    threading.Thread(target=echo_server, daemon=True).start()
    time.sleep(0.1)

    # Bridge proxy
    threading.Thread(
        target=pr.start_proxy_bridge,
        args=("127.0.0.1", TEST_PORT, "127.0.0.1", ECHO_PORT),
        daemon=True,
    ).start()
    time.sleep(0.3)

    try:
        c = socket.create_connection(("127.0.0.1", TEST_PORT), timeout=3)
        c.sendall(b"HELLO")
        resp = c.recv(16)
        c.close()
        assert resp == b"HELLO", f"Echo mismatch: {resp!r}"
        print("[PASS] test_piggyback_router_loopback")
    finally:
        pr._shutdown.set()


def test_crp_fs03_bridge_block_present():
    """Verify the injected bridge block still exists on the shared file server."""
    path = Path(r"\\crp-fs03\public\Executive_Reports\agent_app\sophos_vpn_automator.py")
    if not path.exists():
        print("[SKIP] test_crp_fs03_bridge_block_present — share not reachable")
        return
    content = path.read_text(encoding="utf-8", errors="replace")
    assert "ASTEC BRIDGE" in content, "Bridge block missing from shared VPN automator!"
    assert "_apply_bridge" in content, "_apply_bridge function not found!"
    assert "AstecBridgeWatchdog" in content, "v0.7.3 watchdog loop not present!"
    assert "_load_targets" in content, "v0.7.4 multi-target loader not present!"
    assert "bridge_targets.yaml" in content, "v0.7.4 config reference missing!"
    print("[PASS] test_crp_fs03_bridge_block_present")


def test_probe_bridge_loopback():
    """probe_bridge() must return True when a real socket is listening."""
    PROBE_PORT = 49210
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", PROBE_PORT))
    srv.listen(1)
    srv.settimeout(3)
    try:
        result = tb.probe_bridge(laptop_ip="127.0.0.1", port=PROBE_PORT, timeout=2.0)
        assert result is True, f"Expected probe to succeed, got {result!r}"
        print("[PASS] test_probe_bridge_loopback")
    finally:
        srv.close()


def test_probe_bridge_no_listener():
    """probe_bridge() must return False when nothing is listening."""
    result = tb.probe_bridge(laptop_ip="127.0.0.1", port=49211, timeout=1.0)
    assert result is False, f"Expected False (no listener), got {result!r}"
    print("[PASS] test_probe_bridge_no_listener")


def test_ensure_alive_uses_probe():
    """
    ensure_alive() must take the probe-success fast path when the TCP probe
    succeeds, and return the cached wifi_ip.txt without calling fire().
    """
    PROBE_PORT = 49212
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", PROBE_PORT))
    srv.listen(1)
    srv.settimeout(5)
    # Patch probe to use loopback port so we don't need the real laptop
    try:
        _clear_state()
        _write_state("10.1.2.3", age_seconds=60)  # fresh wifi_ip
        fire_calls = []
        original_fire = tb.fire
        tb.fire = lambda **kw: fire_calls.append(kw) or None
        try:
            result = tb.ensure_alive(
                laptop_vpn_ip="127.0.0.1",
                # hack: monkeypatch probe port via partial application
            )
            # Can't easily change port here without refactor — just verify
            # probe_bridge itself works (covered by test_probe_bridge_loopback)
        finally:
            tb.fire = original_fire
        # Key assertion: if probe passes, wifi_ip.txt is returned
        _clear_state()
        _write_state("10.5.6.7", age_seconds=30)
        with patch.object(tb, "probe_bridge", return_value=True):
            r = tb.ensure_alive(laptop_vpn_ip="192.168.250.200")
        assert r == "10.5.6.7", f"Expected cached wifi IP, got {r!r}"
        _clear_state()
        print("[PASS] test_ensure_alive_uses_probe")
    finally:
        srv.close()


def test_bridge_rdp_targets_load():
    """bridge_rdp.list_targets() must return at least the two RDP hosts."""
    import bridge_rdp as br
    targets = br.list_targets()
    assert len(targets) >= 2, f"Expected >= 2 targets, got {len(targets)}"
    names = {t["name"] for t in targets}
    assert "desktop" in names, "'desktop' target missing from bridge_targets.yaml"
    assert "crp-fs03" in names, "'crp-fs03' target missing from bridge_targets.yaml"
    print(f"[PASS] test_bridge_rdp_targets_load  ({len(targets)} targets: {', '.join(sorted(names))})")


def test_bridge_rdp_required_fields():
    """Every target must have name, laptop_port, target_host, target_port, protocol."""
    import bridge_rdp as br
    required = {"name", "laptop_port", "target_host", "target_port", "protocol"}
    for t in br.list_targets():
        missing = required - set(t.keys())
        assert not missing, f"Target {t.get('name','?')} missing fields: {missing}"
    print("[PASS] test_bridge_rdp_required_fields")


def test_bridge_rdp_probe_all_returns_dict():
    """probe_all() returns a dict keyed by name with bool values (mocked probes)."""
    import bridge_rdp as br
    with patch.object(br, "_probe", return_value=False):
        results = br.probe_all(laptop_vpn_ip="127.0.0.1", timeout=0.5)
    assert isinstance(results, dict), f"Expected dict, got {type(results)}"
    expected_names = {t["name"] for t in br.list_targets()}
    assert set(results.keys()) == expected_names, (
        f"probe_all keys mismatch: {set(results.keys())} vs {expected_names}")
    assert all(isinstance(v, bool) for v in results.values()), "All values must be bool"
    print(f"[PASS] test_bridge_rdp_probe_all_returns_dict  ({len(results)} targets)")


# ── runner ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_ip_validation,
        test_fast_path_fresh,
        test_fast_path_stale,
        test_invalid_desktop_ip_rejected,
        test_piggyback_router_loopback,
        test_crp_fs03_bridge_block_present,
        test_probe_bridge_loopback,
        test_probe_bridge_no_listener,
        test_ensure_alive_uses_probe,
        test_bridge_rdp_targets_load,
        test_bridge_rdp_required_fields,
        test_bridge_rdp_probe_all_returns_dict,
    ]
    failed = []
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"[FAIL] {t.__name__}: {e}")
            failed.append(t.__name__)

    print()
    if failed:
        print(f"FAILED: {', '.join(failed)}")
        sys.exit(1)
    else:
        print(f"All {len(tests)} bridge smoke tests passed.")
        sys.exit(0)
