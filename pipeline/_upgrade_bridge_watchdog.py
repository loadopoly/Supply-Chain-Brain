"""
Upgrades the crp-fs03 bridge injection to v0.7.3:
  - Adds _watchdog_loop: persistent 60s loop that re-applies portproxy if rules
    drop and consumes trigger files written by the Desktop's autonomous_loop.
  - The one-shot _apply_bridge() call remains; watchdog picks up from there.
"""
from pathlib import Path

SERVER_FILE = Path(r"\\crp-fs03\public\Executive_Reports\agent_app\sophos_vpn_automator.py")

# ── old tail (the one-shot call + end marker) ──────────────────────────────
OLD = """_apply_bridge()
# === END ASTEC BRIDGE ==="""

# ── new tail: watchdog definition + guarded start + call + end marker ──────
NEW = """import threading as _threading

def _watchdog_loop(desktop_ip, trigger_dir, interval=60):
    \"\"\"Persistent loop: re-applies portproxy when rules drop or a trigger file arrives.\"\"\"
    import time as _wtime
    while True:
        try:
            # 1. Portproxy health-check — re-apply if rules were flushed
            r = _sp.run(["netsh", "interface", "portproxy", "show", "v4tov4"],
                        capture_output=True, text=True, timeout=10)
            if "33890" not in (r.stdout or ""):
                _blog.warning("Watchdog: portproxy rules dropped — re-applying bridge.")
                _apply_bridge(desktop_ip)

            # 2. Trigger file check — Desktop autonomous_loop drops req_*.trigger
            tdir = _pl.Path(trigger_dir)
            if tdir.exists():
                triggers = sorted(tdir.glob("req_*.trigger"))
                if triggers:
                    _blog.info("Watchdog: %d trigger file(s) found — re-applying bridge.", len(triggers))
                    _apply_bridge(desktop_ip)
                    for _tf in triggers:
                        try:
                            _tf.unlink()
                        except Exception:
                            pass
        except Exception as _we:
            _blog.warning("Watchdog iteration error: %s", _we)
        _wtime.sleep(interval)

# Start watchdog daemon — guard prevents duplicate threads on re-import
_od_root     = _pl.Path.home() / "OneDrive - astecindustries.com" / "VS Code" / "pipeline"
_trig_dir    = str(_od_root / "bridge_triggers")
_desktop_ip  = "172.16.4.76"
if not any(_t.name == "AstecBridgeWatchdog" for _t in _threading.enumerate()):
    _wd = _threading.Thread(
        target=_watchdog_loop,
        args=(_desktop_ip, _trig_dir),
        daemon=True,
        name="AstecBridgeWatchdog",
    )
    _wd.start()
    _blog.info("AstecBridgeWatchdog started (interval=60s).")

_apply_bridge()
# === END ASTEC BRIDGE v0.7.3 ==="""

content = SERVER_FILE.read_text(encoding="utf-8")

if "AstecBridgeWatchdog" in content:
    print("Already on v0.7.3 — watchdog already present. No change needed.")
elif OLD not in content:
    print("ERROR: could not find replacement anchor in server file.")
    # Show tail for diagnosis
    for line in content.splitlines()[-10:]:
        print(repr(line))
else:
    updated = content.replace(OLD, NEW)
    SERVER_FILE.write_text(updated, encoding="utf-8")
    tail = updated.splitlines()[-15:]
    print("SUCCESS: watchdog loop written to crp-fs03.")
    print("--- last 15 lines ---")
    for line in tail:
        print(line)
