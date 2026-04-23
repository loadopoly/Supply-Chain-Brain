"""
bridge_rdp.py  -  Multi-target RDP/service launcher for the AstecBridge  v0.8.0

Reads config/bridge_targets.yaml to know which portproxy slots are mapped to
which internal hosts.  Provides:
  - list_targets()        : all configured targets
  - get_target(name)      : look up one target by name
  - probe_all()           : TCP-probe every target (auto-detects LAN vs bridge)
  - rdp_connect(name)     : launch RDP client to a named target
  - export_rdp_file(name) : write a .rdp file any RDP client can open
  - find_freerdp()        : locate wfreerdp.exe (FreeRDP alternative client)

Importable from the Brain pipeline or callable as a script:
  python bridge_rdp.py list
  python bridge_rdp.py probe
  python bridge_rdp.py connect desktop
  python bridge_rdp.py connect desktop --client freerdp
  python bridge_rdp.py connect desktop --client file
  python bridge_rdp.py export desktop [output_path]
  python bridge_rdp.py export all
  python bridge_rdp.py freerdp

RDP client options (set globally in bridge_targets.yaml as rdp_client, or per-command):
  mstsc    - Windows built-in mstsc.exe  (default)
  freerdp  - open-source wfreerdp.exe    (winget install FreeRDP.FreeRDP)
  file     - export .rdp file and open with the system default RDP application
"""
import socket
import subprocess
import sys
import logging
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="[rdp] %(message)s")

PIPELINE      = Path(__file__).parent
CONFIG_FILE   = PIPELINE / "config" / "bridge_targets.yaml"
LAPTOP_VPN_IP = "192.168.250.200"   # Laptop's VPN-assigned IP (remote relay)
LAPTOP_LAN_IP = "172.16.4.75"       # Laptop's corporate LAN IP (ROADL-4GVKFW3)
_LAN_PROBE_HOST = "172.16.4.76"     # Desktop - detect if on corporate LAN / VPN
_LAN_PROBE_PORT = 3389

# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_targets() -> list[dict]:
    """Parse bridge_targets.yaml.  Returns list of target dicts."""
    try:
        import yaml  # PyYAML - already in requirements.txt
        with open(CONFIG_FILE, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
        return cfg.get("targets", []) if isinstance(cfg, dict) else []
    except Exception as e:
        log.error("Could not load bridge_targets.yaml: %s", e)
        return []


def list_targets() -> list[dict]:
    """Return all configured bridge targets."""
    return _load_targets()


def get_target(name: str) -> Optional[dict]:
    """Return the target dict for *name*, or None."""
    for t in _load_targets():
        if t.get("name") == name:
            return t
    return None


def _load_config() -> dict:
    """Return the full bridge_targets.yaml dict (includes global settings like rdp_client)."""
    try:
        import yaml
        with open(CONFIG_FILE, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
        return cfg if isinstance(cfg, dict) else {}
    except Exception as e:
        log.error("Could not load bridge_targets.yaml: %s", e)
        return {}


# Common FreeRDP install locations on Windows
_FREERDP_SEARCH = [
    Path(r"C:\Program Files\FreeRDP\wfreerdp.exe"),
    Path(r"C:\Program Files (x86)\FreeRDP\wfreerdp.exe"),
    Path(r"C:\Program Files\FreeRDP\bin\wfreerdp.exe"),
    Path(r"C:\ProgramData\chocolatey\bin\wfreerdp.exe"),
    Path(r"C:\tools\freerdp\wfreerdp.exe"),
]


def find_freerdp() -> Optional[Path]:
    """
    Locate wfreerdp.exe on this machine.

    Checks common install paths, then falls back to PATH.
    Install with:  winget install FreeRDP.FreeRDP
    """
    import shutil
    for p in _FREERDP_SEARCH:
        if p.exists():
            return p
    found = shutil.which("wfreerdp") or shutil.which("wfreerdp.exe")
    return Path(found) if found else None


def detect_location(timeout: float = 1.5) -> str:
    """
    Detect whether internal LAN hosts are directly reachable.

    Returns 'lan'    - on corporate LAN or VPN with direct route; use target_host:target_port.
    Returns 'bridge' - remote; must route through laptop portproxy (LAPTOP_VPN_IP:laptop_port).
    """
    try:
        s = socket.create_connection((_LAN_PROBE_HOST, _LAN_PROBE_PORT), timeout=timeout)
        s.close()
        return "lan"
    except OSError:
        return "bridge"


# ---------------------------------------------------------------------------
# TCP probe helpers
# ---------------------------------------------------------------------------

def _probe(host: str, port: int, timeout: float = 3.0) -> bool:
    """Raw TCP connect probe - True if port is open."""
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True
    except OSError:
        return False


def probe_all(laptop_vpn_ip: str = LAPTOP_VPN_IP, timeout: float = 3.0) -> dict[str, bool]:
    """
    TCP-probe every target.  Auto-detects LAN vs bridge mode.

    LAN mode   - probes target_host:target_port directly.
    Bridge mode - probes laptop_vpn_ip:laptop_port via portproxy, or bridge_endpoint if set.

    Returns dict  {target_name: bool}  where True = reachable.
    """
    mode = detect_location(timeout=min(timeout, 1.5))
    log.info("Location: %s mode", mode.upper())
    results: dict[str, bool | None] = {}
    for t in _load_targets():
        name = t.get("name", "?")
        proto = t.get("protocol", "rdp")
        if proto == "vscode_tunnel":
            results[name] = None  # not TCP-probeable
            log.info("  %-20s (vscode_tunnel - not TCP-probeable)", name)
            continue
        if mode == "lan":
            host = t.get("target_host")
            port = t.get("target_port")
        else:
            bridge_ep = t.get("bridge_endpoint")
            if bridge_ep:
                parts = bridge_ep.rsplit(":", 1)
                host, port = parts[0], int(parts[1])
            else:
                host = laptop_vpn_ip
                port = t.get("laptop_port")
        if not host or not port:
            results[name] = False
            continue
        alive = _probe(host, int(port), timeout)
        results[name] = alive
        status = "UP" if alive else "DOWN"
        log.info("  %-20s %s:%s  [%s]", name, host, port, status)
    return results


# ---------------------------------------------------------------------------
# RDP endpoint resolver
# ---------------------------------------------------------------------------

def _build_rdp_endpoint(name: str, laptop_vpn_ip: str = LAPTOP_VPN_IP) -> Optional[str]:
    """
    Resolve the correct host:port for *name* based on current network location.

    LAN mode   - returns target_host:target_port directly.
    Bridge mode - returns bridge_endpoint if set, else laptop_vpn_ip:laptop_port.
    Returns None if no reachable path exists for the current location.
    """
    t = get_target(name)
    if t is None:
        log.error("Unknown target %r", name)
        return None
    mode = detect_location()
    if mode == "lan":
        return f"{t['target_host']}:{t['target_port']}"
    bridge_ep = t.get("bridge_endpoint")
    if bridge_ep:
        return bridge_ep
    port = t.get("laptop_port")
    if port is not None:
        return f"{laptop_vpn_ip}:{port}"
    log.error("Target %r has no reachable path (no laptop_port or bridge_endpoint).", name)
    return None


# ---------------------------------------------------------------------------
# RDP launcher
# ---------------------------------------------------------------------------

def rdp_connect(
    name: str,
    laptop_vpn_ip: str = LAPTOP_VPN_IP,
    fullscreen: bool = False,
    width: int = 1920,
    height: int = 1080,
    client: Optional[str] = None,
) -> bool:
    """
    Launch an RDP session to the named target.

    Automatically selects LAN vs bridge routing based on current location.
    client: 'mstsc' (default) | 'freerdp' | 'file'
    If client is None, reads rdp_client from bridge_targets.yaml (falls back to 'mstsc').

    Example:
        rdp_connect("desktop")                    # mstsc, auto-route
        rdp_connect("desktop", client="freerdp")  # FreeRDP open-source client
        rdp_connect("laptop",  client="file")     # export .rdp and open it
    """
    t = get_target(name)
    if t is None:
        log.error("Unknown target %r - run list_targets() to see options.", name)
        return False

    proto = t.get("protocol", "rdp")
    if proto == "vscode_tunnel":
        log.error("Target %r is a VS Code tunnel target - use tunnel_connect(%r) instead.", name, name)
        return False
    if proto != "rdp":
        log.error("Target %r uses protocol %r - only 'rdp' targets support RDP launch.", name, proto)
        return False

    label    = t.get("label", name)
    endpoint = _build_rdp_endpoint(name, laptop_vpn_ip)
    if endpoint is None:
        return False

    # Resolve RDP client backend
    if client is None:
        cfg    = _load_config()
        client = cfg.get("rdp_client", "mstsc")
    client = client.lower().strip()
    log.info("Opening RDP to %s via %s  [client: %s]", label, endpoint, client)

    if client == "freerdp":
        return _launch_freerdp(endpoint, label, fullscreen, width, height)

    if client == "file":
        rdp_path = export_rdp_file(name, laptop_vpn_ip=laptop_vpn_ip,
                                   width=width, height=height, fullscreen=fullscreen)
        if rdp_path is None:
            return False
        try:
            subprocess.Popen(["cmd.exe", "/c", "start", "", str(rdp_path)])
            return True
        except Exception as e:
            log.error("Could not open .rdp file: %s", e)
            return False

    # Default: mstsc - launch via .rdp file so domain SSO settings apply
    rdp_path = export_rdp_file(name, laptop_vpn_ip=laptop_vpn_ip,
                               width=width, height=height, fullscreen=fullscreen)
    if rdp_path is None:
        return False
    try:
        subprocess.Popen(["mstsc.exe", str(rdp_path)])
        return True
    except FileNotFoundError:
        log.error("mstsc.exe not found - are you running on Windows?")
        return False
    except Exception as e:
        log.error("mstsc launch failed: %s", e)
        return False


def rdp_connect_ip(laptop_vpn_ip: str, port: int) -> bool:
    """Ad-hoc RDP to any laptop_port by VPN IP - useful for dynamic targets."""
    endpoint = f"{laptop_vpn_ip}:{port}"
    log.info("Ad-hoc RDP to %s", endpoint)
    try:
        subprocess.Popen(["mstsc.exe", f"/v:{endpoint}"])
        return True
    except Exception as e:
        log.error("mstsc launch failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# VS Code Remote Tunnel launcher
# ---------------------------------------------------------------------------

def tunnel_connect(name: str) -> bool:
    """
    Open VS Code connected to a vscode_tunnel target via VS Code Remote Tunnels.

    The target must have  protocol: vscode_tunnel  and  tunnel_name: <name>
    in bridge_targets.yaml.  The laptop must have  code tunnel  (or the
    'code-tunnel' service) running and registered with the same account.

    Equivalent to clicking  'Connect to Tunnel…'  in VS Code and selecting
    the machine, or navigating to  vscode.dev/tunnel/<tunnel_name>.

    Example:
        tunnel_connect("laptop-tunnel")
    """
    t = get_target(name)
    if t is None:
        log.error("Unknown target %r - run list_targets() to see options.", name)
        return False

    proto = t.get("protocol", "rdp")
    if proto != "vscode_tunnel":
        log.error(
            "Target %r has protocol %r - tunnel_connect() requires protocol: vscode_tunnel.",
            name, proto,
        )
        return False

    tunnel_name = t.get("tunnel_name")
    if not tunnel_name:
        log.error("Target %r has no tunnel_name configured in bridge_targets.yaml.", name)
        return False

    label = t.get("label", name)

    # Find VS Code executable
    import shutil as _shutil
    _vscode_candidates = [
        Path(r"C:\Users") / _os_env("USERNAME", "") / r"AppData\Local\Programs\Microsoft VS Code\Code.exe",
        Path(r"C:\Program Files\Microsoft VS Code\Code.exe"),
        Path(r"C:\Program Files (x86)\Microsoft VS Code\Code.exe"),
    ]
    vscode_exe: Optional[Path] = None
    for c in _vscode_candidates:
        if c.exists():
            vscode_exe = c
            break
    if vscode_exe is None:
        found = _shutil.which("code") or _shutil.which("code.cmd")
        if found:
            vscode_exe = Path(found)

    # Build the remote URI:  vscode-remote://tunnel+<name>/
    remote_uri = f"vscode-remote://tunnel+{tunnel_name}/"
    log.info("Opening VS Code tunnel to %s  [%s]", label, remote_uri)

    if vscode_exe is not None:
        try:
            subprocess.Popen([str(vscode_exe), "--folder-uri", remote_uri])
            return True
        except Exception as e:
            log.error("VS Code launch failed: %s", e)
            return False

    # Fallback: open in default browser (vscode.dev)
    import webbrowser as _wb
    fallback_url = f"https://vscode.dev/tunnel/{tunnel_name}"
    log.info("VS Code not found locally - opening in browser: %s", fallback_url)
    _wb.open(fallback_url)
    return True


def _os_env(key: str, default: str = "") -> str:
    """Helper - os.environ.get without importing os at module level."""
    import os as _os_inner
    return _os_inner.environ.get(key, default)


# ---------------------------------------------------------------------------
# Alternative client: FreeRDP
# ---------------------------------------------------------------------------

def _launch_freerdp(
    endpoint: str,
    label: str,
    fullscreen: bool = False,
    width: int = 1920,
    height: int = 1080,
) -> bool:
    """
    Launch wfreerdp.exe to the given host:port endpoint.
    Install FreeRDP with:  winget install FreeRDP.FreeRDP
    """
    exe = find_freerdp()
    if exe is None:
        log.error("wfreerdp.exe not found.  Install: winget install FreeRDP.FreeRDP")
        return False
    cmd = [str(exe), f"/v:{endpoint}", "+clipboard", "/cert:ignore", "/log-level:WARN"]
    if fullscreen:
        cmd.append("/f")
    else:
        cmd.extend([f"/w:{width}", f"/h:{height}"])
    log.info("FreeRDP: %s", " ".join(str(a) for a in cmd))
    try:
        subprocess.Popen(cmd)
        return True
    except Exception as e:
        log.error("FreeRDP launch failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# .rdp file export  (universal - works with any RDP client)
# ---------------------------------------------------------------------------

def export_rdp_file(
    name: str,
    output_path=None,
    laptop_vpn_ip: str = LAPTOP_VPN_IP,
    width: int = 1920,
    height: int = 1080,
    fullscreen: bool = False,
) -> Optional[Path]:
    """
    Write a standard .rdp file for *name* that any RDP client can open.

    output_path - directory or full .rdp file path.
                  Defaults to pipeline/rdp_files/<name>.rdp.
    Returns the Path of the written file, or None on error.

    Compatible with: mstsc, FreeRDP, mRemoteNG, Royal TS, Devolutions RDM,
                     Remmina, and any other standard .rdp client.
    """
    t = get_target(name)
    if t is None:
        log.error("Unknown target %r", name)
        return None
    if t.get("protocol", "rdp") != "rdp":
        log.error("Target %r is not an RDP target - export skipped.", name)
        return None

    endpoint = _build_rdp_endpoint(name, laptop_vpn_ip)
    if endpoint is None:
        return None

    # Resolve output directory / file
    out_dir  = PIPELINE / "rdp_files"
    rdp_path: Optional[Path] = None
    if output_path is not None:
        op = Path(output_path)
        if op.suffix.lower() == ".rdp":
            rdp_path = op
            out_dir  = op.parent
        else:
            out_dir = op
    out_dir.mkdir(parents=True, exist_ok=True)
    if rdp_path is None:
        rdp_path = out_dir / f"{name}.rdp"

    screen_mode = "2" if fullscreen else "1"
    # Domain SSO: pre-fill current Windows identity so mstsc uses Kerberos
    # silently - no credential dialog on domain-joined machines.
    import os as _os
    _user   = _os.environ.get("USERNAME", "")
    _domain = _os.environ.get("USERDOMAIN", "")
    _domain_user = f"{_domain}\\{_user}" if _domain and _user else _user
    lines = [
        f"full address:s:{endpoint}",
        f"screen mode id:i:{screen_mode}",
        f"desktopwidth:i:{width}",
        f"desktopheight:i:{height}",
        "session bpp:i:32",
        "compression:i:1",
        "keyboardhook:i:2",
        "displayconnectionbar:i:1",
        "disable wallpaper:i:0",
        "allow font smoothing:i:1",
        "allow desktop composition:i:1",
        "disable full window drag:i:0",
        "disable menu anims:i:0",
        "disable themes:i:0",
        "bitmapcachepersistenable:i:1",
        "audiomode:i:0",
        "redirectprinters:i:0",
        "redirectclipboard:i:1",
        "redirectsmartcards:i:0",
        "autoreconnection enabled:i:1",
        # NLA / CredSSP - pass current Kerberos ticket, no password prompt
        "enablecredsspsupport:i:1",
        "authentication level:i:0",   # 0 = always connect, never warn about cert
        "negotiate security layer:i:1",
        "prompt for credentials:i:0",
        "prompt for credentials on client:i:0",
        f"username:s:{_domain_user}",
        # Pin the server certificate by thumbprint - eliminates security warning
        # for self-signed RDP certs on domain machines.
        *([f"certificate thumbprint:s:{t['cert_thumbprint']}"] if t.get("cert_thumbprint") else []),
        "use multimon:i:0",
        "connection type:i:7",
        "networkautodetect:i:1",
        "bandwidthautodetect:i:1",
    ]
    rdp_path.write_text("\r\n".join(lines) + "\r\n", encoding="utf-8")
    log.info("Exported: %s  (endpoint: %s)", rdp_path, endpoint)
    return rdp_path


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------

def _print_targets():
    targets = list_targets()
    if not targets:
        print("No targets configured in bridge_targets.yaml.")
        return
    cfg    = _load_config()
    client = cfg.get("rdp_client", "mstsc")
    col    = "{:<20} {:<6} {:<18} {:<6} {:<8} {}"
    print(col.format("NAME", "PORT", "TARGET HOST", "TPORT", "PROTOCOL", "LABEL"))
    print("-" * 90)
    for t in targets:
        print(col.format(
            t.get("name", ""),
            t.get("laptop_port") if t.get("laptop_port") is not None else "-",
            t.get("target_host", ""),
            t.get("target_port", ""),
            t.get("protocol", ""),
            t.get("label", ""),
        ))
    print(f"\nDefault RDP client: {client}  (rdp_client in bridge_targets.yaml)")


def _print_probe():
    mode = detect_location(timeout=1.5)
    print(f"Location: {mode.upper()} mode\n")
    results = probe_all()
    for name, alive in results.items():
        t = get_target(name)
        label = t.get("label", name) if t else name
        if alive is None:
            mark = "N/A "
        elif alive:
            mark = "OK  "
        else:
            mark = "FAIL"
        print(f"  [{mark}]  {name:<20}  {label}")
    up    = sum(1 for v in results.values() if v is True)
    na    = sum(1 for v in results.values() if v is None)
    total = sum(1 for v in results.values() if v is not None)
    print(f"\n{up}/{total} targets up" + (f"  ({na} N/A)" if na else "") + ".")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        prog="bridge_rdp.py",
        description="AstecBridge RDP launcher - auto-routes LAN vs bridge",
    )
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("list",  help="List all configured targets")
    sub.add_parser("probe", help="TCP-probe all targets")

    p_connect = sub.add_parser("connect", help="Open an RDP session to a named target")
    p_connect.add_argument("name", help="Target name (see: bridge_rdp.py list)")
    p_connect.add_argument(
        "--client",
        choices=["mstsc", "freerdp", "file"],
        default=None,
        help="RDP client (default: rdp_client in bridge_targets.yaml, or mstsc)",
    )
    p_connect.add_argument("--fullscreen", action="store_true")
    p_connect.add_argument("--width",  type=int, default=1920)
    p_connect.add_argument("--height", type=int, default=1080)

    p_export = sub.add_parser(
        "export",
        help="Write .rdp file(s) - open with mRemoteNG, Royal TS, Devolutions, FreeRDP, etc.",
    )
    p_export.add_argument("name",   help="Target name, or 'all' to export every RDP target")
    p_export.add_argument("output", nargs="?", default=None, help="Output file or directory")

    sub.add_parser("freerdp", help="Check if FreeRDP (wfreerdp.exe) is installed")

    p_tunnel = sub.add_parser("tunnel", help="Open VS Code connected to a vscode_tunnel target")
    p_tunnel.add_argument("name", help="Target name with protocol: vscode_tunnel (e.g. laptop-tunnel)")

    args = parser.parse_args()

    if args.cmd == "list":
        _print_targets()

    elif args.cmd == "probe":
        _print_probe()

    elif args.cmd == "connect":
        ok = rdp_connect(
            args.name,
            fullscreen=args.fullscreen,
            width=args.width,
            height=args.height,
            client=args.client,
        )
        sys.exit(0 if ok else 1)

    elif args.cmd == "export":
        if args.name.lower() == "all":
            exported = []
            for t in list_targets():
                if t.get("protocol") == "rdp":
                    p = export_rdp_file(t["name"], output_path=args.output)
                    if p:
                        exported.append(str(p))
            if exported:
                print("Exported:")
                for p in exported:
                    print(f"  {p}")
            else:
                print("No RDP targets exported.")
                sys.exit(1)
        else:
            p = export_rdp_file(args.name, output_path=args.output)
            if p:
                print(f"Exported: {p}")
            else:
                sys.exit(1)

    elif args.cmd == "freerdp":
        exe = find_freerdp()
        if exe:
            print(f"FreeRDP found: {exe}")
        else:
            print("FreeRDP not found.  Install with:  winget install FreeRDP.FreeRDP")
            sys.exit(1)

    elif args.cmd == "tunnel":
        ok = tunnel_connect(args.name)
        sys.exit(0 if ok else 1)

    else:
        parser.print_help()
        sys.exit(1)
