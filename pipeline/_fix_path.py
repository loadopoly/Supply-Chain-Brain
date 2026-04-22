from pathlib import Path
p = Path(r"\\crp-fs03\public\Executive_Reports\agent_app\sophos_vpn_automator.py")
content = p.read_text(encoding="utf-8")
old = '_state = _OD_ROOT / "pipeline" / "bridge_state" / "wifi_ip.txt"'
new = '_state = _OD_ROOT / "bridge_state" / "wifi_ip.txt"'
if old in content:
    p.write_text(content.replace(old, new, 1), encoding="utf-8")
    print("Fixed double-pipeline path in wifi_ip.txt write")
else:
    print("Pattern not found — current bridge_state lines:")
    for i, l in enumerate(content.splitlines()):
        if "bridge_state" in l or "wifi_ip" in l:
            print(f"  {i+1}: {l}")
