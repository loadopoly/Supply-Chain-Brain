import sys
import time
import socket
import os
import argparse
from pathlib import Path

PORT = 13337

def _scan_payloads(directory_to_watch: str) -> set:
    """Return relative paths (POSIX) of every .pptx/.csv under directory.

    Recursive so that mission-driven artifacts at
    `snapshots/missions/<mission_id>/*.pptx` are surfaced alongside the
    flat reports the legacy Host watched.
    """
    base = Path(directory_to_watch)
    out: set[str] = set()
    if not base.exists():
        return out
    try:
        for p in base.rglob("*"):
            if p.is_file() and p.suffix.lower() in (".pptx", ".csv"):
                try:
                    out.add(p.relative_to(base).as_posix())
                except Exception:
                    out.add(p.name)
    except Exception:
        pass
    return out


def _header_for(rel_path: str) -> str:
    """Build the FILE_INCOMING header. Mission artifacts get a MISSION/<id>/
    prefix the client can use to route to a per-mission folder."""
    parts = Path(rel_path).parts
    if len(parts) >= 3 and parts[0] == "missions":
        return f"MISSION/{parts[1]}/{Path(rel_path).name}"
    return Path(rel_path).name


def run_host(directory_to_watch):
    """
    Binds to the Host IP and actively listens for the user's laptop to connect.
    Once connected, it streams CLI logs and transmits any newly generated PPTX files over the raw socket.
    """
    print(f"[*] Autonomous Agent Uplink Server initializing on 0.0.0.0:{PORT}")
    print(f"[*] Watching '{directory_to_watch}' (recursive) for newly generated reports...")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        # Avoid "address already in use" errors during testing
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", PORT))
        server.listen(1)

        while True:
            print("[*] Awaiting Client Payload Connection...")
            conn, addr = server.accept()
            with conn:
                print(f"\n[+] SECURE UPLINK ESTABLISHED -> Client {addr} connected.")
                conn.sendall(b"[HOST] Uplink active. Monitoring for Autonomous Agent output...\n")

                # Snapshot current files (recursive, includes mission artifacts)
                seen = _scan_payloads(directory_to_watch)
                # Track mtimes so a refreshed living-document is re-sent.
                mtimes: dict[str, float] = {}
                for rel in seen:
                    try:
                        mtimes[rel] = (Path(directory_to_watch) / rel).stat().st_mtime
                    except Exception:
                        mtimes[rel] = 0.0
                try:
                    while True:
                        time.sleep(2)
                        current = _scan_payloads(directory_to_watch)
                        new_files = current - seen
                        # Detect refreshed living artifacts (same path, newer mtime)
                        refreshed_files: set[str] = set()
                        for rel in current & seen:
                            try:
                                mt = (Path(directory_to_watch) / rel).stat().st_mtime
                            except Exception:
                                continue
                            if mt > mtimes.get(rel, 0.0) + 1.0:
                                refreshed_files.add(rel)
                                mtimes[rel] = mt

                        for rel in (new_files | refreshed_files):
                            path = os.path.join(directory_to_watch, rel)
                            header = _header_for(rel)
                            print(f"[*] Payload: {header}. Transmitting via Uplink...")

                            # Send header flag and filename
                            conn.sendall(f"[FILE_INCOMING]:{header}\n".encode('utf-8'))
                            time.sleep(0.5)  # Slight buffer for TCP stream separation

                            # Send file data
                            with open(path, "rb") as file_data:
                                data = file_data.read()
                                conn.sendall(len(data).to_bytes(8, 'big'))
                                conn.sendall(data)

                            print(f"[+] '{header}' transmitted to client successfully.")
                            conn.sendall(b"[HOST] Transmission complete. Resuming watch...\n")
                            try:
                                mtimes[rel] = Path(path).stat().st_mtime
                            except Exception:
                                pass

                        seen = current

                        # Ping client to ensure connection is still alive
                        conn.sendall(b"")
                except (ConnectionResetError, BrokenPipeError):
                    print(f"[-] Client {addr} severed the uplink. Resetting listener...")


def run_client(host_ip, download_dir):
    """
    The CLI payload run physically on the user's laptop. 
    Connects back through the VPN tunnel to the Host Agent's raw TCP socket.
    """
    print(f"[*] Connecting to Autonomous Agent Host at {host_ip}:{PORT}...")
    Path(download_dir).mkdir(parents=True, exist_ok=True)
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((host_ip, PORT))
            print("[+] Uplink established. Awaiting live data stream...")
            
            while True:
                # Read 1 byte at a time until newline to safely parse headers without eating binary data
                header_bytes = bytearray()
                while True:
                    chunk = s.recv(1)
                    if not chunk:
                        break
                    header_bytes += chunk
                    if chunk == b'\n':
                        break
                
                if not header_bytes:
                    break
                
                text_header = header_bytes.decode('utf-8', errors='ignore')
                
                if "[FILE_INCOMING]:" in text_header:
                    filename = text_header.split("[FILE_INCOMING]:")[1].strip()
                    print(f"\n[>>>] INCOMING DATA PAYLOAD: {filename}")

                    # Read 8-byte size header
                    size_data = s.recv(8)
                    if not size_data: break
                    file_size = int.from_bytes(size_data, 'big')
                    print(f"[*] Receiving {file_size} bytes over tunnel...")

                    # Mission artifacts arrive as MISSION/<id>/<file>; route
                    # them into a per-mission folder on the laptop body so
                    # refreshes overwrite in place rather than piling up.
                    if filename.startswith("MISSION/"):
                        rel = Path(filename)
                        out_path = str(Path(download_dir) / rel)
                        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                    else:
                        out_path = os.path.join(download_dir, filename)

                    # Stream file to disk
                    received = 0
                    with open(out_path, "wb") as f:
                        while received < file_size:
                            chunk_size = min(4096, file_size - received)
                            data = s.recv(chunk_size)
                            if not data: break
                            f.write(data)
                            received += len(data)
                            
                    print(f"[+] Payload localized successfully to: {out_path}")
                else:
                    # Print standard server logs exactly as they arrive
                    print(text_header, end="")
                    
        except ConnectionRefusedError:
            print(f"[-] Connection refused. Ensure the Host Uplink Server is running on {host_ip}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Autonomous Agent Raw TCP Uplink Bridge")
    parser.add_argument("--listen", action="store_true", help="[Host Mode] Bind to port and stream new files.")
    parser.add_argument("--connect", type=str, help="[Client Mode] Connect to Host IP to receive files.")
    parser.add_argument("--dir", type=str, default=".", help="Directory to watch (Host) or save payload to (Client)")
    args = parser.parse_args()

    if args.listen:
        run_host(args.dir)
    elif args.connect:
        run_client(args.connect, args.dir)
    else:
        parser.print_help()