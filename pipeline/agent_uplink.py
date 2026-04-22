import sys
import time
import socket
import os
import argparse
from pathlib import Path

PORT = 13337

def run_host(directory_to_watch):
    """
    Binds to the Host IP and actively listens for the user's laptop to connect.
    Once connected, it streams CLI logs and transmits any newly generated PPTX files over the raw socket.
    """
    print(f"[*] Autonomous Agent Uplink Server initializing on 0.0.0.0:{PORT}")
    print(f"[*] Watching '{directory_to_watch}' for newly generated reports...")
    
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
                
                # Snapshot current files
                seen = set(os.listdir(directory_to_watch))
                try:
                    while True:
                        time.sleep(2)
                        current = set(os.listdir(directory_to_watch))
                        new_files = current - seen
                        
                        for f in new_files:
                            if f.endswith('.pptx') or f.endswith('.csv'):
                                path = os.path.join(directory_to_watch, f)
                                print(f"[*] New data payload detected: {f}. Transmitting via Uplink...")
                                
                                # Send header flag and filename
                                conn.sendall(f"[FILE_INCOMING]:{f}\n".encode('utf-8'))
                                time.sleep(0.5) # Slight buffer for TCP stream separation
                                
                                # Send file data
                                with open(path, "rb") as file_data:
                                    data = file_data.read()
                                    conn.sendall(len(data).to_bytes(8, 'big'))
                                    conn.sendall(data)
                                    
                                print(f"[+] '{f}' transmitted to client successfully.")
                                conn.sendall(b"[HOST] Transmission complete. Resuming watch...\n")
                        
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
                    
                    # Stream file to disk
                    received = 0
                    out_path = os.path.join(download_dir, filename)
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