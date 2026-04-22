import socket
import threading
import select
import argparse
import logging
import sys
import time

# v0.7.2 — hardened: timeouts, throttle, clean shutdown
logging.basicConfig(level=logging.INFO, format="[*] %(message)s")

_CONNECT_TIMEOUT   = 10    # seconds to establish outbound connection
_IDLE_TIMEOUT      = 120   # seconds of silence before dropping tunnel
_BACKLOG           = 20    # max pending connections
_MAX_THREADS       = 50    # guard against runaway connection storms
_BUF               = 65536 # larger recv buffer for RDP/SQL throughput

_active_threads = 0
_lock = threading.Lock()
_shutdown = threading.Event()


def forward_data(src, dst):
    """Bidirectional raw TCP byte shuffle with idle timeout."""
    try:
        while not _shutdown.is_set():
            readable, _, exceptional = select.select([src, dst], [], [src, dst], _IDLE_TIMEOUT)
            if exceptional or not readable:
                break
            for s_in, s_out in ((src, dst), (dst, src)):
                if s_in in readable:
                    chunk = s_in.recv(_BUF)
                    if not chunk:
                        return
                    s_out.sendall(chunk)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            s.close()


def handle_client(client_socket, client_addr, target_host, target_port):
    global _active_threads
    with _lock:
        if _active_threads >= _MAX_THREADS:
            logging.warning(f"[!] Thread limit reached ({_MAX_THREADS}). Dropping {client_addr}.")
            client_socket.close()
            return
        _active_threads += 1

    logging.info(f"[+] {client_addr} -> {target_host}:{target_port}  (active={_active_threads})")
    target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    target_socket.settimeout(_CONNECT_TIMEOUT)
    try:
        target_socket.connect((target_host, target_port))
        target_socket.settimeout(None)
        forward_data(client_socket, target_socket)
    except (OSError, socket.timeout) as e:
        logging.error(f"[-] Cannot reach {target_host}:{target_port} — {e}")
        client_socket.close()
        target_socket.close()
    finally:
        with _lock:
            _active_threads -= 1


def start_proxy_bridge(listen_host, listen_port, target_host, target_port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.settimeout(1.0)  # allows clean shutdown polling

    try:
        server.bind((listen_host, listen_port))
        server.listen(_BACKLOG)
        logging.info(f"[GATEWAY ACTIVE] {listen_host}:{listen_port} -> {target_host}:{target_port}")
        logging.info("Waiting for connections... (Ctrl-C to stop)")

        while not _shutdown.is_set():
            try:
                client_socket, addr = server.accept()
            except socket.timeout:
                continue
            t = threading.Thread(
                target=handle_client,
                args=(client_socket, addr, target_host, target_port),
                daemon=True,
            )
            t.start()

    except KeyboardInterrupt:
        pass
    finally:
        _shutdown.set()
        server.close()
        logging.info("[GATEWAY STOPPED]")
        sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dual-Mode Piggyback Router Bridge")
    parser.add_argument("--mode", choices=["laptop-rdp-gateway", "desktop-vpn-gateway", "custom"], required=True,
                        help="Select the operational mode for the bridge.")
    parser.add_argument("--desktop-ip", type=str, default="172.16.4.76", help="The LAN IP of ROADD-5WD1NH3")
    parser.add_argument("--target-resource", type=str, help="The target VPN resource (for custom/desktop modes)")
    parser.add_argument("--target-port", type=int, help="The target port")

    args = parser.parse_args()

    if args.mode == "laptop-rdp-gateway":
        # Runs on the Laptop. 
        # Listens on 0.0.0.0 (all interfaces, including the VPN).
        # Any additional workstation connecting to the laptop's VPN IP on port 33890 
        # is seamlessly tunneled backwards through the RDP line to the Desktop's 3389 port.
        print("\n--- LAPTOP RDP GATEWAY MODE ---")
        print(f"Instruct additional workstations to RDP to: YOUR_LAPTOP_VPN_IP:33890")
        start_proxy_bridge("0.0.0.0", 33890, args.desktop_ip, 3389)

    elif args.mode == "desktop-vpn-gateway":
        # Runs on the Desktop.
        # Binds locally. When the desktop apps hit localhost:port, 
        # the traffic routes through to the laptop's VPN access.
        if not args.target_resource or not args.target_port:
            logging.error("You must specify --target-resource and --target-port for desktop mode.")
            sys.exit(1)
            
        print("\n--- DESKTOP VPN GATEWAY MODE ---")
        print(f"Point your Desktop applications to 127.0.0.1:4444 to ride the laptop's VPN.")
        start_proxy_bridge("127.0.0.1", 4444, args.target_resource, args.target_port)
        
    elif args.mode == "custom":
        start_proxy_bridge("0.0.0.0", 8080, args.target_resource, args.target_port)