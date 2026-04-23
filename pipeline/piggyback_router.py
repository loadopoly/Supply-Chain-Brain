import socket
import threading
import select
import argparse
import logging
import sys
import time

# v0.7.3 â€” added: HTTP/SOCKS5 proxy skill acquisition modes
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


def handle_socks5(client_socket, client_addr):
    try:
        # Handshake
        client_socket.recv(262) # Version & Methods
        client_socket.sendall(b"\x05\x00") # No Auth Required

        # Request
        req = client_socket.recv(4)
        if len(req) < 4 or req[1] != 0x01: # Only CONNECT is supported
            client_socket.close()
            return
            
        addr_type = req[3]
        if addr_type == 0x01: # IPv4
            ip = socket.inet_ntoa(client_socket.recv(4))
        elif addr_type == 0x03: # Domain name
            domain_len = client_socket.recv(1)[0]
            ip = client_socket.recv(domain_len).decode('utf-8')
        elif addr_type == 0x04: # IPv6
            client_socket.close()
            return
            
        port = int.from_bytes(client_socket.recv(2), 'big')

        logging.info(f"[*] SOCKS5 from {client_addr} connecting to {ip}:{port}")
        
        target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        target_socket.settimeout(_CONNECT_TIMEOUT)
        target_socket.connect((ip, port))
        target_socket.settimeout(None)
        
        # Success Reply
        client_socket.sendall(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
        
        forward_data(client_socket, target_socket)
    except Exception as e:
        logging.error(f"[-] SOCKS5 error for {client_addr}: {e}")
        try: client_socket.close()
        except: pass


def handle_http_proxy(client_socket, client_addr):
    try:
        request = b""
        while b"\r\n\r\n" not in request:
            chunk = client_socket.recv(4096)
            if not chunk: return
            request += chunk
            if len(request) > 16384: return

        lines = request.split(b"\r\n")
        first_line = lines[0].split(b" ")
        if len(first_line) < 3: return
            
        method, url, _ = first_line
        
        if method == b"CONNECT":
            host_port = url.split(b":")
            host = host_port[0].decode('utf-8')
            port = int(host_port[1]) if len(host_port) > 1 else 443

            logging.info(f"[*] HTTP CONNECT from {client_addr} to {host}:{port}")

            target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            target_socket.settimeout(_CONNECT_TIMEOUT)
            target_socket.connect((host, port))
            target_socket.settimeout(None)

            client_socket.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            forward_data(client_socket, target_socket)
        else:
            client_socket.sendall(b"HTTP/1.1 501 Not Implemented\r\n\r\nOnly CONNECT supported")
            client_socket.close()
    except Exception as e:
        logging.error(f"[-] HTTP Proxy error for {client_addr}: {e}")
        try: client_socket.close()
        except: pass


def start_proxy_bridge(listen_host, listen_port, target_host=None, target_port=None, proxy_type="forward"):     
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.settimeout(1.0)  # allows clean shutdown polling

    try:
        server.bind((listen_host, listen_port))
        server.listen(_BACKLOG)
        
        if proxy_type == "forward":
            logging.info(f"[GATEWAY ACTIVE] {listen_host}:{listen_port} -> {target_host}:{target_port}")
        else:
            logging.info(f"[{proxy_type.upper()} ACTIVE] {listen_host}:{listen_port}")
            
        logging.info("Waiting for connections... (Ctrl-C to stop)")

        while not _shutdown.is_set():
            try:
                client_socket, addr = server.accept()
            except socket.timeout:
                continue
                
            if proxy_type == "forward":
                target = handle_client
                args = (client_socket, addr, target_host, target_port)
            elif proxy_type == "socks5":
                target = handle_socks5
                args = (client_socket, addr)
            elif proxy_type == "http":
                target = handle_http_proxy
                args = (client_socket, addr)

            t = threading.Thread(target=target, args=args, daemon=True)
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
    parser.add_argument("--mode", choices=["laptop-rdp-gateway", "desktop-vpn-gateway", "custom", "socks5-proxy", "http-proxy"], required=True,
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

    elif args.mode == "socks5-proxy":
        print("\n--- SOCKS5 PROXY MODE ---")
        print(f"Set your applications to use SOCKS5 proxy on 0.0.0.0:1080")
        start_proxy_bridge("0.0.0.0", 1080, proxy_type="socks5")

    elif args.mode == "http-proxy":
        print("\n--- HTTP CONNECT PROXY MODE ---")
        print(f"Set your applications to use HTTP proxy on 0.0.0.0:3128")
        start_proxy_bridge("0.0.0.0", 3128, proxy_type="http")