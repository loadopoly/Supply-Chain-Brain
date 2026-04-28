"""Compute-node daemon — standalone entry point for symbiotic peers.

Run this on any workstation that should join the Supply Chain Brain compute
fabric.  It:
  1. Publishes the local machine's capacity JSON to the OneDrive-synced
     bridge_state/compute_peers/<host>.json rendezvous directory.
  2. Starts the TCP listener on port 8000 to accept HMAC-signed grid jobs.
  3. Re-publishes the heartbeat every 30 s so the master node knows the
     peer is alive.

Usage (from the OneDrive-synced workspace root):
    # Windows — activate venv first, then:
    python pipeline/compute_node_daemon.py

    # Or run via the companion PowerShell script:
    .\\pipeline\\start_compute_node.ps1

Security:
  * The SCBRAIN_GRID_SECRET env var must match the originating node.
  * If not set, the dev default 'scbrain-dev' is used (safe for LAN-only use).
  * Jobs are HMAC-SHA256 authenticated; unsigned payloads are silently dropped.
  * This node never initiates connections — it only receives and executes
    Brain compute tasks (self_expansion_infer_slice, self_expansion_edge_commit,
    self_expansion_compute).  No credentials, Oracle connections, or Azure
    state are ever written or read by these tasks.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Make sure pipeline/src is importable regardless of where the user cwd is.
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent          # …/pipeline/
_WORKSPACE = _HERE.parent                        # …/VS Code/
for _p in (_HERE, _WORKSPACE):
    _s = str(_p)
    if _s not in sys.path:
        sys.path.insert(0, _s)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("compute_node_daemon")


def _banner() -> None:
    import socket
    host = socket.gethostname()
    from src.brain.compute_grid import _cfg
    port = int(_cfg().get("listen_port", 8000))
    secret_set = bool(os.environ.get("SCBRAIN_GRID_SECRET"))
    log.info("=" * 60)
    log.info("Supply Chain Brain — Compute Node Daemon")
    log.info("  Host:    %s", host)
    log.info("  Port:    %d", port)
    log.info("  Secret:  %s", "env var set ✓" if secret_set else "dev default (scbrain-dev)")
    log.info("  Tasks:   self_expansion_infer_slice | edge_commit | compute")
    log.info("=" * 60)


def main() -> None:
    _banner()

    from src.brain.compute_grid import publish_local_capacity, serve_compute_node
    import threading

    # Publish capacity immediately so the originating node sees this peer
    # within one OneDrive sync cycle (~5–15 s).
    try:
        p = publish_local_capacity(ensure_listener=False)
        log.info("Capacity published → cpu=%d  ram=%.1f GB  vram=%.1f GB",
                 p.cpu_count, p.free_ram_gb, p.free_vram_gb)
    except Exception as exc:
        log.warning("publish_local_capacity failed: %s — continuing anyway", exc)

    # Start the TCP listener in a daemon thread so Ctrl-C kills everything.
    t = threading.Thread(target=serve_compute_node, name="grid-node", daemon=True)
    t.start()
    log.info("Listener started on :8000 — waiting for jobs from the Brain fabric …")
    log.info("Press Ctrl-C to stop.")

    # Graceful shutdown on SIGTERM (Windows Task Scheduler stop).
    def _stop(sig, frame):  # noqa: ANN001
        log.info("Received signal %s — shutting down.", sig)
        sys.exit(0)

    try:
        signal.signal(signal.SIGTERM, _stop)
    except (OSError, ValueError):
        pass  # Not available in all contexts

    try:
        while t.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Keyboard interrupt — stopping.")


if __name__ == "__main__":
    main()
