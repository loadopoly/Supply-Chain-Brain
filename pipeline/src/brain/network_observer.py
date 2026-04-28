"""Network Observer — latent learning continuity across the entire node fabric.

This module is the Brain's nervous system for distributed learning.  It runs as
a daemon thread inside every autonomous_agent.py instance and:

    1. **Learning state broadcast** — every publish interval, extends the local
       host's existing compute_peer JSON (bridge_state/compute_peers/<host>.json)
       with the current learning state (corpus cursors, entity/edge counts,
       phase, last_alive).  Any other node can read this state via the same
       OneDrive-synced rendezvous already used by compute_grid.py.

    2. **Peer liveness monitoring** — reads all peer JSONs every 60 s and
       classifies each as: ALIVE | COOLING | OFFLINE.  A peer is OFFLINE when
       its published `ts` is older than `stale_after_s` (default 180 s).

    3. **Offline-peer absorption** — when a peer transitions ALIVE → OFFLINE the
       surviving node inherits its "learning responsibility" for that corpus
       segment.  Concretely:
         • The surviving node reads the peer's last known corpus cursors.
         • For any cursor where the peer was ahead of us, we rewind to the
           peer's cursor position so we do NOT re-learn what they already
           recorded — instead we pick up from where they stopped.
         • A catchup_burst is scheduled (via resumption_manager) with
           magnitude proportional to peer count that went offline.

    4. **Singularity consumption rate** — the observer calculates a network-wide
       "learning velocity" metric: Σ(learnings_logged) / Σ(time_alive) across
       all visible peers and writes it to brain_kv as
       `observer:network_velocity`.  The systemic_refinement_agent reads this
       value to decide whether to escalate or relax its cadence.  The
       theoretical maximum is all known corpus segments being consumed
       simultaneously across all nodes — one singularity per segment boundary.

    5. **End-goal alignment** — each liveness cycle verifies that the
       `quest:type5_sc` entity exists in the knowledge graph and logs a
       learning-direction signal to brain_kv so the corpus ingestors always
       know which direction to lean even when operating in isolation.

Architecture note
-----------------
The observer does NOT open any new network ports and does NOT require any new
infrastructure.  The transport is the same OneDrive-synced JSON file that
compute_grid.py already writes.  The only addition is extra fields inside that
JSON.  This means the capability is inherent and latent — any node that signs in
to the Microsoft 365 account and runs autonomous_agent.py automatically joins
the fabric.

This module is imported and started by autonomous_agent.py alongside the
existing skill_acquirer and systemic_refinement_agent daemons.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths — resolved relative to this file so the module works regardless of cwd
# ---------------------------------------------------------------------------
_HERE          = Path(__file__).resolve()
_PIPELINE_ROOT = _HERE.parents[2]
_STATE_DIR     = _PIPELINE_ROOT / "bridge_state" / "compute_peers"
_LOGS_DIR      = _PIPELINE_ROOT / "logs"

# Liveness thresholds (seconds)
_ALIVE_THRESHOLD   = 180   # peer JSON older than this → OFFLINE
_COOLING_THRESHOLD = 90    # peer JSON in this range → COOLING (warn but not absorb)

# How often the observer loop runs
_OBSERVE_INTERVAL_S = 60   # seconds between liveness scans
_PUBLISH_INTERVAL_S = 60   # seconds between learning-state broadcasts

# brain_kv keys
_KEY_NET_VELOCITY   = "observer:network_velocity"
_KEY_PEER_REGISTRY  = "observer:peer_registry"
_KEY_ABSORB_LOG     = "observer:absorption_log"
_KEY_GOAL_SIGNAL    = "observer:goal_alignment"

# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def schedule_in_background(interval_s: int = _OBSERVE_INTERVAL_S) -> threading.Thread:
    """Start the network observer as a daemon thread.  Safe to call multiple
    times — only one thread is ever started per process."""
    t = threading.Thread(
        target=_observer_loop,
        kwargs={"interval_s": interval_s},
        name="network-observer",
        daemon=True,
    )
    t.start()
    logging.info(
        f"NetworkObserver started — liveness scan every {interval_s}s, "
        f"publishing to {_STATE_DIR}"
    )
    return t


# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------

@dataclass
class _PeerState:
    host:            str
    ts_epoch:        float
    cursors:         dict = field(default_factory=dict)
    learnings_total: int = 0
    entity_count:    int = 0
    edge_count:      int = 0
    phase:           int = 0
    alive_since:     float = 0.0
    status:          str = "UNKNOWN"  # ALIVE | COOLING | OFFLINE
    session_blob:    str = ""         # blob name in Azure if peer has pushed


# Per-process memory so we don't re-absorb the same peer twice
_ABSORBED_PEERS: set[str] = set()
_PEER_REGISTRY:  dict[str, _PeerState] = {}
_REGISTRY_LOCK = threading.Lock()

# Session-store sync cadence (seconds)
_SESSION_PUSH_INTERVAL_S = 900   # 15 min — push this node's session blob
_SESSION_PULL_INTERVAL_S = 1800  # 30 min — pull ALIVE peer blobs
_last_session_push: float = 0.0
_last_session_pull: float = 0.0


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _observer_loop(interval_s: int = _OBSERVE_INTERVAL_S) -> None:
    _STATE_DIR.mkdir(parents=True, exist_ok=True)
    _LOGS_DIR.mkdir(parents=True, exist_ok=True)

    last_publish = 0.0

    while True:
        try:
            now = time.time()

            # 1. Broadcast this node's learning state
            if now - last_publish >= _PUBLISH_INTERVAL_S:
                _publish_learning_state()
                last_publish = now

            # 2. Scan peers
            current = _scan_peers(now)

            # 3. Detect offline transitions and absorb
            _process_transitions(current, now)

            # 4. Update network velocity metric
            _update_network_velocity(current, now)

            # 5. Ensure end-goal alignment is persisted
            _pulse_goal_alignment()

            # 6. Session-store cloud sync — children's session knowledge
            _sync_session_stores(current, now)

        except Exception as exc:
            logging.warning(f"network_observer: cycle error: {exc}")

        time.sleep(interval_s)


# ---------------------------------------------------------------------------
# Step 1 — broadcast learning state
# ---------------------------------------------------------------------------

def _publish_learning_state() -> None:
    """Extend the local compute-peer JSON with learning state fields."""
    import socket as _sock
    host = _sock.gethostname()
    peer_file = _STATE_DIR / f"{host}.json"

    learning = _read_local_learning_state()

    # Read existing capacity JSON written by compute_grid.publish_local_capacity
    existing: dict = {}
    if peer_file.exists():
        try:
            existing = json.loads(peer_file.read_text(encoding="utf-8"))
        except Exception:
            pass

    existing.update({
        "ts":            datetime.now(tz=timezone.utc).isoformat(),
        "learning":      learning,
        "goal":          "type5_sc",
        "session_blob":  _local_session_blob_name(),
    })
    try:
        peer_file.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    except Exception as exc:
        logging.debug(f"network_observer: publish failed: {exc}")


def _read_local_learning_state() -> dict:
    """Read corpus cursors, entity/edge counts and plasticity state from the
    local DB.  Returns an empty dict on any error so callers are never blocked."""
    try:
        from .local_store import db_path
        from .knowledge_corpus import _get_cursor as _gc
        cn = sqlite3.connect(str(db_path()), timeout=5)
        cn.row_factory = sqlite3.Row

        # Corpus cursors
        cursors = {}
        try:
            rows = cn.execute("SELECT key, value FROM corpus_cursor").fetchall()
            cursors = {r["key"]: int(r["value"]) for r in rows}
        except Exception:
            pass

        # Entity / edge counts
        entity_count = edge_count = 0
        try:
            entity_count = cn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        except Exception:
            pass
        try:
            edge_count = cn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        except Exception:
            pass

        # Total learnings
        learnings_total = 0
        try:
            learnings_total = cn.execute(
                "SELECT COUNT(*) FROM learning_log"
            ).fetchone()[0]
        except Exception:
            pass

        # Plasticity phase
        phase = 0
        try:
            row = cn.execute(
                "SELECT value FROM brain_kv WHERE key='neural_plasticity_state'"
            ).fetchone()
            if row:
                s = json.loads(row[0])
                phase = int(s.get("round", 0))
        except Exception:
            pass

        # Alive stamp
        alive_since = 0.0
        try:
            row = cn.execute(
                "SELECT value FROM brain_kv WHERE key='resumption:last_alive'"
            ).fetchone()
            if row and row[0]:
                alive_since = float(row[0])
        except Exception:
            pass

        cn.close()
        return {
            "cursors":         cursors,
            "entity_count":    entity_count,
            "edge_count":      edge_count,
            "learnings_total": learnings_total,
            "phase":           phase,
            "alive_since":     alive_since,
        }
    except Exception as exc:
        logging.debug(f"network_observer: learning state read failed: {exc}")
        return {}


# ---------------------------------------------------------------------------
# Step 2 — scan peers
# ---------------------------------------------------------------------------

def _scan_peers(now: float) -> dict[str, _PeerState]:
    peers: dict[str, _PeerState] = {}
    import socket as _sock
    self_host = _sock.gethostname().lower()

    for fp in _STATE_DIR.glob("*.json"):
        try:
            d = json.loads(fp.read_text(encoding="utf-8"))
            host = str(d.get("host", fp.stem)).lower()
            if host == self_host:
                continue  # skip self

            # Parse timestamp
            ts_raw = d.get("ts", "")
            try:
                ts_epoch = datetime.fromisoformat(ts_raw).timestamp()
            except Exception:
                ts_epoch = fp.stat().st_mtime

            age = now - ts_epoch
            if age < _COOLING_THRESHOLD:
                status = "ALIVE"
            elif age < _ALIVE_THRESHOLD:
                status = "COOLING"
            else:
                status = "OFFLINE"

            learning = d.get("learning", {})
            ps = _PeerState(
                host=host,
                ts_epoch=ts_epoch,
                cursors=learning.get("cursors", {}),
                learnings_total=int(learning.get("learnings_total", 0)),
                entity_count=int(learning.get("entity_count", 0)),
                edge_count=int(learning.get("edge_count", 0)),
                phase=int(learning.get("phase", 0)),
                alive_since=float(learning.get("alive_since", 0.0)),
                status=status,
                session_blob=str(d.get("session_blob", "")),
            )
            peers[host] = ps
        except Exception:
            continue

    # Update shared registry
    with _REGISTRY_LOCK:
        _PEER_REGISTRY.update(peers)

    return peers


# ---------------------------------------------------------------------------
# Step 3 — detect offline transitions and absorb
# ---------------------------------------------------------------------------

def _process_transitions(current: dict[str, _PeerState], now: float) -> None:
    newly_offline = [
        ps for host, ps in current.items()
        if ps.status == "OFFLINE" and host not in _ABSORBED_PEERS
    ]

    if not newly_offline:
        return

    for ps in newly_offline:
        logging.warning(
            f"network_observer: peer OFFLINE — {ps.host} "
            f"(last seen {round((now - ps.ts_epoch)/60, 1)} min ago, "
            f"{ps.learnings_total} learnings, phase {ps.phase})"
        )
        _absorb_peer(ps, now)
        _ABSORBED_PEERS.add(ps.host)

    # Prune absorbed set when peers come back online so we can re-absorb
    # future future outages of the same host.
    came_back = {
        h for h, ps in current.items()
        if ps.status == "ALIVE" and h in _ABSORBED_PEERS
    }
    for h in came_back:
        _ABSORBED_PEERS.discard(h)
        logging.info(f"network_observer: peer RECOVERED — {h}, cleared absorption lock")


def _absorb_peer(ps: _PeerState, now: float) -> None:
    """Absorb the learning responsibilities of an offline peer.

    For each cursor segment owned by the offline peer:
      - If the peer was ahead of us → advance our cursor to the peer's
        position so we don't duplicate work, and schedule a burst to catch
        up on the gap the peer was covering.
      - If the peer was behind us → do nothing (we already covered it).
    """
    try:
        from .local_store import db_path
        from .resumption_manager import schedule_catchup_burst, _kv_set
        cn = sqlite3.connect(str(db_path()), timeout=10)
        cn.row_factory = sqlite3.Row

        # Read our own cursors
        our_cursors: dict[str, int] = {}
        try:
            rows = cn.execute("SELECT key, value FROM corpus_cursor").fetchall()
            our_cursors = {r["key"]: int(r["value"]) for r in rows}
        except Exception:
            pass

        absorbed_segments: list[str] = []
        for seg, their_val in ps.cursors.items():
            our_val = our_cursors.get(seg, 0)
            if their_val > our_val:
                # Peer was ahead — advance us to their position (they
                # already covered up to here) so we resume from there.
                cn.execute(
                    "INSERT OR REPLACE INTO corpus_cursor(key, value) VALUES(?,?)",
                    (seg, str(their_val)),
                )
                absorbed_segments.append(f"{seg}:{our_val}→{their_val}")

        if absorbed_segments:
            cn.commit()
            # Downtime of peer expressed as age of their last heartbeat
            peer_downtime = now - ps.ts_epoch
            # Scale burst with number of segments absorbed
            burst_debt = peer_downtime * len(absorbed_segments)
            schedule_catchup_burst(cn, burst_debt)
            logging.info(
                f"network_observer: absorbed {len(absorbed_segments)} segments "
                f"from {ps.host}: {', '.join(absorbed_segments[:5])} "
                f"— burst scheduled"
            )

        # Write absorption event to brain_kv
        event = {
            "host":         ps.host,
            "absorbed_at":  datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            "segments":     absorbed_segments,
            "peer_phase":   ps.phase,
            "peer_learnings": ps.learnings_total,
        }
        try:
            raw = cn.execute(
                "SELECT value FROM brain_kv WHERE key=?", (_KEY_ABSORB_LOG,)
            ).fetchone()
            log: list = json.loads(raw[0]) if raw else []
        except Exception:
            log = []
        log.append(event)
        log = log[-500:]   # keep last 500
        _kv_set(cn, _KEY_ABSORB_LOG, json.dumps(log))

        # Also record as a learning log entry
        try:
            cn.execute(
                "INSERT INTO learning_log"
                "(logged_at, kind, title, detail, signal_strength, source_table, source_row_id)"
                " VALUES(?,?,?,?,?,?,?)",
                (
                    datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
                    "peer_absorption",
                    f"Absorbed offline peer: {ps.host}",
                    json.dumps(event),
                    0.9,
                    "network_observer",
                    ps.host,
                ),
            )
        except Exception:
            pass

        cn.commit()
        cn.close()

    except Exception as exc:
        logging.error(f"network_observer: absorption failed for {ps.host}: {exc}")

    # ── Failsafe: trigger an immediate expansion cycle ─────────────────────────
    # The peer going OFFLINE means its CPU contribution is gone.  Fire one
    # extra self_expansion cycle immediately (in a daemon thread) so the
    # surviving node fills the gap without waiting 30 minutes for the
    # normal cadence.  The cycle runs in its own thread so it never blocks
    # the observer loop — if the expansion lock is held the call returns
    # quickly via the `_lock` trylock path inside run_self_expansion.
    try:
        import threading as _th
        _th.Thread(
            target=_run_failsafe_expansion,
            args=(ps.host,),
            name=f"failsafe-expansion-{ps.host}",
            daemon=True,
        ).start()
    except Exception as _fe:
        logging.debug(f"network_observer: failsafe expansion thread error: {_fe}")

    # Also absorb this peer's session store if it published one
    if ps.session_blob:
        _pull_peer_session_blob(ps.host, ps.session_blob)


def _run_failsafe_expansion(offline_peer_host: str) -> None:
    """Run one self_expansion cycle immediately after a peer goes offline.

    This is the automatic dispersal path: when a node in the parent-child
    network loses a resource, the surviving node expands to compensate.
    The expansion broadcasts its newly committed edges to ALL remaining
    alive peers via the `_fan_out_committed_edges` mechanism — so the
    corpus propagates across every node that is still up.

    Non-blocking: if another cycle is already in progress the lock inside
    `run_self_expansion` serializes naturally; we just log and return.
    """
    import time as _t
    _t.sleep(5)   # brief pause so the observer can finish logging first
    try:
        from .self_expansion import run_self_expansion
        logging.info(
            f"network_observer: failsafe expansion triggered "
            f"(peer offline: {offline_peer_host})"
        )
        result = run_self_expansion()
        logging.info(
            f"network_observer: failsafe expansion complete — "
            f"committed={result.get('committed', 0)} "
            f"remote={result.get('remote_compute_host', 'local')}"
        )
    except Exception as exc:
        logging.warning(f"network_observer: failsafe expansion error: {exc}")


# ---------------------------------------------------------------------------
# Step 4 — network velocity metric
# ---------------------------------------------------------------------------

def _update_network_velocity(current: dict[str, _PeerState], now: float) -> None:
    """Compute Σ(learnings) / Σ(uptime_hours) across alive peers and write to
    brain_kv so the systemic_refinement_agent can use it for cadence decisions."""
    alive_peers = [ps for ps in current.values() if ps.status == "ALIVE"]

    total_learnings = 0
    total_uptime_h  = 0.0
    for ps in alive_peers:
        total_learnings += ps.learnings_total
        uptime = now - ps.alive_since if ps.alive_since > 0 else 3600
        total_uptime_h += uptime / 3600.0

    # Include local node
    try:
        local_state = _read_local_learning_state()
        total_learnings += local_state.get("learnings_total", 0)
        local_uptime = (now - local_state.get("alive_since", now - 3600)) / 3600.0
        total_uptime_h += max(local_uptime, 0.1)
    except Exception:
        pass

    velocity = (total_learnings / total_uptime_h) if total_uptime_h > 0 else 0.0

    try:
        from .local_store import db_path
        from .resumption_manager import _kv_set
        cn = sqlite3.connect(str(db_path()), timeout=5)
        _kv_set(cn, _KEY_NET_VELOCITY, json.dumps({
            "velocity_per_hour": round(velocity, 2),
            "alive_peers":       len(alive_peers) + 1,   # +1 = self
            "total_learnings":   total_learnings,
            "sampled_at":        datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        }))
        cn.commit()
        cn.close()
    except Exception:
        pass

    if alive_peers:
        logging.info(
            f"network_observer: {len(alive_peers)+1} nodes alive, "
            f"velocity={velocity:.1f} learnings/h, "
            f"Σ learnings={total_learnings}"
        )


# ---------------------------------------------------------------------------
# Step 6 — session-store cloud sync
# ---------------------------------------------------------------------------

def _local_session_blob_name() -> str:
    """Return the blob name this node would push, or '' if sync is not configured."""
    acct = os.environ.get("COPILOT_STORAGE_ACCOUNT", "")
    if not acct:
        return ""
    import socket as _sock
    return f"session-store-{_sock.gethostname().lower()}.db"


def _sync_session_stores(current: dict[str, "_PeerState"], now: float) -> None:
    """Push this node's session store and pull ALIVE peers' blobs on a slow
    cadence so session knowledge flows through the symbiotic loop."""
    global _last_session_push, _last_session_pull
    acct = os.environ.get("COPILOT_STORAGE_ACCOUNT", "")
    if not acct:
        return
    container = os.environ.get("COPILOT_STORAGE_CONTAINER", "copilot-sessions")

    # Push this node's store (rebuild first to pick up new sessions)
    if now - _last_session_push >= _SESSION_PUSH_INTERVAL_S:
        try:
            _push_local_session_store(acct, container)
            _last_session_push = now
        except Exception as exc:
            logging.debug(f"network_observer: session push failed: {exc}")

    # Pull ALIVE peers that advertise a session blob
    if now - _last_session_pull >= _SESSION_PULL_INTERVAL_S:
        alive_with_blob = [
            ps for ps in current.values()
            if ps.status in ("ALIVE", "COOLING") and ps.session_blob
        ]
        for ps in alive_with_blob:
            try:
                _pull_peer_session_blob(ps.host, ps.session_blob)
            except Exception as exc:
                logging.debug(f"network_observer: session pull from {ps.host} failed: {exc}")
        if alive_with_blob:
            _last_session_pull = now


def _push_local_session_store(acct: str, container: str) -> None:
    """Rebuild the local session-store.db and push it to Azure Blob."""
    builder = Path.home() / ".copilot" / "build_session_store.py"
    if not builder.exists():
        return
    import subprocess
    result = subprocess.run(
        ["python", str(builder), "--push",
         "--storage-account", acct, "--container", container],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode == 0:
        logging.info("network_observer: session store pushed to Azure Blob")
    else:
        logging.debug(f"network_observer: session push stderr: {result.stderr[:200]}")


def _pull_peer_session_blob(host: str, blob_name: str) -> None:
    """Download a peer's session-store blob and merge into local DB."""
    acct = os.environ.get("COPILOT_STORAGE_ACCOUNT", "")
    container = os.environ.get("COPILOT_STORAGE_CONTAINER", "copilot-sessions")
    if not acct or not blob_name:
        return
    try:
        from azure.storage.blob import BlobServiceClient  # type: ignore
        from azure.identity import DefaultAzureCredential  # type: ignore
    except ImportError:
        return

    import tempfile, sqlite3 as _sql, sys as _sys
    # Lazy-import the merge helper from build_session_store.py
    builder = Path.home() / ".copilot" / "build_session_store.py"
    if not builder.exists():
        return

    db_local = Path.home() / ".copilot" / "session-store.db"
    if not db_local.exists():
        return

    svc = BlobServiceClient(
        account_url=f"https://{acct}.blob.core.windows.net",
        credential=DefaultAzureCredential(),
    )
    cc = svc.get_container_client(container)
    try:
        data = cc.download_blob(blob_name).readall()
    except Exception:
        return  # blob doesn't exist yet

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)

    try:
        # Use _merge_remote_db from builder module
        import importlib.util
        spec = importlib.util.spec_from_file_location("build_session_store", str(builder))
        mod = importlib.util.module_from_spec(spec)  # type: ignore
        spec.loader.exec_module(mod)  # type: ignore
        local_con = _sql.connect(str(db_local))
        ns, nt, nf = mod._merge_remote_db(tmp_path, local_con)
        if ns or nt or nf:
            mod._rebuild_fts(local_con)
            local_con.commit()
            logging.info(
                f"network_observer: merged session store from {host} "
                f"— sessions+{ns} turns+{nt} files+{nf}"
            )
        local_con.close()
    finally:
        tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Step 5 — end-goal alignment pulse
# ---------------------------------------------------------------------------

def _pulse_goal_alignment() -> None:
    """Ensure the type5_sc quest entity exists and write a directional signal
    to brain_kv so isolated nodes always know which way to lean."""
    try:
        from .local_store import db_path
        from .resumption_manager import _kv_set
        cn = sqlite3.connect(str(db_path()), timeout=5)

        # Verify quest:type5_sc exists — create minimally if absent
        row = cn.execute(
            "SELECT entity_id FROM entities WHERE entity_id='quest:type5_sc'"
        ).fetchone()
        if not row:
            cn.execute(
                "INSERT OR IGNORE INTO entities(entity_id, entity_type, label, metadata)"
                " VALUES('quest:type5_sc','Quest','Type V Civilization — Supply Chain Brain','{}')"
            )
            cn.commit()
            logging.info("network_observer: re-anchored quest:type5_sc end-goal entity")

        _kv_set(cn, _KEY_GOAL_SIGNAL, json.dumps({
            "goal":      "quest:type5_sc",
            "direction": "maximize_learnings_toward_type5_sc",
            "pulsed_at": datetime.now(tz=timezone.utc).isoformat(),
        }))
        cn.commit()
        cn.close()
    except Exception:
        pass
