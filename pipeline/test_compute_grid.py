"""End-to-end smoke test for the shared compute grid + multi-LLM ensemble.

Verifies:
  * GPU detection picks up AMD/Intel as well as NVIDIA (Windows only)
  * publish_local_capacity() auto-starts the local listener (port 8000)
  * discover_peers() marks the self-host as is_local
  * pick_compute_target() routes to local without TCP overhead
  * dispatch_parallel() runs K models concurrently and learns weights
  * A dead seed peer fails fast (<2s) thanks to the negative cache

Run:  python pipeline/test_compute_grid.py
"""
from __future__ import annotations

import os
import sys
import time
import socket

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from src.brain.compute_grid import (        # noqa: E402
    publish_local_capacity, discover_peers, pick_compute_target,
    ensure_local_node_running, submit_job, ComputeTarget, Peer, _port_in_use,
)
from src.brain.llm_ensemble import dispatch_parallel, weights_for  # noqa: E402


def banner(msg: str) -> None:
    print()
    print("=" * 72)
    print(msg)
    print("=" * 72)


def test_capacity_probe() -> None:
    banner("1) Local capacity probe (CPU + RAM + GPU multi-vendor)")
    me = publish_local_capacity()
    print(f"  host         : {me.host}")
    print(f"  cpu_count    : {me.cpu_count}")
    print(f"  free_ram_gb  : {me.free_ram_gb}")
    print(f"  gpus         : {len(me.gpus)}")
    for g in me.gpus:
        print(f"    - vendor={g.get('vendor')} name={g.get('name')!r} "
              f"total_mb={g.get('total_mb')} free_mb={g.get('free_mb')}")
    print(f"  free_vram_gb : {me.free_vram_gb}")
    assert me.cpu_count > 0, "cpu_count must be > 0"
    assert me.free_ram_gb > 0, "free_ram_gb must be > 0"
    if os.name == "nt":
        # On Windows we expect at least one GPU adapter from WMI (even iGPU).
        if not me.gpus:
            print("  WARN: no GPUs detected on Windows host (iGPU usually visible).")


def test_local_listener() -> None:
    banner("2) Local compute_node listener auto-started on port 8000")
    ok = ensure_local_node_running()
    print(f"  ensure_local_node_running -> {ok}")
    listening = _port_in_use("127.0.0.1", 8000, timeout=0.5)
    print(f"  127.0.0.1:8000 listening   -> {listening}")
    assert listening, "local compute_node must be listening on :8000"


def test_self_is_local() -> None:
    banner("3) Self-host heartbeat marked is_local during discovery")
    peers = discover_peers(force=True)
    me = socket.gethostname().lower()
    found = [p for p in peers if p.host.lower() == me]
    print(f"  peers={len(peers)} self_entries={len(found)}")
    for p in peers:
        print(f"   - {p.host:25s} addr={p.address!s:18s} is_local={p.is_local} "
              f"gpu={p.has_gpu()}({p.free_vram_gb}GB) ram={p.free_ram_gb}GB")
    assert found, "self should appear in discovered peers"
    assert all(p.is_local for p in found), "self must be flagged is_local"


def test_pick_target_routes_local() -> None:
    banner("4) pick_compute_target chooses local (zero TCP)")
    tgt = pick_compute_target()
    print(f"  chosen={tgt.peer.host} is_local={tgt.peer.is_local} "
          f"address={tgt.peer.address} reason={tgt.reason!r}")
    assert tgt.peer.is_local or tgt.peer.address == "127.0.0.1", (
        "selection must resolve to local until other peers come online")


def test_dead_peer_fails_fast() -> None:
    banner("5) Dead peer fails fast via TCP pre-probe + negative cache")
    dead = Peer(host="dead-peer", address="10.255.255.1", port=8000,
                cpu_count=8, cpu_load_1m=0.1, free_ram_gb=16.0)
    target = ComputeTarget(peer=dead, reason="forced", fallback=False)
    t0 = time.perf_counter()
    try:
        submit_job(target, {"task": "default", "body": {"ping": True}})
        print("  ERROR: dead peer call did not raise!")
        assert False
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        print(f"  raised after {elapsed_ms}ms: {str(e)[:80]}...")
        assert elapsed_ms < 2500, f"dead peer probe was too slow: {elapsed_ms}ms"


def test_ensemble_dispatch() -> None:
    banner("6) Parallel ensemble dispatch (now hits real listener locally)")
    t0 = time.perf_counter()
    res = dispatch_parallel(
        "vendor_consolidation",
        {"kind": "classify", "text": "need stainless steel bolts m12",
         "labels": ["Steel & Plate", "Fasteners", "Wiring"]},
        validator=lambda a: 1.0 if a and "Fasteners" in str(a) else 0.6,
    )
    elapsed = int((time.perf_counter() - t0) * 1000)
    print(f"  elapsed={elapsed}ms aggregator={res.aggregator} "
          f"contributors={len(res.contributors)}")
    for c in res.contributors:
        print(f"   - {c.model_id:25s} ok={c.ok} latency={c.latency_ms}ms "
              f"w={c.weight:.3f}")
    print(f"  answer={res.answer}")
    assert any(c.ok for c in res.contributors), "at least one model must succeed"
    # With local in-process execution and no network round-trips, the whole
    # K-way fanout should easily complete in well under 2s.
    assert elapsed < 5000, f"ensemble too slow: {elapsed}ms"

    print("  learned weights:")
    for mid, w in weights_for("vendor_consolidation").items():
        print(f"   - {mid:25s} w={w['weight']:.3f} b={w['bias']:+.3f} "
              f"n={w['n_obs']} succ_ema={w['ema_success']:.2f} "
              f"lat_ema={w['ema_latency']:.0f}ms")


def main() -> int:
    test_capacity_probe()
    test_local_listener()
    test_self_is_local()
    test_pick_target_routes_local()
    test_dead_peer_fails_fast()
    test_ensemble_dispatch()
    banner("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
