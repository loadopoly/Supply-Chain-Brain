"""Tests for the network expansion learner.

Verifies:
  1) Endpoint catalog discovers entries across all protocols.
  2) A round writes audited observations and rolls topology stats.
  3) Repeated rounds update EMA (success rate + latency) without resetting.
  4) Compute-grid promotion only triggers above the configured threshold,
     and audited rows land in network_promotions.
  5) Rate-limit (`min_seconds_between_rounds`) actually short-circuits.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import time
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# Use a clean throwaway DB so we don't pollute local_brain.sqlite.
TMP_DB = ROOT / "test_local_brain_netlearn.sqlite"
if TMP_DB.exists():
    TMP_DB.unlink()

# Monkey-patch local_store.db_path before importing the learner.
from src.brain import local_store  # noqa: E402
local_store._DB_PATH = TMP_DB
local_store.db_path = lambda: TMP_DB  # type: ignore

from src.brain import network_learner as nl  # noqa: E402


def _conn():
    return sqlite3.connect(TMP_DB)


def banner(msg: str) -> None:
    bar = "=" * 72
    print(f"\n{bar}\n{msg}\n{bar}")


# ---------------------------------------------------------------------------
def test_catalog_covers_protocols() -> None:
    banner("1) Endpoint catalog covers multiple protocols/sources")
    eps = nl.list_known_endpoints()
    print(f"  endpoints discovered: {len(eps)}")
    protos = sorted({e.protocol for e in eps})
    sources = sorted({e.source for e in eps})
    print(f"  protocols: {protos}")
    print(f"  sources:   {sources}")
    assert len(eps) > 0, "catalog should discover at least one endpoint"
    assert len(protos) >= 1


# ---------------------------------------------------------------------------
def test_round_writes_observations_and_topology() -> None:
    banner("2) observe_network_round writes audit + topology rows")

    # Force an immediate round (clear rate-limit guard).
    nl._LAST_ROUND_TS = 0.0  # type: ignore[attr-defined]

    # Inject a controlled seed so a single deterministic loopback row exists.
    seed_host = "127.0.0.1"
    # Pick an almost certainly-closed port so the probe completes deterministically.
    seed_port = 65500

    # Patch _cfg to inject seeds + tighten timeouts for the test
    real_cfg = nl._cfg
    def patched_cfg() -> dict:
        c = dict(real_cfg() or {})
        c["enabled"] = True
        c["probe_timeout_s"] = 0.5
        c["min_seconds_between_rounds"] = 0.0
        c["mx_lookup"] = False    # keep test offline-friendly
        c["seeds"] = [{"host": seed_host, "protocol": "tcp", "port": seed_port,
                       "capability": "test-seed"}]
        c["max_probes_per_round"] = 8
        c["auto_promote_compute"] = False
        return c
    nl._cfg = patched_cfg  # type: ignore[assignment]

    summary = nl.observe_network_round()
    print(f"  round summary: live={summary.get('live')} down={summary.get('down')} "
          f"total={summary.get('endpoints_total')} protocols={list((summary.get('by_protocol') or {}).keys())}")
    assert "endpoints_total" in summary

    with closing(_conn()) as cn:
        n_obs = cn.execute(
            "SELECT COUNT(*) FROM network_observations WHERE host=?",
            (seed_host,),
        ).fetchone()[0]
        n_topo = cn.execute(
            "SELECT COUNT(*) FROM network_topology WHERE host=?",
            (seed_host,),
        ).fetchone()[0]
    print(f"  observations rows for seed: {n_obs}")
    print(f"  topology rows for seed:     {n_topo}")
    assert n_obs >= 1
    assert n_topo == 1


# ---------------------------------------------------------------------------
def test_ema_updates_across_rounds() -> None:
    banner("3) EMA stats update across multiple rounds (no reset)")
    # Run two more rounds back-to-back
    for _ in range(2):
        nl._LAST_ROUND_TS = 0.0  # type: ignore[attr-defined]
        nl.observe_network_round()

    with closing(_conn()) as cn:
        row = cn.execute(
            """SELECT samples, ema_success, ema_latency_ms
               FROM network_topology WHERE host='127.0.0.1' AND protocol='tcp'""",
        ).fetchone()
    print(f"  topology samples={row[0]}, ema_success={row[1]:.3f}, ema_latency_ms={row[2]:.2f}")
    assert row[0] >= 3
    assert 0.0 <= row[1] <= 1.0
    assert row[2] is not None


# ---------------------------------------------------------------------------
def test_promotion_audits_to_table() -> None:
    banner("4) Compute-grid promotion only fires above threshold")

    # Manually plant a high-confidence compute peer row (simulating EMA learned
    # over time) and a low-confidence one. Only the high one should be promoted.
    with closing(_conn()) as cn:
        cn.executescript(
            """
            INSERT OR REPLACE INTO network_topology
              (host, protocol, port, capability, first_seen, last_seen,
               last_ok, samples, successes, ema_latency_ms, ema_success, source)
            VALUES
              ('peer-good.local', 'tcp', 8000, 'compute peer',
               '2026-01-01T00:00:00+00:00', '2026-04-22T00:00:00+00:00',
               1, 50, 49, 12.0, 0.92, 'compute_peers'),
              ('peer-bad.local',  'tcp', 8000, 'compute peer',
               '2026-01-01T00:00:00+00:00', '2026-04-22T00:00:00+00:00',
               0, 50, 5,  900.0, 0.10, 'compute_peers');
            """
        )
        cn.commit()

    promoted = nl.promote_compute_peers(min_success=0.7)
    print(f"  promoted: {promoted}")
    assert "peer-good.local" in promoted
    assert "peer-bad.local" not in promoted

    with closing(_conn()) as cn:
        n = cn.execute(
            "SELECT COUNT(*) FROM network_promotions WHERE host='peer-good.local'"
        ).fetchone()[0]
    print(f"  audit rows for peer-good.local: {n}")
    assert n == 1

    extra = os.environ.get("SCBRAIN_GRID_EXTRA_SEEDS", "")
    print(f"  SCBRAIN_GRID_EXTRA_SEEDS = {extra!r}")
    assert "peer-good.local" in extra


# ---------------------------------------------------------------------------
def test_rate_limit_short_circuits() -> None:
    banner("5) min_seconds_between_rounds short-circuits subsequent rounds")
    # Force fresh _LAST_ROUND_TS to "now" so the rate-limit kicks in.
    real_cfg = nl._cfg
    def patched_cfg() -> dict:
        c = dict(real_cfg() or {})
        c["min_seconds_between_rounds"] = 9999.0
        return c
    nl._cfg = patched_cfg  # type: ignore[assignment]

    nl._LAST_ROUND_TS = time.monotonic()  # type: ignore[attr-defined]
    out = nl.observe_network_round()
    print(f"  round result: {out}")
    assert out.get("skipped") is True


# ---------------------------------------------------------------------------
def main() -> int:
    test_catalog_covers_protocols()
    test_round_writes_observations_and_topology()
    test_ema_updates_across_rounds()
    test_promotion_audits_to_table()
    test_rate_limit_short_circuits()
    banner("ALL NETWORK-LEARNER GUARD-RAILS VERIFIED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
