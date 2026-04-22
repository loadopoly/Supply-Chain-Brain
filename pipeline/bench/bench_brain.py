"""Synthetic-data benchmarks for the Supply Chain Brain analytics core.

Run from the repo root::

    python -m bench.bench_brain --rows 50000

Results are written as CSV to ``bench/results/bench-YYYYMMDD-HHMMSS.csv``
and as ``bench/results/latest.csv``. The Streamlit page
``pages/14_Benchmarks.py`` renders the latest run.

All benchmarks use synthetic data so the suite is hermetic — no live DB,
no IPS Freight network calls, no Azure SQL. Each timing is the median of
``--repeats`` runs (default 3) after one warm-up.
"""
from __future__ import annotations
import argparse, csv, os, platform, sys, time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))


# --- synthetic data ---------------------------------------------------------
def synth_eoq(n: int, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "part_id":   [f"P{i:06d}" for i in range(n)],
        "demand":    rng.poisson(lam=rng.gamma(2.0, 30.0, size=n)),
        "periods":   rng.integers(6, 24, size=n),
        "on_hand":   rng.integers(0, 500, size=n),
        "open_qty":  rng.integers(0, 200, size=n),
        "unit_cost": rng.gamma(2.0, 25.0, size=n),
        "commodity": rng.choice([f"C{i:02d}" for i in range(20)], size=n),
    })


def synth_otd(n: int, seed: int = 1) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "supplier_key": rng.choice([f"S{i:04d}" for i in range(80)], size=n),
        "part_key":     rng.choice([f"P{i:05d}" for i in range(800)], size=n),
        "promised":     pd.Timestamp("2024-01-01") + pd.to_timedelta(rng.integers(0, 365, n), unit="D"),
        "received":     pd.Timestamp("2024-01-05") + pd.to_timedelta(rng.integers(-3, 30, n), unit="D"),
        "qty":          rng.integers(1, 500, size=n),
        "buyer":        rng.choice(["Buyer-A","Buyer-B","Buyer-C","Buyer-D"], size=n),
        "site":         rng.choice(["MTV","CHA","TUL"], size=n),
    })


def synth_lead_times(n: int, seed: int = 2) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "supplier_key": rng.choice([f"S{i:03d}" for i in range(50)], size=n),
        "lane":         rng.choice(["LAX-MTV","ORD-CHA","ATL-TUL","SEA-MTV"], size=n),
        "duration_days":rng.gamma(2.5, 6.0, size=n).clip(1, 90),
        "event":        rng.choice([0, 1], size=n, p=[0.05, 0.95]),
    })


def synth_demand_orders(periods: int, seed: int = 3) -> tuple[pd.Series, pd.Series]:
    rng = np.random.default_rng(seed)
    demand = pd.Series(rng.normal(100, 20, periods)).clip(lower=0)
    # bullwhipped: amplify variance per echelon
    orders = demand + rng.normal(0, 35, periods)
    return demand, orders


def synth_ips_shipments(n: int, seed: int = 4) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "supplier_key": rng.choice([f"S{i:03d}" for i in range(40)], size=n),
        "mode":         rng.choice(["truck_ftl","truck_ltl","rail","ocean","air"], size=n,
                                   p=[0.55, 0.20, 0.10, 0.10, 0.05]),
        "distance_km":  rng.gamma(2.0, 400.0, size=n),
        "payload_t":    rng.gamma(2.0, 5.0, size=n),
    })


def synth_lanes(n_lanes: int, periods: int, seed: int = 5) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    lanes = [f"L{i:03d}" for i in range(n_lanes)]
    rows = []
    for L in lanes:
        cv_base = rng.uniform(0.1, 1.5)
        for p in range(periods):
            rows.append({"lane_id": L, "period": p,
                         "load_count": max(0, rng.normal(50, 50 * cv_base))})
    return pd.DataFrame(rows)


def synth_suppliers(n: int, seed: int = 6) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "supplier_key":     [f"S{i:03d}" for i in range(n)],
        "unit_cost":        rng.gamma(2.0, 12.0, size=n),
        "lead_time_mean":   rng.gamma(2.0, 7.0, size=n),
        "lead_time_std":    rng.gamma(2.0, 2.0, size=n),
        "disruption_prob":  rng.beta(1.5, 30, size=n),
        "annual_demand":    rng.gamma(3.0, 1500.0, size=n),
    })


def synth_stages(n: int, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "stage_id":  [f"ST{i:02d}" for i in range(n)],
        "T_i":       rng.gamma(2.0, 3.0, size=n),
        "S_i":       rng.gamma(1.5, 1.0, size=n),
        "SI_i":      rng.gamma(2.0, 4.0, size=n),
        "sigma_i":   rng.gamma(2.0, 5.0, size=n),
        "unit_cost": rng.gamma(2.0, 10.0, size=n),
    })



def synth_value_stream(n: int, seed: int=42):
    rng = np.random.default_rng(seed)
    parts = pd.DataFrame({
        'part_key': [f'P{i:05d}' for i in range(n)],
        'part_type': rng.choice(['Make', 'Buy', 'Phantom'], size=n),
        'business_unit_key': rng.choice(['PLANT01', 'PLANT02'], size=n)
    })
    pn = n//2
    po = pd.DataFrame({
        'po_number': [f'PO{i:05d}' for i in range(pn)],
        'part_key': rng.choice(parts['part_key'], size=pn),
        'supplier_key': rng.choice([f'S{i:03d}' for i in range(50)], size=pn),
        'due_date_key': pd.Timestamp('2024-01-01') + pd.to_timedelta(rng.integers(0, 365, pn), unit='D')
    })
    wo = pd.DataFrame({
        'wo_number': [f'WO{i:05d}' for i in range(pn)],
        'part_key': rng.choice(parts['part_key'], size=pn),
        'due_date_key': pd.Timestamp('2024-01-01') + pd.to_timedelta(rng.integers(0, 365, pn), unit='D')
    })
    so = pd.DataFrame({
        'sales_order_number': [f'SO{i:05d}' for i in range(pn)],
        'part_key': rng.choice(parts['part_key'], size=pn),
        'customer_key': rng.choice([f'C{i:03d}' for i in range(50)], size=pn),
        'promised_ship_day_key': pd.Timestamp('2024-01-01') + pd.to_timedelta(rng.integers(0, 365, pn), unit='D')
    })
    return parts, po, wo, so

# --- bench harness ----------------------------------------------------------
def time_it(fn, repeats: int = 3) -> float:
    fn()  # warm-up
    samples = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t0)
    return median(samples)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=20_000)
    ap.add_argument("--repeats", type=int, default=3)
    args = ap.parse_args()
    N = args.rows; R = args.repeats

    # late imports so the smoke-import benchmark above is independent
    from brain import eoq as eoq_mod
    from brain.eoq import EOQInputs
    from brain.cleaning import standard_clean
    from brain.imputation import missingness_profile, mass_impute
    from brain.research.hierarchical_eoq import shrink_rates
    from brain.research.bullwhip import bullwhip_ratio
    from brain.research.lead_time_survival import km_lead_time, per_group_lead_time
    from brain.research.sustainability import shipment_emissions, supplier_sustainability_score
    from brain.research.freight_portfolio import lane_volatility, portfolio_mix
    from brain.research.risk_design import supplier_cost_scenarios
    from brain.research.multi_echelon import safety_stock_per_stage
    from brain.graph_backend import NetworkXBackend
    from brain.findings_index import record_findings_bulk, lookup_findings, clear

    rows = []
    py = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    plat = platform.platform()
    when = datetime.now(timezone.utc).isoformat(timespec="seconds")

    def bench(name: str, scenario: str, fn):
        secs = time_it(fn, repeats=R)
        rows.append({"benchmark": name, "scenario": scenario,
                     "elapsed_s": round(secs, 4),
                     "rows_per_s": round(N / secs, 1) if secs > 0 else None,
                     "n_rows": N, "repeats": R, "python": py, "platform": plat, "ts": when})
        print(f"  {name:<40} {scenario:<14} {secs*1000:>9.1f} ms")

    # ----- prep data
    print(f"Bench rows={N} repeats={R}")
    df_eoq = synth_eoq(N)
    inp = EOQInputs(part_id_col="part_id", demand_col="demand", periods_col="periods",
                    on_hand_col="on_hand", open_qty_col="open_qty", unit_cost_col="unit_cost")
    df_otd = synth_otd(N)
    df_lt = synth_lead_times(min(N, 20_000))
    demand, orders = synth_demand_orders(min(N // 10, 5000))
    df_ips = synth_ips_shipments(min(N, 20_000))
    df_lanes = synth_lanes(n_lanes=min(N // 200, 100), periods=24)
    df_sup = synth_suppliers(min(50, N // 200 + 5))
    df_stg = synth_stages(min(20, N // 500 + 5))

    # ----- core
    bench("eoq.deviation_table",       "synth", lambda: eoq_mod.deviation_table(df_eoq, inp))
    df_eoq_dev = eoq_mod.deviation_table(df_eoq, inp)
    df_eoq_dev["obs_count"] = df_eoq["periods"].values
    df_eoq_dev["commodity"] = df_eoq["commodity"].values
    bench("hierarchical_eoq.shrink_rates", "synth", lambda: shrink_rates(df_eoq_dev))
    bench("cleaning.standard_clean",   "synth", lambda: standard_clean(df_otd))
    bench("imputation.missingness",    "synth", lambda: missingness_profile(df_eoq))
    df_with_nans = df_eoq.copy()
    mask = np.random.default_rng(0).random(df_with_nans.shape) < 0.10
    df_with_nans = df_with_nans.mask(mask)
    bench("imputation.mass_impute",    "synth-10pct-nan", lambda: mass_impute(df_with_nans))

    # ----- research
    bench("bullwhip.ratio",            "synth", lambda: bullwhip_ratio(demand, orders))
    bench("lead_time.km",              "synth", lambda: km_lead_time(df_lt["duration_days"], df_lt["event"]))
    bench("lead_time.per_group_km",    "supplier+lane",
          lambda: per_group_lead_time(df_lt, ["supplier_key","lane"], "duration_days", "event"))
    bench("sustainability.emissions",  "synth", lambda: shipment_emissions(df_ips))
    df_emis = shipment_emissions(df_ips)
    bench("sustainability.supplier_score", "synth",
          lambda: supplier_sustainability_score(df_emis))
    bench("freight.lane_volatility",   "synth", lambda: lane_volatility(df_lanes))
    vol = lane_volatility(df_lanes)
    bench("freight.portfolio_mix",     "synth", lambda: portfolio_mix(vol))
    bench("risk.cvar_pareto",          "n_sims=2000",
          lambda: supplier_cost_scenarios(df_sup, n_sims=2000))
    bench("multi_echelon.safety_stock","synth", lambda: safety_stock_per_stage(df_stg))

    # ----- graph
    g = NetworkXBackend()
    rng = np.random.default_rng(11)
    n_nodes = min(N // 20, 5000)
    for i in range(n_nodes):
        g.add_node(f"P{i}", "Part")
    for _ in range(n_nodes * 2):
        a, b = rng.integers(0, n_nodes, size=2)
        if a != b:
            g.add_edge(f"P{a}", f"P{b}", "rel")
    
    parts, po, wo, so = synth_value_stream(n_nodes)
    def bench_vs_graph():
        from brain.graph_context import GraphContext
        g2 = GraphContext()
        g2.add_parts(parts, id_col="part_key", label_col="part_key")
        g2.add_edges(po, "buy_order", "po_number", "part", "part_key", "procured")
        g2.add_edges(wo, "make_order", "wo_number", "part", "part_key", "manufactured")
        g2.add_edges(so, "part", "part_key", "sell_order", "sales_order_number", "sold")
        return g2

    bench("graph.value_stream_build", f"nodes={n_nodes}", bench_vs_graph)
    bench("graph.degree_centrality",   f"nodes={n_nodes}",
          lambda: g.centrality(kind="degree"))
    bench("graph.eigenvector",         f"nodes={n_nodes}",
          lambda: g.centrality(kind="eigenvector"))

    # ----- findings_index round-trip
    clear("bench")
    items = [{"key": f"K{i}", "score": float(i), "page": "bench"} for i in range(min(2000, N))]
    bench("findings_index.bulk_write", f"n={len(items)}",
          lambda: record_findings_bulk("bench", "bench", items))
    bench("findings_index.lookup",     "synth", lambda: lookup_findings("bench"))
    clear("bench")

    # ----- write results
    out_dir = ROOT / "bench" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    fpath = out_dir / f"bench-{stamp}.csv"
    fields = list(rows[0].keys())
    with fpath.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)
    latest = out_dir / "latest.csv"
    with latest.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)
    print(f"\nWrote {fpath}\nWrote {latest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
