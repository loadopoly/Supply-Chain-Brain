"""
Synthetic data generator for the deck pipeline.

Lets callers verify the Phase 1-8 wiring without live Oracle / Azure access.
Uses the canonical schemas from schemas.py so the same DataFrames can flow
through findings.build_findings unchanged.

Seed is fixed at 9 per spec §6 so outputs are reproducible.
"""
from __future__ import annotations
from datetime import date, timedelta
import numpy as np
import pandas as pd

from .constants import SEED


SITES = [
    "Chattanooga - Manufacturers Road",
    "Chattanooga - Wilson Road",
    "Burlington",
    "Eugene - Airport Road",
    "Parsons",
]

OTD_REASONS = [
    "WH failed to ship",
    "Missing other item on same SO",
    "Manufactured not ready, behind schedule",
    "No purchased part, supplier late",
    "Customer reschedule",
]


def _parts(n=300) -> list[str]:
    rng = np.random.default_rng(SEED)
    return [f"AST-{rng.integers(10000, 99999)}-{i:04d}" for i in range(n)]


def make_otd(T: date, parts: list[str]) -> pd.DataFrame:
    rng = np.random.default_rng(SEED)
    n = 4000
    ship = [T - timedelta(days=int(rng.integers(1, 45))) for _ in range(n)]
    order = [s - timedelta(days=int(rng.integers(3, 40))) for s in ship]
    promised = [o + timedelta(days=int(rng.integers(5, 30))) for o in order]
    late = rng.random(n) < 0.12
    days_late = np.where(late, rng.integers(1, 45, n), 0)
    return pd.DataFrame({
        "Site":              rng.choice(SITES, n),
        "Order Date":        order,
        "Ship Date":         ship,
        "Promised Date":     promised,
        "Adjusted Promise Date": promised,
        "SO No":             [f"SO{100000 + i}" for i in range(n)],
        "Line No":            rng.integers(1, 10, n),
        "Part":              rng.choice(parts, n),
        "Qty":               rng.integers(1, 50, n),
        "Available Qty":     rng.integers(0, 50, n),
        "On Hand Qty":       rng.integers(0, 500, n),
        "OTD Miss (Late)":   late.astype(int),
        "Days Late":         days_late,
        "Customer":          rng.choice(["Acme", "Globex", "Initech", "Umbrella", "Waystar",
                                        "Stark", "Wayne", "Soylent"], n),
        "Customer No":       rng.integers(1000, 9999, n),
        "Supplier Name":     rng.choice(["Supplier-A", "Supplier-B", "Supplier-C",
                                        "Supplier-D", "Supplier-E", "Supplier-F",
                                        np.nan], n),
        "Part Pur/Fab":      rng.choice(["purchased", "fabricated"], n, p=[0.7, 0.3]),
        "Failure Reason":    np.where(late, rng.choice(OTD_REASONS, n), ""),
    })


def make_ifr(T: date, parts: list[str]) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + 1)
    n = 6000
    order = [T - timedelta(days=int(rng.integers(1, 45))) for _ in range(n)]
    hit = rng.random(n) > 0.20
    on_hand = rng.integers(0, 600, n)
    so_qty = rng.integers(1, 40, n)
    # Available Qty is on_hand minus a random allocation chunk
    avail = np.maximum(0, on_hand - rng.integers(0, 200, n))
    return pd.DataFrame({
        "Site":         rng.choice(SITES, n),
        "Order Date":   order,
        "Part":         rng.choice(parts, n),
        "SO Qty":       so_qty,
        "Available Qty":avail,
        "On Hand Qty":  on_hand,
        "Hit Miss":     hit.astype(int),
        "Part Fab/Pur": rng.choice(["purchased", "fabricated"], n, p=[0.7, 0.3]),
        "Supplier Name": rng.choice(["Supplier-A", "Supplier-B", "Supplier-C",
                                     "Supplier-D", "Supplier-E"], n),
        "Failure":      np.where(~hit, rng.choice(["stockout", "allocation", ""], n,
                                                   p=[0.2, 0.1, 0.7]), ""),
        "Customer Name": rng.choice(["Acme", "Globex", "Initech", "Umbrella", "Waystar"], n),
    })


def make_itr(T: date, parts: list[str]) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + 2)
    n = 1200
    txd = [T - timedelta(days=int(rng.integers(1, 90))) for _ in range(n)]
    # Bias towards weekdays
    txd = [d if d.weekday() < 5 else d - timedelta(days=1) for d in txd]
    return pd.DataFrame({
        "Transaction Date": txd,
        "Transaction Type": rng.choice(
            ["Cycle Count Adjustment", "Cycle Count Adjustment", "Issue", "Receipt"],
            n, p=[0.55, 0.2, 0.15, 0.1]
        ),
        "Item Name":        rng.choice(parts, n),
        "Quantity":         rng.integers(-50, 50, n),
        "Net Dollar":       np.round(rng.normal(0, 500, n), 2),
        "Subinventory":     rng.choice(["MAIN", "STAGE", "KIT", "RCV"], n),
        "Transaction Reason Code": np.where(rng.random(n) < 0.15,
                                            rng.choice(["COUNT_ERR", "SHRINK", "FOUND"], n),
                                            ""),
        "Created By":       "SYSTEM",
        "Last Updated By":  "SYSTEM",
    })


def make_pfep(parts: list[str]) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + 3)
    n = len(parts)
    return pd.DataFrame({
        "Item Name":            parts,
        "Item Status":          rng.choice(["20 - Active", "99 - Inactive"], n, p=[0.9, 0.1]),
        "Make or Buy":          rng.choice(["Buy", "Make"], n, p=[0.7, 0.3]),
        "Supplier":             rng.choice(["Supplier-A", "Supplier-B", "Supplier-C"], n),
        "Buyer Name":           rng.choice(["Buyer-1", "Buyer-2", ""], n, p=[0.45, 0.45, 0.1]),
        "Cost":                 np.round(rng.uniform(5, 500, n), 2),
        "Total Usage":          rng.integers(0, 10000, n),
        "Usage Value":          rng.integers(0, 100000, n),
        "Safety Stock":         np.where(rng.random(n) < 0.6, 0, rng.integers(1, 100, n)),
        "Minimum Quantity":     rng.integers(0, 20, n),
        "Maximum Quantity":     rng.integers(20, 500, n),
        "Processing Lead Time": np.where(rng.random(n) < 0.3, 0, rng.integers(3, 60, n)),
        "ABC Inventory Catalog": rng.choice(["A", "B", "C", None], n, p=[0.15, 0.25, 0.5, 0.1]),
        "Item Cycle Count Enabled": rng.integers(0, 2, n),
        "Inventory Planning Method": rng.choice(["Min-Max", "ROP", "MRP"], n),
        "Safety Stock Planning Method": rng.choice(["None", "Stat", "Manual"], n, p=[0.6, 0.2, 0.2]),
    })


def make_all(T: date | None = None) -> dict[str, pd.DataFrame]:
    T = T or date.today()
    parts = _parts(300)
    return {
        "otd":  make_otd(T, parts),
        "ifr":  make_ifr(T, parts),
        "itr":  make_itr(T, parts),
        "pfep": make_pfep(parts),
    }
