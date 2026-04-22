"""What-if sandbox.

Phase 4 — clone the live findings + a (subset of the) graph into a session
snapshot, mutate (e.g., consolidate two suppliers into one, raise a part's
service level, swap a mode on a lane), recompute the KPI suite, diff against
baseline. Snapshots live under ``pipeline/whatif/`` and are git-ignored.
"""
from __future__ import annotations
import json, os, sqlite3, shutil, time
from dataclasses import dataclass
from pathlib import Path
import pandas as pd

from .findings_index import DB_PATH as _DB_PATH

WHATIF_DIR = Path(__file__).resolve().parents[2] / "whatif"
WHATIF_DIR.mkdir(exist_ok=True)


@dataclass
class Snapshot:
    name: str
    db_path: Path
    metadata: dict


def list_snapshots() -> list[Snapshot]:
    out = []
    for p in sorted(WHATIF_DIR.glob("*.db")):
        meta = WHATIF_DIR / f"{p.stem}.json"
        m = json.loads(meta.read_text()) if meta.exists() else {}
        out.append(Snapshot(name=p.stem, db_path=p, metadata=m))
    return out


def create_snapshot(name: str, mutations: dict | None = None) -> Snapshot:
    safe = "".join(c for c in name if c.isalnum() or c in "_-") or f"snap_{int(time.time())}"
    target = WHATIF_DIR / f"{safe}.db"
    if Path(_DB_PATH).exists():
        shutil.copy2(_DB_PATH, target)
    else:
        sqlite3.connect(target).close()
    meta = {"created": int(time.time()), "mutations": mutations or {}}
    (WHATIF_DIR / f"{safe}.json").write_text(json.dumps(meta, indent=2))
    return Snapshot(name=safe, db_path=target, metadata=meta)


def apply_mutation_to_dataframe(df: pd.DataFrame, mutation: dict) -> pd.DataFrame:
    """Supported mutation kinds:
    - ``consolidate_supplier``: {from: "S1", to: "S2", column: "supplier_key"}
    - ``override_lead_time``: {column: "lead_time_days", value: 14}
    - ``scale_demand``: {column: "demand_hat_annual", factor: 1.10}
    """
    if df.empty:
        return df
    out = df.copy()
    kind = mutation.get("kind")
    if kind == "consolidate_supplier":
        col = mutation.get("column", "supplier_key")
        if col in out.columns:
            out[col] = out[col].replace(mutation["from"], mutation["to"])
    elif kind == "override_lead_time":
        col = mutation.get("column", "lead_time_days")
        if col in out.columns:
            out[col] = float(mutation["value"])
    elif kind == "scale_demand":
        col = mutation.get("column", "demand_hat_annual")
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce") * float(mutation.get("factor", 1.0))
    return out


def diff_kpi(baseline: dict, scenario: dict) -> pd.DataFrame:
    keys = sorted(set(baseline) | set(scenario))
    rows = []
    for k in keys:
        b = baseline.get(k); s = scenario.get(k)
        delta = (s - b) if (isinstance(b, (int, float)) and isinstance(s, (int, float))) else None
        pct = (100 * (s - b) / b) if (delta is not None and b) else None
        rows.append({"kpi": k, "baseline": b, "scenario": s,
                    "delta": delta, "pct_change": pct})
    return pd.DataFrame(rows)
