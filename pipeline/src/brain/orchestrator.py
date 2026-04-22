"""
Brain Orchestrator — turns a Mission into a fully-realized result.

Given a Mission's parsed intent, the orchestrator dispatches to the existing
brain analyzer modules (eoq, otd_recursive, data_quality, ...), collects
their findings under a single `mission_id`, scores progress vs the mission's
first run, and hands the whole bundle back to the deck builders.

Every analyzer is wrapped in `_safe_run` so a single failure (DB outage,
missing logical table, sklearn edge case) cannot abort the whole mission.
The orchestrator never mutates the underlying analyzer modules.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Callable
import json
import logging
import time
import traceback

from . import load_config
from .findings_index import record_findings_bulk, lookup_findings
from .quests import Mission
from . import mission_store


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result objects
# ---------------------------------------------------------------------------
@dataclass
class AnalyzerOutcome:
    scope_tag: str
    analyzer: str
    ok: bool
    elapsed_ms: int
    n_findings: int = 0
    metrics: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass
class MissionResult:
    mission_id: str
    site: str
    scope_tags: list[str]
    outcomes: list[AnalyzerOutcome]
    findings: list[dict]              # finding rows tagged with mission_id
    kpi_snapshot: dict[str, Any]      # kpis_to_move → current numeric value
    progress_pct: float
    elapsed_ms: int
    refresh: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "mission_id": self.mission_id,
            "site": self.site,
            "scope_tags": self.scope_tags,
            "outcomes": [asdict(o) for o in self.outcomes],
            "findings_count": len(self.findings),
            "kpi_snapshot": self.kpi_snapshot,
            "progress_pct": self.progress_pct,
            "elapsed_ms": self.elapsed_ms,
            "refresh": self.refresh,
        }


# ---------------------------------------------------------------------------
# Analyzer adapters — each returns AnalyzerOutcome with a small, JSON-safe
# metrics dict + writes findings rows tagged with mission_id.
# ---------------------------------------------------------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_run(scope_tag: str, name: str,
              fn: Callable[[Mission], dict[str, Any]]) -> AnalyzerOutcome:
    t0 = _now_ms()
    try:
        metrics = fn(_CURRENT_MISSION) or {}
        return AnalyzerOutcome(
            scope_tag=scope_tag, analyzer=name, ok=True,
            elapsed_ms=_now_ms() - t0,
            n_findings=int(metrics.pop("_n_findings", 0)),
            metrics=metrics,
        )
    except Exception as e:
        log.warning("Orchestrator analyzer %s failed: %s", name, e)
        return AnalyzerOutcome(
            scope_tag=scope_tag, analyzer=name, ok=False,
            elapsed_ms=_now_ms() - t0,
            error=f"{type(e).__name__}: {e}",
        )


# Shared between adapter calls inside one orchestrator.run() — set/cleared
# only inside run() to keep adapters simple and avoid threading a context
# argument through every analyzer.
_CURRENT_MISSION: Mission | None = None


def _site_where(mission: Mission, site_col: str = "business_unit_id") -> str | None:
    s = (mission.site or "").strip()
    if not s or s.upper() == "ALL":
        return None
    safe = s.replace("'", "''")
    return f"[{site_col}] = '{safe}'"


def _record(mission_id: str, page: str, kind: str,
            items: list[dict]) -> int:
    """Tag findings with mission_id and persist them."""
    enriched = []
    for it in items:
        payload = dict(it.get("payload") or {})
        payload["mission_id"] = mission_id
        enriched.append({"key": it["key"], "score": it.get("score"), "payload": payload})
    return record_findings_bulk(page, kind, enriched)


# --- inventory_sizing -------------------------------------------------------
def _adapter_inventory_sizing(mission: Mission) -> dict[str, Any]:
    from .data_access import fetch_logical
    from .eoq import deviation_table, EOQInputs

    cfg = load_config()
    parts_qual = cfg.get("tables", {}).get("parts")
    on_hand_qual = cfg.get("tables", {}).get("on_hand")
    if not parts_qual or not on_hand_qual:
        raise RuntimeError("eoq tables not mapped in brain.yaml")

    where = _site_where(mission)
    on_hand = fetch_logical("azure_sql", "on_hand", top=20000, where=where)
    if on_hand.empty:
        return {"_n_findings": 0, "rows_scanned": 0,
                "note": "no on-hand rows for site"}

    # The deviation_table function needs a synthetic frame with the columns it
    # expects. We'll pull what we can from on_hand and accept missing-column
    # behavior gracefully — column names follow the col_resolver patterns.
    cols = {c.lower(): c for c in on_hand.columns}
    pid = next((cols[k] for k in cols if k in ("part_key", "part_number",
                                                "item_number", "item_id")), None)
    qoh = next((cols[k] for k in cols if k in ("quantity_on_hand", "on_hand_qty",
                                                "on_hand", "qoh")), None)
    if not pid or not qoh:
        return {"_n_findings": 0, "rows_scanned": len(on_hand),
                "note": "missing part/qoh columns"}

    # Synthesize required EOQ columns when unavailable so the function runs.
    df = on_hand[[pid, qoh]].copy()
    df.columns = ["part_id", "qty_on_hand"]
    df["demand_obs"] = df["qty_on_hand"].fillna(0).astype(float)
    df["periods"] = 1.0
    df["open_qty"] = 0.0
    df["unit_cost"] = 1.0

    inp = EOQInputs(
        part_id_col="part_id", demand_col="demand_obs", periods_col="periods",
        on_hand_col="qty_on_hand", open_qty_col="open_qty",
        unit_cost_col="unit_cost", periods_per_year=12.0,
    )
    dev = deviation_table(df, inp)
    top = dev.head(50)

    overstock_dollars = float(dev["dollar_at_risk"].sum() or 0)
    items = [{"key": str(r["part_id"]), "score": float(r["abs_dev_z"] or 0),
              "payload": {"dev_z": float(r["dev_z"] or 0),
                          "overstock_units": float(r["overstock_units"] or 0),
                          "understock_units": float(r["understock_units"] or 0),
                          "dollar_at_risk": float(r["dollar_at_risk"] or 0)}}
             for _, r in top.iterrows()]
    n = _record(mission.id, page="QuestConsole", kind="eoq_deviation", items=items)

    return {
        "_n_findings": n,
        "rows_scanned": int(len(dev)),
        "dollars_at_risk": round(overstock_dollars, 2),
        "median_abs_dev_z": float(dev["abs_dev_z"].median() or 0),
    }


# --- fulfillment ------------------------------------------------------------
def _adapter_fulfillment(mission: Mission) -> dict[str, Any]:
    from .otd_recursive import run_otd_from_replica

    where = _site_where(mission, site_col="business_unit_id")
    work, summaries = run_otd_from_replica(connector="azure_sql",
                                           where=where, limit=5000)
    if work is None or work.empty:
        return {"_n_findings": 0, "rows_scanned": 0,
                "note": "no OTD source rows"}

    on_time = float(work.get("is_on_time").mean()) if "is_on_time" in work.columns else None
    days_late = work.get("days_late")
    p90 = float(days_late.quantile(0.90)) if days_late is not None and len(days_late) else None
    median = float(days_late.median()) if days_late is not None and len(days_late) else None

    items = []
    for s in summaries[:50]:
        items.append({"key": str(s.get("path") or s.get("cluster_id") or ""),
                      "score": float(s.get("size") or 0),
                      "payload": s})
    n = _record(mission.id, page="QuestConsole", kind="otd_cluster", items=items)

    return {
        "_n_findings": n,
        "rows_scanned": int(len(work)),
        "otd_pct": round((on_time or 0) * 100, 2) if on_time is not None else None,
        "days_late_median": median,
        "days_late_p90": p90,
        "n_clusters": len(summaries),
    }


# --- sourcing ---------------------------------------------------------------
def _adapter_sourcing(mission: Mission) -> dict[str, Any]:
    from .data_access import fetch_logical

    where = _site_where(mission)
    po = fetch_logical("azure_sql", "po_receipts", top=10000, where=where)
    if po.empty:
        return {"_n_findings": 0, "rows_scanned": 0, "note": "no po_receipt rows"}

    cols = {c.lower(): c for c in po.columns}
    sup = next((cols[k] for k in cols if "supplier" in k or "vendor" in k), None)
    if not sup:
        return {"_n_findings": 0, "rows_scanned": len(po), "note": "no supplier column"}

    grp = po.groupby(po[sup].astype(str)).size().sort_values(ascending=False).head(50)
    items = [{"key": k, "score": float(v),
              "payload": {"po_lines": int(v)}} for k, v in grp.items()]
    n = _record(mission.id, page="QuestConsole", kind="supplier_volume", items=items)
    return {
        "_n_findings": n,
        "rows_scanned": int(len(po)),
        "unique_suppliers": int(po[sup].nunique()),
        "top_supplier_share_pct": round(float(grp.iloc[0]) / max(1, int(po[sup].count())) * 100, 2),
    }


# --- data_quality -----------------------------------------------------------
def _adapter_data_quality(mission: Mission) -> dict[str, Any]:
    from .data_access import fetch_logical

    parts = fetch_logical("azure_sql", "parts", top=20000,
                           where=_site_where(mission))
    if parts.empty:
        return {"_n_findings": 0, "rows_scanned": 0, "note": "no parts rows"}

    miss = parts.isna().mean().sort_values(ascending=False)
    miss = miss[miss > 0].head(20)
    items = [{"key": col, "score": float(rate),
              "payload": {"missing_pct": round(float(rate) * 100, 2)}}
             for col, rate in miss.items()]
    n = _record(mission.id, page="QuestConsole", kind="data_gap", items=items)
    return {
        "_n_findings": n,
        "rows_scanned": int(len(parts)),
        "max_missing_pct": round(float(miss.iloc[0]) * 100, 2) if not miss.empty else 0,
        "fields_with_gaps": int((miss > 0).sum()),
    }


# --- lead_time --------------------------------------------------------------
def _adapter_lead_time(mission: Mission) -> dict[str, Any]:
    from .data_access import fetch_logical
    import numpy as np
    import pandas as pd

    po = fetch_logical("azure_sql", "po_receipts", top=10000,
                       where=_site_where(mission))
    if po.empty:
        return {"_n_findings": 0, "rows_scanned": 0, "note": "no po_receipt rows"}

    cols = {c.lower(): c for c in po.columns}
    order_col = next((cols[k] for k in cols if "order_date" in k or "po_date" in k), None)
    receipt_col = next((cols[k] for k in cols if "receipt_date" in k or "received_date" in k), None)
    if not order_col or not receipt_col:
        return {"_n_findings": 0, "rows_scanned": len(po), "note": "no order/receipt date pair"}

    od = pd.to_datetime(po[order_col], errors="coerce")
    rd = pd.to_datetime(po[receipt_col], errors="coerce")
    lt = (rd - od).dt.days.dropna()
    lt = lt[(lt >= 0) & (lt < 365)]
    if lt.empty:
        return {"_n_findings": 0, "rows_scanned": len(po), "note": "no valid lead times"}

    return {
        "_n_findings": 0,
        "rows_scanned": int(len(po)),
        "lead_time_median": float(np.median(lt)),
        "lead_time_p90": float(np.quantile(lt, 0.90)),
        "lead_time_n": int(len(lt)),
    }


# --- demand_distortion ------------------------------------------------------
def _adapter_demand_distortion(mission: Mission) -> dict[str, Any]:
    from .data_access import fetch_logical
    import pandas as pd

    sales = fetch_logical("azure_sql", "sales_order_lines", top=20000,
                          where=_site_where(mission))
    if sales.empty:
        return {"_n_findings": 0, "rows_scanned": 0, "note": "no sales rows"}

    cols = {c.lower(): c for c in sales.columns}
    qty = next((cols[k] for k in cols if k in ("shipped_quantity", "sales_order_quantity",
                                                "sales_qty", "shipped_qty")), None)
    date = next((cols[k] for k in cols if "order_date" in k or "ship_date" in k), None)
    if not qty or not date:
        return {"_n_findings": 0, "rows_scanned": len(sales), "note": "no qty/date columns"}

    s = sales[[date, qty]].copy()
    s[date] = pd.to_datetime(s[date], errors="coerce")
    s = s.dropna()
    if s.empty:
        return {"_n_findings": 0, "rows_scanned": len(sales), "note": "no parseable dates"}

    weekly = s.groupby(s[date].dt.to_period("W"))[qty].sum()
    if len(weekly) < 4 or float(weekly.mean() or 0) == 0:
        return {"_n_findings": 0, "rows_scanned": len(sales), "note": "insufficient series"}

    cov = float(weekly.std() / weekly.mean())
    return {
        "_n_findings": 0,
        "rows_scanned": int(len(sales)),
        "weekly_cov": round(cov, 3),
        "weekly_periods": int(len(weekly)),
    }


# --- network_position -------------------------------------------------------
def _adapter_network_position(mission: Mission) -> dict[str, Any]:
    from .data_access import fetch_logical

    on_hand = fetch_logical("azure_sql", "on_hand", top=20000,
                            where=_site_where(mission))
    if on_hand.empty:
        return {"_n_findings": 0, "rows_scanned": 0, "note": "no on-hand"}

    cols = {c.lower(): c for c in on_hand.columns}
    site_col = next((cols[k] for k in cols if k in ("site", "plant", "facility",
                                                     "business_unit_id", "org_id")), None)
    qty_col = next((cols[k] for k in cols if k in ("quantity_on_hand", "on_hand_qty",
                                                    "on_hand", "qoh")), None)
    if not site_col or not qty_col:
        return {"_n_findings": 0, "rows_scanned": len(on_hand), "note": "missing columns"}

    by_site = on_hand.groupby(on_hand[site_col].astype(str))[qty_col].sum().sort_values(ascending=False).head(20)
    items = [{"key": k, "score": float(v),
              "payload": {"on_hand_qty": float(v)}} for k, v in by_site.items()]
    n = _record(mission.id, page="QuestConsole", kind="network_node", items=items)
    return {
        "_n_findings": n,
        "rows_scanned": int(len(on_hand)),
        "n_nodes": int(by_site.shape[0]),
        "concentration_pct_top1": round(float(by_site.iloc[0]) / max(1, float(by_site.sum())) * 100, 2),
    }


# --- cycle_count ------------------------------------------------------------
def _adapter_cycle_count(mission: Mission) -> dict[str, Any]:
    from .data_access import fetch_logical
    parts = fetch_logical("azure_sql", "parts", top=20000,
                          where=_site_where(mission))
    if parts.empty:
        return {"_n_findings": 0, "rows_scanned": 0, "note": "no parts"}
    cols = {c.lower(): c for c in parts.columns}
    abc_col = next((cols[k] for k in cols if "abc" in k), None)
    if not abc_col:
        return {"_n_findings": 0, "rows_scanned": len(parts), "note": "no ABC column"}
    counts = parts[abc_col].astype(str).value_counts().head(10)
    items = [{"key": k, "score": float(v),
              "payload": {"part_count": int(v)}} for k, v in counts.items()]
    n = _record(mission.id, page="QuestConsole", kind="abc_class", items=items)
    return {
        "_n_findings": n,
        "rows_scanned": int(len(parts)),
        "n_classes": int(parts[abc_col].nunique()),
    }


# ---------------------------------------------------------------------------
# Module registry — declarative scope_tag → analyzer binding
# ---------------------------------------------------------------------------
MODULE_REGISTRY: dict[str, list[tuple[str, Callable[[Mission], dict]]]] = {
    "inventory_sizing":   [("eoq.deviation_table", _adapter_inventory_sizing)],
    "fulfillment":        [("otd_recursive.run_otd_from_replica", _adapter_fulfillment)],
    "sourcing":           [("po_receipts.supplier_volume", _adapter_sourcing)],
    "data_quality":       [("parts.missingness", _adapter_data_quality)],
    "lead_time":          [("po_receipts.lead_time", _adapter_lead_time)],
    "demand_distortion":  [("sales.weekly_cov", _adapter_demand_distortion)],
    "network_position":   [("on_hand.by_site", _adapter_network_position)],
    "cycle_count":        [("parts.abc_class", _adapter_cycle_count)],
}


# ---------------------------------------------------------------------------
# Progress estimation — diff current KPIs vs the mission's first run
# ---------------------------------------------------------------------------
def _kpi_snapshot_from_outcomes(outcomes: list[AnalyzerOutcome]) -> dict[str, Any]:
    snap: dict[str, Any] = {}
    for o in outcomes:
        if not o.ok:
            continue
        for k, v in o.metrics.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                snap[f"{o.scope_tag}.{k}"] = round(float(v), 4)
    return snap


def _progress_pct(current: dict[str, float], baseline: dict[str, float]) -> float:
    """Crude but useful: % of tracked KPIs that moved in a beneficial direction.

    Beneficial direction is encoded by KPI name suffix:
      * higher_is_better — anything containing 'pct', 'accuracy'
      * lower_is_better  — anything containing 'late', 'cov', 'missing',
                           'dollars_at_risk', 'concentration', 'p90'
    Unknown direction is ignored.
    """
    if not baseline:
        return 0.0
    moved = 0
    counted = 0
    for k, cur in current.items():
        base = baseline.get(k)
        if base is None or not isinstance(base, (int, float)):
            continue
        lk = k.lower()
        if any(w in lk for w in ("late", "cov", "missing", "dollars_at_risk",
                                  "concentration", "p90", "median")):
            direction = -1
        elif any(w in lk for w in ("pct", "accuracy")):
            direction = +1
        else:
            continue
        counted += 1
        if direction == +1 and cur > base:
            moved += 1
        elif direction == -1 and cur < base:
            moved += 1
    return round(100.0 * moved / counted, 1) if counted else 0.0


def _baseline_for_mission(mission_id: str) -> dict[str, float]:
    """Read the earliest stored kpi_snapshot from mission_events."""
    from . import mission_store as ms
    events = ms.list_events(mission_id, limit=200)
    earliest = None
    for ev in reversed(events):  # list_events is DESC
        if ev["kind"] == "kpi_snapshot":
            earliest = ev["payload"].get("snapshot") or {}
            break
    return earliest or {}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
class BrainOrchestrator:
    """Thin object so callers can swap in a mock for tests."""

    def __init__(self, registry: dict | None = None):
        self.registry = registry or MODULE_REGISTRY

    def run(self, mission: Mission, *, refresh: bool = False) -> MissionResult:
        global _CURRENT_MISSION
        _CURRENT_MISSION = mission
        mission_store.update_status(mission.id, "running")
        t0 = _now_ms()
        try:
            outcomes: list[AnalyzerOutcome] = []
            for tag in mission.scope_tags:
                for name, fn in self.registry.get(tag, []):
                    outcomes.append(_safe_run(tag, name, fn))
            elapsed = _now_ms() - t0

            snapshot = _kpi_snapshot_from_outcomes(outcomes)
            baseline = _baseline_for_mission(mission.id) if refresh else {}
            progress = _progress_pct(snapshot, baseline) if refresh else 0.0

            # Persist the kpi snapshot so the next refresh can compute progress.
            mission_store.record_event(mission.id, "kpi_snapshot",
                                       {"snapshot": snapshot, "refresh": refresh})

            mission_store.update_progress(mission.id, progress, note=("refresh" if refresh else "initial"))
            if refresh:
                mission_store.mark_refreshed(mission.id)

            # Pull the findings we just wrote so the deck builders have them.
            kinds = {"eoq_deviation", "otd_cluster", "supplier_volume",
                     "data_gap", "network_node", "abc_class"}
            findings: list[dict] = []
            for kind in kinds:
                for f in lookup_findings(kind, limit=200):
                    if (f.get("payload") or {}).get("mission_id") == mission.id:
                        findings.append(f)
        finally:
            _CURRENT_MISSION = None

        return MissionResult(
            mission_id=mission.id,
            site=mission.site,
            scope_tags=mission.scope_tags,
            outcomes=outcomes,
            findings=findings,
            kpi_snapshot=snapshot,
            progress_pct=progress,
            elapsed_ms=elapsed,
            refresh=refresh,
        )


def run_mission(mission: Mission, *, refresh: bool = False) -> MissionResult:
    return BrainOrchestrator().run(mission, refresh=refresh)


__all__ = [
    "BrainOrchestrator", "run_mission", "MissionResult",
    "AnalyzerOutcome", "MODULE_REGISTRY",
]
