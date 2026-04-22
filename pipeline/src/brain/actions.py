"""Actionable insight engine.

Translates raw analytical telemetry into plain-language recommendations with
priority and rough $ value-return estimates. Used by the Supply Chain Pipeline
overview and by the Expert To Do list on the Overview page.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import List
import pandas as pd


@dataclass
class Action:
    stage: str
    title: str
    severity: str          # "🟢 OK" | "🟡 Watch" | "🔴 Act"
    why_it_matters: str    # plain language for laypeople
    do_this: str           # specific operational action
    owner_role: str        # who should act
    value_per_year: float  # rough $ benefit estimate
    confidence: float      # 0..1


# ---------------------------------------------------------------------------
# Per-stage rule packs – tuned for layperson actionability.
# ---------------------------------------------------------------------------

def _sev(friction: float) -> str:
    if friction >= 0.5:
        return "🔴 Act Now"
    if friction >= 0.2:
        return "🟡 Watch"
    return "🟢 OK"


# _VIEW_BIAS tilts the Brain's framing based on the selected View Mode lens
_VIEW_BIAS = {
    # bias_value, bias_friction
    "Workstream":     (1.00, 1.00),
    "Friction Heat":  (0.90, 1.25),  # highlight pain, deflate $ a bit
    "Cost Flow":      (1.30, 0.90),  # amplify $ value
    "Risk Surface":   (0.85, 1.35),  # amplify friction sharply
}


def actions_for_pipeline(tel: dict, window_days: int = 365, view_mode: str = "Workstream") -> List[Action]:
    """Generate actionable recommendations from the telemetry dict produced
    in `pages/1b_Supply_Chain_Pipeline.py`.

    `window_days` gracefully scales annualized value into the decision window using
    a continuous power-curve mapped algorithm so shorter horizons surface 
    smaller (but more proportionately urgent) $ figures.
    `view_mode` tilts the Brain framing so users see Cost-leaning or
    Risk-leaning recommendations as they switch lenses.
    """
    out: list[Action] = []
    
    # Mathematical scaling curve matching the previous static horizon drops:
    # 365d ~ 1.20x | 180d ~ 0.95x | 90d ~ 0.75x | 30d ~ 0.52x | 7d ~ 0.32x
    h_scale = (max(window_days, 1) / 365.0) ** (1/3) * 1.20
    h_scale = min(1.60, max(0.15, h_scale)) # Bound it to sane limits
    
    v_value, v_fric = _VIEW_BIAS.get(view_mode, (1.0, 1.0))

    def add(stage, kpi, title, why, do, owner, v):
        k = tel.get(kpi)
        if not k:
            return
        f_eff = float(min(1.0, k["friction"] * v_fric))
        out.append(Action(
            stage=stage, title=title, severity=_sev(f_eff),
            why_it_matters=why, do_this=do, owner_role=owner,
            value_per_year=float(v) * (0.5 + f_eff) * h_scale * v_value,
            confidence=round(0.6 + 0.4 * (1 - f_eff), 2),
        ))

    add("Demand", "demand_var",
        "Demand variance is wide — forecast risk is elevated",
        "Wider demand swings drive safety-stock up and tie cash in inventory.",
        "Re-segment SKUs by volatility; move top-quartile to weekly review.",
        "S&OP Lead", 250_000)

    add("Forecast", "bullwhip",
        "Bullwhip is amplifying signal between echelons",
        "Order batching is making the plant chase phantom demand.",
        "Smooth ROP triggers; cap weekly PO batches by category.",
        "Planning Manager", 400_000)

    add("EOQ", "eoq_dev",
        "EOQ deviation above 10% on the top movers",
        "We're either ordering too often (freight $) or too much (carrying $).",
        "Re-tune EOQ for top 200 SKUs against current carrying & ordering cost.",
        "Procurement Analyst", 320_000)

    add("Procurement", "supplier_score",
        "Supplier reliability gap on key vendors",
        "Late or short shipments compound downstream into missed customer orders.",
        "Run Vendor Reliability 360; consolidate bottom-decile suppliers.",
        "Strategic Sourcing", 600_000)

    add("Lead Time", "lead_time",
        "Median lead time has crept above plan",
        "Higher LT inflates safety stock everywhere downstream.",
        "Renegotiate top 10 lanes; add second-source on parts >45d LT.",
        "Sourcing Lead", 450_000)

    add("Data Quality", "data_quality",
        "PFEP completeness below 90%",
        "Missing UoM/pack/dock-door silently breaks EOQ and cubing math.",
        "Run the Data Quality VOI heatmap; fill the top 50 highest-value gaps.",
        "PFEP Owner", 180_000)

    add("Inventory", "echelon_balance",
        "Network safety stock is mis-positioned",
        "Stock is sitting where demand isn't — service level looks OK but cash is trapped.",
        "Re-place safety stock per Multi-Echelon recommendation.",
        "Inventory Manager", 700_000)

    add("Mfg WO", "wip_health",
        "WIP velocity below target",
        "Slower WIP = late deliveries even if PO health looks fine.",
        "Walk top-3 bottleneck cells; rebalance staffing for one shift.",
        "Plant Manager", 220_000)

    add("OTD", "otd_pct",
        "On-time delivery trailing customer expectation",
        "Each percentage point typically maps to 1.2 pts customer-experience score.",
        "Run OTD Recursive root cause; assign owners to top 5 root nodes.",
        "Customer Service Lead", 380_000)

    add("Freight", "freight_eff",
        "Freight $/lb running above contract baseline",
        "Lane-mix is drifting toward spot — consolidation is being missed.",
        "Open Freight Portfolio → bid the top 5 lanes; consolidate with peers.",
        "Logistics Manager", 280_000)

    add("Customer", "fill_rate",
        "Fill rate trailing peer benchmark",
        "Below-peer fill rate erodes share-of-wallet over rolling quarters.",
        "Compare to peer in Benchmarks page; close the top 3 gaps.",
        "Sales Ops", 500_000)

    add("ESG", "scope3",
        "Scope-3 intensity above corporate target",
        "Mode shift and consolidation pay back in both $ and CO₂.",
        "Pilot rail/intermodal swap on top 2 long-haul lanes.",
        "Sustainability Lead", 120_000)

    # severity rank: red 0 (top) → yellow 1 → green 2; then by $/yr desc
    _sev_rank = {"🔴 Act Now": 0, "🟡 Watch": 1, "🟢 OK": 2}
    out.sort(key=lambda a: (_sev_rank.get(a.severity, 3), -a.value_per_year))
    return out


def actions_to_dataframe(actions: List[Action]) -> pd.DataFrame:
    return pd.DataFrame([asdict(a) for a in actions])
