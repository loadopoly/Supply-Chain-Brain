"""
Streamlit drill-down helpers. Imported only by pages/.

Pattern:
    selected = drilldown_table(df, key="eoq")
    if selected:
        render_drilldown_panel(selected)
"""
from __future__ import annotations
from typing import Iterable
import pandas as pd
import streamlit as st

from . import load_config
from .findings_index import lookup_findings, record_finding


CITATIONS = {
    "deep_knowledge":  "MIT CTL · Deep Knowledge Lab — https://ctl.mit.edu/research/deep-knowledge-lab-supply-chain-and-logistics",
    "digital_sc":      "MIT CTL · Digital SC Transformation Lab — https://ctl.mit.edu/research/digital-supply-chain-transformation-lab",
    "sc_design":       "MIT CTL · Supply Chain Design Lab — https://ctl.mit.edu/research/supply-chain-design-lab",
    "freightlab":      "MIT CTL · FreightLab — https://ctl.mit.edu/research/freightlab",
    "intelligent":     "MIT CTL · Intelligent Logistics Systems Lab — https://ctl.mit.edu/research/intelligent",
    "sustainable":     "MIT CTL · Sustainable Supply Chain Lab — https://ctl.mit.edu/research/sustainable-supply-chain-lab",
    "cave":            "MIT CTL · CAVE Lab — https://ctl.mit.edu/research/cave",
}


def cite(*lab_keys: str) -> None:
    """Render a small citation footer linking to the originating MIT CTL lab."""
    lines = [CITATIONS[k] for k in lab_keys if k in CITATIONS]
    if lines:
        st.caption("📚 " + " · ".join(lines))


def drilldown_table(df: pd.DataFrame, key: str,
                    column_order: Iterable[str] | None = None,
                    height: int = 420) -> list[dict]:
    """
    Render a Streamlit dataframe with row-selection enabled and return the
    selected rows as a list of dicts. Falls back to a static dataframe on
    older Streamlit (<1.32) where selection is unavailable.
    """
    if df is None or df.empty:
        st.info("No rows.")
        return []
    cols = [c for c in (column_order or df.columns) if c in df.columns]
    try:
        ev = st.dataframe(
            df[cols], key=f"dd_{key}", height=height,
            hide_index=True, width='stretch',
            on_select="rerun", selection_mode="multi-row",
        )
        sel_idx = getattr(getattr(ev, "selection", None), "rows", []) or []
        return df.iloc[sel_idx].to_dict("records") if sel_idx else []
    except TypeError:                                  # older Streamlit
        st.dataframe(df[cols], height=height, hide_index=True, width='stretch')
        st.caption("🔼 Upgrade Streamlit ≥ 1.32 for click-to-drill rows.")
        return []


def render_drilldown_panel(selected: list[dict],
                           id_field: str = "part_id",
                           kind: str = "part") -> None:
    """Show a per-selection card: cross-page findings + jump links."""
    if not selected:
        return
    st.markdown("### 🔎 Drill-down")
    for row in selected:
        key = row.get(id_field)
        if key is None:
            continue
        with st.expander(f"{kind.title()} **{key}**", expanded=True):
            st.json(row, expanded=False)
            findings = lookup_findings(kind=kind, key=str(key), limit=20)
            if findings:
                st.markdown("**Cross-page findings**")
                st.dataframe(
                    pd.DataFrame([{
                        "page": f["page"], "score": f["score"],
                        "when": f["created_at"], **f["payload"],
                    } for f in findings]),
                    hide_index=True, width='stretch',
                )
            else:
                st.caption("No cross-page findings yet for this item.")
            # Allow logging the click as a finding so other pages see it
            if st.button(f"📌 Pin to findings index", key=f"pin_{kind}_{key}"):
                record_finding(page=st.session_state.get("_page", "unknown"),
                               kind=kind, key=str(key),
                               score=row.get("abs_dev_z") or row.get("dollar_at_risk"),
                               payload=row)
                st.toast(f"Pinned {kind} {key}", icon="📌")


def page_header(title: str, subtitle: str = "", *labs: str) -> None:
    st.title(title)
    if subtitle:
        st.caption(subtitle)
    if labs:
        cite(*labs)
    st.markdown("---")
