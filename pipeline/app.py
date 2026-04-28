"""
Supply Chain Brain — app.py
Pure st.navigation() router. All page content lives in pages/*.py.
"""
import sys
import streamlit as st
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

st.set_page_config(
    page_title="Supply Chain Brain",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS: allow Plotly hover tooltips to escape Streamlit's overflow:hidden containers ──
st.markdown("""
<style>
div.block-container,
div[data-testid="stAppViewContainer"],
div[data-testid="stMainBlockContainer"],
div[data-testid="stElementContainer"],
.element-container,
div[data-testid="stVerticalBlock"],
div[data-testid="stVerticalBlockBorderWrapper"],
div[data-testid="stHorizontalBlock"],
div[data-testid="column"],
div[data-testid="stPlotlyChart"],
div[data-testid="stPlotlyChart"] > div {
    overflow: visible !important;
}

div[data-testid="stPlotlyChart"] .js-plotly-plot,
div[data-testid="stPlotlyChart"] .plot-container,
div[data-testid="stPlotlyChart"] .svg-container,
div[data-testid="stPlotlyChart"] .hoverlayer {
    overflow: visible !important;
}

div[data-testid="stPlotlyChart"] .hoverlayer {
    pointer-events: none !important;
}

/* ── Dynamic Brain Insight (DBI) — robust stacking & visibility ───────────── */
/* DBI card sits above all interactive widgets but below modals/popovers.    */
.dbi-container {
    position: relative !important;
    z-index: 950 !important;
    box-shadow: 0 2px 8px rgba(0, 104, 201, 0.12);
}

/* Make the parameter popover button visually anchored to the DBI card and  */
/* ensure its popover surface (which Streamlit portals to <body>) floats    */
/* above Plotly hover layers and other charts.                              */
div[data-testid="stPopover"] {
    z-index: 951 !important;
}
div[data-testid="stPopoverBody"],
div[data-baseweb="popover"] {
    z-index: 9999 !important;
}

/* Subtle pulse when DBI text refreshes — visual confirmation of liveness. */
@keyframes dbiPulse {
    0%   { box-shadow: 0 2px 8px rgba(0, 104, 201, 0.12); }
    50%  { box-shadow: 0 2px 14px rgba(0, 104, 201, 0.32); }
    100% { box-shadow: 0 2px 8px rgba(0, 104, 201, 0.12); }
}
.dbi-container[data-dbi-updated="1"] {
    animation: dbiPulse 0.6s ease-out;
}

.operator-rail {
    border: 1px solid #cbd5e1;
    border-radius: 6px;
    padding: .7rem .75rem;
    background: #ffffff;
    margin: .5rem 0 .75rem 0;
}
.operator-rail h4 {
    margin: 0 0 .35rem 0;
    font-size: .95rem;
}
.operator-rail p {
    margin: .25rem 0;
    color: #475569;
    font-size: .82rem;
    line-height: 1.25;
}
</style>
""", unsafe_allow_html=True)

try:
    from src.brain._version import __version__ as _BV
except Exception:
    _BV = "dev"

_P = Path(__file__).parent / "pages"

pg = st.navigation({
    "🔍 EDAP Console": [
        st.Page(str(_P / "0_Query_Console.py"),        title="Query Console",      icon="🔍"),
        st.Page(str(_P / "0_Schema_Discovery.py"),     title="Schema Discovery",   icon="🗺️"),
    ],
    "🧠 Supply Chain Brain": [
        st.Page(str(_P / "1_Supply_Chain_Brain.py"),   title="Overview & Graph",   icon="🧠"),
        st.Page(str(_P / "1b_Supply_Chain_Pipeline.py"), title="Supply Chain Pipeline", icon="🕸️"),
        st.Page(str(_P / "2_EOQ_Deviation.py"),        title="EOQ Deviation",      icon="📦"),
        st.Page(str(_P / "3_OTD_Recursive.py"),        title="OTD Recursive",      icon="🚚"),
        st.Page(str(_P / "4_Procurement_360.py"),      title="Procurement 360",    icon="🏭"),
        st.Page(str(_P / "5_Data_Quality.py"),         title="Data Quality",       icon="🧩"),
        st.Page(str(_P / "6_Connectors.py"),           title="Connectors",         icon="🔌"),
    ],
    "📡 MIT CTL Research": [
        st.Page(str(_P / "7_Lead_Time_Survival.py"),   title="Lead-Time Survival", icon="⏱️"),
        st.Page(str(_P / "8_Bullwhip.py"),             title="Bullwhip Effect",    icon="🌊"),
        st.Page(str(_P / "9_Multi_Echelon.py"),        title="Multi-Echelon",      icon="🏗️"),
        st.Page(str(_P / "10_Sustainability.py"),      title="Sustainability",     icon="🌱"),
        st.Page(str(_P / "11_Freight_Portfolio.py"),   title="Freight Portfolio",  icon="🚛"),
    ],
    "⚙️ Platform": [
        st.Page(str(_P / "12_What_If.py"),             title="What-If Sandbox",    icon="🧪"),
        st.Page(str(_P / "13_Decision_Log.py"),        title="Decision Log",       icon="📒"),
        st.Page(str(_P / "14_Benchmarks.py"),          title="Benchmarks",         icon="⚡"),
        st.Page(str(_P / "15_Report_Creator.py"),      title="Report Creator",     icon="📊"),
        st.Page(str(_P / "16_Cycle_Count_Accuracy.py"),title="Cycle Count Accuracy", icon="🔄"),
        st.Page(str(_P / "20_WIP_Aging_Review.py"),    title="WIP Aging Review",   icon="🏗️"),
    ],
    "🤖 AI": [
        st.Page(str(_P / "17_Document_RAG.py"),        title="Document Analysis",  icon="📄"),
        st.Page(str(_P / "18_ML_Research.py"),          title="ML Research Hub",    icon="🔬"),
        st.Page(str(_P / "19_Heart_Story.py"),          title="Heart Story",        icon="🫀"),
    ],
})

import json as _json
import logging as _logging
from src.brain.data_access import query_df
from src.brain.db_registry import bootstrap_default_connectors

_SITES_CACHE_PATH = Path(__file__).parent / "config" / "sites_cache.json"

def _load_sites_disk() -> list[str]:
    try:
        data = _json.loads(_SITES_CACHE_PATH.read_text(encoding="utf-8"))
        if isinstance(data, list) and data:
            return data
    except Exception:
        pass
    return []

def _save_sites_disk(sites: list[str]) -> None:
    try:
        _SITES_CACHE_PATH.write_text(_json.dumps(sites, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

@st.cache_data(ttl=3600, show_spinner=False)
def _query_sites_live() -> list[str]:
    try:
        bootstrap_default_connectors()
        df = query_df(
            "azure_sql",
            "SELECT DISTINCT business_unit_id FROM edap_dw_replica.dim_part WITH (NOLOCK) WHERE business_unit_id IS NOT NULL",
        )
        if not df.empty:
            return sorted(df["business_unit_id"].astype(str).tolist())
    except Exception as _e:
        _logging.warning(f"[app] site list query failed: {_e}")
    return []

def _get_sites_global() -> list[str]:
    """Fast path: disk cache first, live SQL only when cache is empty."""
    pool = _load_sites_disk()
    if not pool:
        live = _query_sites_live()
        if live:
            _save_sites_disk(live)
            pool = live
    return [""] + [s for s in pool if s and s.lower() != "unknown"]

def _operator_page_link(page: str, label: str, icon: str, fallback_url: str) -> None:
    try:
        st.page_link(page, label=label, icon=icon)
    except Exception:
        st.markdown(f"[{icon} {label}]({fallback_url})")

from src.brain.global_filters import render_global_filter_sidebar
from src.brain.operator_shell import mark_app_shell_active

with st.sidebar:
    st.markdown("### Global Filters")
    _site_opts = _get_sites_global()
    _cur_site  = st.session_state.get("g_site", "") or ""
    _site_idx  = _site_opts.index(_cur_site) if _cur_site in _site_opts else 0
    global_site = st.selectbox(
        "🏭 Plant (business unit)",
        _site_opts,
        index=_site_idx,
        key="g_site_global",
        help="Filter all pages to a single manufacturing site. Leave blank for all sites.",
    )
    if st.session_state.get("g_site") != global_site:
        st.session_state["g_site"] = global_site
        for k in list(st.session_state.keys()):
            if k.endswith("_sql") or k in ("otd_where", "bw_sql", "eoq_sql"):
                del st.session_state[k]
        st.cache_data.clear()

# Standardized timeline filter, available on every page via session_state.
render_global_filter_sidebar()

with st.sidebar:
    operator_mode = st.toggle(
        "Operator Mode",
        value=st.session_state.get("operator_mode", True),
        key="operator_mode_toggle",
        help="Shows the simplest daily workflow and keeps DBI focused on the next move.",
    )
    st.session_state["operator_mode"] = operator_mode
    if operator_mode:
        _scope_site = st.session_state.get("g_site") or "All plants"
        _scope_start = st.session_state.get("g_date_start")
        _scope_end = st.session_state.get("g_date_end")
        _scope_window = (
            f"{_scope_start} to {_scope_end}"
            if _scope_start and _scope_end else "selected timeline"
        )
        st.markdown(
            f"""
<div class="operator-rail">
  <h4>Daily Control Path</h4>
  <p><b>Scope:</b> {_scope_site} · {_scope_window}</p>
  <p><b>Read DBI first.</b> Work the item marked Action needed before exploring charts.</p>
</div>
""",
            unsafe_allow_html=True,
        )
        _operator_page_link("app.py", "Find part / order / supplier", "🔍", "/")
        _operator_page_link(str(_P / "1_Supply_Chain_Brain.py"), "Review plant risk map", "🧠", "/Supply_Chain_Brain")
        _operator_page_link(str(_P / "15_Report_Creator.py"), "Create bi-weekly one-pager", "📊", "/Report_Creator")

with st.sidebar:
    st.caption(f"Supply Chain Brain · v{_BV}")
    st.divider()
    st.markdown("🔵 Azure SQL · `edap-replica-cms-sqldb`")
    st.markdown("🔴 Oracle Fusion · `DEV13`")

st.session_state["_app_shell_rendered"] = True
mark_app_shell_active(True)

try:
    from src.brain.ui_action_log import log_page_visit
    log_page_visit(pg.title, st.session_state.get("g_site", ""))
except Exception:
    pass

try:
    pg.run()
finally:
    mark_app_shell_active(False)
    st.session_state["_app_shell_rendered"] = False
