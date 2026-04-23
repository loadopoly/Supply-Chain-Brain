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
    ],
    "🤖 AI": [
        st.Page(str(_P / "17_Document_RAG.py"),        title="Document Analysis",  icon="📄"),
        st.Page(str(_P / "18_ML_Research.py"),          title="ML Research Hub",    icon="🔬"),
    ],
})

from src.brain.data_access import query_df
from src.brain.db_registry import bootstrap_default_connectors
@st.cache_data(ttl=3600)
def _get_sites_global():
    try:
        bootstrap_default_connectors()
        df = query_df("azure_sql", "SELECT DISTINCT business_unit_id FROM edap_dw_replica.dim_part WITH (NOLOCK) WHERE business_unit_id IS NOT NULL")
        if not df.empty:
            return [""] + sorted(df["business_unit_id"].astype(str).tolist())
    except Exception as e:
        import logging
        logging.error(f"Failed to fetch sites: {e}")
        pass
    return [""]

from src.brain.global_filters import render_global_filter_sidebar

with st.sidebar:
    global_site = st.selectbox('Global Mfg Site (business_unit)', _get_sites_global(), index=0, key='g_site_global')
    if st.session_state.get('g_site') != global_site:
        st.session_state['g_site'] = global_site
        for k in list(st.session_state.keys()):
            if k.endswith('_sql') or k == 'otd_where' or k == 'bw_sql' or k == 'eoq_sql':
                del st.session_state[k]
        st.cache_data.clear()

# Standardized timeline filter, available on every page via session_state.
render_global_filter_sidebar()

with st.sidebar:
    st.caption(f"Supply Chain Brain · v{_BV}")
    st.divider()
    st.markdown("🔵 Azure SQL · `edap-replica-cms-sqldb`")
    st.markdown("🔴 Oracle Fusion · `DEV13`")

pg.run()
