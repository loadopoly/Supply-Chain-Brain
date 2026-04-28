from __future__ import annotations

import html
import json
from pathlib import Path

import streamlit as st

from src.brain.global_filters import render_global_filter_sidebar


_PIPELINE_ROOT = Path(__file__).resolve().parents[2]
_SITES_CACHE_PATH = _PIPELINE_ROOT / "config" / "sites_cache.json"
_APP_SHELL_ACTIVE = False


def mark_app_shell_active(active: bool) -> None:
    global _APP_SHELL_ACTIVE
    _APP_SHELL_ACTIVE = bool(active)


def is_app_shell_active() -> bool:
    return _APP_SHELL_ACTIVE


def _cached_sites() -> list[str]:
    try:
        data = json.loads(_SITES_CACHE_PATH.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [str(site) for site in data if str(site).strip()]
    except Exception:
        pass
    return []


def _page_link(page: str, label: str, icon: str, fallback_url: str) -> None:
    try:
        st.page_link(page, label=label, icon=icon)
    except Exception:
        st.markdown(f"[{icon} {label}]({fallback_url})")


def render_operator_sidebar_fallback() -> None:
    if is_app_shell_active():
        return

    st.markdown(
        """
<style>
.operator-rail {
    border: 1px solid #cbd5e1;
    border-radius: 6px;
    padding: .7rem .75rem;
    background: #ffffff;
    margin: .5rem 0 .75rem 0;
}
.operator-rail h4 { margin: 0 0 .35rem 0; font-size: .95rem; }
.operator-rail p { margin: .25rem 0; color: #475569; font-size: .82rem; line-height: 1.25; }
</style>
""",
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.markdown("### Global Filters")
        site_options = [""] + [site for site in _cached_sites() if site.lower() != "unknown"]
        current_site = st.session_state.get("g_site", "") or ""
        site_index = site_options.index(current_site) if current_site in site_options else 0
        global_site = st.selectbox(
            "🏭 Plant (business unit)",
            site_options,
            index=site_index,
            key="g_site_global_fallback",
            help="Filter all pages to a single manufacturing site. Leave blank for all sites.",
        )
        st.session_state["g_site"] = global_site

    render_global_filter_sidebar()

    with st.sidebar:
        operator_mode = st.toggle(
            "Operator Mode",
            value=st.session_state.get("operator_mode", True),
            key="operator_mode_toggle_fallback",
            help="Shows the simplest daily workflow and keeps DBI focused on the next move.",
        )
        st.session_state["operator_mode"] = operator_mode
        if operator_mode:
            scope_site = html.escape(st.session_state.get("g_site") or "All plants")
            scope_start = st.session_state.get("g_date_start")
            scope_end = st.session_state.get("g_date_end")
            scope_window = html.escape(
                f"{scope_start} to {scope_end}" if scope_start and scope_end else "selected timeline"
            )
            st.markdown(
                f"""
<div class="operator-rail">
  <h4>Daily Control Path</h4>
  <p><b>Scope:</b> {scope_site} · {scope_window}</p>
  <p><b>Read DBI first.</b> Work the item marked Action needed before exploring charts.</p>
</div>
""",
                unsafe_allow_html=True,
            )
            _page_link("app.py", "Find part / order / supplier", "🔍", "/")
            _page_link("pages/1_Supply_Chain_Brain.py", "Review plant risk map", "🧠", "/Supply_Chain_Brain")
            _page_link("pages/15_Report_Creator.py", "Create bi-weekly one-pager", "📊", "/Report_Creator")

        st.caption("Supply Chain Brain")