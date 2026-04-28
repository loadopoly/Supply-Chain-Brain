import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight
from src.brain.operator_shell import render_operator_sidebar_fallback
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import json
import logging
from pptx import Presentation
import os
from pathlib import Path
import sys
import subprocess
import io
import time
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Report Creator", page_icon="📊", layout="wide")

TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "config" / "templates"
TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)

REPORT_METADATA = [
    {"title": "Bi-Weekly 1 Pager", "url": "biweekly_1pager", "desc": "Bi-weekly operations review, OTD, IFR, cycle count accuracy, PFEP health, brain insights, 30-day actions, single-slide executive summary, widescreen."},
    {"title": "Supply Chain Brain", "url": "1_Supply_Chain_Brain", "desc": "Interactive network graph, cross-domain multi-dimensional graph intelligence, procurement, logistics."},
    {"title": "EOQ Deviation", "url": "2_EOQ_Deviation", "desc": "Economic Order Quantity, deviation, stockouts, holding costs, ordering costs, optimal inventory levels."},
    {"title": "OTD Recursive", "url": "3_OTD_Recursive", "desc": "On-Time Delivery, cascading delays, root-cause of missed orders, delivery performance, late shipments."},
    {"title": "Procurement 360", "url": "4_Procurement_360", "desc": "Supplier reliability, PO history, Vendor scorecards, purchase orders, supplier evaluation."},
    {"title": "Lead Time Survival", "url": "7_Lead_Time_Survival", "desc": "Lead time distribution, survival analysis (Kaplan-Meier), supplier delivery probability, leadtime variability."},
    {"title": "Bullwhip Diagnostics", "url": "8_Bullwhip", "desc": "Bullwhip effect, demand distortion, variance, inventory amplification, cascading fulfillment shocks."},
    {"title": "Multi-Echelon Optimization", "url": "9_Multi_Echelon", "desc": "Multi-echelon inventory optimization, network safety stock, node positioning."},
    {"title": "Sustainability & ESG", "url": "10_Sustainability", "desc": "ESG, carbon emissions, Scope 3, green logistics, environmental supply chain."},
    {"title": "Freight Portfolio", "url": "11_Freight_Portfolio", "desc": "LTL vs FTL, freight costs, shipment zones, carrier performance, transportation, shipping rates."},
    {"title": "What-If Scenario Simulation", "url": "12_What_If", "desc": "Scenario planning, simulation, stress testing, what-if variables, risk modeling."},
    {"title": "Decision Log", "url": "13_Decision_Log", "desc": "Systemic vs operational interventions, tracked changes, ownership, ERP system settings."},
    {"title": "Industry Benchmarks", "url": "14_Benchmarks", "desc": "Peer comparisons, industry standards, cycle count accuracy, IFR/ITR vs peers."},
    {"title": "ASTEC Cycle Count Metrics Report", "url": "cycle_count", "desc": "Cycle count completion by quarter, accuracy, physical inventory auditing, warehouse metrics."},
    {"title": "ADC Classification", "url": "adc_class", "desc": "ADC classification, ABC inventory classification, value categorization, volume mapping."}
]

def suggest_report(query_text):
    documents = [r["title"] + " " + r["desc"] for r in REPORT_METADATA]
    documents.append(query_text)

    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(documents)

    cosine_sim = cosine_similarity(tfidf_matrix[-1], tfidf_matrix[:-1])
    top_indices = cosine_sim[0].argsort()[-3:][::-1]
    
    results = []
    for idx in top_indices:
        if cosine_sim[0][idx] > 0.05:
            results.append((REPORT_METADATA[idx], cosine_sim[0][idx]))

    return results

def strip_pptx_content_to_template(input_bytes, output_path):
    try:
        prs = Presentation(io.BytesIO(input_bytes))
        
        # Scrub slides to retain only template information
        xml_slides = prs.slides._sldIdLst
        slides = list(xml_slides)
        for slide in slides:
            xml_slides.remove(slide)
            
        prs.save(output_path)
        return True
    except Exception as e:
        logger.error(f"Failed to scrub template: {e}")
        return False

_PAGE_DIR = Path(__file__).resolve().parent.parent

render_operator_sidebar_fallback()


def _biweekly_task(site: str, demo: bool, tmpl_path: Path, conn_ref: list) -> tuple:
    """Runs in a background thread; returns (findings, out_path, warnings)."""
    sys.path.insert(0, str(_PAGE_DIR))
    from src.deck import build_findings, render_biweekly_one_pager
    from src.deck.live import load_live_datasets
    from src.deck import demo as _deck_demo
    from datetime import date as _date

    if demo:
        raw = _deck_demo.make_all()
        live_warnings: list[str] = []
    else:
        _live = load_live_datasets(site=site, _conn_ref=conn_ref)
        raw = {"otd": _live.otd, "ifr": _live.ifr, "itr": _live.itr, "pfep": _live.pfep}
        live_warnings = _live.warnings

    findings = build_findings(raw["otd"], raw["ifr"], raw["itr"], raw["pfep"], site=site)

    snap_dir = _PAGE_DIR / "snapshots"
    snap_dir.mkdir(exist_ok=True)
    stamp = _date.today().strftime("%Y%m%d")
    site_slug = site.replace(" ", "_")
    out = snap_dir / f"biweekly_{site_slug}_{stamp}.pptx"

    tmpl = tmpl_path if tmpl_path.exists() else None
    render_biweekly_one_pager(findings, out, template_path=tmpl)
    return findings, out, live_warnings


st.markdown("## 📊 Presentation & Report Creator")
st.markdown("Generate comprehensive cross-dataset presentations, upload slide masters/templates, or let the AI match your specific business question to the right analytical module.")

if st.session_state.get("operator_mode", True):
    _scope_site = st.session_state.get("g_site") or "All plants"
    _scope_start = st.session_state.get("g_date_start")
    _scope_end = st.session_state.get("g_date_end")
    _scope_window = f"{_scope_start} to {_scope_end}" if _scope_start and _scope_end else "selected timeline"
    _fast1, _fast2, _fast3 = st.columns(3)
    with _fast1:
        with st.container(border=True):
            st.markdown("**Default Output**")
            st.caption("Bi-Weekly 1 Pager")
    with _fast2:
        with st.container(border=True):
            st.markdown("**Current Scope**")
            st.caption(f"{_scope_site} · {_scope_window}")
    with _fast3:
        with st.container(border=True):
            st.markdown("**Decision Ready**")
            st.caption("KPI, DBI, four lenses, and 30-day actions")

tab1, tab2, tab3 = st.tabs(["1. Generate Review Deck", "2. Upload Slide Templates", "3. Quest Console"])

with tab1:
    st.subheader("Executive Presentation Builder")

    from src.brain.data_access import query_df
    from src.deck.erp_translation import SITE_ERP_MAP

    _SITES_CACHE_FILE = _PAGE_DIR / "config" / "sites_cache.json"

    # Static structural fallback — always available instantly
    _STATIC_SITES = sorted(SITE_ERP_MAP.keys())

    def _load_cached_sites() -> list[str]:
        """Read last-known site list from disk cache."""
        try:
            data = json.loads(_SITES_CACHE_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list) and data:
                return data
        except Exception:
            pass
        return []

    def _save_cached_sites(sites_list: list[str]) -> None:
        try:
            _SITES_CACHE_FILE.write_text(
                json.dumps(sites_list, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            pass

    @st.cache_data(ttl=3600, show_spinner=False)
    def _query_live_sites() -> list[str]:
        try:
            df = query_df(
                "azure_sql",
                "SELECT DISTINCT business_unit_id "
                "FROM edap_dw_replica.dim_part WITH (NOLOCK) "
                "WHERE business_unit_id IS NOT NULL"
            )
            if not df.empty:
                return sorted(df["business_unit_id"].astype(str).tolist())
        except Exception:
            pass
        return []

    # Build the final list: live SQL > disk cache > static names
    _live_sites = _query_live_sites()
    if _live_sites:
        _save_cached_sites(_live_sites)
        _all_sites = _live_sites
    else:
        _disk = _load_cached_sites()
        _all_sites = _disk if _disk else _STATIC_SITES

    sites = ["ALL"] + [s for s in _all_sites if s != "ALL"]
    default_site = st.session_state.get("g_site", "ALL") or "ALL"

    def _get_sites_for_report() -> list[str]:
        return sites

    # ── Bi-Weekly 1 Pager (default / recommended) ─────────────────────────
    st.markdown(
        "### 📋 Bi-Weekly 1 Pager  *(Default)*\n"
        "One widescreen slide condensing all KPIs, Four Lenses, Brain Insights, "
        "and 30-day actions — optimised for bi-weekly operations reviews."
    )

    bp_col1, bp_col2, bp_col3 = st.columns([1, 1, 2])
    with bp_col1:
        bp_site_idx = sites.index(default_site) if default_site in sites else 0
        bp_site = st.selectbox("Site", options=sites, index=bp_site_idx,
                               key="bp_site")
    with bp_col2:
        bp_window = st.number_input("Window (days)", min_value=7, max_value=90,
                                    value=14, step=7, key="bp_window")
    with bp_col3:
        bp_demo = st.checkbox("Use demo / synthetic data", value=False,
                              key="bp_demo",
                              help="Check this if live DB connections are unavailable.")

    bp_tmpl_path = TEMPLATE_DIR / "Bi-Weekly 1 Pager.pptx"
    if bp_tmpl_path.exists():
        st.caption(f"✅ Template: {bp_tmpl_path.name}  (widescreen 13.33\"×7.50\")")
    else:
        st.caption("ℹ️ Template not found — will use default widescreen layout.")

    # ── Session-state initialisation for async generation ─────────────────
    for _k, _v in [("bp_running", False), ("bp_future", None), ("bp_result", None),
                   ("bp_error", None), ("bp_conn_ref", [])]:
        if _k not in st.session_state:
            st.session_state[_k] = _v

    # Generate button — only shown when idle
    if not st.session_state.bp_running:
        if st.button("🚀 Generate Bi-Weekly 1 Pager", type="primary", key="btn_biweekly"):
            _conn_ref: list = []
            _executor = ThreadPoolExecutor(max_workers=1)
            _future = _executor.submit(_biweekly_task, bp_site, bp_demo, bp_tmpl_path, _conn_ref)
            st.session_state.bp_running = True
            st.session_state.bp_future = _future
            st.session_state.bp_conn_ref = _conn_ref
            st.session_state.bp_result = None
            st.session_state.bp_error = None
            st.rerun()

    # Running state — spinner row + cancel button
    if st.session_state.bp_running:
        _future = st.session_state.bp_future
        _prog_col, _cancel_col = st.columns([5, 1])
        _prog_col.info("⏳ Loading datasets and building findings… (large sites may take up to 2 min)")
        with _cancel_col:
            if st.button("🛑 Cancel", key="btn_cancel_biweekly"):
                # Send cancel to the SQL server via pyodbc, then abandon the future
                _cref = st.session_state.get("bp_conn_ref", [])
                if _cref:
                    try:
                        _cref[0].cancel()
                    except Exception:
                        pass
                st.session_state.bp_running = False
                st.session_state.bp_future = None
                st.session_state.bp_conn_ref = []
                st.warning("Generation cancelled.")
                st.rerun()

        if _future is not None and _future.done():
            st.session_state.bp_running = False
            try:
                st.session_state.bp_result = _future.result()
            except Exception as _e:
                import traceback as _tb
                st.session_state.bp_error = (str(_e), _tb.format_exc())
            st.rerun()
        else:
            time.sleep(0.5)
            st.rerun()

    # Results
    if st.session_state.bp_result:
        _findings, _out, _warnings = st.session_state.bp_result
        for _w in _warnings:
            st.warning(_w)
        st.success(f"Generated: {_out.name}")
        with open(_out, "rb") as _f:
            st.download_button(
                "⬇️ Download Bi-Weekly 1 Pager",
                data=_f,
                file_name=_out.name,
                mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                key="dl_biweekly",
            )
        _kpis = _findings.get("kpis", {})
        st.markdown("**KPI Preview (live values encoded into slide)**")
        _k1, _k2, _k3 = st.columns(3)

        def _kv(kpi, key, default=None):
            return _kpis.get(kpi, {}).get(key, default)

        def _delta_label(kpi):
            v = _kv(kpi, "delta_pp")
            return f"{v:+.1f} pp" if v is not None else None

        _k1.metric("OTD 14D",
                   f"{_kv('otd', 'value', float('nan')):.1f}%"
                   if _kv('otd', 'value') is not None else "—",
                   delta=_delta_label("otd"))
        _k2.metric("IFR 14D",
                   f"{_kv('ifr', 'value', float('nan')):.1f}%"
                   if _kv('ifr', 'value') is not None else "—",
                   delta=_delta_label("ifr"))
        _k3.metric("CC 14D",
                   f"{_kv('cc', 'value', float('nan')):.1f}%"
                   if _kv('cc', 'value') is not None else "—",
                   delta=_delta_label("cc"))

    if st.session_state.bp_error:
        _err, _tb_str = st.session_state.bp_error
        st.error(f"Generation failed: {_err}")
        st.code(_tb_str)

    st.divider()

    # ── Full 14-Slide Cross-Dataset Review Deck ───────────────────────────
    with st.expander("📑 Full Cross-Dataset Review Deck (14 slides)", expanded=False):
        st.markdown(
            "Generates the complete **Cross-Dataset Supply-Chain Review Deck** "
            "using the logic engine in CrossDataset_Agent_Process_Spec.md."
        )

        idx = sites.index(default_site) if default_site in sites else 0
        site_filter = st.selectbox("Site Filter:", options=sites, index=idx,
                                   key="deck_site")
        use_demo = st.checkbox("Use Demo / Synthetic Data", value=True, key="deck_demo")

        available_templates = ["Default Template"]
        if TEMPLATE_DIR.exists():
            tmpl_files = sorted(f.name for f in TEMPLATE_DIR.iterdir()
                                if f.is_file() and f.name.endswith(".pptx"))
            # Surface the Bi-Weekly template first if present
            if "Bi-Weekly 1 Pager.pptx" in tmpl_files:
                tmpl_files = ["Bi-Weekly 1 Pager.pptx"] + [
                    f for f in tmpl_files if f != "Bi-Weekly 1 Pager.pptx"
                ]
            available_templates += tmpl_files

        selected_template = st.selectbox("Select Template Presentation",
                                         options=available_templates,
                                         key="deck_template")

        if st.button("Generate Review Deck (PPTX)", key="btn_full_deck"):
            with st.spinner("Running cross-dataset extraction and compiling findings…"):
                subprocess.run([sys.executable, "pipeline.py", "run"],
                               cwd=str(Path(__file__).parent.parent),
                               capture_output=True)

                cmd = [sys.executable, "pipeline.py", "deck", "--site", site_filter]
                if use_demo:
                    cmd.append("--demo")
                if selected_template != "Default Template":
                    cmd.extend(["--template", str(TEMPLATE_DIR / selected_template)])

                res = subprocess.run(cmd, cwd=str(Path(__file__).parent.parent),
                                     capture_output=True, text=True)

                if res.returncode == 0:
                    st.success("Deck successfully generated!")
                    for line in res.stdout.split("\n"):
                        if line.startswith("wrote") and line.strip().endswith(".pptx"):
                            filepath = line.replace("wrote ", "").strip()
                            try:
                                with open(filepath, "rb") as f:
                                    st.download_button(
                                        "Download Generated Deck",
                                        data=f,
                                        file_name=Path(filepath).name,
                                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                                    )
                            except Exception as e:
                                st.error(f"Could not load generated file: {e}")
                else:
                    st.error("Failed to generate deck. Error:")
                    st.code(res.stderr)

with tab2:
    st.subheader("Manage Presentation Templates")
    st.markdown(
        "Upload existing slide decks (Full decks, 1-pagers, Corporate Templates). "
        "The backend agent will scan the deck to learn its styles and layouts, stripping all existing slides to retain a purely clean template. "
        "This template acts as the visual backbone for the analytical reports generated by CrossDataset_Agent_Process_Spec.md."
    )

    uploaded_file = st.file_uploader("Upload PPTX", type=["pptx"])
    template_name = st.text_input("Name this Template", placeholder="e.g. Master_Corporate_1Pager")

    if st.button("Process & Save Template"):
        if not uploaded_file:
            st.warning("Please upload a .pptx file.")
        elif not template_name:
            st.warning("Please provide a name for the template.")
        else:
            with st.spinner("Agent reviewing and extracting template metadata..."):
                final_name = template_name if template_name.endswith(".pptx") else f"{template_name}.pptx"
                out_path = TEMPLATE_DIR / final_name
                success = strip_pptx_content_to_template(uploaded_file.read(), out_path)
                
                if success:
                    st.success(f"Template '{final_name}' successfully processed into local memory and slides scrubbed.")
                    st.rerun()  # Update the dropdowns
                else:
                    st.error("Failed to process the presentation file. Ensure it is a valid PPTX.")

    if TEMPLATE_DIR.exists():
        files = [f.name for f in TEMPLATE_DIR.iterdir() if f.is_file() and f.name.endswith(".pptx")]
        if files:
            st.markdown("### Saved Templates in Local Memory")
            for f in files:
                st.caption(f"💾 {f}")

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight('Report Creator', ctx)
st.divider()


with tab3:
    # ----------------------------------------------------------------------
    # Brain-driven Quest Console — replaces the legacy "Ask the Data" tab.
    # The User is the Body of the Brain: type a real-world situation, the
    # Brain parses → orchestrates → composes → emits two living PPTXs that
    # auto-refresh as new data shows progress.
    # ----------------------------------------------------------------------
    st.subheader("🧠 Brain-Driven Quest Console")
    st.markdown(
        "Describe a real-world situation in plain language. The Brain parses your "
        "intent into a **Mission** under the seed quest *Optimize Supply Chains*, "
        "runs the relevant analyzers, synthesizes the target-entity schema, and "
        "writes two living PPTX artifacts that overwrite in place as the data shows progress."
    )

    try:
        from src.brain import (
            mission_runner, mission_store, intent_parser, quests,
        )
        _quest_ok = True
        _quest_err = None
    except Exception as _qe:
        _quest_ok = False
        _quest_err = str(_qe)
        st.error(f"Quest Console unavailable: {_quest_err}")

    if _quest_ok:
        # Sidebar — Open Missions
        with st.sidebar:
            st.markdown("### 🎯 Open Missions")
            try:
                _open = mission_store.list_open(limit=25)
            except Exception as _le:
                _open = []
                st.caption(f"(missions unavailable: {_le})")
            if not _open:
                st.caption("No open missions yet — launch one from Tab 3.")
            for _m in _open:
                _label = f"{_m.site} · {_m.target_entity_kind}={_m.target_entity_key}"
                with st.expander(_label, expanded=False):
                    st.caption(f"id: `{_m.id}`")
                    st.caption(f"quest: {_m.quest_id}")
                    st.caption(f"progress: {float(_m.progress_pct or 0):.0f}%")
                    st.caption(f"refreshed: {_m.last_refreshed_at or '—'}")
                    if st.button("🔄 Refresh", key=f"refresh_{_m.id}"):
                        with st.spinner("Refreshing mission…"):
                            _r = mission_runner.refresh(_m.id)
                        st.json(_r)
                        st.rerun()

        # Main pane — three columns: site picker, query, launch
        st.markdown("#### Launch a new Mission")
        _c1, _c2 = st.columns([1, 3])
        with _c1:
            _site_opts = _get_sites_for_report()
            _default_site = st.session_state.get("g_site", "ALL") or "ALL"
            _idx = _site_opts.index(_default_site) if _default_site in _site_opts else 0
            quest_site = st.selectbox("Site", options=_site_opts, index=_idx,
                                      key="quest_site")
            quest_horizon = st.number_input("Horizon (days)", min_value=14,
                                            max_value=365, value=90, step=7,
                                            key="quest_horizon")
        with _c2:
            quest_query = st.text_area(
                "Describe the situation",
                placeholder=(
                    "e.g. 'I'm at Jerome and conducting a restructuring of "
                    "their Warehouse — show me velocity hotspots and the "
                    "parts I'm overstocking.'"
                ),
                height=120, key="quest_query",
            )
            _btn_a, _btn_b = st.columns([1, 1])
            with _btn_a:
                _preview = st.button("🔍 Preview Parsed Intent",
                                     use_container_width=True)
            with _btn_b:
                _launch = st.button("🚀 Launch Mission",
                                    use_container_width=True, type="primary")

        if _preview and quest_query.strip():
            with st.spinner("Parsing intent via LLM ensemble…"):
                _parsed = intent_parser.parse(quest_query, site_default=quest_site)
            st.markdown("**Parsed Intent**")
            st.json(_parsed.as_dict())
            _qid = next(
                (quests.SCOPE_TAG_TO_QUEST[t] for t in _parsed.scope_tags
                 if t in quests.SCOPE_TAG_TO_QUEST),
                quests.ROOT_QUEST_ID,
            )
            _q = quests.get_quest(_qid)
            if _q:
                st.caption(f"Will be filed under quest: **{_q.label}** (`{_q.id}`)")

        if _launch:
            if not quest_query.strip():
                st.warning("Please describe the situation first.")
            else:
                with st.spinner("Brain is parsing, dispatching analyzers, and rendering artifacts…"):
                    try:
                        _mission = mission_runner.launch(
                            user_query=quest_query.strip(),
                            site=quest_site,
                            horizon_days=int(quest_horizon),
                        )
                    except Exception as _le:
                        st.exception(_le)
                        _mission = None

                if _mission is not None:
                    st.success(f"Mission **{_mission.id}** launched.")
                    _l, _r = st.columns([1, 1])
                    with _l:
                        st.markdown("**Mission**")
                        st.write({
                            "id": _mission.id,
                            "quest": _mission.quest_id,
                            "site": _mission.site,
                            "target": f"{_mission.target_entity_kind}={_mission.target_entity_key}",
                            "scope_tags": _mission.scope_tags,
                            "progress_pct": _mission.progress_pct,
                        })
                    with _r:
                        st.markdown("**Artifacts**")
                        for _name, _path in (_mission.artifact_paths or {}).items():
                            try:
                                with open(_path, "rb") as _fh:
                                    st.download_button(
                                        f"⬇ Download {_name}",
                                        data=_fh.read(),
                                        file_name=Path(_path).name,
                                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                                        key=f"dl_{_mission.id}_{_name}",
                                    )
                            except Exception as _de:
                                st.caption(f"({_name}: {_de})")
