import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Report Creator", page_icon="📊", layout="wide")

TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "config" / "templates"
TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)

REPORT_METADATA = [
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

st.markdown("## 📊 Presentation & Report Creator")
st.markdown("Generate comprehensive cross-dataset presentations, upload slide masters/templates, or let the AI match your specific business question to the right analytical module.")

tab1, tab2, tab3 = st.tabs(["1. Generate Review Deck", "2. Upload Slide Templates", "3. Quest Console"])

with tab1:
    st.subheader("Executive Presentation Builder")
    st.markdown("Generates the **Cross-Dataset Supply-Chain Review Deck** using the logic engine in CrossDataset_Agent_Process_Spec.md. Choose a template to map conclusions onto.")

    from src.brain.data_access import query_df
    @st.cache_data(ttl=3600)
    def _get_sites_for_report():
        try:
            df = query_df("azure_sql", "SELECT DISTINCT business_unit_id FROM edap_dw_replica.dim_part WITH (NOLOCK) WHERE business_unit_id IS NOT NULL")
            if not df.empty:
                return ["ALL"] + sorted(df["business_unit_id"].astype(str).tolist())
        except Exception:
            pass
        return ["ALL"]
    
    sites = _get_sites_for_report()
    default_site = st.session_state.get("g_site", "ALL")
    if not default_site:  # Map empty string from global sidebar to "ALL"
        default_site = "ALL"
    idx = sites.index(default_site) if default_site in sites else 0
    
    site_filter = st.selectbox("Site Filter (Select 'ALL' for portfolio view):", options=sites, index=idx)
    use_demo = st.checkbox("Use Demo / Synthetic Data", value=True)

    available_templates = ["Default Template"] 
    if TEMPLATE_DIR.exists():
        available_templates += [f.name for f in TEMPLATE_DIR.iterdir() if f.is_file() and f.name.endswith(".pptx")]

    selected_template = st.selectbox("Select Template Presentation", options=available_templates)

    if st.button("Generate Review Deck (PPTX)"):
        with st.spinner("Running cross-dataset extraction from Oracle Fusion & Azure SQL and compiling findings..."):
            # Update mapped data via the pipeline before generating
            subprocess.run([sys.executable, "pipeline.py", "run"], cwd=str(Path(__file__).parent.parent), capture_output=True)

            cmd = [sys.executable, "pipeline.py", "deck", "--site", site_filter]
            if use_demo:
                cmd.append("--demo")
            if selected_template != "Default Template":
                cmd.extend(["--template", str(TEMPLATE_DIR / selected_template)])

            res = subprocess.run(cmd, cwd=str(Path(__file__).parent.parent), capture_output=True, text=True)

            if res.returncode == 0:
                st.success("Deck successfully generated!")
                out_lines = res.stdout.split(chr(10))
                for line in out_lines:
                    if line.startswith("wrote"):
                        filepath = line.replace("wrote ", "").strip()
                        if filepath.endswith('.pptx'):
                            try:
                                with open(filepath, "rb") as f:
                                    st.download_button("Download Generated Deck", data=f, file_name=Path(filepath).name, mime="application/vnd.openxmlformats-officedocument.presentationml.presentation")
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
