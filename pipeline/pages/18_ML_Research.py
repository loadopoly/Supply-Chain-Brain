"""18_ML_Research.py — Brain ML Research Hub.

Surfaces the Brain's ``ml_research`` module as an interactive Streamlit page.
Browse papers, datasets, and MIT OpenCourseWare courses the Brain has
discovered from:

* **arXiv** — CS/ML/math preprints (replaces HuggingFace Papers)
* **OpenAlex** — 200 M+ scholarly works (replaces Semantic Scholar)
* **CrossRef** — DOI-indexed published papers
* **CORE** — open-access full-text repository
* **NASA NTRS** — systems engineering technical reports
* **Zenodo** — research datasets (replaces HuggingFace Datasets)
* **MIT OCW** — 2 500+ free courses via sitemap keyword scoring

Supply chain structures are an advanced form of systems engineering in the
physical realm — so the OCW + NTRS crawls intentionally span both supply
chain management and foundational systems engineering / operations research.
"""

import sys
from pathlib import Path

import streamlit as st

# Ensure pipeline root is on sys.path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.brain.ml_research import (
    search_papers_interactive,
    search_ocw_interactive,
    research_supply_chain_topics,
    recent_ml_learnings,
    recent_ocw_details,
    deepen_ocw_course,
    cascade_deepen_ocw,
    adaptive_cascade_ocw,
    fetch_ocw_course_detail,
    _SUPPLY_CHAIN_TOPICS,
    _OCW_TOPICS,
)

st.set_page_config(page_title="ML Research Hub", page_icon="🔬", layout="wide")
st.title("🔬 ML Research Hub")
st.caption(
    "Brain-native research discovery — **arXiv**, **OpenAlex**, **CrossRef**, **CORE**, "
    "**NASA NTRS**, **Zenodo** datasets, and **MIT OpenCourseWare**. "
    "Supply chain structures are an advanced form of systems engineering; "
    "coverage spans CS/ML and systems engineering / operations research. "
    "All findings are persisted to the knowledge corpus as first-class entities."
)

# ---------------------------------------------------------------------------
# Sidebar — manual research trigger
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Research Controls")
    st.markdown(
        "The Brain automatically sweeps ML topics and MIT OCW every 4 hours. "
        "Use the button below to trigger a manual cycle."
    )
    if st.button("🔄 Run research cycle now", use_container_width=True):
        with st.spinner("Researching ML topics + MIT OCW…"):
            result = research_supply_chain_topics()
        st.success(
            f"Cycle complete: "
            f"{result.get('papers_found', 0)} papers, "
            f"{result.get('datasets_found', 0)} datasets, "
            f"{result.get('learnings_written', 0)} new ML learnings, "
            f"{result.get('ocw_courses_found', 0)} OCW courses, "
            f"{result.get('ocw_learnings_written', 0)} new OCW learnings."
        )

    st.divider()
    st.subheader("ML Topics")
    for topic in _SUPPLY_CHAIN_TOPICS:
        st.markdown(f"- {topic}")

    st.divider()
    st.subheader("MIT OCW Topics")
    st.caption("Supply chain as systems engineering — spans both domains")
    for topic in _OCW_TOPICS:
        st.markdown(f"- {topic}")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_search, tab_ocw, tab_corpus, tab_graph = st.tabs(
    ["🔍 Live Search", "🎓 MIT OCW", "📚 Corpus Learnings", "🕸️ Knowledge Graph"]
)

# --- Tab 1: Live search ---
with tab_search:
    st.subheader("Search Papers & Datasets")
    query = st.text_input(
        "Research query",
        placeholder="e.g. demand forecasting transformer supply chain",
        key="ml_query",
    )

    if query:
        with st.spinner(f"Searching across arXiv, OpenAlex, CrossRef, CORE, Zenodo for '{query}'…"):
            results = search_papers_interactive(query)

        arxiv_papers  = results.get("arxiv_papers", [])
        oa_papers     = results.get("openalex_papers", [])
        cr_papers     = results.get("crossref_papers", [])
        core_papers   = results.get("core_papers", [])
        datasets      = results.get("datasets", [])

        cols = st.columns(5)
        cols[0].metric("arXiv", len(arxiv_papers))
        cols[1].metric("OpenAlex", len(oa_papers))
        cols[2].metric("CrossRef", len(cr_papers))
        cols[3].metric("CORE", len(core_papers))
        cols[4].metric("Zenodo Datasets", len(datasets))

        def _paper_expander(p: dict, source_label: str) -> None:
            title = p.get("title") or "Unknown"
            year  = p.get("year") or "?"
            cites = p.get("citations", 0)
            header = f"📄 {title} ({year})"
            if cites:
                header += f" — {cites} citations"
            with st.expander(header, expanded=False):
                st.caption(f"Source: **{source_label}** | ID: `{p.get('arxiv_id', '')}` | DOI: `{p.get('doi', '')}`")
                if p.get("url"):
                    st.markdown(f"[Open paper]({p['url']})")
                if p.get("authors"):
                    st.markdown(f"**Authors:** {', '.join(p['authors'][:3])}")
                if p.get("keywords"):
                    st.markdown(f"**Keywords:** {', '.join(p['keywords'][:5])}")
                if p.get("summary"):
                    st.markdown(p["summary"])

        # arXiv
        if arxiv_papers:
            st.markdown("#### arXiv (preprints)")
            for p in arxiv_papers:
                _paper_expander(p, "arXiv")

        # OpenAlex
        if oa_papers:
            st.markdown("#### OpenAlex (scholarly database)")
            for p in oa_papers:
                _paper_expander(p, "OpenAlex")

        # CrossRef
        if cr_papers:
            st.markdown("#### CrossRef (published / peer-reviewed)")
            for p in cr_papers:
                _paper_expander(p, "CrossRef")

        # CORE
        if core_papers:
            st.markdown("#### CORE (open access)")
            for p in core_papers:
                _paper_expander(p, "CORE")

        # Zenodo Datasets
        if datasets:
            st.markdown("#### Zenodo Research Datasets")
            for ds in datasets:
                ds_title = ds.get("title") or ds.get("dataset_id", "Unknown")
                with st.expander(f"🗄️ {ds_title}", expanded=False):
                    st.caption(f"ID: `{ds.get('dataset_id', '')}` | Source: **Zenodo**")
                    if ds.get("url"):
                        st.markdown(f"[View on Zenodo]({ds['url']})")
                    if ds.get("tags"):
                        st.markdown(f"**Keywords:** {', '.join(ds['tags'])}")
                    if ds.get("description"):
                        st.markdown(ds["description"])
    else:
        st.info("Enter a query above to search for papers and datasets across arXiv, OpenAlex, CrossRef, CORE, and Zenodo.")

# --- Tab 2: MIT OCW Courses ---
with tab_ocw:
    st.subheader("MIT OpenCourseWare — Supply Chain as Systems Engineering")
    st.markdown(
        "Search MIT's free academic catalogue. Supply chain structures are a physical "
        "manifestation of systems engineering — so queries like **systems engineering**, "
        "**operations research**, and **stochastic processes** are as relevant as "
        "**supply chain management** itself."
    )

    ocw_query = st.text_input(
        "OCW search query",
        value="supply chain systems engineering",
        key="ocw_query",
    )

    if st.button("Search MIT OCW", key="ocw_search_btn"):
        with st.spinner(f"Searching MIT OCW for '{ocw_query}'…"):
            ocw_results = search_ocw_interactive(ocw_query)
        st.session_state["_ocw_results"] = ocw_results

    ocw_results = st.session_state.get("_ocw_results") or []
    if ocw_results:
        st.success(f"Found {len(ocw_results)} courses")
        for course in ocw_results:
            title = course.get("title", course.get("course_id", "Unknown"))
            num   = course.get("course_number", "")
            url   = course.get("url", "")
            subjs = course.get("subjects", [])
            slug  = course.get("course_id", "")
            label = f"🎓 [{num}] {title}" if num else f"🎓 {title}"
            with st.expander(label, expanded=False):
                if url:
                    st.markdown(f"[Open on MIT OCW]({url})")
                if subjs:
                    st.markdown(f"**Subjects:** {', '.join(subjs)}")
                st.caption(f"Query: *{course.get('query', '')}*")

                detail_key = f"_ocw_detail::{slug}"
                cols = st.columns([1, 1, 4])
                if cols[0].button("🌐 Deep-fetch links", key=f"deep_{slug}"):
                    with st.spinner(f"Crawling {slug}…"):
                        st.session_state[detail_key] = fetch_ocw_course_detail(slug)
                if cols[1].button("💾 Persist to corpus", key=f"persist_{slug}"):
                    with st.spinner(f"Persisting {slug} to corpus…"):
                        res = deepen_ocw_course(slug)
                    st.success(
                        f"Wrote {res.get('rows_written',0)} rows · "
                        f"{res.get('resources',0)} resources · "
                        f"{res.get('related',0)} related · "
                        f"{res.get('external',0)} external"
                    )

                detail = st.session_state.get(detail_key)
                if detail:
                    st.markdown("---")
                    if detail.get("description"):
                        st.markdown(f"**Description:** {detail['description']}")
                    if detail.get("level"):
                        st.markdown(f"**Level:** {detail['level']}")
                    if detail.get("instructors"):
                        st.markdown(
                            "**Instructors:** "
                            + ", ".join(detail["instructors"])
                        )
                    if detail.get("topics"):
                        st.markdown(
                            "**Topics:** "
                            + ", ".join(detail["topics"][:15])
                        )
                    res_list = detail.get("resources") or []
                    rel_list = detail.get("related_courses") or []
                    ext_list = detail.get("external_links") or []
                    rcols = st.columns(3)
                    rcols[0].metric("Course resources", len(res_list))
                    rcols[1].metric("Related courses", len(rel_list))
                    rcols[2].metric("External links",  len(ext_list))

                    if res_list:
                        with st.expander(f"📚 {len(res_list)} course resources",
                                         expanded=False):
                            for r in res_list:
                                st.markdown(
                                    f"- **[{r.get('kind','page')}]** "
                                    f"[{r.get('label','(untitled)')}]({r.get('url','')})"
                                )
                    if rel_list:
                        with st.expander(f"🔗 {len(rel_list)} related courses",
                                         expanded=False):
                            for rc in rel_list:
                                st.markdown(
                                    f"- [{rc.get('slug','?')}]({rc.get('url','')})"
                                )
                    if ext_list:
                        with st.expander(f"🌍 {len(ext_list)} external references",
                                         expanded=False):
                            for ext in ext_list:
                                st.markdown(
                                    f"- *{ext.get('domain','')}* — "
                                    f"[{ext.get('label','(link)')}]({ext.get('url','')})"
                                )
    elif "_ocw_results" in st.session_state:
        st.info("No courses found — OCW may be temporarily unavailable or the query returned no matches.")

    st.divider()

    # ----- Cascade BFS deep-fetch (standard + adaptive modes) -----
    st.markdown("#### 🕸️ Cascade Graph Expansion")
    st.caption(
        "Start from any course slug and BFS-traverse the OCW knowledge graph. "
        "**Adaptive mode** adds semantic edge ranking, Adam inflection detection, "
        "and endpoint tunnel bias to extend traversal through relevance valleys."
    )

    _adapt_mode = st.toggle(
        "⚙️ Adaptive mode — semantic edge potentials + Adam phase-shift + tunnel bias",
        value=False,
        key="cascade_adaptive_toggle",
    )

    c1, c2, c3 = st.columns([3, 1, 1])
    cascade_slug = c1.text_input(
        "Seed course slug",
        placeholder="e.g. 15-762j-supply-chain-planning-spring-2011",
        key="cascade_slug",
    )
    cascade_hops = c2.number_input("Hops", min_value=1, max_value=4, value=2, key="cascade_hops")
    cascade_fan  = c3.number_input("Fan-out", min_value=2, max_value=10, value=5, key="cascade_fan")

    if _adapt_mode:
        with st.expander("🔬 Adaptive tuning parameters", expanded=False):
            ac1, ac2, ac3, ac4 = st.columns(4)
            decay_lambda    = ac1.number_input("Edge decay λ",  min_value=0.5, max_value=8.0, value=2.5, step=0.5, key="cas_lambda")
            tunnel_coeff    = ac2.number_input("Tunnel κ",      min_value=0.0, max_value=1.0, value=0.35, step=0.05, key="cas_tunnel")
            adam_beta1      = ac3.number_input("Adam β₁",       min_value=0.5, max_value=0.99, value=0.9,  step=0.05, key="cas_b1")
            adam_beta2      = ac4.number_input("Adam β₂",       min_value=0.9, max_value=0.9999, value=0.999, step=0.001, key="cas_b2", format="%.4f")
        endpoint_input = st.text_area(
            "Endpoint concept cluster (one concept per line)",
            value=(
                "supply chain optimization machine learning\n"
                "inventory management deep learning reinforcement\n"
                "logistics network optimization operations research\n"
                "demand forecasting neural network probabilistic"
            ),
            height=100,
            key="cascade_endpoints",
        )
    else:
        decay_lambda = 2.5
        tunnel_coeff = 0.35
        adam_beta1   = 0.9
        adam_beta2   = 0.999
        endpoint_input = ""

    _btn_label = "🚀 Launch Adaptive Cascade" if _adapt_mode else "🚀 Launch Cascade Deepen"
    if st.button(_btn_label, key="cascade_btn", disabled=not cascade_slug):
        if _adapt_mode:
            endpoint_concepts = [
                ln.strip() for ln in endpoint_input.splitlines() if ln.strip()
            ] or None
            with st.spinner(
                f"Adaptive BFS from '{cascade_slug}' — up to {cascade_hops} hops "
                f"× {cascade_fan} fan-out · semantic edge ranking · Adam inflection detection…"
            ):
                cascade_result = adaptive_cascade_ocw(
                    cascade_slug.strip(),
                    max_hops=int(cascade_hops),
                    fan_out=int(cascade_fan),
                    endpoint_concepts=endpoint_concepts,
                    decay_lambda=float(decay_lambda),
                    tunneling_coeff=float(tunnel_coeff),
                    beta1=float(adam_beta1),
                    beta2=float(adam_beta2),
                )
        else:
            with st.spinner(
                f"BFS traversal from '{cascade_slug}' — up to {cascade_hops} hops "
                f"× {cascade_fan} branches each…"
            ):
                cascade_result = cascade_deepen_ocw(
                    cascade_slug.strip(),
                    hops=int(cascade_hops),
                    fan_out=int(cascade_fan),
                )
        st.session_state["_cascade_result"] = cascade_result
        st.session_state["_cascade_adaptive"] = _adapt_mode

    cr = st.session_state.get("_cascade_result")
    if cr:
        deepened = cr.get("courses_deepened", [])
        st.success(
            f"Traversal complete — **{len(deepened)} courses** deepened · "
            f"**{cr.get('rows_written', 0)}** corpus rows written · "
            f"**{cr.get('resources', 0)}** resources · "
            f"**{cr.get('related', 0)}** related · "
            f"**{cr.get('external', 0)}** external links"
        )

        # Adaptive-mode extras
        if st.session_state.get("_cascade_adaptive"):
            phase_shifts = cr.get("phase_shifts", [])
            adam_rep     = cr.get("adam_report", {})
            hop_sigs     = cr.get("hop_signals", {})
            eff_hops     = cr.get("effective_hops", cr.get("max_hops", "?"))
            max_hops_v   = cr.get("max_hops", "?")

            _ps_col, _ah_col = st.columns(2)
            with _ps_col:
                if phase_shifts:
                    st.info(
                        f"⚡ **{len(phase_shifts)} phase shift(s)** detected — "
                        f"hops extended: {max_hops_v} → {eff_hops}"
                    )
                    for ps in phase_shifts:
                        st.markdown(
                            f"  - Hop **{ps['hop']}**: Adam est `{ps['adam_estimate']}` · "
                            f"phase amp `{ps['phase_amp']}` · mean signal `{ps['mean_signal']}`"
                        )
                else:
                    st.caption(f"No inflection detected — traversal ran at base hops ({max_hops_v})")

            with _ah_col:
                if hop_sigs:
                    st.markdown("**Hop-level signals (Adam input):**")
                    for hop, sig in sorted(hop_sigs.items()):
                        bar = "█" * max(1, int(sig * 20))
                        st.markdown(f"Hop {hop}: `{sig:.3f}` {bar}")

            with st.expander("🧮 Adam tracker state", expanded=False):
                st.json(adam_rep)

            ep_dict = cr.get("edge_potentials", {})
            if ep_dict:
                with st.expander(
                    f"🔗 Edge potential map ({len(ep_dict)} nodes)", expanded=False
                ):
                    ranked_ep = sorted(ep_dict.items(), key=lambda x: x[1], reverse=True)
                    for slug_ep, pot in ranked_ep:
                        bar = "▓" * max(1, int(pot * 20))
                        st.markdown(
                            f"[{slug_ep}](https://ocw.mit.edu/courses/{slug_ep}/) "
                            f"— potential `{pot:.4f}` {bar}"
                        )

        if deepened:
            with st.expander(f"Courses visited ({len(deepened)})", expanded=False):
                for s in deepened:
                    st.markdown(f"- [{s}](https://ocw.mit.edu/courses/{s}/)")

    st.divider()
    st.markdown("#### Monitored OCW Topics (Brain auto-sweeps)")
    cols = st.columns(3)
    for i, t in enumerate(_OCW_TOPICS):
        cols[i % 3].markdown(f"- {t}")

# --- Tab 3: Corpus Learnings ---
with tab_corpus:
    st.subheader("Accumulated ML Research Learnings")
    limit = st.slider("Show last N learnings", min_value=10, max_value=200, value=50, step=10)

    learnings = recent_ml_learnings(limit=limit)

    if not learnings:
        st.info(
            "No ML research learnings yet. "
            "The Brain will populate this on the next autonomous cycle, "
            "or click **Run research cycle now** in the sidebar."
        )
    else:
        st.markdown(f"**{len(learnings)} learnings** in the corpus (most recent first):")

        for entry in learnings:
            detail = entry.get("detail", {})
            entry_type = detail.get("type", "")
            if entry_type == "paper":
                kind_icon = "📄"
            elif entry_type == "dataset":
                kind_icon = "🗄️"
            elif entry_type == "ocw_course":
                kind_icon = "🎓"
            else:
                kind_icon = "🔬"
            title = (entry.get("title", "")
                     .replace("[ml_research] ", "")
                     .replace("[ml_dataset] ", "")
                     .replace("[ocw] ", ""))
            signal = entry.get("signal_strength") or 0.0
            logged_at = (entry.get("logged_at") or "")[:10]

            with st.expander(f"{kind_icon} {title} — signal: {signal:.2f} | {logged_at}", expanded=False):
                if entry_type == "paper":
                    paper = detail.get("paper", {})
                    st.markdown(f"**Topic:** {detail.get('topic', '')}")
                    if paper.get("url"):
                        st.markdown(f"[Open paper]({paper['url']})")
                    if paper.get("keywords"):
                        st.markdown(f"**Keywords:** {', '.join(paper['keywords'][:5])}")
                    if paper.get("summary"):
                        st.markdown(paper["summary"])
                    if paper.get("citations"):
                        st.markdown(f"**Citations:** {paper['citations']} | **Source:** {paper.get('source', '')}")
                elif entry_type == "dataset":
                    ds = detail.get("dataset", {})
                    st.markdown(f"**Topic:** {detail.get('topic', '')}")
                    if ds.get("url"):
                        st.markdown(f"[Open dataset]({ds['url']})")
                    if ds.get("tags"):
                        st.markdown(f"**Tags:** {', '.join(ds['tags'])}")
                    if ds.get("description"):
                        st.markdown(ds["description"])
                    st.markdown(
                        f"**Downloads:** {ds.get('downloads', 0):,} | "
                        f"**Likes:** {ds.get('likes', 0)}"
                    )
                elif entry_type == "ocw_course":
                    course = detail.get("course", {})
                    st.markdown(f"**Topic:** {detail.get('topic', '')}")
                    if course.get("url"):
                        st.markdown(f"[Open on MIT OCW]({course['url']})")
                    if course.get("subjects"):
                        st.markdown(f"**Subjects:** {', '.join(course['subjects'])}")
                    if course.get("course_number"):
                        st.caption(f"Course: {course['course_number']}")
                else:
                    # ml-intern deep-research output
                    st.markdown(f"**Prompt:** {detail.get('prompt', '')}")
                    if detail.get("output"):
                        st.markdown(detail["output"])

# --- Tab 4: Knowledge Graph ---
with tab_graph:
    st.subheader("🕸️ OCW Knowledge Graph — Harvested Link Lattice")
    st.caption(
        "Every deep-fetched course contributes its full link lattice to this graph: "
        "course pages, lecture notes, assignments, related courses, and external "
        "references from GitHub, journal sites, and university pages."
    )

    graph_limit = st.slider(
        "Show last N graph entries", min_value=20, max_value=500, value=100, step=20,
        key="graph_limit",
    )
    graph_entries = recent_ocw_details(limit=graph_limit)

    if not graph_entries:
        st.info(
            "No deep-fetched OCW data yet. "
            "Use the **🌐 Deep-fetch links** or **💾 Persist to corpus** "
            "buttons on a course in the MIT OCW tab, or run "
            "**Run research cycle now** — the Brain auto-deepens 3 courses per cycle."
        )
    else:
        details    = [e for e in graph_entries if e["kind"] == "ocw_course_detail"]
        resources  = [e for e in graph_entries if e["kind"] == "ocw_resource"]

        m1, m2, m3 = st.columns(3)
        m1.metric("Courses deep-fetched", len(details))
        m2.metric("Resource / link nodes", len(resources))
        m3.metric("Total corpus nodes", len(graph_entries))

        st.divider()

        # Group resources by course_id for display
        from collections import defaultdict
        by_course: dict[str, list[dict]] = defaultdict(list)
        for e in resources:
            cid = e["detail"].get("course_id", "unknown")
            by_course[cid].append(e)

        # Show each detailed course as an expandable card
        if details:
            st.markdown("### Course Detail Nodes")
            for e in details:
                d = e["detail"]
                slug = d.get("course_id", e["title"].replace("[ocw_detail] ", ""))
                desc = d.get("description", "")
                level = d.get("level", "")
                instructors = d.get("instructors", [])
                topics = d.get("topics", [])
                course_resources = by_course.get(slug, [])

                header = f"🎓 {slug}"
                if level:
                    header += f" · {level}"
                with st.expander(header, expanded=False):
                    if desc:
                        st.markdown(desc)
                    if instructors:
                        st.markdown(f"**Instructors:** {', '.join(instructors)}")
                    if topics:
                        st.markdown(f"**Topics:** {', '.join(topics[:12])}")

                    st.markdown(
                        f"[Open on MIT OCW](https://ocw.mit.edu/courses/{slug}/)"
                    )

                    # Partition resources by kind
                    by_kind: dict[str, list[dict]] = defaultdict(list)
                    for re_entry in course_resources:
                        rk = re_entry["detail"].get("resource_kind", "page")
                        by_kind[rk].append(re_entry)

                    if by_kind:
                        st.markdown("**Harvested Links:**")
                        for kind, items in sorted(by_kind.items()):
                            # Determine if these are internal resources, related courses, or external
                            if kind == "related_course":
                                icon = "🔗"
                            elif kind == "external_link":
                                icon = "🌍"
                            else:
                                icon = "📄"
                            with st.expander(
                                f"{icon} {kind.replace('-', ' ').title()} ({len(items)})",
                                expanded=False,
                            ):
                                for item in items:
                                    det = item["detail"]
                                    url = det.get("url", "")
                                    label = (
                                        det.get("label")
                                        or det.get("slug")
                                        or det.get("domain")
                                        or url
                                        or "(link)"
                                    )
                                    if url:
                                        st.markdown(f"- [{label}]({url})")
                                    else:
                                        st.markdown(f"- {label}")
                    else:
                        st.caption("No link data harvested yet for this course.")

        # Orphan resources (courses without a detail node)
        orphan_slugs = set(by_course.keys()) - {
            e["detail"].get("course_id", "") for e in details
        }
        if orphan_slugs:
            with st.expander(
                f"📦 {sum(len(by_course[s]) for s in orphan_slugs)} "
                f"orphan link nodes (courses pending deep-fetch)",
                expanded=False,
            ):
                for slug in sorted(orphan_slugs):
                    items = by_course[slug]
                    st.markdown(f"**{slug}** — {len(items)} link(s)")
                    for item in items[:5]:
                        det = item["detail"]
                        url = det.get("url", "")
                        label = det.get("label") or det.get("slug") or url or "(link)"
                        if url:
                            st.markdown(f"  - [{label}]({url})")

