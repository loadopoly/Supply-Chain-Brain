"""18_ML_Research.py — Brain ML Research Hub.

Surfaces the Brain's ``ml_research`` module as an interactive Streamlit page.
Browse papers, datasets, and MIT OpenCourseWare courses the Brain has
discovered from HuggingFace Papers, Semantic Scholar, HuggingFace Datasets,
and the MIT OCW academic catalogue.

Supply chain structures are an advanced form of systems engineering in the
physical realm — so the OCW crawl intentionally spans both supply chain
management and the foundational systems engineering / operations research
disciplines that underpin it.

Inspired by the HuggingFace ``ml-intern`` project:
https://github.com/huggingface/ml-intern
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
    _SUPPLY_CHAIN_TOPICS,
    _OCW_TOPICS,
)

st.set_page_config(page_title="ML Research Hub", page_icon="🔬", layout="wide")
st.title("🔬 ML Research Hub")
st.caption(
    "Brain-native research discovery — HuggingFace Papers, Semantic Scholar, "
    "HuggingFace Datasets, and **MIT OpenCourseWare**. "
    "Supply chain structures are an advanced form of systems engineering; "
    "OCW coverage spans both domains. "
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
tab_search, tab_ocw, tab_corpus = st.tabs(["🔍 Live Search", "🎓 MIT OCW", "📚 Corpus Learnings"])

# --- Tab 1: Live search ---
with tab_search:
    st.subheader("Search Papers & Datasets")
    query = st.text_input(
        "Research query",
        placeholder="e.g. demand forecasting transformer supply chain",
        key="ml_query",
    )

    if query:
        with st.spinner(f"Searching for '{query}'…"):
            results = search_papers_interactive(query)

        hf_papers = results.get("hf_papers", [])
        s2_papers = results.get("s2_papers", [])
        datasets = results.get("datasets", [])

        col1, col2, col3 = st.columns(3)
        col1.metric("HF Papers", len(hf_papers))
        col2.metric("Semantic Scholar", len(s2_papers))
        col3.metric("HF Datasets", len(datasets))

        # HF Papers
        if hf_papers:
            st.markdown("#### HuggingFace Papers")
            for p in hf_papers:
                with st.expander(f"📄 {p.get('title', 'Unknown')} — ↑{p.get('upvotes', 0)}", expanded=False):
                    st.markdown(f"**arxiv:** `{p.get('arxiv_id', '')}` | **upvotes:** {p.get('upvotes', 0)}")
                    if p.get("url"):
                        st.markdown(f"[View on HuggingFace]({p['url']})")
                    if p.get("keywords"):
                        st.markdown(f"**Keywords:** {', '.join(p['keywords'][:5])}")
                    if p.get("summary"):
                        st.markdown(p["summary"])

        # Semantic Scholar Papers
        if s2_papers:
            st.markdown("#### Semantic Scholar")
            for p in s2_papers:
                with st.expander(
                    f"📄 {p.get('title', 'Unknown')} — {p.get('citations', 0)} citations ({p.get('year', '?')})",
                    expanded=False,
                ):
                    st.markdown(f"**arxiv:** `{p.get('arxiv_id', '')}` | **citations:** {p.get('citations', 0)}")
                    if p.get("url"):
                        st.markdown(f"[View on arXiv]({p['url']})")
                    if p.get("summary"):
                        st.markdown(p["summary"])

        # HF Datasets
        if datasets:
            st.markdown("#### HuggingFace Datasets")
            for ds in datasets:
                with st.expander(
                    f"🗄️ {ds.get('dataset_id', 'Unknown')} — {ds.get('downloads', 0):,} downloads",
                    expanded=False,
                ):
                    st.markdown(f"**ID:** `{ds.get('dataset_id', '')}` | **downloads:** {ds.get('downloads', 0):,} | **likes:** {ds.get('likes', 0)}")
                    if ds.get("url"):
                        st.markdown(f"[View on HuggingFace]({ds['url']})")
                    if ds.get("tags"):
                        st.markdown(f"**Tags:** {', '.join(ds['tags'])}")
                    if ds.get("description"):
                        st.markdown(ds["description"])
    else:
        st.info("Enter a query above to search for ML papers and datasets.")

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

        if ocw_results:
            st.success(f"Found {len(ocw_results)} courses")
            for course in ocw_results:
                title = course.get("title", course.get("course_id", "Unknown"))
                num   = course.get("course_number", "")
                url   = course.get("url", "")
                subjs = course.get("subjects", [])
                label = f"🎓 [{num}] {title}" if num else f"🎓 {title}"
                with st.expander(label, expanded=False):
                    if url:
                        st.markdown(f"[Open on MIT OCW]({url})")
                    if subjs:
                        st.markdown(f"**Subjects:** {', '.join(subjs)}")
                    st.caption(f"Query: *{course.get('query', '')}*")
        else:
            st.info("No courses found — OCW may be temporarily unavailable or the query returned no matches.")

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
