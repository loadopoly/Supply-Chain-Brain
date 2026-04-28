"""
17_Document_RAG.py — Document Analysis page.

All RAG logic is delegated to the Brain's ``src.brain.doc_rag`` module.
The page provides the Streamlit UI; the Brain module owns the data directories,
the FAISS index, and the Proxy-Pointer-RAG integration.

Attribution
-----------
Structural RAG architecture adapted from **Proxy-Pointer** by the
Proxy-Pointer organisation: https://github.com/Proxy-Pointer/Proxy-Pointer-RAG
"""
import sys
from pathlib import Path
import streamlit as st

st.set_page_config(page_title="Document Analysis", page_icon="📄", layout="wide")

# ── Path setup — ensure pipeline root is importable ───────────────────────────
_PIPELINE = Path(__file__).resolve().parents[1]
if str(_PIPELINE) not in sys.path:
    sys.path.insert(0, str(_PIPELINE))

from src.brain.doc_rag import (   # noqa: E402
    is_ready, retrieve_doc_context, index_documents, query_documents,
    _DOCS_DIR, _INDEX_DIR,
)
from src.brain.operator_shell import render_operator_sidebar_fallback  # noqa: E402

render_operator_sidebar_fallback()

# ── UI ─────────────────────────────────────────────────────────────────────────
st.title("📄 Document Analysis")
st.caption(
    "Structural hierarchy-aware retrieval powered by the Brain's doc RAG service. "
    "RAG architecture adapted from [Proxy-Pointer](https://github.com/Proxy-Pointer/Proxy-Pointer-RAG) "
    "by the Proxy-Pointer organisation."
)

# ── Sidebar: index management ──────────────────────────────────────────────────
with st.sidebar:
    st.header("Index Management")

    docs = list(_DOCS_DIR.glob("*.md")) if _DOCS_DIR.exists() else []
    st.metric("Documents", len(docs))
    st.caption(f"`{_DOCS_DIR}`")

    index_exists = (_INDEX_DIR / "index.faiss").exists() if _INDEX_DIR.exists() else False
    st.caption(f"Index: {'✅ ready' if index_exists else '❌ not built yet'}")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Update index", use_container_width=True, type="primary"):
            with st.spinner("Indexing new documents…"):
                result = index_documents(fresh=False)
                if result.get("ok"):
                    st.success(result.get("message", "Done."))
                    st.rerun()
                else:
                    st.error(result.get("message", "Index failed. Check GOOGLE_API_KEY."))
    with col2:
        if st.button("Rebuild fresh", use_container_width=True):
            with st.spinner("Rebuilding from scratch…"):
                result = index_documents(fresh=True)
                if result.get("ok"):
                    st.success(result.get("message", "Done."))
                    st.rerun()
                else:
                    st.error(result.get("message", "Rebuild failed. Check GOOGLE_API_KEY."))

    st.divider()
    st.markdown("**Add documents**")
    st.caption(f"Drop `.md` files into `pipeline/data/documents/` then rebuild.")

    uploaded = st.file_uploader("Upload .md file", type=["md"], label_visibility="collapsed")
    if uploaded:
        _DOCS_DIR.mkdir(parents=True, exist_ok=True)
        dest = _DOCS_DIR / uploaded.name
        dest.write_bytes(uploaded.getvalue())
        st.success(f"Saved `{uploaded.name}`. Rebuild index to include it.")

# ── Query area ─────────────────────────────────────────────────────────────────
if not is_ready():
    st.info(
        "No documents indexed yet. "
        "Upload `.md` files using the sidebar, then click **Update index**."
    )
else:
    query = st.text_input(
        "Ask a question about your documents",
        placeholder="e.g. What are the lead time terms for supplier XYZ?",
    )

    if query:
        with st.spinner("Retrieving and synthesizing…"):
            answer = query_documents(query)
        st.markdown("### Answer")
        st.markdown(answer)

        with st.expander("Source passages", expanded=False):
            passages = retrieve_doc_context(query, k=5)
            if passages:
                for p in passages:
                    st.markdown(f"**{p['breadcrumb']}**")
                    st.text(p["text"][:600])
                    st.divider()
            else:
                st.caption("No source passages returned.")
