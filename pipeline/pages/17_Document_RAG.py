"""
17_Document_RAG.py — Proxy-Pointer Document Analysis
Structural RAG over supply chain documents using hierarchical pointer-based retrieval.
Requires GOOGLE_API_KEY in Proxy-Pointer-RAG/.env
"""
import sys
import os
from pathlib import Path
import streamlit as st

st.set_page_config(page_title="Document Analysis", page_icon="📄", layout="wide")

# ── Path setup ─────────────────────────────────────────────────────────────────
_WORKSPACE = Path(__file__).parent.parent.parent   # VS Code root
_RAG_ROOT  = _WORKSPACE / "Proxy-Pointer-RAG"

if not _RAG_ROOT.exists():
    st.error("Proxy-Pointer-RAG not found. Expected at: " + str(_RAG_ROOT))
    st.stop()

sys.path.insert(0, str(_RAG_ROOT))

# ── Load .env for this sub-project ─────────────────────────────────────────────
from dotenv import load_dotenv
load_dotenv(_RAG_ROOT / ".env")

_api_key = os.getenv("GOOGLE_API_KEY", "")

# ── UI ─────────────────────────────────────────────────────────────────────────
st.title("📄 Document Analysis (Proxy-Pointer RAG)")
st.caption("Structural hierarchy-aware retrieval — indexes full document sections, not truncated chunks.")

if not _api_key or _api_key == "your_gemini_api_key_here":
    st.warning(
        "**GOOGLE_API_KEY not set.** "
        "Add your Gemini API key to `Proxy-Pointer-RAG/.env` to use this page.\n\n"
        "Get a free key at https://aistudio.google.com/app/apikey"
    )
    st.code("GOOGLE_API_KEY=your_key_here", language="bash")
    st.stop()

# ── Lazy imports after env is validated ────────────────────────────────────────
try:
    import google.generativeai as genai
    from src.indexing.build_pp_index import build_proxy_index
    from src import config as pp_config
except Exception as e:
    st.error(f"Failed to import Proxy-Pointer-RAG modules: {e}")
    st.stop()

# ── Sidebar: index management ──────────────────────────────────────────────────
with st.sidebar:
    st.header("Index Management")

    data_dir = pp_config.DATA_DIR
    index_dir = pp_config.INDEX_DIR

    docs = list(data_dir.glob("*.md")) if data_dir.exists() else []
    st.metric("Indexed documents", len(docs))
    st.caption(f"Source: `{data_dir.relative_to(_RAG_ROOT)}`")

    index_exists = (index_dir / "index.faiss").exists() if index_dir.exists() else False
    st.caption(f"Index: {'✅ built' if index_exists else '❌ not built yet'}")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Build index", use_container_width=True, type="primary"):
            with st.spinner("Building index…"):
                try:
                    _orig_cwd = os.getcwd()
                    os.chdir(_RAG_ROOT)
                    build_proxy_index(incremental=True)
                    os.chdir(_orig_cwd)
                    st.success("Index built.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Build failed: {e}")
    with col2:
        if st.button("Rebuild fresh", use_container_width=True):
            with st.spinner("Rebuilding from scratch…"):
                try:
                    _orig_cwd = os.getcwd()
                    os.chdir(_RAG_ROOT)
                    build_proxy_index(incremental=False)
                    os.chdir(_orig_cwd)
                    st.success("Index rebuilt.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Rebuild failed: {e}")

    st.divider()
    st.markdown("**Add documents**")
    st.caption(f"Drop `.md` files into `{data_dir.relative_to(_RAG_ROOT)}` then rebuild the index.")

    uploaded = st.file_uploader("Upload .md file", type=["md"], label_visibility="collapsed")
    if uploaded:
        data_dir.mkdir(parents=True, exist_ok=True)
        dest = data_dir / uploaded.name
        dest.write_bytes(uploaded.getvalue())
        st.success(f"Saved `{uploaded.name}`. Rebuild index to include it.")

# ── Main: query interface ───────────────────────────────────────────────────────
if not index_exists:
    st.info("No index found. Upload documents and click **Build index** in the sidebar to get started.")
    st.stop()

try:
    from src.agent.pp_rag_bot import ProxyPointerRAG
    @st.cache_resource
    def get_bot():
        return ProxyPointerRAG()
    bot = get_bot()
except Exception as e:
    st.error(f"Failed to load RAG bot: {e}")
    st.stop()

query = st.text_input(
    "Ask a question about your documents",
    placeholder="e.g. What are the lead time terms for supplier XYZ?",
)

if query:
    with st.spinner("Retrieving and synthesizing…"):
        try:
            answer = bot.chat(query)
            st.markdown("### Answer")
            st.markdown(answer)
        except Exception as e:
            st.error(f"Query failed: {e}")
