"""doc_rag.py — Brain-native document RAG service.

Wraps the Proxy-Pointer-RAG pipeline as a first-class Brain service.  The
Brain owns the document data directories (``pipeline/data/documents/``,
``pipeline/data/trees/``, ``pipeline/data/index/``).  The cloned repo at
``Proxy-Pointer-RAG/`` is treated as a pure code library — its modules are
imported via ``sys.path`` injection with environment variables pre-set so
that Proxy-Pointer's ``src/config.py`` resolves to the Brain's data paths
instead of the repo-relative defaults.

Attribution
-----------
The structural RAG architecture (skeleton tree builder, pointer-based
hierarchy retrieval, LLM re-ranker) is derived from the **Proxy-Pointer**
project by the Proxy-Pointer organisation:

    https://github.com/Proxy-Pointer/Proxy-Pointer-RAG

Proxy-Pointer achieves vectorless accuracy at vector RAG scale by indexing
structural document pointers rather than raw text chunks, enabling full
section retrieval guided by hierarchical path re-ranking.

Public API
----------
:func:`retrieve_doc_context`
    Return a list of ``{breadcrumb, text}`` dicts for a free-text query.
    Used by ``dbi_rag.py`` as a third retrieval source.

:func:`index_documents`
    Build or incrementally update the FAISS index from ``data/documents/``.
    Called by the autonomous agent loop on each cycle.

:func:`is_ready`
    ``True`` when the FAISS index exists and at least one ``.md`` document
    is present.  Used to gate graceful fallback paths.

All functions degrade silently when ``GOOGLE_API_KEY`` is absent, the
dependencies are not installed, or no documents have been indexed yet.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Path layout — all Brain-owned (relative to this file)
# ---------------------------------------------------------------------------
_BRAIN_DIR      = Path(__file__).resolve().parent          # pipeline/src/brain/
_PIPELINE_ROOT  = _BRAIN_DIR.parents[1]                    # pipeline/
_WORKSPACE_ROOT = _PIPELINE_ROOT.parent                    # VS Code/
_RAG_ROOT       = _WORKSPACE_ROOT / "Proxy-Pointer-RAG"   # cloned repo

_DOCS_DIR   = _PIPELINE_ROOT / "data" / "documents"
_TREES_DIR  = _PIPELINE_ROOT / "data" / "trees"
_INDEX_DIR  = _PIPELINE_ROOT / "data" / "index"

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ensure_dirs() -> None:
    """Create Brain-owned data directories if they don't yet exist."""
    for d in (_DOCS_DIR, _TREES_DIR, _INDEX_DIR):
        d.mkdir(parents=True, exist_ok=True)


def _api_key() -> str | None:
    """Return GOOGLE_API_KEY from env or Proxy-Pointer-RAG/.env."""
    key = os.environ.get("GOOGLE_API_KEY", "").strip()
    if key:
        return key
    env_file = _RAG_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("GOOGLE_API_KEY"):
                parts = line.split("=", 1)
                if len(parts) == 2:
                    candidate = parts[1].strip().strip('"').strip("'")
                    if candidate and candidate != "your_gemini_api_key_here":
                        return candidate
    return None


def _inject_pp_path() -> bool:
    """Add Proxy-Pointer-RAG root to sys.path if present.  Returns success."""
    rag_root = str(_RAG_ROOT)
    if not _RAG_ROOT.exists():
        log.debug(f"doc_rag: Proxy-Pointer-RAG not found at {_RAG_ROOT}")
        return False
    if rag_root not in sys.path:
        sys.path.insert(0, rag_root)
    return True


def _set_env_for_pp() -> None:
    """Pre-set env vars so Proxy-Pointer src/config.py uses Brain paths.

    Must be called BEFORE any ``from src.config import …`` in the PP codebase
    because config.py resolves paths at module-import time via os.getenv().
    """
    os.environ.setdefault("PP_DATA_DIR",   str(_DOCS_DIR))
    os.environ.setdefault("PP_TREES_DIR",  str(_TREES_DIR))
    os.environ.setdefault("PP_INDEX_DIR",  str(_INDEX_DIR))
    key = _api_key()
    if key:
        os.environ.setdefault("GOOGLE_API_KEY", key)


# Module-level cache — ProxyPointerRAG instance (lazy, one-per-process)
_rag_instance: Any | None = None


def _load_rag(force_reload: bool = False) -> Any | None:
    """Load and cache a ``ProxyPointerRAG`` instance.

    Returns ``None`` on any error (missing key, index not built, etc.).
    """
    global _rag_instance
    if _rag_instance is not None and not force_reload:
        return _rag_instance

    if not is_ready():
        log.debug("doc_rag: index not ready — skipping load.")
        return None

    key = _api_key()
    if not key:
        log.debug("doc_rag: GOOGLE_API_KEY not set — skipping load.")
        return None

    if not _inject_pp_path():
        return None

    _set_env_for_pp()

    try:
        import warnings
        warnings.filterwarnings("ignore", category=FutureWarning)
        # Force src.config to reload with our env vars if it was previously
        # imported with different path settings.
        if "src.config" in sys.modules:
            # Patch the module-level path constants to Brain paths
            import src.config as _cfg
            from pathlib import Path as _P
            _cfg.DATA_DIR   = _P(str(_DOCS_DIR))
            _cfg.TREES_DIR  = _P(str(_TREES_DIR))
            _cfg.INDEX_DIR  = _P(str(_INDEX_DIR))
            import google.generativeai as genai
            genai.configure(api_key=key)
        else:
            # First import — env vars already set, config will pick them up
            pass

        from src.agent.pp_rag_bot import ProxyPointerRAG  # noqa: PLC0415
        bot = ProxyPointerRAG(
            index_path=str(_INDEX_DIR),
            data_dir=str(_DOCS_DIR),
        )
        _rag_instance = bot
        log.info("doc_rag: ProxyPointerRAG instance loaded successfully.")
        return bot
    except Exception as exc:
        log.warning(f"doc_rag: failed to load ProxyPointerRAG: {exc}")
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def is_ready() -> bool:
    """Return True when the FAISS index file and at least one .md doc exist."""
    index_file = _INDEX_DIR / "index.faiss"
    has_index  = index_file.exists()
    has_docs   = any(_DOCS_DIR.glob("*.md")) if _DOCS_DIR.exists() else False
    return has_index and has_docs


def retrieve_doc_context(
    query: str,
    k: int = 3,
) -> list[dict[str, str]]:
    """Return up to ``k`` grounded document context snippets.

    Each item is a dict with keys:
    - ``breadcrumb``: full hierarchical path (``doc_id > section > subsection``)
    - ``text``: the source text from the matching document section

    Returns an empty list when the index isn't ready or any error occurs.
    """
    if not query or not query.strip():
        return []

    bot = _load_rag()
    if bot is None:
        return []

    try:
        pointers = bot.retrieve_unique_nodes(query, k_search=50, k_final=k)
        results: list[dict[str, str]] = []
        for p in pointers:
            breadcrumb = p.get("global_breadcrumb") or p.get("breadcrumb", "")
            # Try to load source text from the .md file for richer context
            doc_id   = p.get("doc_id", "")
            md_path  = _DOCS_DIR / f"{doc_id}.md"
            if md_path.exists():
                try:
                    lines = md_path.read_text(encoding="utf-8").splitlines(keepends=True)
                    start = int(p.get("start_line", 0))
                    end   = int(p.get("end_line", start + 30))
                    text  = "".join(lines[start:end]).strip()
                except Exception:
                    text = p.get("content", "")
            else:
                text = p.get("content", "")

            if breadcrumb and text:
                results.append({"breadcrumb": breadcrumb, "text": text})

        return results
    except Exception as exc:
        log.warning(f"doc_rag.retrieve_doc_context failed: {exc}")
        # Invalidate cached instance so next call tries a fresh load
        global _rag_instance
        _rag_instance = None
        return []


def index_documents(fresh: bool = False) -> dict[str, Any]:
    """Build or incrementally update the FAISS document index.

    Parameters
    ----------
    fresh:
        When ``True`` the existing index is deleted and rebuilt from scratch.
        Default is incremental (new/changed documents only).

    Returns a summary dict with keys ``ok``, ``message``, and optionally
    ``error``.  Never raises.
    """
    key = _api_key()
    if not key:
        return {"ok": False, "message": "GOOGLE_API_KEY not configured"}

    _ensure_dirs()

    # Check if any documents exist
    docs = list(_DOCS_DIR.glob("*.md"))
    if not docs:
        return {"ok": False, "message": f"No .md documents found in {_DOCS_DIR}"}

    if not _inject_pp_path():
        return {"ok": False, "message": f"Proxy-Pointer-RAG not found at {_RAG_ROOT}"}

    _set_env_for_pp()

    try:
        import warnings
        warnings.filterwarnings("ignore", category=FutureWarning)

        # Patch config paths before import (or re-patch if already imported)
        if "src.config" in sys.modules:
            import src.config as _cfg
            from pathlib import Path as _P
            _cfg.DATA_DIR  = _P(str(_DOCS_DIR))
            _cfg.TREES_DIR = _P(str(_TREES_DIR))
            _cfg.INDEX_DIR = _P(str(_INDEX_DIR))
            import google.generativeai as genai
            genai.configure(api_key=key)

        from src.indexing.build_pp_index import build_proxy_index  # noqa: PLC0415

        # If fresh, remove existing index so build_proxy_index starts clean
        if fresh:
            import shutil
            if _INDEX_DIR.exists():
                shutil.rmtree(_INDEX_DIR)
            _INDEX_DIR.mkdir(parents=True, exist_ok=True)
            log.info("doc_rag.index_documents: removed existing index for fresh build.")

        build_proxy_index(incremental=not fresh)

        # Reload instance after re-indexing
        global _rag_instance
        _rag_instance = None  # force reload on next retrieve call

        log.info(
            f"doc_rag.index_documents: {'fresh' if fresh else 'incremental'} "
            f"index build complete ({len(docs)} documents)."
        )
        return {
            "ok": True,
            "message": f"Indexed {len(docs)} document(s)",
            "fresh": fresh,
        }
    except Exception as exc:
        log.warning(f"doc_rag.index_documents failed: {exc}")
        return {"ok": False, "message": str(exc), "error": repr(exc)}


def query_documents(question: str) -> str:
    """Answer a free-text question using the full RAG pipeline.

    Returns the synthesized answer string, or an empty string on any failure.
    Used by the Document Analysis page for interactive Q&A.
    """
    bot = _load_rag()
    if bot is None:
        if not is_ready():
            return "(No documents indexed yet. Add .md files and run index.)"
        if not _api_key():
            return "(GOOGLE_API_KEY not configured. Add it to Proxy-Pointer-RAG/.env)"
        return "(Document RAG unavailable — check logs for details.)"

    try:
        return bot.chat(question)
    except Exception as exc:
        log.warning(f"doc_rag.query_documents failed: {exc}")
        global _rag_instance
        _rag_instance = None
        return f"(Query failed: {exc})"
