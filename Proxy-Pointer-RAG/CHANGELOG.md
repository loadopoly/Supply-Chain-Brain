# Changelog

All notable changes to Proxy-Pointer-RAG are documented here.

---

## [1.1.0] — 2026-04-23

### Added
- `data/documents/autocatalytic_agent_networks.md` — Vision document injected into the Brain's RAG knowledge base. Covers Autocatalytic Agent Networks in closed-loop environments including cross-tunnel worming, syncopatic synapse creation, symbiotic value return, and iteratively unbounded system growth. Serves as a directive Vision artifact enabling the Brain to reason about and autonomously expand its own architecture.

---

## [1.0.0] — Initial Release

### Added
- Structural RAG pipeline with hierarchical Proxy-Pointer retrieval
- Skeleton tree builder: pure-Python Markdown to structural tree parser
- LLM noise filter for TOC, glossary, foreword, and reference sections
- FAISS indexing with Gemini 1536-d embeddings
- LLM re-ranker selecting top structural sections by hierarchical path
- Interactive RAG bot (`src/agent/pp_rag_bot.py`)
- Automated benchmarking with LLM-as-a-judge (`src/agent/benchmark.py`)
- FinanceBench evaluation: 100% accuracy at k=5 across 26 questions
- Comprehensive benchmark: 100% accuracy at k=5 across 40 complex financial reasoning questions
- Pre-extracted AMD FY2022 10-K (`data/documents/AMD.md`) for quickstart
- Additional Markdown files for American Express, Boeing, and PepsiCo in `data/documents/md_files/`
