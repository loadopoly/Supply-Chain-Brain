"""ml_research.py — Brain-native ML research module.

Fetches academic papers and datasets from multiple free, open APIs that work
through corporate SSL inspection (truststore-wired throughout):

* **arXiv API**   — CS/ML/math preprint server; authoritative ML paper source
* **OpenAlex**    — open bibliographic database, 200 M+ scholarly works
* **CrossRef**    — DOI-indexed published paper metadata (900 K+ SC results)
* **CORE**        — aggregator of 300 M+ open-access papers with full-text links
* **NASA NTRS**   — technical reports for systems engineering / ops research
* **Zenodo**      — open research data repository (5 K+ supply-chain datasets)
* **MIT OCW**     — 2 500+ free courses scored by keyword overlap via sitemap

Every discovered paper / dataset is persisted as a ``kind="ml_research"`` entry
in ``learning_log`` so it becomes a first-class signal in the Brain's corpus
and surfaces in DBI RAG insight generation alongside session-recall and
document-RAG context.

Usage (standalone)::

    from src.brain.ml_research import research_supply_chain_topics
    result = research_supply_chain_topics()
    # {topics_researched, papers_found, datasets_found, learnings_written}

The ``ml-intern`` CLI is used as an *optional* deep-research fallback when it
is installed and ``ANTHROPIC_API_KEY`` is set.  If unavailable the lightweight
path runs instead — no exception propagates.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Supply-chain ML research topics
# — updated each cycle; the Brain steadily deepens knowledge across these
#   domains as new papers land on HuggingFace and Semantic Scholar.
# ---------------------------------------------------------------------------
_SUPPLY_CHAIN_TOPICS: list[str] = [
    # ── Demand & Forecasting ────────────────────────────────────────────────
    "demand forecasting supply chain transformer",
    "probabilistic demand forecasting uncertainty quantification",
    "intermittent demand forecasting sparse time series",
    "causal demand forecasting external signals",
    "hierarchical forecasting reconciliation",
    # ── Inventory & Replenishment ───────────────────────────────────────────
    "inventory optimization deep learning",
    "safety stock optimization stochastic demand",
    "multi-echelon inventory policy reinforcement learning",
    "vendor managed inventory machine learning",
    "spare parts inventory forecasting",
    # ── Delivery & Logistics ────────────────────────────────────────────────
    "on-time delivery prediction logistics",
    "last mile delivery optimization neural network",
    "dynamic vehicle routing deep reinforcement learning",
    "freight cost prediction machine learning",
    "carrier performance analytics predictive",
    # ── Manufacturing & Production ──────────────────────────────────────────
    "time series forecasting manufacturing",
    "production scheduling reinforcement learning",
    "predictive maintenance industrial IoT machine learning",
    "yield prediction semiconductor manufacturing deep learning",
    "lean manufacturing digital twin simulation",
    # ── Procurement & Sourcing ──────────────────────────────────────────────
    "procurement analytics machine learning",
    "purchase order anomaly detection",
    "spend analysis natural language processing",
    "contract risk extraction large language model",
    "supplier selection multi-criteria decision making",
    # ── Supplier & Risk ─────────────────────────────────────────────────────
    "supplier risk prediction neural network",
    "supply chain disruption detection graph neural network",
    "geopolitical risk supply chain natural language processing",
    "supply chain resilience stress testing simulation",
    "multi-tier supplier visibility knowledge graph",
    # ── Network & Optimization ──────────────────────────────────────────────
    "logistics route optimization graph neural network",
    "distribution network design optimization",
    "warehouse slotting optimization machine learning",
    "cross-docking scheduling optimization",
    "port congestion prediction deep learning",
    # ── Planning & S&OP ─────────────────────────────────────────────────────
    "sales operations planning machine learning",
    "integrated business planning AI",
    "collaborative planning forecasting replenishment digital",
    "constraint-based production planning optimization",
    "master production schedule neural network",
    # ── Pricing & Revenue ───────────────────────────────────────────────────
    "dynamic pricing supply chain reinforcement learning",
    "price elasticity machine learning retail",
    "markdown optimization deep learning",
    "promotion uplift modeling causal inference",
    # ── Sustainability & ESG ────────────────────────────────────────────────
    "sustainable supply chain carbon footprint machine learning",
    "circular economy supply chain optimization",
    "scope 3 emissions supply chain analytics",
    "green logistics decarbonization AI",
    # ── Digital Twin & Simulation ───────────────────────────────────────────
    "supply chain digital twin simulation machine learning",
    "agent-based supply chain simulation",
    "discrete event simulation warehouse optimization",
    "scenario planning supply chain Monte Carlo",
    # ── Graph & Network Intelligence ────────────────────────────────────────
    "supply chain knowledge graph entity extraction",
    "supply chain network graph neural network link prediction",
    "trade flow network analysis deep learning",
    "bill of materials graph transformer",
    # ── NLP & Document Intelligence ─────────────────────────────────────────
    "invoice processing natural language processing",
    "purchase order extraction large language model",
    "supply chain news event extraction NLP",
    "supplier contract clause classification BERT",
    # ── Anomaly & Quality ───────────────────────────────────────────────────
    "supply chain anomaly detection unsupervised learning",
    "product quality prediction defect detection deep learning",
    "returns prediction reverse logistics machine learning",
    "counterfeit detection supply chain machine learning",
    # ── Emerging Technology ─────────────────────────────────────────────────
    "large language model supply chain planning",
    "retrieval augmented generation enterprise knowledge",
    "foundation model time series forecasting supply chain",
    "generative AI procurement automation",
    "federated learning supply chain privacy",
]

# Rotate through topics in round-robin so every cycle covers different ground.
# The cursor is persisted in brain_kv between runs.
_TOPICS_PER_CYCLE = 8   # increased from 5 — broader per-cycle coverage
_PAPERS_PER_TOPIC = 10  # increased from 8 — deeper per-topic acquisition
_DATASETS_PER_TOPIC = 8 # increased from 5 — more dataset signals per cycle

# ---------------------------------------------------------------------------
# Extended Research Topics — derived from the user's active Grok 3 research
# thread ("Introduction to Grok 3 and Capabilities", 553 responses).
#
# Two major tracks surface in that conversation:
#
#   1. UEQGM (Unified Equilibrium Quantum Gravity Model) — a theoretical
#      physics framework combining quantum wavefunction dynamics, biohybrid
#      quantum computing, topological materials, astrophysical timing signals
#      (FRBs, pulsars, muonic decay), and holographic entropy.
#
#   2. AI Knowledge Expansion — knowledge graph self-reference, ensemble LLM
#      architectures, RAG systems, archival data for AI training, and
#      spatio-temporal graph networks.
#
# The Brain fetches papers across these tracks in a separate round-robin so
# they do not crowd out the supply-chain rotation above.
# ---------------------------------------------------------------------------
_EXTENDED_RESEARCH_TOPICS: list[str] = [
    # ── Quantum Dynamics & Wavefunction Models ──────────────────────────────
    "unified quantum gravity model wavefunction observer",
    "Floquet quantum systems modulation photonic",
    "loop quantum gravity Ashtekar variables quantization",
    "holographic entropy Bekenstein-Hawking black hole information",
    "quantum phase transitions dissipative Kerr resonator",
    "parity-time symmetry photonic quantum entanglement filtering",
    "quantum fluctuations effective field theory vacuum",
    # ── Quantum Computing Architectures ────────────────────────────────────
    "superconducting qubit resonator coupling microwave",
    "niobium cavity quantum electrodynamics cryogenic",
    "Weyl semimetal topological nodal quantum circuit",
    "Bayesian quantum state tomography neural network",
    "quantum error correction surface code logical qubit",
    "spatio-temporal graph convolutional network ST-GCN",
    # ── Topological & Condensed Matter Physics ──────────────────────────────
    "moire superlattice topological moiré bilayer",
    "skyrmion plasmonic moire superlattice photonic",
    "Weyl node 1D lattice duality quantum circuit",
    "magnetic coupling qubit resonator proximity effect",
    "levitated optomechanics backaction suppression reflective",
    # ── Biohybrid & Biological Quantum Systems ──────────────────────────────
    "biohybrid quantum computing vesicle transport neural",
    "cryptochrome quantum coherence avian magnetic sensing",
    "vesicle axonal transport presynapse assembly phosphatidylinositol",
    "nanodisk lipid membrane quantum coherence",
    "biological vesicle flux electron transport quantum",
    # ── Astrophysics & Cosmological Timing ─────────────────────────────────
    "fast radio burst FRB timing cosmology millisecond",
    "muonic decay precision measurement quantum",
    "gravitational wave memory binary neutron star merger",
    "pulsar timing millisecond globular cluster",
    "Hubble constant local distance network measurement precision",
    "neutrino superradiance radioactive Bose-Einstein condensate laser",
    "gamma ray beta decay active galactic nucleus jet",
    "parity violating dispersion electron scattering weak force",
    # ── AI Knowledge Graph & Self-Referential Systems ───────────────────────
    "knowledge graph self-referential AI introspection",
    "recursive LLM feedback knowledge accumulation",
    "AI centroidal knowledge graph construction ontology",
    "meta-learning continual learning knowledge expansion",
    "ensemble LLM local inference RAG retrieval augmented",
    "archival data AI training historical corpus quality",
    "graph database knowledge representation RDF ontology",
    "document intelligence OCR knowledge graph construction",
    # ── Advanced ML Architectures (UEQGM-adjacent) ─────────────────────────
    "spatio-temporal neural network Bayesian graph physics",
    "neural ordinary differential equation physical system",
    "physics-informed neural network PDE constraint",
    "quantum machine learning variational circuit",
    "geometric deep learning symmetry equivariant",
    # ── Quipu & Organic Data Structures ─────────────────────────────────────
    "quipu data structure torsion organic computation",
    "topological data structure persistent homology",
    "fractal data structure self-similar information encoding",
]

_EXTENDED_TOPICS_PER_CYCLE = 5  # sweep 5 extended topics alongside SC topics
_EXTENDED_PAPERS_PER_TOPIC = 8  # papers per extended topic

# ---------------------------------------------------------------------------
# MIT OpenCourseWare — supply chain as systems engineering
#
# Supply chain structures are an advanced form of systems engineering in the
# physical realm.  These topics therefore span both disciplines so the Brain
# acquires academic depth across the full intellectual stack: logistics theory,
# operations research, industrial engineering, stochastic systems, and complex
# engineered networks — not just the narrow "supply chain analytics" framing.
# ---------------------------------------------------------------------------
_OCW_SEARCH_URL = "https://ocw.mit.edu/search/"
_OCW_COURSE_BASE = "https://ocw.mit.edu"

_OCW_TOPICS: list[str] = [
    # ── Core supply chain ───────────────────────────────────────────────────
    "supply chain management",
    "supply chain planning",
    "logistics systems",
    "global supply chains",
    "humanitarian logistics",
    # ── Systems engineering (parent discipline) ─────────────────────────────
    "systems engineering",
    "engineering systems design",
    "complex systems",
    "system dynamics",
    "sociotechnical systems",
    # ── Operations research / optimization ──────────────────────────────────
    "operations research",
    "network optimization",
    "stochastic processes",
    "integer programming",
    "convex optimization",
    "dynamic programming",
    "combinatorial optimization",
    # ── Manufacturing & production ───────────────────────────────────────────
    "manufacturing systems",
    "production planning",
    "industrial engineering",
    "lean production",
    "quality management",
    "advanced manufacturing",
    # ── Data science & machine learning ─────────────────────────────────────
    "machine learning",
    "deep learning",
    "data science",
    "reinforcement learning",
    "natural language processing",
    "time series analysis",
    "probabilistic systems",
    # ── Analytical foundations ───────────────────────────────────────────────
    "inventory theory",
    "queuing theory",
    "simulation modeling",
    "decision analysis",
    "risk analysis",
    "game theory",
    # ── Finance, economics & markets ────────────────────────────────────────
    "microeconomics",
    "econometrics",
    "financial engineering",
    "commodity markets",
    "pricing strategy",
    # ── Sustainability & policy ──────────────────────────────────────────────
    "sustainable development",
    "environmental engineering",
    "energy systems",
    "climate policy",
    # ── Digital & emerging tech ─────────────────────────────────────────────
    "digital transformation",
    "internet of things",
    "blockchain technology",
    "artificial intelligence",
]

_OCW_TOPICS_PER_CYCLE = 6   # increased from 4 — more OCW depth per cycle
_OCW_COURSES_PER_QUERY = 10 # increased from 8

# External API endpoints — all verified accessible through corporate SSL inspection
_ARXIV_API    = "https://export.arxiv.org/api/query"    # Atom XML, no auth
_OPENALEX_API = "https://api.openalex.org"              # JSON, 200 M+ works
_CROSSREF_API = "https://api.crossref.org"              # JSON, DOI metadata
_CORE_API     = "https://api.core.ac.uk/v3"             # JSON, open-access
_NTRS_API     = "https://ntrs.nasa.gov/api"             # JSON, systems-eng reports
_ZENODO_API   = "https://zenodo.org/api"                # JSON, research datasets
_HF_PAPERS_API   = "https://huggingface.co/api/daily_papers"  # JSON, trending ML papers
_HF_DATASETS_API = "https://huggingface.co/api/datasets"      # JSON, 100 K+ datasets
_HF_MODELS_API   = "https://huggingface.co/api/models"        # JSON, model hub index
_ARXIV_TIMEOUT    = 15
_OPENALEX_TIMEOUT = 10
_CROSSREF_TIMEOUT = 10
_CORE_TIMEOUT     = 10
_NTRS_TIMEOUT     = 10
_ZENODO_TIMEOUT   = 10
_HF_TIMEOUT       = 15
_OCW_TIMEOUT      = 25

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get_json(url: str, params: dict | None = None, timeout: int = 10) -> Any:
    """Synchronous HTTP GET → parsed JSON.  Returns None on any failure."""
    try:
        import ssl
        import urllib.request
        import urllib.parse
        # truststore uses the Windows certificate store — handles corporate SSL inspection
        try:
            import truststore
            _ssl_ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        except ImportError:
            try:
                import certifi
                _ssl_ctx = ssl.create_default_context(cafile=certifi.where())
            except ImportError:
                _ssl_ctx = ssl.create_default_context()
        full_url = url
        if params:
            full_url = url + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(full_url, headers={"User-Agent": "SupplyChainBrain/1.0"})
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        log.debug(f"ml_research._get_json({url}): {exc}")
        return None


def _get_text(url: str, params: dict | None = None, timeout: int = 15) -> str | None:
    """Synchronous HTTP GET → decoded text body (for XML and plain-text APIs)."""
    try:
        import ssl
        import urllib.request
        import urllib.parse
        try:
            import truststore
            _ssl_ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        except ImportError:
            try:
                import certifi
                _ssl_ctx = ssl.create_default_context(cafile=certifi.where())
            except ImportError:
                _ssl_ctx = ssl.create_default_context()
        full_url = url
        if params:
            full_url = url + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(full_url, headers={"User-Agent": "SupplyChainBrain/1.0"})
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as exc:
        log.debug(f"ml_research._get_text({url}): {exc}")
        return None


# ---------------------------------------------------------------------------
# Paper discovery helpers
# ---------------------------------------------------------------------------

def search_papers_arxiv(query: str, limit: int = _PAPERS_PER_TOPIC) -> list[dict]:
    """Search arXiv for papers matching ``query`` (Atom XML feed).

    Uses ``https://export.arxiv.org/api/query`` — free, no auth, never blocked.
    Returns paper dicts: ``arxiv_id``, ``title``, ``summary``, ``year``,
    ``authors``, ``url``, ``source="arxiv"``, ``query``.
    """
    import xml.etree.ElementTree as ET
    text = _get_text(
        _ARXIV_API,
        params={"search_query": f"all:{query}", "max_results": limit, "sortBy": "relevance"},
        timeout=_ARXIV_TIMEOUT,
    )
    if not text:
        return []
    results: list[dict] = []
    try:
        root = ET.fromstring(text)
        ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "arxiv": "http://arxiv.org/schemas/atom",
        }
        for entry in root.findall("atom:entry", ns):
            raw_id = (entry.findtext("atom:id", "", ns) or "").strip()
            arxiv_id = raw_id.split("/abs/")[-1].split("v")[0] if "/abs/" in raw_id else ""
            title = (entry.findtext("atom:title", "", ns) or "").strip().replace("\n", " ")
            summary = (entry.findtext("atom:summary", "", ns) or "").strip().replace("\n", " ")[:400]
            published = (entry.findtext("atom:published", "", ns) or "")[:4]
            year = int(published) if published.isdigit() else None
            authors = [
                (a.findtext("atom:name", "", ns) or "")
                for a in entry.findall("atom:author", ns)
            ][:4]
            results.append({
                "arxiv_id": arxiv_id,
                "title": title,
                "summary": summary,
                "year": year,
                "authors": authors,
                "upvotes": 0,
                "citations": 0,
                "keywords": [],
                "url": f"https://arxiv.org/abs/{arxiv_id}" if arxiv_id else raw_id,
                "source": "arxiv",
                "query": query,
            })
    except Exception as exc:
        log.debug(f"search_papers_arxiv: XML parse error: {exc}")
    return results


def search_papers_openalex(query: str, limit: int = _PAPERS_PER_TOPIC) -> list[dict]:
    """Search OpenAlex for papers — 200 M+ scholarly works, free, no auth.

    Replaces the Semantic Scholar API (blocked/rate-limited on corporate networks).
    Returns paper dicts: ``arxiv_id``, ``title``, ``summary``, ``citations``,
    ``year``, ``doi``, ``url``, ``source="openalex"``, ``query``.
    """
    data = _get_json(
        f"{_OPENALEX_API}/works",
        params={
            "search": query,
            "per-page": limit,
            # referenced_works gives us the paper's bibliography as OA IDs — no
            # extra request; they become immediate citation_chain seeds
            "select": "id,title,doi,publication_year,cited_by_count,concepts,primary_location,referenced_works",
            "mailto": "brain@supplychainbrain.local",  # polite-pool header
        },
        timeout=_OPENALEX_TIMEOUT,
    )
    if not isinstance(data, dict):
        return []
    results: list[dict] = []
    for w in (data.get("results") or [])[:limit]:
        doi = (w.get("doi") or "").replace("https://doi.org/", "").strip()
        openalex_id = (w.get("id") or "").replace("https://openalex.org/", "")
        # Concept labels serve as a summary proxy (abstract not returned by default)
        concepts = [
            c.get("display_name", "")
            for c in (w.get("concepts") or [])
            if c.get("score", 0) > 0.3
        ]
        summary = "; ".join(concepts[:6]) if concepts else ""
        loc = w.get("primary_location") or {}
        url = loc.get("landing_page_url", "") or (f"https://doi.org/{doi}" if doi else "")
        # Extract arXiv ID from URL if available
        arxiv_id = ""
        if url and "arxiv.org/abs/" in url:
            arxiv_id = url.split("/abs/")[-1].split("v")[0]
        elif doi and "arxiv" in doi.lower():
            arxiv_id = doi.split("/")[-1]
        # Capture bibliography as OA IDs — strip URL prefix to bare W-IDs
        ref_oa_ids = [
            r.replace("https://openalex.org/", "").strip()
            for r in (w.get("referenced_works") or [])
            if r
        ]
        results.append({
            "arxiv_id": arxiv_id,
            "openalex_id": openalex_id,
            "title": (w.get("title") or "").strip(),
            "summary": summary[:400],
            "citations": w.get("cited_by_count", 0),
            "year": w.get("publication_year"),
            "doi": doi,
            "upvotes": 0,
            "keywords": concepts[:5],
            "url": url,
            "source": "openalex",
            "query": query,
            "openalex_ref_ids": ref_oa_ids[:50],  # bibliography — first 50
        })
    return results


def search_papers_crossref(query: str, limit: int = 5) -> list[dict]:
    """Search CrossRef for published, peer-reviewed papers with DOIs.

    ``https://api.crossref.org/works`` \u2014 free, no auth, 130 M+ records.
    Returns paper dicts with ``source="crossref"``.
    """
    data = _get_json(
        f"{_CROSSREF_API}/works",
        params={"query": query, "rows": limit, "select": "title,DOI,published,author,subject"},
        timeout=_CROSSREF_TIMEOUT,
    )
    if not isinstance(data, dict):
        return []
    results: list[dict] = []
    for item in (data.get("message", {}).get("items") or [])[:limit]:
        titles = item.get("title") or [""]
        title = titles[0].strip() if titles else ""
        doi = (item.get("DOI") or "").strip()
        pub = item.get("published") or {}
        date_parts = (pub.get("date-parts") or [[None]])[0]
        year = date_parts[0] if date_parts else None
        authors_raw = item.get("author") or []
        authors = [
            f"{a.get('given', '')} {a.get('family', '')}".strip()
            for a in authors_raw[:3]
        ]
        subjects = item.get("subject") or []
        results.append({
            "arxiv_id": doi.replace("/", "_") if doi else title[:40],
            "title": title,
            "summary": "; ".join(subjects[:5]) if subjects else "",
            "year": year,
            "authors": authors,
            "doi": doi,
            "upvotes": 0,
            "citations": 0,
            "keywords": subjects[:5],
            "url": f"https://doi.org/{doi}" if doi else "",
            "source": "crossref",
            "query": query,
        })
    return results


def search_papers_core(query: str, limit: int = 5) -> list[dict]:
    """Search CORE open-access repository (300 M+ papers, free, no auth).

    ``https://api.core.ac.uk/v3/search/works`` \u2014 returns full-text links.
    Returns paper dicts with ``source="core"``.
    """
    data = _get_json(
        f"{_CORE_API}/search/works",
        params={"q": query, "limit": limit},
        timeout=_CORE_TIMEOUT,
    )
    if not isinstance(data, dict):
        return []
    results: list[dict] = []
    for r in (data.get("results") or [])[:limit]:
        arxiv_id = (r.get("arxivId") or "").strip()
        core_id = str(r.get("id") or "")
        outputs = r.get("outputs") or []
        url = outputs[0] if outputs else ""
        if arxiv_id:
            url = f"https://arxiv.org/abs/{arxiv_id}"
        abstract = (r.get("abstract") or "")[:400]
        authors = [(a.get("name") or "") for a in (r.get("authors") or [])[:3]]
        results.append({
            "arxiv_id": arxiv_id or f"core:{core_id}",
            "title": (r.get("title") or "").strip(),
            "summary": abstract,
            "citations": r.get("citationCount", 0),
            "year": None,
            "authors": authors,
            "upvotes": 0,
            "keywords": [],
            "url": url,
            "source": "core",
            "query": query,
        })
    return results


def search_ntrs(query: str, limit: int = 5) -> list[dict]:
    """Search NASA Technical Reports Server for systems-engineering papers.

    Best for systems engineering / operations research / manufacturing topics.
    ``https://ntrs.nasa.gov/api/citations/search`` \u2014 free, 100 K+ reports.
    Returns paper dicts with ``source="ntrs"``.
    """
    data = _get_json(
        f"{_NTRS_API}/citations/search",
        params={"q": query, "rows": limit},
        timeout=_NTRS_TIMEOUT,
    )
    if not isinstance(data, dict):
        return []
    results: list[dict] = []
    for r in (data.get("results") or [])[:limit]:
        ntrs_id = str(r.get("id") or "")
        abstract = (r.get("abstract") or "")[:400]
        authors = [(a.get("name") or "") for a in (r.get("authors") or [])[:3]]
        created = (r.get("created") or "")[:4]
        year = int(created) if created.isdigit() else None
        results.append({
            "arxiv_id": f"ntrs:{ntrs_id}",
            "title": (r.get("title") or "").strip(),
            "summary": abstract,
            "citations": 0,
            "year": year,
            "authors": authors,
            "upvotes": 0,
            "keywords": [],
            "url": f"https://ntrs.nasa.gov/citations/{ntrs_id}" if ntrs_id else "",
            "source": "ntrs",
            "query": query,
        })
    return results


def fetch_arxiv_recent(limit: int = 10) -> list[dict]:
    """Fetch recent arXiv papers in CS/ML/math/economics categories.

    Covers cs.LG (machine learning), cs.AI, math.OC (optimization & control),
    and econ.GN — all directly relevant to supply chain intelligence.

    Runs alongside the HuggingFace daily papers feed for complementary coverage.
    Returns paper dicts with ``source="arxiv_recent"``.
    """
    import xml.etree.ElementTree as ET
    cat_query = "cat:cs.LG OR cat:cs.AI OR cat:math.OC OR cat:econ.GN"
    text = _get_text(
        _ARXIV_API,
        params={
            "search_query": cat_query,
            "max_results": limit,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        },
        timeout=_ARXIV_TIMEOUT,
    )
    if not text:
        return []
    results: list[dict] = []
    try:
        root = ET.fromstring(text)
        ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "arxiv": "http://arxiv.org/schemas/atom",
        }
        for entry in root.findall("atom:entry", ns):
            raw_id = (entry.findtext("atom:id", "", ns) or "").strip()
            arxiv_id = raw_id.split("/abs/")[-1].split("v")[0] if "/abs/" in raw_id else ""
            title = (entry.findtext("atom:title", "", ns) or "").strip().replace("\n", " ")
            summary = (entry.findtext("atom:summary", "", ns) or "").strip().replace("\n", " ")[:400]
            published = (entry.findtext("atom:published", "", ns) or "")[:4]
            year = int(published) if published.isdigit() else None
            results.append({
                "arxiv_id": arxiv_id,
                "title": title,
                "summary": summary,
                "year": year,
                "upvotes": 0,
                "citations": 0,
                "keywords": [],
                "url": f"https://arxiv.org/abs/{arxiv_id}" if arxiv_id else raw_id,
                "source": "arxiv_recent",
                "query": "recent",
            })
    except Exception as exc:
        log.debug(f"fetch_arxiv_recent: parse error: {exc}")
    return results


def fetch_hf_daily_papers(limit: int = 10) -> list[dict]:
    """Fetch today's trending papers from the HuggingFace daily papers feed.

    ``https://huggingface.co/api/daily_papers`` — free, no auth required.
    Returns the same paper-dict shape as other sources so they flow into
    ``persist_research_findings`` and appear in the Brain corpus.
    Soft-fails silently if HuggingFace is unreachable (e.g. corporate block).
    """
    data = _get_json(_HF_PAPERS_API, timeout=_HF_TIMEOUT)
    if not isinstance(data, list):
        log.debug("fetch_hf_daily_papers: no data returned (blocked or empty)")
        return []
    results: list[dict] = []
    for item in data[:limit]:
        paper = item.get("paper") or item  # response can be list[paper] or list[{paper:...}]
        arxiv_id = (paper.get("id") or "").strip()
        title = (paper.get("title") or "").strip()
        summary = (paper.get("summary") or "")[:400].strip()
        upvotes = int(paper.get("upvotes") or 0)
        authors = [(a.get("name") or "") for a in (paper.get("authors") or [])[:3]]
        results.append({
            "arxiv_id": arxiv_id,
            "title": title,
            "summary": summary,
            "year": None,
            "upvotes": upvotes,
            "citations": 0,
            "keywords": [],
            "authors": authors,
            "url": f"https://huggingface.co/papers/{arxiv_id}" if arxiv_id else "https://huggingface.co/papers",
            "source": "hf_daily_papers",
            "query": "daily",
        })
    log.debug(f"fetch_hf_daily_papers: {len(results)} papers")
    return results


def discover_hf_datasets(query: str, limit: int = _DATASETS_PER_TOPIC) -> list[dict]:
    """Search HuggingFace Hub for datasets relevant to ``query``.

    ``https://huggingface.co/api/datasets`` — free, no auth, 100 K+ datasets.
    Sorted by downloads so the most-used datasets surface first.
    Soft-fails silently if HuggingFace is unreachable.
    Returns dataset dicts with ``source="hf_datasets"``.
    """
    data = _get_json(
        _HF_DATASETS_API,
        params={"search": query, "limit": limit, "sort": "downloads"},
        timeout=_HF_TIMEOUT,
    )
    if not isinstance(data, list):
        log.debug(f"discover_hf_datasets({query!r}): no data returned")
        return []
    results: list[dict] = []
    for item in data[:limit]:
        dataset_id = (item.get("id") or "").strip()
        tags = item.get("tags") or []
        downloads = int(item.get("downloads") or 0)
        likes = int(item.get("likes") or 0)
        results.append({
            "dataset_id": f"hf:{dataset_id}",
            "title": dataset_id,
            "description": f"HuggingFace dataset: {dataset_id}. Tags: {', '.join(tags[:5])}.",
            "downloads": downloads,
            "likes": likes,
            "tags": tags[:5],
            "url": f"https://huggingface.co/datasets/{dataset_id}" if dataset_id else "https://huggingface.co/datasets",
            "source": "hf_datasets",
            "query": query,
        })
    log.debug(f"discover_hf_datasets({query!r}): {len(results)} datasets")
    return results


def discover_zenodo_datasets(query: str, limit: int = _DATASETS_PER_TOPIC) -> list[dict]:
    """Search Zenodo for research datasets relevant to ``query``.

    ``https://zenodo.org/api/records`` — 5 000+ supply-chain datasets, free, no auth.
    Complements the HuggingFace Datasets API with peer-reviewed open data.
    Returns dataset dicts: ``dataset_id``, ``title``, ``description``,
    ``tags``, ``url``, ``source="zenodo"``, ``query``.
    """
    data = _get_json(
        f"{_ZENODO_API}/records",
        params={"q": query, "type": "dataset", "size": limit},
        timeout=_ZENODO_TIMEOUT,
    )
    if not isinstance(data, dict):
        return []
    results: list[dict] = []
    for hit in (data.get("hits", {}).get("hits") or [])[:limit]:
        meta = hit.get("metadata") or {}
        links = hit.get("links") or {}
        record_id = str(hit.get("id", ""))
        title = (meta.get("title") or "").strip()
        description = (meta.get("description") or "")[:300]
        keywords = meta.get("keywords") or []
        url = links.get("html", f"https://zenodo.org/record/{record_id}")
        results.append({
            "dataset_id": f"zenodo:{record_id}",
            "title": title,
            "description": description,
            "downloads": 0,
            "likes": 0,
            "tags": keywords[:5],
            "url": url,
            "source": "zenodo",
            "query": query,
        })
    return results


# ---------------------------------------------------------------------------
# MIT OCW course discovery
# ---------------------------------------------------------------------------

def fetch_ocw_courses(query: str, limit: int = _OCW_COURSES_PER_QUERY) -> list[dict]:
    """Search MIT OpenCourseWare for courses matching ``query``.

    Uses the OCW sitemap (2500+ courses, cached in brain_kv for 24 h) and scores
    each course slug by keyword overlap with the query.  No BeautifulSoup or
    Selenium required — stdlib only.

    Supply chain is treated as applied systems engineering, so queries like
    "systems engineering" or "operations research" are equally valid alongside
    direct supply chain queries.

    Returns a list of dicts:
    ``course_id``, ``title``, ``url``, ``course_number``, ``subjects``, ``query``, ``source``.
    """
    import re
    import ssl
    import urllib.request

    # SSL context — try truststore first (handles corporate SSL inspection / Sophos)
    try:
        import truststore
        _ssl_ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except ImportError:
        try:
            import certifi
            _ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        except ImportError:
            _ssl_ctx = ssl.create_default_context()

    # --- Load or refresh sitemap cache ---
    _SITEMAP_TTL_S = 24 * 3600
    _SITEMAP_URL = "https://ocw.mit.edu/sitemap.xml"
    _CACHE_KEY = "ocw_sitemap_cache"

    sitemap_xml: str | None = None
    try:
        import sqlite3 as _sq3
        import datetime as _dt
        _db = _get_db_path()
        with _sq3.connect(_db) as _cn:
            row = _cn.execute(
                "SELECT value FROM brain_kv WHERE key=?", (_CACHE_KEY,)
            ).fetchone()
            if row:
                ts_str, _, xml_body = row[0].partition("|||")
                age = (
                    _dt.datetime.now(_dt.timezone.utc)
                    - _dt.datetime.fromisoformat(ts_str)
                ).total_seconds()
                if age < _SITEMAP_TTL_S and xml_body:
                    sitemap_xml = xml_body
    except Exception:
        pass

    if sitemap_xml is None:
        try:
            req = urllib.request.Request(
                _SITEMAP_URL,
                headers={"User-Agent": "Mozilla/5.0 (compatible; SupplyChainBrain/1.0)"},
            )
            with urllib.request.urlopen(req, timeout=_OCW_TIMEOUT, context=_ssl_ctx) as resp:
                sitemap_xml = resp.read().decode("utf-8", errors="replace")
            import sqlite3 as _sq3, datetime as _dt
            _db = _get_db_path()
            ts = _dt.datetime.now(_dt.timezone.utc).isoformat()
            with _sq3.connect(_db) as _cn:
                _cn.execute(
                    "INSERT INTO brain_kv(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (_CACHE_KEY, ts + "|||" + sitemap_xml),
                )
                _cn.commit()
            log.debug(f"fetch_ocw_courses: fetched & cached sitemap ({len(sitemap_xml)} chars)")
        except Exception as exc:
            log.debug(f"fetch_ocw_courses: sitemap fetch failed: {exc}")
            return []

    # --- Score every course slug against the query ---
    slug_re = re.compile(r"/courses/([^/]+)/sitemap\.xml")
    all_slugs = slug_re.findall(sitemap_xml)

    _stop = {"a", "an", "the", "and", "or", "of", "in", "for", "to", "with", "by", "on"}
    q_tokens = set(re.findall(r"[a-z]+", query.lower())) - _stop

    scored: list[tuple[int, str]] = []
    for slug in all_slugs:
        slug_tokens = set(re.findall(r"[a-z]+", slug.lower()))
        score = len(q_tokens & slug_tokens)
        if score > 0:
            scored.append((score, slug))
    scored.sort(key=lambda x: -x[0])

    # --- Build result dicts ---
    sem_re = re.compile(r"^(fall|spring|summer|winter)$", re.I)
    year_re = re.compile(r"^\d{4}$")
    _dept_map = {
        "1": "Civil Engineering", "2": "Mechanical Engineering",
        "3": "Materials Science", "6": "EECS", "8": "Physics",
        "10": "Chemical Engineering", "11": "Urban Planning",
        "14": "Economics", "15": "Management", "16": "Aeronautics",
        "18": "Mathematics", "22": "Nuclear Engineering",
    }

    results: list[dict] = []
    for _, slug in scored[:limit]:
        parts = slug.split("-")
        course_number = parts[0].upper() if parts else ""
        title_parts: list[str] = []
        for p in parts[1:]:
            if sem_re.match(p) or year_re.match(p):
                break
            title_parts.append(p)
        title = " ".join(title_parts).title() if title_parts else slug.replace("-", " ").title()
        dept_m = re.match(r"^([0-9]+)", course_number)
        subjects = [_dept_map[dept_m.group(1)]] if dept_m and dept_m.group(1) in _dept_map else []
        results.append({
            "course_id": slug,
            "course_number": course_number,
            "title": title,
            "url": _OCW_COURSE_BASE + "/courses/" + slug + "/",
            "subjects": subjects,
            "query": query,
            "source": "mit_ocw",
        })

    log.debug(f"fetch_ocw_courses({query!r}): {len(scored)} scored, returning {len(results)}")
    return results


def persist_ocw_courses(courses: list[dict], topic: str) -> int:
    """Write OCW course discoveries to ``learning_log`` with ``kind="ocw_course"``.

    De-duplicated by ``course_id`` — safe to re-crawl the same topic.
    Returns the number of new rows written.
    """
    written = 0
    try:
        db = _get_db_path()
        with _connect(db) as cn:
            _ensure_learning_log(cn)
            for course in courses:
                course_id = course.get("course_id", "")
                if not course_id:
                    continue
                title_key = f"[ocw] {course_id}"
                existing = cn.execute(
                    "SELECT id FROM learning_log WHERE kind='ocw_course' AND title=?",
                    (title_key,),
                ).fetchone()
                if existing:
                    continue
                cn.execute(
                    """INSERT INTO learning_log(logged_at, kind, title, detail, signal_strength)
                       VALUES(?,?,?,?,?)""",
                    (
                        datetime.now(timezone.utc).isoformat(),
                        "ocw_course",
                        title_key,
                        json.dumps({
                            "course": course,
                            "topic": topic,
                            "type": "ocw_course",
                        }, default=str),
                        0.8,   # MIT OCW = high-quality academic signal
                    ),
                )
                written += 1
            cn.commit()
    except Exception as exc:
        log.warning(f"ml_research.persist_ocw_courses: {exc}")
    return written


# ---------------------------------------------------------------------------
# OCW deep-fetch — expansive knowledge acquisition from a course page
# ---------------------------------------------------------------------------
#
# Each MIT OCW course page is a portal:  it carries instructors, lecture
# topics, syllabus, readings, lab notebooks, and dozens of hyperlinks that
# reach out to other courses, external papers, GitHub repos, and university
# sites.  ``fetch_ocw_course_detail`` follows those threads so the corpus
# absorbs the *whole* lattice of knowledge each course unlocks — not just
# the slug.
# ---------------------------------------------------------------------------

_OCW_DETAIL_TIMEOUT = 20


def _ocw_ssl_context():
    """Build an SSL context that survives corporate inspection (truststore→certifi→default)."""
    import ssl
    try:
        import truststore
        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except ImportError:
        try:
            import certifi
            return ssl.create_default_context(cafile=certifi.where())
        except ImportError:
            return ssl.create_default_context()


def fetch_ocw_course_detail(slug: str) -> dict:
    """Fetch the OCW course page for ``slug`` and harvest every link it surfaces.

    Returns a dict::

        {
            "course_id":       "<slug>",
            "url":             "https://ocw.mit.edu/courses/<slug>/",
            "description":     "<first meaningful paragraph or meta description>",
            "instructors":     ["Prof. ...", ...],
            "topics":          ["...", ...],         # course-page topic tags
            "level":           "Undergraduate" | "Graduate" | "",
            "resources":       [{"label","url","kind"} ...],   # /resources/, /pages/, etc.
            "related_courses": [{"slug","url"} ...],          # other ocw courses
            "external_links":  [{"label","url","domain"} ...],# off-site references
        }

    All HTTP failures soft-skip (return ``{}``).  Stdlib only (no BS4).
    """
    import re
    import urllib.parse
    import urllib.request
    from html.parser import HTMLParser

    if not slug:
        return {}

    course_url = f"{_OCW_COURSE_BASE}/courses/{slug}/"
    try:
        req = urllib.request.Request(
            course_url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; SupplyChainBrain/1.0)"},
        )
        with urllib.request.urlopen(req, timeout=_OCW_DETAIL_TIMEOUT,
                                     context=_ocw_ssl_context()) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as exc:
        log.debug(f"fetch_ocw_course_detail({slug}): {exc}")
        return {}

    # --- Lightweight HTML parser: collect <a> hrefs + meta + text fragments ---
    class _OCWHarvester(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.links: list[tuple[str, str]] = []   # (href, link_text)
            self.metas: dict[str, str] = {}
            self._cur_href: str | None = None
            self._cur_text: list[str] = []
            self._in_title = False
            self.title_text = ""
            self._capture_text = False

        def handle_starttag(self, tag, attrs):
            attrs_d = dict(attrs)
            if tag == "a" and attrs_d.get("href"):
                self._cur_href = attrs_d["href"]
                self._cur_text = []
                self._capture_text = True
            elif tag == "meta":
                name = attrs_d.get("name") or attrs_d.get("property") or ""
                content = attrs_d.get("content") or ""
                if name and content:
                    self.metas[name.lower()] = content
            elif tag == "title":
                self._in_title = True

        def handle_endtag(self, tag):
            if tag == "a" and self._cur_href is not None:
                text = " ".join(self._cur_text).strip()
                self.links.append((self._cur_href, text))
                self._cur_href = None
                self._cur_text = []
                self._capture_text = False
            elif tag == "title":
                self._in_title = False

        def handle_data(self, data):
            if self._in_title:
                self.title_text += data
            if self._capture_text:
                self._cur_text.append(data)

    parser = _OCWHarvester()
    try:
        parser.feed(html)
    except Exception as exc:
        log.debug(f"fetch_ocw_course_detail({slug}): parse error: {exc}")

    metas = parser.metas
    description = (
        metas.get("description")
        or metas.get("og:description")
        or metas.get("twitter:description")
        or ""
    ).strip()[:600]

    # --- Instructors: pull from JSON-LD or meta or page heuristics ---
    instructors: list[str] = []
    # JSON-LD often carries instructor info
    for m in re.finditer(
        r'"(?:instructor|author)"\s*:\s*(?:\{[^{}]*?"name"\s*:\s*"([^"]{2,80})"'
        r'|\[[^\]]*?"name"\s*:\s*"([^"]{2,80})")',
        html,
    ):
        name = (m.group(1) or m.group(2) or "").strip()
        if name and name not in instructors:
            instructors.append(name)
    # Fallback: link text under /search/?q=&l=Lecturer or /search/?q=&i=
    for href, text in parser.links:
        if "/search/" in href and ("instructor" in href.lower() or "&i=" in href):
            t = text.strip()
            if t and len(t) < 80 and t not in instructors:
                instructors.append(t)

    # --- Topics: OCW exposes /search/?t=<topic> filter links ---
    topics: list[str] = []
    for href, text in parser.links:
        if "/search/" in href and ("&t=" in href or "?t=" in href):
            t = (text or "").strip()
            if t and len(t) < 60 and t not in topics:
                topics.append(t)
    # Also harvest from JSON-LD "about" / "keywords"
    kw_match = re.search(r'"keywords"\s*:\s*"([^"]{1,400})"', html)
    if kw_match:
        for kw in re.split(r"[,;]", kw_match.group(1)):
            kw = kw.strip()
            if kw and kw not in topics:
                topics.append(kw)

    # --- Level (Undergraduate / Graduate) — surfaces in JSON-LD or meta ---
    level = ""
    for pat in (r'"educationalLevel"\s*:\s*"([^"]+)"',
                r"course[_\- ]level[\"']?\s*[:=]\s*[\"']([^\"']+)"):
        m = re.search(pat, html, re.I)
        if m:
            level = m.group(1).strip()
            break

    # --- Categorise hyperlinks ---
    resources: list[dict] = []
    related_courses: list[dict] = []
    external_links: list[dict] = []
    seen: set[str] = set()

    for href, text in parser.links:
        href = (href or "").strip()
        if not href or href.startswith("#") or href.startswith("mailto:"):
            continue
        absolute = urllib.parse.urljoin(course_url, href)
        if absolute in seen:
            continue
        seen.add(absolute)
        text = (text or "").strip()

        parsed = urllib.parse.urlparse(absolute)
        host = parsed.netloc.lower()
        path = parsed.path.lower()

        # Skip nav junk
        if path in ("", "/") or path.endswith(("/login", "/donate", "/about")):
            continue
        # OCW global nav links
        if host == "ocw.mit.edu" and not path.startswith("/courses/") and "/search" not in path:
            continue

        if host == "ocw.mit.edu":
            # /courses/<other-slug>/...
            m = re.match(r"^/courses/([^/]+)/", path)
            if m and m.group(1) != slug:
                related_courses.append({"slug": m.group(1), "url": absolute})
                continue
            # /courses/<slug>/pages/, /resources/, /lecture-notes/ ...
            if f"/courses/{slug}/" in path:
                # Determine resource kind from path segment
                rkind = "page"
                for token in ("lecture-notes", "lecture-videos", "assignments",
                              "exams", "readings", "labs", "projects",
                              "study-materials", "syllabus", "calendar",
                              "tools", "related-resources", "video-lectures",
                              "recitations", "tutorials"):
                    if token in path:
                        rkind = token
                        break
                resources.append({"label": text or rkind, "url": absolute, "kind": rkind})
                continue
        else:
            # Off-site reference — anything not on OCW
            if host and host not in ("www.mit.edu", "mit.edu"):
                external_links.append({
                    "label": text or host,
                    "url": absolute,
                    "domain": host,
                })

    return {
        "course_id":       slug,
        "url":             course_url,
        "description":     description,
        "instructors":     instructors[:10],
        "topics":          topics[:20],
        "level":           level,
        "resources":       resources[:50],
        "related_courses": related_courses[:25],
        "external_links":  external_links[:50],
    }


def persist_ocw_course_detail(slug: str, detail: dict) -> int:
    """Write one ``ocw_course_detail`` row + N ``ocw_resource`` rows to ``learning_log``.

    De-duped by ``title`` so re-crawls don't bloat the log.
    Returns the number of new rows written.
    """
    if not slug or not detail:
        return 0

    written = 0
    try:
        db = _get_db_path()
        with _connect(db) as cn:
            _ensure_learning_log(cn)
            now = datetime.now(timezone.utc).isoformat()

            # 1) The course-level detail row (description + instructors + topics)
            detail_title = f"[ocw_detail] {slug}"
            existing = cn.execute(
                "SELECT id FROM learning_log WHERE kind='ocw_course_detail' AND title=?",
                (detail_title,),
            ).fetchone()
            if not existing:
                cn.execute(
                    """INSERT INTO learning_log(logged_at, kind, title, detail, signal_strength)
                       VALUES(?,?,?,?,?)""",
                    (now, "ocw_course_detail", detail_title,
                     json.dumps({
                         "course_id":   slug,
                         "url":         detail.get("url"),
                         "description": detail.get("description"),
                         "instructors": detail.get("instructors", []),
                         "topics":      detail.get("topics", []),
                         "level":       detail.get("level", ""),
                         "type":        "ocw_course_detail",
                     }, default=str),
                     0.85),
                )
                written += 1

            # 2) One ocw_resource row per harvested link / related course
            def _emit_resource(payload: dict, kind_tag: str, title_key: str):
                nonlocal written
                row = cn.execute(
                    "SELECT id FROM learning_log WHERE kind='ocw_resource' AND title=?",
                    (title_key,),
                ).fetchone()
                if row:
                    return
                cn.execute(
                    """INSERT INTO learning_log(logged_at, kind, title, detail, signal_strength)
                       VALUES(?,?,?,?,?)""",
                    (now, "ocw_resource", title_key,
                     json.dumps({**payload,
                                 "course_id": slug,
                                 "resource_kind": kind_tag,
                                 "type": "ocw_resource"}, default=str),
                     0.7),
                )
                written += 1

            for r in detail.get("resources", []):
                tk = f"[ocw_resource] {slug}::{r.get('kind','page')}::{r.get('url','')}"
                _emit_resource(r, r.get("kind", "page"), tk)

            for rc in detail.get("related_courses", []):
                tk = f"[ocw_related] {slug}->{rc.get('slug','')}"
                _emit_resource(rc, "related_course", tk)

            for ext in detail.get("external_links", []):
                tk = f"[ocw_external] {slug}::{ext.get('url','')}"
                _emit_resource(ext, "external_link", tk)

            cn.commit()
    except Exception as exc:
        log.warning(f"ml_research.persist_ocw_course_detail({slug}): {exc}")
    return written


def deepen_ocw_course(slug: str) -> dict:
    """Convenience: fetch + persist a single course's full link lattice.

    Returns ``{"slug","fetched": bool, "rows_written": int, "resources": int,
    "related": int, "external": int, "instructors": int}``.
    """
    detail = fetch_ocw_course_detail(slug)
    if not detail:
        return {"slug": slug, "fetched": False, "rows_written": 0}
    written = persist_ocw_course_detail(slug, detail)
    return {
        "slug":         slug,
        "fetched":      True,
        "rows_written": written,
        "resources":    len(detail.get("resources", [])),
        "related":      len(detail.get("related_courses", [])),
        "external":     len(detail.get("external_links", [])),
        "instructors":  len(detail.get("instructors", [])),
        "topics":       len(detail.get("topics", [])),
    }


# ---------------------------------------------------------------------------
# Knowledge corpus persistence
# ---------------------------------------------------------------------------

def _get_db_path() -> Path:
    """Locate the Brain's SQLite database."""
    _sys_path_insert = str(Path(__file__).resolve().parents[2])
    if _sys_path_insert not in sys.path:
        sys.path.insert(0, _sys_path_insert)
    from src.brain.local_store import db_path as _db_path
    return _db_path()


_WAL_ENABLED: bool = False  # module-level flag; WAL is set once per process


def _connect(db) -> sqlite3.Connection:
    """Open the Brain SQLite database with WAL mode and a generous timeout.

    WAL (Write-Ahead Logging) allows concurrent reads (Streamlit) and writes
    (daemon threads) without "database is locked" errors.  The timeout=30
    gives other writers 30 s to finish before raising an error, compared to
    the default 5 s which expires under normal Streamlit load.

    WAL mode persists on disk once set, so subsequent calls skip the PRAGMA.
    """
    global _WAL_ENABLED
    cn = sqlite3.connect(str(db), timeout=30, check_same_thread=False)
    if not _WAL_ENABLED:
        try:
            cn.execute("PRAGMA journal_mode=WAL")
            cn.execute("PRAGMA synchronous=NORMAL")  # safe with WAL
            _WAL_ENABLED = True
        except Exception:
            pass
    return cn


def _ensure_learning_log(cn: sqlite3.Connection) -> None:
    """Create learning_log if it doesn't exist (idempotent)."""
    cn.execute(
        """CREATE TABLE IF NOT EXISTS learning_log (
               id              INTEGER PRIMARY KEY AUTOINCREMENT,
               logged_at       TEXT    NOT NULL,
               kind            TEXT    NOT NULL,
               title           TEXT    NOT NULL,
               detail          TEXT,
               signal_strength REAL,
               source_table    TEXT,
               source_row_id   INTEGER
           )"""
    )


def _ensure_citation_chain_state(cn: sqlite3.Connection) -> None:
    """Create citation_chain_state if it doesn't exist (idempotent).

    Matches the schema created by citation_chain_acquirer so rows seeded
    here are immediately visible to the expander daemon without migration.
    """
    cn.execute(
        """CREATE TABLE IF NOT EXISTS citation_chain_state (
               paper_id            TEXT PRIMARY KEY,
               depth               INTEGER NOT NULL DEFAULT 0,
               parent_id           TEXT,
               source_api          TEXT,
               ref_count           INTEGER DEFAULT 0,
               sc_relevance_score  REAL    DEFAULT 0.0,
               logged_at           TEXT,
               fetched_at          TEXT
           )"""
    )


def persist_research_findings(
    papers: list[dict],
    datasets: list[dict],
    topic: str,
) -> int:
    """Write research findings to the Brain's ``learning_log``.

    Papers are de-duplicated by ``arxiv_id`` + ``kind="ml_research"``.
    Datasets are de-duplicated by ``dataset_id``.

    For each OpenAlex paper that carries ``openalex_ref_ids`` (its bibliography),
    the referenced OA IDs are immediately seeded into ``citation_chain_state``
    at depth=1 so the citation-chain expander picks them up in its next cycle
    without waiting for a full corpus round.  This is the entry point for
    directed toroidal knowledge growth — every newly discovered paper donates
    its entire bibliography to the expansion frontier.

    Returns the number of new rows written.
    """
    written = 0
    try:
        db = _get_db_path()
        with _connect(db) as cn:
            _ensure_learning_log(cn)
            _ensure_citation_chain_state(cn)
            now = datetime.now(timezone.utc).isoformat()

            for paper in papers:
                arxiv_id = paper.get("arxiv_id", "")
                oa_id    = paper.get("openalex_id", "")
                doi      = (paper.get("doi") or "").strip()
                title_key = (
                    f"[ml_research] {arxiv_id}" if arxiv_id
                    else f"[ml_research] {paper.get('title','')[:80]}"
                )
                if not title_key.strip():
                    continue

                existing = cn.execute(
                    "SELECT id FROM learning_log WHERE kind='ml_research' AND title=?",
                    (title_key,),
                ).fetchone()
                if not existing:
                    signal = min(1.0, (paper.get("upvotes", 0) + paper.get("citations", 0) * 0.1) / 50.0)
                    cn.execute(
                        """INSERT INTO learning_log(logged_at, kind, title, detail, signal_strength)
                           VALUES(?,?,?,?,?)""",
                        (
                            now,
                            "ml_research",
                            title_key,
                            json.dumps({
                                "paper": paper,
                                "topic": topic,
                                "type": "paper",
                            }, default=str),
                            signal,
                        ),
                    )
                    written += 1

                # ── Toroidal bibliography seeding ────────────────────────────
                # For every OA-sourced paper that carries referenced_works,
                # seed each referenced OA ID into citation_chain_state so the
                # citation-chain expander follows the full bibliography tree.
                # This is depth-1 seeding — the parent is the canonical ID of
                # the paper we just wrote.
                ref_oa_ids = paper.get("openalex_ref_ids") or []
                if ref_oa_ids:
                    # Build canonical parent ID for the edge
                    if doi and not doi.startswith(("openalex:", "core:", "ntrs:")):
                        parent_id = f"doi:{doi}"
                    elif arxiv_id:
                        parent_id = f"arxiv:{arxiv_id}"
                    elif oa_id:
                        parent_id = f"oa:{oa_id}"
                    else:
                        parent_id = None

                    if parent_id:
                        for ref_oa in ref_oa_ids[:40]:
                            if not ref_oa:
                                continue
                            ref_key = f"oa:{ref_oa}"
                            cn.execute(
                                """INSERT OR IGNORE INTO citation_chain_state
                                       (paper_id, depth, parent_id, source_api,
                                        ref_count, sc_relevance_score, logged_at)
                                   VALUES(?,1,?,'openalex_bibliography',0,0.3,?)""",
                                (ref_key, parent_id, now),
                            )

            for ds in datasets:
                ds_id = ds.get("dataset_id", "")
                title_key = f"[ml_dataset] {ds_id}" if ds_id else ""
                if not title_key.strip():
                    continue

                existing = cn.execute(
                    "SELECT id FROM learning_log WHERE kind='ml_research' AND title=?",
                    (title_key,),
                ).fetchone()
                if existing:
                    continue

                signal = min(1.0, (ds.get("downloads", 0) / 10_000.0))
                cn.execute(
                    """INSERT INTO learning_log(logged_at, kind, title, detail, signal_strength)
                       VALUES(?,?,?,?,?)""",
                    (
                        datetime.now(timezone.utc).isoformat(),
                        "ml_research",
                        title_key,
                        json.dumps({
                            "dataset": ds,
                            "topic": topic,
                            "type": "dataset",
                        }, default=str),
                        signal,
                    ),
                )
                written += 1

            cn.commit()

    except Exception as exc:
        log.warning(f"ml_research.persist_research_findings: {exc}")

    return written


# ---------------------------------------------------------------------------
# Optional ml-intern deep-research fallback
# ---------------------------------------------------------------------------

def _ml_intern_available() -> bool:
    """Return True if the ml-intern CLI is installed."""
    import subprocess
    try:
        result = subprocess.run(
            ["ml-intern", "--help"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def run_ml_intern_query(prompt: str, max_iterations: int = 30, timeout: int = 300) -> dict:
    """Run ``ml-intern "<prompt>"`` as a subprocess and capture output.

    This is the optional deep-research path.  If ml-intern is not installed
    or ANTHROPIC_API_KEY is absent, returns ``{"ok": False, "reason": ...}``.

    The output is treated as a free-text research summary and written to
    ``learning_log`` as a single ``kind="ml_research"`` entry.
    """
    import os
    import subprocess

    if not os.environ.get("ANTHROPIC_API_KEY"):
        return {"ok": False, "reason": "ANTHROPIC_API_KEY not set"}

    if not _ml_intern_available():
        return {"ok": False, "reason": "ml-intern CLI not installed"}

    try:
        result = subprocess.run(
            ["ml-intern", "--max-iterations", str(max_iterations), prompt],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        output = (result.stdout or "").strip()
        if not output:
            return {"ok": False, "reason": "no output from ml-intern"}

        # Persist as a single learning_log entry
        title_key = f"[ml_intern] {prompt[:80]}"
        written = 0
        try:
            db = _get_db_path()
            with _connect(db) as cn:
                _ensure_learning_log(cn)
                existing = cn.execute(
                    "SELECT id FROM learning_log WHERE kind='ml_research' AND title LIKE ?",
                    (f"%{prompt[:40]}%",),
                ).fetchone()
                if not existing:
                    cn.execute(
                        """INSERT INTO learning_log(logged_at, kind, title, detail, signal_strength)
                           VALUES(?,?,?,?,?)""",
                        (
                            datetime.now(timezone.utc).isoformat(),
                            "ml_research",
                            title_key,
                            json.dumps({"prompt": prompt, "output": output[:4000]}, default=str),
                            0.7,
                        ),
                    )
                    cn.commit()
                    written = 1
        except Exception as exc:
            log.warning(f"ml_intern persist failed: {exc}")

        return {"ok": True, "output": output[:2000], "learnings_written": written}

    except subprocess.TimeoutExpired:
        return {"ok": False, "reason": f"ml-intern timed out after {timeout}s"}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}


# ---------------------------------------------------------------------------
# Main research sweep
# ---------------------------------------------------------------------------

def research_supply_chain_topics(
    topics: list[str] | None = None,
    topics_per_cycle: int = _TOPICS_PER_CYCLE,
    include_trending: bool = True,
    include_ocw: bool = True,
) -> dict[str, Any]:
    """Sweep supply-chain ML topics and persist findings to the corpus.

    Rotates through :data:`_SUPPLY_CHAIN_TOPICS` in round-robin so every
    cycle covers fresh ground.  Fetches papers from arXiv, OpenAlex, CrossRef,
    and CORE; datasets from Zenodo; plus MIT OCW courses and NASA NTRS reports
    for systems-engineering topics.

    A second rotation sweeps :data:`_EXTENDED_RESEARCH_TOPICS` — topics derived
    from the user's active Grok 3 research thread covering the UEQGM quantum
    physics model, biohybrid computing, astrophysical timing, and AI knowledge
    graph expansion.

    Args:
        topics: Override ML topic list (``None`` uses the built-in rotation).
        topics_per_cycle: How many ML topics to research this cycle.
        include_trending: If ``True``, also fetch recent arXiv CS/ML papers.
        include_ocw: If ``True``, sweep MIT OCW and NASA NTRS alongside ML sources.

    Returns:
        ``{topics_researched, papers_found, datasets_found, learnings_written,
           ocw_courses_found, ocw_learnings_written}``
    """
    # Determine which topics to cover this cycle via round-robin cursor
    if topics is None:
        try:
            db = _get_db_path()
            with _connect(db) as cn:
                row = cn.execute(
                    "SELECT value FROM brain_kv WHERE key='ml_research_topic_cursor'",
                ).fetchone()
            cursor = int(row[0]) if row else 0
        except Exception:
            cursor = 0

        n = len(_SUPPLY_CHAIN_TOPICS)
        selected = [_SUPPLY_CHAIN_TOPICS[(cursor + i) % n] for i in range(topics_per_cycle)]
        new_cursor = (cursor + topics_per_cycle) % n
        try:
            db = _get_db_path()
            with _connect(db) as cn:
                cn.execute(
                    """INSERT INTO brain_kv(key, value) VALUES(?,?)
                       ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                    ("ml_research_topic_cursor", str(new_cursor)),
                )
                cn.commit()
        except Exception:
            pass
    else:
        selected = topics

    all_papers: list[dict] = []
    all_datasets: list[dict] = []
    topics_researched: list[str] = []

    # Recent arXiv CS/ML papers + HuggingFace daily papers
    if include_trending:
        trending = fetch_arxiv_recent(limit=5)
        if trending:
            all_papers.extend(trending)
            log.debug(f"ml_research: fetched {len(trending)} recent arXiv papers")
        hf_papers = fetch_hf_daily_papers(limit=10)
        if hf_papers:
            all_papers.extend(hf_papers)
            log.debug(f"ml_research: fetched {len(hf_papers)} HuggingFace daily papers")

    # -----------------------------------------------------------------------
    # Extended research sweep — UEQGM physics + AI knowledge expansion topics
    # Runs FIRST so that quantum/physics/AI context is already in the corpus
    # when the supply-chain loop executes.  The Brain can then understand SC
    # systems engineering within the broader totality of those disciplines.
    # Uses its own round-robin cursor stored as 'extended_topic_cursor'.
    # -----------------------------------------------------------------------
    try:
        db = _get_db_path()
        with _connect(db) as cn:
            row = cn.execute(
                "SELECT value FROM brain_kv WHERE key='extended_topic_cursor'",
            ).fetchone()
        ext_cursor = int(row[0]) if row else 0
    except Exception:
        ext_cursor = 0

    n_ext = len(_EXTENDED_RESEARCH_TOPICS)
    ext_selected = [
        _EXTENDED_RESEARCH_TOPICS[(ext_cursor + i) % n_ext]
        for i in range(_EXTENDED_TOPICS_PER_CYCLE)
    ]
    new_ext_cursor = (ext_cursor + _EXTENDED_TOPICS_PER_CYCLE) % n_ext

    for ext_topic in ext_selected:
        log.debug(f"ml_research: extended topic '{ext_topic}'")

        # arXiv — primary source for physics + CS preprints
        ax = search_papers_arxiv(ext_topic, limit=_EXTENDED_PAPERS_PER_TOPIC)
        all_papers.extend(ax)

        # OpenAlex — cross-disciplinary coverage including physics journals
        oa = search_papers_openalex(ext_topic, limit=_EXTENDED_PAPERS_PER_TOPIC)
        all_papers.extend(oa)

        # Zenodo — open datasets including physics simulation data
        zd = discover_zenodo_datasets(ext_topic, limit=3)
        all_datasets.extend(zd)

        topics_researched.append(ext_topic)
        time.sleep(0.5)

    try:
        db = _get_db_path()
        with _connect(db) as cn:
            cn.execute(
                """INSERT INTO brain_kv(key, value) VALUES(?,?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                ("extended_topic_cursor", str(new_ext_cursor)),
            )
            cn.commit()
    except Exception:
        pass

    # Per-topic paper + dataset search across four open sources
    for topic in selected:
        log.debug(f"ml_research: researching topic '{topic}'")

        # arXiv — authoritative ML/CS preprint server
        arxiv_papers = search_papers_arxiv(topic, limit=_PAPERS_PER_TOPIC)
        all_papers.extend(arxiv_papers)

        # OpenAlex — 200 M+ scholarly works (Semantic Scholar replacement)
        oa_papers = search_papers_openalex(topic, limit=_PAPERS_PER_TOPIC)
        all_papers.extend(oa_papers)

        # CrossRef — peer-reviewed published papers with DOIs
        cr_papers = search_papers_crossref(topic, limit=3)
        all_papers.extend(cr_papers)

        # CORE — open-access full-text repository
        core_papers = search_papers_core(topic, limit=3)
        all_papers.extend(core_papers)

        # Zenodo — peer-reviewed open research datasets
        datasets = discover_zenodo_datasets(topic, limit=_DATASETS_PER_TOPIC)
        all_datasets.extend(datasets)

        # HuggingFace Hub — 100 K+ ML/data datasets (searched live)
        hf_ds = discover_hf_datasets(topic, limit=_DATASETS_PER_TOPIC)
        all_datasets.extend(hf_ds)

        topics_researched.append(topic)
        time.sleep(0.5)  # polite rate-limiting between topics

    # Persist all ML findings
    written = persist_research_findings(all_papers, all_datasets, topic=", ".join(topics_researched))

    # -----------------------------------------------------------------------
    # MIT OCW sweep + NASA NTRS technical reports
    # -----------------------------------------------------------------------
    ocw_courses_found = 0
    ocw_written = 0
    if include_ocw:
        try:
            db = _get_db_path()
            with _connect(db) as cn:
                row = cn.execute(
                    "SELECT value FROM brain_kv WHERE key='ocw_topic_cursor'",
                ).fetchone()
            ocw_cursor = int(row[0]) if row else 0
        except Exception:
            ocw_cursor = 0

        n_ocw = len(_OCW_TOPICS)
        ocw_selected = [_OCW_TOPICS[(ocw_cursor + i) % n_ocw]
                        for i in range(_OCW_TOPICS_PER_CYCLE)]
        new_ocw_cursor = (ocw_cursor + _OCW_TOPICS_PER_CYCLE) % n_ocw

        all_ocw_courses: list[dict] = []
        for ocw_topic in ocw_selected:
            log.debug(f"ml_research: fetching OCW courses for '{ocw_topic}'")
            courses = fetch_ocw_courses(ocw_topic, limit=_OCW_COURSES_PER_QUERY)
            all_ocw_courses.extend(courses)

            # NASA NTRS technical reports complement OCW on systems-eng topics
            ntrs_papers = search_ntrs(ocw_topic, limit=3)
            if ntrs_papers:
                all_papers.extend(ntrs_papers)
                log.debug(f"ml_research: NTRS {len(ntrs_papers)} reports for '{ocw_topic}'")

            time.sleep(1.0)   # polite crawl delay — OCW is a public resource

        ocw_courses_found = len(all_ocw_courses)
        if all_ocw_courses:
            ocw_written = persist_ocw_courses(
                all_ocw_courses,
                topic=", ".join(ocw_selected),
            )

        # Auto-deepen undiscovered courses — progressively absorbs full link
        # lattices for every newly-discovered course (3 per cycle keeps it fast)
        try:
            deepened = auto_deepen_undiscovered(max_courses=3)
            if deepened:
                log.info(f"ml_research: auto-deepened {deepened} OCW courses")
        except Exception as _exc:
            log.debug(f"ml_research: auto_deepen_undiscovered error: {_exc}")

        try:
            db = _get_db_path()
            with _connect(db) as cn:
                cn.execute(
                    """INSERT INTO brain_kv(key, value) VALUES(?,?)
                       ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                    ("ocw_topic_cursor", str(new_ocw_cursor)),
                )
                cn.commit()
        except Exception:
            pass

    result = {
        "topics_researched": topics_researched,
        "papers_found": len(all_papers),
        "datasets_found": len(all_datasets),
        "learnings_written": written,
        "ocw_courses_found": ocw_courses_found,
        "ocw_learnings_written": ocw_written,
    }
    log.info(
        f"ml_research: topics={len(topics_researched)}, "
        f"papers={len(all_papers)}, datasets={len(all_datasets)}, "
        f"learnings_written={written}, "
        f"ocw_courses={ocw_courses_found}, ocw_written={ocw_written}"
    )
    return result


# ---------------------------------------------------------------------------
# Public query helpers (used by the Streamlit page)
# ---------------------------------------------------------------------------

def recent_ml_learnings(limit: int = 20) -> list[dict]:
    """Return recent ``ml_research`` and ``ocw_course`` entries from ``learning_log``.

    Each dict has: ``id``, ``logged_at``, ``title``, ``detail``, ``signal_strength``.
    """
    try:
        db = _get_db_path()
        with _connect(db) as cn:
            cn.row_factory = sqlite3.Row
            rows = cn.execute(
                """SELECT id, logged_at, title, detail, signal_strength
                   FROM learning_log
                   WHERE kind IN ('ml_research', 'ocw_course')
                   ORDER BY id DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
            results = []
            for r in rows:
                detail = {}
                try:
                    detail = json.loads(r["detail"] or "{}")
                except Exception:
                    pass
                results.append({
                    "id": r["id"],
                    "logged_at": r["logged_at"],
                    "title": r["title"],
                    "detail": detail,
                    "signal_strength": r["signal_strength"],
                })
            return results
    except Exception as exc:
        log.warning(f"ml_research.recent_ml_learnings: {exc}")
        return []


def search_papers_interactive(query: str) -> dict[str, Any]:
    """Convenience function for the Streamlit page: run a multi-source one-shot search.

    Returns:
        ``{arxiv_papers, openalex_papers, crossref_papers, core_papers, datasets}``
    """
    return {
        "arxiv_papers": search_papers_arxiv(query, limit=8),
        "openalex_papers": search_papers_openalex(query, limit=5),
        "crossref_papers": search_papers_crossref(query, limit=5),
        "core_papers": search_papers_core(query, limit=3),
        "datasets": discover_zenodo_datasets(query, limit=5),
    }


def search_ocw_interactive(query: str) -> list[dict]:
    """Search MIT OCW for ``query`` and return course list (for Streamlit page).

    Returns a list of course dicts from :func:`fetch_ocw_courses`.
    """
    return fetch_ocw_courses(query, limit=12)


# ---------------------------------------------------------------------------
# Graph traversal — cascade BFS across OCW's link lattice
# ---------------------------------------------------------------------------

def cascade_deepen_ocw(
    seed_slug: str,
    hops: int = 2,
    fan_out: int = 5,
) -> dict:
    """BFS-traverse the OCW knowledge graph starting from ``seed_slug``.

    At each hop, deepens the current course and enqueues its ``related_courses``
    for the next hop.  Bounded by ``hops`` (depth) and ``fan_out`` (breadth per
    level) to prevent runaway crawls.

    Returns::

        {
            "seed":          "<slug>",
            "hops_executed": int,
            "courses_deepened": [slug, ...],
            "rows_written":  int,
            "resources":     int,
            "related":       int,
            "external":      int,
        }
    """
    from collections import deque

    visited: set[str] = set()
    queue: deque[tuple[str, int]] = deque()  # (slug, depth)
    queue.append((seed_slug, 0))

    courses_deepened: list[str] = []
    total_rows = total_res = total_rel = total_ext = 0

    while queue:
        slug, depth = queue.popleft()
        if slug in visited or depth > hops:
            continue
        visited.add(slug)

        result = deepen_ocw_course(slug)
        if not result.get("fetched"):
            continue

        courses_deepened.append(slug)
        total_rows += result.get("rows_written", 0)
        total_res  += result.get("resources", 0)
        total_rel  += result.get("related", 0)
        total_ext  += result.get("external", 0)

        if depth < hops:
            # Enqueue related courses discovered at this node
            detail = fetch_ocw_course_detail(slug) if result.get("related", 0) else {}
            for rc in (detail.get("related_courses") or [])[:fan_out]:
                rc_slug = rc.get("slug", "")
                if rc_slug and rc_slug not in visited:
                    queue.append((rc_slug, depth + 1))

        time.sleep(0.8)  # polite crawl delay

    return {
        "seed":             seed_slug,
        "hops_executed":    hops,
        "courses_deepened": courses_deepened,
        "rows_written":     total_rows,
        "resources":        total_res,
        "related":          total_rel,
        "external":         total_ext,
    }


def auto_deepen_undiscovered(max_courses: int = 5) -> int:
    """Deepen any ``ocw_course`` entries in ``learning_log`` that have no detail row yet.

    Called automatically at the end of each research cycle to progressively
    absorb full link lattices for every newly-discovered course.

    Returns the number of courses deepened.
    """
    deepened = 0
    try:
        db = _get_db_path()
        with _connect(db) as cn:
            # Courses discovered but not yet detailed
            rows = cn.execute(
                """SELECT title FROM learning_log
                   WHERE kind = 'ocw_course'
                   AND title NOT IN (
                       SELECT REPLACE(title, '[ocw_detail] ', '[ocw] ')
                       FROM learning_log WHERE kind = 'ocw_course_detail'
                   )
                   ORDER BY id DESC
                   LIMIT ?""",
                (max_courses,),
            ).fetchall()
    except Exception as exc:
        log.warning(f"auto_deepen_undiscovered query: {exc}")
        return 0

    for (title,) in rows:
        # title is like "[ocw] some-course-slug-fall-2020"
        slug = title.replace("[ocw] ", "").strip()
        if not slug:
            continue
        result = deepen_ocw_course(slug)
        if result.get("fetched"):
            deepened += 1
            log.debug(
                f"auto_deepen: {slug} → "
                f"{result['rows_written']} rows, "
                f"{result['resources']} resources, "
                f"{result['related']} related, "
                f"{result['external']} external"
            )
        time.sleep(1.0)

    return deepened


def recent_ocw_details(limit: int = 50) -> list[dict]:
    """Return recent ``ocw_course_detail`` and ``ocw_resource`` entries from ``learning_log``.

    Used by the Knowledge Graph tab to surface all harvested link data.
    Each dict has: ``id``, ``logged_at``, ``kind``, ``title``, ``detail``, ``signal_strength``.
    """
    try:
        db = _get_db_path()
        with _connect(db) as cn:
            cn.row_factory = sqlite3.Row
            rows = cn.execute(
                """SELECT id, logged_at, kind, title, detail, signal_strength
                   FROM learning_log
                   WHERE kind IN ('ocw_course_detail', 'ocw_resource')
                   ORDER BY id DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
            results = []
            for r in rows:
                detail = {}
                try:
                    detail = json.loads(r["detail"] or "{}")
                except Exception:
                    pass
                results.append({
                    "id": r["id"],
                    "logged_at": r["logged_at"],
                    "kind": r["kind"],
                    "title": r["title"],
                    "detail": detail,
                    "signal_strength": r["signal_strength"],
                })
            return results
    except Exception as exc:
        log.warning(f"ml_research.recent_ocw_details: {exc}")
        return []


# ---------------------------------------------------------------------------
# Adaptive semantic graph traversal — re-exported for single-import convenience
# ---------------------------------------------------------------------------

def adaptive_cascade_ocw(
    seed_slug: str,
    max_hops: int = 3,
    fan_out: int = 5,
    endpoint_concepts: list[str] | None = None,
    decay_lambda: float = 2.5,
    tunneling_coeff: float = 0.35,
    beta1: float = 0.9,
    beta2: float = 0.999,
) -> dict:
    """Adaptive BFS traversal with semantic edge ranking, Adam phase-shift
    detection, and endpoint tunnel bias.

    Delegates to :mod:`src.brain.semantic_graph.adaptive_cascade_ocw`.

    Result dict includes all keys from ``cascade_deepen_ocw`` PLUS:
    * ``phase_shifts``     — list of hop dicts where inflection was detected
    * ``edge_potentials``  — {slug: float} for every enqueued node
    * ``adam_report``      — final Adam tracker state
    * ``hop_signals``      — {hop_int: mean_signal} per BFS level
    """
    try:
        from src.brain.semantic_graph import (
            adaptive_cascade_ocw as _adaptive,
        )
        return _adaptive(
            seed_slug=seed_slug,
            max_hops=max_hops,
            fan_out=fan_out,
            endpoint_concepts=endpoint_concepts,
            decay_lambda=decay_lambda,
            tunneling_coeff=tunneling_coeff,
            beta1=beta1,
            beta2=beta2,
        )
    except Exception as exc:
        log.warning(f"adaptive_cascade_ocw: falling back to standard cascade: {exc}")
        return cascade_deepen_ocw(seed_slug, hops=max_hops, fan_out=fan_out)


def world_r1_explore(
    seed_slug: str,
    max_iterations: int = 10,
    sample_breadth: int = 5,
    endpoint_concepts: list[str] | None = None,
    coverage_weight: float = 1.0,
    consistency_weight: float = 0.7,
    trajectory_weight: float = 1.2,
    quality_weight: float = 0.8,
    curiosity_weight: float = 0.6,
    dynamic_only_period: int = 4,
    temperature: float = 0.8,
    seed: int | None = None,
) -> dict:
    """World-R1-shaped curiosity exploration of the OCW knowledge surface.

    Multi-axis constraint reward (coverage / consistency / trajectory /
    quality) + GRPO advantage normalisation + softmax sampling + periodic
    dynamic-only regularization phase + curiosity bonus from inverse
    log-frequency of token novelty.

    Delegates to :mod:`src.brain.world_r1_explorer.world_r1_explore`.
    """
    try:
        from src.brain.world_r1_explorer import (
            world_r1_explore as _w1,
        )
        return _w1(
            seed_slug           = seed_slug,
            max_iterations      = max_iterations,
            sample_breadth      = sample_breadth,
            endpoint_concepts   = endpoint_concepts,
            coverage_weight     = coverage_weight,
            consistency_weight  = consistency_weight,
            trajectory_weight   = trajectory_weight,
            quality_weight      = quality_weight,
            curiosity_weight    = curiosity_weight,
            dynamic_only_period = dynamic_only_period,
            temperature         = temperature,
            seed                = seed,
        )
    except Exception as exc:
        log.warning(f"world_r1_explore: falling back to adaptive cascade: {exc}")
        return adaptive_cascade_ocw(
            seed_slug,
            max_hops=max(2, max_iterations // 3),
            fan_out=sample_breadth,
            endpoint_concepts=endpoint_concepts,
        )
