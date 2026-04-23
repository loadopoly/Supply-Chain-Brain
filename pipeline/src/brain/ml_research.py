"""ml_research.py — Brain-native ML research module.

Brings the value of the HuggingFace ``ml-intern`` project
(https://github.com/huggingface/ml-intern) directly into the Brain's knowledge
acquisition pipeline.  Rather than spawning the full ml-intern agent (which
requires ANTHROPIC_API_KEY and heavy LLM calls), this module makes the same
lightweight HTTP calls that ml-intern's ``papers_tool.py`` uses under the hood:

* **HuggingFace Papers API** — trending daily papers and keyword search
* **Semantic Scholar API** — citation-count-ranked search across 200M+ papers
* **HuggingFace Datasets API** — discover datasets tagged for a topic

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
    "demand forecasting supply chain transformer",
    "inventory optimization deep learning",
    "on-time delivery prediction logistics",
    "time series forecasting manufacturing",
    "procurement analytics machine learning",
    "supplier risk prediction neural network",
    "production scheduling reinforcement learning",
    "logistics route optimization graph neural network",
]

# Rotate through topics in round-robin so every cycle covers different ground.
# The cursor is persisted in brain_kv between runs.
_TOPICS_PER_CYCLE = 5   # cover more ground per fetch; corpus ingests every round
_PAPERS_PER_TOPIC = 8
_DATASETS_PER_TOPIC = 5

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
    # Core supply chain
    "supply chain management",
    "supply chain planning",
    "logistics systems",
    # Systems engineering (the parent discipline)
    "systems engineering",
    "engineering systems design",
    "complex systems",
    # Operations research / optimization
    "operations research",
    "network optimization",
    "stochastic processes",
    # Manufacturing & production
    "manufacturing systems",
    "production planning",
    "industrial engineering",
    # Adjacent analytical foundations
    "inventory theory",
    "queuing theory",
    "simulation modeling",
]

_OCW_TOPICS_PER_CYCLE = 4   # OCW is slow-changing; 4 topics/cycle is plenty
_OCW_COURSES_PER_QUERY = 8

# External API endpoints (same as ml-intern/papers_tool.py)
_HF_API = "https://huggingface.co/api"
_S2_API = "https://api.semanticscholar.org"
_S2_TIMEOUT = 8
_HF_TIMEOUT = 8
_OCW_TIMEOUT = 25

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get_json(url: str, params: dict | None = None, timeout: int = _HF_TIMEOUT) -> Any:
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


# ---------------------------------------------------------------------------
# Paper discovery helpers
# ---------------------------------------------------------------------------

def search_papers_hf(query: str, limit: int = _PAPERS_PER_TOPIC) -> list[dict]:
    """Search HuggingFace Papers API for ``query``.

    Returns a list of paper dicts with keys:
    ``arxiv_id``, ``title``, ``summary``, ``upvotes``, ``url``.
    """
    data = _get_json(f"{_HF_API}/papers/search", params={"q": query, "limit": limit})
    if not isinstance(data, list):
        return []

    results: list[dict] = []
    for p in data[:limit]:
        arxiv_id = p.get("id", "")
        results.append({
            "arxiv_id": arxiv_id,
            "title": p.get("title", ""),
            "summary": (p.get("ai_summary") or p.get("summary") or "")[:400],
            "upvotes": p.get("upvotes", 0),
            "keywords": p.get("ai_keywords") or [],
            "url": f"https://huggingface.co/papers/{arxiv_id}" if arxiv_id else "",
            "source": "hf_papers",
            "query": query,
        })
    return results


def search_papers_semantic_scholar(
    query: str,
    limit: int = _PAPERS_PER_TOPIC,
    min_citations: int = 5,
) -> list[dict]:
    """Search Semantic Scholar for ``query``, ranked by citation count.

    Returns a list of paper dicts with keys:
    ``arxiv_id``, ``title``, ``summary``, ``citations``, ``year``, ``url``.
    """
    params: dict[str, Any] = {
        "query": query,
        "limit": limit,
        "fields": "title,externalIds,year,citationCount,tldr",
        "sort": "citationCount:desc",
    }
    if min_citations:
        params["minCitationCount"] = str(min_citations)

    data = _get_json(
        f"{_S2_API}/graph/v1/paper/search/bulk",
        params=params,
        timeout=_S2_TIMEOUT,
    )
    if not isinstance(data, dict):
        return []

    results: list[dict] = []
    for p in (data.get("data") or [])[:limit]:
        arxiv_id = (p.get("externalIds") or {}).get("ArXiv", "")
        tldr = (p.get("tldr") or {}).get("text", "")
        results.append({
            "arxiv_id": arxiv_id,
            "title": p.get("title", ""),
            "summary": tldr[:400] if tldr else "",
            "citations": p.get("citationCount", 0),
            "year": p.get("year"),
            "url": f"https://arxiv.org/abs/{arxiv_id}" if arxiv_id else "",
            "source": "semantic_scholar",
            "query": query,
        })
    return results


def fetch_trending_papers(limit: int = 10) -> list[dict]:
    """Fetch today's trending HuggingFace papers.

    Returns up to ``limit`` paper dicts.
    """
    data = _get_json(f"{_HF_API}/daily_papers", params={"limit": limit})
    if not isinstance(data, list):
        return []

    results: list[dict] = []
    for item in data[:limit]:
        paper = item.get("paper", item)
        arxiv_id = paper.get("id", "")
        results.append({
            "arxiv_id": arxiv_id,
            "title": paper.get("title", ""),
            "summary": (paper.get("ai_summary") or paper.get("summary") or "")[:400],
            "upvotes": paper.get("upvotes", 0),
            "keywords": paper.get("ai_keywords") or [],
            "url": f"https://huggingface.co/papers/{arxiv_id}" if arxiv_id else "",
            "source": "hf_trending",
            "query": "trending",
        })
    return results


def discover_hf_datasets(query: str, limit: int = _DATASETS_PER_TOPIC) -> list[dict]:
    """Discover HuggingFace datasets relevant to ``query``.

    Returns a list of dataset dicts with keys:
    ``dataset_id``, ``title``, ``downloads``, ``likes``, ``tags``, ``url``.
    """
    data = _get_json(
        f"{_HF_API}/datasets",
        params={"search": query, "limit": limit, "sort": "downloads", "direction": -1},
    )
    if not isinstance(data, list):
        return []

    results: list[dict] = []
    for ds in data[:limit]:
        ds_id = ds.get("id", "")
        tags = ds.get("tags") or []
        interesting_tags = [t for t in tags if not t.startswith(("arxiv:", "region:"))][:5]
        results.append({
            "dataset_id": ds_id,
            "title": ds_id,
            "description": (ds.get("description") or "")[:300],
            "downloads": ds.get("downloads", 0),
            "likes": ds.get("likes", 0),
            "tags": interesting_tags,
            "url": f"https://huggingface.co/datasets/{ds_id}" if ds_id else "",
            "source": "hf_datasets",
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
        with sqlite3.connect(db) as cn:
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
# Knowledge corpus persistence
# ---------------------------------------------------------------------------

def _get_db_path() -> Path:
    """Locate the Brain's SQLite database."""
    _sys_path_insert = str(Path(__file__).resolve().parents[2])
    if _sys_path_insert not in sys.path:
        sys.path.insert(0, _sys_path_insert)
    from src.brain.local_store import db_path as _db_path
    return _db_path()


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


def persist_research_findings(
    papers: list[dict],
    datasets: list[dict],
    topic: str,
) -> int:
    """Write research findings to the Brain's ``learning_log``.

    Papers are de-duplicated by ``arxiv_id`` + ``kind="ml_research"``.
    Datasets are de-duplicated by ``dataset_id``.

    Returns the number of new rows written.
    """
    written = 0
    try:
        db = _get_db_path()
        with sqlite3.connect(db) as cn:
            _ensure_learning_log(cn)

            for paper in papers:
                arxiv_id = paper.get("arxiv_id", "")
                title_key = f"[ml_research] {arxiv_id}" if arxiv_id else f"[ml_research] {paper.get('title','')[:80]}"
                if not title_key.strip():
                    continue

                existing = cn.execute(
                    "SELECT id FROM learning_log WHERE kind='ml_research' AND title=?",
                    (title_key,),
                ).fetchone()
                if existing:
                    continue

                signal = min(1.0, (paper.get("upvotes", 0) + paper.get("citations", 0) * 0.1) / 50.0)
                cn.execute(
                    """INSERT INTO learning_log(logged_at, kind, title, detail, signal_strength)
                       VALUES(?,?,?,?,?)""",
                    (
                        datetime.now(timezone.utc).isoformat(),
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
            with sqlite3.connect(db) as cn:
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
    cycle covers fresh ground.  Fetches papers from both HuggingFace and
    Semantic Scholar, plus relevant datasets, then writes each unique
    finding as a ``kind="ml_research"`` entry in ``learning_log``.

    When ``include_ocw=True`` (default), also sweeps MIT OpenCourseWare
    across :data:`_OCW_TOPICS` — supply chain structures are a physical
    manifestation of systems engineering, so OCW coverage spans both the
    SC management and systems engineering / operations research disciplines.

    Args:
        topics: Override ML topic list (``None`` uses the built-in rotation).
        topics_per_cycle: How many ML topics to research this cycle.
        include_trending: If ``True``, also fetch today's trending HF papers.
        include_ocw: If ``True``, sweep MIT OCW alongside the ML sources.

    Returns:
        ``{topics_researched, papers_found, datasets_found, learnings_written,
           ocw_courses_found, ocw_learnings_written}``
    """
    # Determine which topics to cover this cycle via round-robin cursor
    if topics is None:
        try:
            db = _get_db_path()
            with sqlite3.connect(db) as cn:
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
            with sqlite3.connect(db) as cn:
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

    # Trending papers (one-shot fetch, not topic-specific)
    if include_trending:
        trending = fetch_trending_papers(limit=5)
        if trending:
            all_papers.extend(trending)
            log.debug(f"ml_research: fetched {len(trending)} trending papers")

    # Per-topic paper + dataset search
    for topic in selected:
        log.debug(f"ml_research: researching topic '{topic}'")

        # HF Papers search
        hf_papers = search_papers_hf(topic, limit=_PAPERS_PER_TOPIC)
        all_papers.extend(hf_papers)

        # Semantic Scholar search (higher citation threshold for quality)
        s2_papers = search_papers_semantic_scholar(topic, limit=_PAPERS_PER_TOPIC, min_citations=10)
        all_papers.extend(s2_papers)

        # Dataset discovery
        datasets = discover_hf_datasets(topic, limit=_DATASETS_PER_TOPIC)
        all_datasets.extend(datasets)

        topics_researched.append(topic)
        time.sleep(0.5)  # polite rate-limiting between topics

    # Persist all ML findings
    written = persist_research_findings(all_papers, all_datasets, topic=", ".join(topics_researched))

    # -----------------------------------------------------------------------
    # MIT OCW sweep — supply chain as systems engineering
    # -----------------------------------------------------------------------
    ocw_courses_found = 0
    ocw_written = 0
    if include_ocw:
        try:
            db = _get_db_path()
            with sqlite3.connect(db) as cn:
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
            time.sleep(1.0)   # polite crawl delay — OCW is a public resource

        ocw_courses_found = len(all_ocw_courses)
        if all_ocw_courses:
            ocw_written = persist_ocw_courses(
                all_ocw_courses,
                topic=", ".join(ocw_selected),
            )

        try:
            db = _get_db_path()
            with sqlite3.connect(db) as cn:
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
        with sqlite3.connect(db) as cn:
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
    """Convenience function for the Streamlit page: run a one-shot search.

    Returns:
        ``{hf_papers: [...], s2_papers: [...], datasets: [...]}``
    """
    return {
        "hf_papers": search_papers_hf(query, limit=8),
        "s2_papers": search_papers_semantic_scholar(query, limit=5, min_citations=0),
        "datasets": discover_hf_datasets(query, limit=5),
    }


def search_ocw_interactive(query: str) -> list[dict]:
    """Search MIT OCW for ``query`` and return course list (for Streamlit page).

    Returns a list of course dicts from :func:`fetch_ocw_courses`.
    """
    return fetch_ocw_courses(query, limit=12)
