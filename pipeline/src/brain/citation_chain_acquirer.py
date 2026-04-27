"""citation_chain_acquirer.py — Recursive citation-chain follower for the Brain.

The Brain already acquires papers via topic-keyword searches (``ml_research.py``).
This module extends that by **following the bibliography chains** inside those
papers:  for every paper we already know about, fetch its references, persist
each referenced paper, then fetch *their* references — recursively up to a
configurable depth.

The result is a continuously expanding research frontier that deepens around
the papers most relevant to supply-chain intelligence, rather than staying
at a fixed keyword-search horizon.

Sources used (all free, no API key required):
* **Semantic Scholar Graph API** — best citation data; 200 M+ papers
  ``https://api.semanticscholar.org/graph/v1/paper/{id}/references``
* **OpenAlex referenced_works** — 200 M+ works; referenced_works list in the
  work object gives direct foreign-key IDs for further expansion
* **arXiv API** — resolves arXiv IDs to metadata when needed

Supply-chain relevance filter prevents the frontier from drifting into pure
physics / biology: each candidate paper is scored by keyword overlap with the
``_SC_KEYWORDS`` set; papers below the threshold are dropped.

Schema:
    ``citation_chain_state`` (in local_brain.sqlite) — tracks which papers have
    been expanded so we never re-fetch.

Deduplication:
    ``brain_kv`` key ``citation_chain:seen`` — JSON-encoded set of canonical
    paper IDs that have been persisted to ``learning_log``.

Public API:
    seed_from_learning_log(cn, limit=100) -> list[str]
    seed_from_brain_kv(cn) -> list[str]
    run_citation_expansion_cycle(max_depth=3, max_papers_per_run=200) -> dict
    schedule_in_background(interval_s=3600) -> threading.Thread
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

_SEMANTIC_SCHOLAR_API = "https://api.semanticscholar.org/graph/v1"
_OPENALEX_API = "https://api.openalex.org"
_ARXIV_API = "https://export.arxiv.org/api/query"

_SS_TIMEOUT = 15        # Semantic Scholar: strict 1 req/s free tier
_OA_TIMEOUT = 10        # OpenAlex: polite pool, up to 10 req/s
_ARXIV_TIMEOUT = 15

_SS_RATE_LIMIT_S = 1.1  # conservative — 1 req/s + buffer
_OA_RATE_LIMIT_S = 0.2  # 5 req/s — polite-pool tier

_DEFAULT_MAX_DEPTH = 3
_DEFAULT_MAX_PAPERS = 200
_SC_RELEVANCE_THRESHOLD = 0.08   # fraction of SC keywords that must appear
_GUIDELINE_RELEVANCE_THRESHOLD = 0.05
_SEEN_KEY = "citation_chain:seen"

# Supply-chain keyword set for relevance scoring
_SC_KEYWORDS: frozenset[str] = frozenset([
    "supply chain", "inventory", "procurement", "logistics", "demand",
    "forecasting", "supplier", "warehouse", "distribution", "replenishment",
    "purchasing", "vendor", "lead time", "reorder", "safety stock",
    "order quantity", "economic order", "eoq", "otd", "on-time delivery",
    "transportation", "routing", "scheduling", "production planning",
    "manufacturing", "operations", "optimization", "stochastic",
    "multi-echelon", "shortage", "backorder", "fill rate", "service level",
    "lot sizing", "bullwhip", "risk pooling", "network design",
    "facility location", "carrier", "freight", "last mile",
    "spend analysis", "category management", "sourcing", "contract",
    "rfq", "purchase order", "po", "invoice", "accounts payable",
    "cycle count", "ABC analysis", "pareto", "turnover",
    "machine learning", "deep learning", "reinforcement learning",
    "transformer", "neural network", "time series", "prediction",
    "classification", "anomaly detection", "nlp", "llm", "rag",
    "knowledge graph", "graph neural", "gnn",
])

# Broader cross-domain bearings from the Creator's Grok Works Cited.  These are
# used only when a frontier descends from the Pirates Code guideline seed path;
# they let the Brain move beyond the original exploratory conversation without
# turning every citation into supply-chain doctrine.
_GUIDELINE_KEYWORDS: frozenset[str] = frozenset([
    "quantum", "entropy", "entanglement", "gravity", "cosmology",
    "spacetime", "topological", "holographic", "density matrix",
    "loop quantum", "fast radio burst", "magnetar", "tokamak", "fusion",
    "plasma", "magneto", "spin", "vortex", "vortices", "magnon",
    "neuromorphic", "reservoir computing", "materials", "nanomagnet",
    "biohybrid", "cryptochrome", "graph", "complex systems", "optimization",
    "simulation", "control", "adaptive", "machine learning", "systems",
])


# ---------------------------------------------------------------------------
# Database path helpers (identical pattern to ml_research.py)
# ---------------------------------------------------------------------------

def _get_db_path() -> Path:
    """Return the Brain's primary SQLite path (local_brain.sqlite)."""
    _root = str(Path(__file__).resolve().parents[2])
    if _root not in sys.path:
        sys.path.insert(0, _root)
    from src.brain.local_store import db_path as _db_path  # type: ignore
    return _db_path()


@contextmanager
def _conn():
    db = _get_db_path()
    cn = sqlite3.connect(str(db))
    cn.row_factory = sqlite3.Row
    try:
        yield cn
        cn.commit()
    finally:
        cn.close()


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------

def _ensure_schema(cn: sqlite3.Connection) -> None:
    """Create tables required by this module (idempotent)."""
    cn.executescript(
        """
        CREATE TABLE IF NOT EXISTS learning_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            logged_at       TEXT    NOT NULL,
            kind            TEXT    NOT NULL,
            title           TEXT    NOT NULL,
            detail          TEXT,
            signal_strength REAL,
            source_table    TEXT,
            source_row_id   INTEGER
        );
        CREATE INDEX IF NOT EXISTS ix_ll_kind ON learning_log(kind, logged_at);

        CREATE TABLE IF NOT EXISTS brain_kv (
            key        TEXT PRIMARY KEY,
            value      TEXT,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS corpus_edge (
            src_id      TEXT NOT NULL,
            src_type    TEXT NOT NULL,
            dst_id      TEXT NOT NULL,
            dst_type    TEXT NOT NULL,
            rel         TEXT NOT NULL,
            weight      REAL NOT NULL DEFAULT 1.0,
            last_seen   TEXT NOT NULL,
            samples     INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (src_id, src_type, dst_id, dst_type, rel)
        );

        CREATE TABLE IF NOT EXISTS citation_chain_state (
            paper_id            TEXT PRIMARY KEY,
            depth               INTEGER NOT NULL DEFAULT 0,
            parent_id           TEXT,
            source_api          TEXT,
            ref_count           INTEGER NOT NULL DEFAULT 0,
            sc_relevance_score  REAL    NOT NULL DEFAULT 0.0,
            fetched_at          TEXT,
            logged_at           TEXT    NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_ccs_depth
            ON citation_chain_state(depth, fetched_at);
        """
    )


# ---------------------------------------------------------------------------
# Brain-KV helpers
# ---------------------------------------------------------------------------

def _kv_get(cn: sqlite3.Connection, key: str) -> str | None:
    row = cn.execute("SELECT value FROM brain_kv WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def _kv_set(cn: sqlite3.Connection, key: str, value: str) -> None:
    cn.execute(
        "INSERT OR REPLACE INTO brain_kv(key, value, updated_at) VALUES(?,?,?)",
        (key, value, datetime.now(timezone.utc).isoformat()),
    )


def _load_seen_ids(cn: sqlite3.Connection) -> set[str]:
    raw = _kv_get(cn, _SEEN_KEY)
    if raw:
        try:
            return set(json.loads(raw))
        except Exception:
            pass
    return set()


def _save_seen_ids(cn: sqlite3.Connection, seen: set[str]) -> None:
    _kv_set(cn, _SEEN_KEY, json.dumps(sorted(seen)))


# ---------------------------------------------------------------------------
# HTTP helpers (same truststore → certifi → default SSL pattern as ml_research)
# ---------------------------------------------------------------------------

def _make_ssl_ctx():
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


def _http_get_json(url: str, params: dict | None = None, timeout: int = 10,
                   headers: dict | None = None) -> Any:
    """HTTP GET → parsed JSON; returns None on any failure."""
    import urllib.request
    import urllib.parse
    ctx = _make_ssl_ctx()
    try:
        full_url = url if not params else url + "?" + urllib.parse.urlencode(params)
        req_headers = {"User-Agent": "SupplyChainBrain/1.0 (citation-chain-acquirer)"}
        if headers:
            req_headers.update(headers)
        req = urllib.request.Request(full_url, headers=req_headers)
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception as exc:
        log.debug("citation_chain _http_get_json(%s): %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Paper ID normalisation
# ---------------------------------------------------------------------------

def _normalize_id(paper: dict) -> str | None:
    """Return a canonical dedup key for a paper dict.

    Priority: DOI > arXiv ID > Semantic Scholar ID > OpenAlex ID > title slug.
    """
    explicit = (paper.get("paper_id") or "").strip().lower()
    if explicit.startswith(("doi:", "arxiv:", "ss:", "oa:")):
        return explicit
    doi = (paper.get("doi") or "").strip().lower()
    if doi:
        return f"doi:{doi}"
    arxiv = (paper.get("arxiv_id") or "").strip()
    if arxiv and not arxiv.startswith(("openalex:", "core:", "ntrs:")):
        return f"arxiv:{arxiv}"
    ss = (paper.get("ss_id") or "").strip()
    if ss:
        return f"ss:{ss}"
    oa = (paper.get("openalex_id") or "").strip()
    if oa:
        return f"oa:{oa}"
    title = (paper.get("title") or "").strip().lower()
    if title:
        slug = re.sub(r"[^a-z0-9]+", "_", title)[:60]
        return f"title:{slug}"
    return None


def _sc_relevance(title: str, abstract: str, keywords: list[str]) -> float:
    """Score [0..1] based on supply-chain keyword overlap in the paper text."""
    text = " ".join([title, abstract] + keywords).lower()
    tokens = set(re.findall(r"[a-z]+(?:\s+[a-z]+)?", text))
    hits = sum(1 for kw in _SC_KEYWORDS if kw in text)
    return min(1.0, hits / max(1, len(_SC_KEYWORDS) * 0.15))


def _guideline_relevance(title: str, abstract: str, keywords: list[str]) -> float:
    """Score [0..1] for cross-domain Works Cited guideline expansion."""
    text = " ".join([title, abstract] + keywords).lower()
    hits = sum(1 for kw in _GUIDELINE_KEYWORDS if kw in text)
    return min(1.0, hits / max(1, len(_GUIDELINE_KEYWORDS) * 0.20))


def _is_guideline_frontier(cn: sqlite3.Connection, paper_id: str) -> bool:
    """Return True if paper_id was seeded by the Grok Works Cited guideline."""
    try:
        row = cn.execute(
            """SELECT source_api, parent_id FROM citation_chain_state
               WHERE paper_id=? LIMIT 1""",
            (paper_id,),
        ).fetchone()
        if not row:
            return False
        source = (row["source_api"] or "").lower()
        if "grok_works_cited" in source or "pirates_code" in source:
            return True
        parent_id = row["parent_id"]
        if parent_id and parent_id != paper_id:
            return _is_guideline_frontier(cn, parent_id)
    except Exception:
        return False
    return False


# ---------------------------------------------------------------------------
# Semantic Scholar API
# ---------------------------------------------------------------------------

_ss_last_call: float = 0.0


def _ss_rate_limit() -> None:
    global _ss_last_call
    elapsed = time.monotonic() - _ss_last_call
    if elapsed < _SS_RATE_LIMIT_S:
        time.sleep(_SS_RATE_LIMIT_S - elapsed)
    _ss_last_call = time.monotonic()


def fetch_references_semantic_scholar(paper_id: str, limit: int = 50) -> list[dict]:
    """Fetch cited papers for ``paper_id`` via Semantic Scholar Graph API.

    ``paper_id`` can be:
    * ``10.xxxx/yyyy``          — DOI
    * ``arXiv:NNNN.NNNNN``      — arXiv ID
    * Semantic Scholar corpus ID (pure number)

    Returns list of paper dicts with keys:
    ``ss_id``, ``title``, ``abstract``, ``year``, ``doi``, ``arxiv_id``,
    ``citation_count``, ``authors``, ``url``, ``source="semantic_scholar"``.
    """
    _ss_rate_limit()

    # Semantic Scholar accepts DOI and arXiv: prefixes directly
    if paper_id.startswith("doi:"):
        ss_lookup = paper_id[4:]
    elif paper_id.startswith("arxiv:"):
        ss_lookup = "arXiv:" + paper_id[6:]
    elif paper_id.startswith("ss:"):
        ss_lookup = paper_id[3:]
    else:
        ss_lookup = paper_id  # bare corpus ID or direct DOI

    url = f"{_SEMANTIC_SCHOLAR_API}/paper/{ss_lookup}/references"
    data = _http_get_json(
        url,
        params={
            "fields": "title,abstract,year,authors,externalIds,citationCount,publicationTypes",
            "limit": limit,
        },
        timeout=_SS_TIMEOUT,
    )
    if not isinstance(data, dict) or "data" not in data:
        return []

    results: list[dict] = []
    for item in (data.get("data") or []):
        ref = item.get("citedPaper") or {}
        if not ref:
            continue
        external = ref.get("externalIds") or {}
        doi = (external.get("DOI") or "").lower().strip()
        arxiv_id = (external.get("ArXiv") or "").strip()
        ss_id = str(ref.get("paperId") or "").strip()
        title = (ref.get("title") or "").strip()
        abstract = (ref.get("abstract") or "")[:400]
        year = ref.get("year")
        citation_count = ref.get("citationCount", 0) or 0
        authors = [
            a.get("name", "") for a in (ref.get("authors") or [])[:4]
        ]
        url_ref = (
            f"https://arxiv.org/abs/{arxiv_id}" if arxiv_id
            else (f"https://doi.org/{doi}" if doi
                  else (f"https://www.semanticscholar.org/paper/{ss_id}" if ss_id else ""))
        )
        results.append({
            "ss_id": ss_id,
            "title": title,
            "abstract": abstract,
            "year": year,
            "doi": doi,
            "arxiv_id": arxiv_id,
            "citation_count": citation_count,
            "authors": authors,
            "url": url_ref,
            "source": "semantic_scholar",
        })

    log.debug("SS references for %s: %d papers", paper_id, len(results))
    return results


# ---------------------------------------------------------------------------
# OpenAlex referenced_works API
# ---------------------------------------------------------------------------

_oa_last_call: float = 0.0


def _oa_rate_limit() -> None:
    global _oa_last_call
    elapsed = time.monotonic() - _oa_last_call
    if elapsed < _OA_RATE_LIMIT_S:
        time.sleep(_OA_RATE_LIMIT_S - elapsed)
    _oa_last_call = time.monotonic()


def fetch_references_openalex(paper_id: str, limit: int = 50) -> list[dict]:
    """Fetch cited papers for ``paper_id`` via OpenAlex ``referenced_works``.

    ``paper_id`` accepted forms:
    * ``doi:10.xxxx/yyyy``      — resolves via ``https://api.openalex.org/works/doi:...``
    * ``arxiv:NNNN.NNNNN``      — resolves via ``https://api.openalex.org/works/arxiv:...``
    * ``oa:W123456789``         — direct OpenAlex work ID

    Returns list of paper dicts with ``source="openalex"``.
    """
    _oa_rate_limit()

    if paper_id.startswith("doi:"):
        oa_lookup = f"doi:{paper_id[4:]}"
    elif paper_id.startswith("arxiv:"):
        oa_lookup = f"arxiv:{paper_id[6:]}"
    elif paper_id.startswith("oa:"):
        oa_lookup = paper_id[3:]
    else:
        oa_lookup = paper_id

    # Step 1: fetch the parent work to get referenced_works list
    work_data = _http_get_json(
        f"{_OPENALEX_API}/works/{oa_lookup}",
        params={
            "select": "id,referenced_works",
            "mailto": "brain@supplychainbrain.local",
        },
        timeout=_OA_TIMEOUT,
    )
    if not isinstance(work_data, dict):
        return []

    ref_ids: list[str] = (work_data.get("referenced_works") or [])[:limit]
    if not ref_ids:
        return []

    # Step 2: batch-fetch metadata for referenced works (OpenAlex filter API)
    oa_ids = "|".join(r.replace("https://openalex.org/", "") for r in ref_ids[:50])
    _oa_rate_limit()
    batch = _http_get_json(
        f"{_OPENALEX_API}/works",
        params={
            "filter": f"openalex_id:{oa_ids}",
            "select": "id,title,doi,publication_year,cited_by_count,concepts,primary_location,abstract_inverted_index",
            "per-page": min(50, len(ref_ids)),
            "mailto": "brain@supplychainbrain.local",
        },
        timeout=_OA_TIMEOUT,
    )
    if not isinstance(batch, dict):
        return []

    results: list[dict] = []
    for w in (batch.get("results") or []):
        oa_id = (w.get("id") or "").replace("https://openalex.org/", "")
        doi = (w.get("doi") or "").replace("https://doi.org/", "").lower().strip()
        title = (w.get("title") or "").strip()
        year = w.get("publication_year")
        cites = w.get("cited_by_count", 0) or 0
        concepts = [c.get("display_name", "") for c in (w.get("concepts") or []) if c.get("score", 0) > 0.3]
        summary = "; ".join(concepts[:6])
        loc = w.get("primary_location") or {}
        url = loc.get("landing_page_url", "") or (f"https://doi.org/{doi}" if doi else "")
        arxiv_id = ""
        if url and "arxiv.org/abs/" in url:
            arxiv_id = url.split("/abs/")[-1].split("v")[0]

        # Reconstruct abstract from inverted index if present
        inv_idx = w.get("abstract_inverted_index") or {}
        if inv_idx:
            try:
                max_pos = max(max(v) for v in inv_idx.values()) + 1
                words = [""] * max_pos
                for word, positions in inv_idx.items():
                    for pos in positions:
                        if 0 <= pos < max_pos:
                            words[pos] = word
                abstract = " ".join(words[:80])
            except Exception:
                abstract = summary
        else:
            abstract = summary

        results.append({
            "openalex_id": oa_id,
            "title": title,
            "abstract": abstract[:400],
            "year": year,
            "doi": doi,
            "arxiv_id": arxiv_id,
            "citation_count": cites,
            "authors": [],
            "url": url,
            "source": "openalex",
        })

    log.debug("OA references for %s: %d papers", paper_id, len(results))
    return results


# ---------------------------------------------------------------------------
# Seed extraction — mine existing learning_log + brain_kv
# ---------------------------------------------------------------------------

def seed_from_learning_log(cn: sqlite3.Connection, limit: int = 100) -> list[str]:
    """Extract canonical paper IDs from existing ``learning_log`` entries.

    Looks at entries with ``kind IN ('ml_research', 'citation_chain')`` and
    extracts DOIs and arXiv IDs from their JSON ``detail`` blobs.

    Returns a deduplicated list of canonical IDs in priority order
    (most-cited / highest signal first).
    """
    rows = cn.execute(
        """SELECT detail, signal_strength FROM learning_log
           WHERE kind IN ('ml_research', 'citation_chain')
           ORDER BY signal_strength DESC, id DESC
           LIMIT ?""",
        (limit * 3,),  # over-fetch to compensate for non-parseable rows
    ).fetchall()

    seen: set[str] = set()
    ids: list[str] = []
    for row in rows:
        if len(ids) >= limit:
            break
        try:
            blob = json.loads(row["detail"] or "{}")
            paper = blob.get("paper") or blob.get("ref_paper") or {}
            doi = (paper.get("doi") or "").lower().strip()
            arxiv = (paper.get("arxiv_id") or "").strip()
            if doi and not doi.startswith(("openalex:", "core:", "ntrs:")):
                key = f"doi:{doi}"
                if key not in seen:
                    seen.add(key)
                    ids.append(key)
                    continue
            if arxiv and not arxiv.startswith(("openalex:", "core:", "ntrs:")):
                key = f"arxiv:{arxiv}"
                if key not in seen:
                    seen.add(key)
                    ids.append(key)
        except Exception:
            pass

    log.debug("seed_from_learning_log: %d paper IDs extracted", len(ids))
    return ids


def seed_from_brain_kv(cn: sqlite3.Connection) -> list[str]:
    """Extract paper IDs injected via brain_kv by user or Grok research threads.

    Looks for keys matching:
    * ``grok_research:bibliography:*``  — user-injected bibliography entries
    * ``citation_chain:seed:*``         — manually seeded paper IDs
    * ``grok_research:paper:*``         — individual paper records

    Returns list of canonical IDs.
    """
    rows = cn.execute(
        """SELECT key, value FROM brain_kv
           WHERE key LIKE 'grok_research:%' OR key LIKE 'citation_chain:seed:%'""",
    ).fetchall()

    ids: list[str] = []
    seen: set[str] = set()
    for row in rows:
        try:
            val = row["value"] or ""
            # Try JSON parse first
            blob = json.loads(val)
            if isinstance(blob, str):
                # Plain ID stored as JSON string
                if blob not in seen:
                    seen.add(blob)
                    ids.append(blob)
                continue
            if isinstance(blob, list):
                for item in blob:
                    if isinstance(item, str) and item not in seen:
                        seen.add(item)
                        ids.append(item)
                    elif isinstance(item, dict):
                        pid = _normalize_id(item)
                        if pid and pid not in seen:
                            seen.add(pid)
                            ids.append(pid)
                continue
            if isinstance(blob, dict):
                pid = _normalize_id(blob)
                if pid and pid not in seen:
                    seen.add(pid)
                    ids.append(pid)
        except (json.JSONDecodeError, TypeError):
            # Raw string stored directly
            val = val.strip()
            if val and val not in seen:
                seen.add(val)
                ids.append(val)

    log.debug("seed_from_brain_kv: %d paper IDs extracted", len(ids))
    return ids


def _guideline_seed_ids(cn: sqlite3.Connection) -> set[str]:
    """Return paper IDs specifically seeded by the Grok Works Cited guideline."""
    rows = cn.execute(
        """SELECT value FROM brain_kv
           WHERE key='grok_research:bibliography:works_cited'
              OR key LIKE 'grok_research:pirates_code:%'""",
    ).fetchall()
    ids: set[str] = set()
    for row in rows:
        try:
            blob = json.loads(row["value"] or "[]")
        except Exception:
            blob = row["value"]
        items = blob if isinstance(blob, list) else [blob]
        for item in items:
            if isinstance(item, str):
                if item.startswith(("doi:", "arxiv:", "ss:", "oa:")):
                    ids.add(item.lower())
            elif isinstance(item, dict):
                pid = _normalize_id(item)
                if pid:
                    ids.add(pid)
    return ids


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def _log_paper(
    cn: sqlite3.Connection,
    paper: dict,
    depth: int,
    parent_id: str | None,
    source_api: str,
) -> tuple[str | None, bool]:
    """Insert a cited paper into ``learning_log`` as ``kind="citation_chain"``.

    Returns ``(canonical_id, was_new)`` — was_new=False if already present.
    """
    pid = _normalize_id(paper)
    if not pid:
        return None, False

    title = (paper.get("title") or "").strip()
    if not title:
        return None, False

    title_key = f"[citation_chain] {pid}"

    existing = cn.execute(
        "SELECT id FROM learning_log WHERE kind='citation_chain' AND title=?",
        (title_key,),
    ).fetchone()
    if existing:
        return pid, False

    sc_score = _sc_relevance(
        title,
        paper.get("abstract") or "",
        paper.get("keywords") or [],
    )

    cites = paper.get("citation_count", 0) or 0
    signal = min(1.0, 0.3 + sc_score * 0.4 + min(cites, 500) / 2000.0)

    now = datetime.now(timezone.utc).isoformat()
    cn.execute(
        """INSERT INTO learning_log(logged_at, kind, title, detail, signal_strength)
           VALUES(?,?,?,?,?)""",
        (
            now,
            "citation_chain",
            title_key,
            json.dumps({
                "ref_paper": paper,
                "parent_id": parent_id,
                "depth": depth,
                "source_api": source_api,
                "sc_relevance": sc_score,
                "type": "citation_chain",
            }, default=str),
            signal,
        ),
    )

    # Track in citation_chain_state
    cn.execute(
        """INSERT OR IGNORE INTO citation_chain_state
               (paper_id, depth, parent_id, source_api, ref_count, sc_relevance_score, logged_at)
           VALUES(?,?,?,?,?,?,?)""",
        (pid, depth, parent_id, source_api,
         paper.get("citation_count", 0) or 0, sc_score, now),
    )

    return pid, True


def _upsert_corpus_edge(
    cn: sqlite3.Connection,
    src_id: str,
    dst_id: str,
) -> None:
    """Insert or reinforce a PAPER→CITES→PAPER edge in ``corpus_edge``."""
    now = datetime.now(timezone.utc).isoformat()
    cn.execute(
        """INSERT INTO corpus_edge(src_id, src_type, dst_id, dst_type, rel, weight, last_seen, samples)
           VALUES(?,?,?,?,?,?,?,1)
           ON CONFLICT(src_id, src_type, dst_id, dst_type, rel)
           DO UPDATE SET weight  = MIN(weight + 0.1, 5.0),
                         last_seen = excluded.last_seen,
                         samples   = samples + 1""",
        (src_id, "Paper", dst_id, "Paper", "CITES", 1.0, now),
    )


def _mark_expanded(cn: sqlite3.Connection, paper_id: str, ref_count: int) -> None:
    """Record that ``paper_id`` has been expanded (references fetched)."""
    cn.execute(
        """UPDATE citation_chain_state
           SET fetched_at = ?, ref_count = ?
           WHERE paper_id = ?""",
        (datetime.now(timezone.utc).isoformat(), ref_count, paper_id),
    )


# ---------------------------------------------------------------------------
# Core expansion logic
# ---------------------------------------------------------------------------

def _expand_paper(
    cn: sqlite3.Connection,
    paper_id: str,
    depth: int,
    seen: set[str],
    stats: dict,
    max_papers: int,
) -> list[str]:
    """Fetch references for ``paper_id`` and persist new ones.

    Tries Semantic Scholar first; falls back to OpenAlex.
    Returns a list of new canonical IDs discovered (for next-depth expansion).
    """
    if stats["papers_logged"] >= max_papers:
        return []

    # Try Semantic Scholar first
    refs = fetch_references_semantic_scholar(paper_id)
    source_api = "semantic_scholar"
    if not refs:
        refs = fetch_references_openalex(paper_id)
        source_api = "openalex"

    new_ids: list[str] = []
    for ref in refs:
        if stats["papers_logged"] >= max_papers:
            break

        # Supply-chain relevance gate.  If this frontier descends from the
        # Creator's Works Cited Pirates Code, also allow broader cross-domain
        # systems references; those are bearings for exploration, not doctrine.
        sc_score = _sc_relevance(
            ref.get("title") or "",
            ref.get("abstract") or "",
            ref.get("keywords") or [],
        )
        guideline_score = 0.0
        if sc_score < _SC_RELEVANCE_THRESHOLD and _is_guideline_frontier(cn, paper_id):
            guideline_score = _guideline_relevance(
                ref.get("title") or "",
                ref.get("abstract") or "",
                ref.get("keywords") or [],
            )
        if sc_score < _SC_RELEVANCE_THRESHOLD and guideline_score < _GUIDELINE_RELEVANCE_THRESHOLD:
            stats["filtered_low_relevance"] += 1
            continue

        child_id = _normalize_id(ref)
        if not child_id or child_id in seen:
            stats["deduped"] += 1
            continue
        seen.add(child_id)

        pid, was_new = _log_paper(cn, ref, depth, paper_id, source_api)
        if was_new and pid:
            stats["papers_logged"] += 1
            _upsert_corpus_edge(cn, paper_id, pid)
            stats["edges_added"] += 1
            new_ids.append(pid)
        elif pid:
            stats["deduped"] += 1

    _mark_expanded(cn, paper_id, len(refs))
    log.debug(
        "expand_paper(%s, depth=%d): %d refs → %d new",
        paper_id, depth, len(refs), len(new_ids),
    )
    return new_ids


# ---------------------------------------------------------------------------
# Main cycle entry point
# ---------------------------------------------------------------------------

def run_citation_expansion_cycle(
    max_depth: int = _DEFAULT_MAX_DEPTH,
    max_papers_per_run: int = _DEFAULT_MAX_PAPERS,
) -> dict:
    """Run one citation-expansion cycle.

    1. Seed from ``learning_log`` and ``brain_kv``.
    2. Expand each seed (fetch its references) at depth=1.
    3. For each new paper found, expand at depth=2, etc. up to ``max_depth``.
    4. Persist every new paper as ``kind="citation_chain"`` in ``learning_log``.
    5. Persist PAPER→CITES→PAPER edges in ``corpus_edge``.

    Returns a stats dict with counts of papers found, logged, filtered, edges added.
    """
    stats: dict[str, Any] = {
        "depth_reached": 0,
        "seeds": 0,
        "papers_logged": 0,
        "edges_added": 0,
        "deduped": 0,
        "filtered_low_relevance": 0,
        "errors": 0,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        with _conn() as cn:
            _ensure_schema(cn)

            # Load the global seen set (persisted across runs for dedup)
            seen = _load_seen_ids(cn)

            # Gather seeds
            ll_seeds = seed_from_learning_log(cn, limit=100)
            kv_seeds = seed_from_brain_kv(cn)
            guideline_seeds = _guideline_seed_ids(cn)

            # Also seed from citation_chain_state: papers not yet expanded
            unexpanded = cn.execute(
                """SELECT paper_id FROM citation_chain_state
                   WHERE fetched_at IS NULL
                   ORDER BY sc_relevance_score DESC, ref_count DESC
                   LIMIT 50"""
            ).fetchall()
            state_seeds = [r["paper_id"] for r in unexpanded]

            all_seeds = list({*ll_seeds, *kv_seeds, *state_seeds})
            stats["seeds"] = len(all_seeds)
            log.info("citation_chain: %d seeds (ll=%d kv=%d state=%d)",
                     len(all_seeds), len(ll_seeds), len(kv_seeds), len(state_seeds))

            # Ensure all seeds are in citation_chain_state at depth=0
            now = datetime.now(timezone.utc).isoformat()
            for seed_id in all_seeds:
                seed_source = (
                    "grok_works_cited_pirates_code"
                    if seed_id in guideline_seeds else "seed"
                )
                cn.execute(
                    """INSERT OR IGNORE INTO citation_chain_state
                           (paper_id, depth, parent_id, source_api, ref_count,
                            sc_relevance_score, logged_at)
                       VALUES(?,0,NULL,'seed',0,0.5,?)""",
                    (seed_id, now),
                )
                if seed_source != "seed":
                    cn.execute(
                        """UPDATE citation_chain_state
                           SET source_api=?
                           WHERE paper_id=? AND (source_api IS NULL OR source_api='seed')""",
                        (seed_source, seed_id),
                    )
            cn.commit()

            # BFS expansion: frontier[depth] → list of paper IDs to expand
            frontier: list[str] = all_seeds
            for depth in range(1, max_depth + 1):
                if not frontier or stats["papers_logged"] >= max_papers_per_run:
                    break
                stats["depth_reached"] = depth
                next_frontier: list[str] = []

                for pid in frontier:
                    if stats["papers_logged"] >= max_papers_per_run:
                        break
                    try:
                        new_ids = _expand_paper(
                            cn, pid, depth, seen, stats, max_papers_per_run
                        )
                        next_frontier.extend(new_ids)
                    except Exception as exc:
                        log.warning("citation_chain expand(%s): %s", pid, exc)
                        stats["errors"] += 1

                    # Periodic commit to avoid large transactions
                    if stats["papers_logged"] % 20 == 0:
                        _save_seen_ids(cn, seen)
                        cn.commit()

                frontier = next_frontier
                log.info(
                    "citation_chain depth=%d: +%d papers, frontier=%d",
                    depth, stats["papers_logged"], len(frontier),
                )

            # Final persist of seen set
            _save_seen_ids(cn, seen)
            cn.commit()

    except Exception as exc:
        log.error("run_citation_expansion_cycle error: %s", exc, exc_info=True)
        stats["errors"] += 1

    stats["finished_at"] = datetime.now(timezone.utc).isoformat()
    log.info(
        "citation_chain cycle done: seeds=%d logged=%d edges=%d depth=%d filtered=%d errors=%d",
        stats["seeds"], stats["papers_logged"], stats["edges_added"],
        stats["depth_reached"], stats["filtered_low_relevance"], stats["errors"],
    )
    return stats


# ---------------------------------------------------------------------------
# Background scheduler
# ---------------------------------------------------------------------------

_bg_thread: threading.Thread | None = None
_bg_stop = threading.Event()


def schedule_in_background(interval_s: int = 3600) -> threading.Thread:
    """Start the citation-expansion cycle in a background daemon thread.

    Runs immediately, then repeats every ``interval_s`` seconds.
    Safe to call multiple times — only one thread is started.
    Returns the thread (already started).
    """
    global _bg_thread, _bg_stop

    if _bg_thread is not None and _bg_thread.is_alive():
        log.debug("citation_chain_acquirer: background thread already running")
        return _bg_thread

    _bg_stop.clear()

    def _loop():
        log.info("citation_chain_acquirer: background loop started (interval=%ds)", interval_s)
        while not _bg_stop.is_set():
            try:
                result = run_citation_expansion_cycle()
                log.info("citation_chain background cycle: %s", result)
            except Exception as exc:
                log.error("citation_chain background loop error: %s", exc, exc_info=True)
            _bg_stop.wait(interval_s)
        log.info("citation_chain_acquirer: background loop stopped")

    _bg_thread = threading.Thread(target=_loop, name="citation-chain-acquirer", daemon=True)
    _bg_thread.start()
    return _bg_thread


def stop_background() -> None:
    """Signal the background thread to stop cleanly."""
    _bg_stop.set()


# ---------------------------------------------------------------------------
# Status / diagnostics
# ---------------------------------------------------------------------------

def get_status() -> dict:
    """Return a snapshot of citation chain acquisition state."""
    try:
        with _conn() as cn:
            _ensure_schema(cn)
            total = cn.execute(
                "SELECT COUNT(*) FROM citation_chain_state"
            ).fetchone()[0]
            expanded = cn.execute(
                "SELECT COUNT(*) FROM citation_chain_state WHERE fetched_at IS NOT NULL"
            ).fetchone()[0]
            by_depth = cn.execute(
                "SELECT depth, COUNT(*) as n FROM citation_chain_state GROUP BY depth ORDER BY depth"
            ).fetchall()
            ll_count = cn.execute(
                "SELECT COUNT(*) FROM learning_log WHERE kind='citation_chain'"
            ).fetchone()[0]
            edge_count = cn.execute(
                "SELECT COUNT(*) FROM corpus_edge WHERE rel='CITES'"
            ).fetchone()[0]
            seen_count = 0
            raw = cn.execute(
                "SELECT value FROM brain_kv WHERE key=?", (_SEEN_KEY,)
            ).fetchone()
            if raw:
                try:
                    seen_count = len(json.loads(raw[0]))
                except Exception:
                    pass
            return {
                "total_tracked": total,
                "expanded": expanded,
                "pending_expansion": total - expanded,
                "by_depth": {str(r["depth"]): r["n"] for r in by_depth},
                "learning_log_entries": ll_count,
                "cites_edges": edge_count,
                "seen_ids_dedup": seen_count,
                "bg_thread_alive": _bg_thread is not None and _bg_thread.is_alive(),
            }
    except Exception as exc:
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Standalone test entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    print("Running one citation expansion cycle (max_depth=2, max_papers=50)…")
    result = run_citation_expansion_cycle(max_depth=2, max_papers_per_run=50)
    print(json.dumps(result, indent=2))
    print("\nStatus:")
    print(json.dumps(get_status(), indent=2))
