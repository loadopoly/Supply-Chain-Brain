"""DBI OpenRouter redirect engine for Dynamic Brain Insight.

The primary internal DBI path lives in ``brain_dbi.py`` and synthesizes from
the Brain's local neural mapping structures. This module is intentionally the
external redirection fallback used only when that Brain-first path cannot
produce a usable insight.

Redirect pipeline per filter-change:
  1. RETRIEVE  — pull top scored findings from findings_index.db for this site
                 + recent learnings from knowledge_corpus
  2. AUGMENT   — build a structured system + user prompt with the retrieved
                 context, live graph metrics, date window, and page name
    3. GENERATE  — pick the best model with llm_router.select_llm(), then call
                                 OpenRouter directly for a single fast response
  4. FALLBACK  — if the ensemble has no real caller (no OPENROUTER_API_KEY,
                 no compute peers) or returns a placeholder echo, return None
                 so dynamic_insight.py can render the template instead

The result is stored in BrainInsightWorker._insights[key] by the calling
thread; the 2-second @st.fragment re-run picks it up automatically.

Environment variables:
    OPENROUTER_API_KEY   — any registered free-tier OpenRouter API key.
                           Without this, the system runs in template mode.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# Page → ensemble task profile mapping (brain.yaml llms.task_profiles keys).
_PAGE_TASK: dict[str, str] = {
    "Supply Chain Brain":  "cross_dataset_review",
    "Pipeline":            "cross_dataset_review",
    "OTD":                 "otd_root_cause",
    "EOQ":                 "eoq_explain",
    "Procurement":         "cross_dataset_review",
    "Bullwhip":            "forecast_distortion",
    "Freight":             "deck_narration",
    "Sustainability":      "deck_narration",
    "Multi":               "cross_dataset_review",
    "Echelon":             "cross_dataset_review",
    "What":                "cross_dataset_review",
    "Benchmark":           "fast_classify",
    "Decision":            "deck_narration",
    "Cycle":               "fast_classify",
    "Connector":           "fast_classify",
    "Query":               "fast_classify",
    "Schema":              "fast_classify",
}

# ── Caller registration ──────────────────────────────────────────────────────
_caller_registered = False


def _ensure_caller() -> None:
    """Register the OpenRouter HTTP caller once per process."""
    global _caller_registered
    if not _caller_registered:
        try:
            from . import llm_caller_openrouter
            llm_caller_openrouter.register()
        except Exception as exc:
            log.debug("OpenRouter caller registration skipped: %s", exc)
        _caller_registered = True


# ── Retrieval ────────────────────────────────────────────────────────────────

def _retrieve_findings(site: str, limit: int = 6) -> list[dict]:
    """Pull the highest-scored recent findings, boosting site matches."""
    try:
        from .findings_index import DB_PATH
        with sqlite3.connect(DB_PATH) as cn:
            rows = cn.execute(
                """SELECT kind, key, score, payload_json, page, created_at
                   FROM findings
                   ORDER BY COALESCE(score, 0) DESC, id DESC
                   LIMIT ?""",
                (limit * 4,),
            ).fetchall()
    except Exception as exc:
        log.debug("findings_index lookup failed: %s", exc)
        return []

    # Boost rows whose key contains the site string.
    site_lower = (site or "").lower()
    scored: list[tuple[int, float, dict]] = []
    for kind, key, score, payload_json, page, created_at in rows:
        boost = 2 if (site_lower and site_lower not in ("", "all sites")
                      and site_lower in str(key or "").lower()) else 0
        try:
            payload = json.loads(payload_json or "{}")
        except Exception:
            payload = {}
        scored.append((boost, float(score or 0), {
            "kind": kind, "key": key, "score": score,
            "payload": payload, "page": page, "created_at": created_at,
        }))

    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [r[2] for r in scored[:limit]]


def _retrieve_learnings(limit: int = 3) -> list[dict]:
    """Pull the most recent Brain learnings from knowledge_corpus."""
    try:
        from .knowledge_corpus import recent_learnings
        return recent_learnings(limit=limit)
    except Exception as exc:
        log.debug("knowledge_corpus.recent_learnings failed: %s", exc)
        return []


def _retrieve_doc_context(query: str, k: int = 3) -> list[dict]:
    """Retrieve grounded document context from the Brain's doc RAG service.

    Returns a list of ``{breadcrumb, text}`` dicts or an empty list when the
    index is not ready, the API key is absent, or any other error occurs.
    Failures are silent — the caller always gets a valid (possibly empty) list.
    """
    try:
        from .doc_rag import retrieve_doc_context
        return retrieve_doc_context(query, k=k)
    except Exception as exc:
        log.debug("doc_rag.retrieve_doc_context failed: %s", exc)
        return []


# ── Prompt construction ──────────────────────────────────────────────────────

def _build_messages(
    page: str,
    site: str,
    window_str: str,
    metrics: dict[str, Any],
    findings: list[dict],
    learnings: list[dict],
    doc_context: list[dict] | None = None,
) -> list[dict]:
    """Compose the OpenAI-compatible messages list for the ensemble call."""

    system = (
        "You are the Supply Chain Brain, an AI analyst embedded in an enterprise "
        "Streamlit analytics platform. Produce a concise, data-driven insight "
        "(3–5 sentences) for a supply chain analyst. "
        "Rules: cite the exact numbers provided; no hedging phrases like 'I notice' "
        "or 'It appears'; no markdown; plain prose only."
    )

    lines: list[str] = [
        f"Page: {page}",
        f"Site: {site}",
        f"Date window: {window_str}",
    ]

    # Graph topology metrics
    nodes = int(metrics.get("nodes", 0))
    edges = int(metrics.get("edges", 0))
    parts = int(metrics.get("parts", 0))
    po    = int(metrics.get("po", 0))
    so    = int(metrics.get("so", 0))

    if nodes > 0:
        density = round(edges / nodes, 2) if nodes else 0
        lines.append(
            f"Graph: {nodes:,} nodes · {edges:,} edges · avg degree {density} "
            f"(loaded from {parts:,} parts, {po:,} PO receipts, {so:,} SO lines)"
        )
        if metrics.get("cap_hit"):
            lines.append(
                "ALERT: one or more record-caps were reached — "
                "graph may under-represent the full dataset."
            )

    # Retrieved findings
    if findings:
        lines.append("\nTop findings from the findings index:")
        for f in findings:
            score_str = (
                f" score={f['score']:.3f}" if isinstance(f.get("score"), float) else ""
            )
            payload_str = str(f.get("payload") or "")[:200]
            lines.append(
                f"  • [{f.get('kind','?')}] key={f.get('key','?')}{score_str}: {payload_str}"
            )

    # Recent Brain learnings
    if learnings:
        lines.append("\nRecent Brain learnings:")
        for lrn in learnings:
            title  = lrn.get("title", "")
            detail = lrn.get("detail", "")
            if isinstance(detail, dict):
                detail = json.dumps(detail)
            lines.append(f"  • {title}: {str(detail)[:150]}")

    # Document context from Brain's doc RAG
    if doc_context:
        lines.append("\nRelevant document sections:")
        for dc in doc_context:
            breadcrumb = dc.get("breadcrumb", "")
            text = str(dc.get("text", ""))[:300]
            lines.append(f"  [{breadcrumb}] {text}")

    lines.append(
        f"\nTask: write a 3–5 sentence supply chain insight for the analyst "
        f"on the '{page}' dashboard page."
    )

    return [
        {"role": "system", "content": system},
        {"role": "user",   "content": "\n".join(lines)},
    ]


# ── Result extraction ────────────────────────────────────────────────────────

def _extract_text(result: Any) -> str | None:
    """Pull the winning text string from an EnsembleResult."""
    if result is None:
        return None
    answer = getattr(result, "answer", result)
    if answer is None:
        return None

    # weighted_softmax_vote → {"value": {...}, "ensemble_confidence": float}
    if isinstance(answer, dict):
        val = answer.get("value")
        if isinstance(val, dict):
            val = val.get("text") or val.get("value")
        if isinstance(val, str):
            return val.strip() or None
        text_direct = answer.get("text", "")
        return str(text_direct).strip() or None

    if isinstance(answer, str):
        return answer.strip() or None

    return None


def _is_placeholder(text: str) -> bool:
    """True when the ensemble returned an offline echo rather than real LLM output."""
    t = text.strip()
    return (
        not t
        or len(t) < 40
        or t.startswith("[")
        or "offline" in t.lower()
        or "no openrouter" in t.lower()
    )


# ── Public entry point ───────────────────────────────────────────────────────

def generate_insight(page: str, context_dict: dict) -> str | None:
    """Generate an LLM insight for `page` using the current context_dict.

    Returns the insight text string, or None if:
      - No real LLM caller is available (no OPENROUTER_API_KEY)
      - The ensemble timed out or all models failed
      - The returned text was an offline placeholder

    Callers should fall back to the template engine when None is returned.
    """
    _ensure_caller()

    # ── Extract filter context ───────────────────────────────────────────────
    site = (
        context_dict.get("g_site")
        or context_dict.get("selected_site")
        or "all sites"
    )
    date_start = str(context_dict.get("g_date_start") or "").split("T")[0]
    date_end   = str(context_dict.get("g_date_end")   or "").split("T")[0]
    window_str = (
        f"{date_start} → {date_end}" if (date_start and date_end) else "full horizon"
    )

    nodes = int(context_dict.get("dbi_graph_nodes",  0))
    edges = int(context_dict.get("dbi_graph_edges",  0))
    parts = int(context_dict.get("dbi_actual_parts", 0))
    po    = int(context_dict.get("dbi_actual_po",    0))
    so    = int(context_dict.get("dbi_actual_so",    0))
    lim_p = int(context_dict.get("g_np",  200))
    lim_r = int(context_dict.get("g_nr",  750))
    lim_s = int(context_dict.get("g_nso", 750))
    cap_hit = (parts >= lim_p) or (po >= lim_r) or (so >= lim_s)

    metrics: dict[str, Any] = {
        "nodes": nodes, "edges": edges,
        "parts": parts, "po": po, "so": so,
        "cap_hit": cap_hit,
    }

    # ── Retrieve ─────────────────────────────────────────────────────────────
    findings    = _retrieve_findings(site, limit=6)
    learnings   = _retrieve_learnings(limit=3)
    doc_query   = f"{page} supply chain {site}"
    doc_context = _retrieve_doc_context(doc_query, k=3)

    # ── Augment ──────────────────────────────────────────────────────────────
    messages = _build_messages(
        page, site, window_str, metrics, findings, learnings, doc_context
    )

    # ── Generate ─────────────────────────────────────────────────────────────
    task = next(
        (v for k, v in _PAGE_TASK.items() if k in page),
        "insight_narrate",   # falls back to 'default' in llm_router
    )

    try:
        from .llm_router import select_llm
        from .llm_caller_openrouter import openrouter_caller

        decision = select_llm(task)
        response = openrouter_caller(decision, {"messages": messages, "kind": "text"}, {})
        text = None
        if isinstance(response, dict):
            text = str(response.get("text") or "").strip() or None
        elif isinstance(response, str):
            text = response.strip() or None

        if text and not _is_placeholder(text):
            log.info(
                "DBI RAG generated %d chars for page=%s site=%s model=%s",
                len(text), page, site, decision.model_id,
            )
            return text
        log.debug("DBI RAG returned placeholder/empty for page=%s — using template", page)
    except Exception as exc:
        log.warning("DBI routed OpenRouter call failed for page=%s: %s", page, exc)

    return None
