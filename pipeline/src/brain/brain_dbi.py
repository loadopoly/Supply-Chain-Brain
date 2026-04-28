"""Brain-first DBI synthesis.

This module keeps Dynamic Brain Insight grounded in the local Supply Chain Brain
before any external LLM redirection is attempted.  It reads the Brain's own
neural mapping structures: body directives, the touch pressure field, corpus
learnings/graph counts, and neural-plasticity dials.
"""
from __future__ import annotations

from typing import Any


def _safe_text(value: Any, default: str = "") -> str:
    text = str(value if value is not None else default).strip()
    return text or default


def _site_label(context: dict) -> str:
    site = _safe_text(context.get("g_site") or context.get("selected_site"), "all sites")
    if site.upper() in {"ALL", "ALL SITES"}:
        return "all sites"
    return site


def _window_label(context: dict) -> str:
    start = _safe_text(context.get("g_date_start") or context.get("g_date_start_widget"))
    end = _safe_text(context.get("g_date_end") or context.get("g_date_end_widget"))
    start = start.split("T")[0]
    end = end.split("T")[0]
    return f"{start} to {end}" if start and end else "full horizon"


def _top_pressure(touch_field: dict[str, Any]) -> tuple[str, float] | None:
    scored: list[tuple[str, float]] = []
    for key, value in (touch_field or {}).items():
        try:
            scored.append((str(key), float(value)))
        except (TypeError, ValueError):
            continue
    if not scored:
        return None
    scored.sort(key=lambda item: item[1], reverse=True)
    return scored[0]


def _top_directive(directives: list[dict], page: str, site: str) -> dict | None:
    if not directives:
        return None
    page_l = page.lower()
    site_l = site.lower()

    def score(row: dict) -> tuple[float, float]:
        text = " ".join(
            str(row.get(field, ""))
            for field in ("title", "do_this", "why_it_matters", "signal_kind", "target_entity")
        ).lower()
        boost = 0.0
        if site_l not in {"", "all sites"} and site_l in text:
            boost += 0.25
        for token in page_l.replace("_", " ").split():
            if len(token) >= 3 and token in text:
                boost += 0.08
        try:
            priority = float(row.get("priority") or 0.0)
        except (TypeError, ValueError):
            priority = 0.0
        return (priority + boost, priority)

    return sorted(directives, key=score, reverse=True)[0]


def _latest_learning(learnings: list[dict]) -> dict | None:
    return learnings[0] if learnings else None


def _corpus_counts(corpus_summary: dict) -> tuple[int, int]:
    entities = corpus_summary.get("entities_by_type", {}) if isinstance(corpus_summary, dict) else {}
    if isinstance(entities, dict):
        entity_count = sum(int(v or 0) for v in entities.values())
    else:
        entity_count = 0
    edges = corpus_summary.get("edges_by_rel", {}) if isinstance(corpus_summary, dict) else {}
    try:
        edge_count = sum(int(v or 0) for v in edges.values()) if isinstance(edges, dict) else 0
    except (TypeError, ValueError):
        edge_count = 0
    return entity_count, edge_count


def _graph_sentence(context: dict) -> str | None:
    try:
        nodes = int(context.get("dbi_graph_nodes") or 0)
        edges = int(context.get("dbi_graph_edges") or 0)
        parts = int(context.get("dbi_actual_parts") or 0)
        po = int(context.get("dbi_actual_po") or 0)
        so = int(context.get("dbi_actual_so") or 0)
    except (TypeError, ValueError):
        return None
    if nodes <= 0:
        return None
    density = round(edges / nodes, 2) if nodes else 0
    return (
        f"The live neural map has {nodes:,} nodes and {edges:,} edges "
        f"from {parts:,} parts, {po:,} PO receipts, and {so:,} SO lines "
        f"(average degree {density})."
    )


def _dial_sentence(dials: dict) -> str | None:
    if not isinstance(dials, dict) or not dials:
        return None
    brain = dials.get("brain") or {}
    vision = dials.get("vision") or {}
    if not brain and not vision:
        return None
    parts: list[str] = []
    try:
        parts.append(f"brain centrality top {float(brain.get('graph_centrality_top')):.0f}")
    except (TypeError, ValueError):
        pass
    try:
        parts.append(f"vision pressure threshold {float(vision.get('pressure_threshold')):.2f}")
    except (TypeError, ValueError):
        pass
    if not parts:
        return None
    return "Neural plasticity dials are active: " + ", ".join(parts) + "."


def generate_brain_insight(page: str, context_dict: dict) -> str | None:
    """Return a local Brain-derived DBI insight, or None when Brain state is unavailable."""
    page_name = _safe_text(page, "Current page")
    site = _site_label(context_dict)
    window = _window_label(context_dict)

    directives: list[dict] = []
    touch_field: dict[str, Any] = {}
    learnings: list[dict] = []
    corpus: dict = {}
    dials: dict = {}

    try:
        from .brain_body_signals import get_touch_field, list_open_directives
        directives = list_open_directives(limit=8, min_priority=0.2)
        touch_field = get_touch_field()
    except Exception:
        directives = []
        touch_field = {}

    try:
        from .knowledge_corpus import corpus_summary, recent_learnings
        corpus = corpus_summary()
        learnings = recent_learnings(limit=4)
    except Exception:
        corpus = {}
        learnings = []

    try:
        from .neural_plasticity import get_all_dials
        dials = get_all_dials()
    except Exception:
        dials = {}

    graph_line = _graph_sentence(context_dict)
    pressure = _top_pressure(touch_field)
    directive = _top_directive(directives, page_name, site)
    learning = _latest_learning(learnings)
    entity_count, edge_count = _corpus_counts(corpus)

    has_internal_signal = any([
        graph_line,
        pressure,
        directive,
        learning,
        entity_count > 0,
        edge_count > 0,
    ])
    if not has_internal_signal:
        return None

    lines: list[str] = [
        f"Brain neural map for {page_name} at {site} over {window}: local DBI is primary."
    ]
    if graph_line:
        lines.append(graph_line)
    if pressure:
        kind, value = pressure
        pretty_kind = kind.replace("_", " ")
        lines.append(
            f"Touch pressure is highest on {pretty_kind} ({value:.2f}), so route attention there before asking an outside model."
        )
    if directive:
        title = _safe_text(directive.get("title"), "Open Brain directive")
        action = _safe_text(directive.get("do_this"), "Review and assign the directive.")
        lines.append(f"Top Brain directive: {title}. Next move: {action}")
    elif learning:
        title = _safe_text(learning.get("title"), "recent Brain learning")
        lines.append(f"Most recent Brain learning feeding this readout: {title}.")
    if entity_count > 0 or edge_count > 0:
        lines.append(
            f"The internal corpus currently maps {entity_count:,} entities and {edge_count:,} weighted relationships."
        )
    dial_line = _dial_sentence(dials)
    if dial_line:
        lines.append(dial_line)

    return " ".join(lines[:5])
