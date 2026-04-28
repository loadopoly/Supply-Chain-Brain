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


def _float_value(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_value(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _page_playbook(page_name: str, context: dict) -> str | None:
    """Return a plain-language, page-specific operator readout."""
    page = page_name.lower()
    site = _site_label(context)
    window = _window_label(context)
    scope = f"Scope: {site}, {window}."

    if "bullwhip" in page:
        max_ratio = _float_value(context.get("dbi_bullwhip_max_ratio"))
        avg_ratio = _float_value(context.get("dbi_bullwhip_avg_ratio"))
        worst_echelon = _safe_text(context.get("dbi_bullwhip_worst_echelon"), "the worst echelon")
        over_two = _int_value(context.get("dbi_bullwhip_echelons_over_2"))
        if max_ratio is None:
            return (
                "Bullwhip check: waiting on the live ratio table. Do this next: open Echelon Rankings, "
                "sort worst first, and fix the first row over 2x. Ask one plain question: are we batching, "
                f"expediting, or padding orders? {scope}"
            )
        if max_ratio >= 2.0:
            level = "Big swing"
            count = f" {over_two} echelon(s) are over 2x." if over_two is not None else ""
        elif max_ratio >= 1.5:
            level = "Watch this"
            count = " No panic yet, but the signal is getting noisy."
        else:
            level = "Mostly calm"
            count = " Orders are not outrunning demand badly right now."
        avg_text = f" Avg is {avg_ratio:.1f}x." if avg_ratio is not None else ""
        return (
            f"{level}: {worst_echelon} is ordering {max_ratio:.1f}x harder than customer demand.{avg_text}{count} "
            "Do this next: work that echelon first. Stop changing safety stock until you know whether the cause is batching, "
            f"expedites, or forecast padding. {scope}"
        )

    if "otd" in page:
        return (
            "Late orders are the job. Do this next: open the late-line worklist, pick the oldest customer promise, "
            f"and assign one owner before looking at more charts. {scope}"
        )

    if "data quality" in page:
        pct_missing = _float_value(context.get("dbi_pct_missing"))
        if pct_missing is None:
            return (
                "Data check: look for blank fields before trusting the model. Do this next: open Value of Information "
                f"and fix the highest-impact missing column first. {scope}"
            )
        if pct_missing >= 50:
            level = "Data is too broken"
        elif pct_missing >= 20:
            level = "Data has holes"
        else:
            level = "Data is usable"
        return (
            f"{level}: {pct_missing:.1f}% of cells are missing. Do this next: fix the highest-impact blank field first, "
            f"then rerun the page. {scope}"
        )

    if "supply chain brain" in page:
        nodes = _int_value(context.get("dbi_graph_nodes"))
        edges = _int_value(context.get("dbi_graph_edges"))
        if nodes:
            return (
                f"Network map is loaded: {nodes:,} points and {edges or 0:,} links. Do this next: click the biggest connected node, "
                f"then follow it to the owner, supplier, or part creating the pileup. {scope}"
            )
        return (
            "Network map is not loaded yet. Do this next: pick one plant and rebuild the graph before making a decision. "
            f"{scope}"
        )

    if "eoq" in page:
        return (
            "Inventory math check: find the part where reorder quantity is farthest from real demand. Do this next: "
            f"open the worst deviation row and decide buy less, buy more, or change the lead-time assumption. {scope}"
        )

    if "procurement" in page or "supplier" in page or "cvar" in page:
        return (
            "Supplier risk check: one supplier or lane is carrying too much pain. Do this next: open the highest-risk row, "
            f"name the buyer, and choose expedite, split source, or consolidate. {scope}"
        )

    if "lead" in page and "survival" in page:
        return (
            "Lead-time check: find the lane most likely to slip. Do this next: open the worst supplier-part lane and call it "
            f"before it becomes late. {scope}"
        )

    if "multi" in page or "echelon" in page:
        return (
            "Inventory balance check: stock is not sitting in the right layer. Do this next: fix the echelon with the biggest "
            f"short/over gap before moving inventory elsewhere. {scope}"
        )

    if "freight" in page:
        return (
            "Freight check: look for lanes spending money without moving enough value. Do this next: open the worst lane and "
            f"choose consolidate, reroute, or stop. {scope}"
        )

    if "sustainability" in page:
        return (
            "Emissions check: find the lane using the dirtiest or most expensive mode. Do this next: review air freight first, "
            f"then move what can safely ride slower. {scope}"
        )

    if "what" in page and "if" in page:
        return (
            "Scenario check: compare service gain against cost pain. Do this next: keep the option that improves the KPI without "
            f"creating a bigger miss somewhere else. {scope}"
        )

    if "decision" in page:
        return (
            "Decision check: open items need an owner. Do this next: close or assign the oldest unowned decision before adding a new one. "
            f"{scope}"
        )

    if "query" in page:
        return (
            "Query check: ask one concrete thing. Do this next: search by part, order, supplier, customer, or invoice, then inspect the first rows. "
            f"{scope}"
        )

    if "schema" in page:
        return (
            "Schema check: find the table and column before writing SQL. Do this next: search the business word, confirm the data type, "
            f"then run a tiny sample. {scope}"
        )

    if "connector" in page:
        return (
            "Connector check: make sure the data pipe is alive before trusting charts. Do this next: test the active source and fix red status first. "
            f"{scope}"
        )

    if "report" in page:
        return (
            "Report check: do not build a deck from stale scope. Do this next: confirm plant and date window, then create the one-pager. "
            f"{scope}"
        )

    if "benchmark" in page:
        return (
            "Benchmark check: look for slow or stale runs. Do this next: compare the latest row to the previous baseline and flag only real regressions. "
            f"{scope}"
        )

    if "cycle count" in page:
        return (
            "Count check: system quantity and floor quantity must match. Do this next: open the biggest variance bin and assign one inventory owner. "
            f"{scope}"
        )

    return None


def generate_brain_insight(page: str, context_dict: dict) -> str | None:
    """Return a local Brain-derived DBI insight, or None when Brain state is unavailable."""
    page_name = _safe_text(page, "Current page")
    site = _site_label(context_dict)

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

    playbook = _page_playbook(page_name, context_dict)
    if playbook:
        return playbook

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
        f"{page_name}: start with the first red or amber row. Pick one owner, one part, and one next action."
    ]
    if graph_line:
        lines.append(graph_line)
    if directive:
        action = _safe_text(directive.get("do_this"), "Review and assign the top Brain directive.")
        lines.append(f"Brain says next: {action}")
    elif learning:
        title = _safe_text(learning.get("title"), "recent Brain learning")
        lines.append(f"Recent Brain learning: {title}.")
    if pressure:
        kind, value = pressure
        lines.append(f"Internal pressure flag: {kind.replace('_', ' ')} at {value:.2f}.")

    return " ".join(lines[:4])
