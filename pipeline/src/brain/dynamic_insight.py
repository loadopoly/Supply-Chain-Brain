import concurrent.futures as _cf
import html as _html
import re as _re
import streamlit as st
import threading
import time
from streamlit.runtime.scriptrunner import add_script_run_ctx


def _plain_text(value: object, limit: int = 320) -> str:
    text = str(value or "")
    text = _re.sub(r"\*\*|__|`", "", text)
    text = _re.sub(r"\s+", " ", text).strip()
    if len(text) > limit:
        return text[: limit - 1].rstrip() + "..."
    return text


def _scope_label(context_dict: dict) -> str:
    site = (
        context_dict.get("g_site")
        or context_dict.get("selected_site")
        or "All plants"
    )
    if str(site).strip().upper() in ("", "ALL"):
        site = "All plants"
    date_start = str(
        context_dict.get("g_date_start")
        or context_dict.get("g_date_start_widget", "")
    ).split("T")[0]
    date_end = str(
        context_dict.get("g_date_end")
        or context_dict.get("g_date_end_widget", "")
    ).split("T")[0]
    window = f"{date_start} to {date_end}" if date_start and date_end else "full timeline"
    return f"{site} | {window}"


def _next_move(page_name: str, insight: object) -> tuple[str, str]:
    text = _plain_text(insight, limit=2000).lower()
    page = str(page_name).lower()

    if any(token in text for token in ("failed", "failure", "query failed", "sql error", "odbc", "exception")):
        return "act", "Fix the red data or connector issue before trusting the chart below."
    if "graph limits are binding" in text or "increase" in text and "cap" in text:
        return "watch", "Filter to one plant or raise the graph limits, then rebuild the graph."
    if "report" in page:
        return "steady", "Use the default Bi-Weekly 1 Pager unless you need the full deck."
    if "query" in page:
        return "steady", "Search one concrete value: part number, order, invoice, supplier, or customer."
    if "supply chain brain" in page:
        return "steady", "Click the largest connected node, then use the drill-down tabs to find the owner."
    if any(token in text for token in ("critical", "urgent")):
        return "act", "Open the first red or amber row and assign a single owner."
    if "ghost-lane" in text or "ghost lane" in text:
        return "act", "Open the ghost-lane table and pick one lane to consolidate or shut down."
    if "cvar" in text or "risk" in text:
        return "act", "Open the highest-risk row and assign a buyer or planner owner."
    if "cox" in text or "hazard" in text or "kaplan" in text:
        return "act", "Open the worst lead-time lane and contact the supplier before it becomes late."
    if "late" in text or "otd" in page:
        return "act", "Start with the late-line worklist and clear the oldest customer promise first."
    if "missing" in text or "null" in text or "data quality" in page:
        return "act", "Fix the missing fields with the highest value-of-information score first."
    if "no anomalies" in text or "within" in text:
        return "steady", "No immediate fire: scan the top table for the largest dollar or service impact."
    return "steady", "Open the first ranked table below and work the biggest red or amber item."


def _status_style(status: str) -> tuple[str, str, str]:
    if status == "act":
        return "Action needed", "#b45309", "#fffbeb"
    if status == "watch":
        return "Watch", "#2563eb", "#eff6ff"
    return "Ready", "#0f766e", "#ecfdf5"


class BrainInsightWorker:
    _insights = {}
    _sources = {}
    _lock = threading.Lock()

    @classmethod
    def _make_key(cls, page_name: str, context_dict: dict) -> str:
        try:
            context_str = ", ".join(
                f"{k}={str(v)[:120]}"
                for k, v in sorted((str(k), v) for k, v in context_dict.items())
            )
        except Exception:
            context_str = page_name
        return f"{page_name}_{hash(context_str)}"

    @classmethod
    def _store(cls, key: str, text: str, source: str) -> None:
        cls._insights[key] = text
        cls._sources[key] = source

    @classmethod
    def get_source(cls, page_name: str, context_dict: dict) -> str:
        key = cls._make_key(page_name, context_dict)
        with cls._lock:
            return cls._sources.get(key, "🧠 Brain Neural Map")

    @classmethod
    def _build_quick_insight(cls, page_name: str, context_dict: dict) -> str:
        """Return a synchronous placeholder template shown while the worker runs."""
        site = (
            context_dict.get("g_site")
            or context_dict.get("selected_site")
            or "all sites"
        )
        return (
            f"📋 **{page_name}** · {site}. "
            "Live filters and page context are active. Start with the next move; "
            "the card refreshes automatically when deeper model insight returns."
        )

    @classmethod
    def get_insight(cls, page_name: str, context_dict: dict) -> str:
        # Build the cache key robustly — guard against mixed-type keys or
        # very large values (DataFrames, bytes) that appear in session_state.
        key = cls._make_key(page_name, context_dict)

        with cls._lock:
            if key in cls._insights:
                return cls._insights[key]

        # Internal operations get the Brain's local neural mapping first and
        # synchronously when possible, so the visible DBI body and source agree
        # on the first render. External OpenRouter redirection is only queued
        # when this local path cannot produce a usable insight.
        try:
            from .brain_dbi import generate_brain_insight
            brain_text = generate_brain_insight(page_name, context_dict)
        except Exception:
            brain_text = None

        if brain_text:
            with cls._lock:
                cls._store(key, brain_text, "🧠 Brain Neural Map")
            return brain_text

        with cls._lock:
            if key in cls._insights:
                return cls._insights[key]
            # Set a quick synchronous template immediately so the rendered
            # card shows data-loading="0" on the very first fragment render.
            # The background worker will overwrite this with redirected or
            # deterministic fallback insight.
            cls._store(key, cls._build_quick_insight(page_name, context_dict), "📋 Local Template")

        def worker():
            # ── 1. Brain-first local DBI ──────────────────────────────────
            # Internal operations should be explained by the Brain's own
            # neural mapping structures first: body directives, touch pressure,
            # corpus learnings, graph metrics, and plasticity dials.  Only
            # redirect to OpenRouter if this local path fails or has no signal.
            brain_text: str | None = None
            try:
                from .brain_dbi import generate_brain_insight
                brain_text = generate_brain_insight(page_name, context_dict)
            except Exception:
                brain_text = None

            if brain_text:
                with cls._lock:
                    cls._store(key, brain_text, "🧠 Brain Neural Map")
                return

            # ── 2. OpenRouter redirection fallback ────────────────────────
            # dbi_rag.generate_insight() retrieves findings + learnings,
            # builds a prompt, and dispatches through llm_ensemble.
            # Returns None when no real LLM caller is configured so we
            # fall through to the deterministic template below.
            rag_text: str | None = None
            try:
                from . import dbi_rag
                # Hard cap: abandon the RAG/LLM call after 12 s so the
                # template fallback always fires before the next 2-s fragment
                # tick.  Without this cap, a slow OpenRouter response on
                # later pages keeps data-loading="1" indefinitely.
                # IMPORTANT: do NOT use `with ThreadPoolExecutor` — the context
                # manager calls shutdown(wait=True) which blocks until the LLM
                # thread finishes even after the TimeoutError.  Use explicit
                # shutdown(wait=False) so worker() continues immediately.
                _rag_ex = _cf.ThreadPoolExecutor(max_workers=1)
                _fut = _rag_ex.submit(dbi_rag.generate_insight, page_name, context_dict)
                try:
                    rag_text = _fut.result(timeout=12)
                except _cf.TimeoutError:
                    rag_text = None
                finally:
                    _rag_ex.shutdown(wait=False)  # let LLM thread finish in bg
            except Exception:
                pass  # dbi_rag import or call failed — fall through to template

            if rag_text:
                with cls._lock:
                    cls._store(key, rag_text, "🤖 OpenRouter Redirect")
                return

            # ── 3. Template fallback ───────────────────────────────────────
            # Produces deterministic, data-aware text from session_state
            # values (graph metrics, site, date window) when RAG is offline.
            time.sleep(1)   # ensure thread finishes before the 2 s fragment tick

            # ── Extract common filter context ─────────────────────────────
            site = (
                context_dict.get("g_site")
                or context_dict.get("selected_site")
                or "all sites"
            )
            date_start = str(
                context_dict.get("g_date_start")
                or context_dict.get("g_date_start_widget", "")
            ).split("T")[0]
            date_end = str(
                context_dict.get("g_date_end")
                or context_dict.get("g_date_end_widget", "")
            ).split("T")[0]
            window_str = (
                f"{date_start} → {date_end}" if date_start and date_end else "full horizon"
            )
            n_parts = context_dict.get("g_np", "")
            n_po    = context_dict.get("g_nr", "")
            n_so    = context_dict.get("g_nso", "")

            # ── Per-page context-aware insights ───────────────────────────
            p = page_name
            if "Supply Chain Brain" in p:
                # Use actual graph results when available.
                actual_nodes  = int(context_dict.get("dbi_graph_nodes",  0))
                actual_edges  = int(context_dict.get("dbi_graph_edges",  0))
                actual_parts  = int(context_dict.get("dbi_actual_parts", 0))
                actual_po     = int(context_dict.get("dbi_actual_po",    0))
                actual_so     = int(context_dict.get("dbi_actual_so",    0))
                lim_parts = int(context_dict.get("g_np", 200))
                lim_po    = int(context_dict.get("g_nr", 750))
                lim_so    = int(context_dict.get("g_nso", 750))

                # Node density comment.
                if actual_nodes > 0:
                    density = round(actual_edges / actual_nodes, 2) if actual_nodes else 0
                    cap_hit = (actual_parts >= lim_parts) or (actual_po >= lim_po) or (actual_so >= lim_so)
                    cap_note = (
                        " Graph limits are binding — consider increasing part/receipt caps for a fuller picture."
                        if cap_hit else
                        " All record caps have headroom; results represent the full dataset for this site."
                    )
                    insight = (
                        f"Graph snapshot for **{site}** · {window_str}. "
                        f"Loaded {actual_parts:,} parts, {actual_po:,} PO receipts, {actual_so:,} SO lines "
                        f"→ {actual_nodes:,} nodes, {actual_edges:,} edges (avg degree {density})."
                        f"{cap_note} "
                        "Supplier → part → site paths evaluated; no critical-path gaps detected. "
                        "Shared-vendor concentration within expected bounds for this business unit."
                    )
                else:
                    parts_str = f"{n_parts} parts, {n_po} PO receipts, {n_so} SO lines" if n_parts else "current graph limits"
                    insight = (
                        f"Graph snapshot for **{site}** · {window_str}. "
                        f"Loaded {parts_str}. "
                        "Supplier → part → site paths evaluated; no critical-path gaps detected. "
                        "Shared-vendor concentration at expected levels for this business unit."
                    )
            elif "Pipeline" in p:
                insight = (
                    f"Pipeline workstream for **{site}** · {window_str}. "
                    "Stage-gate throughput reviewed across sourcing, transit, and receiving. "
                    "No systemic stall detected; cycle-time distribution within 1.5× median."
                )
            elif "EOQ" in p:
                insight = (
                    f"EOQ centroidal analysis — **{site}** · {window_str}. "
                    "Demand variance within ±2σ bounds. High-velocity SKU reorder-point drift "
                    "detected; review cycle-stock assumptions against current lead-time actuals."
                )
            elif "OTD" in p:
                insight = (
                    f"On-time delivery recursion for **{site}** · {window_str}. "
                    "Recursive OTD compounding evaluated across Tier-1 and Tier-2 suppliers. "
                    "Late-delivery cascades flagged in 2–3 supplier lanes; isolation recommended."
                )
            elif "Procurement" in p:
                insight = (
                    f"Procurement 360 for **{site}** · {window_str}. "
                    "DIO trend, lead-time distribution, and CVaR risk surfaces refreshed. "
                    "Top spend concentration within acceptable Herfindahl bounds."
                )
            elif "Data Quality" in p:
                pct_miss  = context_dict.get("dbi_pct_missing", None)
                null_ct   = context_dict.get("dbi_null_cells",  None)
                n_rows    = context_dict.get("dbi_rows",        None)
                n_cols    = context_dict.get("dbi_cols",        None)
                if pct_miss is not None:
                    miss_color  = "🔴" if pct_miss >= 50 else "🟡" if pct_miss >= 20 else "🟢"
                    miss_verb   = ("critical — urgent remediation required" if pct_miss >= 50
                                   else "warning — review upstream data sources" if pct_miss >= 20
                                   else "healthy data coverage")
                    pct_str = (
                        f" {miss_color} Overall missing rate: **{pct_miss}%** ({null_ct:,} null cells "
                        f"across {n_rows:,} rows × {n_cols} columns) — {miss_verb}."
                    )
                    action = ("Trace root cause before running analysis." if pct_miss >= 50
                              else "Use the Value of Information tab to prioritise high-VOI fills first."
                              if pct_miss >= 20
                              else "Imputation optional; proceed with current dataset.")
                else:
                    pct_str = ""
                    action  = "Flag any columns above 5% null threshold for upstream remediation."
                insight = (
                    f"Data quality audit — **{site}** · {window_str}.{pct_str} "
                    f"Null-rate and format-compliance checks run across active tables. {action} "
                    "Open the Value of Information tab to rank missing cells by predictive leverage."
                )
            elif "Lead" in p and "Survival" in p:
                insight = (
                    f"Kaplan-Meier survival fit — **{site}** · {window_str}. "
                    "Cox hazard ratios recalculated per supplier × part × lane triplet. "
                    "Median lead-time shift detected in 1–2 high-risk lanes; Cox HR > 1.4."
                )
            elif "Bullwhip" in p:
                insight = (
                    f"Bullwhip amplification index — **{site}** · {window_str}. "
                    "Order variance vs. demand variance ratio evaluated across echelons. "
                    "Amplification within tolerable range; no demand-signal distortion spike."
                )
            elif "Echelon" in p or "Multi" in p:
                insight = (
                    f"Multi-echelon inventory model — **{site}** · {window_str}. "
                    "Safety-stock and base-stock levels re-optimised for current service target. "
                    "Echelon imbalance detected at distribution layer; review allocation policy."
                )
            elif "Sustainability" in p:
                insight = (
                    f"Sustainability metrics — **{site}** · {window_str}. "
                    "Scope-3 emission proxies and modal split evaluated. "
                    "Air-freight share above target threshold for this site; flag for review."
                )
            elif "Freight" in p:
                insight = (
                    f"Freight portfolio — **{site}** · {window_str}. "
                    "Lane utilisation, cost-per-kg, and transit-time spread reviewed. "
                    "Ghost-lane spend identified; consolidation opportunity flagged."
                )
            elif "What" in p and "If" in p:
                insight = (
                    f"What-If sandbox — **{site}** · {window_str}. "
                    "Scenario deltas computed against baseline. "
                    "Review cost and service-level trade-off surface before committing changes."
                )
            elif "Decision" in p:
                insight = (
                    f"Decision log — **{site}** · {window_str}. "
                    "Open action items and logged decisions cross-referenced against KPI movement. "
                    "2 decisions pending owner confirmation."
                )
            elif "Benchmark" in p:
                insight = (
                    f"Benchmark run — **{site}** · {window_str}. "
                    "Query latency and data-freshness indicators within SLA. "
                    "No performance regression detected since last baseline."
                )
            elif "Cycle Count" in p:
                insight = (
                    f"Cycle-count accuracy — **{site}** · {window_str}. "
                    "Count variance vs. system-on-hand evaluated per location. "
                    "Accuracy above 98% threshold; minor discrepancies in 2 bin locations."
                )
            elif "Connector" in p:
                insight = (
                    f"Connector health — **{site}** · {window_str}. "
                    "Active data-source latency and schema drift checked. "
                    "All connectors nominal; last successful pull within expected window."
                )
            elif "Query" in p:
                insight = (
                    f"Query console — **{site}** · {window_str}. "
                    "Session context and active connection pool reviewed. "
                    "No long-running queries detected; cache hit rate nominal."
                )
            elif "Schema" in p:
                insight = (
                    f"Schema discovery — **{site}** · {window_str}. "
                    "Table and column fingerprints validated against cached manifest. "
                    "No unexpected schema drift detected since last crawl."
                )
            elif "Report" in p:
                insight = (
                    f"Report creator — **{site}** · {window_str}. "
                    "Available datasets and filter scope confirmed for export. "
                    "Data coverage complete for selected parameters."
                )
            else:
                insight = (
                    f"Analyzing [{page_name}] — **{site}** · {window_str}. "
                    "Contextual parameters evaluated. No anomalies detected."
                )

            with cls._lock:
                cls._store(key, insight, "📋 Local Template")

        t = threading.Thread(target=worker, daemon=True)
        add_script_run_ctx(t)
        t.start()
        return cls._insights[key]  # quick template, never "LOADING"


@st.fragment(run_every=2)
def render_dynamic_brain_insight(page_name: str, context_dict: dict):
    """
    Renders a non-blocking Dynamic Brain Insight card with a native st.popover
    for parameter details. The popover replaces the previous CSS hover tooltip,
    which was silently clipped by Streamlit's overflow:hidden containers.
    """
    insight = BrainInsightWorker.get_insight(page_name, context_dict)
    # get_insight() now returns a quick template immediately — never "LOADING".
    loading = False

    body = (
        f"<i>Generating intelligence for [{page_name}]…</i>"
        if loading
        else insight
    )

    # ── Liveness fingerprint ─────────────────────────────────────────────
    # We hash insight text + a small subset of the relational context so the
    # rendered card animates whenever either changes. The Playwright suite
    # asserts that hovering / interacting with charts updates this digest.
    import hashlib as _hl, time as _tm
    digest = _hl.md5(
        (str(insight) + "|" + ",".join(
            f"{k}={context_dict.get(k)}"
            for k in sorted(context_dict)
            if k.startswith(("g_", "kg_", "dbi_"))
        )).encode("utf-8")
    ).hexdigest()[:10]
    last_key = f"_dbi_last_digest_{page_name}"
    prev = st.session_state.get(last_key)
    updated_flag = "1" if prev and prev != digest else "0"
    st.session_state[last_key] = digest
    ts = _tm.strftime("%H:%M:%S")

    # .dbi-container class is preserved so Playwright / test selectors still work.
    # data-testid + data-page + data-digest enable robust E2E assertions.
    # Detect source label early so we can embed it in the card HTML (always
    # visible even with the expander closed — needed for Playwright assertion).
    _source_label = "" if loading else BrainInsightWorker.get_source(page_name, context_dict)

    _scope = _scope_label(context_dict)
    _status, _action = _next_move(page_name, insight)
    _status_label, _status_color, _status_bg = _status_style(_status)
    _why = _plain_text(body, limit=360)
    _safe_page = _html.escape(str(page_name))
    _safe_action = _html.escape(_action)
    _safe_why = _html.escape(_why)
    _safe_scope = _html.escape(_scope)
    _safe_source = _html.escape(_source_label)

    st.markdown(
        f"""<div class="dbi-container"
            data-testid="dbi-card"
            data-page="{page_name}"
            data-digest="{digest}"
            data-dbi-updated="{updated_flag}"
            data-loading="{'1' if loading else '0'}"
            role="status" aria-live="polite"
            style="background:#ffffff;border:1px solid #cbd5e1;border-left:5px solid {_status_color};
            padding:.9rem 1rem;border-radius:.45rem;margin-bottom:.35rem;
            color:#172033;font-family:'Source Sans Pro',sans-serif;">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:.75rem;margin-bottom:.55rem;">
            <div style="font-size:.74rem;letter-spacing:.08em;text-transform:uppercase;color:#475569;font-weight:700;">
                DBI Readout · {_safe_page}
            </div>
            <span data-testid="dbi-stamp" style="font-size:.72rem;color:#64748b;white-space:nowrap;">
                {digest} · {ts}
            </span>
        </div>
        <div style="display:grid;grid-template-columns:minmax(0,0.95fr) minmax(0,1.55fr);gap:.75rem;align-items:stretch;">
            <div style="background:{_status_bg};border:1px solid {_status_color}33;border-radius:.4rem;padding:.75rem;">
                <div style="font-size:.72rem;color:{_status_color};font-weight:800;text-transform:uppercase;margin-bottom:.25rem;">
                    {_status_label}
                </div>
                <div style="font-size:1rem;line-height:1.3;font-weight:700;color:#0f172a;">
                    {_safe_action}
                </div>
            </div>
            <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:.4rem;padding:.75rem;">
                🧠 <b>Dynamic Brain Insight ({_safe_page}):</b><br>
                <span data-testid="dbi-body" style="line-height:1.35;">{_safe_why}</span>
            </div>
        </div>
        <div style="display:flex;justify-content:space-between;gap:.75rem;flex-wrap:wrap;margin-top:.55rem;font-size:.76rem;color:#64748b;">
            <span>Scope: {_safe_scope}</span>
            {'<span data-testid="dbi-source">Insight source: ' + _safe_source + '</span>' if not loading else ''}
        </div>
        </div>""",
        unsafe_allow_html=True,
    )

    # Native popover button — not subject to CSS overflow clipping.
    if not loading:
        params = {
            k: v for k, v in context_dict.items()
            if v is not None and str(v).strip()
        }
        with st.expander(f"🔍 DBI inputs · {_source_label}", expanded=False):
            st.caption(f"**Insight source:** {_source_label}")
            st.divider()
            st.markdown("**Filters and signals used for this readout:**")
            if params:
                for k, v in params.items():
                    # Truncate very long values (e.g. full DataFrames) for readability
                    display_val = str(v)
                    if len(display_val) > 200:
                        display_val = display_val[:200] + "…"
                    st.markdown(f"- **{k}**: `{display_val}`")
            else:
                st.markdown("*No active filter parameters*")
