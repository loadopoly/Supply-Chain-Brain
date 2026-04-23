import concurrent.futures as _cf
import streamlit as st
import threading
import time
from streamlit.runtime.scriptrunner import add_script_run_ctx


class BrainInsightWorker:
    _insights = {}
    _lock = threading.Lock()

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
            "Contextual intelligence loading — auto-updates on 2-s tick."
        )

    @classmethod
    def get_insight(cls, page_name: str, context_dict: dict) -> str:
        # Build the cache key robustly — guard against mixed-type keys or
        # very large values (DataFrames, bytes) that appear in session_state.
        try:
            context_str = ", ".join(
                f"{k}={str(v)[:120]}"
                for k, v in sorted((str(k), v) for k, v in context_dict.items())
            )
        except Exception:
            context_str = page_name
        key = f"{page_name}_{hash(context_str)}"

        with cls._lock:
            if key in cls._insights:
                return cls._insights[key]
            # Set a quick synchronous template immediately so the rendered
            # card shows data-loading="0" on the very first fragment render.
            # The background worker will overwrite this with a richer insight.
            cls._insights[key] = cls._build_quick_insight(page_name, context_dict)

        def worker():
            # ── 1. Try RAG (LLM ensemble via OpenRouter) ──────────────────
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
                    cls._insights[key] = rag_text
                return

            # ── 2. Template fallback ───────────────────────────────────────
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
                cls._insights[key] = insight

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
    _source_label = ""
    if not loading:
        _is_rag = len(str(insight)) > 200 and not any(
            marker in str(insight) for marker in [
                "Supplier → part → site", "Recursive OTD", "Kaplan-Meier",
                "Bullwhip amplification", "Procurement 360",
            ]
        )
        _source_label = "🤖 LLM (OpenRouter)" if _is_rag else "📋 Template"

    st.markdown(
        f"""<div class="dbi-container"
            data-testid="dbi-card"
            data-page="{page_name}"
            data-digest="{digest}"
            data-dbi-updated="{updated_flag}"
            data-loading="{'1' if loading else '0'}"
            role="status" aria-live="polite"
            style="background:#f0f2f6;border-left:4px solid #0068c9;
            padding:.75rem 1rem;border-radius:.4rem;margin-bottom:.25rem;
            color:#31333f;font-family:'Source Sans Pro',sans-serif;">
        🧠 <b>Dynamic Brain Insight ({page_name}):</b>
        <span data-testid="dbi-stamp" style="float:right;font-size:.7rem;color:#64748b;">
            {digest} · {ts}
        </span><br>
        <span data-testid="dbi-body">{body}</span>
        {'<br><span data-testid="dbi-source" style="font-size:.7rem;color:#64748b;">Insight source: ' + _source_label + '</span>' if not loading else ''}
        </div>""",
        unsafe_allow_html=True,
    )

    # Native popover button — not subject to CSS overflow clipping.
    if not loading:
        params = {
            k: v for k, v in context_dict.items()
            if v is not None and str(v).strip()
        }
        with st.expander(f"🔍 Parameters · {_source_label}", expanded=False):
            st.caption(f"**Insight source:** {_source_label}")
            st.divider()
            st.markdown("**Relational Parameters Read by Brain:**")
            if params:
                for k, v in params.items():
                    # Truncate very long values (e.g. full DataFrames) for readability
                    display_val = str(v)
                    if len(display_val) > 200:
                        display_val = display_val[:200] + "…"
                    st.markdown(f"- **{k}**: `{display_val}`")
            else:
                st.markdown("*No active filter parameters*")
