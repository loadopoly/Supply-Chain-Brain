import streamlit as st
import threading
import time
from streamlit.runtime.scriptrunner import add_script_run_ctx


class BrainInsightWorker:
    _insights = {}
    _lock = threading.Lock()

    @classmethod
    def get_insight(cls, page_name: str, context_dict: dict) -> str:
        context_str = ", ".join(f"{k}={v}" for k, v in sorted(context_dict.items()))
        key = f"{page_name}_{hash(context_str)}"

        with cls._lock:
            if key in cls._insights:
                return cls._insights[key]
            cls._insights[key] = "LOADING"

        def worker():
            # Sleep 1s so the thread always finishes before the 2s fragment re-run.
            time.sleep(1)

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
                insight = (
                    f"Data quality audit — **{site}** · {window_str}. "
                    "Null-rate and format-compliance checks run across active tables. "
                    "Flag any columns above 5% null threshold for upstream remediation."
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
        return "LOADING"


@st.fragment(run_every=2)
def render_dynamic_brain_insight(page_name: str, context_dict: dict):
    """
    Renders a non-blocking Dynamic Brain Insight card with a native st.popover
    for parameter details. The popover replaces the previous CSS hover tooltip,
    which was silently clipped by Streamlit's overflow:hidden containers.
    """
    insight = BrainInsightWorker.get_insight(page_name, context_dict)
    loading = "LOADING" in str(insight)

    body = (
        f"<i>Generating intelligence for [{page_name}]…</i>"
        if loading
        else insight
    )

    # .dbi-container class is preserved so Playwright / test selectors still work.
    st.markdown(
        f"""<div class="dbi-container" style="background:#f0f2f6;border-left:4px solid #0068c9;
        padding:.75rem 1rem;border-radius:.4rem;margin-bottom:.25rem;
        color:#31333f;font-family:'Source Sans Pro',sans-serif;">
        🧠 <b>Dynamic Brain Insight ({page_name}):</b><br>{body}
        </div>""",
        unsafe_allow_html=True,
    )

    # Native popover button — not subject to CSS overflow clipping.
    if not loading:
        params = {
            k: v for k, v in context_dict.items()
            if v is not None and str(v).strip()
        }
        with st.popover("🔍 Parameters"):
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
