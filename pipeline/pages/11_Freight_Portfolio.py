"""Freight Portfolio — smart contract/spot mix + goldfish memory + ghost lane survival."""
from __future__ import annotations
from pathlib import Path
import sys
import pandas as pd
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.operator_shell import render_operator_sidebar_fallback
from src.brain.db_registry import bootstrap_default_connectors, list_connectors, read_sql
from src.brain.research.freight_portfolio import lane_volatility, portfolio_mix, goldfish_score
from src.brain.ips_freight import (
    is_enabled as ips_enabled, get_json,
    ghost_lane_candidates, ghost_lane_survival,
)
from src.brain.col_resolver import discover_table_columns, resolve
from src.brain.label_resolver import get_supplier_labels
from src.brain.global_filters import date_key_window

# set_page_config handled by app.py st.navigation()
render_operator_sidebar_fallback()
bootstrap_default_connectors()

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.markdown("## 🚚 Smart Freight Portfolio")
st.caption("MIT FreightLab · contract/spot/mini-bid mix · goldfish-memory pricing · ghost-lane survival")

# Early DBI card — renders before SQL calls so Playwright finds [data-testid="dbi-card"]
# even when Azure SQL is offline and the page falls back to demo/error state.
_early_fp_ctx = {"page": "freight_portfolio", "g_site": st.session_state.get("g_site", "")}
render_dynamic_brain_insight("Freight Portfolio", _early_fp_ctx)

connectors = list_connectors()
default_cn = connectors[0].name if connectors else "azure_sql"


def _build_port_sql() -> str:
    def _d(c): return f"TRY_CONVERT(date, CONVERT(varchar(8), [{c}]), 112)"
    cols = discover_table_columns("azure_sql", "edap_dw_replica", "fact_po_receipt")
    sup_col  = resolve(cols, "supplier_key") or "supplier_key"
    bu_col   = resolve(cols, "business_unit") or "business_unit_key"
    date_col = resolve(cols, "receipt_date") or "receipt_date_key"
    sk, ek = date_key_window()
    # OD pair = supplier (origin) → business unit (destination plant)
    return f"""
SELECT CAST([{sup_col}] AS varchar(64))
         + ' \u2192 ' + CAST([{bu_col}] AS varchar(64)) AS lane_id,
       CAST([{sup_col}] AS varchar(64))   AS origin_key,
       CAST([{bu_col}] AS varchar(64))    AS destination_key,
       FORMAT({_d(date_col)}, 'yyyy-MM') AS period,
       COUNT(*) AS load_count
FROM [edap_dw_replica].[fact_po_receipt] WITH (NOLOCK)
WHERE {_d(date_col)} IS NOT NULL
  AND [{date_col}] BETWEEN {sk} AND {ek}
GROUP BY CAST([{sup_col}] AS varchar(64)),
         CAST([{bu_col}]  AS varchar(64)),
         FORMAT({_d(date_col)}, 'yyyy-MM')
"""


def _get_port_sql() -> str:
    # Always rebuild so the global timeline window is honoured.
    try:
        st.session_state["_port_sql"] = _build_port_sql()
    except Exception:
        pass
    if "_port_sql" not in st.session_state:
        try:
            st.session_state["_port_sql"] = _build_port_sql()
        except Exception:
            st.session_state["_port_sql"] = """
SELECT CAST([supplier_key] AS varchar(64)) AS lane_id,
       FORMAT(TRY_CONVERT(date, CONVERT(varchar(8), [receipt_date_key]), 112), 'yyyy-MM') AS period,
       COUNT(*) AS load_count
FROM [edap_dw_replica].[fact_po_receipt] WITH (NOLOCK)
WHERE TRY_CONVERT(date, CONVERT(varchar(8), [receipt_date_key]), 112) IS NOT NULL
GROUP BY CAST([supplier_key] AS varchar(64)),
         FORMAT(TRY_CONVERT(date, CONVERT(varchar(8), [receipt_date_key]), 112), 'yyyy-MM')
"""
    return st.session_state["_port_sql"]


tab_port, tab_gold, tab_ghost = st.tabs(["📊 Portfolio Mix", "🐟 Goldfish Memory", "👻 Ghost Lanes"])

with tab_port:
    PORT_SQL = _get_port_sql()
    with st.expander("🔍 SQL / connector", expanded=False):
        cn = st.selectbox("Connector", [c.name for c in connectors], key="port_cn")
        src = st.text_area("SQL", value=PORT_SQL, height=100, key="port_sql")
    cn  = st.session_state.get("port_cn", default_cn)
    src = st.session_state.get("port_sql", PORT_SQL)

    @st.cache_data(ttl=600, show_spinner="Pulling lane × period from replica …")
    def _port(cn_: str, q: str):
        return read_sql(cn_, q, timeout_s=120)
    df_port = _port(cn, src)

    if df_port.attrs.get("_error"):
        st.error(df_port.attrs["_error"])
        st.code(src, language="sql")
    elif df_port.empty:
        st.warning("Live `fact_shipment` returned 0 rows.")
    else:
        # Replace raw supplier_key in lane_id with supplier names where possible
        try:
            _slabels = get_supplier_labels()
            if "origin_key" in df_port.columns and _slabels:
                df_port["origin_name"] = df_port["origin_key"].astype(str).map(
                    lambda k: _slabels.get(k, k))
                df_port["lane_id"] = df_port["origin_name"] + " → " + df_port.get(
                    "destination_key", "").astype(str)
        except Exception:
            pass
        st.markdown(f"🟢 **Live** · {df_port['lane_id'].nunique() if 'lane_id' in df_port.columns else 0} OD pairs (Origin → Destination)")
        vol = lane_volatility(df_port)
        mix = portfolio_mix(vol)

        ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
        render_dynamic_brain_insight('Freight Portfolio', ctx)

        # KPI strip
        p1, p2, p3 = st.columns(3)
        if "contract_pct" in mix.columns:
            p1.metric("📝 Avg Contract %", f"{mix['contract_pct'].mean():.0f}%")
        if "spot_pct" in mix.columns:
            p2.metric("⚡ Avg Spot %", f"{mix['spot_pct'].mean():.0f}%")
        p3.metric("🛣️ Lanes", len(mix))

        # Portfolio scatter: volatility vs volume
        x_vol_col = "volatility" if "volatility" in vol.columns else ("cv" if "cv" in vol.columns else None)
        y_vol_col = "load_count" if "load_count" in vol.columns else ("mean_loads" if "mean_loads" in vol.columns else None)
        if x_vol_col and y_vol_col and "lane_id" in vol.columns:
            mix_for_merge = mix.drop(
                columns=[c for c in (x_vol_col, y_vol_col) if c in mix.columns],
                errors="ignore",
            )
            vol_merge = vol.merge(mix_for_merge, on="lane_id", how="left") if "lane_id" in mix.columns else vol
            fig_scatter = px.scatter(
                vol_merge, x=x_vol_col, y=y_vol_col,
                color="contract_pct" if "contract_pct" in vol_merge.columns else None,
                size=y_vol_col,
                hover_name="lane_id" if "lane_id" in vol_merge.columns else None,
                color_continuous_scale="RdYlGn",
                title="Lane Portfolio: Volatility vs Volume",
                template="plotly",
                labels={x_vol_col:"Demand Volatility (CV)", y_vol_col:"Average Monthly Loads"},
            )
            fig_scatter.update_layout(paper_bgcolor="#0f172a",
                                       height=430, coloraxis_showscale=True)
            port_click = st.plotly_chart(fig_scatter, use_container_width=True,
                                         key="port_scatter", on_select="rerun")
            if port_click and port_click.get("selection",{}).get("points"):
                pt = port_click["selection"]["points"][0]
                st.session_state["port_lane"] = pt.get("hovertext") or str(pt.get("customdata",""))

        # Bar chart: recommended mix per lane
        if not mix.empty:
            mix_cols = [c for c in mix.columns if "_pct" in c]
            id_col = "lane_id" if "lane_id" in mix.columns else mix.columns[0]
            if mix_cols:
                fig_mix = px.bar(mix.head(20), x=id_col, y=mix_cols, barmode="stack",
                                  title="Recommended Contract Mix by Lane (Top 20)",
                                  template="plotly",
                                  labels={"value":"Fraction","variable":"Type"},
                                  color_discrete_map={
                                      "contract_pct":"#38bdf8",
                                      "spot_pct":"#f97316",
                                      "minibid_pct":"#a855f7",
                                  })
                fig_mix.update_layout(paper_bgcolor="#0f172a",
                                       height=380, xaxis_tickangle=-45)
                st.plotly_chart(fig_mix, use_container_width=True)

        # Drill-down on selected lane
        sel_lane = st.session_state.get("port_lane")
        if sel_lane and "lane_id" in df_port.columns:
            st.divider()
            st.subheader(f"🔍 Lane Drill-down: `{sel_lane}`")
            sub_lane = df_port[df_port["lane_id"].astype(str) == str(sel_lane)]
            if not sub_lane.empty and "period" in sub_lane.columns:
                fig_lane = px.bar(sub_lane, x="period", y="load_count",
                                   title=f"Load Volume Over Time: {sel_lane}",
                                   template="plotly",
                                   color_discrete_sequence=["#38bdf8"])
                fig_lane.update_layout(paper_bgcolor="#0f172a", height=280)
                st.plotly_chart(fig_lane, use_container_width=True)

with tab_gold:
    st.subheader("🐟 Goldfish-Memory Contract Pricing")
    st.caption("Shipper-carrier rate vs reliability gap — recommends contract terms to reduce rejection")
    GOLD_SQL = """-- vw_carrier_rate not available; derive rate-vs-reliability from PO receipts.
WITH receipt_rates AS (
    SELECT supplier_key,
           TRY_CONVERT(float, unit_cost_usd) AS rate,
           DATEDIFF(day,
               TRY_CONVERT(date, CONVERT(varchar(8), CAST(due_date_key     AS bigint)), 112),
               TRY_CONVERT(date, CONVERT(varchar(8), CAST(receipt_date_key AS bigint)), 112)
           ) AS lead_days
    FROM edap_dw_replica.fact_po_receipt WITH (NOLOCK)
    WHERE receipt_date_key IS NOT NULL
      AND due_date_key IS NOT NULL
      AND TRY_CONVERT(float, unit_cost_usd) IS NOT NULL
), market AS (
    SELECT AVG(rate) AS market_rate
    FROM receipt_rates
    WHERE lead_days BETWEEN 0 AND 730
), lt AS (
    SELECT supplier_key,
           AVG(rate) AS rate,
           AVG(CAST(lead_days AS float)) AS lead_avg,
           STDEV(CAST(lead_days AS float)) AS lead_std,
           AVG(CASE WHEN lead_days > 30 THEN 1.0 ELSE 0.0 END) AS rejection_rate
    FROM receipt_rates
    WHERE lead_days BETWEEN 0 AND 730
    GROUP BY supplier_key
)
SELECT TOP 200
       CAST(supplier_key AS varchar(64))   AS carrier_id,
       ISNULL(rate, 0)                     AS rate,
       ISNULL(market.market_rate, rate)    AS market_rate,
       ISNULL(rejection_rate, 0.0)         AS rejection_rate,
       1.0 / NULLIF(1.0 + ISNULL(lead_std,0), 0) AS reliability,
       ISNULL(lead_avg, 0)                 AS lead_avg
FROM lt
CROSS JOIN market
WHERE rate IS NOT NULL""".strip()
    with st.expander("🔍 SQL / connector", expanded=False):
        cn2  = st.selectbox("Connector", [c.name for c in connectors], key="g_cn")
        gsql = st.text_area("SQL", value=GOLD_SQL, height=80, key="g_sql")
    cn2  = st.session_state.get("g_cn", default_cn)
    gsql = st.session_state.get("g_sql", GOLD_SQL)

    @st.cache_data(ttl=600, show_spinner="Pulling carrier-rate view …")
    def _gold(cn_: str, q: str):
        return read_sql(cn_, q)
    df_gold = _gold(cn2, gsql)

    if df_gold.attrs.get("_error"):
        st.error(df_gold.attrs["_error"])
        st.code(gsql, language="sql")
    elif df_gold.empty:
        st.warning("Live carrier-rate view returned 0 rows.")
    else:
        st.markdown(f"🟢 **Live** · {len(df_gold):,} rows")
        gs = goldfish_score(df_gold)
        if not gs.empty:
            score_col = [c for c in gs.columns if "score" in c.lower() or "gap" in c.lower()]
            ych = score_col[0] if score_col else gs.columns[-1]
            id_ch = gs.columns[0]
            fig_gs = px.bar(
                gs.nlargest(25, ych) if ych in gs.columns else gs.head(25),
                x=id_ch, y=ych,
                color=ych, color_continuous_scale="RdYlGn_r",
                title="Top 25 Carriers by Goldfish Score (rate vs reliability gap)",
                template="plotly",
            )
            fig_gs.update_layout(paper_bgcolor="#0f172a",
                                  height=420, xaxis_tickangle=-45, coloraxis_showscale=False)
            st.plotly_chart(fig_gs, use_container_width=True)
            st.dataframe(gs, use_container_width=True, hide_index=True)

with tab_ghost:
    st.subheader("👻 Ghost Lane Detector")
    st.caption("Up to 70% of contracted lanes go unused — predict inactivation probability (MIT FreightLab)")
    horizon = st.slider("Inactivation horizon (days)", 14, 180, 30, key="ghost_h")
    _g_sk, _g_ek = date_key_window()
    GHOST_SQL = f"""
SELECT TOP 20000
       supplier_key,
       business_unit_key,
       part_key,
       po_number,
       receipt_date_key,
       due_date_key,
       received_qty,
       unit_cost_usd
FROM edap_dw_replica.fact_po_receipt WITH (NOLOCK)
WHERE receipt_date_key BETWEEN {_g_sk} AND {_g_ek}
""".strip()
    with st.expander("🔍 SQL / connector", expanded=False):
        cn3  = st.selectbox("Connector", [c.name for c in connectors], key="ghost_cn")
        psql = st.text_area("SQL", value=GHOST_SQL, height=80, key="ghost_sql")
    cn3  = st.session_state.get("ghost_cn", default_cn)
    psql = st.session_state.get("ghost_sql", GHOST_SQL)

    @st.cache_data(ttl=600, show_spinner="Pulling PO receipts …")
    def _ghost(cn_: str, q: str):
        return read_sql(cn_, q)
    po = _ghost(cn3, psql)

    if po.attrs.get("_error"):
        st.error(po.attrs["_error"])
        st.code(psql, language="sql")
    elif po.empty:
        st.warning("Live PO receipts returned 0 rows.")
    else:
        st.markdown(f"🟢 **Live** · {len(po):,} PO receipts")
        contracts = pd.DataFrame()
        if ips_enabled():
            data = get_json("api/contracts/lanes") or []
            contracts = pd.DataFrame(data if isinstance(data, list) else data.get("rows", []))

        if contracts.empty:
            ghost_df = ghost_lane_candidates(po, inactive_days=horizon)
        else:
            ghost_df = ghost_lane_survival(contracts, po, horizon_days=horizon)

        if not ghost_df.empty:
            prob_col = [c for c in ghost_df.columns if "prob" in c.lower() or "risk" in c.lower() or "score" in c.lower()]
            ych = prob_col[0] if prob_col else ghost_df.columns[-1]
            id_ch = ghost_df.columns[0]

            g1, g2 = st.columns(2)
            with g1:
                g1.metric("👻 Ghost Candidates", len(ghost_df))
            with g2:
                if ych in ghost_df.columns:
                    g2.metric("⚠️ High Risk (>50%)", int((ghost_df[ych] > 0.5).sum()))

            fig_ghost = px.bar(
                ghost_df.nlargest(25, ych) if ych in ghost_df.columns else ghost_df.head(25),
                x=id_ch, y=ych,
                color=ych, color_continuous_scale="RdYlGn_r",
                title=f"Top 25 Ghost Lane Candidates (horizon: {horizon}d)",
                template="plotly",
                labels={ych:"Inactivation Probability"},
            )
            fig_ghost.add_hline(y=0.5, line_dash="dash", line_color="#eab308",
                                 annotation_text="⚠️ 50% threshold")
            fig_ghost.update_layout(paper_bgcolor="#0f172a",
                                     height=420, xaxis_tickangle=-45,
                                     coloraxis_showscale=False)
            st.plotly_chart(fig_ghost, use_container_width=True)
            st.dataframe(ghost_df, use_container_width=True, hide_index=True)

            # ── Brain action summary ───────────────────────────────────────
            st.markdown("### 🧠 Brain Action Summary")
            try:
                _slabels = get_supplier_labels()
            except Exception:
                _slabels = {}
            top = ghost_df.nlargest(5, ych) if ych in ghost_df.columns else ghost_df.head(5)
            for _, row in top.iterrows():
                lane = str(row.get(id_ch, ""))
                origin, _, dest = lane.partition("→")
                origin_name = _slabels.get(origin.strip(), origin.strip())
                risk = float(row[ych]) if ych in row else 0.0
                sev = "🔴" if risk >= 0.5 else ("🟡" if risk >= 0.3 else "🟢")
                st.markdown(
                    f"- {sev} **{origin_name} → {dest.strip() or 'plant'}** · "
                    f"inactivation risk **{risk:.0%}** in {horizon}d → "
                    f"_action: drop or renegotiate this lane; reallocate volume to nearest active OD pair_"
                )
        else:
            st.info(
                "🧠 **No ghost-lane candidates detected.** Either every contracted lane is "
                "still active in this window or the receipts table holds no inactivity signal. "
                "Try widening the global timeline or extending the horizon slider."
            )
