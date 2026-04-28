"""Lead-Time Survival — Kaplan-Meier + Cox PH risk analysis with interactive charts."""
from __future__ import annotations
from pathlib import Path
import sys
import pandas as pd
import numpy as np
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.db_registry import bootstrap_default_connectors, list_connectors, read_sql
from src.brain.research.lead_time_survival import per_group_lead_time, cox_lead_time
from src.brain.findings_index import record_findings_bulk
from src.brain.col_resolver import discover_table_columns, resolve
from src.brain.label_resolver import enrich_labels
from src.brain.global_filters import date_key_window
from src.brain.operator_shell import render_operator_sidebar_fallback

# set_page_config handled by app.py st.navigation()
render_operator_sidebar_fallback()
bootstrap_default_connectors()

import plotly.express as px
import plotly.graph_objects as go

st.markdown("## ⏱️ Lead-Time Survival Analysis")
st.caption("Kaplan-Meier + Cox Proportional Hazards · per supplier × part × lane · MIT ILP framework")

connectors = list_connectors()
if not connectors:
    st.error("No connectors registered. Visit ⚙️ Connectors page first.")
    st.stop()


def _build_survival_sql() -> str:
    """Build the survival SQL using live-resolved column names."""
    cols = discover_table_columns("azure_sql", "edap_dw_replica", "fact_po_receipt")
    part_col    = resolve(cols, "part_key")     or "part_key"
    sup_col     = resolve(cols, "supplier_key") or "supplier_key"
    order_col   = resolve(cols, "due_date")   or "due_date_key"
    receipt_col = resolve(cols, "receipt_date") or "receipt_date_key"
    # Integer YYYYMMDD keys → convert via varchar(8) with style 112
    def _d(c): return f"TRY_CONVERT(date, CONVERT(varchar(8), [{c}]), 112)"
    sk, ek = date_key_window()
    return f"""
SELECT [{sup_col}]    AS supplier_key,
       [{part_col}]   AS part_key,
       NULL           AS lane_id,
       NULL           AS mode,
       NULL           AS region,
       DATEDIFF(day,
           {_d(order_col)},
           {_d(receipt_col)}) AS lead_time_days,
       1              AS event
FROM [edap_dw_replica].[fact_po_receipt] WITH (NOLOCK)
WHERE {_d(receipt_col)} IS NOT NULL
  AND {_d(order_col)}   IS NOT NULL
  AND DATEDIFF(day,
          {_d(order_col)},
          {_d(receipt_col)}) BETWEEN 0 AND 730
  AND [{receipt_col}] BETWEEN {sk} AND {ek}
"""


def _get_default_sql() -> str:
    # Always rebuild so the global timeline window is honoured.
    try:
        st.session_state["_lt_sql"] = _build_survival_sql()
    except Exception:
        pass
    if "_lt_sql" not in st.session_state:
        try:
            st.session_state["_lt_sql"] = _build_survival_sql()
        except Exception:
            st.session_state["_lt_sql"] = """
SELECT [supplier_key], [part_key], NULL AS lane_id, NULL AS mode, NULL AS region,
       DATEDIFF(day,
           TRY_CONVERT(date, CONVERT(varchar(8), [due_date_key]), 112),
           TRY_CONVERT(date, CONVERT(varchar(8), [receipt_date_key]), 112)
       ) AS lead_time_days,
       1 AS event
FROM [edap_dw_replica].[fact_po_receipt] WITH (NOLOCK)
WHERE TRY_CONVERT(date, CONVERT(varchar(8), [receipt_date_key]), 112) IS NOT NULL
  AND DATEDIFF(day,
       TRY_CONVERT(date, CONVERT(varchar(8), [due_date_key]), 112),
       TRY_CONVERT(date, CONVERT(varchar(8), [receipt_date_key]), 112)
  ) BETWEEN 0 AND 730
"""
    return st.session_state["_lt_sql"]


DEFAULT_SQL = _get_default_sql()

with st.expander("🔍 Connector & SQL Override", expanded=False):
    src_name = st.selectbox("Source", [c.name for c in connectors], key="lt_cn")
    sql = st.text_area("SQL", value=DEFAULT_SQL, height=160, key="lt_sql")
    groups_sel = st.multiselect("Group by",
        ["supplier_key","part_key","lane_id","mode","region"],
        default=["supplier_key"], key="lt_group")

src_name  = st.session_state.get("lt_cn", connectors[0].name)
sql       = st.session_state.get("lt_sql", DEFAULT_SQL)
sel_group = st.session_state.get("lt_group", ["supplier_key"])

@st.cache_data(ttl=600, show_spinner="Running survival analysis on live PO receipts …")
def _load(cn: str, q: str):
    return read_sql(cn, q, timeout_s=120)

df = _load(src_name, sql)
if df.attrs.get("_error"):
    st.error(df.attrs["_error"])
    st.code(sql, language="sql")
    st.stop()
if df.empty:
    st.warning("0 rows returned. Check table mapping in `config/brain.yaml`.")
    st.stop()

st.markdown(f"🟢 **Live** · {len(df):,} PO receipt rows")

# ── Brain action banner ──────────────────────────────────────────────────────────────
import datetime as _dt
_s_d, _e_d = (st.session_state.get("g_date_start"), st.session_state.get("g_date_end"))
_window_caption = f" within {(_e_d-_s_d).days}d window" if _s_d and _e_d else ""


# Enrich supplier/part keys → human-readable labels
df = enrich_labels(df)
# Prefer label column in group selector
_sup_display = "supplier_key_label" if "supplier_key_label" in df.columns else "supplier_key"
_prt_display = "part_key_label"     if "part_key_label"     in df.columns else "part_key"
# Update sel_group to prefer label columns
sel_group = [_sup_display if g == "supplier_key" else (_prt_display if g == "part_key" else g)
             for g in sel_group if g in df.columns or (g == "supplier_key" and _sup_display in df.columns)
                                                     or (g == "part_key" and _prt_display in df.columns)]

group_options = [c for c in sel_group if c in df.columns]
if not group_options:
    group_options = [c for c in ("supplier_key","part_key","lane_id","mode","region") if c in df.columns]
if not group_options:
    st.warning("No valid grouping columns in live data.")
    st.stop()

table = per_group_lead_time(df, group_cols=group_options,
                             duration_col="lead_time_days", event_col="event")
if table.empty:
    st.warning("No groups met minimum sample size.")
    st.stop()

record_findings_bulk("lead_time_survival","supplier",
    [{"key": str(row[group_options[0]]), "score": float(row.get("p95_lt",0)),
      "payload": {k: str(v) for k,v in row.items()}}
     for _, row in table.head(50).iterrows()])

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight('Lead Time Survival', ctx)

# ── KPI strip ────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
k1.metric("🔍 Groups",       f"{len(table):,}")
k2.metric("⏱ Max P95 LT",   f"{table['p95_lt'].max():.0f} d")
k3.metric("📊 Median P50",   f"{table['median_lt'].median():.0f} d")
k4.metric("⚠️ Groups >30d P95",
          int((table["p95_lt"] > 30).sum()) if "p95_lt" in table.columns else "—")

st.divider()

# ── Visualizations ────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["📈 Survival Curves","🌡 Risk Heatmap","🏆 Rankings","🔬 Cox PH Hazards"])

with tab1:
    st.subheader("Kaplan-Meier Survival Curves")
    st.caption("Y = probability of lead time still unsettled by day X · Lower curves = faster suppliers")
    try:
        from lifelines import KaplanMeierFitter

        fig_km = go.Figure()
        color_seq = px.colors.qualitative.Vivid
        groups_to_plot = table.head(10)[group_options[0]].tolist()

        for idx, grp_val in enumerate(groups_to_plot):
            mask = df[group_options[0]].astype(str) == str(grp_val)
            sub = df[mask].dropna(subset=["lead_time_days","event"])
            if len(sub) < 3:
                continue
            kmf = KaplanMeierFitter()
            kmf.fit(sub["lead_time_days"], sub["event"],
                    label=str(grp_val)[:30])
            sf = kmf.survival_function_
            ci = kmf.confidence_interval_survival_function_
            col = color_seq[idx % len(color_seq)]
            fig_km.add_trace(go.Scatter(
                x=sf.index, y=sf.iloc[:, 0],
                name=str(grp_val)[:25],
                line=dict(color=col, width=2),
                hovertemplate=f"{grp_val}<br>Day: %{{x}}<br>S(t): %{{y:.3f}}<extra></extra>",
            ))
            # CI band
            fig_km.add_trace(go.Scatter(
                x=list(ci.index) + list(ci.index[::-1]),
                y=list(ci.iloc[:,0]) + list(ci.iloc[:,1][::-1]),
                fill="toself", line=dict(width=0),
                fillcolor=col.replace("rgb","rgba").replace(")",",0.12)"),
                showlegend=False, hoverinfo="skip",
            ))

        fig_km.update_layout(
            height=500, template="plotly",
            xaxis_title="Days", yaxis_title="Survival Probability",
            title="Kaplan-Meier Lead-Time Survival (Top 10 Groups)",
            yaxis=dict(range=[0,1.05]),
        )
        km_click = st.plotly_chart(fig_km, use_container_width=True,
                                   key="km_plot", on_select="rerun")
        if km_click and km_click.get("selection",{}).get("points"):
            pt = km_click["selection"]["points"][0]
            grp = pt.get("legendgroup") or pt.get("name","")
            if grp:
                st.session_state["lt_selected_group"] = grp
    except Exception as e:
        st.warning(f"KM curves unavailable: {e}")
        st.dataframe(table.head(20), use_container_width=True)

with tab2:
    st.subheader("Lead-Time Risk Heatmap")
    if len(group_options) >= 2:
        try:
            agg_cols = [c for c in group_options[:2] if c in df.columns]
            heat_df = (df.groupby(agg_cols)["lead_time_days"]
                       .mean().reset_index()
                       .rename(columns={"lead_time_days":"avg_lt"}))
            pivot = heat_df.pivot_table(
                index=agg_cols[0], columns=agg_cols[1], values="avg_lt")
            fig_heat = px.imshow(
                pivot, color_continuous_scale="RdYlGn_r",
                title=f"Average Lead Time Days: {agg_cols[0]} × {agg_cols[1]}",
                labels=dict(color="Avg Days"),
                template="plotly",
            )
            fig_heat.update_layout(height=500)
            heat_click = st.plotly_chart(fig_heat, use_container_width=True,
                                         key="lt_heat", on_select="rerun")
            if heat_click and heat_click.get("selection",{}).get("points"):
                pt = heat_click["selection"]["points"][0]
                st.session_state["lt_selected_group"] = f"{pt.get('y','')}"
        except Exception as e:
            st.info(f"Heatmap requires 2+ group columns. {e}")
    else:
        # Single-group violin
        if "lead_time_days" in df.columns and group_options[0] in df.columns:
            top20g = df[group_options[0]].value_counts().head(15).index.tolist()
            sub = df[df[group_options[0]].isin(top20g)]
            fig_vio = px.violin(sub, x=group_options[0], y="lead_time_days",
                                box=True, points="outliers",
                                color=group_options[0],
                                title="Lead-Time Distribution by Group",
                                template="plotly",
                                labels={"lead_time_days":"Days"})
            fig_vio.update_layout(height=450, showlegend=False,
                                  xaxis_tickangle=-45)
            st.plotly_chart(fig_vio, use_container_width=True)

with tab3:
    st.subheader("🏆 Supplier Risk Ranking")
    fig_rank = px.bar(
        table.head(30).sort_values("p95_lt", ascending=False),
        x=group_options[0], y=["median_lt","p95_lt"],
        barmode="group",
        title="Median vs P95 Lead Time by Group (Worst 30)",
        labels={"value":"Days", group_options[0]: group_options[0].replace("_"," ").title()},
        color_discrete_map={"median_lt":"#38bdf8","p95_lt":"#ef4444"},
        template="plotly",
    )
    fig_rank.update_layout(height=480, xaxis_tickangle=-45)
    rank_click = st.plotly_chart(fig_rank, use_container_width=True,
                                 key="lt_rank", on_select="rerun")
    if rank_click and rank_click.get("selection",{}).get("points"):
        pt = rank_click["selection"]["points"][0]
        st.session_state["lt_selected_group"] = str(pt.get("x",""))

    # Drill-down on selected group
    sel_grp = st.session_state.get("lt_selected_group")
    if sel_grp and group_options[0] in df.columns:
        st.divider()
        st.subheader(f"🔍 Drill-down: `{sel_grp}`")
        sub_d = df[df[group_options[0]].astype(str) == str(sel_grp)]
        d1, d2, d3 = st.columns(3)
        d1.metric("Receipts",   len(sub_d))
        d2.metric("Avg LT",     f"{sub_d['lead_time_days'].mean():.0f} d" if "lead_time_days" in sub_d else "—")
        d3.metric("P95 LT",     f"{sub_d['lead_time_days'].quantile(0.95):.0f} d" if "lead_time_days" in sub_d else "—")

        if "lead_time_days" in sub_d.columns:
            fig_dd = px.histogram(sub_d, x="lead_time_days", nbins=40,
                                  color_discrete_sequence=["#38bdf8"],
                                  title=f"Lead-Time Distribution: {sel_grp}",
                                  template="plotly")
            fig_dd.add_vline(x=sub_d["lead_time_days"].median(), line_dash="dash",
                             line_color="#22c55e", annotation_text="P50")
            fig_dd.add_vline(x=sub_d["lead_time_days"].quantile(0.95), line_dash="dash",
                             line_color="#ef4444", annotation_text="P95")
            fig_dd.update_layout(paper_bgcolor="#0f172a", height=300)
            st.plotly_chart(fig_dd, use_container_width=True)
        st.dataframe(sub_d.head(100), use_container_width=True, hide_index=True)

with tab4:
    st.subheader("🔬 Cox Proportional Hazards — Covariate Effects")
    cov = [c for c in ("supplier_key","lane_id","mode","region") if c in df.columns]
    if cov:
        try:
            cph_df = cox_lead_time(df, "lead_time_days", "event", cov)
            if not cph_df.empty:
                if "coef" in cph_df.columns:
                    fig_cph = px.bar(
                        cph_df.sort_values("coef"), x="coef", y=cph_df.index if cph_df.index.name else "covariate",
                        orientation="h", error_x=cph_df.get("se(coef)"),
                        color="coef", color_continuous_scale="RdYlGn",
                        title="Cox PH Hazard Coefficients (positive = longer lead time)",
                        template="plotly",
                    )
                    fig_cph.add_vline(x=0, line_dash="solid", line_color="#64748b")
                    fig_cph.update_layout(height=400, coloraxis_showscale=False)
                    st.plotly_chart(fig_cph, use_container_width=True)
                st.dataframe(cph_df, use_container_width=True)
        except Exception as e:
            st.info(f"Cox PH requires lifelines ≥ 0.27: {e}")
    else:
        st.info("No covariate columns available for Cox PH model.")

