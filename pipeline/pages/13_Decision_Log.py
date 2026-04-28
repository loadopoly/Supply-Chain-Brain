"""Decision Provenance Log — interactive timeline with drill-downs."""
from __future__ import annotations
from pathlib import Path
import sys
import json
import pandas as pd
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.operator_shell import render_operator_sidebar_fallback
from src.brain.findings_index import lookup_decisions, all_kinds

# set_page_config handled by app.py st.navigation()
render_operator_sidebar_fallback()

import plotly.express as px
import plotly.graph_objects as go

st.markdown("## 📒 Decision Provenance Log")
st.caption("Every recommendation's inputs · model · confidence — MIT Digital SC Lab trust pillar")

# ── Filters ─────────────────────────────────────────────────────────────────
f1, f2, f3 = st.columns([2,2,1])
with f1:
    kinds = all_kinds()
    kind = st.selectbox("Filter by kind", ["(any)"] + kinds)
with f2:
    key_filter = st.text_input("Filter by key", placeholder="part_id, supplier…")
with f3:
    limit = st.number_input("Max rows", min_value=50, max_value=5000, value=500, step=50)

rows = lookup_decisions(
    target_kind=None if kind == "(any)" else kind,
    target_key=key_filter or None, limit=int(limit),
)
df = pd.DataFrame(rows)

if df.empty:
    st.info("No decisions logged yet — run any recommendation page to populate this log.")
    st.markdown("""
    **Quick start:**
    - Run **EOQ Deviation** to log re-ranking decisions
    - Run **OTD Recursive** to log cluster assignments  
    - Run **Lead-Time Survival** to log supplier risk scores
    - Run **Procurement 360** to log leverage point analysis
    """)
    st.stop()

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight('Decision Log', ctx)

# ── KPI strip ────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
k1.metric("📝 Decisions", f"{len(df):,}")
k2.metric("🔢 Models Used", df["model"].nunique() if "model" in df.columns else "—")
k3.metric("🗂️ Kinds", df["target_kind"].nunique() if "target_kind" in df.columns else "—")
k4.metric("📅 Latest", str(df["ts"].max())[:16] if "ts" in df.columns else "—")

st.divider()

tab1, tab2, tab3 = st.tabs(["📈 Timeline","📊 Model Activity","🔍 Detail Records"])

with tab1:
    st.subheader("Decision Activity Timeline")
    if "ts" in df.columns and "page" in df.columns:
        try:
            df["ts"] = pd.to_datetime(df["ts"])
            ts_grouped = df.groupby([df["ts"].dt.date, "page"]).size().reset_index()
            ts_grouped.columns = ["date","page","count"]
            fig_ts = px.bar(ts_grouped, x="date", y="count", color="page",
                            title="Decisions Logged per Page per Day",
                            template="plotly",
                            labels={"count":"Decisions","date":"Date"},
                            color_discrete_sequence=px.colors.qualitative.Vivid)
            fig_ts.update_layout(height=380,
                                 xaxis_tickangle=-30)
            st.plotly_chart(fig_ts, use_container_width=True)
        except Exception as e:
            st.info(f"Timeline requires timestamp column: {e}")

with tab2:
    st.subheader("Model & Page Activity")
    c_left, c_right = st.columns(2)
    with c_left:
        if "model" in df.columns:
            model_counts = df["model"].value_counts().reset_index()
            model_counts.columns = ["model","count"]
            fig_mod = px.pie(model_counts, names="model", values="count",
                             title="Decisions by Model",
                             template="plotly",
                             color_discrete_sequence=px.colors.qualitative.Vivid,
                             hole=0.4)
            fig_mod.update_layout(paper_bgcolor="#0f172a", height=350)
            st.plotly_chart(fig_mod, use_container_width=True)
    with c_right:
        if "page" in df.columns:
            page_counts = df["page"].value_counts().reset_index()
            page_counts.columns = ["page","count"]
            fig_pg = px.bar(page_counts, x="count", y="page", orientation="h",
                            title="Decisions by Page",
                            template="plotly",
                            color="count", color_continuous_scale="Viridis",
                            labels={"count":"# Decisions"})
            fig_pg.update_layout(paper_bgcolor="#0f172a",
                                 height=350, coloraxis_showscale=False)
            st.plotly_chart(fig_pg, use_container_width=True)

    if "confidence" in df.columns:
        conf_df = df.dropna(subset=["confidence"])
        if not conf_df.empty:
            fig_conf = px.histogram(conf_df, x="confidence", nbins=20,
                                    color_discrete_sequence=["#38bdf8"],
                                    title="Confidence Score Distribution",
                                    template="plotly")
            fig_conf.update_layout(paper_bgcolor="#0f172a", height=280)
            st.plotly_chart(fig_conf, use_container_width=True)

with tab3:
    st.subheader("🔍 Decision Records")
    search = st.text_input("🔎 Search all fields", placeholder="supplier, part, model…")
    display_df = df.copy()
    if search:
        mask = display_df.astype(str).apply(lambda col: col.str.contains(search, case=False)).any(axis=1)
        display_df = display_df[mask]

    st.markdown(f"**{len(display_df):,}** records shown")

    # Expand JSON inputs column for readability
    if "inputs" in display_df.columns:
        try:
            display_df["inputs_preview"] = display_df["inputs"].apply(
                lambda x: str(x)[:80] + "…" if len(str(x)) > 80 else str(x))
        except Exception:
            pass

    row_click = st.dataframe(
        display_df.head(200), use_container_width=True, hide_index=True,
        on_select="rerun", selection_mode="single-row", key="dec_table"
    )

    if row_click and row_click.get("selection",{}).get("rows"):
        idx = row_click["selection"]["rows"][0]
        row = display_df.iloc[idx]
        st.divider()
        st.subheader(f"🔍 Decision Detail: `{row.get('action','')}`")
        d1, d2, d3 = st.columns(3)
        d1.info(f"**Page:** {row.get('page','—')}")
        d2.info(f"**Model:** {row.get('model','—')}")
        d3.info(f"**Confidence:** {row.get('confidence','—')}")
        if "inputs" in row and row["inputs"]:
            st.json(row["inputs"] if isinstance(row["inputs"], dict)
                    else json.loads(str(row["inputs"])))

    st.download_button("⬇ Export CSV", display_df.to_csv(index=False).encode("utf-8"),
                       file_name="decision_log.csv", mime="text/csv")
