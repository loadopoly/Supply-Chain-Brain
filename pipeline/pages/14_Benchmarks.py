"""Streamlit page: Benchmark results viewer — best-in-class Plotly visualizations."""
from __future__ import annotations
import os, sys
from pathlib import Path
import pandas as pd
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from brain._version import __version__, __release__

# set_page_config handled by app.py st.navigation()

import plotly.express as px
import plotly.graph_objects as go

st.markdown("## ⚡ Performance Benchmarks")
st.caption(f"Supply Chain Brain v**{__version__}** · {__release__} · analytics module timing suite")

results_dir = ROOT / "bench" / "results"
latest = results_dir / "latest.csv"

if not latest.exists():
    st.warning(
        "No benchmark results yet. Run from a terminal:\n\n"
        "```\npython -m bench.bench_brain --rows 20000 --repeats 3\n```"
    )
    st.info("Benchmark results will appear here automatically after running the command above.")
    st.stop()

df = pd.read_csv(latest)

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight('Benchmarks', ctx)

# ── KPI strip ─────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("⚡ Benchmarks",      len(df))
k2.metric("📊 Rows / Scenario", int(df["n_rows"].iloc[0]) if "n_rows" in df.columns else "—")
k3.metric("⏱ Total Time (s)",   f"{df['elapsed_s'].sum():.2f}" if "elapsed_s" in df.columns else "—")
k4.metric("🐢 Slowest Module",  df.loc[df["elapsed_s"].idxmax(), "benchmark"] if "elapsed_s" in df.columns else "—")
k5.metric("🐍 Python",          df["python"].iloc[0] if "python" in df.columns else "—")

st.divider()

tab1, tab2, tab3 = st.tabs(["📊 Timing Charts","🔄 Throughput","📋 Raw Data"])

with tab1:
    st.subheader("Per-Module Elapsed Time")
    if "elapsed_s" in df.columns and "benchmark" in df.columns:
        sorted_df = df.sort_values("elapsed_s", ascending=False)
        fig_bar = px.bar(
            sorted_df, x="benchmark", y="elapsed_s",
            color="elapsed_s", color_continuous_scale="RdYlGn_r",
            title="Elapsed Time by Benchmark Module (sorted worst → best)",
            template="plotly",
            labels={"elapsed_s":"Elapsed (s)","benchmark":"Module"},
            text=sorted_df["elapsed_s"].apply(lambda v: f"{v:.3f}s"),
        )
        fig_bar.update_traces(textposition="outside")
        fig_bar.update_layout(height=480,
                               coloraxis_showscale=False, xaxis_tickangle=-45,
                               uniformtext_minsize=8)
        st.plotly_chart(fig_bar, use_container_width=True)

        # Treemap view of time consumption
        fig_tree = px.treemap(
            sorted_df, path=["benchmark"], values="elapsed_s",
            color="elapsed_s", color_continuous_scale="RdYlGn_r",
            title="Time Allocation Treemap (area = proportion of total time)",
            template="plotly",
        )
        fig_tree.update_layout(paper_bgcolor="#0f172a", height=400)
        st.plotly_chart(fig_tree, use_container_width=True)

with tab2:
    st.subheader("Throughput (rows/s)")
    thru = df.dropna(subset=["rows_per_s"]) if "rows_per_s" in df.columns else pd.DataFrame()
    if not thru.empty:
        thru_sorted = thru.sort_values("rows_per_s", ascending=True)
        fig_thru = px.bar(
            thru_sorted, x="rows_per_s", y="benchmark",
            orientation="h",
            color="rows_per_s", color_continuous_scale="RdYlGn",
            title="Module Throughput: Rows per Second (higher = faster)",
            template="plotly",
            labels={"rows_per_s":"Rows/s","benchmark":"Module"},
        )
        fig_thru.update_layout(height=max(350, len(thru_sorted)*22),
                                coloraxis_showscale=False)
        st.plotly_chart(fig_thru, use_container_width=True)

        # Scatter: time vs throughput to find inefficient modules
        if "elapsed_s" in df.columns:
            base = df.drop(columns=[c for c in ("rows_per_s",) if c in df.columns], errors="ignore")
            merged = base.merge(thru[["benchmark","rows_per_s"]], on="benchmark", how="left")
            fig_sc = px.scatter(
                merged, x="elapsed_s", y="rows_per_s", text="benchmark",
                title="Efficiency Map: Time vs Throughput (bottom-right = fast & slow = bad)",
                template="plotly",
                labels={"elapsed_s":"Elapsed (s)","rows_per_s":"Rows/s"},
                color="elapsed_s", color_continuous_scale="RdYlGn_r",
            )
            fig_sc.update_traces(textposition="top center", marker_size=12)
            fig_sc.update_layout(paper_bgcolor="#0f172a",
                                  height=420, coloraxis_showscale=False)
            st.plotly_chart(fig_sc, use_container_width=True)
    else:
        st.info("No `rows_per_s` column in benchmark results.")

with tab3:
    st.subheader("📋 Full Benchmark Results")
    st.dataframe(
        df.sort_values("elapsed_s", ascending=False),
        use_container_width=True, height=600, hide_index=True,
        column_config={
            "elapsed_s":    st.column_config.ProgressColumn("Elapsed (s)", min_value=0, max_value=float(df["elapsed_s"].max()) if "elapsed_s" in df.columns else 10),
            "rows_per_s":   st.column_config.NumberColumn("Rows/s", format="%,.0f"),
        }
    )
    if st.button("⬇ Export CSV"):
        st.download_button("Download", df.to_csv(index=False).encode("utf-8"),
                           "benchmarks.csv", "text/csv")

st.caption(f"Latest run timestamp (UTC): **{df['ts'].iloc[0]}**  •  Platform: `{df['platform'].iloc[0]}`")

with st.expander("All historical runs"):
    runs = sorted([p.name for p in results_dir.glob("bench-*.csv")], reverse=True)
    pick = st.selectbox("Run", runs)
    if pick:
        st.dataframe(pd.read_csv(results_dir / pick), use_container_width=True, height=400)
