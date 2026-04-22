"""Page 5 — Forward-facing Data Quality / value-of-information interface with Plotly."""
from pathlib import Path
import sys
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.db_registry import bootstrap_default_connectors
from src.brain.data_access import fetch_logical, query_df
from src.brain.global_filters import date_key_window, get_global_window
from src.brain.imputation import (missingness_profile, value_of_information, mass_impute,
                                   xgb, lgb, cb, missingpy)

# set_page_config handled by app.py st.navigation()
st.session_state["_page"] = "data_quality"
bootstrap_default_connectors()

import plotly.express as px
import plotly.graph_objects as go

st.markdown("## 🧩 Data Quality · Value of Information")
st.caption("Surface what the model cannot know · rank cells by counterfactual KPI swing · MIT Digital SC Lab")

# ── Library availability ─────────────────────────────────────────────────────
lib_status = {
    "xgboost":   bool(xgb),
    "lightgbm":  bool(lgb),
    "catboost":  bool(cb),
    "missingpy": bool(missingpy),
}
available = [k for k,v in lib_status.items() if v]
missing_l = [k for k,v in lib_status.items() if not v]

with st.expander("🔬 ML Library Status", expanded=False):
    lib_cols = st.columns(4)
    for i,(lib,ok) in enumerate(lib_status.items()):
        lib_cols[i].metric(lib, "✅ Loaded" if ok else "❌ Missing",
                           delta_color="off" if ok else "off")
    if missing_l:
        st.caption(f"Install for full power: `pip install {' '.join(missing_l)}`")

# ── Source picker ────────────────────────────────────────────────────────────
LOGICAL_NAMES = ["parts","suppliers","on_hand","open_purchase","open_mfg",
                 "po_receipts","po_contract_part","sales_order_lines",
                 "ap_invoice_lines","part_cost"]

with st.expander("🔍 Data Source", expanded=False):
    src_kind = st.radio("Source", ["Logical table (brain.yaml)","Custom SQL"],
                         horizontal=True, key="dq_src_kind")
    if src_kind.startswith("Logical"):
        name = st.selectbox("Logical name", LOGICAL_NAMES, key="dq_logical")
        n    = st.number_input("Rows", 100, 50000, 2000, step=100, key="dq_rows")
        custom_sql = ""
    else:
        name, n = "", 0
        custom_sql = st.text_area("SQL", height=100, key="dq_sql",
                                   value="SELECT TOP 2000 * FROM [edap_dw_replica].[dim_part]")

default_name = st.session_state.get("dq_logical", "parts")
default_rows = int(st.session_state.get("dq_rows", 2000))
default_sql  = st.session_state.get("dq_sql", "")

@st.cache_data(ttl=600, show_spinner="Pulling rows from Azure SQL replica …")
def _load(kind: str, lname: str, rows: int, sql: str, site: str, start_k: int, end_k: int):
    if kind.startswith("Custom") and sql.strip():
        return query_df("azure_sql", sql.strip())
        
    # Logical table dynamic where
    wh = "1=1"
    if site:
        if lname == "parts":
            wh += f" AND business_unit_id = '{site}'"
        else:
            wh += f" AND business_unit_key IN (SELECT business_unit_key FROM edap_dw_replica.dim_business_unit WITH (NOLOCK) WHERE business_unit_id = '{site}')"
            
    # Basic date scopes for tables we know about in the dropdown
    if lname == "po_receipts":
        wh += f" AND receipt_date_key BETWEEN {start_k} AND {end_k}"
    elif lname == "sales_order_lines":
        wh += f" AND order_date_key BETWEEN {start_k} AND {end_k}"
    elif lname == "ap_invoice_lines":
        wh += f" AND invoice_date_key BETWEEN {start_k} AND {end_k}"
    elif lname == "on_hand":
        wh += f" AND snapshot_day_key BETWEEN {start_k} AND {end_k}"

    return fetch_logical("azure_sql", lname, top=rows, where=wh)

_sk, _ek = date_key_window()
df = _load(st.session_state.get("dq_src_kind","Logical table (brain.yaml)"),
           default_name, default_rows, default_sql,
           st.session_state.get("g_site", ""), _sk, _ek)

err = df.attrs.get("_error") if hasattr(df,"attrs") else None
if err:
    st.error(f"Live load failed: {err}")
    st.code(df.attrs.get("_sql",""), language="sql")
    st.stop()
if df.empty:
    st.warning("Live source returned 0 rows.")
    st.stop()

st.markdown(f"🟢 **Live** · {len(df):,} rows × {df.shape[1]} columns from `{default_name or 'custom SQL'}`")

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight('Data Quality', ctx)

# ── KPI Strip ─────────────────────────────────────────────────────────────────
total_cells = df.shape[0] * df.shape[1]
null_cells  = int(df.isnull().sum().sum())
pct_missing = null_cells / max(total_cells, 1) * 100

k1, k2, k3, k4 = st.columns(4)
k1.metric("📊 Rows",       f"{len(df):,}")
k2.metric("📋 Columns",    df.shape[1])
k3.metric("❓ Null Cells", f"{null_cells:,}")
k4.metric("🔴 Missing %",  f"{pct_missing:.1f}%")

st.divider()

tab1, tab2, tab3 = st.tabs(["🔍 Missingness Profile","💡 Value of Information","🔧 Mass Impute"])

with tab1:
    st.subheader("🔍 Missingness Profile")
    prof = missingness_profile(df)

    if not prof.empty:
        null_col = [c for c in prof.columns if "miss" in c.lower() or "null" in c.lower() or "pct" in c.lower()]
        col_col  = [c for c in prof.columns if "col" in c.lower() or "feat" in c.lower()]
        ych = null_col[0] if null_col else prof.columns[-1]
        xch = col_col[0] if col_col else prof.columns[0]

        sorted_p = prof.sort_values(ych, ascending=False)
        fig_miss = px.bar(sorted_p, x=xch, y=ych,
                           color=ych, color_continuous_scale="RdYlGn_r",
                           title="Missing Data by Column",
                           template="plotly",
                           labels={ych:"% Missing",xch:"Column"})
        fig_miss.add_hline(y=0.2, line_dash="dash", line_color="#eab308",
                            annotation_text="⚠️ 20% threshold")
        fig_miss.add_hline(y=0.5, line_dash="dash", line_color="#ef4444",
                            annotation_text="🔴 50% threshold")
        fig_miss.update_layout(height=400,
                                xaxis_tickangle=-45, coloraxis_showscale=False)
        st.plotly_chart(fig_miss, use_container_width=True)

        # Heatmap of missingness pattern
        miss_binary = df.isnull().astype(int)
        if len(miss_binary.columns) <= 50:
            fig_heat = px.imshow(miss_binary.head(100).T,
                                  color_continuous_scale=["#1e293b","#ef4444"],
                                  title="Missingness Pattern Heatmap (red = missing)",
                                  template="plotly",
                                  labels=dict(color="Missing"),
                                  aspect="auto")
            fig_heat.update_layout(paper_bgcolor="#0f172a", height=400)
            st.plotly_chart(fig_heat, use_container_width=True)

        st.dataframe(sorted_p, use_container_width=True, hide_index=True,
                     column_config={ych: st.column_config.ProgressColumn(
                         ych, min_value=0, max_value=1)})

with tab2:
    st.subheader("💡 Value of Information — Fill These Cells First")
    st.caption("GBT trained on downstream KPI · SHAP importances rank which missing cells unlock the most value")
    num_cols = df.select_dtypes(include=["number"]).columns.tolist()
    if not num_cols:
        st.warning("No numeric columns to use as a KPI target.")
    else:
        target = st.selectbox("🎯 KPI Target Column", num_cols)
        with st.spinner("Training GBT and ranking columns by VOI …"):
            voi = value_of_information(df, target_col=target)

        if not voi.empty:
            imp_col = [c for c in voi.columns if "import" in c.lower() or "voi" in c.lower() or "shap" in c.lower()]
            ych = imp_col[0] if imp_col else voi.columns[-1]
            xch = voi.columns[0]

            fig_voi = px.bar(voi.sort_values(ych, ascending=True),
                              x=ych, y=xch, orientation="h",
                              color=ych, color_continuous_scale="Viridis",
                              title=f"Feature Importance for `{target}` — Fill Highest First",
                              template="plotly",
                              labels={ych:"VOI / Importance",xch:"Column"})
            fig_voi.update_layout(height=max(300, len(voi)*22),
                                   coloraxis_showscale=False)
            st.plotly_chart(fig_voi, use_container_width=True)
            st.dataframe(voi, use_container_width=True, hide_index=True)

with tab3:
    st.subheader("🔧 Mass Imputation")
    st.caption("MissForest (Random Forest) or sklearn IterativeImputer for bulk missing-value fill")
    method_used = "MissForest" if missingpy else "IterativeImputer (sklearn)"
    st.info(f"Method: **{method_used}**")

    if st.button("▶️ Run Mass Imputation", type="primary"):
        with st.spinner("Imputing missing values …"):
            imputed = mass_impute(df)
        filled_cells = int(df.isnull().sum().sum()) - int(imputed.isnull().sum().sum())

        m1, m2, m3 = st.columns(3)
        m1.metric("📊 Rows",      f"{imputed.shape[0]:,}")
        m2.metric("📋 Columns",   imputed.shape[1])
        m3.metric("✅ Cells Filled", f"{filled_cells:,}")

        # Before/after comparison
        before_miss = df.isnull().sum() / len(df)
        after_miss  = imputed.isnull().sum() / len(imputed)
        comp_df = pd.DataFrame({"Before":before_miss,"After":after_miss}).reset_index()
        comp_df.columns = ["Column","Before","After"]
        comp_df = comp_df[comp_df["Before"] > 0]

        if not comp_df.empty:
            fig_comp = px.bar(comp_df, x="Column", y=["Before","After"], barmode="group",
                               title="Missing % Before vs After Imputation",
                               template="plotly",
                               color_discrete_map={"Before":"#ef4444","After":"#22c55e"},
                               labels={"value":"% Missing","variable":"Phase"})
            fig_comp.update_layout(height=380,
                                    xaxis_tickangle=-45)
            st.plotly_chart(fig_comp, use_container_width=True)

        st.dataframe(imputed.head(200), use_container_width=True, hide_index=True)
        st.download_button("⬇ Download imputed.csv",
                           imputed.to_csv(index=False).encode(),
                           file_name="imputed.csv", mime="text/csv")
    else:
        st.markdown("Click **Run Mass Imputation** to fill missing values across the dataset.")
