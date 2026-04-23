"""Page 15 - Cycle Count Accuracy and Completion."""
from pathlib import Path
import sys
import streamlit as st
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.db_registry import bootstrap_default_connectors, read_sql
from src.brain.dynamic_insight import render_dynamic_brain_insight

st.session_state["_page"] = "cycle_count_accuracy"
bootstrap_default_connectors()

st.markdown("## 📊 Cycle Count Accuracy & Completion")
st.caption("Identify unclassified (D code) parts with active stock and manage Cycle Count Dashboard artifacts.")

# Early DBI card — renders before SQL queries so Playwright finds it quickly.
# Use only scalar session-state values to avoid slow str() on large objects
# (DataFrames, bytes) accumulated from previous pages in a long test session.
_early_cc_ctx = {
    k: v for k, v in st.session_state.items()
    if not str(k).startswith('_') and not callable(v)
    and isinstance(v, (str, int, float, bool, type(None)))
}
render_dynamic_brain_insight("Cycle Count Accuracy", _early_cc_ctx)

tab_d_code, tab_pb = st.tabs(["📄 D Code Candidates Export", "📈 Power BI Dashboard Guide"])

with tab_d_code:
    st.subheader("D Code Candidates")
    st.markdown("These are items currently missing an ABC classification but holding physical inventory (`quantity_on_hand > 0`). Under new logic, these fall back to **D Code**.")
    
    sql = """
    WITH inv_latest AS (
        SELECT *,
               ROW_NUMBER() OVER(PARTITION BY part_key ORDER BY aud_update_datetime DESC) AS rn
        FROM [edap_dw_replica].[fact_inventory_on_hand]
    )
    SELECT 
        dp.part_number AS [Part Number],
        dp.part_description AS [Part Description],
        COALESCE(dpc.inventory_part_code, dpc.sales_part_code) AS [Current ABC Code],
        'D' AS [New ABC Code: "D"]
    FROM [edap_dw_replica].[dim_part] dp
    LEFT JOIN [edap_dw_replica].[dim_part_code] dpc 
        ON dp.part_key = dpc.part_key AND dpc.current_record_ind = 1
    JOIN inv_latest inv 
        ON dp.part_key = inv.part_key AND inv.rn = 1
    WHERE COALESCE(dpc.inventory_part_code, dpc.sales_part_code) IS NULL
      AND COALESCE(inv.quantity_on_hand, 0.0) > 0.0
    """
    
    with st.spinner("Querying D Code candidates from replica..."):
        df = read_sql("azure_sql", sql)
        
    if df is not None and not df.empty and not df.attrs.get("_error"):
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button(
            label="⬇️ Export Data as CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="D_Code_Candidates.csv",
            mime="text/csv",
            type="primary"
        )
    elif df is not None and df.attrs.get("_error"):
        st.error(f"Failed to load data: {df.attrs.get('_error')}")
    else:
        st.info("No D Code candidates found. All stocked items currently have an ABC classification.")

with tab_pb:
    st.subheader("Cycle Count Dashboard Natively Replicated")
    st.markdown("""
    Instead of relying on the external Power BI template (and manual CSVs), the DAX and Power Query logic has been integrated natively into the data pipeline using the discovered schemas.
    The counts are pulled from live transaction tables in Azure/Oracle directly, but can also run against an uploaded extract via Brain parsing.
    """)
    
    use_upload = st.checkbox("🧠 Process Custom File Upload Instead of Live Data")
    df_cc = None

    if use_upload:
        uploaded_file = st.file_uploader("Upload Raw Counts (Excel/CSV)", type=["csv", "xlsx"])
        if uploaded_file is not None:
            if uploaded_file.name.endswith(".csv"):
                df_upload = pd.read_csv(uploaded_file)
            else:
                df_upload = pd.read_excel(uploaded_file)
            
            st.info("Parsing via Brain logic and compiling compliance metrics...")
            try:
                import src.brain.cycle_count as cc_module
                df_cc = cc_module.process_uploaded_cycle_counts(df_upload, current_year=pd.Timestamp.today().year)
            except Exception as e:
                st.error(f"Error parsing upload: {e}")
    else:    
        st.info("Computing live Q1-Q4 counts & compliance against standard bounds...")
        try:
            import src.brain.cycle_count as cc_module
            
            with st.spinner("Executing DAX measures natively..."):
                from src.brain.db_registry import get
                conn = get("azure_sql").handle()
                df_cc = cc_module.fetch_and_calculate_cycle_counts(conn, current_year=2026)
                
        except Exception as e:
            import traceback
            st.error(f"Error running pipeline logic: {e}")
            with st.expander("Traceback"):
                st.code(traceback.format_exc())

    if df_cc is not None and not df_cc.empty:
        # Recreate YTD logic views
        st.markdown("### YTD Pass Rate by ABC")
        pass_rates = df_cc.groupby("ABC")["Pass_YTD"].mean() * 100
        st.bar_chart(pass_rates)
        
        st.dataframe(df_cc, use_container_width=True, hide_index=True)
        st.download_button(
            label="⬇️ Export Aggregated Cycle Counts (CSV)",
            data=df_cc.to_csv(index=False).encode("utf-8"),
            file_name="Cycle_Count_Compliance.csv",
            mime="text/csv"
        )
    elif df_cc is not None and df_cc.empty:
        st.warning("No cycle count transactions found for the current logic.")
