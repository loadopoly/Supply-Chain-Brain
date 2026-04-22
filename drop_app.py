import os

os.chdir(r"C:\Users\agard\OneDrive - astecindustries.com\VS Code\pipeline")

with open('app.py', 'r', encoding='utf-8') as f:
    text = f.read()

site_func = '''from src.brain.data_access import query_df
@st.cache_data(ttl=3600)
def _get_sites_global():
    try:
        df = query_df("azure_sql", "SELECT DISTINCT business_unit_id FROM edap_dw_replica.dim_part WITH (NOLOCK) WHERE business_unit_id IS NOT NULL")
        if not df.empty:
            return [""] + sorted(df["business_unit_id"].astype(str).tolist())
    except Exception:
        pass
    return [""]
'''

if "_get_sites_global" not in text:
    text = text.replace("with st.sidebar:", site_func + "\nwith st.sidebar:\n    global_site = st.selectbox('Global Mfg Site (business_unit)', _get_sites_global(), index=0, key='g_site_global')\n    st.session_state['g_site'] = global_site\n")

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(text)

