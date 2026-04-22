import os

os.chdir(r"C:\Users\agard\OneDrive - astecindustries.com\VS Code\pipeline")

with open('pages/2_EOQ_Deviation.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_load = '''@st.cache_data(ttl=600, show_spinner="Pulling demand / inventory / cost from Azure SQL …")
def _load(sql: str):
    from src.brain.demo_data import auto_load
    return auto_load(sql=sql, connector="azure_sql", timeout_s=120)

result = _load(sql_to_run)'''

new_load = '''@st.cache_data(ttl=600, show_spinner="Pulling demand / inventory / cost from Azure SQL …")
def _load(sql: str, site: str):
    from src.brain.demo_data import auto_load
    if site:
        sql = f"SELECT * FROM ({sql}) AS subq WHERE business_unit_key = '{site}' OR business_unit_id = '{site}'"
    return auto_load(sql=sql, connector="azure_sql", timeout_s=120)

site = st.session_state.get("g_site", "")
result = _load(sql_to_run, site)'''

text = text.replace('show_spinner="Pulling demand / inventory / cost from Azure SQL â€¦"', 'show_spinner="Pulling demand / inventory / cost from Azure SQL …"')
text = text.replace(old_load, new_load)

with open('pages/2_EOQ_Deviation.py', 'w', encoding='utf-8') as f:
    f.write(text)

