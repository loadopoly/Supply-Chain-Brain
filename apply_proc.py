import os

os.chdir(r"C:\Users\agard\OneDrive - astecindustries.com\VS Code\pipeline")

with open('pages/4_Procurement_360.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_load = '''def _load():
    parts    = fetch_logical("azure_sql", "parts",            top=5000)
    recv     = fetch_logical("azure_sql", "po_receipts",      top=10000)
    on_hand  = fetch_logical("azure_sql", "on_hand",          top=10000)
    cost     = fetch_logical("azure_sql", "part_cost",        top=10000)
    contract = fetch_logical("azure_sql", "po_contract_part", top=5000)
    suppliers = fetch_logical("azure_sql", "suppliers",       top=20000)'''

new_load = '''def _load(site: str):
    w_parts = f"business_unit_id = '{site}'" if site else None
    w_other = f"business_unit_key = '{site}'" if site else None

    parts     = fetch_logical("azure_sql", "parts",            top=5000,  where=w_parts)
    recv      = fetch_logical("azure_sql", "po_receipts",      top=10000, where=w_other)
    on_hand   = fetch_logical("azure_sql", "on_hand",          top=10000, where=w_other)
    cost      = fetch_logical("azure_sql", "part_cost",        top=10000, where=None)   # global or w_other?
    contract  = fetch_logical("azure_sql", "po_contract_part", top=5000,  where=w_other)
    suppliers = fetch_logical("azure_sql", "suppliers",       top=20000, where=None)'''

text = text.replace(old_load, new_load)
text = text.replace('parts, recv, on_hand, cost, contract, suppliers = _load()', 'site = st.session_state.get("g_site", "")\nparts, recv, on_hand, cost, contract, suppliers = _load(site)')

with open('pages/4_Procurement_360.py', 'w', encoding='utf-8') as f:
    f.write(text)

