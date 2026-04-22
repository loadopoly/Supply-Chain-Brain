import os

os.chdir(r"C:\Users\agard\OneDrive - astecindustries.com\VS Code\pipeline")

with open('pages/3_OTD_Recursive.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_run = '''def _run(lim: int, wh: str):
    return run_otd_from_replica(where=wh or None, limit=int(lim))

try:
    df, summaries = _run(
        int(st.session_state.get("otd_limit", 5000)),
        st.session_state.get("otd_where", ""),
    )'''

new_run = '''def _run(lim: int, wh: str, site: str):
    w_final = wh if wh else "1=1"
    if site:
        w_final += f" AND business_unit_key = '{site}'"

    return run_otd_from_replica(where=w_final, limit=int(lim))

try:
    df, summaries = _run(
        int(st.session_state.get("otd_limit", 5000)),
        st.session_state.get("otd_where", ""),
        st.session_state.get("g_site", ""),
    )'''

text = text.replace(old_run, new_run)

with open('pages/3_OTD_Recursive.py', 'w', encoding='utf-8') as f:
    f.write(text)

