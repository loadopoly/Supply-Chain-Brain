import glob, re

target_files = [
    'pipeline/pages/1b_Supply_Chain_Pipeline.py',
    'pipeline/pages/2_EOQ_Deviation.py',
    'pipeline/pages/4_Procurement_360.py'
]

import_statement = "from src.brain.dynamic_insight import render_dynamic_brain_insight\n"

for f in target_files:
    with open(f, 'r', encoding='utf-8') as file:
        content = file.read()
    
    if "dynamic_insight" not in content:
        # insert import below global_filters
        content = re.sub(r'from src\.brain\.global_filters.*?\n', r'\g<0>' + import_statement, content, flags=re.MULTILINE)
    
    # regex to find st.subheader("...") and add insight below it
    def replacer(match):
        header_text = match.group(1).replace('"', '').replace("'", "")
        # Remove emojis
        clean_title = re.sub(r'[^\w\s-]', '', header_text).strip()
        # Add the insight call
        call = f"""st.subheader({match.group(1)})
    render_dynamic_brain_insight("{clean_title}", dict(st.session_state))"""
        return call
        
    content = re.sub(r'st\.subheader\((.*?)\)', replacer, content)
    
    with open(f, 'w', encoding='utf-8') as file:
        file.write(content)
    print(f'Injected insights into {f}')

