import glob
import re
import os

missing = [
    'pipeline/pages/0_Query_Console.py',
    'pipeline/pages/0_Schema_Discovery.py',
    'pipeline/pages/12_What_If.py',
    'pipeline/pages/15_Cycle_Count_Accuracy.py',
    'pipeline/pages/15_Report_Creator.py',
    'pipeline/pages/16_Cycle_Count_Accuracy.py',
    'pipeline/pages/6_Connectors.py'
]

for f in missing:
    with open(f, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # We will inject after the first occurrence of st.caption or st.markdown("## ...")
    lines = content.split('\n')
    new_lines = []
    injected = False
    
    name_clean = os.path.basename(f).replace(".py", "").split("_", 1)[-1].replace("_", " ")

    for line in lines:
        new_lines.append(line)
        if not injected and (line.strip().startswith('st.caption(') or line.strip().startswith('st.markdown("## ')):
            # Found header, see if next line is also caption/markdown, wait we should just inject it safely.
            pass
            
    # Safer to just use regex to find st.caption and insert after it.
    
    match = re.search(r'st\.caption\([^)]*\)', content)
    if not match:
        match = re.search(r'st\.markdown\("##.*?"\)', content)
        
    if match:
        end_idx = match.end()
        # insert string
        inject_str = f"\n\nctx = {{k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}}\nrender_dynamic_brain_insight('{name_clean}', ctx)\nst.divider()\n"
        
        content = content[:end_idx] + inject_str + content[end_idx:]
        
        with open(f, 'w', encoding='utf-8') as file:
            file.write(content)
        print(f"Injected into {f}")
        
    else:
        print(f"Could not find injection point in {f}")

