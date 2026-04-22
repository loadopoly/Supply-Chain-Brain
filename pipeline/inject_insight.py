import os
import glob
import re

import_stmt = "from src.brain.dynamic_insight import render_dynamic_brain_insight\n"

for path in glob.glob("pipeline/pages/*.py"):
    filename = os.path.basename(path)
    
    # Don't inject into pages that already have it (from my manual checks earlier like EOQ / 360, but wait I can skip if 'render_dynamic_brain_insight' is already in there)
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    lines = content.split('\n')
    
    if "render_dynamic_brain_insight" in content:
        print(f"Skipping {filename}, already has dynamic insight.")
        # But wait, did I remove the static st.info?
        # Let's remove any st.info("🧠 Brain reading" or similar if they exist.
        new_lines = []
        skip_mode = False
        for line in lines:
            if 'st.info(' in line and 'Brain reading:' in line:
                skip_mode = True
                continue
            if skip_mode:
                if ')' in line:
                    skip_mode = False
                continue
            new_lines.append(line)
        content = '\n'.join(new_lines)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        continue

    # 1. Add import after 'import streamlit as st'
    new_lines = []
    imported = False
    for line in lines:
        new_lines.append(line)
        if "import streamlit as st" in line and not imported:
            new_lines.append(import_stmt.strip())
            imported = True
            
    # If we couldn't find import streamlit as st, just add at top
    if not imported:
        new_lines.insert(0, import_stmt.strip())

    # 2. Find KPI Strip comment and inject.
    final_lines = []
    injected = False
    
    skip_mode = False
    for line in new_lines:
        # Remove old static "Brain reading"
        if 'st.info(' in line and 'Brain reading:' in line:
            skip_mode = True
            continue
        if skip_mode:
            if ')' in line:
                skip_mode = False
            continue
            
        if not injected and re.search(r'#.*KPI\s+strip', line, re.IGNORECASE):
            # Inject insight
            indent = len(line) - len(line.lstrip())
            space = " " * indent
            
            # format name
            name_clean = filename.replace(".py", "").split("_", 1)[-1].replace("_", " ")
            
            final_lines.append(f"{space}ctx = {{k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}}")
            final_lines.append(f"{space}render_dynamic_brain_insight('{name_clean}', ctx)")
            final_lines.append("")
            final_lines.append(line)
            injected = True
        else:
            final_lines.append(line)
            
    if not injected:
        print(f"Warning: Could not find KPI strip in {filename}")
        
    with open(path, "w", encoding="utf-8") as f:
        f.write('\n'.join(final_lines))

print("Done injecting.")
