import re
import glob

for f in ['pipeline/pages/2_EOQ_Deviation.py', 'pipeline/pages/4_Procurement_360.py']:
    with open(f, 'r', encoding='utf-8') as file: content = file.read()
    
    # Fix broken subheader lines
    def fix_line(m):
        raw = m.group(1)
        if not raw.endswith('")') and not raw.endswith("')") and not raw.endswith(")"):
            if raw.endswith('"'): raw += ')'
            else: raw += '")'
        return f"st.subheader({raw}"
        
    content = re.sub(r'st\.subheader\((.*)(?=\n)', fix_line, content)
    
    with open(f, 'w', encoding='utf-8') as file: file.write(content)
print("Fixed string literals!")
