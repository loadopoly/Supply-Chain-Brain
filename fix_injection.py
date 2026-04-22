import glob, re

target_files = [
    'pipeline/pages/1b_Supply_Chain_Pipeline.py',
    'pipeline/pages/2_EOQ_Deviation.py',
    'pipeline/pages/4_Procurement_360.py'
]

for f in target_files:
    with open(f, 'r', encoding='utf-8') as file: content = file.read()
    
    # Strip previous try/except blocks
    content = re.sub(r'\s+try:\n\s+render_dynamic_brain_insight\(.*?\n\s+except NameError:\n\s+pass.*?\n', '\n', content, flags=re.DOTALL)
    
    # Re-inject the simpler version
    def replacer(match):
        header_text = match.group(1).replace('"', '').replace("'", "")
        clean_title = re.sub(r'[^\w\s-]', '', header_text).strip()
        call = f"""st.subheader({match.group(1)})
    ctx = {{k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}}
    render_dynamic_brain_insight("{clean_title}", ctx)"""
        return call
        
    content = re.sub(r'st\.subheader\((.*?)\)', replacer, content)
    
    with open(f, 'w', encoding='utf-8') as file: file.write(content)
print("Complete!")