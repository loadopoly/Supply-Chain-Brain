import glob, re

target_files = [
    'pipeline/pages/1b_Supply_Chain_Pipeline.py'
]

for f in target_files:
    with open(f, 'r', encoding='utf-8') as file: content = file.read()
    
    if "render_dynamic_brain_insight" not in content:
        content = "from src.brain.dynamic_insight import render_dynamic_brain_insight\n" + content
    
    # regex to find st.title("...")
    def replacer(match):
        header_text = match.group(1).replace('"', '').replace("'", "")
        clean_title = re.sub(r'[^\w\s-]', '', header_text).strip()
        call = f"""st.title({match.group(1)})
ctx = {{k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}}
render_dynamic_brain_insight("{clean_title}", ctx)"""
        return call
        
    content = re.sub(r'st\.title\((.*?)\)', replacer, content)
    
    with open(f, 'w', encoding='utf-8') as file: file.write(content)
print("Complete Pipeline!")
