import glob, re

target_files = [
    'pipeline/pages/2_EOQ_Deviation.py',
    'pipeline/pages/4_Procurement_360.py'
]

for f in target_files:
    with open(f, 'r', encoding='utf-8') as file: content = file.read()
    
    # Strip previous ctx = / render_dynamic_brain_insight combinations
    content = re.sub(r'(\s+)ctx = \{k: v for k.*?render_dynamic_brain_insight\(.*?\)\n', '', content, flags=re.DOTALL)
    
    # Re-inject the properly indented version
    def replacer(match):
        indent = match.group(1)
        subheader_args = match.group(2)
        
        header_text = subheader_args.replace('"', '').replace("'", "")
        clean_title = re.sub(r'[^\w\s-]', '', header_text).strip()
        
        # Output exactly the same indent for each line
        out = f"{indent}st.subheader({subheader_args})\n"
        out += f"{indent}ctx = {{k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}}\n"
        out += f"{indent}render_dynamic_brain_insight(\"{clean_title}\", ctx)"
        return out
        
    content = re.sub(r'^([ \t]+)st\.subheader\((.*?)\)', replacer, content, flags=re.MULTILINE)
    
    with open(f, 'w', encoding='utf-8') as file: file.write(content)
print("Complete Recovery!")
