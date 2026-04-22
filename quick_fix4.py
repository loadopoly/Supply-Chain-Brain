import re

def fix_merged_lines(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()

    # Find ctx) followed by spaces and a letter, replace with ctx) \nspaces
    content = re.sub(
        r'(render_dynamic_brain_insight\(".*?",\s*ctx\))(\s+[a-zA-Z_])',
        r'\1\n\2',
        content
    )

    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)

fix_merged_lines('pipeline/pages/4_Procurement_360.py')
fix_merged_lines('pipeline/pages/2_EOQ_Deviation.py')

print("Applied Regex generic fixes.")
