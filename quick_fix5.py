import re

def fix_merged(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()

    content = content.replace('")st.caption("', '")\nst.caption("')

    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)

fix_merged('pipeline/pages/2_EOQ_Deviation.py')

print("Applied 3rd round of fixes.")
