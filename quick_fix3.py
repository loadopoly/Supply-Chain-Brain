import re

with open('pipeline/pages/4_Procurement_360.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('", ctx)    if not recv.empty', '", ctx)\n    if not recv.empty')

with open('pipeline/pages/4_Procurement_360.py', 'w', encoding='utf-8') as f:
    f.write(content)

with open('pipeline/pages/2_EOQ_Deviation.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('", ctx)                d1,d2,d3,d4', '", ctx)\n                d1,d2,d3,d4')

with open('pipeline/pages/2_EOQ_Deviation.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Applied second round of quick fixes.")
