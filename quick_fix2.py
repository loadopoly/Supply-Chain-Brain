import re

with open('pipeline/pages/4_Procurement_360.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('st.subheader("📦 Days of Inventory Outstanding (DIO)\n', 'st.subheader("📦 Days of Inventory Outstanding (DIO)")\n')
content = content.replace('st.subheader("ðŸ“¦ Days of Inventory Outstanding (DIO)\n', 'st.subheader("📦 Days of Inventory Outstanding (DIO)")\n')

with open('pipeline/pages/4_Procurement_360.py', 'w', encoding='utf-8') as f:
    f.write(content)

with open('pipeline/pages/2_EOQ_Deviation.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('", ctx)    st.caption("', '", ctx)\n    st.caption("')

with open('pipeline/pages/2_EOQ_Deviation.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Applied quick fixes.")
