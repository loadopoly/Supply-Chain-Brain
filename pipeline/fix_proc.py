import re

with open('pages/4_Procurement_360.py', 'r', encoding='utf-8') as f:
    text = f.read()

text = re.sub(
    r'w_other = f"business_unit_key = \'\{site\}\'\" if site else None',
    r'w_other = f"business_unit_key IN (SELECT business_unit_key FROM edap_dw_replica.dim_business_unit WITH (NOLOCK) WHERE business_unit_id = \'\{site\}\')\" if site else None',
    text
)

with open('pages/4_Procurement_360.py', 'w', encoding='utf-8') as f:
    f.write(text)

with open('pages/3_OTD_Recursive.py', 'r', encoding='utf-8') as f:
    text = f.read()

text = re.sub(
    r'business_unit_key = \'\{site\}\'',
    r'business_unit_key IN (SELECT business_unit_key FROM edap_dw_replica.dim_business_unit WITH (NOLOCK) WHERE business_unit_id = \'\{site\}\')',
    text
)

with open('pages/3_OTD_Recursive.py', 'w', encoding='utf-8') as f:
    f.write(text)

with open('pages/2_EOQ_Deviation.py', 'r', encoding='utf-8') as f:
    text = f.read()

text = re.sub(
    r'WHERE business_unit_key = \'\{site\}\' OR business_unit_id = \'\{site\}\'',
    r'WHERE business_unit_key IN (SELECT business_unit_key FROM edap_dw_replica.dim_business_unit WITH (NOLOCK) WHERE business_unit_id = \'\{site\}\') OR business_unit_id = \'\{site\}\'',
    text
)

with open('pages/2_EOQ_Deviation.py', 'w', encoding='utf-8') as f:
    f.write(text)
