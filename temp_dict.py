import pandas as pd
from pathlib import Path

def test_load():
    path = Path("pipeline/docs/OTD file.xlsx")
    xf = pd.ExcelFile(path)
    for sn in ['Opened Yesterday', 'Shipping today', 'Missed Yesterday']:
        d = xf.parse(sn).dropna(how="all").dropna(subset=['Order Date'])
        print(sn, len(d))
test_load()
