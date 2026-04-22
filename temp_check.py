"""Quick check on dates"""
import pandas as pd
from datetime import date
df = pd.read_excel("pipeline/docs/OTD file.xlsx", sheet_name="Export")

def _coerce_date_series(series: pd.Series) -> pd.Series:
    s = series.copy()
    if pd.api.types.is_numeric_dtype(s):
        txt = s.fillna(0).astype(int).astype(str).str.strip()
        if txt.str.fullmatch(r"\d{8}").mean() > 0.6:
            return pd.to_datetime(txt, format="%Y%m%d", errors="coerce")
    return pd.to_datetime(s, errors="coerce")

s = date(2025, 4, 22)
e = date(2026, 4, 22)

mask = pd.Series(False, index=df.index)
for col in ["Ship Date", "Promised Date", "Adjusted Promise Date"]:
    if col in df.columns:
        dt = _coerce_date_series(df[col]).dt.date
        mask |= (dt >= s) & (dt <= e)

print(f"Total lines: {len(df)}")
print(f"Filtered lines: {mask.sum()}")
