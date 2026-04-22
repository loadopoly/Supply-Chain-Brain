# Cycle Count Analytics — Power BI Build Guide
**IPS Supply Chain · Astec Industries**  
For: Power BI Development Team  
Author: Adam Faircloth  
Estimated build time: ~2.5 hours

---

## Overview

This report replicates the Cycle Count Analytics dashboard across three ERP systems:

| ERP | Site | Data source | Schema |
|-----|------|-------------|--------|
| Epicor | Jerome | `2026_Cycle_Count_Master.xlsx` → Completion_Table CSV | Part-level, Q1–Q4 counts |
| Oracle Fusion | Parsons | `Oracle_Count_Completion_rev15.xlsx` → PASTE_RawCounts CSV | Transaction-level, aggregated by Item |
| Syteline | Jerome (SSRS) | `CC_SSRS_CycleCount_Analytics.sql` DS_VarianceSummary | Summary-level, one row per ABC class |

**Business logic** (same across all sources):
- Class A: must be counted once in **each individual quarter** to pass
- Class B: must be counted at least once in H1 (Q1+Q2) **and** H2 (Q3+Q4) to pass YTD
- Class C/D: must be counted at least once **any time in the year** to pass YTD
- `ForcedFail = Y` overrides ALL pass logic → always FAIL

---

## Package contents

| File | Purpose |
|------|---------|
| `CycleCount_PowerQuery.m` | Power Query M — paste into Advanced Editor |
| `CycleCount_DAX_Measures.dax` | DAX calculated columns + measures — paste in DAX editor |
| `CycleCount_Theme_Astec.json` | Power BI theme — import via View → Themes |
| This build guide | Step-by-step reference |

---

## Step 1 — Create the file in Power BI Desktop

1. Open Power BI Desktop
2. Start with a blank report
3. **View → Themes → Browse for themes** → select `CycleCount_Theme_Astec.json`
4. Save as `CycleCount_Analytics.pbix`

---

## Step 2 — Power Query: load and shape the data

### 2a. Set up the three source queries

For each ERP source you're using:

1. **Home → Transform data → Transform data** (opens Power Query Editor)
2. **Home → New Source → Blank Query**
3. **Home → Advanced Editor**
4. Paste the corresponding query from `CycleCount_PowerQuery.m`
5. Rename the query (right-click in the Queries pane):
   - `EpicorSource`
   - `OracleSource`
   - `SytelineSource`
6. **Right-click each source query → Enable Load = OFF** (they're staging queries)

> **Data source paths:** Edit the file path in each query to point to your actual share location.  
> Network share example: `\\server\IPS_Supply_Chain\exports\epicor_completion.csv`  
> SharePoint example: uncomment the Web.Contents option in each query.

### 2b. Create the FactCounts table

1. **New Source → Blank Query → Advanced Editor**
2. Paste the `FactCounts` query from the M file
3. Rename to `FactCounts`
4. **Enable Load = ON**
5. Click **Close & Apply**

### 2c. Verify the data loaded

- Check row counts in the **Data** view (left panel)
- Expected: all source rows combined into one table
- Common issues:
  - File not found → fix path in Advanced Editor
  - Wrong column names → check export format matches the CSV spec in `Deployment_README.md`

---

## Step 3 — Create helper tables

These are small disconnected tables that drive slicers.

### DimPeriod (disconnected slicer — critical)

1. **Home → Enter Data**
2. Create a two-column table:

| Period | SortOrder |
|--------|-----------|
| Q1 | 1 |
| Q2 | 2 |
| H1 | 3 |
| Q3 | 4 |
| Q4 | 5 |
| H2 | 6 |
| YTD | 7 |

3. Name it `DimPeriod` → Load
4. **DO NOT create a relationship** between DimPeriod and FactCounts
5. In Data view → `DimPeriod[Period]` column → **Sort by Column → SortOrder**

### DimABC (optional — cleaner axis labels)

From the DAX file, Section 3 — paste the `DimABC` DATATABLE expression:  
**Modeling → New Table** → paste the DAX

**Create relationship:** DimABC[ABC] → FactCounts[ABC] (many-to-one, cross-filter: Single)

### DimSource (optional — disconnected slicer)

Same process as DimPeriod. Use the `DimSource` DATATABLE from the DAX file.

---

## Step 4 — Add calculated columns to FactCounts

For each column in **Section 1** of `CycleCount_DAX_Measures.dax`:

1. **Data view → select FactCounts table**
2. **Table tools → New Column**
3. Paste the DAX formula, rename to the column name in the comment
4. Repeat for all 10 calculated columns

**Order matters** — add them in this order so dependencies resolve:
1. `Required_Counts`
2. `Q1_Pass`
3. `Q2_Pass`
4. `Q3_Pass`
5. `Q4_Pass`
6. `H1_Pass`
7. `H2_Pass`
8. `YTD_Pass`
9. `Has_Variance`
10. `Frequency_Label`
11. `ABC_Sort`

---

## Step 5 — Create measures

1. **Home → Enter Data** → create a 1-row blank table named `Measures`
2. Delete the blank column Power BI adds
3. **Table tools → New Measure** — paste each measure from **Section 2** of the DAX file

Alternatively: right-click `FactCounts` → **New Measure** (measures can live on any table — using a dedicated Measures table keeps the model clean).

After creating all measures, select each one and set **Home Table = Measures** in the Properties pane.

---

## Step 6 — Data model relationships

Verify these relationships in **Model view**:

| From | To | Cardinality | Cross-filter |
|------|----|-------------|--------------|
| FactCounts[ABC] | DimABC[ABC] | Many-to-one | Single |
| FactCounts[Source] | (none — use slicer with SELECTEDVALUE in DAX) | — | — |
| DimPeriod | (none — disconnected) | — | — |

---

## Step 7 — Report pages

Apply the Astec theme first (Step 1). Each page uses a consistent layout:
- **Page background:** #f8f9fa (set in Page → Canvas background)
- **Accent color:** #E87722 (Astec orange) for borders/headers
- **Card visuals:** white background, light border, 8px corner radius

---

### PAGE 1: Executive Summary

**Canvas size:** 1280 × 720 (16:9)

```
┌─────────────────────────────────────────────────────────────────────┐
│  TITLE: Cycle Count Analytics                  [LOGO / BRAND BOX]  │
│  Subtitle: IPS Supply Chain · {Year} · {ERP Source}               │
├──────────────────────────────────────────────────────────────────────┤
│  SLICERS (horizontal): [Period] [ABC Class] [Warehouse] [Year]     │
├──────┬──────┬──────┬──────┬──────┬──────────────────────────────────┤
│ KPI1 │ KPI2 │ KPI3 │ KPI4 │ KPI5 │                                │
│Total │Compl │Class │Class │Class │                                │
│Parts │  %   │  A   │  B   │  C   │  ← 5 KPI Cards, equal width   │
│      │      │  %   │  %   │  %   │                                │
├──────┴──────┴──────┴──────┴──────┴──────────────────────────────────┤
│                          │                                          │
│  CLUSTERED BAR CHART     │   CLUSTERED BAR CHART                  │
│  Count Completion %      │   Value Accuracy %                     │
│  by ABC Class            │   by ABC Class                         │
│  (ABC on X, % on Y)      │   (green/amber/red conditional color)  │
│                          │                                          │
├──────────────────────────┴──────────────────────────────────────────┤
│  MATRIX: Count Completion Summary                                   │
│  Rows: DimABC[Label]                                               │
│  Cols: Parts | Obligations | Fulfilled | Remaining | Completion %  │
│  Totals row: on                                                     │
│  Completion % column: conditional formatting (green/amber/red)     │
└─────────────────────────────────────────────────────────────────────┘
```

**KPI Cards:**
- KPI1: `[Total Parts]` — label "Total Parts"
- KPI2: `[Completion %]` — label "Period Completion" — format 0.0% — goal line 95%
- KPI3: `CALCULATE([Completion %], FactCounts[ABC]="A")` — label "Class A"
- KPI4: `CALCULATE([Completion %], FactCounts[ABC]="B")` — label "Class B"
- KPI5: `CALCULATE([Completion %], FactCounts[ABC]="C")` — label "Class C"

**Completion bar chart:**
- X-axis: `DimABC[Label]`
- Y-axis: `[Completion %]` — format 0%
- Color: use `ABC Color` measure as data colors (requires conditional formatting setup)
- Reference line at 95% (constant line)

**Value Accuracy bar chart:**
- X-axis: `DimABC[Label]`
- Y-axis: `[Value Accuracy]` — format 0%
- Conditional color: ≥95% → #16a34a, ≥80% → #d97706, else #dc2626

**Matrix — Completion Summary:**
- Rows: `DimABC[Label]`
- Values: `[Total Parts]`, `[Period Obligations]`, `[Period Fulfilled]`, `[Period Remaining]`, `[Completion %]`
- `[Completion %]`: conditional background color — use the `Completion Color Hex` measure
- Subtotals: On (shows ALL row)

---

### PAGE 2: Quarterly Detail

```
┌─────────────────────────────────────────────────────────────────────┐
│  Header: Quarterly Obligation Status   [Year slicer] [WH slicer]  │
├──────────────────────────────────────────────────────────────────────┤
│  MATRIX: Quarterly Grid                                             │
│  Rows: DimABC[Label]                                               │
│  Columns (values):                                                  │
│    Q1 Completion %  |  Q2 Completion %  |  H1 Completion %         │
│    Q3 Completion %  |  Q4 Completion %  |  H2 Completion %         │
│    YTD Completion %                                                  │
│  → Conditional formatting on every value cell                      │
│  → Show "done / total" as tooltip                                  │
├──────────────────────────────┬──────────────────────────────────────┤
│  KPI: ForcedFail Count       │  KPI: Overcount Flag Count           │
│  (large number, red color)   │  (large number, amber color)        │
├──────────────────────────────┴──────────────────────────────────────┤
│  STACKED BAR CHART: Pass vs Fail by ABC Class                      │
│  X: DimABC[Label]                                                   │
│  Stack 1: Period Fulfilled (green)                                  │
│  Stack 2: Period Remaining (red/light)                             │
└─────────────────────────────────────────────────────────────────────┘
```

**Quarterly matrix — how to build:**
1. Insert Matrix visual
2. Rows: `DimABC[Label]`
3. Values: drag in Q1 through YTD completion % measures
4. **Format → Values → Conditional formatting → Background color** on each measure:
   - Rules: Value ≥ 0.95 → #dcfce7 (light green); ≥ 0.70 → #fef9c3 (light amber); else #fee2e2 (light red)
5. Add tooltip with the underlying done/total counts

---

### PAGE 3: Variance & Accuracy

```
┌─────────────────────────────────────────────────────────────────────┐
│  Header: Variance / Accuracy Analysis          [Period] [WH]       │
├──────────────────────────────────────────────────────────────────────┤
│  MATRIX: Variance by ABC                                            │
│  Rows: DimABC[Label]                                               │
│  Cols: Count Seqs | Seqs w/ Var | Var Rate % | Total Frozen $ |    │
│        Abs Var $ | Value Accuracy %                                 │
│  Conditional: Accuracy col → green/amber/red                       │
├─────────────────────────────────────────────────────────────────────┤
│  HORIZONTAL BAR CHART        │  SCATTER CHART                      │
│  Abs Var $ by ABC class      │  X: Total Frozen $                  │
│  (ABC on Y, $ on X)          │  Y: Abs Var $                       │
│                              │  Size: Total Parts                  │
│                              │  Color: ABC class                   │
└──────────────────────────────┴──────────────────────────────────────┘
```

**Variance matrix measures:**
- `[Total Parts]` → "Count Seqs"
- `[Seqs With Variance]` → "Seqs w/ Var"
- `[Variance Rate]` → "Var Rate" (format 0.0%)
- `[Total Frozen $]` → format $#,##0
- `[Total Abs Var $]` → format $#,##0
- `[Value Accuracy]` → format 0.0% — conditional background color

---

### PAGE 4: Part Detail (Drill-Through)

```
┌─────────────────────────────────────────────────────────────────────┐
│  Header: Part Detail  [back button]  [search box]  [ABC] [WH]     │
├──────────────────────────────────────────────────────────────────────┤
│  TABLE:                                                             │
│  Part | Desc | ABC | WH | Q1 | Q2 | Q3 | Q4 | Total | Req |       │
│  {Period} Pass | YTD Pass | Abs Var $ | Frozen $ | FF | OC        │
│                                                                     │
│  → Alternating row color                                           │
│  → Conditional formatting: Pass cols (PASS=green, FAIL=red)        │
│  → Sort by ABC, then Part                                          │
└─────────────────────────────────────────────────────────────────────┘
```

**Set up drill-through:**
1. In Page Information panel → **Allow use as drill-through target: On**
2. Drill-through fields: `FactCounts[ABC]`, `FactCounts[WH]`
3. Users can right-click any bar/matrix cell on other pages → **Drill through → Part Detail**

**Table columns:**
- `FactCounts[Part]`
- `FactCounts[Desc]`
- `FactCounts[ABC]` — conditional background color (blue/purple/cyan)
- `FactCounts[WH]`
- `FactCounts[Q1_Count]` through `FactCounts[Q4_Count]` — format as integer
- `FactCounts[Required_Counts]`
- Dynamic pass measure for selected period (use `[Period Fulfilled]` or add calculated column)
- `FactCounts[YTD_Pass]` — conditional format (1=green, 0=red)
- `FactCounts[Abs_Dollar_Var]` — conditional format (>0 = red)
- `FactCounts[ForcedFail]` — filter to show "Y" in red

---

### PAGE 5: Data Source & Refresh

```
┌─────────────────────────────────────────────────────────────────────┐
│  Header: Data Sources & Refresh Status                             │
├───────────────────────────────────────────────────────────────────────┤
│  KPI: Last Refresh      KPI: Epicor Rows    KPI: Oracle Rows       │
│  [MAX(RefreshedAt)]     [Epicor Row Count]  [Oracle Row Count]     │
│                                                                     │
│  BAR CHART: Row counts by Source                                   │
│                                                                     │
│  TEXTBOX: Data Source Details                                       │
│  "Epicor: \\server\...\epicor_completion.csv"                      │
│  "Oracle: \\server\...\oracle_rawcounts.csv"                       │
│  "Syteline: \\server\...\syteline_summary.csv"                     │
│  "SSRS SQL: CC_SSRS_CycleCount_Analytics.sql"                      │
│  "DBA contact: Justin Thomsen (Godlin Consulting)"                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Step 8 — Slicers setup

### Horizontal slicer bar (Pages 1–3)

Add these slicers in a row at the top of each page:

| Slicer | Field | Style | Default |
|--------|-------|-------|---------|
| Period | `DimPeriod[Period]` | Dropdown | Q1 |
| ABC Class | `DimABC[ABC]` | Dropdown | All |
| Warehouse | `FactCounts[WH]` | Dropdown | All |
| Year | `FactCounts[Year]` | Dropdown | 2026 |
| ERP Source | `DimSource[Source]` | Dropdown | All |

> **Period slicer behavior:** Because DimPeriod is disconnected, the measures use `SELECTEDVALUE(DimPeriod[Period])` to read the selection and apply the correct pass/fail window. No relationship needed — this is intentional.

### Sync slicers across pages

1. **View → Sync slicers**
2. Select each slicer → check all pages in the sync panel

---

## Step 9 — Conditional formatting rules

Apply these rules consistently across all matrix and table visuals:

### Completion % cells
Field-based rules on `[Completion %]`:
- ≥ 0.95 → Background: #dcfce7 (light green), Font: #166534
- ≥ 0.70 → Background: #fef9c3 (light amber), Font: #854d0e
- < 0.70 → Background: #fee2e2 (light red),   Font: #991b1b

### Pass/Fail columns (YTD_Pass, Q1_Pass etc.)
Field-based rules on the 0/1 integer value:
- 1 → Font: #16a34a (green)
- 0 → Font: #dc2626 (red)

Use **Format → Conditional formatting → Font color** and select the measure/column.

### ABC class color (bar charts)
Use **Format → Data colors → Default series color → Conditional formatting → Field value** and point to the `ABC Color` measure.

---

## Step 10 — Publish to Power BI Service

1. **Home → Publish**
2. Select your workspace (e.g., IPS Supply Chain)
3. After publishing:
   - **Settings → Scheduled refresh** → connect to gateway (required for network share CSVs)
   - Set refresh: Daily, 7:00 AM (after the PowerShell script runs at 6:00 AM)
   - Or use **OneDrive for Business** as data source (no gateway needed)

### Gateway requirement
Network share sources (`File.Contents("\\server\...")`) require an **On-premises data gateway** installed on a machine with access to that share. Contact IT to install and configure.

**Alternative to avoid gateway:** Export CSVs to SharePoint or OneDrive, use `Web.Contents()` in Power Query instead of `File.Contents()`. This allows scheduled refresh in the cloud with no gateway.

---

## Conditional formatting quick reference

| Element | Condition | Color |
|---------|-----------|-------|
| Completion % | ≥ 95% | #16a34a (green) |
| Completion % | 70–94% | #d97706 (amber) |
| Completion % | < 70% | #dc2626 (red) |
| Value Accuracy | ≥ 95% | #16a34a |
| Value Accuracy | 80–94% | #d97706 |
| Value Accuracy | < 80% | #dc2626 |
| ABC Class A | any | #3b82f6 (blue) |
| ABC Class B | any | #8b5cf6 (purple) |
| ABC Class C | any | #06b6d4 (cyan) |
| ABC Class D | any | #6b7280 (gray) |
| Pass | = 1 or "PASS" | #16a34a |
| Fail | = 0 or "FAIL" | #dc2626 |
| Abs Var $ | > 0 | #dc2626 |
| ForcedFail | = "Y" | #dc2626 |

---

## Questions & contacts

| Topic | Contact |
|-------|---------|
| Business logic / DAX questions | Adam Faircloth (IPS Supply Chain) |
| Syteline SQL / table schemas | Justin Thomsen (Godlin Consulting) |
| Oracle data structure | Adam Faircloth |
| Epicor Power Query columns | Adam Faircloth |
| Power BI gateway / service setup | IT / BI Team |
