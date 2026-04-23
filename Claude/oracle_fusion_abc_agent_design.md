# Oracle Fusion ABC Class Update Agent — Design Document
**Generated:** 2026-04-23  
**Context:** Burlington (3165_US_BUR_MFG) DEV13 — 18 items needing ABC class reassignment

---

## Core Goal

Update 18 Burlington inventory items in Oracle Fusion DEV13 from their current ABC cycle count classes (A, B, C) to new classes (D or P) by navigating the Inventory Management (Classic) Manage Cycle Counts wizard via Playwright browser automation.

---

## Action Tree

```
START
│
├─ 1. AUTHENTICATE
│     Load oracle_session.json → context.add_cookies()
│     Validate: page.title() should NOT contain "Sign In"
│
├─ 2. NAVIGATE TO MANAGE CYCLE COUNTS
│     │
│     ├─ 2a. Go to FuseWelcome
│     │       page.goto(HOST + "/fscmUI/faces/FuseWelcome")
│     │
│     ├─ 2b. Click "Supply Chain Execution" tab
│     │       Selector: a/div with exact text "Supply Chain Execution"
│     │       Constraint: y between 40–280 (nav bar height)
│     │       → wait 2500ms
│     │
│     ├─ 2c. Click "Show More" (if present, repeat until gone)
│     │
│     ├─ 2d. Click "Inventory Management (Classic)" tile
│     │       Must use page.mouse.click(cx, cy) — el.click() ignored by Oracle ADF
│     │       Visibility check: 6-level parent CSS traversal
│     │       → wait_for_load_state + 5000ms
│     │
│     ├─ 2e. Open Task Panel
│     │       Check: SELECT element at x > 900 is visible?
│     │         YES → already open, skip toggle
│     │         NO  → find toggle icon at right edge (x > 1000, y 150–600)
│     │               Try [title='Tasks'] selector first
│     │               Fallback: positional click on right-edge icons
│     │
│     ├─ 2f. Select "Counts" from task panel SELECT
│     │       page.mouse.click on SELECT → keyboard Home → ArrowDown N times → Enter
│     │       Verify selectedIndex changed to "Counts"
│     │
│     ├─ 2g. Click "Manage Cycle Counts" task link
│     │       Get coords via JS (no el.click!) → page.mouse.click(cx, cy)
│     │       Wait for: h1 containing "Manage Cycle Counts" OR input with cycle count hint
│     │
│     └─ 2h. Handle Org Dialog
│             Input centered at approx x:500–900, y:350–500
│             Type "3165_US_BUR_MFG" → Tab → click suggestion OR wait
│             Click OK button → wait 4000ms
│
├─ 3. DISCOVER CYCLE COUNTS
│     Scan <a> elements at x:10–400, y:200–800 in results table
│     Filter out nav labels: Help, Actions, View, Save, Cancel, Reset
│     Burlington counts: BUR 2025 ELEC CC, BUR 2025 WH II CC,
│                        STEEL 1ST QTR CC 2025 BUR, STEEL 2ND QTR CC 2025 BUR
│
├─ 4. FOR EACH CYCLE COUNT — PROBE ITEMS AT STEP 5
│     │
│     ├─ 4a. Open cycle count
│     │       Find <a> by exact text → page.mouse.click → wait 5000ms
│     │
│     ├─ 4b. Navigate to step 5 "Define Items in Item Categories"
│     │       Click "Next" button 4 times (each: mouse.click → wait 3000ms)
│     │       Verify h1 contains "Define Items"
│     │
│     ├─ 4c. Scroll through ALL category sections (A, B, C, D, P, ...)
│     │       For each scroll position:
│     │         Find Item column filter: input at x:200–350, y:400–500
│     │         Identify category section from parent h2/h3 heading
│     │         For each target item:
│     │           Click filter field → Ctrl+A → type item → Enter
│     │           Wait 2500ms → scan <a>,<td> at y:440–700 for exact match
│     │           Clear filter: Ctrl+A → Delete → Enter → wait 800ms
│     │         Scroll down 300px → repeat
│     │
│     └─ 4d. Cancel wizard (return to list)
│             Find button text="Cancel" → page.mouse.click
│             Wait for confirmation dialog (Yes/OK) → page.mouse.click  ← must use mouse.click
│             Wait until BUR cycle count links visible again
│
├─ 5. DETERMINE ACTION PLAN
│     Items going to D: remove from current class, add to D class
│     Items going to P: remove from current class, add to P class
│     If D/P class doesn't exist in the cycle count → create it first (step 5 wizard)
│
└─ 6. EXECUTE UPDATES (per item)
      Open cycle count → navigate to step 5 → find item in current class
      Move item: remove row (delete icon) → add to target class section
      Save wizard (step 7 or Finish button)
```

---

## Key Technical Rules (Oracle Fusion ADF)

| Rule | Detail |
|------|--------|
| **Never use `el.click()` in JS** | Oracle ADF ignores JavaScript synthetic clicks. Always get coords via JS then call `page.mouse.click(cx, cy)` |
| **`offsetParent` check is unreliable** | Use `getBoundingClientRect()` size check instead: `r.width > 0 && r.height > 0` |
| **Task panel is a `<select>`** | At x > 900 in the Redwood dashboard. Already-open check: does SELECT exist at x > 900? If yes, don't toggle (would close it). |
| **Org dialog has no title** | Input centered at x:500–900, y:350–500. No label text helps identify it. |
| **Wizard Cancel confirmation** | Must use `page.mouse.click()` for Yes/OK in dialog — `el.click()` is silently ignored |
| **Column filter = first input, x~286, y~440** | This is the Item column header filter in the ADF items table. Press `Enter` (not Tab, not clicking anything) to apply. |
| **LOV Add Item field = x~58, y~633–665** | This is for ADDING new items to a category. Do NOT use for searching existing items. |
| **Playwright key: `"Enter"` not `"Return"`** | `page.keyboard.press("Return")` throws `Unknown key` error |
| **ADF table pagination** | Category items tables may paginate; column filter triggers server-side refresh showing only matches |

---

## Page Layout at Step 5 (Define Items in Item Categories)

```
y=0    ┌─ Oracle Fusion nav bar ─────────────────────────────────┐
       │  Search (462,44) │ ... right-side icons (x>1400) ...    │
y=200  ├─ Wizard step nav: 1 2 3 4 [5] 6 7 ─────────────────────┤
       │                                                          │
y=220  │  Category A heading (h2/h3)                             │
       │  ┌─ Items table ──────────────────────────────────────┐ │
y=440  │  │ [Item filter(286)] [Desc(404)] [Freq(527)] ...     │ │  ← Column filter row
y=460  │  │ row: 502-06259-80   Active Items  4   Annual  ...  │ │
y=480  │  │ row: 644-13038-10   ...                            │ │
       │  └────────────────────────────────────────────────────┘ │
y=633  │  [Add Item LOV (58,633)] [Include in Schedule▼] [+]    │
       │                                                          │
       │  Category B heading  (scroll down to see)               │
       │  ┌─ Items table ─────────────────────────────────────┐  │
       │  │ [Item filter] ...  ← same x~286, different y       │  │
       │  └────────────────────────────────────────────────────┘  │
       │  [Add Item LOV]                                          │
       │                                                          │
       │  Category C heading  (scroll further)                    │
       │  ...                                                     │
y=900  └──────────────────────────────────────────────────────────┘
```

---

## Burlington Cycle Count Inventory

| Count Name | Classes | Type | Notes |
|------------|---------|------|-------|
| BUR 2025 WH II CC | A, B, C | By Item Category | General warehouse — likely contains most of the 18 items |
| BUR 2025 ELEC CC | A | By Item Category | Electrical items only |
| BUR TEST STEEL | P | By Item | Test count |
| STEEL 1ST QTR CC 2025 BUR | P, S, Y | By Item | P=Steel Pipe, S=Structural, Y=Plate |
| STEEL 2ND QTR CC 2025 BUR | P, S, Y | By Item | Same as above, 2nd quarter |
| X-* counts | P/S/Y variants | By Item | Inactive/archive (X- prefix) |

---

## Target Items (18 Burlington items from Burlington_Strat_Change_04212026.xlsx)

```python
BURLINGTON_UPDATES = {
    "02040RIP4-SPRAY": "D",   # old C — PAINT PRIMER SPRAY
    "114-15721-01":    "P",   # old B — PL 1-1/4" X 14" X 14"
    "212-00011-04":    "D",   # old C
    "22459YXF-SPRAY":  "D",   # old C — SPRAY
    "298-00114-93":    "D",   # old C
    "30001167":        "D",   # old C
    "30003263":        "D",   # old C
    "30004360":        "D",   # old C
    "398-02077-73":    "D",   # old C
    "398-11000-19":    "D",   # old C
    "398-11000-22":    "D",   # old C
    "398-14000-21":    "D",   # old C
    "398-20000-23":    "D",   # old C
    "398-20000-25":    "D",   # old C
    "398-20000-37":    "D",   # old C
    "398-20000-39":    "D",   # old C
    "399-20442-36":    "P",   # old C
    "60-66593-01":     "D",   # old C — ENCLOSURE FOR TC CMJB (ELEC subinv)
}
# Special case: 60-66593-01 is in ELEC subinventory → likely in BUR 2025 ELEC CC
# 114-15721-01 and 399-20442-36 → going to P class (possibly steel-related)
```

---

## Current Status (2026-04-23)

- Navigation to Manage Cycle Counts: **WORKING**
- Session auth via cookies: **WORKING**
- Cancel wizard + back to list: **WORKING** (uses mouse.click for confirmation)
- Step 5 column filter detection: **WORKING** (input at x~286, y~440)
- Item search across ALL categories (scroll): **IN PROGRESS** — currently only sees top category; scrolling fix deployed, not yet validated
- Items found in any cycle count: **0 of 18** — items not yet located (B/C categories likely below fold)
- D/P class creation: **NOT STARTED**
- Actual item reassignment: **NOT STARTED**

---

## Agent Design Principles for Oracle Fusion ADF Automation

1. **Coordinate-first**: All navigation clicks must use real browser coordinates. Get coords from JS, fire from Python.
2. **State detection before action**: Always check if a panel/dialog is already open before clicking its toggle.
3. **Scroll-aware search**: Oracle ADF tables paginate and stack vertically. Scroll through all sections before declaring "not found."
4. **Confirmation dialog awareness**: Every destructive/navigational action (Cancel, Delete, Save) may trigger a Yes/OK dialog that requires `page.mouse.click()`.
5. **Wait for AJAX**: After any filter/search action, wait 2500–3000ms for Oracle ADF's partial page refresh.
6. **Screenshot checkpoints**: Save screenshots at each major state transition for debugging and audit.
7. **Session persistence**: Use `oracle_session.json` cookie cache. Reload session without re-login when cookies are still valid.
