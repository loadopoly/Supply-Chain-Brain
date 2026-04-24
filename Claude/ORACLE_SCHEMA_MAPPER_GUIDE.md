# Oracle Fusion Schema Mapper — Technical Guide

**Created:** 2026-04-24  
**Status:** Active — Run 5 in progress  
**Scope:** Structural crawl of all Oracle Fusion DEV13 module task panels

---

## Overview

`oracle_schema_mapper.py` is a Playwright-based crawler that navigates every Oracle Fusion tab and tile, opens the task panel for each module, and extracts the full task list into a structured JSON schema. Output is used to build intersection maps for item-level write-op analysis.

**Output files:**
- `pipeline/oracle_schema_map.json` — full schema (incremental, survives restarts)
- `pipeline/oracle_schema_map.txt` — human-readable flat view
- `pipeline/pim_screenshots/80446-04/write_ops/intersection_map.json` — part-specific cross-reference
- `pipeline/pim_screenshots/80446-04/write_ops/intersection_map.txt` — human-readable intersection

---

## Current Schema Status (as of run 5, 2026-04-24)

| Tab | Module | Task Count | Status |
|-----|--------|-----------|--------|
| Order Management | Pricing Administration | 11 | Confirmed |
| Supply Chain Execution | Inventory Management (Classic) | 56 | Confirmed |
| Supply Chain Execution | Cost Accounting | 12 | Confirmed |
| Supply Chain Execution | Work Execution | 1* | Regression — run 5 fixing |
| Supply Chain Planning | Plan Inputs | 3* | Regression — run 5 fixing |
| Supply Chain Planning | Demand Management | 18 | Confirmed (Redwood) |
| Supply Chain Planning | Supply Planning | 26 | Confirmed |
| Supply Chain Planning | Replenishment Planning | 23 | Confirmed (Redwood) |
| Supply Chain Planning | Demand Priority Rules | 4 | Confirmed |
| Product Management | Browse Commercial Items | 14 | Confirmed |
| Product Management | Manufacturers (Classic) | 13 | Confirmed |
| Product Management | Product Development | 5 | Confirmed (Redwood) |
| Product Management | Product Information Management | 7 | Confirmed (Redwood) |
| General Accounting | Manage Journals | 5* | Saved-search capture issue |
| Procurement | Suppliers | 9 | Confirmed |
| Procurement | Purchase Orders | 3 | Confirmed |
| Procurement | Purchase Requisitions | 4 | Confirmed |
| Procurement | Catalogs | 9 | Confirmed |

*Regressions being corrected in run 5. "Saved-search capture" = SELECT widget shows saved search names instead of real task tabs.

**41 modules** still need task content (0 real tasks). Many are list-view pages or navigate-to-home tiles.

---

## Key Technical Concepts

### ADF Classic vs Redwood UI

Two fundamentally different page layouts exist in Oracle Fusion:

| Attribute | ADF Classic | Redwood |
|-----------|-------------|---------|
| Task panel | Behind a right-rail "Tasks" button | Rendered open on page load |
| Open method | Click Tasks icon | None needed — precheck captures it |
| CSS classes | `xmu`, `x16g` (obfuscated, changes per release) | N/A |
| Detection | Content absent before Tasks click | Content present at precheck |

### Precheck Pattern (critical)

```python
# BEFORE calling open_task_panel(), read content to detect Redwood
precheck = get_task_panel_content(page)
precheck_task_count = sum(len(s.get('tasks', [])) for s in precheck)
if precheck_task_count >= 3:
    # Redwood — panel already open, don't toggle it closed
    task_opened = True
else:
    # ADF Classic — need to click Tasks button
    task_opened = open_task_panel(page)
```

**Why threshold ≥ 3**: Some ADF Classic pages have stray elements at x>1100 (org selectors, Save buttons) that register as 1-item precheck results. Genuine Redwood panels have 5–23 tasks.

### Section Header Detection (font-weight heuristic)

ADF CSS class names (`xmu`, `x16g`) are release-obfuscated and unreliable. Instead, section headers are detected by computed style:

```javascript
const fw = window.getComputedStyle(el).fontWeight;
const bold = parseInt(fw) >= 600;
const isSection = tag === 'DIV' && (bold || isAdfHeader || isHeading)
                  && !hasAnchorChild && r.height < 35;
```

### NOISE Task Filter

These strings appear in task panels as UI controls, not real tasks:
```python
NOISE = {'Add Fields', 'Help', 'Done', 'Save', 'Personal Information', 'Refresh'}
```

Used in:
1. `_module_has_content()` — resume-skip logic (requires ≥2 non-NOISE tasks)
2. `map_module()` — "keep better data" protection

### Resume Mode

The mapper loads existing `oracle_schema_map.json` and skips any module already having ≥2 real (non-NOISE) tasks. This means runs are safe to restart without re-probing completed modules.

### "Keep Better Data" Protection

Before writing a module's result to schema, compares new capture vs existing:

```python
if new_count < existing_count:
    print(f"(keeping existing {existing_count} tasks — new capture has only {new_count})")
    go_home(page)
    return
```

Prevents regression when a page is in an unusual state during re-probe.

---

## Known Issues

### Modules That Navigate to Home

These tiles return to the Oracle Fusion home page (`Oracle Fusion Cloud Applications` title) instead of opening the module — requires URL-based navigation, not yet implemented:

- Receipt Accounting
- Financial Orchestration
- Supply Orchestration
- Supply Chain Orchestration

### List-View Saved Search Capture

Pages like `Manage Journals`, `Manage Price Lists`, `Plan Inputs` are list/grid views with a `<SELECT>` dropdown for saved searches. The mapper's `get_task_panel_select_options()` captures this SELECT thinking it's a task-panel tab switcher. Result: tabs like "Application Default", "findByName" with only "Add Fields" under each.

**Current mitigation:** NOISE filter excludes "Add Fields" so these modules get re-probed each run but never improve.

**Correct fix (not yet implemented):** Detect list-view pages by URL/title pattern and skip SELECT capture entirely, instead looking for a task panel icon or sidebar.

### Quality Management — 0 Tasks Despite Panel Opening

The Quality Management tile navigates to a "Quality Issues" view. The task panel appears to open (Tasks button found) but `get_task_panel_content()` reads 0 tasks. Likely a y-threshold or element visibility issue on this specific page layout.

---

## Running the Mapper

```bash
cd pipeline
python -u oracle_schema_mapper.py
```

The `-u` flag forces unbuffered output so log lines appear in real time. The mapper will:
1. Load existing schema (resume from prior run)
2. Navigate to Oracle Fusion DEV13 home
3. Iterate all tabs and tiles
4. Skip modules with ≥2 real tasks
5. Probe remaining modules, save JSON/TXT after each

**Estimated time:** 5–15 minutes depending on how many modules need re-probing.

---

## Intersection Map (Part 80446-04)

`build_intersection_map.py` cross-references `oracle_schema_map.json` with confirmed write operations for part 80446-04 to classify every Oracle Fusion module as Confirmed, Adjacent, or Low relevance.

```bash
cd pipeline
python build_intersection_map.py
```

### Current Results (run as of 2026-04-24)

| Category | Count | Details |
|----------|-------|---------|
| Confirmed | 4 modules | 16 confirmed write-op tasks |
| Adjacent | 20 modules | Keyword-matched to item attributes |
| Low | 31 modules | No relevant keyword overlap |

### Confirmed Touchpoints

| Module | Evidence | Confirmed Write Ops |
|--------|----------|---------------------|
| SCE / Work Execution | 264 work orders at 3145_US_MAN_MFG with item=80446-04 | Manage Work Orders, Create, Release |
| SCE / Inventory Management (Classic) | All stocked items including 80446-04 | Manage Item Quantities, Create Misc Transaction, +6 more |
| Procurement / Purchase Orders | Part found in Agreements search; Buyer: Gard, Adam | Manage Agreements, Manage Watchlist, Save |
| Procurement / Approved Supplier List | Accessible from PO Tasks panel | Manage ASL Entries, Manage Suppliers |

### Adjacency Keywords

```python
ADJACENT_KEYWORDS = [
    "purchase", "purchas", "requisition", "supplier", "agreement",
    "receipt", "receive", "inventory", "stock", "lot", "serial",
    "work order", "manufacturing", "cost", "planning", "supply",
    "item", "catalog", "price",
]
```

A module scoring ≥2 keyword hits across its name + task list is classified Adjacent.

---

## File Relationships

```
oracle_schema_mapper.py           ← crawler
    └── oracle_schema_map.json    ← output (incremental)
    └── oracle_schema_map.txt     ← human-readable output

build_intersection_map.py         ← cross-reference engine
    ├── oracle_schema_map.json    ← input
    ├── write_ops_report_v3.json  ← input (part 80446-04 write ops)
    └── pim_screenshots/80446-04/write_ops/
        ├── intersection_map.json ← output
        └── intersection_map.txt  ← output
```

---

## Document Version: 1.0 — 2026-04-24
