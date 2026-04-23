# Oracle Fusion Module Operations Map — Comprehensive Agent Design
**Generated:** 2026-04-23  
**Context:** Burlington (3165_US_BUR_MFG) DEV13 — Extended module coverage  
**Base Pattern:** [ABC Agent Design](oracle_fusion_abc_agent_design.md)

---

## Overview

This document extends the ABC Agent design pattern to map interaction workflows for all major Oracle Fusion supply chain modules. Each module section includes:
- **Navigation Path**: How to reach the module from FuseWelcome
- **Key Operations**: Open, Change, Manipulate, Interact workflows
- **Technical Rules**: Module-specific selectors, coordinates, wait times
- **Common Patterns**: Reusable action sequences

---

## Module: Order Management > Order Management

### Navigation Path

```
START → FuseWelcome
  → Click "Order Management" tab (SCE level)
  → Click "Show More" (if needed)
  → Click "Orders" tile (or specific order type)
  → Click "Manage Orders" task
```

### Key Operations

#### 1. **OPEN Order**
- Navigate to Orders list view
- Query parameters: order type, status, date range, supplier
- Filter by: Order Number, Supplier, Status, PO Date
- **Technical Rule**: Order search uses predictive LOV input at x~286, y~350–400
- Wait 2500ms after filter apply
- Click exact order number match (left sidebar x:10–300, y:200–700)
- Wait 5000ms for order detail load

#### 2. **CHANGE Order Header**
- From order detail page, locate Header section (y:220–280)
- Click "Edit" button (near Status field) or header edit icon
- **Editable Fields**: 
  - Supplier (LOV at y~250)
  - Order Type (dropdown at y~270)
  - Currency (dropdown at y~290)
  - Ship-to Address (LOV at y~310)
- Apply changes via "Save" button (x:700–800, y:100–150)
- Wait 3000ms for AJAX response

#### 3. **MANIPULATE Order Lines**
- Navigate to "Lines" section (y:400–600)
- Add line: Click "[+]" button at y~650 → Fill Line Item LOV (item search)
- Change line: Click row → edit quantity, unit price, date fields
- Delete line: Click row selector → click trash icon → confirm
- **Technical Rule**: Line items table has column filters at x~286 (Item), x~450 (Qty), x~600 (Unit Price)
- Line add LOV positioned at x~58, y~633–665 (pattern from ABC design)

#### 4. **INTERACT Order Flow**
- Submit order: "Submit" button at (x:750, y:120) → confirmation dialog
- Approve order: If status allows, "Approve" button → wait 3000ms
- Reject order: "Reject" button → reason field (text area y~400) → confirm
- Close order: "Close" button → confirmation → wait 2000ms

---

## Module: Supply Chain Execution > Work Definition

### Navigation Path

```
START → FuseWelcome
  → Click "Supply Chain Execution" tab
  → Click "Work Definition" (or Show More)
  → Click "Work Orders" or "Work Order Templates" tile
  → Click "Manage Work Orders" task
```

### Key Operations

#### 1. **OPEN Work Order / Template**
- Navigate to Work Order list
- Filter by: Work Order ID, Status, Priority, Department, Date Range
- **Search input** at x~286, y~350–380
- Click work order ID → wait 4000ms for detail load
- Expected load: Header (y:200–300), Lines (y:400–700), Routing (y:800+)

#### 2. **CHANGE Work Order Definition**
- Header section editable fields: Priority, Department, Scheduled Start/End
- Click edit icon or "Edit" button
- **Modal or inline edit** depending on Redwood layout
- Update fields → "Save" button confirmation

#### 3. **MANIPULATE Work Order Steps / Routing**
- Navigate to "Routing" or "Steps" section
- Add step: "[+]" button → Operation LOV search (x~286, y~600)
- Reorder steps: Drag row or use up/down arrows (x:50–100, y:row_y)
- Edit step: Click row → update Operation, Resource, Duration fields
- Delete step: Select row → click trash icon

#### 4. **INTERACT Work Order Execution**
- Release work order: "Release" button → confirmation
- Start work order: "Start" → time clock recorded
- Complete work order: "Complete" → validates all steps done
- Rework option: "Rework" button → reopens closed work order

---

## Module: Supply Chain Execution > Work Execution

### Navigation Path

```
START → FuseWelcome
  → Click "Supply Chain Execution" tab
  → Click "Work Execution" (show more if needed)
  → Click "Execution Dashboard" or "My Work" tile
  → Click "Manage Work Queue" task
```

### Key Operations

#### 1. **OPEN Work in Progress**
- My Work inbox displays: Assigned tasks, Status, Priority, Due Date
- Filter by: Status (In Progress, Pending), Resource, Priority
- Click task row to open detail view
- **Status column** shows state machine: Not Started → In Progress → Done

#### 2. **CHANGE Work Assignment**
- From work detail, click "Assignment" section (y:300–350)
- Change assignee: LOV input at x~286, y~330 (Resource/Employee search)
- Update: Priority (dropdown y~310), Due Date (date picker y~340)
- Save changes → wait 2500ms

#### 3. **MANIPULATE Execution Timeline**
- Clock in: "Start Work" button (x:700, y:150) → records timestamp
- Clock out: "Complete Work" button → timestamps logged
- Add time entry: "Add Manual Entry" → Date, Start, End, Notes fields
- Pause/Resume: If supported, "Pause" button toggles to "Resume"

#### 4. **INTERACT Work Queue**
- Accept work: "Accept" button on task card → moves to My Work
- Reject work: "Reject" → reason field (text area) → confirmation
- Escalate work: "Escalate" → select manager from LOV
- Add comment: Comment box at bottom (y:900+) → "@mention" support

---

## Module: Supply Chain Execution > Quality Management

### Navigation Path

```
START → FuseWelcome
  → Click "Supply Chain Execution" tab
  → Click "Quality Management" tile
  → Select: "Inspections" or "Quality Orders" or "Lot Controls"
  → Click "Manage [Type]" task
```

### Key Operations

#### 1. **OPEN Inspection Record**
- Navigate to Inspections list
- Filter by: Inspection ID, Status, Lot, Item, Date Range
- Click inspection ID (x:100–250, y:400–700) → detail load
- Detail shows: Lot info, Item, Characteristics, Test Results, Approval Status

#### 2. **CHANGE Inspection Attributes**
- Header editable: Lot Reference, Inspection Plan, Status
- Characteristics section: For each characteristic row, edit observed value (x~600, y:row_y)
- **Test Results table**: Click cells to enter pass/fail, numeric values
- Save: Bottom "Save" or "Next" button (x:750, y:100)

#### 3. **MANIPULATE Quality Holds / Lot Controls**
- From inspection, if defects found: Create Quality Hold
- Click "Create Hold" button (y:750) → Hold Type LOV (x~286, y~400)
- Set hold details: Hold Reason (dropdown), Quantity Held, Hold Until (date)
- Release hold: "Release Hold" button → reason confirmation

#### 4. **INTERACT Quality Approvals**
- Approve inspection: "Approve" button → confirmation → wait 2000ms
- Reject inspection: "Reject" → reason (text area y~500) → retest required
- Forward to quality manager: "Forward" → optional comment
- Create deviation: "Create Deviation" → deviation type LOV → linked to quality order

---

## Module: Supply Chain Execution > Cost Accounting

### Navigation Path

```
START → FuseWelcome
  → Click "Supply Chain Execution" tab
  → Click "Cost Accounting" tile
  → Click "Cost Management" or "Costing Period" tile
  → Click "Manage Costs" task
```

### Key Operations

#### 1. **OPEN Cost Allocation / Period**
- Navigate to Costing Periods list or Cost Details
- Filter by: Period Name, Status, Item, Department, Cost Type
- Click period/item row → loads cost breakdown (y:200–900)
- Displays: Material costs, Labor costs, Overhead allocation, Totals

#### 2. **CHANGE Cost Allocation**
- Locate allocation rules section (y:300–500)
- Edit allocation percentages: Click % field (x~600, y:row_y) → enter new %
- **Technical Rule**: Allocation % fields must total 100% — validation on Save
- Update overhead pool: Click "Edit Pools" → pool allocation table
- Save changes → "Save" button (x:750, y:120) → wait 3000ms

#### 3. **MANIPULATE Cost Details**
- Add cost line: "[+]" button → Cost Type LOV (x~286, y~400)
- Update line: Amount field (x~550, y:row_y) → new value
- Delete line: Select row → trash icon → confirmation
- Split cost: "Split Cost" button → creates sub-allocations across departments

#### 4. **INTERACT Cost Transactions**
- Post costs: "Post Costs" button → validation → wait 4000ms
- Rollback: If not posted, "Rollback Changes" button
- Generate cost report: "Generate Report" → PDF output
- Cost variance analysis: "Variance Report" button → opens comparison view

---

## Module: Supply Chain Execution > Receipt Accounting

### Navigation Path

```
START → FuseWelcome
  → Click "Supply Chain Execution" tab
  → Click "Receipt Accounting" tile
  → Click "Receipts" or "Receipt Lines" tile
  → Click "Manage Receipts" task
```

### Key Operations

#### 1. **OPEN Receipt**
- Navigate to Receipts list
- Filter by: Receipt ID, PO Number, Supplier, Receipt Date, Status
- Receipt search input at x~286, y~350–380
- Click receipt ID → detail view loads (y:200–800)
- Shows: Header (PO, Supplier, Qty), Lines (Item, Qty, UoM, Location), Inspection status

#### 2. **CHANGE Receipt Details**
- Receipt header editable: Location (LOV x~286, y~250), Notes field
- Receipt line editable: Received Qty (x~550, y:row_y), Location override, Lot/Serial
- Click row to edit → save after each change or batch with "Save All"
- **Technical Rule**: Quantity changes may trigger inspection requirement checks

#### 3. **MANIPULATE Receipt Lines**
- Add line: Manually add if missing (rare) → "[+]" → PO Line LOV search
- Split line: Select row → "Split Line" button → qty distribution dialog
- Correct line: Click Qty field → correct received amount
- Reject line: "Mark as Rejected" button → rejection reason (dropdown y~400)

#### 4. **INTERACT Receipt Processing**
- Complete receipt: "Complete Receipt" button → confirms all lines → wait 3000ms
- Correct receipt: If after-close, "Create Receipt Correction" → adjusts qty
- Reverse receipt: "Reverse Receipt" → creates reversing transaction
- Link to inspection: Auto-creates if inspection plan present; manual link via "Link Inspection"

---

## Module: Supply Chain Execution > Financial Orchestration

### Navigation Path

```
START → FuseWelcome
  → Click "Supply Chain Execution" tab
  → Click "Financial Orchestration" tile
  → Click "Reconciliation" or "Financial Transactions" tile
  → Click "Manage Orchestration" task
```

### Key Operations

#### 1. **OPEN Orchestration / Reconciliation**
- Navigate to Orchestration Dashboard or Reconciliation list
- Filter by: Period, Legal Entity, Status, Transaction Type
- Click reconciliation record → detail page (y:200–900)
- Displays: Matched items, Unmatched items, Discrepancies, Approval status

#### 2. **CHANGE Reconciliation Decisions**
- Unmatched items section (y:400–600): Click item → Mark as Matched/Exception
- For exceptions: Click "Resolve Exception" → Reason dropdown (y~450)
- Update tolerance thresholds: Click "Tolerances" section → % or $ amount fields
- Apply changes → "Save & Validate" button (x:750, y:120) → wait 3000ms

#### 3. **MANIPULATE Financial Matching**
- Manual matching: Select two unmatched items (rows x:20, y:row_y)
- Click "Match Items" button → confirmation → matched row moves to "Matched" section
- Unmatch items: Click matched row → "Unmatch" button → reverses pairing
- Adjustment entries: "Create Adjustment" → Amount, Reason, GL Account fields

#### 4. **INTERACT Orchestration Workflow**
- Submit for approval: "Submit" button (x:750, y:120) → wait 2000ms
- Approve orchestration: "Approve" button (if role allows) → confirmation
- Reject: "Reject" → Reason field (text area y~500)
- Close period: "Close Period" button → validates all reconciliation complete

---

## Module: Supply Chain Execution > Supply Orchestration

### Navigation Path

```
START → FuseWelcome
  → Click "Supply Chain Execution" tab
  → Click "Supply Orchestration" tile
  → Click "Supply Plans" or "Execution Schedule" tile
  → Click "Manage Supply Schedule" task
```

### Key Operations

#### 1. **OPEN Supply Plan / Schedule**
- Navigate to Supply Plans list
- Filter by: Plan Name, Status, Item, Supplier, Effective Date
- Click plan ID (x:100–300, y:400–700) → detail view
- Detail shows: Plan header, Item supply lines, Schedule (timeline view)

#### 2. **CHANGE Plan Definition**
- Plan header: Status (dropdown y~250), Effective Date (date picker y~280)
- Supply lines: Click line → edit Supplier (LOV x~286, y~350), Qty fields
- Schedule: Click date cell (x~600+, y:row_y) → change qty for that week/month
- Save plan → "Save & Publish" button (x:750, y:120) → wait 3000ms

#### 3. **MANIPULATE Supply Allocations**
- Add supply line: "[+]" button → Item LOV search (x~286, y~400)
- Rebalance qty: Click rows → redistribute total qty across time periods
- Suspend supply: Select line → "Suspend" button → resume date picker
- Expedite supply: "Expedite" button → changes priority to high, moves schedule forward

#### 4. **INTERACT Plan Execution**
- Activate plan: "Activate" button → begins orchestration execution
- Pause plan: "Pause" button → halts generation of purchase orders
- Resume: "Resume" button → continues from pause point
- Compare plan versions: "Compare to Previous" → side-by-side diff view

---

## Module: Supply Chain Execution > Supply Chain Orchestration

### Navigation Path

```
START → FuseWelcome
  → Click "Supply Chain Execution" tab
  → Click "Supply Chain Orchestration" tile
  → Click "Orchestration Dashboard" or "Event Management" tile
  → Click "Manage Orchestration" task
```

### Key Operations

#### 1. **OPEN Orchestration Event / Shipment**
- Navigate to Orchestration Dashboard or Event list
- Filter by: Event ID, Status, Supplier, Item, Date Range
- Event search input at x~286, y~350–380
- Click event ID → detail view loads (y:200–900)
- Shows: Event header, Ship details, Tracking, Delivery status

#### 2. **CHANGE Event Routing**
- From event detail, click "Routing" section (y:300–400)
- Update carrier: LOV input at x~286, y~330
- Change ship-to location: Address LOV (x~286, y~360)
- Update tracking #: Text field at x~550, y~380
- Save routing → "Save Changes" button

#### 3. **MANIPULATE Exception Handling**
- If exception detected (status shows "Exception"), click to view details
- Exception reasons: Delay, Damage, Missing items, etc.
- Create corrective action: "Create Action" button → Action Type dropdown
- Link to quality issue: "Link to Quality" → QO LOV search
- Assign owner: Resource LOV at x~286, y~450

#### 4. **INTERACT Shipment Tracking**
- Update status: Click Status dropdown → change to In Transit, Delivered, etc.
- Receive shipment: "Mark as Received" button → confirmation → wait 2000ms
- Create return: "Initiate Return" button → return reason dropdown
- Exception resolution: "Resolve Exception" → reason selection → "Close"

---

## Module: Supply Chain Planning > Plan Inputs

### Navigation Path

```
START → FuseWelcome
  → Click "Supply Chain Planning" tab
  → Click "Plan Inputs" tile
  → Click "Manage Demand" or "Upload Plan Data" tile
  → Click task based on input type
```

### Key Operations

#### 1. **OPEN Plan Input / Data File**
- Navigate to Plan Inputs list
- Filter by: Input Name, Input Type, Load Date, Status
- Click input record → preview/detail view
- Shows: Columns mapped, Row count, Validation status, Load history

#### 2. **CHANGE Input Parameters**
- Input header editable: Plan Horizon (date range pickers y~250–280)
- Level of Detail (dropdown): Monthly, Weekly, Daily (y~300)
- Upload file: "Upload New File" button → file chooser
- Map columns: If new file, show column mapping table (x~286–600, y:400–700)
- Save mapping → "Save Configuration" button

#### 3. **MANIPULATE Input Data**
- If inline editable grid: Click cell → edit values
- Batch import: Upload CSV → "Validate" button → shows errors/warnings
- Correct data: Click error row → edit field → revalidate
- Delete row: Select row → trash icon → confirmation

#### 4. **INTERACT Plan Loading**
- Load/import data: "Load Plan" button → confirmation → wait 5000ms (depends on size)
- Run validation: "Validate All" button → generates validation report
- View results: "Validation Report" link → shows errors and fix suggestions
- Archive input: "Archive" button → moves to history (can re-use as template)

---

## Module: Supply Chain Planning > Demand Management

### Navigation Path

```
START → FuseWelcome
  → Click "Supply Chain Planning" tab
  → Click "Demand Management" tile
  → Click "Forecast" or "Customer Demand" tile
  → Click "Manage Demand" task
```

### Key Operations

#### 1. **OPEN Demand Forecast / Order**
- Navigate to Demand list
- Filter by: Item, Customer, Forecast Date, Status, Planning Org
- Forecast search input at x~286, y~350–380
- Click demand record → detail view (y:200–900)
- Shows: Header (Item, Customer, Qty), Time buckets (weeks/months), Approval status

#### 2. **CHANGE Demand Forecast**
- Header editable: Customer LOV (x~286, y~250), Qty field (x~550, y~250)
- Time bucket grid: Click qty cell (x~500+, y:row_y) → new value
- Approval status: If unapproved, can edit; if approved, shows "locked"
- Save changes → "Save Forecast" button (x:750, y:120)

#### 3. **MANIPULATE Demand Allocation**
- Split forecast: "Split Demand" button → qty distribution across time periods
- Reallocate to different customer: Select rows → "Reassign to Customer" → LOV
- Add demand line: "[+]" button → Item LOV (x~286, y~400)
- Delete line: Select row → trash icon → confirmation

#### 4. **INTERACT Demand Approval Flow**
- Submit forecast: "Submit" button → sends to planner/manager → wait 2000ms
- Approve forecast: "Approve" button (if role allows) → confirmation
- Reject forecast: "Reject" → reason field (text area y~500)
- Create purchase order: "Create PO" button → auto-generates order from demand

---

## Module: Supply Chain Planning > Supply Planning

### Navigation Path

```
START → FuseWelcome
  → Click "Supply Chain Planning" tab
  → Click "Supply Planning" tile
  → Click "Supply Plan" or "Procurement Plan" tile
  → Click "Manage Supply Plan" task
```

### Key Operations

#### 1. **OPEN Supply Plan**
- Navigate to Supply Plans list
- Filter by: Plan Name, Status, Item, Supplier, Planned Date
- Click plan ID (x:100–300, y:400–700) → detail view
- Shows: Supply plan lines, Timeline, Proposed actions (create PO, etc.)

#### 2. **CHANGE Supply Plan**
- Plan header: Status (dropdown y~250), Review Date (date picker y~280)
- Supply lines: Click line → edit Supplier (LOV x~286, y~350), Qty, Lead Time
- Save plan → "Save Plan" button (x:750, y:120) → wait 2500ms

#### 3. **MANIPULATE Supply Actions**
- Add supply line: "[+]" button → Item LOV (x~286, y~400)
- Approve proposed actions: Select action rows → "Approve Actions" button
- Create POs: Select supply lines → "Create Purchase Orders" button → wait 4000ms
- Expedite: Select line → "Expedite" button → priority/date override

#### 4. **INTERACT Supply Plan Workflow**
- Publish plan: "Publish" button → releases to execution → wait 3000ms
- Generate exception report: "Exceptions Report" button → shows shortages, overages
- Baseline comparison: "Compare to Baseline" → shows changes from last version
- Rollback to previous: "Revert" button → loads prior approved version

---

## Module: Supply Chain Planning > Replenishment Planning

### Navigation Path

```
START → FuseWelcome
  → Click "Supply Chain Planning" tab
  → Click "Replenishment Planning" tile
  → Click "Manage Reorder Points" or "Replenishment Strategy" tile
  → Click task based on strategy
```

### Key Operations

#### 1. **OPEN Replenishment Strategy / Reorder Point**
- Navigate to Reorder Points or Replenishment Strategies list
- Filter by: Item, Inventory Org, Strategy Type, Min Stock Level
- Click record ID (x:100–300, y:400–700) → detail view
- Shows: Current settings, Reorder point, Safety stock, Lead time, Demand variability

#### 2. **CHANGE Replenishment Parameters**
- Header editable: Strategy Type (dropdown y~250), Reorder Point (qty x~550, y~250)
- Lead Time fields: Min, Expected, Max (x~550+, y:300–350)
- Safety stock: % or qty field (x~550, y~370)
- Demand parameters: Avg demand (read-only), Variability % (editable y~400)
- Save changes → "Save Strategy" button (x:750, y:120)

#### 3. **MANIPULATE Replenishment Rules**
- Adjust reorder point: Qty spinner (x:550–600, y~250) or input field
- Update safety stock: % dropdown or qty input (y~370)
- Change supplier: Supplier LOV (x~286, y~390)
- Set replenishment frequency: Dropdown (weekly, monthly, etc.) y~410
- Apply to bulk items: "Apply to Category" button → LOV for item category

#### 4. **INTERACT Replenishment Execution**
- Simulate replenishment: "Simulate" button → shows suggested orders without committing
- Generate orders: "Generate Replenishment Orders" button → creates purchase orders → wait 4000ms
- Activate strategy: "Activate" button → begins automated monitoring
- Pause: "Pause" button → stops auto-generation until resumed
- Forecast adjustment: "Adjust Forecast" link → modifies demand input for calculation

---

## Module: Product Management > Product Development

### Navigation Path

```
START → FuseWelcome
  → Click "Product Management" tab
  → Click "Product Development" tile
  → Click "New Product Introduction" or "Product Lifecycle" tile
  → Click "Manage Products" task
```

### Key Operations

#### 1. **OPEN Product Development Record / NPI**
- Navigate to Product Development list
- Filter by: Product ID, Status, Development Stage, Team, Target Launch Date
- Click product record ID (x:100–300, y:400–700) → detail view
- Shows: Product header, Specs, Development phases, Approval gates, Timeline

#### 2. **CHANGE Product Specifications**
- Header editable: Product Name (text x~550, y~250), Description (text area y~300)
- Classification: Category LOV (x~286, y~350), Sub-category (x~286, y~370)
- Attributes: Click attribute row → enter value (x~550, y:row_y)
- Save specifications → "Save Product" button (x:750, y:120)

#### 3. **MANIPULATE Product BOM / Formulation**
- Add component: "[+]" button → Component LOV search (x~286, y~400)
- Edit line: Qty (x~550, y:row_y), Unit of Measure (dropdown x~650, y:row_y)
- Change sequence: Row reorder via up/down arrows (x:50–100, y:row_y)
- Delete component: Select row → trash icon → confirmation
- Add alternate: "Add Alternate" button → select alternative component

#### 4. **INTERACT Development Gates / Approval**
- Move to next phase: "Advance to [NextPhase]" button → gate validation
- Gate approval: "Approve Gate" button (if role allows) → confirmation
- Request review: "Request Engineering Review" → assignee LOV
- Create issue: "Create Issue" → Issue Type (dropdown y~450) → description
- Launch product: "Launch Product" button (final gate) → confirmation → wait 3000ms

---

## Module: Product Management > Product Information Management

### Navigation Path

```
START → FuseWelcome
  → Click "Product Management" tab
  → Click "Product Information Management" tile
  → Click "Manage Products" or "Product Catalog" tile
  → Click task based on operation
```

### Key Operations

#### 1. **OPEN Product Record / SKU**
- Navigate to Product Catalog or Master list
- Filter by: Product ID, Product Name, Category, Status, Last Modified Date
- Product search input at x~286, y~350–380
- Click product ID → detail view loads (y:200–900)
- Shows: Product master data, Variants, Cross-references, Attachments, Approval status

#### 2. **CHANGE Product Master Data**
- Basic info editable: Name (text x~550, y~250), Description (text area y~300)
- Category (LOV x~286, y~350), UoM (dropdown x~650, y~370)
- Status (dropdown y~390): Active, Inactive, Obsolete, etc.
- Approval gate: If "Draft", can edit all; if "Active", limited to certain fields
- Save master data → "Save Product" button (x:750, y:120)

#### 3. **MANIPULATE Product Variants / SKUs**
- Add variant: "[+]" button in Variants section → Attribute LOV (x~286, y~400)
- Edit variant: Click variant row → modify attribute values (x~550+, y:row_y)
- Link cross-reference: "Add Cross-Ref" button → External system LOV (x~286, y~450)
- Attachment: "Add Attachment" button → file upload dialog
- Delete variant: Select row → trash icon → confirmation

#### 4. **INTERACT Product Lifecycle / Approval**
- Request approval: "Submit for Approval" button → approval chain initiated → wait 2000ms
- Approve product: "Approve" button (if role allows) → confirmation
- Reject with feedback: "Reject" → feedback text area (y~500) → resubmit required
- Obsolete product: "Mark Obsolete" button → effective date picker → confirmation
- Activate variant: "Activate" button → makes variant available for transactions

---

## Module: Supply Chain Execution > Inventory Management (Classic) — EXTENDED

### Navigation Path (Review from ABC Design)

```
START → FuseWelcome
  → Click "Supply Chain Execution" tab
  → Click "Inventory Management (Classic)" tile (x,y from abc design)
  → Click "Manage Cycle Counts" or "Inventory Transactions" task
```

### Key Operations

#### 1. **OPEN Inventory Transaction / Cycle Count** (ALREADY COVERED IN ABC DESIGN)

**Additional inventory transactions (beyond cycle counts):**

- **Move inventory**: From Inventory list, click transaction row → detail view
  - Edit: From Loc (LOV x~286, y~250), To Loc (LOV x~286, y~280), Qty (x~550, y~300)
  - Save → "Complete Move" button (x:750, y:120)

- **Adjust inventory**: Click adjustment transaction
  - Edit: Item, Qty Adjustment (can be negative), Reason (dropdown y~350)
  - Save → "Post Adjustment" button

#### 2. **CHANGE Cycle Count Parameters** (FROM ABC DESIGN)

- Already detailed: Org selection, cycle count selection, step navigation
- Additional: Edit count name, change schedule, update count frequency

#### 3. **MANIPULATE Cycle Count Items** (FROM ABC DESIGN)

- Add items to category: Already covered in ABC design (step 5 LOV add)
- Remove items: Delete row in category section
- Reassign items to different class: Remove from current class (delete), add to target class
- **D/P class creation**: If not present, create via "Add Category" button (y:650)

#### 4. **INTERACT Cycle Count Posting** (EXTENSION)

- Post count: After all items verified, click "Post Count" button (x:750, y:120) → wait 5000ms
- Count results: System calculates variances (counted vs. system qty)
- Variance investigation: Click variance row → investigation details
- Approve variances: "Approve Variances" button (manager role)
- Recount: "Create Recount" button → new count record for items with issues
- Cycle count history: "View History" button → prior counts for this org/schedule

---

## Cross-Module Technical Rules & Patterns

### 1. **Navigation Consistency**

| Pattern | Coordinates | Wait Time |
|---------|-------------|-----------|
| Module tab click | x: 462–1000, y: 40–120 | 2000–2500ms |
| Tile click | x: 200–1000, y: 200–700 | 5000ms |
| Task link click | x: 10–300, y: 200–900 | 3000–5000ms |
| Show More toggle | x: 900–1100, y: 40–120 | 1500–2000ms |

### 2. **LOV (List of Values) Search Pattern**

```
1. Click input field at approximately x:286, y:TARGET_Y
2. Keyboard: Ctrl+A (select all) → type search term
3. Wait 1500–2000ms for predictive results
4. Press Enter or click exact match
5. Wait 2000ms for field population
```

### 3. **Confirmation Dialog Handling**

```
Type 1: Alert (OK only)
  - Locate "OK" button (usually x:700–800, y:500–550)
  - page.mouse.click(cx, cy) — NOT el.click()
  - Wait 2000ms

Type 2: Confirm (Yes/No or OK/Cancel)
  - Default focus on "Cancel" (right button)
  - Click "OK"/"Yes" (left button) at x:600–700, y:550
  - Wait 2000–3000ms for action completion
```

### 4. **Table Editing Patterns**

| Operation | Selector | Coordinates | Wait |
|-----------|----------|------------|------|
| Column filter | `input[placeholder*="Filter"]` or x~286 | y: table_y + 40 | 2500ms |
| Row select | Checkbox x~20–50 | y: row_y | 500ms |
| Edit cell | Click cell | x: col_x, y: row_y | inline, then Tab or Enter |
| Add row | "[+]" button | x: 50–100, y: table_bottom + 50 | 1500ms |
| Delete row | Trash icon | x: 1400+, y: row_y | 2000ms |

### 5. **AJAX / Partial Page Refresh Pattern**

```
After filter/search/action:
  1. Do NOT wait for full page reload
  2. Wait 2500–3000ms for Oracle ADF partial refresh
  3. Check for "Loading" spinner at (x:700, y:300) — if visible, extend wait 2000ms more
  4. Verify new content appears (scroll if needed)
```

### 6. **Session State Detection**

```
Before navigation:
  1. Check page.title() — should NOT contain "Sign In"
  2. If sign in detected → re-load cookies from oracle_session.json
  3. Cookie validation: context.add_cookies() then page.goto(url)
  4. Wait 5000ms, then verify title changed
```

---

## Implementation Checklist

### Phase 1: Core Navigation (Weeks 1–2)
- [ ] Map all module entry points (tab → show more → tile → task)
- [ ] Create coordinate reference database (x, y by screen type)
- [ ] Test navigation without data manipulation (dry run)
- [ ] Screenshot each major module landing page

### Phase 2: Read Operations (Weeks 3–4)
- [ ] Implement list view filtering for each module
- [ ] Create LOV search utility function (used across 80% of modules)
- [ ] Implement detail view load & field extraction
- [ ] Build screenshot checkpoint system

### Phase 3: Write Operations (Weeks 5–8)
- [ ] Header field updates (status, dates, text fields)
- [ ] Table row manipulation (add, edit, delete, reorder)
- [ ] Modal/dialog input handling
- [ ] Confirmation dialog patterns for each module

### Phase 4: Workflow Interactions (Weeks 9–10)
- [ ] Status transitions & approval workflows
- [ ] Exception handling & correction flows
- [ ] Report generation & data export
- [ ] Batch operations (multi-item updates)

### Phase 5: Testing & Optimization (Weeks 11–12)
- [ ] End-to-end workflow tests (2–3 complete flows per module)
- [ ] Error recovery & retry logic
- [ ] Performance profiling & wait time optimization
- [ ] Edge case handling (empty lists, max items, etc.)

---

## Module Priority for Implementation

### Tier 1 (Foundation — use most)
1. Inventory Management (Classic) — cycle counts, transactions, moves
2. Order Management — PO creation, changes, receipt
3. Work Definition — work order lifecycle

### Tier 2 (Core Supply Chain)
4. Receipt Accounting — PO receipt processing
5. Supply Orchestration — shipping & exception handling
6. Demand Management — forecast & planning

### Tier 3 (Planning & Strategic)
7. Supply Planning — replenishment, supplier selection
8. Replenishment Planning — reorder point optimization
9. Plan Inputs — data loading

### Tier 4 (Quality & Cost)
10. Quality Management — inspection, holds, deviations
11. Cost Accounting — cost allocation, post-close adjustments

### Tier 5 (Product & Advanced)
12. Product Management — NPI, catalog maintenance
13. Financial Orchestration — period close, reconciliation
14. Work Execution — time tracking, queue management

---

## Known Limitations & Gaps

1. **Nested modals**: Some workflows (e.g., create PO from demand) open modals within modals — coordinate mapping incomplete
2. **Dynamic grid rendering**: Large datasets (1000+ rows) may have pagination issues — scroll detection needs refinement
3. **Mobile responsiveness**: Coordinates may shift on tablet/mobile — assume desktop (1920x1080) as baseline
4. **Concurrent user edits**: No optimistic locking pattern detected — last-write-wins, potential for overwrites
5. **Oracle Forms embedded**: Some legacy tasks (GL posting, fixed assets) may use embedded Oracle Forms — not mapped yet

---

## Future Enhancements

- **Vision AI**: Screenshot parsing to auto-detect element locations (adaptive coordinates)
- **Record playback**: Capture user actions in GUI, replay via agent
- **Cross-module workflows**: E.g., PO → Receipt → Invoice → GL posting (full trace)
- **Performance analytics**: Heatmap of click locations & wait times to optimize agent paths
- **Error recovery**: Auto-retry with exponential backoff + screenshot diff for debugging

---

**Document Version:** 1.0  
**Last Updated:** 2026-04-23  
**Next Review:** After Phase 2 implementation (estimated 2026-05-07)
