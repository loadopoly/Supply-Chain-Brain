# eDap Power BI Dashboard — Table Reference

> Source: Power BI exports from the eDap reporting environment (Astec Industries).
> These tables define the exact columns, grain, and business logic behind each dashboard
> report page. Use this document when building new Supply Chain Brain pages that need to
> replicate or extend these KPIs.
>
> Last updated: 2026-04-20 | Derived from: exported .xlsx files

---

## Contents

1. [Fill Rate Report](#1-fill-rate-report)
2. [Fill Rate ABC Code](#2-fill-rate-abc-code)
3. [Sales Backlog](#3-sales-backlog)
4. [Sales Details — Dealer](#4-sales-details--dealer)
5. [Sales Part Code](#5-sales-part-code)
6. [On Time Delivery (Part Code Reference)](#6-on-time-delivery-part-code-reference)
7. [On Time Delivery ABC](#7-on-time-delivery-abc)
8. [Spare Parts Availability](#8-spare-parts-availability)

---

## 1. Fill Rate Report

**Dashboard tab:** Fill Rate  
**Grain:** One row per sales order line  
**Primary source tables:** `fact_sales_order_line`, `fact_inventory_on_hand`, `dim_part`, `dim_supplier`  
**Total columns:** 32

### Business Logic

Fill rate measures whether the warehouse can fulfill a customer order from on-hand
stock at the moment the order is picked:

```
Fillrate % = CASE WHEN On Hand Qty >= SO Qty THEN 100 ELSE (On Hand Qty / SO Qty * 100) END
Hit        = 1 when Fillrate % = 100  (or Available Qty >= SO Qty)
Miss       = 0 when Fillrate % < 100
FR         = Hit/Miss as a 0 or 1 integer flag
Qty Can Be Filled = MIN(Available Qty, SO Qty)
```

### Column Definitions

| Column | Azure SQL Mapping | Type | Notes |
|--------|------------------|------|-------|
| Site | `business_unit_key` / `site_name` | text | Manufacturing plant / distribution center |
| Order Date | `order_date_key` | date (MM/DD/YYYY) | Date sales order was entered |
| Promised Date | `promised_ship_day_key` | date | Customer-committed ship date |
| Ship Date | `ship_day_key` | date | Actual ship date; null = not yet shipped |
| SO No | `sales_order_number` | text | Sales order number (Oracle DOO_LINES_ALL) |
| Line No | `sales_order_line` | decimal | Line item number within the order |
| Part | `part_number` | text | Astec internal part number |
| Description | `part_description` | text | Part description from dim_part |
| Supplier Name | `supplier_name` | text | Source/vendor name (ASTEC = internal fab) |
| SO Qty | `sales_order_quantity` | int | Quantity ordered |
| Price | `unit_price_local` | decimal | Unit list price in transaction currency |
| Ext Price With Discount | `extended_price_local` | decimal | SO Qty × Price × (1 - discount %) |
| Available Qty | `available_qty` | int | On-hand minus reserved/committed qty |
| On Hand Qty | `quantity_on_hand` | int | Total on-hand from `fact_inventory_on_hand` |
| Fillrate % | *(computed)* | decimal | 0–100; see Business Logic above |
| Hit Miss | *(computed)* | text | "Hit" or "Miss" |
| FR | *(computed)* | int | 1 = Hit, 0 = Miss |
| Qty Can Be Filled | *(computed)* | int | MIN(Available Qty, SO Qty) |
| Part Fab/Pur | `fabricated_purchased` | text | "fabricated" or "purchased" |
| Part Class | `sales_part_code` | text | ABC code: A Prime, B Prime, C Low Volume, D New, E |
| Drop Ship | `drop_ship_flag` | text | "Yes" / "No" |
| Status | `order_status` | text | e.g. "This is a closed order" |
| Failure | `failure_reason` | text | Root-cause description for miss |
| Domestic/International | `domestic_international_flag` | text | "Domestic" / "International" |
| Customer No | `customer_number` | text | Customer account code |
| Customer Name | `customer_name` | text | Customer display name |
| Ship To Country | `ship_to_country` | text | Destination country |
| Max Lead Time | `max_lead_time_days` | int | Maximum component lead time for the part |
| Max Min Value | `min_max_value` | decimal | Min/Max stocking level limit value |
| Lead Time Date | `lead_time_date` | date | Order date + Max Lead Time |
| Future Order | `future_order_flag` | text | "Yes" / "No" — order not yet due |
| Requested Date | `requested_ship_date_key` | date | Customer-requested ship date |

### Filters Applied in Dashboard

- Date range slicer on `Order Date`
- Multi-select on `Site`, `Part Class`, `Domestic/International`
- `Future Order` = "No" is typically excluded from Fill Rate KPI

---

## 2. Fill Rate ABC Code

**Dashboard tab:** Fill Rate by ABC Code  
**Grain:** One row per sales order line  
**Schema:** Identical to [Fill Rate Report](#1-fill-rate-report) (32 columns)

This report is a filtered / sorted view of the Fill Rate Report, grouped and ranked by
`Part Class` (ABC code). The KPI layout shows hit/miss broken down by:
- A Prime (highest velocity/value)
- B Prime
- C Low Volume
- D New (new introductions)
- E (end-of-life / other)

No additional columns. Computations identical to Fill Rate Report.

---

## 3. Sales Backlog

**Dashboard tab:** Sales Backlog  
**Grain:** One row per open sales order line (shipped qty < ordered qty)  
**Primary source tables:** `fact_sales_order_line`, `dim_part`  
**Total columns:** 19

### Business Logic

```
Backlog = sales orders where Shipped Quantity < Sales Order Quantity
Backlog Amount = Backlog Units × Unit Price (transaction currency)

Backlog Bucket classification:
  "Past Due"  → Promised Ship Date < TODAY()
  "Current"   → Promised Ship Date between TODAY() and TODAY()+30
  "Future"    → Promised Ship Date > TODAY()+30
```

### Column Definitions

| Column | Azure SQL Mapping | Type | Notes |
|--------|------------------|------|-------|
| Site | `business_unit_key` | text | Plant / DC |
| Sales Order Number | `sales_order_number` | text | |
| Sales Order Line | `sales_order_line` | int | Line item within the order |
| Order Date | `order_date_key` | int/date | YYYYMMDD key |
| Promised Ship Date | `promised_ship_day_key` | int/date | Committed ship date |
| Estimated Ship Date | `estimated_ship_date` | date | Latest re-promise date (nullable) |
| Sales Order Quantity | `sales_order_quantity` | int | |
| Shipped Quantity | `shipped_quantity` | int | Cumulative qty shipped to date |
| Part Number | `part_number` | text | |
| Safety Stock Limit | `safety_stock_qty` | int | Min stocking quantity for this part |
| Part Description | `part_description` | text | |
| Backlog Bucket | *(computed)* | text | Past Due / Current / Future |
| Backlog Amount | *(computed)* | decimal | (SO Qty − Shipped Qty) × Unit Price |
| Order Type | `order_type` | text | "Parts" / "Machine" / "Equipment" |
| Part Type | `fabricated_purchased` | text | Fabricated / Purchased |
| Customer | `customer_name` | text | |
| PO Number | `customer_po_number` | text | Customer purchase order reference |
| PSR | `psr` | text | Product Sales Representative |
| Last Ship Date | `last_ship_date` | date | Most recent partial shipment date |

---

## 4. Sales Details — Dealer

**Dashboard tab:** Sales Details / Dealer  
**Grain:** One row per open sales order line (dealer orders only)  
**Primary source tables:** `fact_sales_order_line`, `dim_part`, `dim_customer`  
**Total columns:** 14

A subset of the Sales Backlog filtered to dealer/distributor customer accounts.
Omits line-level qty columns, focuses on order-level identification.

### Column Definitions

| Column | Azure SQL Mapping | Type | Notes |
|--------|------------------|------|-------|
| Site | `business_unit_key` | text | |
| Sales Order Number | `sales_order_number` | text | |
| Order Date | `order_date_key` | int/date | |
| Estimated Ship Date | `estimated_ship_date` | date | Nullable — no promise date column |
| Part Number | `part_number` | text | |
| Safety Stock Limit | `safety_stock_qty` | int | Nullable for configured items |
| Part Description | `part_description` | text | |
| Backlog Bucket | *(computed)* | text | Past Due / Current / Future |
| Backlog Amount | *(computed)* | decimal | Extended backlog value |
| Order Type | `order_type` | text | Parts / Machine / Equipment |
| Part Type | `fabricated_purchased` | text | |
| Customer | `customer_name` | text | |
| PO Number | `customer_po_number` | text | |
| PSR | `psr` | text | |

**Differences vs. Sales Backlog:** No `Sales Order Line`, `Promised Ship Date`,
`Sales Order Quantity`, `Shipped Quantity`, `Last Ship Date`.

---

## 5. Sales Part Code

**Dashboard tab:** Sales Part Code (reference / filter slicer)  
**Grain:** One row per site × part  
**Primary source tables:** `dim_part` (enriched with ABC classification)  
**Total columns:** 5

This is the **ABC classification master** used as a slicer/filter across all other
dashboard pages. It maps each part to both a Sales ABC code and an Inventory ABC code.

### Column Definitions

| Column | Azure SQL Mapping | Type | Notes |
|--------|------------------|------|-------|
| Site | `business_unit_key` | text | Classification can differ by site |
| Part# | `part_number` | text | |
| Description | `part_description` | text | |
| Sales Part Code | `sales_part_code` | text | A Prime, A, B Prime, B, C Low Volume, C, D New, E |
| Inventory Part Code | `inventory_part_code` | text | A, B, C (standard ABC) |

### ABC Code Definitions

| Sales Part Code | Description |
|----------------|-------------|
| A Prime | Highest velocity + highest value; always stock |
| A | High velocity; stock to min/max |
| B Prime | Medium velocity + high value |
| B | Medium velocity; stock to reorder point |
| C Low Volume | Low velocity; stock selectively |
| C | Low velocity; order on demand |
| D New | New part introduction; no history |
| E | End-of-life / superseded; deplete only |

| Inventory Part Code | Description |
|--------------------|-------------|
| A | Top ~20% of parts driving ~80% of demand |
| B | Middle tier |
| C | Bottom tier; slow-movers |

---

## 6. On Time Delivery (Part Code Reference)

**Dashboard tab:** On Time Delivery (filter reference panel)  
**Grain:** One row per site × part  
**Schema:** Identical to [Sales Part Code](#5-sales-part-code) (5 columns)

Despite the filename "On Time Delivery.xlsx", this file contains the same part
classification reference data as Sales Part Code. It is the dimension lookup used
as a filter/slicer in the OTD dashboard pages. All column mappings are identical.

---

## 7. On Time Delivery ABC

**Dashboard tab:** On Time Delivery ABC  
**Grain:** One row per shipped/closed sales order line  
**Primary source tables:** `fact_sales_order_line`, `dim_part`, `dim_supplier`  
**Total columns:** 26

### Business Logic

```
OTD Miss (Late) = 1 when Ship Date > Promised Date (or Adjusted Promise Date)
                  0 when Ship Date <= Promised Date
Days Late       = Ship Date − Adjusted Promise Date (positive = late, negative = early)
OTD %           = SUM(lines on time) / COUNT(all lines) × 100
```

### Column Definitions

| Column | Azure SQL Mapping | Type | Notes |
|--------|------------------|------|-------|
| Site | `business_unit_key` | text | Full site name (e.g. "Chattanooga - Jerome Avenue") |
| Order Date | `order_date_key` | int/date | |
| Ship Date | `ship_day_key` | int/date | Actual ship date |
| SO No | `sales_order_number` | text | |
| Line No | `sales_order_line` | int | |
| Part | `part_number` | text | |
| Description | `part_description` | text | |
| Qty | `sales_order_quantity` | int | |
| Available Qty | `available_qty` | int | Inventory available at time of picking |
| On Hand Qty | `quantity_on_hand` | int | Total on-hand |
| Unit Price | `unit_price_local` | decimal | |
| Ext Price With Discount | `extended_price_local` | decimal | |
| OTD Miss (Late) | *(computed)* | int | 0 = on time, 1 = late |
| Days Late | *(computed)* | int | Signed days; negative = early |
| Supplier Name | `supplier_name` | text | "ASTEC" for fabricated |
| Customer | `customer_name` | text | |
| Customer No | `customer_number` | text | |
| Part Pur/Fab | `fabricated_purchased` | text | "purchased" / "fabricated" |
| Part Class | `sales_part_code` | text | ABC classification |
| Drop Ship | `drop_ship_flag` | text | |
| Failure Reason | `failure_reason` | text | Root-cause; "Not Implemented" = no failure code |
| Domestic/International | `domestic_international_flag` | text | |
| Promised Date | `promised_ship_day_key` | int/date | Original promise date |
| Adjusted Promise Date | `adjusted_promise_date` | int/date | Re-promise date if rescheduled |
| Status Code | `order_status_code` | text | "OPEN" / "CLOSED" / "CANCELLED" |
| Freight Terms | `freight_terms` | text | e.g. "Not Implemented", "FOB", "Prepaid" |

---

## 8. Spare Parts Availability

**Dashboard tab:** Spare Parts Availability  
**Grain:** One row per Machine × Part (wide matrix with one block of columns per date)  
**Primary source tables:** `fact_inventory_on_hand`, `fact_sales_order_line`, `dim_part`  
**Format:** Pivot / wide matrix — date blocks repeat across columns

### Structure

The report is a **wide matrix** (pivot format). Row 1 contains the date axis (Excel
serial dates, 5 columns per date). Row 2 contains metric sub-headers. Rows 3+
contain data.

```
Row 1: [Date label] [date_serial] [date_serial] [date_serial] [date_serial] [date_serial]  → repeats per day
Row 2: [Machine]  [Part #] [Required Qty] [Filled Yes/No] [On Hand Qty] [Available] [Invoiced Qty] [List Price]
Row 3+: data
```

The 5 metrics repeated per date:

| Sub-column | Azure SQL / Computed | Notes |
|-----------|---------------------|-------|
| Filled Yes/No | *(computed)* | "Yes" if On Hand Qty >= Required Qty |
| On Hand Qty | `quantity_on_hand` | Snapshot inventory at that date |
| Available | `available_qty` | On Hand − committed |
| Invoiced Qty | `invoiced_qty` | Qty invoiced (shipped) on that date |
| List Price | `list_price_local` | Published unit list price |

### Row Dimension Columns

| Column | Azure SQL Mapping | Type | Notes |
|--------|------------------|------|-------|
| Machine | `machine_type` | text | Equipment model (e.g. "SB-3000") |
| Part # | `part_number` | text | Spare part number |
| Required Qty | `required_qty` | int | Bill-of-material required quantity per machine |
| Date | *(date axis)* | date | Excel serial → DATEVALUE(serial) |

### De-pivoting for Supply Chain Brain

To load this as a normalized fact table, the wide format must be unpivoted:

```sql
-- Conceptual unpivot structure
CREATE TABLE fact_spare_parts_availability (
    snapshot_date      DATE,           -- converted from Excel serial date
    machine_type       NVARCHAR(100),
    part_number        NVARCHAR(50),
    required_qty       INT,
    filled_flag        BIT,            -- 1=Yes, 0=No
    quantity_on_hand   INT,
    available_qty      INT,
    invoiced_qty       INT,
    list_price_local   DECIMAL(18,4)
);
```

---

## Cross-Reference: Dashboard Columns → Azure SQL

| Dashboard Column | Azure SQL Column | Table |
|-----------------|-----------------|-------|
| Site | `business_unit_key` or `site_name` | multiple |
| Part / Part# | `part_number` | `dim_part` |
| Description | `part_description` | `dim_part` |
| SO No | `sales_order_number` | `fact_sales_order_line` |
| Line No | `sales_order_line` | `fact_sales_order_line` |
| Order Date | `order_date_key` (YYYYMMDD int) | `fact_sales_order_line` |
| Promised Date | `promised_ship_day_key` | `fact_sales_order_line` |
| Ship Date | `ship_day_key` | `fact_sales_order_line` |
| Requested Date | `requested_ship_date_key` | `fact_sales_order_line` |
| Estimated Ship Date | `estimated_ship_date` | `fact_sales_order_line` |
| Adjusted Promise Date | `adjusted_promise_date` | `fact_sales_order_line` |
| SO Qty / Qty | `sales_order_quantity` | `fact_sales_order_line` |
| Shipped Quantity | `shipped_quantity` | `fact_sales_order_line` |
| Price / Unit Price | `unit_price_local` | `fact_sales_order_line` |
| Ext Price With Discount | `extended_price_local` | `fact_sales_order_line` |
| On Hand Qty | `quantity_on_hand` | `fact_inventory_on_hand` |
| Available Qty | `available_qty` | *(computed: on_hand − committed)* |
| Supplier Name | `supplier_name` | `dim_supplier` |
| Customer No | `customer_number` | `dim_customer` |
| Customer Name | `customer_name` | `dim_customer` |
| Part Fab/Pur | `fabricated_purchased` | `dim_part` |
| Part Class | `sales_part_code` | `dim_part` (ABC enrichment) |
| Inventory Part Code | `inventory_part_code` | `dim_part` |
| Drop Ship | `drop_ship_flag` | `fact_sales_order_line` |
| Status / Status Code | `order_status` / `order_status_code` | `fact_sales_order_line` |
| Failure / Failure Reason | `failure_reason` | `fact_sales_order_line` |
| Domestic/International | `domestic_international_flag` | `fact_sales_order_line` |
| PO Number | `customer_po_number` | `fact_sales_order_line` |
| PSR | `psr` | `fact_sales_order_line` |
| Safety Stock Limit | `safety_stock_qty` | `dim_part` |
| Backlog Bucket | *(computed)* | logic on `promised_ship_day_key` |
| Backlog Amount | *(computed)* | (SO Qty − Shipped) × Unit Price |
| OTD Miss (Late) | *(computed)* | Ship Date > Adjusted Promise Date |
| Days Late | *(computed)* | Ship − Promise in days |
| Freight Terms | `freight_terms` | `fact_sales_order_line` |
| Max Lead Time | `max_lead_time_days` | `dim_part` |
| Machine | `machine_type` | `dim_equipment` (to be created) |
| Required Qty | `required_qty` | BOM / `fact_spare_parts_availability` |
| Fillrate % | *(computed)* | fill rate formula |
| FR | *(computed)* | 1/0 hit flag |
| Qty Can Be Filled | *(computed)* | MIN(available_qty, so_qty) |

---

## Gaps: Columns in Dashboards Not Currently in Azure SQL Schema

The following columns appear in the Power BI exports but are **not confirmed** in
`schema_cache.json`. They need to be added to the Azure SQL replica or computed:

| Column | Status | Recommended Action |
|--------|--------|-------------------|
| `available_qty` | Missing | Add to `fact_inventory_on_hand`: on_hand − reserved |
| `estimated_ship_date` | Missing | Add to `fact_sales_order_line` |
| `adjusted_promise_date` | Missing | Add to `fact_sales_order_line` |
| `customer_po_number` | Missing | Add to `fact_sales_order_line` |
| `psr` | Missing | Add to `fact_sales_order_line` |
| `sales_part_code` | Missing | Add to `dim_part` |
| `inventory_part_code` | Missing | Add to `dim_part` |
| `safety_stock_qty` | Missing | Add to `dim_part` |
| `failure_reason` | Missing | Add to `fact_sales_order_line` |
| `drop_ship_flag` | Missing | Add to `fact_sales_order_line` |
| `domestic_international_flag` | Missing | Add to `fact_sales_order_line` |
| `order_status_code` | Missing | Add to `fact_sales_order_line` |
| `max_lead_time_days` | Missing | Add to `dim_part` |
| `min_max_value` | Missing | Add to `dim_part` |
| `machine_type` | Missing | New table `dim_equipment` |
| `required_qty` (BOM) | Missing | New table `fact_spare_parts_availability` |
| `invoiced_qty` | Missing | Add to `fact_ap_invoice_lines` join |
| `list_price_local` | Missing | Add to `dim_part` or `fact_sales_order_line` |
