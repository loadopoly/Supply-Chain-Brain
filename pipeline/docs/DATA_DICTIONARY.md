# Supply Chain Brain — Data Dictionary

> Unified reference for all columns in the Azure SQL replica, Oracle Fusion source
> mappings, and eDap Power BI dashboard exports.
>
> **Scope:** `edap_dw_replica` and `stg_replica` schemas in Azure SQL + dashboard-derived KPIs  
> **Last updated:** 2026-04-20 | Schema source: `pipeline/config/schema_cache.json`  
> **Dashboard source:** `pipeline/docs/EDAP_DASHBOARD_TABLES.md`

---

## Contents

1. [Azure SQL Tables](#azure-sql-tables)
   - [fact_sales_order_line](#fact_sales_order_line)
   - [fact_inventory_on_hand](#fact_inventory_on_hand)
   - [fact_inventory_open_orders](#fact_inventory_open_orders)
   - [fact_po_receipt](#fact_po_receipt)
   - [fact_part_cost](#fact_part_cost-stg_replica)
   - [dim_part](#dim_part)
   - [dim_supplier](#dim_supplier)
2. [Computed / Derived Metrics](#computed--derived-metrics)
3. [Code / Classification Reference](#code--classification-reference)
4. [Dashboard Column Glossary](#dashboard-column-glossary)
5. [Schema Gaps](#schema-gaps--columns-needed)
6. [Date Key Convention](#date-key-convention)

---

## Azure SQL Tables

### `fact_sales_order_line`

**Schema:** `edap_dw_replica`  
**Grain:** One row per shipped or open sales order line  
**Oracle source:** `DOO_LINES_ALL` (Oracle Fusion Order Management)

| Column | Type | Description |
|--------|------|-------------|
| `exchange_rate_key` | int | FK → exchange rate dimension; selects USD conversion rate at order date |
| `business_unit_key` | int | FK → site / plant dimension. Maps to plant name (Chattanooga, McKinney, etc.) |
| `ship_day_key` | int | Actual ship date as YYYYMMDD integer (0 = not shipped). Convert: `TRY_CONVERT(date, TRY_CONVERT(varchar(8), ship_day_key))` |
| `promised_ship_day_key` | int | Customer-committed ship date as YYYYMMDD integer |
| `estimated_ship_date_key` | int | Latest re-promise date (scheduling system); YYYYMMDD integer |
| `requested_ship_date_key` | int | Customer's originally requested ship date; YYYYMMDD integer |
| `order_date_key` | int | Date order was entered into system; YYYYMMDD integer |
| `sales_order_line_classification_key` | int | FK → classification dimension (order type / line class) |
| `customer_key` | int | FK → dim_customer |
| `part_key` | int | FK → dim_part |
| `sales_order_number` | nvarchar | Sales order number from Oracle DOO |
| `sales_order_line` | decimal | Line number within the sales order |
| `shipping_record_id` | nvarchar | Internal shipping / pack slip reference |
| `part_description` | nvarchar | Denormalized part description (at time of order) |
| `sales_order_quantity` | int | Quantity ordered on this line |
| `shipped_quantity` | int | Cumulative quantity shipped to date |
| `serial_number` | nvarchar | Machine / equipment serial number if applicable |
| `product_code_id` | nvarchar | Internal product code / family |
| `model_number` | nvarchar | Equipment model number |
| `currency_code` | char(3) | ISO transaction currency (e.g. USD, CAD, AUD) |
| `unit_cost_local` | decimal(18,4) | Unit cost in transaction currency |
| `unit_cost_usd` | decimal(18,4) | Unit cost converted to USD |
| `extended_cost_local` | decimal(18,4) | `unit_cost_local × sales_order_quantity` |
| `extended_cost_usd` | decimal(18,4) | Extended cost in USD |
| `unit_price_local` | decimal(18,4) | Unit selling price in transaction currency |
| `unit_price_usd` | decimal(18,4) | Unit selling price in USD |
| `extended_price_local` | decimal(18,4) | Gross extended price (before discount) |
| `extended_price_usd` | decimal(18,4) | Gross extended price in USD |
| `customer_discount_local` | decimal(18,4) | Discount amount in transaction currency |
| `customer_discount_usd` | decimal(18,4) | Discount amount in USD |
| `gross_margin_local` | decimal(18,4) | `extended_price − extended_cost` in local currency |
| `gross_margin_usd` | decimal(18,4) | Gross margin in USD |
| `profit_local` | decimal(18,4) | Net profit after discounts, local currency |
| `profit_usd` | decimal(18,4) | Net profit in USD |
| `tax_local` | decimal(18,4) | Tax amount in local currency |
| `tax_usd` | decimal(18,4) | Tax amount in USD |
| `job_complete` | bit | 1 if associated WIP job is complete |
| `dropship` | bit | 1 if this line is a drop-ship (vendor ships direct to customer) |
| `last_supplier_key` | int | FK → dim_supplier; most recent supplier for this part |
| `kit_flag` | bit | 1 if this is a configured kit / BOM item |
| `RMA_REASON` | nvarchar | Return Merchandise Authorization reason code (null = not a return) |
| `freight_terms` | nvarchar | Freight responsibility terms (FOB, Prepaid, etc.) |
| `aud_create_user` | nvarchar | ETL or user that created the record |
| `aud_create_datetime` | datetime | Record creation timestamp |
| `aud_update_user` | nvarchar | ETL or user that last updated the record |
| `aud_update_datetime` | datetime | Record last-updated timestamp |

**Notes:**  
- All `_key` date columns are YYYYMMDD integers. A value of `0` or `NULL` means the date is unknown/not set.  
- `shipped_quantity < sales_order_quantity` = open backlog.  
- `ship_day_key > promised_ship_day_key` = late delivery (OTD miss).

---

### `fact_inventory_on_hand`

**Schema:** `edap_dw_replica`  
**Grain:** One row per site × part × snapshot date  
**Oracle source:** `INV_ONHAND_QUANTITIES_DETAIL`

| Column | Type | Description |
|--------|------|-------------|
| `last_supplier_key` | int | FK → dim_supplier; last vendor to supply this part |
| `snapshot_day_key` | int | Inventory snapshot date as YYYYMMDD integer |
| `exchange_rate_key` | int | FK → exchange rate dimension |
| `business_unit_key` | int | FK → site / plant |
| `part_key` | int | FK → dim_part |
| `last_supplier_address` | nvarchar | Supplier address (denormalized) |
| `last_supplier_number` | nvarchar | Supplier vendor number |
| `oem_name` | nvarchar | OEM manufacturer name if part is OEM-sourced |
| `oem_part_desc` | nvarchar | OEM part description |
| `oem_part_number` | nvarchar | OEM's part number |
| `inventory_uom` | nvarchar | Unit of measure (EA, FT, LB, etc.) |
| `currency_code` | char(3) | Transaction currency |
| `part_price_local` | decimal(18,4) | Current part price in local currency |
| `part_price_usd` | decimal(18,4) | Current part price in USD |
| `order_lead_time` | int | Replenishment lead time in days |
| `quantity_on_hand` | int | Current on-hand quantity (mapped in dashboards as "On Hand Qty") |
| `safety_stock_limit` | int | Minimum stocking quantity (safety stock). Dashboards call this "Safety Stock Limit" |
| `part_quantity_max` | int | Max reorder quantity (min/max policy) |
| `part_quantity_min` | int | Min reorder quantity (min/max policy) |
| `part_order_quantity_max` | int | Max order quantity allowed per PO |
| `part_order_quantity_min` | int | Min order quantity (MOQ) |
| `demand_order_part_quantity` | int | Total open demand (sales orders) for this part at this site |
| `pre_standardization_supplier_name` | nvarchar | Supplier name before master data cleanse; use `dim_supplier.supplier_name` when possible |
| `last_paid_price_date` | date | Date of most recent paid invoice for this part |
| `demand_order_count` | int | Count of open demand orders for this part |
| `domestic_international` | nvarchar | "Domestic" or "International" — part origin |
| `aud_create_user` | nvarchar | ETL or user that created the record |
| `aud_create_datetime` | datetime | Record creation timestamp |
| `aud_update_user` | nvarchar | ETL or user that last updated |
| `aud_update_datetime` | datetime | Record last-updated timestamp |

**Notes:**  
- `quantity_on_hand − demand_order_part_quantity` ≈ available quantity (not stored; compute at query time).  
- `safety_stock_limit` in this table matches "Safety Stock Limit" in Sales Backlog / Sales Details dashboards.

---

### `fact_inventory_open_orders`

**Schema:** `edap_dw_replica`  
**Grain:** One row per open PO line (quantity not yet received)  
**Oracle source:** `PO_LINE_LOCATIONS_ALL` (Oracle Fusion Procurement)

| Column | Type | Description |
|--------|------|-------------|
| `part_key` | int | FK → dim_part |
| `snapshot_day_key` | int | Snapshot date as YYYYMMDD integer |
| `order_date_key` | int | PO order date as YYYYMMDD integer |
| `business_unit_key` | int | FK → site / plant |
| `supplier_key` | int | FK → dim_supplier |
| `exchange_rate_key` | int | FK → exchange rate |
| `due_date_key` | int | PO due / need-by date as YYYYMMDD integer |
| `po_number` | nvarchar | Purchase order number |
| `po_line_number` | int | Line number within the PO |
| `po_release` | int | Blanket PO release number (null if not a blanket) |
| `quantity_not_received` | int | Open quantity (ordered − received). Col_resolver alias: `open_order_qty` |
| `currency_code` | char(3) | Transaction currency |
| `amount_not_received_local` | decimal(18,4) | Open value in local currency |
| `amount_not_received_usd` | decimal(18,4) | Open value in USD |
| `supplier_id` | nvarchar | Supplier identifier from Oracle |
| `supplier_part_number` | nvarchar | Supplier's own part number |
| `pre_standardization_supplier_name` | nvarchar | Raw supplier name before MDM cleanse |
| `aud_create_user` | nvarchar | ETL or user that created the record |
| `aud_create_datetime` | datetime | Record creation timestamp |
| `aud_update_user` | nvarchar | ETL or user that last updated |
| `aud_update_datetime` | datetime | Record last-updated timestamp |

---

### `fact_po_receipt`

**Schema:** `edap_dw_replica`  
**Grain:** One row per PO receipt transaction  
**Oracle source:** `RCV_TRANSACTIONS` (Oracle Fusion Receiving)

| Column | Type | Description |
|--------|------|-------------|
| `supplier_key` | int | FK → dim_supplier |
| `receipt_date_key` | int | Date goods were received as YYYYMMDD integer |
| `exchange_rate_key` | int | FK → exchange rate |
| `business_unit_key` | int | FK → site / plant |
| `business_unit_gl_account_key` | int | GL account for this receipt |
| `part_key` | int | FK → dim_part |
| `due_date_key` | int | Original PO need-by date as YYYYMMDD integer |
| `direct_purchase_key` | int | Direct purchase order reference key |
| `po_number` | nvarchar | Purchase order number |
| `po_line_number` | int | Line within the PO |
| `po_release` | int | Blanket release number |
| `vendor_part_number` | nvarchar | Supplier's part number |
| `part_description` | nvarchar | Denormalized part description at time of receipt |
| `part_description_2` | nvarchar | Secondary description line |
| `unit_cost_local` | decimal(18,4) | Unit cost at receipt in local currency |
| `unit_cost_usd` | decimal(18,4) | Unit cost in USD |
| `received_qty` | int | Quantity received in this transaction |
| `receipt_document_number` | nvarchar | Receipt document reference number |
| `receipt_document_line_number` | int | Line number on the receipt document |
| `pre_standardization_supplier_name` | nvarchar | Raw supplier name |
| `buyer_id` | nvarchar | Buyer responsible for this PO |
| `po_uom` | nvarchar | PO unit of measure |
| `inventory_uom` | nvarchar | Inventory unit of measure (may differ) |
| `order_qty` | int | Original PO order quantity |
| `aud_create_user` | nvarchar | ETL or user that created the record |
| `aud_create_datetime` | datetime | Record creation timestamp |
| `aud_update_user` | nvarchar | ETL or user that last updated |
| `aud_update_datetime` | datetime | Record last-updated timestamp |

---

### `fact_part_cost` (`stg_replica`)

**Schema:** `stg_replica`  
**Grain:** One row per site × part × cost type (effective date range)  
**Oracle source:** `CST_COST_DETAILS` (Oracle Fusion Cost Management)

| Column | Type | Description |
|--------|------|-------------|
| `business_unit_key` | int | FK → site / plant |
| `part_key` | int | FK → dim_part |
| `cost_type` | nvarchar | "Standard", "Actual", "Frozen", etc. |
| `plant_code` | nvarchar | Manufacturing plant code |
| `currency_code` | char(3) | Transaction currency |
| `exchange_rate_key` | int | FK → exchange rate |
| `uom` | nvarchar | Unit of measure |
| `fabricated_purchased` | nvarchar | "fabricated" or "purchased" |
| `cost_amount_local` | decimal(18,4) | Total unit cost in local currency |
| `mtl_cost_local` | decimal(18,4) | Material cost component |
| `labor_cost_local` | decimal(18,4) | Labor cost component |
| `burden_cost_local` | decimal(18,4) | Overhead/burden cost component |
| `sub_cont_cost_local` | decimal(18,4) | Subcontracting cost component |
| `misc_cost_local` | decimal(18,4) | Miscellaneous cost component |
| `freight_cost_local` | decimal(18,4) | Inbound freight cost component |
| `total_cost_amount_usd` | decimal(18,4) | Total cost converted to USD |
| `mtl_cost_usd` | decimal(18,4) | Material cost in USD |
| `labor_cost_usd` | decimal(18,4) | Labor cost in USD |
| `burden_cost_usd` | decimal(18,4) | Burden cost in USD |
| `sub_cont_cost_usd` | decimal(18,4) | Subcontracting cost in USD |
| `misc_cost_usd` | decimal(18,4) | Misc cost in USD |
| `freight_cost_usd` | decimal(18,4) | Freight cost in USD |
| `effective_date` | date | Cost record effective from date |
| `expiry_date` | date | Cost record valid through date (null = current) |
| `current_record_ind` | bit | 1 = currently active cost record |
| `aud_create_user` | nvarchar | ETL or user that created the record |
| `aud_create_datetime` | datetime | Record creation timestamp |
| `aud_update_user` | nvarchar | ETL or user that last updated |
| `aud_update_datetime` | datetime | Record last-updated timestamp |

---

### `dim_part`

**Schema:** `edap_dw_replica`  
**Grain:** One row per site × part (SCD Type 2 — historical versions tracked)  
**Oracle source:** `EGP_SYSTEM_ITEMS_B` (Oracle Fusion Item Master)

| Column | Type | Description |
|--------|------|-------------|
| `part_key` | int | Surrogate primary key |
| `business_unit_id` | int | Site / plant identifier |
| `part_type` | nvarchar | Classification: "Make", "Buy", "Phantom", etc. |
| `part_number` | nvarchar | Astec internal part number (natural key) |
| `part_description` | nvarchar | Primary description from item master |
| `part_description_2` | nvarchar | Secondary description line |
| `part_notes` | nvarchar | Free-text notes from item master |
| `unspsc_number` | nvarchar | UNSPSC commodity classification code |
| `oem_name` | nvarchar | Original equipment manufacturer name |
| `oem_part_number` | nvarchar | OEM's part number |
| `level_1_category` | nvarchar | Top-level procurement category |
| `level_2_category` | nvarchar | Sub-category |
| `level_3_category` | nvarchar | Detail category |
| `category_manager` | nvarchar | Buyer / category manager name |
| `categories_verified` | bit | 1 if category assignment has been validated |
| `packaging_string` | nvarchar | Packaging description |
| `uom` | nvarchar | Primary unit of measure |
| `effective_date` | date | SCD record valid from date |
| `expiry_date` | date | SCD record valid through date (null = current) |
| `current_record_ind` | bit | 1 = current active version |
| `list_price_local` | decimal(18,4) | Published list price in local currency |
| `list_price_currency_code` | char(3) | Currency for list price |
| `part_group` | nvarchar | Part group code |
| `part_group_name` | nvarchar | Part group display name |
| `aud_create_user` | nvarchar | ETL or user that created the record |
| `aud_create_datetime` | datetime | Record creation timestamp |
| `aud_update_user` | nvarchar | ETL or user that last updated |
| `aud_update_datetime` | datetime | Record last-updated timestamp |

**Gap — columns in dashboards, not in current schema:**

| Missing Column | Source Dashboard | Recommended Home |
|---------------|-----------------|------------------|
| `sales_part_code` | Fill Rate, OTD ABC, Sales Backlog | Add to `dim_part` |
| `inventory_part_code` | Sales Part Code, OTD | Add to `dim_part` |
| `fabricated_purchased` | Fill Rate, OTD ABC, Sales Backlog | Add to `dim_part` |
| `max_lead_time_days` | Fill Rate Report | Add to `dim_part` |
| `min_max_value` | Fill Rate Report | Add to `dim_part` |

---

### `dim_supplier`

**Schema:** `edap_dw_replica`  
**Grain:** One row per supplier  
**Oracle source:** `POZ_SUPPLIERS` (Oracle Fusion Supplier Master)

| Column | Type | Description |
|--------|------|-------------|
| `supplier_key` | int | Surrogate primary key |
| `business_unit_id` | int | Site / plant context |
| `supplier_name` | nvarchar | Standardized supplier display name |
| `supplier_address` | nvarchar | Street address |
| `supplier_city` | nvarchar | City |
| `supplier_state` | nvarchar | State / province |
| `supplier_country` | nvarchar | Country |
| `supplier_zip_code` | nvarchar | Postal / ZIP code |
| `aud_create_user` | nvarchar | ETL or user that created the record |
| `aud_create_datetime` | datetime | Record creation timestamp |
| `aud_update_user` | nvarchar | ETL or user that last updated |
| `aud_update_datetime` | datetime | Record last-updated timestamp |

---

## Computed / Derived Metrics

These columns do not exist in Azure SQL — they are computed at query time by the
Supply Chain Brain app or Power BI.

| Metric | Formula | Source Columns | Dashboards |
|--------|---------|----------------|------------|
| `fillrate_pct` | `CASE WHEN quantity_on_hand >= sales_order_quantity THEN 100.0 ELSE (CAST(quantity_on_hand AS float) / sales_order_quantity * 100) END` | `quantity_on_hand`, `sales_order_quantity` | Fill Rate |
| `hit_miss_flag` | `CASE WHEN quantity_on_hand >= sales_order_quantity THEN 'Hit' ELSE 'Miss' END` | `quantity_on_hand`, `sales_order_quantity` | Fill Rate |
| `fr` | `CASE WHEN quantity_on_hand >= sales_order_quantity THEN 1 ELSE 0 END` | `quantity_on_hand`, `sales_order_quantity` | Fill Rate |
| `qty_can_be_filled` | `MIN(available_qty, sales_order_quantity)` | `available_qty`, `sales_order_quantity` | Fill Rate |
| `available_qty` | `quantity_on_hand - demand_order_part_quantity` | `fact_inventory_on_hand` | All dashboards |
| `backlog_qty` | `sales_order_quantity - shipped_quantity` | `fact_sales_order_line` | Sales Backlog |
| `backlog_amount` | `(sales_order_quantity - shipped_quantity) × unit_price_local` | `fact_sales_order_line` | Sales Backlog |
| `backlog_bucket` | `CASE WHEN promise < TODAY() THEN 'Past Due' WHEN promise <= TODAY()+30 THEN 'Current' ELSE 'Future' END` | `promised_ship_day_key` | Sales Backlog |
| `otd_miss_late` | `CASE WHEN ship_day_key > adjusted_promise_date THEN 1 ELSE 0 END` | `ship_day_key`, `adjusted_promise_date` | OTD ABC |
| `days_late` | `DATEDIFF(day, adjusted_promise_date, ship_day_key)` (positive = late) | `ship_day_key`, `adjusted_promise_date` | OTD ABC |
| `lead_time_date` | `order_date + max_lead_time_days` | `order_date_key`, `max_lead_time_days` | Fill Rate |
| `filled_flag` (spare parts) | `CASE WHEN quantity_on_hand >= required_qty THEN 'Yes' ELSE 'No' END` | `quantity_on_hand`, `required_qty` | Spare Parts Availability |

---

## Code / Classification Reference

### Sales Part Code (ABC Classification)

Used in dashboards as "Part Class". Source: eDap ABC classification model.

| Code | Name | Description |
|------|------|-------------|
| `A Prime` | A Prime | Highest velocity and/or highest value; always maintain safety stock |
| `A` | A | High velocity; stock to min/max levels |
| `B Prime` | B Prime | Medium velocity, high value; stock to reorder point |
| `B` | B | Medium velocity; stock to reorder point |
| `C Low Volume` | C Low Volume | Low velocity; stock selectively based on lead time risk |
| `C` | C | Low velocity; order on demand |
| `D New` | D New | New part introduction; insufficient demand history |
| `E` | E | End-of-life or superseded; deplete existing stock only |

### Inventory Part Code (Standard ABC)

Used internally for inventory prioritization and cycle counting frequency.

| Code | Description |
|------|-------------|
| `A` | Top ~20% of SKUs driving ~80% of demand value; cycle count monthly |
| `B` | Middle tier (~30% of SKUs, ~15% of value); cycle count quarterly |
| `C` | Bottom tier (~50% of SKUs, ~5% of value); cycle count annually |

### Backlog Bucket

| Value | Definition |
|-------|-----------|
| `Past Due` | Promised Ship Date < Today (overdue) |
| `Current` | Promised Ship Date ≥ Today and ≤ Today + 30 days |
| `Future` | Promised Ship Date > Today + 30 days |

### Order Status Codes

| Value | Description |
|-------|-------------|
| `OPEN` | Active open order; not yet shipped |
| `CLOSED` | Order fully shipped / closed |
| `CANCELLED` | Order cancelled |
| `"This is a closed order"` | Verbose status text used in some Fill Rate dashboard rows |

### OTD Failure Reason Codes

| Value | Description |
|-------|-------------|
| `Not Implemented` | No failure code recorded; placeholder value |
| *(other codes TBD)* | Root-cause codes for late delivery analysis |

### Freight Terms

| Value | Description |
|-------|-------------|
| `Not Implemented` | No freight terms recorded; placeholder |
| `FOB` | Free on board — customer pays freight |
| `Prepaid` | Astec pays freight |
| *(others as configured in Oracle)* | |

### Domestic / International

| Value | Description |
|-------|-------------|
| `Domestic` | US-based customer or destination |
| `International` | Non-US customer or destination |

---

## Dashboard Column Glossary

Alphabetical reference of all columns appearing in Power BI exports, with their
meaning and Azure SQL mapping.

| Column Name (Dashboard) | Azure SQL Column | Table | Notes |
|------------------------|-----------------|-------|-------|
| Adjusted Promise Date | `adjusted_promise_date` | `fact_sales_order_line` | Gap — not yet in schema |
| Available Qty | *(computed)* | — | `quantity_on_hand − demand_order_part_quantity` |
| Backlog Amount | *(computed)* | — | `(SO Qty − Shipped Qty) × unit_price_local` |
| Backlog Bucket | *(computed)* | — | Past Due / Current / Future by promise date |
| Customer | `customer_name` | `dim_customer` | |
| Customer No | `customer_number` | `dim_customer` | |
| Days Late | *(computed)* | — | `ship_day_key − adjusted_promise_date` |
| Description | `part_description` | `dim_part` | |
| Domestic/International | `domestic_international` | `fact_inventory_on_hand` | |
| Drop Ship | `dropship` | `fact_sales_order_line` | Column name: `dropship` (bit) |
| Estimated Ship Date | `estimated_ship_date_key` | `fact_sales_order_line` | YYYYMMDD key |
| Ext Price With Discount | `extended_price_local` | `fact_sales_order_line` | After customer discount |
| Failure / Failure Reason | `failure_reason` | `fact_sales_order_line` | Gap — not yet in schema |
| Fillrate % | *(computed)* | — | Fill rate formula |
| FR | *(computed)* | — | 1=Hit, 0=Miss |
| Freight Terms | `freight_terms` | `fact_sales_order_line` | |
| Hit Miss | *(computed)* | — | "Hit" or "Miss" string |
| Inventory Part Code | `inventory_part_code` | `dim_part` | Gap — not yet in schema |
| Invoiced Qty | `invoiced_qty` | `fact_ap_invoice_lines` | Gap — join to AP table |
| Last Ship Date | `last_ship_date` | `fact_sales_order_line` | Gap — not yet in schema |
| Lead Time Date | *(computed)* | — | `order_date + max_lead_time_days` |
| Line No | `sales_order_line` | `fact_sales_order_line` | |
| List Price | `list_price_local` | `dim_part` | |
| Machine | `machine_type` | `dim_equipment` | Gap — table doesn't exist yet |
| Max Lead Time | `max_lead_time_days` | `dim_part` | Gap — not yet in schema |
| Max Min Value | `min_max_value` | `dim_part` | Gap — not yet in schema |
| OTD Miss (Late) | *(computed)* | — | 1=late, 0=on time |
| On Hand Qty | `quantity_on_hand` | `fact_inventory_on_hand` | |
| Order Date | `order_date_key` | `fact_sales_order_line` | YYYYMMDD int |
| Order Type | `order_type` | `fact_sales_order_line` | Gap — not yet in schema |
| Part / Part# | `part_number` | `dim_part` | |
| Part Class | `sales_part_code` | `dim_part` | Gap — not yet in schema |
| Part Fab/Pur | `fabricated_purchased` | `fact_part_cost` / `dim_part` | Available in `stg_replica.fact_part_cost` |
| PO Number | `customer_po_number` | `fact_sales_order_line` | Gap — not yet in schema |
| Price / Unit Price | `unit_price_local` | `fact_sales_order_line` | |
| Promised Date | `promised_ship_day_key` | `fact_sales_order_line` | YYYYMMDD int |
| PSR | `psr` | `fact_sales_order_line` | Product Sales Rep — gap |
| Qty / SO Qty | `sales_order_quantity` | `fact_sales_order_line` | |
| Qty Can Be Filled | *(computed)* | — | `MIN(available_qty, sales_order_quantity)` |
| Required Qty | `required_qty` | BOM table | Gap — no BOM table exists |
| Requested Date | `requested_ship_date_key` | `fact_sales_order_line` | YYYYMMDD int |
| Safety Stock Limit | `safety_stock_limit` | `fact_inventory_on_hand` | ✓ exists |
| Sales Order Line | `sales_order_line` | `fact_sales_order_line` | |
| Sales Order Number | `sales_order_number` | `fact_sales_order_line` | |
| Sales Order Quantity | `sales_order_quantity` | `fact_sales_order_line` | |
| Sales Part Code | `sales_part_code` | `dim_part` | Gap — not yet in schema |
| Ship Date | `ship_day_key` | `fact_sales_order_line` | YYYYMMDD int |
| Ship To Country | `ship_to_country` | `dim_customer` | Gap — not in confirmed schema |
| Shipped Quantity | `shipped_quantity` | `fact_sales_order_line` | |
| Site | `business_unit_key` | multiple | int key; join to dim_business_unit for name |
| SO No | `sales_order_number` | `fact_sales_order_line` | |
| Status / Status Code | `order_status_code` | `fact_sales_order_line` | Gap — not yet in schema |
| Supplier Name | `supplier_name` | `dim_supplier` | |

---

## Schema Gaps — Columns Needed

Columns confirmed in Power BI exports that are **not present** in `schema_cache.json`.
These represent the delta between what's in the data warehouse and what the dashboards use.

| Column | Type | Recommended Table | Priority | Notes |
|--------|------|------------------|----------|-------|
| `sales_part_code` | nvarchar | `dim_part` | HIGH | ABC code used across all dashboards |
| `inventory_part_code` | nvarchar | `dim_part` | HIGH | Standard ABC (A/B/C) for cycle counting |
| `adjusted_promise_date` | int (YYYYMMDD) | `fact_sales_order_line` | HIGH | Required for OTD Miss computation |
| `failure_reason` | nvarchar | `fact_sales_order_line` | HIGH | OTD root-cause analysis |
| `order_status_code` | nvarchar | `fact_sales_order_line` | HIGH | OPEN/CLOSED/CANCELLED |
| `available_qty` | int | `fact_inventory_on_hand` | HIGH | on_hand − demand_order_part_quantity |
| `customer_po_number` | nvarchar | `fact_sales_order_line` | MEDIUM | Customer PO reference |
| `psr` | nvarchar | `fact_sales_order_line` | MEDIUM | Product Sales Representative |
| `order_type` | nvarchar | `fact_sales_order_line` | MEDIUM | Parts / Machine / Equipment |
| `max_lead_time_days` | int | `dim_part` | MEDIUM | Max component lead time |
| `min_max_value` | decimal | `dim_part` | MEDIUM | Min/max stocking level value |
| `fabricated_purchased` | nvarchar | `dim_part` | MEDIUM | Already in `fact_part_cost`; add to `dim_part` |
| `last_ship_date` | date | `fact_sales_order_line` | MEDIUM | Last partial ship date |
| `safety_stock_qty` | int | `dim_part` | LOW | Already in `fact_inventory_on_hand`; denorm to dim |
| `drop_ship_flag` | nvarchar | `fact_sales_order_line` | LOW | Text version of `dropship` bit |
| `domestic_international_flag` | nvarchar | `fact_sales_order_line` | LOW | Domestic/International text |
| `ship_to_country` | nvarchar | `dim_customer` | LOW | Destination country |
| `machine_type` | nvarchar | new `dim_equipment` | LOW | For Spare Parts dashboard |
| `required_qty` | int | new `fact_spare_parts_availability` | LOW | BOM qty per machine |
| `invoiced_qty` | int | `fact_ap_invoice_lines` | LOW | Join to AP table needed |

---

## Date Key Convention

All date foreign keys in Azure SQL `edap_dw_replica` are stored as **YYYYMMDD integers**.

```sql
-- Convert YYYYMMDD integer to DATE (SQL Server):
TRY_CONVERT(date, CONVERT(varchar(8), CAST(date_key AS bigint)), 112)
--  ↑ Style 112 = YYYYMMDD format string. Must CAST to bigint first (some keys stored as numeric/decimal).
--  TRY_CONVERT(date, 20240115) silently returns NULL — integer → date has no implicit path.

-- Validity guard (exclude sentinel / null keys):
CAST(date_key AS bigint) BETWEEN 19000101 AND 21001231

-- Python / pandas:
pd.to_datetime(series.astype(str), format="%Y%m%d", errors="coerce")

-- Examples:
-- 20240115 → 2024-01-15
-- 0        → NULL (unknown / not set)
-- 99991231 → end-of-time sentinel (some SCD records)
```

**Date key columns by table:**

| Column | Table |
|--------|-------|
| `order_date_key` | `fact_sales_order_line`, `fact_inventory_open_orders`, `fact_po_receipt` |
| `ship_day_key` | `fact_sales_order_line` |
| `promised_ship_day_key` | `fact_sales_order_line` |
| `estimated_ship_date_key` | `fact_sales_order_line` |
| `requested_ship_date_key` | `fact_sales_order_line` |
| `receipt_date_key` | `fact_po_receipt` |
| `due_date_key` | `fact_po_receipt`, `fact_inventory_open_orders` |
| `snapshot_day_key` | `fact_inventory_on_hand`, `fact_inventory_open_orders` |

---

## Oracle Fusion Cloud — Source Data Dictionary

> **Connection:** BIP REST API on `fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com`  
> **Schema:** `FUSION` (all tables)  
> **Auth:** OAM session cookie (same-browser SSO). See `config/oracle_session.json`.  
> **Naming convention:** Oracle column names are UPPER_SNAKE_CASE. All map to lower_snake_case in Azure SQL per `config/mappings.yaml`.  
> **Validated against:** `config/mappings.yaml` + `config/schema_cache.json` + Power BI dashboard exports

---

### `POZ_SUPPLIERS` → `edap_dw_replica.dim_supplier`

**Business entity:** Supplier / Vendor master  
**Grain:** One row per registered supplier  
**Join key:** `VENDOR_ID` = `supplier_key`

| Oracle Column | Type | Azure SQL Column | Description |
|--------------|------|-----------------|-------------|
| `VENDOR_ID` | NUMBER | `supplier_key` | Surrogate PK (Oracle auto-assigned vendor ID) |
| `VENDOR_NAME` | VARCHAR2(360) | `supplier_name` | Official supplier display name |
| `CREATION_DATE` | DATE | `aud_create_datetime` | Record creation date |
| `LAST_UPDATE_DATE` | DATE | `aud_update_datetime` | Record last updated |

---

### `POZ_SUPPLIER_SITES_ALL_M` → `edap_dw_replica.dim_supplier` (address extension)

**Business entity:** Supplier remittance / ship-from address  
**Grain:** One row per supplier site (a supplier may have multiple sites)

| Oracle Column | Type | Azure SQL Column | Description |
|--------------|------|-----------------|-------------|
| `VENDOR_ID` | NUMBER | `supplier_key` | FK → `POZ_SUPPLIERS.VENDOR_ID` |
| `VENDOR_SITE_ID` | NUMBER | *(merge key)* | Site identifier (not stored in dim_supplier) |
| `ADDRESS_LINE1` | VARCHAR2(240) | `supplier_address` | Street address line 1 |
| `CITY` | VARCHAR2(25) | `supplier_city` | City |
| `STATE` | VARCHAR2(150) | `supplier_state` | State / province |
| `COUNTRY` | VARCHAR2(25) | `supplier_country` | ISO country code (e.g. US, CA, MX) |
| `ZIP_CODE` | VARCHAR2(20) | `supplier_zip_code` | Postal / ZIP code |

---

### `EGP_SYSTEM_ITEMS_B` → `edap_dw_replica.dim_part`

**Business entity:** Item / Part master  
**Grain:** One row per item × organization (site)  
**Join key:** `INVENTORY_ITEM_ID` = `part_key`, `ORGANIZATION_ID` = `business_unit_id`

| Oracle Column | Type | Azure SQL Column | Description |
|--------------|------|-----------------|-------------|
| `INVENTORY_ITEM_ID` | NUMBER | `part_key` | Oracle auto-assigned item ID; surrogate PK in Azure |
| `SEGMENT1` | VARCHAR2(40) | `part_number` | Astec internal part number (natural key) |
| `DESCRIPTION` | VARCHAR2(240) | `part_description` | Primary item description |
| `LONG_DESCRIPTION` | VARCHAR2(4000) | `part_description_2` | Extended description |
| `ITEM_TYPE` | VARCHAR2(30) | `part_type` | Oracle item type code (e.g. Purchased Item, Make Item) |
| `PRIMARY_UOM_CODE` | VARCHAR2(3) | `uom` | Primary unit of measure (EA, FT, LB, etc.) |
| `PURCHASING_ITEM_FLAG` | VARCHAR2(1) | `fabricated_purchased` | Y = Purchased, N = Fabricated/Make |
| `ORGANIZATION_ID` | NUMBER | `business_unit_id` | Inventory organization (site/plant) |
| `CREATION_DATE` | DATE | `aud_create_datetime` | Record creation date |
| `LAST_UPDATE_DATE` | DATE | `aud_update_datetime` | Record last updated |

**Note:** `SEGMENT1` is the Astec part number displayed in all dashboards as "Part" or "Part#". Always join via `part_key` for performance; use `SEGMENT1` only for display.

---

### `INV_ONHAND_QUANTITIES_DETAIL` → `edap_dw_replica.fact_inventory_on_hand`

**Business entity:** Current inventory snapshot  
**Grain:** One row per item × organization × subinventory  
**Join key:** `INVENTORY_ITEM_ID` = `part_key`, `ORGANIZATION_ID` = `business_unit_key`

| Oracle Column | Type | Azure SQL Column | Description |
|--------------|------|-----------------|-------------|
| `INVENTORY_ITEM_ID` | NUMBER | `part_key` | FK → item master |
| `ORGANIZATION_ID` | NUMBER | `business_unit_key` | Inventory organization |
| `PRIMARY_TRANSACTION_QUANTITY` | NUMBER | `quantity_on_hand` | On-hand quantity in primary UOM |
| `SUBINVENTORY_CODE` | VARCHAR2(10) | `oem_name` | Sub-inventory / storage location (repurposed column) |
| `LAST_UPDATE_DATE` | DATE | `aud_update_datetime` | Snapshot timestamp |

**Note:** `PRIMARY_TRANSACTION_QUANTITY` maps to `quantity_on_hand` in Azure SQL — the canonical "On Hand Qty" KPI. The App's col_resolver resolves `on_hand_qty` → `quantity_on_hand` as first match.

---

### `PO_LINE_LOCATIONS_ALL` → `edap_dw_replica.fact_inventory_open_orders`

**Business entity:** Open purchase order schedule lines (unreceived)  
**Grain:** One row per PO schedule / shipment line  
**Join key:** `LINE_LOCATION_ID` (receipt schedule ID)

| Oracle Column | Type | Azure SQL Column | Description |
|--------------|------|-----------------|-------------|
| `PO_HEADER_ID` | NUMBER | `po_number` | PO header reference |
| `PO_LINE_ID` | NUMBER | `po_line_number` | PO line reference |
| `RELEASE_NUM` | NUMBER | `po_release` | Blanket release number (null = standard PO) |
| `INVENTORY_ITEM_ID` | NUMBER | `part_key` | FK → item master |
| `VENDOR_ID` | NUMBER | `supplier_key` | FK → supplier master |
| `NEED_BY_DATE` | DATE | `due_date_key` | Date goods are required (stored as YYYYMMDD int in Azure) |
| `QUANTITY` | NUMBER | `quantity_not_received` | Open quantity (ordered − received). Dashboard: "Open Order Qty" |
| `AMOUNT` | NUMBER | `amount_not_received_local` | Open dollar value in transaction currency |
| `CURRENCY_CODE` | VARCHAR2(15) | `currency_code` | Transaction currency |
| `ORG_ID` | NUMBER | `business_unit_key` | Operating unit / site |
| `CREATION_DATE` | DATE | `order_date_key` | PO creation date (stored as YYYYMMDD int) |
| `LAST_UPDATE_DATE` | DATE | `aud_update_datetime` | Record last updated |

---

### `WIP_DISCRETE_JOBS` → `edap_dw_replica.fact_inventory_open_mfg_orders`

**Business entity:** Open manufacturing / work-in-process jobs  
**Grain:** One row per discrete WIP job

| Oracle Column | Type | Azure SQL Column | Description |
|--------------|------|-----------------|-------------|
| `WIP_ENTITY_ID` | NUMBER | `mfg_order_key` | WIP job surrogate key |
| `INVENTORY_ITEM_ID` | NUMBER | `part_key` | Item being manufactured |
| `ORGANIZATION_ID` | NUMBER | `business_unit_key` | Manufacturing plant |
| `STATUS_TYPE` | NUMBER | `order_status` | 1=Unreleased, 3=Released, 4=Complete, 5=Complete/No Charges, 6=Hold, 7=Cancelled |
| `START_QUANTITY` | NUMBER | `order_qty` | Planned production quantity |
| `QUANTITY_COMPLETED` | NUMBER | `qty_completed` | Quantity completed to date |
| `SCHEDULED_COMPLETION_DATE` | DATE | `due_date_key` | Expected completion date (YYYYMMDD int) |
| `DATE_RELEASED` | DATE | `order_date_key` | Job release date (YYYYMMDD int) |
| `CLASS_CODE` | VARCHAR2(10) | `order_type` | Work order class (Discrete, Repetitive, etc.) |
| `LAST_UPDATE_DATE` | DATE | `aud_update_datetime` | Record last updated |

---

### `RCV_TRANSACTIONS` → `edap_dw_replica.fact_po_receipt`

**Business entity:** Purchase order receipt transactions  
**Grain:** One row per receiving transaction (each receipt event)  
**Join key:** `TRANSACTION_ID` (unique receipt transaction ID)  
**OTD source:** This table is the primary source for On-Time Delivery analysis  
(`TRANSACTION_DATE` vs `EXPECTED_RECEIPT_DATE`)

| Oracle Column | Type | Azure SQL Column | Description |
|--------------|------|-----------------|-------------|
| `PO_HEADER_ID` | NUMBER | `po_number` | PO header reference |
| `PO_LINE_ID` | NUMBER | `po_line_number` | PO line reference |
| `PO_RELEASE_ID` | NUMBER | `po_release` | Blanket release number |
| `VENDOR_ID` | NUMBER | `supplier_key` | FK → supplier master |
| `INVENTORY_ITEM_ID` | NUMBER | `part_key` | FK → item master |
| `ITEM_DESCRIPTION` | VARCHAR2(240) | `part_description` | Part description at time of receipt |
| `UNIT_OF_MEASURE` | VARCHAR2(25) | `po_uom` | PO unit of measure |
| `PRIMARY_UNIT_OF_MEAS` | VARCHAR2(25) | `inventory_uom` | Inventory UOM (may differ from PO UOM) |
| `QUANTITY` | NUMBER | `received_qty` | Quantity received in this transaction |
| `TRANSACTION_DATE` | DATE | `receipt_date_key` | Actual receipt date (YYYYMMDD int in Azure) |
| `EXPECTED_RECEIPT_DATE` | DATE | `due_date_key` | Original PO need-by / promised date (YYYYMMDD int) |
| `CREATION_DATE` | DATE | `order_date_key` | PO creation date (YYYYMMDD int) |
| `ACTUAL_COST` | NUMBER | `unit_cost_local` | Unit cost at receipt in local currency |
| `CURRENCY_CODE` | VARCHAR2(15) | `currency_code` | Transaction currency |
| `VENDOR_ITEM_NUM` | VARCHAR2(25) | `vendor_part_number` | Supplier's part number |
| `DOCUMENT_NUM` | NUMBER | `receipt_document_number` | Receipt slip / document number |
| `SHIPMENT_LINE_ID` | NUMBER | `receipt_document_line_number` | Receipt document line |
| `ORGANIZATION_ID` | NUMBER | `business_unit_key` | Receiving organization / site |
| `AGENT_ID` | NUMBER | `buyer_id` | Purchasing agent / buyer ID |

**OTD calculation:**  
`days_late = TRANSACTION_DATE − EXPECTED_RECEIPT_DATE` (positive = late, negative = early)  
`is_on_time = 1 WHEN TRANSACTION_DATE ≤ EXPECTED_RECEIPT_DATE ELSE 0`

---

### `PO_HEADERS_ALL` + `PO_LINES_ALL` → `edap_dw_replica.fact_po_contract_part`

**Business entity:** Purchase order contracts (blankets, standards, and agreements)  
**Grain:** Header: one row per PO. Lines: one row per PO line (part).

#### PO_HEADERS_ALL

| Oracle Column | Type | Azure SQL Column | Description |
|--------------|------|-----------------|-------------|
| `PO_HEADER_ID` | NUMBER | `po_header_key` | PO header surrogate key |
| `SEGMENT1` | VARCHAR2(20) | `po_number` | Human-readable PO number |
| `VENDOR_ID` | NUMBER | `supplier_key` | FK → supplier master |
| `CURRENCY_CODE` | VARCHAR2(15) | `currency_code` | PO currency |
| `TYPE_LOOKUP_CODE` | VARCHAR2(25) | `po_type` | BLANKET, CONTRACT, or STANDARD |
| `APPROVED_DATE` | DATE | `approved_date` | Date PO was approved |
| `AMOUNT_LIMIT` | NUMBER | `blanket_amount_limit` | Total blanket not-to-exceed amount |
| `ORG_ID` | NUMBER | `business_unit_key` | Operating unit |
| `CREATION_DATE` | DATE | `aud_create_datetime` | PO creation date |
| `LAST_UPDATE_DATE` | DATE | `aud_update_datetime` | Last updated |

#### PO_LINES_ALL

| Oracle Column | Type | Azure SQL Column | Description |
|--------------|------|-----------------|-------------|
| `PO_HEADER_ID` | NUMBER | `po_header_key` | FK → PO header |
| `PO_LINE_ID` | NUMBER | `po_line_key` | PO line surrogate key |
| `LINE_NUM` | NUMBER | `po_line_number` | Line number within PO |
| `INVENTORY_ITEM_ID` | NUMBER | `part_key` | FK → item master |
| `VENDOR_PRODUCT_NUM` | VARCHAR2(25) | `supplier_part_number` | Supplier's part number |
| `ITEM_DESCRIPTION` | VARCHAR2(240) | `part_description` | Part description at PO line |
| `UNIT_PRICE` | NUMBER | `unit_cost_local` | Negotiated unit price |
| `QUANTITY` | NUMBER | `order_qty` | Ordered / blanket quantity |
| `UNIT_MEAS_LOOKUP_CODE` | VARCHAR2(25) | `uom` | Unit of measure |
| `EXPIRATION_DATE` | DATE | `expiry_date` | Contract line expiration |
| `CREATION_DATE` | DATE | `aud_create_datetime` | Line creation date |

---

### `DOO_LINES_ALL` → `edap_dw_replica.fact_sales_order_line`

**Business entity:** Customer sales order fulfillment lines  
**Grain:** One row per fulfillment line (one shipment event per order line)  
**Join key:** `FULFILLMENT_LINE_ID`  
**OTD customer-facing:** `SHIP_DATE` vs `PROMISE_SHIP_DATE`

| Oracle Column | Type | Azure SQL Column | Description |
|--------------|------|-----------------|-------------|
| `SOURCE_ORDER_ID` | NUMBER | `sales_order_number` | Customer order number |
| `SOURCE_LINE_ID` | NUMBER | `sales_order_line` | Line number within the order |
| `INVENTORY_ITEM_ID` | NUMBER | `part_key` | FK → item master |
| `ORDERED_QUANTITY` | NUMBER | `sales_order_quantity` | Quantity ordered |
| `SHIPPED_QUANTITY` | NUMBER | `shipped_quantity` | Quantity shipped to date |
| `UNIT_SELLING_PRICE` | NUMBER | `unit_price_local` | Customer net unit price |
| `EXTENDED_SELLING_PRICE` | NUMBER | `extended_price_local` | Total line price |
| `ORDERED_DATE` | DATE | `order_date_key` | Order entry date (YYYYMMDD int) |
| `SHIP_DATE` | DATE | `ship_day_key` | Actual ship date (YYYYMMDD int) |
| `REQUEST_SHIP_DATE` | DATE | `requested_ship_date_key` | Customer-requested date (YYYYMMDD int) |
| `PROMISE_SHIP_DATE` | DATE | `promised_ship_day_key` | Committed ship date (YYYYMMDD int) |
| `SOLD_TO_ORG_ID` | NUMBER | `customer_key` | FK → dim_customer (customer account) |
| `SHIP_FROM_ORG_ID` | NUMBER | `business_unit_key` | Shipping warehouse / plant |
| `CURRENCY_CODE` | VARCHAR2(15) | `currency_code` | Transaction currency |
| `FREIGHT_TERMS_CODE` | VARCHAR2(30) | `freight_terms` | FOB, Prepaid, etc. |
| `UNIT_COST` | NUMBER | `unit_cost_local` | Manufacturing cost at time of order |
| `CREATION_DATE` | DATE | `aud_create_datetime` | Record creation date |
| `LAST_UPDATE_DATE` | DATE | `aud_update_datetime` | Record last updated |

**Customer OTD:**  
`ship_day_key > promised_ship_day_key` → OTD Miss (late to customer)

---

### `AP_INVOICES_ALL` + `AP_INVOICE_LINES_ALL` → `edap_dw_replica.fact_ap_invoice_lines`

**Business entity:** Accounts Payable supplier invoices  
**Grain:** Header: one row per invoice. Lines: one row per invoice line.

#### AP_INVOICES_ALL (header)

| Oracle Column | Type | Azure SQL Column | Description |
|--------------|------|-----------------|-------------|
| `INVOICE_ID` | NUMBER | `invoice_key` | Invoice surrogate key |
| `INVOICE_NUM` | VARCHAR2(50) | `invoice_number` | Supplier's invoice number |
| `VENDOR_ID` | NUMBER | `supplier_key` | FK → supplier master |
| `INVOICE_DATE` | DATE | `invoice_date_key` | Invoice date (YYYYMMDD int) |
| `GL_DATE` | DATE | `accounting_date_key` | Accounting period date (YYYYMMDD int) |
| `INVOICE_AMOUNT` | NUMBER | `invoice_amount_local` | Total invoice amount |
| `INVOICE_CURRENCY_CODE` | VARCHAR2(15) | `currency_code` | Invoice currency |
| `ORG_ID` | NUMBER | `business_unit_key` | Operating unit |
| `CREATION_DATE` | DATE | `aud_create_datetime` | Record creation date |
| `LAST_UPDATE_DATE` | DATE | `aud_update_datetime` | Record last updated |

#### AP_INVOICE_LINES_ALL (lines)

| Oracle Column | Type | Azure SQL Column | Description |
|--------------|------|-----------------|-------------|
| `INVOICE_ID` | NUMBER | `invoice_key` | FK → invoice header |
| `LINE_NUMBER` | NUMBER | `line_number` | Line number on the invoice |
| `LINE_TYPE_LOOKUP_CODE` | VARCHAR2(25) | `line_type` | ITEM, FREIGHT, MISCELLANEOUS, TAX |
| `INVENTORY_ITEM_ID` | NUMBER | `part_key` | FK → item master (if applicable) |
| `QUANTITY_INVOICED` | NUMBER | `shipped_quantity` | Invoiced quantity |
| `UNIT_PRICE` | NUMBER | `unit_cost_local` | Unit price on invoice |
| `AMOUNT` | NUMBER | `extended_cost_local` | Line amount |
| `DESCRIPTION` | VARCHAR2(240) | `part_description` | Line description |
| `PO_HEADER_ID` | NUMBER | `po_number` | Matched PO header |
| `PO_LINE_ID` | NUMBER | `po_line_number` | Matched PO line |
| `CREATION_DATE` | DATE | `aud_create_datetime` | Record creation date |

---

### `CST_COST_DETAILS` → `stg_replica.fact_part_cost`

**Business entity:** Standard / Frozen cost per item  
**Grain:** One row per item × organization × cost type  
**Join key:** `INVENTORY_ITEM_ID` + `ORGANIZATION_ID` + `COST_TYPE_ID`

| Oracle Column | Type | Azure SQL Column | Description |
|--------------|------|-----------------|-------------|
| `INVENTORY_ITEM_ID` | NUMBER | `part_key` | FK → item master |
| `ORGANIZATION_ID` | NUMBER | `business_unit_key` | Manufacturing organization |
| `COST_TYPE_ID` | NUMBER | `cost_type` | Frozen, Pending, Average, etc. |
| `ITEM_COST` | NUMBER | `cost_amount_local` | Total unit cost in local currency |
| `MATERIAL_COST` | NUMBER | `mtl_cost_local` | Material component |
| `RESOURCE_COST` | NUMBER | `labor_cost_local` | Labor / resource component |
| `OVERHEAD_COST` | NUMBER | `burden_cost_local` | Overhead / burden component |
| `OUTSIDE_PROC_COST` | NUMBER | `sub_cont_cost_local` | Subcontracting component |
| `CURRENCY_CODE` | VARCHAR2(15) | `currency_code` | Cost currency |
| `EFFECTIVE_DATE` | DATE | `effective_date` | Cost effective from (real DATE, not YYYYMMDD int) |
| `DISABLE_DATE` | DATE | `expiry_date` | Cost effective through |
| `CURRENT_COST_FLAG` | VARCHAR2(1) | `current_record_ind` | Y = current active cost record |

**Note:** `fact_part_cost` lives in `stg_replica` (staging schema), not `edap_dw_replica`. Filter `WHERE current_record_ind = 'Y'` (or `= 1`) to get current cost. The ETL maps the Oracle VARCHAR `'Y'` to a SQL bit `1`.

---

## Semantic Role → Physical Column Mapping

Quick reference used by `src/brain/col_resolver.py` to find the right column by role:

| Semantic Role | Primary Column | Table | Notes |
|--------------|---------------|-------|-------|
| `part_key` | `part_key` | all fact + dim tables | Surrogate PK; Oracle: `INVENTORY_ITEM_ID` |
| `part_number` | `part_number` | `dim_part` | Natural key; Oracle: `SEGMENT1` |
| `part_description` | `part_description` | `dim_part`, fact tables | Oracle: `DESCRIPTION` / `ITEM_DESCRIPTION` |
| `supplier_key` | `supplier_key` | `dim_supplier`, fact tables | Oracle: `VENDOR_ID` |
| `supplier_name` | `supplier_name` | `dim_supplier` | Fallback: `pre_standardization_supplier_name` |
| `customer_key` | `customer_key` | `fact_sales_order_line` | Oracle: `SOLD_TO_ORG_ID` |
| `quantity_on_hand` | `quantity_on_hand` | `fact_inventory_on_hand` | Oracle: `PRIMARY_TRANSACTION_QUANTITY` |
| `demand_order_part_quantity` | `demand_order_part_quantity` | `fact_inventory_on_hand` | Open demand qty |
| `quantity_not_received` | `quantity_not_received` | `fact_inventory_open_orders` | Open PO qty |
| `received_qty` | `received_qty` | `fact_po_receipt` | Oracle: `QUANTITY` (receipt transaction qty) |
| `sales_order_quantity` | `sales_order_quantity` | `fact_sales_order_line` | Oracle: `ORDERED_QUANTITY` |
| `shipped_quantity` | `shipped_quantity` | `fact_sales_order_line` | Oracle: `SHIPPED_QUANTITY` |
| `order_lead_time` | `order_lead_time` | `fact_inventory_on_hand` | Replenishment lead time in days |
| `unit_cost_local` | `unit_cost_local` | `fact_po_receipt`, `fact_sales_order_line` | |
| `receipt_date_key` | `receipt_date_key` | `fact_po_receipt` | Oracle: `TRANSACTION_DATE` (YYYYMMDD int) |
| `due_date_key` | `due_date_key` | `fact_po_receipt`, `fact_inventory_open_orders` | Oracle: `EXPECTED_RECEIPT_DATE` / `NEED_BY_DATE` |
| `ship_day_key` | `ship_day_key` | `fact_sales_order_line` | Oracle: `SHIP_DATE` (YYYYMMDD int) |
| `promised_ship_day_key` | `promised_ship_day_key` | `fact_sales_order_line` | Oracle: `PROMISE_SHIP_DATE` |
| `safety_stock_limit` | `safety_stock_limit` | `fact_inventory_on_hand` | Min stocking quantity |
| `cost_amount_local` | `cost_amount_local` | `stg_replica.fact_part_cost` | Oracle: `ITEM_COST` |
| `buyer_id` | `buyer_id` | `fact_po_receipt` | Oracle: `AGENT_ID` |

---

*Document version: 0.4.5 — validated 2026-04-22 against `config/schema_cache.json`, `config/mappings.yaml`, and Power BI CycleCount / EDAP dashboard exports.*

## Value Stream Mapping (0.5.0 Additions)

### Manufacturing Work Orders (Make Stream)
- Table: \act_inventory_open_mfg_orders- Join keys: \part_key\, \usiness_unit_key- Timestamps: \due_date_key\ (Used for Make friction mapping).

### Value Stream Graph Dependencies
Value stream mapping integrates via \part_key\ and \usiness_unit_key\, utilizing:
* \act_inventory_open_orders\ (Purchase) vs \due_date_key\.
* \act_inventory_open_mfg_orders\ (Manufacturing) vs \due_date_key\.
* \act_sales_order_line\ (Fulfillment) vs \promised_ship_day_key\.

Friction multiplies automatically. Late delivery points scale connection weight up to 3x based on MIT SCALE metrics.
