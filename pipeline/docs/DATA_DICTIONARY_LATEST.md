# Data Infrastructure & Relational Schema
**Last Auto-Updated:** 2026-04-21 17:53:30

This document is automatically maintained by the Autonomous Agent. It provides an up-to-date map of database structures, variables, and relational linkages for fluid user capability inside the reporting layer.

## Discovered Schema Information & Relationships
`	ext
Starting schema discovery for both sources ...

>>> Azure SQL
[Azure SQL] Connecting to edap-replica-cms-sqlserver.database.windows.net / edap-replica-cms-sqldb ...
[Azure SQL] Using stored credentials for agard@astecindustries.com (ActiveDirectoryPassword).
[Azure SQL] Connected.

============================================================
AZURE SQL — SCHEMA DISCOVERY
============================================================
Schema           Table                                           # Columns
---------------  --------------------------------------------  -----------
edap_dw_replica  dim_accounting_month                                    8
edap_dw_replica  dim_ap_payment_terms                                    7
edap_dw_replica  dim_ar_payment_terms                                    7
edap_dw_replica  dim_bs_report_hierarchy                                10
edap_dw_replica  dim_business_unit                                      20
edap_dw_replica  dim_business_unit_department                            8
edap_dw_replica  dim_business_unit_gl_account                           11
edap_dw_replica  dim_contract_part_validation_progress                   7
edap_dw_replica  dim_corporate_gl_account                               20
edap_dw_replica  dim_customer                                           24
edap_dw_replica  dim_direct_purchase                                     7
edap_dw_replica  dim_dwcorp_exchange_rate_monthly                        9
edap_dw_replica  dim_exchange_rate                                      10
edap_dw_replica  dim_exchange_rate_for_foreign_sub                       9
edap_dw_replica  dim_exchange_rate_monthly                               9
edap_dw_replica  dim_oracle_erp_ledger                                  15
edap_dw_replica  dim_part                                               28
edap_dw_replica  dim_part_code                                          14
edap_dw_replica  dim_po_contract                                        14
edap_dw_replica  dim_rb_exchange_rate                                    9
edap_dw_replica  dim_rb_exchange_rate_monthly                            9
edap_dw_replica  dim_sales_channel                                       7
edap_dw_replica  dim_sales_order_classification                         12
edap_dw_replica  dim_sales_order_incoterms                               7
edap_dw_replica  dim_sales_order_line_classification                    13
edap_dw_replica  dim_sales_order_ship_type                               7
edap_dw_replica  dim_sales_order_status                                  7
edap_dw_replica  dim_sales_order_type                                    7
edap_dw_replica  dim_source_data_standardization                         8
edap_dw_replica  dim_supplier                                           12
edap_dw_replica  dim_time_day                                           12
edap_dw_replica  dim_time_month                                          8
edap_dw_replica  dim_time_week                                           9
edap_dw_replica  dim_warehouse                                           9
edap_dw_replica  dim_works_order_status                                  7
edap_dw_replica  fact_ap_invoice_line_accounts                          15
edap_dw_replica  fact_ap_invoice_lines                                  27
edap_dw_replica  fact_ap_invoice_payments                               12
edap_dw_replica  fact_ap_invoices                                       15
edap_dw_replica  fact_ar_invoice_lines                                  20
edap_dw_replica  fact_ar_invoice_payments                               12
edap_dw_replica  fact_ar_invoices                                       37
edap_dw_replica  fact_inventory_on_hand                                 30
edap_dw_replica  fact_inventory_on_hand_warehouse                       31
edap_dw_replica  fact_inventory_open_mfg_orders                         21
edap_dw_replica  fact_inventory_open_orders                             21
edap_dw_replica  fact_ofs_extract_cx_ib                                 50
edap_dw_replica  fact_ofs_extract_field_tech_absence                    18
edap_dw_replica  fact_ofs_extract_field_tech_expense_invoices           24
edap_dw_replica  fact_ofs_extract_field_tech_expense_sa                 21
edap_dw_replica  fact_ofs_extract_field_tech_timecard_extract           17
edap_dw_replica  fact_ofs_extract_ib_meter                              16
edap_dw_replica  fact_ofs_extract_item_cost                             24
edap_dw_replica  fact_ofs_extract_order_header                          52
edap_dw_replica  fact_ofs_extract_order_lines                           48
edap_dw_replica  fact_ofs_extract_service_request                       75
edap_dw_replica  fact_ofs_extract_service_work_order                    69
edap_dw_replica  fact_ofs_extract_time_capture                          23
edap_dw_replica  fact_ofs_xml_appointment                         
`

*See complete output directly in the schema cache created by the discovery pipeline.*