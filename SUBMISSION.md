# NammaMart Quick Commerce — ETL & ELT Pipeline Submission
**Author:** Kummar | **Warehouse:** `include/warehouse_kummar.duckdb`

## Part 0: Discovery & Data Issues Log

| File | Row Key | Column | Problem Found | Quality Dimension | Proposed Rule | Severity |
| --- | --- | --- | --- | --- | --- | --- |
| `orders.csv` | `order_id` | `order_id` | 12 duplicate order IDs | Uniqueness | Keep first occurrence, quarantine duplicates | Critical |
| `orders.csv` | `order_id` | `customer_id`, `delivery_minutes` | 18 null `customer_id`; 164 null `delivery_minutes` | Completeness | Reject null `customer_id` & DELIVERED orders missing delivery time | Critical |
| `orders.csv` | `order_id` | `order_ts`, `order_status`, `payment_mode` | Invalid date `31/09/2026`; status `'Pending??'`; `'upi'`, `'cod'`, `'Cashh'` | Validity | Standardize casings; quarantine invalid dates/statuses | Critical |
| `orders.csv` | `order_id` | `quantity`, `discount_pct`, `order_amount`, `delivery_minutes` | 10 qty <= 0; 3 bad discounts; 18 amount mismatches; 6 delivery times <= 0 or > 120m | Accuracy | Enforce `qty > 0`, formula check ±0.01, `1 <= delivery_minutes <= 120` | Critical |
| `orders.csv` | `order_id` | `customer_id`, `product_id`, `store_id` | 18 unknown customers, 9 unknown products, 5 unknown stores | Consistency | Foreign-key check against clean master tables | Critical |
| `payments.csv` | `payment_id` | `payment_id`, `order_id`, `amount_paid` | 4 duplicate IDs; 8 orphan `order_id`s; 14 SUCCESS amount mismatches | Uniqueness / Consistency | Deduplicate; flag mismatches in reconciliation table | Critical |
| `customers.csv` | `customer_id` | `customer_id`, `signup_date`, `loyalty_tier`, `email` | 5 duplicate IDs; future signup `2027-01-05`; mixed casing; 4 null emails | Uniqueness / Timeliness | Quarantine duplicates & future dates; standardize casing; mask PII | Critical / Warning |
| `products.csv` | `product_id` | `product_id`, `category`, `mrp`, `cost_price` | 2 duplicate IDs; 3 null categories; 5 `mrp <= 0` or `cost_price > mrp` | Completeness / Accuracy | Quarantine null categories and negative-margin items | Critical |
| `stores.csv` | `store_id` | `zone`, `capacity_orders_per_day` | 1 null `zone`; 1 store with `capacity <= 0` | Completeness / Accuracy | Quarantine incomplete/invalid store rows | Critical |
| `orders_2026-10-01_increment.csv` | `order_id` | `order_status` | 150 new Oct 1 orders + 10 updates to Sept orders (late `RETURNED`) | Consistency / Timeliness | Append to `raw_orders`; deduplicate/upsert in `stg_orders` | Critical |

### 3 Client Questions Before Building
1. **Formula mismatches & negative quantities:** Quarantine for ops review rather than auto-recalculating? *(Agreed: Quarantine).*
2. **Delivery SLA threshold:** Quarantine `delivery_minutes > 120` as app bugs while measuring 10-min promise at `<= 15 mins`? *(Agreed).*
3. **Late returns (`orders_2026-10-01_increment.csv`):** Reflect latest status in reporting while preserving original row in `raw_orders`? *(Agreed).*

## Business Questions — SQL & Live Results
- **ETL September Net Revenue:** `₹930,779.95`
- **ELT September-Only Net Revenue (after Oct 1 return updates):** `₹927,200.25`
- **ELT Total Net Revenue (Sept 1 – Oct 1):** `₹991,180.80`

### Q1. Daily Net Revenue by Zone & Store (Top 10 Stores)
| zone   | store_id   | store_name                  |   delivered_orders |   net_revenue |
|:-------|:-----------|:----------------------------|-------------------:|--------------:|
| South  | S06        | NammaMart Banashankari      |                145 |       87573.6 |
| South  | S05        | NammaMart JP Nagar          |                134 |       82229.8 |
| North  | S01        | NammaMart Hebbal            |                141 |       80069   |
| East   | S10        | NammaMart Marathahalli      |                141 |       79973.4 |
| West   | S13        | NammaMart Malleshwaram      |                137 |       77848.6 |
| West   | S15        | NammaMart Basaveshwaranagar |                143 |       76634.9 |
| North  | S03        | NammaMart RT Nagar          |                137 |       76506   |
| East   | S08        | NammaMart Whitefield        |                155 |       75466.2 |
| East   | S11        | NammaMart HSR Layout        |                144 |       74197.1 |
| West   | S14        | NammaMart Vijayanagar       |                162 |       73946.4 |

### Q2. Revenue & Gross Margin by Product Category
| category            |   delivered_orders |   revenue |   gross_margin |
|:--------------------|-------------------:|----------:|---------------:|
| Staples             |                256 |  224774   |        57398.6 |
| Personal Care       |                287 |  187729   |        42030   |
| Household           |                304 |  178978   |        45340.7 |
| Snacks & Beverages  |                302 |  160260   |        37384.2 |
| Dairy & Bakery      |                357 |  158331   |        42097.2 |
| Fruits & Vegetables |                329 |   81109.2 |        19116.6 |

### Q3. Average Delivery Time per Store & % Within 15 Minutes
| store_id   | store_name                  | zone   |   delivered_orders |   avg_delivery_minutes |   pct_within_15_min |
|:-----------|:----------------------------|:-------|-------------------:|-----------------------:|--------------------:|
| S08        | NammaMart Whitefield        | East   |                155 |                  20.6  |               35.48 |
| S01        | NammaMart Hebbal            | North  |                141 |                  20.62 |               31.21 |
| S03        | NammaMart RT Nagar          | North  |                137 |                  20.71 |               29.2  |
| S11        | NammaMart HSR Layout        | East   |                144 |                  20.97 |               27.78 |
| S02        | NammaMart Yelahanka         | North  |                139 |                  20.99 |               30.94 |
| S09        | NammaMart Indiranagar       | East   |                127 |                  21.14 |               27.56 |
| S05        | NammaMart JP Nagar          | South  |                134 |                  21.24 |               29.1  |
| S14        | NammaMart Vijayanagar       | West   |                162 |                  21.5  |               31.48 |
| S04        | NammaMart Jayanagar         | South  |                130 |                  21.59 |               26.92 |
| S13        | NammaMart Malleshwaram      | West   |                137 |                  21.8  |               27.01 |
| S10        | NammaMart Marathahalli      | East   |                141 |                  21.9  |               26.95 |
| S15        | NammaMart Basaveshwaranagar | West   |                143 |                  22.33 |               20.98 |
| S06        | NammaMart Banashankari      | South  |                145 |                  22.33 |               22.07 |

### Q4. Revenue by Payment Mode & Total Unreconciled Amount
| payment_mode     |   delivered_revenue |   total_unreconciled_amount |
|:-----------------|--------------------:|----------------------------:|
| UPI              |              534319 |                      103416 |
| Card             |              203773 |                      103416 |
| Cash on Delivery |              139249 |                      103416 |
| Wallet           |              113839 |                      103416 |

### Q5. Top 10 Customers by Spend (PII Masked)
| customer_id   | full_name      | email             | phone      | loyalty_tier   |   delivered_orders |   total_spend |
|:--------------|:---------------|:------------------|:-----------|:---------------|-------------------:|--------------:|
| C00115        | Sanjay Kumar   | sa***@example.com | 96******85 | Bronze         |                  7 |       7337.7  |
| C00310        | Kiran Patil    | ki***@example     | 91******35 | Bronze         |                  8 |       7258.95 |
| C00265        | Vikram Hegde   | vi***@example.com | 91******69 | Gold           |                  7 |       6940.3  |
| C00236        | Sneha Naidu    | sn***@example.com | 90******01 | Platinum       |                  9 |       6921.2  |
| C00072        | Nandini Murthy | na***@example.com | 98******05 | Silver         |                 11 |       6433.95 |
| C00283        | Nikhil Rao     | ni***@example.com | 91******43 | Bronze         |                  6 |       6241.4  |
| C00202        | Priya Bhat     | pr***@example.com | 91******11 | Silver         |                  7 |       6096.75 |
| C00043        | Ganesh Joshi   | ga***@example.com | 98******21 | Silver         |                  6 |       5956.35 |
| C00328        | Ritu Hegde     | ri***@example.com | 95******90 | Bronze         |                  4 |       5877.9  |
| C00338        | Bhavana Iyer   | bh***@example.com | 94******05 | Silver         |                  7 |       5845.35 |

### Q6. Rows Rejected Today by File & Reason (ELT `stg_orders`)
| file_name                       | failure_reason                                                                  |   rejected_rows |
|:--------------------------------|:--------------------------------------------------------------------------------|----------------:|
| orders.csv                      | Unknown store_id;                                                               |             283 |
| orders.csv                      | Unknown product_id;                                                             |             108 |
| orders.csv                      | Unknown product_id; Unknown store_id;                                           |              16 |
| orders.csv                      | Duplicate order_id;                                                             |              15 |
| orders_2026-10-01_increment.csv | Unknown product_id;                                                             |              14 |
| orders.csv                      | Null customer_id; Unknown customer_id;                                          |              14 |
| orders.csv                      | Unknown customer_id;                                                            |              14 |
| orders_2026-10-01_increment.csv | Unknown store_id;                                                               |              13 |
| orders.csv                      | Order amount formula mismatch;                                                  |              10 |
| orders.csv                      | Quantity <= 0;                                                                  |               9 |
| orders.csv                      | Delivered order missing delivery_minutes;                                       |               7 |
| orders.csv                      | Delivery minutes out of range;                                                  |               5 |
| orders.csv                      | Duplicate order_id; Unknown product_id;                                         |               4 |
| orders.csv                      | Future order_ts;                                                                |               4 |
| orders.csv                      | Invalid order_ts;                                                               |               4 |
| orders.csv                      | Null customer_id; Unknown customer_id; Unknown store_id;                        |               4 |
| orders.csv                      | Invalid order_status;                                                           |               3 |
| orders.csv                      | Duplicate order_id; Unknown store_id;                                           |               3 |
| orders.csv                      | Invalid discount_pct; Order amount formula mismatch;                            |               3 |
| orders.csv                      | Future order_ts; Unknown store_id;                                              |               2 |
| orders.csv                      | Order amount formula mismatch; Unknown product_id;                              |               1 |
| orders.csv                      | Order amount formula mismatch; Unknown product_id; Unknown store_id;            |               1 |
| orders.csv                      | Delivered order missing delivery_minutes; Unknown product_id; Unknown store_id; |               1 |
| orders.csv                      | Invalid payment_mode; Unknown store_id;                                         |               1 |
| orders.csv                      | Order amount formula mismatch; Unknown customer_id; Unknown store_id;           |               1 |
| orders.csv                      | Unknown customer_id; Unknown store_id;                                          |               1 |
| orders.csv                      | Invalid order_ts; Unknown product_id;                                           |               1 |
| orders.csv                      | Delivery minutes out of range; Unknown store_id;                                |               1 |
| orders.csv                      | Quantity <= 0; Unknown product_id;                                              |               1 |
| orders.csv                      | Future order_ts; Unknown product_id;                                            |               1 |
| orders.csv                      | Order amount formula mismatch; Unknown customer_id;                             |               1 |
| orders.csv                      | Invalid payment_mode;                                                           |               1 |
| orders.csv                      | Invalid order_ts; Unknown store_id;                                             |               1 |
| orders_2026-10-01_increment.csv | Unknown customer_id;                                                            |               1 |
| orders.csv                      | Unknown customer_id; Unknown product_id;                                        |               1 |
| orders.csv                      | Order amount formula mismatch; Unknown store_id;                                |               1 |

## Part C: Scheduling (9:00 AM IST, Monday–Saturday)
1. **Cron:** `0 9 * * 1-6` (9:00 AM Mon–Sat).
2. **UTC Time in UI:** `03:30 UTC` (`IST = UTC+5:30`).
3. **If `start_date` were UTC:** Airflow would run at `09:00 UTC = 14:30 IST`, missing the 9:30 AM stand-up.
4. **Why `catchup=False`:** Prevents flooding missed runs when Docker/laptop is off on weekends/holidays.
5. **If laptop/Docker is off at 9:00 AM:** Scheduler is paused while Docker is stopped; on restart, missed runs are skipped (`catchup=False`).

## Part D: Data Quality Scorecard (`dq_results`)
| dag_id               | check_name                            | dimension                      | severity   |   rows_checked |   rows_failed | status   |
|:---------------------|:--------------------------------------|:-------------------------------|:-----------|---------------:|--------------:|:---------|
| nammamart_elt_kummar | elt_customers_dq                      | Uniqueness/Timeliness          | Critical   |            405 |             8 | FAIL     |
| nammamart_elt_kummar | elt_orders_dq                         | Completeness/Validity/Accuracy | Critical   |           2572 |           551 | FAIL     |
| nammamart_elt_kummar | elt_payments_dq                       | Consistency                    | Critical   |           2228 |           479 | FAIL     |
| nammamart_elt_kummar | elt_products_dq                       | Uniqueness/Accuracy            | Critical   |            152 |            10 | FAIL     |
| nammamart_elt_kummar | elt_stores_dq                         | Completeness                   | Critical   |             15 |             2 | FAIL     |
| nammamart_etl_kummar | customers_consistency_tier_casing     | Consistency                    | Warning    |            405 |             8 | WARN     |
| nammamart_etl_kummar | customers_timeliness_signup           | Timeliness                     | Critical   |            405 |             3 | FAIL     |
| nammamart_etl_kummar | customers_validity_email              | Validity                       | Warning    |            405 |            12 | WARN     |
| nammamart_etl_kummar | orders_accuracy_amount_qty_deliv      | Accuracy                       | Critical   |           2412 |            34 | FAIL     |
| nammamart_etl_kummar | orders_completeness_customer_delivery | Completeness                   | Critical   |           2412 |            26 | FAIL     |
| nammamart_etl_kummar | orders_consistency_fk                 | Consistency                    | Critical   |           2412 |           462 | FAIL     |
| nammamart_etl_kummar | orders_timeliness_future              | Timeliness                     | Critical   |           2412 |             7 | FAIL     |
| nammamart_etl_kummar | orders_uniqueness_id                  | Uniqueness                     | Critical   |           2412 |            12 | FAIL     |
| nammamart_etl_kummar | orders_validity_status_ts_pm          | Validity                       | Critical   |           2412 |            14 | FAIL     |
| nammamart_etl_kummar | payments_consistency_order_fk         | Consistency                    | Critical   |           2228 |           475 | FAIL     |
| nammamart_etl_kummar | payments_uniqueness_id                | Uniqueness                     | Critical   |           2228 |             4 | FAIL     |
| nammamart_etl_kummar | products_accuracy_price               | Accuracy                       | Critical   |            152 |             5 | FAIL     |
| nammamart_etl_kummar | products_uniqueness_id                | Uniqueness                     | Critical   |            152 |             2 | FAIL     |
| nammamart_etl_kummar | stores_completeness_zone              | Completeness                   | Critical   |             15 |             1 | FAIL     |

## Part E: Data Governance
1. **Ownership:** Ops owns `orders`/`stores`; Finance owns `payments`; CRM owns `customers`; Merchandising owns `products`; Data Engineering owns warehouse layers. Rule changes require joint Ops + Finance sign-off.
2. **Data Dictionary:** `etl_fact_orders`/`stg_orders` (`order_id` PK, `order_ts` TIMESTAMP, `order_status` ENUM, `order_amount` DOUBLE INR, `delivery_minutes` DOUBLE — PII: No); `etl_dim_customer`/`stg_customers` (`customer_id` PK, `full_name` PII: Yes, `email` masked `xx***@domain` PII: Yes, `phone` masked `98******21` PII: Yes, `loyalty_tier` ENUM PII: No).
3. **Lineage:** Traceable via `store_id` + `order_date` from `mart_daily_store_kpis` → `stg_orders` → `raw_orders` (`_source_file`, `_run_id`, `_loaded_at`). Easier in ELT because all layers coexist in SQL.
4. **Personal Data & DPDP Act:** PII (`full_name`, `email`, `phone`, `area`, `pincode`) is masked rather than deleted to preserve join/uniqueness audits without exposing contact info, complying with **India's DPDP Act, 2023**.
5. **Access Control:** Only Data Engineers/Auditors read `raw_customers` and quarantine CSVs; Analysts read `stg_*` (masked); Finance/Managers read `mart_*` only.
6. **Retention:** Quarantine CSVs purged after 30 days; `raw_*` retained 7 years for financial audit; PII erased upon customer deletion request under DPDP.
7. **Secrets:** Stored in Airflow Variables/Connections or `.env` (gitignored), read via `Variable.get()`, never in GitHub.
8. **Audit Proof:** In ETL, inspect `include/quarantine/kummar/orders_failed_<run_id>.csv` and confirm `0` rows in `etl_fact_orders`. In ELT, query `SELECT * FROM stg_orders WHERE dq_status = 'FAIL'`.
9. **Change History (Sept 3 Delivered → Oct 1 Returned):** ELT `stg_orders` updates status to `RETURNED`, updating Sept net revenue for Finance while `raw_orders` retains both rows. **SCD Type 2** preserves both views.

## Part F: Observability Tests & 5 Pillars
### Run Audit Table (`pipeline_runs`)
| dag_id               |   rows_extracted |   rows_loaded |   rows_quarantined | status   |
|:---------------------|-----------------:|--------------:|-------------------:|:---------|
| nammamart_etl_kummar |             5212 |          4197 |               1015 | SUCCESS  |
| nammamart_elt_kummar |             5372 |          4322 |               1050 | SUCCESS  |

| # | Experiment | Expected | Observed | Where Seen | What I Would Change |
| --- | --- | --- | --- | --- | --- |
| 1 | Happy path | All tasks green; parallel extract | Both DAGs green; 5 parallel extracts ran concurrently | Airflow Graph UI & `pipeline_runs` | Add Slack completion alert |
| 2 | Re-run (idempotency) | Fact/mart counts unchanged; raw appends | ETL/ELT marts unchanged; `raw_orders` appended new `_run_id` | DuckDB counts & `pipeline_runs` | Delete matching `_run_id` before raw insert |
| 3 | Incremental load | 150 inserted, 10 Sept orders updated | Deduplicated on `order_id` (`_is_increment DESC`); 0 duplicates | `stg_orders` & `mart_daily_store_kpis` | Use SQL `MERGE` as data grows |
| 4 | Missing source file | 2 retries, then `failed`/`upstream_failed` | Retried twice, failed loudly; `log_pipeline_run` logged `FAILED` | Airflow Task Logs & `pipeline_runs` | Add `on_failure_callback` Slack alert |
| 5 | New bad data (5 rows) | All 5 caught with exact `failure_reason` | Null cust, neg qty, bad store, `SHIPPED`, `2027` quarantined | Quarantine CSV & `dq_results` | Keep current rules |
| 6 | Schema change | Fail loudly on renamed `order_amount` | ETL raised `KeyError`; ELT raised SQL `Binder Error` | Airflow Task Logs | Add schema contract check after extract |
| 7 | Volume anomaly (50 rows) | Warn on >50% drop | Passed 50 valid rows without volume alert | `pipeline_runs.rows_extracted` | Add rolling 7-day volume check |
| 8 | Distribution drift (`*100`) | Catch price spike | Mart revenue spiked ~100x without row-level failure | `mart_daily_store_kpis` | Add Z-score / price outlier check |
| 9 | Freshness (30d old) | Warn on stale `order_ts` | Loaded because dates weren't future | `stg_orders` | Add `MAX(order_ts) >= CURRENT_DATE - 1` check |
| 10 | Recovery (`Clear` task) | Only failed + downstream re-run | Skipped green extracts; re-ran transform + audit log | Airflow Graph UI | Document ops runbook |

- **5 Pillars:** **Lineage** (strongest: `_source_file`, `_run_id`, raw→stg→mart), **Schema** (loud failures), **Freshness** (`order_ts` + SLA), **Volume** (`pipeline_runs`), **Distribution** (weakest: needs statistical outlier checks).

## Part G: Recommendation to Sameer (CTO) — Adopt ELT
NammaMart should adopt **ELT** with strict **Staging Quality Gates**:
1. **Auditability:** `raw_*` retains every source record untouched for compliance and replay, whereas ETL pushes rejected rows into flat CSVs outside the warehouse.
2. **Quality Guarantees:** Filtering `WHERE dq_status = 'PASS'` in `stg_*` guarantees Finance never sees corrupt rows in `mart_*`, matching ETL's reconciled numbers.
3. **Ease of Change:** Business KPIs and SLAs live in modular `.sql` files updated without touching Python extraction code.
4. **1000× Scale & Cost:** Pushing joins and window functions into columnar SQL scales cleanly to millions of orders without heavy Python worker memory.
5. **Top 3 Risks & Mitigations:** (1) *Raw PII in `raw_customers`* → restrict `raw_*` access strictly to Data Engineering; (2) *SQL join duplication* → block Mart refresh if `dq_results` uniqueness fails; (3) *Unbounded raw growth* → partition `raw_*` by load date and archive after 1 year.
