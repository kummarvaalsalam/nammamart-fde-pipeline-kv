from datetime import timedelta
import os
import re
from airflow.decorators import dag, task
import duckdb
import pandas as pd
import pendulum

DATA_DIR = "/usr/local/airflow/include/data/nammamart"
QUARANTINE_DIR = "/usr/local/airflow/include/quarantine/kummar"
DB_PATH = "/usr/local/airflow/include/warehouse_kummar.duckdb"
IST = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "kummar",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}

def mask_email(val):
    if pd.isna(val) or "@" not in str(val):
        return None
    user, domain = str(val).strip().split("@", 1)
    return f"{user[:2]}***@{domain}"

def mask_phone(val):
    digits = re.sub(r"\D", "", str(val)) if pd.notna(val) else ""
    if len(digits) < 4:
        return None
    return f"{digits[:2]}******{digits[-2:]}"

@dag(
    dag_id="nammamart_etl_kummar",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, 9, 0, tz=IST),
    catchup=False,
    default_args=default_args,
    tags=["nammamart", "etl", "kummar"],
)
def nammamart_etl_pipeline():

    @task
    def extract_file(filename: str) -> str:
        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing source file: {path}")
        return path

    @task
    def validate_transform_load(
        orders_path: str, payments_path: str, customers_path: str, products_path: str, stores_path: str, **context
    ) -> dict:
        run_id = context["run_id"]
        dag_id = context["dag"].dag_id
        checked_at = pendulum.now(IST).to_iso8601_string()
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        safe_run = re.sub(r"[^a-zA-Z0-9_-]", "_", run_id)

        orders = pd.read_csv(orders_path)
        payments = pd.read_csv(payments_path)
        customers = pd.read_csv(customers_path)
        products = pd.read_csv(products_path)
        stores = pd.read_csv(stores_path)

        total_extracted = len(orders) + len(payments) + len(customers) + len(products) + len(stores)
        dq_records = []

        def log_dq(name, dim, sev, checked, failed):
            dq_records.append({
                "run_id": run_id, "dag_id": dag_id, "check_name": name, "dimension": dim,
                "severity": sev, "rows_checked": int(checked), "rows_failed": int(failed),
                "status": "FAIL" if (failed > 0 and sev == "Critical") else ("WARN" if failed > 0 else "PASS"),
                "checked_at": checked_at,
            })

        # 1. STORES
        stores["failure_reason"] = ""
        r_dup_s = stores["store_id"].duplicated(keep="first")
        r_zone_null = stores["zone"].isna() | (stores["zone"].astype(str).str.strip() == "")
        r_cap = pd.to_numeric(stores["capacity_orders_per_day"], errors="coerce").fillna(0) <= 0
        stores.loc[r_dup_s, "failure_reason"] += "Duplicate store_id; "
        stores.loc[r_zone_null, "failure_reason"] += "Null zone; "
        stores.loc[r_cap, "failure_reason"] += "Invalid capacity <= 0; "
        log_dq("stores_completeness_zone", "Completeness", "Critical", len(stores), r_zone_null.sum())
        stores_bad = stores[stores["failure_reason"] != ""]
        stores_clean = stores[stores["failure_reason"] == ""].drop(columns=["failure_reason"]).copy()
        stores_bad.to_csv(f"{QUARANTINE_DIR}/stores_failed_{safe_run}.csv", index=False)

        # 2. PRODUCTS
        products["failure_reason"] = ""
        r_dup_p = products["product_id"].duplicated(keep="first")
        r_cat_null = products["category"].isna() | (products["category"].astype(str).str.strip() == "")
        mrp = pd.to_numeric(products["mrp"], errors="coerce")
        cost = pd.to_numeric(products["cost_price"], errors="coerce")
        r_price = (mrp <= 0) | (cost > mrp) | mrp.isna() | cost.isna()
        products.loc[r_dup_p, "failure_reason"] += "Duplicate product_id; "
        products.loc[r_cat_null, "failure_reason"] += "Null category; "
        products.loc[r_price, "failure_reason"] += "Invalid mrp or cost_price > mrp; "
        log_dq("products_uniqueness_id", "Uniqueness", "Critical", len(products), r_dup_p.sum())
        log_dq("products_accuracy_price", "Accuracy", "Critical", len(products), r_price.sum())
        products_bad = products[products["failure_reason"] != ""]
        products_clean = products[products["failure_reason"] == ""].drop(columns=["failure_reason"]).copy()
        products_bad.to_csv(f"{QUARANTINE_DIR}/products_failed_{safe_run}.csv", index=False)

        # 3. CUSTOMERS
        customers["failure_reason"] = ""
        r_dup_c = customers["customer_id"].duplicated(keep="first")
        signup_dt = pd.to_datetime(customers["signup_date"], errors="coerce")
        r_future_c = signup_dt > pd.Timestamp("2026-10-02")
        r_bad_email = ~customers["email"].astype(str).str.contains(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", regex=True, na=False)
        r_tier_case = ~customers["loyalty_tier"].isin(["Bronze", "Silver", "Gold", "Platinum"])
        customers.loc[r_dup_c, "failure_reason"] += "Duplicate customer_id; "
        customers.loc[r_future_c, "failure_reason"] += "Future signup_date; "
        log_dq("customers_timeliness_signup", "Timeliness", "Critical", len(customers), r_future_c.sum())
        log_dq("customers_validity_email", "Validity", "Warning", len(customers), r_bad_email.sum())
        log_dq("customers_consistency_tier_casing", "Consistency", "Warning", len(customers), r_tier_case.sum())
        customers_bad = customers[customers["failure_reason"] != ""]
        customers_clean = customers[customers["failure_reason"] == ""].drop(columns=["failure_reason"]).copy()
        customers_bad.to_csv(f"{QUARANTINE_DIR}/customers_failed_{safe_run}.csv", index=False)
        customers_clean["loyalty_tier"] = customers_clean["loyalty_tier"].astype(str).str.strip().str.title()
        customers_clean["email"] = customers_clean["email"].apply(mask_email)
        customers_clean["phone"] = customers_clean["phone"].apply(mask_phone)

        # 4. ORDERS
        orders["failure_reason"] = ""
        status_clean = orders["order_status"].astype(str).str.strip().str.upper()
        pm_map = {"UPI": "UPI", "CARD": "Card", "CASH ON DELIVERY": "Cash on Delivery", "COD": "Cash on Delivery", "WALLET": "Wallet"}
        pm_clean = orders["payment_mode"].astype(str).str.strip().str.upper().map(pm_map)
        r_dup_o = orders["order_id"].duplicated(keep="first")
        r_null_cust = orders["customer_id"].isna()
        parsed_ts = pd.to_datetime(orders["order_ts"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
        r_bad_ts = parsed_ts.isna()
        r_future_o = parsed_ts > pd.Timestamp("2026-10-02 23:59:59")
        r_bad_status = ~status_clean.isin(["DELIVERED", "CANCELLED", "RETURNED"])
        r_bad_pm = pm_clean.isna()
        r_qty = pd.to_numeric(orders["quantity"], errors="coerce").fillna(0) <= 0
        disc = pd.to_numeric(orders["discount_pct"], errors="coerce")
        r_disc = (disc < 0) | (disc > 100) | disc.isna()
        calc_amt = (orders["quantity"] * orders["unit_price"] * (1 - orders["discount_pct"] / 100)).round(2)
        r_amt = (pd.to_numeric(orders["order_amount"], errors="coerce") - calc_amt).abs() > 0.01
        deliv = pd.to_numeric(orders["delivery_minutes"], errors="coerce")
        r_deliv_missing = (status_clean == "DELIVERED") & deliv.isna()
        r_deliv_range = deliv.notna() & ((deliv <= 0) | (deliv > 120))
        r_fk_cust = ~orders["customer_id"].isin(customers_clean["customer_id"])
        r_fk_prod = ~orders["product_id"].isin(products_clean["product_id"])
        r_fk_store = ~orders["store_id"].isin(stores_clean["store_id"])

        for mask, msg in [
            (r_dup_o, "Duplicate order_id; "), (r_null_cust, "Null customer_id; "),
            (r_bad_ts, "Invalid order_ts; "), (r_future_o, "Future order_ts; "),
            (r_bad_status, "Invalid order_status; "), (r_bad_pm, "Invalid payment_mode; "),
            (r_qty, "Quantity <= 0; "), (r_disc, "Invalid discount_pct; "),
            (r_amt, "Order amount formula mismatch; "), (r_deliv_missing, "Delivered order missing delivery_minutes; "),
            (r_deliv_range, "Delivery minutes out of range (1-120); "), (r_fk_cust, "Unknown customer_id; "),
            (r_fk_prod, "Unknown product_id; "), (r_fk_store, "Unknown store_id; "),
        ]:
            orders.loc[mask, "failure_reason"] += msg

        log_dq("orders_completeness_customer_delivery", "Completeness", "Critical", len(orders), (r_null_cust | r_deliv_missing).sum())
        log_dq("orders_uniqueness_id", "Uniqueness", "Critical", len(orders), r_dup_o.sum())
        log_dq("orders_validity_status_ts_pm", "Validity", "Critical", len(orders), (r_bad_ts | r_bad_status | r_bad_pm | r_disc).sum())
        log_dq("orders_accuracy_amount_qty_deliv", "Accuracy", "Critical", len(orders), (r_qty | r_amt | r_deliv_range).sum())
        log_dq("orders_consistency_fk", "Consistency", "Critical", len(orders), (r_fk_cust | r_fk_prod | r_fk_store).sum())
        log_dq("orders_timeliness_future", "Timeliness", "Critical", len(orders), r_future_o.sum())

        orders["order_status"] = status_clean
        orders["payment_mode"] = pm_clean.fillna(orders["payment_mode"])
        orders_bad = orders[orders["failure_reason"] != ""]
        orders_clean = orders[orders["failure_reason"] == ""].drop(columns=["failure_reason"]).copy()
        orders_bad.to_csv(f"{QUARANTINE_DIR}/orders_failed_{safe_run}.csv", index=False)

        # 5. PAYMENTS
        payments["failure_reason"] = ""
        r_dup_pay = payments["payment_id"].duplicated(keep="first")
        r_fk_ord = ~payments["order_id"].isin(orders_clean["order_id"])
        payments.loc[r_dup_pay, "failure_reason"] += "Duplicate payment_id; "
        payments.loc[r_fk_ord, "failure_reason"] += "Orphan order_id; "
        log_dq("payments_uniqueness_id", "Uniqueness", "Critical", len(payments), r_dup_pay.sum())
        log_dq("payments_consistency_order_fk", "Consistency", "Critical", len(payments), r_fk_ord.sum())
        payments_bad = payments[payments["failure_reason"] != ""]
        payments_clean = payments[payments["failure_reason"] == ""].drop(columns=["failure_reason"]).copy()
        payments_bad.to_csv(f"{QUARANTINE_DIR}/payments_failed_{safe_run}.csv", index=False)

        # 6. TRANSFORM & RECONCILE
        orders_clean["order_ts"] = pd.to_datetime(orders_clean["order_ts"])
        orders_clean["order_date"] = orders_clean["order_ts"].dt.date
        orders_clean = orders_clean.merge(products_clean[["product_id", "cost_price", "category"]], on="product_id", how="left")
        orders_clean = orders_clean.merge(stores_clean[["store_id", "store_name", "zone"]], on="store_id", how="left")
        is_deliv = orders_clean["order_status"] == "DELIVERED"
        orders_clean["net_revenue"] = orders_clean["order_amount"].where(is_deliv, 0.0).round(2)
        orders_clean["gross_margin"] = (orders_clean["order_amount"] - (orders_clean["quantity"] * orders_clean["cost_price"])).where(is_deliv, 0.0).round(2)
        orders_clean["within_15_min"] = is_deliv & (orders_clean["delivery_minutes"] <= 15)

        succ_pay = payments_clean[payments_clean["payment_status"].str.upper() == "SUCCESS"].groupby("order_id", as_index=False)["amount_paid"].sum()
        recon = orders_clean.merge(succ_pay, on="order_id", how="left")
        recon["amount_paid"] = recon["amount_paid"].fillna(0.0)
        recon["difference"] = (recon["order_amount"] - recon["amount_paid"]).round(2)

        def recon_reason(row):
            if row["order_status"] == "CANCELLED" and row["amount_paid"] > 0:
                return "Cancelled order has SUCCESS payment"
            if row["order_status"] == "DELIVERED" and row["amount_paid"] == 0:
                return "Delivered order missing SUCCESS payment"
            if row["order_status"] == "DELIVERED" and abs(row["difference"]) > 0.01:
                return "Paid amount does not match order_amount"
            return ""

        recon["mismatch_reason"] = recon.apply(recon_reason, axis=1)
        recon_mismatches = recon[recon["mismatch_reason"] != ""][
            ["order_id", "order_status", "payment_mode", "order_amount", "amount_paid", "difference", "mismatch_reason"]
        ].copy()

        # 7. LOAD TO DUCKDB
        con = duckdb.connect(DB_PATH)
        con.execute("CREATE OR REPLACE TABLE etl_dim_customer AS SELECT * FROM customers_clean")
        con.execute("CREATE OR REPLACE TABLE etl_dim_product AS SELECT * FROM products_clean")
        con.execute("CREATE OR REPLACE TABLE etl_dim_store AS SELECT * FROM stores_clean")
        con.execute("CREATE OR REPLACE TABLE etl_fact_orders AS SELECT * FROM orders_clean")
        con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM recon_mismatches")
        con.execute("CREATE OR REPLACE TABLE etl_daily_zone_revenue AS SELECT order_date, zone, store_id, store_name, COUNT(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders, ROUND(SUM(net_revenue), 2) AS net_revenue FROM etl_fact_orders GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3")
        con.execute("CREATE OR REPLACE TABLE etl_category_margin AS SELECT category, COUNT(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders, ROUND(SUM(net_revenue), 2) AS revenue, ROUND(SUM(gross_margin), 2) AS gross_margin FROM etl_fact_orders GROUP BY 1 ORDER BY revenue DESC")

        dq_df = pd.DataFrame(dq_records)
        con.execute("CREATE TABLE IF NOT EXISTS dq_results (run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at VARCHAR)")
        con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
        con.execute("INSERT INTO dq_results SELECT * FROM dq_df")

        total_loaded = len(orders_clean) + len(payments_clean) + len(customers_clean) + len(products_clean) + len(stores_clean)
        total_quarantined = len(orders_bad) + len(payments_bad) + len(customers_bad) + len(products_bad) + len(stores_bad)
        print("=== ETL CATEGORY MARGIN REPORT ===")
        print(con.execute("SELECT * FROM etl_category_margin").df().to_string(index=False))
        print(f"ETL Summary -> Extracted: {total_extracted} | Loaded: {total_loaded} | Quarantined: {total_quarantined}")
        con.close()
        return {"rows_extracted": total_extracted, "rows_loaded": total_loaded, "rows_quarantined": total_quarantined}

    @task(trigger_rule="all_done")
    def log_pipeline_run(metrics: dict | None = None, **context):
        run_id = context["run_id"]
        dag_id = context["dag"].dag_id
        m = metrics or {"rows_extracted": 0, "rows_loaded": 0, "rows_quarantined": 0}
        con = duckdb.connect(DB_PATH)
        con.execute("CREATE TABLE IF NOT EXISTS pipeline_runs (run_id VARCHAR, dag_id VARCHAR, start_time VARCHAR, end_time VARCHAR, rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR)")
        con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
        con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [run_id, dag_id, str(context["dag_run"].start_date), pendulum.now(IST).to_iso8601_string(), m["rows_extracted"], m["rows_loaded"], m["rows_quarantined"], "SUCCESS" if metrics else "FAILED"])
        con.close()

    f_orders = extract_file.override(task_id="extract_orders")("orders.csv")
    f_payments = extract_file.override(task_id="extract_payments")("payments.csv")
    f_customers = extract_file.override(task_id="extract_customers")("customers.csv")
    f_products = extract_file.override(task_id="extract_products")("products.csv")
    f_stores = extract_file.override(task_id="extract_stores")("stores.csv")
    log_pipeline_run(validate_transform_load(f_orders, f_payments, f_customers, f_products, f_stores))

nammamart_etl_pipeline()
