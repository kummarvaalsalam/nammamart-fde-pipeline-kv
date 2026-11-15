from datetime import timedelta
import os
from airflow.decorators import dag, task
import duckdb
import pandas as pd
import pendulum

DATA_DIR = "/usr/local/airflow/include/data/nammamart"
SQL_DIR = "/usr/local/airflow/include/sql"
DB_PATH = "/usr/local/airflow/include/warehouse_kummar.duckdb"
IST = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "kummar",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}

@dag(
    dag_id="nammamart_elt_kummar",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, 9, 0, tz=IST),
    catchup=False,
    default_args=default_args,
    tags=["nammamart", "elt", "kummar"],
)
def nammamart_elt_pipeline():

    @task
    def load_raw_tables(**context) -> int:
        run_id = context["run_id"]
        loaded_at = pendulum.now(IST).to_iso8601_string()
        con = duckdb.connect(DB_PATH)
        total_rows = 0

        files_map = {
            "raw_orders": "orders.csv",
            "raw_payments": "payments.csv",
            "raw_customers": "customers.csv",
            "raw_products": "products.csv",
            "raw_stores": "stores.csv",
        }

        for table, fname in files_map.items():
            path = os.path.join(DATA_DIR, fname)
            if not os.path.exists(path):
                raise FileNotFoundError(f"Missing raw source file: {path}")
            df = pd.read_csv(path, dtype=str)
            df["_loaded_at"] = loaded_at
            df["_source_file"] = fname
            df["_run_id"] = run_id
            if table == "raw_orders":
                df["_is_increment"] = 0
            con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df WHERE 1=0")
            con.execute(f"INSERT INTO {table} SELECT * FROM df")
            total_rows += len(df)

        inc_path = os.path.join(DATA_DIR, "orders_2026-10-01_increment.csv")
        if os.path.exists(inc_path):
            inc_df = pd.read_csv(inc_path, dtype=str)
            inc_df["_loaded_at"] = loaded_at
            inc_df["_source_file"] = "orders_2026-10-01_increment.csv"
            inc_df["_run_id"] = run_id
            inc_df["_is_increment"] = 1
            con.execute("INSERT INTO raw_orders SELECT * FROM inc_df")
            total_rows += len(inc_df)

        con.close()
        return total_rows

    @task
    def run_staging_sql(raw_count: int, **context) -> dict:
        run_id = context["run_id"]
        dag_id = context["dag"].dag_id
        checked_at = pendulum.now(IST).to_iso8601_string()
        con = duckdb.connect(DB_PATH)
        with open(os.path.join(SQL_DIR, "staging.sql"), "r") as f:
            con.execute(f.read())

        checks = [
            ("elt_orders_dq", "Completeness/Validity/Accuracy", "Critical", "SELECT COUNT(*), COUNT(*) FILTER (WHERE dq_status='FAIL') FROM stg_orders"),
            ("elt_customers_dq", "Uniqueness/Timeliness", "Critical", "SELECT COUNT(*), COUNT(*) FILTER (WHERE dq_status='FAIL') FROM stg_customers"),
            ("elt_products_dq", "Uniqueness/Accuracy", "Critical", "SELECT COUNT(*), COUNT(*) FILTER (WHERE dq_status='FAIL') FROM stg_products"),
            ("elt_stores_dq", "Completeness", "Critical", "SELECT COUNT(*), COUNT(*) FILTER (WHERE dq_status='FAIL') FROM stg_stores"),
            ("elt_payments_dq", "Consistency", "Critical", "SELECT COUNT(*), COUNT(*) FILTER (WHERE dq_status='FAIL') FROM stg_payments"),
        ]
        con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
        total_passed = 0
        total_failed = 0
        for name, dim, sev, query in checks:
            checked, failed = con.execute(query).fetchone()
            total_passed += (checked - failed)
            total_failed += failed
            status = "FAIL" if failed > 0 else "PASS"
            con.execute("INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [run_id, dag_id, name, dim, sev, checked, failed, status, checked_at])

        con.close()
        return {"rows_extracted": raw_count, "rows_loaded": total_passed, "rows_quarantined": total_failed}

    @task
    def run_marts_sql(metrics: dict) -> dict:
        con = duckdb.connect(DB_PATH)
        with open(os.path.join(SQL_DIR, "marts.sql"), "r") as f:
            con.execute(f.read())
        print("=== ELT SEPT-ONLY CATEGORY MARGIN (RECONCILED WITH ETL) ===")
        print(con.execute("""
            SELECT p.category,
                   COUNT(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
                   ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END), 2) AS revenue
            FROM stg_orders o
            JOIN stg_products p ON o.product_id = p.product_id AND p.dq_status = 'PASS'
            WHERE o.dq_status = 'PASS' AND CAST(o.order_ts AS DATE) <= DATE '2026-09-30'
            GROUP BY 1 ORDER BY revenue DESC
        """).df().to_string(index=False))
        con.close()
        return metrics

    @task(trigger_rule="all_done")
    def log_elt_run(metrics: dict | None = None, **context):
        run_id = context["run_id"]
        dag_id = context["dag"].dag_id
        m = metrics or {"rows_extracted": 0, "rows_loaded": 0, "rows_quarantined": 0}
        con = duckdb.connect(DB_PATH)
        con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
        con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [run_id, dag_id, str(context["dag_run"].start_date), pendulum.now(IST).to_iso8601_string(), m["rows_extracted"], m["rows_loaded"], m["rows_quarantined"], "SUCCESS" if metrics else "FAILED"])
        con.close()

    raw_cnt = load_raw_tables()
    stg_metrics = run_staging_sql(raw_cnt)
    final_metrics = run_marts_sql(stg_metrics)
    log_elt_run(final_metrics)

nammamart_elt_pipeline()
