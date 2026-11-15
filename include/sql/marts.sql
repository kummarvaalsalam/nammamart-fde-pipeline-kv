CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    CAST(o.order_ts AS DATE) AS order_date,
    s.zone,
    o.store_id,
    s.store_name,
    COUNT(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END), 2) AS net_revenue
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id AND s.dq_status = 'PASS'
WHERE o.dq_status = 'PASS'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    COUNT(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END), 2) AS revenue,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN (o.order_amount - o.quantity * p.cost_price) ELSE 0 END), 2) AS gross_margin
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id AND p.dq_status = 'PASS'
WHERE o.dq_status = 'PASS'
GROUP BY 1
ORDER BY revenue DESC;

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    o.store_id,
    s.store_name,
    s.zone,
    COUNT(*) AS delivered_orders,
    ROUND(AVG(o.delivery_minutes), 2) AS avg_delivery_minutes,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.delivery_minutes <= 15) / NULLIF(COUNT(*), 0), 2) AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id AND s.dq_status = 'PASS'
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY 1, 2, 3
ORDER BY avg_delivery_minutes ASC;

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH succ_pay AS (
    SELECT order_id, SUM(amount_paid) AS amount_paid
    FROM stg_payments
    WHERE dq_status = 'PASS' AND payment_status = 'SUCCESS'
    GROUP BY 1
)
SELECT
    o.order_id,
    o.order_status,
    o.payment_mode,
    o.order_amount,
    COALESCE(p.amount_paid, 0.0) AS amount_paid,
    ROUND(o.order_amount - COALESCE(p.amount_paid, 0.0), 2) AS difference,
    CASE
        WHEN o.order_status = 'CANCELLED' AND COALESCE(p.amount_paid, 0.0) > 0 THEN 'Cancelled order has SUCCESS payment'
        WHEN o.order_status = 'DELIVERED' AND COALESCE(p.amount_paid, 0.0) = 0 THEN 'Delivered order missing SUCCESS payment'
        WHEN o.order_status = 'DELIVERED' AND ABS(o.order_amount - COALESCE(p.amount_paid, 0.0)) > 0.01 THEN 'Paid amount does not match order_amount'
        ELSE ''
    END AS mismatch_reason
FROM stg_orders o
LEFT JOIN succ_pay p ON o.order_id = p.order_id
WHERE o.dq_status = 'PASS'
  AND (
      (o.order_status = 'CANCELLED' AND COALESCE(p.amount_paid, 0.0) > 0)
      OR (o.order_status = 'DELIVERED' AND COALESCE(p.amount_paid, 0.0) = 0)
      OR (o.order_status = 'DELIVERED' AND ABS(o.order_amount - COALESCE(p.amount_paid, 0.0)) > 0.01)
  );

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    o.customer_id,
    c.full_name,
    c.email,
    c.phone,
    c.loyalty_tier,
    COUNT(*) AS delivered_orders,
    ROUND(SUM(o.order_amount), 2) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id AND c.dq_status = 'PASS'
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY 1, 2, 3, 4, 5
ORDER BY total_spend DESC
LIMIT 10;
