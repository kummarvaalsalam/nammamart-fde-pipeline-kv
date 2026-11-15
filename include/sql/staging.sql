CREATE OR REPLACE TABLE stg_stores AS
WITH dedup AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY _loaded_at) AS rn
    FROM raw_stores
)
SELECT
    store_id, store_name, zone, area, pincode,
    TRY_CAST(open_date AS DATE) AS open_date,
    manager_name,
    TRY_CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
    CASE
        WHEN rn > 1 THEN 'FAIL'
        WHEN zone IS NULL OR TRIM(zone) = '' THEN 'FAIL'
        WHEN TRY_CAST(capacity_orders_per_day AS INTEGER) IS NULL OR TRY_CAST(capacity_orders_per_day AS INTEGER) <= 0 THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    TRIM(
        CASE WHEN rn > 1 THEN 'Duplicate store_id; ' ELSE '' END ||
        CASE WHEN zone IS NULL OR TRIM(zone) = '' THEN 'Null zone; ' ELSE '' END ||
        CASE WHEN TRY_CAST(capacity_orders_per_day AS INTEGER) IS NULL OR TRY_CAST(capacity_orders_per_day AS INTEGER) <= 0 THEN 'Invalid capacity; ' ELSE '' END
    ) AS failure_reason
FROM dedup;

CREATE OR REPLACE TABLE stg_products AS
WITH dedup AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _loaded_at) AS rn
    FROM raw_products
)
SELECT
    product_id, product_name, category, sub_category, brand,
    TRY_CAST(mrp AS DOUBLE) AS mrp,
    TRY_CAST(cost_price AS DOUBLE) AS cost_price,
    unit, is_perishable,
    TRY_CAST(launch_date AS DATE) AS launch_date,
    CASE
        WHEN rn > 1 THEN 'FAIL'
        WHEN category IS NULL OR TRIM(category) = '' THEN 'FAIL'
        WHEN TRY_CAST(mrp AS DOUBLE) IS NULL OR TRY_CAST(mrp AS DOUBLE) <= 0
             OR TRY_CAST(cost_price AS DOUBLE) IS NULL OR TRY_CAST(cost_price AS DOUBLE) > TRY_CAST(mrp AS DOUBLE) THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    TRIM(
        CASE WHEN rn > 1 THEN 'Duplicate product_id; ' ELSE '' END ||
        CASE WHEN category IS NULL OR TRIM(category) = '' THEN 'Null category; ' ELSE '' END ||
        CASE WHEN TRY_CAST(mrp AS DOUBLE) IS NULL OR TRY_CAST(mrp AS DOUBLE) <= 0
             OR TRY_CAST(cost_price AS DOUBLE) IS NULL OR TRY_CAST(cost_price AS DOUBLE) > TRY_CAST(mrp AS DOUBLE) THEN 'Invalid mrp or cost_price > mrp; ' ELSE '' END
    ) AS failure_reason
FROM dedup;

CREATE OR REPLACE TABLE stg_customers AS
WITH dedup AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at) AS rn
    FROM raw_customers
)
SELECT
    customer_id, full_name,
    CASE WHEN email LIKE '%@%' THEN SUBSTR(TRIM(email), 1, 2) || '***@' || SPLIT_PART(TRIM(email), '@', 2) ELSE NULL END AS email,
    CASE WHEN LENGTH(REGEXP_REPLACE(COALESCE(phone, ''), '\D', '', 'g')) >= 4
         THEN SUBSTR(REGEXP_REPLACE(phone, '\D', '', 'g'), 1, 2) || '******' || RIGHT(REGEXP_REPLACE(phone, '\D', '', 'g'), 2)
         ELSE NULL END AS phone,
    city, area, pincode,
    TRY_CAST(signup_date AS DATE) AS signup_date,
    UPPER(SUBSTR(TRIM(loyalty_tier), 1, 1)) || LOWER(SUBSTR(TRIM(loyalty_tier), 2)) AS loyalty_tier,
    is_active,
    CASE
        WHEN rn > 1 THEN 'FAIL'
        WHEN TRY_CAST(signup_date AS DATE) > DATE '2026-10-02' THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    TRIM(
        CASE WHEN rn > 1 THEN 'Duplicate customer_id; ' ELSE '' END ||
        CASE WHEN TRY_CAST(signup_date AS DATE) > DATE '2026-10-02' THEN 'Future signup_date; ' ELSE '' END
    ) AS failure_reason
FROM dedup;

CREATE OR REPLACE TABLE stg_orders AS
WITH dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _is_increment DESC, _loaded_at DESC) AS rn
    FROM raw_orders
),
norm AS (
    SELECT
        order_id,
        TRY_STRPTIME(order_ts, '%Y-%m-%d %H:%M:%S') AS order_ts,
        customer_id, store_id, product_id,
        TRY_CAST(quantity AS INTEGER) AS quantity,
        TRY_CAST(unit_price AS DOUBLE) AS unit_price,
        TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
        TRY_CAST(order_amount AS DOUBLE) AS order_amount,
        CASE UPPER(TRIM(payment_mode))
            WHEN 'UPI' THEN 'UPI'
            WHEN 'CARD' THEN 'Card'
            WHEN 'CASH ON DELIVERY' THEN 'Cash on Delivery'
            WHEN 'COD' THEN 'Cash on Delivery'
            WHEN 'WALLET' THEN 'Wallet'
            ELSE NULL
        END AS payment_mode,
        UPPER(TRIM(order_status)) AS order_status,
        channel,
        TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
        rn, _source_file
    FROM dedup
)
SELECT
    n.*,
    CASE
        WHEN n.rn > 1 THEN 'FAIL'
        WHEN n.customer_id IS NULL OR TRIM(n.customer_id) = '' THEN 'FAIL'
        WHEN n.order_ts IS NULL OR n.order_ts > TIMESTAMP '2026-10-02 23:59:59' THEN 'FAIL'
        WHEN n.order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') OR n.order_status IS NULL THEN 'FAIL'
        WHEN n.payment_mode IS NULL THEN 'FAIL'
        WHEN n.quantity IS NULL OR n.quantity <= 0 THEN 'FAIL'
        WHEN n.discount_pct IS NULL OR n.discount_pct < 0 OR n.discount_pct > 100 THEN 'FAIL'
        WHEN n.order_amount IS NULL OR ABS(n.order_amount - ROUND(n.quantity * n.unit_price * (1 - n.discount_pct / 100.0), 2)) > 0.01 THEN 'FAIL'
        WHEN n.order_status = 'DELIVERED' AND n.delivery_minutes IS NULL THEN 'FAIL'
        WHEN n.delivery_minutes IS NOT NULL AND (n.delivery_minutes <= 0 OR n.delivery_minutes > 120) THEN 'FAIL'
        WHEN c.customer_id IS NULL OR p.product_id IS NULL OR s.store_id IS NULL THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    TRIM(
        CASE WHEN n.rn > 1 THEN 'Duplicate order_id; ' ELSE '' END ||
        CASE WHEN n.customer_id IS NULL OR TRIM(n.customer_id) = '' THEN 'Null customer_id; ' ELSE '' END ||
        CASE WHEN n.order_ts IS NULL THEN 'Invalid order_ts; ' ELSE '' END ||
        CASE WHEN n.order_ts > TIMESTAMP '2026-10-02 23:59:59' THEN 'Future order_ts; ' ELSE '' END ||
        CASE WHEN n.order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') OR n.order_status IS NULL THEN 'Invalid order_status; ' ELSE '' END ||
        CASE WHEN n.payment_mode IS NULL THEN 'Invalid payment_mode; ' ELSE '' END ||
        CASE WHEN n.quantity IS NULL OR n.quantity <= 0 THEN 'Quantity <= 0; ' ELSE '' END ||
        CASE WHEN n.discount_pct IS NULL OR n.discount_pct < 0 OR n.discount_pct > 100 THEN 'Invalid discount_pct; ' ELSE '' END ||
        CASE WHEN n.order_amount IS NULL OR ABS(n.order_amount - ROUND(n.quantity * n.unit_price * (1 - n.discount_pct / 100.0), 2)) > 0.01 THEN 'Order amount formula mismatch; ' ELSE '' END ||
        CASE WHEN n.order_status = 'DELIVERED' AND n.delivery_minutes IS NULL THEN 'Delivered order missing delivery_minutes; ' ELSE '' END ||
        CASE WHEN n.delivery_minutes IS NOT NULL AND (n.delivery_minutes <= 0 OR n.delivery_minutes > 120) THEN 'Delivery minutes out of range; ' ELSE '' END ||
        CASE WHEN c.customer_id IS NULL THEN 'Unknown customer_id; ' ELSE '' END ||
        CASE WHEN p.product_id IS NULL THEN 'Unknown product_id; ' ELSE '' END ||
        CASE WHEN s.store_id IS NULL THEN 'Unknown store_id; ' ELSE '' END
    ) AS failure_reason
FROM norm n
LEFT JOIN stg_customers c ON n.customer_id = c.customer_id AND c.dq_status = 'PASS'
LEFT JOIN stg_products p ON n.product_id = p.product_id AND p.dq_status = 'PASS'
LEFT JOIN stg_stores s ON n.store_id = s.store_id AND s.dq_status = 'PASS';

CREATE OR REPLACE TABLE stg_payments AS
WITH dedup AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _loaded_at) AS rn
    FROM raw_payments
)
SELECT
    d.payment_id, d.order_id,
    TRY_STRPTIME(d.payment_ts, '%Y-%m-%d %H:%M:%S') AS payment_ts,
    d.payment_mode,
    TRY_CAST(d.amount_paid AS DOUBLE) AS amount_paid,
    UPPER(TRIM(d.payment_status)) AS payment_status,
    d.gateway_ref,
    CASE WHEN d.rn > 1 OR o.order_id IS NULL THEN 'FAIL' ELSE 'PASS' END AS dq_status,
    TRIM(
        CASE WHEN d.rn > 1 THEN 'Duplicate payment_id; ' ELSE '' END ||
        CASE WHEN o.order_id IS NULL THEN 'Orphan order_id; ' ELSE '' END
    ) AS failure_reason
FROM dedup d
LEFT JOIN stg_orders o ON d.order_id = o.order_id AND o.dq_status = 'PASS';
