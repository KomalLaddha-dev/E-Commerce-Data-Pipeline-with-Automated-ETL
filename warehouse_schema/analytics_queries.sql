-- ============================================================
-- BUSINESS ANALYTICS QUERIES
-- E-commerce Data Pipeline with Automated ETL
-- Run these against the data warehouse (star schema)
-- ============================================================


-- ────────────────────────────────────────────
-- 1. TOTAL SALES PER DAY
-- Shows daily revenue trends with order counts
-- ────────────────────────────────────────────
SELECT
    d.full_date,
    d.day_name,
    COUNT(DISTINCT f.order_id)      AS total_orders,
    SUM(f.quantity)                  AS total_items_sold,
    SUM(f.total_amount)             AS gross_revenue,
    SUM(f.discount_amount)          AS total_discounts,
    SUM(f.net_amount)               AS net_revenue,
    ROUND(AVG(f.net_amount), 2)     AS avg_order_value
FROM sales_fact f
JOIN date_dim d ON f.date_key = d.date_key
WHERE d.year = 2025
  AND f.order_status NOT IN ('cancelled', 'returned')
GROUP BY d.full_date, d.day_name
ORDER BY d.full_date DESC;


-- ────────────────────────────────────────────
-- 2. BEST SELLING PRODUCTS (TOP 10)
-- Identifies highest-revenue products
-- ────────────────────────────────────────────
SELECT
    p.product_name,
    p.category,
    p.brand,
    SUM(f.quantity)                  AS total_units_sold,
    SUM(f.net_amount)               AS total_revenue,
    ROUND(AVG(f.unit_price), 2)     AS avg_selling_price,
    p.profit_margin                  AS margin_pct
FROM sales_fact f
JOIN product_dim p ON f.product_key = p.product_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY p.product_name, p.category, p.brand, p.profit_margin
ORDER BY total_revenue DESC
LIMIT 10;


-- ────────────────────────────────────────────
-- 3. CUSTOMER LIFETIME VALUE (TOP 20)
-- Ranks customers by total spending
-- ────────────────────────────────────────────
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name   AS customer_name,
    c.city,
    c.segment,
    COUNT(DISTINCT f.order_id)            AS total_orders,
    SUM(f.net_amount)                     AS lifetime_value,
    ROUND(AVG(f.net_amount), 2)           AS avg_order_value,
    MIN(d.full_date)                      AS first_purchase,
    MAX(d.full_date)                      AS last_purchase,
    MAX(d.full_date) - MIN(d.full_date)   AS customer_tenure_days
FROM sales_fact f
JOIN customer_dim c ON f.customer_key = c.customer_key
JOIN date_dim d ON f.date_key = d.date_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.segment
ORDER BY lifetime_value DESC
LIMIT 20;


-- ────────────────────────────────────────────
-- 4. MONTHLY REVENUE TREND
-- Shows month-over-month growth
-- ────────────────────────────────────────────
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    SUM(f.net_amount)                   AS monthly_revenue,
    LAG(SUM(f.net_amount)) OVER (
        ORDER BY d.year, d.month
    )                                   AS prev_month_revenue,
    ROUND(
        (SUM(f.net_amount) - LAG(SUM(f.net_amount)) OVER (ORDER BY d.year, d.month))
        / NULLIF(LAG(SUM(f.net_amount)) OVER (ORDER BY d.year, d.month), 0) * 100,
        2
    )                                   AS mom_growth_pct
FROM sales_fact f
JOIN date_dim d ON f.date_key = d.date_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- ────────────────────────────────────────────
-- 5. PAYMENT METHOD ANALYSIS
-- Shows most popular payment methods
-- ────────────────────────────────────────────
SELECT
    pm.payment_method,
    COUNT(f.order_id)                     AS transaction_count,
    SUM(f.net_amount)                     AS total_revenue,
    ROUND(AVG(f.net_amount), 2)           AS avg_transaction_value,
    ROUND(
        COUNT(f.order_id) * 100.0 / SUM(COUNT(f.order_id)) OVER(), 2
    )                                     AS percentage_share
FROM sales_fact f
JOIN payment_dim pm ON f.payment_key = pm.payment_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY pm.payment_method
ORDER BY total_revenue DESC;


-- ────────────────────────────────────────────
-- 6. SALES BY PRODUCT CATEGORY
-- Category-level revenue breakdown
-- ────────────────────────────────────────────
SELECT
    p.category,
    COUNT(DISTINCT p.product_id)          AS product_count,
    COUNT(DISTINCT f.order_id)            AS total_orders,
    SUM(f.quantity)                        AS total_units_sold,
    SUM(f.net_amount)                     AS total_revenue,
    ROUND(AVG(p.profit_margin), 2)        AS avg_margin_pct,
    ROUND(
        SUM(f.net_amount) * 100.0 / SUM(SUM(f.net_amount)) OVER(), 2
    )                                     AS revenue_share_pct
FROM sales_fact f
JOIN product_dim p ON f.product_key = p.product_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY p.category
ORDER BY total_revenue DESC;


-- ────────────────────────────────────────────
-- 7. CUSTOMER SEGMENTATION ANALYSIS
-- Breakdown by customer segment
-- ────────────────────────────────────────────
SELECT
    c.segment,
    COUNT(DISTINCT c.customer_id)         AS customer_count,
    COUNT(DISTINCT f.order_id)            AS total_orders,
    SUM(f.net_amount)                     AS total_revenue,
    ROUND(AVG(f.net_amount), 2)           AS avg_order_value,
    ROUND(
        SUM(f.net_amount) / COUNT(DISTINCT c.customer_id), 2
    )                                     AS revenue_per_customer
FROM sales_fact f
JOIN customer_dim c ON f.customer_key = c.customer_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY c.segment
ORDER BY total_revenue DESC;


-- ────────────────────────────────────────────
-- 8. WEEKEND vs WEEKDAY SALES
-- Compare performance by day type
-- ────────────────────────────────────────────
SELECT
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(DISTINCT f.order_id)            AS total_orders,
    SUM(f.net_amount)                     AS total_revenue,
    ROUND(AVG(f.net_amount), 2)           AS avg_order_value,
    ROUND(
        COUNT(DISTINCT f.order_id) * 1.0 / COUNT(DISTINCT d.full_date), 2
    )                                     AS avg_orders_per_day
FROM sales_fact f
JOIN date_dim d ON f.date_key = d.date_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
  AND d.year = 2025
GROUP BY CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END;


-- ────────────────────────────────────────────
-- 9. ORDER STATUS DISTRIBUTION
-- Funnel analysis of order completion
-- ────────────────────────────────────────────
SELECT
    f.order_status,
    COUNT(f.order_id)                     AS order_count,
    SUM(f.total_amount)                   AS total_value,
    ROUND(
        COUNT(f.order_id) * 100.0 / SUM(COUNT(f.order_id)) OVER(), 2
    )                                     AS percentage
FROM sales_fact f
GROUP BY f.order_status
ORDER BY order_count DESC;


-- ────────────────────────────────────────────
-- 10. GEOGRAPHIC SALES ANALYSIS
-- Revenue breakdown by city and state
-- ────────────────────────────────────────────
SELECT
    c.state,
    c.city,
    COUNT(DISTINCT c.customer_id)         AS customer_count,
    COUNT(DISTINCT f.order_id)            AS total_orders,
    SUM(f.net_amount)                     AS total_revenue,
    ROUND(AVG(f.net_amount), 2)           AS avg_order_value
FROM sales_fact f
JOIN customer_dim c ON f.customer_key = c.customer_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY c.state, c.city
ORDER BY total_revenue DESC
LIMIT 15;


-- ────────────────────────────────────────────
-- 11. QUARTERLY PERFORMANCE SUMMARY
-- High-level quarterly KPIs
-- ────────────────────────────────────────────
SELECT
    d.year,
    d.quarter,
    'Q' || d.quarter || ' ' || d.year     AS quarter_label,
    COUNT(DISTINCT f.order_id)            AS total_orders,
    COUNT(DISTINCT f.customer_key)        AS unique_customers,
    SUM(f.net_amount)                     AS quarterly_revenue,
    SUM(f.discount_amount)               AS total_discounts,
    ROUND(AVG(f.net_amount), 2)          AS avg_order_value,
    ROUND(
        SUM(f.discount_amount) / NULLIF(SUM(f.total_amount), 0) * 100, 2
    )                                     AS discount_rate_pct
FROM sales_fact f
JOIN date_dim d ON f.date_key = d.date_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;


-- ────────────────────────────────────────────
-- 12. REPEAT CUSTOMER RATE
-- Identify returning customers
-- ────────────────────────────────────────────
WITH customer_orders AS (
    SELECT
        customer_key,
        COUNT(DISTINCT order_id) AS order_count
    FROM sales_fact
    WHERE order_status NOT IN ('cancelled', 'returned')
    GROUP BY customer_key
)
SELECT
    CASE
        WHEN order_count = 1 THEN 'One-time'
        WHEN order_count BETWEEN 2 AND 3 THEN 'Returning (2-3)'
        WHEN order_count BETWEEN 4 AND 10 THEN 'Loyal (4-10)'
        ELSE 'VIP (10+)'
    END AS customer_type,
    COUNT(*)                               AS customer_count,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2
    )                                      AS percentage
FROM customer_orders
GROUP BY
    CASE
        WHEN order_count = 1 THEN 'One-time'
        WHEN order_count BETWEEN 2 AND 3 THEN 'Returning (2-3)'
        WHEN order_count BETWEEN 4 AND 10 THEN 'Loyal (4-10)'
        ELSE 'VIP (10+)'
    END
ORDER BY customer_count DESC;
