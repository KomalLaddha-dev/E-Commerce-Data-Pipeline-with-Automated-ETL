-- ============================================================
-- BUSINESS ANALYTICS QUERIES
-- E-commerce Data Pipeline with Automated ETL
-- Run against ecommerce_dw (star schema)
-- ============================================================


-- 1. DAILY SALES SUMMARY
SELECT
    d.full_date,
    d.day_name,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_items_sold,
    SUM(f.line_total) AS gross_revenue,
    SUM(f.net_amount) AS net_revenue,
    SUM(f.gross_profit) AS gross_profit,
    ROUND(AVG(f.net_amount), 2) AS avg_line_value,
    COUNT(DISTINCT f.customer_key) AS unique_customers
FROM fact_order_items f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = EXTRACT(YEAR FROM CURRENT_DATE)::INT
  AND f.order_status NOT IN ('cancelled', 'returned')
GROUP BY d.full_date, d.day_name
ORDER BY d.full_date DESC;


-- 2. MONTHLY REVENUE TREND WITH MoM GROWTH
SELECT
    d.year, d.month, d.month_name,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.net_amount) AS monthly_revenue,
    SUM(f.gross_profit) AS monthly_profit,
    LAG(SUM(f.net_amount)) OVER (ORDER BY d.year, d.month) AS prev_month_revenue,
    ROUND(
        (SUM(f.net_amount) - LAG(SUM(f.net_amount)) OVER (ORDER BY d.year, d.month))
        / NULLIF(LAG(SUM(f.net_amount)) OVER (ORDER BY d.year, d.month), 0) * 100, 2
    ) AS mom_growth_pct
FROM fact_order_items f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- 3. TOP 10 PRODUCTS BY REVENUE
SELECT
    p.product_name, p.category, p.brand,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.net_amount) AS total_revenue,
    SUM(f.gross_profit) AS total_profit,
    p.profit_margin AS margin_pct
FROM fact_order_items f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY p.product_name, p.category, p.brand, p.profit_margin
ORDER BY total_revenue DESC
LIMIT 10;


-- 4. CUSTOMER LIFETIME VALUE (TOP 20)
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.city, c.segment,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.net_amount) AS lifetime_value,
    ROUND(AVG(f.net_amount), 2) AS avg_line_value,
    MIN(d.full_date) AS first_purchase,
    MAX(d.full_date) AS last_purchase,
    MAX(d.full_date) - MIN(d.full_date) AS tenure_days
FROM fact_order_items f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.segment
ORDER BY lifetime_value DESC
LIMIT 20;


-- 5. CUSTOMER SEGMENTATION ANALYSIS
SELECT
    c.segment,
    COUNT(DISTINCT c.customer_key) AS customer_count,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.net_amount) AS total_revenue,
    SUM(f.gross_profit) AS total_profit,
    ROUND(AVG(f.net_amount), 2) AS avg_line_value,
    ROUND(SUM(f.net_amount) / NULLIF(COUNT(DISTINCT c.customer_key), 0), 2) AS revenue_per_customer
FROM fact_order_items f
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY c.segment
ORDER BY total_revenue DESC;


-- 6. CATEGORY PERFORMANCE
SELECT
    p.category,
    COUNT(DISTINCT p.product_key) AS product_count,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.net_amount) AS total_revenue,
    SUM(f.gross_profit) AS total_profit,
    ROUND(AVG(p.profit_margin), 2) AS avg_margin_pct,
    ROUND(SUM(f.net_amount) * 100.0 / SUM(SUM(f.net_amount)) OVER (), 2) AS revenue_share_pct
FROM fact_order_items f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY p.category
ORDER BY total_revenue DESC;


-- 7. PAYMENT METHOD ANALYSIS
SELECT
    pm.display_name AS payment_method,
    pm.method_type,
    COUNT(DISTINCT f.order_id) AS transaction_count,
    SUM(f.net_amount) AS total_revenue,
    ROUND(AVG(f.net_amount), 2) AS avg_transaction_value,
    ROUND(COUNT(DISTINCT f.order_id) * 100.0 / SUM(COUNT(DISTINCT f.order_id)) OVER (), 2) AS pct_share
FROM fact_order_items f
JOIN dim_payment_method pm ON f.payment_method_key = pm.payment_method_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY pm.display_name, pm.method_type
ORDER BY total_revenue DESC;


-- 8. GEOGRAPHIC SALES ANALYSIS
SELECT
    l.region, l.state, l.city, l.tier,
    COUNT(DISTINCT c.customer_key) AS customer_count,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.net_amount) AS total_revenue,
    ROUND(AVG(f.net_amount), 2) AS avg_line_value
FROM fact_order_items f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_location l ON f.location_key = l.location_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY l.region, l.state, l.city, l.tier
ORDER BY total_revenue DESC
LIMIT 20;


-- 9. WEEKEND vs WEEKDAY SALES
SELECT
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.net_amount) AS total_revenue,
    SUM(f.gross_profit) AS total_profit,
    ROUND(AVG(f.net_amount), 2) AS avg_line_value
FROM fact_order_items f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END;


-- 10. ORDER STATUS DISTRIBUTION
SELECT
    f.order_status,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.line_total) AS total_value,
    ROUND(COUNT(DISTINCT f.order_id) * 100.0
          / SUM(COUNT(DISTINCT f.order_id)) OVER (), 2) AS percentage
FROM fact_order_items f
GROUP BY f.order_status
ORDER BY order_count DESC;


-- 11. REPEAT CUSTOMER RATE
WITH customer_orders AS (
    SELECT
        customer_key,
        COUNT(DISTINCT order_id) AS order_count
    FROM fact_order_items
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
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM customer_orders
GROUP BY
    CASE
        WHEN order_count = 1 THEN 'One-time'
        WHEN order_count BETWEEN 2 AND 3 THEN 'Returning (2-3)'
        WHEN order_count BETWEEN 4 AND 10 THEN 'Loyal (4-10)'
        ELSE 'VIP (10+)'
    END
ORDER BY customer_count DESC;


-- 12. QUARTERLY PERFORMANCE
SELECT
    d.year, d.quarter,
    'Q' || d.quarter || ' ' || d.year AS quarter_label,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    SUM(f.net_amount) AS quarterly_revenue,
    SUM(f.gross_profit) AS quarterly_profit,
    ROUND(AVG(f.net_amount), 2) AS avg_line_value
FROM fact_order_items f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;
