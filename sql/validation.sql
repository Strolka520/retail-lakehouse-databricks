-- Row counts
SELECT 'f_sales' AS tbl, COUNT(*) AS rows FROM gold.f_sales
UNION ALL SELECT 'd_customer', COUNT(*) FROM gold.d_customer
UNION ALL SELECT 'd_product',  COUNT(*) FROM gold.d_product
UNION ALL SELECT 'd_calendar', COUNT(*) FROM gold.d_calendar
;

-- Null FK diagnostics
SELECT COUNT(*) AS null_customer_in_fact
FROM gold.f_sales
WHERE customer_id IS NULL
;

SELECT COUNT(*) AS missing_product_dim
FROM gold.f_sales f
LEFT JOIN gold.d_product p ON p.product_id = f.product_id
WHERE p.product_id IS NULL
;

-- Basic reasonableness checks
SELECT
  MIN(order_date) AS min_date
, MAX(order_date) AS max_date
, COUNT(*)        AS fact_rows
, SUM(item_price) AS total_revenue
, SUM(freight)    AS total_freight
FROM gold.f_sales
;

-- Top 10 product categories by revenue
SELECT
  product_category_name
, SUM(item_price) AS revenue
FROM gold.vw_sales_enriched
GROUP BY product_category_name
ORDER BY revenue DESC
LIMIT 10
;

-- Revenue by state & day
SELECT
  order_date
, customer_state
, SUM(item_price) AS revenue›
FROM gold.vw_sales_enriched
GROUP BY order_date, customer_state
ORDER BY order_date, revenue DESC
LIMIT 200
;
