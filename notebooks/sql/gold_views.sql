-- Create a BI-friendly view joining fact and dims

CREATE OR REPLACE VIEW retail_gold.vw_sales_enriched AS
SELECT
  fs.order_id
, fs.order_item_id
, fs.customer_id
, fs.product_id
, fs.order_date
, dc.customer_city
, dc.customer_state
, dp.product_category_name
, fs.item_price
, fs.freight
FROM retail_gold.f_sales AS fs
LEFT JOIN retail_gold.d_customer AS dc
  ON dc.customer_id = fs.customer_id
LEFT JOIN retail_gold.d_product AS dp
  ON dp.product_id = fs.product_id
;
