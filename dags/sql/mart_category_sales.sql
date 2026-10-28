-- Monthly revenue per product category: a star query over the fact and two dimensions.
CREATE OR REPLACE TABLE gold.mart_category_sales AS
SELECT
  DATE_TRUNC(f.order_date, MONTH) AS order_month,
  p.category,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(f.quantity) AS units_sold,
  SUM(f.line_total) AS revenue
FROM gold.fact_sales AS f
JOIN gold.dim_product AS p ON p.product_id = f.product_id
WHERE f.is_revenue
GROUP BY order_month, p.category;