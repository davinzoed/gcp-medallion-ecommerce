-- Daily revenue KPIs: a star query over the fact and the date dimension.
CREATE OR REPLACE TABLE gold.mart_daily_sales AS
SELECT
  d.date_key AS order_date,
  d.year,
  d.month,
  d.day_name,
  d.is_weekend,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(f.quantity) AS units_sold,
  SUM(f.line_total) AS revenue
FROM gold.fact_sales AS f
JOIN gold.dim_date AS d
ON d.date_key = f.order_date
WHERE f.is_revenue
GROUP BY d.date_key, d.year, d.month, d.day_name, d.is_weekend;