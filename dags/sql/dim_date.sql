-- Date dimension. One row per calendar day, covering the full range of order dates.
CREATE OR REPLACE TABLE gold.dim_date AS
WITH bounds AS (
  SELECT MIN(order_date) AS min_date, MAX(order_date) AS max_date
  FROM silver.orders
)
SELECT
  d AS date_key,
  EXTRACT(YEAR FROM d) AS year,
  EXTRACT(QUARTER FROM d) AS quarter,
  EXTRACT(MONTH FROM d) AS month,
  FORMAT_DATE('%B', d) AS month_name,
  EXTRACT(DAY FROM d) AS day_of_month,
  FORMAT_DATE('%A', d) AS day_name,
  EXTRACT(DAYOFWEEK FROM d) IN (1, 7) AS is_weekend
FROM bounds,
UNNEST(GENERATE_DATE_ARRAY(min_date, max_date)) AS d;