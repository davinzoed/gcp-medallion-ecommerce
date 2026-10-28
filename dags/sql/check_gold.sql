-- Data quality gate for the Gold layer.
-- The BigQuery job fails, and so does the Airflow task, if any assertion is false.

ASSERT (SELECT COUNT(*) FROM gold.fact_sales) > 0
  AS 'gold.fact_sales is empty';

ASSERT (SELECT COUNT(*) FROM gold.fact_sales) = (SELECT COUNT(*) FROM silver.order_items)
  AS 'gold.fact_sales row count does not match silver.order_items';

ASSERT (
  SELECT COUNTIF(c.customer_id IS NULL OR p.product_id IS NULL OR d.date_key IS NULL)
  FROM gold.fact_sales AS f
  LEFT JOIN gold.dim_customer AS c ON c.customer_id = f.customer_id
  LEFT JOIN gold.dim_product AS p ON p.product_id = f.product_id
  LEFT JOIN gold.dim_date AS d ON d.date_key = f.order_date
) = 0
  AS 'gold.fact_sales contains keys that are missing from a dimension';