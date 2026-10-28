-- Fact table. Grain: one row per order line.
-- Holds measures and foreign keys only; descriptive attributes live in the dimensions.
-- Revenue rule, defined once here: only Completed and Shipped orders count as revenue.
CREATE OR REPLACE TABLE gold.fact_sales
PARTITION BY DATE_TRUNC(order_date, MONTH)
CLUSTER BY customer_id, product_id
AS
SELECT
  oi.order_id,
  oi.product_id,        -- FK to dim_product
  o.customer_id,        -- FK to dim_customer
  o.order_date,         -- FK to dim_date.date_key
  o.order_status,
  o.payment_method,
  oi.quantity,
  oi.unit_price,
  oi.quantity * oi.unit_price AS line_total,
  o.order_status IN ('Completed', 'Shipped') AS is_revenue
FROM silver.order_items AS oi
JOIN silver.orders AS o 
ON o.order_id = oi.order_id;