-- Customer dimension. One row per customer.
CREATE OR REPLACE TABLE gold.dim_customer AS
SELECT
  customer_id,
  customer_name,
  city,
  state,
  country,
  signup_date
FROM silver.customers;
