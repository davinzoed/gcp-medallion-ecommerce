-- Product dimension. One row per product.
-- list_price is the catalogue price and can be NULL; revenue is calculated
-- from the order-line unit_price in the fact table, not from this column.
CREATE OR REPLACE TABLE gold.dim_product AS
SELECT
  product_id,
  product_name,
  category,
  price AS list_price
FROM silver.products;