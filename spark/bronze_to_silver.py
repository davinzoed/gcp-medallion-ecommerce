"""
Bronze-to-Silver transformation for the e-commerce medallion pipeline.

Reads the raw CSV files from the Bronze layer in Cloud Storage, applies explicit
typing, cleaning, de-duplication and referential-integrity rules, and writes the
result as Parquet files to the Silver layer.

Storage layout:
    Input:  gs://<bucket>/bronze/<table>/<table>.csv
    Output: gs://<bucket>/silver/<table>/

Processing rules:
    - All columns are read as strings, then cast explicitly. Values that cannot
      be cast become NULL and are removed by the validity filters.
    - Duplicate primary keys are removed (one record is kept per key).
    - Missing "city" and "payment_method" values are replaced with "Unknown".
    - Order lines with a non-positive quantity or a missing unit price are dropped.
    - Records that reference a non-existent parent (customer, order or product)
      are dropped, so every foreign key in Silver is valid.

Usage:
    spark-submit bronze_to_silver.py --bucket <bucket-name>
"""


import argparse
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
TABLES = ["customers", "products", "orders", "order_items"]


def parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments.

    Returns:
        A namespace with the "bucket" attribute: the name of the Cloud Storage
        bucket (without the "gs://" prefix) that holds the Bronze and Silver layers.
    """
    parser = argparse.ArgumentParser(description="Bronze-to-Silver transformation.")
    parser.add_argument("--bucket", required=True, help="Cloud Storage bucket name.")
    return parser.parse_args()


def read_bronze(spark: SparkSession, root: str, table: str) -> DataFrame:
    """
    Reads one raw CSV table from the Bronze layer.

    Schema inference is intentionally disabled, so every column is read as a
    string. Typing is applied explicitly in the transformation functions.

    Args:
        spark: The active Spark session.
        root: The storage root, for example "gs://my-bucket".
        table: The table name, which is also the folder and file name.

    Returns:
        A DataFrame with all columns of type string.
    """
    return spark.read.option("header", True).csv(f"{root}/bronze/{table}/{table}.csv")


def fill_unknown(column: str) -> Column:
    """
    Replaces missing or blank string values with the literal "Unknown".

    Args:
        column: The name of the string column to clean.

    Returns:
        A column expression with the same name as the input column.
    """
    trimmed = F.trim(F.col(column))
    return F.when(trimmed != "", trimmed).otherwise(F.lit("Unknown")).alias(column)


def transform_customers(df: DataFrame) -> DataFrame:
    """
    Cleans the customers table.

    Args:
        df: Raw customers from the Bronze layer.

    Returns:
        Typed customers with one row per "customer_id".
    """
    return (
        df.select(
            F.col("customer_id").cast("int").alias("customer_id"),
            F.trim("customer_name").alias("customer_name"),
            fill_unknown("city"),
            F.trim("state").alias("state"),
            F.trim("country").alias("country"),
            F.to_date("signup_date").alias("signup_date"),
        )
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )


def transform_products(df: DataFrame) -> DataFrame:
    """
    Cleans the products table.

    Args:
        df: Raw products from the Bronze layer.

    Returns:
        Typed products with one row per "product_id". A missing catalogue
        "price" is kept as NULL, because revenue is calculated from the
        order-line "unit_price" instead.
    """
    return (
        df.select(
            F.col("product_id").cast("int").alias("product_id"),
            F.trim("product_name").alias("product_name"),
            F.trim("category").alias("category"),
            F.col("price").cast("decimal(12,2)").alias("price"),
        )
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
    )


def transform_orders(df: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Cleans the orders table and enforces the customer foreign key.

    Args:
        df: Raw orders from the Bronze layer.
        customers: Clean customers, used to validate "customer_id".

    Returns:
        Typed orders with one row per "order_id", restricted to orders that
        belong to an existing customer.
    """
    return (
        df.select(
            F.col("order_id").cast("int").alias("order_id"),
            F.col("customer_id").cast("int").alias("customer_id"),
            F.to_date("order_date").alias("order_date"),
            fill_unknown("payment_method"),
            F.trim("order_status").alias("order_status"),
        )
        .filter(F.col("order_id").isNotNull())
        .dropDuplicates(["order_id"])
        # A left-semi join keeps rows whose key exists in the right table.
        .join(customers.select("customer_id"), "customer_id", "left_semi")
    )


def transform_order_items(
    df: DataFrame, orders: DataFrame, products: DataFrame) -> DataFrame:
    """
    Cleans the order_items table and enforces the order and product foreign keys.

    Args:
        df: Raw order lines from the Bronze layer.
        orders: Clean orders, used to validate "order_id".
        products: Clean products, used to validate "product_id".

    Returns:
        Typed order lines with a positive quantity and a known unit price,
        restricted to lines that belong to an existing order and product.
    """
    return (
        df.select(
            F.col("order_id").cast("int").alias("order_id"),
            F.col("product_id").cast("int").alias("product_id"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("unit_price").cast("decimal(12,2)").alias("unit_price"),
        )
        .filter((F.col("quantity") > 0) & F.col("unit_price").isNotNull())
        .join(orders.select("order_id"), "order_id", "left_semi")
        .join(products.select("product_id"), "product_id", "left_semi")
    )


def main():
    """Runs the Bronze-to-Silver job and prints row counts for each table."""
    root = f"gs://{parse_args().bucket}"
    spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()

    raw = {table: read_bronze(spark, root, table) for table in TABLES}

    # Order matters: each table is validated against its already-cleaned parent.
    customers = transform_customers(raw["customers"])
    products = transform_products(raw["products"])
    orders = transform_orders(raw["orders"], customers)
    order_items = transform_order_items(raw["order_items"], orders, products)

    silver = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }

    for table, df in silver.items():
        # Overwrite mode makes the job idempotent: re-runs produce the same output.
        df.write.mode("overwrite").parquet(f"{root}/silver/{table}")
        print(f"RESULT {table}: raw={raw[table].count()} silver={df.count()}")


if __name__ == "__main__":
    main()