"""
E-commerce medallion pipeline, orchestrated by Cloud Composer (Apache Airflow).

Pipeline stages:
    1. Bronze to Silver: a PySpark batch on Dataproc Serverless cleans the raw
       CSV files and writes Parquet to the Silver layer in Cloud Storage.
    2. Silver load: the Parquet files are loaded into the BigQuery "silver" dataset.
    3. Gold: dimension tables, then the fact table, then the marts, are built in
       the BigQuery "gold" dataset with SQL (star schema).
    4. Quality gate: assertions verify row counts and key integrity.

Configuration is read from environment variables that are set when the
Composer environment is created, so the DAG contains no hard-coded project values:
    MEDALLION_PROJECT_ID, MEDALLION_REGION, MEDALLION_BUCKET, MEDALLION_SA_EMAIL
"""


import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = os.environ["MEDALLION_PROJECT_ID"]
REGION = os.environ["MEDALLION_REGION"]
BUCKET = os.environ["MEDALLION_BUCKET"]
SERVICE_ACCOUNT = os.environ["MEDALLION_SA_EMAIL"]

SQL_DIR = Path(__file__).parent / "sql"
SILVER_TABLES = ["customers", "products", "orders", "order_items"]
DIMENSIONS = ["dim_customer", "dim_product", "dim_date"]
MARTS = ["mart_daily_sales", "mart_category_sales"]

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def run_sql(sql_file: str) -> BigQueryInsertJobOperator:
    """
    Creates a task that runs a SQL file from "dags/sql" as a BigQuery job.

    Args:
        sql_file: The file name without the ".sql" extension. It is also used as the task ID.

    Returns:
        A BigQueryInsertJobOperator that executes the SQL in the configured
        project and region.
    """
    query = (SQL_DIR / f"{sql_file}.sql").read_text()
    return BigQueryInsertJobOperator(
        task_id=sql_file,
        project_id=PROJECT_ID,
        location=REGION,
        configuration={"query": {"query": query, "useLegacySql": False}},
    )


with DAG(
    dag_id="ecommerce_medallion",
    description="Bronze to Silver (Dataproc Serverless) to Gold star schema (BigQuery)",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["medallion", "dataproc", "bigquery"],
    doc_md=__doc__,
) as dag:

    # Stage 1: clean the raw files with Spark. The batch ID must be unique,
    # so it includes the run timestamp and the try number (a retry never
    # collides with a failed batch).
    bronze_to_silver = DataprocCreateBatchOperator(
        task_id="bronze_to_silver",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="medallion-b2s-{{ ts_nodash | lower }}-{{ ti.try_number }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{BUCKET}/code/bronze_to_silver.py",
                "args": ["--bucket", BUCKET],
            },
            "runtime_config": {"version": "2.3"},
            "environment_config": {
                "execution_config": {
                    "service_account": SERVICE_ACCOUNT,
                    "subnetwork_uri": (
                        f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default"
                    ),
                }
            },
        },
    )

    # Stage 2: load the Silver Parquet files into BigQuery. WRITE_TRUNCATE
    # replaces the table contents, so re-runs are idempotent.
    load_silver = [
        GCSToBigQueryOperator(
            task_id=f"load_silver_{table}",
            bucket=BUCKET,
            source_objects=[f"silver/{table}/*.parquet"],
            destination_project_dataset_table=f"{PROJECT_ID}.silver.{table}",
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            location=REGION,
        )
        for table in SILVER_TABLES
    ]

    # Stage 3: build the star schema (dimensions, then fact, then marts).
    dimension_tasks = [run_sql(name) for name in DIMENSIONS]
    fact_sales = run_sql("fact_sales")
    mart_tasks = [run_sql(name) for name in MARTS]

    # Stage 4: quality gate.
    check_gold = run_sql("check_gold")

    # Task dependencies.
    bronze_to_silver >> load_silver
    for load_task in load_silver:
        load_task >> dimension_tasks
    for dimension_task in dimension_tasks:
        dimension_task >> fact_sales
    fact_sales >> mart_tasks
    for mart_task in mart_tasks:
        mart_task >> check_gold