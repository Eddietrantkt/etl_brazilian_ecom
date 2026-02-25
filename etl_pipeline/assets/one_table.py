import pandas as pd
from dagster import asset, AssetIn, Output, Definitions, AssetOut, multi_asset

from resources.mysql_io_manager import MySQLIOManager
from resources.minio_io_manager import MinioIOManager
from resources.psql_io_manager import PostgresqlIOManager
from assets.bronze_layer import bronze_ecom
from assets.silver_layer import dim_products, fact_sales
from assets.gold_layer import sales_values_by_category

@multi_asset(
    ins={
        "bronze_olist_orders_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        )
    },
    outs={
        "olist_orders_dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "primary_key": [
                    "order_id",
                    "customer_id"],
                "columns": [
                    "order_id",
                    "customer_id",
                    "order_status",
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date"
                ],
            },

        )
    },
    compute_kind = "PostgreSQL"
)

def olist_orders_dataset(bronze_olist_orders_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        bronze_olist_orders_dataset,
        metadata={
           "schema": "public",
           "table": "olist_orders_dataset",
           "record_count": len(bronze_olist_orders_dataset),
        },
    )
    
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "admin",
    "password": "admin123",
    "database": "brazillian_ecommerce",
}
MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
    "bucket_name": "warehouse",
}
PSQL_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "admin",
    "password": "admin123",
    "database": "postgres",
}
defs = Definitions(
    assets=[
        bronze_ecom,
        dim_products,
        fact_sales,
        sales_values_by_category,
        olist_orders_dataset,
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(config=MYSQL_CONFIG),
        "minio_io_manager": MinioIOManager(config=MINIO_CONFIG),
        "psql_io_manager": PostgresqlIOManager(config=PSQL_CONFIG),
    },
)



    