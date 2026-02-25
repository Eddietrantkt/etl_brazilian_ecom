import pandas as pd
from dagster import asset, Output, multi_asset, AssetOut


BRONZE_TABLES = [
    "olist_order_items_dataset",
    "olist_payments_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "product_category_name_translation",
]


@multi_asset(
    outs={
        f"bronze_{table}": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["bronze", "ecom"],
            metadata={"table": table},
        )
        for table in BRONZE_TABLES
    },
    required_resource_keys={"mysql_io_manager"},
    compute_kind="MySQL",
)
def bronze_ecom(context):
    """Extract tất cả bảng từ MySQL và lưu vào MinIO (bronze/ecom/<table_name>)."""
    for table in BRONZE_TABLES:
        sql_stm = f"SELECT * FROM {table}"
        pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
        context.log.info(f"Extracted {table}: {len(pd_data)} records")
        yield Output(
            pd_data,
            output_name=f"bronze_{table}",
            metadata={
                "table": table,
                "record_count": len(pd_data),
            },
        )
