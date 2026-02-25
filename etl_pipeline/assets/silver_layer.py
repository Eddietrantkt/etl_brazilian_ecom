import pandas as pd
from dagster import asset, AssetIn, Output


@asset(
    ins={
        "bronze_olist_products_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
        "bronze_product_category_name_translation": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    compute_kind="Pandas",
)
def dim_products(
    bronze_olist_products_dataset: pd.DataFrame,
    bronze_product_category_name_translation: pd.DataFrame,
) -> Output[pd.DataFrame]:
    """
    dim_products = olist_products_dataset JOIN product_category_name_translation
    Kết quả: product_id, product_category_name_english
    """
    df = bronze_olist_products_dataset.merge(
        bronze_product_category_name_translation,
        on="product_category_name",
        how="inner",
    )[["product_id", "product_category_name_english"]]

    return Output(
        df,
        metadata={
            "table": "dim_products",
            "record_count": len(df),
            "columns": list(df.columns),
        },
    )


@asset(
    ins={
        "bronze_olist_orders_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
        "bronze_olist_order_items_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
        "bronze_olist_payments_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    compute_kind="Pandas",
)
def fact_sales(
    bronze_olist_orders_dataset: pd.DataFrame,
    bronze_olist_order_items_dataset: pd.DataFrame,
    bronze_olist_payments_dataset: pd.DataFrame,
) -> Output[pd.DataFrame]:
    """
    fact_sales = olist_orders JOIN olist_order_items JOIN olist_order_payments
    Kết quả: order_id, customer_id, order_purchase_timestamp,
             product_id, payment_value, order_status
    """
    df = (
        bronze_olist_orders_dataset
        .merge(bronze_olist_order_items_dataset, on="order_id", how="inner")
        .merge(bronze_olist_payments_dataset, on="order_id", how="inner")
    )[
        [
            "order_id",
            "customer_id",
            "order_purchase_timestamp",
            "product_id",
            "payment_value",
            "order_status",
        ]
    ]

    return Output(
        df,
        metadata={
            "table": "fact_sales",
            "record_count": len(df),
            "columns": list(df.columns),
        },
    )
