import pandas as pd
from dagster import asset, AssetIn, Output


@asset(
    ins={
        "fact_sales": AssetIn(
            key_prefix=["silver", "ecom"],
        ),
        "dim_products": AssetIn(
            key_prefix=["silver", "ecom"],
        ),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "ecom"],
    compute_kind="Pandas",
)
def sales_values_by_category(
    fact_sales: pd.DataFrame,
    dim_products: pd.DataFrame,
) -> Output[pd.DataFrame]:
    """
    Tính tổng doanh thu và số đơn hàng theo tháng và danh mục sản phẩm.
    Chỉ lấy đơn hàng đã giao (delivered).
    """
    # 1. Lọc đơn hàng delivered
    delivered = fact_sales[fact_sales["order_status"] == "delivered"].copy()

    # 2. Chuyển order_purchase_timestamp → daily (date)
    delivered["daily"] = pd.to_datetime(delivered["order_purchase_timestamp"]).dt.date

    # 3. daily_sales_products: GROUP BY daily, product_id
    daily_sales_products = (
        delivered
        .groupby(["daily", "product_id"])
        .agg(
            sales=("payment_value", lambda x: round(x.astype(float).sum(), 2)),
            bills=("order_id", "nunique"),
        )
        .reset_index()
    )

    # 4. JOIN dim_products → daily_sales_categories
    daily_sales_categories = daily_sales_products.merge(
        dim_products, on="product_id", how="inner"
    )
    daily_sales_categories["daily"] = pd.to_datetime(daily_sales_categories["daily"])
    daily_sales_categories["monthly"] = daily_sales_categories["daily"].dt.to_period("M").astype(str)
    daily_sales_categories["category"] = daily_sales_categories["product_category_name_english"]

    # 5. GROUP BY monthly, category → final result
    result = (
        daily_sales_categories
        .groupby(["monthly", "category"])
        .agg(
            total_sales=("sales", "sum"),
            total_bills=("bills", "sum"),
        )
        .reset_index()
    )
    result["values_per_bills"] = round(result["total_sales"] / result["total_bills"], 2)

    return Output(
        result,
        metadata={
            "table": "sales_values_by_category",
            "record_count": len(result),
            "columns": list(result.columns),
        },
    )
