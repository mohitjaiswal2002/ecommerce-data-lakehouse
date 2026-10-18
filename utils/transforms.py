"""
utils/transforms.py
-------------------
Reusable DataFrame transformations shared by the Silver jobs.
Kept free of I/O so they can be unit tested against small in-memory DataFrames.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def keep_latest_snapshot(
    df: DataFrame,
    key_col: str = "id",
    order_col: str = "source_file",
) -> DataFrame:
    """
    Keep one row per *key_col*: the one with the greatest *order_col*.

    Bronze keeps every snapshot and snapshot file names end in YYYYMMDD_HHMMSS,
    so ordering by source file path descending puts the most recent version of
    each record first. *order_col* is dropped from the result.
    """
    latest_first = Window.partitionBy(key_col).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_row_num", F.row_number().over(latest_first))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", order_col)
    )


def add_product_derived_columns(df: DataFrame, low_stock_threshold: int) -> DataFrame:
    """Add discounted_price and stock_status to a Silver products DataFrame."""
    return df.withColumn(
        "discounted_price",
        F.round(F.col("price") * (1 - F.col("discount_percentage") / 100), 2),
    ).withColumn(
        "stock_status",
        F.when(F.col("stock") == 0, "Out of Stock")
        .when(F.col("stock") < low_stock_threshold, "Low Stock")
        .otherwise("In Stock"),
    )


def add_user_derived_columns(df: DataFrame) -> DataFrame:
    """Add full_name and age_group to a Silver users DataFrame."""
    return df.withColumn(
        "full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    ).withColumn(
        "age_group",
        F.when(F.col("age") < 18, "Under 18")
        .when(F.col("age") < 30, "18-29")
        .when(F.col("age") < 45, "30-44")
        .when(F.col("age") < 60, "45-59")
        .otherwise("60+"),
    )
