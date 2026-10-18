"""
transformations/gold_carts.py
------------------------------
Reads Silver Parquet data for products, users, and carts.
Computes Gold-layer KPI tables and writes them as Parquet under data/gold/.

Gold tables produced:
    1. gold/product_metrics    – per-product sales and stock KPIs
    2. gold/category_metrics   – revenue and volume rolled up by category
    3. gold/user_metrics       – per-user spend and cart activity
    4. gold/low_stock_alerts   – products with stock < LOW_STOCK_THRESHOLD
    5. gold/top_products       – top N products by cart frequency
"""

import os
import sys
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import get_logger
from utils.config import (
    SILVER_PRODUCTS_PATH,
    SILVER_USERS_PATH,
    SILVER_CARTS_PATH,
    GOLD_PRODUCT_METRICS,
    GOLD_CATEGORY_METRICS,
    GOLD_USER_METRICS,
    GOLD_LOW_STOCK_ALERTS,
    GOLD_TOP_PRODUCTS,
    LOW_STOCK_THRESHOLD,
    TOP_N_PRODUCTS,
)

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("GoldCarts")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead",  "CORRECTED")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def write_gold(df, path: str, label: str) -> None:
    """Overwrite a Gold Parquet table and log row count."""
    os.makedirs(path, exist_ok=True)
    count = df.count()
    df.write.mode("overwrite").parquet(path)
    logger.info(f"{label}: {count} rows written to {path}")


# ---------------------------------------------------------------------------
# KPI builders
# ---------------------------------------------------------------------------
def build_product_metrics(spark: SparkSession):
    """
    Per-product KPIs joining silver products with silver cart items.
        - total_quantity_in_carts
        - cart_appearances
        - revenue_estimate  (quantity * price)
        - is_low_stock      (stock < LOW_STOCK_THRESHOLD)
    """
    logger.info("Building product_metrics …")

    products = spark.read.parquet(SILVER_PRODUCTS_PATH)

    # Reading the parent SILVER_CARTS_PATH directly has no Parquet files — must use sub-path.
    cart_items = spark.read.parquet(os.path.join(SILVER_CARTS_PATH, "cart_items"))

    cart_agg = (
        cart_items
        .groupBy("product_id")
        .agg(
            F.sum("quantity").cast("long").alias("total_quantity_in_carts"),
            F.countDistinct("cart_id").alias("cart_appearances"),
        )
    )

    df = (
        products
        .join(cart_agg, products["product_id"] == cart_agg["product_id"], "left")
        .select(
            products["product_id"],
            products["title"],
            products["category"],
            products["price"],
            products["rating"],
            products["stock"],
            F.coalesce(cart_agg["total_quantity_in_carts"], F.lit(0)).alias("total_quantity_in_carts"),
            F.coalesce(cart_agg["cart_appearances"], F.lit(0)).alias("cart_appearances"),
            F.round(
                F.coalesce(cart_agg["total_quantity_in_carts"], F.lit(0)) * products["price"], 2
            ).alias("revenue_estimate"),
            (products["stock"] < LOW_STOCK_THRESHOLD).cast("boolean").alias("is_low_stock"),
            F.current_date().alias("gold_date"),
        )
    )

    write_gold(df, GOLD_PRODUCT_METRICS, "product_metrics")
    return df


def build_category_metrics(product_metrics_df):
    """
    Roll product_metrics up to category level:
        product_count, total_revenue_estimate, avg_price,
        avg_rating, total_stock, low_stock_product_count
    """
    logger.info("Building category_metrics …")

    df = (
        product_metrics_df
        .groupBy("category")
        .agg(
            F.count("product_id").alias("product_count"),
            F.round(F.sum("revenue_estimate"), 2).alias("total_revenue_estimate"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.sum("stock").alias("total_stock"),
            F.sum(F.col("is_low_stock").cast("int")).alias("low_stock_product_count"),
        )
        .orderBy(F.col("total_revenue_estimate").desc())
    )

    write_gold(df, GOLD_CATEGORY_METRICS, "category_metrics")
    return df


def build_user_metrics(spark: SparkSession):
    """
    Per-user spend KPIs joined from silver users + silver cart summary.
        total_carts, total_items_ordered, total_spend_estimate, avg_spend_per_cart
    """
    logger.info("Building user_metrics …")

    users = spark.read.parquet(SILVER_USERS_PATH)
    cart_summary = spark.read.parquet(os.path.join(SILVER_CARTS_PATH, "cart_summary"))

    # cart_summary has: cart_id, user_id, total_quantity, discounted_total
    user_agg = (
        cart_summary
        .groupBy("user_id")
        .agg(
            F.countDistinct("cart_id").alias("total_carts"),
            F.sum("total_quantity").cast("long").alias("total_items_ordered"),
            F.round(F.sum("discounted_total"), 2).alias("total_spend_estimate"),
        )
        .withColumn(
            "avg_spend_per_cart",
            F.round(F.col("total_spend_estimate") / F.col("total_carts"), 2),
        )
    )

    df = (
        users
        .join(user_agg, users["user_id"] == user_agg["user_id"], "left")
        .select(
            users["user_id"],
            users["first_name"],
            users["last_name"],
            users["email"],
            F.coalesce(user_agg["total_carts"],          F.lit(0)).alias("total_carts"),
            F.coalesce(user_agg["total_items_ordered"],  F.lit(0)).alias("total_items_ordered"),
            F.coalesce(user_agg["total_spend_estimate"], F.lit(0.0)).alias("total_spend_estimate"),
            F.coalesce(user_agg["avg_spend_per_cart"],   F.lit(0.0)).alias("avg_spend_per_cart"),
            F.current_date().alias("gold_date"),
        )
        .orderBy(F.col("total_spend_estimate").desc())
    )

    write_gold(df, GOLD_USER_METRICS, "user_metrics")
    return df


def build_low_stock_alerts(product_metrics_df):
    """Products with stock below LOW_STOCK_THRESHOLD, sorted by urgency (stock asc)."""
    logger.info("Building low_stock_alerts …")

    df = (
        product_metrics_df
        .filter(F.col("is_low_stock"))
        .select("product_id", "title", "category", "stock", "price", "gold_date")
        .orderBy("stock")
    )

    write_gold(df, GOLD_LOW_STOCK_ALERTS, "low_stock_alerts")
    return df


def build_top_products(product_metrics_df, n: int = TOP_N_PRODUCTS):
    """Top N products ranked by cart_appearances (most carted)."""
    logger.info(f"Building top_{n}_products …")

    # product_id breaks ties so the ranking is deterministic between runs.
    rank_window = Window.orderBy(F.col("cart_appearances").desc(), F.col("product_id"))

    df = (
        product_metrics_df
        .withColumn("rank", F.row_number().over(rank_window))
        .filter(F.col("rank") <= n)
        .select(
            "rank", "product_id", "title", "category",
            "cart_appearances", "revenue_estimate", "price", "rating",
        )
    )

    write_gold(df, GOLD_TOP_PRODUCTS, f"top_{n}_products")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 60)
    logger.info("Gold Carts Pipeline — START")
    logger.info("=" * 60)

    spark = get_spark()

    try:
        product_metrics_df = build_product_metrics(spark)
        build_category_metrics(product_metrics_df)
        build_user_metrics(spark)
        build_low_stock_alerts(product_metrics_df)
        build_top_products(product_metrics_df)

        logger.info("=" * 60)
        logger.info("Gold Carts Pipeline — COMPLETE")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Gold Carts pipeline failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()