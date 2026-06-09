import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.config import SILVER_PRODUCTS_PATH, GOLD_PRODUCT_METRICS
from utils.logger import get_logger


logger = get_logger(__name__)


def transform_products():
    logger.info("=== Gold Carts Pipeline — START ===")

    spark = SparkSession.builder.appName("ProductsSilverToGold").getOrCreate()

    logger.info(f"Reading Silver Parquet data from: {SILVER_PRODUCTS_PATH}")
    df = spark.read.parquet(SILVER_PRODUCTS_PATH)

    logger.info(f"Silver row count: {df.count()}")

    # ---------------------------------------------------------------------------
    # 1. Price Tier Segmentation
    # ---------------------------------------------------------------------------
    logger.info("Building: price tier segmentation")

    df = df.withColumn(
        "price_tier",
        F.when(F.col("discounted_price") < 20, "Budget")
        .when(F.col("discounted_price") < 100, "Mid-Range")
        .when(F.col("discounted_price") < 500, "Premium")
        .otherwise("Luxury"),
    )

    # ---------------------------------------------------------------------------
    # 2. Category Summary
    # ---------------------------------------------------------------------------
    logger.info("Building: category summary")

    brand_counts = df.groupBy("category", "brand").agg(
        F.count("product_id").alias("brand_product_count")
    )
    window_top_brand = Window.partitionBy("category").orderBy(
        F.col("brand_product_count").desc()
    )
    top_brand_per_category = (
        brand_counts.withColumn("rank", F.rank().over(window_top_brand))
        .filter(F.col("rank") == 1)
        .select(F.col("category"), F.col("brand").alias("top_brand"))
    )

    category_summary_df = (
        df.groupBy("category")
        .agg(
            F.count("product_id").alias("total_products"),
            F.round(F.avg("price"), 2).alias("avg_original_price"),
            F.round(F.avg("discounted_price"), 2).alias("avg_discounted_price"),
            F.round(F.avg("discount_percentage"), 2).alias("avg_discount_percentage"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.sum("stock").alias("total_stock"),
            F.round(F.sum(F.col("discounted_price") * F.col("stock")), 2).alias(
                "total_inventory_value"
            ),
            F.count(F.when(F.col("stock_status") == "Out of Stock", 1)).alias(
                "out_of_stock_count"
            ),
            F.count(F.when(F.col("stock_status") == "Low Stock", 1)).alias(
                "low_stock_count"
            ),
        )
        .join(top_brand_per_category, on="category", how="left")
        .withColumn("ingestion_date", F.current_date())
    )

    logger.info("Sample category summary:")
    category_summary_df.show(5, truncate=False)

    # ---------------------------------------------------------------------------
    # 3. Brand Performance
    # ---------------------------------------------------------------------------
    logger.info("Building: brand performance")

    brand_performance_df = (
        df.groupBy("brand")
        .agg(
            F.count("product_id").alias("total_products"),
            F.countDistinct("category").alias("categories_present"),
            F.round(F.avg("price"), 2).alias("avg_original_price"),
            F.round(F.avg("discounted_price"), 2).alias("avg_discounted_price"),
            F.round(F.avg("discount_percentage"), 2).alias("avg_discount_percentage"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.round(F.sum(F.col("discounted_price") * F.col("stock")), 2).alias(
                "revenue_potential"
            ),
            F.sum("stock").alias("total_stock"),
        )
        .withColumn(
            "brand_tier",
            F.when(F.col("avg_discounted_price") < 20, "Budget")
            .when(F.col("avg_discounted_price") < 100, "Mid-Range")
            .when(F.col("avg_discounted_price") < 500, "Premium")
            .otherwise("Luxury"),
        )
        .withColumn("ingestion_date", F.current_date())
        .orderBy(F.col("revenue_potential").desc())
    )

    logger.info("Sample brand performance:")
    brand_performance_df.show(5, truncate=False)

    # ---------------------------------------------------------------------------
    # 4. Product Enriched (full fact table with all derived fields)
    # ---------------------------------------------------------------------------
    logger.info("Building: enriched product fact table")

    window_category_rank = Window.partitionBy("category").orderBy(
        F.col("rating").desc()
    )

    product_enriched_df = (
        df.withColumn(
            "rating_rank_in_category", F.rank().over(window_category_rank)
        )
        .withColumn(
            "is_top_rated_in_category",
            F.col("rating_rank_in_category") == 1,
        )
        .withColumn(
            "inventory_value",
            F.round(F.col("discounted_price") * F.col("stock"), 2),
        )
        .withColumn(
            "savings_per_unit",
            F.round(F.col("price") - F.col("discounted_price"), 2),
        )
        .withColumn("ingestion_date", F.current_date())
    )

    logger.info("Sample enriched products:")
    product_enriched_df.select(
        "product_id",
        "title",
        "price_tier",
        "discounted_price",
        "savings_per_unit",
        "stock_status",
        "rating_rank_in_category",
        "is_top_rated_in_category",
    ).show(5, truncate=False)

    logger.info(f"Final Gold enriched product count: {product_enriched_df.count()}")

    # ---------------------------------------------------------------------------
    # Write Gold Tables
    # ---------------------------------------------------------------------------
    category_summary_path  = os.path.join(GOLD_PRODUCT_METRICS, "category_summary")
    brand_performance_path = os.path.join(GOLD_PRODUCT_METRICS, "brand_performance")
    product_enriched_path  = os.path.join(GOLD_PRODUCT_METRICS, "product_enriched")

    logger.info(f"Writing category_summary to: {category_summary_path}")
    category_summary_df.write.mode("overwrite").parquet(category_summary_path)

    logger.info(f"Writing brand_performance to: {brand_performance_path}")
    brand_performance_df.write.mode("overwrite").parquet(brand_performance_path)

    logger.info(f"Writing product_enriched to: {product_enriched_path}")
    (
        product_enriched_df.write.mode("overwrite")
        .partitionBy("category")
        .parquet(product_enriched_path)
    )

    logger.info("=== Gold Products Pipeline — COMPLETE ===")
    spark.stop()


if __name__ == "__main__":
    transform_products()