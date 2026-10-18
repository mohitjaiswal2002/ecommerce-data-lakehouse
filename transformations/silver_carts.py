import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.config import BRONZE_CARTS_PATH, SILVER_CARTS_PATH
from utils.logger import get_logger
from utils.transforms import keep_latest_snapshot


logger = get_logger(__name__)


def transform_carts():
    logger.info("=== Silver Carts Pipeline - START ===")

    spark = SparkSession.builder.appName("CartsBronzeToSilver").getOrCreate()

    # ---------------------------------------------------------------------------
    # 1. Read Bronze JSON Data
    # ---------------------------------------------------------------------------
    logger.info(f"Reading Bronze JSON data from: {BRONZE_CARTS_PATH}")
    raw_df = spark.read.options(multiline="true").json(
        os.path.join(BRONZE_CARTS_PATH, "*.json")
    )

    # ---------------------------------------------------------------------------
    # 2. Flatten Cart Header Records
    # ---------------------------------------------------------------------------
    carts_df = raw_df.select(
        F.col("_metadata.file_path").alias("source_file"),
        F.explode("carts").alias("cart"),
    ).select(
        F.col("source_file"),
        F.col("cart.id").alias("id"),
        F.col("cart.userId").alias("userId"),
        F.col("cart.totalProducts").alias("totalProducts"),
        F.col("cart.totalQuantity").alias("totalQuantity"),
        F.col("cart.total").alias("total"),
        F.col("cart.discountedTotal").alias("discountedTotal"),
        F.col("cart.products").alias("products"),
    )

    logger.info(f"Bronze carts row count: {carts_df.count()}")

    # ---------------------------------------------------------------------------
    # 3. Deduplicate Carts
    # ---------------------------------------------------------------------------
    before = carts_df.count()
    # Bronze keeps every snapshot; keep only the most recent version of each cart.
    carts_df = keep_latest_snapshot(carts_df)
    after = carts_df.count()
    logger.info(f"Deduplication: {before - after} older/duplicate cart versions removed")

    # ---------------------------------------------------------------------------
    # 4. Build Cart Summary Table
    # ---------------------------------------------------------------------------
    cart_summary_df = carts_df.select(
        F.col("id").cast(IntegerType()).alias("cart_id"),
        F.col("userId").cast(IntegerType()).alias("user_id"),
        F.col("totalProducts").cast(IntegerType()).alias("total_products"),
        F.col("totalQuantity").cast(IntegerType()).alias("total_quantity"),
        F.col("total").cast(DoubleType()).alias("total_amount"),
        F.col("discountedTotal").cast(DoubleType()).alias("discounted_total"),
        F.current_date().alias("ingestion_date"),
    )

    # ---------------------------------------------------------------------------
    # 5. Build Cart Items Table
    # ---------------------------------------------------------------------------
    cart_items_df = carts_df.select(
        F.col("id").alias("cart_id"),
        F.col("userId").alias("user_id"),
        F.explode("products").alias("product"),
    ).select(
        F.col("cart_id").cast(IntegerType()).alias("cart_id"),
        F.col("user_id").cast(IntegerType()).alias("user_id"),
        F.col("product.id").cast(IntegerType()).alias("product_id"),
        F.col("product.title").cast(StringType()).alias("product_title"),
        F.col("product.price").cast(DoubleType()).alias("unit_price"),
        F.col("product.quantity").cast(IntegerType()).alias("quantity"),
        F.col("product.total").cast(DoubleType()).alias("line_total"),
        F.col("product.discountPercentage").cast(DoubleType()).alias("discount_percentage"),
        F.col("product.discountedTotal").cast(DoubleType()).alias("discounted_line_total"),
        F.current_date().alias("ingestion_date"),
    )

    # ---------------------------------------------------------------------------
    # 6. Handle Null Values
    # ---------------------------------------------------------------------------
    cart_summary_df = cart_summary_df.fillna(
        {
            "total_products": 0,
            "total_quantity": 0,
            "total_amount": 0.0,
            "discounted_total": 0.0,
        }
    )

    cart_items_df = cart_items_df.fillna(
        {
            "product_title": "Unknown",
            "unit_price": 0.0,
            "quantity": 0,
            "line_total": 0.0,
            "discount_percentage": 0.0,
            "discounted_line_total": 0.0,
        }
    )

    # ---------------------------------------------------------------------------
    # 7. Apply Data Quality Rules
    # ---------------------------------------------------------------------------
    invalid_cart_totals = cart_summary_df.filter(F.col("total_amount") <= 0).count()
    invalid_quantities = cart_summary_df.filter(F.col("total_quantity") < 0).count()
    missing_user_id = cart_summary_df.filter(F.col("user_id").isNull()).count()

    if invalid_cart_totals > 0:
        logger.warning(f"Found {invalid_cart_totals} carts with invalid totals - dropping them")
        cart_summary_df = cart_summary_df.filter(F.col("total_amount") > 0)

    if invalid_quantities > 0:
        logger.warning(f"Found {invalid_quantities} carts with invalid quantity - dropping them")
        cart_summary_df = cart_summary_df.filter(F.col("total_quantity") >= 0)

    if missing_user_id > 0:
        logger.warning(f"Found {missing_user_id} carts without user_id - dropping them")
        cart_summary_df = cart_summary_df.filter(F.col("user_id").isNotNull())

    cart_items_df = cart_items_df.filter(
        (F.col("cart_id").isNotNull())
        & (F.col("product_id").isNotNull())
        & (F.col("quantity") >= 0)
        & (F.col("line_total") >= 0)
    )

    # ---------------------------------------------------------------------------
    # 8. Add Derived Cart Columns
    # ---------------------------------------------------------------------------
    cart_summary_df = cart_summary_df.withColumn(
        "discount_amount",
        F.round(F.col("total_amount") - F.col("discounted_total"), 2),
    )

    cart_summary_df = cart_summary_df.withColumn(
        "avg_item_value",
        F.when(
            F.col("total_quantity") > 0,
            F.round(F.col("discounted_total") / F.col("total_quantity"), 2),
        ).otherwise(0.0),
    )

    cart_items_df = cart_items_df.withColumn(
        "line_discount_amount",
        F.round(F.col("line_total") - F.col("discounted_line_total"), 2),
    )

    # ---------------------------------------------------------------------------
    # 9. Preview Silver Output
    # ---------------------------------------------------------------------------
    logger.info("Sample Silver cart summary data:")
    cart_summary_df.select(
        "cart_id",
        "user_id",
        "total_products",
        "total_quantity",
        "total_amount",
        "discounted_total",
    ).show(5, truncate=False)

    logger.info("Sample Silver cart items data:")
    cart_items_df.select(
        "cart_id",
        "product_id",
        "product_title",
        "quantity",
        "line_total",
        "discounted_line_total",
    ).show(5, truncate=False)

    logger.info(f"Final Silver cart summary row count: {cart_summary_df.count()}")
    logger.info(f"Final Silver cart items row count: {cart_items_df.count()}")

    # ---------------------------------------------------------------------------
    # 10. Write Silver Carts
    # ---------------------------------------------------------------------------
    cart_summary_path = os.path.join(SILVER_CARTS_PATH, "cart_summary")
    cart_items_path = os.path.join(SILVER_CARTS_PATH, "cart_items")

    logger.info(f"Writing Silver cart summary to: {cart_summary_path}")
    cart_summary_df.write.mode("overwrite").parquet(cart_summary_path)

    logger.info(f"Writing Silver cart items to: {cart_items_path}")
    cart_items_df.write.mode("overwrite").parquet(cart_items_path)

    logger.info("=== Carts Silver Pipeline - COMPLETE ===")
    spark.stop()


if __name__ == "__main__":
    transform_carts()