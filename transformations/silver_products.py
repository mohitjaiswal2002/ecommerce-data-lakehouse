import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.config import BRONZE_PRODUCTS_PATH, SILVER_PRODUCTS_PATH
from utils.logger import get_logger


logger = get_logger(__name__)
logger.info("=== Silver Products Pipeline - START ===")

def transform_products():

    spark = SparkSession.builder.appName("ProductsBronzeToSilver").getOrCreate()

    # ---------------------------------------------------------------------------
    # 1. Read Bronze JSON Data
    # ---------------------------------------------------------------------------
    logger.info(f"Reading Bronze JSON data from: {BRONZE_PRODUCTS_PATH}")
    raw_df = spark.read.options(multiline="true").json(
        os.path.join(BRONZE_PRODUCTS_PATH, "*.json")
    )

    # ---------------------------------------------------------------------------
    # 2. Flatten Product Records
    # ---------------------------------------------------------------------------
    df = raw_df.select(F.explode("products").alias("product")).select(
        F.col("product.id").alias("id"),
        F.col("product.title").alias("title"),
        F.col("product.description").alias("description"),
        F.col("product.price").alias("price"),
        F.col("product.discountPercentage").alias("discountPercentage"),
        F.col("product.rating").alias("rating"),
        F.col("product.stock").alias("stock"),
        F.col("product.brand").alias("brand"),
        F.col("product.category").alias("category"),
    )

    logger.info(f"Bronze row count: {df.count()}")

    # ---------------------------------------------------------------------------
    # 3. Deduplicate Products
    # ---------------------------------------------------------------------------
    before = df.count()
    df = df.dropDuplicates(["id"])
    after = df.count()
    logger.info(f"Deduplication: {before - after} duplicate rows removed")

    # ---------------------------------------------------------------------------
    # 4. Enforce Silver Schema
    # ---------------------------------------------------------------------------
    df = df.select(
        F.col("id").cast(IntegerType()).alias("product_id"),
        F.col("title").cast(StringType()).alias("title"),
        F.col("description").cast(StringType()).alias("description"),
        F.col("price").cast(DoubleType()).alias("price"),
        F.col("discountPercentage").cast(DoubleType()).alias("discount_percentage"),
        F.col("rating").cast(DoubleType()).alias("rating"),
        F.col("stock").cast(IntegerType()).alias("stock"),
        F.col("brand").cast(StringType()).alias("brand"),
        F.col("category").cast(StringType()).alias("category"),
        F.current_date().alias("ingestion_date"),
    )

    # ---------------------------------------------------------------------------
    # 5. Handle Null Values
    # ---------------------------------------------------------------------------
    df = df.fillna(
        {
            "price": 0.0,
            "discount_percentage": 0.0,
            "rating": 0.0,
            "stock": 0,
            "brand": "Unknown",
            "category": "Uncategorized",
            "description": "",
        }
    )

    # ---------------------------------------------------------------------------
    # 6. Apply Data Quality Rules
    # ---------------------------------------------------------------------------
    invalid_price = df.filter(F.col("price") < 0).count()
    invalid_stock = df.filter(F.col("stock") < 0).count()
    invalid_rating = df.filter((F.col("rating") < 0) | (F.col("rating") > 5)).count()

    if invalid_price > 0:
        logger.warning(f"Found {invalid_price} rows with negative price - dropping them")
        df = df.filter(F.col("price") >= 0)

    if invalid_stock > 0:
        logger.warning(f"Found {invalid_stock} rows with negative stock - dropping them")
        df = df.filter(F.col("stock") >= 0)

    if invalid_rating > 0:
        logger.warning(f"Found {invalid_rating} rows with invalid rating - dropping them")
        df = df.filter((F.col("rating") >= 0) & (F.col("rating") <= 5))

    # ---------------------------------------------------------------------------
    # 7. Add Derived Product Columns
    # ---------------------------------------------------------------------------
    df = df.withColumn(
        "discounted_price",
        F.round(F.col("price") * (1 - F.col("discount_percentage") / 100), 2),
    )

    df = df.withColumn(
        "stock_status",
        F.when(F.col("stock") == 0, "Out of Stock")
        .when(F.col("stock") < 10, "Low Stock")
        .otherwise("In Stock"),
    )

    # ---------------------------------------------------------------------------
    # 8. Preview Silver Output
    # ---------------------------------------------------------------------------
    logger.info("Sample Silver products data:")
    df.select(
        "product_id",
        "title",
        "price",
        "discounted_price",
        "stock_status",
        "category",
    ).show(5, truncate=False)
    logger.info(f"Final Silver row count: {df.count()}")

    # ---------------------------------------------------------------------------
    # 9. Write Silver Products
    # ---------------------------------------------------------------------------
    logger.info(f"Writing Silver Parquet data to: {SILVER_PRODUCTS_PATH}")
    (
        df.write.mode("overwrite")
        .partitionBy("category")
        .parquet(SILVER_PRODUCTS_PATH)
    )

    logger.info("=== Silver Products Pipeline - COMPLETE ===")
    spark.stop()


if __name__ == "__main__":
    transform_products()
