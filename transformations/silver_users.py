import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.config import BRONZE_USERS_PATH, SILVER_USERS_PATH
from utils.logger import get_logger
from utils.transforms import add_user_derived_columns, keep_latest_snapshot


logger = get_logger(__name__)


def transform_users():
    logger.info("=== Silver Users Pipeline - START ===")

    spark = SparkSession.builder.appName("UsersBronzeToSilver").getOrCreate()

    # ---------------------------------------------------------------------------
    # 1. Read Bronze JSON Data
    # ---------------------------------------------------------------------------
    logger.info(f"Reading Bronze JSON data from: {BRONZE_USERS_PATH}")
    raw_df = spark.read.options(multiline="true").json(
        os.path.join(BRONZE_USERS_PATH, "*.json")
    )

    # ---------------------------------------------------------------------------
    # 2. Flatten User Records
    # ---------------------------------------------------------------------------
    df = raw_df.select(
        F.col("_metadata.file_path").alias("source_file"),
        F.explode("users").alias("user"),
    ).select(
        F.col("source_file"),
        F.col("user.id").alias("id"),
        F.col("user.firstName").alias("firstName"),
        F.col("user.lastName").alias("lastName"),
        F.col("user.gender").alias("gender"),
        F.col("user.age").alias("age"),
        F.col("user.email").alias("email"),
        F.col("user.phone").alias("phone"),
        F.col("user.username").alias("username"),
        F.col("user.address.city").alias("city"),
        F.col("user.address.state").alias("state"),
        F.col("user.address.stateCode").alias("stateCode"),
        F.col("user.address.country").alias("country"),
        F.col("user.company.department").alias("companyDepartment"),
        F.col("user.company.name").alias("companyName"),
        F.col("user.company.title").alias("companyTitle"),
        F.col("user.role").alias("role"),
    )

    logger.info(f"Bronze users row count: {df.count()}")

    # ---------------------------------------------------------------------------
    # 3. Deduplicate Users
    # ---------------------------------------------------------------------------
    before = df.count()
    # Bronze keeps every snapshot; keep only the most recent version of each user.
    df = keep_latest_snapshot(df)
    after = df.count()
    logger.info(f"Deduplication: {before - after} older/duplicate user versions removed")

    # ---------------------------------------------------------------------------
    # 4. Enforce Silver Schema
    # ---------------------------------------------------------------------------
    df = df.select(
        F.col("id").cast(IntegerType()).alias("user_id"),
        F.col("firstName").cast(StringType()).alias("first_name"),
        F.col("lastName").cast(StringType()).alias("last_name"),
        F.col("gender").cast(StringType()).alias("gender"),
        F.col("age").cast(IntegerType()).alias("age"),
        F.col("email").cast(StringType()).alias("email"),
        F.col("phone").cast(StringType()).alias("phone"),
        F.col("username").cast(StringType()).alias("username"),
        F.col("city").cast(StringType()).alias("city"),
        F.col("state").cast(StringType()).alias("state"),
        F.col("stateCode").cast(StringType()).alias("state_code"),
        F.col("country").cast(StringType()).alias("country"),
        F.col("companyDepartment").cast(StringType()).alias("company_department"),
        F.col("companyName").cast(StringType()).alias("company_name"),
        F.col("companyTitle").cast(StringType()).alias("company_title"),
        F.col("role").cast(StringType()).alias("role"),
        F.current_date().alias("ingestion_date"),
    )

    # ---------------------------------------------------------------------------
    # 5. Handle Null Values
    # ---------------------------------------------------------------------------
    df = df.fillna(
        {
            "first_name": "Unknown",
            "last_name": "Unknown",
            "gender": "Unknown",
            "email": "Unknown",
            "phone": "Unknown",
            "username": "Unknown",
            "city": "Unknown",
            "state": "Unknown",
            "state_code": "Unknown",
            "country": "Unknown",
            "company_department": "Unknown",
            "company_name": "Unknown",
            "company_title": "Unknown",
            "role": "Unknown",
        }
    )

    # ---------------------------------------------------------------------------
    # 6. Apply Data Quality Rules
    # ---------------------------------------------------------------------------
    missing_user_id = df.filter(F.col("user_id").isNull()).count()
    invalid_age = df.filter((F.col("age") < 0) | (F.col("age") > 120)).count()

    if missing_user_id > 0:
        logger.warning(f"Found {missing_user_id} rows without user_id - dropping them")
        df = df.filter(F.col("user_id").isNotNull())

    if invalid_age > 0:
        logger.warning(f"Found {invalid_age} users with invalid age - dropping them")
        df = df.filter((F.col("age") >= 0) & (F.col("age") <= 120))

    # ---------------------------------------------------------------------------
    # 7. Add Derived User Columns
    # ---------------------------------------------------------------------------
    df = add_user_derived_columns(df)

    # ---------------------------------------------------------------------------
    # 8. Preview Silver Output
    # ---------------------------------------------------------------------------
    logger.info("Sample Silver users data:")
    df.select("user_id", "full_name", "gender", "age", "age_group", "city", "state").show(
        5, truncate=False
    )
    logger.info(f"Final Silver users row count: {df.count()}")

    # ---------------------------------------------------------------------------
    # 9. Write Silver Users
    # ---------------------------------------------------------------------------
    logger.info(f"Writing Silver users Parquet data to: {SILVER_USERS_PATH}")
    df.write.mode("overwrite").parquet(SILVER_USERS_PATH)

    logger.info("=== Silver Users Pipeline - COMPLETE ===")
    spark.stop()


if __name__ == "__main__":
    transform_users()