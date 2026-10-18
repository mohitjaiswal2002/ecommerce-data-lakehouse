"""
transformations/gold_users.py
-----------------------------
Reads Silver users and cart summaries, and builds customer-focused Gold tables.

Gold tables produced:
    1. gold/user_enriched        – user profile + spend metrics + value segment
    2. gold/demographic_metrics  – spend rolled up by age group, gender, country
    3. gold/user_spend_metrics   – per-user spend KPIs
    4. gold/top_users            – top N users by total spend
    5. gold/user_segments        – value segment combined with purchase frequency
"""

import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.config import (
    SILVER_USERS_PATH,
    SILVER_CARTS_PATH,
    GOLD_USER_ENRICHED,
    GOLD_DEMOGRAPHIC_METRICS,
    GOLD_USER_SPEND,
    GOLD_USER_SEGMENTS,
    GOLD_TOP_USERS,
    TOP_N_USERS,
)
from utils.logger import get_logger


logger = get_logger(__name__)


def transform_users():
    logger.info("=== Gold Users Pipeline — START ===")

    spark = SparkSession.builder.appName("UsersSilverToGold").getOrCreate()

    # ---------------------------------------------------------------------------
    # 1. Read Silver Data
    # ---------------------------------------------------------------------------
    logger.info(f"Reading Silver users data from: {SILVER_USERS_PATH}")
    users_df = spark.read.parquet(SILVER_USERS_PATH)
    logger.info(f"Silver users row count: {users_df.count()}")

    carts_summary_path = os.path.join(SILVER_CARTS_PATH, "cart_summary")
    logger.info(f"Reading Silver cart summary data from: {carts_summary_path}")
    carts_df = spark.read.parquet(carts_summary_path)
    logger.info(f"Silver carts row count: {carts_df.count()}")

    # ---------------------------------------------------------------------------
    # 2. User Spend Metrics
    # ---------------------------------------------------------------------------
    logger.info("Building: user spend metrics")

    user_spend_df = carts_df.groupBy("user_id").agg(
        F.count("cart_id").alias("total_carts"),
        F.round(F.sum("discounted_total"), 2).alias("total_spent"),
        F.round(F.avg("discounted_total"), 2).alias("avg_cart_value"),
        F.sum("total_products").alias("total_products_purchased"),
        F.max("discounted_total").alias("max_cart_value"),
        F.min("discounted_total").alias("min_cart_value"),
    )

    logger.info("Sample user spend data:")
    user_spend_df.show(5, truncate=False)

    # ---------------------------------------------------------------------------
    # 3. Enrich Users With Spend (left join keeps users who never shopped)
    # ---------------------------------------------------------------------------
    logger.info("Building: enriched user metrics")

    user_enriched_df = users_df.join(user_spend_df, on="user_id", how="left").fillna(
        {
            "total_carts": 0,
            "total_spent": 0.0,
            "avg_cart_value": 0.0,
            "total_products_purchased": 0,
            "max_cart_value": 0.0,
            "min_cart_value": 0.0,
        }
    )

    # ---------------------------------------------------------------------------
    # 4. Value Segmentation (top third / middle third / bottom third by spend)
    # ---------------------------------------------------------------------------
    logger.info("Building: user value segments")

    spend_window = Window.orderBy(F.col("total_spent").desc())

    user_enriched_df = (
        user_enriched_df.withColumn("percent_rank", F.percent_rank().over(spend_window))
        .withColumn(
            "value_segment",
            F.when(F.col("percent_rank") <= 0.33, "High Value")
            .when(F.col("percent_rank") <= 0.67, "Mid Value")
            .otherwise("Low Value"),
        )
        .withColumn("ingestion_date", F.current_date())
    )

    logger.info("Sample enriched users:")
    user_enriched_df.select(
        "user_id",
        "full_name",
        "age_group",
        "city",
        "total_spent",
        "total_carts",
        "value_segment",
    ).show(5, truncate=False)

    # ---------------------------------------------------------------------------
    # 5. Demographic Metrics
    # ---------------------------------------------------------------------------
    logger.info("Building: demographic user metrics")

    demographic_metrics_df = (
        user_enriched_df.groupBy("age_group", "gender", "country")
        .agg(
            F.count("user_id").alias("total_users"),
            F.round(F.avg("total_spent"), 2).alias("avg_spending"),
            F.round(F.sum("total_spent"), 2).alias("total_demographic_spending"),
            F.round(F.avg("age"), 0).alias("avg_age"),
            F.round(F.avg("total_carts"), 2).alias("avg_carts_per_user"),
        )
        .withColumn("ingestion_date", F.current_date())
    )

    logger.info("Sample demographic metrics:")
    demographic_metrics_df.show(5, truncate=False)

    # ---------------------------------------------------------------------------
    # 6. Top Users By Spend
    # ---------------------------------------------------------------------------
    logger.info(f"Building: top {TOP_N_USERS} users ranking")

    # user_id breaks ties so the ranking is deterministic between runs.
    rank_window = Window.orderBy(F.col("total_spent").desc(), F.col("user_id"))

    top_users_df = (
        user_enriched_df.withColumn("rank", F.row_number().over(rank_window))
        .filter(F.col("rank") <= TOP_N_USERS)
        .select(
            "rank",
            "user_id",
            "full_name",
            "email",
            "age_group",
            "city",
            "country",
            "total_spent",
            "total_carts",
            "avg_cart_value",
            "value_segment",
        )
        .withColumn("ingestion_date", F.current_date())
    )

    logger.info(f"Top {TOP_N_USERS} users by spending:")
    top_users_df.show(TOP_N_USERS, truncate=False)

    # ---------------------------------------------------------------------------
    # 7. Behavioral Segments (value segment + purchase frequency)
    # ---------------------------------------------------------------------------
    logger.info("Building: behavioral user segments")

    user_segments_df = (
        user_enriched_df.withColumn(
            "purchase_frequency",
            F.when(F.col("total_carts") == 0, "Never Shopped")
            .when(F.col("total_carts") == 1, "One-time Buyer")
            .when(F.col("total_carts") <= 3, "Occasional Shopper")
            .otherwise("Regular Shopper"),
        )
        .withColumn(
            "segment_name",
            F.concat_ws(" - ", F.col("value_segment"), F.col("purchase_frequency")),
        )
        .select(
            "user_id",
            "full_name",
            "email",
            "age_group",
            "country",
            "total_spent",
            "total_carts",
            "value_segment",
            "purchase_frequency",
            "segment_name",
        )
        .withColumn("ingestion_date", F.current_date())
    )

    logger.info("Sample user segments:")
    user_segments_df.show(5, truncate=False)

    # ---------------------------------------------------------------------------
    # 8. Write Gold Tables
    # ---------------------------------------------------------------------------
    outputs = [
        ("user_enriched", user_enriched_df, GOLD_USER_ENRICHED),
        ("demographic_metrics", demographic_metrics_df, GOLD_DEMOGRAPHIC_METRICS),
        ("user_spend_metrics", user_spend_df, GOLD_USER_SPEND),
        ("top_users", top_users_df, GOLD_TOP_USERS),
        ("user_segments", user_segments_df, GOLD_USER_SEGMENTS),
    ]
    for label, df, path in outputs:
        logger.info(f"Writing {label} to: {path}")
        df.write.mode("overwrite").parquet(path)

    logger.info("=== Gold Users Pipeline — COMPLETE ===")
    spark.stop()


if __name__ == "__main__":
    transform_users()
