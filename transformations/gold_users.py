# ============================================================================
# IMPORTS SECTION
# ============================================================================

import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# These imports bring in Spark functionality:
# - SparkSession: creates the Spark connection
# - Window: used for window functions (ranking, row numbering within groups)
# - functions as F: shortcuts for PySpark SQL functions like F.col(), F.sum(), etc.

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# This line adds the parent directory to Python's path so we can import from utils/

from utils.config import (
    SILVER_USERS_PATH,
    SILVER_CARTS_PATH,
    GOLD_USER_METRICS,
    GOLD_USER_SPEND,
    GOLD_USER_SEGMENTS,
    GOLD_TOP_USERS,
)
# Importing all the file paths we need:
# - SILVER_USERS_PATH: where cleaned users data is stored
# - SILVER_CARTS_PATH: where cleaned carts data is stored
# - GOLD_USER_*: where we'll write the transformed business tables

from utils.logger import get_logger
# Import logging utility to track pipeline execution

logger = get_logger(__name__)
# Create a logger object for this module


# ============================================================================
# MAIN TRANSFORMATION FUNCTION
# ============================================================================

def transform_users():
    logger.info("=== Gold Users Pipeline — START ===")
    # Log that the pipeline has started

    spark = SparkSession.builder.appName("UsersSilverToGold").getOrCreate()
    # Create/get a Spark session named "UsersSilverToGold"
    # If one already exists, it returns that one (getOrCreate)

    # =========================================================================
    # 1. READ SILVER DATA (Users and Carts)
    # =========================================================================
    
    logger.info(f"Reading Silver Users data from: {SILVER_USERS_PATH}")
    # Log where we're reading users from
    
    users_df = spark.read.parquet(SILVER_USERS_PATH)
    # Read the cleaned users data from parquet format into a DataFrame
    
    logger.info(f"Silver users row count: {users_df.count()}")
    # Count and log how many user records we have
    
    logger.info(f"Reading Silver Carts data from: {SILVER_CARTS_PATH}")
    carts_summary_path = os.path.join(SILVER_CARTS_PATH, "cart_summary")
    # Build the full path to cart_summary (inside SILVER_CARTS_PATH)
    
    carts_df = spark.read.parquet(carts_summary_path)
    # Read the cart summary data (has user_id and spending info)
    
    logger.info(f"Silver carts row count: {carts_df.count()}")
    # Count and log how many cart records we have

    # =========================================================================
    # 2. USER-CART AGGREGATION (Calculate spending per user)
    # =========================================================================
    
    logger.info("Building: user spend metrics")
    # Log what we're about to calculate
    
    user_spend_df = (
        carts_df.groupBy("user_id")  # Group all carts by the user who made them
        .agg(
            F.count("cart_id").alias("total_carts"),
            # Count how many carts this user has = total_carts column
            
            F.sum("discounted_total").alias("total_spent"),
            # Sum all the discounted totals = total money spent
            
            F.round(F.avg("discounted_total"), 2).alias("avg_cart_value"),
            # Calculate average cart value (total_spent / number of carts)
            # F.round(..., 2) rounds to 2 decimal places
            
            F.sum("total_products").alias("total_products_purchased"),
            # Sum total products across all carts
            
            F.max("discounted_total").alias("max_cart_value"),
            # Find the highest single cart value
            
            F.min("discounted_total").alias("min_cart_value"),
            # Find the lowest single cart value
        )
    )
    # This creates a DataFrame with one row per user showing their spending behavior
    
    logger.info("Sample user spend data:")
    user_spend_df.show(5, truncate=False)
    # Show first 5 rows to verify the data looks correct

    # =========================================================================
    # 3. JOIN USER DATA WITH SPENDING DATA
    # =========================================================================
    
    logger.info("Building: enriched user metrics")
    
    user_enriched_df = (
        users_df.join(
            user_spend_df,
            on="user_id",  # Join on user_id column (must exist in both)
            how="left"     # LEFT join: keep all users even if they have no carts
        )
    )
    # Combine user demographic info with their spending metrics
    
    # Fill NULL values for users with no carts (spending data will be NULL)
    user_enriched_df = user_enriched_df.fillna(
        {
            "total_carts": 0,
            # If user has no carts, set to 0
            
            "total_spent": 0.0,
            "avg_cart_value": 0.0,
            "total_products_purchased": 0,
            "max_cart_value": 0.0,
            "min_cart_value": 0.0,
            # All spending metrics default to 0 if user never shopped
        }
    )

    # =========================================================================
    # 4. USER VALUE SEGMENTATION (Classify users as High/Mid/Low value)
    # =========================================================================
    
    logger.info("Building: user value segments")
    
    # Window function: calculate what percentile each user's spending is at
    window_percentile = Window.orderBy(F.col("total_spent").desc())
    # Order users from highest spender to lowest
    
    user_enriched_df = user_enriched_df.withColumn(
        "percent_rank",
        # Calculate what percentage of users spend less than this user
        F.percent_rank().over(window_percentile)
    )
    
    user_enriched_df = user_enriched_df.withColumn(
        "value_segment",
        # Classify users into spending tiers
        F.when(F.col("percent_rank") <= 0.33, "High Value")
        # If in top 33%, they're High Value
        
        .when(F.col("percent_rank") <= 0.67, "Mid Value")
        # If in next 34% (33%-67%), they're Mid Value
        
        .otherwise("Low Value")
        # Remaining users are Low Value
    )
    
    user_enriched_df = user_enriched_df.withColumn(
        "ingestion_date",
        F.current_date()  # Add today's date to track when this was calculated
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
    # Show sample data to verify transformations

    # =========================================================================
    # 5. USER METRICS SUMMARY (Aggregate stats by demographics)
    # =========================================================================
    
    logger.info("Building: demographic user metrics")
    
    demographic_metrics_df = (
        user_enriched_df.groupBy("age_group", "gender", "country")
        # Group by demographics to see patterns
        
        .agg(
            F.count("user_id").alias("total_users"),
            # How many users in this demographic group
            
            F.countDistinct("user_id").alias("unique_users"),
            # Distinct count (in case of duplicates, though shouldn't be)
            
            F.round(F.avg("total_spent"), 2).alias("avg_spending"),
            # Average spending in this demographic
            
            F.round(F.sum("total_spent"), 2).alias("total_demographic_spending"),
            # Total revenue from this demographic group
            
            F.round(F.avg("age"), 0).alias("avg_age"),
            # Average age in this group
            
            F.round(F.avg("total_carts"), 2).alias("avg_carts_per_user"),
            # On average, how many carts do users in this group have
        )
        .withColumn("ingestion_date", F.current_date())
        # Add date tracking
    )
    
    logger.info("Sample demographic metrics:")
    demographic_metrics_df.show(5, truncate=False)

    # =========================================================================
    # 6. TOP USERS BY SPENDING
    # =========================================================================
    
    logger.info("Building: top users ranking")
    
    top_users_df = (
        user_enriched_df.select(
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
        .orderBy(F.col("total_spent").desc())
        # Sort from highest spender to lowest
        
        .limit(10)
        # Keep only top 10 users
    )
    
    top_users_df = top_users_df.withColumn(
        "rank",
        F.row_number().over(Window.orderBy(F.col("total_spent").desc()))
        # Assign a rank number (1 for highest spender, 2 for second, etc.)
    )
    
    top_users_df = top_users_df.withColumn(
        "ingestion_date",
        F.current_date()
    )
    
    logger.info("Top 10 users by spending:")
    top_users_df.show(10, truncate=False)

    # =========================================================================
    # 7. USER SEGMENTS (Advanced segmentation based on behavior)
    # =========================================================================
    
    logger.info("Building: behavioral user segments")
    
    user_segments_df = (
        user_enriched_df.withColumn(
            "purchase_frequency",
            # Classify users by how often they shop
            F.when(F.col("total_carts") == 0, "Never Shopped")
            .when(F.col("total_carts") == 1, "One-time Buyer")
            .when(F.col("total_carts") <= 3, "Occasional Shopper")
            .otherwise("Regular Shopper")
            # More than 3 carts = regular
        )
        .withColumn(
            "segment_name",
            # Create a combined segment by combining value + frequency
            F.concat_ws(
                " - ",
                F.col("value_segment"),
                F.col("purchase_frequency")
            )
            # Creates names like "High Value - Regular Shopper"
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

    # =========================================================================
    # 8. WRITE GOLD TABLES (Save all outputs)
    # =========================================================================
    
    logger.info(f"Writing user_enriched to: {os.path.join(GOLD_USER_METRICS, 'user_enriched')}")
    user_enriched_df.write.mode("overwrite").parquet(
        os.path.join(GOLD_USER_METRICS, "user_enriched")
    )
    # Save the enriched user data (combines users + spending)
    # mode="overwrite" replaces any existing file
    
    logger.info(f"Writing demographic_metrics to: {os.path.join(GOLD_USER_METRICS, 'demographic_metrics')}")
    demographic_metrics_df.write.mode("overwrite").parquet(
        os.path.join(GOLD_USER_METRICS, "demographic_metrics")
    )
    # Save demographic aggregations
    
    logger.info(f"Writing user_spend to: {GOLD_USER_SPEND}")
    user_spend_df.write.mode("overwrite").parquet(GOLD_USER_SPEND)
    # Save just the spending metrics
    
    logger.info(f"Writing top_users to: {GOLD_TOP_USERS}")
    top_users_df.write.mode("overwrite").parquet(GOLD_TOP_USERS)
    # Save top 10 users
    
    logger.info(f"Writing user_segments to: {GOLD_USER_SEGMENTS}")
    user_segments_df.write.mode("overwrite").parquet(GOLD_USER_SEGMENTS)
    # Save behavioral segments

    logger.info("=== Gold Users Pipeline — COMPLETE ===")
    # Log that we're done
    
    spark.stop()
    # Close the Spark session to free up resources


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    transform_users()
    # Run this function if the file is executed directly
    # (not imported as a module into another file)