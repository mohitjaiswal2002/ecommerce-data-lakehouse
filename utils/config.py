import os

#======================================
# API configuration
#======================================

API_URL = "https://dummyjson.com/products"
API_LIMIT = 100

#======================================
# Project paths
#======================================
BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

DATA_PATH = os.path.join(BASE_DIR, "data")

BRONZE_PATH = os.path.join(DATA_PATH, "bronze")

SILVER_PATH = os.path.join(DATA_PATH, "silver")

GOLD_PATH = os.path.join(DATA_PATH, "gold")

#======================================
# Layer paths
#======================================

# Bronze paths: raw JSON data from APIs.
BRONZE_PRODUCTS_PATH = os.path.join(BRONZE_PATH, "products")
BRONZE_USERS_PATH = os.path.join(BRONZE_PATH, "users")
BRONZE_CARTS_PATH = os.path.join(BRONZE_PATH, "carts")

# Silver paths: cleaned and flattened Parquet data.
SILVER_PRODUCTS_PATH = os.path.join(SILVER_PATH, "products")
SILVER_USERS_PATH = os.path.join(SILVER_PATH, "users")
SILVER_CARTS_PATH = os.path.join(SILVER_PATH, "carts")

# Gold paths: business KPI tables.
GOLD_REVENUE_PATH = os.path.join(GOLD_PATH, "revenue_metrics")
GOLD_CUSTOMER_PATH = os.path.join(GOLD_PATH, "customer_metrics")
GOLD_PRODUCTS_PATH = os.path.join(GOLD_PATH, "product_metrics")
#======================================
# log path
#======================================
LOG_PATH = os.path.join(BASE_DIR, "logs", "pipeline.log")

