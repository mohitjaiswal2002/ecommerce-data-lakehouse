import os

#======================================
# API configuration
#======================================

API_URL = "https://dummyjson.com/products"
PRODUCTS_API_URL = "https://dummyjson.com/products"
USERS_API_URL = "https://dummyjson.com/users"
CARTS_API_URL = "https://dummyjson.com/carts"
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
SILVER_PRODUCTS_PATH = os.path.join(BASE_DIR, "data", "silver", "products")
SILVER_USERS_PATH    = os.path.join(BASE_DIR, "data", "silver", "users")
SILVER_CARTS_PATH    = os.path.join(BASE_DIR, "data", "silver", "carts")


# Gold paths: business KPI tables.
GOLD_PRODUCT_METRICS   = os.path.join(BASE_DIR, "data", "gold", "product_metrics")
GOLD_CATEGORY_METRICS  = os.path.join(BASE_DIR, "data", "gold", "category_metrics")
GOLD_USER_METRICS      = os.path.join(BASE_DIR, "data", "gold", "user_metrics")
GOLD_LOW_STOCK_ALERTS  = os.path.join(BASE_DIR, "data", "gold", "low_stock_alerts")
GOLD_TOP_PRODUCTS      = os.path.join(BASE_DIR, "data", "gold", "top_products")
GOLD_USER_SPEND    = os.path.join(BASE_DIR, "data", "gold", "user_spend_metrics")
GOLD_USER_SEGMENTS = os.path.join(BASE_DIR, "data", "gold", "user_segments")
GOLD_TOP_USERS     = os.path.join(BASE_DIR, "data", "gold", "top_users")

#======================================
# log path
#======================================
LOG_PATH              = os.path.join(BASE_DIR, "logs", "pipeline.log")
LOW_STOCK_THRESHOLD   = 10
TOP_N_PRODUCTS        = 10
TOP_N_USERS             = 10


