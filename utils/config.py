import os

#======================================
# API configuration
#======================================

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
SILVER_PRODUCTS_PATH = os.path.join(SILVER_PATH, "products")
SILVER_USERS_PATH    = os.path.join(SILVER_PATH, "users")
SILVER_CARTS_PATH    = os.path.join(SILVER_PATH, "carts")


# Gold paths: business KPI tables. Every table has its own folder so that
# overwrite-mode writes from one job can never delete another job's output.

# gold_products.py
GOLD_CATEGORY_SUMMARY    = os.path.join(GOLD_PATH, "category_summary")
GOLD_BRAND_PERFORMANCE   = os.path.join(GOLD_PATH, "brand_performance")
GOLD_PRODUCT_ENRICHED    = os.path.join(GOLD_PATH, "product_enriched")

# gold_carts.py
GOLD_PRODUCT_METRICS     = os.path.join(GOLD_PATH, "product_metrics")
GOLD_CATEGORY_METRICS    = os.path.join(GOLD_PATH, "category_metrics")
GOLD_USER_METRICS        = os.path.join(GOLD_PATH, "user_metrics")
GOLD_LOW_STOCK_ALERTS    = os.path.join(GOLD_PATH, "low_stock_alerts")
GOLD_TOP_PRODUCTS        = os.path.join(GOLD_PATH, "top_products")

# gold_users.py
GOLD_USER_ENRICHED       = os.path.join(GOLD_PATH, "user_enriched")
GOLD_DEMOGRAPHIC_METRICS = os.path.join(GOLD_PATH, "demographic_metrics")
GOLD_USER_SPEND          = os.path.join(GOLD_PATH, "user_spend_metrics")
GOLD_USER_SEGMENTS       = os.path.join(GOLD_PATH, "user_segments")
GOLD_TOP_USERS           = os.path.join(GOLD_PATH, "top_users")

#======================================
# log path
#======================================
LOG_PATH              = os.path.join(BASE_DIR, "logs", "pipeline.log")
LOW_STOCK_THRESHOLD   = 10
TOP_N_PRODUCTS        = 10
TOP_N_USERS           = 10


