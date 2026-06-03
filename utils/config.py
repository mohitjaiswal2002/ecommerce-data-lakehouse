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

BRONZE_PRODUCTS_PATH = os.path.join(BRONZE_PATH, "products")
SILVER_PRODUCTS_PATH = os.path.join(SILVER_PATH, "products")
GOLD_PRODUCTS_PATH = os.path.join(GOLD_PATH, "products")


#======================================
# log path
#======================================
LOG_PATH = os.path.join(BASE_DIR, "logs", "pipeline.log")

