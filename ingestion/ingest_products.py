"""
ingest_products.py
------------------
Fetches all products from the DummyJSON API (paginated) and saves the raw
JSON to the Bronze layer.

Run:
    python ingestion/ingest_products.py
"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.api_utils import fetch_all_pages, save_bronze_snapshot
from utils.config import API_LIMIT, BRONZE_PRODUCTS_PATH, PRODUCTS_API_URL
from utils.logger import get_logger

logger = get_logger(__name__)


def ingest_products():
    logger.info("=== Products Ingestion - START ===")

    records = fetch_all_pages(PRODUCTS_API_URL, data_key="products", limit=API_LIMIT)
    output_file = save_bronze_snapshot(records, "products", BRONZE_PRODUCTS_PATH)

    logger.info(f"Products ingestion completed: {len(records)} records saved to {output_file}")


if __name__ == "__main__":
    ingest_products()
