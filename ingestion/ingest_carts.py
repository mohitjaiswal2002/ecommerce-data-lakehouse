"""
ingest_carts.py
---------------
Fetches all carts from the DummyJSON API (paginated) and saves the raw
JSON to the Bronze layer.

Run:
    python ingestion/ingest_carts.py
"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.api_utils import fetch_all_pages, save_bronze_snapshot
from utils.config import API_LIMIT, BRONZE_CARTS_PATH, CARTS_API_URL
from utils.logger import get_logger

logger = get_logger(__name__)


def ingest_carts():
    logger.info("=== Carts Ingestion - START ===")

    records = fetch_all_pages(CARTS_API_URL, data_key="carts", limit=API_LIMIT)
    output_file = save_bronze_snapshot(records, "carts", BRONZE_CARTS_PATH)

    logger.info(f"Carts ingestion completed: {len(records)} records saved to {output_file}")


if __name__ == "__main__":
    ingest_carts()
