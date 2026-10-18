"""
ingest_users.py
---------------
Fetches all users from the DummyJSON API (paginated) and saves the raw
JSON to the Bronze layer.

Run:
    python ingestion/ingest_users.py
"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.api_utils import fetch_all_pages, save_bronze_snapshot
from utils.config import API_LIMIT, BRONZE_USERS_PATH, USERS_API_URL
from utils.logger import get_logger

logger = get_logger(__name__)


def ingest_users():
    logger.info("=== Users Ingestion - START ===")

    records = fetch_all_pages(USERS_API_URL, data_key="users", limit=API_LIMIT)
    output_file = save_bronze_snapshot(records, "users", BRONZE_USERS_PATH)

    logger.info(f"Users ingestion completed: {len(records)} records saved to {output_file}")


if __name__ == "__main__":
    ingest_users()
