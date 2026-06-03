import requests
import json
import os
import logging
from datetime import datetime
from utils.logger import get_logger
from utils.config import BRONZE_PRODUCTS_PATH
from utils.config import API_URL

logger = get_logger(__name__)


# Fetch data
response = requests.get(API_URL, timeout=30)
response.raise_for_status()

# Convert to json
data = response.json()

# Create timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Output folder
output_folder = BRONZE_PRODUCTS_PATH

# Create folder if not exists
os.makedirs(output_folder, exist_ok=True)

# Output file path
output_file = f"{output_folder}/products_{timestamp}.json"

# Save raw JSON
with open(output_file, "w") as file:
    json.dump(data, file, indent=4)

logger.info(f"Data ingestion completed successfully! Data saved to {output_file}")