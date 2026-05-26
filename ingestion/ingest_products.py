import requests
import json
import os
from datetime import datetime

# API endpoint
api_url = "https://dummyjson.com/products"

# Fetch data
response = requests.get(api_url)

# Convert response to JSON
data = response.json()

# Create timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Output folder
output_folder = "ecommerce-data-lakehouse/data/bronze/products"

# Create folder if not exists
os.makedirs(output_folder, exist_ok=True)

# Output file path
output_file = f"{output_folder}/products_{timestamp}.json"

# Save raw JSON
with open(output_file, "w") as file:
    json.dump(data, file, indent=4)

print("Raw data saved successfully!")
print("File location:", output_file)