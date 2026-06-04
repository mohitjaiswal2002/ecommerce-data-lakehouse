# E-Commerce Data Lakehouse Project

This is a data engineering project where I am building an e-commerce lakehouse pipeline step by step using Python and PySpark.

The idea is to take data from an API, store the raw data first, clean and transform it, and then create business-level tables that can be used for analysis or dashboards.

Current flow:

DummyJSON API -> Bronze -> Silver -> Gold

## Tech Used

- Python
- PySpark
- Parquet
- Requests
- Python logging
- Git

## Data Source

I am using the DummyJSON API for sample e-commerce data.

Current endpoint used:

https://dummyjson.com/products

Right now the project is focused on products. Users and carts will be added later.

## Project Structure

ecommerce_data_lakehouse/
|
|-- data/
|   |-- bronze/
|   |-- silver/
|   |-- gold/
|
|-- ingestion/
|   |-- ingest_products.py
|
|-- transformations/
|   |-- silver_products.py
|   |-- gold_products.py
|
|-- utils/
|   |-- config.py
|   |-- logger.py
|
|-- logs/
|   |-- pipeline.log
|
|-- tests/
|-- requirements.txt
|-- README.md

## Bronze Layer

The Bronze layer stores raw API data.

For products, the ingestion script calls the API and saves the response as JSON files.

Script:

ingestion/ingest_products.py

Output:

data/bronze/products

The files are saved with timestamps so that old raw data is not overwritten.

## Silver Layer

The Silver layer takes the raw product JSON and converts it into a cleaner structure.

Script:

transformations/silver_products.py

Main steps:

- Read raw JSON files from Bronze
- Explode the nested `products` array
- Flatten product fields
- Cast columns to proper data types
- Remove duplicate products
- Handle null values
- Apply basic data quality checks
- Add derived columns like `discounted_price` and `stock_status`
- Write the output as Parquet

Output:

data/silver/products

The Silver product data is partitioned by category.

## Gold Layer

The Gold layer creates product-level business metrics from the Silver data.

Script:

transformations/gold_products.py

Gold outputs:

data/gold/product_metrics/category_summary
data/gold/product_metrics/brand_performance
data/gold/product_metrics/product_enriched

These tables can be used for analysis or a dashboard.

Some of the metrics created are:

- Category summary
- Average price by category
- Average discount by category
- Total stock
- Inventory value
- Top brand by category
- Brand performance
- Product price tier
- Product rank inside category
- Savings per unit

## How To Run

Run the scripts in this order:

python ingestion\ingest_products.py
python transformations\silver_products.py
python transformations\gold_products.py

If running through Spark submit:

spark-submit transformations\silver_products.py
spark-submit transformations\gold_products.py

## Logging

Logs are written to:

logs/pipeline.log

The logs show when each job starts, row counts, output paths, and when each transformation completes.

## Current Status

Completed so far:

- Product API ingestion
- Bronze product JSON storage
- Silver product transformation
- Gold product metrics
- Logging
- Parquet output
- Category partitioning

Still pending:

- Users ingestion
- Carts ingestion
- Silver users transformation
- Silver carts transformation
- Gold revenue/customer metrics
- Airflow DAG
- More validation checks
- Unit tests
- Dashboard
- Delta Lake support

## Next Steps

My next plan is to complete the remaining data sources one by one:

1. Add users ingestion
2. Add carts ingestion
3. Create Silver tables for users and carts
4. Create Gold metrics using products, users, and carts together
5. Add Airflow scheduling once the scripts are stable

After that, I want to add Delta Lake and dashboard reporting.

## Notes

This project is still in progress. I am building it in phases so that each layer works properly before adding more complexity.
