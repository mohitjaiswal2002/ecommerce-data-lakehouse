# E-Commerce Data Lakehouse with PySpark

An end-to-end Data Engineering project implementing a Medallion Architecture (Bronze → Silver → Gold) using PySpark, orchestrated with Apache Airflow running in Docker.

The pipeline ingests e-commerce data from a public REST API, stores raw snapshots, performs data cleansing and enrichment, and generates business-ready analytical datasets that can be consumed by reporting and BI tools.

## Architecture

```text
                DummyJSON API
                       │   paginated REST extraction (retry + backoff)
                       ▼
                Bronze Layer
          (Raw JSON API Snapshots)
                       │   flatten, dedupe to latest snapshot, cast, validate
                       ▼
                Silver Layer
      (Cleaned, Standardized Parquet)
                       │   joins, aggregations, window functions
                       ▼
                 Gold Layer
        (Business KPIs & Analytics)

        Orchestrated daily by an Apache Airflow DAG (Docker)
```

## Tech Stack

- Python
- PySpark (DataFrame API, Window functions)
- Parquet
- Apache Airflow
- Docker / Docker Compose
- Requests
- PyTest
- Git

## Data Source

This project uses the DummyJSON API as a sample e-commerce dataset.

Endpoints:

- https://dummyjson.com/products
- https://dummyjson.com/users
- https://dummyjson.com/carts

## Key Features

- End-to-end Medallion Architecture implementation
- Paginated API ingestion with retry and exponential backoff
- Raw data preservation with timestamped Bronze snapshots
- Snapshot-aware deduplication (latest version of each record wins)
- Schema enforcement, null handling and data quality rules in Silver
- Category-based Parquet partitioning
- Customer spend analytics and value segmentation
- Product inventory and cart analytics
- Airflow DAG with layer-aware task dependencies
- Dockerized Airflow runtime with Java and Spark
- Centralized configuration and logging
- Unit tests for API ingestion and PySpark transformation logic

---

## Project Structure

```text
ecommerce_data_lakehouse/
│
├── airflow/
│   ├── dags/
│   │   └── ecommerce_data_lakehouse_dag.py
│   ├── Dockerfile
│   ├── README.md
│   ├── start-airflow.ps1
│   └── stop-airflow.ps1
│
├── data/                      (generated, not committed)
│   ├── bronze/
│   │   ├── products/
│   │   ├── users/
│   │   └── carts/
│   │
│   ├── silver/
│   │   ├── products/          (partitioned by category)
│   │   ├── users/
│   │   └── carts/
│   │       ├── cart_summary/
│   │       └── cart_items/
│   │
│   └── gold/
│       ├── category_summary/
│       ├── brand_performance/
│       ├── product_enriched/  (partitioned by category)
│       ├── product_metrics/
│       ├── category_metrics/
│       ├── user_metrics/
│       ├── low_stock_alerts/
│       ├── top_products/
│       ├── user_enriched/
│       ├── demographic_metrics/
│       ├── user_spend_metrics/
│       ├── top_users/
│       └── user_segments/
│
├── ingestion/
│   ├── ingest_products.py
│   ├── ingest_users.py
│   └── ingest_carts.py
│
├── transformations/
│   ├── silver_products.py
│   ├── silver_users.py
│   ├── silver_carts.py
│   ├── gold_products.py
│   ├── gold_users.py
│   └── gold_carts.py
│
├── utils/
│   ├── api_utils.py
│   ├── config.py
│   ├── logger.py
│   └── transforms.py
│
├── tests/
│   ├── conftest.py
│   ├── test_api_utils.py
│   └── test_transforms.py
│
├── docker-compose.yml
├── pytest.ini
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

---

# Bronze Layer

The Bronze layer stores raw API responses as received from the source system.

Each ingestion script pages through its endpoint (`limit`/`skip`) using the shared helpers in `utils/api_utils.py`, retrying network and 5xx errors with exponential backoff, and writes a timestamped JSON snapshot.

### Ingestion Scripts

```text
ingestion/ingest_products.py
ingestion/ingest_users.py
ingestion/ingest_carts.py
```

### Bronze Outputs

```text
data/bronze/products/products_YYYYMMDD_HHMMSS.json
data/bronze/users/users_YYYYMMDD_HHMMSS.json
data/bronze/carts/carts_YYYYMMDD_HHMMSS.json
```

### Bronze Responsibilities

- API extraction with pagination and retries
- Raw data preservation
- Historical snapshot retention
- No transformations applied

---

# Silver Layer

The Silver layer transforms raw JSON into structured, analytics-ready Parquet datasets.

### Common Transformations

- JSON flattening
- Deduplication that keeps the most recent snapshot of each record (window function over the source file)
- Schema standardization and type casting
- Null handling
- Data quality rules (invalid prices, stock, ratings, ages, cart totals)
- Derived column creation

Shared, I/O-free transformations (snapshot deduplication, derived columns) live in `utils/transforms.py` so they can be unit tested.

---

## Product Transformations

Derived columns:

- discounted_price
- stock_status (Out of Stock / Low Stock / In Stock)

Output (partitioned by category):

```text
data/silver/products
```

---

## User Transformations

Derived columns:

- full_name
- age_group

Output:

```text
data/silver/users
```

---

## Cart Transformations

Cart data is normalized into two datasets:

- **cart_summary**: one record per cart, with discount_amount and avg_item_value
- **cart_items**: one record per product within a cart, with line_discount_amount

Outputs:

```text
data/silver/carts/cart_summary
data/silver/carts/cart_items
```

---

# Gold Layer

The Gold layer produces business-focused datasets for reporting, analytics, and dashboard consumption. Each table is written to its own folder under `data/gold/`.

## Product Analytics (`gold_products.py`)

| Table | Description |
|---|---|
| `category_summary` | Per category: product count, average original/discounted price, average discount %, average rating, total stock, total inventory value, out-of-stock and low-stock counts, top brand |
| `brand_performance` | Per brand: product count, categories present, pricing and rating averages, revenue potential, total stock, brand tier |
| `product_enriched` | Product fact table with price tier, rating rank within category, top-rated flag, inventory value and savings per unit (partitioned by category) |

## Cart Analytics (`gold_carts.py`)

| Table | Description |
|---|---|
| `product_metrics` | Per product: total quantity in carts, cart appearances, revenue estimate, low-stock flag |
| `category_metrics` | Per category: product count, total revenue estimate, average price and rating, total stock, low-stock product count |
| `user_metrics` | Per user: total carts, items ordered, total spend, average spend per cart |
| `low_stock_alerts` | Products below the inventory threshold, most urgent first |
| `top_products` | Top N products ranked by cart appearances |

## User Analytics (`gold_users.py`)

| Table | Description |
|---|---|
| `user_enriched` | Customer master: demographics, spend metrics and value segment (High / Mid / Low, by spend percentile) |
| `demographic_metrics` | Users and spend aggregated by age group, gender and country |
| `user_spend_metrics` | Per-user total carts, total/average/min/max cart value, products purchased |
| `top_users` | Top N customers ranked by total spend |
| `user_segments` | Value segment combined with purchase frequency (e.g. "High Value - Regular Shopper") |

Thresholds such as `LOW_STOCK_THRESHOLD`, `TOP_N_PRODUCTS` and `TOP_N_USERS` live in `utils/config.py`.

---

# How to Run

## Prerequisites

- Python 3.10+
- Java 17 (required by Spark)
- On Windows, `HADOOP_HOME` with `winutils.exe` for local Spark writes

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements-dev.txt
```

## Option A: Run Locally

Execute scripts from the project root in the following order.

```powershell
# Step 1: Ingestion (Bronze)
python ingestion\ingest_products.py
python ingestion\ingest_users.py
python ingestion\ingest_carts.py

# Step 2: Silver Transformations
python transformations\silver_products.py
python transformations\silver_users.py
python transformations\silver_carts.py

# Step 3: Gold Transformations
python transformations\gold_products.py
python transformations\gold_carts.py
python transformations\gold_users.py
```

## Option B: Run With Airflow (Docker)

```powershell
docker compose up --build
```

Open http://localhost:8080, sign in with the admin credentials printed in the container logs, and trigger the `ecommerce_data_lakehouse` DAG. See [airflow/README.md](airflow/README.md) for details.

The DAG runs daily, and each task depends only on the layers it reads:

```text
ingest_products → silver_products → gold_products
ingest_users    → silver_users  ─┬→ gold_users
ingest_carts    → silver_carts  ─┤
                silver_products ─┴→ gold_carts
```

## Run Tests

```powershell
python -m pytest
```

- `tests/test_api_utils.py`: pagination, retry on 5xx, no retry on 4xx, retry exhaustion, Bronze snapshot format (network calls are mocked)
- `tests/test_transforms.py`: latest-snapshot deduplication, discounted price and stock status rules, age group boundaries (runs a local SparkSession; requires Java)

---

# Logging

All pipeline execution logs are written to the console and to:

```text
logs/pipeline.log
```

Logging captures:

- Job start and completion
- Record counts and deduplication results
- Data quality warnings
- Output locations
- Runtime errors

---

# Skills Demonstrated

## Data Engineering

- ETL Pipeline Development
- Medallion Architecture
- Data Lakehouse Design
- Data Modeling
- Batch Processing
- Workflow Orchestration (Airflow)

## PySpark

- DataFrame API
- Aggregations
- Joins
- Window Functions
- Schema Management
- Partitioning

## Software Engineering

- Modular Code Design
- Reusable Utility Modules
- Logging Framework
- Configuration Management
- Containerization (Docker)
- Unit Testing (PyTest)
- Git Version Control

---

# Future Enhancements

- Delta Lake integration
- Incremental data processing
- CI/CD pipeline
- Power BI dashboard development
- Data quality monitoring framework
