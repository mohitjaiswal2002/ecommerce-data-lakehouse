# E-Commerce Data Lakehouse with PySpark

An end-to-end Data Engineering project implementing a Medallion Architecture (Bronze → Silver → Gold) using PySpark.

The pipeline ingests e-commerce data from public REST APIs, stores raw snapshots, performs data cleansing and enrichment, and generates business-ready analytical datasets that can be consumed by reporting and BI tools.

## Architecture

```text
                DummyJSON API
                       │
                       ▼
                Bronze Layer
          (Raw JSON API Snapshots)
                       │
                       ▼
                Silver Layer
      (Cleaned, Standardized Data)
                       │
                       ▼
                 Gold Layer
        (Business KPIs & Analytics)
```

## Tech Stack

- Python
- PySpark
- Parquet
- Requests
- Python Logging
- Git

## Data Source

This project uses the DummyJSON API as a sample e-commerce dataset.

Endpoints:

- https://dummyjson.com/products
- https://dummyjson.com/users
- https://dummyjson.com/carts

## Key Features

- End-to-end Medallion Architecture implementation
- API-based data ingestion
- Raw data preservation in Bronze layer
- Modular PySpark transformation pipelines
- Data cleansing and standardization
- Category-based partitioning
- User demographic enrichment
- Customer spending analytics
- Product inventory analytics
- Business KPI generation
- Centralized configuration management
- Centralized logging framework
- Reusable utility modules

---

## Project Structure

```text
ecommerce_data_lakehouse/
│
├── data/
│   ├── bronze/
│   │   ├── products/
│   │   ├── users/
│   │   └── carts/
│   │
│   ├── silver/
│   │   ├── products/
│   │   │   ├── category=beauty/
│   │   │   ├── category=fragrances/
│   │   │   ├── category=furniture/
│   │   │   └── category=groceries/
│   │   │
│   │   ├── users/
│   │   │
│   │   └── carts/
│   │       ├── cart_summary/
│   │       └── cart_items/
│   │
│   └── gold/
│       ├── category_metrics/
│       ├── product_metrics/
│       ├── low_stock_alerts/
│       ├── top_products/
│       ├── top_users/
│       ├── user_metrics/
│       ├── user_spend_metrics/
│       ├── user_segments/
│       └── user_metrics/
│           ├── demographic_metrics/
│           └── user_enriched/
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
│   └── spark_session.py
│
├── tests/
│
├── logs/
│   └── pipeline.log
│
├── requirements.txt
└── README.md
```

---

# Bronze Layer

The Bronze layer stores raw API responses exactly as received from the source system.

Each ingestion script extracts data from the respective API endpoint and stores timestamped JSON snapshots.

### Ingestion Scripts

```text
ingestion/ingest_products.py
ingestion/ingest_users.py
ingestion/ingest_carts.py
```

### Bronze Outputs

```text
data/bronze/products
data/bronze/users
data/bronze/carts
```

### Bronze Responsibilities

- API extraction
- Raw data preservation
- Historical snapshot retention
- No transformations applied

---

# Silver Layer

The Silver layer transforms raw JSON into structured, analytics-ready datasets.

### Common Transformations

- JSON flattening
- Schema standardization
- Data type casting
- Duplicate removal
- Null handling
- Derived column creation
- Business logic enrichment

---

## Product Transformations

Source:

```text
data/bronze/products
```

Derived columns:

- discounted_price
- discount_amount
- stock_status

Additional processing:

- Product schema standardization
- Price calculations
- Inventory enrichment

Output:

```text
data/silver/products
```

Products are partitioned by category for optimized query performance.

---

## User Transformations

Source:

```text
data/bronze/users
```

Derived columns:

- full_name
- age_group

Additional processing:

- User profile standardization
- Demographic enrichment

Output:

```text
data/silver/users
```

---

## Cart Transformations

Source:

```text
data/bronze/carts
```

Cart data is normalized into two datasets:

### cart_summary

One record per cart.

### cart_items

One record per product within a cart.

Outputs:

```text
data/silver/carts/cart_summary
data/silver/carts/cart_items
```

---

# Gold Layer

The Gold layer produces business-focused datasets for reporting, analytics, and dashboard consumption.

---

## Product Analytics

### product_metrics

Product-level KPI dataset containing:

- Revenue potential
- Inventory value
- Product rankings
- Stock indicators
- Pricing insights

Output:

```text
data/gold/product_metrics
```

---

### category_metrics

Category-level aggregations:

- Average price
- Average discount percentage
- Total inventory value
- Total stock
- Top brand per category

Output:

```text
data/gold/category_metrics
```

---

## User Analytics

### user_enriched

Customer master dataset enriched with:

- Demographics
- Spending information
- Cart activity

Output:

```text
data/gold/user_metrics/user_enriched
```

---

### demographic_metrics

Aggregated customer insights by:

- Gender
- Age group

Output:

```text
data/gold/user_metrics/demographic_metrics
```

---

### user_spend_metrics

Customer spending KPIs:

- Total spend
- Average spend
- Total carts
- Total items purchased

Output:

```text
data/gold/user_spend_metrics
```

---

### user_segments

Customer segmentation based on spending behavior:

- High Value
- Mid Value
- Low Value

Output:

```text
data/gold/user_segments
```

---

### top_users

Top spending customers ranked by total spend.

Output:

```text
data/gold/top_users
```

---

## Cart Analytics

### user_metrics

Customer purchasing activity metrics:

- Total carts
- Total items ordered
- Total spend

Output:

```text
data/gold/user_metrics
```

---

### top_products

Most frequently purchased products.

Output:

```text
data/gold/top_products
```

---

### low_stock_alerts

Products below inventory threshold requiring attention.

Output:

```text
data/gold/low_stock_alerts
```

---

# How to Run

Execute scripts in the following order.

## Step 1: Ingestion

```powershell
python ingestion\ingest_products.py
python ingestion\ingest_users.py
python ingestion\ingest_carts.py
```

## Step 2: Silver Transformations

```powershell
python transformations\silver_products.py
python transformations\silver_users.py
python transformations\silver_carts.py
```

## Step 3: Gold Transformations

```powershell
python transformations\gold_products.py
python transformations\gold_carts.py
python transformations\gold_users.py
```

---

# Logging

All pipeline execution logs are written to:

```text
logs/pipeline.log
```

Logging captures:

- Job start and completion
- Record counts
- Transformation progress
- Output locations
- Runtime errors and warnings

---

# Skills Demonstrated

## Data Engineering

- ETL / ELT Pipeline Development
- Medallion Architecture
- Data Lakehouse Design
- Data Modeling
- Batch Processing

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
- Git Version Control

---

# Future Enhancements

Planned improvements:

- Apache Airflow orchestration
- Docker containerization
- Delta Lake integration
- Incremental data processing
- CI/CD pipeline
- Automated testing with PyTest
- Power BI dashboard development
- Data quality monitoring framework

---

# Notes

This project demonstrates the implementation of a complete Medallion Data Lakehouse using PySpark, covering the full data lifecycle from API ingestion through business-ready analytics.

The architecture follows modern data engineering practices by separating raw, cleansed, and analytical datasets into Bronze, Silver, and Gold layers respectively, enabling scalability, maintainability, and analytical flexibility.