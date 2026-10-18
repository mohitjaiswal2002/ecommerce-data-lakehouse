"""
Airflow DAG for the local e-commerce data lakehouse pipeline.

The project is mounted into the Airflow container by docker-compose.yml, and
ECOMMERCE_LAKEHOUSE_HOME points at the mount path (default below):

    /opt/airflow/projects/ecommerce_data_lakehouse

Each task depends only on the layers it actually reads, and max_active_tasks=1
runs one Spark job at a time so parallel JVMs don't exhaust container memory.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_HOME = os.environ.get(
    "ECOMMERCE_LAKEHOUSE_HOME",
    "/opt/airflow/projects/ecommerce_data_lakehouse",
)

PYTHON_BIN = os.environ.get("ECOMMERCE_LAKEHOUSE_PYTHON", "python")


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def project_command(script_path: str) -> str:
    return (
        f"cd {PROJECT_HOME} "
        f"&& PYTHONPATH={PROJECT_HOME}:$PYTHONPATH {PYTHON_BIN} {script_path}"
    )


with DAG(
    dag_id="ecommerce_data_lakehouse",
    description="Ingest DummyJSON e-commerce data and build Bronze, Silver, and Gold layers.",
    default_args=default_args,
    start_date=datetime(2026, 8, 12),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["ecommerce", "pyspark", "lakehouse"],
) as dag:
    ingest_products = BashOperator(
        task_id="ingest_products",
        bash_command=project_command("ingestion/ingest_products.py"),
    )

    ingest_users = BashOperator(
        task_id="ingest_users",
        bash_command=project_command("ingestion/ingest_users.py"),
    )

    ingest_carts = BashOperator(
        task_id="ingest_carts",
        bash_command=project_command("ingestion/ingest_carts.py"),
    )

    silver_products = BashOperator(
        task_id="silver_products",
        bash_command=project_command("transformations/silver_products.py"),
    )

    silver_users = BashOperator(
        task_id="silver_users",
        bash_command=project_command("transformations/silver_users.py"),
    )

    silver_carts = BashOperator(
        task_id="silver_carts",
        bash_command=project_command("transformations/silver_carts.py"),
    )

    gold_products = BashOperator(
        task_id="gold_products",
        bash_command=project_command("transformations/gold_products.py"),
    )

    gold_carts = BashOperator(
        task_id="gold_carts",
        bash_command=project_command("transformations/gold_carts.py"),
    )

    gold_users = BashOperator(
        task_id="gold_users",
        bash_command=project_command("transformations/gold_users.py"),
    )

    ingest_products >> silver_products
    ingest_users >> silver_users
    ingest_carts >> silver_carts

    silver_products >> gold_products
    [silver_users, silver_carts] >> gold_users
    [silver_products, silver_users, silver_carts] >> gold_carts
