# Local Airflow Setup

This folder contains the Airflow DAG and Docker runtime setup for the
e-commerce data lakehouse project.

## Files

- `dags/ecommerce_data_lakehouse_dag.py`: Airflow DAG for Bronze, Silver, and Gold pipeline execution.
- `Dockerfile`: Custom Airflow image with Java and project Python dependencies.

## Start Airflow

From the project root:

```powershell
docker compose up --build
```

Or run the helper script:

```powershell
.\airflow\start-airflow.ps1
```

Open Airflow:

```text
http://localhost:8080
```

The `airflow standalone` command prints the generated admin username and
password in the container logs.

## Run The Pipeline

1. Open `http://localhost:8080`.
2. Sign in using the credentials printed in Docker logs.
3. Find the `ecommerce_data_lakehouse` DAG.
4. Toggle it on.
5. Click the trigger/run button.

## Stop Airflow

```powershell
docker compose down
```

Or run:

```powershell
.\airflow\stop-airflow.ps1
```
