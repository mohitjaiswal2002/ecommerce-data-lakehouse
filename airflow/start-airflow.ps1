$ErrorActionPreference = "Stop"

Set-Location "$PSScriptRoot\.."

Write-Host "Building and starting local Airflow..." -ForegroundColor Cyan
docker compose up --build
