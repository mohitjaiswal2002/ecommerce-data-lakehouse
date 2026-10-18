$ErrorActionPreference = "Stop"

Set-Location "$PSScriptRoot\.."

Write-Host "Stopping local Airflow..." -ForegroundColor Cyan
docker compose down
