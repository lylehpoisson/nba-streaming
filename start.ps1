# NBA Pipeline startup script
# Run this from the project root to start the Dagster UI
# Usage: .\start.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "Loading environment variables from .env..."
Get-Content .env | ForEach-Object {
    if ($_ -match '^([^#][^=]+)=(.+)$') {
        [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim())
    }
}

Write-Host "Parsing dbt project to regenerate manifest..."
Set-Location dbt_project
dbt parse
if ($LASTEXITCODE -ne 0) {
    Write-Error "dbt parse failed. Fix dbt errors before starting Dagster."
    exit 1
}
Set-Location ..

Write-Host "Starting Dagster..."
Set-Location dagster_project\nba_pipeline
$env:PYTHONLEGACYWINDOWSSTDIO = "1"
dagster dev