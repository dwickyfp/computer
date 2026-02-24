# Start the Rosetta Worker service (Windows PowerShell - HIGH PERFORMANCE)
# Usage: .\start.ps1 [-Concurrency N] [-Queues "preview,default"] [-LogLevel "info"] [-Beat]
#
# Note: If you get "cannot be loaded because running scripts is disabled", run:
#   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
#
# Parameters:
#   -Concurrency N    Number of worker threads (default: 8)
#   -Queues Q         Comma-separated queue names (default: preview,default)
#   -LogLevel LEVEL   Log level (default: info)
#   -Beat             Also start Celery Beat scheduler

param(
    [int]$Concurrency = $env:WORKER_CONCURRENCY,
    [string]$Queues = "preview,default",
    [string]$LogLevel = $env:LOG_LEVEL,
    [switch]$Beat
)

# Set defaults if not provided
if (-not $Concurrency) { $Concurrency = 8 }
if (-not $LogLevel) { $LogLevel = "info" }

# Change to script directory
Set-Location $PSScriptRoot

Write-Host "Starting Rosetta Worker..." -ForegroundColor Green
Write-Host "  Concurrency: $Concurrency"
Write-Host "  Queues: $Queues"
Write-Host "  Log Level: $LogLevel"

# Start health API server in background
$ServerPort = if ($env:SERVER_PORT) { $env:SERVER_PORT } else { 8002 }
Write-Host "Starting health API server on port ${ServerPort}..." -ForegroundColor Cyan

$HealthJob = Start-Job -ScriptBlock {
    param($Port)
    Set-Location $using:PSScriptRoot
    python server.py
} -ArgumentList $ServerPort

Write-Host "  Health API Job ID: $($HealthJob.Id)" -ForegroundColor Gray

# Function to cleanup health server on exit
function Cleanup {
    Write-Host "`nStopping health API server..." -ForegroundColor Yellow
    Stop-Job -Job $HealthJob -ErrorAction SilentlyContinue
    Remove-Job -Job $HealthJob -Force -ErrorAction SilentlyContinue
}

# Register cleanup on exit
Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action { Cleanup } | Out-Null

# Trap Ctrl+C
try {
    # Build celery command
    $CeleryArgs = @(
        "-A", "main", "worker",
        "--loglevel=$LogLevel",
        "-Q", $Queues,
        "-c", $Concurrency,
        "--pool=threads"
    )

    if ($Beat) {
        Write-Host "  Beat: enabled" -ForegroundColor Cyan
        $CeleryArgs += "--beat"
    }

    Write-Host "`nStarting Celery worker..." -ForegroundColor Green
    & celery $CeleryArgs
}
finally {
    Cleanup
}
