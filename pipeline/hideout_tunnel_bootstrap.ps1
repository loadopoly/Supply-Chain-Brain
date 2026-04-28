# Black Bulls Hideout — Dev Tunnel Host + Compute Node Daemon
# Runs on the HIDEOUT machine.  Keeps the devtunnel alive and the
# compute_node_daemon running so the Brain can dispatch GPU jobs to it.
#
# First run: GitHub device-code login at https://github.com/login/device
# Subsequent runs: starts tunnel + daemon immediately (auth is cached).
#
# Usage:  .\hideout_tunnel_bootstrap.ps1
#   Optional: -NoDaemon  — skip compute node, only host the tunnel
#   Optional: -PythonExe  "C:\path\to\python.exe"

param(
    [switch]$NoDaemon,
    [string]$PythonExe = ""
)

$ErrorActionPreference = "Continue"
$TunnelId = "scbrain-hideout.use2"
$dtExe    = "$env:TEMP\dt.exe"

# ---------------------------------------------------------------------------
# 1. Ensure dt.exe
# ---------------------------------------------------------------------------
if (-not (Test-Path $dtExe)) {
    Write-Host "Downloading dt.exe..." -ForegroundColor Cyan
    Invoke-WebRequest 'https://aka.ms/TunnelsCliDownload/win-x64' `
        -OutFile $dtExe -UseBasicParsing
    Write-Host "Done." -ForegroundColor Green
}

# ---------------------------------------------------------------------------
# 2. GitHub login (device-code flow — only needed once; caches credential)
# ---------------------------------------------------------------------------
$userInfo = & $dtExe user show 2>&1
if ($userInfo -match 'not logged in|No user') {
    Write-Host ""
    Write-Host "LOGIN REQUIRED — follow the prompts below:" -ForegroundColor Yellow
    Write-Host "  1. A code will appear — go to https://github.com/login/device" -ForegroundColor Cyan
    Write-Host "  2. Enter the code and approve" -ForegroundColor Cyan
    Write-Host "  3. Come back here — tunnel starts automatically" -ForegroundColor Cyan
    Write-Host ""
    & $dtExe user login --github -d
}

# ---------------------------------------------------------------------------
# 3. Locate Python / venv for the compute_node_daemon
# ---------------------------------------------------------------------------
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $PythonExe) {
    $candidates = @(
        (Join-Path $ScriptDir ".venv\Scripts\python.exe"),
        "python",
        "python3"
    )
    foreach ($c in $candidates) {
        if (Get-Command $c -ErrorAction SilentlyContinue) {
            $PythonExe = $c; break
        }
    }
}

# ---------------------------------------------------------------------------
# 4. Start compute_node_daemon as a background job
# ---------------------------------------------------------------------------
$daemonJob = $null
if (-not $NoDaemon -and $PythonExe) {
    Write-Host ""
    Write-Host "Starting compute_node_daemon..." -ForegroundColor Cyan
    $daemonScript = @"
import sys, os
sys.path.insert(0, r'$ScriptDir')
os.chdir(r'$ScriptDir')
from src.brain.compute_grid import serve_compute_node, publish_local_capacity
publish_local_capacity()
serve_compute_node()
"@
    $daemonJob = Start-Job -ScriptBlock {
        param($py, $code)
        & $py -c $code
    } -ArgumentList $PythonExe, $daemonScript
    Write-Host "Compute node started (job $($daemonJob.Id))." -ForegroundColor Green
} elseif (-not $NoDaemon) {
    Write-Host "WARNING: Python not found — compute_node_daemon NOT started." `
        -ForegroundColor Yellow
    Write-Host "         Pass -PythonExe 'C:\path\to\python.exe' to fix." `
        -ForegroundColor Yellow
}

# ---------------------------------------------------------------------------
# 5. Host the tunnel — restart on exit (network blips, token refresh, etc.)
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "Hosting tunnel $TunnelId — KEEP THIS WINDOW OPEN." -ForegroundColor Green
Write-Host "(Ctrl+C to stop)" -ForegroundColor DarkGray

while ($true) {
    & $dtExe host $TunnelId
    $exit = $LASTEXITCODE
    Write-Host "$(Get-Date -Format 'HH:mm:ss')  tunnel exited ($exit) — restarting in 5 s..." `
        -ForegroundColor Yellow
    Start-Sleep -Seconds 5
    # Restart compute daemon if it died
    if ($daemonJob -and (Get-Job -Id $daemonJob.Id -ErrorAction SilentlyContinue).State -ne 'Running') {
        Write-Host "Restarting compute_node_daemon..." -ForegroundColor Cyan
        $daemonJob = Start-Job -ScriptBlock {
            param($py, $code)
            & $py -c $code
        } -ArgumentList $PythonExe, $daemonScript
    }
}
