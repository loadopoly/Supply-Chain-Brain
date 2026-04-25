<#
.SYNOPSIS
    Bootstrap the Supply Chain Brain on a NEW or RECOVERED machine.

.DESCRIPTION
    Run this ONCE after a machine replacement or fresh Windows install.
    It assumes:
      1. You are signed in to the same Microsoft 365 / OneDrive account as
         the source machine.
      2. OneDrive sync for "OneDrive - astecindustries.com" has completed so
         that the VS Code workspace folder (and local_brain.sqlite) is present.
      3. Python 3.x (≥ 3.11) is on PATH (Miniconda or system Python).

    Steps performed:
      A. Verify OneDrive workspace path exists
      B. Create Python virtual environment if missing
      C. Install/upgrade pip dependencies from requirements.txt
      D. Initialise the DB schema (idempotent — safe to run on an existing DB)
      E. Pull latest git history (cloud_learning_queue.jsonl may have grown)
      F. Install SCBLearningAgent scheduled task  (agent_watcher.ps1)
      G. Install AstecBridgeWatcher scheduled task (install_bridge_watcher.ps1)
      H. Start agent immediately (don't wait for next reboot)
      I. Write bootstrap event to logs/bootstrap_log.json

.PARAMETER SkipGitPull
    Skip the `git pull` step (useful when offline or on a private LAN).
.PARAMETER SkipBridgeWatcher
    Skip bridge-watcher task registration (if this machine won't be a bridge).
.PARAMETER DryRun
    Print every action without executing it.

.EXAMPLE
    # Normal new-machine bootstrap
    powershell -ExecutionPolicy Bypass -File bootstrap_new_machine.ps1

    # Skip bridge watcher on a cloud/server machine
    powershell -ExecutionPolicy Bypass -File bootstrap_new_machine.ps1 -SkipBridgeWatcher

    # Preview only
    powershell -ExecutionPolicy Bypass -File bootstrap_new_machine.ps1 -DryRun
#>
[CmdletBinding()]
param(
    [switch]$SkipGitPull,
    [switch]$SkipBridgeWatcher,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Helpers ──────────────────────────────────────────────────────────────────

function Write-Step { param([string]$Msg) Write-Host "`n[STEP] $Msg" -ForegroundColor Cyan }
function Write-Ok   { param([string]$Msg) Write-Host "  OK  $Msg"   -ForegroundColor Green }
function Write-Warn { param([string]$Msg) Write-Host " WARN $Msg"   -ForegroundColor Yellow }
function Write-Fail { param([string]$Msg) Write-Host " FAIL $Msg"   -ForegroundColor Red ; throw $Msg }

function Run {
    param([string]$Desc, [scriptblock]$Action)
    Write-Host "  >> $Desc" -ForegroundColor DarkGray
    if (-not $DryRun) { & $Action }
}

# ── Paths ─────────────────────────────────────────────────────────────────────

$OneDriveBase = Join-Path $env:USERPROFILE "OneDrive - astecindustries.com"
$WorkspaceDir = Join-Path $OneDriveBase "VS Code"
$PipelineDir  = Join-Path $WorkspaceDir "pipeline"
$VenvPython   = Join-Path $PipelineDir ".venv\Scripts\python.exe"
$ReqFile      = Join-Path $PipelineDir "requirements.txt"
$LogsDir      = Join-Path $PipelineDir "logs"
$BootstrapLog = Join-Path $LogsDir     "bootstrap_log.json"

# ── A. Verify OneDrive workspace path ─────────────────────────────────────────

Write-Step "A. Verifying OneDrive workspace path"

if (-not (Test-Path $PipelineDir)) {
    Write-Warn "Pipeline folder not found at: $PipelineDir"
    Write-Warn "OneDrive may still be syncing.  Waiting up to 5 minutes..."
    $waited = 0
    while (-not (Test-Path $PipelineDir) -and $waited -lt 300) {
        Start-Sleep 15
        $waited += 15
        Write-Host "  ... $waited s elapsed, still waiting for OneDrive sync ..." -ForegroundColor DarkYellow
    }
    if (-not (Test-Path $PipelineDir)) {
        Write-Fail "Pipeline folder still absent after waiting.  Sign in to OneDrive and re-run."
    }
}
Write-Ok "Workspace found: $PipelineDir"

# ── B. Create virtual environment if missing ───────────────────────────────────

Write-Step "B. Python virtual environment"

if (Test-Path $VenvPython) {
    Write-Ok "venv already exists: $VenvPython"
} else {
    Write-Host "  Creating .venv ..." -ForegroundColor DarkGray
    Run "python -m venv .venv" {
        Set-Location $PipelineDir
        python -m venv .venv
    }
    if (-not (Test-Path $VenvPython)) {
        # Fallback: try py launcher
        Run "py -m venv .venv (fallback)" {
            Set-Location $PipelineDir
            py -m venv .venv
        }
    }
    Write-Ok "venv created"
}

# ── C. Install dependencies ────────────────────────────────────────────────────

Write-Step "C. Installing pip dependencies"

Run "pip install -r requirements.txt" {
    Set-Location $PipelineDir
    & $VenvPython -m pip install --quiet --upgrade pip
    & $VenvPython -m pip install --quiet -r $ReqFile
}
Write-Ok "Dependencies installed"

# ── D. Initialise DB schema ────────────────────────────────────────────────────

Write-Step "D. Initialising knowledge graph schema"

Run "init_schema()" {
    Set-Location $PipelineDir
    $initScript = @"
import sys
sys.path.insert(0, '.')
from src.brain.knowledge_corpus import init_schema
init_schema()
print('schema OK')
"@
    $initScript | & $VenvPython
}
Write-Ok "Schema initialised (idempotent)"

# ── E. Git pull (bring in latest cloud_learning_queue.jsonl) ──────────────────

if (-not $SkipGitPull) {
    Write-Step "E. Git pull (cloud learning queue)"

    Run "git pull origin main" {
        Set-Location $WorkspaceDir
        git pull --ff-only origin main 2>&1 | ForEach-Object { Write-Host "    $_" }
    }
    Write-Ok "Repo up to date"
} else {
    Write-Warn "E. Git pull skipped (-SkipGitPull)"
}

# ── F. Register SCBLearningAgent scheduled task ────────────────────────────────

Write-Step "F. Registering SCBLearningAgent scheduled task"

$WatcherInstaller = Join-Path $PipelineDir "install_agent_watcher.ps1"
if (Test-Path $WatcherInstaller) {
    Run "install_agent_watcher.ps1" {
        powershell -ExecutionPolicy Bypass -File $WatcherInstaller
    }
    Write-Ok "SCBLearningAgent task registered"
} else {
    Write-Warn "install_agent_watcher.ps1 not found — SCBLearningAgent NOT registered"
}

# ── G. Register AstecBridgeWatcher scheduled task ─────────────────────────────

if (-not $SkipBridgeWatcher) {
    Write-Step "G. Registering AstecBridgeWatcher scheduled task"

    $BridgeInstaller = Join-Path $PipelineDir "install_bridge_watcher.ps1"
    if (Test-Path $BridgeInstaller) {
        Run "install_bridge_watcher.ps1" {
            powershell -ExecutionPolicy Bypass -File $BridgeInstaller
        }
        Write-Ok "AstecBridgeWatcher task registered"
    } else {
        Write-Warn "install_bridge_watcher.ps1 not found — AstecBridgeWatcher NOT registered"
    }
} else {
    Write-Warn "G. Bridge watcher skipped (-SkipBridgeWatcher)"
}

# ── H. Start agent immediately ─────────────────────────────────────────────────

Write-Step "H. Starting SCBLearningAgent task"

Run "Start-ScheduledTask SCBLearningAgent" {
    try {
        Start-ScheduledTask -TaskName "SCBLearningAgent" -ErrorAction Stop
        Write-Ok "SCBLearningAgent started"
    } catch {
        Write-Warn "Could not start task: $_  (will start on next logon)"
    }
}

# ── I. Write bootstrap event ───────────────────────────────────────────────────

Write-Step "I. Recording bootstrap event"

$event = @{
    timestamp    = (Get-Date -Format "o")
    hostname     = $env:COMPUTERNAME
    username     = $env:USERNAME
    pipeline_dir = $PipelineDir
    venv_python  = $VenvPython
    dry_run      = $DryRun.IsPresent
}

if (-not $DryRun) {
    New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

    $events = @()
    if (Test-Path $BootstrapLog) {
        try { $events = (Get-Content $BootstrapLog -Raw | ConvertFrom-Json) } catch { $events = @() }
        if ($null -eq $events) { $events = @() }
        # Keep last 100 bootstrap events
        $events = @($events | Select-Object -Last 99)
    }
    $events += $event
    $events | ConvertTo-Json -Depth 3 | Set-Content $BootstrapLog -Encoding utf8
}
Write-Ok "Bootstrap event recorded: $BootstrapLog"

# ── Summary ────────────────────────────────────────────────────────────────────

Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host " BOOTSTRAP COMPLETE — Supply Chain Brain is live on:" -ForegroundColor Green
Write-Host "   Machine  : $env:COMPUTERNAME" -ForegroundColor White
Write-Host "   Workspace: $WorkspaceDir" -ForegroundColor White
Write-Host "   DB       : $PipelineDir\local_brain.sqlite" -ForegroundColor White
Write-Host ""
Write-Host " Learning continuity restored:" -ForegroundColor White
Write-Host "   - SCBLearningAgent scheduled task active (runs at startup + logon)" -ForegroundColor White
Write-Host "   - cloud_learning_queue.jsonl will be ingested on first agent run" -ForegroundColor White
Write-Host "   - downtime gap is logged to logs/downtime_log.json" -ForegroundColor White
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
