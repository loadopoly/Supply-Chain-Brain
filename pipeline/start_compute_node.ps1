<#
.SYNOPSIS
    Start the Supply Chain Brain compute-node daemon on this workstation.

.DESCRIPTION
    Run this script from the RDP session on CRP-FS03 (or any peer machine)
    to join the Brain compute fabric as a symbiotic expansion node.

    What it does:
      1. Locates the OneDrive-synced workspace.
      2. Activates the Python venv (creates it + installs deps if missing).
      3. Sets SCBRAIN_GRID_SECRET so HMAC auth matches the originating node.
      4. Starts pipeline/compute_node_daemon.py which:
            - Publishes this machine's capacity to bridge_state/compute_peers/
            - Listens on :8000 for signed grid jobs from the Brain
            - Republishes a heartbeat every 30 s

.NOTES
    Run with:  .\pipeline\start_compute_node.ps1
    Stop with: Ctrl-C  (or close the window)

    The SCBRAIN_GRID_SECRET value below MUST match the env var set on the
    originating node (ROADD-5WD1NH3).  If you use a different secret, set
    it in your environment before running this script.
#>
param(
    [string]$Secret = $env:SCBRAIN_GRID_SECRET,
    [switch]$Background
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Locate workspace ──────────────────────────────────────────────────────
$OneDrivePath = "$env:USERPROFILE\OneDrive - astecindustries.com\VS Code"
if (-not (Test-Path $OneDrivePath)) {
    # Fallback: search common OneDrive root names
    $OneDrivePath = Get-ChildItem "$env:USERPROFILE" -Directory |
        Where-Object { $_.Name -like "OneDrive*astec*" } |
        Select-Object -First 1 -ExpandProperty FullName
}
if (-not $OneDrivePath -or -not (Test-Path $OneDrivePath)) {
    Write-Error "Cannot find the OneDrive workspace folder. Set `$OneDrivePath manually."
    exit 1
}

$WorkspaceRoot = $OneDrivePath
$PipelineRoot  = Join-Path $WorkspaceRoot "pipeline"
$VenvPython    = Join-Path $PipelineRoot ".venv\Scripts\python.exe"
$DaemonScript  = Join-Path $PipelineRoot "compute_node_daemon.py"

Write-Host ""
Write-Host "=== Supply Chain Brain — Compute Node Startup ===" -ForegroundColor Cyan
Write-Host "  Workspace : $WorkspaceRoot"
Write-Host "  Daemon    : $DaemonScript"

# ── Ensure venv ───────────────────────────────────────────────────────────
if (-not (Test-Path $VenvPython)) {
    Write-Host ""
    Write-Host "Python venv not found — creating …" -ForegroundColor Yellow
    Push-Location $PipelineRoot
    try {
        # Try py launcher first (Windows), then python3, then python
        $pyExe = @("py", "python3", "python") |
            Where-Object { Get-Command $_ -ErrorAction SilentlyContinue } |
            Select-Object -First 1
        if (-not $pyExe) {
            Write-Error "No Python interpreter found. Install Python 3.10+ and retry."
            exit 1
        }
        & $pyExe -m venv .venv
        Write-Host "  Venv created." -ForegroundColor Green

        Write-Host "  Installing minimum requirements …"
        $pip = Join-Path $PipelineRoot ".venv\Scripts\pip.exe"
        # Core dependencies required by compute_node_daemon + compute_grid
        $reqs = Join-Path $PipelineRoot "requirements.txt"
        if (Test-Path $reqs) {
            & $pip install -r $reqs --quiet
        } else {
            & $pip install psutil pyyaml --quiet
        }
        Write-Host "  Dependencies installed." -ForegroundColor Green
    }
    finally {
        Pop-Location
    }
}

# ── Auth secret ───────────────────────────────────────────────────────────
if (-not $Secret) {
    Write-Host ""
    Write-Host "SCBRAIN_GRID_SECRET not set — using dev default 'scbrain-dev'." -ForegroundColor DarkYellow
    Write-Host "  To use a real secret, set it in your environment or pass -Secret <value>."
    $Secret = "scbrain-dev"
}
$env:SCBRAIN_GRID_SECRET = $Secret

# ── Change to workspace root so relative paths resolve ───────────────────
Push-Location $WorkspaceRoot

# ── Start ─────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "Starting compute node …" -ForegroundColor Green
Write-Host "  Press Ctrl-C to stop." -ForegroundColor DarkGray
Write-Host ""

try {
    if ($Background) {
        # Background mode: start a hidden window so it survives session disconnect
        $proc = Start-Process -FilePath $VenvPython `
            -ArgumentList $DaemonScript `
            -WorkingDirectory $WorkspaceRoot `
            -WindowStyle Hidden `
            -PassThru
        Write-Host "  Daemon started in background (PID $($proc.Id))." -ForegroundColor Green
        Write-Host "  Stop with:  Stop-Process -Id $($proc.Id)"
    } else {
        # Foreground: output visible in this terminal
        & $VenvPython $DaemonScript
    }
} finally {
    Pop-Location
}
