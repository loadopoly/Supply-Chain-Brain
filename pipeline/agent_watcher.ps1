# agent_watcher.ps1
# Persistent watchdog for autonomous_agent.py — run as a Scheduled Task.
# Never lets the learning process stop:
#   - Starts autonomous_agent.py immediately on boot
#   - Detects crashes and restarts within 30 seconds
#   - Writes heartbeat every 30 s so downtime is measurable
#   - Records downtime windows to logs/downtime_log.json for learning debt

$ErrorActionPreference = "Continue"
$Root      = "$env:USERPROFILE\OneDrive - astecindustries.com\VS Code\pipeline"
$LogDir    = "$Root\logs"
$Heartbeat = "$LogDir\agent_heartbeat.txt"
$DowntimeLog = "$LogDir\downtime_log.json"
$PythonExe = "$Root\.venv\Scripts\python.exe"
$AgentScript = "$Root\autonomous_agent.py"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Write-WatchLog($msg) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$ts [WATCHER] $msg" | Out-File "$LogDir\agent_watcher.log" -Append -Encoding utf8
    Write-Host "$ts [WATCHER] $msg"
}

function Write-Heartbeat {
    $now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $now | Out-File -FilePath $Heartbeat -Encoding utf8 -NoNewline
}

function Record-DowntimeWindow($startEpoch, $endEpoch) {
    $windows = @()
    if (Test-Path $DowntimeLog) {
        try {
            $raw = Get-Content $DowntimeLog -Raw | ConvertFrom-Json
            $windows = @($raw.windows)
        } catch {}
    }
    $seconds = $endEpoch - $startEpoch
    if ($seconds -gt 60) {
        $windows += @{
            start  = $startEpoch
            end    = $endEpoch
            seconds = $seconds
            start_iso = [DateTimeOffset]::FromUnixTimeSeconds($startEpoch).UtcDateTime.ToString("o")
        }
        # Keep last 500 downtime windows
        if ($windows.Count -gt 500) { $windows = $windows[-500..-1] }
        @{ windows = $windows } | ConvertTo-Json -Depth 5 | Out-File $DowntimeLog -Encoding utf8
    }
}

function Get-LastHeartbeat {
    if (-not (Test-Path $Heartbeat)) { return 0 }
    try {
        $val = (Get-Content $Heartbeat -Raw).Trim()
        return [long]$val
    } catch { return 0 }
}

# ─── Verify Python and agent script exist ─────────────────────────────────────
if (-not (Test-Path $PythonExe)) {
    # Fall back to system Python
    $PythonExe = (Get-Command python -ErrorAction SilentlyContinue)?.Source
    if (-not $PythonExe) {
        Write-WatchLog "FATAL: Python executable not found. Exiting watchdog."
        exit 1
    }
    Write-WatchLog "Using system Python: $PythonExe"
}
if (-not (Test-Path $AgentScript)) {
    Write-WatchLog "FATAL: autonomous_agent.py not found at $AgentScript"
    exit 1
}

Write-WatchLog "Watchdog started. Agent: $AgentScript | Python: $PythonExe"
Write-Heartbeat

# ─── Main watchdog loop ───────────────────────────────────────────────────────
$downSince = 0  # epoch when agent was last seen as DOWN (0 = currently UP)

while ($true) {
    # Launch agent as a child process
    Write-WatchLog "Starting autonomous_agent.py ..."
    try {
        $proc = Start-Process -FilePath $PythonExe `
            -ArgumentList $AgentScript `
            -WorkingDirectory $Root `
            -PassThru -NoNewWindow

        Write-WatchLog "Agent PID=$($proc.Id) launched."
        if ($downSince -gt 0) {
            $now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
            Record-DowntimeWindow $downSince $now
            Write-WatchLog "Downtime window recorded: $([math]::Round(($now - $downSince)/60,1)) min"
            $downSince = 0
        }

        # ── Monitor loop: heartbeat every 30 s, detect if agent dies ──────────
        while ($true) {
            Start-Sleep -Seconds 30
            Write-Heartbeat

            if ($proc.HasExited) {
                Write-WatchLog "Agent exited (code=$($proc.ExitCode)). Restarting in 15 s ..."
                $downSince = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
                Start-Sleep -Seconds 15
                break  # break inner loop → outer loop restarts agent
            }

            # Also write a readable ISO timestamp line for human inspection
            $iso = [DateTimeOffset]::UtcNow.ToString("yyyy-MM-dd HH:mm:ss UTC")
            "$iso | PID=$($proc.Id) | running" | Out-File "$LogDir\agent_heartbeat_readable.txt" -Encoding utf8 -NoNewline
        }
    } catch {
        Write-WatchLog "Failed to start agent: $_. Retrying in 30 s ..."
        if ($downSince -eq 0) { $downSince = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds() }
        Start-Sleep -Seconds 30
    }
}
