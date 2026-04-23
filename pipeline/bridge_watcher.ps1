# bridge_watcher.ps1
# Persistent agent that runs on the LAPTOP under a Scheduled Task.
# Watches the OneDrive-synced trigger folder and applies port-forward rules
# on demand, with no human interaction required after the one-time bootstrap.

$ErrorActionPreference = "Continue"
$TriggerDir = "$env:USERPROFILE\OneDrive - astecindustries.com\VS Code\pipeline\bridge_triggers"
$StateDir   = "$env:USERPROFILE\OneDrive - astecindustries.com\VS Code\pipeline\bridge_state"
$LogFile    = "$StateDir\watcher.log"

New-Item -ItemType Directory -Force -Path $TriggerDir | Out-Null
New-Item -ItemType Directory -Force -Path $StateDir   | Out-Null

function Write-Log($msg) {
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$stamp $msg" | Out-File -FilePath $LogFile -Append -Encoding utf8
}

function Set-PortProxy($desktopIp) {
    $ports = @(
        @{listen=33890; target=3389},   # RDP
        @{listen=14330; target=1433},   # SQL
        @{listen=8000;  target=8000}    # LLM/API + compute_grid
    )
    foreach ($p in $ports) {
        netsh interface portproxy delete v4tov4 listenport=$($p.listen) listenaddress=0.0.0.0 2>$null | Out-Null
        netsh interface portproxy add    v4tov4 listenport=$($p.listen) listenaddress=0.0.0.0 connectport=$($p.target) connectaddress=$desktopIp | Out-Null
    }
    netsh advfirewall firewall delete rule name="AstecBridge" 2>$null | Out-Null
    netsh advfirewall firewall add rule name="AstecBridge" dir=in action=allow protocol=TCP localport=33890,14330,8000 | Out-Null
}

function Start-ComputeNode() {
    # Spawned on every domain workstation when a `compute_*.trigger` lands.
    # Reuses the same OneDrive-synced piggyback fabric — no new transport.
    $marker = "$StateDir\compute_node.pid"
    if (Test-Path $marker) {
        try {
            $pidVal = [int](Get-Content $marker -Raw).Trim()
            if (Get-Process -Id $pidVal -ErrorAction SilentlyContinue) {
                return  # already running
            }
        } catch {}
    }
    $pipeline = "$env:USERPROFILE\OneDrive - astecindustries.com\VS Code\pipeline"
    $py = (Get-Command python -ErrorAction SilentlyContinue).Source
    if (-not $py) { Write-Log "compute_node: python not on PATH"; return }
    $procArgs = @(
        "-c",
        "import sys; sys.path.insert(0, r'$pipeline'); from src.brain.compute_grid import serve_compute_node; serve_compute_node()"
    )
    $proc = Start-Process -FilePath $py -ArgumentList $procArgs `
        -WindowStyle Hidden -PassThru -WorkingDirectory $pipeline
    $proc.Id | Out-File $marker -Encoding ascii -Force
    Write-Log "compute_node spawned (pid $($proc.Id)) on port 8000"
}

function Publish-WifiIp() {
    $wifi = Get-NetIPAddress -AddressFamily IPv4 |
        Where-Object { $_.InterfaceAlias -match "Wi-Fi" -and $_.IPAddress -notmatch "^169\." } |
        Select-Object -First 1
    if ($wifi) {
        $wifi.IPAddress | Out-File -FilePath "$StateDir\wifi_ip.txt" -Encoding ascii -Force
        return $wifi.IPAddress
    }
    return $null
}

Write-Log "Watcher started."
Publish-WifiIp | Out-Null

while ($true) {
    $triggers = Get-ChildItem -Path $TriggerDir -Filter "*.trigger" -ErrorAction SilentlyContinue
    foreach ($t in $triggers) {
        try {
            $name = $t.Name
            if ($name -like "compute_*") {
                # Compute-grid wake-up — start the local compute_node daemon
                # and publish a fresh capacity heartbeat. The trigger payload
                # is JSON; we don't need to parse it for the spawn step.
                Write-Log "Compute trigger received -> ensuring compute_node"
                Start-ComputeNode
                Remove-Item $t.FullName -Force
                continue
            }
            $desktopIp = (Get-Content $t.FullName -Raw).Trim()
            if (-not $desktopIp) { $desktopIp = "172.16.4.76" }
            Write-Log "Trigger received -> applying portproxy to $desktopIp"
            Set-PortProxy $desktopIp
            $ip = Publish-WifiIp
            Write-Log "Bridge active. Wi-Fi IP: $ip"
            Remove-Item $t.FullName -Force
            "ok $(Get-Date -Format o) ip=$ip" | Out-File "$StateDir\last_run.txt" -Encoding ascii -Force
        } catch {
            Write-Log "ERROR: $_"
        }
    }
    Start-Sleep -Seconds 5
}
