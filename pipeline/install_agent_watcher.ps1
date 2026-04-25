# install_agent_watcher.ps1
# ONE-TIME bootstrap — run as Administrator from this machine.
# Installs agent_watcher.ps1 as a persistent Windows Scheduled Task named
# "SCBLearningAgent" that:
#   - Starts at boot and at logon
#   - Restarts automatically within 1 minute if the watcher script crashes
#   - Runs hidden with highest privileges
#
# After this, the learning process survives reboots, crashes, and user logouts.

$ErrorActionPreference = "Stop"

$Root       = "$env:USERPROFILE\OneDrive - astecindustries.com\VS Code\pipeline"
$Watcher    = "$Root\agent_watcher.ps1"
$TaskName   = "SCBLearningAgent"

if (-not (Test-Path $Watcher)) {
    throw "agent_watcher.ps1 not found at $Watcher — cannot install."
}

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$Watcher`"" `
    -WorkingDirectory $Root

# Fire at boot (before logon) AND at logon — covers both scenarios
$triggerBoot   = New-ScheduledTaskTrigger -AtStartup
$triggerLogon  = New-ScheduledTaskTrigger -AtLogOn

$principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType S4U `
    -RunLevel Highest

# Aggressive restart settings: restart up to 9999 times, 1 min apart
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Days 3650) `
    -RestartCount 9999 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -MultipleInstances IgnoreNew

# Remove any stale version first
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask `
    -TaskName  $TaskName `
    -Action    $action `
    -Trigger   @($triggerBoot, $triggerLogon) `
    -Principal $principal `
    -Settings  $settings | Out-Null

Write-Host "[+] Scheduled Task '$TaskName' registered."
Write-Host "    Triggers: AtStartup + AtLogOn"
Write-Host "    Auto-restart: every 1 min, up to 9999 times"
Write-Host ""

# Start immediately — no reboot required
Start-ScheduledTask -TaskName $TaskName
Write-Host "[+] Task started. Learning will never stop."
Write-Host "    Heartbeat: $Root\logs\agent_heartbeat.txt"
Write-Host "    Downtime log: $Root\logs\downtime_log.json"
Write-Host "    Watcher log: $Root\logs\agent_watcher.log"
