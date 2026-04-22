# install_bridge_watcher.ps1
# ONE-TIME bootstrap. Run this ONCE on the laptop as Administrator (via your
# active RDP session). After this, the laptop will respond to remote triggers
# from any agent forever, no further user action required.

$ErrorActionPreference = "Stop"

$watcher = "$env:USERPROFILE\OneDrive - astecindustries.com\VS Code\pipeline\bridge_watcher.ps1"
if (-not (Test-Path $watcher)) { throw "bridge_watcher.ps1 not found at $watcher" }

$taskName = "AstecBridgeWatcher"
$action   = New-ScheduledTaskAction -Execute "powershell.exe" `
            -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$watcher`""
$trigger  = New-ScheduledTaskTrigger -AtLogOn
$trigger2 = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" `
             -LogonType S4U -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries -StartWhenAvailable `
            -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1)

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $taskName -Action $action `
    -Trigger @($trigger, $trigger2) -Principal $principal -Settings $settings | Out-Null

Start-ScheduledTask -TaskName $taskName
Write-Host "[+] Bridge watcher installed and started. Future agents can trigger it remotely."
