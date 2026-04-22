param (
    [string]$TargetIP = "192.168.250.200",
    [string]$Username = "agard@astecindustries.com",
    [string]$Password = ""
)

Write-Host "=== Remote VPN Trigger ==="
Write-Host "Initiating remote execution back to Client Laptop ($TargetIP)"

if ([string]::IsNullOrEmpty($Password)) {
    $cred = Get-Credential -UserName $Username -Message "Enter the Windows/Admin password for your local physical laptop ($TargetIP)"
} else {
    $secpasswd = ConvertTo-SecureString $Password -AsPlainText -Force
    $cred = New-Object System.Management.Automation.PSCredential ($Username, $secpasswd)
}

# The path where the script is located on the shared drive
$RemoteScriptPath = "\\crp-fs03\public\Executive_Reports\agent_app\sophos_vpn_automator.py"
$StateFile = "$env:USERPROFILE\OneDrive - astecindustries.com\VS Code\pipeline\bridge_state\wifi_ip.txt"
$DesktopIP = "172.16.4.76"

Write-Host "[*] Executing Sophos VPN Automator + Bridge Setup remotely on $TargetIP..."

# Execute VPN automator AND apply portproxy bridge in the same WinRM session
Invoke-Command -ComputerName $TargetIP -Credential $cred -ScriptBlock {
    param($ScriptPath, $DesktopIP, $StateFile)

    # 1. Run the VPN automator
    Write-Host "Running Python VPN script on physical host..."
    python $ScriptPath

    # 2. Apply kernel-level portproxy rules (netsh, no dependencies)
    Write-Host "[*] Applying portproxy bridge rules..."
    netsh interface portproxy delete v4tov4 listenport=33890 listenaddress=0.0.0.0 2>$null | Out-Null
    netsh interface portproxy add    v4tov4 listenport=33890 listenaddress=0.0.0.0 connectport=3389  connectaddress=$DesktopIP | Out-Null
    netsh interface portproxy delete v4tov4 listenport=14330 listenaddress=0.0.0.0 2>$null | Out-Null
    netsh interface portproxy add    v4tov4 listenport=14330 listenaddress=0.0.0.0 connectport=1433  connectaddress=$DesktopIP | Out-Null
    netsh interface portproxy delete v4tov4 listenport=8000  listenaddress=0.0.0.0 2>$null | Out-Null
    netsh interface portproxy add    v4tov4 listenport=8000  listenaddress=0.0.0.0 connectport=8000  connectaddress=$DesktopIP | Out-Null
    netsh advfirewall firewall delete rule name="AstecBridge" 2>$null | Out-Null
    netsh advfirewall firewall add rule name="AstecBridge" dir=in action=allow protocol=TCP localport=33890,14330,8000 | Out-Null
    Write-Host "[+] Portproxy rules applied."

    # 3. Discover home Wi-Fi IP and write it back to the shared OneDrive state file
    $wifi = Get-NetIPAddress -AddressFamily IPv4 |
        Where-Object { $_.InterfaceAlias -match 'Wi-Fi' -and $_.IPAddress -notmatch '^169\.' -and $_.IPAddress -ne '192.168.250.200' } |
        Select-Object -First 1
    if ($wifi) {
        $stateDir = Split-Path $StateFile
        if (-not (Test-Path $stateDir)) { New-Item -ItemType Directory -Force -Path $stateDir | Out-Null }
        $wifi.IPAddress | Out-File -FilePath $StateFile -Encoding ascii -Force
        Write-Host "[+] Bridge active. Gaming PC connects to: $($wifi.IPAddress):33890"
    } else {
        Write-Host "[!] Could not detect home Wi-Fi IP."
    }

} -ArgumentList $RemoteScriptPath, $DesktopIP, $StateFile

Write-Host "[+] Remote VPN + Bridge execution finished."

