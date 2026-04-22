Write-Host 'Enabling Internet Connection Sharing (ICS)...'
# Note: ICS automation via COM objects requires Elevation
if (-Not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Warning 'Please run this script as Administrator!'
    Pause
    Exit
}
Write-Host 'Due to VPN driver restrictions, ICS is best enabled via the Network Connections GUI (ncpa.cpl).'
Write-Host 'Opening Network Connections for you now...'
Start-Process ncpa.cpl
