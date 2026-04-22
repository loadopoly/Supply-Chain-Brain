$NetShare = New-Object -ComObject HNetCfg.HNetShare
$Connections = $NetShare.EnumEveryConnection

Write-Host "Looking for VPN and Wi-Fi adapters..."

$PublicConnection = $null
$PrivateConnection = $null

foreach ($Item in $Connections) {
    $Props = $NetShare.NetConnectionProps($Item)
    if ($Props.Name -match "Sophos" -or $Props.Name -match "VPN") {
        $PublicConnection = $Item
        Write-Host "Found Public/VPN Adapter: " $Props.Name
    }
    if ($Props.Name -match "Wi-Fi" -or $Props.Name -match "Ethernet") {
        $PrivateConnection = $Item
        Write-Host "Found Private/LAN Adapter: " $Props.Name
    }
}

if ($PublicConnection -and $PrivateConnection) {
    Try {
        $PublicConfig = $NetShare.INetSharingConfigurationForINetConnection($PublicConnection)
        $PrivateConfig = $NetShare.INetSharingConfigurationForINetConnection($PrivateConnection)
        
        Write-Host "Enabling sharing..."
        $PublicConfig.EnableSharing(0)  # 0 indicates public/internet connection
        $PrivateConfig.EnableSharing(1) # 1 indicates private/local network
        
        Write-Host "ICS successfully enabled via COM automation!"
    } Catch {
        Write-Host "Failed to automate ICS. You must run this as Administrator."
        Write-Host $_.Exception.Message
    }
} else {
    Write-Host "Could not automatically identify both the VPN and Wi-Fi adapters."
}
Pause
