$TunnelName = "roadd-5wd1nh3"
$WorkspacePath = "/c:/Users/agard/OneDrive%20-%20astecindustries.com/VS%20Code"

Write-Host "Select connection method:"
Write-Host "1) Open in Browser (vscode.dev)"
Write-Host "2) Open in VS Code Desktop"
Write-Host "3) Connect via SSH"
$choice = Read-Host "Choice (1/2/3)"

switch ($choice) {
    "1" {
        Write-Host "Opening in browser..."
        Start-Process "https://vscode.dev/tunnel/$TunnelName$WorkspacePath"
    }
    "2" {
        Write-Host "Opening in VS Code Desktop..."
        code --folder-uri "vscode-remote://tunnel+$TunnelName$WorkspacePath"
    }
    "3" {
        Write-Host "Connecting via SSH..."
        code tunnel ssh $TunnelName
    }
    default {
        Write-Host "Invalid choice."
    }
}