import subprocess
import logging
import sys
import os

sys.path.insert(0, r"c:\Users\agard\OneDrive - astecindustries.com\VS Code\pipeline")
from src.connections.secrets import get_credentials

logging.basicConfig(level=logging.INFO, format="[*] %(message)s")

def get_cached_password():
    auth_cache = get_credentials('oracle_fusion') or get_credentials('azure_sql')
    if not auth_cache:
        return None
    return auth_cache.get("password")

def run_bridge_deploy(pwd):
    ps_script = """
    param($Password)
    $Username = "agard@astecindustries.com"
    $TargetIP = "ROADL-4GVKFW3.roadtec.astec.local"

    $secpasswd = ConvertTo-SecureString $Password -AsPlainText -Force
    $cred = New-Object System.Management.Automation.PSCredential ($Username, $secpasswd)

    Write-Host "Connecting to Laptop ($TargetIP) via WinRM..."
    Invoke-Command -ComputerName $TargetIP -Credential $cred -ScriptBlock {
        Write-Host "Successfully connected to ROADL-4GVKFW3!"
        
        # 1. Get Home Wi-Fi IP (Excluding the VPN IP, Loopback, and APIPA)
        $wifi = Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.IPAddress -notmatch "^169\." -and $_.IPAddress -notmatch "^127\." -and $_.IPAddress -ne "192.168.250.200" -and $_.IPAddress -notmatch "^172\." } | Select-Object -First 1
        
        $home_ip = $wifi.IPAddress
        Write-Host "--------------------------------------------------------"
        Write-Host ">>> HOME WI-FI IP DETECTED: $home_ip <<<"
        Write-Host "--------------------------------------------------------"
        
        # 2. Make a local temp directory and copy the script so it doesn't fail over double-hop UNC paths
        $LocalDir = "C:\AstecBridge"
        if (-not (Test-Path $LocalDir)) { New-Item -ItemType Directory -Force -Path $LocalDir | Out-Null }
        
        $RemoteScript = "\\crp-fs03\public\Executive_Reports\agent_app\piggyback_router.py"
        $LocalScript = "$LocalDir\piggyback_router.py"
        
        Copy-Item -Path $RemoteScript -Destination $LocalScript -Force -ErrorAction SilentlyContinue
        
        # Check if the copy worked, if not, write the script manually to avoid double-hop issues
        if (-not (Test-Path $LocalScript)) {
            Write-Host "UNC double-hop blocked the copy. Creating local copy via raw text..."
            $RawScript = Get-Content -Path $RemoteScript -Raw
            Set-Content -Path $LocalScript -Value $RawScript
        }

        # 3. Stop any existing background instances (be careful not to kill other python tasks if not needed, 
        # but here we assume this is the main python task for the bridge)
        # Get-Process -Name "python" -ErrorAction SilentlyContinue | Stop-Process -Force
        
        # 4. Launch the piggyback router silently
        Write-Host "Starting Piggyback Router locally on Laptop in the background..."
        $DesktopIP = "172.16.4.76"
        Start-Process -FilePath "python" -ArgumentList "$LocalScript --mode laptop-rdp-gateway --desktop-ip $DesktopIP" -WindowStyle Hidden
        
        Write-Host "Success! The Laptop ($home_ip) is now acting as the gateway port forwarding 33890 -> $DesktopIP"
        Write-Host "========================================================"
        Write-Host "GAMING PC INSTRUCTIONS:"
        Write-Host "Open Remote Desktop Connection on your Gaming PC at home."
        Write-Host "Connect to: $($home_ip):33890"
        Write-Host "========================================================"
    }
    """
    
    ps_file = os.path.join(os.path.dirname(__file__), "deploy_bridge.ps1")
    with open(ps_file, "w") as f:
        f.write(ps_script)
        
    cmd = ["powershell.exe", "-ExecutionPolicy", "Bypass", "-File", ps_file, "-Password", pwd]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print("\n--- DEPLOYMENT RESULTS ---")
    print(result.stdout)
    if result.stderr:
        print("Errors/Warnings:", result.stderr)
        
    # Cleanup 
    if os.path.exists(ps_file):
        os.remove(ps_file)

if __name__ == "__main__":
    pwd = get_cached_password()
    if pwd:
        run_bridge_deploy(pwd)
    else:
        logging.error("Could not retrieve password from DPAPI vault.")
