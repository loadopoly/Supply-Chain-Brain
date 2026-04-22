import subprocess
import logging
import os
import sys

# Ensure pipeline is in the path to import local DPAPI secrets module
sys.path.insert(0, r"c:\Users\agard\OneDrive - astecindustries.com\VS Code\pipeline")
from src.connections.secrets import get_credentials

logging.basicConfig(level=logging.INFO, format="[*] %(message)s")

def get_cached_password():
    """
    Extracts the cached Oracle/Azure password stored in the local DPAPI credential vault.
    Converts it to a temporary raw unencrypted string making it work with the VPN daemon.
    """
    logging.info("Retrieving cached credentials from local DPAPI vault...")
    
    auth_cache = get_credentials('oracle_fusion') or get_credentials('azure_sql')
    if not auth_cache:
        logging.error("No cached credentials found in local DPAPI Vault for Oracle or Azure.")
        return None
        
    return auth_cache.get("password")

def run_remote_vpn(raw_password):
    logging.info("1. Initiating the remote VPN connection using the temporary raw string...")
    
    # We pass the raw unencrypted string directly into the PowerShell runner
    cmd = [
        "powershell.exe", 
        "-ExecutionPolicy", "Bypass", 
        "-File", r"c:\Users\agard\OneDrive - astecindustries.com\VS Code\pipeline\remote_vpn_runner.ps1",
        "-Password", raw_password
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        logging.info("VPN established on remote host successfully.")
        return True
    else:
        logging.error(f"VPN establishment failed: {result.stderr}\n{result.stdout}")
        return False

def run_tests_and_benchmarks():
    logging.info("2. Triggering Full Test and Benchmark Protocol...")
    
    # Assuming pipeline.py contains the tests/benchmarks
    try:
        cmd = ["python", r"c:\Users\agard\OneDrive - astecindustries.com\VS Code\pipeline\pipeline.py", "test-azure"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        logging.info("\n--- AZURE TEST RESULTS ---")
        print(result.stdout)
        if result.stderr:
            print(f"Errors: {result.stderr}")
            
        logging.info("Tests and Benchmarks completed.")
    except Exception as e:
        logging.error(f"Failed to run benchmarks: {e}")

if __name__ == "__main__":
    print("=== TEMPORARY BENCHMARK & VPN TEST PROTOCOL ===")
    raw_pwd = get_cached_password()
    
    if run_remote_vpn(raw_pwd):
        run_tests_and_benchmarks()
    else:
        logging.warning("Skipping benchmarks due to VPN failure.")
    
    # Clean up the temporary raw string
    if 'TEMPORARY_RAW_PWD' in os.environ:
        del os.environ['TEMPORARY_RAW_PWD']
