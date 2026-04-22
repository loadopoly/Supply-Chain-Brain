import subprocess
import logging
import os
import getpass

# Configure logging
logging.basicConfig(level=logging.INFO, format="[*] %(message)s")

SOPHOS_CLI_PATH = r"C:\Program Files (x86)\Sophos\Connect\sccli.exe"

def connect_vpn_cli(connection_name, username, password):
    """
    Automates the Sophos Connect VPN connection using its native CLI for Basic Auth connections.
    """
    if not os.path.exists(SOPHOS_CLI_PATH):
        logging.error(f"Sophos Connect CLI not found at {SOPHOS_CLI_PATH}")
        return False

    logging.info(f"Attempting to connect to Sophos VPN profile: {connection_name} as {username}")
    
    # sccli requires exactly: sccli enable -n "connection_name" -u "username" -p "password"
    cmd = [SOPHOS_CLI_PATH, "enable", "-n", connection_name, "-u", username, "-p", password]
    
    try:
        # Run the command and capture output
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logging.info("VPN connection established successfully via CLI.")
            return True
        else:
            logging.error(f"Failed. Output: {result.stdout.strip()} | Err: {result.stderr.strip()}")
            return False
            
    except Exception as e:
        logging.error(f"Failed to connect to VPN via CLI. Error: {e}")
        return False

def check_status():
    """Checks the current status of Sophos Connect connections."""
    if not os.path.exists(SOPHOS_CLI_PATH):
        return
        
    try:
        result = subprocess.run([SOPHOS_CLI_PATH, "status"], capture_output=True, text=True)
        logging.info("Current VPN Status:")
        print(result.stdout)
    except Exception as e:
        logging.error(f"Failed to get status: {e}")

if __name__ == "__main__":
    import sys
    print("=== Sophos VPN Automator Test ===")
    
    # The connection profile name exactly as it appears in the Sophos GUI
    target_vpn = "vpn2.astecindustries.com"
    
    # Let's grab the username from env or default to your email
    default_user = os.environ.get("ORACLE_FUSION_USER", "agard@astecindustries.com")
    
    # Safely ask for the password so it doesn't appear on screen or in chat logs
    print(f"Targeting VPN Profile: {target_vpn}")
    print(f"Authenticating as: {default_user}")
    
    password = getpass.getpass(f"Enter password for {default_user}: ")
    
    if password:
        connect_vpn_cli(target_vpn, default_user, password)
        check_status()
    else:
        print("Password cannot be empty.")

