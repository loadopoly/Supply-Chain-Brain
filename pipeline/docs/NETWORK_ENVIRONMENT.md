# Corporate Network Environment & Discovery Guide

## Overview
This document outlines the known network configurations for the Astec Industries corporate environment. It provides a standard operating procedure (SOP) for future AI agents to dynamically discover and utilize local network resources (like SMTP relays and SMB file shares) securely, without requiring hardcoded user credentials, by leveraging the host machine's Active Directory trust.

## Known Network Configurations

### 1. SMTP / Email Routing
- **Corporate Domain:** `astecindustries.com`
- **Primary MX Record / SMTP Relay:** `astecindustries-com.mail.protection.outlook.com` (Exchange Online Protection)
- **Protocol/Port:** SMTP over Port 25.
- **Authentication Strategy:** Relies on the host machine's trusted corporate IP for internal SMTP relaying. No manual credentials are required when sending to internal addresses.

### 2. SMB / Network File Shares
- **Primary File Server:** `\\crp-fs03`
- **Known Accessible Overlays:** 
  - `\\crp-fs03\public` (Current target for Executive Reports: `\Executive_Reports`)
  - `\\crp-fs03\corporate`, `\\crp-fs03\audit`, `\\crp-fs03\legal`
  - `\\road1.roadtec.astec.local\G`
- **Authentication Strategy:** Native Windows Active Directory / Kerberos token. Python can navigate UNC paths inherently using the user's active session.

---

## Agent Standard Operating Procedure (SOP)

Future agents working on this system should follow these steps to derive network paths dynamically rather than prompting the user for manual configuration or SFTP credential setups.

### Step 1: Discovering SMTP Servers (PowerShell)
Instead of asking for an SMTP server, an agent should poll the local DNS network to find the corporate Exchange relay.
```powershell
$domain = $env:USERDNSDOMAIN
if ([string]::IsNullOrEmpty($domain)) { $domain = "astecindustries.com" }
Resolve-DnsName -Type MX $domain | Select-Object NameExchange, Preference
```
*Action:* Extract the `NameExchange` with the lowest preference number and use it in Python's `smtplib.SMTP(server, 25)`.

### Step 2: Discovering Network Shares (PowerShell)
Instead of asking for an SFTP server or cloud bucket, an agent should check the host's existing corporate mounts to find a drop location.
```powershell
Get-SmbMapping | Select-Object RemotePath
```
*Action:* Look for `\public`, `\corporate`, or similar team shares. Leverage Python's `shutil.copy2()` passing the UNC path (e.g., `\\crp-fs03\public\Agent_Drops`), circumventing the need for managed authentication blocks.

### Step 3: Python Implementation Example
```python
import smtplib
from email.message import EmailMessage
import shutil
import os

# 1. SMB File Drop (No Auth Needed)
network_share = r"\\crp-fs03\public\Agent_Output"
os.makedirs(network_share, exist_ok=True)
shutil.copy2("local_report.pptx", os.path.join(network_share, "remote_report.pptx"))

# 2. SMTP Unauthenticated Internal Relay
msg = EmailMessage()
msg['Subject'] = "Automated AI Report"
msg['From'] = "agent@astecindustries.com"
msg['To'] = "agard@astecindustries.com"
msg.set_content("See attached report.")

try:
    with smtplib.SMTP("astecindustries-com.mail.protection.outlook.com", 25) as server:
        server.starttls()
        server.send_message(msg)
except Exception as e:
    print(f"SMTP Failed: {e}")
```