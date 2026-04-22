@echo off
echo [*] Enabling Native Windows Kernel Port Forwarding (Bypassing ICS & Python)...
echo [*] Routing RDP (3389)
netsh interface portproxy add v4tov4 listenport=33890 listenaddress=0.0.0.0 connectport=3389 connectaddress=172.16.4.76
echo [*] Routing SQL Server (1433)
netsh interface portproxy add v4tov4 listenport=14330 listenaddress=0.0.0.0 connectport=1433 connectaddress=172.16.4.76
echo [*] Routing LLM / Python APIs (8000)
netsh interface portproxy add v4tov4 listenport=8000 listenaddress=0.0.0.0 connectport=8000 connectaddress=172.16.4.76
echo.
echo [+] SUCCESS! Native OS bridges established.
echo [+] No Python, no background processes, no ICS NAT conflicts.
pause
