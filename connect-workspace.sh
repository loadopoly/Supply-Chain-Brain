#!/bin/bash
# Description: Quick connect script for Loadopoly-OCR workspace

TUNNEL_NAME="roadd-5wd1nh3"
WORKSPACE_PATH="/c:/Users/agard/OneDrive%20-%20astecindustries.com/VS%20Code"

echo "Select connection method:"
echo "1) Open in Browser (vscode.dev)"
echo "2) Open in VS Code Desktop"
echo "3) Connect via SSH"
read -p "Choice (1/2/3): " choice

case $choice in
  1)
    echo "Opening in browser..."
    # Linux / Mac / Windows browser open
    if command -v xdg-open > /dev/null; then xdg-open "https://vscode.dev/tunnel/$TUNNEL_NAME$WORKSPACE_PATH"; 
    elif command -v open > /dev/null; then open "https://vscode.dev/tunnel/$TUNNEL_NAME$WORKSPACE_PATH"; 
    else start "https://vscode.dev/tunnel/$TUNNEL_NAME$WORKSPACE_PATH"; fi
    ;;
  2)
    echo "Opening in VS Code Desktop..."
    code --folder-uri "vscode-remote://tunnel+$TUNNEL_NAME$WORKSPACE_PATH"
    ;;
  3)
    echo "Connecting via SSH..."
    code tunnel ssh "$TUNNEL_NAME"
    ;;
  *)
    echo "Invalid choice."
    ;;
esac