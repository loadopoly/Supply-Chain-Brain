import os
import time
import sys

def check_agent_health():
    heartbeat_file = os.path.join("logs", "agent_heartbeat.txt")
    if not os.path.exists(heartbeat_file):
        print("FAIL: Heartbeat file not found. Agent might not be running.", file=sys.stderr)
        sys.exit(1)
    
    with open(heartbeat_file, "r") as f:
        timestamp_str = f.read().strip()
    
    try:
        last_heartbeat = float(timestamp_str)
    except ValueError:
        print("FAIL: Invalid heartbeat data inside the file.", file=sys.stderr)
        sys.exit(1)
        
    current_time = time.time()
    time_diff = current_time - last_heartbeat
    
    # Check if the heartbeat is older than a 5 minute margin
    if time_diff > 300:
        print(f"FAIL: Agent is dead/hung. Last heartbeat was {time_diff:.2f} seconds ago.", file=sys.stderr)
        sys.exit(1)
        
    print(f"PASS: Agent is healthy. Last heartbeat was {time_diff:.2f} seconds ago.")
    sys.exit(0)

if __name__ == "__main__":
    check_agent_health()
