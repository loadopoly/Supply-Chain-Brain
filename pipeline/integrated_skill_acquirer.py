import os
import time
import json
import logging
from datetime import datetime
from pathlib import Path
import urllib.request
import urllib.error
import subprocess

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [SKILL ACQUIRER] - %(message)s',
    handlers=[
        logging.FileHandler('skill_acquirer.log'),
        logging.StreamHandler()
    ]
)

# Shared trigger state for immediate spin up by the Brain
PIPELINE_DIR = Path(os.environ.get("USERPROFILE", "")) / "OneDrive - astecindustries.com" / "VS Code" / "pipeline"
TRIGGER_DIR = PIPELINE_DIR / "bridge_triggers"
STATE_DIR = PIPELINE_DIR / "bridge_state"
TRIGGER_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

# The Brain's Local API (Accessible over the bridge proxy loopback port 8000)
BRAIN_API = "http://127.0.0.1:8000/api/skills/queue" 

# Minimum set schedule: 4 Hours
MIN_SCHEDULE_SECONDS = 3600 * 4 

def check_for_brain_triggers():
    """Opportunity for Spin Up by The Brain: Check if the Brain dropped a trigger file."""
    for trigger_file in TRIGGER_DIR.glob("acquire_*.trigger"):
        logging.info(f"Detected immediate skill acquisition spin-up from The Brain: {trigger_file.name}")
        try:
            with open(trigger_file, "r") as f:
                task_data = json.load(f)
            perform_skill_acquisition(task_data)
            trigger_file.unlink() # Cleanup after consumption
        except Exception as e:
            logging.error(f"Failed to process immediate trigger {trigger_file.name}: {e}")

def pull_scheduled_tasks_from_brain():
    """Poll the Brain's API over the bridge tunnel on the minimum set schedule."""
    logging.info("Checking The Brain for scheduled skill requirements...")
    try:
        # Reaches over the local port forward back to the Desktop's AI backend
        req = urllib.request.Request(BRAIN_API, method="GET")
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())
                if data.get("tasks"):
                    logging.info(f"The Brain scheduled {len(data['tasks'])} new skill acquisitions.")
                    for task in data["tasks"]:
                        perform_skill_acquisition(task)
                else:
                    logging.info("The Brain has no pending skill requests on its schedule.")
    except urllib.error.URLError as e:
        logging.warning(f"Could not reach The Brain API at {BRAIN_API} - is the bridge up? {e}")
    except Exception as e:
        logging.error(f"Scheduled task pull failed: {e}")

def perform_skill_acquisition(task):
    """Executes the internet-bound skill acquisition using the piggybacking bridge proxy."""
    skill_name = task.get("name", "Unknown")
    action = task.get("action", "")
    target = task.get("target", "")

    logging.info(f"--- Acquiring skill: {skill_name} ---")

    if action == "pip_install":
        logging.info(f"Running autonomous PIP install for {target}")
        env = os.environ.copy()
        # Enforce proxy routing on the acquirer process natively 
        env["HTTP_PROXY"] = "http://127.0.0.1:3128"
        env["HTTPS_PROXY"] = "http://127.0.0.1:3128"
        try:
            result = subprocess.run(["pip", "install", target], capture_output=True, text=True, env=env)
            logging.info(f"PIP output: {result.stdout}")
        except Exception as e:
            logging.error(f"PIP install failed: {e}")

    elif action == "web_scrape":
        logging.info(f"Scraping web documentation/knowledge from {target}")
        try:
            req = urllib.request.Request(target, headers={'User-Agent': 'Astec-Brain-Acquirer/1.0'})
            # Route scrape request over HTTP Proxy if configured, or direct if running on laptop
            proxy_handler = urllib.request.ProxyHandler({'http': 'http://127.0.0.1:3128', 'https': 'http://127.0.0.1:3128'})
            opener = urllib.request.build_opener(proxy_handler)
            html = opener.open(req, timeout=15).read().decode('utf-8')
            
            # Deliver back to the brain via OneDrive shared state
            out_file = STATE_DIR / f"acquired_knowledge_{skill_name.replace(' ', '_')}_{int(time.time())}.txt"
            with open(out_file, "w", encoding="utf-8") as f:
                f.write(html)
            logging.info(f"Pushed acquired knowledge back to the Brain via {out_file.name}")
        except Exception as e:
            logging.error(f"Failed to acquire web knowledge {target}: {e}")
    elif action == "shell_exec":
        logging.info(f"Executing shell command for skill: {skill_name}")
        try:
            result = subprocess.run(target, shell=True, capture_output=True, text=True, timeout=300)
            out_file = STATE_DIR / f"shell_result_{skill_name.replace(' ', '_')}_{int(time.time())}.txt"
            with open(out_file, "w", encoding="utf-8") as f:
                f.write(f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}")
            logging.info(f"Shell exec completed (rc={result.returncode}). Output: {out_file.name}")
        except Exception as e:
            logging.error(f"Shell exec failed for {skill_name}: {e}")

    else:
        logging.warning(f"Unknown skill action requested by The Brain: {action}")

def run_acquirer_loop():
    logging.info("Integrated Autonomous Agent: Skill Acquirer Online.")
    while True:
        try:
            # Loop 1: Full schedule sync block with the Brain
            pull_scheduled_tasks_from_brain()
            
            # Loop 2: Micro-interval (15 seconds) for immediate Brain spin-up
            intervals = int(MIN_SCHEDULE_SECONDS / 15)
            for _ in range(intervals):
                check_for_brain_triggers()
                time.sleep(15)
        except Exception as e:
            logging.error(f"Acquirer loop encountered an error: {e}. Retrying in 60s...")
            time.sleep(60)

if __name__ == "__main__":
    run_acquirer_loop()
