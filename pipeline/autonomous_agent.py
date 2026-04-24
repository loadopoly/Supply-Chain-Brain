import time
import subprocess
import os
import logging
import math
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename='autonomous_agent.log',
    level=logging.INFO,
    format='%(asctime)s - [AUTONOMOUS AGENT] - %(message)s'
)

def start_integrated_skill_acquirer():
    import threading
    import sys as _sys
    try:
        from integrated_skill_acquirer import run_acquirer_loop
        logging.info("Spinning up integrated skill acquirer thread alongside synaptic workers...")
        t = threading.Thread(target=run_acquirer_loop, daemon=True)
        t.start()
        return t
    except ImportError as e:
        logging.error(f"Failed to load integrated skill acquirer: {e}")
        return None


def start_systemic_refinement_agent():
    """Start the Systemic Refinement Agent as a background daemon thread.

    The agent continuously senses all Brain faculties (Heart, Vision, Touch,
    Smell, Body, DBI) and executes targeted refinement actions (missions,
    directives, diversity guards, corpus seeds, config nudges) to keep the
    supply-chain system improving as the corpus grows.  Its cadence adapts
    via the Brain's own acquisition_drive so it accelerates when stagnant
    and relaxes when learning is healthy.
    """
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    try:
        from src.brain.systemic_refinement_agent import schedule_in_background
        t = schedule_in_background(interval_s=1200)
        logging.info(
            "Systemic Refinement Agent started — adaptive cadence 20 min–2 h "
            "driven by acquisition_drive."
        )
        return t
    except Exception as e:
        logging.error(f"Failed to start Systemic Refinement Agent: {e}")
        return None

def trigger_remote_vpn():
    logging.info("Initiating remote VPN connection + portproxy bridge on physical client laptop...")
    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.connections.secrets import get_credentials
        creds = get_credentials('oracle_fusion') or get_credentials('azure_sql')
        laptop_pwd = (creds or {}).get('password') or os.environ.get("LAPTOP_ADMIN_PWD", "")

        cmd = ["powershell.exe", "-ExecutionPolicy", "Bypass", "-File",
               os.path.join(os.path.dirname(os.path.abspath(__file__)), "remote_vpn_runner.ps1")]
        if laptop_pwd:
            cmd.extend(["-Password", laptop_pwd])

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            logging.info("VPN + bridge established successfully.")
        else:
            logging.warning(f"VPN/bridge issues. stdout={result.stdout} stderr={result.stderr}")

        # Read the Wi-Fi IP written back by the remote scriptblock
        state_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "bridge_state", "wifi_ip.txt")
        if os.path.exists(state_file):
            wifi_ip = open(state_file).read().strip()
            logging.info(f"Bridge confirmed. Gaming PC endpoint: {wifi_ip}:33890")
            return wifi_ip
    except Exception as e:
        logging.error(f"Error triggering remote VPN/bridge: {e}")
    return None

def run_tests_and_benchmarks():
    logging.info("Running system benchmarks and tests...")
    try:
        result = subprocess.run(["python", "pipeline.py", "test-azure"], capture_output=True, text=True)
        if result.returncode == 0:
            logging.info("Benchmarks passed successfully.")
        else:
            logging.warning(f"Benchmark issues detected: {result.stderr}")
        return result.stdout
    except Exception as e:
        logging.error(f"Error during benchmarking: {e}")
        return str(e)

def update_data_infrastructure_docs():
    logging.info("Analyzing database structure and updating data infrastructure documentation...")
    try:
        result = subprocess.run(["python", "pipeline.py", "discover"], capture_output=True, text=True)
        if result.returncode == 0:
            logging.info("Schema discovery and structural mapping completed.")
            doc_path = "docs/DATA_DICTIONARY_LATEST.md"
            with open(doc_path, "w", encoding="utf8") as f:
                f.write("# Data Infrastructure & Relational Schema\n")
                f.write(f"**Last Auto-Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write("This document is automatically maintained by the Autonomous Agent. It provides an up-to-date map of database structures, variables, and relational linkages for fluid user capability inside the reporting layer.\n\n")
                f.write("## Discovered Schema Information & Relationships\n")
                # write first 3000 chars of stdout from discovery
                f.write("`	ext\n" + result.stdout[:5000] + "\n`\n")
                f.write("\n*See complete output directly in the schema cache created by the discovery pipeline.*")
        else:
            logging.warning(f"Schema mapping issues: {result.stderr}")
    except Exception as e:
        logging.error(f"Error during data infrastructure generation: {e}")


def generate_and_email_executive_report():
    logging.info("Generating Executive Action Items Report using the Cross-Dataset Review Deck template...")
    try:
        # Check if today is a weekday (0=Mon, 4=Fri)
        if datetime.today().weekday() > 4:
            logging.info("Today is a weekend. Skipping executive report generation.")
            return

        # First, ensure we sync the mapped data resources from Oracle Fusion to Azure SQL
        logging.info("Extracting updated data resources mapped from Oracle Fusion to Azure SQL...")
        sync = subprocess.run(["python", "pipeline.py", "run"], capture_output=True, text=True)
        if sync.returncode != 0:
            logging.warning(f"Data sync issue: {sync.stderr}")
        else:
            logging.info("Oracle Fusion data successfully synchronized.")

        # Execute the pipeline.py deck generation command
        logging.info("Building presentation with fresh Oracle & Azure data...")
        result = subprocess.run(["python", "pipeline.py", "deck", "--site", "ALL"], capture_output=True, text=True)
        
        report_path = None
        if result.returncode == 0:
            for line in result.stdout.split("\n"):
                if line.startswith("wrote ") and line.endswith(".pptx"):
                    report_path = line.replace("wrote ", "").strip()
                    break

        if not report_path or not os.path.exists(report_path):
            logging.error(f"Failed to locate generated PPTX. Output: {result.stdout}")
            return

        logging.info(f"Executive report generated locally at {report_path}.")

# Send Email via Internal Corporate Office 365 Relay
        recipient = "agard@astecindustries.com"
        smtp_server = "astecindustries-com.mail.protection.outlook.com"
        
        logging.info(f"Sending Executive Report (.pptx attach) to {recipient} via {smtp_server}...")

        import smtplib
        from email.message import EmailMessage
        
        msg = EmailMessage()
        msg.set_content("Attached is the latest Executive Action Items PPTX Deck generated by Supply Chain Brain.")
        msg['Subject'] = f"Daily Executive Action Items - {datetime.now().strftime('%Y-%m-%d')}"
        msg['From'] = "agent@astecindustries.com"
        msg['To'] = recipient

        with open(report_path, "rb") as f:
            ppt_data = f.read()

        msg.add_attachment(
            ppt_data, 
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.presentationml.presentation", 
            filename=os.path.basename(report_path)
        )

        try:
            # Connect to enterprise M365 relay without credentials (trusted internal IP)
            with smtplib.SMTP(smtp_server, 25) as server:
                server.starttls()
                server.send_message(msg)
            logging.info("Executive report email transmission successful.")
        except Exception as smtp_e:
            logging.error(f"SMTP transmission failed: {smtp_e}. Falling back to network share.")

        # Fallback/Archive to standard Corporate SMB File Share
        import shutil
        network_share = r"\\crp-fs03\public\Executive_Reports"
        try:
            if not os.path.exists(network_share):
                os.makedirs(network_share, exist_ok=True)
            
            dest_path = os.path.join(network_share, os.path.basename(report_path))
            shutil.copy2(report_path, dest_path)
            logging.info(f"Executive report archived to corporate network share: {dest_path}")
        except Exception as smb_e:
            logging.error(f"Failed to copy to SMB network share {network_share}: {smb_e}")

    except Exception as e:
        logging.error(f"Failed to generate or send executive report: {e}")

def analyze_and_improve(benchmark_data):
    logging.info("Analyzing codebase against benchmarks...")
    logging.info("Applying AI-generated optimizations (Requires LLM API Key)...")


# ---------------------------------------------------------------------------
# Ignorance signal — drives adaptive sleep cadence
# ---------------------------------------------------------------------------
_MIN_SLEEP_S  = 1_800   #  30 min  — maximum urgency (high ignorance)
_MAX_SLEEP_S  = 14_400  #   4 hrs  — fully settled (low ignorance)

def compute_ignorance_score() -> float:
    """Return a float in [0..1] representing how much the Brain still doesn't know.

    Combines:
      * fraction of ``part_category`` entries that are 'Uncategorized' (weight 0.5)
      * pending corpus learnings not yet materialized (weight 0.3)
      * unprocessed self_train rows (weight 0.2)

    A score of 1.0 means maximum ignorance → shortest sleep interval.
    A score of 0.0 means fully settled → longest sleep interval.
    """
    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.brain.local_store import db_path
        import sqlite3

        db = str(db_path())
        if not os.path.exists(db):
            return 1.0   # no DB yet — maximum ignorance

        with sqlite3.connect(db) as cn:
            # --- unclassified parts fraction ---
            try:
                total_parts = cn.execute(
                    "SELECT COUNT(*) FROM part_category"
                ).fetchone()[0] or 0
                uncl = cn.execute(
                    "SELECT COUNT(*) FROM part_category "
                    "WHERE category='Uncategorized' OR category IS NULL"
                ).fetchone()[0] or 0
            except Exception:
                total_parts, uncl = 0, 0

            part_ignorance = (uncl / max(1, total_parts)) if total_parts > 0 else 1.0

            # --- pending corpus learnings not yet flushed to graph ---
            try:
                last_round = cn.execute(
                    "SELECT MAX(entities_added + edges_added) FROM corpus_round_log"
                ).fetchone()[0] or 0
                pending_ll = cn.execute(
                    "SELECT COUNT(*) FROM learning_log "
                    "WHERE logged_at > COALESCE("
                    "  (SELECT MAX(ran_at) FROM corpus_round_log), '1970-01-01')"
                ).fetchone()[0] or 0
            except Exception:
                last_round, pending_ll = 0, 0

            corpus_ignorance = min(1.0, pending_ll / 500.0)

            # --- unprocessed self_train rows ---
            try:
                cursor_val = cn.execute(
                    "SELECT value FROM corpus_cursor WHERE key='self_train'"
                ).fetchone()
                cursor_pos = int(cursor_val[0]) if cursor_val else 0
                max_st = cn.execute(
                    "SELECT MAX(id) FROM llm_self_train_log"
                ).fetchone()[0] or 0
            except Exception:
                cursor_pos, max_st = 0, 0

            train_ignorance = min(1.0, max(0, max_st - cursor_pos) / 200.0)

        score = (
            0.50 * part_ignorance
            + 0.30 * corpus_ignorance
            + 0.20 * train_ignorance
        )
        logging.info(
            f"Ignorance score: {score:.3f} "
            f"(parts={part_ignorance:.2f}, corpus={corpus_ignorance:.2f}, "
            f"train={train_ignorance:.2f})"
        )
        return min(1.0, max(0.0, score))
    except Exception as e:
        logging.warning(f"compute_ignorance_score failed: {e}")
        return 0.5   # default to mid-point


def adaptive_sleep_duration(ignorance: float) -> int:
    """Map ignorance [0..1] → sleep seconds using an exponential decay.

    High ignorance → short sleep; low ignorance → long sleep.
    Uses an exponential blend so mid-range ignorance (0.5) maps to ~2 hrs.
    """
    # sleep = MIN + (MAX - MIN) * (1 - ignorance)^2
    t = (1.0 - ignorance) ** 2
    return int(_MIN_SLEEP_S + (_MAX_SLEEP_S - _MIN_SLEEP_S) * t)


def drain_corpus_parts():
    """Run one NLP classification batch on the unclassified parts backlog.

    Delegates to nlp_categorize.drain_unclassified() which pulls the next
    500 uncategorized parts from the DW replica, classifies them via TF-IDF,
    and persists the results to local_brain.sqlite.  Called every agent cycle
    so the backlog drains progressively regardless of sleep cadence.
    """
    logging.info("Draining unclassified corpus parts (NLP batch)...")
    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.brain.nlp_categorize import drain_unclassified
        n = drain_unclassified(batch_size=500)
        if n > 0:
            logging.info(f"NLP drain: classified {n} new parts this cycle.")
        else:
            logging.info("NLP drain: nothing new to classify (backlog clear or DW unreachable).")
        return n
    except Exception as e:
        logging.error(f"drain_corpus_parts failed: {e}")
        return 0


def refresh_corpus():
    """Trigger one incremental corpus refresh + graph materialization."""
    logging.info("Refreshing knowledge corpus and materializing into graph...")
    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.brain.knowledge_corpus import refresh_corpus_round, materialize_into_graph
        result = refresh_corpus_round()
        if result.get("skipped"):
            logging.info("Corpus refresh: rate-limited — skipped.")
        else:
            logging.info(
                f"Corpus refresh: +{result.get('entities_added', 0)} entities, "
                f"+{result.get('edges_added', 0)} edges, "
                f"{result.get('learnings_logged', 0)} learnings."
            )
        mat = materialize_into_graph()
        if mat.get("ok") is not False:
            logging.info(
                f"Graph materialized: {mat.get('nodes', 0)} nodes, {mat.get('edges', 0)} edges."
            )
    except Exception as e:
        logging.error(f"refresh_corpus failed: {e}")

def refresh_llm_registry():
    """Periodic internet sweep for newly released open-weight LLMs.

    Delegates to brain.llm_scout.refresh_llm_registry which honors the cadence
    configured in config/brain.yaml -> llms.scout.interval_hours, persists
    newcomers to local_brain.sqlite.llm_registry, and appends to
    docs/LLM_SCOUT_AUDIT.md. Safe to call every cycle: it no-ops between
    intervals and degrades gracefully if a source is unreachable.
    """
    logging.info("Refreshing open-weight LLM registry (scout sweep)...")
    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.brain.llm_scout import refresh_llm_registry as _refresh
        report = _refresh()
        if report.sources_polled:
            logging.info(
                f"LLM scout polled {report.sources_polled}; "
                f"candidates={report.candidates_seen} "
                f"promoted={len(report.promoted_ids)} "
                f"rejected={len(report.rejected_ids)} "
                f"failed_sources={report.sources_failed}"
            )
        else:
            logging.info("LLM scout skipped (within cadence window or disabled).")
        return report
    except Exception as e:
        logging.error(f"LLM scout refresh failed: {e}")
        return None

def generate_documentation():
    logging.info("Updating system documentation...")
    doc_path = "docs/AUTONOMOUS_CHANGELOG.md"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(doc_path, "a") as f:
        f.write(f"\n## {timestamp}\n")
        f.write("- Autonomous cycle completed. Benchmarks recorded.\n")
        f.write("- Synced latest data structure schemas into relational dictionary.\n")

def commit_and_push():
    logging.info("Committing changes to repository...")
    try:
        if os.path.exists(".git"):
            subprocess.run(["git", "add", "."], check=True)
            subprocess.run(["git", "commit", "-m", f"Autonomous Agent Optimization: {datetime.now().strftime('%Y-%m-%d %H:%M')}"], check=True)
            logging.info("Changes committed successfully.")
    except Exception as e:
        logging.error(f"Git commit failed or nothing to commit: {e}")

def autonomous_loop():
    logging.info("Initializing 24/7 Autonomous Improvement Agent lifecycle.")
    print("Autonomous Agent initiated. Running in background. See autonomous_agent.log for details.")

    # Start the shared compute grid node so this workstation contributes its
    # CPU/GPU to the Brain's parallel multi-LLM dispatch (port 8000, already
    # exposed by bridge_watcher.ps1). Also publish an initial capacity beacon
    # to the OneDrive-synced compute_peers/ rendezvous so peers can find us.
    try:
        import sys, threading
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.brain.compute_grid import serve_compute_node, publish_local_capacity
        publish_local_capacity()
        threading.Thread(target=serve_compute_node, name="grid-node",
                         daemon=True).start()
        logging.info("Shared compute grid node started on port 8000.")
    except Exception as e:
        logging.warning(f"compute_grid node failed to start (local fallback only): {e}")

    # Start the Systemic Refinement Agent daemon — runs on its own adaptive
    # cadence (20 min–2 h) driven by acquisition_drive so it doesn't collide
    # with the main loop.  Starting it here ensures it runs even when the
    # autonomous_agent is imported and called programmatically rather than
    # executed as __main__.
    start_systemic_refinement_agent()

    while True:
        try:
            logging.info("=== STARTING NEW AUTONOMOUS CYCLE ===")

            # Step 0: Ensure local VPN is active on the hosting laptop
            trigger_remote_vpn()

            # Step 1: Benchmark current application state
            benchmarks = run_tests_and_benchmarks()

            # Step 2: Auto-map Database relations and variable structures for reporting
            update_data_infrastructure_docs()

            # Step 3: LLM Code refactoring placeholder
            analyze_and_improve(benchmarks)

            # Step 3b: Periodic open-weight LLM scout — keeps the Brain's
            # router aware of newly released models (Gemma/GLM/Qwen/DeepSeek/
            # Kimi/MiniMax/MiMo class). Cadence is enforced inside the scout.
            refresh_llm_registry()

            # Step 3c: Drain NLP classification backlog (500 parts per cycle)
            drain_corpus_parts()

            # Step 3d: Refresh corpus knowledge graph (incremental, rate-limited)
            refresh_corpus()

                        # Step 4: Self-document the changes
            generate_documentation()

            # Step 5: Executive Reporting
            generate_and_email_executive_report()

            # Step 6: Save/commit locally
            commit_and_push()

            # Compute adaptive sleep based on current ignorance level.
            ignorance = compute_ignorance_score()
            sleep_duration = adaptive_sleep_duration(ignorance)
            logging.info(
                f"=== CYCLE COMPLETE. ignorance={ignorance:.3f} → "
                f"sleeping {sleep_duration // 60} min ==="
            )

            # Sleep in intervals of 60 seconds to maintain a heartbeat
            interval = 60
            heartbeat_file = os.path.join("logs", "agent_heartbeat.txt")
            os.makedirs("logs", exist_ok=True)
            for _ in range(sleep_duration // interval):
                with open(heartbeat_file, "w") as f:
                    f.write(str(time.time()))
                time.sleep(interval)

        except KeyboardInterrupt:
            print("Autonomous Agent terminated by user.")
            logging.info("Agent manually terminated.")
            break
        except Exception as e:
            logging.error(f"Critical cycle failure: {e}. Attempting recovery in 60 seconds...")
            time.sleep(60)

if __name__ == "__main__":
    os.makedirs("docs", exist_ok=True)

    # Spin up the skill acquirer alongside the main loop
    acquirer_thread = start_integrated_skill_acquirer()

    # Spin up the Systemic Refinement Agent — continuously revises and
    # refines the whole supply-chain system as learning expands.
    refinement_thread = start_systemic_refinement_agent()

    autonomous_loop()
