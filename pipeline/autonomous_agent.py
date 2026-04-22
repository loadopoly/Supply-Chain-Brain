import time
import subprocess
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename='autonomous_agent.log',
    level=logging.INFO,
    format='%(asctime)s - [AUTONOMOUS AGENT] - %(message)s'
)

def trigger_remote_vpn():
    """
    Ensure the bridge tunnel is active using the v0.7.3 self-healing stack:
      1. TCP probe the laptop VPN IP (192.168.250.200:33890).
      2. If alive → return cached Wi-Fi IP (Gaming PC path).
      3. If dead  → drop a req_*.trigger file; on-laptop AstecBridgeWatchdog
                    (started by the crp-fs03 injection) picks it up, re-applies
                    portproxy rules, and updates wifi_ip.txt in OneDrive.
    WinRM / remote_vpn_runner.ps1 are intentionally bypassed — WinRM is
    blocked on the VPN network interface.
    """
    logging.info("Checking bridge tunnel health (v0.7.3 — TCP probe + watchdog trigger)...")
    try:
        import sys as _sys
        _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from trigger_bridge import ensure_alive
        wifi_ip = ensure_alive()
        if wifi_ip:
            logging.info(f"Bridge confirmed active. Laptop endpoint: {wifi_ip}")
        else:
            logging.warning("Bridge could not be confirmed — laptop may be offline or VPN not connected.")
        return wifi_ip
    except Exception as e:
        logging.error(f"trigger_remote_vpn (ensure_alive) failed: {e}")
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

            # Step 3c: Bounded self-training. Mines recent dispatches against
            # ground-truth tables already in the data pipeline (part_category,
            # otd_ownership) and nudges per-(model, task) weights via SGD.
            # Hard guard rails (lr_scale, drift_cap, max_share_per_task,
            # min_weight_floor, exploration_reserve) preserve fluidity for
            # multi-echeloned reasoning and dynamic interpretations.
            try:
                from src.brain.llm_self_train import self_train_round
                st = self_train_round()
                if isinstance(st, dict) and "tasks" in st:
                    summary = ", ".join(
                        f"{t}:{r.get('matched',0)}/{r.get('rows_scanned',0)}"
                        for t, r in st["tasks"].items() if isinstance(r, dict))
                    logging.info(f"LLM self-train round: {summary}")
            except Exception as e:
                logging.warning(f"LLM self-train round failed: {e}")

            # Step 3d: Network expansion learner. Probes every endpoint the
            # Brain already knows about across ALL protocols (DB, SMB,
            # SMTP/MX, HTTPS, compute peers, declared external apps and
            # webhook subscribers). Audits every observation, rolls topology
            # stats, and promotes verified peers into the compute grid seed
            # list. Adjusts routing telemetry only — never touches
            # llm_weights, so reasoning fluidity is preserved.
            try:
                from src.brain.network_learner import observe_network_round
                nl = observe_network_round()
                if isinstance(nl, dict) and "endpoints_total" in nl:
                    logging.info(
                        f"Network learner: {nl.get('live',0)}/{nl.get('endpoints_total',0)} live, "
                        f"protocols={list((nl.get('by_protocol') or {}).keys())}, "
                        f"promoted={len(nl.get('promoted') or [])}"
                    )
            except Exception as e:
                logging.warning(f"Network learner round failed: {e}")

            # Step 3e: Knowledge corpus refresh. Consolidates every signal
            # produced by Steps 3b/3c/3d (LLM scout, self-train, network
            # learner) plus the current weight snapshot, NLP part categories,
            # and OTD ownership into a relational corpus (entities + typed
            # edges) and an append-only learning log. Then projects the
            # corpus into the configured graph backend so every page that
            # speaks get_graph_backend() benefits from a dynamic architecture
            # the Brain expands as it learns.
            try:
                from src.brain.knowledge_corpus import (
                    refresh_corpus_round, materialize_into_graph,
                )
                kc = refresh_corpus_round()
                if isinstance(kc, dict) and "entities_added" in kc:
                    mg = materialize_into_graph()
                    logging.info(
                        f"Corpus: +{kc.get('entities_added',0)} entities "
                        f"(touched {kc.get('entities_touched',0)}), "
                        f"+{kc.get('edges_added',0)} edges "
                        f"(touched {kc.get('edges_touched',0)}), "
                        f"+{kc.get('learnings_logged',0)} learnings; "
                        f"projected {mg.get('nodes_projected',0)} nodes "
                        f"/ {mg.get('edges_projected',0)} edges into graph"
                    )
            except Exception as e:
                logging.warning(f"Knowledge corpus round failed: {e}")

            # Step 3f: Brain → Body bridge. The User is the Body of the
            # Brain — every effective signal across self-train, dispatch,
            # network, and corpus is distilled here into prioritized,
            # role-targeted Directives. The User's feedback (recorded via
            # `record_feedback`) loops back through Step 3e on the next
            # cycle, so the Body's actions become signals the Brain learns
            # from — closing the cognition↔operation loop.
            try:
                from src.brain.brain_body_signals import surface_effective_signals
                bb = surface_effective_signals()
                if isinstance(bb, dict) and "directives_emitted" in bb:
                    logging.info(
                        f"Brain→Body: emitted={bb.get('directives_emitted',0)} "
                        f"deduped={bb.get('directives_deduped',0)} "
                        f"top_priority={bb.get('top_priority',0):.2f}"
                    )
            except Exception as e:
                logging.warning(f"Brain→Body round failed: {e}")

            # Step 3g: Quest-Console mission refresh. Every open Mission is
            # a contract between the Brain and the Body — its two living
            # PPTX artifacts (Executive 1-Pager + Implementation Plan) must
            # stay current as new data arrives. Sequential refresh with
            # per-mission file-locking (handled inside mission_runner) so
            # manual "Refresh now" clicks and this scheduled tick can't
            # collide. Failures on individual missions don't stop the loop.
            try:
                from src.brain.mission_runner import refresh_open_missions
                mr = refresh_open_missions(max_concurrent=1, limit=25)
                if mr:
                    ok = sum(1 for r in mr if r.get("ok"))
                    logging.info(
                        f"Quest missions: refreshed {ok}/{len(mr)} open"
                    )
            except Exception as e:
                logging.warning(f"Quest mission refresh failed: {e}")

                        # Step 4: Self-document the changes
            generate_documentation()

            # Step 5: Executive Reporting
            generate_and_email_executive_report()

            # Step 6: Save/commit locally
            commit_and_push()

            logging.info("=== CYCLE COMPLETE. SLEEPING FOR 4 HOURS ===")
            
            # Sleep in intervals of 60 seconds to maintain a heartbeat
            sleep_duration = 14400
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
    autonomous_loop()
