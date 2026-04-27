"""
Kick-start background threads that the current agent child is missing:
  - citation_chain_acquirer  (wired into _run_agent_child but child started before patch)
  - Explicit network observer cycle trigger

Runs until KeyboardInterrupt.
"""
import sys, os, logging, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [KICKSTART] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join("logs", "kickstart.log"), encoding="utf-8"),
    ]
)
os.makedirs("logs", exist_ok=True)

# ── Citation-Chain Acquirer ────────────────────────────────────────────────
try:
    from src.brain.citation_chain_acquirer import schedule_in_background
    t = schedule_in_background(interval_s=3600)
    logging.info(
        "Citation-Chain Acquirer started — 337 direct + 313 discovered seeds, "
        "Semantic Scholar + OpenAlex, 60-min cadence."
    )
except Exception as e:
    logging.error(f"Citation-Chain Acquirer failed: {e}")
    t = None

# ── Explicit Network Observer immediate cycle ──────────────────────────────
try:
    from src.brain.network_observer import run_one_cycle
    logging.info("Running immediate network observer cycle...")
    result = run_one_cycle()
    logging.info(f"Network observer cycle result: {result}")
except ImportError:
    logging.warning("network_observer.run_one_cycle not available — observer runs on its own cadence")
except Exception as e:
    logging.error(f"Network observer cycle error: {e}")

# Keep alive so citation chain thread keeps running
logging.info("Kickstart running — citation chain acquirer active. Ctrl-C to stop.")
try:
    while True:
        time.sleep(60)
        if t and t.is_alive():
            logging.info("Citation-Chain thread alive.")
        else:
            logging.warning("Citation-Chain thread ended. Re-starting...")
            try:
                from src.brain.citation_chain_acquirer import schedule_in_background
                t = schedule_in_background(interval_s=3600)
            except Exception as e:
                logging.error(f"Re-start failed: {e}")
except KeyboardInterrupt:
    logging.info("Kickstart stopped.")
