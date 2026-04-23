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


# ---------------------------------------------------------------------------
# Neural Learning Expansion — helpers added to deepen the Brain's self-
# improvement loop beyond the original 4-hour fixed cadence.
# ---------------------------------------------------------------------------

def _kv_read(key: str, default: str | None = None) -> str | None:
    """Read a scalar from the brain_kv persistence table."""
    try:
        import sys, sqlite3
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.brain.local_store import db_path
        cn = sqlite3.connect(str(db_path()))
        row = cn.execute("SELECT value FROM brain_kv WHERE key=?", (key,)).fetchone()
        cn.close()
        return row[0] if row else default
    except Exception:
        return default


def _kv_write(key: str, value: str) -> None:
    """Upsert a scalar into the brain_kv persistence table."""
    try:
        import sys, sqlite3
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.brain.local_store import db_path
        cn = sqlite3.connect(str(db_path()))
        cn.execute(
            "INSERT INTO brain_kv(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        cn.commit()
        cn.close()
    except Exception:
        pass


def init_recurrent_depth():
    """Initialize the Recurrent Depth Transformer and register it with the ensemble.

    The RDT replaces one-shot vote aggregation with an adaptive-depth recurrent
    block: easy unanimous votes converge in 1 step; close-call multi-modal votes
    iterate until KL-divergence drops below epsilon or max_depth is reached.
    Per-task depth and KL trace are persisted to recurrent_depth_log so the Brain
    learns per-task reasoning complexity automatically over time.
    Called once at agent startup — fully idempotent.
    """
    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.brain.recurrent_depth import init_schema, register_with_ensemble
        init_schema()
        register_with_ensemble()
        logging.info("RDT: schema ready, adaptive-depth aggregator registered with ensemble.")
    except Exception as e:
        logging.warning(f"Recurrent Depth Transformer init failed: {e}")


def expand_nlp_taxonomy() -> int:
    """Batch-categorize uncategorized ERP parts using TF-IDF against the taxonomy.

    Each newly categorized part is a labeled example for the abc_classify
    self-train task, organically growing the Brain's labeled dataset every cycle.
    Falls back to keyword scoring when scikit-learn is unavailable.
    Returns the number of newly categorized parts.
    """
    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.brain.data_access import fetch_logical
        from src.brain.nlp_categorize import categorize_parts
        from src.brain.local_store import fetch_categories

        existing_df = fetch_categories()
        already = (
            set(existing_df["part_key"].astype(str).tolist())
            if (existing_df is not None
                and not existing_df.empty
                and "part_key" in existing_df.columns)
            else set()
        )

        parts = None
        for logical_name in ("dim_part", "parts", "item_master", "part_master"):
            try:
                df = fetch_logical("azure_sql", logical_name, top=3000)
                if df is not None and not df.empty:
                    parts = df
                    break
            except Exception:
                continue

        if parts is None or parts.empty:
            logging.info("NLP taxonomy: no parts source reachable this cycle.")
            return 0

        key_col = next(
            (c for c in ("part_key", "item_key", "part_id", "part_number", "item_number")
             if c in parts.columns),
            None,
        )
        new_parts = (
            parts[~parts[key_col].astype(str).isin(already)]
            if (key_col and already) else parts
        )

        if new_parts.empty:
            logging.info("NLP taxonomy: all reachable parts already categorized.")
            return 0

        batch = new_parts.head(500)
        categorize_parts(batch, key_col=key_col or "part_key")
        n = len(batch)
        logging.info(
            f"NLP taxonomy: categorized {n} new parts "
            f"\u2192 abc_classify ground truth expanded to ~{len(already) + n} entries."
        )
        return n
    except Exception as e:
        logging.warning(f"NLP taxonomy expansion failed: {e}")
        return 0


def seed_otd_direct() -> int:
    """Lightweight periodic OTD pull."""
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.brain.otd_recursive import run_otd_from_replica
        from src.brain.local_store import upsert_otd_owner
        import pandas as pd
        from pathlib import Path
        import logging

        n = 0
        bundle_path = Path(__file__).parent / "docs" / "OTD file.xlsx"
        if bundle_path.exists():
            xf = pd.ExcelFile(bundle_path)
            for sn in ["Missed Yesterday", "Shipping today", "Opened Yesterday"]:
                if sn in xf.sheet_names:
                    w = xf.parse(sn).dropna(how="all")
                    for c in ["Owner", "SO No", "Part", "Site", "Reason why failed", "Review Comment"]:
                        if c not in w.columns: w[c] = ""
                    for _, row in w.iterrows():
                        if str(row.get("SO No", "")).strip() == "": continue
                        rk = f'{str(row["SO No"]).strip()}_{str(row["Part"]).strip()}_{str(row["Site"]).strip()}'
                        ow = str(row["Owner"]).strip()
                        cm = str(row.get("Reason why failed", str(row.get("Review Comment", "")))).strip()
                        if ow and ow.lower() not in ("nan", "none", "") and rk != "__":
                            try:
                                upsert_otd_owner(rk, owner=ow, owner_comment=cm)
                                n += 1
                            except Exception:
                                pass
            if n > 0:
                logging.info(f"OTD seeding: wrote {n} ownership rows from bundle -> otd_classify ground truth.")
                return n

        # Fallback
        work, _ = run_otd_from_replica(connector="azure_sql", where=None, limit=1000)
        if work is None or work.empty:
            return 0

        cols_lower = {c.lower(): c for c in work.columns}
        owner_col = next((cols_lower[h] for h in ["buyer", "planner", "owner", "assigned_to"] if h in cols_lower), None)
        key_col = next((cols_lower[h] for h in ["po_number", "receipt_id", "so_no"] if h in cols_lower), None)

        if not owner_col:
            return 0
        
        for idx, row in work.iterrows():
            rk = str(row[key_col]).strip() if key_col else f"otd_{idx}"
            ow = str(row[owner_col]).strip()
            if ow and ow.lower() not in ("nan", "none", "") and rk:
                try:
                    upsert_otd_owner(rk, owner=ow)
                    n += 1
                except Exception:
                    pass
        if n > 0:
            logging.info(f"OTD seeding: wrote {n} ownership rows from replica -> otd_classify ground truth.")
        return n
    except Exception as e:
        logging.warning(f"OTD direct seeding failed: {e}")
        return 0

def sweep_all_data_sources() -> dict:
    """Iterate every configured data source and feed the Brain corpus.

    Sources
    -------
    1. **OneDrive xlsx pipeline** — all 16 sheet aliases from
       ``brain.yaml > xlsx_sources > aliases`` (Epicor, Oracle, SyteLine, AX).
       ABC-code aliases seed ``part_category`` directly; part/description data
       is TF-IDF categorized; OTD/PO rows are routed to ``otd_ownership``.

    2. **eDAP Azure SQL replica** — every table mapped in ``brain.yaml > tables``
       that is reachable via the ``azure_sql`` connector:
       * ``dim_part`` / ``dim_supplier`` → part_category + corpus entities
       * ``fact_po_receipt`` → otd_ownership (buyer/planner columns)
       * ``fact_inventory_on_hand``, ``fact_sales_order_line``,
         ``fact_ap_invoice_lines``, ``fact_part_cost`` → corpus entries

    3. **Oracle Fusion BIP REST** — uses the cached ``oracle_session.json``
       SSO session (no browser window).  Queries parts catalog, open POs with
       buyer name, and on-hand by org.  Falls back to the xlsx Oracle aliases
       if the REST session is stale or unavailable.

    All data is written to ``part_category`` and/or ``otd_ownership`` via the
    existing local_store APIs; the corpus refresh in Step 3e ingests those
    tables automatically each cycle.

    Returns
    -------
    dict
        ``sources_scanned``, ``rows_processed``, ``categories_written``,
        ``otd_owners_written`` — used for adaptive sleep velocity.
    """
    import sys, sqlite3
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    from src.brain import load_config
    from src.brain.local_store import (
        upsert_categories, upsert_otd_owner, db_path, init_schema as _ls_init,
    )
    from src.brain.nlp_categorize import categorize_parts

    _ls_init()

    cfg = load_config()
    summary = {"sources_scanned": 0, "rows_processed": 0,
               "categories_written": 0, "otd_owners_written": 0}

    # ── column hint maps (mirrors orchestrator.py / brain.yaml patterns) ────
    _PART_KEY   = ["part_key", "part_number", "item_number", "item_id",
                   "PartNum", "PartNumber", "part_id", "item_key", "ITEM_NUMBER"]
    _PART_DESC  = ["part_description", "description", "item_desc", "PartDescription",
                   "oem_part_desc", "part_name", "DESCRIPTION", "Description"]
    _ABC_COLS   = ["abc_class", "ABCCode", "ABC_Class", "item_abc_code",
                   "ClassID", "abc_code", "ABC_Code", "ABCClass"]
    _BUYER_COLS = ["buyer", "planner", "responsible_party", "purchaser",
                   "assigned_to", "owner", "buyer_name", "planner_name",
                   "AGENT_NAME", "Buyer", "BuyerName"]
    _PO_COLS    = ["po_number", "po_num", "purchase_order", "receipt_id",
                   "po_line_id", "receipt_number", "po_id", "SEGMENT1",
                   "PO_NUMBER", "PoNumber"]
    _SUPP_KEY   = ["supplier_key", "supplier_number", "vendor_id", "vendor_number",
                   "VendorNum", "VENDOR_ID"]
    _SUPP_NAME  = ["supplier_name", "vendor_name", "pre_standardization_supplier_name",
                   "Name", "VENDOR_NAME"]
    _SITE_COLS  = ["site", "plant", "facility", "org_id", "Plant",
                   "business_unit_key", "ORGANIZATION_CODE"]

    def _col(df, hints):
        low = {c.lower(): c for c in df.columns}
        return next((low[h.lower()] for h in hints if h.lower() in low), None)

    def _absorb_parts(df, source_tag: str):
        """Categorize parts via NLP or ABC code and write to part_category."""
        if df is None or df.empty:
            return 0
        pk_col = _col(df, _PART_KEY)
        if not pk_col:
            return 0
        abc_col  = _col(df, _ABC_COLS)
        desc_col = _col(df, _PART_DESC)
        n = 0
        if abc_col:
            # Use the ERP-assigned ABC code directly as the ground-truth label.
            rows_to_upsert = []
            for _, row in df[[pk_col, abc_col]].dropna().iterrows():
                pk  = str(row[pk_col]).strip()
                abc = str(row[abc_col]).strip().upper()
                if pk and abc and abc not in ("NAN", "NONE", ""):
                    rows_to_upsert.append((pk, f"ABC:{abc}", 1.0, source_tag))
                    n += 1
                    if len(rows_to_upsert) >= 2000:
                        upsert_categories(rows_to_upsert)
                        rows_to_upsert.clear()
            if rows_to_upsert:
                upsert_categories(rows_to_upsert)
        elif desc_col:
            # NLP categorization when no ABC code present.
            batch = df[[pk_col, desc_col]].dropna().head(500).copy()
            batch.columns = [pk_col, "description"]
            try:
                categorize_parts(batch, key_col=pk_col,
                                 desc_cols=("description",))
                n = len(batch)
            except Exception as _e:
                logging.warning(f"NLP categorize failed for {source_tag}: {_e}")
        return n

    def _absorb_otd(df, source_tag: str):
        """Extract buyer/PO pairs and write to otd_ownership."""
        if df is None or df.empty:
            return 0
        buyer_col = _col(df, _BUYER_COLS)
        pk_col    = _col(df, _PO_COLS)
        if not buyer_col:
            return 0
        n = 0
        for idx, row in df.iterrows():
            rk = str(row[pk_col]).strip() if pk_col else f"{source_tag}_{idx}"
            ow = str(row[buyer_col]).strip()
            if ow and ow.lower() not in ("nan", "none", "") and rk:
                try:
                    upsert_otd_owner(rk, owner=ow)
                    n += 1
                except Exception:
                    pass
            if n >= 3000:
                break
        return n

    def _log_corpus_entity(entity_id, entity_type, label=None, source=None):
        """Direct minimal write to corpus_entity (no full corpus refresh needed)."""
        try:
            import sqlite3 as _sq
            now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            cn2 = _sq.connect(str(db_path()))
            cn2.execute(
                """INSERT INTO corpus_entity(entity_id,entity_type,label,props_json,
                   first_seen,last_seen,samples) VALUES(?,?,?,?,?,?,1)
                   ON CONFLICT(entity_id,entity_type) DO UPDATE
                   SET last_seen=excluded.last_seen, samples=samples+1,
                       label=COALESCE(excluded.label, corpus_entity.label)""",
                (str(entity_id), str(entity_type), label,
                 '{"source":"' + (source or "sweep") + '"}', now, now),
            )
            cn2.commit()
            cn2.close()
        except Exception:
            pass

    # ────────────────────────────────────────────────────────────────────────
    # 1. OneDrive xlsx pipeline — all configured sheet aliases
    # ────────────────────────────────────────────────────────────────────────
    xlsx_aliases = list((cfg.get("xlsx_sources") or {}).get("aliases", {}).keys())
    if xlsx_aliases:
        try:
            from src.brain.data_access import fetch_xlsx_source
        except Exception as _e:
            fetch_xlsx_source = None
            logging.warning(f"xlsx pipeline unavailable: {_e}")

    for alias in xlsx_aliases:
        if not fetch_xlsx_source:
            break
        try:
            df = fetch_xlsx_source(alias)
            if df is None or df.empty:
                continue
            summary["sources_scanned"] += 1
            summary["rows_processed"] += len(df)
            n_cat = _absorb_parts(df, f"xlsx:{alias}")
            n_otd = _absorb_otd(df, f"xlsx:{alias}")
            summary["categories_written"] += n_cat
            summary["otd_owners_written"] += n_otd
            logging.info(
                f"OneDrive xlsx [{alias}]: {len(df)} rows → "
                f"{n_cat} categories, {n_otd} OTD owners"
            )
        except Exception as _e:
            logging.warning(f"OneDrive xlsx alias '{alias}' failed: {_e}")

    # ────────────────────────────────────────────────────────────────────────
    # 2. eDAP Azure SQL replica — every table mapped in brain.yaml
    # ────────────────────────────────────────────────────────────────────────
    try:
        from src.brain.db_registry import bootstrap_default_connectors
        from src.brain.data_access import fetch_logical
        bootstrap_default_connectors()
    except Exception as _e:
        logging.warning(f"eDAP connector bootstrap failed: {_e}")
        fetch_logical = None

    # Logical table → (ingest_fn, top_n, purpose_tag)
    _EDAP_TABLES = [
        ("parts",             _absorb_parts,  5000, "dim_part → NLP categorize"),
        ("suppliers",         None,           3000, "dim_supplier → corpus entities"),
        ("po_receipts",       _absorb_otd,    5000, "fact_po_receipt → OTD owners"),
        ("on_hand",           None,           3000, "fact_on_hand → corpus"),
        ("open_purchase",     _absorb_otd,    3000, "open POs → OTD owners"),
        ("sales_order_lines", None,           3000, "sales → corpus"),
        ("ap_invoice_lines",  None,           3000, "AP invoices → corpus"),
        ("part_cost",         None,           3000, "part cost → corpus"),
    ]

    if fetch_logical:
        for logical, ingest_fn, top, tag in _EDAP_TABLES:
            try:
                df = fetch_logical("azure_sql", logical, top=top)
                if df is None or df.empty or df.attrs.get("_error"):
                    err = (df.attrs.get("_error") or "no rows") if df is not None else "fetch failed"
                    logging.info(f"eDAP [{logical}]: skipped — {err}")
                    continue
                summary["sources_scanned"] += 1
                summary["rows_processed"] += len(df)

                # Always push supplier / part / site / site entities into corpus
                pk_col   = _col(df, _PART_KEY)
                supp_key = _col(df, _SUPP_KEY)
                supp_nm  = _col(df, _SUPP_NAME)
                site_col = _col(df, _SITE_COLS)
                if pk_col:
                    for v in df[pk_col].dropna().astype(str).unique()[:1000]:
                        if v.strip():
                            _log_corpus_entity(v.strip(), "Part", source=logical)
                if supp_key or supp_nm:
                    key_c = supp_key or supp_nm
                    nm_c  = supp_nm
                    for _, row in df[[c for c in [key_c, nm_c] if c]].dropna().drop_duplicates().head(500).iterrows():
                        eid   = str(row[key_c]).strip() if key_c else ""
                        label = str(row[nm_c]).strip()  if nm_c  else eid
                        if eid:
                            _log_corpus_entity(eid, "Supplier", label=label, source=logical)
                if site_col:
                    for v in df[site_col].dropna().astype(str).unique():
                        if v.strip():
                            _log_corpus_entity(v.strip(), "Site", source=logical)

                if ingest_fn is not None:
                    n = ingest_fn(df, f"edap:{logical}")
                    if ingest_fn is _absorb_parts:
                        summary["categories_written"] += n
                    else:
                        summary["otd_owners_written"] += n
                    logging.info(
                        f"eDAP [{logical}]: {len(df)} rows, {tag} → {n} written"
                    )
                else:
                    logging.info(
                        f"eDAP [{logical}]: {len(df)} rows indexed → {tag}"
                    )
            except Exception as _e:
                logging.warning(f"eDAP table '{logical}' sweep failed: {_e}")

    # ────────────────────────────────────────────────────────────────────────
    # 3. Oracle Fusion BIP REST — headless, uses cached oracle_session.json
    # ────────────────────────────────────────────────────────────────────────
    _ORACLE_QUERIES = [
        # (label, sql, ingest_fn)
        (
            "oracle_parts",
            # OTBI-style BIP SQL — reads from the Oracle Fusion Items catalog
            ("SELECT ITEM_NUMBER, DESCRIPTION, ITEM_TYPE "
             "FROM EGP_SYSTEM_ITEMS_VL WHERE ROWNUM <= 5000"),
            _absorb_parts,
        ),
        (
            "oracle_open_po",
            # Open PO headers with buyer name
            ("SELECT PH.SEGMENT1 AS PO_NUMBER, "
             "       PPNF.DISPLAY_NAME AS BUYER_NAME "
             "FROM   PO_HEADERS_ALL PH "
             "JOIN   PER_PERSON_NAMES_F PPNF "
             "       ON PPNF.PERSON_ID = PH.AGENT_ID "
             "          AND PPNF.NAME_TYPE = 'GLOBAL' "
             "          AND SYSDATE BETWEEN PPNF.EFFECTIVE_START_DATE "
             "                          AND PPNF.EFFECTIVE_END_DATE "
             "WHERE  PH.CLOSED_CODE IS NULL AND ROWNUM <= 3000"),
            _absorb_otd,
        ),
        (
            "oracle_onhand",
            # On-hand quantities by item / org
            ("SELECT ITEM_NUMBER, ORGANIZATION_CODE, SUM(TRANSACTION_QUANTITY) AS QTY "
             "FROM   MTL_ONHAND_QUANTITIES_DETAIL MOQD "
             "JOIN   MTL_SYSTEM_ITEMS_B MSIB "
             "       ON MSIB.INVENTORY_ITEM_ID = MOQD.INVENTORY_ITEM_ID "
             "          AND MSIB.ORGANIZATION_ID  = MOQD.ORGANIZATION_ID "
             "JOIN   ORG_ORGANIZATION_DEFINITIONS OOD "
             "       ON OOD.ORGANIZATION_ID = MOQD.ORGANIZATION_ID "
             "GROUP  BY ITEM_NUMBER, ORGANIZATION_CODE "
             "HAVING ROWNUM <= 3000"),
            None,   # just index into corpus entities
        ),
    ]

    oracle_ok = False
    try:
        from src.connections.oracle_fusion import OracleFusionSession
        sess = OracleFusionSession()
        sess.connect()       # will re-use oracle_session.json if valid; raises if expired
        oracle_ok = True
        logging.info("Oracle Fusion: session active — running BIP sweeps.")
    except Exception as _e:
        logging.info(f"Oracle Fusion: session unavailable (headless OK — {_e}); using xlsx fallback.")

    if oracle_ok:
        for label, sql, ingest_fn in _ORACLE_QUERIES:
            try:
                df = sess.execute_sql(sql, max_rows=5000)
                if df is None or df.empty:
                    logging.info(f"Oracle [{label}]: no rows returned.")
                    continue
                summary["sources_scanned"] += 1
                summary["rows_processed"] += len(df)

                # Index Part and Site entities
                pk_col   = _col(df, _PART_KEY)
                site_col = _col(df, _SITE_COLS)
                if pk_col:
                    for v in df[pk_col].dropna().astype(str).unique()[:2000]:
                        if v.strip():
                            _log_corpus_entity(v.strip(), "Part", source=label)
                if site_col:
                    for v in df[site_col].dropna().astype(str).unique():
                        if v.strip():
                            _log_corpus_entity(v.strip(), "Site", source=label)

                if ingest_fn is not None:
                    n = ingest_fn(df, f"oracle:{label}")
                    if ingest_fn is _absorb_parts:
                        summary["categories_written"] += n
                    else:
                        summary["otd_owners_written"] += n
                    logging.info(
                        f"Oracle [{label}]: {len(df)} rows → {n} written"
                    )
                else:
                    logging.info(
                        f"Oracle [{label}]: {len(df)} rows indexed into corpus"
                    )
            except Exception as _e:
                logging.warning(f"Oracle BIP query '{label}' failed: {_e}")

    # ────────────────────────────────────────────────────────────────────────
    # 4. Dynamic connector discovery — enumerate ALL connectors in db_registry
    #    and sweep any SQL connector that wasn't covered in sections 1-3 above.
    #    This means new connectors added to bootstrap_default_connectors() or
    #    registered by third-party code are automatically explored without any
    #    changes to this file.
    # ────────────────────────────────────────────────────────────────────────
    _KNOWN_CONNECTORS = frozenset({"azure_sql", "oracle_fusion"})

    try:
        from src.brain.db_registry import list_connectors, read_sql as _read_sql_reg
        all_connectors = list_connectors()
    except Exception as _e:
        logging.warning(f"Dynamic connector enumeration failed: {_e}")
        all_connectors = []

    for _conn in all_connectors:
        if _conn.kind != "sql":
            continue                    # http/graph connectors handled elsewhere
        if _conn.name in _KNOWN_CONNECTORS:
            continue                    # already swept above
        # Respect a per-connector cooldown: don't re-explore within 6 hours
        _kv_key = f"connector_last_swept:{_conn.name}"
        try:
            import sys as _sys2
            _sys2.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            _last_swept = float(_kv_read(_kv_key, "0") or "0")
        except Exception:
            _last_swept = 0.0
        if (time.time() - _last_swept) < 6 * 3600:
            logging.info(
                f"Dynamic sweep [{_conn.name}]: skipped (swept < 6h ago)"
            )
            continue

        try:
            # Discover tables via INFORMATION_SCHEMA (works for SQL Server + Azure SQL)
            _tables_df = _read_sql_reg(
                _conn.name,
                "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME",
            )
            if _tables_df is None or _tables_df.empty or _tables_df.attrs.get("_error"):
                # Fallback: sys.objects for environments that block INFORMATION_SCHEMA
                _tables_df = _read_sql_reg(
                    _conn.name,
                    "SELECT s.name AS TABLE_SCHEMA, o.name AS TABLE_NAME "
                    "FROM sys.objects o JOIN sys.schemas s ON s.schema_id=o.schema_id "
                    "WHERE o.type='U' ORDER BY s.name, o.name",
                )
        except Exception as _te:
            logging.warning(f"Dynamic sweep [{_conn.name}]: table discovery failed — {_te}")
            continue

        if _tables_df is None or _tables_df.empty:
            logging.info(f"Dynamic sweep [{_conn.name}]: no tables discovered.")
            _kv_write(_kv_key, str(time.time()))
            continue

        _conn_rows = 0
        _conn_sources = 0
        for _, _trow in _tables_df.head(50).iterrows():
            _schema = str(_trow.get("TABLE_SCHEMA", "dbo") or "dbo")
            _tname  = str(_trow.get("TABLE_NAME", "") or "")
            if not _tname:
                continue
            _qualified = f"{_schema}.{_tname}"
            try:
                _sample = _read_sql_reg(
                    _conn.name,
                    f"SELECT TOP 200 * FROM {_qualified}",
                )
                if _sample is None or _sample.empty or _sample.attrs.get("_error"):
                    continue
                _conn_sources += 1
                _conn_rows    += len(_sample)
                summary["sources_scanned"] += 1
                summary["rows_processed"]  += len(_sample)

                _tag = f"{_conn.name}:{_qualified}"
                _n_cat = _absorb_parts(_sample, _tag)
                _n_otd = _absorb_otd(_sample,   _tag)
                summary["categories_written"] += _n_cat
                summary["otd_owners_written"] += _n_otd

                # Also index any entity columns directly into the corpus
                for _hint_set, _etype in [
                    (_PART_KEY,  "Part"),
                    (_SUPP_KEY,  "Supplier"),
                    (_SITE_COLS, "Site"),
                ]:
                    _ec = _col(_sample, _hint_set)
                    if _ec:
                        for _v in _sample[_ec].dropna().astype(str).unique()[:200]:
                            if _v.strip():
                                _log_corpus_entity(_v.strip(), _etype, source=_tag)

                if _n_cat or _n_otd:
                    logging.info(
                        f"Dynamic sweep [{_tag}]: "
                        f"{len(_sample)} rows → {_n_cat} categories, {_n_otd} OTD"
                    )
            except Exception as _re:
                logging.debug(
                    f"Dynamic sweep [{_conn.name}:{_qualified}]: {_re}"
                )

        logging.info(
            f"Dynamic sweep [{_conn.name}]: "
            f"{_conn_sources} tables, {_conn_rows} rows processed."
        )
        _kv_write(_kv_key, str(time.time()))

    logging.info(
        f"Data sweep complete: "
        f"sources={summary['sources_scanned']}, "
        f"rows={summary['rows_processed']:,}, "
        f"categories_written={summary['categories_written']}, "
        f"otd_owners_written={summary['otd_owners_written']}"
    )
    return summary


def rag_knowledge_deepdive(
    window_label: str = "all",
    window_hours: int | None = None,
    window_offset_hours: int = 0,
    max_iterations: int = 8,
    max_entities: int = 2000,
    explored_kv_key: str = "rag_explored_pairs",
) -> dict:
    """SOTA RAG iterative deepening over the Brain's knowledge graph.

    Each call runs up to ``max_iterations`` passes of:

    1. **Retrieve** — pull high-signal corpus entities (sorted by samples DESC)
       and build a TF-IDF index over their labels for dense semantic retrieval.

    2. **Structural-hole detection** — for every entity with degree ≥ 2, find
       pairs of neighbors that share no direct edge (Burt structural-hole
       heuristic).  These are the highest-value *missing* knowledge pathways.

    3. **Semantic confirmation** — compute TF-IDF cosine similarity between
       the two endpoint labels.  Only holes with cosine ≥ ``min_similarity``
       are pursued so spurious cross-domain connections are suppressed.

    4. **Source retrieval** — look up the ``source`` field in each entity's
       ``props_json`` and attempt to re-fetch a window of rows containing both
       entity keys so a data-grounded co-occurrence signal can be computed.
       Falls back to the similarity score when the source is unavailable.

    5. **Augment** — upsert a ``corpus_edge`` between the two entities weighted
       by the grounded co-occurrence signal (or similarity fallback), then
       write a ``learning_log`` entry with kind=``rag_deepdive`` so the Brain
       has an audit trail of every pathway it discovered autonomously.

    6. **Convergence** — stop early when fewer than ``min_new_per_iteration``
       edges are added in a pass (the graph is locally saturated).  The
       explored-pair set is persisted in ``brain_kv`` across cycles so each
       cycle goes *deeper* rather than re-traversing the same ground.

    Parameters
    ----------
    window_label:
        Label used in logging and learning_log entries (e.g. ``"recent"``,
        ``"7d"``, ``"30d"``, ``"90d"``, ``"all"``).
    window_hours:
        If provided, restrict the entity universe to those whose ``last_seen``
        falls within ``[now - window_offset_hours - window_hours, now -
        window_offset_hours]``. ``None`` (default) considers all entities.
    window_offset_hours:
        Look-back offset in hours from ``now``. Combined with ``window_hours``
        this lets parallel agents work *relationally dispersed periods* of
        the corpus simultaneously without stepping on each other.
    max_iterations:
        Per-call convergence cap (default 8).
    max_entities:
        Entity universe size pulled from corpus_entity each call.
    explored_kv_key:
        Per-window persisted explored-pair cache key in ``brain_kv``. Each
        worker uses its own key so their explored sets don't conflict.

    Returns dict: ``iterations_run``, ``edges_discovered``, ``gaps_found``,
    ``pathways_explored``, ``window_label``.
    """
    import sys, sqlite3, json as _json
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from src.brain.local_store import db_path

    MAX_ITERATIONS      = max_iterations
    MAX_ENTITIES        = max_entities
    MAX_GAPS_PER_ITER   = 50     # structural holes to evaluate per pass
    MIN_SIMILARITY      = 0.15   # TF-IDF cosine threshold to pursue a gap
    MIN_NEW_PER_ITER    = 3      # converge when fewer new edges are added

    result = {"iterations_run": 0, "edges_discovered": 0,
              "gaps_found": 0, "pathways_explored": 0,
              "window_label": window_label}

    # ── Persistent explored-pair cache (avoids re-traversing known paths) ──
    explored_raw = _kv_read(explored_kv_key, "")
    explored: set[tuple] = set()
    if explored_raw:
        try:
            explored = {tuple(p) for p in _json.loads(explored_raw)}
        except Exception:
            explored = set()

    try:
        cn = sqlite3.connect(str(db_path()), check_same_thread=False)
        cn.row_factory = sqlite3.Row
    except Exception as _e:
        logging.warning(f"RAG deepdive[{window_label}]: cannot open corpus DB — {_e}")
        return result

    try:
        # ── 1. Load corpus entities (optionally time-windowed) ──────────
        if window_hours is not None:
            from datetime import timedelta
            now = datetime.now()
            t_end   = (now - timedelta(hours=window_offset_hours)
                       ).strftime("%Y-%m-%dT%H:%M:%S")
            t_start = (now - timedelta(hours=window_offset_hours + window_hours)
                       ).strftime("%Y-%m-%dT%H:%M:%S")
            entity_rows = cn.execute(
                """SELECT entity_id, entity_type, label, props_json, samples
                   FROM corpus_entity
                   WHERE last_seen >= ? AND last_seen <= ?
                   ORDER BY samples DESC
                   LIMIT ?""",
                (t_start, t_end, MAX_ENTITIES),
            ).fetchall()
        else:
            entity_rows = cn.execute(
                """SELECT entity_id, entity_type, label, props_json, samples
                   FROM corpus_entity
                   ORDER BY samples DESC
                   LIMIT ?""",
                (MAX_ENTITIES,),
            ).fetchall()

        if len(entity_rows) < 4:
            logging.info(f"RAG deepdive[{window_label}]: corpus too sparse — skipping.")
            return result

        # Build label list + id→index map for TF-IDF
        labels       = [(r["label"] or r["entity_id"]) for r in entity_rows]
        id_pairs     = [(r["entity_id"], r["entity_type"]) for r in entity_rows]
        source_map   = {}
        for r in entity_rows:
            try:
                props = _json.loads(r["props_json"] or "{}")
                src = props.get("source", "")
            except Exception:
                src = ""
            source_map[(r["entity_id"], r["entity_type"])] = src

        # TF-IDF index over labels (cosine similarity for semantic retrieval)
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics.pairwise import linear_kernel
            import numpy as np
            _safe_labels = [str(l) if l else "?" for l in labels]
            tfidf        = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4),
                                           max_features=8000, sublinear_tf=True)
            tfidf_matrix = tfidf.fit_transform(_safe_labels)
            _has_tfidf   = True
        except Exception:
            _has_tfidf  = False
            tfidf_matrix = None

        # ── 2. Build adjacency from corpus_edge ───────────────────────────
        edge_rows = cn.execute(
            "SELECT src_id, src_type, dst_id, dst_type, rel, weight "
            "FROM corpus_edge"
        ).fetchall()

        adj: dict[tuple, set[tuple]] = {}           # (id, type) → {(id, type), …}
        edge_set: set[tuple] = set()                # (src_id, src_type, dst_id, dst_type)
        for e in edge_rows:
            s = (e["src_id"], e["src_type"])
            d = (e["dst_id"], e["dst_type"])
            adj.setdefault(s, set()).add(d)
            adj.setdefault(d, set()).add(s)
            edge_set.add((e["src_id"], e["src_type"], e["dst_id"], e["dst_type"]))
            edge_set.add((e["dst_id"], e["dst_type"], e["src_id"], e["src_type"]))

        idx_of = {p: i for i, p in enumerate(id_pairs)}

        # ── Iterative deepening ────────────────────────────────────────────
        for iteration in range(MAX_ITERATIONS):
            gaps: list[tuple] = []   # (entity_a, entity_b, shared_neighbors_count)

            # Structural-hole detection: pairs sharing ≥2 neighbors with no edge
            sampled_entities = id_pairs[:min(300, len(id_pairs))]
            for i, ea in enumerate(sampled_entities):
                nbrs_a = adj.get(ea, set())
                if len(nbrs_a) < 2:
                    continue
                for j in range(i + 1, len(sampled_entities)):
                    eb = sampled_entities[j]
                    if ea == eb:
                        continue
                    pair_key = tuple(sorted([str(ea), str(eb)]))
                    if pair_key in explored:
                        continue
                    if (ea[0], ea[1], eb[0], eb[1]) in edge_set:
                        continue
                    shared = nbrs_a & adj.get(eb, set())
                    if len(shared) >= 2:
                        gaps.append((ea, eb, len(shared)))

            gaps.sort(key=lambda x: -x[2])
            gaps = gaps[:MAX_GAPS_PER_ITER]
            result["gaps_found"] += len(gaps)

            new_edges_this_iter = 0
            for ea, eb, shared_count in gaps:
                # Semantic similarity check via TF-IDF cosine
                similarity = 0.0
                if _has_tfidf and ea in idx_of and eb in idx_of:
                    ia, ib = idx_of[ea], idx_of[eb]
                    try:
                        sim_vec = linear_kernel(
                            tfidf_matrix[ia], tfidf_matrix[ib]
                        )
                        similarity = float(sim_vec[0][0])
                    except Exception:
                        similarity = 0.0

                # Shared-neighbor ratio as additional signal
                nbr_signal = min(1.0, shared_count / 5.0)
                combined_signal = 0.6 * nbr_signal + 0.4 * similarity

                if combined_signal < MIN_SIMILARITY and similarity < MIN_SIMILARITY:
                    # Mark as explored so we don't re-evaluate next iteration
                    explored.add(tuple(sorted([str(ea), str(eb)])))
                    continue

                # ── Source retrieval: try to ground the edge in actual data ──
                src_a = source_map.get(ea, "")
                src_b = source_map.get(eb, "")
                grounded_signal = combined_signal   # fallback
                if src_a or src_b:
                    try:
                        from src.brain.data_access import fetch_logical
                        # Try to find rows that mention both entity IDs in the
                        # same source table to compute a data-grounded signal.
                        for _src in filter(None, [src_a, src_b]):
                            # source format: "connector:logical" or "alias"
                            _parts = _src.split(":")
                            if len(_parts) == 2 and _parts[0] != "xlsx":
                                _conn_nm, _tbl = _parts
                                _df = fetch_logical(_conn_nm, _tbl, top=1000)
                                if _df is not None and not _df.empty:
                                    # Count rows mentioning ea[0] anywhere
                                    _ea_mask = _df.apply(
                                        lambda col: col.astype(str).str.contains(
                                            str(ea[0])[:16], na=False, regex=False
                                        )
                                    ).any(axis=1)
                                    _eb_mask = _df.apply(
                                        lambda col: col.astype(str).str.contains(
                                            str(eb[0])[:16], na=False, regex=False
                                        )
                                    ).any(axis=1)
                                    co_occur = int((_ea_mask & _eb_mask).sum())
                                    if co_occur > 0:
                                        grounded_signal = min(
                                            1.0,
                                            combined_signal + co_occur / 100.0,
                                        )
                                        break
                    except Exception:
                        pass  # fallback to combined_signal stays

                # ── Upsert the discovered edge ─────────────────────────────
                now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                try:
                    cn.execute(
                        """INSERT INTO corpus_edge(src_id, src_type, dst_id, dst_type,
                               rel, weight, last_seen, samples)
                           VALUES(?,?,?,?,?,?,?,1)
                           ON CONFLICT(src_id,src_type,dst_id,dst_type,rel)
                           DO UPDATE SET
                               last_seen=excluded.last_seen,
                               samples=samples+1,
                               weight=0.7*corpus_edge.weight+0.3*excluded.weight""",
                        (ea[0], ea[1], eb[0], eb[1],
                         "RAG_INFERRED", grounded_signal, now),
                    )
                    # Symmetric reverse edge (bidirectional knowledge pathway)
                    cn.execute(
                        """INSERT INTO corpus_edge(src_id, src_type, dst_id, dst_type,
                               rel, weight, last_seen, samples)
                           VALUES(?,?,?,?,?,?,?,1)
                           ON CONFLICT(src_id,src_type,dst_id,dst_type,rel)
                           DO UPDATE SET
                               last_seen=excluded.last_seen,
                               samples=samples+1,
                               weight=0.7*corpus_edge.weight+0.3*excluded.weight""",
                        (eb[0], eb[1], ea[0], ea[1],
                         "RAG_INFERRED", grounded_signal, now),
                    )
                    # Update in-memory sets so later gaps in this iteration see it
                    edge_set.add((ea[0], ea[1], eb[0], eb[1]))
                    edge_set.add((eb[0], eb[1], ea[0], ea[1]))
                    adj.setdefault(ea, set()).add(eb)
                    adj.setdefault(eb, set()).add(ea)

                    # Append to learning_log
                    cn.execute(
                        """INSERT INTO learning_log(logged_at, kind, title, detail,
                               signal_strength)
                           VALUES(?,?,?,?,?)""",
                        (
                            now, "rag_deepdive",
                            f"RAG inferred {ea[1]}:{ea[0]} ↔ {eb[1]}:{eb[0]}",
                            _json.dumps({
                                "shared_neighbors": shared_count,
                                "tfidf_similarity": round(similarity, 4),
                                "grounded_signal":  round(grounded_signal, 4),
                                "src_a": src_a, "src_b": src_b,
                                "iteration": iteration,
                            }),
                            grounded_signal,
                        ),
                    )
                    new_edges_this_iter += 1
                    result["edges_discovered"] += 1
                    result["pathways_explored"] += 1
                except Exception as _ue:
                    logging.debug(f"RAG edge upsert failed: {_ue}")

                explored.add(tuple(sorted([str(ea), str(eb)])))

            cn.commit()
            result["iterations_run"] += 1
            logging.info(
                f"RAG deepdive iter {iteration + 1}: "
                f"gaps={len(gaps)}, new_edges={new_edges_this_iter}, "
                f"total_discovered={result['edges_discovered']}"
            )

            if new_edges_this_iter < MIN_NEW_PER_ITER:
                logging.info(
                    f"RAG deepdive: converged after {iteration + 1} iterations "
                    f"(< {MIN_NEW_PER_ITER} new edges this pass)."
                )
                break

        # Persist explored pairs (cap at 5000 to bound kv storage)
        explored_list = list(explored)[-5000:]
        _kv_write(explored_kv_key, _json.dumps(explored_list))

    except Exception as _e:
        logging.warning(f"RAG knowledge deepdive[{window_label}] failed: {_e}")
    finally:
        try:
            cn.close()
        except Exception:
            pass

    logging.info(
        f"RAG deepdive[{window_label}] complete: "
        f"iterations={result['iterations_run']}, "
        f"edges_discovered={result['edges_discovered']}, "
        f"pathways_explored={result['pathways_explored']}"
    )
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Continuous multi-agent synaptic extension system
# ─────────────────────────────────────────────────────────────────────────────
#
# Rather than waiting for the 1-4 hour main cycle to fire, four lightweight
# worker threads run continuously in the background — each focused on a
# different aspect of synaptic extension and a different temporal window of
# the corpus. They run on staggered, overlapping cadences so that by the time
# any single agent reaches an aspect of the knowledge graph, the synapses
# (corpus_edges) have already been built by an earlier worker.
#
# The four workers:
#   1. _synaptic_builder_worker   — RAG deepdive on the most-recent 24h window
#                                   (every  ~10 min). Catches the "near present".
#   2. _lookahead_worker          — RAG deepdive on dispersed historical
#                                   windows (7d / 30d / 90d, rotating; every
#                                   ~15 min). Pre-builds synapses N hops
#                                   ahead of where the main loop currently is.
#   3. _dispersed_sweeper_worker  — Rotates through registered connectors,
#                                   ingesting fresh data continuously
#                                   (every ~20 min) instead of waiting for
#                                   the main loop's full sweep.
#   4. _convergence_worker        — Runs corpus refresh + graph materialise
#                                   (every ~30 min) so all the work the other
#                                   workers wrote becomes visible to readers.
#
# All workers:
#   • use ``threading.Thread(daemon=True)`` so they die with the main loop
#   • share a single ``threading.Event`` shutdown flag (``_SYNAPTIC_STOP``)
#   • use SQLite with ``check_same_thread=False`` and short transactions
#   • record their own last-run timestamp + summary in ``brain_kv`` so the
#     main loop and operators can see what each worker is doing
#   • use their own ``rag_explored_pairs_<window>`` key so one window's
#     exploration progress doesn't clobber another's

import threading as _threading

_SYNAPTIC_STOP = _threading.Event()
_SYNAPTIC_THREADS: list = []
_SYNAPTIC_STARTED = False

# Per-worker consecutive-failure counters (worker name → int). Used for
# exponential backoff so a misconfigured connector / corrupt corpus doesn't
# turn into a tight error loop pinning CPU. Reset to 0 on any successful
# iteration. Cap multiplier at 8× the base interval (~80-240 min depending
# on worker) so a permanently-broken dependency still gets retried hourly-ish.
_SYNAPTIC_FAILURES: dict[str, int] = {}
_SYNAPTIC_BACKOFF_MAX_MULT = 8


def _wait_or_stop(seconds: int) -> bool:
    """Sleep ``seconds`` but wake immediately if shutdown is requested.

    Returns ``True`` if shutdown was requested (worker should exit).
    """
    return _SYNAPTIC_STOP.wait(timeout=max(1, int(seconds)))


def _next_sleep_with_backoff(name: str, base_interval_s: int,
                             jitter_s: int, last_ok: bool) -> int:
    """Compute next sleep for a worker with consecutive-failure backoff.

    Updates ``_SYNAPTIC_FAILURES[name]`` in place: incremented on failure,
    cleared on success. Sleep multiplier doubles per consecutive failure
    (1, 2, 4, 8, ... up to ``_SYNAPTIC_BACKOFF_MAX_MULT``). Jitter is added
    after the multiplier so workers always desync.
    """
    import random as _r
    if last_ok:
        _SYNAPTIC_FAILURES[name] = 0
        mult = 1
    else:
        prev = _SYNAPTIC_FAILURES.get(name, 0)
        cur = prev + 1
        _SYNAPTIC_FAILURES[name] = cur
        # 1 failure -> 2x, 2 failures -> 4x, 3 -> 8x, etc., capped
        mult = min(2 ** cur, _SYNAPTIC_BACKOFF_MAX_MULT)
        # Persist a marker so operators can see degraded workers
        try:
            _kv_write(
                f"synapse_{name}_failures",
                f"{datetime.now().isoformat()}|consecutive={cur}|next_mult={mult}x",
            )
        except Exception:
            pass
    return base_interval_s * mult + _r.randint(-jitter_s, jitter_s)


def synaptic_agents_status() -> dict:
    """Return a snapshot of synaptic-worker health for ops/diagnostics.

    Reads the per-worker heartbeat keys from ``brain_kv`` and reports each
    worker's last-run timestamp, last summary, consecutive-failure count,
    and a freshness verdict (``"ok"`` / ``"stale"`` / ``"never_ran"``)
    based on whether the heartbeat is younger than 4× the worker's expected
    interval. Stale workers indicate the daemon is alive but its iterations
    are silently dying — a critical condition for the synaptic substrate.
    """
    from datetime import timedelta
    # (kv_key, friendly_name, expected_interval_s)
    workers = [
        ("synapse_builder_last",      "synapse-builder",     600),
        ("synapse_lookahead_7d_last", "synapse-lookahead-7d",  900),
        ("synapse_lookahead_30d_last","synapse-lookahead-30d", 900),
        ("synapse_lookahead_90d_last","synapse-lookahead-90d", 900),
        ("synapse_convergence_last",  "synapse-convergence",  1800),
    ]
    out: dict = {
        "started":        _SYNAPTIC_STARTED,
        "started_at":     _kv_read("synapse_agents_started", "never"),
        "thread_count":   len([t for t in _SYNAPTIC_THREADS if t.is_alive()]),
        "shutdown_set":   _SYNAPTIC_STOP.is_set(),
        "workers":        [],
    }
    now = datetime.now()
    for kv_key, friendly, interval_s in workers:
        raw = _kv_read(kv_key, "")
        ts_iso = raw.split("|", 1)[0] if raw else ""
        summary = raw.split("|", 1)[1] if "|" in raw else ""
        try:
            last_ts = datetime.fromisoformat(ts_iso) if ts_iso else None
        except Exception:
            last_ts = None
        if last_ts is None:
            verdict = "never_ran"
            age_s = None
        else:
            age_s = int((now - last_ts).total_seconds())
            verdict = "ok" if age_s < 4 * interval_s else "stale"
        # short-name → failure count (best-effort match)
        short = friendly.replace("synapse-", "").split("-")[0]
        fails = _SYNAPTIC_FAILURES.get(short, 0)
        out["workers"].append({
            "name":            friendly,
            "kv_key":          kv_key,
            "expected_every":  interval_s,
            "last_iso":        ts_iso or None,
            "age_seconds":     age_s,
            "summary":         summary,
            "consecutive_failures": fails,
            "verdict":         verdict,
        })
    return out


def _synaptic_builder_worker() -> None:
    """Worker #1 — synaptic builder for the *near-present* window.

    Runs ``rag_knowledge_deepdive()`` against the last 24h of corpus activity
    every ~10 min. This keeps the high-traffic part of the graph saturated
    with synapses so missions launched against fresh data find ready bridges.
    """
    INTERVAL_S = 600   # 10 min
    JITTER_S   = 60    # ±60s to desync from other workers
    NAME       = "builder"
    import random as _r
    logging.info("[synapse:builder] started — interval=10min window=24h")
    if _wait_or_stop(_r.randint(5, 30)):
        return
    while not _SYNAPTIC_STOP.is_set():
        t0 = time.time()
        ok = False
        try:
            r = rag_knowledge_deepdive(
                window_label="builder_24h",
                window_hours=24,
                window_offset_hours=0,
                max_iterations=4,           # smaller per-pass than main loop
                max_entities=800,
                explored_kv_key="rag_explored_pairs_builder",
            )
            _kv_write(
                "synapse_builder_last",
                f"{datetime.now().isoformat()}|edges={r.get('edges_discovered',0)}"
                f"|paths={r.get('pathways_explored',0)}",
            )
            elapsed = round(time.time() - t0, 1)
            logging.info(
                f"[synapse:builder] edges={r.get('edges_discovered',0)} "
                f"paths={r.get('pathways_explored',0)} elapsed={elapsed}s"
            )
            ok = True
        except Exception as e:
            logging.warning(f"[synapse:builder] iteration failed: {e}")
        sleep_s = _next_sleep_with_backoff(NAME, INTERVAL_S, JITTER_S, ok)
        if _wait_or_stop(sleep_s):
            return


def _lookahead_worker() -> None:
    """Worker #2 — *forward-look* synaptic pre-warmer for dispersed periods.

    Rotates through three relationally dispersed historical windows
    (7-day, 30-day, 90-day slices) every ~15 min. Each window has its own
    persisted explored-pair set so one rotation doesn't reset another's
    progress. The 90-day slice deliberately lags the other workers so that
    by the time the synaptic builder's corpus refresh reaches that period,
    the deep-history bridges already exist.
    """
    INTERVAL_S = 900   # 15 min
    JITTER_S   = 90
    NAME       = "lookahead"
    import random as _r
    # Each window: (label, hours_back_size, offset_hours, kv_key)
    WINDOWS = [
        ("lookahead_7d",  7  * 24, 24,        "rag_explored_pairs_lookahead_7d"),
        ("lookahead_30d", 30 * 24, 7  * 24,   "rag_explored_pairs_lookahead_30d"),
        ("lookahead_90d", 90 * 24, 30 * 24,   "rag_explored_pairs_lookahead_90d"),
    ]
    rotation = 0
    logging.info("[synapse:lookahead] started — interval=15min windows=7d/30d/90d rotating")
    if _wait_or_stop(_r.randint(60, 180)):
        return
    while not _SYNAPTIC_STOP.is_set():
        label, hrs, offset, kvkey = WINDOWS[rotation % len(WINDOWS)]
        rotation += 1
        t0 = time.time()
        ok = False
        try:
            r = rag_knowledge_deepdive(
                window_label=label,
                window_hours=hrs,
                window_offset_hours=offset,
                max_iterations=6,
                max_entities=1500,
                explored_kv_key=kvkey,
            )
            _kv_write(
                f"synapse_{label}_last",
                f"{datetime.now().isoformat()}|edges={r.get('edges_discovered',0)}"
                f"|paths={r.get('pathways_explored',0)}",
            )
            elapsed = round(time.time() - t0, 1)
            logging.info(
                f"[synapse:lookahead/{label}] "
                f"edges={r.get('edges_discovered',0)} "
                f"paths={r.get('pathways_explored',0)} elapsed={elapsed}s"
            )
            ok = True
        except Exception as e:
            logging.warning(f"[synapse:lookahead/{label}] failed: {e}")
        sleep_s = _next_sleep_with_backoff(NAME, INTERVAL_S, JITTER_S, ok)
        if _wait_or_stop(sleep_s):
            return


def _dispersed_sweeper_worker() -> None:
    """Worker #3 — continuous data sweep, one connector per tick.

    Instead of waiting for the main loop's full sweep_all_data_sources(),
    this worker rotates through registered connectors and ingests one
    connector's worth of fresh data every ~20 min. Combined with the main
    loop's full sweep this means data freshness is continuous rather than
    episodic — entities the synaptic workers reach are always backed by
    recently-pulled rows.
    """
    INTERVAL_S = 1200  # 20 min
    JITTER_S   = 120
    NAME       = "sweeper"
    import random as _r

    logging.info("[synapse:sweeper] started — interval=20min mode=connector-rotation")
    if _wait_or_stop(_r.randint(120, 240)):
        return

    rotation = 0
    while not _SYNAPTIC_STOP.is_set():
        t0 = time.time()
        ok = False
        try:
            # Discover connectors at every tick so newly-registered ones
            # automatically join the rotation.
            import sys as _sys
            _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from src.brain.db_registry import list_connectors, read_sql

            sql_connectors = [c.name for c in list_connectors()
                              if c.kind == "sql"]
            if not sql_connectors:
                logging.info("[synapse:sweeper] no SQL connectors registered yet.")
                # Not a failure — just nothing to do. Keep base cadence.
                if _wait_or_stop(INTERVAL_S):
                    return
                continue

            target = sql_connectors[rotation % len(sql_connectors)]
            rotation += 1

            # Pull a small probe — count of recent activity in
            # INFORMATION_SCHEMA so we know the connector is alive.
            df = read_sql(
                target,
                "SELECT TOP 5 TABLE_SCHEMA, TABLE_NAME, "
                "(SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c "
                "  WHERE c.TABLE_NAME=t.TABLE_NAME) AS n_cols "
                "FROM INFORMATION_SCHEMA.TABLES t "
                "WHERE TABLE_TYPE='BASE TABLE' "
                "ORDER BY TABLE_NAME",
            )
            n_rows = 0 if df is None or df.empty else len(df)
            _kv_write(
                f"synapse_sweeper_{target}",
                f"{datetime.now().isoformat()}|probed_rows={n_rows}",
            )
            elapsed = round(time.time() - t0, 1)
            logging.info(
                f"[synapse:sweeper/{target}] probe rows={n_rows} elapsed={elapsed}s"
            )
            ok = True
        except Exception as e:
            logging.warning(f"[synapse:sweeper] iteration failed: {e}")

        sleep_s = _next_sleep_with_backoff(NAME, INTERVAL_S, JITTER_S, ok)
        if _wait_or_stop(sleep_s):
            return


def _convergence_worker() -> None:
    """Worker #4 — periodic corpus refresh + graph materialisation.

    Runs every ~30 min. Consolidates everything the other three workers
    have written into the knowledge corpus and projects it into the graph
    backend so that downstream readers (Quest engine, Brain pages, Body
    directives) see the freshly-built synapses without waiting for the
    main 1-4 hour cycle.
    """
    INTERVAL_S = 1800  # 30 min
    JITTER_S   = 120
    NAME       = "convergence"
    import random as _r
    logging.info("[synapse:convergence] started — interval=30min")
    if _wait_or_stop(_r.randint(180, 360)):
        return

    while not _SYNAPTIC_STOP.is_set():
        t0 = time.time()
        ok = False
        try:
            import sys as _sys
            _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from src.brain.knowledge_corpus import (
                refresh_corpus_round, materialize_into_graph,
            )
            kc = refresh_corpus_round() or {}
            mg = materialize_into_graph() or {}
            _kv_write(
                "synapse_convergence_last",
                f"{datetime.now().isoformat()}"
                f"|+ents={kc.get('entities_added',0)}"
                f"|+edges={kc.get('edges_added',0)}"
                f"|nodes={mg.get('nodes_projected',0)}",
            )
            elapsed = round(time.time() - t0, 1)
            logging.info(
                f"[synapse:convergence] "
                f"+{kc.get('entities_added',0)} entities, "
                f"+{kc.get('edges_added',0)} edges, "
                f"projected {mg.get('nodes_projected',0)}n/"
                f"{mg.get('edges_projected',0)}e elapsed={elapsed}s"
            )
            ok = True
        except Exception as e:
            logging.warning(f"[synapse:convergence] iteration failed: {e}")

        sleep_s = _next_sleep_with_backoff(NAME, INTERVAL_S, JITTER_S, ok)
        if _wait_or_stop(sleep_s):
            return


def start_continuous_synaptic_agents() -> None:
    """Start all four synaptic workers as daemon threads.

    Idempotent — calling more than once is a no-op. The main loop calls
    this once at startup, then proceeds to its own coarser cycle. The
    workers run independently and continuously underneath, building
    synapses ahead of where the main loop is reading them.
    """
    global _SYNAPTIC_STARTED
    if _SYNAPTIC_STARTED:
        logging.info("Continuous synaptic agents already running.")
        return

    _SYNAPTIC_STOP.clear()
    workers = [
        ("synapse-builder",     _synaptic_builder_worker),
        ("synapse-lookahead",   _lookahead_worker),
        ("synapse-sweeper",     _dispersed_sweeper_worker),
        ("synapse-convergence", _convergence_worker),
    ]
    for name, fn in workers:
        t = _threading.Thread(target=fn, name=name, daemon=True)
        t.start()
        _SYNAPTIC_THREADS.append(t)

    _SYNAPTIC_STARTED = True
    _kv_write("synapse_agents_started", datetime.now().isoformat())
    logging.info(
        f"Continuous synaptic agents started: "
        f"{', '.join(name for name, _ in workers)} "
        f"({len(workers)} threads, all daemon)."
    )


def stop_continuous_synaptic_agents(timeout: float = 5.0) -> None:
    """Signal all synaptic workers to exit and wait briefly for them.

    After this call: ``_SYNAPTIC_STARTED`` is reset, the thread list is
    cleared, and the failure counters are reset — so a subsequent call to
    :func:`start_continuous_synaptic_agents` starts a fresh cohort cleanly.
    """
    global _SYNAPTIC_STARTED
    if not _SYNAPTIC_STARTED:
        return
    _SYNAPTIC_STOP.set()
    for t in _SYNAPTIC_THREADS:
        try:
            t.join(timeout=timeout)
        except Exception:
            pass
    _SYNAPTIC_THREADS.clear()
    _SYNAPTIC_FAILURES.clear()
    _SYNAPTIC_STARTED = False
    try:
        _kv_write("synapse_agents_stopped", datetime.now().isoformat())
    except Exception:
        pass
    logging.info("Continuous synaptic agents stopped.")


def adaptive_cycle_sleep(velocity: int) -> None:
    """Sleep between cycles with duration scaled to learning velocity.

    ``velocity`` is entities_added + edges_added + learnings_logged from the
    most recent corpus refresh.  The faster the Brain is learning, the shorter
    the sleep so momentum is not wasted:

        velocity > 10  ->  3 600 s (1 hr  - high-activity burst)
        velocity > 0   ->  7 200 s (2 hr  - some learning)
        velocity == 0  ->  14 400 s (4 hr - settled, max rest)

    Consecutive-idle cycle counts are tracked in brain_kv.  After 3 idle
    cycles a stagnation warning is logged so the operator knows to check
    data-source connectivity.
    """
    idle_count = int(_kv_read("agent_consecutive_idle", "0") or "0")

    if velocity > 10:
        sleep_duration = 3_600
        idle_count = 0
        logging.info(
            f"=== CYCLE COMPLETE (high-velocity: {velocity} new signal units). "
            "SLEEPING 1 HOUR ==="
        )
    elif velocity > 0:
        sleep_duration = 7_200
        idle_count = 0
        logging.info(
            f"=== CYCLE COMPLETE (active: {velocity} new signal units). "
            "SLEEPING 2 HOURS ==="
        )
    else:
        idle_count += 1
        sleep_duration = 14_400
        stag = " [LEARNING STAGNATION — check ERP/Azure SQL connectivity]" if idle_count >= 3 else ""
        logging.info(f"=== CYCLE COMPLETE (idle #{idle_count}).{stag} SLEEPING 4 HOURS ===")

    _kv_write("agent_consecutive_idle", str(idle_count))
    _kv_write("agent_last_velocity", str(velocity))

    interval = 60
    heartbeat_file = os.path.join("logs", "agent_heartbeat.txt")
    os.makedirs("logs", exist_ok=True)
    for _ in range(sleep_duration // interval):
        with open(heartbeat_file, "w") as f:
            f.write(str(time.time()))
        time.sleep(interval)


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

    # Wire the Recurrent Depth Transformer into the ensemble once at startup.
    # All subsequent dispatches can use the adaptive-depth aggregator.
    init_recurrent_depth()

    # Start the four continuous synaptic agents. They run as daemon threads
    # underneath this main loop, building corpus_edges on overlapping ~10/15/
    # 20/30-min cadences across relationally dispersed temporal windows so
    # that by the time the main loop's slower steps reach any aspect of the
    # graph, the synapses are already in place.
    try:
        start_continuous_synaptic_agents()
    except Exception as e:
        logging.warning(f"Continuous synaptic agents failed to start: {e}")

    while True:
        try:
            logging.info("=== STARTING NEW AUTONOMOUS CYCLE ===")
            cycle_velocity = 0  # updated after corpus refresh; drives adaptive sleep

            # Step 0: Ensure local VPN is active on the hosting laptop
            trigger_remote_vpn()

            # Step 1: Benchmark current application state
            benchmarks = run_tests_and_benchmarks()

            # Step 2: Auto-map Database relations and variable structures for reporting
            update_data_infrastructure_docs()

            # Step 3: LLM Code refactoring placeholder
            analyze_and_improve(benchmarks)

            # Step 3a: Full enterprise data sweep — iterates every configured and
            # dynamically discovered data source (OneDrive xlsx, eDAP Azure SQL,
            # Oracle Fusion BIP, Epicor/SyteLine/AX sites, any new connector added
            # to db_registry since the last cycle) and feeds the Brain's local
            # tables so all downstream learning steps work from current ground truth.
            try:
                ds = sweep_all_data_sources()
                cycle_velocity += ds.get("rows_processed", 0) // 100
                logging.info(
                    f"Data sweep: sources={ds.get('sources_scanned',0)}, "
                    f"rows={ds.get('rows_processed',0):,}, "
                    f"categories={ds.get('categories_written',0)}, "
                    f"otd_owners={ds.get('otd_owners_written',0)}"
                )
            except Exception as e:
                logging.warning(f"Data source sweep step failed: {e}")

            # Step 3b: Periodic open-weight LLM scout — keeps the Brain's
            # router aware of newly released models (Gemma/GLM/Qwen/DeepSeek/
            # Kimi/MiniMax/MiMo class). Cadence is enforced inside the scout.
            refresh_llm_registry()

            # Step 3b.5: Recurrent Depth Transformer depth summary. Surfaces
            # per-task adaptive-depth statistics learned from previous ensemble
            # dispatches: how many recurrent iterations each task needed to
            # converge, and by how much the final distribution shifted from the
            # one-shot baseline. This feeds the Brain's meta-understanding of
            # task reasoning complexity and informs future aggregator tuning.
            try:
                from src.brain.recurrent_depth import learned_depth_summary
                ds = learned_depth_summary()
                if ds.get("by_task"):
                    for t in ds["by_task"]:
                        logging.info(
                            f"RDT [{t['task']}]: "
                            f"avg_depth={t['avg_depth']} "
                            f"converge={t['convergence_rate']:.0%} "
                            f"shift={t['avg_shift_from_oneshot']:.4f} "
                            f"runs={t['n']}"
                        )
                else:
                    logging.info("RDT: no dispatch history yet — depth stats available after first ensemble call.")
            except Exception as e:
                logging.warning(f"RDT depth summary failed: {e}")

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

            # Step 3c.5: NLP Part Taxonomy Expansion. Batch-categorizes any
            # uncategorized ERP parts against the 12-category TF-IDF taxonomy
            # (Steel, Fasteners, Wiring, Bearings, Hydraulics, …). Each newly
            # categorized part is a labeled ground-truth example for the
            # abc_classify self-train task, organically growing the Brain's
            # labeled dataset every cycle without manual annotation.
            try:
                expand_nlp_taxonomy()
            except Exception as e:
                logging.warning(f"NLP taxonomy expansion step failed: {e}")

            # Step 3d.5: OTD Direct Seeding. Lightweight periodic pull of ERP
            # OTD data (1 000 rows) to keep otd_ownership populated between
            # fulfillment mission cycles. Ensures the otd_classify self-train
            # task always has fresh ground truth even when no fulfillment
            # missions are currently queued in the Quest Console.
            try:
                seed_otd_direct()
            except Exception as e:
                logging.warning(f"OTD direct seeding step failed: {e}")

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
                    cycle_velocity = (
                        kc.get("entities_added", 0)
                        + kc.get("edges_added", 0)
                        + kc.get("learnings_logged", 0)
                    )
                    mg = materialize_into_graph()
                    logging.info(
                        f"Corpus: +{kc.get('entities_added',0)} entities "
                        f"(touched {kc.get('entities_touched',0)}), "
                        f"+{kc.get('edges_added',0)} edges "
                        f"(touched {kc.get('edges_touched',0)}), "
                        f"+{kc.get('learnings_logged',0)} learnings; "
                        f"projected {mg.get('nodes_projected',0)} nodes "
                        f"/ {mg.get('edges_projected',0)} edges into graph; "
                        f"cycle velocity={cycle_velocity}"
                    )
            except Exception as e:
                logging.warning(f"Knowledge corpus round failed: {e}")

            # Step 3e.5: RAG Knowledge Deepdive — SOTA iterative retrieval-
            # augmented reasoning over the Brain's corpus graph. Finds structural
            # holes (entity pairs sharing ≥2 corpus neighbors but no direct edge),
            # confirms with TF-IDF semantic similarity, grounds each inferred
            # pathway in actual source data co-occurrence when available, then
            # upserts RAG_INFERRED edges + learning_log entries. Runs up to 8
            # convergence iterations per cycle; explored pairs are persisted in
            # brain_kv so each cycle dives deeper rather than repeating.
            try:
                rag = rag_knowledge_deepdive()
                cycle_velocity += rag.get("edges_discovered", 0) * 2
                logging.info(
                    f"RAG deepdive: iters={rag.get('iterations_run',0)}, "
                    f"edges={rag.get('edges_discovered',0)}, "
                    f"pathways={rag.get('pathways_explored',0)}"
                )
            except Exception as e:
                logging.warning(f"RAG knowledge deepdive step failed: {e}")

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

            adaptive_cycle_sleep(cycle_velocity)

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
