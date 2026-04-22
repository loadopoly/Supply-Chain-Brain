"""
pipeline.py — main CLI orchestrator.

Usage:
    python pipeline.py discover      # Schema discovery for both sources
    python pipeline.py extract       # Extract sample data
    python pipeline.py transform     # Transform using mappings.yaml
    python pipeline.py load          # Load/upsert into Azure SQL staging
    python pipeline.py sync          # Reconcile Oracle Fusion vs Azure SQL
    python pipeline.py run           # Full pipeline (extract→transform→load→sync)
    python pipeline.py test-azure    # Quick Azure SQL connection test
    python pipeline.py test-oracle   # Quick Oracle Fusion connection test
    python pipeline.py deck [--site NAME] [--demo] [--out path.pptx]
                                     # Cross-Dataset Supply-Chain Review deck
                                     # Implements CrossDataset_Agent_Process_Spec.md
"""

import sys
import yaml
from pathlib import Path

# Allow running from the pipeline/ directory
sys.path.insert(0, str(Path(__file__).parent))

from src.connections import azure_sql
from src.connections.oracle_fusion import OracleFusionSession
from src.discovery import schema_discovery
from src.extract.extractor import extract_azure_table, extract_oracle_table
from src.transform.transformer import transform
from src.load.loader import upsert
from src.sync.reconciler import reconcile, print_summary, export_report


MAPPINGS_FILE = Path(__file__).parent / "config" / "mappings.yaml"


def load_mappings() -> list[dict]:
    with open(MAPPINGS_FILE) as f:
        return yaml.safe_load(f).get("mappings", [])


def cmd_test_azure():
    print("=== Azure SQL Connection Test ===")
    conn = azure_sql.get_connection()
    schemas = azure_sql.list_schemas(conn)
    print(f"Schemas found: {schemas}")
    tables = azure_sql.list_tables(conn, schemas[0] if schemas else "dbo")
    print(f"Tables in first schema: {[t['table'] for t in tables[:10]]}")
    conn.close()
    print("=== Azure SQL OK ===\n")


def cmd_test_oracle():
    print("=== Oracle Fusion Connection Test ===")
    session = OracleFusionSession()
    session.connect()
    try:
        areas = session.list_subject_areas()
        print(f"Subject areas (first 5): {areas[:5]}")
    except Exception as e:
        print(f"Subject areas failed: {e}")
        print("Trying SQL test ...")
        df = session.execute_sql("SELECT SYSDATE FROM DUAL")
        print(f"SYSDATE: {df}")
    print("=== Oracle Fusion OK ===\n")


def cmd_discover():
    schema_discovery.run()


def cmd_extract():
    mappings = load_mappings()
    if not mappings:
        print("No mappings configured. Run 'discover' first, then populate config/mappings.yaml.")
        return

    conn = azure_sql.get_connection()
    session = OracleFusionSession()
    session.connect()

    for m in mappings:
        print(f"\n--- Extracting {m['oracle_table']} from Oracle Fusion ---")
        sql = m.get("oracle_query", f"SELECT * FROM {m.get('oracle_schema', 'FUSION')}.{m['oracle_table']}")
        if "oracle_query" not in m:
            sql += " WHERE ROWNUM <= 1000"
        df = extract_oracle_table(session, sql)
        print(df.head())

        print(f"\n--- Extracting {m['azure_table']} from Azure SQL ---")
        df_az = extract_azure_table(conn, m.get("azure_schema", "dbo"), m["azure_table"])
        print(df_az.head())

    conn.close()


def cmd_transform():
    mappings = load_mappings()
    if not mappings:
        print("No mappings configured.")
        return

    session = OracleFusionSession()
    session.connect()

    for m in mappings:
        sql = m.get("oracle_query", f"SELECT * FROM {m.get('oracle_schema', 'FUSION')}.{m['oracle_table']}")
        if "oracle_query" not in m:
            sql += " WHERE ROWNUM <= 100"
        df_raw = extract_oracle_table(session, sql)
        df_clean = transform(df_raw, m)
        print(f"\n[{m['oracle_table']}] Before: {df_raw.shape} → After: {df_clean.shape}")
        print(df_clean.head())


def cmd_load():
    mappings = load_mappings()
    if not mappings:
        print("No mappings configured.")
        return

    conn = azure_sql.get_connection()
    session = OracleFusionSession()
    session.connect()

    for m in mappings:
        sql = m.get("oracle_query", f"SELECT * FROM {m.get('oracle_schema', 'FUSION')}.{m['oracle_table']}")
        df = extract_oracle_table(session, sql)
        df = transform(df, m)
        upsert(
            conn,
            df,
            schema=m.get("azure_schema", "dbo"),
            table=m["azure_table"],
            key_columns=m.get("key_columns", []),
        )

    conn.close()
    print("Load complete.")


def cmd_sync():
    mappings = load_mappings()
    if not mappings:
        print("No mappings configured.")
        return

    conn = azure_sql.get_connection()
    session = OracleFusionSession()
    session.connect()

    for m in mappings:
        sql = m.get("oracle_query", f"SELECT * FROM {m.get('oracle_schema', 'FUSION')}.{m['oracle_table']}")
        oracle_df = extract_oracle_table(session, sql)
        oracle_df = transform(oracle_df, m)

        azure_df = extract_azure_table(conn, m.get("azure_schema", "dbo"), m["azure_table"])

        result = reconcile(oracle_df, azure_df, key_columns=m.get("key_columns", []))
        print_summary(result)
        export_report(result, f"reconciliation_{m['azure_table']}.xlsx")

    conn.close()


def cmd_run():
    cmd_discover()
    cmd_extract()
    cmd_transform()
    cmd_load()
    cmd_sync()


def cmd_deck(argv: list[str] | None = None):
    """Render the Cross-Dataset Supply-Chain Review deck.

    --site NAME   limit scope to a single site (default: ALL / portfolio view)
    --demo        use synthetic data from src.deck.demo instead of the live DBs
    --out PATH    output .pptx path (default: snapshots/cross_dataset_review_<date>.pptx)
    --json PATH   also write the Phase 8a findings JSON to PATH
    """
    import argparse
    from datetime import date as _date
    from src.deck import build_findings, render_pptx
    from src.deck.builder import dump_findings_json
    from src.deck import demo as deck_demo
    from src.deck.live import load_live_datasets

    parser = argparse.ArgumentParser(prog="pipeline.py deck")
    parser.add_argument("--site", default="ALL")
    parser.add_argument("--demo", action="store_true",
                        help="use synthetic data instead of the configured live databases")
    parser.add_argument("--out", default=None)
    parser.add_argument("--json", default=None)
    parser.add_argument("--template", default=None, help="PPTX template to use as a base")
    args = parser.parse_args(argv or sys.argv[2:])

    if args.demo:
        data = deck_demo.make_all()
    else:
        live = load_live_datasets(site=args.site)
        data = {
            "otd": live.otd,
            "ifr": live.ifr,
            "itr": live.itr,
            "pfep": live.pfep,
        }
        for warning in live.warnings:
            print(f"[deck] warning: {warning}")

    findings = build_findings(
        data["otd"], data["ifr"], data["itr"], data["pfep"], site=args.site
    )

    snap_dir = Path(__file__).parent / "snapshots"
    snap_dir.mkdir(exist_ok=True)
    stamp = _date.today().strftime("%Y%m%d")
    default_out = snap_dir / f"cross_dataset_review_{args.site.replace(' ', '_')}_{stamp}.pptx"
    out = Path(args.out) if args.out else default_out
    render_pptx(findings, out, template_path=args.template)
    print(f"wrote {out}")

    if args.json:
        jpath = dump_findings_json(findings, args.json)
        print(f"wrote {jpath}")


COMMANDS = {
    "discover": cmd_discover,
    "extract": cmd_extract,
    "transform": cmd_transform,
    "load": cmd_load,
    "sync": cmd_sync,
    "run": cmd_run,
    "test-azure": cmd_test_azure,
    "test-oracle": cmd_test_oracle,
    "deck": cmd_deck,
}


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(__doc__)
        print(f"Available commands: {', '.join(COMMANDS)}")
        sys.exit(1)

    COMMANDS[sys.argv[1]]()
