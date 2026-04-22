"""
Schema discovery — enumerate tables and columns from both sources,
print a formatted report, and write discovered_schema.yaml.
"""

import yaml
from pathlib import Path
from tabulate import tabulate

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.connections import azure_sql
from src.connections.oracle_fusion import OracleFusionSession


OUTPUT_FILE = Path(__file__).parent.parent.parent / "discovered_schema.yaml"


def discover_azure_sql() -> dict:
    conn = azure_sql.get_connection()
    schemas = azure_sql.list_schemas(conn)
    result = {}
    for schema in schemas:
        tables = azure_sql.list_tables(conn, schema)
        if not tables:
            continue
        result[schema] = {}
        for t in tables:
            cols = azure_sql.list_columns(conn, schema, t["table"])
            result[schema][t["table"]] = cols
    conn.close()
    return result


def discover_oracle_fusion() -> dict:
    session = OracleFusionSession()
    session.connect()
    result = {}
    try:
        areas = session.list_subject_areas()
        if isinstance(areas, list):
            for area in areas[:20]:  # cap discovery at 20 subject areas
                name = area if isinstance(area, str) else area.get("name", str(area))
                try:
                    tables = session.list_tables(name)
                    result[name] = [
                        t if isinstance(t, str) else t.get("name", str(t))
                        for t in (tables or [])
                    ]
                except Exception:
                    result[name] = []
        else:
            result["_raw"] = areas
    except Exception as e:
        print(f"[Oracle Fusion] Subject area discovery failed: {e}")
        print("[Oracle Fusion] Falling back to SQL-based table discovery ...")
        try:
            df = session.execute_sql(
                "SELECT TABLE_NAME, TABLE_TYPE FROM ALL_TABLES WHERE ROWNUM <= 200 ORDER BY TABLE_NAME"
            )
            result["ALL_TABLES"] = df.to_dict(orient="records")
        except Exception as e2:
            print(f"[Oracle Fusion] SQL discovery also failed: {e2}")
            result["error"] = str(e2)
    return result


def print_azure_report(schema_data: dict):
    print("\n" + "=" * 60)
    print("AZURE SQL — SCHEMA DISCOVERY")
    print("=" * 60)
    rows = []
    for schema, tables in schema_data.items():
        for table, cols in tables.items():
            rows.append([schema, table, len(cols)])
    print(tabulate(rows, headers=["Schema", "Table", "# Columns"], tablefmt="simple"))
    print(f"\nTotal: {sum(len(t) for t in schema_data.values())} tables across {len(schema_data)} schemas\n")


def print_oracle_report(schema_data: dict):
    print("\n" + "=" * 60)
    print("ORACLE FUSION CLOUD — SCHEMA DISCOVERY")
    print("=" * 60)
    rows = []
    for area, tables in schema_data.items():
        if isinstance(tables, list):
            rows.append([area, len(tables)])
        else:
            rows.append([area, "?"])
    print(tabulate(rows, headers=["Subject Area / Schema", "# Tables"], tablefmt="simple"))
    print()


def run():
    print("Starting schema discovery for both sources ...\n")

    azure_data = {}
    oracle_data = {}

    print(">>> Azure SQL")
    try:
        azure_data = discover_azure_sql()
        print_azure_report(azure_data)
    except Exception as e:
        print(f"[Azure SQL] Discovery failed: {e}")

    print(">>> Oracle Fusion Cloud")
    try:
        oracle_data = discover_oracle_fusion()
        print_oracle_report(oracle_data)
    except Exception as e:
        print(f"[Oracle Fusion] Discovery failed: {e}")

    output = {
        "azure_sql": azure_data,
        "oracle_fusion": oracle_data,
    }

    with open(OUTPUT_FILE, "w") as f:
        yaml.dump(output, f, default_flow_style=False, allow_unicode=True)

    print(f"Schema written to: {OUTPUT_FILE}")
    return output


if __name__ == "__main__":
    run()
