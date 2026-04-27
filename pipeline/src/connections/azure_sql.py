"""
Azure SQL connector — prefers stored credentials (ActiveDirectoryPassword,
non-interactive) when present in the DPAPI vault, otherwise falls back to
ActiveDirectoryInteractive (Entra MFA browser popup) configured in
connections.yaml.

To save credentials once:
    python -m src.connections.secrets set azure_sql --user <upn> --password <pw>
"""

import os
import pyodbc
import yaml
from pathlib import Path

from . import secrets as _secrets


def _load_config() -> dict:
    cfg_path = Path(__file__).parent.parent.parent / "config" / "connections.yaml"
    with open(cfg_path) as f:
        return yaml.safe_load(f)["azure_sql"]


def _build_conn_str(cfg: dict, *, password: str | None = None, auth_override: str | None = None) -> str:
    auth = auth_override or cfg["authentication"]
    parts = [
        f"DRIVER={{{cfg['driver']}}}",
        f"SERVER={cfg['server']}",
        f"DATABASE={cfg['database']}",
        f"UID={cfg['user']}",
        f"Authentication={auth}",
        f"Encrypt={cfg['encrypt']}",
        f"TrustServerCertificate={cfg['trust_server_certificate']}",
        f"Connection Timeout={cfg['connection_timeout']}",
    ]
    if password:
        parts.append(f"PWD={password}")
    return ";".join(parts) + ";"


def get_connection() -> pyodbc.Connection:
    cfg = _load_config()
    print(f"[Azure SQL] Connecting to {cfg['server']} / {cfg['database']} ...")

    use_vault = os.environ.get("ASTEC_DISABLE_VAULT") != "1"
    stored = _secrets.get_credentials("azure_sql") if use_vault else None
    if stored and stored.get("password"):
        upn = stored.get("user") or cfg["user"]
        cfg = {**cfg, "user": upn}
        print(f"[Azure SQL] Using stored credentials for {upn} (ActiveDirectoryPassword).")
        try:
            conn = pyodbc.connect(_build_conn_str(cfg, password=stored["password"], auth_override="ActiveDirectoryPassword"))
            conn.timeout = int(cfg.get("query_timeout", 120))
            print("[Azure SQL] Connected.")
            return conn
        except pyodbc.Error as exc:
            print(f"[Azure SQL] Stored-credential login failed ({exc.args[0] if exc.args else exc}); "
                  "falling back to interactive auth.")

    print("[Azure SQL] A browser MFA popup may appear — complete sign-in to continue.")
    conn = pyodbc.connect(_build_conn_str(cfg))
    conn.timeout = int(cfg.get("query_timeout", 120))
    print("[Azure SQL] Connected.")
    return conn


def is_alive(conn: pyodbc.Connection) -> bool:
    """Return True if the connection is still usable."""
    try:
        conn.cursor().execute("SELECT 1")
        return True
    except Exception:
        return False


def get_or_reconnect(conn: pyodbc.Connection) -> pyodbc.Connection:
    """Return conn if alive, otherwise open a fresh connection."""
    if is_alive(conn):
        return conn
    print("[Azure SQL] Connection lost — reconnecting ...")
    return get_connection()


def list_schemas(conn: pyodbc.Connection) -> list[str]:
    cursor = conn.cursor()
    cursor.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME")
    return [row[0] for row in cursor.fetchall()]


def list_tables(conn: pyodbc.Connection, schema: str = "dbo") -> list[dict]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
        ORDER BY TABLE_NAME
        """,
        schema,
    )
    return [{"schema": r[0], "table": r[1], "type": r[2]} for r in cursor.fetchall()]


def list_columns(conn: pyodbc.Connection, schema: str, table: str) -> list[dict]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        schema,
        table,
    )
    return [
        {
            "column": r[0],
            "type": r[1],
            "nullable": r[2],
            "max_length": r[3],
            "precision": r[4],
        }
        for r in cursor.fetchall()
    ]
