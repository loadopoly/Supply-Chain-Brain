"""
Epicor 9 SQL Server connector.

Epicor 9 stores all transactional data in a SQL Server database.  Each site
can have its own SQL Server instance or share one with a separate database per
company.  This module reads the site-specific config block from
connections.yaml and handles authentication identically to azure_sql.py:

  1. DPAPI vault (non-interactive, preferred for pipelines)
  2. Windows Integrated / Kerberos (ActiveDirectoryIntegrated via domain join)
  3. SQL auth fallback (env vars EPICOR_<SITE>_USER / EPICOR_<SITE>_PASS)

Config section name pattern: ``epicor_<site_key>``
  e.g. epicor_jerome_ave, epicor_manufacturers_rd, epicor_wilson_rd

Supported site keys (must match connections.yaml top-level keys):
  jerome_ave        — Chattanooga Jerome Avenue
  manufacturers_rd  — Chattanooga Manufacturers Road
  wilson_rd         — Chattanooga Wilson Road

Usage:
    from src.connections.epicor import get_connection
    conn = get_connection("jerome_ave")
    conn = get_connection("manufacturers_rd")
"""
from __future__ import annotations

import os
import pyodbc
import yaml
from pathlib import Path
from typing import Optional

from . import secrets as _secrets


def _load_config(site_key: str) -> dict:
    cfg_path = Path(__file__).parent.parent.parent / "config" / "connections.yaml"
    with open(cfg_path) as f:
        full = yaml.safe_load(f)
    key = f"epicor_{site_key}"
    if key not in full:
        raise KeyError(
            f"No connections.yaml section '{key}'. "
            f"Known epicor sites: {[k for k in full if k.startswith('epicor_')]}"
        )
    return full[key]


def _resolve_env(cfg: dict, field: str, env_var: str) -> str:
    """Return cfg[field] if set, otherwise os.environ[env_var]."""
    val = cfg.get(field, "").strip()
    if val:
        return val
    env_val = os.environ.get(env_var, "").strip()
    if env_val:
        return env_val
    raise RuntimeError(
        f"Epicor config missing '{field}' and env var '{env_var}' is not set. "
        f"Add it to connections.yaml or set the environment variable."
    )


def _build_conn_str(
    cfg: dict,
    site_key: str,
    *,
    user: Optional[str] = None,
    password: Optional[str] = None,
    auth_override: Optional[str] = None,
) -> str:
    server = _resolve_env(cfg, "server", f"EPICOR_{site_key.upper()}_SERVER")
    database = _resolve_env(cfg, "database", f"EPICOR_{site_key.upper()}_DATABASE")
    driver = cfg.get("driver", "ODBC Driver 18 for SQL Server")
    encrypt = cfg.get("encrypt", "yes")
    trust_cert = cfg.get("trust_server_certificate", "yes")   # internal CA
    timeout = cfg.get("connection_timeout", 30)
    auth = auth_override or cfg.get("authentication", "ActiveDirectoryIntegrated")

    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        f"DATABASE={database}",
        f"Encrypt={encrypt}",
        f"TrustServerCertificate={trust_cert}",
        f"Connection Timeout={timeout}",
        f"Authentication={auth}",
    ]
    if user:
        parts.append(f"UID={user}")
    if password:
        parts.append(f"PWD={password}")
    return ";".join(parts) + ";"


def get_connection(site_key: str) -> pyodbc.Connection:
    """
    Open a pyodbc connection to the named Epicor 9 site.

    Args:
        site_key: one of ``jerome_ave``, ``manufacturers_rd``, ``wilson_rd``
                  (or any key with a matching ``epicor_<site_key>`` config block).

    Returns:
        A live ``pyodbc.Connection`` with ``timeout`` set to 30 s.
    """
    cfg = _load_config(site_key)
    server_display = cfg.get("server") or f"$EPICOR_{site_key.upper()}_SERVER"
    db_display = cfg.get("database") or f"$EPICOR_{site_key.upper()}_DATABASE"
    print(f"[Epicor {site_key}] Connecting to {server_display} / {db_display} ...")

    vault_scope = f"epicor_{site_key}"
    use_vault = os.environ.get("ASTEC_DISABLE_VAULT") != "1"
    stored = _secrets.get_credentials(vault_scope) if use_vault else None

    if stored and stored.get("password"):
        upn = stored.get("user") or os.environ.get(f"EPICOR_{site_key.upper()}_USER", "")
        print(f"[Epicor {site_key}] Using vault credentials for {upn} (SQL auth).")
        try:
            conn = pyodbc.connect(
                _build_conn_str(cfg, site_key, user=upn, password=stored["password"],
                                auth_override="SqlPassword")
            )
            conn.timeout = 30
            print(f"[Epicor {site_key}] Connected.")
            return conn
        except pyodbc.Error as exc:
            print(
                f"[Epicor {site_key}] Vault login failed ({exc.args[0] if exc.args else exc}); "
                "falling back to integrated auth."
            )

    # Try Windows Integrated / Kerberos (domain-joined machine)
    auth = cfg.get("authentication", "ActiveDirectoryIntegrated")
    print(f"[Epicor {site_key}] Using {auth} ...")
    conn = pyodbc.connect(_build_conn_str(cfg, site_key))
    conn.timeout = 30
    print(f"[Epicor {site_key}] Connected.")
    return conn


def is_alive(conn: pyodbc.Connection) -> bool:
    """Return True if the connection is still usable."""
    try:
        conn.cursor().execute("SELECT 1")
        return True
    except Exception:
        return False


def get_or_reconnect(site_key: str, conn: pyodbc.Connection) -> pyodbc.Connection:
    """Return ``conn`` if alive, otherwise open a fresh connection."""
    if is_alive(conn):
        return conn
    return get_connection(site_key)
