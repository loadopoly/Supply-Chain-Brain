"""
Microsoft Dynamics AX connector — Eugene Airport Rd (site: airport_rd).

Dynamics AX (AX 2012 or D365 F&O) exposes data via:
  1. Direct SQL Server connection to AX business database (AOSDatabase / axdb)
  2. OData REST endpoint  (D365 F&O only)
  3. Excel/CSV export fallback — handled by xlsx_extractor.py

This module implements the SQL Server path (AX 2012 style, matching the
IIJ* journal IDs seen in the OneDrive exports).

Config block in connections.yaml:
  ax_airport_rd:
    driver:   "ODBC Driver 18 for SQL Server"
    server:   ""            # fill in: <SQL_SERVER_HOST>
    database: "MicrosoftDynamicsAX"   # default AX 2012 business DB name
    schema:   "dbo"
    auth:     "ActiveDirectoryIntegrated"  # or sql_auth
    user:     ""
    password: ""
    timeout:  30

Environment variable fallbacks:
  AX_AIRPORT_RD_SERVER   AX_AIRPORT_RD_DATABASE
  AX_AIRPORT_RD_USER     AX_AIRPORT_RD_PASS
"""
from __future__ import annotations

import os
import yaml
from pathlib import Path
from typing import Optional

# Same optional import pattern as epicor.py / syteline.py
try:
    import pyodbc
    _HAS_PYODBC = True
except ImportError:
    _HAS_PYODBC = False

from . import secrets as _secrets

_CONFIG_KEY_PREFIX = "ax_"


def _load_config(site_key: str) -> dict:
    cfg_path = Path(__file__).parent.parent.parent / "config" / "connections.yaml"
    with open(cfg_path) as f:
        full = yaml.safe_load(f)
    key = f"{_CONFIG_KEY_PREFIX}{site_key}"
    if key not in full:
        raise KeyError(
            f"No connections.yaml section '{key}'. "
            f"Known ax sites: {[k for k in full if k.startswith('ax_')]}"
        )
    return full[key]


def _cfg(site_key: str) -> dict:
    """Return the ax_<site_key> block from connections.yaml (or empty dict)."""
    try:
        return _load_config(site_key)
    except (KeyError, FileNotFoundError):
        return {}


def get_connection(site_key: str = "airport_rd",
                   timeout: Optional[int] = None) -> "pyodbc.Connection":
    """
    Open a pyodbc connection to the Dynamics AX SQL Server database.

    Parameters
    ----------
    site_key : str
        Key suffix matching the connections.yaml block, e.g. ``"airport_rd"``
        for ``ax_airport_rd:``.
    timeout : int, optional
        Connection timeout override in seconds.

    Returns
    -------
    pyodbc.Connection

    Raises
    ------
    RuntimeError
        If pyodbc is not installed or the config block is missing.
    """
    if not _HAS_PYODBC:
        raise RuntimeError("pyodbc is not installed; run: pip install pyodbc")

    cfg = _cfg(site_key)
    site_upper = site_key.upper().replace("-", "_")

    server   = (cfg.get("server", "") or
                os.environ.get(f"AX_{site_upper}_SERVER", "")).strip()
    database = (cfg.get("database", "") or
                os.environ.get(f"AX_{site_upper}_DATABASE", "MicrosoftDynamicsAX")).strip()
    driver   = cfg.get("driver", "ODBC Driver 18 for SQL Server")
    auth     = cfg.get("auth", cfg.get("authentication", "ActiveDirectoryIntegrated"))
    to       = str(timeout or cfg.get("connection_timeout", 30))

    if not server:
        raise RuntimeError(
            f"AX site '{site_key}': no server configured. "
            f"Set connections.yaml ax_{site_key}.server or "
            f"env var AX_{site_upper}_SERVER."
        )

    # Try DPAPI vault (via secrets module) first, fall back to env / config
    user = cfg.get("user", "")
    password = (cfg.get("password", "")
                or _secrets.get(f"ax_{site_key}_pass", "")
                or os.environ.get(f"AX_{site_upper}_PASS", ""))

    if auth in ("ActiveDirectoryIntegrated", "windows", "Trusted_Connection"):
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout={to};"
        )
    else:
        user = user or os.environ.get(f"AX_{site_upper}_USER", "")
        if not user or not password:
            raise RuntimeError(
                f"AX '{site_key}': SQL auth requires user + password. "
                f"Set connections.yaml or env vars AX_{site_upper}_USER / AX_{site_upper}_PASS."
            )
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout={to};"
        )

    return pyodbc.connect(conn_str, autocommit=True)


def is_alive(conn: "pyodbc.Connection") -> bool:
    """Return True if the connection is still usable."""
    try:
        conn.execute("SELECT 1")
        return True
    except Exception:
        return False


def get_or_reconnect(conn: Optional["pyodbc.Connection"],
                     site_key: str = "airport_rd") -> "pyodbc.Connection":
    """Return *conn* if alive, else open a fresh connection."""
    if conn is not None and is_alive(conn):
        return conn
    return get_connection(site_key)
