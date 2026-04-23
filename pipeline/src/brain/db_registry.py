"""
Pluggable connector registry — every data source (Azure SQL, Oracle Fusion,
external HTTP app) registers a Connector and is reachable by name.

Adding a new database is one config edit + one Connector subclass — no page
edits needed (req 4).
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional
import pandas as pd


@dataclass
class Connector:
    name: str
    kind: str                                  # 'sql' | 'http' | 'graph'
    description: str = ""
    handle_factory: Optional[Callable[[], Any]] = None
    _handle: Any = field(default=None, repr=False)

    def handle(self) -> Any:
        if self._handle is None and self.handle_factory is not None:
            self._handle = self.handle_factory()
        return self._handle

    def reset(self) -> None:
        self._handle = None


_REGISTRY: Dict[str, Connector] = {}


def register(connector: Connector) -> None:
    _REGISTRY[connector.name] = connector


def get(name: str) -> Connector:
    if name not in _REGISTRY:
        raise KeyError(f"connector '{name}' not registered. Known: {list(_REGISTRY)}")
    return _REGISTRY[name]


def list_connectors() -> list[Connector]:
    return list(_REGISTRY.values())


def _healthy_conn(connector_name: str):
    """Return a live connection, auto-reconnecting if the handle is stale."""
    c = get(connector_name)
    if c._handle is not None:
        try:
            c._handle.cursor().execute("SELECT 1")   # health-check ping
            return c._handle
        except Exception:
            c._handle = None                          # force reconnect
    return c.handle()


def read_sql(connector_name: str, sql: str, params: list | None = None,
             timeout_s: int = 120) -> pd.DataFrame:
    """Execute SQL on a registered SQL connector. Returns empty DF on failure."""
    conn = _healthy_conn(connector_name)
    cursor = conn.cursor()
    try:
        cursor.timeout = timeout_s
    except Exception:
        pass
    try:
        cursor.execute(sql, params or [])
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame.from_records(rows, columns=cols)
    except Exception as exc:                  # graceful: log via empty + attr
        df = pd.DataFrame()
        df.attrs["_error"] = str(exc)
        df.attrs["_sql"] = sql
        return df
    finally:
        try:
            cursor.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Built-in registrations — wired to existing pipeline.connections modules
# ---------------------------------------------------------------------------
def bootstrap_default_connectors() -> None:
    """Register Azure SQL replica + Oracle Fusion using the existing modules."""
    if "azure_sql" not in _REGISTRY:
        try:
            from pipeline.src.connections import azure_sql              # noqa
        except Exception:
            from src.connections import azure_sql                        # noqa
        register(Connector(
            name="azure_sql",
            kind="sql",
            description="Azure SQL replica · edap-replica-cms-sqldb",
            handle_factory=azure_sql.get_connection,
        ))

    if "oracle_fusion" not in _REGISTRY:
        try:
            from pipeline.src.connections.oracle_fusion import OracleFusionSession
        except Exception:
            from src.connections.oracle_fusion import OracleFusionSession

        def _oracle():
            s = OracleFusionSession()
            s.connect()
            return s

        register(Connector(
            name="oracle_fusion",
            kind="http",
            description="Oracle Fusion Cloud · DEV13",
            handle_factory=_oracle,
        ))

    # Epicor 9 sites — one connector per site; each is a plain pyodbc SQL Server connection.
    # Sites are registered lazily: if the connections.yaml block has no server configured
    # (server: "") the connector is still registered but will raise at first use, so
    # pages that don't need a given site are unaffected.
    _epicor_sites = [
        ("jerome_ave",       "Epicor 9 · Jerome Ave (Chattanooga)"),
        ("manufacturers_rd", "Epicor 9 · Manufacturers Rd (Chattanooga)"),
        ("wilson_rd",        "Epicor 9 · Wilson Rd (Chattanooga)"),
    ]
    for _site_key, _desc in _epicor_sites:
        _conn_name = f"epicor_{_site_key}"
        if _conn_name not in _REGISTRY:
            try:
                from pipeline.src.connections import epicor as _epicor_mod  # noqa
            except Exception:
                from src.connections import epicor as _epicor_mod            # noqa

            def _make_epicor_factory(sk=_site_key, mod=_epicor_mod):
                return lambda: mod.get_connection(sk)

            register(Connector(
                name=_conn_name,
                kind="sql",
                description=_desc,
                handle_factory=_make_epicor_factory(),
            ))

    # SyteLine sites (Parsons = PFI_App.dbo; add rows as more sites migrate to SyteLine)
    _syteline_sites = [
        ("parsons", "SyteLine · Parsons  (PFI_SLMiscApps_DB.cycle_count)"),
    ]
    for _site_key, _desc in _syteline_sites:
        _conn_name = f"syteline_{_site_key}"
        if _conn_name not in _REGISTRY:
            try:
                from pipeline.src.connections import syteline as _syteline_mod  # noqa
            except Exception:
                from src.connections import syteline as _syteline_mod            # noqa

            def _make_syteline_factory(sk=_site_key, mod=_syteline_mod):
                return lambda: mod.get_connection(sk)

            register(Connector(
                name=_conn_name,
                kind="sql",
                description=_desc,
                handle_factory=_make_syteline_factory(),
            ))

    # Microsoft Dynamics AX sites — Eugene Airport Road (AX 2012 SQL Server)
    _ax_sites = [
        ("airport_rd", "Dynamics AX · Eugene Airport Rd  (MicrosoftDynamicsAX)"),
    ]
    for _site_key, _desc in _ax_sites:
        _conn_name = f"ax_{_site_key}"
        if _conn_name not in _REGISTRY:
            try:
                from pipeline.src.connections import ax as _ax_mod  # noqa
            except Exception:
                from src.connections import ax as _ax_mod            # noqa

            def _make_ax_factory(sk=_site_key, mod=_ax_mod):
                return lambda: mod.get_connection(sk)

            register(Connector(
                name=_conn_name,
                kind="sql",
                description=_desc,
                handle_factory=_make_ax_factory(),
            ))
