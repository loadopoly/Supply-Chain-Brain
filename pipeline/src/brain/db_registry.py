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
