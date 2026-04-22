"""Pluggable graph-DB backend.

Default: in-memory NetworkX (Phase 1). Optional: Neo4j (bolt) or Azure Cosmos DB
Gremlin. Same interface across all three so pages can stay backend-agnostic.

Switch via ``config/brain.yaml``::

    graph:
      backend: networkx          # or "neo4j" / "cosmos_gremlin"
      neo4j:
        uri: bolt://...
        user: neo4j
        password_env: NEO4J_PASSWORD
        database: neo4j
      cosmos_gremlin:
        endpoint: wss://...gremlin.cosmos.azure.com:443/
        database: scbrain
        graph: scbrain
        key_env: COSMOS_GREMLIN_KEY
"""
from __future__ import annotations
import os
from typing import Any, Iterable, Mapping, Sequence

try:
    from . import load_config
except Exception:                                                # pragma: no cover
    load_config = lambda: {}                                     # type: ignore


def _try(modname: str):
    try:
        import importlib
        return importlib.import_module(modname)
    except Exception:
        return None


class GraphBackend:
    """Common interface."""

    def add_node(self, node_id: str, label: str, **props: Any) -> None: ...
    def add_edge(self, src: str, dst: str, rel: str, **props: Any) -> None: ...
    def neighbors(self, node_id: str, rel: str | None = None) -> list[dict]: ...
    def shared_neighbors(self, a: str, b: str, rel: str | None = None) -> list[str]: ...
    def centrality(self, kind: str = "eigenvector") -> dict[str, float]: ...
    def cypher(self, query: str, **params) -> list[dict]:
        raise NotImplementedError("cypher not supported on this backend")
    def gremlin(self, query: str, **params) -> list[dict]:
        raise NotImplementedError("gremlin not supported on this backend")
    def close(self) -> None: ...


# ---------------------------------------------------------------------------- NX
class NetworkXBackend(GraphBackend):
    """Thin nx.MultiDiGraph wrapper. Used as default backend."""

    def __init__(self):
        nx = _try("networkx")
        if nx is None:
            raise RuntimeError("networkx not installed (`pip install networkx`)")
        self._nx = nx
        self.g = nx.MultiDiGraph()

    def add_node(self, node_id, label, **props):
        self.g.add_node(node_id, label=label, **props)

    def add_edge(self, src, dst, rel, **props):
        self.g.add_edge(src, dst, rel=rel, **props)

    def neighbors(self, node_id, rel=None):
        if node_id not in self.g:
            return []
        out = []
        for _, nbr, data in self.g.out_edges(node_id, data=True):
            if rel is None or data.get("rel") == rel:
                out.append({"id": nbr, "rel": data.get("rel"),
                            **{k: v for k, v in data.items() if k != "rel"}})
        return out

    def shared_neighbors(self, a, b, rel=None):
        if a not in self.g or b not in self.g:
            return []
        na = {n for _, n, d in self.g.out_edges(a, data=True) if rel in (None, d.get("rel"))}
        nb = {n for _, n, d in self.g.out_edges(b, data=True) if rel in (None, d.get("rel"))}
        return list(na & nb)

    def centrality(self, kind="eigenvector"):
        nx = self._nx
        ug = self.g.to_undirected()
        try:
            if kind == "eigenvector":
                return nx.eigenvector_centrality_numpy(ug)
            if kind == "betweenness":
                return nx.betweenness_centrality(ug, k=min(200, len(ug) or 1))
            if kind == "degree":
                return nx.degree_centrality(ug)
        except Exception:
            pass
        return nx.degree_centrality(ug)


# ---------------------------------------------------------------------------- Neo4j
class Neo4jBackend(GraphBackend):
    def __init__(self, uri: str, user: str, password: str, database: str = "neo4j"):
        nx_drv = _try("neo4j")
        if nx_drv is None:
            raise RuntimeError("neo4j driver not installed (`pip install neo4j`)")
        self._drv = nx_drv.GraphDatabase.driver(uri, auth=(user, password))
        self._db = database

    def _run(self, q, **p):
        with self._drv.session(database=self._db) as s:
            return [r.data() for r in s.run(q, **p)]

    def add_node(self, node_id, label, **props):
        label_safe = "".join(c for c in label if c.isalnum() or c == "_") or "Node"
        self._run(f"MERGE (n:{label_safe} {{id:$id}}) SET n += $p", id=node_id, p=props)

    def add_edge(self, src, dst, rel, **props):
        rel_safe = "".join(c for c in rel if c.isalnum() or c == "_").upper() or "REL"
        self._run(
            f"MATCH (a {{id:$s}}),(b {{id:$d}}) MERGE (a)-[r:{rel_safe}]->(b) SET r += $p",
            s=src, d=dst, p=props,
        )

    def neighbors(self, node_id, rel=None):
        if rel:
            r_safe = "".join(c for c in rel if c.isalnum() or c == "_").upper()
            q = f"MATCH (a {{id:$id}})-[r:{r_safe}]->(b) RETURN b.id AS id, type(r) AS rel, properties(r) AS p"
        else:
            q = "MATCH (a {id:$id})-[r]->(b) RETURN b.id AS id, type(r) AS rel, properties(r) AS p"
        return self._run(q, id=node_id)

    def shared_neighbors(self, a, b, rel=None):
        r_safe = "".join(c for c in (rel or "") if c.isalnum() or c == "_").upper()
        q = (
            f"MATCH (x {{id:$a}})-[:{r_safe}]->(s)<-[:{r_safe}]-(y {{id:$b}}) RETURN s.id AS id"
            if r_safe
            else "MATCH (x {id:$a})-[]->(s)<-[]-(y {id:$b}) RETURN DISTINCT s.id AS id"
        )
        return [r["id"] for r in self._run(q, a=a, b=b)]

    def centrality(self, kind="eigenvector"):
        proc = "gds.eigenvector.stream" if kind == "eigenvector" else "gds.betweenness.stream"
        try:
            rows = self._run(
                f"CALL {proc}({{nodeProjection:'*', relationshipProjection:'*'}}) "
                "YIELD nodeId, score RETURN gds.util.asNode(nodeId).id AS id, score"
            )
            return {r["id"]: float(r["score"]) for r in rows}
        except Exception:
            return {}

    def cypher(self, query, **params):
        return self._run(query, **params)

    def close(self):
        try:
            self._drv.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------- Cosmos Gremlin
class CosmosGremlinBackend(GraphBackend):
    def __init__(self, endpoint: str, database: str, graph: str, key: str):
        client_mod = _try("gremlin_python.driver.client")
        if client_mod is None:
            raise RuntimeError("gremlinpython not installed (`pip install gremlinpython`)")
        from gremlin_python.driver import client, serializer
        self._client = client.Client(
            endpoint, "g",
            username=f"/dbs/{database}/colls/{graph}",
            password=key,
            message_serializer=serializer.GraphSONSerializersV2d0(),
        )

    def _run(self, q):
        return self._client.submit(q).all().result()

    def add_node(self, node_id, label, **props):
        prop_clause = "".join(f".property('{k}', '{v}')" for k, v in props.items())
        self._run(f"g.V('{node_id}').fold().coalesce(unfold(), addV('{label}').property('id','{node_id}'){prop_clause})")

    def add_edge(self, src, dst, rel, **props):
        prop_clause = "".join(f".property('{k}', '{v}')" for k, v in props.items())
        self._run(
            f"g.V('{src}').coalesce(outE('{rel}').where(inV().hasId('{dst}')), "
            f"addE('{rel}').to(g.V('{dst}'))){prop_clause}"
        )

    def neighbors(self, node_id, rel=None):
        edge = f"out('{rel}')" if rel else "out()"
        rows = self._run(f"g.V('{node_id}').{edge}.id()")
        return [{"id": r, "rel": rel} for r in rows]

    def shared_neighbors(self, a, b, rel=None):
        edge = f"out('{rel}')" if rel else "out()"
        return self._run(f"g.V('{a}').{edge}.where(__.in('{rel or ''}').hasId('{b}')).id()")

    def centrality(self, kind="eigenvector"):
        # Cosmos Gremlin lacks native eigenvector; fall back to in-degree.
        rows = self._run("g.V().group().by(id).by(inE().count())")
        out = {}
        for d in rows:
            out.update(d if isinstance(d, dict) else {})
        return {k: float(v) for k, v in out.items()}

    def gremlin(self, query, **params):
        return self._run(query)


# ---------------------------------------------------------------------------- factory
def get_graph_backend() -> GraphBackend:
    cfg = (load_config() or {}).get("graph", {})
    backend = (cfg.get("backend") or "networkx").lower()
    if backend == "neo4j":
        nc = cfg.get("neo4j", {})
        return Neo4jBackend(
            uri=nc["uri"], user=nc.get("user", "neo4j"),
            password=os.environ.get(nc.get("password_env", "NEO4J_PASSWORD"), ""),
            database=nc.get("database", "neo4j"),
        )
    if backend in ("cosmos_gremlin", "cosmos", "gremlin"):
        cc = cfg.get("cosmos_gremlin", {})
        return CosmosGremlinBackend(
            endpoint=cc["endpoint"], database=cc["database"], graph=cc["graph"],
            key=os.environ.get(cc.get("key_env", "COSMOS_GREMLIN_KEY"), ""),
        )
    return NetworkXBackend()
