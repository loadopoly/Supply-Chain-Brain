"""
Graph context — NetworkX-backed Part ↔ Supplier ↔ PO ↔ Receipt ↔ SO ↔ Customer
↔ SR multi-relation graph. Phase 1 in-process; Phase 2 swap-in for Neo4j /
Cosmos Gremlin via the same `GraphContext` API.
"""
from __future__ import annotations
from typing import Iterable
import pandas as pd

try:
    import networkx as nx
    HAS_NX = True
except Exception:
    HAS_NX = False
    nx = None  # type: ignore


class GraphContext:
    """Lightweight wrapper so pages don't import networkx directly."""

    def __init__(self) -> None:
        if not HAS_NX:
            raise RuntimeError("networkx not installed — `pip install networkx`")
        self.g = nx.MultiDiGraph()

    # --- ingest helpers ----------------------------------------------------
    def add_parts(self, df: pd.DataFrame, id_col: str,
                  label_col: str | None = None, **attr_cols: str) -> None:
        id_idx = df.columns.get_loc(id_col)
        label_idx = df.columns.get_loc(label_col) if (label_col and label_col in df.columns) else None
        attr_idx = {k: df.columns.get_loc(v) for k, v in attr_cols.items() if v in df.columns}
        for row in df.itertuples(index=False, name=None):
            nid = ("part", str(row[id_idx]))
            data = {k: row[idx] for k, idx in attr_idx.items()}
            data["kind"] = "part"
            raw_label = row[label_idx] if label_idx is not None else None
            data["label"] = str(raw_label) if (raw_label is not None and pd.notna(raw_label)) else str(row[id_idx])
            self.g.add_node(nid, **data)

    def add_suppliers(self, df: pd.DataFrame, id_col: str, name_col: str | None = None) -> None:
        id_idx = df.columns.get_loc(id_col)
        name_idx = df.columns.get_loc(name_col) if (name_col and name_col in df.columns) else None
        for row in df.itertuples(index=False, name=None):
            nid = ("supplier", str(row[id_idx]))
            raw_name = row[name_idx] if name_idx is not None else None
            label = str(raw_name) if (raw_name is not None and pd.notna(raw_name)) else str(row[id_idx])
            self.g.add_node(nid, kind="supplier", label=label,
                            name=raw_name if name_idx is not None else None)

    def add_edges(self, df: pd.DataFrame, src_kind: str, src_col: str,
                  dst_kind: str, dst_col: str, edge_kind: str,
                  weight_col: str | None = None,
                  src_label_col: str | None = None,
                  dst_label_col: str | None = None) -> None:
        src_idx = df.columns.get_loc(src_col)
        dst_idx = df.columns.get_loc(dst_col)
        weight_idx = df.columns.get_loc(weight_col) if (weight_col and weight_col in df.columns) else None
        src_label_idx = df.columns.get_loc(src_label_col) if (src_label_col and src_label_col in df.columns) else None
        dst_label_idx = df.columns.get_loc(dst_label_col) if (dst_label_col and dst_label_col in df.columns) else None
        for row in df.itertuples(index=False, name=None):
            s = (src_kind, str(row[src_idx])); d = (dst_kind, str(row[dst_idx]))
            s_raw = row[src_label_idx] if src_label_idx is not None else None
            d_raw = row[dst_label_idx] if dst_label_idx is not None else None
            s_label = str(s_raw) if (s_raw is not None and pd.notna(s_raw)) else str(row[src_idx])
            d_label = str(d_raw) if (d_raw is not None and pd.notna(d_raw)) else str(row[dst_idx])
            if s not in self.g:
                self.g.add_node(s, kind=src_kind, label=s_label)
            else:
                if "kind" not in self.g.nodes[s]:
                    self.g.nodes[s]["kind"] = src_kind
                if "label" not in self.g.nodes[s] or self.g.nodes[s]["label"] == str(row[src_idx]):
                    self.g.nodes[s]["label"] = s_label   # upgrade raw-key label → human name
            if d not in self.g:
                self.g.add_node(d, kind=dst_kind, label=d_label)
            else:
                if "kind" not in self.g.nodes[d]:
                    self.g.nodes[d]["kind"] = dst_kind
                if "label" not in self.g.nodes[d] or self.g.nodes[d]["label"] == str(row[dst_idx]):
                    self.g.nodes[d]["label"] = d_label
            w_raw = row[weight_idx] if weight_idx is not None else None
            w = float(w_raw) if (w_raw is not None and pd.notna(w_raw)) else 1.0
            self.g.add_edge(s, d, kind=edge_kind, weight=w)

    # convenience alias used by some pages
    @property
    def graph(self):
        return self.g

    def explain_node(self, node) -> dict:
        """Discovery: why is this node central? Return its edge inventory + neighbor breakdown."""
        if node not in self.g:
            return {"error": f"node {node} not in graph"}
        edges_in  = list(self.g.in_edges(node, data=True, keys=True))  if self.g.is_directed() else []
        edges_out = list(self.g.out_edges(node, data=True, keys=True)) if self.g.is_directed() else []
        if not self.g.is_directed():
            edges_out = list(self.g.edges(node, data=True, keys=True))
        neighbor_kinds: dict[str, int] = {}
        edge_kinds: dict[str, int] = {}
        for *_, k, d in [(*e,) for e in edges_in + edges_out]:
            edge_kinds[d.get("kind", k)] = edge_kinds.get(d.get("kind", k), 0) + 1
        for nb in list(self.g.successors(node)) + list(self.g.predecessors(node)):
            kk = self.g.nodes[nb].get("kind", "unknown")
            neighbor_kinds[kk] = neighbor_kinds.get(kk, 0) + 1
        return {
            "node": node,
            "kind": self.g.nodes[node].get("kind"),
            "label": self.g.nodes[node].get("label"),
            "degree": self.g.degree(node),
            "in_degree": self.g.in_degree(node) if self.g.is_directed() else None,
            "out_degree": self.g.out_degree(node) if self.g.is_directed() else None,
            "neighbor_kinds": neighbor_kinds,
            "edge_kinds": edge_kinds,
        }

    # --- analytics ---------------------------------------------------------
    def neighbors(self, kind: str, key: str, depth: int = 1) -> set:
        start = (kind, str(key))
        if start not in self.g:
            return set()
        seen = {start}
        frontier = {start}
        for _ in range(depth):
            nxt = set()
            for n in frontier:
                nxt.update(self.g.successors(n))
                nxt.update(self.g.predecessors(n))
            frontier = nxt - seen
            seen.update(nxt)
        return seen - {start}

    def shared_suppliers(self, parts: Iterable[str]) -> dict[str, list[str]]:
        """Suppliers connected to ≥ 2 of the given parts → leverage candidates."""
        seen: dict[str, list[str]] = {}
        for p in parts:
            for n in self.neighbors("part", p, depth=1):
                if n[0] == "supplier":
                    seen.setdefault(n[1], []).append(str(p))
        return {s: ps for s, ps in seen.items() if len(ps) >= 2}

    def centrality(self, top_n: int = 25) -> pd.DataFrame:
        """Eigenvector + betweenness for leverage-point detection."""
        if self.g.number_of_nodes() == 0:
            return pd.DataFrame(columns=["node_kind","node_id","node","eigen",
                                          "eigenvector","betweenness","centrality"])
        try:
            ev = nx.eigenvector_centrality_numpy(self.g.to_undirected())
        except Exception:
            ev = nx.degree_centrality(self.g.to_undirected())
        bw = nx.betweenness_centrality(self.g.to_undirected(), k=min(200, len(self.g) or 1))
        rows = [{"node_kind": k[0], "node_id": k[1], "node": k,
                 "eigen": ev.get(k, 0.0),
                 "eigenvector": ev.get(k, 0.0),
                 "betweenness": bw.get(k, 0.0),
                 "centrality": ev.get(k, 0.0) + bw.get(k, 0.0)}
                for k in self.g.nodes()]
        df = pd.DataFrame(rows)
        return df.sort_values(["centrality"], ascending=False).head(top_n).reset_index(drop=True)

    def to_edge_frame(self, max_rows: int = 1000) -> pd.DataFrame:
        rows = []
        for s, d, k in list(self.g.edges(keys=True))[:max_rows]:
            rows.append({"src_kind": s[0], "src": s[1],
                         "dst_kind": d[0], "dst": d[1], "edge_kind": k})
        return pd.DataFrame(rows)
