"""
Schema Synthesizer — assemble a target-entity schema for a Mission.

Given a Mission's target_entity_kind (site/warehouse/supplier/customer/
part_family/process), this module pulls the relevant tables out of
brain.yaml `tables` + the cached `discovered_schema.yaml`, infers
relationships from the column-pattern dictionary in brain.yaml, and emits
a JSON document plus a Mermaid ER snippet that the deck builders can
embed (as text or a rendered PNG when mermaid-cli is available).

This module never mutates brain.yaml or discovered_schema.yaml.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from functools import cache
from pathlib import Path
from typing import Any
import yaml

from . import load_config


_DISCOVERED = Path(__file__).resolve().parents[2] / "discovered_schema.yaml"


# ---------------------------------------------------------------------------
# Output objects
# ---------------------------------------------------------------------------
@dataclass
class ColumnRef:
    name: str
    type: str
    nullable: bool
    logical: str | None = None     # which brain.yaml column_pattern matched


@dataclass
class TableRef:
    logical_name: str              # brain.yaml key, e.g. "parts"
    qualified: str                 # connector qualified, e.g. "edap_dw_replica.dim_part"
    columns: list[ColumnRef] = field(default_factory=list)


@dataclass
class Relationship:
    from_table: str
    to_table: str
    on: str                        # logical column name (e.g. "part_id")
    confidence: float = 1.0        # 1.0 if both sides have the logical column


@dataclass
class EntitySchema:
    target_entity_kind: str
    target_entity_key: str
    tables: list[TableRef] = field(default_factory=list)
    relationships: list[Relationship] = field(default_factory=list)
    mermaid: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "target_entity_kind": self.target_entity_kind,
            "target_entity_key": self.target_entity_key,
            "tables": [
                {"logical_name": t.logical_name, "qualified": t.qualified,
                 "columns": [asdict(c) for c in t.columns]}
                for t in self.tables
            ],
            "relationships": [asdict(r) for r in self.relationships],
            "mermaid": self.mermaid,
        }


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------
@cache
def _load_discovered() -> dict[str, Any]:
    """Cached: read once per process lifetime."""
    if not _DISCOVERED.exists():
        return {}
    try:
        with open(_DISCOVERED, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


@cache
def _load_config_cached() -> dict[str, Any]:
    """Cached wrapper around load_config() — avoids re-reading brain.yaml."""
    return load_config()


# Which logical tables matter for which entity kind. Picked from brain.yaml
# `tables` keys; we keep this short on purpose so the schema slide is readable.
_TABLES_BY_KIND: dict[str, list[str]] = {
    "site":         ["parts", "on_hand", "open_purchase", "open_mfg",
                     "po_receipts", "sales_order_lines"],
    "warehouse":    ["parts", "on_hand", "open_purchase", "po_receipts"],
    "supplier":     ["suppliers", "po_receipts", "po_contract_part",
                     "ap_invoice_lines"],
    "customer":     ["sales_order_lines"],
    "part_family":  ["parts", "on_hand", "po_receipts", "sales_order_lines",
                     "part_cost"],
    "process":      ["po_receipts", "open_purchase", "open_mfg"],
}


def _build_pattern_index(patterns: dict[str, list[str]]) -> dict[str, str]:
    """Flatten {logical: [hints]} into {hint_lower: logical} for O(1) lookup."""
    index: dict[str, str] = {}
    for logical, hints in (patterns or {}).items():
        for h in hints or []:
            index[h.lower()] = logical
    return index


def _logical_for_column(col_name: str, pattern_index: dict[str, str]) -> str | None:
    """Reverse-lookup using a pre-built hint index."""
    name = (col_name or "").lower()
    for hint, logical in pattern_index.items():
        if hint in name:
            return logical
    return None


# Per-qualified-table column cache: keyed by (qualified, id(pattern_index))
# Reset by calling _columns_for.cache_clear() if schema reloads are needed.
_col_cache: dict[str, list[ColumnRef]] = {}


def _columns_for(qualified: str, discovered: dict, pattern_index: dict[str, str]) -> list[ColumnRef]:
    """Look up columns for `connector.schema.table` from discovered_schema.

    Results are memoised in ``_col_cache`` so repeated calls for the same
    qualified table pay no cost after the first lookup.
    """
    if qualified in _col_cache:
        return _col_cache[qualified]

    parts = qualified.split(".")
    if len(parts) < 2:
        _col_cache[qualified] = []
        return []
    schema, table = parts[-2], parts[-1]
    # discovered: { connector: { schema: { table: [ {column,...}, ... ] } } }
    for conn_block in discovered.values():
        if not isinstance(conn_block, dict):
            continue
        sblock = conn_block.get(schema)
        if not isinstance(sblock, dict):
            continue
        rows = sblock.get(table)
        if not isinstance(rows, list):
            continue
        out: list[ColumnRef] = [
            ColumnRef(
                name=(name := str(r.get("column", ""))),
                type=str(r.get("type", "")),
                nullable=(str(r.get("nullable", "YES")).upper() == "YES"),
                logical=_logical_for_column(name, pattern_index),
            )
            for r in rows if isinstance(r, dict)
        ]
        _col_cache[qualified] = out
        return out
    _col_cache[qualified] = []
    return []


def _infer_relationships(tables: list[TableRef]) -> list[Relationship]:
    """Two tables are related if they both expose the same logical column
    that looks like an id (part_id, supplier_id, ...).
    """
    id_logicals = {"part_id", "supplier_id", "site"}
    by_logical: dict[str, list[str]] = {}
    for t in tables:
        for c in t.columns:
            if c.logical in id_logicals:
                by_logical.setdefault(c.logical, []).append(t.logical_name)
    rels: list[Relationship] = []
    seen: set[tuple[str, str, str]] = set()
    for logical, table_names in by_logical.items():
        uniq = sorted(set(table_names))
        for i in range(len(uniq)):
            for j in range(i + 1, len(uniq)):
                key = (uniq[i], uniq[j], logical)
                if key in seen:
                    continue
                seen.add(key)
                rels.append(Relationship(from_table=uniq[i],
                                         to_table=uniq[j],
                                         on=logical,
                                         confidence=1.0))
    return rels


def _mermaid_er(tables: list[TableRef], rels: list[Relationship]) -> str:
    """Render an ER diagram in Mermaid syntax. Compact column lists."""
    lines = ["erDiagram"]
    for t in tables:
        cols = [c for c in t.columns if c.logical] or t.columns[:6]
        cols = cols[:8]
        if not cols:
            lines.append(f"    {t.logical_name} {{ }}")
            continue
        body = " ".join(f'{(c.logical or c.name).replace(" ", "_")}'
                        for c in cols)
        lines.append(f"    {t.logical_name} {{ {body} }}")
    for r in rels:
        lines.append(f'    {r.from_table} ||--o{{ {r.to_table} : "{r.on}"')
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def synthesize(target_entity_kind: str, target_entity_key: str = "") -> EntitySchema:
    """Build an EntitySchema for the given target entity kind."""
    cfg = _load_config_cached()              # cached — no disk I/O after first call
    tables_map = cfg.get("tables", {}) or {}
    patterns = cfg.get("column_patterns", {}) or {}
    pattern_index = _build_pattern_index(patterns)   # O(M) once per call; cheap
    discovered = _load_discovered()          # cached — no disk I/O after first call

    logicals = _TABLES_BY_KIND.get(target_entity_kind,
                                   _TABLES_BY_KIND["site"])
    tables: list[TableRef] = []
    for logical in logicals:
        qualified = tables_map.get(logical)
        if not qualified:
            continue
        cols = _columns_for(qualified, discovered, pattern_index)
        tables.append(TableRef(logical_name=logical, qualified=qualified, columns=cols))

    rels = _infer_relationships(tables)
    mermaid = _mermaid_er(tables, rels)

    return EntitySchema(
        target_entity_kind=target_entity_kind,
        target_entity_key=target_entity_key,
        tables=tables,
        relationships=rels,
        mermaid=mermaid,
    )


__all__ = ["synthesize", "EntitySchema", "TableRef", "ColumnRef", "Relationship"]
