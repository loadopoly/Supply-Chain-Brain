"""LLaDA2 sign-bit child acquisition for model-understanding expansion.

This module turns directionality changes into graph growth.

Interpretation
--------------
LLaDA-style diffusion models repeatedly move masked / uncertain token states
toward a denoised state.  In this Brain graph, the equivalent uncertainty
surface is the signed directionality frame:

    expansion_score, coherence, bifurcation_index

Each axis is centered at 0.5 and represented as a sign bit.  When an axis
crosses the center, its sign bit flips.  A flip is treated as evidence that the
model-understanding state has crossed a local boundary.  The system responds by
acquiring child nodes beneath high-uncertainty parents, with Model entities
preferred over generic corpus nodes.

Public entry point
------------------
    acquire_llada_signbit_children(cn) -> dict

The function is deterministic, idempotent per sign signature, and uses only the
existing corpus_entity / corpus_edge / kv_store tables.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

import numpy as np


AXES: tuple[str, str, str] = ("expansion", "coherence", "bifurcation")

_STATE_KEY = "llada2_signbit_state"
_LAST_KEY = "llada2_signbit_last"

_CHILD_TYPE = "LLaDAChild"
_MODEL_TYPE = "Model"

_REL_CHILD_OF = "CHILD_OF"
_REL_ACQUIRES = "ACQUIRES_CHILD"
_REL_SIGN_FLIP = "SIGNBIT_FLIP"

_PARENT_TYPES = (
    "Model",
    "Quest",
    "Endpoint",
    "Part",
    "Supplier",
    "DataTable",
    "Category",
)


@dataclass(frozen=True)
class ParentCandidate:
    """Corpus parent that can acquire LLaDA sign-bit children."""

    entity_id: str
    entity_type: str
    label: str
    uncertainty: float


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _ensure_kv_store(cn: sqlite3.Connection) -> None:
    cn.execute(
        "CREATE TABLE IF NOT EXISTS kv_store("
        "key TEXT PRIMARY KEY, value TEXT)"
    )


def _kv_read(cn: sqlite3.Connection, key: str) -> dict:
    try:
        row = cn.execute("SELECT value FROM kv_store WHERE key=?", (key,)).fetchone()
    except sqlite3.OperationalError:
        return {}
    if not row or not row[0]:
        return {}
    try:
        val = json.loads(row[0])
        return val if isinstance(val, dict) else {}
    except Exception:
        return {}


def _kv_write(cn: sqlite3.Connection, key: str, value: dict) -> None:
    cn.execute(
        "INSERT INTO kv_store(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, json.dumps(value, default=str)),
    )


def normalise_axes(raw: Mapping[str, float] | None) -> dict[str, float]:
    """Normalize directionality fields into the canonical AXES names."""
    raw = raw or {}
    expansion = raw.get("expansion", raw.get("expansion_score", 0.5))
    coherence = raw.get("coherence", 0.5)
    bifurcation = raw.get("bifurcation", raw.get("bifurcation_index", 0.5))
    return {
        "expansion": _clip01(float(expansion)),
        "coherence": _clip01(float(coherence)),
        "bifurcation": _clip01(float(bifurcation)),
    }


def sign_bits_from_axes(axes: Mapping[str, float]) -> dict[str, int]:
    """Map each centered directionality axis to a sign bit (-1 or +1)."""
    norm = normalise_axes(axes)
    return {axis: (1 if norm[axis] >= 0.5 else -1) for axis in AXES}


def flip_delta(
    previous_bits: Mapping[str, int] | None,
    current_bits: Mapping[str, int],
) -> dict[str, tuple[int, int]]:
    """Return axes whose sign bits changed.

    Missing previous bits are treated as 0, so the first observation bootstraps
    child acquisition instead of waiting for a second tick.
    """
    previous_bits = previous_bits or {}
    out: dict[str, tuple[int, int]] = {}
    for axis in AXES:
        old = int(previous_bits.get(axis, 0) or 0)
        new = int(current_bits.get(axis, 0) or 0)
        if old != new:
            out[axis] = (old, new)
    return out


def bit_signature(bits: Mapping[str, int]) -> str:
    """Compact deterministic signature such as exp+_coh-_bif+."""
    parts = []
    aliases = {"expansion": "exp", "coherence": "coh", "bifurcation": "bif"}
    for axis in AXES:
        parts.append(f"{aliases[axis]}{'+' if int(bits[axis]) >= 0 else '-'}")
    return "_".join(parts)


def directionality_axes_from_corpus(cn: sqlite3.Connection) -> dict[str, float]:
    """Read latest directionality axes, falling back to cheap corpus proxies."""
    try:
        row = cn.execute(
            "SELECT expansion_score, coherence, bifurcation_index "
            "FROM directionality_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row:
            return normalise_axes({
                "expansion_score": float(row[0] or 0.5),
                "coherence": float(row[1] or 0.5),
                "bifurcation_index": float(row[2] or 0.5),
            })
    except sqlite3.OperationalError:
        pass

    entity_count = _safe_scalar(cn, "SELECT COUNT(*) FROM corpus_entity", 0)
    edge_count = _safe_scalar(cn, "SELECT COUNT(*) FROM corpus_edge", 0)
    child_count = _safe_scalar(
        cn,
        "SELECT COUNT(*) FROM corpus_entity WHERE entity_type='LLaDAChild'",
        0,
    )
    tunnel_count = _safe_scalar(
        cn,
        "SELECT COUNT(*) FROM corpus_edge "
        "WHERE rel IN ('SYMBIOTIC_TUNNEL','GROUNDED_TUNNEL')",
        0,
    )
    weights = _safe_weights(cn)
    if weights.size:
        mean_w = float(weights.mean())
        cv = float(weights.std() / (mean_w + 1e-8))
        coherence = 1.0 - min(1.0, cv)
    else:
        coherence = 0.5

    expansion = min(1.0, float(edge_count + tunnel_count) / max(float(entity_count), 1.0))
    bifurcation = min(1.0, float(child_count + tunnel_count) / max(float(entity_count), 1.0))
    return normalise_axes({
        "expansion": expansion,
        "coherence": coherence,
        "bifurcation": bifurcation,
    })


def _safe_scalar(cn: sqlite3.Connection, sql: str, default: int) -> int:
    try:
        row = cn.execute(sql).fetchone()
        return int(row[0]) if row and row[0] is not None else int(default)
    except sqlite3.OperationalError:
        return int(default)


def _safe_weights(cn: sqlite3.Connection) -> np.ndarray:
    try:
        rows = cn.execute(
            "SELECT weight FROM corpus_edge WHERE weight IS NOT NULL LIMIT 500"
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    return np.asarray([float(r[0] or 0.0) for r in rows], dtype=float)


def seed_model_entities_from_llm_weights(cn: sqlite3.Connection) -> int:
    """Project llm_weights rows into Model corpus entities when available."""
    try:
        rows = cn.execute(
            "SELECT model_id, COUNT(DISTINCT task) AS tasks, "
            "AVG(weight) AS avg_weight, AVG(ema_success) AS avg_success, "
            "SUM(n_obs) AS observations "
            "FROM llm_weights GROUP BY model_id"
        ).fetchall()
    except sqlite3.OperationalError:
        return 0

    now_s = datetime.now().isoformat()
    added = 0
    for model_id, tasks, avg_weight, avg_success, observations in rows:
        if not model_id:
            continue
        exists = cn.execute(
            "SELECT 1 FROM corpus_entity WHERE entity_id=? AND entity_type=?",
            (str(model_id), _MODEL_TYPE),
        ).fetchone()
        props = {
            "source": "llm_weights",
            "llada_version": "2.0",
            "tasks": int(tasks or 0),
            "avg_weight": round(float(avg_weight or 0.0), 6),
            "avg_success": round(float(avg_success or 0.0), 6),
            "observations": int(observations or 0),
        }
        cn.execute(
            "INSERT INTO corpus_entity"
            "(entity_id,entity_type,label,props_json,first_seen,last_seen,samples) "
            "VALUES(?,?,?,?,?,?,1) "
            "ON CONFLICT(entity_id,entity_type) "
            "DO UPDATE SET last_seen=excluded.last_seen, "
            "  samples=samples+1, props_json=excluded.props_json",
            (str(model_id), _MODEL_TYPE, str(model_id), json.dumps(props), now_s, now_s),
        )
        if not exists:
            added += 1
    return added


def parent_uncertainty(cn: sqlite3.Connection, entity_id: str, entity_type: str) -> float:
    """Estimate how much child-node acquisition pressure a parent has."""
    try:
        rows = cn.execute(
            "SELECT weight, samples FROM corpus_edge "
            "WHERE (src_id=? AND src_type=?) OR (dst_id=? AND dst_type=?) "
            "LIMIT 200",
            (entity_id, entity_type, entity_id, entity_type),
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []

    if rows:
        weights = np.asarray([float(r[0] or 0.0) for r in rows], dtype=float)
        samples = np.asarray([float(r[1] or 1.0) for r in rows], dtype=float)
        mean_w = float(weights.mean())
        cv = float(weights.std() / (mean_w + 1e-8)) if weights.size > 1 else 0.0
        sample_gap = 1.0 / (1.0 + float(samples.mean()))
        return _clip01(0.50 * (1.0 - mean_w) + 0.30 * min(1.0, cv) + 0.20 * sample_gap)

    # Model entities seeded from llm_weights carry enough signal to estimate
    # uncertainty even before they have corpus edges.
    try:
        row = cn.execute(
            "SELECT props_json FROM corpus_entity WHERE entity_id=? AND entity_type=?",
            (entity_id, entity_type),
        ).fetchone()
        props = json.loads(row[0]) if row and row[0] else {}
    except Exception:
        props = {}
    avg_weight = float(props.get("avg_weight", 0.5) or 0.5)
    avg_success = float(props.get("avg_success", 0.5) or 0.5)
    return _clip01(0.5 * (1.0 - avg_weight) + 0.5 * (1.0 - avg_success))


def candidate_parents(
    cn: sqlite3.Connection,
    *,
    max_parents: int = 4,
) -> list[ParentCandidate]:
    """Return parents most likely to benefit from acquired children."""
    placeholders = ",".join(["?"] * len(_PARENT_TYPES))
    try:
        rows = cn.execute(
            "SELECT entity_id, entity_type, label FROM corpus_entity "
            f"WHERE entity_type IN ({placeholders})",
            _PARENT_TYPES,
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    type_bonus = {
        "Model": 0.25,
        "Quest": 0.10,
        "Endpoint": 0.05,
    }
    candidates: list[ParentCandidate] = []
    for entity_id, entity_type, label in rows:
        uncertainty = parent_uncertainty(cn, entity_id, entity_type)
        ranked = _clip01(uncertainty + type_bonus.get(entity_type, 0.0))
        candidates.append(ParentCandidate(
            entity_id=str(entity_id),
            entity_type=str(entity_type),
            label=str(label or entity_id),
            uncertainty=ranked,
        ))
    candidates.sort(key=lambda p: (-p.uncertainty, p.entity_type, p.entity_id))
    return candidates[:max(0, int(max_parents))]


def _child_id(parent: ParentCandidate, axis: str, new_bit: int, signature: str) -> str:
    h = hashlib.blake2b(
        f"{parent.entity_type}:{parent.entity_id}:{axis}:{new_bit}:{signature}".encode("utf-8"),
        digest_size=10,
    ).hexdigest()
    return f"llada2:{h}"


def _edge_exists(
    cn: sqlite3.Connection,
    src_id: str,
    src_type: str,
    dst_id: str,
    dst_type: str,
    rel: str,
) -> bool:
    row = cn.execute(
        "SELECT 1 FROM corpus_edge "
        "WHERE src_id=? AND src_type=? AND dst_id=? AND dst_type=? AND rel=?",
        (src_id, src_type, dst_id, dst_type, rel),
    ).fetchone()
    return row is not None


def _upsert_edge(
    cn: sqlite3.Connection,
    src_id: str,
    src_type: str,
    dst_id: str,
    dst_type: str,
    rel: str,
    weight: float,
    now_s: str,
) -> bool:
    existed = _edge_exists(cn, src_id, src_type, dst_id, dst_type, rel)
    cn.execute(
        "INSERT INTO corpus_edge"
        "(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) "
        "VALUES(?,?,?,?,?,?,?,1) "
        "ON CONFLICT(src_id,src_type,dst_id,dst_type,rel) "
        "DO UPDATE SET last_seen=excluded.last_seen, "
        "  samples=samples+1, weight=max(weight, excluded.weight)",
        (src_id, src_type, dst_id, dst_type, rel, float(weight), now_s),
    )
    return not existed


def _acquire_child(
    cn: sqlite3.Connection,
    parent: ParentCandidate,
    axis: str,
    old_bit: int,
    new_bit: int,
    axes: Mapping[str, float],
    bits: Mapping[str, int],
    signature: str,
) -> tuple[int, int]:
    """Insert one child node plus relationship edges. Returns (nodes, edges)."""
    child_id = _child_id(parent, axis, new_bit, signature)
    now_s = datetime.now().isoformat()
    existed = cn.execute(
        "SELECT 1 FROM corpus_entity WHERE entity_id=? AND entity_type=?",
        (child_id, _CHILD_TYPE),
    ).fetchone()

    axis_amp = abs(float(axes[axis]) - 0.5) * 2.0
    polarity = "positive" if new_bit > 0 else "negative"
    confidence = _clip01(0.25 + 0.35 * axis_amp + 0.40 * parent.uncertainty)
    props = {
        "llada_version": "2.0",
        "parent_id": parent.entity_id,
        "parent_type": parent.entity_type,
        "flip_axis": axis,
        "old_bit": old_bit,
        "new_bit": new_bit,
        "polarity": polarity,
        "sign_bits": dict(bits),
        "axes": {k: round(float(v), 6) for k, v in axes.items()},
        "signature": signature,
        "confidence": round(confidence, 6),
    }
    label = f"LLaDA2 {axis} {polarity} child of {parent.label}"
    cn.execute(
        "INSERT INTO corpus_entity"
        "(entity_id,entity_type,label,props_json,first_seen,last_seen,samples) "
        "VALUES(?,?,?,?,?,?,1) "
        "ON CONFLICT(entity_id,entity_type) "
        "DO UPDATE SET last_seen=excluded.last_seen, samples=samples+1, "
        "  props_json=excluded.props_json",
        (child_id, _CHILD_TYPE, label, json.dumps(props), now_s, now_s),
    )

    edges_added = 0
    edges_added += int(_upsert_edge(
        cn, child_id, _CHILD_TYPE, parent.entity_id, parent.entity_type,
        _REL_CHILD_OF, 1.0, now_s,
    ))
    edges_added += int(_upsert_edge(
        cn, parent.entity_id, parent.entity_type, child_id, _CHILD_TYPE,
        _REL_ACQUIRES, confidence, now_s,
    ))
    edges_added += int(_upsert_edge(
        cn, parent.entity_id, parent.entity_type, child_id, _CHILD_TYPE,
        _REL_SIGN_FLIP, confidence, now_s,
    ))
    return (0 if existed else 1), edges_added


def acquire_llada_signbit_children(
    cn: sqlite3.Connection,
    *,
    axes: Mapping[str, float] | None = None,
    max_children: int = 12,
    max_parents: int = 4,
) -> dict:
    """Acquire child nodes when LLaDA2 sign bits flip.

    Parameters
    ----------
    cn:
        Open SQLite connection whose corpus tables are available.
    axes:
        Optional test / caller override for directionality axes.  When omitted,
        latest directionality_log values are used with a cheap corpus fallback.
    max_children:
        Safety cap per tick.
    max_parents:
        Number of parent entities considered per tick.
    """
    _ensure_kv_store(cn)
    models_seeded = seed_model_entities_from_llm_weights(cn)
    current_axes = normalise_axes(axes) if axes is not None else directionality_axes_from_corpus(cn)
    current_bits = sign_bits_from_axes(current_axes)

    prior = _kv_read(cn, _STATE_KEY)
    previous_bits = prior.get("bits", {}) if isinstance(prior, dict) else {}
    flips = flip_delta(previous_bits, current_bits)
    signature = bit_signature(current_bits)

    stats = {
        "axes": {k: round(float(v), 6) for k, v in current_axes.items()},
        "bits": current_bits,
        "flips": {k: [v[0], v[1]] for k, v in flips.items()},
        "models_seeded": models_seeded,
        "parents_seen": 0,
        "child_nodes_added": 0,
        "child_edges_added": 0,
    }

    if flips:
        parents = candidate_parents(cn, max_parents=max_parents)
        stats["parents_seen"] = len(parents)
        acquired = 0
        for parent in parents:
            for axis, (old_bit, new_bit) in flips.items():
                if acquired >= max_children:
                    break
                nodes, edges = _acquire_child(
                    cn, parent, axis, old_bit, new_bit,
                    current_axes, current_bits, signature,
                )
                stats["child_nodes_added"] += nodes
                stats["child_edges_added"] += edges
                acquired += 1
            if acquired >= max_children:
                break

    payload = {
        "bits": current_bits,
        "axes": stats["axes"],
        "signature": signature,
        "updated_at": datetime.now().isoformat(),
    }
    _kv_write(cn, _STATE_KEY, payload)
    _kv_write(cn, _LAST_KEY, stats)
    return stats


__all__ = [
    "AXES",
    "ParentCandidate",
    "acquire_llada_signbit_children",
    "bit_signature",
    "candidate_parents",
    "directionality_axes_from_corpus",
    "flip_delta",
    "normalise_axes",
    "parent_uncertainty",
    "seed_model_entities_from_llm_weights",
    "sign_bits_from_axes",
    "_CHILD_TYPE",
    "_REL_ACQUIRES",
    "_REL_CHILD_OF",
    "_REL_SIGN_FLIP",
]