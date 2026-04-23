"""Knowledge Corpus & Recent-Learnings Log.

The Brain is constantly producing signals across multiple subsystems:

    * `llm_self_train_log`      — bounded SGD nudges from pipeline ground truth
    * `llm_dispatch_log`        — every multi-LLM ensemble call + validator
    * `llm_weights`             — current (model, task) weights & biases
    * `network_observations`    — every cross-protocol probe
    * `network_topology`        — rolling per-host EMA stats
    * `network_promotions`      — peer compute nodes added to the grid
    * `part_category`           — NLP-derived part class
    * `otd_ownership`           — recursive OTD owner attribution
    * The replica DW            — parts, suppliers, sites, POs (read-only)

This module merges those streams into:

    1. `learning_log`           — append-only roll-up of each "thing the Brain
                                  just learned" with kind / title / signal_strength.
    2. `corpus_entity`          — a normalized entity catalog
                                  (Part, Supplier, Site, Model, Peer, Protocol,
                                   Task, Category, Owner, Endpoint).
    3. `corpus_edge`            — typed relationships between entities
                                  (PART→CATEGORY, SUPPLIER→PART, MODEL→TASK,
                                   PEER→PROTOCOL, OWNER→PO, etc.) with weight.

Then `materialize_into_graph()` projects (corpus_entity, corpus_edge) into the
configured `graph_backend` (NetworkX by default, Neo4j or Cosmos Gremlin in
prod) so every page that already speaks `get_graph_backend()` instantly
benefits from the new dynamic architecture.

Public API:
    refresh_corpus_round() -> dict
    materialize_into_graph() -> dict
    recent_learnings(limit=50, kind=None) -> list[dict]
    schedule_in_background(interval_s=900) -> threading.Thread
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from contextlib import contextmanager, closing
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from . import load_config
from .local_store import db_path as _local_db_path


_PIPELINE_ROOT = Path(__file__).resolve().parents[2]
_REFRESH_LOCK = threading.Lock()
_LAST_REFRESH_TS: float = 0.0


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def _cfg() -> dict:
    return ((load_config().get("llms") or {}).get("knowledge_corpus") or {})


def _enabled() -> bool:
    return bool(_cfg().get("enabled", True))


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------
@contextmanager
def _conn():
    cn = sqlite3.connect(_local_db_path())
    cn.row_factory = sqlite3.Row
    try:
        yield cn
        cn.commit()
    finally:
        cn.close()


def init_schema() -> None:
    with _conn() as cn:
        cn.executescript(
            """
            CREATE TABLE IF NOT EXISTS learning_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                logged_at       TEXT NOT NULL,
                kind            TEXT NOT NULL,           -- self_train | network | dispatch | promotion | corpus | schema
                title           TEXT NOT NULL,
                detail          TEXT,                    -- JSON blob
                signal_strength REAL,                    -- [0..1] heuristic confidence
                source_table    TEXT,
                source_row_id   INTEGER
            );
            CREATE INDEX IF NOT EXISTS ix_learning_log_kind
                ON learning_log(kind, logged_at);

            CREATE TABLE IF NOT EXISTS corpus_entity (
                entity_id   TEXT NOT NULL,             -- normalized id
                entity_type TEXT NOT NULL,             -- Part, Supplier, Site, Model, Peer, Protocol, Task, Category, Owner, Endpoint
                label       TEXT,
                props_json  TEXT,
                first_seen  TEXT NOT NULL,
                last_seen   TEXT NOT NULL,
                samples     INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (entity_id, entity_type)
            );

            CREATE TABLE IF NOT EXISTS corpus_edge (
                src_id      TEXT NOT NULL,
                src_type    TEXT NOT NULL,
                dst_id      TEXT NOT NULL,
                dst_type    TEXT NOT NULL,
                rel         TEXT NOT NULL,
                weight      REAL NOT NULL DEFAULT 1.0,
                last_seen   TEXT NOT NULL,
                samples     INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (src_id, src_type, dst_id, dst_type, rel)
            );

            CREATE TABLE IF NOT EXISTS corpus_round_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ran_at      TEXT NOT NULL,
                entities_added   INTEGER NOT NULL DEFAULT 0,
                entities_touched INTEGER NOT NULL DEFAULT 0,
                edges_added      INTEGER NOT NULL DEFAULT 0,
                edges_touched    INTEGER NOT NULL DEFAULT 0,
                learnings_logged INTEGER NOT NULL DEFAULT 0,
                graph_backend    TEXT,
                notes            TEXT
            );
            """
        )


# ---------------------------------------------------------------------------
# Helpers — upserts that preserve last_seen + sample counts
# ---------------------------------------------------------------------------
@dataclass
class _Stats:
    entities_added:   int = 0
    entities_touched: int = 0
    edges_added:      int = 0
    edges_touched:    int = 0
    learnings_logged: int = 0


def _upsert_entity(cn, stats: _Stats, *, entity_id: str, entity_type: str,
                   label: str | None = None, props: dict | None = None) -> None:
    if not entity_id:
        return
    now = datetime.now(timezone.utc).isoformat()
    row = cn.execute(
        "SELECT samples FROM corpus_entity WHERE entity_id=? AND entity_type=?",
        (entity_id, entity_type),
    ).fetchone()
    if row is None:
        cn.execute(
            """INSERT INTO corpus_entity(entity_id, entity_type, label, props_json,
                first_seen, last_seen, samples) VALUES (?,?,?,?,?,?,1)""",
            (entity_id, entity_type, label, json.dumps(props or {}, default=str),
             now, now),
        )
        stats.entities_added += 1
    else:
        cn.execute(
            """UPDATE corpus_entity SET last_seen=?, samples=samples+1,
                  label=COALESCE(?, label),
                  props_json=COALESCE(?, props_json)
               WHERE entity_id=? AND entity_type=?""",
            (now, label, json.dumps(props, default=str) if props is not None else None,
             entity_id, entity_type),
        )
        stats.entities_touched += 1


def _upsert_edge(cn, stats: _Stats, *, src_id: str, src_type: str,
                 dst_id: str, dst_type: str, rel: str, weight: float = 1.0) -> None:
    if not src_id or not dst_id:
        return
    now = datetime.now(timezone.utc).isoformat()
    row = cn.execute(
        """SELECT samples, weight FROM corpus_edge
           WHERE src_id=? AND src_type=? AND dst_id=? AND dst_type=? AND rel=?""",
        (src_id, src_type, dst_id, dst_type, rel),
    ).fetchone()
    if row is None:
        cn.execute(
            """INSERT INTO corpus_edge(src_id, src_type, dst_id, dst_type, rel,
                  weight, last_seen, samples) VALUES(?,?,?,?,?,?,?,1)""",
            (src_id, src_type, dst_id, dst_type, rel, float(weight), now),
        )
        stats.edges_added += 1
    else:
        # EMA-style smoothing on weight (alpha=0.30)
        new_w = 0.7 * float(row["weight"]) + 0.3 * float(weight)
        cn.execute(
            """UPDATE corpus_edge SET last_seen=?, samples=samples+1, weight=?
               WHERE src_id=? AND src_type=? AND dst_id=? AND dst_type=? AND rel=?""",
            (now, new_w, src_id, src_type, dst_id, dst_type, rel),
        )
        stats.edges_touched += 1


def _log_learning(cn, stats: _Stats, *, kind: str, title: str,
                  signal: float | None = None, detail: dict | None = None,
                  source_table: str | None = None,
                  source_row_id: int | None = None) -> None:
    cn.execute(
        """INSERT INTO learning_log(logged_at, kind, title, detail,
                signal_strength, source_table, source_row_id)
           VALUES(?,?,?,?,?,?,?)""",
        (datetime.now(timezone.utc).isoformat(), kind, title,
         json.dumps(detail or {}, default=str),
         float(signal) if signal is not None else None,
         source_table, source_row_id),
    )
    stats.learnings_logged += 1


# ---------------------------------------------------------------------------
# Stream ingesters — each consumes one source table and updates the corpus
# ---------------------------------------------------------------------------
def _ingest_self_train(cn, stats: _Stats, since_id: int) -> int:
    """Replay self-training rows since the last cursor."""
    try:
        rows = cn.execute(
            """SELECT id, ran_at, task, samples, matched, avg_validator,
                      drift_capped, diversity_dampened, notes
               FROM llm_self_train_log
               WHERE id > ? ORDER BY id LIMIT 200""",
            (int(since_id),),
        ).fetchall()
    except sqlite3.OperationalError:
        return since_id   # table not yet created
    last = since_id
    for r in rows:
        last = max(last, int(r["id"]))
        task = r["task"] or "unknown"
        _upsert_entity(cn, stats, entity_id=task, entity_type="Task", label=task)
        _match_rate = (
            float(r["matched"]) / max(1, int(r["samples"]))
            if r["samples"] else 0.0
        )
        _log_learning(
            cn, stats,
            kind="self_train",
            title=f"Self-train round on '{task}': matched {r['matched']}/{r['samples']}",
            signal=(
                float(r["avg_validator"])
                if r["avg_validator"] is not None
                else _match_rate
            ),
            detail={
                "task": task,
                "samples": r["samples"],
                "matched": r["matched"],
                "drift_capped": r["drift_capped"],
                "diversity_dampened": r["diversity_dampened"],
                "notes": r["notes"],
            },
            source_table="llm_self_train_log",
            source_row_id=int(r["id"]),
        )
    return last


def _ingest_dispatch(cn, stats: _Stats, since_id: int) -> int:
    """Mirror llm_dispatch_log into Model↔Task edges weighted by validator."""
    try:
        rows = cn.execute(
            """SELECT id, ran_at, model_id, task, validator
               FROM llm_dispatch_log
               WHERE id > ? AND model_id IS NOT NULL
               ORDER BY id LIMIT 500""",
            (int(since_id),),
        ).fetchall()
    except sqlite3.OperationalError:
        return since_id
    last = since_id
    for r in rows:
        last = max(last, int(r["id"]))
        m, t = r["model_id"], r["task"] or "unknown"
        v = float(r["validator"]) if r["validator"] is not None else 0.5
        _upsert_entity(cn, stats, entity_id=m, entity_type="Model", label=m)
        _upsert_entity(cn, stats, entity_id=t, entity_type="Task", label=t)
        _upsert_edge(cn, stats, src_id=m, src_type="Model",
                     dst_id=t, dst_type="Task", rel="ANSWERS",
                     weight=max(0.0, min(1.0, v)))
    return last


def _ingest_weights(cn, stats: _Stats) -> int:
    """Snapshot llm_weights as Model→Task edges weighted by current weight."""
    try:
        rows = cn.execute(
            "SELECT model_id, task, weight FROM llm_weights"
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    n = 0
    for r in rows:
        m, t = r["model_id"], r["task"] or "unknown"
        w = float(r["weight"] or 0.0)
        _upsert_entity(cn, stats, entity_id=m, entity_type="Model", label=m)
        _upsert_entity(cn, stats, entity_id=t, entity_type="Task", label=t)
        _upsert_edge(cn, stats, src_id=m, src_type="Model",
                     dst_id=t, dst_type="Task", rel="WEIGHTED_FOR",
                     weight=max(0.0, min(1.0, w / 2.0)))   # weights up to ~2 in practice
        n += 1
    return n


def _ingest_network(cn, stats: _Stats, since_id: int) -> int:
    """Replay network probes — Endpoint nodes + ENDPOINT_USES_PROTOCOL edges."""
    try:
        rows = cn.execute(
            """SELECT id, observed_at, source, protocol, host, port,
                      capability, ok, latency_ms
               FROM network_observations
               WHERE id > ? ORDER BY id LIMIT 500""",
            (int(since_id),),
        ).fetchall()
    except sqlite3.OperationalError:
        return since_id
    last = since_id
    for r in rows:
        last = max(last, int(r["id"]))
        host = r["host"] or "?"
        proto = r["protocol"] or "tcp"
        ep_id = f"{proto}://{host}" + (f":{r['port']}" if r["port"] else "")
        _upsert_entity(
            cn, stats, entity_id=ep_id, entity_type="Endpoint",
            label=host, props={"capability": r["capability"], "source": r["source"]},
        )
        _upsert_entity(cn, stats, entity_id=proto, entity_type="Protocol", label=proto)
        _upsert_edge(cn, stats, src_id=ep_id, src_type="Endpoint",
                     dst_id=proto, dst_type="Protocol", rel="USES",
                     weight=1.0 if r["ok"] else 0.2)
    return last


def _ingest_promotions(cn, stats: _Stats, since_id: int) -> int:
    try:
        rows = cn.execute(
            """SELECT id, promoted_at, target, host, reason
               FROM network_promotions WHERE id > ? ORDER BY id""",
            (int(since_id),),
        ).fetchall()
    except sqlite3.OperationalError:
        return since_id
    last = since_id
    for r in rows:
        last = max(last, int(r["id"]))
        _upsert_entity(cn, stats, entity_id=r["host"], entity_type="Peer",
                       label=r["host"], props={"target": r["target"]})
        _log_learning(
            cn, stats, kind="promotion",
            title=f"Promoted peer '{r['host']}' into {r['target']}",
            signal=0.9, detail={"reason": r["reason"]},
            source_table="network_promotions", source_row_id=int(r["id"]),
        )
    return last


def _ingest_ml_research(cn, stats: _Stats, since_id: int) -> int:
    """Promote ml_research learning_log entries into the corpus graph.

    Reads every ``kind='ml_research'`` row in ``learning_log`` with
    ``id > since_id`` and upserts:

    * ``MLPaper``       — one entity per paper (entity_id = arxiv_id or DOI slug)
    * ``MLDataset``     — one entity per research dataset (entity_id = dataset_id)
    * ``ResearchTopic`` — one entity per supply-chain query string
    * Edges:
        - ``MLPaper    ─RESEARCHED_FOR─► ResearchTopic``
        - ``MLDataset  ─APPLIES_TO────► ResearchTopic``
        - keyword-based ``INFORMS`` edges to any existing ``Task`` entities
          whose label overlaps a paper's keywords or title tokens

    Sources handled: arxiv, openalex, crossref, core, ntrs, arxiv_recent,
    zenodo (datasets), ml_intern (deep-research summaries).
    """
    try:
        rows = cn.execute(
            """SELECT id, logged_at, title, detail, signal_strength
               FROM learning_log
               WHERE kind='ml_research' AND id > ?
               ORDER BY id LIMIT 300""",
            (int(since_id),),
        ).fetchall()
    except sqlite3.OperationalError:
        return since_id   # learning_log may not exist yet

    # Pre-fetch existing Task entity labels for keyword cross-linking
    try:
        task_rows = cn.execute(
            "SELECT entity_id, label FROM corpus_entity WHERE entity_type='Task'"
        ).fetchall()
        task_labels = {(r["label"] or r["entity_id"]).lower(): r["entity_id"]
                       for r in task_rows}
    except Exception:
        task_labels = {}

    last = since_id
    for r in rows:
        last = max(last, int(r["id"]))
        try:
            detail = json.loads(r["detail"] or "{}")
        except Exception:
            detail = {}

        topic_str = (detail.get("topic") or "").strip()
        signal = float(r["signal_strength"] or 0.3)
        entry_type = detail.get("type", "")

        # Upsert ResearchTopic entity
        if topic_str:
            for t in topic_str.split(","):
                t = t.strip()
                if t:
                    _upsert_entity(cn, stats, entity_id=t,
                                   entity_type="ResearchTopic", label=t)

        if entry_type == "paper":
            paper = detail.get("paper") or {}
            arxiv_id = (paper.get("arxiv_id") or "").strip()
            title_raw = (paper.get("title") or "").strip()
            entity_id = arxiv_id or title_raw[:80] or r["title"][:80]
            if not entity_id:
                continue

            props = {
                "arxiv_id": arxiv_id,
                "doi": paper.get("doi", ""),
                "url": paper.get("url", ""),
                "upvotes": paper.get("upvotes", 0),
                "citations": paper.get("citations", 0),
                "year": paper.get("year"),
                "source": paper.get("source", ""),
                "keywords": paper.get("keywords", []),
                "authors": paper.get("authors", []),
            }
            _upsert_entity(cn, stats, entity_id=entity_id,
                           entity_type="MLPaper",
                           label=title_raw or entity_id,
                           props=props)

            # Paper → ResearchTopic edges
            if topic_str:
                for t in topic_str.split(","):
                    t = t.strip()
                    if t:
                        _upsert_edge(cn, stats,
                                     src_id=entity_id, src_type="MLPaper",
                                     dst_id=t, dst_type="ResearchTopic",
                                     rel="RESEARCHED_FOR", weight=signal)

            # Keyword cross-link to Task entities already in corpus
            kw_tokens = set()
            for kw in (paper.get("keywords") or []):
                kw_tokens.update(kw.lower().split())
            title_tokens = set((title_raw or "").lower().split())
            all_tokens = kw_tokens | title_tokens
            for task_label_lower, task_id in task_labels.items():
                task_tokens = set(task_label_lower.split())
                if len(task_tokens & all_tokens) >= 2:
                    _upsert_edge(cn, stats,
                                 src_id=entity_id, src_type="MLPaper",
                                 dst_id=task_id, dst_type="Task",
                                 rel="INFORMS", weight=signal)

        elif entry_type == "dataset":
            ds = detail.get("dataset") or {}
            ds_id = (ds.get("dataset_id") or "").strip()
            if not ds_id:
                continue

            props = {
                "url": ds.get("url", ""),
                "downloads": ds.get("downloads", 0),
                "likes": ds.get("likes", 0),
                "tags": ds.get("tags", []),
                "source": ds.get("source", ""),
            }
            _upsert_entity(cn, stats, entity_id=ds_id,
                           entity_type="MLDataset", label=ds_id, props=props)

            # Dataset → ResearchTopic edges
            if topic_str:
                for t in topic_str.split(","):
                    t = t.strip()
                    if t:
                        _upsert_edge(cn, stats,
                                     src_id=ds_id, src_type="MLDataset",
                                     dst_id=t, dst_type="ResearchTopic",
                                     rel="APPLIES_TO", weight=signal)

        elif detail.get("prompt"):
            # ml-intern deep-research output — represent as an MLInsight entity
            prompt = (detail.get("prompt") or "")[:80]
            entity_id = f"ml_intern::{prompt}"
            _upsert_entity(cn, stats, entity_id=entity_id,
                           entity_type="MLInsight", label=prompt,
                           props={"prompt": prompt,
                                  "output_preview": (detail.get("output") or "")[:200]})
            if topic_str:
                _upsert_edge(cn, stats,
                             src_id=entity_id, src_type="MLInsight",
                             dst_id=topic_str.split(",")[0].strip(),
                             dst_type="ResearchTopic",
                             rel="SYNTHESIZED_FOR", weight=signal)

    return last


def _ingest_ocw_courses(cn, stats: _Stats, since_id: int) -> int:
    """Promote MIT OCW course discoveries into the corpus graph.

    Reads every ``kind='ocw_course'`` row in ``learning_log`` with
    ``id > since_id`` and upserts:

    * ``OCWCourse``         — one entity per course slug (entity_id = course_id)
    * ``AcademicTopic``     — one entity per OCW search query (entity_id = query)
    * ``SystemsEngDomain``  — one entity per extracted subject tag
      (e.g. "Systems Engineering", "Operations Research", "Logistics")
    * Edges:
        - ``OCWCourse   ─TEACHES──────► AcademicTopic``
        - ``OCWCourse   ─BELONGS_TO──► SystemsEngDomain``
        - ``AcademicTopic ─GROUNDS────► ResearchTopic``  (cross-link to ML topics)
        - ``OCWCourse   ─INFORMS─────► Task``            (when title tokens match)

    This wires MIT's academic course catalogue into the same entity graph
    that holds ML papers, supply-chain tasks, and network peers — giving the
    RAG deepdive and synaptic agents a path from "supply chain planning" as a
    real-world task all the way back to the MIT course that originally
    formalised that theory.
    """
    try:
        rows = cn.execute(
            """SELECT id, logged_at, title, detail, signal_strength
               FROM learning_log
               WHERE kind='ocw_course' AND id > ?
               ORDER BY id LIMIT 300""",
            (int(since_id),),
        ).fetchall()
    except sqlite3.OperationalError:
        return since_id

    # Pre-fetch Task labels for cross-linking academic topics → operational tasks
    try:
        task_rows = cn.execute(
            "SELECT entity_id, label FROM corpus_entity WHERE entity_type='Task'"
        ).fetchall()
        task_labels = {(r["label"] or r["entity_id"]).lower(): r["entity_id"]
                       for r in task_rows}
    except Exception:
        task_labels = {}

    # Pre-fetch ResearchTopic ids for academic→ML cross-links
    try:
        rt_rows = cn.execute(
            "SELECT entity_id FROM corpus_entity WHERE entity_type='ResearchTopic'"
        ).fetchall()
        research_topics = {r["entity_id"].lower(): r["entity_id"] for r in rt_rows}
    except Exception:
        research_topics = {}

    last = since_id
    for r in rows:
        last = max(last, int(r["id"]))
        try:
            detail = json.loads(r["detail"] or "{}")
        except Exception:
            detail = {}

        course = detail.get("course") or {}
        topic_str = (detail.get("topic") or "").strip()
        signal = float(r["signal_strength"] or 0.8)

        course_id   = (course.get("course_id") or "").strip()
        title_raw   = (course.get("title") or "").strip()
        url         = course.get("url", "")
        course_num  = course.get("course_number", "")
        subjects    = course.get("subjects") or []
        query       = (course.get("query") or topic_str).strip()

        if not course_id:
            continue

        # Upsert OCWCourse entity
        _upsert_entity(cn, stats, entity_id=course_id,
                       entity_type="OCWCourse",
                       label=title_raw or course_id,
                       props={"url": url, "course_number": course_num,
                              "subjects": subjects, "query": query})

        # Upsert AcademicTopic entity (the OCW search query)
        if query:
            _upsert_entity(cn, stats, entity_id=query,
                           entity_type="AcademicTopic", label=query)
            _upsert_edge(cn, stats,
                         src_id=course_id, src_type="OCWCourse",
                         dst_id=query, dst_type="AcademicTopic",
                         rel="TEACHES", weight=signal)

            # Cross-link AcademicTopic → ResearchTopic if any ML topic is close
            query_lower = query.lower()
            for rt_lower, rt_id in research_topics.items():
                # Match on 2+ overlapping tokens
                qt = set(query_lower.split())
                rt = set(rt_lower.split())
                if len(qt & rt) >= 2:
                    _upsert_edge(cn, stats,
                                 src_id=query, src_type="AcademicTopic",
                                 dst_id=rt_id, dst_type="ResearchTopic",
                                 rel="GROUNDS", weight=0.7)

        # Upsert SystemsEngDomain entities for each subject tag
        for subj in subjects:
            subj = subj.strip()
            if not subj:
                continue
            _upsert_entity(cn, stats, entity_id=subj,
                           entity_type="SystemsEngDomain", label=subj)
            _upsert_edge(cn, stats,
                         src_id=course_id, src_type="OCWCourse",
                         dst_id=subj, dst_type="SystemsEngDomain",
                         rel="BELONGS_TO", weight=signal)

        # Cross-link OCWCourse → Task when 2+ title tokens match task labels
        title_tokens = set((title_raw or "").lower().split())
        for task_label_lower, task_id in task_labels.items():
            task_tokens = set(task_label_lower.split())
            if len(task_tokens & title_tokens) >= 2:
                _upsert_edge(cn, stats,
                             src_id=course_id, src_type="OCWCourse",
                             dst_id=task_id, dst_type="Task",
                             rel="INFORMS", weight=signal)

    return last


def _ingest_part_category(cn, stats: _Stats) -> int:
    try:
        rows = cn.execute(
            "SELECT part_key, category FROM part_category LIMIT 5000"
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    n = 0
    for r in rows:
        pk, cat = (r["part_key"] or "").strip(), (r["category"] or "").strip()
        if not pk or not cat:
            continue
        _upsert_entity(cn, stats, entity_id=pk, entity_type="Part", label=pk)
        _upsert_entity(cn, stats, entity_id=cat, entity_type="Category", label=cat)
        _upsert_edge(cn, stats, src_id=pk, src_type="Part",
                     dst_id=cat, dst_type="Category",
                     rel="CLASSIFIED_AS", weight=1.0)
        n += 1
    return n


def _ingest_otd_ownership(cn, stats: _Stats) -> int:
    try:
        rows = cn.execute(
            "SELECT row_key, owner FROM otd_ownership"
            " WHERE row_key IS NOT NULL AND owner IS NOT NULL LIMIT 5000"
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    n = 0
    for r in rows:
        row_key, owner = (r["row_key"] or "").strip(), (r["owner"] or "").strip()
        if not row_key or not owner:
            continue
        _upsert_entity(cn, stats, entity_id=row_key, entity_type="PO", label=row_key)
        _upsert_entity(cn, stats, entity_id=owner, entity_type="Owner", label=owner)
        _upsert_edge(cn, stats, src_id=owner, src_type="Owner",
                     dst_id=row_key, dst_type="PO", rel="OWNS", weight=1.0)
        n += 1
    return n


def _ingest_body_feedback(cn, stats: _Stats, since_id: int) -> int:
    """Close the loop: every User feedback row becomes a learning_log entry
    and an EXECUTED_BY edge from the User-as-Body back into the corpus."""
    try:
        rows = cn.execute(
            """SELECT bf.id, bf.directive_id, bf.logged_at, bf.status,
                      bf.outcome, bf.executed_by,
                      bd.title, bd.signal_kind, bd.target_entity, bd.owner_role
                 FROM body_feedback bf
                 LEFT JOIN body_directives bd ON bd.id = bf.directive_id
                WHERE bf.id > ? ORDER BY bf.id LIMIT 200""",
            (int(since_id),),
        ).fetchall()
    except sqlite3.OperationalError:
        return since_id
    last = since_id
    for r in rows:
        last = max(last, int(r["id"]))
        actor = (r["executed_by"] or "User").strip() or "User"
        _upsert_entity(cn, stats, entity_id=actor, entity_type="Body",
                       label=actor, props={"role": r["owner_role"]})
        if r["target_entity"]:
            try:
                t_type, t_id = (r["target_entity"].split("::", 1) + [None, None])[:2]
            except Exception:
                t_type, t_id = None, None
            if t_type and t_id:
                _upsert_entity(cn, stats, entity_id=t_id, entity_type=t_type, label=t_id)
                weight = 1.0 if (r["status"] or "") in ("done", "in_progress") else 0.3
                _upsert_edge(cn, stats, src_id=actor, src_type="Body",
                             dst_id=t_id, dst_type=t_type,
                             rel=f"EXECUTED_{(r['status'] or 'noop').upper()}",
                             weight=weight)
        signal = 0.9 if (r["status"] == "done") else (
                  0.6 if r["status"] == "in_progress" else (
                  0.3 if r["status"] == "ack" else 0.1))
        _log_learning(
            cn, stats, kind="body_feedback",
            title=f"Body {r['status']}: {r['title'] or 'directive'}",
            signal=signal,
            detail={
                "directive_id": int(r["directive_id"]),
                "status": r["status"],
                "signal_kind": r["signal_kind"],
                "outcome": r["outcome"],
                "executed_by": actor,
            },
            source_table="body_feedback", source_row_id=int(r["id"]),
        )
    return last


def _ingest_missions(cn, stats: _Stats, since_event_id: int) -> int:
    """Project Quest-Console missions into the corpus.

      Quest          (entity_type=Quest)        ← seeded from quests registry
      Mission        (entity_type=Mission)
        ─INSTANCE_OF→ Quest
        ─TARGETS────→ Site / Supplier / Part / PartFamily / Process / Customer
        ─SCOPED_BY──→ Tag (entity_type=ScopeTag)
      Body          (the User)
        ─LAUNCHED───→ Mission
        ─CLOSED─────→ Mission   (when status flips to done)

    Plus a learning_log entry for every new `mission_events` row so the
    Brain literally watches the User-Body work the open queue.
    """
    try:
        from . import mission_store, quests, findings_index
    except Exception:
        return since_event_id

    # Seed the Quest taxonomy (cheap; skipped if already present).
    try:
        for q in quests.list_quests(parent_id=None):
            _upsert_entity(cn, stats, entity_id=q.id, entity_type="Quest",
                           label=q.label,
                           props={"parent": q.parent_id, "scope_tags": list(q.scope_tags)})
            for sub in quests.list_quests(parent_id=q.id):
                _upsert_entity(cn, stats, entity_id=sub.id, entity_type="Quest",
                               label=sub.label,
                               props={"parent": sub.parent_id,
                                      "scope_tags": list(sub.scope_tags)})
                _upsert_edge(cn, stats, src_id=sub.id, src_type="Quest",
                             dst_id=q.id, dst_type="Quest",
                             rel="CHILD_OF", weight=1.0)
    except Exception:
        pass

    # Walk every mission (cheap — there are tens, not thousands).
    try:
        missions = mission_store.list_missions(limit=500)
    except Exception:
        return since_event_id

    type_for_kind = {
        "site":        "Site",
        "warehouse":   "Site",
        "supplier":    "Supplier",
        "customer":    "Customer",
        "part_family": "PartFamily",
        "process":     "Process",
    }

    for m in missions:
        mid = getattr(m, "id", "") or ""
        if not mid:
            continue
        site = getattr(m, "site", "") or "unknown"
        quest_id = getattr(m, "quest_id", "") or ""
        target_kind = getattr(m, "target_entity_kind", "") or "site"
        target_key = getattr(m, "target_entity_key", "") or site
        status = getattr(m, "status", "open") or "open"
        try:
            pct = float(getattr(m, "progress_pct", 0.0) or 0.0)
        except Exception:
            pct = 0.0

        _upsert_entity(cn, stats, entity_id=mid, entity_type="Mission",
                       label=f"{quest_id} @ {site}",
                       props={"site": site, "status": status,
                              "progress_pct": pct,
                              "quest_id": quest_id,
                              "target": f"{target_kind}:{target_key}"})

        if quest_id:
            _upsert_entity(cn, stats, entity_id=quest_id, entity_type="Quest",
                           label=quest_id)
            _upsert_edge(cn, stats, src_id=mid, src_type="Mission",
                         dst_id=quest_id, dst_type="Quest",
                         rel="INSTANCE_OF", weight=1.0)

        # Target entity edge
        target_type = type_for_kind.get(target_kind, "Site")
        if target_key:
            _upsert_entity(cn, stats, entity_id=str(target_key),
                           entity_type=target_type, label=str(target_key))
            _upsert_edge(cn, stats, src_id=mid, src_type="Mission",
                         dst_id=str(target_key), dst_type=target_type,
                         rel="TARGETS", weight=1.0)

        # Scope-tag edges
        for tag in (getattr(m, "scope_tags", []) or []):
            tag = str(tag)
            _upsert_entity(cn, stats, entity_id=tag, entity_type="ScopeTag",
                           label=tag)
            _upsert_edge(cn, stats, src_id=mid, src_type="Mission",
                         dst_id=tag, dst_type="ScopeTag",
                         rel="SCOPED_BY", weight=1.0)

        # Body launched / closed edges
        parsed_intent = getattr(m, "parsed_intent", {}) or {}
        actor = (parsed_intent.get("executed_by") or "User")
        _upsert_entity(cn, stats, entity_id=actor, entity_type="Body",
                       label=actor)
        _upsert_edge(cn, stats, src_id=actor, src_type="Body",
                     dst_id=mid, dst_type="Mission",
                     rel="LAUNCHED", weight=1.0)
        if status in ("done", "closed", "rejected"):
            _upsert_edge(cn, stats, src_id=actor, src_type="Body",
                         dst_id=mid, dst_type="Mission",
                         rel="CLOSED", weight=1.0)

    # Stream new mission_events as learnings (incremental cursor).
    last = since_event_id
    try:
        with findings_index._conn() as fcn:
            fcn.row_factory = sqlite3.Row
            rows = fcn.execute(
                """SELECT id, mission_id, kind, payload_json, created_at
                     FROM mission_events
                    WHERE id > ?
                    ORDER BY id LIMIT 500""",
                (int(since_event_id),),
            ).fetchall()
    except Exception:
        rows = []

    for r in rows:
        last = max(last, int(r["id"]))
        kind = (r["kind"] or "event")
        signal = {"kpi_snapshot": 0.6, "progress": 0.5,
                  "refreshed": 0.4, "status_changed": 0.7,
                  "artifact_attached": 0.5, "created": 0.3,
                  "launched": 0.4}.get(kind, 0.4)
        try:
            payload = json.loads(r["payload_json"] or "{}")
        except Exception:
            payload = {}
        _log_learning(
            cn, stats, kind="mission",
            title=f"Mission {r['mission_id']}: {kind}",
            signal=signal,
            detail={"mission_id": r["mission_id"], "kind": kind,
                    "payload": payload, "ts": r["created_at"]},
            source_table="mission_events", source_row_id=int(r["id"]),
        )
    return last


# ---------------------------------------------------------------------------
# Cursor management — so each round is incremental
# ---------------------------------------------------------------------------
def _get_cursor(cn, key: str) -> int:
    cn.execute(
        """CREATE TABLE IF NOT EXISTS corpus_cursor (
              key TEXT PRIMARY KEY, value INTEGER NOT NULL DEFAULT 0)"""
    )
    row = cn.execute("SELECT value FROM corpus_cursor WHERE key=?", (key,)).fetchone()
    return int(row["value"]) if row else 0


def _set_cursor(cn, key: str, val: int) -> None:
    cn.execute(
        "INSERT OR REPLACE INTO corpus_cursor(key, value) VALUES(?,?)",
        (key, int(val)),
    )


# ---------------------------------------------------------------------------
# Round driver
# ---------------------------------------------------------------------------
def _ingest_bridge_observations(cn, stats: _Stats) -> None:
    """Promote network_topology + bridge_rdp observations into the corpus graph.

    Called on every corpus refresh round.  Reads the ``network_topology``
    table written by :mod:`src.brain.network_learner` and the declared bridge
    targets from ``bridge_rdp.list_targets()`` then upserts:

    * ``Endpoint``  entity per discovered host:protocol:port (EMA-updated)
    * ``REACHABLE`` / ``UNREACHABLE`` edge → linked ``Site`` or ``Peer`` entity
    * ``SERVES``    edge Endpoint → Peer for reachable compute peers
    * ``BRIDGES_TO`` edge laptop-relay Endpoint → desktop Endpoint when the
      piggyback route is verified alive

    This wires the Brain's live network vision into the same entity graph
    that holds Parts, Suppliers, and MIT OCW courses — giving the RAG deepdive
    a path from e.g. ``Supplier`` → SQL-server ``Endpoint`` → ``Site`` that
    hosts the purchase-order database, grounding abstract supply-chain concepts
    in concrete, observable infrastructure.
    """
    # ── Bridge target entities (from bridge_rdp config) ──────────────────────
    try:
        import bridge_rdp  # type: ignore[import]
        bridge_targets   = bridge_rdp.list_targets()
        bridge_results   = bridge_rdp.probe_all()   # TCP probe each declared target
    except Exception:
        bridge_targets  = []
        bridge_results  = {}

    now_s = datetime.now(timezone.utc).isoformat()
    for target in bridge_targets:
        tname   = target.get("name") or ""
        host    = target.get("target_host") or ""
        port    = int(target.get("target_port") or 3389)
        proto   = target.get("protocol") or "rdp"
        ep_id   = f"bridge:{tname}"
        alive   = bridge_results.get(tname)   # True / False / None

        if not host:
            continue

        _upsert_entity(cn, stats, entity_id=ep_id, entity_type="Endpoint",
                       label=f"{tname} ({host}:{port})",
                       props={"host": host, "port": port,
                              "protocol": proto, "bridge_name": tname})

        # Link to an existing Site entity that shares this host address
        site_row = cn.execute(
            "SELECT entity_id FROM corpus_entity WHERE entity_type='Site' "
            "AND (entity_id=? OR props_json LIKE ?)",
            (host, f"%{host}%"),
        ).fetchone()
        if site_row:
            rel    = "REACHABLE" if alive else "UNREACHABLE"
            weight = 0.9 if alive else 0.1
            _upsert_edge(cn, stats, src_id=ep_id, src_type="Endpoint",
                         dst_id=site_row[0], dst_type="Site", rel=rel, weight=weight)

        # Piggyback: when bridge route is live, create laptop-relay → target edge
        if alive and proto in ("rdp", "tcp"):
            relay_id = "bridge:laptop-relay"
            _upsert_entity(cn, stats, entity_id=relay_id, entity_type="Endpoint",
                           label="Laptop RDP Relay",
                           props={"role": "relay", "protocol": "rdp"})
            _upsert_edge(cn, stats, src_id=relay_id, src_type="Endpoint",
                         dst_id=ep_id, dst_type="Endpoint",
                         rel="BRIDGES_TO", weight=0.85)

    # ── Network topology rows (from network_learner) ──────────────────────────
    try:
        topo_rows = cn.execute(
            "SELECT host, protocol, port, capability, "
            "last_ok, ema_success, ema_latency_ms, source "
            "FROM network_topology"
        ).fetchall()
    except Exception:
        topo_rows = []

    for row in topo_rows:
        host, proto, port, cap, last_ok, ema_ok, ema_lat, source = (
            row["host"], row["protocol"], row["port"], row["capability"],
            row["last_ok"], row["ema_success"], row["ema_latency_ms"], row["source"],
        )
        ep_id = f"{proto}:{host}:{port or 0}"
        label = f"{cap or host} [{proto}:{port or '?'}]"
        _upsert_entity(cn, stats, entity_id=ep_id, entity_type="Endpoint",
                       label=label,
                       props={"host": host, "port": port, "protocol": proto,
                              "ema_success": ema_ok, "ema_latency_ms": ema_lat,
                              "source": source})

        # Reachable compute peer → SERVES edge
        if ema_ok is not None and float(ema_ok) >= 0.5:
            peer_row = cn.execute(
                "SELECT entity_id FROM corpus_entity WHERE entity_type='Peer' "
                "AND (entity_id=? OR props_json LIKE ?)",
                (host, f"%{host}%"),
            ).fetchone()
            if peer_row:
                _upsert_edge(cn, stats, src_id=ep_id, src_type="Endpoint",
                             dst_id=peer_row[0], dst_type="Peer",
                             rel="SERVES", weight=float(ema_ok))


def refresh_corpus_round() -> dict:
    """One incremental sweep across every source stream."""
    if not _enabled():
        return {"enabled": False}

    init_schema()
    cfg = _cfg()
    min_seconds = float(cfg.get("min_seconds_between_rounds", 60.0))

    global _LAST_REFRESH_TS
    with _REFRESH_LOCK:
        if time.monotonic() - _LAST_REFRESH_TS < min_seconds:
            return {"skipped": True, "reason": "rate-limited"}
        _LAST_REFRESH_TS = time.monotonic()

    stats = _Stats()
    notes: list[str] = []

    # Ensure the network learner schema exists so _ingest_network /
    # _ingest_promotions can find their tables even if the learner hasn't
    # run yet (e.g. tests that skip the full autonomous_agent cycle).
    try:
        from .network_learner import init_schema as _nl_init
        _nl_init()
    except Exception as _e:
        notes.append(f"network_schema_init: {_e}")

    with _conn() as cn:
        # Incremental cursors
        c_st  = _get_cursor(cn, "self_train")
        c_dsp = _get_cursor(cn, "dispatch")
        c_net = _get_cursor(cn, "network")
        c_prm = _get_cursor(cn, "promotions")

        try: c_st  = _ingest_self_train(cn, stats, c_st)
        except Exception as e: notes.append(f"self_train: {e}")
        try: c_dsp = _ingest_dispatch(cn, stats, c_dsp)
        except Exception as e: notes.append(f"dispatch: {e}")
        try: _ingest_weights(cn, stats)
        except Exception as e: notes.append(f"weights: {e}")
        try: c_net = _ingest_network(cn, stats, c_net)
        except Exception as e: notes.append(f"network: {e}")
        try: c_prm = _ingest_promotions(cn, stats, c_prm)
        except Exception as e: notes.append(f"promotions: {e}")
        try: _ingest_part_category(cn, stats)
        except Exception as e: notes.append(f"part_category: {e}")
        try: _ingest_otd_ownership(cn, stats)
        except Exception as e: notes.append(f"otd_ownership: {e}")
        c_bf = _get_cursor(cn, "body_feedback")
        try: c_bf = _ingest_body_feedback(cn, stats, c_bf)
        except Exception as e: notes.append(f"body_feedback: {e}")
        c_msn = _get_cursor(cn, "missions")
        try: c_msn = _ingest_missions(cn, stats, c_msn)
        except Exception as e: notes.append(f"missions: {e}")
        c_mlr = _get_cursor(cn, "ml_research")
        try: c_mlr = _ingest_ml_research(cn, stats, c_mlr)
        except Exception as e: notes.append(f"ml_research: {e}")
        c_ocw = _get_cursor(cn, "ocw_courses")
        try: c_ocw = _ingest_ocw_courses(cn, stats, c_ocw)
        except Exception as e: notes.append(f"ocw_courses: {e}")
        try: _ingest_bridge_observations(cn, stats)
        except Exception as e: notes.append(f"bridge_observations: {e}")

        _set_cursor(cn, "self_train",  c_st)
        _set_cursor(cn, "dispatch",    c_dsp)
        _set_cursor(cn, "network",     c_net)
        _set_cursor(cn, "promotions",  c_prm)
        _set_cursor(cn, "body_feedback", c_bf)
        _set_cursor(cn, "missions",    c_msn)
        _set_cursor(cn, "ml_research", c_mlr)
        _set_cursor(cn, "ocw_courses", c_ocw)

        graph_backend = ((load_config().get("graph") or {}).get("backend")) or "networkx"

        cn.execute(
            """INSERT INTO corpus_round_log(ran_at, entities_added, entities_touched,
                  edges_added, edges_touched, learnings_logged, graph_backend, notes)
               VALUES(?,?,?,?,?,?,?,?)""",
            (datetime.now(timezone.utc).isoformat(),
             stats.entities_added, stats.entities_touched,
             stats.edges_added, stats.edges_touched,
             stats.learnings_logged, graph_backend, "; ".join(notes) or None),
        )

    return {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "entities_added": stats.entities_added,
        "entities_touched": stats.entities_touched,
        "edges_added": stats.edges_added,
        "edges_touched": stats.edges_touched,
        "learnings_logged": stats.learnings_logged,
        "notes": notes,
    }


# ---------------------------------------------------------------------------
# Materialize into the configured graph backend
# ---------------------------------------------------------------------------
def materialize_into_graph(*, max_entities: int = 5000,
                            max_edges: int = 20000) -> dict:
    """Project (corpus_entity, corpus_edge) into the active graph backend
    so every page that calls `get_graph_backend()` benefits."""
    try:
        from .graph_backend import get_graph_backend
        gb = get_graph_backend()
    except Exception as e:
        return {"ok": False, "error": f"graph backend unavailable: {e}"}

    n_nodes = 0
    n_edges = 0
    with _conn() as cn:
        ents = cn.execute(
            "SELECT entity_id, entity_type, label, props_json FROM corpus_entity LIMIT ?",
            (int(max_entities),),
        ).fetchall()
        for r in ents:
            try:
                props = json.loads(r["props_json"] or "{}")
            except Exception:
                props = {}
            node_id = f"{r['entity_type']}::{r['entity_id']}"
            try:
                gb.add_node(node_id,
                            label=r["label"] or r["entity_id"],
                            entity_type=r["entity_type"], **props)
                n_nodes += 1
            except Exception as e:
                logging.debug(f"corpus: add_node failed for {node_id}: {e}")
        edges = cn.execute(
            """SELECT src_id, src_type, dst_id, dst_type, rel, weight, samples
               FROM corpus_edge LIMIT ?""",
            (int(max_edges),),
        ).fetchall()
        for r in edges:
            src = f"{r['src_type']}::{r['src_id']}"
            dst = f"{r['dst_type']}::{r['dst_id']}"
            try:
                gb.add_edge(src, dst, r["rel"],
                            weight=float(r["weight"]),
                            samples=int(r["samples"]))
                n_edges += 1
            except Exception as e:
                logging.debug(f"corpus: add_edge failed {src}->{dst}: {e}")
    return {"ok": True, "nodes_projected": n_nodes, "edges_projected": n_edges}


# ---------------------------------------------------------------------------
# Public read APIs — for the Decision Log + Graph pages
# ---------------------------------------------------------------------------
def recent_learnings(limit: int = 50, kind: str | None = None) -> list[dict]:
    init_schema()
    with _conn() as cn:
        if kind:
            rows = cn.execute(
                """SELECT id, logged_at, kind, title, detail, signal_strength,
                          source_table, source_row_id
                   FROM learning_log WHERE kind=?
                   ORDER BY id DESC LIMIT ?""",
                (kind, int(limit)),
            ).fetchall()
        else:
            rows = cn.execute(
                """SELECT id, logged_at, kind, title, detail, signal_strength,
                          source_table, source_row_id
                   FROM learning_log ORDER BY id DESC LIMIT ?""",
                (int(limit),),
            ).fetchall()
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        try:
            d["detail"] = json.loads(d.get("detail") or "{}")
        except Exception:
            pass
        out.append(d)
    return out


def corpus_summary() -> dict:
    init_schema()
    with _conn() as cn:
        ents = cn.execute(
            "SELECT entity_type, COUNT(*) AS n FROM corpus_entity GROUP BY entity_type"
        ).fetchall()
        edges = cn.execute(
            "SELECT rel, COUNT(*) AS n FROM corpus_edge GROUP BY rel"
        ).fetchall()
    return {
        "entities_by_type": {r["entity_type"]: int(r["n"]) for r in ents},
        "edges_by_rel":     {r["rel"]:         int(r["n"]) for r in edges},
    }


def schedule_in_background(interval_s: int | None = None) -> threading.Thread:
    interval = int(interval_s or _cfg().get("interval_s", 900))

    def _loop() -> None:
        while True:
            try:
                refresh_corpus_round()
                materialize_into_graph()
            except Exception as e:
                logging.warning(f"knowledge_corpus background round failed: {e}")
            time.sleep(max(60, interval))

    t = threading.Thread(target=_loop, name="knowledge_corpus", daemon=True)
    t.start()
    return t
