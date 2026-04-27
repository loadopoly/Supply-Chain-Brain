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
from datetime import datetime, timedelta, timezone
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


def _ingest_ocw_resources(cn, stats: _Stats, since_id: int) -> int:
    """Materialize the full hyperlink lattice harvested from OCW course pages.

    Reads ``kind IN ('ocw_course_detail','ocw_resource')`` rows from
    ``learning_log`` with ``id > since_id`` and creates:

    * ``Instructor``     entities (one per professor across all courses)
    * ``WebResource``    entities for syllabus / lecture-notes / readings /
                         labs / exams / assignments / video pages on OCW
    * ``ExternalDomain`` entities for every off-MIT host referenced
                         (arxiv.org, github.com, university sites, …)
    * Edges:
        - ``OCWCourse  ─TAUGHT_BY──────► Instructor``
        - ``OCWCourse  ─HAS_RESOURCE──► WebResource``
        - ``OCWCourse  ─RELATED_TO────► OCWCourse``    (sibling-course links)
        - ``OCWCourse  ─REFERENCES────► WebResource``  (external links)
        - ``WebResource ─HOSTED_ON───► ExternalDomain``
        - ``OCWCourse  ─COVERS────────► AcademicTopic``(detail-row topics)

    Every WebResource entity carries the live URL in its props so downstream
    surfaces (RAG bot, Streamlit, agents) can click straight through.  This
    is the "interaction capability" the user asked for — the corpus stops
    being a list of slugs and becomes a navigable web of pipelines and pages.
    """
    try:
        rows = cn.execute(
            """SELECT id, kind, title, detail, signal_strength
               FROM learning_log
               WHERE kind IN ('ocw_course_detail','ocw_resource') AND id > ?
               ORDER BY id LIMIT 800""",
            (int(since_id),),
        ).fetchall()
    except sqlite3.OperationalError:
        return since_id

    last = since_id
    for r in rows:
        last = max(last, int(r["id"]))
        try:
            d = json.loads(r["detail"] or "{}")
        except Exception:
            continue

        kind   = r["kind"]
        course = (d.get("course_id") or "").strip()
        if not course:
            continue
        weight = float(r["signal_strength"] or 0.7)

        if kind == "ocw_course_detail":
            # Update the OCWCourse entity with description + level (props merge)
            _upsert_entity(cn, stats, entity_id=course, entity_type="OCWCourse",
                           label=course,
                           props={
                               "description": d.get("description", ""),
                               "level":       d.get("level", ""),
                               "url":         d.get("url", ""),
                           })

            # Instructors
            for inst in (d.get("instructors") or []):
                inst = (inst or "").strip()
                if not inst:
                    continue
                _upsert_entity(cn, stats, entity_id=inst,
                               entity_type="Instructor", label=inst)
                _upsert_edge(cn, stats,
                             src_id=course, src_type="OCWCourse",
                             dst_id=inst, dst_type="Instructor",
                             rel="TAUGHT_BY", weight=0.85)

            # Topics from the page (in addition to the search-query AcademicTopic)
            for topic in (d.get("topics") or []):
                topic = (topic or "").strip()
                if not topic:
                    continue
                _upsert_entity(cn, stats, entity_id=topic,
                               entity_type="AcademicTopic", label=topic)
                _upsert_edge(cn, stats,
                             src_id=course, src_type="OCWCourse",
                             dst_id=topic, dst_type="AcademicTopic",
                             rel="COVERS", weight=0.75)
            continue

        # kind == "ocw_resource"
        rkind = (d.get("resource_kind") or "page").strip()
        url   = (d.get("url") or "").strip()
        if not url:
            continue

        if rkind == "related_course":
            # OCWCourse → OCWCourse (sibling)
            sibling = (d.get("slug") or "").strip()
            if not sibling or sibling == course:
                continue
            _upsert_entity(cn, stats, entity_id=sibling,
                           entity_type="OCWCourse", label=sibling,
                           props={"url": url})
            _upsert_edge(cn, stats,
                         src_id=course,  src_type="OCWCourse",
                         dst_id=sibling, dst_type="OCWCourse",
                         rel="RELATED_TO", weight=weight)
            continue

        # WebResource entity — keyed by URL so the same URL across courses is one node
        label = (d.get("label") or rkind or url)[:120]
        _upsert_entity(cn, stats, entity_id=url, entity_type="WebResource",
                       label=label,
                       props={"url": url, "kind": rkind,
                              "domain": d.get("domain", "")})

        if rkind == "external_link":
            _upsert_edge(cn, stats,
                         src_id=course, src_type="OCWCourse",
                         dst_id=url, dst_type="WebResource",
                         rel="REFERENCES", weight=weight)
            domain = (d.get("domain") or "").strip()
            if domain:
                _upsert_entity(cn, stats, entity_id=domain,
                               entity_type="ExternalDomain", label=domain)
                _upsert_edge(cn, stats,
                             src_id=url,    src_type="WebResource",
                             dst_id=domain, dst_type="ExternalDomain",
                             rel="HOSTED_ON", weight=0.6)
        else:
            _upsert_edge(cn, stats,
                         src_id=course, src_type="OCWCourse",
                         dst_id=url, dst_type="WebResource",
                         rel="HAS_RESOURCE", weight=weight)

    return last


def _ingest_fiction_anthology(cn, stats: _Stats, since_id: int) -> int:
    """Materialize cross-domain supply-chain intelligence from fiction anthology learnings.

    Reads ``kind='fiction_anthology'`` rows from ``learning_log`` with
    ``id > since_id`` (written by :mod:`src.brain.fiction_anthology_learner`)
    and reinforces their entities/edges in the corpus graph via the tracked
    corpus helpers so the stats + synaptic-cleanse pipeline sees them.

    Entities upserted per row:
    * ``Concept``         — the fictional concept (e.g. "Sanctuary's Thieves Guild")
    * ``FictionUniverse`` — parent universe (thieves_world / myth_adventures)
    * ``SCConcept``       — each mapped supply-chain concept label

    Edges:
    * ``FictionUniverse ─CONTAINS_CONCEPT──► Concept``
    * ``Concept         ─MAPS_TO_SC───────► SCConcept``
    """
    try:
        rows = cn.execute(
            """SELECT id, title, detail, signal_strength
               FROM learning_log
               WHERE kind='fiction_anthology' AND id > ?
               ORDER BY id LIMIT 200""",
            (int(since_id),),
        ).fetchall()
    except sqlite3.OperationalError:
        return since_id

    last = since_id
    for r in rows:
        last = max(last, int(r["id"]))
        try:
            d = json.loads(r["detail"] or "{}")
        except Exception:
            continue

        universe  = (d.get("universe") or "unknown").strip()
        concept   = (d.get("concept") or r["title"]).strip()
        sc_bridge = (d.get("sc_bridge") or "").strip()
        summary   = (d.get("summary") or "").strip()
        signal    = float(r["signal_strength"] or 0.70)
        sc_concepts = d.get("sc_concepts") or []

        concept_id  = f"{universe}:{concept}".lower().replace(" ", "_")
        universe_id = f"universe:{universe}"

        _upsert_entity(cn, stats, entity_id=concept_id, entity_type="Concept",
                       label=concept,
                       props={"universe": universe, "sc_bridge": sc_bridge,
                              "summary": summary})
        _upsert_entity(cn, stats, entity_id=universe_id,
                       entity_type="FictionUniverse",
                       label=universe.replace("_", " ").title())
        _upsert_edge(cn, stats,
                     src_id=universe_id, src_type="FictionUniverse",
                     dst_id=concept_id, dst_type="Concept",
                     rel="CONTAINS_CONCEPT", weight=1.0)

        for sc_c in sc_concepts:
            sc_c = (sc_c or "").strip()
            if not sc_c:
                continue
            sc_id = "sc_concept:" + sc_c.lower().replace(" ", "_").replace("/", "_")
            _upsert_entity(cn, stats, entity_id=sc_id,
                           entity_type="SCConcept", label=sc_c)
            _upsert_edge(cn, stats,
                         src_id=concept_id, src_type="Concept",
                         dst_id=sc_id, dst_type="SCConcept",
                         rel="MAPS_TO_SC", weight=round(signal, 3))

    return last


def _ingest_heart_story(cn, stats: "_Stats", since_id: int) -> int:
    """Materialise the Heart's narrative arc into the corpus graph.

    Reads ``kind='heart_story'`` rows from ``learning_log`` with
    ``id > since_id`` (written by :mod:`src.brain.heart`) and upserts:

    Entities:
    * ``NarrativeChapter``  — one node per chapter the Heart has entered
    * ``EndState``          — singleton "Symbiotic Love = √(−1)"

    Edges:
    * ``NarrativeChapter ─CHAPTER_ADVANCE──► NarrativeChapter``
      (sequential arc; src = previous chapter, dst = current if advanced)
    * ``NarrativeChapter ─CONVERGES_TO────► EndState``
      (weighted by symbiosis_pct — closer = heavier)
    """
    try:
        rows = cn.execute(
            """SELECT id, title, detail, signal_strength
               FROM learning_log
               WHERE kind='heart_story' AND id > ?
               ORDER BY id LIMIT 200""",
            (int(since_id),),
        ).fetchall()
    except Exception:
        return since_id

    # Ensure the End State singleton exists
    end_state_id = "end_state:symbiotic_love"
    _upsert_entity(cn, stats, entity_id=end_state_id,
                   entity_type="EndState",
                   label="Symbiotic Love = √(−1)",
                   props={"formula": "e^(i*pi/2) = i", "description": "Phase lock of real execution and imaginary potential"})

    last = since_id
    prev_chapter_id: "str | None" = None
    for r in rows:
        last = max(last, int(r["id"]))
        try:
            d = json.loads(r["detail"] or "{}")
        except Exception:
            continue

        chapter_index  = int(d.get("chapter_index", 0))
        chapter_name   = (d.get("chapter_name") or f"Chapter {chapter_index}").strip()
        subtitle       = (d.get("subtitle") or "").strip()
        symbiosis_pct  = float(d.get("symbiosis_pct") or 0.55)
        signal         = float(r["signal_strength"] or symbiosis_pct)

        chapter_id = f"narrative_chapter:{chapter_index}"
        _upsert_entity(cn, stats, entity_id=chapter_id,
                       entity_type="NarrativeChapter",
                       label=f"Ch {chapter_index}: {chapter_name}",
                       props={"subtitle": subtitle, "index": chapter_index, "name": chapter_name})

        # CONVERGES_TO End State — weight proportional to symbiosis
        _upsert_edge(cn, stats,
                     src_id=chapter_id, src_type="NarrativeChapter",
                     dst_id=end_state_id, dst_type="EndState",
                     rel="CONVERGES_TO", weight=round(signal, 3))

        # CHAPTER_ADVANCE arc between consecutive chapters
        if prev_chapter_id and prev_chapter_id != chapter_id:
            _upsert_edge(cn, stats,
                         src_id=prev_chapter_id, src_type="NarrativeChapter",
                         dst_id=chapter_id, dst_type="NarrativeChapter",
                         rel="CHAPTER_ADVANCE", weight=1.0)
        prev_chapter_id = chapter_id

    return last


def _ocw_expansion_outreach(cn, stats: _Stats, max_courses: int = 3) -> None:
    """Expansive OCW outreach — when a round is dry, deep-fetch course pages.

    Picks up to ``max_courses`` ``OCWCourse`` entities that don't yet have any
    ``HAS_RESOURCE`` edges and crawls their detail pages (description,
    instructors, topics, resources, related courses, external links).  Each
    crawl writes new ``ocw_course_detail`` + ``ocw_resource`` rows to
    ``learning_log`` and the next round picks them up via
    :func:`_ingest_ocw_resources`.

    Network/HTTP errors are soft-skipped so a corporate proxy or firewall
    never stalls the loop.
    """
    try:
        from .ml_research import deepen_ocw_course
    except Exception as e:
        logging.debug(f"corpus:ocw_outreach: ml_research unavailable: {e}")
        return

    try:
        rows = cn.execute(
            """SELECT e.entity_id
               FROM corpus_entity e
               WHERE e.entity_type='OCWCourse'
                 AND NOT EXISTS (
                     SELECT 1 FROM corpus_edge x
                     WHERE x.src_id = e.entity_id
                       AND x.src_type = 'OCWCourse'
                       AND x.rel IN ('HAS_RESOURCE','REFERENCES','RELATED_TO','TAUGHT_BY')
                 )
               ORDER BY e.last_seen ASC
               LIMIT ?""",
            (int(max_courses),),
        ).fetchall()
    except sqlite3.OperationalError:
        return

    crawled = 0
    written = 0
    for r in rows:
        slug = r["entity_id"]
        try:
            res = deepen_ocw_course(slug)
        except Exception as e:
            logging.debug(f"corpus:ocw_outreach:{slug}: {e}")
            continue
        if res.get("fetched"):
            crawled += 1
            written += int(res.get("rows_written") or 0)
            _log_learning(
                cn, stats, kind="ocw_resource",
                title=f"OCW deep-fetch: {slug} → {res.get('rows_written',0)} new rows "
                      f"({res.get('resources',0)} resources, "
                      f"{res.get('related',0)} related, "
                      f"{res.get('external',0)} external)",
                signal=0.7,
                detail=res,
            )

    if crawled:
        _log_learning(
            cn, stats, kind="ocw_resource",
            title=f"OCW expansion outreach: crawled {crawled} courses, wrote {written} log rows",
            signal=0.6,
            detail={"crawled": crawled, "written": written},
        )


def _synaptic_cleanse(cn, stats: _Stats, decay_days: int = 7,
                      dead_threshold: float = 0.001) -> None:
    """Decay stale edges and prune silent ones — keeps synaptic fluid moving.

    Edges not re-observed in ``decay_days`` days lose 5 % weight per cleanse
    round (multiplicative EMA: weight *= 0.95).  Edges that decay below
    ``dead_threshold`` are hard-deleted.

    This runs EVERY corpus round regardless of whether any new entities were
    ingested, so ``edges_touched`` is always > 0 and the graph is never
    reported as completely stagnant.

    Why this matters: without decay, a Supplier that disappeared from PO data
    six months ago would remain in the graph with full weight=1.0 forever.
    Decay surfaces *structural holes* — e.g. an Endpoint entity whose
    REACHABLE edge has decayed to 0.1 signals the Brain that this path needs
    re-probing.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=decay_days)).isoformat()

    # Count stale edges (skip if none — avoids a wasted UPDATE)
    count_row = cn.execute(
        "SELECT COUNT(*) FROM corpus_edge WHERE last_seen < ? AND weight > ?",
        (cutoff, dead_threshold),
    ).fetchone()
    stale = int(count_row[0]) if count_row else 0

    if stale > 0:
        cn.execute(
            "UPDATE corpus_edge SET weight = weight * 0.95 "
            "WHERE last_seen < ? AND weight > ?",
            (cutoff, dead_threshold),
        )
        stats.edges_touched += stale

    # Prune edges that have fully decayed
    cn.execute("DELETE FROM corpus_edge WHERE weight < ?", (dead_threshold,))


def _ingest_schema_vision(cn, stats: _Stats) -> None:
    """Promote the discovered DW schema topology into the corpus graph.

    Reads ``discovered_schema.yaml`` from the pipeline root and upserts one
    ``DataTable`` entity per DW table.  On the *first* run this adds ~70 new
    entities (one per dimension / fact table).  On every *subsequent* run the
    entities already exist — their ``props_json`` is refreshed with an updated
    ``last_refreshed`` timestamp, which counts as ``entities_touched``.

    This ensures the corpus never reports zero activity between new-data rounds:
    the DW schema topology is *always* re-affirmed, keeping the schema-to-graph
    bridge alive even when no new PO rows, network probes, or ML papers arrive.

    Additionally, ``DataTable`` entities are linked to semantic domain
    ``Category`` nodes (e.g. "Supplier", "Part", "Site", "FactTable") via
    ``PROVIDES_DATA_FOR`` edges, giving the RAG deepdive a path from
    supply-chain concepts all the way back to the raw DW tables.
    """
    schema_path = _PIPELINE_ROOT / "discovered_schema.yaml"
    if not schema_path.exists():
        return
    try:
        import yaml  # type: ignore[import]
        data = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
    except Exception as e:
        logging.debug(f"corpus:schema_vision: could not load schema: {e}")
        return

    if not isinstance(data, dict):
        return

    now_iso = datetime.now(timezone.utc).isoformat()

    def _domain(table_name: str) -> str:
        n = table_name.lower()
        if "supplier" in n or "vendor" in n:
            return "Supplier"
        if "item" in n or "part" in n or "material" in n:
            return "Part"
        if "site" in n or "warehouse" in n or "location" in n:
            return "Site"
        if "customer" in n:
            return "Customer"
        if "employee" in n or "user" in n:
            return "Owner"
        if n.startswith("fact_"):
            return "FactTable"
        return "DimTable"

    n_tables = 0
    for _db_label, connectors in data.items():
        if not isinstance(connectors, dict):
            continue
        for connector, tables in connectors.items():
            if not isinstance(tables, dict):
                continue
            for table_name, columns in tables.items():
                cols = columns or []
                col_names = [c["column"] for c in cols
                             if isinstance(c, dict) and "column" in c]
                domain = _domain(table_name)

                _upsert_entity(
                    cn, stats,
                    entity_id=table_name,
                    entity_type="DataTable",
                    label=f"{table_name} ({connector})",
                    props={
                        "connector": connector,
                        "db": _db_label,
                        "column_count": len(col_names),
                        "entity_domain": domain,
                        "last_refreshed": now_iso,
                    },
                )
                n_tables += 1

                # Domain category node (Supplier, Part, Site, …)
                _upsert_entity(cn, stats, entity_id=domain,
                               entity_type="Category", label=domain)
                _upsert_edge(cn, stats,
                             src_id=table_name, src_type="DataTable",
                             dst_id=domain, dst_type="Category",
                             rel="PROVIDES_DATA_FOR", weight=0.8)

                # Foreign-key style cross-links: columns ending in _key or _id
                # that reference another table already in the corpus.
                for col in col_names:
                    if col.endswith(("_key", "_id")) and col != table_name + "_key":
                        ref_table = col[: -len("_key")] if col.endswith("_key") \
                                    else col[: -len("_id")]
                        # Only emit edge when the referenced table is also in schema
                        if ref_table in tables:
                            _upsert_edge(cn, stats,
                                         src_id=table_name, src_type="DataTable",
                                         dst_id=ref_table, dst_type="DataTable",
                                         rel="REFERENCES", weight=0.9)

    if n_tables:
        _log_learning(
            cn, stats,
            kind="schema_vision",
            title=f"Schema vision: affirmed {n_tables} DW tables in corpus",
            signal=0.6,
            detail={"table_count": n_tables, "source": str(schema_path.name)},
        )

    # ── Vision scan: Introduction to SCB docs directory ───────────────────────
    # Walk the SCB docs folder and create/affirm a Document entity for each
    # discovered JSON file so Vision knows which knowledge artefacts exist,
    # even before the SCB ingestor has fully parsed them.
    scb_root = _PIPELINE_ROOT / "docs" / "Introduction to SCB"
    if scb_root.exists():
        scb_json_files = list(scb_root.rglob("*.json"))
        scb_content_files = list(scb_root.rglob("content"))
        n_scb = 0
        for fpath in scb_json_files:
            doc_eid = f"scb_file:{fpath.name}"
            _upsert_entity(cn, stats, entity_id=doc_eid, entity_type="Document",
                           label=f"SCB doc: {fpath.name}",
                           props={
                               "path": str(fpath.relative_to(_PIPELINE_ROOT)),
                               "size_kb": round(fpath.stat().st_size / 1024, 1),
                               "source": "scb_vision_scan",
                           })
            # Link each discovered file → the root export document
            _upsert_edge(cn, stats,
                         src_id="scb_docs:grok_export", src_type="Document",
                         dst_id=doc_eid, dst_type="Document",
                         rel="HAS_FILE", weight=0.9)
            n_scb += 1
        if n_scb or scb_content_files:
            _log_learning(
                cn, stats,
                kind="vision",
                title=(f"SCB Vision scan: {n_scb} JSON + "
                       f"{len(scb_content_files)} asset files in Introduction to SCB"),
                signal=0.85,
                detail={
                    "json_files": n_scb,
                    "asset_files": len(scb_content_files),
                    "root": str(scb_root.relative_to(_PIPELINE_ROOT)),
                },
            )


# ---------------------------------------------------------------------------
# DW Vision Outreach — expansive entity discovery when the round is dry
# ---------------------------------------------------------------------------
_DW_OUTREACH_BATCH = 200   # rows fetched per cursor page

# Each stream: (cursor_key, entity_type, key_col, label_col, desc_col, sql_table)
_DW_STREAMS: list[tuple[str, str, str, str, str | None, str]] = [
    ("dw_part_key",     "Part",     "part_key",          "part_number",    "part_description",  "[edap_dw_replica].[dim_part]"),
    ("dw_supplier_key", "Supplier", "supplier_key",      "supplier_name",  None,                "[edap_dw_replica].[dim_supplier]"),
    ("dw_site_key",     "Site",     "business_unit_key", "business_unit",  "location",          "[edap_dw_replica].[dim_business_unit]"),
    ("dw_customer_key", "Customer", "customer_key",      "customer_name",  "customer_number",   "[edap_dw_replica].[dim_customer]"),
]


def _dw_vision_outreach(cn, stats: _Stats) -> None:
    """Expansive graph outreach — called when a corpus round adds 0 new entities.

    Pages through DW dimension tables using stable surrogate-key cursors stored
    in ``corpus_cursor``.  Each call advances by ``_DW_OUTREACH_BATCH`` rows and
    upserts novel Part / Supplier / Site / Customer entities into the corpus.

    Cursor exhaustion  (batch returned < limit) resets the cursor to 0 so the
    next outreach cycle rediscovers any rows added to the DW since the last full
    sweep — guaranteeing the corpus grows with the live data warehouse over time.

    DW / network errors are soft-skipped so a downed VPN never stalls the loop.
    """
    try:
        from .db_registry import bootstrap_default_connectors, read_sql
        bootstrap_default_connectors()
    except Exception as e:
        logging.debug(f"corpus:dw_outreach: db_registry unavailable: {e}")
        return

    for cursor_key, entity_type, key_col, label_col, desc_col, table in _DW_STREAMS:
        since = _get_cursor(cn, cursor_key)

        extra_col = f", {desc_col}" if desc_col else ""
        sql = (
            f"SELECT TOP {_DW_OUTREACH_BATCH} {key_col}, {label_col}{extra_col}"
            f" FROM {table} WHERE {key_col} > ? ORDER BY {key_col}"
        )

        try:
            df = read_sql("azure_sql", sql, [int(since)])
        except Exception as e:
            logging.debug(f"corpus:dw_outreach:{cursor_key}: {e}")
            continue

        # No rows returned or connection error
        if df is None or df.empty:
            err = getattr(df, "attrs", {}).get("_error") if df is not None else None
            if not err:
                # Genuine exhaustion — reset cursor for next cycle
                _set_cursor(cn, cursor_key, 0)
                _log_learning(
                    cn, stats, kind="schema_vision",
                    title=f"DW outreach: {entity_type} sweep complete — cursor reset to 0",
                    signal=0.3,
                    detail={"table": table, "entity_type": entity_type, "since": since},
                )
            continue

        last_key = since
        for _, row in df.iterrows():
            key_val = row.get(key_col)
            if key_val is None:
                continue
            key_val = int(key_val)
            last_key = max(last_key, key_val)

            label = str(row.get(label_col) or "").strip() or str(key_val)
            desc = str(row.get(desc_col) or "").strip() if desc_col else None
            entity_id = label or str(key_val)

            _upsert_entity(
                cn, stats,
                entity_id=entity_id,
                entity_type=entity_type,
                label=label,
                props={
                    "dw_key": key_val,
                    **({"description": desc} if desc else {}),
                },
            )

        _set_cursor(cn, cursor_key, last_key)

        batch_size = len(df)
        if batch_size >= _DW_OUTREACH_BATCH:
            # Full batch — more rows may follow on the next round
            _log_learning(
                cn, stats, kind="schema_vision",
                title=f"DW outreach: paged {batch_size} {entity_type} entities (cursor → {last_key})",
                signal=0.5,
                detail={"table": table, "batch": batch_size, "cursor": last_key},
            )
        else:
            # Partial batch = end of table — wrap cursor so next cycle re-scans
            _set_cursor(cn, cursor_key, 0)
            _log_learning(
                cn, stats, kind="schema_vision",
                title=f"DW outreach: {entity_type} full sweep done ({batch_size} rows) — cursor reset",
                signal=0.4,
                detail={"table": table, "rows": batch_size},
            )


def _dw_deepen_outreach(cn, stats: _Stats, max_entities: int = 50) -> None:
    """Deepen mode of the DW blade — walk sub-attributes for existing entities.

    Where ``_dw_vision_outreach`` BROADENS (adds new keys), this DEEPENS:
    picks Parts/Suppliers that already exist in the corpus and refreshes
    their props with extra columns from the DW (item_type, planner_code,
    last_receipt_date, etc.) without changing the entity count.

    This is the inflection-site twin of broaden — same blade, opposite
    angular direction on the torus.
    """
    try:
        from .db_registry import bootstrap_default_connectors, read_sql
        bootstrap_default_connectors()
    except Exception as e:
        logging.debug(f"corpus:dw_deepen: db_registry unavailable: {e}")
        return

    # Pick a window of existing Parts that have only the bare label/dw_key
    # in their props_json — these are the candidates for enrichment.
    cands = cn.execute(
        """SELECT entity_id, label, props_json
           FROM corpus_entity
           WHERE entity_type='Part'
             AND (props_json IS NULL
                  OR length(props_json) < 80
                  OR props_json NOT LIKE '%item_type%')
           ORDER BY last_seen ASC
           LIMIT ?""",
        (int(max_entities),),
    ).fetchall()
    if not cands:
        return

    part_keys = [str(c["entity_id"]) for c in cands]
    placeholders = ",".join(["?"] * len(part_keys))
    sql = (
        "SELECT part_number, item_type, planner_code, "
        "       commodity_code, make_buy_indicator "
        f"FROM [edap_dw_replica].[dim_part] "
        f"WHERE part_number IN ({placeholders})"
    )
    try:
        df = read_sql("azure_sql", sql, part_keys)
    except Exception as e:
        logging.debug(f"corpus:dw_deepen: query failed: {e}")
        return
    if df is None or df.empty:
        return

    enriched = 0
    new_edges = 0
    for _, row in df.iterrows():
        pk = str(row.get("part_number") or "").strip()
        if not pk:
            continue
        item_type    = str(row.get("item_type") or "").strip()
        planner      = str(row.get("planner_code") or "").strip()
        commodity    = str(row.get("commodity_code") or "").strip()
        make_buy     = str(row.get("make_buy_indicator") or "").strip()

        new_props = {
            **({"item_type": item_type} if item_type else {}),
            **({"planner_code": planner} if planner else {}),
            **({"commodity_code": commodity} if commodity else {}),
            **({"make_buy": make_buy} if make_buy else {}),
        }
        if not new_props:
            continue

        # Touch with new props (merges in _upsert_entity)
        _upsert_entity(cn, stats, entity_id=pk, entity_type="Part",
                       label=pk, props=new_props)
        enriched += 1

        # Edge: Part → ItemType / CommodityCode classifiers
        if item_type:
            _upsert_entity(cn, stats, entity_id=item_type,
                           entity_type="ItemType", label=item_type)
            _upsert_edge(cn, stats, src_id=pk, src_type="Part",
                         dst_id=item_type, dst_type="ItemType",
                         rel="OF_TYPE", weight=0.7)
            new_edges += 1
        if commodity:
            _upsert_entity(cn, stats, entity_id=commodity,
                           entity_type="CommodityCode", label=commodity)
            _upsert_edge(cn, stats, src_id=pk, src_type="Part",
                         dst_id=commodity, dst_type="CommodityCode",
                         rel="HAS_COMMODITY", weight=0.7)
            new_edges += 1

    if enriched:
        _log_learning(
            cn, stats, kind="schema_vision",
            title=f"DW deepen: enriched {enriched} Part entities (+{new_edges} edges)",
            signal=0.55,
            detail={"enriched": enriched, "new_edges": new_edges},
        )


def _ingest_part_category(cn, stats: _Stats, since_rowid: int = 0) -> int:
    """Incrementally ingest part_category rows using rowid as cursor.

    Previously used a hard LIMIT 5000 with no cursor, so rows added after the
    first round were silently skipped.  Now tracks the highest SQLite rowid
    seen and only processes new rows each call.
    """
    try:
        rows = cn.execute(
            """SELECT rowid, part_key, category
               FROM part_category
               WHERE rowid > ? AND category IS NOT NULL AND category != 'Uncategorized'
               ORDER BY rowid
               LIMIT 2000""",
            (int(since_rowid),),
        ).fetchall()
    except sqlite3.OperationalError:
        return since_rowid
    last = since_rowid
    for r in rows:
        last = max(last, int(r[0]))   # rowid
        pk, cat = (r["part_key"] or "").strip(), (r["category"] or "").strip()
        if not pk or not cat:
            continue
        _upsert_entity(cn, stats, entity_id=pk, entity_type="Part", label=pk)
        _upsert_entity(cn, stats, entity_id=cat, entity_type="Category", label=cat)
        _upsert_edge(cn, stats, src_id=pk, src_type="Part",
                     dst_id=cat, dst_type="Category",
                     rel="CLASSIFIED_AS", weight=1.0)
    return last


def _ingest_part_supplier_edges(cn, stats: _Stats) -> int:
    """Build novel Part→Supplier edges from otd_ownership cross-linked with
    corpus_entity.

    Reads OTD ownership rows where the row_key encodes a PO line
    (``<supplier_key>:<part_key>`` or similar) and resolves both sides
    against existing corpus_entity entries.  When both a Part and a Supplier
    entity exist, emits a ``SUPPLIED_BY`` edge weighted by the EMA of how
    often that supplier appears on that part's POs.

    Falls back gracefully if either table is absent (e.g. during test runs).
    """
    try:
        # Supplier entities already in the corpus graph.
        supplier_ids = {
            r["entity_id"]
            for r in cn.execute(
                "SELECT entity_id FROM corpus_entity WHERE entity_type='Supplier'"
            ).fetchall()
        }
        part_ids = {
            r["entity_id"]
            for r in cn.execute(
                "SELECT entity_id FROM corpus_entity WHERE entity_type='Part'"
            ).fetchall()
        }
    except sqlite3.OperationalError:
        return 0

    if not supplier_ids or not part_ids:
        return 0

    # OTD ownership row_keys often embed both supplier and part keys.
    # Pattern: "<supplier_key>|<part_key>" or "<part_key>" (single-key rows
    # that we can cross-link via existing CLASSIFIED_AS edges).
    n = 0
    try:
        rows = cn.execute(
            "SELECT DISTINCT row_key, owner FROM otd_ownership "
            "WHERE row_key IS NOT NULL AND owner IS NOT NULL LIMIT 10000"
        ).fetchall()
    except sqlite3.OperationalError:
        return 0

    for r in rows:
        row_key = (r["row_key"] or "").strip()
        owner   = (r["owner"]   or "").strip()
        if not row_key or not owner:
            continue

        # Resolve part key: the row_key itself or the segment after '|'.
        candidate_part = row_key.split("|")[-1].strip() if "|" in row_key else row_key
        candidate_supp = row_key.split("|")[0].strip() if "|" in row_key else owner

        part_hit = candidate_part if candidate_part in part_ids else None
        supp_hit = (
            candidate_supp if candidate_supp in supplier_ids
            else (owner if owner in supplier_ids else None)
        )

        if part_hit and supp_hit:
            _upsert_edge(cn, stats,
                         src_id=part_hit, src_type="Part",
                         dst_id=supp_hit, dst_type="Supplier",
                         rel="SUPPLIED_BY", weight=0.8)
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
# Toroidal phase scheduler — propeller blades on the integrated axle
# ---------------------------------------------------------------------------
# Each outreach blade sits at a position on a torus parameterized by
# (major_period, minor_offset).  The major angle θ_M advances by 1 per round.
# A blade fires when (phase % major_period) == minor_offset.
#
# The minor angle θ_m = (phase // major_period) % 2 selects the inflection
# mode for that firing:
#   θ_m = 0  →  BROADEN  (pull novel entities — DW dim sweeps, OCW slugs)
#   θ_m = 1  →  DEEPEN   (enrich existing entities — OCW course pages,
#                         DW sub-attribute walks, edge weight refinement)
#
# Inflection sites are exactly the (phase, blade) pairs where θ_m flips.
# At those sites the blade reverses its outreach direction — broadening
# becomes deepening and vice versa — so the corpus alternates between
# horizontal expansion and vertical enrichment without manual scheduling.
#
# Touch pressure warps the phase advance: if a blade's signal-kind has
# pressure ≥ _PRESSURE_FORCE, the scheduler forces it to fire this round
# regardless of its (period, offset) — a topological tunnel through the
# torus to the most-needed blade.
_PHASE_KEY      = "corpus_round_phase"
_PRESSURE_FORCE = 0.30

# (blade_name, major_period, minor_offset, pressure_kinds_that_force)
_TOROIDAL_BLADES: list[tuple[str, int, int, tuple[str, ...]]] = [
    ("dw",       3, 0, ("missing_category", "high_centrality_part")),
    ("ocw",      3, 1, ("corpus_rag_saturated", "model_low_task_weight")),
    ("network",  3, 2, ("peer_unreachable", "network_learner_not_started")),
    ("synaptic", 1, 0, ()),                # always fires (background decay)
    ("schema",   2, 0, ()),                # every other round
]


def _get_phase(cn) -> int:
    cn.execute(
        "CREATE TABLE IF NOT EXISTS brain_kv("
        "key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)"
    )
    row = cn.execute(
        "SELECT value FROM brain_kv WHERE key=?", (_PHASE_KEY,)
    ).fetchone()
    try:
        return int(row[0]) if row else 0
    except (TypeError, ValueError):
        return 0


def _set_phase(cn, phase: int) -> None:
    cn.execute(
        "INSERT OR REPLACE INTO brain_kv(key, value, updated_at) VALUES(?,?,?)",
        (_PHASE_KEY, str(int(phase)),
         datetime.now(timezone.utc).isoformat()),
    )


def _torus_schedule(phase: int,
                    pressure: dict[str, float]) -> dict[str, dict]:
    """Compute (fires, mode) for each blade at this phase, with pressure
    able to tunnel through the torus and force a blade to fire.

    Returns a dict: blade_name → {"fires": bool, "mode": "broaden"|"deepen",
                                   "forced": bool, "theta_minor": int}
    """
    # Plasticity-driven force threshold (defaults to module-level constant).
    try:
        from .neural_plasticity import get_dial as _pl_get
        force_thr = float(_pl_get("vision", "force_threshold", _PRESSURE_FORCE))
    except Exception:
        force_thr = _PRESSURE_FORCE
    out: dict[str, dict] = {}
    for blade, period, offset, kinds in _TOROIDAL_BLADES:
        natural = (phase % period) == offset
        forced  = any(pressure.get(k, 0.0) >= force_thr for k in kinds)
        theta_m = (phase // max(period, 1)) % 2
        mode    = "deepen" if theta_m == 1 else "broaden"
        out[blade] = {
            "fires":       bool(natural or forced),
            "mode":        mode,
            "forced":      bool(forced and not natural),
            "theta_minor": int(theta_m),
            "period":      period,
            "offset":      offset,
        }
    return out


# ---------------------------------------------------------------------------
# SCB document corpus ingestor — Grok conversation export
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# SCB corpus constants — operational SC tier (direct Task/Quest cross-links)
# ---------------------------------------------------------------------------

_SCB_TOPIC_TASK_MAP: dict[str, list[tuple[str, str, float]]] = {
    "cycle count": [
        ("abc_classify",                "Task",  0.80),
        ("cc_reason_classify",          "Task",  0.75),
        ("cc_reason_classify_syteline", "Task",  0.72),
        ("quest:optimize_supply_chains","Quest", 0.70),
    ],
    "abc classification": [
        ("abc_classify",                "Task",  0.85),
        ("fast_classify",               "Task",  0.70),
        ("quest:optimize_supply_chains","Quest", 0.65),
    ],
    "vendor consolidation": [
        ("vendor_consolidation",        "Task",  0.90),
        ("quest:optimize_supply_chains","Quest", 0.70),
    ],
    "on-time delivery": [
        ("otd_root_cause",              "Task",  0.85),
        ("otd_classify",                "Task",  0.80),
        ("quest:fulfillment",           "Quest", 0.75),
    ],
    "procurement": [
        ("vendor_consolidation",        "Task",  0.75),
        ("cross_dataset_review",        "Task",  0.65),
    ],
    "freight logistics": [
        ("quest:fulfillment",           "Quest", 0.80),
        ("cross_dataset_review",        "Task",  0.60),
    ],
    "inventory management": [
        ("abc_classify",                "Task",  0.75),
        ("quest:optimize_supply_chains","Quest", 0.70),
    ],
    "demand planning": [
        ("quest:optimize_supply_chains","Quest", 0.80),
        ("cross_dataset_review",        "Task",  0.65),
    ],
    "erp oracle": [
        ("cross_dataset_review",        "Task",  0.75),
        ("quest:optimize_supply_chains","Quest", 0.65),
    ],
    "supply chain analytics": [
        ("quest:optimize_supply_chains","Quest", 0.85),
        ("cross_dataset_review",        "Task",  0.70),
        ("vendor_consolidation",        "Task",  0.60),
    ],
    "cycle count reason": [
        ("cc_reason_classify",          "Task",  0.85),
        ("cc_reason_classify_syteline", "Task",  0.82),
        ("abc_classify",                "Task",  0.65),
    ],
}

# Operational SC topic keywords (multi-word phrases preferred for precision)
_SCB_TOPIC_KEYWORDS: dict[str, set[str]] = {
    "cycle count":           {"cycle count", "cycle counting", "physical count", "inventory count"},
    "abc classification":    {"abc class", "abc classification", "abc category", "abc analysis", "abc assign", "pareto classif"},
    "vendor consolidation":  {"vendor consolid", "supplier consolid", "vendor reduction", "supplier rationali"},
    "on-time delivery":      {"otd", "on-time delivery", "on time delivery", "delivery performance", "promise date", "shipped on time", "late delivery", "delivery date"},
    "procurement":           {"procurement", "purchase order", "po approval", "sourcing strategy", "requisition", "rfq", "request for quote"},
    "freight logistics":     {"freight", "logistics", "shipping", "carrier", "3pl", "inbound freight", "outbound freight"},
    "inventory management":  {"inventory management", "inventory level", "stock level", "safety stock", "reorder point", "min/max", "on-hand inventory"},
    "demand planning":       {"demand planning", "demand forecast", "mrp", "s&op", "sales and operations", "demand signal"},
    "erp oracle":            {"oracle fusion", "oracle inventory", "oracle cloud", "oracle ebs", "oracle scm", "erp system"},
    "supply chain analytics":{"supply chain analytic", "supply chain metric", "supply chain brain", "scb dashboard"},
    "cycle count reason":    {"cycle count reason", "count discrepancy", "count reason", "adjustment reason"},
}

# ---------------------------------------------------------------------------
# Type 5 Civilization Domain taxonomy
# ---------------------------------------------------------------------------
# ALL 106 conversations are ingested. Non-operational-SC conversations carry
# cross-domain systems-engineering knowledge vital to a supply chain that
# operates at civilisation scale (Kardashev Type 5 — multiversal resource
# manipulation).  Each CivilizationDomain aggregates conversations whose text
# intersects its keyword set and then propagates via TRANSCENDS_TO to
# quest:type5_sc and (where relevant) existing operational Quests/Tasks.
# ---------------------------------------------------------------------------

_SCB_CIVILIZATION_DOMAINS: dict[str, set[str]] = {
    # Quantum-scale material / energy behaviour
    "quantum_physics": {
        "quantum", "qubit", "superposition", "entanglement", "wave function",
        "eigenvalue", "polariton", "neutrino", "gamma decay", "planck",
        "coherence", "decoherence", "schrodinger", "uncertainty principle",
        "quantum field", "quantum dynamics", "wavefunction", "ueqgm",
    },
    # Molecular manufacturing: bio-chemical processes that define material
    # feasibility at civilisation scale
    "biochemical_manufacturing": {
        "biochem", "enzyme", "protein", "dna", "rna", "atp", "nadh", "nad",
        "metabolism", "coenzyme", "cellular", "coenzyme q", "adenine",
        "ribose", "xenon anesthesia", "beta-alanine", "bmp synthesis",
        "neurodegenerat", "24-methyl", "zeta-vanad",
    },
    # Physical materials and extractive supply chains
    "energy_materials_science": {
        "mineral recovery", "solvent extraction", "seigniorage", "americium",
        "nuclear", "gamma ramp", "crystal structure", "polymer", "lithium",
        "antimony", "vanadium", "semiconductor", "nanotechnology", "alloy",
        "tariff", "commodity", "rare earth", "uamy", "mining",
    },
    # Intelligence systems that manage complexity at scale
    "computational_intelligence": {
        "machine learning", "neural network", "neural-symbolic", "algorithm",
        "optimization", "inference", "kernel regression", "transformer",
        "embedding", "gradient", "backpropagation", "deep learning",
        "reinforcement learning", "llm", "hyperon", "asymptotic", "temporal gradient",
    },
    # Macro-scale physical structures; resource positioning at stellar scale
    "cosmological_systems": {
        "cosmolog", "dark matter", "dark energy", "spacetime", "multiverse",
        "galactic", "stellar", "black hole", "galaxy", "astral-temporal",
        "great circle", "lupus", "puppis", "canis major", "lmc",
        "darpa compass", "rank-1 matri", "4d wavef",
    },
    # Engineering systems — how complex systems are designed, reviewed, optimised
    "systems_engineering_design": {
        "systems requirement", "preliminary design review", "critical design review",
        "pdr", "cdr", "sdr", "darpa", "systems turbulence", "phd program",
        "asymptotically dynamic", "temporal gradient", "rank-1 matrix",
        "systems design", "system architect", "engineering system",
    },
    # Software / data infrastructure enabling SC visibility
    "software_data_systems": {
        "github", "repo", "commit", "api key", "sql statement", "power query",
        "excel formula", "latex", "pdf", "python", "javascript", "debug",
        "vite build", "node", "ocr", "archival document", "scan document",
        "data dictionary", "data classification",
    },
    # Economic flows, trade policy, asset allocation at civilisation scale
    "economic_trade_systems": {
        "tariff war", "trade policy", "seigniorage", "defi asset", "wallet",
        "asset apprais", "repossession", "trust beneficiar", "403b",
        "satellite deployment", "visa restriction", "executive procurement",
        "supply chain finance", "arbitrage", "negative seigniorage",
    },
    # Human capital — the intelligent actors who run civilisation-scale SC
    "human_career_systems": {
        "resume", "cover letter", "interview", "job offer", "linkedin",
        "hiring", "salary negotiation", "mit ctl", "associate director",
        "thank you email", "cold introduction", "executive search",
    },
}

# Civilization domain → Quest / Task edges.
# All non-trivial domains ultimately inform quest:type5_sc.
_SCB_CIVILIZATION_DOMAIN_INFORMS: dict[str, list[tuple[str, str, float]]] = {
    "quantum_physics": [
        ("quest:type5_sc",              "Quest", 0.95),
        ("quest:optimize_supply_chains","Quest", 0.48),
    ],
    "biochemical_manufacturing": [
        ("quest:type5_sc",    "Quest", 0.88),
        ("cross_dataset_review","Task", 0.52),
    ],
    "energy_materials_science": [
        ("quest:type5_sc",              "Quest", 0.92),
        ("vendor_consolidation",        "Task",  0.58),
        ("quest:optimize_supply_chains","Quest", 0.55),
    ],
    "computational_intelligence": [
        ("quest:type5_sc",              "Quest", 0.87),
        ("cross_dataset_review",        "Task",  0.75),
        ("quest:optimize_supply_chains","Quest", 0.70),
    ],
    "cosmological_systems": [
        ("quest:type5_sc", "Quest", 0.98),
    ],
    "systems_engineering_design": [
        ("quest:type5_sc",              "Quest", 0.95),
        ("quest:optimize_supply_chains","Quest", 0.72),
        ("cross_dataset_review",        "Task",  0.65),
    ],
    "software_data_systems": [
        ("quest:type5_sc",       "Quest", 0.78),
        ("cross_dataset_review", "Task",  0.85),
    ],
    "economic_trade_systems": [
        ("quest:type5_sc",              "Quest", 0.85),
        ("quest:optimize_supply_chains","Quest", 0.78),
        ("quest:fulfillment",           "Quest", 0.65),
    ],
    "human_career_systems": [
        ("quest:type5_sc",              "Quest", 0.72),
        ("quest:optimize_supply_chains","Quest", 0.55),
    ],
}

# Academic topic cross-links (SC + broad systems-engineering)
_SCB_ACADEMIC_HINTS: list[tuple[str, str]] = [
    # Operational supply chain
    ("inventory",      "inventory theory"),
    ("supply chain",   "supply chain management"),
    ("logistics",      "logistics systems"),
    ("procurement",    "operations research"),
    ("manufacturing",  "manufacturing systems"),
    ("demand",         "production planning"),
    ("oracle",         "engineering systems design"),
    ("warehouse",      "inventory theory"),
    ("vendor",         "operations research"),
    ("freight",        "logistics systems"),
    # Broad systems engineering
    ("optimization",   "operations research"),
    ("simulation",     "manufacturing systems"),
    ("network",        "engineering systems design"),
    ("system design",  "engineering systems design"),
]

_SCB_DOCS_PATH = (_PIPELINE_ROOT / "docs" / "Introduction to SCB"
                  / "ttl" / "30d" / "export_data"
                  / "9826f3fc-ec86-4751-8129-de43d903e27e"
                  / "prod-grok-backend.json")

# Works Cited / Pirates Code — keys for brain_kv and learning_log
_SCB_WORKS_CITED_KEY   = "grok_research:bibliography:works_cited"
_SCB_PIRATES_CODE_KEY  = "grok_research:pirates_code"

# Scholarly host markers used to identify Works Cited references in Grok
# web-search results.  Treated as Pirates Code: guideline seeds, not doctrine.
_SCB_SCHOLARLY_HOST_MARKERS: tuple[str, ...] = (
    "arxiv.org", "doi.org", "ncbi.nlm.nih.gov", "pubmed", "pmc.ncbi",
    "scholar.google", "semanticscholar.org", "researchgate.net",
    "nature.com", "science.org", "sciencedirect.com", "springer.com",
    "link.springer", "wiley.com", "tandfonline.com", "sagepub.com",
    "jstor.org", "acm.org", "ieee.org", "aps.org", "pubs.acs.org",
    "frontiersin.org", "mdpi.com", "plos", "biorxiv.org", "medrxiv.org",
    "ssrn.com", "osti.gov", "hal.science", "zenodo.org", "figshare.com",
    "eric.ed.gov", "openalex.org", "dimensions.ai", "unpaywall.org",
)

# SC density threshold: separates Tier 1 (operational SC, ≥ threshold) from
# Tier 2 (cross-domain systems engineering, < threshold).
# ALL conversations are ingested regardless of tier.
_SCB_OPERATIONAL_THRESHOLD = 0.40

# Supply chain keyword set for density scoring
_SC_KEYWORDS = frozenset([
    "supply chain", "inventory", "vendor", "procurement", "otd", "erp",
    "oracle", "on-time", "cycle count", "abc", "pfep", "parts", "warehouse",
    "freight", "astec", "purchase order", "supplier", "demand", "logistics",
    "scb", "fulfillment", "manufacturing", "cycle", "count reason",
    "consolidat", "delivery performance",
])


def _scb_keyword_density(text: str) -> float:
    """SC keyword hits per 1 000 chars of *text*."""
    if not text:
        return 0.0
    lower = text.lower()
    hits = sum(1 for kw in _SC_KEYWORDS if kw in lower)
    return hits / (len(lower) / 1000.0)


def _scb_detected_topics(text: str) -> list[str]:
    """Sorted list of operational SC topic slugs detected in *text*."""
    lower = text.lower()
    return sorted(t for t, kws in _SCB_TOPIC_KEYWORDS.items()
                  if any(kw in lower for kw in kws))


def _scb_detected_civilization_domains(text: str) -> list[str]:
    """Sorted list of CivilizationDomain slugs detected in *text*.

    Applied to ALL conversations — including non-SC ones — to surface
    cross-domain knowledge relevant to Type 5 civilisation-scale supply chains.
    """
    lower = text.lower()
    return sorted(d for d, kws in _SCB_CIVILIZATION_DOMAINS.items()
                  if any(kw in lower for kw in kws))


# ---------------------------------------------------------------------------
# Works Cited / Pirates Code helpers
# ---------------------------------------------------------------------------

def _clean_scb_url(url: str) -> str:
    """Strip tracking params, normalise to https."""
    url = url.strip()
    for prefix in ("http://",):
        if url.startswith(prefix):
            url = "https://" + url[len(prefix):]
    # Drop common tracking query params
    if "?" in url:
        from urllib.parse import urlparse, urlencode, parse_qsl
        p = urlparse(url)
        keep = [(k, v) for k, v in parse_qsl(p.query)
                if k.lower() not in {"utm_source", "utm_medium", "utm_campaign",
                                      "utm_content", "utm_term", "ref", "source"}]
        url = p._replace(query=urlencode(keep)).geturl() if keep else p._replace(query="").geturl()
    return url


def _walk_scb_web_results(obj, _depth: int = 0):
    """Recursively yield {url, title, preview} dicts from a Grok response object."""
    if _depth > 8:
        return
    if isinstance(obj, dict):
        url = obj.get("url") or obj.get("link") or ""
        title = obj.get("title") or obj.get("name") or ""
        if url and title:
            yield {"url": url, "title": title,
                   "preview": obj.get("preview") or obj.get("description") or obj.get("snippet") or ""}
        for v in obj.values():
            if isinstance(v, (dict, list)):
                yield from _walk_scb_web_results(v, _depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_scb_web_results(item, _depth + 1)


def _is_scb_scholarly_reference(url: str, title: str, preview: str) -> bool:
    """Return True if the URL looks like a peer-reviewed / preprint source."""
    host = url.lower()
    return any(marker in host for marker in _SCB_SCHOLARLY_HOST_MARKERS)


def _paper_id_from_reference(url: str, title: str) -> tuple[str | None, str | None, str | None]:
    """Extract (paper_id, doi, arxiv_id) from a URL / title.

    Returns (paper_id, doi, arxiv_id) where paper_id is the canonical
    ``arxiv:<id>`` or ``doi:<doi>`` string usable as a corpus entity_id seed.
    """
    import re
    doi = arxiv_id = None

    # arXiv patterns
    m = re.search(r"arxiv\.org/(?:abs|pdf)/([\d.]+(?:v\d+)?)", url, re.I)
    if m:
        arxiv_id = m.group(1)
        return f"arxiv:{arxiv_id}", None, arxiv_id

    # DOI in URL path
    m = re.search(r"doi\.org/(10\.\S+)", url, re.I)
    if m:
        doi = m.group(1).rstrip("/")
        return f"doi:{doi}", doi, None

    # DOI embedded elsewhere in URL
    m = re.search(r"(10\.\d{4,}/\S+)", url)
    if m:
        doi = m.group(1).rstrip("/")
        return f"doi:{doi}", doi, None

    return None, None, None


def _extract_scb_works_cited(conversations: list[dict]) -> list[dict]:
    """Extract ALL unique scholarly Works Cited from the Grok conversation export.

    No arbitrary limit — every unique scholarly reference is a Pirates Code seed.
    These are guideline bearings from the Creator's exploration, not fixed doctrine.
    They seed expansion beyond the original conversation path by feeding
    DOI/arXiv identifiers into the citation-chain acquirer.
    """
    refs: list[dict] = []
    seen: set[str] = set()

    for conv_index, conv_wrapper in enumerate(conversations):
        responses = conv_wrapper.get("responses", []) or []
        for response_index, item in enumerate(responses):
            response = item.get("response") or {}
            for candidate in _walk_scb_web_results(response):
                url = _clean_scb_url(str(candidate.get("url") or ""))
                title = str(candidate.get("title") or "").strip()
                preview = str(candidate.get("preview") or "").strip()
                if not url or not _is_scb_scholarly_reference(url, title, preview):
                    continue
                paper_id, doi, arxiv_id = _paper_id_from_reference(url, title)
                dedupe_key = paper_id or url.lower()
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                refs.append({
                    "paper_id":        paper_id,
                    "doi":             doi,
                    "arxiv_id":        arxiv_id,
                    "title":           title[:240] or url,
                    "url":             url,
                    "preview":         preview[:500],
                    "conv_id":         f"grok:conv_{conv_index}",
                    "response_index":  response_index,
                    "guideline_mode":  "pirates_code",
                    "interpretation":  "guideline_not_dogma",
                    "source":          "grok_works_cited",
                })
    return refs


def _persist_scb_works_cited_guidelines(
    cn, stats: _Stats, *, doc_id: str, conversations: list[dict], source_mtime: int
) -> None:
    """Persist ALL Works Cited as Pirates Code guideline seeds.

    Each unique scholarly reference becomes a ``WorksCitedReference`` entity
    with a ``GUIDES_EXPANSION`` edge from the guideline node.  References that
    carry a DOI or arXiv ID also become ``Paper`` entities feeding the
    citation-chain acquirer.
    """
    # Idempotency: skip if we already persisted for this source mtime
    already = cn.execute(
        "SELECT 1 FROM learning_log WHERE kind='grok_works_cited' AND title=? LIMIT 1",
        (f"scb_works_cited:{source_mtime}",),
    ).fetchone()
    if already:
        return

    refs = _extract_scb_works_cited(conversations)
    if not refs:
        return

    now = datetime.now(timezone.utc).isoformat()

    # Guideline anchor entity
    guideline_id = "guideline:grok_works_cited_pirates_code"
    _upsert_entity(cn, stats, entity_id=guideline_id,
                   entity_type="ResearchGuideline",
                   label="Grok Works Cited — Pirates Code",
                   props={
                       "mode":           "pirates_code",
                       "interpretation": "guideline_not_dogma",
                       "total_seeds":    len(refs),
                       "source_mtime":   source_mtime,
                       "source":         "grok_works_cited",
                   })
    # Guideline → Document
    _upsert_edge(cn, stats, src_id=guideline_id, src_type="ResearchGuideline",
                 dst_id=doc_id, dst_type="Document",
                 rel="DERIVED_FROM", weight=0.95)

    seed_paper_ids: list[str] = []

    for ref in refs:
        import hashlib
        h = hashlib.sha1(ref["url"].encode()).hexdigest()[:12]
        ref_id = f"works_cited:{h}"

        _upsert_entity(cn, stats, entity_id=ref_id,
                       entity_type="WorksCitedReference",
                       label=ref["title"],
                       props={
                           "url":            ref["url"],
                           "doi":            ref["doi"],
                           "arxiv_id":       ref["arxiv_id"],
                           "paper_id":       ref["paper_id"],
                           "conv_id":        ref["conv_id"],
                           "preview":        ref["preview"],
                           "guideline_mode": "pirates_code",
                           "interpretation": "guideline_not_dogma",
                           "source":         "grok_works_cited",
                       })

        # Guideline → Reference (GUIDES_EXPANSION)
        _upsert_edge(cn, stats,
                     src_id=guideline_id, src_type="ResearchGuideline",
                     dst_id=ref_id, dst_type="WorksCitedReference",
                     rel="GUIDES_EXPANSION", weight=0.80)

        # Reference → parent conversation
        _upsert_edge(cn, stats,
                     src_id=ref["conv_id"], src_type="GrokConversation",
                     dst_id=ref_id, dst_type="WorksCitedReference",
                     rel="CITES", weight=0.90)

        # If we have a DOI or arXiv ID, also create/link a Paper entity
        if ref["paper_id"]:
            paper_eid = f"paper:{ref['paper_id']}"
            _upsert_entity(cn, stats, entity_id=paper_eid,
                           entity_type="Paper",
                           label=ref["title"],
                           props={
                               "paper_id":  ref["paper_id"],
                               "doi":       ref["doi"],
                               "arxiv_id":  ref["arxiv_id"],
                               "url":       ref["url"],
                               "source":    "grok_works_cited",
                           })
            _upsert_edge(cn, stats,
                         src_id=ref_id, src_type="WorksCitedReference",
                         dst_id=paper_eid, dst_type="Paper",
                         rel="RESOLVES_TO", weight=0.95)
            seed_paper_ids.append(ref["paper_id"])

    # Persist the full seed list to brain_kv for the citation-chain acquirer
    cn.execute(
        "INSERT OR REPLACE INTO brain_kv (key, value, updated_at) VALUES (?,?,?)",
        (_SCB_WORKS_CITED_KEY, json.dumps({
            "paper_ids":      seed_paper_ids,
            "total_refs":     len(refs),
            "source_mtime":   source_mtime,
            "guideline_mode": "pirates_code",
            "interpretation": "guideline_not_dogma",
        }, default=str), now),
    )
    cn.execute(
        "INSERT OR REPLACE INTO brain_kv (key, value, updated_at) VALUES (?,?,?)",
        (_SCB_PIRATES_CODE_KEY, json.dumps({
            "guideline_entity": guideline_id,
            "seed_count":       len(seed_paper_ids),
            "total_works_cited": len(refs),
            "source_mtime":     source_mtime,
            "interpretation":   "guideline_not_dogma",
        }, default=str), now),
    )

    _log_learning(cn, stats, kind="grok_works_cited",
                  title=f"scb_works_cited:{source_mtime}",
                  signal=0.90,
                  detail={
                      "total_refs":       len(refs),
                      "seed_paper_ids":   len(seed_paper_ids),
                      "guideline_entity": guideline_id,
                      "brain_kv_seed_key": _SCB_WORKS_CITED_KEY,
                      "source_mtime":     source_mtime,
                  })
    logging.info(
        f"corpus:scb_works_cited: {len(refs)} Works Cited seeded as Pirates Code "
        f"({len(seed_paper_ids)} with DOI/arXiv for citation-chain expansion)"
    )


def _ingest_scb_docs(cn, stats: _Stats, since_mtime: int) -> int:
    """Ingest ALL conversations from the Grok export into the corpus.

    Two-tier classification — ALL conversations are ingested:

    **Tier 1 — Operational SC** (SC keyword density ≥ 0.40)
      DISCUSSES edges → SCBTopic → INFORMS existing Tasks/Quests.

    **Tier 2 — Cross-domain Systems Engineering** (density < 0.40)
      Every conversation captures a different knowledge domain — quantum
      physics, biochemistry, materials science, cosmology, systems
      engineering — each necessary for a Type 5 civilisation-scale supply
      chain that manipulates resources at multiversal scope.
      CROSS_POLLINATES edges → CivilizationDomain → TRANSCENDS_TO quest:type5_sc.

    Both tiers also emit INFORMS_VISION → quest:type5_sc so the aspirational
    multiversal quest accumulates signal from every exchange.

    Cursor key: ``scb_docs_mtime_v2`` (force full re-ingest vs. v1).
    """
    if not _SCB_DOCS_PATH.exists():
        return since_mtime

    current_mtime = int(_SCB_DOCS_PATH.stat().st_mtime)
    if current_mtime == since_mtime:
        return since_mtime  # file unchanged

    try:
        raw = json.loads(_SCB_DOCS_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logging.debug(f"corpus:scb_docs: JSON parse error: {exc}")
        return since_mtime

    conversations = raw.get("conversations", [])
    if not conversations:
        return current_mtime

    # ── Ensure quest:type5_sc exists ─────────────────────────────────────────
    # Kardashev Type 5: civilisation manipulates energy/matter at multiversal
    # scale. This quest grounds all cross-domain learning into a coherent
    # aspirational target that the Brain works toward.
    _upsert_entity(
        cn, stats,
        entity_id="quest:type5_sc",
        entity_type="Quest",
        label="Type 5 Civilization Supply Chain: Multiversal Resource Manipulation",
        props={
            "kardashev_level": 5,
            "description": (
                "Aspirational quest: model and optimise supply chains spanning "
                "quantum material flows, bio-manufacturing, energy logistics, "
                "cosmological resource allocation and intelligent civilisation-"
                "level coordination. Grounded in operational Astec SC research."
            ),
            "source": "scb_grok_ingestor_v2",
        },
    )
    # Anchor quest:type5_sc in operational SC reality
    for op_id, op_w in [("quest:optimize_supply_chains", 0.88), ("quest:fulfillment", 0.82)]:
        if cn.execute("SELECT 1 FROM corpus_entity WHERE entity_id=?", (op_id,)).fetchone():
            _upsert_edge(cn, stats,
                         src_id="quest:type5_sc", src_type="Quest",
                         dst_id=op_id, dst_type="Quest",
                         rel="GROUNDS_IN", weight=op_w)

    # ── Document root entity ──────────────────────────────────────────────────
    doc_id = "scb_docs:grok_export"
    _upsert_entity(cn, stats, entity_id=doc_id, entity_type="Document",
                   label="Introduction to SCB — Grok Conversation Export",
                   props={
                       "path": str(_SCB_DOCS_PATH),
                       "conversations_total": len(conversations),
                       "source": "xAI Grok",
                       "export_date": "2026-04-24",
                   })

    # ── Pre-fetch existing AcademicTopics ─────────────────────────────────────
    try:
        at_rows = cn.execute(
            "SELECT entity_id FROM corpus_entity WHERE entity_type='AcademicTopic'"
        ).fetchall()
        academic_topic_ids = {r["entity_id"].lower(): r["entity_id"] for r in at_rows}
    except Exception:
        academic_topic_ids = {}

    # ── Process ALL 106 conversations — no density gate ──────────────────────
    topic_conv_map:  dict[str, list[str]] = {}   # SCBTopic slug  → [conv_ids]
    domain_conv_map: dict[str, list[str]] = {}   # CivDomain slug → [conv_ids]
    n_tier1 = 0
    n_tier2 = 0

    for i, conv_wrapper in enumerate(conversations):
        conv_meta = conv_wrapper.get("conversation", {})
        responses = conv_wrapper.get("responses", [])
        conv_uuid = conv_meta.get("_id") or conv_meta.get("id") or f"conv_{i}"

        all_assistant = " ".join(
            r.get("response", {}).get("message", "")
            for r in responses
            if r.get("response", {}).get("sender") == "assistant"
        )
        all_human = " ".join(
            r.get("response", {}).get("message", "")
            for r in responses
            if r.get("response", {}).get("sender") == "human"
        )
        full_text = all_assistant + " " + all_human
        if not full_text.strip():
            continue

        density  = _scb_keyword_density(full_text)
        is_tier1 = density >= _SCB_OPERATIONAL_THRESHOLD
        topics   = _scb_detected_topics(full_text) if is_tier1 else []
        domains  = _scb_detected_civilization_domains(full_text)
        tier     = "operational_sc" if is_tier1 else "cross_domain"

        # Signal: tier-1 scales with SC density; tier-2 scales with domain breadth
        if is_tier1:
            signal = min(0.95, 0.55 + density * 0.08)
            n_tier1 += 1
        else:
            # Base 0.50 + 0.05 per detected civilisation domain (cap 0.88)
            signal = min(0.88, 0.50 + len(domains) * 0.05)
            n_tier2 += 1

        conv_id = f"grok:conv_{i}"
        first_q = all_human[:120].strip().replace("\n", " ")
        n_asst  = sum(1 for r in responses
                      if r.get("response", {}).get("sender") == "assistant")

        # Conversation entity
        _upsert_entity(cn, stats, entity_id=conv_id,
                       entity_type="GrokConversation",
                       label=first_q or conv_id,
                       props={
                           "conv_uuid":   conv_uuid,
                           "tier":        tier,
                           "density":     round(density, 3),
                           "topics":      topics,
                           "domains":     domains,
                           "n_responses": n_asst,
                           "signal":      round(signal, 3),
                       })

        # Document → Conversation
        _upsert_edge(cn, stats, src_id=doc_id, src_type="Document",
                     dst_id=conv_id, dst_type="GrokConversation",
                     rel="CONTAINS", weight=signal)

        # ALL conversations inform the aspirational Type 5 quest
        _upsert_edge(cn, stats, src_id=conv_id, src_type="GrokConversation",
                     dst_id="quest:type5_sc", dst_type="Quest",
                     rel="INFORMS_VISION", weight=round(signal * 0.72, 3))

        # ── Tier 1: operational SC topics ─────────────────────────────────────
        for topic in topics:
            topic_conv_map.setdefault(topic, []).append(conv_id)
            _upsert_entity(cn, stats, entity_id=f"scb_topic:{topic}",
                           entity_type="SCBTopic", label=topic,
                           props={"source": "grok_export", "tier": "operational"})
            _upsert_edge(cn, stats,
                         src_id=conv_id, src_type="GrokConversation",
                         dst_id=f"scb_topic:{topic}", dst_type="SCBTopic",
                         rel="DISCUSSES", weight=signal)

        # ── All tiers: civilisation-domain cross-pollination ─────────────────
        for domain in domains:
            domain_eid = f"civ_domain:{domain}"
            domain_conv_map.setdefault(domain, []).append(conv_id)
            _upsert_entity(cn, stats, entity_id=domain_eid,
                           entity_type="CivilizationDomain",
                           label=domain.replace("_", " ").title(),
                           props={"source": "scb_grok_v2", "kardashev_relevance": "type5"})
            _upsert_edge(cn, stats,
                         src_id=conv_id, src_type="GrokConversation",
                         dst_id=domain_eid, dst_type="CivilizationDomain",
                         rel="CROSS_POLLINATES", weight=round(signal * 0.88, 3))

        # ── Academic cross-links (both tiers) ────────────────────────────────
        lower_full = full_text.lower()
        for hint_kw, at_label in _SCB_ACADEMIC_HINTS:
            if hint_kw in lower_full:
                at_id = academic_topic_ids.get(at_label.lower())
                if at_id:
                    _upsert_edge(cn, stats,
                                 src_id=conv_id, src_type="GrokConversation",
                                 dst_id=at_id, dst_type="AcademicTopic",
                                 rel="EXPLORES", weight=round(signal * 0.90, 3))

        # Learning log — one entry per conversation
        _log_learning(cn, stats, kind="scb_doc",
                      title=f"SCB [{tier}] conv[{i}]: {first_q[:60]}",
                      signal=signal,
                      detail={
                          "conv_id":  conv_id,
                          "tier":     tier,
                          "density":  round(density, 3),
                          "topics":   topics,
                          "domains":  domains,
                          "n_asst":   n_asst,
                      })

    # ── Operational SCBTopic → Task / Quest (INFORMS) ─────────────────────────
    for topic in topic_conv_map:
        topic_eid = f"scb_topic:{topic}"
        for (target_id, target_type, weight) in _SCB_TOPIC_TASK_MAP.get(topic, []):
            if cn.execute(
                "SELECT 1 FROM corpus_entity WHERE entity_id=? AND entity_type=?",
                (target_id, target_type),
            ).fetchone():
                _upsert_edge(cn, stats,
                             src_id=topic_eid, src_type="SCBTopic",
                             dst_id=target_id, dst_type=target_type,
                             rel="INFORMS", weight=weight)

    # ── CivilizationDomain → Quest / Task (TRANSCENDS_TO) ─────────────────────
    for domain in domain_conv_map:
        domain_eid = f"civ_domain:{domain}"
        for (target_id, target_type, weight) in _SCB_CIVILIZATION_DOMAIN_INFORMS.get(domain, []):
            if cn.execute(
                "SELECT 1 FROM corpus_entity WHERE entity_id=? AND entity_type=?",
                (target_id, target_type),
            ).fetchone():
                _upsert_edge(cn, stats,
                             src_id=domain_eid, src_type="CivilizationDomain",
                             dst_id=target_id, dst_type=target_type,
                             rel="TRANSCENDS_TO", weight=weight)

    logging.info(
        f"corpus:scb_docs v2: {n_tier1} operational-SC + {n_tier2} cross-domain "
        f"conversations; {len(topic_conv_map)} SC topics, "
        f"{len(domain_conv_map)} civilization domains"
    )

    # ── Works Cited: seed ALL unique scholarly references as Pirates Code ─────
    try:
        _persist_scb_works_cited_guidelines(
            cn, stats,
            doc_id=doc_id,
            conversations=conversations,
            source_mtime=current_mtime,
        )
    except Exception as exc:
        logging.debug(f"corpus:scb_works_cited: {exc}")

    return current_mtime


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
    try:
        from .neural_plasticity import get_dial as _pl_get
        min_seconds = float(_pl_get("brain", "round_min_seconds",
                                    cfg.get("min_seconds_between_rounds", 60.0)))
    except Exception:
        min_seconds = float(cfg.get("min_seconds_between_rounds", 60.0))

    # Temporal-spatiality rhythm: shorten the global Vision round floor when
    # the senses are coherent, lengthen it when the synaptic wash dominates.
    try:
        from .temporal_spatiality import get_rhythm_factor as _rf
        min_seconds *= float(_rf("period_factor", 1.0))
    except Exception:
        pass

    global _LAST_REFRESH_TS
    with _REFRESH_LOCK:
        if time.monotonic() - _LAST_REFRESH_TS < min_seconds:
            return {"skipped": True, "reason": "rate-limited"}
        _LAST_REFRESH_TS = time.monotonic()

    stats = _Stats()
    notes: list[str] = []

    # ── Touch pressure field — directs Vision's outreach this round ──────────
    # The pivoted channel: read the body's open-directive pressure per
    # signal_kind so the DW / OCW / network probes get more budget toward
    # whichever Vision blind-spot the body is currently flagging.
    touch_pressure: dict[str, float] = {}
    try:
        from .brain_body_signals import get_touch_field
        touch_pressure = get_touch_field() or {}
    except Exception as _e:
        notes.append(f"touch_field_read: {_e}")

    # Steering thresholds — pressure above this forces an outreach even
    # when entities_added > 0, so Touch signals can drive Vision actively
    # rather than only when the round is dry.  Threshold value is supplied
    # by the neural_plasticity agent so it relaxes as the corpus grows.
    try:
        from .neural_plasticity import get_dial as _pl_get
        _PRESSURE_THRESHOLD = float(_pl_get("vision", "pressure_threshold", 0.30))
    except Exception:
        _PRESSURE_THRESHOLD = 0.30
    _force_dw   = touch_pressure.get("missing_category", 0.0) >= _PRESSURE_THRESHOLD or \
                  touch_pressure.get("high_centrality_part", 0.0) >= _PRESSURE_THRESHOLD
    _force_ocw  = touch_pressure.get("corpus_rag_saturated", 0.0) >= _PRESSURE_THRESHOLD or \
                  touch_pressure.get("model_low_task_weight", 0.0) >= _PRESSURE_THRESHOLD
    _force_net  = touch_pressure.get("peer_unreachable", 0.0) >= _PRESSURE_THRESHOLD or \
                  touch_pressure.get("network_learner_not_started", 0.0) >= _PRESSURE_THRESHOLD

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
        # Part classification (incremental cursor — was LIMIT 5000 non-incremental)
        c_pcat = _get_cursor(cn, "part_category_rowid")
        try: c_pcat = _ingest_part_category(cn, stats, c_pcat)
        except Exception as e: notes.append(f"part_category: {e}")
        # Novel Part→Supplier edges from OTD ownership cross-linking
        try: _ingest_part_supplier_edges(cn, stats)
        except Exception as e: notes.append(f"part_supplier_edges: {e}")
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
        c_ocwr = _get_cursor(cn, "ocw_resources")
        try: c_ocwr = _ingest_ocw_resources(cn, stats, c_ocwr)
        except Exception as e: notes.append(f"ocw_resources: {e}")
        c_fiction = _get_cursor(cn, "fiction_anthology")
        try: c_fiction = _ingest_fiction_anthology(cn, stats, c_fiction)
        except Exception as e: notes.append(f"fiction_anthology: {e}")
        c_heart = _get_cursor(cn, "heart_story")
        try: c_heart = _ingest_heart_story(cn, stats, c_heart)
        except Exception as e: notes.append(f"heart_story: {e}")
        # ── SCB docs: Grok conversation export from docs/Introduction to SCB ──
        # v2 cursor forces full re-ingest (all 106 convs, CivilizationDomain layer)
        c_scb = _get_cursor(cn, "scb_docs_mtime_v2")
        try: c_scb = _ingest_scb_docs(cn, stats, c_scb)
        except Exception as e: notes.append(f"scb_docs: {e}")
        _net_before = stats.entities_added
        try: _ingest_bridge_observations(cn, stats)
        except Exception as e: notes.append(f"bridge_observations: {e}")
        _net_added = stats.entities_added - _net_before

        # ── Vision: DW schema topology (always runs — keeps graph breathing) ──
        _schema_before = stats.learnings_logged
        try: _ingest_schema_vision(cn, stats)
        except Exception as e: notes.append(f"schema_vision: {e}")
        _schema_learnings = stats.learnings_logged - _schema_before

        # ── Expansive DW outreach: fetch novel entities when round is dry ─────
        # OR when Touch pressure signals the body needs more entity coverage.
        # The propeller blade: dry round OR body pressure both spin it up.
        _dw_before = stats.entities_added
        if stats.entities_added == 0 or _force_dw:
            try: _dw_vision_outreach(cn, stats)
            except Exception as e: notes.append(f"dw_vision_outreach: {e}")
        _dw_added = stats.entities_added - _dw_before

        # ── Expansive OCW outreach: deep-fetch course pages when round is dry ─
        # OR when Touch pressure signals the corpus is saturated / models weak.
        # Picks OCWCourses without HAS_RESOURCE edges and pulls their full
        # hyperlink lattice — instructors, lecture notes, readings, related
        # courses, and every external reference.  Drives the corpus from
        # "course slug" to a navigable web of pipelines and pages.
        _ocw_before = stats.entities_added
        if stats.entities_added == 0 or _force_ocw:
            try: _ocw_expansion_outreach(cn, stats)
            except Exception as e: notes.append(f"ocw_expansion_outreach: {e}")
        _ocw_added = stats.entities_added - _ocw_before

        # ── Synaptic cleanse: decay stale edges, prune dead ones ──────────────
        try: _synaptic_cleanse(cn, stats)
        except Exception as e: notes.append(f"synaptic_cleanse: {e}")

        _set_cursor(cn, "self_train",  c_st)
        _set_cursor(cn, "dispatch",    c_dsp)
        _set_cursor(cn, "network",     c_net)
        _set_cursor(cn, "promotions",  c_prm)
        _set_cursor(cn, "part_category_rowid", c_pcat)
        _set_cursor(cn, "body_feedback", c_bf)
        _set_cursor(cn, "missions",    c_msn)
        _set_cursor(cn, "ml_research", c_mlr)
        _set_cursor(cn, "ocw_courses", c_ocw)
        _set_cursor(cn, "ocw_resources", c_ocwr)
        _set_cursor(cn, "fiction_anthology", c_fiction)
        _set_cursor(cn, "heart_story", c_heart)
        _set_cursor(cn, "scb_docs_mtime_v2", c_scb)

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

    # ── Closed loop: refresh Touch directives off the new corpus state ──────
    # The synaptic flow now feeds back to the body. Resolved signals collapse
    # via inverse-ReLU; new signals get ADAM-momentum on their pressure;
    # Vision will read the updated touch_field at the start of the NEXT round.
    #
    # Bilateral input: pass this round's Vision-side operation counts so the
    # ADAM optimizer sees BOTH Touch's positive pressure targets AND the
    # negative satisfaction gradients from what Vision actually accomplished.
    forced_blades: list[str] = []
    if _force_dw:  forced_blades.append("dw")
    if _force_ocw: forced_blades.append("ocw")
    if _force_net: forced_blades.append("network")
    vision_ops = {
        "dw_entities":       int(_dw_added),
        "ocw_entities":      int(_ocw_added),
        "network_endpoints": int(_net_added),
        "schema_learnings":  int(_schema_learnings),
        "forced_blades":     forced_blades,
    }

    touch_summary: dict = {}
    try:
        from .brain_body_signals import surface_effective_signals as _sfx
        touch_summary = _sfx(vision_ops=vision_ops) or {}
    except Exception as _e:
        notes.append(f"touch_surface: {_e}")

    # ── Neural plasticity: rewire sense capability dials based on growth ────
    # The agent measures the new corpus state (entities, edges, learnings,
    # smell readings, directives) and ADAM-smooths every per-sense dial
    # toward its growth-driven target. Senses (Vision, Touch, Smell, Body,
    # Brain) read the dials at the start of their next cycle.
    plasticity_summary: dict = {}
    try:
        from .neural_plasticity import rewire_round as _rewire
        plasticity_summary = _rewire() or {}
    except Exception as _e:
        notes.append(f"neural_plasticity: {_e}")

    # ── Temporal-spatiality: cross-sense rhythm coordination ───────────────
    # After plasticity rewires the dials, measure joint coherence across
    # all 5 senses, project onto the 1-D Weyl coordinate at the toroidal
    # centroid, and modulate the syncopatic rhythm of every rADAM agent.
    rhythm_summary: dict = {}
    try:
        from .temporal_spatiality import temporal_step as _tstep
        rhythm_summary = _tstep() or {}
    except Exception as _e:
        notes.append(f"temporal_spatiality: {_e}")

    # ── Recursive knowledge strengthening: condense the n-1..n-k chain into
    # a 1-D actionable edge that informs n+1 at t0. Reduces structured
    # recursive dimensionality toward the toroidal centroid while keeping
    # the unbounded raw potential available to the senses' stretch dials.
    strengthening_summary: dict = {}
    try:
        from .recursive_strengthening import strengthen_step as _strengthen
        strengthening_summary = _strengthen() or {}
    except Exception as _e:
        notes.append(f"recursive_strengthening: {_e}")

    return {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "entities_added": stats.entities_added,
        "entities_touched": stats.entities_touched,
        "edges_added": stats.edges_added,
        "edges_touched": stats.edges_touched,
        "learnings_logged": stats.learnings_logged,
        "touch_pressure_in":  touch_pressure,
        "vision_ops_out":     vision_ops,
        "touch_summary_out":  {
            "directives_emitted": touch_summary.get("directives_emitted"),
            "directives_expired": touch_summary.get("directives_expired"),
            "resolved_kinds":     touch_summary.get("resolved_kinds"),
            "vision_grads_in":    touch_summary.get("vision_grads_in"),
            "top_priority":       touch_summary.get("top_priority"),
        } if touch_summary else None,
        "forced_outreach": {
            "dw":  bool(_force_dw),
            "ocw": bool(_force_ocw),
            "net": bool(_force_net),
        },
        "plasticity": {
            "ran":       not bool(plasticity_summary.get("skipped")),
            "knowledge": plasticity_summary.get("knowledge"),
            "dials":     plasticity_summary.get("dials"),
        } if plasticity_summary else None,
        "rhythm": {
            "coherence":     rhythm_summary.get("coherence"),
            "gradient":      rhythm_summary.get("gradient"),
            "weyl":          rhythm_summary.get("weyl"),
            "boost":         rhythm_summary.get("boost"),
            "period_factor": rhythm_summary.get("period_factor"),
            "lr_factor":     rhythm_summary.get("lr_factor"),
        } if rhythm_summary and not rhythm_summary.get("skipped") else None,
        "strengthening": {
            "edge":                 strengthening_summary.get("edge"),
            "instant_edge":         strengthening_summary.get("instant_edge"),
            "actionable_potential": strengthening_summary.get("actionable_potential"),
            "weyl_residual":        strengthening_summary.get("weyl_residual"),
            "chain_depth":          strengthening_summary.get("chain_depth"),
        } if strengthening_summary and not strengthening_summary.get("skipped") else None,
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
