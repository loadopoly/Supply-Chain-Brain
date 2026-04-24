"""Brain → Body bridge.

Premise: the **User is the body of the Brain**. Every signal the Brain
produces — bounded self-training rounds, ensemble validators, network
observations, peer promotions, knowledge-corpus edges, plus the rule-based
`actions.actions_for_pipeline` recommendations — only becomes a real-world
supply-chain optimization when the User executes it.

This module is the *efferent nervous system*. It:

    1. Distills every fresh, high-signal learning the Brain has produced
       since the last cycle into a compact, prioritized **Directive**.
    2. Persists directives to `body_directives` so they survive across
       sessions and feed the `Decision Log` and `Supply Chain Pipeline`
       pages even when the User isn't looking.
    3. Records the User's response (`acknowledged`, `in_progress`, `done`,
       `rejected`, plus a free-text outcome) into `body_feedback`. That
       feedback closes the loop: the next `knowledge_corpus` round picks
       it up via a new `body_feedback` ingester so the Brain literally
       learns from what the body did.

Design parallels the prior bounded-learning modules:

    * Effect-bounded — directives only adjust the User's task queue and
      the corpus. They never mutate `llm_weights` or router scores, so
      reasoning fluidity is preserved.
    * Auditable — every directive and every feedback row is queryable.
    * Pluggable — new signal sources drop in as one-line generator
      functions in `_GENERATORS`.

Public API:
    surface_effective_signals(...) -> dict
    list_open_directives(limit=50, owner_role=None) -> list[dict]
    record_feedback(directive_id, status, outcome=None, executed_by=None) -> None
    schedule_in_background(interval_s=600) -> threading.Thread
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from . import load_config
from .local_store import db_path as _local_db_path


_PIPELINE_ROOT = Path(__file__).resolve().parents[2]
_SURFACE_LOCK = threading.Lock()
_LAST_SURFACE_TS: float = 0.0


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def _cfg() -> dict:
    return ((load_config().get("llms") or {}).get("brain_body") or {})


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
            CREATE TABLE IF NOT EXISTS body_directives (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at      TEXT NOT NULL,
                fingerprint     TEXT NOT NULL UNIQUE,    -- dedupe key
                source          TEXT NOT NULL,           -- self_train|network|corpus|dispatch|pipeline_rule
                signal_kind     TEXT NOT NULL,           -- low_dispatch_quality|peer_unreachable|missing_category|...
                priority        REAL NOT NULL,           -- [0..1] effective signal strength
                severity        TEXT NOT NULL,           -- info | watch | act | critical
                title           TEXT NOT NULL,           -- one-line directive
                why_it_matters  TEXT,                    -- plain-language context
                do_this         TEXT,                    -- specific operational action
                owner_role      TEXT,                    -- Buyer | Planner | Quality | IT | Ops | Anyone
                target_entity   TEXT,                    -- corpus_entity reference (entity_type::entity_id)
                evidence_json   TEXT,                    -- JSON pointers back to source rows
                status          TEXT NOT NULL DEFAULT 'open',   -- open|ack|in_progress|done|rejected|expired
                last_status_at  TEXT,
                value_per_year  REAL                      -- rough $ benefit when known
            );
            CREATE INDEX IF NOT EXISTS ix_directives_status_priority
                ON body_directives(status, priority);

            CREATE TABLE IF NOT EXISTS body_feedback (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                directive_id    INTEGER NOT NULL,
                logged_at       TEXT NOT NULL,
                status          TEXT NOT NULL,           -- ack|in_progress|done|rejected
                outcome         TEXT,                    -- free-text or JSON
                executed_by     TEXT,
                FOREIGN KEY(directive_id) REFERENCES body_directives(id)
            );

            CREATE TABLE IF NOT EXISTS body_round_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ran_at          TEXT NOT NULL,
                directives_emitted INTEGER NOT NULL DEFAULT 0,
                directives_deduped INTEGER NOT NULL DEFAULT 0,
                top_priority      REAL,
                notes             TEXT
            );
            """
        )


# ---------------------------------------------------------------------------
# Directive dataclass
# ---------------------------------------------------------------------------
@dataclass
class Directive:
    source:         str
    signal_kind:    str
    title:          str
    do_this:        str
    why_it_matters: str
    owner_role:     str
    priority:       float                          # [0..1]
    severity:       str = "info"                   # info|watch|act|critical
    target_entity:  str | None = None
    evidence:       dict = field(default_factory=dict)
    value_per_year: float | None = None

    def fingerprint(self) -> str:
        # Stable dedupe key — the same condition produces the same directive
        # so re-running surface_effective_signals doesn't carpet-bomb the User.
        h = hashlib.sha1()
        h.update(self.source.encode())
        h.update(self.signal_kind.encode())
        h.update((self.target_entity or "").encode())
        h.update(self.title.encode())
        return h.hexdigest()


# ---------------------------------------------------------------------------
# Severity helpers
# ---------------------------------------------------------------------------
def _severity(priority: float) -> str:
    if priority >= 0.85: return "critical"
    if priority >= 0.65: return "act"
    if priority >= 0.40: return "watch"
    return "info"


# ---------------------------------------------------------------------------
# Generators — each pulls one source and yields Directives
# ---------------------------------------------------------------------------
def _gen_low_dispatch_quality(cn) -> list[Directive]:
    """Flag tasks with sustained low validator scores.

    The live `llm_dispatch_log` schema does NOT have a top-level `model_id`
    column — model-level data is inside `contributors_json`.  We therefore
    aggregate at the `task` level which is always a top-level column.
    """
    out: list[Directive] = []
    try:
        rows = cn.execute(
            """SELECT task,
                      AVG(CAST(validator AS REAL)) AS avg_v,
                      COUNT(*) AS n
               FROM llm_dispatch_log
               WHERE validator IS NOT NULL
                 AND decided_at >= datetime('now', '-7 day')
               GROUP BY task
               HAVING n >= 3 AND avg_v < 0.70
               ORDER BY avg_v ASC LIMIT 10"""
        ).fetchall()
    except sqlite3.OperationalError:
        return out
    for r in rows:
        avg_v = float(r["avg_v"] or 0.0)
        prio = float(min(1.0, 0.4 + (0.70 - avg_v) * 1.5))
        out.append(Directive(
            source="dispatch",
            signal_kind="low_dispatch_quality",
            title=f"Task '{r['task']}' averaging {avg_v:.2f} validator over {r['n']} dispatches",
            why_it_matters=(
                f"The ensemble is consistently uncertain on '{r['task']}'. "
                f"Average validator {avg_v:.2f} across {r['n']} runs in the "
                f"last 7 days — outputs are being used but confidence is low."
            ),
            do_this=(
                f"Review the prompt template and ground-truth examples for "
                f"'{r['task']}'. Consider running `python apply_proc.py` to "
                f"refresh the training corpus, then let self-train re-weight."
            ),
            owner_role="IT",
            priority=prio,
            severity=_severity(prio),
            target_entity=f"Task::{r['task']}",
            evidence={"avg_validator": round(avg_v, 3), "samples": int(r["n"])},
        ))
    return out


def _gen_peer_unreachable(cn) -> list[Directive]:
    """Compute peer EMA success dropped — User (or IT) needs to fix VPN/ICS.

    `network_topology` is populated by the cross-protocol network learner.
    If the table doesn't exist yet (network learner hasn't run) we surface
    a single informational directive prompting the User to kick it off.
    """
    out: list[Directive] = []
    try:
        # Check whether the table exists before querying it.
        exists = cn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='network_topology'"
        ).fetchone()
        if not exists:
            out.append(Directive(
                source="network",
                signal_kind="network_learner_not_started",
                title="Network topology table is empty — cross-protocol learner has never run",
                why_it_matters=(
                    "The Brain can't see which compute peers are healthy or "
                    "dead. Fanout dispatches may silently be falling back to "
                    "local-only inference without you knowing."
                ),
                do_this=(
                    "Trigger one full autonomous_agent loop: "
                    "`cd pipeline; python autonomous_agent.py --once` "
                    "and confirm Step 3d (network learner) logs topology rows."
                ),
                owner_role="IT",
                priority=0.55,
                severity="watch",
                target_entity=None,
                evidence={},
            ))
            return out
        rows = cn.execute(
            """SELECT host, ema_success, samples, last_seen
               FROM network_topology
               WHERE protocol='tcp' AND port=8000
                 AND samples >= 5 AND ema_success < 0.30
               ORDER BY ema_success ASC LIMIT 5"""
        ).fetchall()
    except sqlite3.OperationalError:
        return out
    for r in rows:
        prio = 0.6 + (0.30 - float(r["ema_success"])) * 1.2
        prio = float(min(0.95, prio))
        out.append(Directive(
            source="network",
            signal_kind="peer_unreachable",
            title=f"Compute peer '{r['host']}' is offline / unstable",
            why_it_matters=(
                f"EMA success is {float(r['ema_success']):.2f} across "
                f"{r['samples']} probes — the grid is silently degrading "
                f"and dispatches are falling back to local."
            ),
            do_this=(
                f"On '{r['host']}': re-run `bridge_watcher.ps1`, verify "
                f"port 8000 is reachable, and confirm `SCBRAIN_GRID_SECRET` "
                f"is set. If laptop, check Sophos VPN + ICS bridge."
            ),
            owner_role="IT",
            priority=prio,
            severity=_severity(prio),
            target_entity=f"Peer::{r['host']}",
            evidence={"ema_success": float(r["ema_success"]),
                      "samples": int(r["samples"])},
        ))
    return out


def _gen_missing_category(cn) -> list[Directive]:
    """Parts the NLP categorizer hasn't classified.

    The corpus `Part` entities are populated lazily.  Until the DW snapshot
    is ingested, we instead check `part_category` (which IS populated from
    the pipeline) vs the distinct parts seen in `llm_dispatch_log` contributors.
    If `part_category` is empty entirely, that itself is a directive.
    """
    out: list[Directive] = []
    try:
        n_categorized = cn.execute(
            "SELECT COUNT(DISTINCT part_id) FROM part_category"
        ).fetchone()[0] or 0
    except sqlite3.OperationalError:
        return out

    # Also check corpus for unclassified Part entities (populated once DW ingest runs).
    n_corpus_missing = 0
    try:
        n_corpus_missing = cn.execute(
            """SELECT COUNT(*) FROM corpus_entity e
               LEFT JOIN corpus_edge x
                 ON x.src_id=e.entity_id AND x.src_type='Part' AND x.rel='CLASSIFIED_AS'
               WHERE e.entity_type='Part' AND x.src_id IS NULL"""
        ).fetchone()[0] or 0
    except sqlite3.OperationalError:
        pass

    if n_categorized == 0:
        out.append(Directive(
            source="corpus",
            signal_kind="missing_category",
            title="part_category table is empty — NLP categorizer has never run against the DW",
            why_it_matters=(
                "Vendor consolidation and EOQ deviation analytics both rely on "
                "part categories. With zero categories, every spend analysis "
                "is missing its primary dimension."
            ),
            do_this=(
                "Run `python apply_proc.py` to pull the latest DW snapshot "
                "and rebuild `part_category`. Then open EOQ Deviation → "
                "'NLP Recategorize' to verify coverage."
            ),
            owner_role="Planner",
            priority=0.70,
            severity="act",
            target_entity=None,
            evidence={"part_category_rows": 0},
        ))
    elif n_corpus_missing > 0:
        prio = float(min(0.85, 0.30 + min(n_corpus_missing, 1000) / 1000.0 * 0.5))
        out.append(Directive(
            source="corpus",
            signal_kind="missing_category",
            title=f"{n_corpus_missing} corpus parts still unclassified",
            why_it_matters=(
                "Vendor consolidation and EOQ deviation analytics rely on "
                "part categories. Every uncategorized part is a blind spot."
            ),
            do_this=(
                "Open the **EOQ Deviation** page → 'NLP Recategorize' button, "
                "or run `python apply_proc.py` to rebuild `part_category` from "
                "the latest replica DW snapshot."
            ),
            owner_role="Planner",
            priority=prio,
            severity=_severity(prio),
            target_entity=None,
            evidence={"unclassified_parts": int(n_corpus_missing)},
        ))
    return out


def _gen_self_train_drift(cn) -> list[Directive]:
    """If the most recent self-train round capped a lot of drifts, the User
    should know the Brain is being held back from a strong signal."""
    out: list[Directive] = []
    try:
        rows = cn.execute(
            """SELECT id, ran_at, task, samples, matched, drift_capped
               FROM llm_self_train_log
               WHERE ran_at >= datetime('now', '-1 day')
               ORDER BY id DESC LIMIT 5"""
        ).fetchall()
    except sqlite3.OperationalError:
        return out
    for r in rows:
        if int(r["drift_capped"] or 0) <= 0:
            continue
        prio = 0.45
        out.append(Directive(
            source="self_train",
            signal_kind="drift_capped",
            title=f"Self-train on '{r['task']}' hit drift cap "
                  f"({r['drift_capped']} models clamped)",
            why_it_matters=(
                f"The pipeline is producing strong corrective signal on "
                f"'{r['task']}' but the bounded learner is throttling it "
                f"to preserve fluidity. Confirm the ground-truth source "
                f"is still trustworthy."
            ),
            do_this=(
                f"Review the latest `llm_self_train_log` row, then "
                f"either widen `llms.self_train.drift_cap` (cautiously) "
                f"or refresh the ground-truth table for '{r['task']}'."
            ),
            owner_role="IT",
            priority=prio,
            severity=_severity(prio),
            target_entity=f"Task::{r['task']}",
            evidence={
                "matched": int(r["matched"] or 0),
                "samples": int(r["samples"] or 0),
                "drift_capped": int(r["drift_capped"]),
            },
        ))
    return out


def _gen_high_centrality_part(cn) -> list[Directive]:
    """A Part (or Model, once supply-chain data flows) with many corpus edges.

    Until the DW snapshot populates Part entities, this generator watches
    Model nodes in the corpus for low average edge weight — a model that
    is connected to many tasks with low WEIGHTED_FOR scores is under-delivering
    on coverage and is a tuning opportunity for the User.
    """
    out: list[Directive] = []

    # ── Part-level signal (when Parts exist in corpus) ──────────────────────
    try:
        part_rows = cn.execute(
            """SELECT src_id AS part, COUNT(*) AS n_edges
                 FROM corpus_edge
                WHERE src_type='Part'
                GROUP BY src_id
               HAVING n_edges >= 3
               ORDER BY n_edges DESC LIMIT 3"""
        ).fetchall()
    except sqlite3.OperationalError:
        part_rows = []
    for r in part_rows:
        prio = float(min(0.75, 0.35 + int(r["n_edges"]) * 0.05))
        out.append(Directive(
            source="corpus",
            signal_kind="high_centrality_part",
            title=f"High-centrality part '{r['part']}' "
                  f"({r['n_edges']} corpus edges) — consolidation candidate",
            why_it_matters=(
                "This part touches many suppliers/categories/sites in the "
                "relational corpus. Concentrating spend or renegotiating "
                "could move the needle quickly."
            ),
            do_this=(
                f"Open **Procurement 360** filtered to part `{r['part']}` "
                f"and review supplier share + average unit cost. Consider "
                f"an RFQ if top-2 supplier share < 80%."
            ),
            owner_role="Buyer",
            priority=prio,
            severity=_severity(prio),
            target_entity=f"Part::{r['part']}",
            evidence={"corpus_edges": int(r["n_edges"])},
        ))

    # ── Model-level signal (always available from corpus) ────────────────────
    if not part_rows:
        try:
            model_rows = cn.execute(
                """SELECT src_id AS model, COUNT(*) AS n_tasks,
                          AVG(weight) AS avg_w, MIN(weight) AS min_w
                     FROM corpus_edge
                    WHERE src_type='Model' AND rel='WEIGHTED_FOR'
                    GROUP BY src_id
                   HAVING n_tasks >= 1 AND avg_w < 0.90
                   ORDER BY avg_w ASC LIMIT 3"""
            ).fetchall()
        except sqlite3.OperationalError:
            model_rows = []
        for r in model_rows:
            avg_w = float(r["avg_w"] or 0.0)
            prio = float(min(0.72, 0.40 + (0.90 - avg_w) * 0.8))
            out.append(Directive(
                source="corpus",
                signal_kind="model_low_task_weight",
                title=f"Model '{r['model']}' has low avg corpus weight {avg_w:.2f} across {r['n_tasks']} tasks",
                why_it_matters=(
                    f"The relational graph shows '{r['model']}' is "
                    f"consistently under-weighted by the ensemble router. "
                    f"It is consuming API budget but not influencing answers."
                ),
                do_this=(
                    f"Review `llm_weights` for '{r['model']}' across all tasks. "
                    f"If the model is stale or slow, add it to "
                    f"`llms.scout.blocklist` in `brain.yaml` and re-run "
                    f"a self-train round to redistribute weight."
                ),
                owner_role="IT",
                priority=prio,
                severity=_severity(prio),
                target_entity=f"Model::{r['model']}",
                evidence={"avg_weight": round(avg_w, 3), "tasks": int(r["n_tasks"])},
            ))
    return out


def _gen_weak_llm_weights(cn) -> list[Directive]:
    """Read `llm_weights` directly — the authoritative learned weight table.
    Flag any (task, model) where `weight` has decayed below 1.0 or
    `ema_success` dropped below 0.80 with sufficient observations.
    Also flags the `underdog` pattern: a real model stuck at weight ≤ 0.5.
    """
    out: list[Directive] = []
    try:
        rows = cn.execute(
            """SELECT task, model_id, weight, ema_success, ema_latency, n_obs
                 FROM llm_weights
                WHERE n_obs >= 5
                  AND (weight < 1.0 OR ema_success < 0.80)
                ORDER BY weight ASC, ema_success ASC LIMIT 8"""
        ).fetchall()
    except sqlite3.OperationalError:
        return out
    for r in rows:
        w = float(r["weight"] or 0.0)
        ok = float(r["ema_success"] or 0.0)
        n = int(r["n_obs"] or 0)
        prio = float(min(0.88, 0.40 + (1.0 - min(w, 1.0)) * 0.3 + (0.80 - min(ok, 0.80)) * 0.5))
        out.append(Directive(
            source="dispatch",
            signal_kind="weak_model_weight",
            title=f"Model '{r['model_id']}' on '{r['task']}': weight={w:.2f}, EMA ok={ok:.1%} ({n} obs)",
            why_it_matters=(
                f"The router has down-weighted or penalized '{r['model_id']}' "
                f"on '{r['task']}' based on {n} real observations. It is "
                f"either slow ({r['ema_latency']:.0f}ms avg) or unreliable."
            ),
            do_this=(
                f"Inspect the most recent `llm_dispatch_log` rows for "
                f"task='{r['task']}' and model='{r['model_id']}'. If the model "
                f"is consistently timing out, blocklist it in `brain.yaml`. "
                f"If it's a test artifact, delete it from `llm_weights`."
            ),
            owner_role="IT",
            priority=prio,
            severity=_severity(prio),
            target_entity=f"Model::{r['model_id']}",
            evidence={"weight": round(w, 3), "ema_success": round(ok, 3), "n_obs": n},
        ))
    return out


def _gen_mission_signals(cn) -> list[Directive]:
    """Surface the open Quest-Console missions back to the Body so the User
    sees them alongside every other Brain directive. Three sub-signals:

      1. **stalled** — open mission with no refresh in > 3 days.
      2. **hot_findings** — mission whose latest run produced ≥ 5 findings
         with score ≥ 0.7 (the Brain wants the Body's eyes on it).
      3. **near_complete** — progress_pct ≥ 0.85 → time to close it out.

    The `cn` argument is for the local brain DB; missions live in
    `findings_index.db`, so we open that connection separately.
    """
    out: list[Directive] = []
    try:
        from . import mission_store, findings_index, quests
    except Exception:
        return out

    try:
        open_missions = mission_store.list_open(limit=50)
    except Exception:
        return out

    now = datetime.now(timezone.utc)
    for m in open_missions:
        mid = getattr(m, "id", "") or ""
        site = getattr(m, "site", "") or "unknown"
        quest_id = getattr(m, "quest_id", "") or ""
        try:
            quest = quests.get_quest(quest_id)
            quest_label = quest.name if quest else quest_id
        except Exception:
            quest_label = quest_id
        parsed_intent = getattr(m, "parsed_intent", {}) or {}
        owner = parsed_intent.get("owner_role") or "Anyone"
        target_kind = getattr(m, "target_entity_kind", "") or "site"
        target_key = getattr(m, "target_entity_key", "") or site
        target_entity = f"Mission::{mid}"

        # 1) Stalled mission?
        last_ref = (getattr(m, "last_refreshed_at", "")
                    or getattr(m, "created_at", "") or "")
        try:
            ts = datetime.fromisoformat(last_ref.replace("Z", "+00:00"))
            age_days = (now - ts).total_seconds() / 86400.0
        except Exception:
            age_days = 0.0
        if age_days >= 3.0:
            prio = float(min(1.0, 0.55 + age_days / 30.0))
            out.append(Directive(
                source="mission",
                signal_kind="mission_stalled",
                title=f"Mission '{quest_label}' @ {site} hasn't refreshed in {age_days:.0f}d",
                why_it_matters=(
                    "An open mission is the Brain's contract with the Body. "
                    "If it isn't being refreshed, the living one-pager and "
                    "implementation-plan PPTXs are going stale."
                ),
                do_this=(
                    f"Open Quest Console → Mission {mid}, click 'Refresh now', "
                    f"or close the mission if it's no longer relevant."
                ),
                owner_role=owner,
                priority=prio,
                severity=_severity(prio),
                target_entity=target_entity,
                evidence={"mission_id": mid, "age_days": round(age_days, 1),
                          "site": site, "target": f"{target_kind}:{target_key}"},
            ))

        # 2) Hot findings? — findings live in findings_index.db, not the
        # local brain DB, so always query the right connection directly.
        n_hot = 0
        try:
            with findings_index._conn() as fcn:
                fcn.row_factory = sqlite3.Row
                row = fcn.execute(
                    """SELECT COUNT(*) AS n FROM findings
                        WHERE json_extract(payload_json, '$.mission_id') = ?
                          AND score >= 0.7""",
                    (mid,),
                ).fetchone()
                n_hot = int(row["n"]) if row else 0
        except Exception:
            n_hot = 0
        if n_hot >= 5:
            prio = float(min(1.0, 0.6 + n_hot / 50.0))
            out.append(Directive(
                source="mission",
                signal_kind="mission_hot_findings",
                title=f"Mission '{quest_label}' @ {site}: {n_hot} high-score findings",
                why_it_matters=(
                    "The Brain's analyzers flagged a cluster of strong signals "
                    "for this mission. The Body needs to triage them while "
                    "they're fresh."
                ),
                do_this=(
                    f"Open Quest Console → Mission {mid}, review the top "
                    f"findings table and decide which become directives."
                ),
                owner_role=owner,
                priority=prio,
                severity=_severity(prio),
                target_entity=target_entity,
                evidence={"mission_id": mid, "high_score_findings": n_hot},
            ))

        # 3) Near-complete mission? (progress_pct stored on 0..100 scale)
        try:
            pct = float(getattr(m, "progress_pct", 0.0) or 0.0)
        except Exception:
            pct = 0.0
        if pct >= 85.0 and (getattr(m, "status", "open") or "open") == "open":
            prio = 0.7 + (pct - 85.0) / 50.0
            prio = float(min(1.0, max(0.0, prio)))
            out.append(Directive(
                source="mission",
                signal_kind="mission_near_complete",
                title=f"Mission '{quest_label}' @ {site} is {pct:.0f}% complete",
                why_it_matters=(
                    "Closing a mission promotes its findings into the durable "
                    "decision log and frees the Brain's attention for the next "
                    "quest."
                ),
                do_this=(
                    f"Open Quest Console → Mission {mid}, verify the KPI "
                    f"deltas hold, then mark it done."
                ),
                owner_role=owner,
                priority=prio,
                severity=_severity(prio),
                target_entity=target_entity,
                evidence={"mission_id": mid, "progress_pct": round(pct, 2)},
            ))

    return out


def _gen_corpus_stagnation(cn) -> list[Directive]:
    """Detect when the RAG deepdive has been running but producing zero edges.

    If the last 3+ learning_log entries for kind='rag_deepdive' all report
    zero edges_discovered, the explored-pair cache is saturated — the Brain
    needs fresh data to find new pathways. Surface a directive so the User
    knows to check ERP/Azure SQL connectivity or to clear the cache.
    """
    out: list[Directive] = []
    try:
        rows = cn.execute(
            """SELECT id, logged_at, signal_strength
               FROM learning_log
               WHERE kind='rag_deepdive'
                 AND logged_at >= datetime('now', '-48 hour')
               ORDER BY id DESC LIMIT 5"""
        ).fetchall()
    except sqlite3.OperationalError:
        return out
    if len(rows) < 3:
        return out
    # All recent deepdive entries have zero signal = no new edges
    if all(float(r["signal_strength"] or 0.0) == 0.0 for r in rows):
        out.append(Directive(
            source="corpus",
            signal_kind="corpus_rag_saturated",
            title="RAG deepdive has discovered 0 new edges in last 48 h — corpus saturated",
            why_it_matters=(
                "The explored-pair cache in brain_kv is exhausted. The Brain is "
                "re-traversing known paths without finding new knowledge. Fresh "
                "data from ERP or a cache reset would unlock deeper learning."
            ),
            do_this=(
                "Run `python -c \"from pipeline.autonomous_agent import _kv_write; "
                "_kv_write('rag_explored_pairs', '[]')\"` to clear the explored "
                "cache, then verify ERP / Azure SQL data sources are active. "
                "Alternatively, check if corpus_entity has grown since last week."
            ),
            owner_role="IT",
            priority=0.50,
            severity="watch",
            target_entity=None,
            evidence={"recent_deepdive_entries": len(rows)},
        ))
    return out


def _gen_doc_rag_coverage(cn) -> list[Directive]:
    """Alert when doc RAG documents exist but the index is stale or missing.

    Checks whether Markdown files in ``pipeline/data/documents/`` are newer
    than the last ``doc_rag_last_index`` timestamp in ``brain_kv``. If so,
    the FAISS index is stale and DBI insight calls are missing context.
    """
    out: list[Directive] = []
    try:
        import os
        from pathlib import Path
        doc_dir = _PIPELINE_ROOT / "data" / "documents"
        if not doc_dir.is_dir():
            return out
        md_files = list(doc_dir.rglob("*.md"))
        if not md_files:
            return out

        last_indexed_ts = 0.0
        try:
            row = cn.execute(
                "SELECT value FROM brain_kv WHERE key='doc_rag_last_index'"
            ).fetchone()
            if row:
                last_indexed_ts = float(row[0] or 0)
        except sqlite3.OperationalError:
            pass

        newest_doc_ts = max(f.stat().st_mtime for f in md_files)
        n_stale = sum(
            1 for f in md_files if f.stat().st_mtime > last_indexed_ts
        )
        if n_stale == 0:
            return out

        prio = float(min(0.65, 0.35 + n_stale / 20.0))
        out.append(Directive(
            source="corpus",
            signal_kind="doc_rag_stale_index",
            title=f"{n_stale} document(s) in data/documents/ not yet indexed for RAG",
            why_it_matters=(
                "The Brain's DBI insight engine uses the FAISS document index "
                "as a third retrieval source. Stale or missing indexing means "
                "LLM answers lack the context captured in your process documents."
            ),
            do_this=(
                "The autonomous agent will re-index within 6 hours automatically. "
                "To force it now: `cd pipeline; python -c \"from src.brain.doc_rag "
                "import index_documents; index_documents(fresh=True)\"`"
            ),
            owner_role="IT",
            priority=prio,
            severity=_severity(prio),
            target_entity=None,
            evidence={"stale_docs": n_stale, "total_docs": len(md_files)},
        ))
    except Exception as _e:
        logging.debug(f"_gen_doc_rag_coverage: {_e}")
    return out


_GENERATORS: list[Callable] = [
    _gen_low_dispatch_quality,
    _gen_peer_unreachable,
    _gen_missing_category,
    _gen_self_train_drift,
    _gen_high_centrality_part,
    _gen_weak_llm_weights,
    _gen_mission_signals,
    _gen_corpus_stagnation,
    _gen_doc_rag_coverage,
]


# ---------------------------------------------------------------------------
# Synaptic field — full ADAM with Bayesian-Poisson centroid targets
# ---------------------------------------------------------------------------
# Revised from the conversation-corpus pattern (used in EOQ centroid sensing
# and bullwhip demand smoothing): each signal_kind has a Poisson firing-rate
# whose Bayesian centroid (Gamma posterior mean) is the gradient *target*
# for the ADAM optimizer.  The optimizer then walks pressure toward that
# centroid with first/second-moment momentum and bias correction.
#
#   Prior:        rate ~ Gamma(α₀, β₀)  with α₀=1.0, β₀=2.0
#   Likelihood:   directive_count_t  ~  Poisson(rate)
#   Posterior:    rate_t = (α₀ + Σ counts) / (β₀ + n_rounds)
#   Centroid:     λ_t = posterior_mean × mean_priority_t
#                 (rate-weighted urgency — a kind that fires often AND
#                  with high priority gets a larger target)
#
#   ADAM step:    g_t   = λ_t − pressure_{t-1}
#                 m_t   = β1·m_{t-1} + (1−β1)·g_t
#                 v_t   = β2·v_{t-1} + (1−β2)·g_t²
#                 m̂_t   = m_t / (1 − β1^t)        ← bias correction
#                 v̂_t   = v_t / (1 − β2^t)
#                 p_t   = p_{t-1} + lr · m̂_t / (√v̂_t + ε)
#                 p_t   ∈ [0, 1]                   ← saturating clamp
#
# Resolved kinds get a *negative* synthetic gradient through the same ADAM
# machinery — the inverted-ReLU floor of the dual.  Because m and v retain
# their history, a kind that oscillates fire→resolve→fire develops high v
# (variance) which damps the effective step size — the optimizer becomes
# cautious about flapping signals exactly the way real ADAM does on noisy
# gradients.  This is the propeller's torsional damping.
_TOUCH_BETA1   = 0.85       # 1st moment momentum
_TOUCH_BETA2   = 0.999      # 2nd moment momentum (real-ADAM scale)
_TOUCH_LR      = 0.30       # learning rate per round
_TOUCH_EPS     = 1e-6       # numerical floor for √v
_BAYES_ALPHA0  = 1.0        # Gamma prior shape
_BAYES_BETA0   = 2.0        # Gamma prior rate
_RESOLVED_GRAD = -0.50      # synthetic negative gradient for resolved kinds
_TOUCH_FIELD_KEY      = "touch_field_state"        # consumer-facing {kind: p}
_TOUCH_FIELD_FULL_KEY = "touch_field_full_state"   # internal optimizer state


def _kv_read(cn, key: str) -> dict:
    try:
        row = cn.execute(
            "SELECT value FROM brain_kv WHERE key=?", (key,)
        ).fetchone()
        if row and row[0]:
            return json.loads(row[0])
    except (sqlite3.OperationalError, json.JSONDecodeError):
        pass
    return {}


def _kv_write(cn, key: str, val: dict) -> None:
    cn.execute(
        "CREATE TABLE IF NOT EXISTS brain_kv("
        "key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)"
    )
    cn.execute(
        "INSERT OR REPLACE INTO brain_kv(key, value, updated_at) VALUES(?,?,?)",
        (key, json.dumps(val, default=str),
         datetime.now(timezone.utc).isoformat()),
    )


def _resolve_stale_directives(cn, fresh_fingerprints: set[str]) -> int:
    """Mark open directives as `expired` when their generator no longer fires.

    The dual floor: each open directive is the inverse of a Vision observation.
    When Vision's next round shows the signal has resolved (the generator
    didn't re-emit a directive with the same fingerprint), we collapse that
    directive — closing the inward arm of the closed loop.
    """
    now = datetime.now(timezone.utc).isoformat()
    rows = cn.execute(
        "SELECT id, fingerprint, signal_kind FROM body_directives "
        "WHERE status IN ('open','ack')"
    ).fetchall()
    expired = 0
    for r in rows:
        if r["fingerprint"] not in fresh_fingerprints:
            cn.execute(
                "UPDATE body_directives SET status='expired', "
                "last_status_at=? WHERE id=?",
                (now, int(r["id"])),
            )
            expired += 1
    return expired


def _bayesian_poisson_centroid(state: dict, this_count: int,
                               this_mean_priority: float) -> float:
    """Posterior mean of a Gamma-Poisson conjugate model, scaled by the
    round's mean priority. Returns the ADAM gradient target.

    state carries 'sum_counts' and 'n_rounds' across all prior observations
    so the centroid evolves as more data arrives — exactly the EOQ centroid
    pattern adapted to directive firing rates.
    """
    sum_counts = float(state.get("sum_counts", 0.0)) + float(this_count)
    n_rounds   = float(state.get("n_rounds",   0.0)) + 1.0
    posterior_rate = (_BAYES_ALPHA0 + sum_counts) / (_BAYES_BETA0 + n_rounds)

    # Update state in place for next round
    state["sum_counts"] = sum_counts
    state["n_rounds"]   = n_rounds

    # Rate-weighted urgency target ∈ [0, 1] (clamped because both factors
    # can be > 1 in extreme bursts but we don't want pressure unbounded)
    return float(min(1.0, posterior_rate * max(0.0, this_mean_priority)))


def _adam_step(state: dict, gradient: float) -> float:
    """One ADAM update on a single signal_kind's pressure.

    state must hold {m, v, t, pressure}.  Mutates state in place and
    returns the new pressure value clamped to [0, 1].
    """
    m_prev = float(state.get("m", 0.0))
    v_prev = float(state.get("v", 0.0))
    t_prev = int(state.get("t", 0))
    p_prev = float(state.get("pressure", 0.0))
    t      = t_prev + 1

    m = _TOUCH_BETA1 * m_prev + (1.0 - _TOUCH_BETA1) * gradient
    v = _TOUCH_BETA2 * v_prev + (1.0 - _TOUCH_BETA2) * (gradient * gradient)

    # Bias correction (real ADAM)
    m_hat = m / (1.0 - _TOUCH_BETA1 ** t)
    v_hat = v / (1.0 - _TOUCH_BETA2 ** t)

    step  = _TOUCH_LR * m_hat / ((v_hat ** 0.5) + _TOUCH_EPS)
    p_new = max(0.0, min(1.0, p_prev + step))

    state["m"]        = m
    state["v"]        = v
    state["t"]        = t
    state["pressure"] = p_new
    return p_new


def _update_touch_field(cn, fresh: list[Directive],
                        resolved_kinds: set[str],
                        vision_grads: dict[str, float] | None = None
                        ) -> dict[str, float]:
    """Bayesian-Poisson-centroid ADAM optimizer over per-signal-kind pressure.

    Inputs come from BOTH sides of the closed loop:
      * Touch side  — `fresh` directives (positive pressure-target gradient
        via Bayesian-Poisson centroid) and `resolved_kinds` (synthetic
        negative gradient).
      * Vision side — `vision_grads`: per-kind gradients derived from what
        Vision actually accomplished this round (entities discovered,
        blades fired, learnings logged).  These are negative — successful
        Vision operations relieve pressure on the kinds they served.

    All three streams pass through the SAME ADAM update so m and v see
    every input source and the variance accumulator captures noise across
    all of them — Vision overshoot, Touch overreaction, and resolution
    flapping all damp the next step uniformly.

    Returns the consumer-facing {kind: pressure} dict; also persists the
    full optimizer state (m, v, t, sum_counts, n_rounds, pressure) so each
    round picks up exactly where the last left off.
    """
    full_state = _kv_read(cn, _TOUCH_FIELD_FULL_KEY) or {}
    vision_grads = vision_grads or {}

    # Aggregate per-kind firing for this round.
    counts: dict[str, int]   = {}
    sums:   dict[str, float] = {}
    for d in fresh:
        counts[d.signal_kind] = counts.get(d.signal_kind, 0) + 1
        sums[d.signal_kind]   = sums.get(d.signal_kind, 0.0) + float(d.priority)

    pressure_view: dict[str, float] = {}
    all_kinds = (
        set(full_state.keys())
        | set(counts.keys())
        | set(resolved_kinds)
        | set(vision_grads.keys())
    )

    for kind in all_kinds:
        st = dict(full_state.get(kind) or {})
        v_grad = float(vision_grads.get(kind, 0.0))

        if kind in counts:
            # Touch firing: Bayesian-Poisson centroid drives the gradient.
            mean_p   = sums[kind] / counts[kind]
            target   = _bayesian_poisson_centroid(st, counts[kind], mean_p)
            t_grad   = target - float(st.get("pressure", 0.0))
            grad     = t_grad + v_grad        # Touch + Vision in one step
        elif kind in resolved_kinds:
            # Resolved this round: synthetic negative gradient + Vision relief.
            grad = _RESOLVED_GRAD * float(st.get("pressure", 0.0)) + v_grad
            _ = _bayesian_poisson_centroid(st, 0, 0.0)
        elif v_grad != 0.0:
            # Vision did work for a kind Touch isn't tracking this round —
            # still apply the relief (e.g., DW added entities even though no
            # missing_category directive fired).
            decay = -0.05 * float(st.get("pressure", 0.0))
            grad  = decay + v_grad
            _ = _bayesian_poisson_centroid(st, 0, 0.0)
        else:
            # Quiet from both sides — pure decay toward 0.
            grad = -0.05 * float(st.get("pressure", 0.0))

        new_p = _adam_step(st, grad)
        full_state[kind] = st

        if new_p > 0.005 or kind in counts or kind in resolved_kinds or v_grad != 0.0:
            pressure_view[kind] = round(new_p, 4)

    _kv_write(cn, _TOUCH_FIELD_FULL_KEY, full_state)
    _kv_write(cn, _TOUCH_FIELD_KEY,      pressure_view)
    return pressure_view


def get_touch_field() -> dict[str, float]:
    """Public read accessor — Vision calls this at the start of each round
    to see which signal_kinds the body is most pressured by, and bias its
    outreach budget accordingly. The pivoted channel that locks Vision and
    Touch into a single dimensional axis."""
    init_schema()
    with _conn() as cn:
        return _kv_read(cn, _TOUCH_FIELD_KEY) or {}


def get_touch_field_full() -> dict:
    """Diagnostic accessor — returns the full ADAM state per signal_kind
    (m, v, t, sum_counts, n_rounds, pressure) so you can inspect how the
    optimizer is converging."""
    init_schema()
    with _conn() as cn:
        return _kv_read(cn, _TOUCH_FIELD_FULL_KEY) or {}


# ---------------------------------------------------------------------------
# Vision-ops → gradient mapping
# ---------------------------------------------------------------------------
# Vision operations (entities discovered, blades fired, learnings logged) are
# *negative* evidence on the corresponding Touch signal_kinds — they prove
# that the body's outreach actually moved the corpus, so the pressure that
# triggered them should relax.  This gives the closed loop a true bilateral
# input: Touch's directives push pressure UP, Vision's discoveries push it
# DOWN, and ADAM mediates between them with momentum and variance damping.
#
# A blade's discovery effort produces a "satisfaction gradient" sized by
# how much it accomplished, scaled by an empirical weight per kind.
_VISION_OPS_MAP: dict[str, tuple[str, float]] = {
    # vision_op_key      → (signal_kind that it relieves, weight per unit)
    "dw_entities":         ("missing_category",            -0.015),
    "dw_deepen_entities":  ("high_centrality_part",        -0.020),
    "ocw_entities":        ("corpus_rag_saturated",        -0.010),
    "ocw_resources":       ("model_low_task_weight",       -0.012),
    "network_endpoints":   ("peer_unreachable",            -0.025),
    "network_endpoints2":  ("network_learner_not_started", -0.025),
    "schema_learnings":    ("self_train_drift",            -0.008),
    "rag_chunks":          ("doc_rag_coverage",            -0.015),
    "mission_signals":     ("mission_signals",             -0.020),
}


def _vision_ops_gradients(vision_ops: dict) -> dict[str, float]:
    """Translate a Vision round's operation counts into per-signal_kind
    negative gradients (relief).  Multiple ops may target the same kind;
    contributions are summed.

    vision_ops example:
        {"dw_entities": 32, "ocw_entities": 369, "network_endpoints": 8,
         "schema_learnings": 12, "forced_blades": ["ocw"]}
    """
    grads: dict[str, float] = {}
    if not vision_ops:
        return grads
    for op_key, (kind, weight_per_unit) in _VISION_OPS_MAP.items():
        n = float(vision_ops.get(op_key, 0) or 0)
        if n <= 0:
            continue
        # Saturating: 50 entities is "fully relieved", more doesn't help.
        relief = weight_per_unit * min(n, 50.0)
        grads[kind] = grads.get(kind, 0.0) + relief

    # Forced-blade firing is a confirmation signal — Vision believed Touch
    # enough to override its natural schedule.  Bump confidence (lower
    # variance push) on the kinds that drove that force.
    for blade in (vision_ops.get("forced_blades") or []):
        # Heuristic: each forced blade gives a small extra relief to its
        # primary kind so the optimizer learns "yes, that was real".
        if blade == "dw":
            grads["missing_category"] = grads.get("missing_category", 0.0) - 0.05
        elif blade == "ocw":
            grads["corpus_rag_saturated"] = grads.get("corpus_rag_saturated", 0.0) - 0.05
        elif blade == "network":
            grads["peer_unreachable"] = grads.get("peer_unreachable", 0.0) - 0.05
    return grads


# ---------------------------------------------------------------------------
# Round driver
# ---------------------------------------------------------------------------
def surface_effective_signals(*, top_k: int | None = None,
                              vision_ops: dict | None = None) -> dict:
    """Run every generator, dedupe by fingerprint, persist, return summary.

    Closed-loop pass — bilateral inputs:
      1. Run every generator → collect fresh Directive set      (Touch in)
      2. Translate `vision_ops` → per-kind relief gradients     (Vision in)
      3. Mark any open directive whose fingerprint is NOT fresh as `expired`
         (the inverse-ReLU floor: signal resolved → directive collapses)
      4. Apply ADAM-style update to per-signal-kind pressure field, fed
         simultaneously by Touch's positive Bayesian-Poisson targets and
         Vision's negative satisfaction gradients
      5. Persist new directives + pressure field to brain_kv
      6. Vision reads the pressure field next round to steer outreach
    """
    if not _enabled():
        return {"enabled": False}

    init_schema()
    cfg = _cfg()
    min_seconds = float(cfg.get("min_seconds_between_rounds", 30.0))

    global _LAST_SURFACE_TS
    with _SURFACE_LOCK:
        if time.monotonic() - _LAST_SURFACE_TS < min_seconds:
            return {"skipped": True, "reason": "rate-limited"}
        _LAST_SURFACE_TS = time.monotonic()

    all_directives: list[Directive] = []
    notes: list[str] = []
    with _conn() as cn:
        for gen in _GENERATORS:
            try:
                all_directives.extend(gen(cn) or [])
            except Exception as e:
                notes.append(f"{gen.__name__}: {e}")

        # Sort by priority descending, optional top-k cap.
        all_directives.sort(key=lambda d: -d.priority)
        cap = int(top_k or cfg.get("max_directives_per_round", 25))
        all_directives = all_directives[:cap]

        # ── Stale-directive collapse (inverse-ReLU floor) ─────────────────
        fresh_fps = {d.fingerprint() for d in all_directives}
        previously_open = cn.execute(
            "SELECT signal_kind, fingerprint FROM body_directives "
            "WHERE status IN ('open','ack')"
        ).fetchall()
        prev_open_kinds = {r["signal_kind"] for r in previously_open}
        expired = _resolve_stale_directives(cn, fresh_fps)
        firing_kinds = {d.signal_kind for d in all_directives}
        resolved_kinds = prev_open_kinds - firing_kinds

        emitted = 0
        deduped = 0
        now = datetime.now(timezone.utc).isoformat()
        for d in all_directives:
            fp = d.fingerprint()
            existing = cn.execute(
                "SELECT id, status FROM body_directives WHERE fingerprint=?",
                (fp,),
            ).fetchone()
            if existing is not None:
                # Re-open if it had been auto-expired but the signal is
                # still firing; otherwise leave User-driven status alone.
                if existing["status"] == "expired":
                    cn.execute(
                        """UPDATE body_directives SET status='open',
                              last_status_at=? WHERE id=?""",
                        (now, int(existing["id"])),
                    )
                deduped += 1
                continue
            cn.execute(
                """INSERT INTO body_directives(
                      created_at, fingerprint, source, signal_kind, priority,
                      severity, title, why_it_matters, do_this, owner_role,
                      target_entity, evidence_json, value_per_year)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (now, fp, d.source, d.signal_kind, float(d.priority),
                 d.severity, d.title, d.why_it_matters, d.do_this,
                 d.owner_role, d.target_entity,
                 json.dumps(d.evidence, default=str), d.value_per_year),
            )
            emitted += 1

        top_p = max((d.priority for d in all_directives), default=0.0)

        # ── ADAM-style synaptic pressure update ───────────────────────────
        # The propeller axle: this field is what Vision reads next round
        # to bias outreach budget per signal_kind. Resolved kinds get an
        # inverted-ReLU cooldown so Vision stops wasting cycles on them.
        # Bilateral input: Touch directives (positive targets) AND Vision
        # operation counts (negative relief) both pass through ADAM here.
        v_grads = _vision_ops_gradients(vision_ops or {})
        touch_field = _update_touch_field(cn, all_directives,
                                          resolved_kinds, v_grads)

        cn.execute(
            """INSERT INTO body_round_log(ran_at, directives_emitted,
                  directives_deduped, top_priority, notes)
               VALUES(?,?,?,?,?)""",
            (now, emitted, deduped, float(top_p),
             "; ".join(notes) or None),
        )

    return {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "directives_emitted": emitted,
        "directives_deduped": deduped,
        "directives_expired": expired,
        "top_priority":       top_p,
        "considered":         len(all_directives),
        "touch_field":        touch_field,
        "resolved_kinds":     sorted(resolved_kinds),
        "vision_grads_in":    v_grads,
        "notes":              notes,
    }


# ---------------------------------------------------------------------------
# User-facing read / write APIs
# ---------------------------------------------------------------------------
def list_open_directives(limit: int = 50,
                         owner_role: str | None = None,
                         min_priority: float = 0.0) -> list[dict]:
    init_schema()
    with _conn() as cn:
        if owner_role:
            rows = cn.execute(
                """SELECT * FROM body_directives
                   WHERE status IN ('open','ack','in_progress')
                     AND owner_role=? AND priority >= ?
                   ORDER BY priority DESC, id DESC LIMIT ?""",
                (owner_role, float(min_priority), int(limit)),
            ).fetchall()
        else:
            rows = cn.execute(
                """SELECT * FROM body_directives
                   WHERE status IN ('open','ack','in_progress')
                     AND priority >= ?
                   ORDER BY priority DESC, id DESC LIMIT ?""",
                (float(min_priority), int(limit)),
            ).fetchall()
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        try:
            d["evidence"] = json.loads(d.pop("evidence_json", "") or "{}")
        except Exception:
            d["evidence"] = {}
        out.append(d)
    return out


def record_feedback(directive_id: int, status: str, *,
                    outcome: str | None = None,
                    executed_by: str | None = None) -> dict:
    """User reports back: ack | in_progress | done | rejected. The next
    knowledge_corpus round will pick this up and feed it back into the
    Brain (the loop closes)."""
    valid = {"ack", "in_progress", "done", "rejected"}
    if status not in valid:
        raise ValueError(f"status must be one of {valid}, got {status!r}")
    init_schema()
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as cn:
        cn.execute(
            """INSERT INTO body_feedback(directive_id, logged_at, status,
                   outcome, executed_by) VALUES(?,?,?,?,?)""",
            (int(directive_id), now, status, outcome, executed_by),
        )
        cn.execute(
            """UPDATE body_directives SET status=?, last_status_at=?
                WHERE id=?""",
            (status, now, int(directive_id)),
        )
    return {"directive_id": directive_id, "status": status, "logged_at": now}


def directive_summary() -> dict:
    init_schema()
    with _conn() as cn:
        by_status = cn.execute(
            "SELECT status, COUNT(*) AS n FROM body_directives GROUP BY status"
        ).fetchall()
        by_owner = cn.execute(
            """SELECT owner_role, COUNT(*) AS n FROM body_directives
                WHERE status IN ('open','ack','in_progress')
                GROUP BY owner_role"""
        ).fetchall()
        by_severity = cn.execute(
            """SELECT severity, COUNT(*) AS n FROM body_directives
                WHERE status IN ('open','ack','in_progress')
                GROUP BY severity"""
        ).fetchall()
    return {
        "by_status":   {r["status"]: int(r["n"]) for r in by_status},
        "by_owner":    {r["owner_role"]: int(r["n"]) for r in by_owner},
        "by_severity": {r["severity"]: int(r["n"]) for r in by_severity},
    }


def schedule_in_background(interval_s: int | None = None) -> threading.Thread:
    interval = int(interval_s or _cfg().get("interval_s", 600))

    def _loop() -> None:
        while True:
            try:
                surface_effective_signals()
            except Exception as e:
                logging.warning(f"brain_body_signals background round failed: {e}")
            time.sleep(max(60, interval))

    t = threading.Thread(target=_loop, name="brain_body_signals", daemon=True)
    t.start()
    return t
