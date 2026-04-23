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
            quest_label = quest.label if quest else quest_id
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

        # 2) Hot findings?
        try:
            hot = cn.execute(
                """SELECT COUNT(*) AS n FROM findings
                    WHERE json_extract(payload_json, '$.mission_id') = ?
                      AND score >= 0.7""",
                (mid,),
            ).fetchone()
        except sqlite3.OperationalError:
            hot = None
        # findings live in findings_index.db, not the local brain DB the
        # generator was handed — so query the right connection if local missed.
        n_hot = int((hot or {"n": 0})["n"]) if hot else 0
        if n_hot == 0:
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


_GENERATORS: list[Callable] = [
    _gen_low_dispatch_quality,
    _gen_peer_unreachable,
    _gen_missing_category,
    _gen_self_train_drift,
    _gen_high_centrality_part,
    _gen_weak_llm_weights,
    _gen_mission_signals,
]


# ---------------------------------------------------------------------------
# Round driver
# ---------------------------------------------------------------------------
def surface_effective_signals(*, top_k: int | None = None) -> dict:
    """Run every generator, dedupe by fingerprint, persist, return summary."""
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
        "top_priority":       top_p,
        "considered":         len(all_directives),
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
