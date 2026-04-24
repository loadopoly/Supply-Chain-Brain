"""Systemic Refinement Agent — continuous adaptive improvement of the entire
Supply Chain Brain and its supply-chain systems-engineering footprint.

Architecture
------------
The agent runs as a daemon thread alongside the autonomous_agent loop.  Each
cycle it executes five phases:

1. **SENSE**   — snapshot the full system state across all six faculties:
                 Brain (LLM ensemble health, self-train quality, RDT depth),
                 Vision (corpus entity/edge density per SC domain),
                 Touch (torus gap field, manifold pressure per dimension),
                 Smell (carrier mass, scent posterior, burst rate),
                 Body (directive aging, feedback ratio, stalled actions),
                 Heart (coherence scalar, Weyl centroid, rhythm factor),
                 DBI   (findings density, action friction, page insight quality).

2. **DIAGNOSE** — ten supply-chain refinement strategies each compute a
                  priority score [0..1].  Non-zero scores generate candidate
                  ``RefinementAction`` objects.

3. **RANK**     — candidates sorted by
                  ``priority × acquisition_drive × rhythm_factor``
                  so effort concentrates where the Brain is hungriest AND
                  the domain gap is widest.

4. **EXECUTE**  — top-``_MAX_ACTIONS_PER_CYCLE`` actions executed in order.
                  Each action produces exactly one traceable side-effect:
                    • launch a Mission (mission_runner)
                    • surface a Body directive (brain_body_signals)
                    • drop a skill-acquisition trigger file
                    • append a corpus seed to learning_log
                    • write a brain_kv config nudge
                    • record a high-priority findings row

5. **LEARN**    — full result written to ``systemic_refinement_log``.  Next
                  cycle the agent reads its own log to skip actions whose
                  content hash appeared within the last N hours with no
                  body feedback (feedback-gated deduplication).

Design constraints (shared with all bounded-learning modules):
    * Effect-bounded  — never mutates llm_weights, never deletes rows.
    * Auditable       — every action logged with full JSON context.
    * Fluidity-safe   — never touches narrative / what-if task weights.
    * Rate-limited    — 20-minute hard floor between full cycles.

Adaptive cadence
----------------
The inter-cycle sleep is modulated by the Brain's own learning health::

    base_interval = 1200 s (20 min)
    adjusted      = base_interval / max(0.1, acquisition_drive)
                    then clamped to [1200, 7200] s

High acquisition_drive (Brain hungry, stagnant) → cycles run often.
Low acquisition_drive (Brain settled, learning fast) → cycles relax.

Public API
----------
    run_refinement_cycle() -> RefinementReport
    schedule_in_background(interval_s=1200) -> threading.Thread
    recent_refinements(limit=50) -> list[dict]
    refinement_summary() -> dict
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable

from .local_store import db_path as _local_db_path
from . import load_config

log = logging.getLogger(__name__)

_PIPELINE_ROOT = Path(__file__).resolve().parents[2]
_CYCLE_LOCK    = threading.Lock()
_LAST_CYCLE_TS: float = 0.0
_CYCLE_MIN_SEC: float = 1200.0    # 20-minute hard floor
_STOP_EVENT    = threading.Event()

_MAX_ACTIONS_PER_CYCLE: int = 5    # rate-limiter on side-effects
_DEDUP_HOURS:           int = 6    # skip repeated identical actions for this window

# Supply-chain scope tags (mirrors quests.SCOPE_TAGS)
_SC_SCOPES = (
    "inventory_sizing",
    "fulfillment",
    "sourcing",
    "data_quality",
    "lead_time",
    "demand_distortion",
    "network_position",
    "cycle_count",
)


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
_DDL = """
CREATE TABLE IF NOT EXISTS systemic_refinement_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ran_at          TIMESTAMP NOT NULL,
    cycle_duration_ms INTEGER NOT NULL DEFAULT 0,
    state_json      TEXT,
    actions_json    TEXT,
    skipped_json    TEXT,
    notes           TEXT
);
CREATE TABLE IF NOT EXISTS refinement_actions_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id        INTEGER REFERENCES systemic_refinement_log(id),
    strategy        TEXT NOT NULL,
    priority        REAL NOT NULL,
    action_kind     TEXT NOT NULL,
    title           TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    payload_json    TEXT,
    executed        INTEGER NOT NULL DEFAULT 0,
    outcome_json    TEXT,
    executed_at     TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_ral_hash     ON refinement_actions_log(content_hash, executed_at);
CREATE INDEX IF NOT EXISTS ix_ral_strategy ON refinement_actions_log(strategy, executed_at);
CREATE INDEX IF NOT EXISTS ix_srl_ran_at   ON systemic_refinement_log(ran_at DESC);
"""


@contextmanager
def _conn():
    cn = sqlite3.connect(str(_local_db_path()))
    cn.row_factory = sqlite3.Row
    try:
        yield cn
        cn.commit()
    finally:
        cn.close()


def _init_schema() -> None:
    with _conn() as cn:
        cn.executescript(_DDL)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class SystemState:
    """Snapshot of every relevant Brain signal at cycle start."""
    # Heart
    coherence:          float = 0.5
    rhythm_factor:      float = 1.0
    weyl_centroid:      float = 0.0

    # Brain — LLM health
    self_train_quality:     float = 0.5   # normalised avg_validator
    self_train_stagnation:  float = 0.0   # fraction of tasks below quality floor
    rdt_mean_depth:         float = 1.0   # mean recurrent steps across tasks
    diversity_collapsed_tasks: list[str] = field(default_factory=list)

    # Brain — corpus
    entity_count:       int   = 0
    edge_count:         int   = 0
    learning_count:     int   = 0
    corpus_saturation:  float = 0.0
    sc_domain_density:  dict[str, float] = field(default_factory=dict)  # scope→entity density
    pending_learnings:  int   = 0

    # Vision — coverage gaps
    domains_without_missions: list[str] = field(default_factory=list)
    high_friction_domains:    list[str] = field(default_factory=list)

    # Touch — torus health
    torus_gap_mean:     float = 0.0
    torus_gap_by_dim:   list[float] = field(default_factory=list)  # length 7
    most_bunched_dims:  list[int]   = field(default_factory=list)

    # Smell
    carrier_mass:       float = 1.0
    burst_rate:         float = 0.0
    dominant_scent:     str   = "baseline"
    smell_certainty:    float = 0.0

    # Body
    open_directive_count:   int   = 0
    stalled_directive_count: int  = 0   # in_progress > 48 h
    body_feedback_ratio:    float = 0.0  # (done+rejected) / total

    # DBI — insight quality
    findings_count:     int   = 0
    high_score_findings: int  = 0
    pages_with_no_findings: list[str] = field(default_factory=list)

    # Learning drive
    acquisition_drive:  float = 0.1
    noise_sigma:        float = 0.0
    pivot_alpha:        float = 1.0

    def as_dict(self) -> dict:
        d = asdict(self)
        return d


@dataclass
class RefinementAction:
    strategy:       str
    action_kind:    str    # mission | directive | skill_trigger | corpus_seed | config_nudge | finding
    title:          str
    priority:       float  # [0..1]
    payload:        dict   = field(default_factory=dict)
    content_hash:   str    = ""

    def __post_init__(self):
        raw = json.dumps({"strategy": self.strategy, "kind": self.action_kind,
                          "title": self.title, "payload": self.payload},
                         sort_keys=True, default=str)
        self.content_hash = hashlib.sha256(raw.encode()).hexdigest()[:16]


@dataclass
class RefinementReport:
    cycle_id:          int
    started_at:        str
    elapsed_ms:        int
    state:             SystemState
    executed_actions:  list[dict]   = field(default_factory=list)
    skipped_actions:   list[dict]   = field(default_factory=list)
    notes:             list[str]    = field(default_factory=list)


# ---------------------------------------------------------------------------
# Phase 1: SENSE — read all subsystems
# ---------------------------------------------------------------------------

def _sense_heart() -> dict:
    try:
        from .temporal_spatiality import get_rhythm, get_rhythm_factor, measure_coherence
        rhythm = get_rhythm()
        return {
            "coherence":      float(rhythm.get("coherence", 0.5)),
            "rhythm_factor":  float(get_rhythm_factor("boost", 1.0)),
            "weyl_centroid":  float(rhythm.get("weyl_centroid", 0.0)),
        }
    except Exception as e:
        log.debug("_sense_heart: %s", e)
        return {"coherence": 0.5, "rhythm_factor": 1.0, "weyl_centroid": 0.0}


def _sense_learning_drive() -> dict:
    try:
        from .learning_drive import get_drive
        d = get_drive()
        return {
            "acquisition_drive": float(d.acquisition_drive),
            "noise_sigma":       float(d.noise_sigma),
            "pivot_alpha":       float(d.pivot_alpha),
            "corpus_saturation": float(d.corpus_saturation),
            "self_train_quality": float(getattr(d, "self_train_quality", 0.5)),
        }
    except Exception as e:
        log.debug("_sense_learning_drive: %s", e)
        return {"acquisition_drive": 0.1, "noise_sigma": 0.0,
                "pivot_alpha": 1.0, "corpus_saturation": 0.0,
                "self_train_quality": 0.5}


def _sense_corpus() -> dict:
    """Return entity/edge/learning counts and per-SC-domain entity density."""
    out: dict = {"entity_count": 0, "edge_count": 0, "learning_count": 0,
                 "pending_learnings": 0, "sc_domain_density": {}}
    try:
        db = str(_local_db_path())
        with sqlite3.connect(db) as cn:
            out["entity_count"] = (
                cn.execute("SELECT COUNT(*) FROM corpus_entity").fetchone()[0] or 0)
            out["edge_count"] = (
                cn.execute("SELECT COUNT(*) FROM corpus_edge").fetchone()[0] or 0)
            out["learning_count"] = (
                cn.execute("SELECT COUNT(*) FROM learning_log").fetchone()[0] or 0)
            out["pending_learnings"] = (
                cn.execute(
                    "SELECT COUNT(*) FROM learning_log "
                    "WHERE logged_at > COALESCE("
                    "  (SELECT MAX(ran_at) FROM corpus_round_log), '1970-01-01')"
                ).fetchone()[0] or 0)
            # Per-domain density — count corpus entities whose name/kind hints at
            # each SC scope. Heuristic keyword match is intentionally loose.
            _kw = {
                "inventory_sizing": ("part", "sku", "stock", "eoq", "inventory"),
                "fulfillment":      ("otd", "order", "delivery", "fulfil"),
                "sourcing":         ("supplier", "vendor", "procurement", "buyer"),
                "data_quality":     ("quality", "missing", "null", "error"),
                "lead_time":        ("lead", "transit", "cycle"),
                "demand_distortion":("bullwhip", "forecast", "demand", "distortion"),
                "network_position": ("echelon", "warehouse", "dc", "network"),
                "cycle_count":      ("cycle", "count", "abc", "inventory_count"),
            }
            for scope, keywords in _kw.items():
                like_clauses = " OR ".join(
                    f"LOWER(name) LIKE '%{kw}%' OR LOWER(kind) LIKE '%{kw}%'"
                    for kw in keywords
                )
                n = cn.execute(
                    f"SELECT COUNT(*) FROM corpus_entity WHERE {like_clauses}"
                ).fetchone()[0] or 0
                out["sc_domain_density"][scope] = n
    except Exception as e:
        log.debug("_sense_corpus: %s", e)
    return out


def _sense_llm_health() -> dict:
    out: dict = {"self_train_quality": 0.5, "self_train_stagnation": 0.0,
                 "rdt_mean_depth": 1.0, "diversity_collapsed_tasks": []}
    try:
        db = str(_local_db_path())
        with sqlite3.connect(db) as cn:
            # Self-train quality — recent avg_validator per task
            rows = cn.execute(
                "SELECT task, AVG(avg_validator) as q "
                "FROM llm_self_train_log "
                "WHERE ran_at > datetime('now', '-7 days') "
                "GROUP BY task"
            ).fetchall()
            if rows:
                qualities = [r[1] or 0 for r in rows]
                out["self_train_quality"] = sum(qualities) / len(qualities)
                out["self_train_stagnation"] = sum(
                    1 for q in qualities if q < 0.4) / len(qualities)

            # Diversity collapse — tasks where one model holds > 80% weight mass
            weight_rows = cn.execute(
                "SELECT task, model_id, weight FROM llm_weights"
            ).fetchall()
            task_weights: dict[str, dict[str, float]] = {}
            for task, model_id, w in weight_rows:
                task_weights.setdefault(task, {})[model_id] = float(w or 0)
            collapsed = []
            for task, wmap in task_weights.items():
                total = sum(wmap.values())
                if total > 0 and max(wmap.values()) / total > 0.80:
                    collapsed.append(task)
            out["diversity_collapsed_tasks"] = collapsed

            # RDT depth
            depth_rows = cn.execute(
                "SELECT AVG(depth) FROM recurrent_depth_log "
                "WHERE ran_at > datetime('now', '-3 days')"
            ).fetchone()
            if depth_rows and depth_rows[0]:
                out["rdt_mean_depth"] = float(depth_rows[0])
    except Exception as e:
        log.debug("_sense_llm_health: %s", e)
    return out


def _sense_missions() -> dict:
    out: dict = {"domains_without_missions": [], "high_friction_domains": []}
    try:
        from .findings_index import DB_PATH
        with sqlite3.connect(str(DB_PATH)) as cn:
            # Which SC scope tags have no open mission in the last 30 days?
            tagged_rows = cn.execute(
                "SELECT scope_tags_json FROM missions "
                "WHERE status IN ('open','running','refreshed') "
                "AND created_at > datetime('now', '-30 days')"
            ).fetchall()
            covered: set = set()
            for row in tagged_rows:
                try:
                    tags = json.loads(row[0] or "[]")
                    covered.update(tags)
                except Exception:
                    pass
            out["domains_without_missions"] = [s for s in _SC_SCOPES if s not in covered]

            # High friction domains — scope tags that appear most in recent findings
            friction_rows = cn.execute(
                "SELECT kind, COUNT(*) as n "
                "FROM findings "
                "WHERE created_at > datetime('now', '-7 days') "
                "GROUP BY kind ORDER BY n DESC LIMIT 10"
            ).fetchall()
            for row in friction_rows:
                kind_lower = (row[0] or "").lower()
                for scope in _SC_SCOPES:
                    if any(k in kind_lower for k in scope.split("_")):
                        if scope not in out["high_friction_domains"]:
                            out["high_friction_domains"].append(scope)
    except Exception as e:
        log.debug("_sense_missions: %s", e)
    return out


def _sense_torus() -> dict:
    out: dict = {"torus_gap_mean": 0.0, "torus_gap_by_dim": [],
                 "most_bunched_dims": []}
    try:
        db = str(_local_db_path())
        with sqlite3.connect(db) as cn:
            rows = cn.execute(
                "SELECT gap_json FROM torus_gap_readings "
                "ORDER BY recorded_at DESC LIMIT 1"
            ).fetchone()
            if rows:
                gaps = json.loads(rows[0] or "[]")
                if isinstance(gaps, list) and len(gaps) == 7:
                    out["torus_gap_by_dim"] = [float(g) for g in gaps]
                    out["torus_gap_mean"] = float(sum(gaps) / 7)
                    threshold = out["torus_gap_mean"] * 1.5
                    out["most_bunched_dims"] = [
                        i for i, g in enumerate(gaps) if g > threshold
                    ]
    except Exception as e:
        log.debug("_sense_torus (no torus_gap_readings table yet): %s", e)
    return out


def _sense_smell() -> dict:
    out: dict = {"carrier_mass": 1.0, "burst_rate": 0.0,
                 "dominant_scent": "baseline", "smell_certainty": 0.0}
    try:
        db = str(_local_db_path())
        with sqlite3.connect(db) as cn:
            row = cn.execute(
                "SELECT payload_json FROM smell_readings ORDER BY recorded_at DESC LIMIT 1"
            ).fetchone()
            if row:
                data = json.loads(row[0] or "{}")
                out["carrier_mass"]    = float(data.get("carrier_mass", 1.0))
                out["burst_rate"]      = float(data.get("burst_rate", 0.0))
                out["dominant_scent"]  = str(data.get("dominant_scent", "baseline"))
                out["smell_certainty"] = float(data.get("certainty_lb", 0.0))
    except Exception as e:
        log.debug("_sense_smell: %s", e)
    return out


def _sense_body() -> dict:
    out: dict = {"open_directive_count": 0, "stalled_directive_count": 0,
                 "body_feedback_ratio": 0.0}
    try:
        db = str(_local_db_path())
        threshold_dt = (
            datetime.now(tz=timezone.utc) - timedelta(hours=48)
        ).isoformat()
        with sqlite3.connect(db) as cn:
            out["open_directive_count"] = (
                cn.execute(
                    "SELECT COUNT(*) FROM body_directives "
                    "WHERE status IN ('open','in_progress')"
                ).fetchone()[0] or 0)
            out["stalled_directive_count"] = (
                cn.execute(
                    "SELECT COUNT(*) FROM body_directives "
                    "WHERE status='in_progress' AND surfaced_at < ?",
                    (threshold_dt,)
                ).fetchone()[0] or 0)
            total = cn.execute(
                "SELECT COUNT(*) FROM body_feedback").fetchone()[0] or 0
            resolved = cn.execute(
                "SELECT COUNT(*) FROM body_feedback "
                "WHERE status IN ('done','rejected')"
            ).fetchone()[0] or 0
            out["body_feedback_ratio"] = (resolved / total) if total > 0 else 0.0
    except Exception as e:
        log.debug("_sense_body: %s", e)
    return out


def _sense_dbi() -> dict:
    out: dict = {"findings_count": 0, "high_score_findings": 0,
                 "pages_with_no_findings": []}
    _PAGES = [
        "Supply Chain Brain", "EOQ", "OTD", "Procurement", "Bullwhip",
        "Lead Time", "Multi-Echelon", "Cycle Count", "Benchmarks",
    ]
    try:
        from .findings_index import DB_PATH
        with sqlite3.connect(str(DB_PATH)) as cn:
            out["findings_count"] = (
                cn.execute("SELECT COUNT(*) FROM findings").fetchone()[0] or 0)
            out["high_score_findings"] = (
                cn.execute(
                    "SELECT COUNT(*) FROM findings WHERE score >= 0.7"
                ).fetchone()[0] or 0)
            covered_pages = {
                row[0]
                for row in cn.execute("SELECT DISTINCT page FROM findings").fetchall()
            }
            out["pages_with_no_findings"] = [
                p for p in _PAGES
                if not any(p.lower() in cp.lower() for cp in covered_pages)
            ]
    except Exception as e:
        log.debug("_sense_dbi: %s", e)
    return out


def _build_state() -> SystemState:
    heart   = _sense_heart()
    drive   = _sense_learning_drive()
    corpus  = _sense_corpus()
    llm     = _sense_llm_health()
    missions= _sense_missions()
    torus   = _sense_torus()
    smell   = _sense_smell()
    body    = _sense_body()
    dbi     = _sense_dbi()

    return SystemState(
        coherence=heart["coherence"],
        rhythm_factor=heart["rhythm_factor"],
        weyl_centroid=heart["weyl_centroid"],
        self_train_quality=llm["self_train_quality"],
        self_train_stagnation=llm["self_train_stagnation"],
        rdt_mean_depth=llm["rdt_mean_depth"],
        diversity_collapsed_tasks=llm["diversity_collapsed_tasks"],
        entity_count=corpus["entity_count"],
        edge_count=corpus["edge_count"],
        learning_count=corpus["learning_count"],
        corpus_saturation=drive["corpus_saturation"],
        sc_domain_density=corpus["sc_domain_density"],
        pending_learnings=corpus["pending_learnings"],
        domains_without_missions=missions["domains_without_missions"],
        high_friction_domains=missions["high_friction_domains"],
        torus_gap_mean=torus["torus_gap_mean"],
        torus_gap_by_dim=torus["torus_gap_by_dim"],
        most_bunched_dims=torus["most_bunched_dims"],
        carrier_mass=smell["carrier_mass"],
        burst_rate=smell["burst_rate"],
        dominant_scent=smell["dominant_scent"],
        smell_certainty=smell["smell_certainty"],
        open_directive_count=body["open_directive_count"],
        stalled_directive_count=body["stalled_directive_count"],
        body_feedback_ratio=body["body_feedback_ratio"],
        findings_count=dbi["findings_count"],
        high_score_findings=dbi["high_score_findings"],
        pages_with_no_findings=dbi["pages_with_no_findings"],
        acquisition_drive=drive["acquisition_drive"],
        noise_sigma=drive["noise_sigma"],
        pivot_alpha=drive["pivot_alpha"],
    )


# ---------------------------------------------------------------------------
# Phase 2: DIAGNOSE — ten refinement strategies
# ---------------------------------------------------------------------------

def _strategy_mission_coverage(st: SystemState) -> list[RefinementAction]:
    """Launch a targeted Mission for each uncovered SC scope with friction."""
    actions = []
    for domain in st.domains_without_missions:
        friction_boost = 0.3 if domain in st.high_friction_domains else 0.0
        priority = min(1.0, 0.5 + friction_boost + 0.2 * st.acquisition_drive)
        actions.append(RefinementAction(
            strategy="mission_coverage",
            action_kind="mission",
            title=f"Launch '{domain}' mission — no active coverage",
            priority=priority,
            payload={"scope": domain,
                     "query": f"Analyze and improve {domain.replace('_', ' ')} "
                              f"across all sites",
                     "site": "ALL"},
        ))
    return actions


def _strategy_directive_aging(st: SystemState) -> list[RefinementAction]:
    """Re-surface stalled Body directives as escalated findings."""
    if st.stalled_directive_count == 0:
        return []
    priority = min(1.0, 0.4 + 0.3 * (st.stalled_directive_count / max(1, st.open_directive_count)))
    return [RefinementAction(
        strategy="directive_aging",
        action_kind="finding",
        title=f"{st.stalled_directive_count} directive(s) stalled >48 h — escalate to DBI",
        priority=priority,
        payload={"stalled_count": st.stalled_directive_count,
                 "kind": "stalled_directive_escalation",
                 "score": priority},
    )]


def _strategy_llm_diversity(st: SystemState) -> list[RefinementAction]:
    """Trigger diversity guard for tasks with collapsed model weights."""
    if not st.diversity_collapsed_tasks:
        return []
    priority = min(1.0, 0.55 + 0.1 * len(st.diversity_collapsed_tasks))
    return [RefinementAction(
        strategy="llm_diversity",
        action_kind="skill_trigger",
        title=f"LLM diversity collapsed on {len(st.diversity_collapsed_tasks)} task(s) — rebalance",
        priority=priority,
        payload={"tasks": st.diversity_collapsed_tasks,
                 "action": "apply_diversity_guard"},
    )]


def _strategy_corpus_seeding(st: SystemState) -> list[RefinementAction]:
    """Inject corpus seeds for SC domains with near-zero entity coverage."""
    actions = []
    for scope, count in st.sc_domain_density.items():
        if count < 5:
            density_gap = 1.0 - min(1.0, count / 20.0)
            priority = round(0.4 * density_gap + 0.3 * st.acquisition_drive, 3)
            if priority > 0.3:
                actions.append(RefinementAction(
                    strategy="corpus_seeding",
                    action_kind="corpus_seed",
                    title=f"Seed corpus for '{scope}' — only {count} entities",
                    priority=priority,
                    payload={"scope": scope, "seed_kind": scope.upper(),
                             "signal_strength": priority},
                ))
    return actions


def _strategy_self_train_health(st: SystemState) -> list[RefinementAction]:
    """Run a self-train round when quality is below threshold."""
    if st.self_train_quality >= 0.55 and st.self_train_stagnation < 0.4:
        return []
    priority = min(1.0, 0.6 + 0.4 * st.self_train_stagnation)
    return [RefinementAction(
        strategy="self_train_health",
        action_kind="skill_trigger",
        title=f"Self-train quality {st.self_train_quality:.2f} below threshold — run round",
        priority=priority,
        payload={"action": "self_train_round",
                 "stagnation": st.self_train_stagnation},
    )]


def _strategy_torus_pressure(st: SystemState) -> list[RefinementAction]:
    """Boost touch pressure on persistently bunched torus dimensions."""
    if not st.most_bunched_dims or st.torus_gap_mean < 0.2:
        return []
    priority = min(1.0, 0.35 + 0.5 * min(1.0, st.torus_gap_mean))
    return [RefinementAction(
        strategy="torus_pressure",
        action_kind="config_nudge",
        title=f"Torus dimensions {st.most_bunched_dims} bunched — amplify pressure",
        priority=priority,
        payload={"dims": st.most_bunched_dims,
                 "gap_mean": st.torus_gap_mean,
                 "kv_key": "torus_pressure_boost",
                 "kv_value": json.dumps({"dims": st.most_bunched_dims,
                                         "multiplier": 1.5})},
    )]


def _strategy_dbi_gap_filling(st: SystemState) -> list[RefinementAction]:
    """Launch targeted missions for pages with zero DBI findings coverage."""
    actions = []
    for page in st.pages_with_no_findings:
        priority = min(1.0, 0.4 + 0.2 * st.acquisition_drive)
        actions.append(RefinementAction(
            strategy="dbi_gap_filling",
            action_kind="mission",
            title=f"Page '{page}' has no DBI findings — seed with discovery mission",
            priority=priority,
            payload={"query": f"Surface key supply chain insights for {page}",
                     "site": "ALL",
                     "scope": "data_quality"},
        ))
    return actions


def _strategy_network_expansion(st: SystemState) -> list[RefinementAction]:
    """Trigger a network observation round when carrier mass is high and burst rate low."""
    if st.carrier_mass < 0.5 or st.burst_rate > 0.3:
        return []
    priority = round(0.3 + 0.4 * st.carrier_mass * (1.0 - st.burst_rate), 3)
    if priority < 0.35:
        return []
    return [RefinementAction(
        strategy="network_expansion",
        action_kind="skill_trigger",
        title="Carrier mass high, burst rate low — expand network observation",
        priority=priority,
        payload={"action": "observe_network_round",
                 "carrier_mass": st.carrier_mass},
    )]


def _strategy_quest_scope_expansion(st: SystemState) -> list[RefinementAction]:
    """Detect emerging SC topics in recent learnings not yet in SCOPE_TAGS."""
    actions = []
    try:
        db = str(_local_db_path())
        with sqlite3.connect(db) as cn:
            rows = cn.execute(
                "SELECT title, kind FROM learning_log "
                "ORDER BY logged_at DESC LIMIT 200"
            ).fetchall()
        emerging: dict[str, int] = {}
        _EMERGING_KW = {
            "supplier_risk":    ("supplier risk", "supply risk", "dual source"),
            "carbon_footprint": ("carbon", "co2", "sustainability", "emissions"),
            "supplier_diversification": ("single source", "sole source", "diversif"),
            "demand_sensing":   ("pos data", "point of sale", "demand signal"),
            "total_cost":       ("total cost", "landed cost", "tco"),
        }
        for title, kind in rows:
            text = ((title or "") + " " + (kind or "")).lower()
            for topic, keywords in _EMERGING_KW.items():
                if topic not in _SC_SCOPES and any(kw in text for kw in keywords):
                    emerging[topic] = emerging.get(topic, 0) + 1
        for topic, count in emerging.items():
            if count >= 3:
                priority = min(1.0, 0.4 + 0.05 * count)
                actions.append(RefinementAction(
                    strategy="quest_scope_expansion",
                    action_kind="directive",
                    title=f"Emerging SC topic '{topic}' seen {count}× — propose quest expansion",
                    priority=priority,
                    payload={"topic": topic, "signal_count": count,
                             "directive_text":
                                 f"Consider adding '{topic}' as a new supply-chain Quest scope. "
                                 f"Seen {count} times in recent learnings."},
                ))
    except Exception as e:
        log.debug("_strategy_quest_scope_expansion: %s", e)
    return actions


def _strategy_systemic_coherence(st: SystemState) -> list[RefinementAction]:
    """Trigger a full temporal-spatiality step when coherence drops below 0.35."""
    if st.coherence >= 0.35:
        return []
    priority = min(1.0, 0.6 + 0.4 * (1.0 - st.coherence))
    return [RefinementAction(
        strategy="systemic_coherence",
        action_kind="skill_trigger",
        title=f"System coherence {st.coherence:.2f} below 0.35 — force temporal step",
        priority=priority,
        payload={"action": "temporal_step", "coherence": st.coherence},
    )]


_STRATEGIES: list[Callable[[SystemState], list[RefinementAction]]] = [
    _strategy_mission_coverage,
    _strategy_directive_aging,
    _strategy_llm_diversity,
    _strategy_corpus_seeding,
    _strategy_self_train_health,
    _strategy_torus_pressure,
    _strategy_dbi_gap_filling,
    _strategy_network_expansion,
    _strategy_quest_scope_expansion,
    _strategy_systemic_coherence,
]


# ---------------------------------------------------------------------------
# Phase 3: RANK
# ---------------------------------------------------------------------------

def _rank_actions(candidates: list[RefinementAction],
                  st: SystemState) -> list[RefinementAction]:
    def _score(a: RefinementAction) -> float:
        return a.priority * max(0.1, st.acquisition_drive) * max(0.5, st.rhythm_factor)
    return sorted(candidates, key=_score, reverse=True)


# ---------------------------------------------------------------------------
# Phase 4: EXECUTE — one executor per action_kind
# ---------------------------------------------------------------------------

def _exec_mission(action: RefinementAction) -> dict:
    try:
        from .mission_runner import launch
        result = launch(
            user_query=action.payload.get("query", action.title),
            site=action.payload.get("site", "ALL"),
        )
        return {"ok": True, "mission_id": result.get("id", "?") if isinstance(result, dict) else str(result)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _exec_directive(action: RefinementAction) -> dict:
    try:
        from .brain_body_signals import surface_effective_signals
        result = surface_effective_signals(top_k=1)
        return {"ok": True, "directives_surfaced": result.get("directives_surfaced", 0),
                "proposed": action.payload.get("directive_text", "")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _exec_skill_trigger(action: RefinementAction) -> dict:
    skill_action = action.payload.get("action", "")
    try:
        if skill_action == "apply_diversity_guard":
            from .llm_self_train import apply_diversity_guard
            result = apply_diversity_guard()
            return {"ok": True, "result": result}
        elif skill_action == "self_train_round":
            from .llm_self_train import self_train_round
            result = self_train_round()
            return {"ok": True, "result": result}
        elif skill_action == "observe_network_round":
            from .network_learner import observe_network_round
            result = observe_network_round()
            return {"ok": True, "result": result}
        elif skill_action == "temporal_step":
            from .temporal_spatiality import temporal_step
            result = temporal_step(force=True)
            return {"ok": True, "result": result}
        else:
            # Drop a bridge trigger file for the integrated_skill_acquirer
            trigger_dir = _PIPELINE_ROOT / "bridge_triggers"
            trigger_dir.mkdir(parents=True, exist_ok=True)
            fname = trigger_dir / f"acquire_{skill_action}_{int(time.time())}.trigger"
            fname.write_text(json.dumps(action.payload, default=str))
            return {"ok": True, "trigger_file": str(fname)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _exec_corpus_seed(action: RefinementAction) -> dict:
    try:
        db = str(_local_db_path())
        with sqlite3.connect(db) as cn:
            cn.execute(
                "INSERT OR IGNORE INTO learning_log "
                "(kind, title, signal_strength, logged_at) VALUES (?,?,?,?)",
                (action.payload.get("seed_kind", "SEED"),
                 action.title,
                 float(action.payload.get("signal_strength", 0.5)),
                 datetime.now(tz=timezone.utc).isoformat()),
            )
        return {"ok": True, "scope": action.payload.get("scope")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _exec_config_nudge(action: RefinementAction) -> dict:
    try:
        key   = action.payload.get("kv_key", "")
        value = action.payload.get("kv_value", "")
        if not key:
            return {"ok": False, "error": "no kv_key in payload"}
        db = str(_local_db_path())
        with sqlite3.connect(db) as cn:
            cn.execute(
                "INSERT OR REPLACE INTO brain_kv (key, value, updated_at) "
                "VALUES (?, ?, ?)",
                (key, str(value), datetime.now(tz=timezone.utc).isoformat()),
            )
        return {"ok": True, "key": key}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _exec_finding(action: RefinementAction) -> dict:
    try:
        from .findings_index import record_findings_bulk
        record_findings_bulk([{
            "page":         "Systemic Refinement",
            "kind":         action.payload.get("kind", "systemic_refinement"),
            "key":          action.content_hash,
            "score":        float(action.payload.get("score", action.priority)),
            "payload_json": json.dumps(action.payload, default=str),
        }], mission_id=None)
        return {"ok": True}
    except Exception as e:
        # Graceful fallback: direct insert
        try:
            from .findings_index import DB_PATH
            with sqlite3.connect(str(DB_PATH)) as cn:
                cn.execute(
                    "INSERT INTO findings(page,kind,key,score,payload_json,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    ("Systemic Refinement",
                     action.payload.get("kind", "systemic_refinement"),
                     action.content_hash,
                     float(action.payload.get("score", action.priority)),
                     json.dumps(action.payload, default=str),
                     datetime.now(tz=timezone.utc).isoformat()),
                )
        except Exception as e2:
            return {"ok": False, "error": str(e2)}
        return {"ok": True}


_EXECUTORS: dict[str, Callable[[RefinementAction], dict]] = {
    "mission":       _exec_mission,
    "directive":     _exec_directive,
    "skill_trigger": _exec_skill_trigger,
    "corpus_seed":   _exec_corpus_seed,
    "config_nudge":  _exec_config_nudge,
    "finding":       _exec_finding,
}


# ---------------------------------------------------------------------------
# Phase 5: LEARN — dedup + persist
# ---------------------------------------------------------------------------

def _is_dedup(content_hash: str) -> bool:
    """Return True if this hash was executed successfully within _DEDUP_HOURS."""
    try:
        cutoff = (
            datetime.now(tz=timezone.utc) - timedelta(hours=_DEDUP_HOURS)
        ).isoformat()
        with _conn() as cn:
            row = cn.execute(
                "SELECT id FROM refinement_actions_log "
                "WHERE content_hash=? AND executed=1 AND executed_at > ?",
                (content_hash, cutoff),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _persist_cycle(state: SystemState,
                   executed: list[tuple[RefinementAction, dict]],
                   skipped:  list[RefinementAction],
                   elapsed_ms: int,
                   notes: list[str]) -> int:
    """Write cycle summary to systemic_refinement_log; return cycle_id."""
    _init_schema()
    now = datetime.now(tz=timezone.utc).isoformat()
    with _conn() as cn:
        cur = cn.execute(
            "INSERT INTO systemic_refinement_log "
            "(ran_at, cycle_duration_ms, state_json, actions_json, skipped_json, notes) "
            "VALUES (?,?,?,?,?,?)",
            (now, elapsed_ms,
             json.dumps(state.as_dict(), default=str),
             json.dumps([{"strategy": a.strategy, "kind": a.action_kind,
                          "title": a.title, "priority": round(a.priority, 3),
                          "outcome": o} for a, o in executed], default=str),
             json.dumps([{"strategy": a.strategy, "title": a.title,
                          "reason": "dedup" if _is_dedup(a.content_hash) else "rank"}
                         for a in skipped], default=str),
             "\n".join(notes)),
        )
        cycle_id = cur.lastrowid
        now_exec = datetime.now(tz=timezone.utc).isoformat()
        for action, outcome in executed:
            cn.execute(
                "INSERT INTO refinement_actions_log "
                "(cycle_id,strategy,priority,action_kind,title,content_hash,"
                " payload_json,executed,outcome_json,executed_at) "
                "VALUES(?,?,?,?,?,?,?,1,?,?)",
                (cycle_id, action.strategy, round(action.priority, 4),
                 action.action_kind, action.title, action.content_hash,
                 json.dumps(action.payload, default=str),
                 json.dumps(outcome, default=str), now_exec),
            )
        for action in skipped:
            cn.execute(
                "INSERT INTO refinement_actions_log "
                "(cycle_id,strategy,priority,action_kind,title,content_hash,"
                " payload_json,executed) VALUES(?,?,?,?,?,?,?,0)",
                (cycle_id, action.strategy, round(action.priority, 4),
                 action.action_kind, action.title, action.content_hash,
                 json.dumps(action.payload, default=str)),
            )
    return cycle_id or 0


# ---------------------------------------------------------------------------
# Public API — main cycle
# ---------------------------------------------------------------------------

def run_refinement_cycle() -> RefinementReport:
    """Execute one full sense→diagnose→rank→execute→learn cycle.

    Thread-safe. Rate-limited to _CYCLE_MIN_SEC between successful cycles.
    Returns a RefinementReport describing what happened.
    """
    global _LAST_CYCLE_TS
    _init_schema()

    with _CYCLE_LOCK:
        now = time.monotonic()
        if now - _LAST_CYCLE_TS < _CYCLE_MIN_SEC:
            # Return a minimal stub rather than blocking
            return RefinementReport(
                cycle_id=0,
                started_at=datetime.now(tz=timezone.utc).isoformat(),
                elapsed_ms=0,
                state=SystemState(),
                notes=["rate-limited: skipped"],
            )
        _LAST_CYCLE_TS = now

    t0 = time.monotonic()
    started_at = datetime.now(tz=timezone.utc).isoformat()
    notes: list[str] = []

    log.info("[SRA] Refinement cycle starting")

    # ── 1. SENSE ─────────────────────────────────────────────────────────
    try:
        state = _build_state()
        notes.append(
            f"state: entities={state.entity_count} edges={state.edge_count} "
            f"coherence={state.coherence:.2f} drive={state.acquisition_drive:.2f}"
        )
    except Exception as e:
        log.warning("[SRA] _build_state failed: %s", e)
        state = SystemState()
        notes.append(f"state build failed: {e}")

    # ── 2. DIAGNOSE ───────────────────────────────────────────────────────
    candidates: list[RefinementAction] = []
    for strategy_fn in _STRATEGIES:
        try:
            actions = strategy_fn(state)
            candidates.extend(actions)
        except Exception as e:
            log.debug("[SRA] strategy %s failed: %s", strategy_fn.__name__, e)

    notes.append(f"strategies produced {len(candidates)} candidate action(s)")

    # ── 3. RANK ───────────────────────────────────────────────────────────
    ranked = _rank_actions(candidates, state)

    # ── 4. EXECUTE ────────────────────────────────────────────────────────
    executed: list[tuple[RefinementAction, dict]] = []
    skipped:  list[RefinementAction]              = []

    for action in ranked:
        if len(executed) >= _MAX_ACTIONS_PER_CYCLE:
            skipped.append(action)
            continue
        if _is_dedup(action.content_hash):
            skipped.append(action)
            log.debug("[SRA] dedup skip: %s", action.title)
            continue
        executor = _EXECUTORS.get(action.action_kind)
        if executor is None:
            skipped.append(action)
            continue
        try:
            outcome = executor(action)
            executed.append((action, outcome))
            log.info("[SRA] executed [%s] '%s' → ok=%s",
                     action.action_kind, action.title, outcome.get("ok"))
        except Exception as e:
            executed.append((action, {"ok": False, "error": str(e)}))
            log.warning("[SRA] executor failed for '%s': %s", action.title, e)

    notes.append(
        f"executed={len(executed)} skipped={len(skipped)} "
        f"max_per_cycle={_MAX_ACTIONS_PER_CYCLE}"
    )

    # ── 5. LEARN ──────────────────────────────────────────────────────────
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    cycle_id = 0
    try:
        cycle_id = _persist_cycle(state, executed, skipped, elapsed_ms, notes)
    except Exception as e:
        log.warning("[SRA] persist_cycle failed: %s", e)

    log.info("[SRA] Refinement cycle complete: cycle_id=%d elapsed=%d ms "
             "executed=%d skipped=%d",
             cycle_id, elapsed_ms, len(executed), len(skipped))

    return RefinementReport(
        cycle_id=cycle_id,
        started_at=started_at,
        elapsed_ms=elapsed_ms,
        state=state,
        executed_actions=[
            {"strategy": a.strategy, "kind": a.action_kind, "title": a.title,
             "priority": round(a.priority, 3), "outcome": o}
            for a, o in executed
        ],
        skipped_actions=[
            {"strategy": a.strategy, "title": a.title, "priority": round(a.priority, 3)}
            for a in skipped
        ],
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Adaptive background scheduling
# ---------------------------------------------------------------------------

def _adaptive_interval(base_s: float = 1200.0) -> float:
    """Compute next sleep interval using acquisition_drive.

    High drive (Brain hungry / stagnant) → shorter interval.
    Low drive (Brain learning well) → longer interval.
    Clamped to [1200, 7200] s (20 min – 2 h).
    """
    try:
        from .learning_drive import get_drive
        drive = get_drive().acquisition_drive
    except Exception:
        drive = 0.15
    # interval = base / max(0.1, drive)   clipped to [1200, 7200]
    raw = base_s / max(0.10, drive)
    return max(1200.0, min(7200.0, raw))


def _refinement_worker(base_interval_s: float) -> None:
    log.info("[SRA] Refinement worker thread started (base_interval=%ds)", int(base_interval_s))
    while not _STOP_EVENT.is_set():
        try:
            run_refinement_cycle()
        except Exception as e:
            log.warning("[SRA] Unhandled exception in refinement cycle: %s", e)
        interval = _adaptive_interval(base_interval_s)
        log.info("[SRA] Next refinement cycle in %.0f s", interval)
        # Interruptible sleep using small ticks
        ticks = int(interval / 30)
        for _ in range(max(1, ticks)):
            if _STOP_EVENT.is_set():
                break
            time.sleep(30)
    log.info("[SRA] Refinement worker thread stopping")


def schedule_in_background(interval_s: float = 1200.0) -> threading.Thread:
    """Start the refinement agent as a daemon thread.

    Safe to call multiple times — each call starts a new thread but the
    cycle lock prevents concurrent execution.
    """
    t = threading.Thread(
        target=_refinement_worker,
        args=(interval_s,),
        name="systemic-refinement-agent",
        daemon=True,
    )
    t.start()
    return t


def stop_background_agent() -> None:
    """Signal the background worker to stop after its current sleep tick."""
    _STOP_EVENT.set()


# ---------------------------------------------------------------------------
# Audit readers
# ---------------------------------------------------------------------------

def recent_refinements(limit: int = 50) -> list[dict]:
    """Return the most recent refinement cycle summaries."""
    _init_schema()
    try:
        with _conn() as cn:
            rows = cn.execute(
                "SELECT id, ran_at, cycle_duration_ms, actions_json, notes "
                "FROM systemic_refinement_log "
                "ORDER BY ran_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            {"id": r["id"], "ran_at": r["ran_at"],
             "elapsed_ms": r["cycle_duration_ms"],
             "actions": json.loads(r["actions_json"] or "[]"),
             "notes": r["notes"]}
            for r in rows
        ]
    except Exception as e:
        log.debug("recent_refinements: %s", e)
        return []


def refinement_summary() -> dict:
    """Return aggregate health metrics for the Systemic Refinement Agent."""
    _init_schema()
    out: dict = {
        "total_cycles": 0,
        "total_actions_executed": 0,
        "actions_by_strategy": {},
        "actions_by_kind": {},
        "last_cycle_at": None,
        "last_cycle_elapsed_ms": None,
    }
    try:
        with _conn() as cn:
            out["total_cycles"] = (
                cn.execute("SELECT COUNT(*) FROM systemic_refinement_log").fetchone()[0] or 0)
            out["total_actions_executed"] = (
                cn.execute(
                    "SELECT COUNT(*) FROM refinement_actions_log WHERE executed=1"
                ).fetchone()[0] or 0)
            by_strategy = cn.execute(
                "SELECT strategy, COUNT(*) FROM refinement_actions_log "
                "WHERE executed=1 GROUP BY strategy ORDER BY COUNT(*) DESC"
            ).fetchall()
            out["actions_by_strategy"] = {r[0]: r[1] for r in by_strategy}
            by_kind = cn.execute(
                "SELECT action_kind, COUNT(*) FROM refinement_actions_log "
                "WHERE executed=1 GROUP BY action_kind ORDER BY COUNT(*) DESC"
            ).fetchall()
            out["actions_by_kind"] = {r[0]: r[1] for r in by_kind}
            last = cn.execute(
                "SELECT ran_at, cycle_duration_ms FROM systemic_refinement_log "
                "ORDER BY ran_at DESC LIMIT 1"
            ).fetchone()
            if last:
                out["last_cycle_at"]         = last["ran_at"]
                out["last_cycle_elapsed_ms"] = last["cycle_duration_ms"]
    except Exception as e:
        log.debug("refinement_summary: %s", e)
    return out
