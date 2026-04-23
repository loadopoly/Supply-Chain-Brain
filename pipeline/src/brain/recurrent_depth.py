"""Recurrent Depth Transformer — adaptive-depth refinement of ensemble votes.

The Brain's one-shot aggregators (`weighted_softmax_vote`, `weighted_mean`,
`json_merge`) collapse K contributors into a single answer in a single pass.
For tasks where contributors disagree but a clear majority can be sharpened
out with a few iterations of mutual-attention reasoning, a one-shot vote
leaves signal on the table.

This module adds a **Recurrent Depth Transformer** (RDT) aggregator inspired
by Geiping et al. 2025 ("Scaling up Test-Time Compute with Latent Reasoning")
adapted to the discrete vote-distribution simplex used by the ensemble. The
core idea: a single set of learned parameters runs recurrently for a variable
number of steps, with adaptive halting based on convergence of the latent
state.

Algorithm
---------
State: ``p_t`` — probability distribution over vote buckets (the labels).

Each iteration applies the same weight-tied recurrent block:

    1. Compute per-contributor attention:
            agree_i = p_t[label_i]                     (current belief in i's vote)
            score_i = w_i * conf_i * (1 + lambda*agree_i)
            a       = softmax(score / temp_attn)        (attention over contributors)
    2. Re-aggregate to candidate distribution:
            q[k] = sum_i  a_i * conf_i  if label_i==k else 0
    3. Temperature sharpening (decays per step):
            q   = softmax(log(q + eps) / temp_t)
    4. Residual blend:
            p_{t+1} = (1 - alpha) * p_t + alpha * q
    5. Adaptive halt:
            if KL(p_{t+1} || p_t) < epsilon  OR  depth >= max_depth: stop

The halting criterion makes the depth *task-adaptive*: easy unanimity-like
votes converge in one step; close-call multi-modal votes naturally use more
depth. Per-call depth and the full KL trace are persisted to
``recurrent_depth_log`` so the Brain learns the optimal ``max_depth`` per
task over time.

Design parallels to the prior bounded-learning modules
------------------------------------------------------
* Effect-bounded — RDT only adjusts the *aggregation* of the same K outputs.
  It never re-weights `llm_weights` and never spends extra API budget. The
  ensemble's reasoning fluidity is preserved.
* Auditable — every recurrent run logs depth, final KL, and full trace.
* Pluggable — registers as a new entry in ``llm_ensemble._AGGREGATORS``
  named ``recurrent_depth_vote``; flip ``aggregator_default`` in
  ``brain.yaml`` to enable it globally.

Public API
----------
    rdt_aggregate(outcomes, cfg) -> dict          # the aggregator function
    register_with_ensemble() -> None              # plugs into _AGGREGATORS
    init_schema() -> None                         # creates recurrent_depth_log
    recent_runs(limit=20, task=None) -> list      # audit reader
    learned_depth_summary() -> dict               # per-task adaptive depth stats
"""
from __future__ import annotations

import json
import math
import sqlite3
import threading
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from . import load_config
from .local_store import db_path as _local_db_path


_RDT_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def _cfg() -> dict:
    """Return the recurrent_depth config block.

    Looks under ``llms.ensemble.aggregators.recurrent_depth_vote`` first
    (since the aggregators block is the natural home), then falls back
    to ``llms.recurrent_depth`` for backward-compat / standalone tuning.
    """
    llms = (load_config().get("llms") or {})
    in_ensemble = (((llms.get("ensemble") or {}).get("aggregators") or {})
                   .get("recurrent_depth_vote") or {})
    standalone = (llms.get("recurrent_depth") or {})
    merged = {**standalone, **in_ensemble}  # ensemble block wins
    return merged


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
            CREATE TABLE IF NOT EXISTS recurrent_depth_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ran_at          TEXT NOT NULL,
                task            TEXT,
                contributors    INTEGER NOT NULL,
                halted_at_depth INTEGER NOT NULL,
                max_depth       INTEGER NOT NULL,
                final_kl        REAL NOT NULL,
                converged       INTEGER NOT NULL,        -- 1 if KL<eps, 0 if cap
                winner_label    TEXT,
                winner_prob     REAL,
                shift_from_oneshot REAL,                 -- L1 distance vs t=0
                kl_trace_json   TEXT NOT NULL,
                evidence_json   TEXT
            );
            CREATE INDEX IF NOT EXISTS ix_rdt_task_time
              ON recurrent_depth_log(task, ran_at);
            """
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _vote_key(resp: Any) -> str:
    if isinstance(resp, dict):
        return str(resp.get("label") or resp.get("text")
                   or json.dumps(resp, sort_keys=True))
    return str(resp)


def _extract_conf(resp: Any) -> float:
    if isinstance(resp, dict):
        try:
            return float(resp.get("confidence", 0.5))
        except Exception:
            return 0.5
    return 0.5


def _softmax(xs: list[float], temperature: float) -> list[float]:
    if not xs:
        return []
    t = max(float(temperature), 1e-6)
    m = max(xs)
    exps = [math.exp((x - m) / t) for x in xs]
    s = sum(exps) or 1.0
    return [e / s for e in exps]


def _kl(p: list[float], q: list[float], eps: float = 1e-9) -> float:
    """KL(p || q) with smoothing."""
    out = 0.0
    for pi, qi in zip(p, q):
        if pi <= 0:
            continue
        out += pi * math.log((pi + eps) / (qi + eps))
    return max(0.0, out)


def _l1(p: list[float], q: list[float]) -> float:
    return sum(abs(a - b) for a, b in zip(p, q))


# ---------------------------------------------------------------------------
# Latent state representation
# ---------------------------------------------------------------------------
@dataclass
class _Contributor:
    label:        str
    confidence:   float
    weight:       float          # learned w_i (from llm_weights)
    router_score: float
    raw:          Any            # original response for the winner samples dict


def _ingest_outcomes(outcomes: list) -> tuple[list[str], list[_Contributor]]:
    """Convert WorkerOutcome list into (bucket_labels, contributor records)."""
    buckets: list[str] = []
    seen_labels: set[str] = set()
    contribs: list[_Contributor] = []
    for o in outcomes:
        if not getattr(o, "ok", False) or o.response is None:
            continue
        lbl = _vote_key(o.response)
        if lbl not in seen_labels:
            seen_labels.add(lbl)
            buckets.append(lbl)
        contribs.append(_Contributor(
            label=lbl,
            confidence=float(_extract_conf(o.response)),
            weight=float(getattr(o, "weight", 1.0)),
            router_score=max(float(getattr(o, "router_score", 0.0)), 0.01),
            raw=o.response,
        ))
    return buckets, contribs


def _initial_distribution(buckets: list[str],
                          contribs: list[_Contributor]) -> list[float]:
    """One-shot weighted vote — this is the t=0 latent."""
    mass: dict[str, float] = {b: 0.0 for b in buckets}
    for c in contribs:
        mass[c.label] += max(0.0, c.weight * c.router_score) * max(c.confidence, 1e-3)
    total = sum(mass.values()) or 1.0
    return [mass[b] / total for b in buckets]


# ---------------------------------------------------------------------------
# Recurrent block (weight-tied, applied per step)
# ---------------------------------------------------------------------------
def _recurrent_step(p_prev: list[float],
                    buckets: list[str],
                    contribs: list[_Contributor],
                    *,
                    temp_attn:    float,
                    temp_state:   float,
                    alpha:        float,
                    agree_lambda: float) -> list[float]:
    """One iteration of the weight-tied recurrent block.

    Mathematically a soft-attention pooling across contributors, where the
    query is the current latent belief ``p_prev`` and each contributor's
    key/value is weighted by its agreement with that belief. Applied
    repeatedly with the same params (true weight-tying), it sharpens the
    distribution toward a coalition winner.
    """
    p_lookup = {b: pi for b, pi in zip(buckets, p_prev)}

    # 1) attention scores over contributors
    raw_scores: list[float] = []
    for c in contribs:
        agree = p_lookup.get(c.label, 0.0)
        score = c.weight * c.confidence * (1.0 + agree_lambda * agree)
        raw_scores.append(score)
    a = _softmax(raw_scores, temp_attn)

    # 2) re-aggregate into candidate distribution q
    q_mass: dict[str, float] = {b: 0.0 for b in buckets}
    for c, ai in zip(contribs, a):
        q_mass[c.label] += ai * c.confidence
    q_total = sum(q_mass.values()) or 1.0
    q = [q_mass[b] / q_total for b in buckets]

    # 3) temperature sharpening
    log_q = [math.log(qi + 1e-9) for qi in q]
    q = _softmax(log_q, temp_state)

    # 4) residual blend with previous latent
    p_next = [(1.0 - alpha) * p_prev[i] + alpha * q[i] for i in range(len(buckets))]
    s = sum(p_next) or 1.0
    return [pi / s for pi in p_next]


# ---------------------------------------------------------------------------
# Public aggregator
# ---------------------------------------------------------------------------
def rdt_aggregate(outcomes: list, cfg: dict | None = None) -> Any:
    """Aggregate a fanout's WorkerOutcomes via recurrent-depth refinement.

    Compatible with the ``llm_ensemble._AGGREGATORS`` callable signature so
    it can be registered alongside ``weighted_softmax_vote`` and friends.
    Returns a dict with the same shape as the softmax-vote aggregator
    plus ``rdt_meta`` with the depth telemetry.
    """
    cfg = {**(_cfg() or {}), **(cfg or {})}
    if not cfg.get("enabled", True):
        return None

    max_depth   = int(cfg.get("max_depth", 8))
    min_depth   = int(cfg.get("min_depth", 1))
    eps         = float(cfg.get("kl_epsilon", 1e-3))
    temp_attn   = float(cfg.get("temp_attention", 0.5))
    temp_start  = float(cfg.get("temp_state_start", 1.0))
    temp_end    = float(cfg.get("temp_state_end", 0.35))
    alpha       = float(cfg.get("residual_alpha", 0.6))
    agree_lambda = float(cfg.get("agreement_lambda", 1.5))

    buckets, contribs = _ingest_outcomes(outcomes)
    if not buckets or not contribs:
        return None

    # t = 0
    p = _initial_distribution(buckets, contribs)
    p_zero = list(p)
    kl_trace: list[float] = []
    converged = False
    halted_at = 0

    for step in range(1, max_depth + 1):
        # Anneal temperature linearly across depth
        if max_depth > 1:
            frac = (step - 1) / (max_depth - 1)
        else:
            frac = 1.0
        temp_state = temp_start + frac * (temp_end - temp_start)

        p_new = _recurrent_step(
            p, buckets, contribs,
            temp_attn=temp_attn, temp_state=temp_state,
            alpha=alpha, agree_lambda=agree_lambda,
        )
        kl = _kl(p_new, p)
        kl_trace.append(round(kl, 6))
        p = p_new
        halted_at = step
        if step >= min_depth and kl < eps:
            converged = True
            break

    # Pick winner from final distribution
    winner_idx = max(range(len(p)), key=lambda i: p[i])
    winner_label = buckets[winner_idx]
    winner_prob = float(p[winner_idx])

    # Find the original raw response for this winner (first contributor with that label)
    winner_raw: Any = None
    for c in contribs:
        if c.label == winner_label:
            winner_raw = c.raw
            break

    # Vote distribution in the same shape weighted_softmax_vote returns
    vote_distribution = {b: round(p[i], 4) for i, b in enumerate(buckets)}
    shift = _l1(p, p_zero)
    task_name = cfg.get("_task")  # injected by the dispatcher

    # Persist audit row
    try:
        _log_run(
            task=task_name,
            n_contribs=len(contribs),
            halted_at=halted_at,
            max_depth=max_depth,
            final_kl=kl_trace[-1] if kl_trace else 0.0,
            converged=converged,
            winner_label=winner_label,
            winner_prob=winner_prob,
            shift=shift,
            kl_trace=kl_trace,
            evidence={
                "buckets": buckets,
                "p_zero": [round(x, 4) for x in p_zero],
                "p_final": [round(x, 4) for x in p],
                "n_buckets": len(buckets),
            },
        )
    except Exception:
        pass  # never let logging break the dispatch

    return {
        "value": winner_raw,
        "ensemble_confidence": round(winner_prob, 4),
        "vote_distribution": vote_distribution,
        "rdt_meta": {
            "halted_at_depth": halted_at,
            "max_depth": max_depth,
            "converged": converged,
            "final_kl": kl_trace[-1] if kl_trace else 0.0,
            "shift_from_oneshot": round(shift, 4),
            "kl_trace": kl_trace,
        },
    }


# ---------------------------------------------------------------------------
# Audit logging
# ---------------------------------------------------------------------------
def _log_run(*, task, n_contribs, halted_at, max_depth, final_kl, converged,
             winner_label, winner_prob, shift, kl_trace, evidence) -> None:
    init_schema()
    with _conn() as cn:
        cn.execute(
            """INSERT INTO recurrent_depth_log
               (ran_at, task, contributors, halted_at_depth, max_depth,
                final_kl, converged, winner_label, winner_prob,
                shift_from_oneshot, kl_trace_json, evidence_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (datetime.now(timezone.utc).isoformat(),
             task, int(n_contribs), int(halted_at), int(max_depth),
             float(final_kl), 1 if converged else 0,
             winner_label, float(winner_prob), float(shift),
             json.dumps(kl_trace), json.dumps(evidence)),
        )


def recent_runs(limit: int = 20, task: str | None = None) -> list[dict]:
    init_schema()
    with _conn() as cn:
        if task:
            rows = cn.execute(
                """SELECT * FROM recurrent_depth_log
                    WHERE task=? ORDER BY id DESC LIMIT ?""",
                (task, int(limit)),
            ).fetchall()
        else:
            rows = cn.execute(
                "SELECT * FROM recurrent_depth_log ORDER BY id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
    return [dict(r) for r in rows]


def learned_depth_summary() -> dict:
    """Per-task adaptive depth statistics — feeds the Brain's understanding
    of how much latent reasoning each task actually needs."""
    init_schema()
    with _conn() as cn:
        rows = cn.execute(
            """SELECT task,
                      COUNT(*)                AS n,
                      AVG(halted_at_depth)    AS avg_depth,
                      MAX(halted_at_depth)    AS max_depth_seen,
                      AVG(CASE WHEN converged THEN 1.0 ELSE 0.0 END) AS p_converged,
                      AVG(shift_from_oneshot) AS avg_shift,
                      AVG(winner_prob)        AS avg_winner_prob
                 FROM recurrent_depth_log
                GROUP BY task
                ORDER BY n DESC"""
        ).fetchall()
    return {
        "by_task": [
            {
                "task": r["task"],
                "n": int(r["n"]),
                "avg_depth": round(float(r["avg_depth"] or 0), 2),
                "max_depth_seen": int(r["max_depth_seen"] or 0),
                "convergence_rate": round(float(r["p_converged"] or 0), 3),
                "avg_shift_from_oneshot": round(float(r["avg_shift"] or 0), 4),
                "avg_winner_prob": round(float(r["avg_winner_prob"] or 0), 4),
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# Registration shim — call once at import time from llm_ensemble
# ---------------------------------------------------------------------------
def register_with_ensemble() -> None:
    """Insert ``recurrent_depth_vote`` into llm_ensemble._AGGREGATORS.

    Deferred import to avoid a circular module load during package init.
    """
    try:
        from . import llm_ensemble
        llm_ensemble._AGGREGATORS.setdefault("recurrent_depth_vote", rdt_aggregate)
    except Exception:
        pass
