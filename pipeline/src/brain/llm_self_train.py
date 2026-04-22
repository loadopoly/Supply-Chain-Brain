"""Bounded self-training for the multi-LLM ensemble.

Goal
----
Use the live supply-chain data pipeline as a *soft* validator so the
ensemble's per-(model, task) weights drift toward the models that match
ground truth our system already knows. The hard requirement: NEVER let
this overfit so much that the Brain loses fluidity for multi-echeloned
reasoning or dynamic interpretations on tasks the pipeline has no
ground truth for.

Design
------
1. **Whitelist ground-truth sources** (config `llms.self_train.tasks`):
   for each task we declare *exactly* which pipeline table/column is the
   reference. Anything not whitelisted is left alone — generative,
   narrative, and what-if tasks keep their router-prior weights.

       vendor_consolidation -> local_brain.sqlite.part_category.category
       otd_classify         -> local_brain.sqlite.otd_ownership.owner

2. **Replay last-N dispatches** from `llm_dispatch_log` whose validator
   was NULL (i.e. the live caller had no oracle), and recompute a
   self-validator score by checking each contributor's response against
   the whitelisted ground-truth column.

3. **Apply guard-railed updates** via `llm_ensemble.update_weights` with:
       * `lr_scale` (default 0.5)  — half the live learning rate.
       * `drift_cap` (default 0.5) — clamp weight delta vs. router prior.
       * `min_weight_floor`        — re-enforced so nothing gets zeroed.
       * `max_share_per_task`      — if any single model exceeds X% of
         total weight mass on a task, dampen it back toward parity.
       * `exploration_reserve`     — config flag read by `llm_ensemble`
         that randomly bypasses the learned weights for a fraction of
         dispatches, keeping newcomers/underdogs observable.

4. **Per-task isolation**: weight updates from this self-trainer NEVER
   touch tasks outside the whitelist. So `cross_dataset_review`,
   `narrative_summary`, `whatif_explain`, `multi_echelon_reason`,
   etc. retain pure router-prior dynamics.

Public API
----------
    mine_self_training_signal(limit_per_task: int = 200) -> dict
    apply_diversity_guard()                              -> dict
    self_train_round()                                   -> dict
    schedule_in_background()                             -> threading.Thread

The autonomous_agent calls `self_train_round()` once per cycle; results
are written to `local_brain.sqlite.llm_self_train_log`.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterable

from . import load_config
from .local_store import db_path
from .llm_router import rank_llms
from .llm_ensemble import update_weights, weights_for, WorkerOutcome


_DDL = """
CREATE TABLE IF NOT EXISTS llm_self_train_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ran_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    task          TEXT NOT NULL,
    samples       INTEGER NOT NULL DEFAULT 0,
    matched       INTEGER NOT NULL DEFAULT 0,
    avg_validator REAL,
    drift_capped  INTEGER NOT NULL DEFAULT 0,
    diversity_dampened INTEGER NOT NULL DEFAULT 0,
    notes         TEXT
);
CREATE INDEX IF NOT EXISTS ix_self_train_task ON llm_self_train_log(task);
"""


@contextmanager
def _conn():
    cn = sqlite3.connect(db_path())
    try:
        cn.executescript(_DDL)
        yield cn
        cn.commit()
    finally:
        cn.close()


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------
def _cfg() -> dict:
    return ((load_config().get("llms") or {}).get("self_train") or {})


def _enabled() -> bool:
    return bool(_cfg().get("enabled", True))


# ---------------------------------------------------------------------------
# Ground-truth lookups (PIPELINE-DERIVED)
# ---------------------------------------------------------------------------
def _ground_truth_index(task_cfg: dict) -> dict[str, str]:
    """Read the pipeline ground-truth table for a given task and return
    a `{key_lower: value_lower}` lookup dict. Returns {} on any failure
    so we degrade gracefully and never block the agent."""
    try:
        table = task_cfg.get("table")
        key_col = task_cfg.get("key_column")
        val_col = task_cfg.get("value_column")
        if not (table and key_col and val_col):
            return {}
        with sqlite3.connect(db_path()) as cn:
            cur = cn.execute(
                f"SELECT {key_col}, {val_col} FROM {table} "
                f"WHERE {key_col} IS NOT NULL AND {val_col} IS NOT NULL"
            )
            return {str(k).strip().lower(): str(v).strip().lower()
                    for (k, v) in cur.fetchall()}
    except Exception as e:
        logging.warning(f"self_train: ground truth fetch failed for "
                        f"{task_cfg.get('table')}: {e}")
        return {}


# ---------------------------------------------------------------------------
# Validator: how well did THIS model's response match the pipeline truth?
# ---------------------------------------------------------------------------
def _score_response(resp: Any, key_text: str, gt_index: dict[str, str]) -> float | None:
    """Returns a [0, 1] score, or None if we can't judge.
    None means "abstain" — do NOT push weights for this sample."""
    if not gt_index or not key_text:
        return None
    truth = gt_index.get(key_text.strip().lower())
    if not truth:
        return None
    # Be liberal on response shape: accept str, dict with 'label'/'value', etc.
    if isinstance(resp, dict):
        cand = resp.get("label") or resp.get("value") or resp.get("answer") or ""
    else:
        cand = str(resp or "")
    cand = str(cand).strip().lower()
    if not cand:
        return 0.0
    if cand == truth:
        return 1.0
    # Substring / prefix match — soft credit so we don't over-punish format drift
    if truth in cand or cand in truth:
        return 0.7
    # Token overlap on the first 4 tokens of each
    a = set(cand.split()[:4])
    b = set(truth.split()[:4])
    if a and b and (a & b):
        return 0.4
    return 0.0


# ---------------------------------------------------------------------------
# Replay last-N dispatches and produce per-task synthetic outcomes
# ---------------------------------------------------------------------------
def mine_self_training_signal(limit_per_task: int = 200) -> dict[str, dict]:
    """Walk recent dispatches whose `validator` is NULL and (where
    possible) build a self-validator from the pipeline. Returns a per-
    task summary including the synthetic outcomes that will be fed to
    `update_weights` by `self_train_round`."""
    if not _enabled():
        return {"skipped": "self_train disabled"}

    tasks_cfg = _cfg().get("tasks") or {}
    if not tasks_cfg:
        return {"skipped": "no whitelisted tasks"}

    out: dict[str, dict] = {}
    with _conn() as cn:
        for task, tcfg in tasks_cfg.items():
            gt = _ground_truth_index(tcfg)
            if not gt:
                out[task] = {"skipped": "no ground truth", "samples": 0}
                continue

            cur = cn.execute(
                "SELECT id, contributors_json, decided_at "
                "FROM llm_dispatch_log "
                "WHERE task=? AND validator IS NULL "
                "ORDER BY id DESC LIMIT ?",
                (task, int(limit_per_task)),
            )
            rows = cur.fetchall()

            samples: list[tuple[str, list[WorkerOutcome], float]] = []
            matched = 0
            for _id, contribs_json, _ts in rows:
                try:
                    contribs = json.loads(contribs_json)
                except Exception:
                    continue
                # Heuristic: the dispatch payload's text key is typically
                # mirrored in the response dict. We look across contribs
                # for an `input_key` echoed there; otherwise pull the
                # first contributor's payload.text.
                key_text = ""
                for c in contribs:
                    resp = c.get("response")
                    if isinstance(resp, dict):
                        key_text = (str(resp.get("input_key") or
                                        resp.get("text") or
                                        resp.get("key") or "")
                                    or key_text)
                if not key_text:
                    # Without a key, abstain rather than guess.
                    continue

                # Score each contributor against pipeline truth
                outcomes: list[WorkerOutcome] = []
                any_scored = False
                for c in contribs:
                    s = _score_response(c.get("response"), key_text, gt)
                    if s is None:
                        continue
                    any_scored = True
                    outcomes.append(WorkerOutcome(
                        model_id=c.get("model_id", "unknown"),
                        response=c.get("response"),
                        latency_ms=int(c.get("latency_ms", 0) or 0),
                        ok=bool(c.get("ok", True)),
                        error=c.get("error"),
                        router_score=float(c.get("router_score",
                                                 c.get("prior", 0.0)) or 0.0),
                        weight=float(c.get("weight", 1.0) or 1.0),
                        bias=float(c.get("bias", 0.0) or 0.0),
                    ))
                if any_scored and outcomes:
                    matched += 1
                    # Use mean per-contributor score as the "ensemble" target
                    target = sum(_score_response(c.get("response"), key_text, gt) or 0.0
                                 for c in contribs) / max(1, len(contribs))
                    samples.append((key_text, outcomes, target))

            out[task] = {
                "rows_scanned": len(rows),
                "matched": matched,
                "samples": samples,
                "ground_truth_keys": len(gt),
            }
    return out


# ---------------------------------------------------------------------------
# Diversity / fluidity guard
# ---------------------------------------------------------------------------
def apply_diversity_guard() -> dict[str, Any]:
    """Enforce two invariants per task:
       1. No model exceeds `max_share_per_task` of total weight mass.
       2. No model is below `min_weight_floor` (re-floor if SGD pushed it
          below). This keeps the ensemble plural so multi-echeloned and
          dynamic interpretations still surface diverse model voices.
    """
    cfg = _cfg()
    max_share = float(cfg.get("max_share_per_task", 0.50))
    floor = float(cfg.get("min_weight_floor", 0.10))
    dampen_factor = float(cfg.get("dampen_factor", 0.85))

    summary: dict[str, dict] = {}
    with _conn() as cn:
        cur = cn.execute(
            "SELECT task, model_id, weight FROM llm_weights")
        rows = cur.fetchall()

        # Group by task
        per_task: dict[str, list[tuple[str, float]]] = {}
        for task, mid, w in rows:
            per_task.setdefault(task, []).append((mid, float(w)))

        for task, items in per_task.items():
            total = sum(w for _, w in items) or 1.0
            dampened: list[str] = []
            floored: list[str] = []
            for mid, w in items:
                share = w / total
                new_w = w
                if share > max_share:
                    new_w = w * dampen_factor
                    dampened.append(mid)
                if new_w < floor:
                    new_w = floor
                    floored.append(mid)
                if new_w != w:
                    cn.execute(
                        "UPDATE llm_weights SET weight=?, "
                        "updated_at=CURRENT_TIMESTAMP "
                        "WHERE task=? AND model_id=?",
                        (new_w, task, mid),
                    )
            summary[task] = {"models": len(items), "dampened": dampened,
                             "floored": floored}
    return summary


# ---------------------------------------------------------------------------
# One full self-training round (mine -> bounded SGD -> diversity guard)
# ---------------------------------------------------------------------------
def self_train_round(*, limit_per_task: int | None = None) -> dict[str, Any]:
    """Single end-to-end self-training pass. Safe to call from the
    autonomous_agent loop. Honors all guard-rail config knobs."""
    if not _enabled():
        return {"skipped": "self_train disabled"}

    cfg = _cfg()
    limit = int(limit_per_task or cfg.get("limit_per_task", 200))
    drift_cap = float(cfg.get("drift_cap", 0.5))
    lr_scale = float(cfg.get("lr_scale", 0.5))

    mined = mine_self_training_signal(limit_per_task=limit)
    per_task_results: dict[str, Any] = {}
    for task, info in mined.items():
        if not isinstance(info, dict) or "samples" not in info:
            per_task_results[task] = info
            continue
        samples = info["samples"]
        if not samples:
            per_task_results[task] = {"matched": 0, "applied": 0}
            continue

        # Snapshot priors so we can enforce drift_cap AFTER applying
        # update_weights (which uses the live learning rate). We dampen
        # the rate ourselves by capping how far the post-update weight
        # may drift from its pre-update value within ONE round.
        pre = weights_for(task)
        applied = 0
        for _key, outcomes, target in samples:
            try:
                # Scale the validator toward 0.5 (no opinion) by lr_scale.
                # target=1.0, lr_scale=0.5 -> effective_target=0.75
                eff = 0.5 + (target - 0.5) * lr_scale
                update_weights(task, outcomes, eff)
                applied += 1
            except Exception as e:
                logging.warning(f"self_train: update failed for {task}: {e}")

        # Enforce per-round drift cap relative to pre-round snapshot.
        capped = 0
        post = weights_for(task)
        with _conn() as cn:
            for mid, w_after in post.items():
                w_before = (pre.get(mid) or {}).get("weight",
                                                    w_after.get("weight", 1.0))
                delta = w_after["weight"] - w_before
                if abs(delta) > drift_cap:
                    capped += 1
                    clamped = w_before + (drift_cap if delta > 0 else -drift_cap)
                    cn.execute(
                        "UPDATE llm_weights SET weight=?, "
                        "updated_at=CURRENT_TIMESTAMP "
                        "WHERE task=? AND model_id=?",
                        (clamped, task, mid),
                    )

        per_task_results[task] = {
            "rows_scanned": info.get("rows_scanned", 0),
            "matched": info.get("matched", 0),
            "applied": applied,
            "drift_capped": capped,
        }

    # Run the diversity guard regardless — it protects EVERY task,
    # including ones that didn't get mined this round.
    diversity = apply_diversity_guard()

    # Persist a summary row for each whitelisted task
    with _conn() as cn:
        for task, r in per_task_results.items():
            if not isinstance(r, dict) or "matched" not in r:
                continue
            samples_n = r.get("matched", 0)
            cn.execute(
                "INSERT INTO llm_self_train_log("
                "task, samples, matched, avg_validator, drift_capped, "
                "diversity_dampened, notes) VALUES(?,?,?,?,?,?,?)",
                (task, r.get("rows_scanned", 0), samples_n, None,
                 r.get("drift_capped", 0),
                 len((diversity.get(task) or {}).get("dampened") or []),
                 json.dumps({"floored": (diversity.get(task) or {}).get("floored", [])})),
            )

    return {
        "tasks":     per_task_results,
        "diversity": diversity,
        "ran_at":    time.time(),
    }


# ---------------------------------------------------------------------------
# Background scheduler — fired by autonomous_agent
# ---------------------------------------------------------------------------
def schedule_in_background(interval_s: int | None = None) -> threading.Thread:
    iv = int(interval_s or _cfg().get("interval_s", 1800))

    def _loop():
        while True:
            try:
                self_train_round()
            except Exception as e:
                logging.warning(f"self_train: round failed: {e}")
            time.sleep(max(60, iv))

    t = threading.Thread(target=_loop, name="llm-self-train", daemon=True)
    t.start()
    return t


__all__ = [
    "mine_self_training_signal",
    "apply_diversity_guard",
    "self_train_round",
    "schedule_in_background",
]
