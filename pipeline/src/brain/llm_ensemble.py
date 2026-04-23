"""LLM Ensemble — parallel multi-model dispatch with learnable weights/biases.

The Brain treats each task answer as a learned linear combination of K free,
open-weight LLMs running in parallel. Conceptually:

    Answer(task) = aggregate_T( { (wᵢ·sᵢ + bᵢ) · responseᵢ  for i in fanout_K } )

where:
    sᵢ           = router score for model i on this task (capability prior)
    wᵢ, bᵢ       = persisted weight & bias for (model_i, task), updated online
    responseᵢ    = the actual model output (text / number / json)
    aggregate_T  = task-configured aggregator (softmax-vote, weighted-mean,
                   json-merge) chosen in config/brain.yaml -> llms.ensemble.

After every dispatch the Brain calls `update_weights(...)` with a validator
score in [0,1] (e.g. 1.0 if the answer matched ground truth, lower otherwise).
A single SGD step with weight-decay nudges each contributing (model, task)
weight & bias toward the success target. Latency and success EMAs are also
tracked so future dispatches can route around degraded models.

Concurrency is provided by `concurrent.futures.ThreadPoolExecutor` so HTTP
calls run truly in parallel; the dispatcher returns as soon as either every
worker finishes or `request_timeout_s` elapses (partial results aggregated).

Public API:
    dispatch_parallel(task, payload, *, validator=None) -> EnsembleResult
    update_weights(task, contributions, validator_score) -> None
    weights_for(task) -> dict[model_id, dict]
    set_caller(callable)              # plug in your real HTTP/SDK caller
"""
from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Iterable

from . import load_config
from .local_store import db_path
from .llm_router import rank_llms, Decision


# ---------------------------------------------------------------------------
# Persistence: per-(model, task) weight/bias + EMA telemetry
# ---------------------------------------------------------------------------
_DDL = """
CREATE TABLE IF NOT EXISTS llm_weights (
    task         TEXT NOT NULL,
    model_id     TEXT NOT NULL,
    weight       REAL NOT NULL DEFAULT 1.0,
    bias         REAL NOT NULL DEFAULT 0.0,
    n_obs        INTEGER NOT NULL DEFAULT 0,
    ema_success  REAL NOT NULL DEFAULT 0.5,
    ema_latency  REAL NOT NULL DEFAULT 0.0,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (task, model_id)
);
CREATE TABLE IF NOT EXISTS llm_dispatch_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    task         TEXT,
    fanout       INTEGER,
    elapsed_ms   INTEGER,
    aggregator   TEXT,
    contributors_json TEXT,
    validator    REAL,
    decided_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
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
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass
class WorkerOutcome:
    model_id: str
    response: Any
    latency_ms: int
    ok: bool
    error: str | None = None
    router_score: float = 0.0
    weight: float = 1.0
    bias: float = 0.0


@dataclass
class EnsembleResult:
    task: str
    answer: Any
    aggregator: str
    elapsed_ms: int
    contributors: list[WorkerOutcome] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "task": self.task,
            "answer": self.answer,
            "aggregator": self.aggregator,
            "elapsed_ms": self.elapsed_ms,
            "contributors": [asdict(c) for c in self.contributors],
            "skipped": self.skipped,
        }


# ---------------------------------------------------------------------------
# Pluggable model caller. Production wires this to whatever HTTP/SDK shim the
# pipeline uses. Default is an offline echo so unit tests & smoke runs work.
# ---------------------------------------------------------------------------
_CALLER_LOCK = threading.Lock()
_DEFAULT_CALLER: Callable[[Decision, Any, dict], Any] | None = None


def set_caller(fn: Callable[[Decision, Any, dict], Any]) -> None:
    """Register the function that actually invokes a model.
    Signature: fn(decision, payload, ensemble_cfg) -> response (any)."""
    global _DEFAULT_CALLER
    with _CALLER_LOCK:
        _DEFAULT_CALLER = fn


def _offline_caller(decision: Decision, payload: Any, _cfg: dict) -> Any:
    """Deterministic local stand-in so the Brain can self-test without network.
    Each model 'votes' a small variation of its router score so aggregators
    have something realistic to combine."""
    time.sleep(0.01 + (hash(decision.model_id) % 30) / 1000.0)
    if isinstance(payload, dict) and payload.get("kind") == "classify":
        labels = payload.get("labels") or ["A", "B", "C"]
        idx = (hash(decision.model_id) + hash(payload.get("text", ""))) % len(labels)
        return {"label": labels[idx], "confidence": min(1.0, 0.5 + decision.score / 2)}
    if isinstance(payload, dict) and payload.get("kind") == "score":
        return {"value": max(0.0, min(1.0, decision.score + (hash(decision.model_id) % 10) / 100)),
                "confidence": 0.7}
    return {"text": f"[{decision.model_id}] {payload}",
            "confidence": min(1.0, 0.5 + decision.score / 2)}


def _grid_dispatch_caller(decision: Decision, payload: Any, cfg: dict) -> Any:
    """Default caller used when no `set_caller` override is registered.
    Routes the model invocation through the shared compute grid (peer
    workstation CPU/GPU) and falls back to the local device on any failure.
    The remote compute_node executes the same offline caller for now;
    swap `_execute_locally` for the real model invocation in production."""
    from .compute_grid import pick_compute_target, submit_job
    target = pick_compute_target(job_hint={
        "needs_gpu": bool((cfg.get("ensemble") or {}).get("prefer_gpu", False)),
        "task_model": decision.model_id,
    })
    grid_payload = {
        "task":   getattr(decision, "_task", "default"),
        "model":  decision.model_id,
        "vendor": decision.vendor,
        "score":  decision.score,
        "body":   payload,
    }
    try:
        result = submit_job(target, grid_payload)
    except Exception:
        # Local safety net — never let a peer outage stall the ensemble.
        from .compute_grid import local, submit_job as _resubmit
        result = _resubmit(type(target)(peer=local(), reason="peer failed",
                                        fallback=True), grid_payload)
    return result.get("response", result)


# ---------------------------------------------------------------------------
# Weight bookkeeping
# ---------------------------------------------------------------------------
def _ensemble_cfg() -> dict:
    return ((load_config().get("llms") or {}).get("ensemble") or {})


def _load_weight(cur: sqlite3.Cursor, task: str, model_id: str,
                 prior: float) -> tuple[float, float]:
    cur.execute(
        "SELECT weight, bias FROM llm_weights WHERE task=? AND model_id=?",
        (task, model_id),
    )
    row = cur.fetchone()
    if row is not None:
        return float(row[0]), float(row[1])
    # Initialize from router prior, clipped to ensemble bounds.
    cfg = _ensemble_cfg()
    lo = float(cfg.get("learning", {}).get("min_weight", 0.05))
    hi = float(cfg.get("learning", {}).get("max_weight", 3.0))
    w0 = max(lo, min(hi, max(prior, 0.0) + 1.0))
    cur.execute(
        "INSERT OR IGNORE INTO llm_weights(task, model_id, weight, bias) "
        "VALUES(?, ?, ?, 0.0)",
        (task, model_id, w0),
    )
    return w0, 0.0


def weights_for(task: str) -> dict[str, dict[str, float]]:
    """Inspect the persisted weight matrix for `task` (used by UIs/tests)."""
    out: dict[str, dict[str, float]] = {}
    with _conn() as cn:
        for mid, w, b, n, succ, lat in cn.execute(
            "SELECT model_id, weight, bias, n_obs, ema_success, ema_latency "
            "FROM llm_weights WHERE task=?", (task,)
        ).fetchall():
            out[mid] = {"weight": w, "bias": b, "n_obs": n,
                        "ema_success": succ, "ema_latency": lat}
    return out


# ---------------------------------------------------------------------------
# Aggregators
# ---------------------------------------------------------------------------
def _softmax(xs: list[float], temperature: float) -> list[float]:
    if not xs:
        return []
    t = max(temperature, 1e-6)
    m = max(xs)
    exps = [math.exp((x - m) / t) for x in xs]
    s = sum(exps) or 1.0
    return [e / s for e in exps]


def _agg_weighted_softmax_vote(outcomes: list[WorkerOutcome], cfg: dict) -> Any:
    temp = float(cfg.get("temperature", 0.5))
    score = [(c.weight * (c.router_score) + c.bias) for c in outcomes]
    probs = _softmax(score, temp)
    buckets: Counter = Counter()
    samples: dict[str, Any] = {}
    for c, p in zip(outcomes, probs):
        key = _vote_key(c.response)
        buckets[key] += p * float(_extract_conf(c.response))
        samples.setdefault(key, c.response)
    if not buckets:
        return None
    winner, mass = buckets.most_common(1)[0]
    return {"value": samples[winner], "ensemble_confidence": round(mass, 4),
            "vote_distribution": dict(buckets)}


def _agg_weighted_mean(outcomes: list[WorkerOutcome], cfg: dict) -> Any:
    clip = cfg.get("clip") or [None, None]
    num, den = 0.0, 0.0
    for c in outcomes:
        v = _extract_numeric(c.response)
        if v is None:
            continue
        w = max(0.0, c.weight * max(c.router_score, 0.01) + c.bias)
        num += w * v
        den += w
    if den == 0:
        return None
    val = num / den
    if clip[0] is not None:
        val = max(float(clip[0]), val)
    if clip[1] is not None:
        val = min(float(clip[1]), val)
    return {"value": val, "ensemble_confidence": round(min(1.0, den / len(outcomes)), 4)}


def _agg_json_merge(outcomes: list[WorkerOutcome], cfg: dict) -> Any:
    conf_field = cfg.get("confidence_field", "confidence")
    merged: dict[str, Any] = {}
    best_conf: dict[str, float] = {}
    for c in outcomes:
        if not isinstance(c.response, dict):
            continue
        conf = float(c.response.get(conf_field, 0.5))
        eff = c.weight * max(c.router_score, 0.01) + c.bias
        for k, v in c.response.items():
            score = conf * eff
            if score > best_conf.get(k, -1.0):
                merged[k] = v
                best_conf[k] = score
    return merged or None


_AGGREGATORS: dict[str, Callable[[list[WorkerOutcome], dict], Any]] = {
    "weighted_softmax_vote": _agg_weighted_softmax_vote,
    "weighted_mean":          _agg_weighted_mean,
    "json_merge":             _agg_json_merge,
}

# Register the Recurrent Depth Transformer aggregator. Deferred import
# avoids a circular load during package init.
try:
    from . import recurrent_depth as _rdt
    _rdt.register_with_ensemble()
except Exception:
    pass


def _vote_key(resp: Any) -> str:
    if isinstance(resp, dict):
        return str(resp.get("label") or resp.get("text") or json.dumps(resp, sort_keys=True))
    return str(resp)


def _extract_conf(resp: Any) -> float:
    if isinstance(resp, dict):
        return float(resp.get("confidence", 0.5))
    return 0.5


def _extract_numeric(resp: Any) -> float | None:
    if isinstance(resp, (int, float)):
        return float(resp)
    if isinstance(resp, dict):
        for k in ("value", "score", "number"):
            if isinstance(resp.get(k), (int, float)):
                return float(resp[k])
    return None


# ---------------------------------------------------------------------------
# Parallel dispatch
# ---------------------------------------------------------------------------
def dispatch_parallel(task: str, payload: Any, *,
                      validator: Callable[[Any], float] | None = None,
                      caller: Callable[[Decision, Any, dict], Any] | None = None,
                      ) -> EnsembleResult:
    """Run the top-K eligible free models for `task` concurrently and combine.

    Args:
        task:      one of llms.task_profiles keys (e.g. "vendor_consolidation")
        payload:   request body (dict / str). Passed verbatim to each model.
        validator: optional fn(answer)->[0,1] used to update weights inline.
        caller:    optional override of the registered model caller.
    """
    cfg = _ensemble_cfg()
    if not cfg.get("enabled", True):
        # Fallback: single best model, no parallelism.
        from .llm_router import select_llm
        d = select_llm(task)
        single_caller = caller or _DEFAULT_CALLER or _grid_dispatch_caller
        t0 = time.perf_counter()
        try:
            resp = single_caller(d, payload, cfg)
            ok, err = True, None
        except Exception as e:
            resp, ok, err = None, False, str(e)
        elapsed = int((time.perf_counter() - t0) * 1000)
        outcome = WorkerOutcome(d.model_id, resp, elapsed, ok, err, d.score, 1.0, 0.0)
        return EnsembleResult(task=task, answer=resp,
                              aggregator="single", elapsed_ms=elapsed,
                              contributors=[outcome])

    fanout = int(cfg.get("fanout_k", 5))
    workers = int(cfg.get("parallel_workers", fanout))
    timeout = float(cfg.get("request_timeout_s", 45))
    aggregator_name = cfg.get("aggregator_default", "weighted_softmax_vote")
    agg_cfg = (cfg.get("aggregators") or {}).get(aggregator_name, {})

    ranked = [d for d in rank_llms(task) if d.passed_filters][:fanout]
    if not ranked:
        ranked = rank_llms(task)[:fanout]
    skipped = [d.model_id for d in rank_llms(task)[fanout:]]

    # Exploration reserve: with probability `exploration_reserve` we bypass
    # learned weights entirely and use the pure router prior. This keeps
    # newcomer / underdog models reachable and prevents the self-trainer
    # from collapsing the ensemble onto whatever was historically right
    # for the narrow set of tasks it can validate from the data pipeline.
    import random
    explore_p = float(((load_config().get("llms") or {}).get("self_train") or {})
                      .get("exploration_reserve", 0.15))
    explore_now = random.random() < explore_p

    # Hydrate weight & bias for each contributor (lazy-init via prior).
    with _conn() as cn:
        cur = cn.cursor()
        prepared: list[tuple[Decision, float, float]] = []
        for d in ranked:
            if explore_now:
                # Use router prior verbatim — no learned suppression.
                w, b = float(d.score), 0.0
            else:
                w, b = _load_weight(cur, task, d.model_id, prior=d.score)
            prepared.append((d, w, b))

    invoke = caller or _DEFAULT_CALLER or _grid_dispatch_caller
    outcomes: list[WorkerOutcome] = []
    # Thread the task name onto each Decision so _grid_dispatch_caller can
    # forward it to the remote compute_node (was always 'default' before).
    for d, _w, _b in prepared:
        try:
            object.__setattr__(d, "_task", task)
        except Exception:
            pass
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        future_to_meta = {
            pool.submit(_run_one, invoke, d, payload, cfg, w, b): (d, w, b)
            for (d, w, b) in prepared
        }
        try:
            for fut in as_completed(future_to_meta, timeout=timeout):
                outcomes.append(fut.result())
        except Exception:
            # Timeout: collect whatever finished
            for fut, (d, w, b) in future_to_meta.items():
                if fut.done() and not any(o.model_id == d.model_id for o in outcomes):
                    try:
                        outcomes.append(fut.result(timeout=0))
                    except Exception as e:
                        outcomes.append(WorkerOutcome(d.model_id, None, 0, False,
                                                     str(e), d.score, w, b))
                elif not fut.done():
                    outcomes.append(WorkerOutcome(d.model_id, None,
                                                  int(timeout * 1000), False,
                                                  "timeout", d.score, w, b))

    elapsed = int((time.perf_counter() - t0) * 1000)
    successes = [o for o in outcomes if o.ok and o.response is not None]
    aggregator_fn = _AGGREGATORS.get(aggregator_name, _agg_weighted_softmax_vote)
    # Thread task name into agg_cfg so per-task aggregators (like the
    # recurrent depth transformer) can attribute their audit logs.
    agg_cfg_runtime = {**agg_cfg, "_task": task}
    answer = aggregator_fn(successes, agg_cfg_runtime) if successes else None

    result = EnsembleResult(task=task, answer=answer, aggregator=aggregator_name,
                            elapsed_ms=elapsed, contributors=outcomes,
                            skipped=skipped)

    val = float(validator(answer)) if (validator is not None and answer is not None) else None
    _record_dispatch(result, val)
    if val is not None:
        update_weights(task, outcomes, val)
    else:
        # Even without an explicit validator, telemetry (latency/success) is
        # still useful — update EMAs only.
        _update_telemetry(task, outcomes)
    return result


def _run_one(invoke: Callable, d: Decision, payload: Any, cfg: dict,
             weight: float, bias: float) -> WorkerOutcome:
    t0 = time.perf_counter()
    try:
        resp = invoke(d, payload, cfg)
        return WorkerOutcome(d.model_id, resp,
                             int((time.perf_counter() - t0) * 1000),
                             True, None, d.score, weight, bias)
    except Exception as e:
        return WorkerOutcome(d.model_id, None,
                             int((time.perf_counter() - t0) * 1000),
                             False, str(e), d.score, weight, bias)


# ---------------------------------------------------------------------------
# Online learning: weight & bias update with weight decay + EMA telemetry
# ---------------------------------------------------------------------------
def update_weights(task: str, outcomes: Iterable[WorkerOutcome],
                   validator_score: float) -> None:
    """SGD-style update: each contributor's (w, b) shifts toward the validator
    target proportional to its router-score-weighted activation, with L2
    decay. Failed models receive a downward nudge. Latency/success EMAs are
    refreshed in the same transaction.
    """
    cfg = (_ensemble_cfg().get("learning") or {})
    lr_w = float(cfg.get("lr_weight", 0.05))
    lr_b = float(cfg.get("lr_bias", 0.02))
    l2 = float(cfg.get("l2_reg", 0.001))
    target = float(cfg.get("success_target", 1.0))
    alpha = float(cfg.get("ema_alpha", 0.2))
    lo = float(cfg.get("min_weight", 0.05))
    hi = float(cfg.get("max_weight", 3.0))

    with _conn() as cn:
        for o in outcomes:
            cur = cn.execute(
                "SELECT weight, bias, n_obs, ema_success, ema_latency "
                "FROM llm_weights WHERE task=? AND model_id=?",
                (task, o.model_id),
            )
            row = cur.fetchone()
            if row is None:
                w, b, n, es, el = max(lo, 1.0 + o.router_score), 0.0, 0, 0.5, 0.0
            else:
                w, b, n, es, el = row
            success = 1.0 if (o.ok and o.response is not None) else 0.0
            es = (1 - alpha) * float(es) + alpha * success
            el = (1 - alpha) * float(el) + alpha * float(o.latency_ms)
            # Effective contribution = (w * router_score + b)
            # Gradient toward target: dL/dw = -(target - score) * router_score
            err = target - (validator_score if success > 0.5 else 0.0)
            grad_w = -err * max(o.router_score, 0.01) + l2 * w
            grad_b = -err + l2 * b
            w_new = max(lo, min(hi, float(w) - lr_w * grad_w))
            b_new = float(b) - lr_b * grad_b
            cn.execute(
                "INSERT INTO llm_weights(task, model_id, weight, bias, n_obs, "
                "ema_success, ema_latency, updated_at) VALUES(?,?,?,?,?,?,?,CURRENT_TIMESTAMP) "
                "ON CONFLICT(task, model_id) DO UPDATE SET "
                " weight=excluded.weight, bias=excluded.bias, "
                " n_obs=excluded.n_obs, ema_success=excluded.ema_success, "
                " ema_latency=excluded.ema_latency, updated_at=CURRENT_TIMESTAMP",
                (task, o.model_id, w_new, b_new, int(n) + 1, es, el),
            )


def _update_telemetry(task: str, outcomes: Iterable[WorkerOutcome]) -> None:
    """Refresh EMA latency/success without changing weight or bias."""
    cfg = (_ensemble_cfg().get("learning") or {})
    alpha = float(cfg.get("ema_alpha", 0.2))
    with _conn() as cn:
        for o in outcomes:
            cur = cn.execute(
                "SELECT weight, bias, n_obs, ema_success, ema_latency "
                "FROM llm_weights WHERE task=? AND model_id=?",
                (task, o.model_id),
            )
            row = cur.fetchone()
            if row is None:
                continue
            w, b, n, es, el = row
            success = 1.0 if (o.ok and o.response is not None) else 0.0
            es = (1 - alpha) * float(es) + alpha * success
            el = (1 - alpha) * float(el) + alpha * float(o.latency_ms)
            cn.execute(
                "UPDATE llm_weights SET ema_success=?, ema_latency=?, n_obs=?, "
                "updated_at=CURRENT_TIMESTAMP WHERE task=? AND model_id=?",
                (es, el, int(n) + 1, task, o.model_id),
            )


def _record_dispatch(result: EnsembleResult, validator: float | None) -> None:
    with _conn() as cn:
        cn.execute(
            "INSERT INTO llm_dispatch_log(task, fanout, elapsed_ms, aggregator, "
            "contributors_json, validator) VALUES(?,?,?,?,?,?)",
            (result.task, len(result.contributors), result.elapsed_ms,
             result.aggregator,
             json.dumps([asdict(c) for c in result.contributors], default=str),
             validator),
        )


__all__ = [
    "EnsembleResult",
    "WorkerOutcome",
    "dispatch_parallel",
    "update_weights",
    "weights_for",
    "set_caller",
]
