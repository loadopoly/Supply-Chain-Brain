# Vision ↔ Touch Closed Loop

**Module map**
- `pipeline/src/brain/brain_body_signals.py` — Touch (efferent / pressure)
- `pipeline/src/brain/knowledge_corpus.py` — Vision (afferent / outreach)

The two halves were previously one-way (Vision → corpus → Touch reads,
no path back). They are now bilaterally coupled through a single ADAM
optimizer whose state lives in `brain_kv.touch_field_full_state`.

---

## Synaptic field

Per signal_kind we hold:

```
{ pressure, m, v, t, sum_counts, n_rounds }
```

- `pressure ∈ [0, 1]` — the consumer-facing value Vision reads
- `m`, `v`, `t` — first/second moments and step counter for ADAM
- `sum_counts`, `n_rounds` — Gamma-Poisson posterior accumulators

**Hyperparameters** (in `brain_body_signals.py`):

| Constant         | Value | Role                              |
|------------------|-------|-----------------------------------|
| `_TOUCH_BETA1`   | 0.85  | 1st-moment momentum               |
| `_TOUCH_BETA2`   | 0.999 | 2nd-moment momentum (real-ADAM)   |
| `_TOUCH_LR`      | 0.30  | Learning rate per round           |
| `_TOUCH_EPS`     | 1e-6  | √v floor                          |
| `_BAYES_ALPHA0`  | 1.0   | Gamma prior shape                 |
| `_BAYES_BETA0`   | 2.0   | Gamma prior rate                  |
| `_RESOLVED_GRAD` | -0.50 | Synthetic neg-grad on resolution  |

---

## Bayesian-Poisson centroid (Touch positive input)

Each round, every signal_kind's directive count is treated as a Poisson
observation; the Gamma–Poisson conjugate posterior gives a smoothed
firing rate. That rate is scaled by the round's mean priority to form
the ADAM gradient target:

```
posterior_rate = (α₀ + Σcounts) / (β₀ + n_rounds)
λ_t            = min(1, posterior_rate × mean_priority_t)
gradient       = λ_t − pressure_{t−1}
```

This replaces the old single-step momentum, so a kind that fires often
*and* with high priority is pulled toward `p=1` quickly, while a kind
that fires once with low priority barely moves.

---

## Vision-ops gradients (Vision negative input)

`refresh_corpus_round()` measures per-blade entity deltas
(`_dw_added`, `_ocw_added`, `_net_added`, `_schema_learnings`) plus the
list of forced blades, and packs them into a `vision_ops` dict that is
passed into `surface_effective_signals(vision_ops=...)`. The mapping:

| Vision op          | Relieves signal_kind             | Weight per unit |
|--------------------|----------------------------------|-----------------|
| `dw_entities`      | `missing_category`               | −0.015          |
| `dw_deepen_entities` | `high_centrality_part`         | −0.020          |
| `ocw_entities`     | `corpus_rag_saturated`           | −0.010          |
| `ocw_resources`    | `model_low_task_weight`          | −0.012          |
| `network_endpoints`| `peer_unreachable`               | −0.025          |
| `network_endpoints2` | `network_learner_not_started`  | −0.025          |
| `schema_learnings` | `self_train_drift`               | −0.008          |
| `rag_chunks`       | `doc_rag_coverage`               | −0.015          |
| `mission_signals`  | `mission_signals`                | −0.020          |
| `forced_blades`    | extra −0.05 on the blade's primary kind | confirmation bonus |

Unit weight is saturated at 50 (`min(n, 50.0)`) so a mega-round can't
zero the field in one step. Vision and Touch gradients are summed
*before* the ADAM update, so both feed the same `m`/`v` accumulators
and a noisy kind develops high `v` and damps regardless of which side
introduced the noise — torsional damping on the propeller axle.

---

## Stale-directive collapse (inverse-ReLU floor)

Every round, any `body_directives` row whose `fingerprint` is no longer
in the freshly-generated set is marked `status='expired'`. Its
`signal_kind` joins `resolved_kinds`, which feeds the synthetic
`_RESOLVED_GRAD × pressure_{t−1}` gradient through the ADAM step.

---

## Toroidal phase scheduler

Outreach blades sit on a torus parameterised by
`(major_period, minor_offset)`. The major angle θ_M advances by 1 per
round; a blade fires when `(phase % period) == offset`. The minor angle
`θ_m = (phase // period) % 2` selects the inflection mode:

- `θ_m = 0` → BROADEN (pull novel entities)
- `θ_m = 1` → DEEPEN (enrich existing entities)

Touch pressure can tunnel through the torus: any blade whose pressure
kinds exceed `_PRESSURE_FORCE = 0.30` is forced to fire regardless of
phase position. Default blade table (`_TOROIDAL_BLADES`):

| Blade      | Period | Offset | Forcing kinds                                    |
|------------|--------|--------|--------------------------------------------------|
| `dw`       | 3      | 0      | `missing_category`, `high_centrality_part`       |
| `ocw`      | 3      | 1      | `corpus_rag_saturated`, `model_low_task_weight`  |
| `network`  | 3      | 2      | `peer_unreachable`, `network_learner_not_started`|
| `synaptic` | 1      | 0      | (always fires — background decay)                |
| `schema`   | 2      | 0      | (every other round)                              |

Phase persisted in `brain_kv.corpus_round_phase`.

---

## Round-output observability

`refresh_corpus_round()` returns:

```jsonc
{
  "touch_pressure_in":  { "kind": pressure, ... },     // what Vision read
  "vision_ops_out":     { "dw_entities": 32, ... },    // what Vision did
  "touch_summary_out": {
    "directives_emitted": 0,
    "directives_expired": 5,
    "resolved_kinds":     ["..."],
    "vision_grads_in":    { "missing_category": -0.48, ... },
    "top_priority":       0.88
  },
  "forced_outreach":   { "dw": false, "ocw": true, "net": false }
}
```

`surface_effective_signals()` returns the same `vision_grads_in` plus
the new `touch_field`. `get_touch_field()` is the lightweight pressure
read for any consumer; `get_touch_field_full()` exposes the full ADAM
state per kind for diagnostics.

---

## Validated behaviour

| Test                              | File                | Result |
|-----------------------------------|---------------------|--------|
| ADAM ramp + resolve + flap        | `_test_adam.py`     | Steady-firing converges to `p=1.0` in ~4 steps; resolved kinds decay; flapping develops `v ≈ 0.0019` and step size shrinks |
| Bilateral Vision-ops injection    | `_test_bilateral.py`| Synthetic Vision ops produced correct relief gradients; unserved kinds (`model_low_task_weight=0.866`) retained pressure; served kinds (`missing_category`, `corpus_rag_saturated`) clamped to 0 |
| Live closed loop                  | manual round runs   | Round 1 expired 5 stale directives; round 2 read pressure 0.324 → forced OCW outreach → 369 entities added → relief gradient applied next round |
