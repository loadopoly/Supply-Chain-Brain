# Neural Plasticity — the Rewiring Agent

**Module:** `pipeline/src/brain/neural_plasticity.py`

## Purpose

The five senses (Brain, Vision, Touch, Smell, Body) were previously
hardcoded with the same capability parameters regardless of how much
knowledge the corpus had accumulated. As the graph grows from thousands
to millions of entities, the appropriate batch sizes, cadences, learning
rates, and pressure thresholds change. This module is the agent that
rewires those parameters live — it measures knowledge state every round
and ADAM-smooths every per-sense dial toward a growth-driven target.

## Architecture

```
  ┌────────────────────┐     ┌──────────────────────┐
  │  measure_knowledge │ ──► │ compute_capability_  │
  │  _state()          │     │ targets()            │
  │                    │     │                      │
  │ • entity_count     │     │ Saturating maps from │
  │ • edge_count       │     │ growth → dial value  │
  │ • learning_count   │     │  (one per sense)     │
  │ • doc_chunk_count  │     │                      │
  │ • smell_readings   │     │                      │
  │ • directives       │     │                      │
  │ • rounds           │     │                      │
  └────────────────────┘     └──────────┬───────────┘
                                        │
                                        ▼
                          ┌─────────────────────────────┐
                          │ rewire_round() — ADAM-      │
                          │ smoothed update toward      │
                          │ targets, persisted to       │
                          │ brain_kv.neural_plasticity_ │
                          │ state                       │
                          └──────────────┬──────────────┘
                                         │
        ┌──────────────┬─────────────────┼─────────────────┬──────────────┐
        ▼              ▼                 ▼                 ▼              ▼
     Vision         Touch              Smell             Body          Brain
   (knowledge_  (brain_body_       (sense_of_smell  (cadence,      (round
    corpus)      signals)            sensitivity)    directive cap) intervals)
```

## Knowledge metrics

`measure_knowledge_state()` reads from SQLite:

| Metric          | Source table            | Scale (mid-range) |
|-----------------|-------------------------|-------------------|
| `entities`      | `corpus_entity`         | 30,000            |
| `edges`         | `corpus_edge`           | 20,000            |
| `learnings`     | `learning_log`          | 10,000            |
| `doc_chunks`    | `doc_chunk`             | 2,000             |
| `smell_readings`| `sense_of_smell`        | 1,000             |
| `directives`    | `body_directives`       | 200               |
| `rounds`        | `corpus_round_log`      | 1,000             |

Each metric `m` is mapped through a saturating function
`f(m) = m / (m + scale) ∈ [0, 1]` so the dial inputs are bounded.

## Per-sense dials

| Sense  | Dial                  | Direction with growth | Default → target at 10×     |
|--------|-----------------------|------------------------|------------------------------|
| Vision | `pressure_threshold`  | ↓ (force more easily)  | 0.30 → 0.19                  |
| Vision | `force_threshold`     | ↓ (toroidal scheduler) | 0.30 → 0.19                  |
| Vision | `dw_batch_size`       | ↑ (absorb more)        | 500 → 1864                   |
| Vision | `ocw_batch_size`      | ↑                      | 50 → 186                     |
| Vision | `min_seconds`         | ↓ (faster cadence)     | 60s → 24s                    |
| Vision | `blade_period_dw`     | ↓ (spin faster)        | 3 → 2.1                      |
| Vision | `blade_period_ocw`    | ↓                      | 3 → 2.1                      |
| Touch  | `max_directives`      | ↑ (more capacity)      | 25 → 93                      |
| Touch  | `learning_rate`       | ↓ (annealing)          | 0.30 → 0.12                  |
| Touch  | `resolved_grad`       | ↑ (more decisive)      | −0.50 → −0.64                |
| Touch  | `min_seconds`         | ↓                      | 30s → 12s                    |
| Smell  | `sensitivity`         | ↑ (Dirichlet evidence) | 1.0 → 2.4                    |
| Smell  | `burst_priority`      | ↓ (less burst-chasing) | 0.50 → 0.32                  |
| Smell  | `tau_jitter`          | ↓ (less noise)         | 0.020 → 0.006                |
| Body   | `cadence_seconds`     | ↓                      | 60s → 19s                    |
| Body   | `value_per_year_mult` | ↑ (confidence)         | 1.0 → 1.45                   |
| Body   | `owner_role_breadth`  | ↑                      | 3 → 7.5                      |
| Brain  | `round_min_seconds`   | ↓                      | 60s → 24s                    |
| Brain  | `graph_centrality_top`| ↑                      | 50 → 277                     |
| Brain  | `synaptic_decay`      | ↓ (more inertia)       | 0.050 → 0.023                |

All dials are linearly interpolated between default and "stretch" max
through the saturating axis. Movement is smoothed by a per-dial ADAM
optimizer (β₁=0.9, β₂=0.999, lr=0.20) so the senses never see a step
change between rounds.

## Senses reading dials

Each sense calls `get_dial(sense, name, default)` at the start of its
cycle. Today wired:

| Sense file                  | Dial reads                                   |
|-----------------------------|----------------------------------------------|
| `knowledge_corpus.py`       | `vision.pressure_threshold`, `vision.force_threshold` (in `_torus_schedule`) |
| `brain_body_signals.py`     | `touch.max_directives`, `touch.learning_rate` (in `_adam_step`) |

Wiring more dials is a one-liner per call site — the plasticity module
ships defaults that exactly match the current hardcoded values, so any
sense can opt in incrementally without behaviour change.

## Driver

`rewire_round()` is called from the closed-loop tail of
`refresh_corpus_round()` after `surface_effective_signals()`. It is
rate-limited to once every 30 s by default. Output is added to the
round summary as `plasticity.{ran, knowledge, dials}`.

## Persistence

Full state (current dial values, ADAM optimizer state per dial,
knowledge measurement, last-run timestamp) lives in
`brain_kv.neural_plasticity_state`. Read with `get_plasticity_state()`
for diagnostics or `get_all_dials()` for just the current dial values
that senses are seeing.

## Validated behaviour

- `_test_plasticity.py` confirms:
  - Direction of every dial vs corpus growth matches the table above
  - ADAM smoothing prevents step-changes (two consecutive rewires move
    each dial only a fraction of the way to target)
  - Senses with no data yet (Smell with 0 readings) see defaults
  - Live state at 16,885 entities / 6,225 learnings / 14 directives
    moved Vision pressure threshold 0.30 → 0.22 and Touch directive
    cap 25 → 34
