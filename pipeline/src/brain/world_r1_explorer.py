"""world_r1_explorer.py — World-R1-inspired multi-axis reward-shaped exploration.

Synthesizes Microsoft World-R1 (Flow-GRPO multi-axis 3D-constraint rewards +
periodic dynamic-only regularization) with curiosity-driven unsupervised
exploration ("the unsupervised child"), to guide the Brain's interactions
with external knowledge surfaces ("TheOther" — OCW, arXiv, web, RAG corpus).

Concept mapping (World-R1 → Brain)
----------------------------------

    Camera-aware latent init      →  Topical seed conditioning
                                       (prior token distribution shapes the
                                       initial frontier composition)

    Multi-axis 3D reward          →  Multi-axis CONSTRAINT reward:
        meta_view assessment      →    coverage_axis  — topological breadth
        reconstruction consistency→    consistency_axis — token coherence with corpus
        trajectory alignment      →    trajectory_axis  — endpoint cluster alignment
                                       (delegated to EndpointTunnelBias)

    General visual / aesthetic    →  quality_axis  — signal_strength × resource density

    Periodic dynamic-only phase   →  CURIOSITY-ONLY phase: every K iterations,
                                       all constraint rewards are zeroed and
                                       only the unsupervised novelty bonus
                                       drives the policy — preserves motion
                                       diversity / prevents over-constraining
                                       the corpus to a single geometry.

    Flow-GRPO post-training       →  GRPO-like advantage normalisation across
                                       the candidate frontier — each candidate
                                       node's reward is normalised against the
                                       group mean / std before softmax sampling.

    "The unsupervised child"      →  CuriosityBonus: novelty = inverse log-
                                       frequency of token overlap with already-
                                       visited corpus.  High for tokens the
                                       Brain has rarely encountered — drives
                                       the System into TheOther's frontier.

The result is a policy that produces *multidimensional generation* of corpus
expansion paths: each step samples from a softmax over (constraint rewards +
periodic curiosity-only resets), creating a rich spectrum of trajectories
through TheOther rather than greedy collapse onto a single high-potential path.

Usage::

    from src.brain.world_r1_explorer import world_r1_explore

    result = world_r1_explore(
        seed_slug="6-034-artificial-intelligence-fall-2010",
        max_iterations=12,
        sample_breadth=5,
        endpoint_concepts=[...],
        dynamic_only_period=4,   # every 4th iteration is curiosity-only
    )
"""

from __future__ import annotations

import logging
import math
import random
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Reward axes — each returns a scalar in [0, 1]
# ---------------------------------------------------------------------------

@dataclass
class RewardAxis:
    """One dimension of the multi-axis constraint reward.

    Attributes
    ----------
    name:
        Human-readable label.
    weight:
        Default weight in the composite reward (overridable per-iteration).
    fn:
        Callable ``(candidate_emb, context) -> float`` returning [0, 1].
    """
    name: str
    weight: float
    fn: Callable


def _coverage_axis(
    candidate_emb,
    context: dict,
) -> float:
    """meta_view → topological breadth.

    Reward higher for candidates whose token set is *least* overlapping with
    the union of already-visited node tokens.  Encourages BFS to spread
    across distinct topological regions.
    """
    visited_tokens: set = context.get("visited_token_union", set())
    if not visited_tokens or not candidate_emb.tokens:
        return 0.5
    overlap = len(candidate_emb.tokens & visited_tokens)
    own = len(candidate_emb.tokens)
    # Coverage = 1 - (overlap fraction).  Pure-novel = 1.0; fully-covered = 0.0.
    return 1.0 - (overlap / max(own, 1))


def _consistency_axis(
    candidate_emb,
    context: dict,
) -> float:
    """reconstruction → semantic consistency with the *seed* trajectory.

    Reward higher for candidates whose tokens align with the seed
    embedding's signature.  Prevents pure-random drift while still allowing
    spread (paired against coverage_axis).
    """
    seed_emb = context.get("seed_emb")
    if seed_emb is None:
        return 0.5
    return seed_emb.similarity_to(candidate_emb)


def _trajectory_axis(
    candidate_emb,
    context: dict,
) -> float:
    """trajectory → endpoint cluster alignment.

    Delegates to EndpointTunnelBias if an endpoint is configured.  Provides
    directional pressure toward a known target concept cluster.
    """
    tunnel = context.get("tunnel")
    if tunnel is None:
        return 0.5
    return tunnel.similarity_to_endpoint(candidate_emb)


def _quality_axis(
    candidate_emb,
    context: dict,
) -> float:
    """general/aesthetic → intrinsic node quality.

    Uses the embedding's ``signal_strength`` (set by the deepen step from
    resource density and other signals) directly.
    """
    return float(getattr(candidate_emb, "signal_strength", 0.5))


# ---------------------------------------------------------------------------
# Curiosity bonus — "the unsupervised child"
# ---------------------------------------------------------------------------

class CuriosityBonus:
    """Novelty score from inverse log-frequency of token overlap with corpus.

    The bonus is HIGH for candidates whose tokens are rare in the
    already-visited corpus and LOW for candidates whose tokens have been
    seen many times — exactly the "unsupervised child" pattern: surprise
    drives attention.

    Mathematically::

        bonus(c) = mean_{t ∈ tokens(c)} 1 / log(2 + freq(t))
    """

    def __init__(self) -> None:
        self._token_freq: Counter[str] = Counter()

    def observe(self, tokens: frozenset[str] | set[str]) -> None:
        """Record that these tokens have been seen (increments their counts)."""
        self._token_freq.update(tokens)

    def score(self, tokens: frozenset[str] | set[str]) -> float:
        """Return novelty score in (0, 1].

        Tokens never seen → ``1 / log(2) ≈ 1.44`` → clipped to 1.0.
        Frequently seen → approaches 0.
        """
        if not tokens:
            return 0.5
        scores = [1.0 / math.log(2.0 + self._token_freq[t]) for t in tokens]
        mean = sum(scores) / len(scores)
        return min(1.0, mean)

    @property
    def vocab_size(self) -> int:
        return len(self._token_freq)


# ---------------------------------------------------------------------------
# GRPO-like advantage normalisation
# ---------------------------------------------------------------------------

def grpo_normalize(rewards: list[float], eps: float = 1e-8) -> list[float]:
    """Normalise rewards across the candidate group → mean 0, std 1.

    This is the Flow-GRPO step: instead of using raw rewards, the policy
    sees *advantages* (how much each candidate's reward exceeds the group
    mean, scaled by the group's standard deviation).  Reduces variance
    in the sampling distribution and keeps the policy from collapsing
    onto a single high-reward path.
    """
    if not rewards:
        return []
    mean = sum(rewards) / len(rewards)
    var = sum((r - mean) ** 2 for r in rewards) / len(rewards)
    std = math.sqrt(var) + eps
    return [(r - mean) / std for r in rewards]


def softmax_sample(
    advantages: list[float],
    items: list,
    k: int,
    temperature: float = 1.0,
    rng: random.Random | None = None,
) -> list:
    """Sample ``k`` items WITHOUT replacement from a softmax over advantages.

    Higher temperature → more exploration; lower → more greedy.
    """
    if not items:
        return []
    rng = rng or random.Random()
    k = min(k, len(items))
    # Numerically stable softmax
    scaled = [a / max(temperature, 1e-6) for a in advantages]
    m = max(scaled)
    weights = [math.exp(s - m) for s in scaled]

    sampled: list = []
    pool = list(zip(items, weights))
    for _ in range(k):
        if not pool:
            break
        total = sum(w for _, w in pool)
        r = rng.uniform(0.0, total)
        acc = 0.0
        for idx, (item, w) in enumerate(pool):
            acc += w
            if acc >= r:
                sampled.append(item)
                pool.pop(idx)
                break
    return sampled


# ---------------------------------------------------------------------------
# Iteration accounting
# ---------------------------------------------------------------------------

@dataclass
class IterationLog:
    """One iteration's worth of policy state for diagnostic display."""
    iteration:        int
    mode:             str           # "constraint" or "dynamic_only"
    expanded_slug:    str
    candidates_seen:  int
    candidates_taken: int
    reward_breakdown: dict          # {axis_name: mean_value}
    curiosity_bonus:  float
    composite_mean:   float
    advantage_max:    float
    new_visits:       list[str]
    rows_written:     int


# ---------------------------------------------------------------------------
# WorldR1Explorer — the integrated policy
# ---------------------------------------------------------------------------

class WorldR1Explorer:
    """Multi-axis reward-shaped corpus exploration with periodic curiosity-only
    regularization phase, GRPO advantage normalisation, and softmax sampling.

    Algorithmic loop
    ----------------
    For each iteration up to ``max_iterations``:

    1. Pop the next slug from the priority frontier (seeded with ``seed_slug``).
    2. Deepen it via ``deepen_fn`` and pull its detail via ``detail_fn``.
    3. Build a NodeEmbedding for the expanded node; observe its tokens in
       both the curiosity model and the visited-token union.
    4. For each related-course candidate at this node:
         a. Build candidate embedding.
         b. Compute each constraint reward axis (or zero them in
            dynamic-only mode).
         c. Composite reward = sum(weight_i × axis_i) + curiosity_weight × bonus
    5. GRPO-normalise the composite rewards across the candidate group.
    6. Softmax-sample ``sample_breadth`` candidates (without replacement).
    7. Append samples to the priority frontier (FIFO).
    8. Log the iteration.

    Every ``dynamic_only_period`` iterations the policy switches to
    dynamic-only mode: all constraint axes return zero and only the
    curiosity bonus drives selection.  This is the World-R1 dynamic-only
    regularization, reinterpreted: it prevents the corpus from over-fitting
    to a single topical geometry.
    """

    def __init__(
        self,
        # Constraint reward weights
        coverage_weight:     float = 1.0,
        consistency_weight:  float = 0.7,
        trajectory_weight:   float = 1.2,
        quality_weight:      float = 0.8,
        # Curiosity weight (mixes into both modes)
        curiosity_weight:    float = 0.6,
        # Periodic dynamic-only reset
        dynamic_only_period: int   = 4,
        # Sampling
        sample_breadth:      int   = 5,
        temperature:         float = 0.8,
        # Crawl politeness
        crawl_delay:         float = 0.6,
        # PRNG seed for reproducibility
        seed:                int | None = None,
    ) -> None:
        self.axes: list[RewardAxis] = [
            RewardAxis("coverage",    coverage_weight,    _coverage_axis),
            RewardAxis("consistency", consistency_weight, _consistency_axis),
            RewardAxis("trajectory",  trajectory_weight,  _trajectory_axis),
            RewardAxis("quality",     quality_weight,     _quality_axis),
        ]
        self.curiosity_weight    = curiosity_weight
        self.dynamic_only_period = max(1, dynamic_only_period)
        self.sample_breadth      = sample_breadth
        self.temperature         = temperature
        self.crawl_delay         = crawl_delay
        self._rng                = random.Random(seed)
        self._curiosity          = CuriosityBonus()

    # ------------------------------------------------------------------
    # Public entry-point
    # ------------------------------------------------------------------

    def explore(
        self,
        seed_slug: str,
        max_iterations: int = 10,
        endpoint_concepts: list[str] | None = None,
        deepen_fn: Callable | None = None,
        detail_fn: Callable | None = None,
    ) -> dict:
        """Run the World-R1-shaped curiosity exploration loop.

        Returns a result dict structured for compatibility with the
        existing cascade UI plus extra World-R1 diagnostics:

            {
                seed, iterations_run,
                courses_deepened, rows_written,
                resources, related, external,
                iteration_logs: [IterationLog as dict, ...],
                axis_weights, dynamic_only_period,
                vocab_size_final,
                edge_potentials, hop_signals,   # for Knowledge Graph compat
            }
        """
        # Lazy-import to avoid circulars
        from src.brain.semantic_graph import (
            NodeEmbedding,
            EndpointTunnelBias,
        )
        if deepen_fn is None or detail_fn is None:
            from src.brain.ml_research import (
                deepen_ocw_course,
                fetch_ocw_course_detail,
            )
            deepen_fn = deepen_fn or deepen_ocw_course
            detail_fn = detail_fn or fetch_ocw_course_detail

        tunnel: EndpointTunnelBias | None = (
            EndpointTunnelBias(endpoint_concepts) if endpoint_concepts else None
        )

        seed_emb = NodeEmbedding.from_title(seed_slug, seed_slug)
        visited_token_union: set[str] = set()
        visited: set[str] = set()
        frontier: list[str] = [seed_slug]
        iteration_logs: list[IterationLog] = []
        edge_potentials: dict[str, float] = {seed_slug: 1.0}
        hop_signals: dict[int, float] = {}

        courses_deepened: list[str] = []
        total_rows = total_res = total_rel = total_ext = 0

        context: dict = {
            "seed_emb":             seed_emb,
            "visited_token_union":  visited_token_union,
            "tunnel":               tunnel,
        }

        for it in range(max_iterations):
            if not frontier:
                log.info(f"world_r1_explore: frontier exhausted at iter {it}")
                break

            slug = frontier.pop(0)
            if slug in visited:
                continue
            visited.add(slug)

            mode = "dynamic_only" if (it + 1) % self.dynamic_only_period == 0 else "constraint"

            # ── Deepen the chosen node ─────────────────────────────────
            result = deepen_fn(slug)
            if not result.get("fetched"):
                continue

            courses_deepened.append(slug)
            total_rows += result.get("rows_written", 0)
            total_res  += result.get("resources", 0)
            total_rel  += result.get("related", 0)
            total_ext  += result.get("external", 0)

            detail = detail_fn(slug) if (
                result.get("related", 0) or result.get("resources", 0)
            ) else {}
            resource_density = min(1.0, result.get("resources", 0) / 30.0)

            # Build expanded-node embedding
            ep_sim = (
                tunnel.similarity_to_endpoint(NodeEmbedding.from_title(slug, slug))
                if tunnel else 0.5
            )
            sig = 0.6 * ep_sim + 0.4 * resource_density
            expanded_emb = NodeEmbedding.from_detail(slug, detail, signal_strength=sig)
            visited_token_union.update(expanded_emb.tokens)
            self._curiosity.observe(expanded_emb.tokens)
            hop_signals[it] = round(sig, 4)

            # ── Build candidate set from related courses ───────────────
            candidates: list[NodeEmbedding] = []
            for rc in (detail.get("related_courses") or []):
                rc_slug = rc.get("slug", "")
                if not rc_slug or rc_slug in visited:
                    continue
                cand_emb = NodeEmbedding.from_title(rc_slug, rc_slug)
                candidates.append(cand_emb)

            new_visits: list[str] = []
            axis_breakdown: dict = {a.name: 0.0 for a in self.axes}
            curiosity_mean = 0.0
            advantage_max  = 0.0

            if candidates:
                # ── Compute multi-axis rewards (or zero in dynamic-only) ─
                composites: list[float] = []
                for cand in candidates:
                    axis_vals: dict[str, float] = {}
                    for axis in self.axes:
                        if mode == "dynamic_only":
                            axis_vals[axis.name] = 0.0
                        else:
                            axis_vals[axis.name] = axis.fn(cand, context)

                    constraint_sum = sum(
                        a.weight * axis_vals[a.name] for a in self.axes
                    )
                    cur = self._curiosity.score(cand.tokens)
                    composite = constraint_sum + self.curiosity_weight * cur
                    composites.append(composite)

                    # accumulate breakdown
                    for k, v in axis_vals.items():
                        axis_breakdown[k] += v
                    curiosity_mean += cur

                n = len(candidates)
                axis_breakdown = {k: round(v / n, 4) for k, v in axis_breakdown.items()}
                curiosity_mean = curiosity_mean / n
                composite_mean = sum(composites) / n

                # ── GRPO normalise → softmax sample ─────────────────────
                advantages = grpo_normalize(composites)
                advantage_max = max(advantages) if advantages else 0.0

                # Record edge potentials for diagnostic display
                for cand, adv in zip(candidates, advantages):
                    edge_potentials[cand.slug] = max(
                        edge_potentials.get(cand.slug, 0.0),
                        # rescale advantage into [0, 1]-ish range for display
                        max(0.0, min(1.0, 0.5 + adv * 0.25)),
                    )

                sampled = softmax_sample(
                    advantages, candidates, self.sample_breadth,
                    temperature=self.temperature, rng=self._rng,
                )
                for cand in sampled:
                    if cand.slug not in visited and cand.slug not in frontier:
                        frontier.append(cand.slug)
                        new_visits.append(cand.slug)
            else:
                composite_mean = 0.0

            iteration_logs.append(IterationLog(
                iteration       = it,
                mode            = mode,
                expanded_slug   = slug,
                candidates_seen = len(candidates),
                candidates_taken= len(new_visits),
                reward_breakdown= axis_breakdown,
                curiosity_bonus = round(curiosity_mean, 4),
                composite_mean  = round(composite_mean, 4),
                advantage_max   = round(advantage_max, 4),
                new_visits      = new_visits,
                rows_written    = result.get("rows_written", 0),
            ))

            time.sleep(self.crawl_delay)

        return {
            "seed":                seed_slug,
            "iterations_run":      len(iteration_logs),
            "courses_deepened":    courses_deepened,
            "rows_written":        total_rows,
            "resources":           total_res,
            "related":             total_rel,
            "external":            total_ext,
            "iteration_logs":      [log_.__dict__ for log_ in iteration_logs],
            "axis_weights":        {a.name: a.weight for a in self.axes},
            "curiosity_weight":    self.curiosity_weight,
            "dynamic_only_period": self.dynamic_only_period,
            "vocab_size_final":    self._curiosity.vocab_size,
            "edge_potentials":     edge_potentials,
            "hop_signals":         hop_signals,
        }


# ---------------------------------------------------------------------------
# Convenience wrapper — callable directly from ml_research
# ---------------------------------------------------------------------------

_DEFAULT_ENDPOINT_CONCEPTS: list[str] = [
    "supply chain optimization machine learning",
    "inventory management deep learning reinforcement",
    "logistics network optimization operations research",
    "demand forecasting neural network probabilistic",
    "procurement analytics natural language processing",
    "supplier risk prediction regression classification",
    "production scheduling planning constraint programming",
]


def world_r1_explore(
    seed_slug: str,
    max_iterations: int = 10,
    sample_breadth: int = 5,
    endpoint_concepts: list[str] | None = None,
    coverage_weight: float = 1.0,
    consistency_weight: float = 0.7,
    trajectory_weight: float = 1.2,
    quality_weight: float = 0.8,
    curiosity_weight: float = 0.6,
    dynamic_only_period: int = 4,
    temperature: float = 0.8,
    crawl_delay: float = 0.6,
    seed: int | None = None,
) -> dict:
    """One-shot helper: build a ``WorldR1Explorer`` and run ``explore``.

    Drop-in callable from ``ml_research`` and the Streamlit page.
    """
    explorer = WorldR1Explorer(
        coverage_weight     = coverage_weight,
        consistency_weight  = consistency_weight,
        trajectory_weight   = trajectory_weight,
        quality_weight      = quality_weight,
        curiosity_weight    = curiosity_weight,
        dynamic_only_period = dynamic_only_period,
        sample_breadth      = sample_breadth,
        temperature         = temperature,
        crawl_delay         = crawl_delay,
        seed                = seed,
    )
    return explorer.explore(
        seed_slug         = seed_slug,
        max_iterations    = max_iterations,
        endpoint_concepts = endpoint_concepts or _DEFAULT_ENDPOINT_CONCEPTS,
    )
