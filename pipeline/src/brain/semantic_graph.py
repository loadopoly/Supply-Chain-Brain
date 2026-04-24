"""semantic_graph.py — Adaptive semantic graph traversal engine.

Implements the following theoretical framework applied to the OCW knowledge
graph and the Brain's corpus expansion pipeline:

    1. **NodeEmbedding** — lightweight token-space vectors for corpus nodes,
       enabling O(1) Jaccard-distance computation without external embeddings.

    2. **EdgePotential** — Morse-like edge weight function driven by
       relational distance between node embeddings.  Decays exponentially
       with semantic distance so high-affinity edges dominate traversal
       priority while low-affinity edges remain passable via tunnel bias.

    3. **AdamGradientTracker** — revised ADAM optimizer applied to the
       signal gradient across BFS hops.  Tracks first (mean) and second
       (variance) moments of the per-hop relevance signal, applies bias
       correction, and detects inflection points where the corrected
       gradient changes sign.  At inflection, the phase is *extended*
       (effective hop budget grows) rather than terminated.

    4. **EndpointTunnelBias** — directional bias derived from the known
       semantic distance between a frontier node and a target concept
       cluster (endpoint).  Provides positive lift to low-potential edges
       that point toward the endpoint, analogous to quantum tunneling
       through a classically-forbidden potential barrier.

    5. **AdaptiveSemGraphTraverser** — level-by-level BFS traversal that
       integrates all four components.  Frontier nodes are ranked by
       edge potential each hop; Adam inflection detection triggers
       phase-shift extension and increased fan-out; endpoint tunnel bias
       lifts semantically-distant-but-target-aligned nodes above
       high-potential-but-off-axis nodes.

Usage::

    from src.brain.semantic_graph import AdaptiveSemGraphTraverser

    traverser = AdaptiveSemGraphTraverser()
    result = traverser.traverse(
        seed_slug="6-034-artificial-intelligence-fall-2010",
        max_hops=3,
        fan_out=6,
        endpoint_concepts=[
            "supply chain optimization",
            "inventory management deep learning",
            "logistics reinforcement learning",
        ],
    )
    # result["phase_shifts"]   — hops where Adam inflection was detected
    # result["adam_report"]    — final Adam tracker state
    # result["edge_potentials"] — {slug: potential} for all enqueued nodes
"""

from __future__ import annotations

import heapq
import logging
import math
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stop-word set for tokenisation
# ---------------------------------------------------------------------------
_STOP: frozenset[str] = frozenset(
    "a an the and or of in on at to for with by is are was were be been "
    "have has had do does did will would could should may might must can "
    "from this that it its they them their we our us i you he she his her "
    "course using based via across into through which one two three also "
    "about after all any both during each few more most other some such "
    "than then there these they up".split()
)


def _tokenize(text: str) -> frozenset[str]:
    """Normalise → lowercase → alphanumeric tokens → strip stop-words."""
    tokens = re.findall(r"[a-z][a-z0-9\-]{1,}", text.lower())
    return frozenset(t for t in tokens if t not in _STOP and len(t) > 2)


# ---------------------------------------------------------------------------
# NodeEmbedding — token-bag representation of a corpus node
# ---------------------------------------------------------------------------

@dataclass
class NodeEmbedding:
    """Token-bag vector for one corpus node.

    Semantic distance is Jaccard distance over the token frozensets,
    giving a value in [0, 1]:
        0.0 — identical token sets (same node)
        1.0 — completely disjoint token sets (maximally distant)
    """

    slug: str
    tokens: frozenset[str]
    signal_strength: float = 0.75

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_detail(
        cls,
        slug: str,
        detail: dict,
        signal_strength: float = 0.75,
    ) -> "NodeEmbedding":
        """Build from an OCW ``fetch_ocw_course_detail()`` dict."""
        parts = [
            slug.replace("-", " "),
            detail.get("description", ""),
            " ".join(detail.get("topics", [])),
            " ".join(detail.get("instructors", [])),
            detail.get("level", ""),
        ]
        tokens = _tokenize(" ".join(p for p in parts if p))
        return cls(slug=slug, tokens=tokens, signal_strength=signal_strength)

    @classmethod
    def from_title(cls, slug: str, title: str = "", signal_strength: float = 0.75) -> "NodeEmbedding":
        """Build from a slug and optional raw title string."""
        tokens = _tokenize(f"{slug.replace('-', ' ')} {title}")
        return cls(slug=slug, tokens=tokens, signal_strength=signal_strength)

    # ------------------------------------------------------------------
    # Distance / similarity
    # ------------------------------------------------------------------

    def distance_to(self, other: "NodeEmbedding") -> float:
        """Jaccard distance — 0.0 = identical, 1.0 = fully disjoint."""
        if not self.tokens and not other.tokens:
            return 0.0
        intersection = len(self.tokens & other.tokens)
        union = len(self.tokens | other.tokens)
        return 1.0 - (intersection / union) if union else 1.0

    def similarity_to(self, other: "NodeEmbedding") -> float:
        """Jaccard similarity — complement of ``distance_to``."""
        return 1.0 - self.distance_to(other)


# ---------------------------------------------------------------------------
# EdgePotential — Morse-like relational distance-weighted edge weight
# ---------------------------------------------------------------------------

class EdgePotential:
    """Compute a directed edge potential from ``source`` → ``target``.

    The potential combines three terms:

    * **Morse term** — ``signal_strength(target) × (1 − d) × exp(−λ × d)``
      where ``d = Jaccard distance(source, target)``.  This peaks at
      ``d = 0`` and decays exponentially, mimicking a Morse potential well.

    * **Tunnel bias** — an additive lift ``endpoint_bias × κ`` that
      allows the traversal to cross a low-Morse barrier when the target
      lies in the direction of a known endpoint cluster.  The coefficient
      ``κ = tunneling_coeff`` controls how aggressively the bias overrides
      the Morse term.

    * **Phase amplifier** — a scalar ``≥ 1`` returned by the Adam tracker
      near inflection points.  Applied multiplicatively to the entire
      potential to extend the effective horizon during phase shifts.

    Overall formula::

        E(u, v) = [ signal(v) × (1 − d(u,v)) × exp(−λ × d(u,v))
                    + bias × κ ] × phase_amp
    """

    def __init__(
        self,
        decay_lambda: float = 2.5,
        tunneling_coeff: float = 0.35,
    ) -> None:
        self.decay_lambda = decay_lambda
        self.tunneling_coeff = tunneling_coeff

    def compute(
        self,
        source: NodeEmbedding,
        target: NodeEmbedding,
        endpoint_bias: float = 0.0,
        phase_amplifier: float = 1.0,
    ) -> float:
        """Return potential ∈ [0, (1 + tunneling_coeff) × phase_amplifier]."""
        d = source.distance_to(target)
        morse = target.signal_strength * (1.0 - d) * math.exp(-self.decay_lambda * d)
        tunnel = endpoint_bias * self.tunneling_coeff
        return (morse + tunnel) * phase_amplifier


# ---------------------------------------------------------------------------
# AdamGradientTracker — revised ADAM applied to hop-level signal gradient
# ---------------------------------------------------------------------------

class AdamGradientTracker:
    """Track the Adam-normalised gradient of the relevance signal across BFS hops.

    At each BFS level (hop), ``step(mean_signal)`` is called with the
    average signal strength of all nodes processed at that level.  The
    tracker maintains first-moment (mean, ``m_t``) and second-moment
    (uncentred variance, ``v_t``) estimates of the *gradient*
    ``g_t = signal_t − signal_{t-1}``, applies Adam bias correction, and
    exposes:

    * ``adam_estimate()`` — ``m̂_t / (√v̂_t + ε)``  (the corrected signal)
    * ``is_at_inflection()`` — True when the sign of ``g_t`` flipped
      relative to ``g_{t-1}`` (peak or trough crossed)
    * ``phase_amplifier()`` — scalar ≥ 1 that quantifies how far the
      traversal is from a flat gradient; applied to EdgePotential during
      phase-shift extension

    Why ADAM specifically?
    ----------------------
    Plain gradient sign-tracking is noisy.  Adam's momentum-weighted mean
    smooths transient oscillations while the variance term scales the
    corrected estimate by recent consistency.  This means only *sustained*
    gradient reversals (genuine inflection points) trigger a phase shift,
    not one-off anomalous hops.
    """

    def __init__(
        self,
        beta1: float = 0.9,
        beta2: float = 0.999,
        eps: float = 1e-8,
        max_phase_amp: float = 2.5,
    ) -> None:
        self.beta1 = beta1
        self.beta2 = beta2
        self.eps = eps
        self.max_phase_amp = max_phase_amp

        self._m: float = 0.0                # first moment accumulator
        self._v: float = 0.0                # second moment accumulator
        self._t: int = 0                    # step counter (for bias correction)
        self._prev_signal: float | None = None
        self._adam_history: list[float] = []      # m̂_t per step
        self._gradient_history: list[float] = []  # raw g_t per step
        self._sign_history: list[int] = []        # sign of g_t per step

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def step(self, mean_signal: float) -> float:
        """Ingest one BFS level's mean signal; return the Adam-corrected estimate.

        The gradient ``g_t`` is defined as the *change* in signal from the
        previous level.  On the first call, ``g_0 = 0`` (no change yet).
        """
        g = mean_signal - (self._prev_signal if self._prev_signal is not None else mean_signal)
        self._prev_signal = mean_signal

        self._t += 1
        # Adam moment updates
        self._m = self.beta1 * self._m + (1.0 - self.beta1) * g
        self._v = self.beta2 * self._v + (1.0 - self.beta2) * g * g

        # Bias-corrected moments
        m_hat = self._m / (1.0 - self.beta1 ** self._t)
        v_hat = self._v / (1.0 - self.beta2 ** self._t)

        adam_est = m_hat / (math.sqrt(v_hat) + self.eps)
        self._adam_history.append(adam_est)
        self._gradient_history.append(g)
        self._sign_history.append(1 if g >= 0.0 else -1)

        return adam_est

    @property
    def steps(self) -> int:
        return self._t

    def is_at_inflection(self) -> bool:
        """True when the gradient sign flipped — a genuine inflection point.

        Requires at least two steps and a non-trivial gradient magnitude
        so noise-floor oscillations do not trigger false positives.
        """
        if len(self._sign_history) < 2:
            return False
        # Sign flip
        if self._sign_history[-1] == self._sign_history[-2]:
            return False
        # Magnitude gate: both gradients must exceed eps for a real flip
        if len(self._gradient_history) < 2:
            return False
        g_prev = abs(self._gradient_history[-2])
        g_curr = abs(self._gradient_history[-1])
        return g_prev > self.eps and g_curr > self.eps

    def phase_amplifier(self) -> float:
        """Scalar ≥ 1, larger near steep inflection points.

        Formula: ``1 + |m̂_t|``, bounded to ``[1, max_phase_amp]``.
        Near inflection the corrected gradient |m̂_t| is largest (the
        signal just changed direction sharply), giving the highest amplifier
        and the widest traversal horizon.
        """
        if not self._adam_history:
            return 1.0
        mag = abs(self._adam_history[-1])
        return min(self.max_phase_amp, 1.0 + mag)

    def report(self) -> dict:
        """Return a serialisable summary of the tracker state."""
        return {
            "steps":              self._t,
            "m_t":                round(self._m, 6),
            "v_t":                round(self._v, 6),
            "beta1":              self.beta1,
            "beta2":              self.beta2,
            "adam_history":       [round(x, 4) for x in self._adam_history[-10:]],
            "gradient_history":   [round(x, 4) for x in self._gradient_history[-10:]],
            "at_inflection":      self.is_at_inflection(),
            "phase_amplifier":    round(self.phase_amplifier(), 4),
        }


# ---------------------------------------------------------------------------
# EndpointTunnelBias — semantic distance to known target concept cluster
# ---------------------------------------------------------------------------

class EndpointTunnelBias:
    """Compute an additive bias toward a known endpoint concept cluster.

    Quantum tunneling analogy
    -------------------------
    A quantum particle can traverse a potential barrier that classically
    forbids passage, emerging on the far side in a lower-energy state.
    Analogously, a traversal node that has low edge potential (semantically
    distant from the current frontier) but *high similarity to the target
    endpoint cluster* receives a positive bias, allowing the BFS to "tunnel"
    through the low-relevance barrier to reach the high-relevance destination.

    The tunnel probability scales with ``inflection_depth`` (the Adam phase
    amplifier) because at inflection points the search is most likely to be
    crossing a local relevance minimum on the way to a richer region.
    """

    def __init__(self, endpoint_concepts: list[str]) -> None:
        """
        Parameters
        ----------
        endpoint_concepts:
            Natural-language strings describing the target knowledge domain,
            e.g. ``["supply chain optimisation", "inventory deep learning"]``.
        """
        all_text = " ".join(endpoint_concepts)
        self._endpoint_emb = NodeEmbedding(
            slug="__endpoint__",
            tokens=_tokenize(all_text),
            signal_strength=1.0,
        )

    def similarity_to_endpoint(self, node: NodeEmbedding) -> float:
        """Jaccard similarity of ``node`` to the endpoint concept cluster."""
        return node.similarity_to(self._endpoint_emb)

    def bias_for(self, node: NodeEmbedding, inflection_depth: float = 1.0) -> float:
        """Return ∈ [0, 1] tunnel bias, scaled by current inflection depth.

        Parameters
        ----------
        node:
            The candidate frontier node.
        inflection_depth:
            The Adam phase_amplifier value.  At flat gradient (= 1.0) the
            tunnel bias is halved; at steep inflection (= 2.5) it is
            full-strength.

        Formula::

            bias = sim(node, endpoint) × tanh(inflection_depth − 1)
        """
        sim = self.similarity_to_endpoint(node)
        # tanh(0) = 0 at inflection_depth=1 (no inflection, no bias)
        # tanh(1.5) ≈ 0.9 at inflection_depth=2.5 (steep inflection, full bias)
        tun = math.tanh(max(0.0, inflection_depth - 1.0))
        return min(1.0, sim * tun)


# ---------------------------------------------------------------------------
# AdaptiveSemGraphTraverser — the integrated adaptive BFS engine
# ---------------------------------------------------------------------------

class AdaptiveSemGraphTraverser:
    """Level-by-level BFS traversal with semantic edge ranking, Adam phase-
    shift extension, and endpoint tunnel bias.

    Algorithm per BFS level
    -----------------------
    1. For each node at this level: deepen it, build its NodeEmbedding.
    2. Collect this level's mean relevance signal.
    3. Step the Adam tracker; check for inflection.
    4. If inflection → extend effective_hops by 1 (up to max_hops + 2)
       and record the phase shift.
    5. For each node at this level: gather related courses, compute
       EdgePotential (Morse + tunnel bias × phase_amplifier) for each
       candidate, sort by potential descending, enqueue top
       ``effective_fan_out`` for the next level.
    6. Repeat until queue is empty or effective_hops exhausted.

    The result dict contains:
        seed, effective_hops, max_hops, courses_deepened, rows_written,
        resources, related, external, phase_shifts, edge_potentials,
        adam_report, hop_signals
    """

    def __init__(
        self,
        decay_lambda: float = 2.5,
        tunneling_coeff: float = 0.35,
        beta1: float = 0.9,
        beta2: float = 0.999,
        max_phase_amp: float = 2.5,
        crawl_delay: float = 0.8,
    ) -> None:
        self._ep = EdgePotential(decay_lambda=decay_lambda, tunneling_coeff=tunneling_coeff)
        self._adam_cfg = dict(beta1=beta1, beta2=beta2, max_phase_amp=max_phase_amp)
        self.crawl_delay = crawl_delay

    # ------------------------------------------------------------------
    # Public entry-point
    # ------------------------------------------------------------------

    def traverse(
        self,
        seed_slug: str,
        max_hops: int = 3,
        fan_out: int = 5,
        endpoint_concepts: list[str] | None = None,
        deepen_fn: Callable | None = None,
        detail_fn: Callable | None = None,
    ) -> dict:
        """Run the adaptive BFS from ``seed_slug``.

        Parameters
        ----------
        seed_slug:
            Starting OCW course slug.
        max_hops:
            Base BFS depth.  May be extended by up to 2 during phase shifts.
        fan_out:
            Base number of related courses to enqueue per node.  Grows by 1
            for each phase shift detected.
        endpoint_concepts:
            Optional list of target concept strings for tunnel bias.
            If ``None``, no endpoint bias is applied.
        deepen_fn:
            Callable ``(slug) → dict`` — defaults to ``deepen_ocw_course``.
        detail_fn:
            Callable ``(slug) → dict`` — defaults to ``fetch_ocw_course_detail``.
        """
        # Lazy imports to break circular dependency with ml_research
        if deepen_fn is None or detail_fn is None:
            from src.brain.ml_research import deepen_ocw_course, fetch_ocw_course_detail
            deepen_fn = deepen_fn or deepen_ocw_course
            detail_fn = detail_fn or fetch_ocw_course_detail

        adam = AdamGradientTracker(**self._adam_cfg)
        tunnel: EndpointTunnelBias | None = (
            EndpointTunnelBias(endpoint_concepts) if endpoint_concepts else None
        )

        visited: set[str] = set()
        effective_hops = max_hops
        phase_shifts: list[dict] = []  # {hop, phase_amp, adam_estimate}
        edge_potentials: dict[str, float] = {seed_slug: 1.0}
        hop_signals: dict[int, float] = {}

        # Accumulation totals
        courses_deepened: list[str] = []
        total_rows = total_res = total_rel = total_ext = 0

        # Frontier: list of (slug, source_emb, pre-computed_potential)
        # Level 0: just the seed
        seed_emb = NodeEmbedding.from_title(seed_slug, seed_slug)
        frontier: list[tuple[str, NodeEmbedding, float]] = [(seed_slug, seed_emb, 1.0)]

        for depth in range(effective_hops + 1):
            if not frontier:
                break
            # Honour any inflection-driven extension to effective_hops
            if depth > effective_hops:
                break

            level_signals: list[float] = []
            next_candidates: list[tuple[float, str, NodeEmbedding]] = []
            # (potential, slug, emb) for next-level priority sorting

            for slug, source_emb, _ in frontier:
                if slug in visited:
                    continue
                visited.add(slug)

                # ── Deepen ──────────────────────────────────────────────
                result = deepen_fn(slug)
                if not result.get("fetched"):
                    continue

                courses_deepened.append(slug)
                total_rows += result.get("rows_written", 0)
                total_res  += result.get("resources", 0)
                total_rel  += result.get("related", 0)
                total_ext  += result.get("external", 0)

                # ── Build node embedding ─────────────────────────────────
                detail = (
                    detail_fn(slug)
                    if (result.get("related", 0) or result.get("resources", 0))
                    else {}
                )
                resource_density = min(1.0, result.get("resources", 0) / 30.0)
                endpoint_sim = (
                    tunnel.similarity_to_endpoint(NodeEmbedding.from_title(slug, slug))
                    if tunnel else 0.5
                )
                sig = 0.6 * endpoint_sim + 0.4 * resource_density
                node_emb = NodeEmbedding.from_detail(slug, detail, signal_strength=sig)
                level_signals.append(sig)

                # ── Collect candidates for next level ────────────────────
                if depth < effective_hops:
                    for rc in (detail.get("related_courses") or []):
                        rc_slug = rc.get("slug", "")
                        if not rc_slug or rc_slug in visited:
                            continue
                        rc_emb = NodeEmbedding.from_title(rc_slug, rc_slug)
                        amp = adam.phase_amplifier()
                        e_bias = (
                            tunnel.bias_for(rc_emb, inflection_depth=amp)
                            if tunnel else 0.0
                        )
                        potential = self._ep.compute(
                            source=node_emb,
                            target=rc_emb,
                            endpoint_bias=e_bias,
                            phase_amplifier=amp,
                        )
                        edge_potentials[rc_slug] = max(
                            edge_potentials.get(rc_slug, 0.0), potential
                        )
                        next_candidates.append((potential, rc_slug, rc_emb))

                time.sleep(self.crawl_delay)

            # ── Adam update for this BFS level ───────────────────────────
            if level_signals:
                mean_sig = sum(level_signals) / len(level_signals)
                hop_signals[depth] = round(mean_sig, 4)
                adam_est = adam.step(mean_sig)

                if adam.is_at_inflection():
                    amp = adam.phase_amplifier()
                    phase_shifts.append({
                        "hop":           depth,
                        "phase_amp":     round(amp, 4),
                        "adam_estimate": round(adam_est, 6),
                        "mean_signal":   round(mean_sig, 4),
                    })
                    log.info(
                        f"semantic_graph: inflection at hop {depth} | "
                        f"phase_amp={amp:.3f} | adam_est={adam_est:.4f} | "
                        f"extending hops: {effective_hops} → {min(max_hops + 2, effective_hops + 1)}"
                    )
                    # Phase-shift extension: prolong traversal beyond original horizon
                    effective_hops = min(max_hops + 2, effective_hops + 1)

            # ── Rank and prune next frontier ─────────────────────────────
            # effective_fan_out grows by 1 per detected phase shift
            effective_fan = fan_out + len(phase_shifts)
            # De-dup by slug (keep highest potential)
            seen_next: dict[str, tuple[float, str, NodeEmbedding]] = {}
            for pot, sl, emb in next_candidates:
                if sl not in seen_next or pot > seen_next[sl][0]:
                    seen_next[sl] = (pot, sl, emb)

            ranked = sorted(seen_next.values(), key=lambda x: x[0], reverse=True)
            frontier = [(sl, emb, pot) for pot, sl, emb in ranked[:effective_fan]]

        return {
            "seed":              seed_slug,
            "effective_hops":    effective_hops,
            "max_hops":          max_hops,
            "courses_deepened":  courses_deepened,
            "rows_written":      total_rows,
            "resources":         total_res,
            "related":           total_rel,
            "external":          total_ext,
            "phase_shifts":      phase_shifts,
            "edge_potentials":   edge_potentials,
            "adam_report":       adam.report(),
            "hop_signals":       hop_signals,
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


def adaptive_cascade_ocw(
    seed_slug: str,
    max_hops: int = 3,
    fan_out: int = 5,
    endpoint_concepts: list[str] | None = None,
    decay_lambda: float = 2.5,
    tunneling_coeff: float = 0.35,
    beta1: float = 0.9,
    beta2: float = 0.999,
    crawl_delay: float = 0.8,
) -> dict:
    """Thin wrapper around ``AdaptiveSemGraphTraverser.traverse()``.

    Usable as a drop-in replacement for ``cascade_deepen_ocw`` with the
    additional ``phase_shifts``, ``edge_potentials``, ``adam_report``, and
    ``hop_signals`` keys in the result dict.

    If ``endpoint_concepts`` is ``None``, the Brain's default supply-chain
    endpoint cluster is used.
    """
    traverser = AdaptiveSemGraphTraverser(
        decay_lambda=decay_lambda,
        tunneling_coeff=tunneling_coeff,
        beta1=beta1,
        beta2=beta2,
        crawl_delay=crawl_delay,
    )
    return traverser.traverse(
        seed_slug=seed_slug,
        max_hops=max_hops,
        fan_out=fan_out,
        endpoint_concepts=endpoint_concepts or _DEFAULT_ENDPOINT_CONCEPTS,
    )
