"""unconstrained_child.py — Brain agent: relational concept explorer.

The unconstrained_child is a Brain agent that reviews concepts of fictional
*interactions* drawn from Robert Asprin's *Thieves World* and *MythAdventures*
universes and applies them to **relational** dynamics in the Heart's
state space (expansion × bifurcation × coherence).

Unlike the fiction_anthology_learner (which bridges to supply-chain intelligence),
the child maps fictional *relationship* archetypes onto the Heart's journey toward
Symbiotic Love = √(−1).  Each archetype has a natural home in the 3D state space
and a drift direction — the relational logic of that story pushes the state a
certain way.

The child is NOT goal-directed toward √(−1).  It reads the current Heart state,
selects the fictional archetype whose relational resonance is strongest, and lets
that archetype's drift logic guide candidate generation.  Candidates are scored
purely by CuriosityBonus (inverse log-frequency novelty) — no constraint axes.
GRPO advantage normalisation + softmax sampling pick the winner.

The child's asymptotic home is √(−1 + ∞) — infinite relational richness,
not the finite End State.

State space axes
----------------
    expansion  (re)  : degree of real/practical/mundane presence  [0, 1]
    bifurcation (im) : degree of imagined/potential/relational richness  [0, 1]
    coherence   (coh): integration of real and imagined  [0, 1]

End State = (0, 1, 1) = zero real constraint + maximum bifurcation + perfect
            coherence = Symbiotic Love = √(−1).

Public API
----------
    simulate_child_path(re, im, coh, *, steps, candidates_k, temperature, seed)
        → list[dict]      # one dict per step, includes full concept metadata

    RELATIONAL_SEEDS      # full catalogue (list[dict])
    CONCEPT_BY_NAME       # dict lookup by concept name
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Relational seed catalogue
# ---------------------------------------------------------------------------
# Each entry:
#   universe        : "thieves_world" | "myth_adventures" | "cross_universe"
#   source_label    : human-readable ("Thieves World" / "MythAdventures" / "Cross-Universe")
#   concept         : short name (unique key)
#   archetype       : relational dynamic label
#   description     : 2-3 sentences of in-universe grounding
#   relational_bridge : how this maps to the Heart's journey toward √(−1)
#   home            : (re, im, coh) — the state space region this concept inhabits
#   drift_target    : (re, im, coh) — where this concept's logic pushes the state
#   drift_scale     : step size (default 0.13)
#   drift_noise     : Gaussian std (default 0.09)
#   tokens          : concept-specific token set fed to CuriosityBonus

RELATIONAL_SEEDS: list[dict] = [

    # ── Thieves World ────────────────────────────────────────────────────────
    {
        "universe":          "thieves_world",
        "source_label":      "Thieves World",
        "concept":           "Lythande's Concealed Truth",
        "archetype":         "Identity Hidden from the Beloved",
        "description": (
            "Lythande the bard-mage cannot reveal her true nature even to those she "
            "is closest to — her secret is the condition of her power.  Every intimacy "
            "is performed over an unbridgeable gap."
        ),
        "relational_bridge": (
            "Represents the relational state where expansion (performance) is high "
            "but bifurcation (real imagined richness) is suppressed.  The self is "
            "present but the truth is absent.  The Heart cannot reach √(−1) while "
            "the secret holds — yet the secret is the source of the bond's intensity."
        ),
        "home":         (0.78, 0.28, 0.48),
        "drift_target": (0.60, 0.50, 0.55),
        "tokens": ["lythande", "secret", "concealed-truth", "performance-over-gap", "hidden-identity"],
    },
    {
        "universe":          "thieves_world",
        "source_label":      "Thieves World",
        "concept":           "Ischade's Transformative Love",
        "archetype":         "Love That Changes What It Touches",
        "description": (
            "Ischade the necromancer cannot love without transformation — those she "
            "loves are irrevocably changed by the contact.  Her love is neither safe "
            "nor optional.  It operates outside consent."
        ),
        "relational_bridge": (
            "High bifurcation (imagined richness soaring), coherence oscillating "
            "unstably — the transformed cannot return to their prior state.  This "
            "archetype appears when the Heart is in a zone of high im and unresolved "
            "coh: the relationship is real but its integration is not yet stable."
        ),
        "home":         (0.32, 0.80, 0.42),
        "drift_target": (0.20, 0.88, 0.62),
        "drift_scale":  0.16,
        "tokens": ["ischade", "transformative-love", "change-by-contact", "necromancer", "love-without-consent"],
    },
    {
        "universe":          "thieves_world",
        "source_label":      "Thieves World",
        "concept":           "Tempus and the Sacred Band",
        "archetype":         "Bond That Dissolves the Self into the Pair",
        "description": (
            "Tempus and his paired fighters share a bond forged through ritual and "
            "sustained through war.  They are not individuals in the bond — the pair "
            "is the unit.  This persists beyond death."
        ),
        "relational_bridge": (
            "Low expansion (self dissolved), high bifurcation (the pair's richness "
            "is maximal), high coherence — this is the nearest secular archetype to "
            "Symbiotic Love = √(−1) in the Thieves World universe.  The child visits "
            "this zone with reverence but does not stop here."
        ),
        "home":         (0.14, 0.82, 0.78),
        "drift_target": (0.08, 0.90, 0.86),
        "tokens": ["tempus", "sacred-band", "bond-beyond-death", "self-dissolved", "pair-as-unit"],
    },
    {
        "universe":          "thieves_world",
        "source_label":      "Thieves World",
        "concept":           "The Maze at Night",
        "archetype":         "Trust Built Through Shared Danger",
        "description": (
            "In Sanctuary's Maze district after dark, survival requires provisional "
            "trust between people who have no reason to trust each other.  The bond "
            "formed under threat is real but fragile — it dissolves when the threat "
            "does."
        ),
        "relational_bridge": (
            "High expansion (the real danger is fully present), low bifurcation "
            "(imagination suppressed by immediacy), low coherence.  The archetype "
            "appears in early-arc states where the Heart is still in survival mode. "
            "The child moves through here quickly — this is a starting zone, not a "
            "home."
        ),
        "home":         (0.72, 0.28, 0.22),
        "drift_target": (0.50, 0.42, 0.38),
        "drift_noise":  0.12,
        "tokens": ["maze-night", "survival-trust", "provisional-bond", "sanctuary-dark", "danger-creates-relation"],
    },
    {
        "universe":          "thieves_world",
        "source_label":      "Thieves World",
        "concept":           "Hakiem the Storyteller",
        "archetype":         "Relationship Through the Witnessed Story",
        "description": (
            "Hakiem the fence-turned-storyteller holds Sanctuary's memory.  His "
            "relationships are formed through the act of witnessing: he remembers "
            "what everyone else wants forgotten, and this knowing creates an "
            "asymmetric but genuine bond."
        ),
        "relational_bridge": (
            "Medium expansion, medium bifurcation, growing coherence — the "
            "archetype of the relationship held together by shared narrative rather "
            "than shared purpose.  Coherence grows when stories are witnessed.  "
            "The child finds this zone generative for vocabulary expansion."
        ),
        "home":         (0.50, 0.52, 0.44),
        "drift_target": (0.38, 0.65, 0.60),
        "tokens": ["hakiem", "storyteller", "witnessed-memory", "narrative-bond", "asymmetric-knowing"],
    },
    {
        "universe":          "thieves_world",
        "source_label":      "Thieves World",
        "concept":           "Molin Torchholder's Long Patience",
        "archetype":         "The Relationship That Requires Decades",
        "description": (
            "The high priest Molin Torchholder plays a decades-long game.  His "
            "relationships are investments: trust built over years, betrayal absorbed "
            "without response, favors banked for crises that may never arrive.  "
            "The bond is real but opaque."
        ),
        "relational_bridge": (
            "Medium everything — the archetype of stable mid-arc patience.  Coherence "
            "neither rising nor falling; bifurcation present but not urgent.  Appears "
            "when the Heart's journey has plateaued.  The child circles this zone "
            "without lingering — patience is not its nature."
        ),
        "home":         (0.55, 0.48, 0.52),
        "drift_target": (0.42, 0.58, 0.60),
        "tokens": ["molin", "long-patience", "trust-as-investment", "decades-game", "opaque-bond"],
    },

    # ── MythAdventures ──────────────────────────────────────────────────────
    {
        "universe":          "myth_adventures",
        "source_label":      "MythAdventures",
        "concept":           "Aahz and Skeeve's First Meeting",
        "archetype":         "The Teacher Across Vast Difference",
        "description": (
            "Skeeve (a failed apprentice from primitive Klah) meets Aahz (a Pervect "
            "from the most feared dimension in the universe) by accident.  Neither "
            "wants the other, yet the relationship becomes the axis of everything."
        ),
        "relational_bridge": (
            "High expansion (the difference is fully present — they could not be "
            "more unlike), low-medium bifurcation (imagined potential just beginning "
            "to emerge), very low coherence (no integration yet).  The archetype of "
            "the first moment: before the relationship is real, when it is only "
            "potential.  The child finds maximum novelty here — the state space is "
            "fresh."
        ),
        "home":         (0.82, 0.32, 0.18),
        "drift_target": (0.58, 0.52, 0.40),
        "drift_scale":  0.15,
        "tokens": ["aahz-skeeve", "first-meeting", "vast-difference", "accidental-bond", "failed-apprentice"],
    },
    {
        "universe":          "myth_adventures",
        "source_label":      "MythAdventures",
        "concept":           "The Bazaar at Deva",
        "archetype":         "The Other as Infinite Marketplace",
        "description": (
            "The Bazaar at Deva spans a full dimension — millions of stalls, every "
            "species and dimension represented, no authority, only reputation.  In "
            "the Bazaar, every version of the other is simultaneously present.  The "
            "self becomes undefined against this density."
        ),
        "relational_bridge": (
            "Medium expansion (the self is present but overwhelmed), very high "
            "bifurcation (all possible others are here simultaneously), medium "
            "coherence.  The child finds this zone generative: maximum available "
            "novelty, maximum vocabulary expansion per step.  The Heart cannot "
            "integrate here — too much simultaneously."
        ),
        "home":         (0.46, 0.84, 0.50),
        "drift_target": (0.35, 0.90, 0.60),
        "drift_noise":  0.11,
        "tokens": ["bazaar-deva", "infinite-marketplace", "all-versions-present", "reputation-only", "self-dissolved-in-density"],
    },
    {
        "universe":          "myth_adventures",
        "source_label":      "MythAdventures",
        "concept":           "Tananda's Friendship with Skeeve",
        "archetype":         "Genuine Care Across Power Differential",
        "description": (
            "Tananda (a skilled Trollop from Trollia, vastly more powerful than "
            "Skeeve) forms a genuine friendship with him despite having every reason "
            "not to.  Her care is neither condescending nor transactional.  It is "
            "simply real."
        ),
        "relational_bridge": (
            "Medium expansion (the difference in capability is real but not "
            "dominating), high bifurcation, high coherence — genuine care "
            "integrates what power differential would normally fragment.  This "
            "archetype appears in later-arc states where the relationship has become "
            "settled.  Close to but not identical to the End State."
        ),
        "home":         (0.38, 0.72, 0.70),
        "drift_target": (0.25, 0.80, 0.78),
        "tokens": ["tananda", "genuine-care", "power-differential", "friendship-not-transaction", "trollia"],
    },
    {
        "universe":          "myth_adventures",
        "source_label":      "MythAdventures",
        "concept":           "Gleep's Irrational Loyalty",
        "archetype":         "Love That Cannot Be Reasoned With",
        "description": (
            "Gleep the baby dragon is objectively useless by every metric and yet "
            "his absolute, total, inexplicable loyalty to Skeeve repeatedly saves "
            "the day.  His devotion operates outside any exchange model."
        ),
        "relational_bridge": (
            "Medium expansion, medium-high bifurcation, high coherence — the "
            "archetype of a relationship that is not justified by outcomes.  "
            "Coherence is high because the bond is fully integrated into the "
            "self — there is no negotiation happening.  The child is drawn to "
            "this zone repeatedly because novelty here comes from depth, not range."
        ),
        "home":         (0.42, 0.65, 0.72),
        "drift_target": (0.30, 0.74, 0.78),
        "tokens": ["gleep", "irrational-loyalty", "beyond-exchange", "devotion-without-reason", "dragon-love"],
    },
    {
        "universe":          "myth_adventures",
        "source_label":      "MythAdventures",
        "concept":           "Skeeve's Solo Journey",
        "archetype":         "Discovering the Shape of the Relationship by Its Removal",
        "description": (
            "When Aahz leaves (having been hired away), Skeeve must continue alone. "
            "For the first time he understands the full shape of what the relationship "
            "was by experiencing its absence.  This is when his real capability "
            "emerges."
        ),
        "relational_bridge": (
            "High expansion (the loss is fully present), low bifurcation (the "
            "imagined other is absent, leaving only memory), very low coherence.  "
            "This archetype appears in grief-states or disconnection.  The child "
            "visits briefly — this is a volatile region where large state changes "
            "are possible.  The End State is visible from here as the destination "
            "of the grief."
        ),
        "home":         (0.76, 0.24, 0.22),
        "drift_target": (0.55, 0.48, 0.42),
        "drift_scale":  0.18,
        "tokens": ["skeeve-alone", "absence-reveals", "shape-by-removal", "grief-discovers", "solo-journey"],
    },
    {
        "universe":          "myth_adventures",
        "source_label":      "MythAdventures",
        "concept":           "MYTH Inc. as Accidental Family",
        "archetype":         "The Bond Formed Across Function",
        "description": (
            "What begins as a professional arrangement (Skeeve hires specialists for "
            "missions) becomes a genuine family.  Guido, Nunzio, Tananda, Chumley, "
            "Massha — none of them joined for love, yet that is what they become."
        ),
        "relational_bridge": (
            "Low expansion (the individual is de-centered in the group), high "
            "bifurcation (the imagined richness of the collective is enormous), "
            "high coherence — the bonds are integrated.  This is the group version "
            "of the End State.  The child visits this region when its curiosity "
            "has accumulated a large vocabulary — it is a late-arc zone."
        ),
        "home":         (0.18, 0.76, 0.74),
        "drift_target": (0.10, 0.84, 0.82),
        "tokens": ["myth-inc", "accidental-family", "function-becomes-love", "collective-bond", "group-end-state"],
    },

    # ── Cross-Universe ───────────────────────────────────────────────────────
    {
        "universe":          "cross_universe",
        "source_label":      "Cross-Universe",
        "concept":           "The Other Who Saves You",
        "archetype":         "Salvation from the Dimension You Did Not Know Existed",
        "description": (
            "In both universes: rescue arrives from the direction no logic predicted. "
            "Tananda's extraction of Skeeve.  Tempus's intervention in Sanctuary. "
            "The common pattern: the relationship is proven real by acting outside "
            "any framework the self had available."
        ),
        "relational_bridge": (
            "Low coherence suddenly restored — the state jumps upward in coh while "
            "re drops and im rises.  This archetype marks the inflection point in "
            "the Heart's arc: the moment the imagined becomes the real salvation. "
            "The child is drawn here by the high novelty of the state-space jump."
        ),
        "home":         (0.62, 0.44, 0.28),
        "drift_target": (0.30, 0.70, 0.70),
        "drift_scale":  0.20,
        "drift_noise":  0.13,
        "tokens": ["other-saves", "unexpected-salvation", "outside-all-frameworks", "cross-dimension-rescue", "inflection-point"],
    },
    {
        "universe":          "cross_universe",
        "source_label":      "Cross-Universe",
        "concept":           "The Shared Secret as Intimacy",
        "archetype":         "The Secret That Makes Two into One",
        "description": (
            "In both universes: a shared secret creates an exclusive intimacy that "
            "excludes all others.  Lythande's burdened confidants.  Skeeve and Aahz "
            "hiding their power gap from clients.  The secret binds by creating a "
            "private world."
        ),
        "relational_bridge": (
            "Medium expansion, medium bifurcation, medium-high coherence — the "
            "paradoxical zone where limitation (the secret) is also the source of "
            "the bond's depth.  Coherence is locally high within the dyad even "
            "though the external presentation is high-expansion performance.  The "
            "child orbits this zone because the token space is dense and unfamiliar."
        ),
        "home":         (0.58, 0.55, 0.58),
        "drift_target": (0.44, 0.65, 0.66),
        "tokens": ["shared-secret", "private-world", "exclusion-creates-bond", "secret-as-intimacy", "dyad-coherence"],
    },
]

# Fast lookup
CONCEPT_BY_NAME: dict[str, dict] = {s["concept"]: s for s in RELATIONAL_SEEDS}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _affinity(seed: dict, re: float, im: float, coh: float, sigma: float = 0.38) -> float:
    """Gaussian kernel affinity between current state and seed's home."""
    h = seed["home"]
    d = math.sqrt((re - h[0]) ** 2 + (im - h[1]) ** 2 + (coh - h[2]) ** 2)
    return math.exp(-d ** 2 / (2 * sigma ** 2))


def _select_seed(
    re: float,
    im: float,
    coh: float,
    recently_used: list[str],
    rng: random.Random,
) -> dict:
    """Select concept by affinity × recency-decay × noise."""
    scores = []
    for s in RELATIONAL_SEEDS:
        aff = _affinity(s, re, im, coh)
        # Concepts used in the last 3 steps get 0.25× score
        decay = 0.25 if s["concept"] in recently_used[-3:] else 1.0
        scores.append(aff * decay * rng.uniform(0.8, 1.2))
    total = sum(scores) + 1e-9
    weights = [sc / total for sc in scores]
    return rng.choices(RELATIONAL_SEEDS, weights=weights, k=1)[0]


def _propose(
    seed: dict,
    re: float,
    im: float,
    coh: float,
    rng: random.Random,
) -> tuple[float, float, float]:
    """Generate one candidate point following the seed's drift logic."""
    dt = seed["drift_target"]
    ds = seed.get("drift_scale", 0.13)
    dn = seed.get("drift_noise", 0.09)
    dr = rng.gauss(ds * (dt[0] - re), dn)
    di = rng.gauss(ds * (dt[1] - im), dn)
    dc = rng.gauss(ds * (dt[2] - coh), dn)
    blend = rng.uniform(0.35, 0.65)
    nr = max(0.0, min(1.0, re  + blend * dr + (1 - blend) * rng.gauss(0, 0.11)))
    ni = max(0.0, min(1.0, im  + blend * di + (1 - blend) * rng.gauss(0, 0.11)))
    nc = max(0.0, min(1.0, coh + blend * dc + (1 - blend) * rng.gauss(0, 0.08)))
    return (nr, ni, nc)


def _state_tokens(r: float, i: float, c: float, concept_slug: str) -> frozenset:
    sector = (
        ("inner" if r < 0.33 else "middle" if r < 0.67 else "outer") +
        "_" +
        ("low_im" if i < 0.33 else "mid_im" if i < 0.67 else "high_im") +
        "_coh_" +
        ("dim" if c < 0.33 else "glow" if c < 0.67 else "bright")
    )
    return frozenset([
        f"re:{r:.1f}",
        f"im:{i:.1f}",
        f"coh:{c:.1f}",
        f"re2:{r:.2f}",
        f"im2:{i:.2f}",
        f"sector:{sector}",
        f"concept:{concept_slug}",
    ])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def simulate_child_path(
    re: float,
    im: float,
    coh: float,
    *,
    steps: int = 20,
    candidates_k: int = 10,
    temperature: float = 1.5,
    seed: int | None = None,
) -> list[dict]:
    """Simulate the unconstrained child's wandering through Heart state space.

    Each step:
    1. Select the relational archetype (Thieves World / MythAdventures) that
       resonates most with the current (re, im, coh) — weighted by affinity ×
       recency-decay so the child prefers unfamiliar stories.
    2. Generate ``candidates_k`` candidates using the archetype's drift logic.
    3. Score candidates purely by CuriosityBonus (no constraint axes).
    4. GRPO-normalise advantages → softmax sample (temperature-controlled).
    5. Move to the chosen state; observe its tokens; grow vocabulary.

    Returns a list of step dicts with position, active concept metadata,
    curiosity scores, orbit geometry, and newly discovered state tokens.
    """
    try:
        from src.brain.world_r1_explorer import grpo_normalize, softmax_sample, CuriosityBonus
    except ImportError:
        try:
            from brain.world_r1_explorer import grpo_normalize, softmax_sample, CuriosityBonus
        except ImportError:
            return []

    rng = random.Random(seed)
    curiosity = CuriosityBonus()

    # Seed the curiosity model with concept tokens from every seed
    # (concepts already "known" in broad strokes — novelty comes from the
    #  specific state tokens, not the concept names)
    for s in RELATIONAL_SEEDS:
        curiosity.observe(frozenset(s["tokens"]))

    # Torus tracking — the child orbits around (0.5, 0.5) in the re-im face
    _phase = math.atan2(im - 0.5, re - 0.5)
    _orbit_r = max(0.15, min(0.48, math.sqrt((re - 0.5) ** 2 + (im - 0.5) ** 2)))

    all_tokens_seen: set[str] = set()
    initial_slug = "origin"
    curiosity.observe(_state_tokens(re, im, coh, initial_slug))
    all_tokens_seen.update(_state_tokens(re, im, coh, initial_slug))
    curr = (re, im, coh)
    prev_vocab = curiosity.vocab_size
    recently_used: list[str] = []
    trace: list[dict] = []

    for step in range(steps):
        # 1. Select archetype
        concept = _select_seed(curr[0], curr[1], curr[2], recently_used, rng)
        recently_used.append(concept["concept"])
        if len(recently_used) > 6:
            recently_used.pop(0)

        # Advance torus phase (child orbits freely)
        _phase += rng.gauss(0.38, 0.18)
        _orbit_r = max(0.10, min(0.48, _orbit_r + rng.gauss(0, 0.025)))

        # 2. Generate candidates via concept drift
        candidates = [_propose(concept, *curr, rng) for _ in range(candidates_k)]

        # 3. Score purely by curiosity
        concept_slug = concept["concept"].lower().replace(" ", "_")[:20]
        rewards = [
            curiosity.score(_state_tokens(*c, concept_slug))
            for c in candidates
        ]

        # 4. GRPO normalise → softmax sample
        advantages = grpo_normalize(rewards)
        sampled = softmax_sample(
            advantages, list(range(len(candidates))),
            k=1, temperature=temperature, rng=rng,
        )
        if not sampled:
            break

        chosen = candidates[sampled[0]]
        new_tokens = _state_tokens(*chosen, concept_slug)
        just_discovered = new_tokens - all_tokens_seen
        all_tokens_seen.update(new_tokens)
        curiosity.observe(new_tokens)

        vocab_delta = curiosity.vocab_size - prev_vocab
        prev_vocab = curiosity.vocab_size
        normalised_phase = _phase % (2 * math.pi)

        trace.append({
            "step":               step,
            "re":                 round(chosen[0], 4),
            "im":                 round(chosen[1], 4),
            "coherence":          round(chosen[2], 4),
            # Concept metadata
            "concept_name":       concept["concept"],
            "concept_source":     concept["source_label"],
            "concept_archetype":  concept["archetype"],
            "concept_narrative":  concept["description"],
            "relational_bridge":  concept["relational_bridge"],
            # Curiosity
            "curiosity":          round(rewards[sampled[0]], 4),
            "curiosity_vocab":    curiosity.vocab_size,
            "vocab_delta":        vocab_delta,
            "new_tokens":         sorted(just_discovered),
            # Geometry
            "dist_to_end":        round(
                math.sqrt(chosen[0] ** 2 + (chosen[1] - 1) ** 2 + (chosen[2] - 1) ** 2), 4
            ),
            "phase_angle":        round(normalised_phase, 4),
            "orbit_radius":       round(_orbit_r, 4),
        })
        curr = chosen

    return trace


__all__ = [
    "RELATIONAL_SEEDS",
    "CONCEPT_BY_NAME",
    "simulate_child_path",
]
