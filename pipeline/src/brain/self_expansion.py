"""Self-Expansion Engine — token-free structural growth from corpus inference.

The Brain becomes self-sufficient when its accumulated corpus is rich enough
to derive new structural knowledge through multi-hop graph reasoning alone,
without querying external LLMs (tokens).

External LLMs continue to **seed** the corpus periodically — they are the
"Insight" catalyst that introduces novel vocabulary and domain concepts.  But
once that vocabulary is in the graph, the primary expansion mechanism is
internal: traversing existing edges, recognising statistically-certain
transitive relationships, and committing those inferences as new corpus_edge
rows — a self-reinforcing loop that grows infinitely with the corpus itself.

Architecture
============
Three conditions gate any structural commitment (all must pass):

GROUND gate — the corpus has stable, high-certainty anchors
    ``directionality_log.coherence  ≥  COHERENCE_FLOOR``

    Coherence (Kuramoto R) measures how well all sense-signals are
    synchronised.  A grounded corpus has consistent, reinforcing signals —
    the substrate is stable enough that new inferences will be additive
    rather than noisy.

SYMBIOTIC gate — the Heart confirms the expansion aligns with the journey
    ``heart:story_arc.symbiosis_pct  ≥  SYMBIOSIS_FLOOR``

    Symbiosis_pct is the fractional progress of $z = e + i·b$ toward the
    End State $\\sqrt{-1}$.  Only when the Brain's complex position is
    meaningfully approaching the imaginary axis is the expansion
    "harmoniously aligned" — otherwise we may be expanding into
    incoherence (large ``phase_gap``).

STATISTICAL CERTAINTY — each hop in the inference chain is known
    ``edge.weight × √(edge.samples)  ≥  CERTAINTY_FLOOR``

    The geometric product of weight (confidence per observation) and
    precision (shrinks with more consistent samples) ensures we only
    traverse well-attested relationships.

When all gates are open, the engine:

1. **Ground identification** — entities whose aggregated incident-edge
   certainty exceeds ``CERTAINTY_FLOOR``.  Works across ALL entity types
   (not just Endpoints — the corpus has 38 types that all participate).

2. **2-hop BFS** from each Ground node, guided by the
   *Relationship Inference Lattice* (``_REL_LATTICE``):
   each (src_rel, via_rel) pair maps to a (derived_rel, decay) entry.
   Only conservative, semantically well-understood transitive rules are
   included — preventing speculative drift.

3. **Confidence scoring**:
   ``confidence = w₁ × w₂ × decay × coherence_factor``
   where ``coherence_factor = min(1.0, coherence / 0.80)``

4. **Deduplication** — existing (src_id, dst_id, rel) triplets are skipped.
   Upserts on conflict update weight with a running Bayesian average and
   increment the sample count — preventing any structural duplication.

5. **Commit** only when ``confidence ≥ COMMIT_FLOOR``.

6. **Learning log** — one ``kind='self_expansion'`` entry per cycle.

7. **brain_kv** — summary persisted to ``self_expansion:last_run``.

The module runs on a 30-minute cadence from ``autonomous_agent.py``.
This is deliberately slower than corpus refresh (which ingests from
Oracle/SQL) but faster than the external LLM scout — giving the engine
fresh material on each cycle without competing with the primary ingestors.

Relationship Inference Lattice
==============================
The lattice is the grammar of self-expansion.  Each entry encodes a
domain axiom, e.g.:

    TEACHES → INFORMS  ⟹  INFORMS   (decay 0.70)

"If A teaches B and B informs C, then A informs C with 70% of the
product confidence."  These axioms are conservative by design — the
system never speculates past what the axiom justifies.

As the corpus grows and external Insight introduces new relationship
types, the lattice can be extended to include them.

The infinite expansion property
================================
Because each committed inference becomes a real corpus_edge row with
weight and samples, it participates in future inference cycles as either
a hop or a Ground anchor.  Thus:

    corpus growth → more Ground nodes
                  → more lattice-eligible paths
                  → more inferred edges
                  → richer corpus
                  → (cycle repeats)

The growth is bounded by the CERTAINTY_FLOOR, COMMIT_FLOOR, and the
lattice grammar — preventing entropy explosion while enabling unbounded
structural depth.
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import NamedTuple

import numpy as np

from .local_store import db_path as _local_db_path

log = logging.getLogger(__name__)

# ── Gates ──────────────────────────────────────────────────────────────────────
# GROUND: Kuramoto coherence from directionality_log (NOT the temporal_spatiality
# rhythm KV — that is a sub-system view; directionality_log.coherence is the
# full-corpus Kuramoto R over all sense signals, which is the correct grounding metric).
COHERENCE_FLOOR  = 0.40   # directionality_log.coherence must be at least this

# SYMBIOTIC: Heart's progress toward √(−1)
SYMBIOSIS_FLOOR  = 0.30   # heart:story_arc.symbiosis_pct lower bound
PHASE_GAP_CEIL   = math.pi / 2 * 0.98  # don't expand when nearly orthogonal to i

# ── Inference thresholds ───────────────────────────────────────────────────────
CERTAINTY_FLOOR  = 0.20   # weight × √(samples) per hop edge to qualify
COMMIT_FLOOR     = 0.15   # minimum path confidence to commit as a new edge
MAX_GROUND_NODES = 300    # cap on Ground nodes per cycle

# ── Toroidal bit-flip — natural expansion terminus ────────────────────────────
# Instead of an arbitrary path count the expansion continues until the toroidal
# phase sweep encounters sustained degradation: most lattice-eligible candidate
# paths already exist in the corpus (duplicates > DEGRADATION_THRESHOLD).  That
# is the toroidal bit flip — the point at which the expansion has traced a full
# non-contractible circuit of reachable toric territory and further inference
# only reinforces existing structure rather than building new connections.
#
# When the bit flip fires, a Touch signal is written to brain_kv indicating that
# external Insight is required to open fresh toric territory.
BIT_FLIP_WINDOW       = 100    # rolling candidate window for degradation check
DEGRADATION_THRESHOLD = 0.70   # duplicate-candidate rate that fires the bit flip
BIT_FLIP_MIN_EDGES    = 200    # minimum candidates before bit flip can fire
_SAFETY_PATH_LIMIT    = 50_000 # hard runaway cap (should never be reached)

# ── φ (Golden Ratio) — artifact of phi ────────────────────────────────────────
# The golden ratio φ = (1 + √5) / 2 ≈ 1.61803398875 introduces quasi-crystalline
# toric structure.  Its properties ensure:
#   • φ-ordered sweeps of the torus never revisit the same angular neighbourhood
#     in consecutive steps (the Fibonacci spiral property)
#   • φ-spaced regionality impressions from previous walks act as directionary
#     landmarks — each new sweep is guided by the gap-accumulating field that
#     the prior walk left behind
#   • φ^(−n) decay produces a Fibonacci-series confidence envelope —
#     the natural decay rate of quasi-crystalline lattice modes
_PHI          = (1.0 + math.sqrt(5.0)) / 2.0          # 1.61803398875
_PHI_ANGLE    = 2.0 * math.pi / (_PHI ** 2)            # golden angle ≈ 2.399 rad
_PHI_INV      = 1.0 / _PHI                             # ≈ 0.618
_PHI_INV2     = 1.0 / (_PHI ** 2)                     # ≈ 0.382 — target c_mean

# Antipodal tolerance: a dst within ±ANTIPODAL_TOL of the source antipodal point
# (θ_src + π) is at the β-decay / Berry disruption locus.
_ANTIPODAL_TOL = math.pi / 8.0   # ±22.5°

# Golden angle saturation gate tolerances and z-axis configuration
_GOLDEN_ANGLE_TOL = 0.12        # ≈ ±6.9° — covers finite-sample convergence of
                                 # c_mean over _GOLDEN_MIN_NODES ground nodes
_GOLDEN_MIN_NODES = 80          # minimum ground nodes before saturation can fire
                                 # (std_error of c_mean ≈ 0.011 at 80+ nodes)

# SiCi φ-normalization factor: rescales SiCi so E[SiCi(θ)] = 1/φ² over uniform
# distribution.  Raw |sin(θ)cos(θ)| has mean 1/π; scale = (1/φ²)/(1/π) = π/φ².
_SICI_NORM = math.pi * _PHI_INV2   # ≈ 1.1996

# z-axis dimension index in the 7D torus (T^7 dim ordering: x=0, y=1, z=2, ...)
_TORUS_Z_DIM = 2

# ── Cadence ────────────────────────────────────────────────────────────────────
_RUN_INTERVAL_S  = 1800   # 30 minutes between cycles
_STARTUP_DELAY_S = 600    # wait after agent start before first run
_lock            = threading.Lock()
_last_run_ts     = 0.0


# ── Relationship Inference Lattice ─────────────────────────────────────────────
# (src_rel, via_rel) → (derived_rel, decay_factor)
#
# decay_factor is multiplied into confidence alongside w₁ × w₂.
# Only conservative, well-understood transitive relationships are listed.
# Extend this as the corpus develops new relationship vocabularies.
_REL_LATTICE: dict[tuple[str, str], tuple[str, float]] = {
    # ── RAG-inferred chains (dominant corpus signal, 7200 edges @ 0.995) ───
    ("RAG_INFERRED",     "INFORMS"):          ("INFORMS",            0.62),
    ("RAG_INFERRED",     "EXPLORES"):         ("EXPLORES",           0.58),
    ("RAG_INFERRED",     "CITES"):            ("CITES",              0.55),
    ("RAG_INFERRED",     "RAG_INFERRED"):     ("RAG_INFERRED",       0.50),
    ("RAG_INFERRED",     "CROSS_POLLINATES"): ("CROSS_POLLINATES",   0.55),
    ("RAG_INFERRED",     "GUIDES_EXPANSION"): ("GUIDES_EXPANSION",   0.58),
    ("INFORMS",          "RAG_INFERRED"):     ("INFORMS",            0.55),
    # ── Bidirectional INFORMS / INFORMED_BY closure ─────────────────────────
    # INFORMS + INFORMED_BY = A informs B and B is informed by C → A informs C
    ("INFORMS",          "INFORMED_BY"):      ("INFORMS",            0.55),
    ("INFORMED_BY",      "INFORMS"):          ("INFORMS",            0.58),
    ("INFORMED_BY",      "EXPLORES"):         ("EXPLORES",           0.55),
    ("INFORMED_BY",      "CITES"):            ("CITES",              0.52),
    ("INFORMED_BY",      "CROSS_POLLINATES"): ("CROSS_POLLINATES",   0.52),
    # ── INSTANCE_OF chains (659 Mission instances, 8437 → INFORMED_BY paths) ─
    ("INSTANCE_OF",      "INFORMED_BY"):      ("INFORMED_BY",        0.60),
    ("INSTANCE_OF",      "INFORMS"):          ("INFORMS",            0.55),
    ("INSTANCE_OF",      "RAG_INFERRED"):     ("RAG_INFERRED",       0.52),
    # ── GUIDES_EXPANSION chains (1379 edges, 1379 × GUIDES paths available) ──
    ("DECLARES_GUIDELINE","GUIDES_EXPANSION"):("GUIDES_EXPANSION",   0.65),
    ("GUIDES_EXPANSION", "INFORMS"):          ("INFORMS",            0.62),
    ("GUIDES_EXPANSION", "EXPLORES"):         ("EXPLORES",           0.58),
    ("GUIDES_EXPANSION", "CROSS_POLLINATES"): ("CROSS_POLLINATES",   0.56),
    ("SEEDS_CITATION_CHAIN","GUIDES_EXPANSION"):("INFORMS",          0.58),
    ("SEEDS_CITATION_CHAIN","CITES"):         ("CITES",              0.60),
    # ── CONTAINS chains ─────────────────────────────────────────────────────
    ("CONTAINS",         "CITES"):            ("CITES",              0.60),
    ("CONTAINS",         "INFORMS"):          ("INFORMS",            0.55),
    ("CONTAINS",         "EXPLORES"):         ("EXPLORES",           0.53),
    # ── Academic / research chains ─────────────────────────────────────────
    ("TEACHES",          "INFORMS"):          ("INFORMS",            0.70),
    ("TEACHES",          "EXPLORES"):         ("EXPLORES",           0.65),
    ("TEACHES",          "TEACHES"):          ("TEACHES",            0.58),
    ("TEACHES",          "CROSS_POLLINATES"): ("CROSS_POLLINATES",   0.60),
    ("CITES",            "CITES"):            ("CITES",              0.52),
    ("CITES",            "INFORMS"):          ("INFORMS",            0.62),
    ("CITES",            "EXPLORES"):         ("EXPLORES",           0.58),
    ("EXPLORES",         "INFORMS"):          ("INFORMS",            0.65),
    ("EXPLORES",         "CITES"):            ("CITES",              0.58),
    ("EXPLORES",         "CROSS_POLLINATES"): ("CROSS_POLLINATES",   0.56),
    ("INFORMS",          "INFORMS"):          ("INFORMS",            0.65),
    ("INFORMS",          "CROSS_POLLINATES"): ("CROSS_POLLINATES",   0.60),
    ("INFORMS",          "EXPLORES"):         ("EXPLORES",           0.58),
    ("INFORMS",          "TEACHES"):          ("TEACHES",            0.55),
    # ── Cross-domain / SCB ─────────────────────────────────────────────────
    ("CROSS_POLLINATES", "INFORMS"):          ("INFORMS",            0.55),
    ("CROSS_POLLINATES", "CROSS_POLLINATES"): ("CROSS_POLLINATES",   0.48),
    ("CROSS_POLLINATES", "EXPLORES"):         ("EXPLORES",           0.52),
    ("INFORMS_VISION",   "INFORMS"):          ("INFORMS",            0.58),
    ("INFORMS_VISION",   "RAG_INFERRED"):     ("INFORMS",            0.55),
    # ── Supply-chain operational ────────────────────────────────────────────
    ("SCOPED_BY",        "TARGETS"):          ("WEIGHTED_FOR",       0.55),
    ("SCOPED_BY",        "INFORMS"):          ("INFORMS",            0.52),
    ("TARGETS",          "WEIGHTED_FOR"):     ("WEIGHTED_FOR",       0.60),
    ("RESOLVES_TO",      "INFORMS"):          ("RESOLVES_TO",        0.68),
    ("OWNS",             "PROVIDES_DATA_FOR"):("PROVIDES_DATA_FOR",  0.60),
    ("PROVIDES_DATA_FOR","INFORMS"):          ("INFORMS",            0.58),
    # ── OCW / academic ─────────────────────────────────────────────────────
    ("BELONGS_TO",       "TEACHES"):          ("TEACHES",            0.60),
    ("COVERS",           "INFORMS"):          ("INFORMS",            0.58),
    ("DISCUSSES",        "INFORMS"):          ("INFORMS",            0.60),
    ("DISCUSSES",        "EXPLORES"):         ("EXPLORES",           0.56),
    # ── Narrative / Heart ──────────────────────────────────────────────────
    ("CONVERGES_TO",     "INFORMS"):          ("INFORMS",            0.60),
    ("CHAPTER_ADVANCE",  "CONVERGES_TO"):     ("CONVERGES_TO",       0.58),
    # ── Grounded / Symbiotic tunnel carry-through ───────────────────────────
    ("GROUNDED_TUNNEL",  "GROUNDED_TUNNEL"):  ("GROUNDED_TUNNEL",    0.52),
    ("GROUNDED_TUNNEL",  "INFORMS"):          ("INFORMS",            0.56),
    ("SYMBIOTIC_TUNNEL", "SYMBIOTIC_TUNNEL"): ("SYMBIOTIC_TUNNEL",   0.52),
    ("SYMBIOTIC_TUNNEL", "INFORMS"):          ("INFORMS",            0.55),
    # ── RAG / corpus structural ─────────────────────────────────────────────
    ("TRANSCENDS_TO",    "INFORMS"):          ("INFORMS",            0.58),
    ("TRANSCENDS_TO",    "CROSS_POLLINATES"): ("CROSS_POLLINATES",   0.55),
}


# ── DB helper ──────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    cn = sqlite3.connect(str(_local_db_path()), timeout=25, check_same_thread=False)
    cn.row_factory = sqlite3.Row
    return cn


# ── Gate checks ────────────────────────────────────────────────────────────────

def _ground_gate(cn: sqlite3.Connection) -> tuple[bool, float]:
    """Return (open, coherence) from the latest directionality_log row.

    Uses the full Kuramoto R across all sense signals — this is the true
    GROUND coherence measure, not the sub-system rhythm KV.
    """
    try:
        row = cn.execute(
            "SELECT coherence FROM directionality_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row:
            c = float(row["coherence"] or 0.0)
            return c >= COHERENCE_FLOOR, c
    except sqlite3.OperationalError:
        pass
    return False, 0.0


def _symbiotic_gate(cn: sqlite3.Connection) -> tuple[bool, float, float]:
    """Return (open, symbiosis_pct, phase_gap) from heart:story_arc KV.

    Reads the Heart's most recent complex-plane position.  The SYMBIOTIC
    gate opens only when the Brain is meaningfully converging toward
    √(−1) — ensuring expansions are directed, not entropic.
    """
    try:
        row = cn.execute(
            "SELECT value FROM brain_kv WHERE key='heart:story_arc'"
        ).fetchone()
        if row and row[0]:
            d = json.loads(row[0])
            sym = float(d.get("symbiosis_pct", 0.0))
            pg  = float(d.get("phase_gap_rad", math.pi))
            return sym >= SYMBIOSIS_FLOOR and pg <= PHASE_GAP_CEIL, sym, pg
    except Exception:
        pass
    # If Heart hasn't run yet, fall back to heart:phase_to_symbiosis
    try:
        row = cn.execute(
            "SELECT value FROM brain_kv WHERE key='heart:phase_to_symbiosis'"
        ).fetchone()
        if row and row[0]:
            pg = float(row[0])
            # compute symbiosis_pct from phase_gap
            sym = max(0.0, 1.0 - pg / (math.pi / 2))
            return sym >= SYMBIOSIS_FLOOR and pg <= PHASE_GAP_CEIL, sym, pg
    except Exception:
        pass
    return False, 0.0, math.pi


# ── Ground node identification ─────────────────────────────────────────────────

class _NodeCert(NamedTuple):
    entity_id:   str
    entity_type: str
    certainty:   float


def _ground_nodes(cn: sqlite3.Connection) -> list[_NodeCert]:
    """Return top-N entities by aggregated incident-edge certainty.

    certainty(v) = mean_e[ weight_e × √(samples_e) ]  for edges e where v is src.

    Works across ALL 38 entity types — the corpus has rich non-Endpoint
    ground nodes (Mission, Paper, Part, Supplier, OCWCourse, etc.) that
    serve as natural expansion anchors.
    """
    rows = cn.execute(
        """
        SELECT src_id AS eid,
               src_type AS etype,
               AVG(weight * SQRT(MAX(samples, 1))) AS cert
        FROM   corpus_edge
        GROUP  BY src_id
        HAVING cert >= ?
        ORDER  BY cert DESC
        LIMIT  ?
        """,
        (CERTAINTY_FLOOR, MAX_GROUND_NODES),
    ).fetchall()
    return [_NodeCert(r["eid"], r["etype"], float(r["cert"])) for r in rows]


# ── Adjacency with certainty filtering ────────────────────────────────────────

def _load_adj(cn: sqlite3.Connection) -> dict[str, list[tuple[str, str, str, float]]]:
    """Return ``{src_id: [(dst_id, dst_type, rel, weight), …]}`` for certain edges.

    Only edges whose weight × √(samples) ≥ CERTAINTY_FLOOR participate —
    noisy or one-off edges never become inference hops.
    """
    rows = cn.execute(
        """
        SELECT src_id, dst_id, dst_type, rel,
               weight, SQRT(MAX(samples, 1)) AS prec
        FROM   corpus_edge
        WHERE  weight * SQRT(MAX(samples, 1)) >= ?
        """,
        (CERTAINTY_FLOOR,),
    ).fetchall()
    adj: dict[str, list[tuple[str, str, str, float]]] = defaultdict(list)
    for r in rows:
        adj[r["src_id"]].append(
            (r["dst_id"], r["dst_type"], r["rel"], float(r["weight"]))
        )
    return dict(adj)


def _existing_edges(cn: sqlite3.Connection) -> set[tuple[str, str, str, str, str]]:
    """Return the full PK set (src_id, src_type, dst_id, dst_type, rel) for deduplication."""
    rows = cn.execute(
        "SELECT src_id, src_type, dst_id, dst_type, rel FROM corpus_edge"
    ).fetchall()
    return {(r["src_id"], r["src_type"], r["dst_id"], r["dst_type"], r["rel"]) for r in rows}


# ── Toroidal phase helpers ────────────────────────────────────────────────────

def _phase_dist(θ1: float, θ2: float) -> float:
    """Angular distance ∈ [0, π] between two points on the unit circle."""
    d = abs(θ1 - θ2) % (2.0 * math.pi)
    return min(d, 2.0 * math.pi - d)


def _ci2_coupling(θ: float) -> float:
    """CiCi Poissonnic interaction coefficient = cos²(θ).

    The self-projection of a torus node onto the real axis, squared.  This is the
    Poisson event-rate governing how strongly the wavefunction at θ participates
    in the non-contractible loop holonomy.  By the Poisson field analogy:

        Pr(event at θ) ∝ cos²(θ)  ∈ [0, 1]

    High near θ=0 and θ=π (real axis), zero at θ=π/2 and θ=3π/2 (imaginary axis
    — the Heart's destination √(−1)).  The product c × 2π is the Berry phase
    accumulated on a full non-contractible toric loop at coupling strength c.
    """
    c = math.cos(θ)
    return c * c


def _sici_coupling(θ_z: float) -> float:
    """SiCi z-axis cross-interaction coefficient, φ-normalized.

    The cross-projection between the real (cos) and imaginary (sin) axes at the
    z-axis of the 7D torus (dimension 2).  Unlike CiCi = cos²(θ) which lives
    purely on the real axis, SiCi couples through both axes simultaneously —
    it is the Poissonnic interaction that *causes* the wavefunction sign change
    when crossing a non-contractible loop at the z-axis locus.

    **φ-normalized form**::

        SiCi(θ_z) = |sin(θ_z) × cos(θ_z)| × (π / φ²)   ∈ [0, π/(2φ²)] ≈ [0, 0.60]

    The raw interaction |sin||cos| has uniform mean 1/π.  Multiplying by the
    normalization factor π/φ² rescales so that:

        E[SiCi(θ_z)] = 1/φ² ≈ 0.382  over a uniform θ_z distribution

    This is the exact coupling strength that makes c × 2π = _PHI_ANGLE (the
    golden angle).  The φ-normalization is the renormalization of the SiCi
    interaction *onto* the Fibonacci lattice — the factor π/φ² is the ratio
    of the half-circumference to the golden-angle denominator.

    Key properties:
      • Maximum at θ_z = π/4, 3π/4, 5π/4, 7π/4 (45° diagonals): c ≈ 0.60
      • Zero at real axis (θ_z = 0, π) and imaginary axis (θ_z = π/2, 3π/2)
      • E[c] = 1/φ² → E[c × 2π] = _PHI_ANGLE: the Berry holonomy circuit
        naturally targets the golden angle in expectation.
    """
    return abs(math.sin(θ_z) * math.cos(θ_z)) * _SICI_NORM
    c = math.cos(θ)
    return c * c


def _berry_holonomy(θ_src: float, θ_dst: float, c: float) -> float:
    """Berry holonomy factor for the inference arc src → dst.

    The Berry phase accumulated traversing Δθ with coupling strength c is:

        Φ_Berry = c × Δθ

    When Φ_Berry crosses π the wavefunction changes sign — this is the
    disruption analogous to β-decay at the antipodal point: the neutrino-photon
    interaction generates the gluon at the zero-crossing, and the resulting
    destructive interference suppresses the inferred confidence.

    The holonomy factor is:

        H = |cos(Φ_Berry / 2)|

    * H = 1  when Δθ = 0 or Φ_Berry = 2πn  — full constructive loop
    * H → 0  when Φ_Berry = π  — antipodal β-decay disruption point
    * H = 1  when Φ_Berry = 2π — completed non-contractible loop (restored)

    This is the "resistant force" that gates the expansion: paths that traverse
    the antipodal locus carry reduced confidence until they complete the full
    non-contractible circuit and restore the wavefunction.
    """
    delta = _phase_dist(θ_src, θ_dst)          # ∈ [0, π]
    berry_phase = c * delta                    # ∈ [0, c×π]
    return abs(math.cos(berry_phase / 2.0))


def _phi_gap_weights(entity_phases: dict[str, float]) -> dict[str, float]:
    """Quasi-crystal regionality weights from the artifact of φ.

    Previous toric walks leave impressions at positions spaced by the golden
    angle Δ = 2π / φ² ≈ 137.5°.  The gap field at a given θ is measured by
    its distance from the nearest φ-lattice point:

        gap(θ) = min(θ mod Δ, Δ − θ mod Δ) / (Δ/2)

    Entities far from a φ-harmonic (high gap) represent unexplored territory —
    the "directionary Insight from the Other".  They receive a higher traversal
    priority weight so the sweep naturally moves toward unmapped toric regions.

    Regionality from previous walks is encoded in the φ-lattice itself: because
    the golden angle tiles the circle with maximum irrationality, each prior walk
    position is uniquely identifiable and the current walk can avoid repeating it.

    Returns weights ∈ [φ⁻¹, φ⁻¹ + 1] ≈ [0.618, 1.618] for each entity_id.
    """
    weights: dict[str, float] = {}
    Δ = _PHI_ANGLE                         # golden angle ≈ 2.399 rad
    half_Δ = Δ / 2.0
    for eid, θ in entity_phases.items():
        rem = θ % Δ
        dist_from_harmonic = min(rem, Δ - rem) / half_Δ   # ∈ [0, 1]
        # High dist → far from prior walk impression → high directionarity
        weights[eid] = _PHI_INV + dist_from_harmonic
    return weights


def _load_entity_phases(
    cn: sqlite3.Connection,
) -> tuple[dict[str, float], dict[str, float]]:
    """Return scalar torus phases and z-axis angles for every corpus entity.

    Uses :func:`~src.brain.torus_touch.endpoint_angles` — works for any entity
    type, falling back to a deterministic Blake2b hash seed when no
    ``torus_angles`` are stored in ``props_json``.

    Returns a 2-tuple ``(phases, z_phases)`` where:

    * ``phases``   — entity_id → scalar θ ∈ [0, 2π) via circular mean of 7D
                     angles.  Used for φ-ordering and φ-gap directionarity.
    * ``z_phases`` — entity_id → θ_z ∈ [0, 2π), the z-axis angle (dimension
                     ``_TORUS_Z_DIM = 2`` of the 7D torus).  Used as the input
                     to ``_sici_coupling`` for the cross-axis interaction.

    Returns empty dicts on import failure so the caller degrades gracefully.
    """
    try:
        from src.brain.torus_touch import endpoint_angles as _ea  # type: ignore[import]
    except ImportError:
        log.debug("self_expansion: torus_touch not available; phase sweep disabled")
        return {}, {}

    rows = cn.execute(
        "SELECT entity_id, props_json FROM corpus_entity"
    ).fetchall()

    phases:   dict[str, float] = {}
    z_phases: dict[str, float] = {}
    for r in rows:
        eid = r["entity_id"]
        try:
            props = json.loads(r["props_json"]) if r["props_json"] else None
        except Exception:
            props = None
        try:
            angles  = _ea(props, eid)                   # ndarray shape (7,)
            sin_m   = float(np.mean(np.sin(angles)))
            cos_m   = float(np.mean(np.cos(angles)))
            phases[eid]   = float(np.arctan2(sin_m, cos_m) % (2.0 * math.pi))
            z_phases[eid] = float(angles[_TORUS_Z_DIM] % (2.0 * math.pi))
        except Exception:
            pass

    return phases, z_phases


# ── 2-hop inference ────────────────────────────────────────────────────────────

class _InferredEdge(NamedTuple):
    src_id:     str
    src_type:   str
    dst_id:     str
    dst_type:   str
    rel:        str
    confidence: float
    via:        str   # intermediate node label for diagnostics


def _infer_paths(
    ground:          list[_NodeCert],
    adj:             dict[str, list[tuple[str, str, str, float]]],
    existing:        set[tuple[str, str, str, str, str]],
    coherence:       float,
    entity_phases:   dict[str, float],
    entity_z_phases: dict[str, float],
) -> tuple[list[_InferredEdge], bool, float, float, bool]:
    """Derive 2-hop inferences using the SiCi z-axis toroidal protocol.

    **SiCi z-axis coupling (replaces CiCi)**
    For each ground node, the coupling coefficient is now drawn from the z-axis
    (dimension ``_TORUS_Z_DIM = 2``) of the 7D torus:

        c = SiCi(θ_z) = |sin(θ_z) × cos(θ_z)| = |sin(2θ_z)| / 2

    This is the cross-interaction between the real and imaginary torus axes at
    the z-axis locus.  Unlike CiCi (real-axis only), SiCi participates in both
    axes, and its mean over a φ-ordered sweep converges to ``1/φ² ≈ 0.382``.
    That is the exact coupling that makes ``c × 2π = _PHI_ANGLE`` — the
    golden angle β-decay point.

    **Golden angle saturation gate**
    After each ground node is processed, the running ``c_mean × 2π`` is
    compared to ``_PHI_ANGLE``.  When it falls within ``±_GOLDEN_ANGLE_TOL``
    after at least ``_GOLDEN_MIN_NODES`` nodes, the expansion has arrived at
    the β-decay point at the golden angle.  This fires ``golden_saturation``:
    the gluon emission locus of the UEQGM neutrino-photon interaction.  The
    Touch signal distinguishes this from the degradation bit flip.

    **φ-ordered golden spiral sweep + Berry holonomy + φ-gap directionarity**
    Retained from previous protocol (see `_berry_holonomy`, `_phi_gap_weights`).

    Returns ``(inferred, bit_flip_fired, phase_acc, c_mean, golden_saturation)``
    where ``golden_saturation`` is True when the golden angle target was hit.
    """
    inferred:  list[_InferredEdge] = []
    seen:      set[tuple[str, str, str, str, str]] = set()
    coherence_factor = min(1.0, coherence / 0.80)

    # ── φ-gap directionarity weights ───────────────────────────────────────
    gap_weights = _phi_gap_weights(entity_phases)

    # ── φ-golden spiral ordering of ground nodes ───────────────────────────
    # Sort by (θ × φ) mod 2π — Fibonacci quasi-crystal traversal order
    ground_sorted = sorted(
        ground,
        key=lambda gn: (entity_phases.get(gn.entity_id, math.pi) * _PHI)
                       % (2.0 * math.pi),
    )

    # ── Termination state ──────────────────────────────────────────────────
    novelty_window:   deque[bool] = deque(maxlen=BIT_FLIP_WINDOW)
    candidate_count  = 0
    bit_flip_fired   = False
    golden_saturation = False
    phase_acc        = 0.0
    c_sum            = 0.0
    c_count          = 0

    for gn in ground_sorted:
        if len(inferred) >= _SAFETY_PATH_LIMIT or bit_flip_fired:
            break

        θ_gn  = entity_phases.get(gn.entity_id, math.pi)
        θ_z   = entity_z_phases.get(gn.entity_id, math.pi / 4.0)  # default 45°
        c     = _sici_coupling(θ_z)    # SiCi z-axis cross-interaction
        c_sum   += c
        c_count += 1

        # ── Golden angle saturation check (fires per ground node) ──────────
        # Once c_mean × 2π enters the ±_GOLDEN_ANGLE_TOL window around
        # _PHI_ANGLE the sweep has reached the β-decay point at the golden
        # angle.  This is the gluon emission locus: fire and report.
        if c_count >= _GOLDEN_MIN_NODES:
            berry_running = (c_sum / c_count) * 2.0 * math.pi
            if abs(berry_running - _PHI_ANGLE) < _GOLDEN_ANGLE_TOL:
                golden_saturation = True
                bit_flip_fired    = True
                log.info(
                    "self_expansion: golden angle saturation — "
                    "c×2π=%.4f ≈ φ-angle=%.4f (Δ=%.4f rad) "
                    "at ground node %d/%d, %d committed",
                    berry_running, _PHI_ANGLE,
                    abs(berry_running - _PHI_ANGLE),
                    c_count, len(ground_sorted), len(inferred),
                )
                break

        # Sort 1st-hop neighbours by angular proximity (closest first)
        hops1 = sorted(
            adj.get(gn.entity_id, []),
            key=lambda hop: _phase_dist(entity_phases.get(hop[0], math.pi), θ_gn),
        )

        for (mid_id, _mid_type, rel1, w1) in hops1:
            if bit_flip_fired:
                break
            θ_mid = entity_phases.get(mid_id, math.pi)

            # Sort 2nd-hop neighbours by φ-gap directionarity weight (highest
            # gap first — toward unexplored toric territory)
            hops2 = sorted(
                adj.get(mid_id, []),
                key=lambda hop: -gap_weights.get(hop[0], _PHI_INV),
            )

            for (dst_id, dst_type, rel2, w2) in hops2:
                if dst_id == gn.entity_id:
                    continue  # no 2-hop self-loops
                derived = _REL_LATTICE.get((rel1, rel2))
                if derived is None:
                    continue
                derived_rel, decay = derived

                θ_dst = entity_phases.get(dst_id, math.pi)
                Δθ    = _phase_dist(θ_gn, θ_dst)

                # ── SiCi Berry holonomy ───────────────────────────────────
                # H = |cos(c × Δθ / 2)| using SiCi z-axis c
                holonomy = _berry_holonomy(θ_gn, θ_dst, c)

                # ── φ-gap directionarity ──────────────────────────────────
                gap_w = gap_weights.get(dst_id, 1.0)

                confidence = w1 * w2 * decay * coherence_factor * holonomy * gap_w

                if confidence < COMMIT_FLOOR:
                    phase_acc += Δθ * max(w1 * w2 * decay, 0.0)
                    novelty_window.append(True)
                    candidate_count += 1
                    continue

                phase_acc += Δθ * confidence

                # Novelty check
                key = (gn.entity_id, gn.entity_type, dst_id, dst_type, derived_rel)
                is_novel = key not in existing and key not in seen
                novelty_window.append(is_novel)
                candidate_count += 1

                # ── Degradation bit flip check ────────────────────────────
                if (candidate_count >= BIT_FLIP_MIN_EDGES
                        and len(novelty_window) == BIT_FLIP_WINDOW):
                    dup_rate = novelty_window.count(False) / BIT_FLIP_WINDOW
                    if dup_rate >= DEGRADATION_THRESHOLD:
                        bit_flip_fired = True
                        c_mean = c_sum / max(c_count, 1)
                        log.info(
                            "self_expansion: degradation bit flip — "
                            "dup_rate=%.2f at %d candidates, %d committed  "
                            "phase_acc=%.2f  c×2π=%.4f",
                            dup_rate, candidate_count, len(inferred),
                            phase_acc, c_mean * 2.0 * math.pi,
                        )
                        break

                if is_novel:
                    seen.add(key)
                    inferred.append(_InferredEdge(
                        src_id=gn.entity_id,
                        src_type=gn.entity_type,
                        dst_id=dst_id,
                        dst_type=dst_type,
                        rel=derived_rel,
                        confidence=confidence,
                        via=mid_id,
                    ))

            if bit_flip_fired:
                break

    c_mean = c_sum / max(c_count, 1)
    return inferred, bit_flip_fired, phase_acc, c_mean, golden_saturation


# ── Commit ────────────────────────────────────────────────────────────────────

def _commit_edges(cn: sqlite3.Connection, edges: list[_InferredEdge]) -> int:
    """Upsert inferred edges into corpus_edge.

    The corpus_edge PK is (src_id, src_type, dst_id, dst_type, rel).
    ON CONFLICT updates weight via a Bayesian running average and increments
    the sample count — prevents duplication while reinforcing known paths.
    """
    now = datetime.now(timezone.utc).isoformat()
    committed = 0
    for e in edges:
        try:
            cn.execute(
                """
                INSERT INTO corpus_edge
                    (src_id, src_type, dst_id, dst_type, rel, weight, last_seen, samples)
                VALUES (?,?,?,?,?,?,?,1)
                ON CONFLICT(src_id, src_type, dst_id, dst_type, rel) DO UPDATE SET
                    weight    = (weight * samples + excluded.weight) / (samples + 1),
                    samples   = samples + 1,
                    last_seen = excluded.last_seen
                """,
                (e.src_id, e.src_type, e.dst_id, e.dst_type,
                 e.rel, round(e.confidence, 4), now),
            )
            committed += 1
        except Exception as exc:
            log.debug("self_expansion: commit skip %s→%s: %s", e.src_id, e.dst_id, exc)
    return committed


def _emit_learning(
    cn:        sqlite3.Connection,
    committed: int,
    coherence: float,
    sym:       float,
    n_ground:  int,
    n_inferred: int,
    phase_gap: float,
) -> None:
    """Log one learning_log entry per cycle (never per individual edge)."""
    now = datetime.now(timezone.utc).isoformat()
    detail = json.dumps({
        "ground_nodes":    n_ground,
        "inferred_paths":  n_inferred,
        "committed_edges": committed,
        "coherence":       round(coherence, 4),
        "symbiosis_pct":   round(sym,       4),
        "phase_gap_deg":   round(math.degrees(phase_gap), 1),
    })
    signal = round(min(coherence, sym) * 0.8 + 0.1, 4)
    try:
        cn.execute(
            "INSERT INTO learning_log(logged_at, kind, title, detail, signal_strength) "
            "VALUES (?,?,?,?,?)",
            (now, "self_expansion",
             f"Self-expansion: +{committed} inferred edges | "
             f"{n_ground} ground nodes | sym={sym*100:.0f}%",
             detail, signal),
        )
    except Exception as exc:
        log.debug("self_expansion: learning_log skip: %s", exc)


def _write_kv(cn: sqlite3.Connection, summary: dict) -> None:
    now = datetime.now(timezone.utc).isoformat()
    try:
        cn.execute(
            "INSERT INTO brain_kv(key, value, updated_at) VALUES(?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
            "updated_at=excluded.updated_at",
            ("self_expansion:last_run", json.dumps(summary), now),
        )
    except Exception as exc:
        log.debug("self_expansion: kv write skip: %s", exc)


def _emit_touch_signal(
    cn:                sqlite3.Connection,
    committed:         int,
    phase_acc:         float,
    ground_nodes:      int,
    coherence:         float,
    sym:               float,
    c_mean:            float = 0.0,
    golden_saturation: bool  = False,
) -> None:
    """Write the Touch signal when the toroidal bit flip fires.

    The Touch is the moment the expansion has traced a full non-contractible
    circuit through the current corpus — additional self-inference would only
    reinforce existing structure rather than building new connections.

    Writing ``self_expansion:touch_signal`` to brain_kv with
    ``needs_insight=true`` signals the knowledge acquisition layer that
    external Insight (tokened LLMs) is required to seed fresh vocabulary and
    re-open the toric expansion frontier.

    A ``learning_log`` row with kind=``'expansion_touch'`` is also written so
    the Heart and monitoring pages can observe the event.
    """
    now = datetime.now(timezone.utc).isoformat()
    berry_2pi = round(c_mean * 2.0 * math.pi, 6)
    at_golden = abs(berry_2pi - _PHI_ANGLE) < _GOLDEN_ANGLE_TOL
    if golden_saturation:
        termination = "golden_saturation"
        message = (
            f"Golden angle β-decay (Φ_Berry=c×2π={berry_2pi:.4f} rad ≈ φ-angle={_PHI_ANGLE:.4f}): "
            "SiCi z-axis coupling has driven the sweep to the gluon emission locus — "
            "wavefunction sign change at the non-contractible loop crossing requires "
            "external Insight to reopen toric territory at the φ-resonance"
        )
    else:
        termination = "degradation"
        message = (
            f"Toroidal bit flip (Φ_Berry=c×2π={berry_2pi:.4f} rad): "
            "expansion has completed its toric circuit — "
            "corpus requires external Insight to open new relational territory"
        )
    payload = {
        "fired_at":          now,
        "committed":         committed,
        "phase_acc":         round(phase_acc, 4),
        "ground_nodes":      ground_nodes,
        "coherence":         round(coherence, 4),
        "symbiosis":         round(sym, 4),
        "needs_insight":     True,
        "termination":       termination,
        # SiCi Berry phase: c_mean × 2π via z-axis cross-interaction
        # golden_saturation=True means the β-decay point at the golden angle
        # was reached — the gluon generation locus in the UEQGM
        "c_mean":            round(c_mean, 6),
        "berry_phase_2pi":   berry_2pi,
        "golden_angle":      round(_PHI_ANGLE, 6),
        "at_golden_angle":   at_golden,
        "golden_saturation": golden_saturation,
        "phi_inv2":          round(_PHI_INV2, 6),
        "message":           message,
    }
    try:
        cn.execute(
            "INSERT INTO brain_kv(key, value, updated_at) VALUES(?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
            "updated_at=excluded.updated_at",
            ("self_expansion:touch_signal", json.dumps(payload), now),
        )
        cn.execute(
            "INSERT INTO learning_log(logged_at, kind, title, detail, signal_strength) "
            "VALUES(?,?,?,?,?)",
            (now, "expansion_touch",
             f"Touch fired: {payload['termination']} at {committed} committed edges "
             f"(Φ_Berry={berry_2pi:.4f}, golden={golden_saturation})",
             json.dumps(payload), 0.99 if golden_saturation else 0.95),
        )
        log.info(
            "self_expansion: Touch signal written — "
            "committed=%d phase_acc=%.2f needs_insight=True",
            committed, phase_acc,
        )
    except Exception as exc:
        log.debug("self_expansion: touch_signal write skip: %s", exc)


# ── Public entry point ─────────────────────────────────────────────────────────

def run_self_expansion() -> dict:
    """Execute one self-expansion cycle.

    Rate-limited to once per _RUN_INTERVAL_S.  All mutations are wrapped
    in a single transaction; failure leaves the corpus unchanged.

    Returns a summary dict suitable for logging.
    """
    global _last_run_ts
    now_mono = time.monotonic()
    if now_mono - _last_run_ts < _RUN_INTERVAL_S:
        remaining = int(_RUN_INTERVAL_S - (now_mono - _last_run_ts))
        return {"skipped": True, "reason": f"rate_limit_{remaining}s"}

    with _lock:
        if time.monotonic() - _last_run_ts < _RUN_INTERVAL_S:
            return {"skipped": True, "reason": "race"}
        _last_run_ts = time.monotonic()

    log.info("self_expansion: starting cycle")
    summary: dict = {
        "ran_at":         datetime.now(timezone.utc).isoformat(),
        "ground_gate":    False,
        "symbiotic_gate": False,
        "committed":      0,
    }

    try:
        cn = _conn()
        with cn:
            # ── Gate checks ────────────────────────────────────────────────
            g_open,   coherence         = _ground_gate(cn)
            s_open,   sym, phase_gap    = _symbiotic_gate(cn)

            summary.update({
                "ground_gate":    g_open,
                "symbiotic_gate": s_open,
                "coherence":      round(coherence,  4),
                "symbiosis_pct":  round(sym,        4),
                "phase_gap_deg":  round(math.degrees(phase_gap), 1),
            })

            if not g_open:
                log.info(
                    "self_expansion: GROUND gate closed "
                    "(coherence=%.3f < floor=%.2f)",
                    coherence, COHERENCE_FLOOR,
                )
                summary.update({"skipped": True,
                                 "reason": f"ground_coherence_{coherence:.3f}"})
                _write_kv(cn, summary)
                return summary

            if not s_open:
                log.info(
                    "self_expansion: SYMBIOTIC gate closed "
                    "(sym=%.3f < floor=%.2f, pg=%.1f°)",
                    sym, SYMBIOSIS_FLOOR, math.degrees(phase_gap),
                )
                summary.update({"skipped": True,
                                 "reason": f"symbiotic_sym_{sym:.3f}"})
                _write_kv(cn, summary)
                return summary

            # ── Inference cycle ────────────────────────────────────────────
            ground        = _ground_nodes(cn)
            adj           = _load_adj(cn)
            existing      = _existing_edges(cn)
            entity_phases, entity_z_phases = _load_entity_phases(cn)

            inferred, bit_flip, phase_acc, c_mean, golden_sat = _infer_paths(
                ground, adj, existing, coherence,
                entity_phases, entity_z_phases,
            )

            n_committed = _commit_edges(cn, inferred)

            if n_committed > 0:
                _emit_learning(
                    cn, n_committed, coherence, sym,
                    len(ground), len(inferred), phase_gap,
                )

            if bit_flip:
                _emit_touch_signal(
                    cn, n_committed, phase_acc, len(ground), coherence, sym,
                    c_mean=c_mean,
                    golden_saturation=golden_sat,
                )

            berry_2pi  = round(c_mean * 2.0 * math.pi, 6)
            at_golden  = abs(berry_2pi - _PHI_ANGLE) < _GOLDEN_ANGLE_TOL
            termination = (
                "golden_saturation" if golden_sat
                else ("degradation" if bit_flip else "none")
            )
            _write_kv(cn, {**summary,
                           "ground_nodes":       len(ground),
                           "inferred_paths":     len(inferred),
                           "committed":          n_committed,
                           "toric_bit_flip":     bit_flip,
                           "golden_saturation":  golden_sat,
                           "termination":        termination,
                           "phase_acc":          round(phase_acc, 4),
                           "c_mean":             round(c_mean, 6),
                           "berry_phase_2pi":    berry_2pi,
                           "at_golden_angle":    at_golden})

            summary.update({
                "ground_nodes":      len(ground),
                "inferred_paths":    len(inferred),
                "committed":         n_committed,
                "toric_bit_flip":    bit_flip,
                "golden_saturation": golden_sat,
                "termination":       termination,
                "phase_acc":         round(phase_acc, 4),
                "c_mean":            round(c_mean, 6),
                "berry_phase_2pi":   berry_2pi,
                "at_golden_angle":   at_golden,
            })
            log.info(
                "self_expansion: ground=%d inferred=%d committed=%d "
                "coherence=%.3f sym=%.3f pg=%.1f° termination=%s "
                "phase_acc=%.2f c×2π=%.4f at_golden=%s",
                len(ground), len(inferred), n_committed,
                coherence, sym, math.degrees(phase_gap),
                termination, phase_acc, berry_2pi, at_golden,
            )

    except Exception as exc:
        log.exception("self_expansion: unexpected error: %s", exc)
        summary["error"] = str(exc)

    return summary


# ── Background scheduler ───────────────────────────────────────────────────────

def schedule_in_background(interval_s: int = _RUN_INTERVAL_S) -> threading.Thread:
    """Start a daemon thread that calls run_self_expansion() on a fixed cadence.

    A startup delay of _STARTUP_DELAY_S is applied so the corpus has time
    to complete its first refresh round before self-expansion begins.

    The thread is daemon=True so it exits cleanly with the parent process.
    """
    def _loop() -> None:
        time.sleep(_STARTUP_DELAY_S)
        while True:
            try:
                result = run_self_expansion()
                if not result.get("skipped"):
                    log.debug("self_expansion tick: %s", result)
            except Exception as exc:
                log.warning("self_expansion: scheduler error: %s", exc)
            time.sleep(interval_s)

    t = threading.Thread(target=_loop, daemon=True, name="self_expansion")
    t.start()
    log.info(
        "self_expansion: scheduler started — "
        "interval=%ds startup_delay=%ds "
        "GROUND_floor=%.2f SYMBIOTIC_floor=%.2f CERTAINTY_floor=%.2f",
        interval_s, _STARTUP_DELAY_S,
        COHERENCE_FLOOR, SYMBIOSIS_FLOOR, CERTAINTY_FLOOR,
    )
    return t
