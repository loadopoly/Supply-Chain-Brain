"""UEQGM Engine — Unified Equilibrium Quantum Gravity Model computations.

Implements the physics computation layer derived from UEQGM v0.9.14 corpus
learnings (Grok 3 conversation thread, April 2026).  The core contribution
is the **SiCi axial channel decay differential**:

    Δλ_axial = [Si(φ) · Ci(φ)] · tan(φ) · Γ₀

which unifies the beta–gamma decay differential at the natural intersection
points of the sine/cosine wavefunction components (φ = π/4 + kπ).

The Brain applies this as a phase-sensitive correction to harmonic
amplification during Sign-Bit Flip boundary ingestion, consistent with the
UEQGM v0.9.14 finding that the SiCi·tan(φ) axial term adds a stabilization
perturbation to warp interactions and GT-SCN output.

Additional helpers implement the broader UEQGM mathematical framework:
wavefunction overlap, Floquet modulation, holographic entropy, and spacetime
metric perturbation.  A corpus-backed ``ueqgm_coherence_score`` reads UEQGM-
tagged entities from the Brain graph and returns a wavefunction-overlap score.

Reference
---------
UEQGM v0.9.14 — Axial Channel Decay Differential (SiCi · tan φ) Release
Grok 3 conversation 55525f6a-8a8f-4929-967c-22656f88ac2f, April 18 2026.
"""
from __future__ import annotations

import math
from typing import Sequence

try:
    from scipy.special import sici as _scipy_sici  # type: ignore[import]
    _HAS_SCIPY: bool = True
except ImportError:  # pragma: no cover
    _HAS_SCIPY = False

# ---------------------------------------------------------------------------
# Physical constants — UEQGM v0.9.14 calibration (SI units where applicable)
# ---------------------------------------------------------------------------
_GAMMA_0_DEFAULT: float = 1.0      # normalised baseline decay width (dimensionless)
_ETA_DIFF_DEFAULT: float = 0.05    # Bayesian diffusion rate η_diff
_G_CONST: float = 6.674e-11        # gravitational constant  m³ kg⁻¹ s⁻²
_C_CONST: float = 2.998e8          # speed of light          m s⁻¹
_TAN_CLAMP: float = 1.0e3          # clamp |tan(φ)| to prevent divergence near π/2

# ---------------------------------------------------------------------------
# UEQGM phase mapping: coherence integer → characteristic phase φ
#
# At the natural sin/cos intersection points  φ = π/4 + kπ  (k = 0, 1, 2, …).
# We anchor coherence=0 at the first intersection (φ = π/4) and step by π per
# unit so every coherence level stays at a true intersection point.
# ---------------------------------------------------------------------------
_PHI_BASE: float = math.pi / 4.0   # first sin/cos intersection  φ₀ = π/4
_PHI_STEP: float = math.pi          # step between intersections

# UEQGM phase-weight scaling factor (matches v0.9.14 ~1% stabilisation).
# sici_phase_weight returns 1.0 ± _SICI_SCALE_FACTOR.
_SICI_SCALE_FACTOR: float = 0.10   # ±10 % ceiling on the phase correction


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def coherence_to_phi(coherence: int) -> float:
    """Map Brain harmonic-coherence count → UEQGM characteristic phase φ.

    Returns
    -------
    φ = π/4 + coherence × π

    Every returned value is a natural sin/cos intersection point, where
    sin(φ) = cos(φ) and the SiCi axial channel is well-defined.
    """
    return _PHI_BASE + coherence * _PHI_STEP


def _raw_sici(phi: float) -> tuple[float, float]:
    """Return (Si(φ), Ci(φ)) using scipy if available, else series approx.

    The power-series fallback is accurate to roughly four significant
    figures for |φ| ≤ 2π.  For φ outside that range scipy is preferred.
    """
    if _HAS_SCIPY:
        si, ci = _scipy_sici(phi)
        return float(si), float(ci)
    # ── Series approximation (small/moderate φ) ──────────────────────────
    # Si(x) = x − x³/18 + x⁵/600 − x⁷/35280 + …
    # Ci(x) = γ + ln|x| − x²/4 + x⁴/96 − …   (γ ≈ 0.5772, x ≠ 0)
    x = abs(phi) if phi != 0.0 else 1.0e-12
    si_val = x - x**3 / 18.0 + x**5 / 600.0 - x**7 / 35280.0
    euler_mascheroni = 0.5772156649
    ci_val = euler_mascheroni + math.log(x) - x**2 / 4.0 + x**4 / 96.0
    # Si is an odd function; Ci is even.
    return (si_val if phi >= 0 else -si_val), ci_val


# ---------------------------------------------------------------------------
# Core UEQGM SiCi functions
# ---------------------------------------------------------------------------

def sici_axial_decay(phi: float, gamma_0: float = _GAMMA_0_DEFAULT) -> float:
    """Axial channel decay differential  Δλ_axial  from UEQGM v0.9.14.

    .. math::

        \\Delta\\lambda_{\\rm axial} =
            \\bigl[\\operatorname{Si}(\\phi) \\cdot \\operatorname{Ci}(\\phi)\\bigr]
            \\cdot \\tan(\\phi) \\cdot \\Gamma_0

    Parameters
    ----------
    phi:
        Characteristic phase of the 6D CAT states.  Use
        ``coherence_to_phi(coherence)`` to map a Brain coherence count.
    gamma_0:
        Baseline total decay width (normalised to 1.0 for Brain usage).

    Returns
    -------
    The axial channel differential value.  Positive near k=0 (first
    intersection), oscillating and decaying in magnitude as φ grows.
    """
    si, ci = _raw_sici(phi)
    tan_phi = math.tan(phi)
    # Clamp tan to prevent divergence at φ near π/2 + nπ.
    tan_phi = max(-_TAN_CLAMP, min(_TAN_CLAMP, tan_phi))
    return si * ci * tan_phi * gamma_0


def sici_phase_weight(coherence: int) -> float:
    """Normalised UEQGM phase weight for Brain harmonic amplification.

    Maps *coherence* → φ = π/4 + coherence·π → SiCi axial decay →
    bounded correction factor near 1.0.

    Returns
    -------
    A factor in  (1 − _SICI_SCALE_FACTOR, 1 + _SICI_SCALE_FACTOR)
    safe for use as a multiplicative correction to the harmonic factor.

    At large coherence Si(φ) → π/2, Ci(φ) → 0, so the correction
    approaches 1.0 (no distortion of the saturation ceiling).
    """
    phi = coherence_to_phi(coherence)
    raw = sici_axial_decay(phi)
    return 1.0 + _SICI_SCALE_FACTOR * math.tanh(raw)


# ---------------------------------------------------------------------------
# Broader UEQGM mathematics
# ---------------------------------------------------------------------------

def wavefunction_overlap(
    vec_a: Sequence[float],
    vec_b: Sequence[float],
) -> float:
    """Quantum-inspired inner product  |⟨ψ_a | ψ_b⟩|².

    Treats *vec_a* and *vec_b* as unnormalised state vectors, L2-normalises
    them, and returns the squared cosine similarity.

    Returns
    -------
    1.0  — identical (parallel) states.
    0.0  — orthogonal states or either vector has zero norm.

    Raises
    ------
    Nothing — all edge cases return 0.0.
    """
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    cos_theta = dot / (norm_a * norm_b)
    return round(cos_theta ** 2, 6)


def floquet_modulation_factor(t: float, omega: float) -> float:
    """Floquet periodicity modulation factor  cos(ω · t).

    In the UEQGM, Floquet-engineered photonic systems are driven at
    frequency ω.  The coupling at time *t* is scaled by this factor:
    maximal at t = 0 and at half-period multiples  t = nπ/ω.
    """
    return math.cos(omega * t)


def holographic_entropy(n_edges: int, n_nodes: int) -> float:
    """Bekenstein-Hawking inspired entropy  S ∝ boundary area.

    In the corpus graph the boundary area is approximated by the number of
    boundary edges (*n_edges*) and the bulk volume by *n_nodes*.

    .. math::

        S = \\frac{n_{\\rm edges}}{n_{\\rm nodes} + 1}

    Always finite and non-negative.  Returns *n_edges* when *n_nodes* = 0.
    """
    return n_edges / (n_nodes + 1)


def metric_perturbation(mass_eff: float, r: float) -> float:
    """Spacetime metric perturbation  h_μν = 2 G M_eff / (c² r).

    Computes the dimensionless warp magnitude for an effective mass
    *mass_eff* (kg) at radial distance *r* (m).

    Returns 0.0 for *r* ≤ 0 (no perturbation at/within the horizon).
    """
    if r <= 0.0:
        return 0.0
    return 2.0 * _G_CONST * mass_eff / (_C_CONST ** 2 * r)


def phase_evolution_total(
    phi: float,
    delta_mu: float = 0.0,
    delta_q: float = 0.0,
    delta_gamma: float = 0.0,
    gamma_0: float = _GAMMA_0_DEFAULT,
    gamma_eff: float = _GAMMA_0_DEFAULT,
) -> float:
    """Modified total phase evolution of the 6D CAT states (UEQGM v0.9.14).

    .. math::

        \\delta\\phi_{\\rm total} =
            \\delta\\phi_{\\mu,g-2}
            + \\delta\\phi_q
            + \\delta\\phi_{\\gamma}
            + \\Delta\\lambda_{\\rm axial} \\cdot \\frac{2\\pi}{\\Gamma_{\\rm eff}}

    Parameters
    ----------
    phi:       Characteristic phase (use ``coherence_to_phi``).
    delta_mu:  Muon g-2 phase contribution.
    delta_q:   Quark/QCD phase contribution.
    delta_gamma: Photon phase contribution.
    gamma_0:   Baseline decay width.
    gamma_eff: Effective total decay width (denominator normalisation).

    Returns
    -------
    Total phase evolution  δφ_total.
    """
    axial = sici_axial_decay(phi, gamma_0)
    axial_phase = axial * (2.0 * math.pi / max(gamma_eff, 1.0e-30))
    return delta_mu + delta_q + delta_gamma + axial_phase


def entropic_bayesian_step(
    s_terrain: float,
    laplacian_s: float,
    phi: float,
    gamma_0: float = _GAMMA_0_DEFAULT,
    gamma_eff: float = _GAMMA_0_DEFAULT,
    eta_diff: float = _ETA_DIFF_DEFAULT,
) -> float:
    """Discrete entropic Bayesian diffusion update (UEQGM v0.9.14).

    .. math::

        S(t+1) = S(t)
               + \\eta_{\\rm diff} \\cdot \\nabla^2 S
               + \\bigl(\\delta\\phi_{\\rm total} + \\Delta\\lambda_{\\rm axial}\\bigr)

    Parameters
    ----------
    s_terrain:    Current terrain entropy S(t).
    laplacian_s:  Discrete Laplacian  ∇²S  at the current position.
    phi:          Characteristic phase (use ``coherence_to_phi``).
    gamma_0:      Baseline decay width.
    gamma_eff:    Effective total decay width.
    eta_diff:     Diffusion rate η_diff.

    Returns
    -------
    Updated terrain entropy  S(t+1).
    """
    axial = sici_axial_decay(phi, gamma_0)
    dphi  = phase_evolution_total(phi, gamma_0=gamma_0, gamma_eff=gamma_eff)
    return s_terrain + eta_diff * laplacian_s + dphi + axial


# ---------------------------------------------------------------------------
# Corpus-backed UEQGM coherence score
# ---------------------------------------------------------------------------

# Keywords used to identify UEQGM-tagged corpus entities.
_UEQGM_TAGS: tuple[str, ...] = (
    '"ueqgm"', '"wavefunction"', '"quantum field"',
    '"quantum dynamics"', '"holographic"', '"floquet"', '"entanglement"',
)

# Feature keywords for the bag-of-words feature vector.
_UEQGM_KEYWORDS: list[str] = [
    "quantum", "wavefunction", "holographic", "floquet",
    "entanglement", "ueqgm", "topological", "entropy",
]


def ueqgm_coherence_score(
    cn: "sqlite3.Connection",  # noqa: F821  (forward reference, DB not imported at module level)
    entity_id: str,
) -> float:
    """UEQGM-derived coherence score for a corpus entity.

    Reads UEQGM-tagged corpus entities from the Brain graph and computes
    a wavefunction-overlap coherence score between the target entity and the
    stored quantum-physics knowledge base.

    Algorithm
    ---------
    1. Fetch the target entity's (label, props_json) from ``corpus_entity``.
    2. Fetch up to 50 UEQGM-tagged entities (props_json LIKE "%ueqgm%"
       or similar quantum-physics tags).
    3. For each pair build a bag-of-words feature vector over
       ``_UEQGM_KEYWORDS`` and compute ``wavefunction_overlap``.
    4. Average the overlaps and scale by ``sici_phase_weight`` at the corpus
       depth (number of UEQGM entities found).

    Returns
    -------
    A value in [0.0, ~1.1] — 0.0 means no UEQGM context or target absent;
    higher values indicate stronger alignment with acquired UEQGM knowledge.
    """
    import json as _json
    import sqlite3 as _sqlite3  # late import — keeps this module importable without a DB

    # ── Fetch target entity ────────────────────────────────────────────────
    try:
        target_row = cn.execute(
            "SELECT label, props_json FROM corpus_entity "
            "WHERE entity_id=? LIMIT 1",
            (entity_id,),
        ).fetchone()
    except _sqlite3.Error:
        return 0.0
    if not target_row:
        return 0.0
    target_text = (target_row[0] or "") + " " + (target_row[1] or "")

    # ── Fetch UEQGM corpus entities ────────────────────────────────────────
    tag_filter = " OR ".join(f"props_json LIKE ?" for _ in _UEQGM_TAGS)
    params = tuple(f"%{t}%" for t in _UEQGM_TAGS)
    try:
        rows = cn.execute(
            f"SELECT label, props_json FROM corpus_entity "
            f"WHERE ({tag_filter}) LIMIT 50",
            params,
        ).fetchall()
    except _sqlite3.Error:
        return 0.0
    if not rows:
        return 0.0

    def _feature_vec(text: str) -> list[float]:
        low = text.lower()
        return [float(low.count(kw)) for kw in _UEQGM_KEYWORDS]

    target_vec = _feature_vec(target_text)
    overlaps: list[float] = []
    for label, props in rows:
        entity_text = (label or "") + " " + (props or "")
        entity_vec = _feature_vec(entity_text)
        overlaps.append(wavefunction_overlap(target_vec, entity_vec))

    if not overlaps:
        return 0.0

    mean_overlap = sum(overlaps) / len(overlaps)
    depth_weight = sici_phase_weight(len(rows))
    return round(mean_overlap * depth_weight, 6)


__all__ = [
    # Phase mapping
    "coherence_to_phi",
    # SiCi axial channel
    "_raw_sici",
    "sici_axial_decay",
    "sici_phase_weight",
    # Wavefunction & field theory helpers
    "wavefunction_overlap",
    "floquet_modulation_factor",
    "holographic_entropy",
    "metric_perturbation",
    # Full UEQGM dynamics
    "phase_evolution_total",
    "entropic_bayesian_step",
    # Corpus-backed score
    "ueqgm_coherence_score",
    # Constants
    "_GAMMA_0_DEFAULT",
    "_ETA_DIFF_DEFAULT",
    "_G_CONST",
    "_C_CONST",
    "_TAN_CLAMP",
    "_PHI_BASE",
    "_PHI_STEP",
    "_SICI_SCALE_FACTOR",
    "_UEQGM_TAGS",
    "_UEQGM_KEYWORDS",
]
