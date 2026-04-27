"""Tests for ueqgm_engine.py — UEQGM v0.9.14 physics computation layer.

Covers the SiCi axial channel, wavefunction overlap, Floquet modulation,
holographic entropy, metric perturbation, phase evolution, entropic Bayesian
diffusion step, and the corpus-backed coherence score.
"""

from __future__ import annotations

import math
import sqlite3

import pytest

from src.brain.ueqgm_engine import (
    _G_CONST,
    _C_CONST,
    _PHI_BASE,
    _PHI_STEP,
    _SICI_SCALE_FACTOR,
    coherence_to_phi,
    entropic_bayesian_step,
    floquet_modulation_factor,
    holographic_entropy,
    metric_perturbation,
    phase_evolution_total,
    sici_axial_decay,
    sici_phase_weight,
    ueqgm_coherence_score,
    wavefunction_overlap,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ueqgm_db() -> sqlite3.Connection:
    """In-memory DB with corpus_entity for testing ueqgm_coherence_score."""
    cn = sqlite3.connect(":memory:")
    cn.execute(
        "CREATE TABLE corpus_entity("
        "entity_id TEXT, entity_type TEXT, label TEXT, props_json TEXT, "
        "first_seen TEXT, last_seen TEXT, samples INTEGER DEFAULT 1, "
        "UNIQUE(entity_id, entity_type))"
    )
    return cn


def _insert_entity(cn: sqlite3.Connection, eid: str, label: str, props: str) -> None:
    cn.execute(
        "INSERT OR IGNORE INTO corpus_entity "
        "(entity_id, entity_type, label, props_json, first_seen, last_seen) "
        "VALUES(?,?,?,?,?,?)",
        (eid, "CorpusEntity", label, props, "2026-01-01", "2026-01-01"),
    )
    cn.commit()


# ── coherence_to_phi ──────────────────────────────────────────────────────────

def test_coherence_to_phi_zero_is_pi_over_4():
    """coherence=0 maps to φ=π/4 (first sin/cos intersection)."""
    assert coherence_to_phi(0) == pytest.approx(math.pi / 4, abs=1e-9)


def test_coherence_to_phi_steps_by_pi():
    """Each coherence unit increases φ by exactly π."""
    for k in range(1, 5):
        assert coherence_to_phi(k) == pytest.approx(
            math.pi / 4 + k * math.pi, abs=1e-9
        )


def test_coherence_to_phi_all_intersection_points():
    """tan(φ) at every intersection point equals 1.0."""
    for k in range(6):
        phi = coherence_to_phi(k)
        # tan(π/4 + kπ) = tan(π/4) = 1.0  for all integer k
        assert math.tan(phi) == pytest.approx(1.0, abs=1e-9)


# ── sici_axial_decay ──────────────────────────────────────────────────────────

def test_sici_axial_decay_coherence_0_positive():
    """At coherence=0 the axial decay product Si·Ci·tan is positive."""
    phi = coherence_to_phi(0)
    result = sici_axial_decay(phi)
    assert result > 0.0


def test_sici_axial_decay_gamma_scales_linearly():
    """Doubling Γ₀ doubles the axial decay."""
    phi = coherence_to_phi(0)
    v1 = sici_axial_decay(phi, gamma_0=1.0)
    v2 = sici_axial_decay(phi, gamma_0=2.0)
    assert v2 == pytest.approx(2.0 * v1, rel=1e-6)


def test_sici_axial_decay_large_phi_shrinks():
    """At large φ Ci(φ) → 0, so |Δλ_axial| decreases."""
    small = abs(sici_axial_decay(coherence_to_phi(1)))
    large = abs(sici_axial_decay(coherence_to_phi(20)))
    assert large < small


# ── sici_phase_weight ─────────────────────────────────────────────────────────

def test_sici_phase_weight_returns_near_one():
    """The phase weight must stay within [1 − scale, 1 + scale]."""
    lo = 1.0 - _SICI_SCALE_FACTOR
    hi = 1.0 + _SICI_SCALE_FACTOR
    for c in range(12):
        w = sici_phase_weight(c)
        assert lo <= w <= hi, f"sici_phase_weight({c})={w} out of [{lo}, {hi}]"


def test_sici_phase_weight_large_coherence_approaches_one():
    """At large coherence Ci(φ) → 0, so weight → 1.0."""
    for c in (100, 500, 1000):
        w = sici_phase_weight(c)
        assert abs(w - 1.0) < 0.002, f"sici_phase_weight({c})={w} not near 1.0"


def test_sici_phase_weight_coherence_0_above_one():
    """At coherence=0 the axial product is positive so weight > 1.0."""
    assert sici_phase_weight(0) > 1.0


# ── wavefunction_overlap ──────────────────────────────────────────────────────

def test_wavefunction_overlap_identical_vectors_is_one():
    """⟨ψ|ψ⟩² = 1 for any non-zero vector."""
    assert wavefunction_overlap([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == pytest.approx(1.0, abs=1e-6)


def test_wavefunction_overlap_orthogonal_vectors_is_zero():
    """⟨ψ_a|ψ_b⟩² = 0 for orthogonal vectors."""
    assert wavefunction_overlap([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0, abs=1e-6)


def test_wavefunction_overlap_parallel_scaled_is_one():
    """Scaling a vector does not change the overlap (normalised inner product)."""
    assert wavefunction_overlap([1.0, 0.0], [5.0, 0.0]) == pytest.approx(1.0, abs=1e-6)


def test_wavefunction_overlap_zero_vector_returns_zero():
    """Zero norm vector → 0.0 (no meaningful state)."""
    assert wavefunction_overlap([0.0, 0.0], [1.0, 1.0]) == 0.0


def test_wavefunction_overlap_mismatched_length_returns_zero():
    """Vectors of different length → 0.0."""
    assert wavefunction_overlap([1.0, 2.0], [1.0, 2.0, 3.0]) == 0.0


def test_wavefunction_overlap_empty_returns_zero():
    """Empty vectors → 0.0."""
    assert wavefunction_overlap([], []) == 0.0


def test_wavefunction_overlap_range_zero_to_one():
    """Overlap is always in [0, 1] for real vectors."""
    vecs = [
        ([1.0, 0.0], [0.707, 0.707]),
        ([1.0, 1.0, 1.0], [1.0, -1.0, 0.0]),
        ([3.0, 4.0], [4.0, 3.0]),
    ]
    for a, b in vecs:
        ov = wavefunction_overlap(a, b)
        assert 0.0 <= ov <= 1.0


# ── floquet_modulation_factor ─────────────────────────────────────────────────

def test_floquet_modulation_factor_at_zero_is_one():
    """cos(0) = 1 — full coupling at t=0."""
    assert floquet_modulation_factor(0.0, omega=1.0) == pytest.approx(1.0)


def test_floquet_modulation_factor_at_half_period_is_minus_one():
    """cos(ω · π/ω) = cos(π) = −1."""
    omega = 2.5
    assert floquet_modulation_factor(math.pi / omega, omega) == pytest.approx(-1.0, abs=1e-10)


def test_floquet_modulation_factor_period():
    """cos(ω · 2π/ω) = cos(2π) = 1 — full period restores coupling."""
    omega = 3.7
    assert floquet_modulation_factor(2 * math.pi / omega, omega) == pytest.approx(1.0, abs=1e-10)


# ── holographic_entropy ───────────────────────────────────────────────────────

def test_holographic_entropy_scales_with_edges():
    """More boundary edges → higher entropy."""
    assert holographic_entropy(10, 5) > holographic_entropy(5, 5)


def test_holographic_entropy_zero_nodes():
    """n_nodes=0 → S = n_edges (boundary = volume)."""
    assert holographic_entropy(7, 0) == pytest.approx(7.0)


def test_holographic_entropy_non_negative():
    """Entropy must always be ≥ 0."""
    assert holographic_entropy(0, 100) == 0.0
    assert holographic_entropy(50, 200) >= 0.0


# ── metric_perturbation ───────────────────────────────────────────────────────

def test_metric_perturbation_positive_for_positive_mass():
    """h_μν > 0 for mass > 0, r > 0."""
    h = metric_perturbation(1.0e30, 1.0e10)
    assert h > 0.0


def test_metric_perturbation_zero_for_nonpositive_r():
    """h_μν = 0 at/within the event horizon (r ≤ 0)."""
    assert metric_perturbation(1.0e30, 0.0) == 0.0
    assert metric_perturbation(1.0e30, -1.0) == 0.0


def test_metric_perturbation_formula():
    """Verify  h = 2·G·M/(c²·r)."""
    M, r = 2.0e30, 1.0e11
    expected = 2.0 * _G_CONST * M / (_C_CONST ** 2 * r)
    assert metric_perturbation(M, r) == pytest.approx(expected, rel=1e-9)


# ── phase_evolution_total ─────────────────────────────────────────────────────

def test_phase_evolution_total_zero_contributions_is_axial_only():
    """With all δφ contributions = 0, result = axial term only."""
    phi = coherence_to_phi(0)
    axial = sici_axial_decay(phi)
    axial_phase = axial * (2.0 * math.pi)   # gamma_eff defaults to 1.0
    expected = axial_phase
    result = phase_evolution_total(phi)
    assert result == pytest.approx(expected, rel=1e-6)


def test_phase_evolution_total_adds_contributions():
    """δφ contributions are additive."""
    phi = coherence_to_phi(0)
    base = phase_evolution_total(phi)
    with_mu = phase_evolution_total(phi, delta_mu=0.5)
    assert with_mu == pytest.approx(base + 0.5, rel=1e-6)


# ── entropic_bayesian_step ────────────────────────────────────────────────────

def test_entropic_bayesian_step_increases_with_positive_laplacian():
    """Positive Laplacian → terrain increases."""
    phi = coherence_to_phi(0)
    s0 = 1.0
    s1 = entropic_bayesian_step(s0, laplacian_s=1.0, phi=phi, eta_diff=0.05)
    assert s1 > s0


def test_entropic_bayesian_step_decreases_with_negative_laplacian():
    """Sufficiently negative Laplacian dominates → terrain can decrease."""
    phi = coherence_to_phi(0)
    s0 = 1.0
    # Use a very large negative Laplacian to overpower the axial term.
    s1 = entropic_bayesian_step(s0, laplacian_s=-1000.0, phi=phi, eta_diff=0.05)
    assert s1 < s0


def test_entropic_bayesian_step_deterministic():
    """Same inputs always produce the same output."""
    phi = coherence_to_phi(2)
    s1 = entropic_bayesian_step(0.5, 0.1, phi)
    s2 = entropic_bayesian_step(0.5, 0.1, phi)
    assert s1 == s2


# ── ueqgm_coherence_score ─────────────────────────────────────────────────────

def test_ueqgm_coherence_score_missing_entity_returns_zero():
    """Target entity not in DB → 0.0."""
    cn = _ueqgm_db()
    assert ueqgm_coherence_score(cn, "does-not-exist") == 0.0


def test_ueqgm_coherence_score_no_ueqgm_entities_returns_zero():
    """Target exists but no UEQGM-tagged entities in corpus → 0.0."""
    cn = _ueqgm_db()
    _insert_entity(cn, "target-1", "Supply Chain KPI", '{"topic": "OTD"}')
    score = ueqgm_coherence_score(cn, "target-1")
    assert score == 0.0


def test_ueqgm_coherence_score_with_ueqgm_entity_positive():
    """Target overlaps with UEQGM entity → score > 0."""
    cn = _ueqgm_db()
    # Insert a UEQGM-tagged entity that will be matched.
    _insert_entity(
        cn,
        "ueqgm-paper-1",
        "Wavefunction dynamics quantum ueqgm",
        '{"tags": ["ueqgm", "wavefunction", "quantum"], "topic": "quantum dynamics"}',
    )
    # Target entity shares quantum/ueqgm keywords.
    _insert_entity(
        cn,
        "target-q",
        "quantum entropy holographic ueqgm",
        '{"tags": ["ueqgm", "holographic"]}',
    )
    score = ueqgm_coherence_score(cn, "target-q")
    assert score > 0.0


def test_ueqgm_coherence_score_unrelated_target_zero_overlap():
    """Target with no quantum keywords has zero overlap with UEQGM entities."""
    cn = _ueqgm_db()
    _insert_entity(
        cn,
        "ueqgm-entity",
        "quantum wavefunction holographic ueqgm",
        '{"tags": ["ueqgm", "wavefunction"]}',
    )
    _insert_entity(
        cn,
        "sc-entity",
        "inventory OTD delivery",
        '{"topic": "supply chain"}',
    )
    score = ueqgm_coherence_score(cn, "sc-entity")
    # Feature vec for sc-entity is all zeros against UEQGM keywords → 0.0
    assert score == 0.0


def test_ueqgm_coherence_score_bounded():
    """Score must be in [0.0, 1.2] — overlap ∈[0,1] × weight ∈[0.9,1.1]."""
    cn = _ueqgm_db()
    _insert_entity(
        cn,
        "ueqgm-ref",
        "quantum wavefunction holographic ueqgm floquet entanglement topological entropy",
        '{"tags": ["ueqgm"]}',
    )
    _insert_entity(
        cn,
        "target-rich",
        "quantum wavefunction holographic ueqgm floquet entanglement topological entropy",
        '{"tags": ["ueqgm"]}',
    )
    score = ueqgm_coherence_score(cn, "target-rich")
    assert 0.0 <= score <= 1.2
