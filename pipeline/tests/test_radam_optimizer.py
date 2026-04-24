"""Tests for radam_optimizer — proves it is a strict superset of vanilla Adam
and that each extension knob behaves as documented."""

from __future__ import annotations

import math
import random

import pytest

from src.brain.radam_optimizer import (
    DEFAULT_BETA1, DEFAULT_BETA2, DEFAULT_EPS, DEFAULT_LR,
    pivoted_relu, radam_step, reduces_to_vanilla_adam,
)


# ---------------------------------------------------------------------------
# 1. Identity reduction — with all extension knobs at defaults, radam_step
#    must produce exactly the same trajectory as vanilla Adam.
# ---------------------------------------------------------------------------
def test_identity_reduction_matches_vanilla_adam():
    state_radam   = {"pressure": 0.2}
    state_vanilla = {"pressure": 0.2}
    grads = [0.5, -0.1, 0.3, 0.0, 0.4, -0.2, 0.15, 0.05]
    for g in grads:
        p_r = radam_step(state_radam, g)
        p_v = reduces_to_vanilla_adam(state_vanilla, g)
        # Mirror vanilla state forward
        state_vanilla["m"] = (DEFAULT_BETA1 * state_vanilla.get("m", 0.0)
                              + (1 - DEFAULT_BETA1) * g)
        state_vanilla["v"] = (DEFAULT_BETA2 * state_vanilla.get("v", 0.0)
                              + (1 - DEFAULT_BETA2) * g * g)
        state_vanilla["t"] = state_vanilla.get("t", 0) + 1
        state_vanilla["pressure"] = p_v
        assert p_r == pytest.approx(p_v, abs=1e-12), (
            f"rADAM diverged from vanilla Adam at g={g}: {p_r} vs {p_v}")


# ---------------------------------------------------------------------------
# 2. Pivoted ReLU pivots correctly and is identity at alpha=1.
# ---------------------------------------------------------------------------
def test_pivoted_relu_identity_at_alpha_one():
    for x in [-1.0, 0.0, 0.3, 0.5, 1.5]:
        assert pivoted_relu(x, pivot=0.5, alpha=1.0) == pytest.approx(x)


def test_pivoted_relu_attenuates_below_pivot():
    # At alpha=0.1, x=0.2 with pivot 0.5 → 0.5 + 0.1*(-0.3) = 0.47
    assert pivoted_relu(0.2, pivot=0.5, alpha=0.1) == pytest.approx(0.47)
    # Above pivot is identity
    assert pivoted_relu(0.7, pivot=0.5, alpha=0.1) == pytest.approx(0.7)


# ---------------------------------------------------------------------------
# 3. Complex gradient: imag-only input still moves Adam moments because the
#    magnitude is non-zero, AND advances the toroidal phase.
# ---------------------------------------------------------------------------
def test_complex_gradient_advances_phase():
    state = {"pressure": 0.5}
    radam_step(state, grad_real=1.0, grad_imag=1.0)
    # arg(1+i) = pi/4
    assert state["theta"] == pytest.approx(math.pi / 4, abs=1e-9)


def test_complex_gradient_magnitude_drives_moments():
    s_real = {"pressure": 0.5}
    s_cplx = {"pressure": 0.5}
    # |1 + 0i| == |0.6 + 0.8i| == 1.0; sign carried from real part
    radam_step(s_real, grad_real=1.0, grad_imag=0.0)
    radam_step(s_cplx, grad_real=0.6, grad_imag=0.8)
    assert s_real["pressure"] == pytest.approx(s_cplx["pressure"], abs=1e-9)


def test_grad_imag_zero_is_identity_to_vanilla():
    """grad_imag=0 must produce exactly the same result as not passing it."""
    s1 = {"pressure": 0.3}
    s2 = {"pressure": 0.3}
    radam_step(s1, grad_real=0.5)
    radam_step(s2, grad_real=0.5, grad_imag=0.0)
    assert s1["pressure"] == pytest.approx(s2["pressure"])
    assert s1["theta"] == pytest.approx(s2["theta"])


def test_nonzero_grad_imag_changes_pressure_and_phase():
    """Passing grad_imag != 0 must change both the magnitude (pressure) and
    the stored theta compared to grad_imag=0."""
    s_real = {"pressure": 0.3}
    s_cplx = {"pressure": 0.3}
    radam_step(s_real, grad_real=0.4, grad_imag=0.0)
    radam_step(s_cplx, grad_real=0.4, grad_imag=0.3)  # hypot(0.4,0.3)=0.5 ≠ 0.4
    # Magnitude differs → exact float values must differ (different computation paths)
    assert s_real["pressure"] != s_cplx["pressure"]
    # Phase differs → arg(0.4+0i)=0  vs  arg(0.4+0.3i)≠0
    assert s_real["theta"] != pytest.approx(s_cplx["theta"], abs=1e-9)


# ---------------------------------------------------------------------------
# 4. Langevin noise is gated by coherence: coherence=1 ⇒ deterministic.
# ---------------------------------------------------------------------------
def test_full_coherence_disables_noise():
    rng = random.Random(42)
    s1 = {"pressure": 0.3}
    s2 = {"pressure": 0.3}
    radam_step(s1, 0.4, noise_sigma=10.0, coherence=1.0, rng=rng)
    radam_step(s2, 0.4, noise_sigma=10.0, coherence=1.0, rng=rng)
    assert s1["pressure"] == pytest.approx(s2["pressure"])


def test_low_coherence_introduces_variance():
    p_runs = []
    for seed in range(20):
        rng = random.Random(seed)
        s = {"pressure": 0.3}
        radam_step(s, 0.4, noise_sigma=1.0, coherence=0.0, rng=rng)
        p_runs.append(s["pressure"])
    # Spread across seeds should be non-trivial
    assert max(p_runs) - min(p_runs) > 1e-3


# ---------------------------------------------------------------------------
# 5. Toroidal projection keeps pressure in [0, 1] and reflects the cos*cos law.
# ---------------------------------------------------------------------------
def test_torus_projection_in_unit_interval():
    state = {"pressure": 0.5}
    for _ in range(50):
        p = radam_step(state, grad_real=0.3, grad_imag=0.1,
                       use_torus=True, external_phase=0.7)
        assert 0.0 <= p <= 1.0


def test_torus_projection_zero_phases_gives_one():
    # theta=0, phi=0 → p = 0.5*(1 + 1*1) = 1.0
    state = {"pressure": 0.0, "theta": 0.0}
    p = radam_step(state, grad_real=0.0, grad_imag=0.0,
                   use_torus=True, external_phase=0.0)
    assert p == pytest.approx(1.0)


def test_external_phase_changes_torus_pressure():
    """Two calls with the same gradient but different external_phase must
    produce different pressures under torus projection."""
    s1 = {"pressure": 0.5, "theta": math.pi / 4}
    s2 = {"pressure": 0.5, "theta": math.pi / 4}
    p1 = radam_step(s1, 0.3, use_torus=True, external_phase=0.0)
    p2 = radam_step(s2, 0.3, use_torus=True, external_phase=math.pi / 2)
    assert p1 != pytest.approx(p2, abs=1e-6)


# ---------------------------------------------------------------------------
# 6. Heartbeat omega phase-locks beta1 oscillation and changes trajectory.
# ---------------------------------------------------------------------------
def test_heartbeat_modulation_changes_trajectory():
    # Use small gradients and a low starting pressure so neither run saturates
    # at the [0,1] clamp — the difference must be visible in the 1st moment.
    s_flat = {"pressure": 0.05}
    s_beat = {"pressure": 0.05}
    grads = [0.04, 0.03, 0.05, 0.02, 0.04, 0.03, 0.05, 0.02]
    for g in grads:
        radam_step(s_flat, g, heartbeat_kappa=0.0,  heartbeat_omega=0.897)
        radam_step(s_beat, g, heartbeat_kappa=0.25, heartbeat_omega=0.897)
    # Modulated beta1 produces different moment accumulation → different pressure
    assert s_flat["m"] != pytest.approx(s_beat["m"], abs=1e-8)


# ---------------------------------------------------------------------------
# 7. Convergence: rADAM with default knobs converges to a constant target
#    just like vanilla Adam (sanity check on the optimizer dynamics).
# ---------------------------------------------------------------------------
def test_converges_to_target():
    state = {"pressure": 0.0}
    target = 0.7
    for _ in range(200):
        g = target - state["pressure"]
        radam_step(state, g)
    assert state["pressure"] == pytest.approx(target, abs=0.05)

