"""rADAM — Relational/Rectified Adam with toroidal phase coupling.

A strict mathematical superset of the existing :func:`brain_body_signals._adam_step`
optimizer.  When all extension knobs are at their identity values, ``radam_step``
produces bit-for-bit the same trajectory as vanilla Adam.  Turn the knobs on and
the optimizer gains five additional behaviours, each of which corresponds to a
concrete piece of the Supply Chain Brain's existing closed loop:

    1. Complex (bifurcated) gradient  g_re + i*g_im
       - g_re : realised gradient (Touch firings + Resolved relief + Vision ops)
       - g_im : latent / unrealised gradient (Vision potential not yet observed)
       Magnitude drives Adam's moments; phase rotates the toroidal coordinate.

    2. Pivoted ReLU about a learned operating point pi_t (running mean pressure):
           pReLU(x; pi, alpha) = max(alpha*(x - pi), x - pi) + pi
       Expands the active region around the current nodal edge instead of zero.

    3. Heart-beat momentum modulation (syncopatic flow):
           beta1(t) = beta1_bar + kappa * sin(omega * t)
       Couples first-moment momentum to a slow oscillator. Phase-locks to the
       temporal_spatiality rhythm so high-coherence moments accelerate, jittery
       moments calm down.

    4. Langevin noise scaled by external incoherence  (1 - c_t):
           g_t  <-  g_t + sigma * sqrt(1 - c_t) * N(0, 1)
       c_t is sense_of_smell.carrier_mass (already in the brain).  External
       incoherence injects exploration; coherence collapses to deterministic.

    5. Toroidal projection with internal phase theta (closed loop) and external
       phase phi (injected from external system loops):
           theta_t = theta_{t-1} + arg(g_re + i*g_im)  (mod 2*pi)
           p_t     = 0.5 * (1 + cos(theta_t) * cos(phi_t))
       Pressure rides the surface of T^2 instead of a clamped scalar — the
       "expanding nodal edge of the toroid" with closed/external loop coupling.

Identity reduction
------------------
Calling ``radam_step(state, grad_real=g)`` with all other arguments at their
defaults reduces algebraically to::

    m = beta1*m + (1-beta1)*g
    v = beta2*v + (1-beta2)*g*g
    p = clip(p + lr * m_hat / (sqrt(v_hat) + eps), 0, 1)

i.e. the existing ``_adam_step`` body. Verified by ``test_radam_optimizer.py``.
"""

from __future__ import annotations

import math
import random
from typing import Mapping, MutableMapping

# ---- Defaults (chosen to match brain_body_signals._adam_step exactly) ----
DEFAULT_BETA1   = 0.85
DEFAULT_BETA2   = 0.999
DEFAULT_LR      = 0.30
DEFAULT_EPS     = 1e-6

# Extension knobs — identity values mean "behave like vanilla Adam".
IDENTITY_ALPHA  = 1.0   # pivoted-ReLU slope (1.0 == linear, no rectification)
IDENTITY_KAPPA  = 0.0   # heart-beat modulation amplitude
IDENTITY_OMEGA  = 0.0   # heart-beat angular frequency
IDENTITY_SIGMA  = 0.0   # Langevin noise scale
IDENTITY_COHER  = 1.0   # external coherence (1.0 == fully coherent => no noise)
IDENTITY_PHI    = 0.0   # external toroidal phase
IDENTITY_GIM    = 0.0   # imaginary (latent) gradient component


def pivoted_relu(x: float, pivot: float, alpha: float = IDENTITY_ALPHA) -> float:
    """Leaky ReLU rectified about ``pivot`` instead of zero.

        pReLU(x; pi, alpha) = max(alpha*(x - pi), x - pi) + pi

    With alpha == 1.0 this is the identity function (vanilla Adam path).
    With 0 < alpha < 1 inputs below the pivot are attenuated, inputs above
    pass through unchanged — the active region is centred on the current
    operating point rather than on zero.
    """
    delta = x - pivot
    if delta >= 0.0:
        return x
    return pivot + alpha * delta


def _heartbeat_beta1(t: int,
                     beta1_bar: float = DEFAULT_BETA1,
                     kappa: float = IDENTITY_KAPPA,
                     omega: float = IDENTITY_OMEGA) -> float:
    """beta1(t) = beta1_bar + kappa*sin(omega*t), clamped to (0, 1)."""
    if kappa == 0.0 or omega == 0.0:
        return beta1_bar
    b = beta1_bar + kappa * math.sin(omega * t)
    # Keep beta1 strictly inside (0, 1) so bias correction remains well defined.
    return min(0.999, max(0.001, b))


def radam_step(
    state: MutableMapping[str, float],
    grad_real: float,
    grad_imag: float = IDENTITY_GIM,
    *,
    lr: float = DEFAULT_LR,
    beta1: float = DEFAULT_BETA1,
    beta2: float = DEFAULT_BETA2,
    eps: float = DEFAULT_EPS,
    pivot_alpha: float = IDENTITY_ALPHA,
    heartbeat_kappa: float = IDENTITY_KAPPA,
    heartbeat_omega: float = IDENTITY_OMEGA,
    noise_sigma: float = IDENTITY_SIGMA,
    coherence: float = IDENTITY_COHER,
    external_phase: float = IDENTITY_PHI,
    rng: random.Random | None = None,
    use_torus: bool = False,
) -> float:
    """One rADAM update on a single signal_kind's pressure.

    ``state`` carries ``{m, v, t, pressure, pivot_ema, theta}``; mutated in place.
    All extension arguments default to their identity values, so the call

        radam_step(state, g)

    is mathematically identical to the existing ``_adam_step(state, g)``.

    Parameters
    ----------
    grad_real, grad_imag
        Bifurcated gradient. magnitude = ``sqrt(g_re^2 + g_im^2)`` drives the
        Adam moments; ``arg(g_re + i*g_im)`` advances the toroidal phase.
    pivot_alpha
        Pivoted-ReLU slope on the *update step* below the running pressure
        pivot. ``1.0`` disables rectification.
    heartbeat_kappa, heartbeat_omega
        Modulate beta1 as ``beta1 + kappa*sin(omega*t)``.  Both zero disables.
    noise_sigma, coherence
        Langevin noise. Effective sigma is ``noise_sigma * sqrt(1 - coherence)``.
        ``coherence == 1.0`` disables noise entirely.
    external_phase
        Externally injected toroidal phase phi.  Combined with internal theta
        as ``p = 0.5*(1 + cos(theta)*cos(phi))`` when ``use_torus=True``.
    use_torus
        If True, project onto T^2 instead of clamping to [0, 1].

    Returns the new pressure value in ``[0, 1]``.
    """
    # --- read prior state ---
    m_prev      = float(state.get("m", 0.0))
    v_prev      = float(state.get("v", 0.0))
    t_prev      = int(state.get("t", 0))
    p_prev      = float(state.get("pressure", 0.0))
    pivot_prev  = float(state.get("pivot_ema", p_prev))
    theta_prev  = float(state.get("theta", 0.0))
    t = t_prev + 1

    # --- 1. complex gradient: magnitude drives Adam, phase drives torus ---
    if grad_imag == 0.0:
        g_mag   = grad_real                         # preserve sign for Adam
        g_phase = 0.0
    else:
        g_norm  = math.hypot(grad_real, grad_imag)
        # Signed magnitude: keep the sign of the real component so positive /
        # negative directives still push pressure the correct way.
        g_mag   = math.copysign(g_norm, grad_real if grad_real != 0.0 else 1.0)
        g_phase = math.atan2(grad_imag, grad_real)

    # --- 4. Langevin noise scaled by (1 - coherence) ---
    if noise_sigma > 0.0 and coherence < 1.0:
        r = rng if rng is not None else random
        g_mag += noise_sigma * math.sqrt(max(0.0, 1.0 - coherence)) * r.gauss(0.0, 1.0)

    # --- 3. heart-beat modulated beta1 ---
    beta1_t = _heartbeat_beta1(t, beta1_bar=beta1,
                               kappa=heartbeat_kappa, omega=heartbeat_omega)

    # --- standard Adam moments + bias correction ---
    m = beta1_t * m_prev + (1.0 - beta1_t) * g_mag
    v = beta2   * v_prev + (1.0 - beta2)   * (g_mag * g_mag)
    m_hat = m / (1.0 - beta1_t ** t)
    v_hat = v / (1.0 - beta2   ** t)
    raw_step = lr * m_hat / (math.sqrt(v_hat) + eps)

    # --- 2. pivoted ReLU on the candidate post-step pressure ---
    candidate = p_prev + raw_step
    if pivot_alpha != IDENTITY_ALPHA:
        candidate = pivoted_relu(candidate, pivot=pivot_prev, alpha=pivot_alpha)

    # --- 5. toroidal projection (optional) ---
    theta = (theta_prev + g_phase) % (2.0 * math.pi)
    if use_torus:
        p_new = 0.5 * (1.0 + math.cos(theta) * math.cos(external_phase))
    else:
        p_new = max(0.0, min(1.0, candidate))

    # --- update pivot EMA (slow tracker of operating point) ---
    pivot_new = 0.95 * pivot_prev + 0.05 * p_new

    # --- write back ---
    state["m"]         = m
    state["v"]         = v
    state["t"]         = t
    state["pressure"]  = p_new
    state["pivot_ema"] = pivot_new
    state["theta"]     = theta
    return p_new


def reduces_to_vanilla_adam(state: Mapping[str, float],
                            grad: float,
                            lr: float = DEFAULT_LR,
                            beta1: float = DEFAULT_BETA1,
                            beta2: float = DEFAULT_BETA2,
                            eps: float = DEFAULT_EPS) -> float:
    """Reference implementation of vanilla Adam on a single scalar parameter
    clamped to [0, 1].  Used by tests to verify radam_step's identity reduction.
    """
    m_prev = float(state.get("m", 0.0))
    v_prev = float(state.get("v", 0.0))
    t      = int(state.get("t", 0)) + 1
    p_prev = float(state.get("pressure", 0.0))
    m = beta1 * m_prev + (1.0 - beta1) * grad
    v = beta2 * v_prev + (1.0 - beta2) * grad * grad
    m_hat = m / (1.0 - beta1 ** t)
    v_hat = v / (1.0 - beta2 ** t)
    return max(0.0, min(1.0, p_prev + lr * m_hat / (math.sqrt(v_hat) + eps)))
