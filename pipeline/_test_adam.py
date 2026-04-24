from src.brain.brain_body_signals import _adam_step, _bayesian_poisson_centroid, _RESOLVED_GRAD

print("=== Firing rounds (kind ramps from low to high pressure) ===")
state = {}
for i, (count, prio) in enumerate([(2, 0.5), (3, 0.6), (4, 0.7), (5, 0.8), (6, 0.9)], 1):
    target = _bayesian_poisson_centroid(state, count, prio)
    grad = target - state.get("pressure", 0.0)
    p = _adam_step(state, grad)
    m, v, t = state["m"], state["v"], state["t"]
    print(f"  r{i}: count={count} prio={prio} target={target:.3f} grad={grad:+.3f} -> p={p:.4f}  m={m:+.3f} v={v:.5f} t={t}")

print("=== Resolved rounds (synthetic negative gradient) ===")
for i in range(1, 4):
    grad = _RESOLVED_GRAD * state["pressure"]
    _ = _bayesian_poisson_centroid(state, 0, 0.0)
    p = _adam_step(state, grad)
    print(f"  resolved {i}: grad={grad:+.3f} -> p={p:.4f}  m={state['m']:+.3f} v={state['v']:.5f} t={state['t']}")

print("=== Flapping kind (fire/resolve every other round) ===")
state2 = {}
for i in range(1, 9):
    if i % 2:
        target = _bayesian_poisson_centroid(state2, 5, 0.7)
        grad = target - state2.get("pressure", 0.0)
        label = "FIRE  "
    else:
        grad = _RESOLVED_GRAD * state2.get("pressure", 0.0)
        _ = _bayesian_poisson_centroid(state2, 0, 0.0)
        label = "RESOLV"
    p = _adam_step(state2, grad)
    print(f"  {label} t={i}: grad={grad:+.3f} -> p={p:.4f}  v={state2['v']:.5f}  (high v damps step)")

print("=== Steady firing (centroid should converge) ===")
state3 = {}
for i in range(1, 11):
    target = _bayesian_poisson_centroid(state3, 3, 0.6)
    grad = target - state3.get("pressure", 0.0)
    p = _adam_step(state3, grad)
    print(f"  r{i}: target={target:.3f} -> p={p:.4f}  v={state3['v']:.5f}")
