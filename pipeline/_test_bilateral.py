"""Validate bilateral closed loop: Touch + Vision both feed ADAM."""
from src.brain.brain_body_signals import (
    surface_effective_signals, get_touch_field, get_touch_field_full,
    _vision_ops_gradients, _VISION_OPS_MAP,
)
import json

print("=== Vision-ops → gradient mapping ===")
print(json.dumps({k: v for k, v in _VISION_OPS_MAP.items()}, indent=2, default=str))

print("\n=== Synthetic Vision-only ops (no Touch directives) ===")
fake_ops = {"dw_entities": 32, "ocw_entities": 369,
            "network_endpoints": 8, "schema_learnings": 12,
            "forced_blades": ["ocw"]}
print("vision_ops:", fake_ops)
print("computed gradients:", json.dumps(_vision_ops_gradients(fake_ops), indent=2))

print("\n=== Live surface call with vision_ops injected ===")
out = surface_effective_signals(vision_ops=fake_ops) or {}
keys = ["directives_emitted", "directives_expired", "resolved_kinds",
        "vision_grads_in", "top_priority"]
print(json.dumps({k: out.get(k) for k in keys}, indent=2, default=str))

print("\n=== Pressure view after bilateral update ===")
print(json.dumps(get_touch_field(), indent=2))

print("\n=== Full ADAM state (kinds touched by Vision) ===")
fs = get_touch_field_full()
touched = ["missing_category", "high_centrality_part", "corpus_rag_saturated",
           "model_low_task_weight", "peer_unreachable",
           "network_learner_not_started", "self_train_drift"]
for k in touched:
    if k in fs:
        s = fs[k]
        print(f"  {k:35s}  p={s.get('pressure', 0):.4f}  m={s.get('m', 0):+.3f}  "
              f"v={s.get('v', 0):.5f}  t={s.get('t', 0)}")
