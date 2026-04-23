import sys, re

path_version = "pipeline/src/brain/_version.py"
with open(path_version, "r", encoding="utf-8") as f:
    text_ver = f.read()

# Replace version
text_ver = text_ver.replace('__version__ = "0.14.4"', '__version__ = "0.14.5"')

old_release = '''__release__ = (

    "Continuous Multi-Agent Synaptic Extension. autonomous_agent.py: "'''

new_release = '''__release__ = (

    "OTD Recursive Hardening & Training Loop Fix. Fixed tuple unpacking for OTD loaders, "
    "restored offline fallback clustering for bundled files using OTDConfig, enforced "
    "get_global_window filter on trend charts, rewrote ownership tab to dynamic Daily "
    "Review worklists natively iterating over Excel worklist sheets. Repaired zero-samples "
    "bug in seed_otd_direct to seed otd_classify training truth out of bundled Excel "
    "sheet fallback so training cycle is unblocked offline."

)'''

text_ver = re.sub(r'__release__ = \(.*?\)', new_release, text_ver, flags=re.DOTALL)

new_phase = '''    "0.14.4": "Continuous Multi-Agent Synaptic Extension. Four daemon worker threads (synaptic-builder/10min/24h, lookahead/15min/rotating-7d-30d-90d, dispersed-sweeper/20min/connector-rotation, convergence/30min) run continuously underneath the main cycle, building synapses on relationally dispersed temporal windows so they're ready before the next agent traverses that aspect. rag_knowledge_deepdive() parameterised with window_label/window_hours/window_offset_hours/explored_kv_key for per-worker temporal targeting.",

    "0.14.5": "OTD Recursive Hardening & Training Loop Fix. Daily Review worklists, offline fallback clustering, strict TF trending windows, and seed_otd_direct offline ground-truth seeding.",'''

text_ver = text_ver.replace(
    '    "0.14.4": "Continuous Multi-Agent Synaptic Extension. Four daemon worker threads (synaptic-builder/10min/24h, lookahead/15min/rotating-7d-30d-90d, dispersed-sweeper/20min/connector-rotation, convergence/30min) run continuously underneath the main cycle, building synapses on relationally dispersed temporal windows so they\'re ready before the next agent traverses that aspect. rag_knowledge_deepdive() parameterised with window_label/window_hours/window_offset_hours/explored_kv_key for per-worker temporal targeting.",',
    new_phase
)

with open(path_version, "w", encoding="utf-8") as f:
    f.write(text_ver)


path_changelog = "pipeline/CHANGELOG.md"
with open(path_changelog, "r", encoding="utf-8") as f:
    text_chg = f.read()

new_log = """# Changelog

All notable changes to **Supply Chain Brain** are documented here. Versions
follow [Semantic Versioning](https://semver.org). The single source of
truth for the version number is `src/brain/_version.py`.

## 0.14.5 — OTD Recursive Hardening & Training Loop Fix (2026-04-22)

### Fixed
- **OTD File Parsing Tuples**: Fixed `_load_otd_file_from_path` unpacking errors due to missing tuple structures.
- **Offline Clustering Base Fallback**: Brought back `OTDConfig` so bundled files cluster appropriately without the replica instead of defaulting all rows to "ROOT".
- **Global Timeframe Bounds for Trend Charts**: Forced timeframe timeline filtering internally on trend charts to strictly observe the global bounds.
- **Dynamic Daily Review Tab Rewrite**: Ripped off the old generic ownership dropdown and dynamically constructed `Daily Review` worklists iteratively iterating through the native OTD bundle ("Missed Yesterday", "Shipping today", "Opened Yesterday").
- **OTD Classifier Zero-Samples Fallback Seed Loop**: Re-routed `seed_otd_direct` to load missing ground truth dynamically out of the local bundled Excel cache seamlessly mapping keys `SO No_Part_Site` when `azure_sql` is offline. Ensures the `otd_classify` zero-shot task has fresh samples to continuously self-train properly.

## 0.14.4"""

text_chg = text_chg.replace("""# Changelog

All notable changes to **Supply Chain Brain** are documented here. Versions
follow [Semantic Versioning](https://semver.org). The single source of
truth for the version number is `src/brain/_version.py`.

## 0.14.4""", new_log)

with open(path_changelog, "w", encoding="utf-8") as f:
    f.write(text_chg)

print("Versions updated.")
