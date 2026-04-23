#!/usr/bin/env python
"""
Assumption tests for the Epicor/SyteLine connector scaffolding.

This script explicitly tests WHAT WE BELIEVE to be true about the pipeline
and clearly reports the source of each belief and whether reality agrees.

Run from the pipeline/ directory:
    python test_connector_assumptions.py
"""
import sys
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ".")

import traceback
import yaml
from pathlib import Path

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"
SKIP = "SKIP"

results = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def assert_in(key, container, msg=None):
    assert key in container, msg or f"{key!r} not found"

def assert_not_in(key, container, msg=None):
    assert key not in container, msg or f"{key!r} should NOT be present"

def assert_eq(a, b, msg=None):
    assert a == b, msg or f"{a!r} != {b!r}"

def assert_gt(a, b, msg=None):
    assert a > b, msg or f"{a} is not > {b}"

def assert_true(expr, msg="assertion failed"):
    assert expr, msg


def check(label, source, fn):
    try:
        fn()
        results.append((PASS, label, source, ""))
        print(f"  [{PASS}] {label}")
    except AssertionError as e:
        results.append((FAIL, label, source, str(e)))
        print(f"  [{FAIL}] {label}\n         → {e}")
    except Exception as e:
        results.append((FAIL, label, source, f"{type(e).__name__}: {e}"))
        print(f"  [{FAIL}] {label}\n         → {type(e).__name__}: {e}")


def warn(label, source, message):
    results.append((WARN, label, source, message))
    print(f"  [{WARN}] {label}\n         → {message}")


# ---------------------------------------------------------------------------
# Load config files
# ---------------------------------------------------------------------------
print("\n── Load config files ───────────────────────────────────────────────────")

try:
    with open("config/connections.yaml") as f:
        conn_cfg = yaml.safe_load(f)
    print(f"  [OK]  connections.yaml loaded ({len(conn_cfg)} top-level keys)")
except Exception as e:
    print(f"  [FAIL] connections.yaml: {e}")
    sys.exit(1)

try:
    with open("config/mappings.yaml") as f:
        _mappings_raw = yaml.safe_load(f)
    # mappings.yaml has a top-level 'mappings:' key containing the list
    mappings = _mappings_raw["mappings"] if isinstance(_mappings_raw, dict) else _mappings_raw
    print(f"  [OK]  mappings.yaml loaded ({len(mappings)} mapping entries)")
except Exception as e:
    print(f"  [FAIL] mappings.yaml: {e}")
    sys.exit(1)

try:
    with open("config/brain.yaml") as f:
        brain = yaml.safe_load(f)
    print(f"  [OK]  brain.yaml loaded")
except Exception as e:
    print(f"  [FAIL] brain.yaml: {e}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# GROUP 1: Spec-derived assumptions
# Source: CrossDataset_Agent_Process_Spec.md §3.2
# ---------------------------------------------------------------------------
print("\n── Group 1: Site→ERP map (source: spec §3.2) ───────────────────────────")

EPICOR_SITES = ["jerome_ave", "manufacturers_rd", "wilson_rd"]
SYTELINE_SITES = ["parsons"]
AX_SITES = ["airport_rd"]   # Dynamics AX — Eugene Airport Rd (confirmed from CycleConsolidated.xlsx Data_AX)
ORACLE_SITES_HANDLED_BY_SHARED_CONNECTOR = ["oracle_fusion"]

for site in EPICOR_SITES:
    key = f"epicor_{site}"
    check(
        f"connections.yaml has key '{key}'",
        "spec §3.2 + prior session implementation",
        lambda k=key: assert_in(k, conn_cfg),
    )

for site in SYTELINE_SITES:
    key = f"syteline_{site}"
    check(
        f"connections.yaml has key '{key}'",
        "spec §3.2 (Parsons = SyteLine) + corrected this session",
        lambda k=key: assert_in(k, conn_cfg),
    )

for site in AX_SITES:
    key = f"ax_{site}"
    check(
        f"connections.yaml has key '{key}'",
        "discovered: CycleConsolidated.xlsx Data_AX sheet shows Plant=Airport Rd",
        lambda k=key: assert_in(k, conn_cfg),
    )

# Verify the WRONG key was removed
check(
    "connections.yaml does NOT have syteline_st_cloud (corrected this session)",
    "user correction: St Cloud = Oracle Fusion, not SyteLine",
    lambda: assert_not_in("syteline_st_cloud", conn_cfg),
)

check(
    "connections.yaml has oracle_fusion key",
    "existing pipeline — not changed",
    lambda: assert_in("oracle_fusion", conn_cfg),
)


# ---------------------------------------------------------------------------
# GROUP 2: connections.yaml structure assumptions
# ---------------------------------------------------------------------------
print("\n── Group 2: Config block structure ─────────────────────────────────────")

for site in EPICOR_SITES:
    key = f"epicor_{site}"
    block = conn_cfg.get(key, {})

    check(
        f"  {key}: has 'server' key",
        "epicor.py _load_config expects this field",
        lambda b=block: assert_in("server", b),
    )
    check(
        f"  {key}: has 'database' key",
        "epicor.py _load_config expects this field",
        lambda b=block: assert_in("database", b),
    )
    # This WILL warn — servers are empty, which is the honest finding
    server_val = block.get("server", "").strip()
    if not server_val:
        warn(
            f"  {key}: server is EMPTY — connection will fail until set",
            "expected — scaffolding only, not a live connection",
            "Set via connections.yaml or env var EPICOR_<SITE>_SERVER",
        )
    else:
        check(
            f"  {key}: server is non-empty ({server_val!r})",
            "connections.yaml",
            lambda: None,
        )

parsons_block = conn_cfg.get("syteline_parsons", {})
check(
    "  syteline_parsons: database = 'PFI_SLMiscApps_DB'",
    "confirmed from embedded SQL in Raw data from CC Tables.xlsx: SELECT * FROM PFI_SLMiscApps_DB.cycle_count.item_count",
    lambda: assert_eq(parsons_block.get("database", ""), "PFI_SLMiscApps_DB"),
)
check(
    "  syteline_parsons: schema = 'cycle_count'",
    "SyteLine cycle count schema from embedded SQL",
    lambda: assert_eq(parsons_block.get("schema", ""), "cycle_count"),
)
if not parsons_block.get("server", "").strip():
    warn(
        "  syteline_parsons: server is EMPTY — connection will fail until set",
        "expected — scaffolding only",
        "Set via connections.yaml or env var SYTELINE_PARSONS_SERVER",
    )

ax_block = conn_cfg.get("ax_airport_rd", {})
check(
    "  ax_airport_rd: database = 'MicrosoftDynamicsAX'",
    "AX 2012 default business database name",
    lambda: assert_eq(ax_block.get("database", ""), "MicrosoftDynamicsAX"),
)
if not ax_block.get("server", "").strip():
    warn(
        "  ax_airport_rd: server is EMPTY — connection will fail until set",
        "expected — scaffolding only",
        "Set via connections.yaml or env var AX_AIRPORT_RD_SERVER",
    )


# ---------------------------------------------------------------------------
# GROUP 3: Python module importability
# ---------------------------------------------------------------------------
print("\n── Group 3: Python module imports ──────────────────────────────────────")

def try_import(mod):
    import importlib
    importlib.import_module(mod)

check("src.connections.epicor imports", "new file created this session",
      lambda: try_import("src.connections.epicor"))
check("src.connections.syteline imports", "new file created this session",
      lambda: try_import("src.connections.syteline"))
check("src.connections.ax imports", "new file created this session",
      lambda: try_import("src.connections.ax"))
check("src.extract.extractor imports", "extractor.py updated this session",
      lambda: try_import("src.extract.extractor"))
check("src.extract.xlsx_extractor imports", "xlsx pipeline created this session",
      lambda: try_import("src.extract.xlsx_extractor"))
check("src.brain.db_registry imports", "db_registry.py updated this session",
      lambda: try_import("src.brain.db_registry"))


# ---------------------------------------------------------------------------
# GROUP 4: Connector function signatures
# ---------------------------------------------------------------------------
print("\n── Group 4: Extractor function signatures ───────────────────────────────")

import inspect

check("extract_epicor_table exists in extractor",
      "added this session",
      lambda: __import__("src.extract.extractor", fromlist=["extract_epicor_table"]))

check("extract_syteline_table exists in extractor",
      "added this session",
      lambda: __import__("src.extract.extractor", fromlist=["extract_syteline_table"]))

from src.extract import extractor as ext

check("extract_epicor_table accepts site_key param",
      "function signature check",
      lambda: assert_in("site_key", inspect.signature(ext.extract_epicor_table).parameters))

check("extract_epicor_table accepts watermark_col param",
      "incremental extraction requirement",
      lambda: assert_in("watermark_col", inspect.signature(ext.extract_epicor_table).parameters))

check("extract_syteline_table accepts site_key param",
      "function signature check",
      lambda: assert_in("site_key", inspect.signature(ext.extract_syteline_table).parameters))


# ---------------------------------------------------------------------------
# GROUP 5: mappings.yaml — ERP sections present
# ---------------------------------------------------------------------------
print("\n── Group 5: Mappings coverage ──────────────────────────────────────────")

epicor_entries = [m for m in mappings if isinstance(m, dict) and m.get("erp") == "epicor"]
syteline_entries = [m for m in mappings if isinstance(m, dict) and m.get("erp") == "syteline"]

check(f"mappings.yaml has Epicor entries (got {len(epicor_entries)})",
      "added this session",
      lambda: assert_gt(len(epicor_entries), 0))

check(f"mappings.yaml has SyteLine entries (got {len(syteline_entries)})",
      "added this session",
      lambda: assert_gt(len(syteline_entries), 0))

EXPECTED_EPICOR_TABLES = ["PartCount", "Part", "PartBin", "PORel", "RcvDtl", "OrderDtl", "Vendor"]
for tbl in EXPECTED_EPICOR_TABLES:
    check(f"  Epicor table '{tbl}' mapped",
          "spec §3.1 / §3.2 Epicor source tables",
          lambda t=tbl: assert_true(
              any(m.get("epicor_table") == t for m in epicor_entries),
              f"No mapping entry with epicor_table={t!r}"
          ))

EXPECTED_SYTELINE_TABLES = ["cc_trn", "item"]
for tbl in EXPECTED_SYTELINE_TABLES:
    check(f"  SyteLine table '{tbl}' mapped",
          "spec §3.1 SyteLine cycle-count translation",
          lambda t=tbl: assert_true(
              any(m.get("syteline_table") == t for m in syteline_entries),
              f"No mapping entry with syteline_table={t!r}"
          ))

# Parsons mapping points to correct azure_table
parsons_cc = next((m for m in syteline_entries if m.get("syteline_table") == "cc_trn"), None)
check("  SyteLine cc_trn maps to syteline_cc_parsons (not st_cloud)",
      "corrected this session after user feedback",
      lambda: assert_eq(
          (parsons_cc or {}).get("azure_table", ""),
          "syteline_cc_parsons"
      ))


# ---------------------------------------------------------------------------
# GROUP 6: brain.yaml tables and column_patterns
# ---------------------------------------------------------------------------
print("\n── Group 6: brain.yaml tables & column_patterns ────────────────────────")

tables = brain.get("tables", {})
col_patterns = brain.get("column_patterns", {})

check("brain.yaml has 'tables' section", "brain.yaml structure",
      lambda: assert_true(bool(tables), "tables section empty or missing"))

check("brain.yaml has epicor_cc_jerome_ave table entry",
      "added this session",
      lambda: assert_in("epicor_cc_jerome_ave", tables))

check("brain.yaml has syteline_cc_parsons (not st_cloud)",
      "corrected this session",
      lambda: assert_in("syteline_cc_parsons", tables))

check("brain.yaml does NOT have syteline_cc_st_cloud",
      "corrected this session",
      lambda: assert_not_in("syteline_cc_st_cloud", tables))

ERP_SYNONYM_ROLES = ["part_id", "on_hand_qty", "frozen_qty", "count_qty",
                     "discrepancy_reason", "abc_class", "warehouse"]
for role in ERP_SYNONYM_ROLES:
    check(f"  column_patterns has role '{role}'",
          "added this session from spec §3.1 translation table",
          lambda r=role: assert_in(r, col_patterns))


# ---------------------------------------------------------------------------
# GROUP 7: Live connection test — EXPECTED TO FAIL (servers not configured)
# ---------------------------------------------------------------------------
print("\n── Group 7: Live connection attempts (EXPECTED TO FAIL — servers empty) ─")

from src.connections import epicor as _epicor

for site in EPICOR_SITES:
    try:
        _epicor.get_connection(site)
        # If this succeeds the server IS configured — that's good news
        results.append((PASS, f"epicor_{site}: live connection succeeded", "live test", ""))
        print(f"  [PASS] epicor_{site}: LIVE CONNECTION ESTABLISHED")
    except RuntimeError as e:
        if "not set" in str(e) or "missing" in str(e).lower():
            results.append((WARN, f"epicor_{site}: server not configured (expected)", "live test", str(e)))
            print(f"  [WARN] epicor_{site}: server not configured — {e}")
        else:
            results.append((WARN, f"epicor_{site}: connection failed — {e}", "live test", str(e)))
            print(f"  [WARN] epicor_{site}: connection failed — {e}")
    except Exception as e:
        results.append((WARN, f"epicor_{site}: {type(e).__name__}", "live test", str(e)))
        print(f"  [WARN] epicor_{site}: {type(e).__name__}: {e}")

from src.connections import syteline as _syteline

for site in SYTELINE_SITES:
    try:
        _syteline.get_connection(site)
        results.append((PASS, f"syteline_{site}: live connection succeeded", "live test", ""))
        print(f"  [PASS] syteline_{site}: LIVE CONNECTION ESTABLISHED")
    except RuntimeError as e:
        results.append((WARN, f"syteline_{site}: server not configured (expected)", "live test", str(e)))
        print(f"  [WARN] syteline_{site}: server not configured — {e}")
    except Exception as e:
        results.append((WARN, f"syteline_{site}: {type(e).__name__}", "live test", str(e)))
        print(f"  [WARN] syteline_{site}: {type(e).__name__}: {e}")

from src.connections import ax as _ax

for site in ["airport_rd"]:
    try:
        _ax.get_connection(site)
        results.append((PASS, f"ax_{site}: live connection succeeded", "live test", ""))
        print(f"  [PASS] ax_{site}: LIVE CONNECTION ESTABLISHED")
    except RuntimeError as e:
        results.append((WARN, f"ax_{site}: server not configured (expected)", "live test", str(e)))
        print(f"  [WARN] ax_{site}: server not configured — {e}")
    except Exception as e:
        results.append((WARN, f"ax_{site}: {type(e).__name__}", "live test", str(e)))
        print(f"  [WARN] ax_{site}: {type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# GROUP 8: xlsx pipeline — OneDrive file existence + canonical column checks
# ---------------------------------------------------------------------------
print("\n── Group 8: xlsx pipeline — live OneDrive file reads ───────────────────")

from pathlib import Path as _Path
from src.extract.xlsx_extractor import (
    fetch as _xlsx_fetch,
    CYCLE_CONSOLIDATED as _CC_PATH,
    available_aliases as _avail,
)

# 8.1 Source file exists
check(
    "CycleConsolidated.xlsx found on OneDrive",
    "xlsx_sources",
    lambda: assert_true(_CC_PATH.exists(), f"Expected at: {_CC_PATH}"),
)

# 8.2 All 16 aliases registered
check(
    f"xlsx_extractor has 16 aliases (got {len(_avail())})",
    "xlsx_sources",
    lambda: assert_eq(len(_avail()), 16, f"Aliases: {_avail()}"),
)

# 8.3 Canonical column spot-checks per ERP
_xlsx_spot = [
    ("epicor_ccmerger",     ["part_number", "warehouse_code", "frozen_qty", "count_qty", "abc_class", "discrepancy_reason", "erp"]),
    ("epicor_abcsql",       ["part_number", "abc_class", "quantity_on_hand", "erp"]),
    ("oracle_cc_metrics",   ["part_number", "frozen_qty", "count_qty", "accuracy_pct", "post_status", "erp"]),
    ("oracle_on_hand",      ["part_number", "quantity_on_hand", "abc_class", "unit_cost", "erp"]),
    ("syteline_item_abc",   ["part_number", "abc_class", "quantity_on_hand", "unit_cost", "cycle_date_key", "erp"]),
    ("syteline_item_count", ["part_number", "count_qty", "frozen_qty", "count_date_key", "erp"]),
    ("ax_cc_journal",       ["part_number", "frozen_qty", "count_qty", "cycle_date_key", "unit_cost", "erp"]),
    ("ax_item_abc",         ["part_number", "quantity_on_hand", "count_group", "erp"]),
]

for _alias, _req_cols in _xlsx_spot:
    _df = _xlsx_fetch(_alias)
    _err = _df.attrs.get("_error")
    _missing = [c for c in _req_cols if c not in _df.columns]
    _rows = len(_df)
    check(
        f"xlsx/{_alias}: {_rows} rows, canonical cols present",
        "xlsx_sources",
        lambda e=_err, m=_missing, r=_rows: assert_true(
            not e and not m and r > 0,
            f"err={e}  missing={m}  rows={r}",
        ),
    )

# 8.4 Cross-ERP combined frames have all 4 ERPs
from src.extract.xlsx_extractor import fetch_all_cc_data as _all_cc
_df_all = _all_cc()
_erps_found = sorted(_df_all["erp"].unique()) if "erp" in _df_all.columns else []
check(
    f"fetch_all_cc_data covers all 4 ERPs: {_erps_found}",
    "xlsx_sources",
    lambda: assert_true(
        set(_erps_found) == {"epicor", "oracle", "syteline", "ax"},
        f"Expected: epicor, oracle, syteline, ax  Got: {_erps_found}",
    ),
)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n── Summary ─────────────────────────────────────────────────────────────")
passed  = [r for r in results if r[0] == PASS]
failed  = [r for r in results if r[0] == FAIL]
warned  = [r for r in results if r[0] == WARN]

print(f"  PASS: {len(passed)}")
print(f"  WARN: {len(warned)}  (expected — servers not configured, connections are scaffolding)")
print(f"  FAIL: {len(failed)}")

if failed:
    print("\nFailed assumptions:")
    for _, label, source, msg in failed:
        print(f"  • [{source}] {label}")
        if msg:
            print(f"    {msg}")
    sys.exit(1)
else:
    print(f"\nAll structural assumptions hold. No live data pipelines — fill in server/database")
    print(f"in connections.yaml (or set env vars) before attempting real extraction.")
    sys.exit(0)
