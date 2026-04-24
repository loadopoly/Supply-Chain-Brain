"""
Oracle Fusion Intersection Map — Part 80446-04
=================================================
Cross-references the full Oracle Fusion schema map (oracle_schema_map.json)
with confirmed write operations for part 80446-04 (write_ops_report_v3.json)
to produce a flat document showing every module/task that touches this part,
and flagging adjacent modules where the part COULD appear.

Output:
  pipeline/pim_screenshots/80446-04/write_ops/intersection_map.json
  pipeline/pim_screenshots/80446-04/write_ops/intersection_map.txt
"""
import json, sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SCHEMA_FILE  = Path(__file__).parent / "oracle_schema_map.json"
WRITE_OPS    = Path(__file__).parent / "pim_screenshots" / "80446-04" / "write_ops" / "write_ops_report_v3.json"
OUT_JSON     = Path(__file__).parent / "pim_screenshots" / "80446-04" / "write_ops" / "intersection_map.json"
OUT_TXT      = Path(__file__).parent / "pim_screenshots" / "80446-04" / "write_ops" / "intersection_map.txt"

# ── confirmed touchpoints from find_write_ops_80446.py ───────────────────────
# These are derived from the write_ops_report and the probe run.
CONFIRMED = {
    "Procurement": {
        "Purchase Orders": {
            "evidence": "Part 80446-04 found as keyword in Agreements search. 'Buyer: Gard, Adam'.",
            "confirmed_tasks": ["Manage Agreements", "Manage Watchlist", "Save..."],
        },
        "Approved Supplier List Entries": {
            "evidence": "Manage Approved Supplier List Entries accessible from PO Tasks panel.",
            "confirmed_tasks": ["Manage Approved Supplier List Entries", "Manage Suppliers"],
        },
    },
    "Supply Chain Execution": {
        "Work Execution": {
            "evidence": "264 work orders found at org 3145_US_MAN_MFG with item=80446-04. "
                        "Part is classified 'Procured' in PIM but used as component in WOs.",
            "confirmed_tasks": ["Manage Work Orders", "Create", "Release"],
        },
        "Inventory Management (Classic)": {
            "evidence": "Item quantity management tasks apply to all stocked items including 80446-04.",
            "confirmed_tasks": [
                "Manage Item Quantities", "Create Miscellaneous Transaction",
                "Create Subinventory Transfer", "Create Interorganization Transfer",
                "Manage Reservations and Picks", "Manage Transfer Orders",
                "Manage Lots", "Manage Serial Numbers",
            ],
        },
    },
}

# ── adjacency rules: item attributes → likely-touched modules ────────────────
# 80446-04 is a Procured item used as a manufacturing component.
ADJACENT_KEYWORDS = [
    "purchase", "purchas", "requisition", "supplier", "agreement",
    "receipt", "receive", "inventory", "stock", "lot", "serial",
    "work order", "manufacturing", "cost", "planning", "supply",
    "item", "catalog", "price",
]

def all_tasks_for_module(mod_data: dict) -> list[str]:
    """Flatten all task texts from a module's task_sections."""
    tasks = []
    for sections in mod_data.get("task_sections", {}).values():
        for sec in sections:
            tasks.extend(t["text"] for t in sec.get("tasks", []))
    return tasks


def classify_module(tab: str, mod_name: str, tasks: list[str],
                    confirmed: dict) -> str:
    """Return 'confirmed' | 'adjacent' | 'low' for relevance to 80446-04."""
    # Confirmed: explicitly in the probe results
    if tab in confirmed and mod_name in confirmed[tab]:
        return "confirmed"
    # Adjacent: name or tasks contain procurement/inventory/planning keywords
    combined = (tab + " " + mod_name + " " + " ".join(tasks)).lower()
    hits = sum(1 for kw in ADJACENT_KEYWORDS if kw in combined)
    if hits >= 2:
        return "adjacent"
    return "low"


def build_intersection(schema: dict, write_ops: dict) -> dict:
    result = {
        "part": "80446-04",
        "summary": {
            "confirmed_modules": 0,
            "adjacent_modules": 0,
            "low_relevance_modules": 0,
            "total_confirmed_tasks": 0,
            "total_adjacent_tasks": 0,
        },
        "tabs": {}
    }

    for tab, tab_data in schema.items():
        tab_entry = {}
        for mod_name, mod_data in tab_data.get("modules", {}).items():
            if "error" in mod_data:
                continue
            tasks = all_tasks_for_module(mod_data)
            relevance = classify_module(tab, mod_name, tasks, CONFIRMED)

            # Pull confirmed task list and evidence if available
            conf_info = CONFIRMED.get(tab, {}).get(mod_name, {})

            entry = {
                "relevance": relevance,
                "page_title": mod_data.get("title", ""),
                "all_tasks": tasks,
                "confirmed_tasks": conf_info.get("confirmed_tasks", []),
                "evidence": conf_info.get("evidence", ""),
            }

            tab_entry[mod_name] = entry

            if relevance == "confirmed":
                result["summary"]["confirmed_modules"] += 1
                result["summary"]["total_confirmed_tasks"] += len(conf_info.get("confirmed_tasks", []))
            elif relevance == "adjacent":
                result["summary"]["adjacent_modules"] += 1
                result["summary"]["total_adjacent_tasks"] += len(tasks)
            else:
                result["summary"]["low_relevance_modules"] += 1

        if tab_entry:
            result["tabs"][tab] = tab_entry

    return result


def write_txt(intersection: dict, path: Path):
    lines = [
        "Oracle Fusion Intersection Map — Part 80446-04",
        "=" * 60,
        "",
        f"Confirmed modules : {intersection['summary']['confirmed_modules']}",
        f"Adjacent modules  : {intersection['summary']['adjacent_modules']}",
        f"Low-relevance     : {intersection['summary']['low_relevance_modules']}",
        f"Confirmed tasks   : {intersection['summary']['total_confirmed_tasks']}",
        "",
    ]

    # Output order: confirmed first, then adjacent, then low
    for relevance_filter in ("confirmed", "adjacent", "low"):
        label = relevance_filter.upper()
        lines.append(f"\n{'='*60}")
        lines.append(f"{label} MODULES")
        lines.append("=" * 60)

        for tab, tab_mods in intersection["tabs"].items():
            tab_printed = False
            for mod_name, entry in tab_mods.items():
                if entry["relevance"] != relevance_filter:
                    continue
                if not tab_printed:
                    lines.append(f"\n  [{tab}]")
                    tab_printed = True

                lines.append(f"    MODULE: {mod_name}")
                if entry["evidence"]:
                    lines.append(f"      Evidence: {entry['evidence']}")
                if entry["confirmed_tasks"]:
                    lines.append(f"      Confirmed write ops:")
                    for t in entry["confirmed_tasks"]:
                        lines.append(f"        + {t}")
                if entry["all_tasks"] and relevance_filter != "low":
                    lines.append(f"      All available tasks ({len(entry['all_tasks'])}):")
                    for t in entry["all_tasks"][:20]:
                        lines.append(f"        - {t}")
                    if len(entry["all_tasks"]) > 20:
                        lines.append(f"        ... +{len(entry['all_tasks'])-20} more")
                lines.append("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"TXT saved: {path}")


def main():
    with open(SCHEMA_FILE, encoding="utf-8") as f:
        schema = json.load(f)
    with open(WRITE_OPS, encoding="utf-8") as f:
        write_ops = json.load(f)

    intersection = build_intersection(schema, write_ops)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(intersection, f, indent=2, ensure_ascii=False)
    print(f"JSON saved: {OUT_JSON}")

    write_txt(intersection, OUT_TXT)

    # Print summary
    s = intersection["summary"]
    print(f"\nPart 80446-04 intersection summary:")
    print(f"  Confirmed modules : {s['confirmed_modules']}")
    print(f"  Adjacent modules  : {s['adjacent_modules']}")
    print(f"  Low-relevance     : {s['low_relevance_modules']}")
    print(f"  Confirmed tasks   : {s['total_confirmed_tasks']}")


if __name__ == "__main__":
    main()
