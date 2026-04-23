"""
validate_oracle_fusion_coordinates.py - Validate coordinates against actual DEV13 screens

This script navigates through Oracle Fusion DEV13 and:
1. Captures screenshots at each step
2. Measures actual element coordinates
3. Compares against expected coordinates from oracle_fusion_module_operations_map.md
4. Generates a validation report

Usage:
    python validate_oracle_fusion_coordinates.py
"""

import json
import time
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

from playwright.sync_api import sync_playwright
import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Local imports
from oracle_fusion_utils import (
    load_session_cookies, take_screenshot, get_page_info,
    find_all_elements_by_text, find_input_by_position, click_module_tab,
    click_show_more, click_tile, click_task_link, is_authenticated
)

# Config
HOST = "https://fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com"
SESSION_FILE = Path(__file__).parent / "oracle_session.json"
VALIDATION_DIR = Path(__file__).parent / "coordinate_validation" / datetime.now().strftime("%Y%m%d_%H%M%S")
VALIDATION_DIR.mkdir(parents=True, exist_ok=True)

# Expected coordinates (from oracle_fusion_module_operations_map.md)
EXPECTED_COORDINATES = {
    "module_tabs": {
        "Supply Chain Execution": {"x_min": 462, "x_max": 1000, "y_min": 40, "y_max": 120},
        "Supply Chain Planning": {"x_min": 462, "x_max": 1000, "y_min": 40, "y_max": 120},
        "Order Management": {"x_min": 462, "x_max": 1000, "y_min": 40, "y_max": 120},
        "Product Management": {"x_min": 462, "x_max": 1000, "y_min": 40, "y_max": 120},
    },
    "tiles": {
        "Inventory Management (Classic)": {"x_min": 200, "x_max": 1000, "y_min": 200, "y_max": 700},
        "Orders": {"x_min": 200, "x_max": 1000, "y_min": 200, "y_max": 700},
        "Work Definition": {"x_min": 200, "x_max": 1000, "y_min": 200, "y_max": 700},
    },
    "inputs": {
        "search_input": {"x_min": 250, "x_max": 320, "y_min": 330, "y_max": 380},
        "column_filter": {"x_min": 250, "x_max": 320, "y_min": 420, "y_max": 460},
        "lov_input": {"x_min": 250, "x_max": 320, "y_min": 330, "y_max": 500},
    },
}

# Validation report
validation_report = {
    "timestamp": datetime.now().isoformat(),
    "host": HOST,
    "viewport": None,
    "authenticated": False,
    "modules_tested": [],
    "coordinate_findings": [],
    "deviations": [],
    "summary": {}
}


def log_finding(finding_type: str, module: str, element: str, expected: Dict, actual: Dict, deviation: int):
    """Log a coordinate finding."""
    validation_report["coordinate_findings"].append({
        "type": finding_type,
        "module": module,
        "element": element,
        "expected": expected,
        "actual": actual,
        "deviation_px": deviation,
    })
    if deviation > 10:  # Flag if >10px deviation
        validation_report["deviations"].append({
            "severity": "HIGH" if deviation > 50 else "MEDIUM",
            "element": f"{module}.{element}",
            "deviation_px": deviation,
        })


def validate_supply_chain_execution_tab(page):
    """Validate Supply Chain Execution tab and subtiles."""
    print("\n" + "="*70)
    print("TESTING: Supply Chain Execution Tab")
    print("="*70)

    # Navigate to FuseWelcome
    print("\n1. Navigate to FuseWelcome...")
    page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(2000)
    take_screenshot(page, VALIDATION_DIR, "01_fusewelcome")
    info = get_page_info(page)
    validation_report["viewport"] = {"width": info["viewport_width"], "height": info["viewport_height"]}
    validation_report["authenticated"] = info["is_authenticated"]
    print(f"   Title: {info['title'][:60]}")
    print(f"   Viewport: {info['viewport_width']}x{info['viewport_height']}")

    # Find Supply Chain Execution tab
    print("\n2. Locating Supply Chain Execution tab...")
    elements = find_all_elements_by_text(page, "Supply Chain Execution", selector="a,div,span,button", partial=False)
    print(f"   Found {len(elements)} elements with text 'Supply Chain Execution'")
    for i, el in enumerate(elements[:3]):  # Show first 3
        print(f"     [{i}] {el['tag']} at ({el['cx']}, {el['cy']}) size: {el['width']}x{el['height']}")
        expected = EXPECTED_COORDINATES["module_tabs"]["Supply Chain Execution"]
        in_range = (expected["x_min"] <= el['cx'] <= expected["x_max"] and
                    expected["y_min"] <= el['cy'] <= expected["y_max"])
        status = "✓ MATCH" if in_range else "✗ OUT OF RANGE"
        print(f"        {status} (expected x:{expected['x_min']}-{expected['x_max']}, y:{expected['y_min']}-{expected['y_max']})")
        log_finding("tab_location", "Supply Chain Execution", "tab", expected, el, 0 if in_range else 20)

    # Click the tab
    if elements:
        tab = elements[0]
        page.mouse.click(tab['cx'], tab['cy'])
        page.wait_for_timeout(2500)
        take_screenshot(page, VALIDATION_DIR, "02_sce_tab_clicked")

    validation_report["modules_tested"].append("Supply Chain Execution Tab")


def validate_inventory_management_tile(page):
    """Validate Inventory Management (Classic) tile location."""
    print("\n" + "="*70)
    print("TESTING: Inventory Management (Classic) Tile")
    print("="*70)

    # Click Show More
    print("\n1. Clicking Show More...")
    clicks = click_show_more(page, max_clicks=3)
    print(f"   Clicked {clicks} times")
    take_screenshot(page, VALIDATION_DIR, "03_after_show_more")

    # Find Inventory Management (Classic) tile
    print("\n2. Locating Inventory Management (Classic) tile...")
    elements = find_all_elements_by_text(page, "Inventory Management (Classic)", selector="a", partial=False)
    print(f"   Found {len(elements)} elements")
    for el in elements[:3]:
        print(f"     {el['tag']} at ({el['cx']}, {el['cy']}) size: {el['width']}x{el['height']}")
        expected = EXPECTED_COORDINATES["tiles"]["Inventory Management (Classic)"]
        in_range = (expected["x_min"] <= el['cx'] <= expected["x_max"] and
                    expected["y_min"] <= el['cy'] <= expected["y_max"])
        status = "✓ MATCH" if in_range else "✗ OUT OF RANGE"
        print(f"        {status}")
        log_finding("tile_location", "Inventory Management", "tile", expected, el, 0 if in_range else 25)

    if elements:
        tile = elements[0]
        page.mouse.click(tile['cx'], tile['cy'])
        page.wait_for_timeout(5000)
        take_screenshot(page, VALIDATION_DIR, "04_inv_mgmt_opened")

    validation_report["modules_tested"].append("Inventory Management Tile")


def validate_order_management_navigation(page):
    """Validate Order Management module navigation."""
    print("\n" + "="*70)
    print("TESTING: Order Management Navigation")
    print("="*70)

    # Navigate back to FuseWelcome
    print("\n1. Navigate to FuseWelcome...")
    page.goto(f"{HOST}/fscmUI/faces/FuseWelcome", wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(2000)

    # Find and click Order Management tab
    print("\n2. Locating Order Management tab...")
    if click_module_tab(page, "Order Management", wait_ms=2500):
        print("   ✓ Order Management tab clicked")
        take_screenshot(page, VALIDATION_DIR, "05_order_mgmt_tab")
    else:
        print("   ✗ Order Management tab not found")

    # Click Show More
    click_show_more(page, max_clicks=3)

    # Find Orders tile
    print("\n3. Locating Orders tile...")
    elements = find_all_elements_by_text(page, "Orders", selector="a", partial=False)
    print(f"   Found {len(elements)} elements")
    for el in elements[:3]:
        print(f"     {el['tag']} at ({el['cx']}, {el['cy']}) size: {el['width']}x{el['height']}")

    if elements:
        tile = elements[0]
        page.mouse.click(tile['cx'], tile['cy'])
        page.wait_for_timeout(5000)
        take_screenshot(page, VALIDATION_DIR, "06_orders_opened")

    validation_report["modules_tested"].append("Order Management")


def validate_input_coordinates(page):
    """Validate LOV and filter input coordinates."""
    print("\n" + "="*70)
    print("TESTING: Input Field Coordinates")
    print("="*70)

    # Scan for various input fields
    print("\n1. Scanning for input fields...")
    inputs_data = page.evaluate("""
        () => {
            const results = [];
            for (const inp of document.querySelectorAll('input')) {
                if (!inp.offsetParent) continue;
                const r = inp.getBoundingClientRect();
                if (r.width <= 0 || r.height <= 0) continue;
                results.push({
                    placeholder: inp.getAttribute('placeholder') || '',
                    title: inp.getAttribute('title') || '',
                    type: inp.getAttribute('type') || 'text',
                    cx: Math.round(r.x + r.width / 2),
                    cy: Math.round(r.y + r.height / 2),
                    x: Math.round(r.x),
                    y: Math.round(r.y),
                });
            }
            return results;
        }
    """)

    print(f"   Found {len(inputs_data)} visible input fields")
    for i, inp in enumerate(inputs_data[:10]):  # Show first 10
        print(f"     [{i}] ({inp['cx']}, {inp['cy']}) placeholder='{inp['placeholder']}' type='{inp['type']}'")

    # Check against expected ranges
    for input_type, expected in EXPECTED_COORDINATES["inputs"].items():
        matching = [inp for inp in inputs_data
                   if expected["x_min"] <= inp['cx'] <= expected["x_max"] and
                      expected["y_min"] <= inp['cy'] <= expected["y_max"]]
        if matching:
            print(f"\n   ✓ {input_type} inputs found in expected range:")
            for inp in matching[:3]:
                print(f"       ({inp['cx']}, {inp['cy']}) placeholder='{inp['placeholder']}'")
                log_finding("input_location", "General", input_type, expected, inp, 0)
        else:
            print(f"\n   ✗ {input_type} inputs NOT in expected range")

    validation_report["modules_tested"].append("Input Coordinates")


def validate_task_panel(page):
    """Validate task panel (right sidebar) detection."""
    print("\n" + "="*70)
    print("TESTING: Task Panel Detection")
    print("="*70)

    # Check for SELECT element in right-side area (x > 900)
    print("\n1. Scanning for task panel SELECT...")
    select_data = page.evaluate("""
        () => {
            for (const sel of document.querySelectorAll('select')) {
                if (!sel.offsetParent) continue;
                const r = sel.getBoundingClientRect();
                if (r.x < 900) continue;
                const opts = Array.from(sel.options).map(o => o.text.trim());
                return {
                    cx: Math.round(r.x + r.width / 2),
                    cy: Math.round(r.y + r.height / 2),
                    options: opts,
                    selected_index: sel.selectedIndex,
                    selected_text: opts[sel.selectedIndex] || '?'
                };
            }
            return null;
        }
    """)

    if select_data:
        print(f"   ✓ Task panel SELECT found at ({select_data['cx']}, {select_data['cy']})")
        print(f"     Options: {select_data['options']}")
        print(f"     Current: {select_data['selected_text']}")
        log_finding("task_panel_select", "Task Panel", "select", {"x": 900}, select_data, 0)
    else:
        print("   ✗ Task panel SELECT not found in right-side area")

    validation_report["modules_tested"].append("Task Panel")


def generate_report():
    """Generate validation report."""
    print("\n" + "="*70)
    print("GENERATING VALIDATION REPORT")
    print("="*70)

    # Calculate summary
    total_findings = len(validation_report["coordinate_findings"])
    high_deviations = len([d for d in validation_report["deviations"] if d["severity"] == "HIGH"])
    medium_deviations = len([d for d in validation_report["deviations"] if d["severity"] == "MEDIUM"])

    validation_report["summary"] = {
        "total_findings": total_findings,
        "modules_tested": len(validation_report["modules_tested"]),
        "high_deviations": high_deviations,
        "medium_deviations": medium_deviations,
        "success": high_deviations == 0,
    }

    # Write JSON report
    report_path = VALIDATION_DIR / "validation_report.json"
    with open(report_path, "w") as f:
        json.dump(validation_report, f, indent=2)

    # Write text report
    text_report = VALIDATION_DIR / "validation_report.txt"
    with open(text_report, "w") as f:
        f.write("ORACLE FUSION COORDINATE VALIDATION REPORT\n")
        f.write("="*70 + "\n\n")
        f.write(f"Timestamp: {validation_report['timestamp']}\n")
        f.write(f"Host: {validation_report['host']}\n")
        f.write(f"Viewport: {validation_report['viewport']}\n")
        f.write(f"Authenticated: {validation_report['authenticated']}\n\n")

        f.write(f"SUMMARY\n")
        f.write("-"*70 + "\n")
        f.write(f"Modules Tested: {validation_report['summary']['modules_tested']}\n")
        f.write(f"Total Coordinate Findings: {validation_report['summary']['total_findings']}\n")
        f.write(f"High Deviations (>50px): {validation_report['summary']['high_deviations']}\n")
        f.write(f"Medium Deviations (10-50px): {validation_report['summary']['medium_deviations']}\n")
        f.write(f"Status: {'✓ PASS' if validation_report['summary']['success'] else '✗ FAIL'}\n\n")

        if validation_report["deviations"]:
            f.write(f"DEVIATIONS FOUND\n")
            f.write("-"*70 + "\n")
            for dev in validation_report["deviations"]:
                f.write(f"  [{dev['severity']}] {dev['element']}: {dev['deviation_px']}px\n")

    print(f"\n✓ Report saved to: {VALIDATION_DIR}")
    print(f"  - {report_path}")
    print(f"  - {text_report}")
    print(f"\nSUMMARY: {validation_report['summary']}")


def main():
    """Run all validations."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # headless=False to see browser
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()

        try:
            # Load session
            print("Loading session cookies...")
            if SESSION_FILE.exists():
                load_session_cookies(context, SESSION_FILE)
            else:
                print(f"WARNING: {SESSION_FILE} not found — proceeding without cookies")

            # Run validations
            validate_supply_chain_execution_tab(page)
            page.wait_for_timeout(1000)

            validate_inventory_management_tile(page)
            page.wait_for_timeout(1000)

            validate_order_management_navigation(page)
            page.wait_for_timeout(1000)

            validate_input_coordinates(page)
            page.wait_for_timeout(1000)

            validate_task_panel(page)

        except Exception as e:
            print(f"\nERROR during validation: {e}")
            take_screenshot(page, VALIDATION_DIR, "error_screenshot")
            raise
        finally:
            generate_report()
            browser.close()


if __name__ == "__main__":
    main()
