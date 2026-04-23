"""
test_oracle_fusion_navigation.py - Comprehensive navigation test for all module entry points

This script tests the complete navigation flow for each Oracle Fusion module WITHOUT
making any data changes. It:

1. Authenticates to DEV13
2. Navigates to each module's entry point
3. Validates that the page loaded correctly
4. Captures screenshots at each step
5. Generates a test report

Usage:
    python test_oracle_fusion_navigation.py
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

from playwright.sync_api import sync_playwright

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from oracle_fusion_utils import (
    load_session_cookies, take_screenshot, get_page_info,
    click_module_tab, click_show_more, click_tile, click_task_link,
    is_authenticated, wait_for_page_ready
)

# Config
HOST = "https://fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com"
SESSION_FILE = Path(__file__).parent / "oracle_session.json"
TEST_DIR = Path(__file__).parent / "navigation_tests" / datetime.now().strftime("%Y%m%d_%H%M%S")
TEST_DIR.mkdir(parents=True, exist_ok=True)

# Module navigation map: (module_tab, tile_name, task_name)
MODULES_TO_TEST = [
    # Order Management
    ("Order Management", "Orders", "Manage Orders"),

    # Supply Chain Execution - Inventory Management
    ("Supply Chain Execution", "Inventory Management (Classic)", "Manage Cycle Counts"),

    # Supply Chain Execution - Work Definition
    ("Supply Chain Execution", "Work Definition", "Manage Work Orders"),

    # Supply Chain Execution - Work Execution
    ("Supply Chain Execution", "Work Execution", "Manage Work Queue"),

    # Supply Chain Execution - Quality Management
    ("Supply Chain Execution", "Quality Management", "Manage Inspections"),

    # Supply Chain Execution - Cost Accounting
    ("Supply Chain Execution", "Cost Accounting", "Manage Costs"),

    # Supply Chain Execution - Receipt Accounting
    ("Supply Chain Execution", "Receipt Accounting", "Manage Receipts"),

    # Supply Chain Planning - Plan Inputs
    ("Supply Chain Planning", "Plan Inputs", "Manage Demand"),

    # Supply Chain Planning - Demand Management
    ("Supply Chain Planning", "Demand Management", "Manage Demand"),

    # Supply Chain Planning - Supply Planning
    ("Supply Chain Planning", "Supply Planning", "Manage Supply Plan"),

    # Supply Chain Planning - Replenishment Planning
    ("Supply Chain Planning", "Replenishment Planning", "Manage Reorder Points"),

    # Product Management - Product Development
    ("Product Management", "Product Development", "Manage Products"),

    # Product Management - Product Information
    ("Product Management", "Product Information Management", "Manage Products"),
]

# Test results
test_results = {
    "timestamp": datetime.now().isoformat(),
    "host": HOST,
    "total_modules": len(MODULES_TO_TEST),
    "passed": 0,
    "failed": 0,
    "partial": 0,
    "details": [],
}


def test_module_navigation(page, module_tab: str, tile_name: str, task_name: str,
                          test_index: int) -> Dict:
    """
    Test navigation to a module's entry point.

    Returns:
        Dict with test results
    """
    result = {
        "index": test_index,
        "module_tab": module_tab,
        "tile_name": tile_name,
        "task_name": task_name,
        "status": "PENDING",
        "steps_passed": 0,
        "error": None,
        "page_title": None,
        "page_url": None,
    }

    ss_prefix = f"test_{test_index:02d}_{module_tab.replace(' ', '_')}"

    try:
        # Step 1: Navigate to FuseWelcome
        print(f"\n[{test_index}] Testing: {module_tab} → {tile_name} → {task_name}")
        print("  Step 1: Navigate to FuseWelcome...", end=" ")
        page.goto(f"{HOST}/fscmUI/faces/FuseWelcome",
                 wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(2000)
        result["steps_passed"] += 1
        print("✓")

        # Step 2: Click module tab
        print("  Step 2: Click module tab...", end=" ")
        if not click_module_tab(page, module_tab, wait_ms=2500):
            print("✗")
            result["status"] = "FAILED"
            result["error"] = f"Failed to click tab '{module_tab}'"
            take_screenshot(page, TEST_DIR, f"{ss_prefix}_error_tab")
            return result
        result["steps_passed"] += 1
        print("✓")
        take_screenshot(page, TEST_DIR, f"{ss_prefix}_01_tab_clicked")

        # Step 3: Click Show More
        print("  Step 3: Click Show More...", end=" ")
        click_show_more(page, max_clicks=3)
        result["steps_passed"] += 1
        print("✓")
        take_screenshot(page, TEST_DIR, f"{ss_prefix}_02_show_more")

        # Step 4: Click tile
        print(f"  Step 4: Click tile '{tile_name}'...", end=" ")
        if not click_tile(page, tile_name, wait_ms=5000):
            print("✗")
            result["status"] = "PARTIAL"
            result["error"] = f"Failed to click tile '{tile_name}'"
            take_screenshot(page, TEST_DIR, f"{ss_prefix}_error_tile")
            return result
        result["steps_passed"] += 1
        print("✓")
        take_screenshot(page, TEST_DIR, f"{ss_prefix}_03_tile_clicked")

        # Step 5: Click task link
        print(f"  Step 5: Click task '{task_name}'...", end=" ")
        if not click_task_link(page, task_name, wait_ms=3000):
            print("✗")
            result["status"] = "PARTIAL"
            result["error"] = f"Failed to click task '{task_name}'"
            take_screenshot(page, TEST_DIR, f"{ss_prefix}_error_task")
            return result
        result["steps_passed"] += 1
        print("✓")
        take_screenshot(page, TEST_DIR, f"{ss_prefix}_04_task_clicked")

        # Step 6: Verify page loaded
        print("  Step 6: Verify page loaded...", end=" ")
        page_info = get_page_info(page)
        result["page_title"] = page_info["title"]
        result["page_url"] = page_info["url"]

        if page_info["is_loading"]:
            print("⚠ (still loading)")
            result["status"] = "PARTIAL"
        elif not is_authenticated(page):
            print("✗")
            result["status"] = "FAILED"
            result["error"] = "Not authenticated"
            return result
        else:
            print("✓")
            result["status"] = "PASSED"
            result["steps_passed"] += 1

        take_screenshot(page, TEST_DIR, f"{ss_prefix}_05_final_state")

        return result

    except Exception as e:
        print(f"✗ EXCEPTION: {str(e)[:80]}")
        result["status"] = "FAILED"
        result["error"] = str(e)[:200]
        take_screenshot(page, TEST_DIR, f"{ss_prefix}_error_exception")
        return result


def generate_test_report():
    """Generate a comprehensive test report."""
    print("\n" + "="*70)
    print("GENERATING TEST REPORT")
    print("="*70)

    # Calculate summary
    test_results["passed"] = len([r for r in test_results["details"] if r["status"] == "PASSED"])
    test_results["partial"] = len([r for r in test_results["details"] if r["status"] == "PARTIAL"])
    test_results["failed"] = len([r for r in test_results["details"] if r["status"] == "FAILED"])

    test_results["success_rate"] = (test_results["passed"] / test_results["total_modules"] * 100
                                    if test_results["total_modules"] > 0 else 0)

    # Write JSON report
    report_path = TEST_DIR / "navigation_test_report.json"
    with open(report_path, "w") as f:
        json.dump(test_results, f, indent=2)

    # Write text report
    text_report = TEST_DIR / "navigation_test_report.txt"
    with open(text_report, "w") as f:
        f.write("ORACLE FUSION NAVIGATION TEST REPORT\n")
        f.write("="*70 + "\n\n")
        f.write(f"Timestamp: {test_results['timestamp']}\n")
        f.write(f"Host: {test_results['host']}\n\n")

        f.write("SUMMARY\n")
        f.write("-"*70 + "\n")
        f.write(f"Total Modules Tested: {test_results['total_modules']}\n")
        f.write(f"Passed: {test_results['passed']} ({test_results['passed']/test_results['total_modules']*100:.1f}%)\n")
        f.write(f"Partial: {test_results['partial']}\n")
        f.write(f"Failed: {test_results['failed']}\n")
        f.write(f"Success Rate: {test_results['success_rate']:.1f}%\n\n")

        f.write("DETAILED RESULTS\n")
        f.write("-"*70 + "\n")
        for i, result in enumerate(test_results["details"], 1):
            f.write(f"\n[{result['index']}] {result['module_tab']} → {result['tile_name']} → {result['task_name']}\n")
            f.write(f"    Status: {result['status']}\n")
            f.write(f"    Steps Passed: {result['steps_passed']}/6\n")
            if result["error"]:
                f.write(f"    Error: {result['error']}\n")
            if result["page_title"]:
                f.write(f"    Page Title: {result['page_title'][:80]}\n")

        f.write("\n" + "="*70 + "\n")
        f.write("FAILED/PARTIAL MODULES\n")
        f.write("-"*70 + "\n")
        failed_partial = [r for r in test_results["details"] if r["status"] in ["FAILED", "PARTIAL"]]
        if failed_partial:
            for r in failed_partial:
                f.write(f"• {r['module_tab']} > {r['tile_name']}: {r['error']}\n")
        else:
            f.write("None — all modules passed!\n")

    print(f"\n✓ Test report saved to: {TEST_DIR}")
    print(f"  - {report_path}")
    print(f"  - {text_report}")
    print(f"\nSUMMARY:")
    print(f"  Passed: {test_results['passed']}/{test_results['total_modules']} ({test_results['success_rate']:.1f}%)")
    print(f"  Partial: {test_results['partial']}")
    print(f"  Failed: {test_results['failed']}")


def main():
    """Run all navigation tests."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # headless=False to see browser
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()

        try:
            # Load session
            print("Loading session cookies...")
            if SESSION_FILE.exists():
                if load_session_cookies(context, SESSION_FILE):
                    print("✓ Session cookies loaded")
                else:
                    print("⚠ Could not load session cookies — proceeding without")
            else:
                print(f"⚠ {SESSION_FILE} not found — proceeding without cookies")

            print("\n" + "="*70)
            print("STARTING NAVIGATION TESTS")
            print(f"Total modules to test: {len(MODULES_TO_TEST)}")
            print("="*70)

            # Run tests
            for i, (module_tab, tile_name, task_name) in enumerate(MODULES_TO_TEST, 1):
                result = test_module_navigation(page, module_tab, tile_name, task_name, i)
                test_results["details"].append(result)
                page.wait_for_timeout(1000)  # Brief pause between tests

        except Exception as e:
            print(f"\nFATAL ERROR: {e}")
            take_screenshot(page, TEST_DIR, "error_fatal")
            raise
        finally:
            generate_test_report()
            browser.close()


if __name__ == "__main__":
    main()
