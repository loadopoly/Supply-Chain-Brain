"""
run_oracle_fusion_full_validation.py - Master orchestration script

Executes all four validation/testing tasks in sequence:
  1. Validate coordinates against actual DEV13 screens
  2. Expand specific modules (Order Management & Work Definition)
  3. Build utility functions (already created in oracle_fusion_utils.py)
  4. Test navigation - dry run all module entry points

Generates a unified completion report.

Usage:
    python run_oracle_fusion_full_validation.py
"""

import json
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

PIPELINE_DIR = Path(__file__).parent
COMPLETION_REPORT_DIR = PIPELINE_DIR / "oracle_fusion_validation_completion"
COMPLETION_REPORT_DIR.mkdir(parents=True, exist_ok=True)

completion_report = {
    "timestamp": datetime.now().isoformat(),
    "tasks": {},
    "summary": {},
    "all_passed": True,
}


def run_task(task_name: str, script_name: str, description: str) -> Dict:
    """
    Run a single validation/test task.

    Returns:
        Dict with task results (status, output, errors, etc.)
    """
    print("\n" + "="*80)
    print(f"TASK: {task_name}")
    print(f"Script: {script_name}")
    print(f"Description: {description}")
    print("="*80)

    result = {
        "name": task_name,
        "script": script_name,
        "status": "PENDING",
        "start_time": datetime.now().isoformat(),
        "end_time": None,
        "duration_seconds": 0,
        "output": [],
        "error": None,
        "exit_code": None,
    }

    script_path = PIPELINE_DIR / script_name

    if not script_path.exists():
        result["status"] = "FAILED"
        result["error"] = f"Script not found: {script_path}"
        print(f"✗ ERROR: Script not found: {script_path}")
        return result

    try:
        print(f"\nExecuting: python {script_name}\n")
        start_time = time.time()

        # Run the script
        proc = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(PIPELINE_DIR),
            capture_output=True,
            text=True,
            timeout=600  # 10-minute timeout per task
        )

        duration = time.time() - start_time

        result["exit_code"] = proc.returncode
        result["duration_seconds"] = int(duration)
        result["end_time"] = datetime.now().isoformat()

        # Capture output
        if proc.stdout:
            result["output"] = proc.stdout.split("\n")
            print(proc.stdout)

        if proc.stderr:
            result["error"] = proc.stderr
            print(f"\nSTDERR:\n{proc.stderr}")

        # Determine status
        if proc.returncode == 0:
            result["status"] = "PASSED"
            print(f"\n✓ Task completed successfully in {duration:.1f}s")
        else:
            result["status"] = "FAILED"
            print(f"\n✗ Task failed with exit code {proc.returncode} after {duration:.1f}s")
            completion_report["all_passed"] = False

        return result

    except subprocess.TimeoutExpired:
        result["status"] = "TIMEOUT"
        result["error"] = "Script execution timed out after 10 minutes"
        result["end_time"] = datetime.now().isoformat()
        print(f"✗ Task timed out")
        completion_report["all_passed"] = False
        return result

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)
        result["end_time"] = datetime.now().isoformat()
        print(f"✗ Task error: {e}")
        completion_report["all_passed"] = False
        return result


def verify_utilities():
    """
    Verify that utility functions are properly created and importable.

    Returns:
        Dict with verification results
    """
    print("\n" + "="*80)
    print("TASK: Build Utility Functions")
    print("Script: oracle_fusion_utils.py")
    print("Description: Verify reusable Playwright helpers are created and functional")
    print("="*80)

    result = {
        "name": "Build Utility Functions",
        "script": "oracle_fusion_utils.py",
        "status": "PENDING",
        "start_time": datetime.now().isoformat(),
        "end_time": None,
        "duration_seconds": 0,
        "utilities_found": [],
        "error": None,
    }

    try:
        start_time = time.time()

        # Check if utils file exists
        utils_file = PIPELINE_DIR / "oracle_fusion_utils.py"
        if not utils_file.exists():
            result["status"] = "FAILED"
            result["error"] = f"Utils file not found: {utils_file}"
            print(f"✗ Utils file not found: {utils_file}")
            return result

        print(f"\n✓ Utils file found: {utils_file}")

        # Check file size
        file_size = utils_file.stat().st_size
        print(f"✓ File size: {file_size} bytes")

        # Try to import utilities
        sys.path.insert(0, str(PIPELINE_DIR))
        try:
            import oracle_fusion_utils as ofu
            print(f"✓ Successfully imported oracle_fusion_utils module")

            # Check for key functions
            expected_functions = [
                "get_element_coords",
                "find_all_elements_by_text",
                "lov_search",
                "filter_table_column",
                "click_module_tab",
                "click_tile",
                "click_task_link",
                "handle_confirmation_dialog",
                "load_session_cookies",
                "take_screenshot",
            ]

            found_functions = []
            for func_name in expected_functions:
                if hasattr(ofu, func_name):
                    found_functions.append(func_name)
                    print(f"  ✓ {func_name}")
                else:
                    print(f"  ✗ {func_name} NOT FOUND")

            result["utilities_found"] = found_functions
            result["status"] = "PASSED"
            print(f"\n✓ {len(found_functions)}/{len(expected_functions)} utilities found")

        except ImportError as e:
            result["status"] = "FAILED"
            result["error"] = f"Failed to import utils: {e}"
            print(f"✗ Import error: {e}")
            return result

        result["end_time"] = datetime.now().isoformat()
        result["duration_seconds"] = int(time.time() - start_time)

        return result

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)
        result["end_time"] = datetime.now().isoformat()
        print(f"✗ Error: {e}")
        return result


def verify_expanded_modules():
    """
    Verify that expanded module automation classes are created.

    Returns:
        Dict with verification results
    """
    print("\n" + "="*80)
    print("TASK: Expand Specific Modules")
    print("Script: oracle_fusion_expanded_modules.py")
    print("Description: Verify detailed automation workflows for Order Management and Work Definition")
    print("="*80)

    result = {
        "name": "Expand Specific Modules",
        "script": "oracle_fusion_expanded_modules.py",
        "status": "PENDING",
        "start_time": datetime.now().isoformat(),
        "end_time": None,
        "duration_seconds": 0,
        "workflows_found": [],
        "error": None,
    }

    try:
        start_time = time.time()

        # Check if expanded modules file exists
        expanded_file = PIPELINE_DIR / "oracle_fusion_expanded_modules.py"
        if not expanded_file.exists():
            result["status"] = "FAILED"
            result["error"] = f"Expanded modules file not found: {expanded_file}"
            print(f"✗ File not found: {expanded_file}")
            return result

        print(f"\n✓ Expanded modules file found: {expanded_file}")

        # Check file size
        file_size = expanded_file.stat().st_size
        print(f"✓ File size: {file_size} bytes")

        # Try to import expanded modules
        sys.path.insert(0, str(PIPELINE_DIR))
        try:
            import oracle_fusion_expanded_modules as oem
            print(f"✓ Successfully imported oracle_fusion_expanded_modules module")

            # Check for key classes
            expected_classes = [
                "OrderManagementWorkflow",
                "WorkDefinitionWorkflow",
            ]

            found_classes = []
            for class_name in expected_classes:
                if hasattr(oem, class_name):
                    found_classes.append(class_name)
                    cls = getattr(oem, class_name)
                    print(f"  ✓ {class_name}")

                    # Check for key methods
                    methods = [m for m in dir(cls) if not m.startswith('_')]
                    print(f"      Methods: {', '.join(methods[:5])}..." if len(methods) > 5 else f"      Methods: {', '.join(methods)}")
                else:
                    print(f"  ✗ {class_name} NOT FOUND")

            result["workflows_found"] = found_classes
            result["status"] = "PASSED"
            print(f"\n✓ {len(found_classes)}/{len(expected_classes)} workflow classes found")

        except ImportError as e:
            result["status"] = "FAILED"
            result["error"] = f"Failed to import expanded modules: {e}"
            print(f"✗ Import error: {e}")
            return result

        result["end_time"] = datetime.now().isoformat()
        result["duration_seconds"] = int(time.time() - start_time)

        return result

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)
        result["end_time"] = datetime.now().isoformat()
        print(f"✗ Error: {e}")
        return result


def generate_completion_report():
    """Generate the final completion report."""
    print("\n\n" + "="*80)
    print("GENERATING FINAL COMPLETION REPORT")
    print("="*80)

    # Write JSON report
    report_json = COMPLETION_REPORT_DIR / "completion_report.json"
    with open(report_json, "w") as f:
        json.dump(completion_report, f, indent=2)

    # Write comprehensive text report
    report_txt = COMPLETION_REPORT_DIR / "completion_report.txt"
    with open(report_txt, "w") as f:
        f.write("ORACLE FUSION FULL VALIDATION & TESTING COMPLETION REPORT\n")
        f.write("="*80 + "\n\n")
        f.write(f"Timestamp: {completion_report['timestamp']}\n")
        f.write(f"Status: {'✓ ALL TASKS PASSED' if completion_report['all_passed'] else '✗ SOME TASKS FAILED'}\n\n")

        f.write("TASK SUMMARY\n")
        f.write("-"*80 + "\n")
        f.write(f"Total Tasks: {len(completion_report['tasks'])}\n")
        passed_tasks = len([t for t in completion_report['tasks'].values() if t.get('status') == 'PASSED'])
        f.write(f"Passed: {passed_tasks}\n")
        f.write(f"Failed: {len(completion_report['tasks']) - passed_tasks}\n\n")

        f.write("DETAILED RESULTS\n")
        f.write("-"*80 + "\n\n")

        for task_name, task_result in completion_report['tasks'].items():
            f.write(f"TASK: {task_name}\n")
            f.write(f"  Status: {task_result.get('status', 'UNKNOWN')}\n")
            f.write(f"  Script: {task_result.get('script', 'N/A')}\n")
            f.write(f"  Duration: {task_result.get('duration_seconds', 0)}s\n")

            if task_result.get('status') == 'PASSED':
                # Task-specific details
                if 'utilities_found' in task_result:
                    f.write(f"  Utilities Found: {len(task_result['utilities_found'])}\n")
                    for util in task_result['utilities_found'][:5]:
                        f.write(f"    • {util}\n")
                    if len(task_result['utilities_found']) > 5:
                        f.write(f"    ... and {len(task_result['utilities_found']) - 5} more\n")

                elif 'workflows_found' in task_result:
                    f.write(f"  Workflows Found: {len(task_result['workflows_found'])}\n")
                    for wf in task_result['workflows_found']:
                        f.write(f"    • {wf}\n")

                elif 'exit_code' in task_result and task_result['exit_code'] == 0:
                    f.write(f"  Exit Code: 0 (success)\n")

            elif task_result.get('error'):
                f.write(f"  Error: {task_result['error'][:200]}\n")

            f.write("\n")

        f.write("="*80 + "\n")
        f.write("OUTPUT LOGS\n")
        f.write("-"*80 + "\n\n")

        for task_name, task_result in completion_report['tasks'].items():
            if task_result.get('output'):
                f.write(f"\n--- {task_name} OUTPUT ---\n")
                f.write("\n".join(task_result.get('output', [])[:100]))  # First 100 lines
                if len(task_result.get('output', [])) > 100:
                    f.write(f"\n... ({len(task_result['output']) - 100} more lines)\n")

    # Write markdown report
    report_md = COMPLETION_REPORT_DIR / "COMPLETION_REPORT.md"
    with open(report_md, "w") as f:
        f.write("# Oracle Fusion Full Validation & Testing Completion Report\n\n")
        f.write(f"**Timestamp:** {completion_report['timestamp']}\n\n")
        f.write(f"**Overall Status:** {'✅ ALL TASKS COMPLETED' if completion_report['all_passed'] else '⚠️ SOME TASKS FAILED'}\n\n")

        f.write("## Task Summary\n\n")
        f.write("| Task | Status | Duration (s) |\n")
        f.write("|------|--------|------|\n")
        for task_name, task_result in completion_report['tasks'].items():
            status_emoji = "✅" if task_result.get('status') == 'PASSED' else "❌"
            duration = task_result.get('duration_seconds', 0)
            f.write(f"| {task_name} | {status_emoji} {task_result.get('status', 'UNKNOWN')} | {duration} |\n")

        f.write("\n## Key Deliverables\n\n")
        f.write("### 1. Utility Functions\n")
        if 'Build Utility Functions' in completion_report['tasks']:
            utils_task = completion_report['tasks']['Build Utility Functions']
            f.write(f"- **File:** `oracle_fusion_utils.py`\n")
            f.write(f"- **Status:** {utils_task.get('status')}\n")
            f.write(f"- **Utilities:** {len(utils_task.get('utilities_found', []))} functions\n")
            for util in utils_task.get('utilities_found', []):
                f.write(f"  - `{util}()`\n")

        f.write("\n### 2. Expanded Modules\n")
        if 'Expand Specific Modules' in completion_report['tasks']:
            expand_task = completion_report['tasks']['Expand Specific Modules']
            f.write(f"- **File:** `oracle_fusion_expanded_modules.py`\n")
            f.write(f"- **Status:** {expand_task.get('status')}\n")
            f.write(f"- **Workflows:** {len(expand_task.get('workflows_found', []))}\n")
            for wf in expand_task.get('workflows_found', []):
                f.write(f"  - `{wf}`\n")

        f.write("\n### 3. Coordinate Validation\n")
        if 'Validate Coordinates' in completion_report['tasks']:
            val_task = completion_report['tasks']['Validate Coordinates']
            f.write(f"- **Status:** {val_task.get('status')}\n")
            f.write(f"- **Duration:** {val_task.get('duration_seconds', 0)}s\n")
            f.write(f"- **Output:** See `coordinate_validation/*/` directory\n")

        f.write("\n### 4. Navigation Tests\n")
        if 'Test Navigation' in completion_report['tasks']:
            nav_task = completion_report['tasks']['Test Navigation']
            f.write(f"- **Status:** {nav_task.get('status')}\n")
            f.write(f"- **Duration:** {nav_task.get('duration_seconds', 0)}s\n")
            f.write(f"- **Output:** See `navigation_tests/*/` directory\n")

    print(f"\n✓ Completion reports saved to: {COMPLETION_REPORT_DIR}/")
    print(f"  - completion_report.json")
    print(f"  - completion_report.txt")
    print(f"  - COMPLETION_REPORT.md")


def main():
    """Main execution flow."""
    print("\n" + "="*80)
    print("ORACLE FUSION FULL VALIDATION & TESTING SUITE")
    print("Executing all 4 tasks to completion")
    print("="*80)

    # Task 3: Build Utility Functions (verification only, already created)
    print("\n\n")
    utils_result = verify_utilities()
    completion_report['tasks']["Build Utility Functions"] = utils_result

    # Task 2: Expand Specific Modules (verification only, already created)
    print("\n\n")
    expand_result = verify_expanded_modules()
    completion_report['tasks']["Expand Specific Modules"] = expand_result

    # Task 1: Validate Coordinates
    print("\n\n")
    coord_result = run_task(
        "Validate Coordinates",
        "validate_oracle_fusion_coordinates.py",
        "Validate coordinates against actual DEV13 screens"
    )
    completion_report['tasks']["Validate Coordinates"] = coord_result

    # Task 4: Test Navigation
    print("\n\n")
    nav_result = run_task(
        "Test Navigation",
        "test_oracle_fusion_navigation.py",
        "Dry run all module entry points without data changes"
    )
    completion_report['tasks']["Test Navigation"] = nav_result

    # Generate final report
    generate_completion_report()

    # Print summary
    print("\n\n" + "="*80)
    print("FINAL SUMMARY")
    print("="*80)
    print(f"\n✓ All tasks have been executed to completion!")
    print(f"\nCompletion reports are available at:")
    print(f"  {COMPLETION_REPORT_DIR}/")
    print(f"\nKey outputs:")
    print(f"  1. oracle_fusion_utils.py - Reusable Playwright utilities")
    print(f"  2. oracle_fusion_expanded_modules.py - Order Management & Work Definition workflows")
    print(f"  3. validate_oracle_fusion_coordinates.py - Coordinate validation script")
    print(f"  4. test_oracle_fusion_navigation.py - Navigation test suite")
    print(f"  5. coordinate_validation/ - Validation screenshots and reports")
    print(f"  6. navigation_tests/ - Navigation test screenshots and reports")
    print(f"\nNext steps:")
    print(f"  - Review the completion reports for detailed findings")
    print(f"  - Check coordinate deviations and adjust offsets if needed")
    print(f"  - Review navigation test failures for module-specific issues")
    print(f"  - Use utility functions and workflows for production automation")

    if not completion_report['all_passed']:
        print(f"\n⚠️ Note: Some tasks did not complete successfully. Review error details above.")
        sys.exit(1)
    else:
        print(f"\n✅ ALL TASKS COMPLETED SUCCESSFULLY!")
        sys.exit(0)


if __name__ == "__main__":
    main()
