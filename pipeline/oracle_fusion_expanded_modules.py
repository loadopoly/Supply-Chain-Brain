"""
oracle_fusion_expanded_modules.py - Detailed automation workflows for Order Management and Work Definition

Provides step-by-step automation patterns for:
  - Order Management > Order Management (create, edit, submit orders)
  - Supply Chain Execution > Work Definition (create, edit, execute work orders)

Each workflow includes:
  - Navigation sequence
  - State detection
  - Error handling
  - Validation checks
  - Screenshots at each step
"""

import time
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
from playwright.sync_api import Page, BrowserContext

from oracle_fusion_utils import (
    take_screenshot, get_page_info, find_all_elements_by_text,
    find_input_by_position, click_module_tab, click_show_more,
    click_tile, click_task_link, lov_search, filter_table_column,
    handle_confirmation_dialog, wait_for_page_ready, get_element_coords
)


# ============================================================================
# ORDER MANAGEMENT > ORDER MANAGEMENT WORKFLOWS
# ============================================================================

class OrderManagementWorkflow:
    """Workflow for navigating and managing purchase orders."""

    def __init__(self, page: Page, host: str, ss_dir: Path):
        self.page = page
        self.host = host
        self.ss_dir = ss_dir
        self.log_messages = []

    def log(self, message: str):
        """Log a message."""
        print(f"  {message}")
        self.log_messages.append(message)

    def navigate_to_orders_list(self) -> bool:
        """
        Navigate to Order Management > Manage Orders list view.

        Expected sequence:
          1. Go to FuseWelcome
          2. Click Order Management tab
          3. Click Show More (if needed)
          4. Click Orders tile
          5. Click Manage Orders task

        Returns:
            True if successful, False otherwise
        """
        self.log("WORKFLOW: Navigate to Orders List")
        self.log("Step 1: Navigate to FuseWelcome")
        self.page.goto(f"{self.host}/fscmUI/faces/FuseWelcome",
                      wait_until="domcontentloaded", timeout=60000)
        self.page.wait_for_timeout(2000)
        take_screenshot(self.page, self.ss_dir, "om_01_fusewelcome")

        self.log("Step 2: Click Order Management tab")
        if not click_module_tab(self.page, "Order Management", wait_ms=2500):
            self.log("✗ Failed to click Order Management tab")
            take_screenshot(self.page, self.ss_dir, "om_error_tab")
            return False
        take_screenshot(self.page, self.ss_dir, "om_02_order_mgmt_tab")

        self.log("Step 3: Click Show More")
        clicks = click_show_more(self.page, max_clicks=3)
        self.log(f"  Clicked Show More {clicks} times")
        take_screenshot(self.page, self.ss_dir, "om_03_show_more")

        self.log("Step 4: Click Orders tile")
        if not click_tile(self.page, "Orders", wait_ms=5000):
            self.log("✗ Failed to click Orders tile")
            take_screenshot(self.page, self.ss_dir, "om_error_orders_tile")
            return False
        take_screenshot(self.page, self.ss_dir, "om_04_orders_tile")

        self.log("Step 5: Click Manage Orders task")
        if not click_task_link(self.page, "Manage Orders", wait_ms=3000):
            self.log("✗ Failed to click Manage Orders task")
            take_screenshot(self.page, self.ss_dir, "om_error_manage_orders")
            return False
        take_screenshot(self.page, self.ss_dir, "om_05_manage_orders")

        self.log("✓ Orders list loaded successfully")
        return True

    def search_order(self, order_number: str) -> Optional[Dict[str, Any]]:
        """
        Search for an order by order number in the list.

        Returns:
            Dict with order details (text, cx, cy, etc.), or None if not found
        """
        self.log(f"WORKFLOW: Search for order '{order_number}'")

        self.log("Step 1: Locate search input")
        # Search input typically at x~286, y~350-380
        input_coords = find_input_by_position(self.page, x_min=250, x_max=320,
                                             y_min=330, y_max=380)
        if not input_coords:
            self.log("✗ Search input not found")
            return None

        self.log(f"Step 2: Enter search term '{order_number}'")
        result = lov_search(self.page, order_number,
                           input_coords=(input_coords['cx'], input_coords['cy']),
                           wait_results_ms=2500, exact_match=True)

        if result:
            self.log(f"✓ Order found: {result['text']}")
            take_screenshot(self.page, self.ss_dir, f"om_search_{order_number}")
            return result
        else:
            self.log(f"✗ Order '{order_number}' not found")
            take_screenshot(self.page, self.ss_dir, f"om_search_failed_{order_number}")
            return None

    def open_order(self, order_number: str) -> bool:
        """
        Open a specific order from the list by clicking it.

        Returns:
            True if order opened, False otherwise
        """
        self.log(f"WORKFLOW: Open order '{order_number}'")

        self.log("Step 1: Search for order")
        order = self.search_order(order_number)
        if not order:
            return False

        self.log(f"Step 2: Click order row at ({order['cx']}, {order['cy']})")
        self.page.mouse.click(order['cx'], order['cy'])
        self.page.wait_for_timeout(4000)
        take_screenshot(self.page, self.ss_dir, f"om_order_detail_{order_number}")

        self.log("Step 3: Verify order detail page loaded")
        title = self.page.title()
        if "Order" in title or "PO" in title:
            self.log(f"✓ Order detail page loaded: {title}")
            return True
        else:
            self.log(f"✗ Order detail page may not have loaded: {title}")
            return False

    def update_order_header_field(self, field_name: str, new_value: str) -> bool:
        """
        Update a header field in order detail (e.g., Supplier, Currency).

        Expected flow:
          1. Locate field label
          2. Click edit icon or enable edit mode
          3. Update field value (text input, dropdown, or LOV)
          4. Save changes

        Returns:
            True if updated successfully
        """
        self.log(f"WORKFLOW: Update order header field '{field_name}' to '{new_value}'")

        self.log(f"Step 1: Locate '{field_name}' field")
        # Search for field label
        field_labels = find_all_elements_by_text(self.page, field_name,
                                               selector="label,span,td", partial=True)
        if not field_labels:
            self.log(f"✗ Field label '{field_name}' not found")
            return False

        label = field_labels[0]
        self.log(f"  Found label at ({label['cx']}, {label['cy']})")

        # Find input/select field near label (to the right, same y)
        input_coords = self.page.evaluate(f"""
            (labelY) => {{
                for (const el of document.querySelectorAll('input, select, textarea')) {{
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    if (Math.abs(r.y - labelY) > 30) continue;
                    if (r.x < 200) continue;  // Must be to the right
                    return {{
                        cx: Math.round(r.x + r.width / 2),
                        cy: Math.round(r.y + r.height / 2),
                        tag: el.tagName.toLowerCase(),
                        type: el.getAttribute('type') || 'text'
                    }};
                }}
                return null;
            }}
        """, [label['cy']])

        if not input_coords:
            self.log(f"✗ Input field not found near label")
            take_screenshot(self.page, self.ss_dir, f"om_field_error_{field_name}")
            return False

        self.log(f"Step 2: Update field (type: {input_coords['tag']})")

        if input_coords['tag'] == 'select':
            # Dropdown field
            self.page.mouse.click(input_coords['cx'], input_coords['cy'])
            self.page.wait_for_timeout(500)
            # Find and click option matching new_value
            options = find_all_elements_by_text(self.page, new_value,
                                               selector="option", partial=True)
            if options:
                opt = options[0]
                self.page.mouse.click(opt['cx'], opt['cy'])
                self.page.wait_for_timeout(1000)
                self.log(f"✓ Selected option '{new_value}'")
            else:
                self.log(f"✗ Option '{new_value}' not found in dropdown")
                return False

        elif input_coords['tag'] == 'input':
            # Text/LOV input
            self.page.mouse.click(input_coords['cx'], input_coords['cy'])
            self.page.wait_for_timeout(300)
            self.page.keyboard.press("Control+A")
            self.page.keyboard.type(new_value, delay=30)
            self.page.wait_for_timeout(1500)
            self.log(f"✓ Entered text '{new_value}'")

        self.log("Step 3: Save changes")
        # Look for Save button (typically x:700-800, y:100-150)
        save_btn = self.page.evaluate("""
            () => {
                for (const btn of document.querySelectorAll('button')) {
                    if (!btn.offsetParent) continue;
                    const text = btn.textContent.trim().toUpperCase();
                    if (text !== 'SAVE') continue;
                    const r = btn.getBoundingClientRect();
                    return {
                        cx: Math.round(r.x + r.width / 2),
                        cy: Math.round(r.y + r.height / 2)
                    };
                }
                return null;
            }
        """)

        if save_btn:
            self.page.mouse.click(save_btn['cx'], save_btn['cy'])
            self.page.wait_for_timeout(2500)
            self.log("✓ Clicked Save button")
            take_screenshot(self.page, self.ss_dir, f"om_field_saved_{field_name}")
            return True
        else:
            self.log("⚠ Save button not found — assuming in-place save")
            return True

    def submit_order(self) -> bool:
        """
        Submit an order (typically from detail view).

        Returns:
            True if submitted successfully
        """
        self.log("WORKFLOW: Submit order")

        self.log("Step 1: Look for Submit button")
        submit_btn = self.page.evaluate("""
            () => {
                for (const btn of document.querySelectorAll('button')) {
                    if (!btn.offsetParent) continue;
                    const text = btn.textContent.trim().toUpperCase();
                    if (!text.includes('SUBMIT')) continue;
                    const r = btn.getBoundingClientRect();
                    return {
                        cx: Math.round(r.x + r.width / 2),
                        cy: Math.round(r.y + r.height / 2)
                    };
                }
                return null;
            }
        """)

        if not submit_btn:
            self.log("✗ Submit button not found")
            take_screenshot(self.page, self.ss_dir, "om_submit_not_found")
            return False

        self.log("Step 2: Click Submit button")
        self.page.mouse.click(submit_btn['cx'], submit_btn['cy'])
        self.page.wait_for_timeout(2000)

        self.log("Step 3: Handle confirmation dialog")
        if handle_confirmation_dialog(self.page, action="confirm", wait_ms=2000):
            self.log("✓ Submitted order")
            take_screenshot(self.page, self.ss_dir, "om_submitted")
            return True
        else:
            self.log("⚠ No confirmation dialog, order may be submitted")
            return True


# ============================================================================
# WORK DEFINITION WORKFLOWS
# ============================================================================

class WorkDefinitionWorkflow:
    """Workflow for managing work orders and work definitions."""

    def __init__(self, page: Page, host: str, ss_dir: Path):
        self.page = page
        self.host = host
        self.ss_dir = ss_dir
        self.log_messages = []

    def log(self, message: str):
        """Log a message."""
        print(f"  {message}")
        self.log_messages.append(message)

    def navigate_to_work_orders(self) -> bool:
        """
        Navigate to Supply Chain Execution > Work Definition > Manage Work Orders.

        Expected sequence:
          1. Go to FuseWelcome
          2. Click Supply Chain Execution tab
          3. Click Show More (if needed)
          4. Click Work Definition tile
          5. Click Manage Work Orders task

        Returns:
            True if successful
        """
        self.log("WORKFLOW: Navigate to Work Orders List")
        self.log("Step 1: Navigate to FuseWelcome")
        self.page.goto(f"{self.host}/fscmUI/faces/FuseWelcome",
                      wait_until="domcontentloaded", timeout=60000)
        self.page.wait_for_timeout(2000)
        take_screenshot(self.page, self.ss_dir, "wd_01_fusewelcome")

        self.log("Step 2: Click Supply Chain Execution tab")
        if not click_module_tab(self.page, "Supply Chain Execution", wait_ms=2500):
            self.log("✗ Failed to click Supply Chain Execution tab")
            return False
        take_screenshot(self.page, self.ss_dir, "wd_02_sce_tab")

        self.log("Step 3: Click Show More")
        clicks = click_show_more(self.page, max_clicks=3)
        self.log(f"  Clicked Show More {clicks} times")
        take_screenshot(self.page, self.ss_dir, "wd_03_show_more")

        self.log("Step 4: Click Work Definition tile")
        if not click_tile(self.page, "Work Definition", wait_ms=5000):
            self.log("✗ Failed to click Work Definition tile")
            take_screenshot(self.page, self.ss_dir, "wd_error_tile")
            return False
        take_screenshot(self.page, self.ss_dir, "wd_04_work_definition_tile")

        self.log("Step 5: Click Manage Work Orders task")
        if not click_task_link(self.page, "Manage Work Orders", wait_ms=3000):
            self.log("✗ Failed to click Manage Work Orders task")
            take_screenshot(self.page, self.ss_dir, "wd_error_manage")
            return False
        take_screenshot(self.page, self.ss_dir, "wd_05_manage_work_orders")

        self.log("✓ Work Orders list loaded successfully")
        return True

    def filter_work_orders_by_status(self, status: str) -> bool:
        """
        Filter work orders by status (e.g., 'In Progress', 'Pending', 'Done').

        Returns:
            True if filter applied
        """
        self.log(f"WORKFLOW: Filter work orders by status '{status}'")

        self.log("Step 1: Locate status filter input")
        # Status filter is typically a column filter or separate filter
        filter_input = find_input_by_position(self.page, x_min=250, x_max=320,
                                             y_min=330, y_max=380)
        if not filter_input:
            self.log("✗ Filter input not found")
            return False

        self.log(f"Step 2: Apply filter '{status}'")
        if filter_table_column(self.page, status,
                              filter_coords=(filter_input['cx'], filter_input['cy']),
                              wait_filter_ms=2500):
            self.log(f"✓ Filter applied: {status}")
            take_screenshot(self.page, self.ss_dir, f"wd_filtered_{status}")
            return True
        else:
            self.log(f"✗ Failed to apply filter")
            return False

    def open_work_order(self, work_order_id: str) -> bool:
        """
        Open a work order from the list.

        Returns:
            True if opened successfully
        """
        self.log(f"WORKFLOW: Open work order '{work_order_id}'")

        self.log("Step 1: Search for work order")
        wo_elements = find_all_elements_by_text(self.page, work_order_id,
                                               selector="a,td,tr", partial=False)
        if not wo_elements:
            self.log(f"✗ Work order '{work_order_id}' not found")
            return False

        wo = wo_elements[0]
        self.log(f"Step 2: Click work order at ({wo['cx']}, {wo['cy']})")
        self.page.mouse.click(wo['cx'], wo['cy'])
        self.page.wait_for_timeout(4000)
        take_screenshot(self.page, self.ss_dir, f"wd_detail_{work_order_id}")

        self.log("Step 3: Verify detail page loaded")
        title = self.page.title()
        if "Work Order" in title or "Detail" in title:
            self.log(f"✓ Work order detail page loaded")
            return True
        else:
            self.log("⚠ Detail page may not have loaded")
            return False

    def add_routing_step(self, operation: str, sequence: int = 10) -> bool:
        """
        Add a routing step (operation) to a work order.

        Expected flow:
          1. Navigate to Routing section
          2. Click Add/[+] button
          3. Search for operation
          4. Set sequence number
          5. Save

        Returns:
            True if step added successfully
        """
        self.log(f"WORKFLOW: Add routing step '{operation}' at sequence {sequence}")

        self.log("Step 1: Locate Routing section and Add button")
        add_btn = self.page.evaluate("""
            () => {
                for (const btn of document.querySelectorAll('button')) {
                    if (!btn.offsetParent) continue;
                    const text = btn.textContent.trim();
                    if (!text.includes('+') && !text.includes('Add')) continue;
                    const r = btn.getBoundingClientRect();
                    if (r.y < 600 || r.y > 900) continue;  # Routing section is typically lower on page
                    return {
                        cx: Math.round(r.x + r.width / 2),
                        cy: Math.round(r.y + r.height / 2)
                    };
                }
                return null;
            }
        """)

        if not add_btn:
            self.log("✗ Add button not found in Routing section")
            take_screenshot(self.page, self.ss_dir, "wd_add_step_not_found")
            return False

        self.log("Step 2: Click Add button")
        self.page.mouse.click(add_btn['cx'], add_btn['cy'])
        self.page.wait_for_timeout(1500)
        take_screenshot(self.page, self.ss_dir, "wd_add_step_modal")

        self.log(f"Step 3: Search for operation '{operation}'")
        input_coords = find_input_by_position(self.page, x_min=250, x_max=320,
                                             y_min=330, y_max=450)
        if input_coords:
            result = lov_search(self.page, operation,
                               input_coords=(input_coords['cx'], input_coords['cy']),
                               exact_match=True)
            if result:
                self.log(f"✓ Operation selected: {result['text']}")
            else:
                self.log(f"✗ Operation '{operation}' not found")
                return False
        else:
            self.log("⚠ Could not find LOV input for operation")

        self.log("Step 4: Save routing step")
        save_btn = self.page.evaluate("""
            () => {
                for (const btn of document.querySelectorAll('button')) {
                    if (!btn.offsetParent) continue;
                    const text = btn.textContent.trim().toUpperCase();
                    if (text !== 'OK' && text !== 'SAVE') continue;
                    const r = btn.getBoundingClientRect();
                    return {
                        cx: Math.round(r.x + r.width / 2),
                        cy: Math.round(r.y + r.height / 2)
                    };
                }
                return null;
            }
        """)

        if save_btn:
            self.page.mouse.click(save_btn['cx'], save_btn['cy'])
            self.page.wait_for_timeout(2000)
            self.log("✓ Routing step added")
            take_screenshot(self.page, self.ss_dir, f"wd_step_added_{operation}")
            return True
        else:
            self.log("✗ Save button not found")
            return False

    def update_work_order_priority(self, new_priority: str) -> bool:
        """
        Update work order priority (High, Medium, Low, etc.).

        Returns:
            True if updated
        """
        self.log(f"WORKFLOW: Update work order priority to '{new_priority}'")

        self.log("Step 1: Locate Priority field")
        priority_labels = find_all_elements_by_text(self.page, "Priority",
                                                   selector="label,span", partial=True)
        if not priority_labels:
            self.log("✗ Priority label not found")
            return False

        label = priority_labels[0]
        self.log(f"  Priority field at y={label['cy']}")

        self.log("Step 2: Find and update Priority dropdown")
        priority_input = self.page.evaluate(f"""
            (labelY) => {{
                for (const el of document.querySelectorAll('select')) {{
                    if (!el.offsetParent) continue;
                    const r = el.getBoundingClientRect();
                    if (Math.abs(r.y - labelY) > 30) continue;
                    return {{
                        cx: Math.round(r.x + r.width / 2),
                        cy: Math.round(r.y + r.height / 2)
                    }};
                }}
                return null;
            }}
        """, [label['cy']])

        if priority_input:
            self.page.mouse.click(priority_input['cx'], priority_input['cy'])
            self.page.wait_for_timeout(500)
            options = find_all_elements_by_text(self.page, new_priority, partial=True)
            if options:
                self.page.mouse.click(options[0]['cx'], options[0]['cy'])
                self.page.wait_for_timeout(1000)
                self.log(f"✓ Priority updated to '{new_priority}'")
                take_screenshot(self.page, self.ss_dir, f"wd_priority_{new_priority}")
                return True

        self.log(f"✗ Could not update priority")
        return False
