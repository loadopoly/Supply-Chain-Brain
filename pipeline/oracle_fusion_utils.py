"""
oracle_fusion_utils.py - Reusable Playwright utilities for Oracle Fusion ADF automation

Provides:
  - LOV search pattern (List of Values)
  - Table operations (filter, add, delete, reorder)
  - Confirmation dialog handling
  - Navigation helpers
  - Coordinate detection
  - Screenshot utilities
  - State detection
"""

import json
import time
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
from playwright.sync_api import Page, BrowserContext


# ============================================================================
# COORDINATE HELPERS
# ============================================================================

def get_element_coords(page: Page, selector_or_js: str, is_js: bool = False) -> Optional[Dict[str, int]]:
    """
    Get center coordinates (cx, cy) of an element.

    Args:
        page: Playwright page object
        selector_or_js: CSS selector or JavaScript code
        is_js: If True, treat as JavaScript code; if False, treat as selector

    Returns:
        Dict with 'cx', 'cy' keys, or None if not found/visible
    """
    if is_js:
        result = page.evaluate(selector_or_js)
    else:
        result = page.evaluate(f"""
            (selector) => {{
                const el = document.querySelector(selector);
                if (!el || !el.offsetParent) return null;
                const r = el.getBoundingClientRect();
                if (r.width <= 0 || r.height <= 0) return null;
                return {{
                    cx: Math.round(r.x + r.width / 2),
                    cy: Math.round(r.y + r.height / 2),
                    x: Math.round(r.x),
                    y: Math.round(r.y),
                    width: Math.round(r.width),
                    height: Math.round(r.height)
                }};
            }}
        """, selector_or_js)
    return result


def find_all_elements_by_text(page: Page, text: str, selector: str = "*",
                              visible_only: bool = True, partial: bool = False) -> List[Dict[str, Any]]:
    """
    Find all elements containing given text and return their coordinates.

    Args:
        page: Playwright page object
        text: Text to search for
        selector: CSS selector to search within (default: all elements)
        visible_only: Only return visible elements
        partial: If True, match partial text; if False, exact match

    Returns:
        List of dicts with element info (text, cx, cy, tag, etc.)
    """
    js = f"""
        (searchText, sel, visibleOnly, partial) => {{
            const results = [];
            const matchType = partial ? 'includes' : 'exact';
            for (const el of document.querySelectorAll(sel)) {{
                if (visibleOnly && !el.offsetParent) continue;
                const r = el.getBoundingClientRect();
                if (r.width <= 0 || r.height <= 0) continue;
                const elText = el.textContent.trim().replace(/\\s+/g, ' ');
                let matches = false;
                if (matchType === 'exact') {{
                    matches = elText === searchText;
                }} else {{
                    matches = elText.includes(searchText);
                }}
                if (!matches) continue;
                results.push({{
                    text: elText.slice(0, 100),
                    tag: el.tagName,
                    cx: Math.round(r.x + r.width / 2),
                    cy: Math.round(r.y + r.height / 2),
                    x: Math.round(r.x),
                    y: Math.round(r.y),
                    width: Math.round(r.width),
                    height: Math.round(r.height)
                }});
            }}
            return results;
        }}
    """
    return page.evaluate(js, [text, selector, visible_only, partial])


def find_input_by_position(page: Page, x_min: int = 250, x_max: int = 350,
                           y_min: int = 400, y_max: int = 500) -> Optional[Dict[str, int]]:
    """
    Find an input field within a coordinate range (e.g., column filter).

    Returns:
        Dict with 'cx', 'cy' keys, or None
    """
    result = page.evaluate(f"""
        (xMin, xMax, yMin, yMax) => {{
            for (const el of document.querySelectorAll('input')) {{
                if (!el.offsetParent) continue;
                const r = el.getBoundingClientRect();
                if (r.x < xMin || r.x > xMax) continue;
                if (r.y < yMin || r.y > yMax) continue;
                return {{
                    cx: Math.round(r.x + r.width / 2),
                    cy: Math.round(r.y + r.height / 2)
                }};
            }}
            return null;
        }}
    """, [x_min, x_max, y_min, y_max])
    return result


# ============================================================================
# LOV (LIST OF VALUES) PATTERN
# ============================================================================

def lov_search(page: Page, search_term: str, input_coords: Optional[Tuple[int, int]] = None,
               wait_results_ms: int = 2000, exact_match: bool = False) -> Optional[Dict[str, Any]]:
    """
    Perform LOV search pattern: click input, type, wait for results, click match.

    Args:
        page: Playwright page object
        search_term: Search term to enter
        input_coords: (cx, cy) tuple. If None, searches for input at x~286, y~350
        wait_results_ms: Wait time for results to appear
        exact_match: Only click if text exactly matches

    Returns:
        Dict with selected result info, or None if no match found
    """
    if input_coords is None:
        coords = find_input_by_position(page, x_min=250, x_max=320, y_min=330, y_max=380)
        if not coords:
            return None
        input_coords = (coords['cx'], coords['cy'])

    cx, cy = input_coords
    page.mouse.click(cx, cy)
    page.wait_for_timeout(300)
    page.keyboard.press("Control+A")
    page.wait_for_timeout(100)
    page.keyboard.type(search_term, delay=30)
    page.wait_for_timeout(wait_results_ms)

    # Find suggestion
    suggestion = find_all_elements_by_text(page, search_term, selector="li,td,tr,div[role='option']",
                                          visible_only=True, partial=not exact_match)

    if not suggestion:
        return None

    # Click first match
    result = suggestion[0]
    page.mouse.click(result['cx'], result['cy'])
    page.wait_for_timeout(1000)

    return result


def lov_keyboard_select(page: Page, input_coords: Tuple[int, int], option_count: int,
                       enter_after: bool = True) -> bool:
    """
    Navigate LOV dropdown using keyboard (Home, ArrowDown, Enter).

    Args:
        page: Playwright page object
        input_coords: (cx, cy) of input field
        option_count: How many ArrowDown presses to reach target option
        enter_after: If True, press Enter to confirm selection

    Returns:
        True if successful
    """
    cx, cy = input_coords
    page.mouse.click(cx, cy)
    page.wait_for_timeout(300)
    page.keyboard.press("Home")
    page.wait_for_timeout(100)
    for _ in range(option_count):
        page.keyboard.press("ArrowDown")
        page.wait_for_timeout(80)
    if enter_after:
        page.keyboard.press("Enter")
        page.wait_for_timeout(1500)
    return True


# ============================================================================
# TABLE OPERATIONS
# ============================================================================

def filter_table_column(page: Page, search_term: str, filter_coords: Optional[Tuple[int, int]] = None,
                        wait_filter_ms: int = 2500) -> bool:
    """
    Apply column filter in table (e.g., Item filter in cycle counts).

    Args:
        page: Playwright page object
        search_term: Term to filter by
        filter_coords: (cx, cy) of filter input. If None, uses default x~286, y~440
        wait_filter_ms: Wait for filter results (AJAX refresh)

    Returns:
        True if filter applied
    """
    if filter_coords is None:
        coords = find_input_by_position(page, x_min=250, x_max=320, y_min=420, y_max=460)
        if not coords:
            return False
        filter_coords = (coords['cx'], coords['cy'])

    cx, cy = filter_coords
    page.mouse.click(cx, cy)
    page.wait_for_timeout(300)
    page.keyboard.press("Control+A")
    page.wait_for_timeout(100)
    page.keyboard.type(search_term, delay=30)
    page.wait_for_timeout(300)
    page.keyboard.press("Enter")
    page.wait_for_timeout(wait_filter_ms)

    return True


def clear_table_filter(page: Page, filter_coords: Optional[Tuple[int, int]] = None) -> bool:
    """
    Clear (delete all) text from a table column filter.

    Args:
        page: Playwright page object
        filter_coords: (cx, cy) of filter input

    Returns:
        True if cleared
    """
    if filter_coords is None:
        coords = find_input_by_position(page, x_min=250, x_max=320, y_min=420, y_max=460)
        if not coords:
            return False
        filter_coords = (coords['cx'], coords['cy'])

    cx, cy = filter_coords
    page.mouse.click(cx, cy)
    page.wait_for_timeout(300)
    page.keyboard.press("Control+A")
    page.wait_for_timeout(100)
    page.keyboard.press("Delete")
    page.wait_for_timeout(300)
    page.keyboard.press("Enter")
    page.wait_for_timeout(800)

    return True


def find_table_row_by_text(page: Page, text: str, selector: str = "tr,td,li",
                           partial: bool = False) -> Optional[Dict[str, Any]]:
    """
    Find a table row containing specific text.

    Returns:
        Dict with row info (text, cx, cy, etc.), or None
    """
    results = find_all_elements_by_text(page, text, selector=selector, partial=partial)
    return results[0] if results else None


def click_table_row_action(page: Page, row_coords: Tuple[int, int], action: str = "edit") -> Optional[Dict[str, int]]:
    """
    Click action button on a table row (edit, delete, etc.).

    Args:
        page: Playwright page object
        row_coords: (cx, cy) of row
        action: "edit", "delete", "add", etc.

    Returns:
        Coordinates of clicked action, or None
    """
    action_icons = {
        "delete": "trash",
        "edit": "pencil",
        "add": "+",
        "view": "eye",
    }

    icon_text = action_icons.get(action, action)

    # Search for icon in the row vicinity (to the right)
    rx, ry = row_coords
    icon_coords = find_input_by_position(page, x_min=1350, x_max=1450, y_min=ry-20, y_max=ry+20)

    if icon_coords:
        page.mouse.click(icon_coords['cx'], icon_coords['cy'])
        page.wait_for_timeout(1000)
        return icon_coords

    return None


# ============================================================================
# CONFIRMATION DIALOGS
# ============================================================================

def handle_confirmation_dialog(page: Page, action: str = "confirm", wait_ms: int = 2000) -> bool:
    """
    Handle OK/Cancel or Yes/No confirmation dialogs.

    Args:
        page: Playwright page object
        action: "confirm" (click OK/Yes), "cancel" (click Cancel/No)
        wait_ms: Wait time after clicking

    Returns:
        True if dialog clicked, False if no dialog found
    """
    buttons = page.evaluate("""
        () => {
            const results = [];
            for (const btn of document.querySelectorAll('button')) {
                if (!btn.offsetParent) continue;
                const text = btn.textContent.trim().toUpperCase();
                const r = btn.getBoundingClientRect();
                if (r.width < 40 || r.height < 20) continue;
                results.push({
                    text: text,
                    cx: Math.round(r.x + r.width / 2),
                    cy: Math.round(r.y + r.height / 2)
                });
            }
            return results;
        }
    """)

    target_texts = {
        "confirm": ["OK", "YES", "CONFIRM", "PROCEED"],
        "cancel": ["CANCEL", "NO", "CLOSE"],
    }

    for btn in buttons:
        if btn['text'] in target_texts.get(action, []):
            page.mouse.click(btn['cx'], btn['cy'])
            page.wait_for_timeout(wait_ms)
            return True

    return False


# ============================================================================
# NAVIGATION PATTERNS
# ============================================================================

def click_module_tab(page: Page, tab_name: str, wait_ms: int = 2500) -> bool:
    """
    Click a module tab (Supply Chain Execution, etc.) in navigation bar.

    Args:
        page: Playwright page object
        tab_name: Tab name to click
        wait_ms: Wait after click

    Returns:
        True if clicked, False if not found
    """
    coords = page.evaluate(f"""
        (tabName) => {{
            for (const el of document.querySelectorAll('a,div,span,button')) {{
                if (!el.offsetParent) continue;
                const text = el.textContent.trim().replace(/\\s+/g, ' ');
                if (text !== tabName) continue;
                const r = el.getBoundingClientRect();
                if (r.y < 40 || r.y > 120) continue;
                return {{
                    cx: Math.round(r.x + r.width / 2),
                    cy: Math.round(r.y + r.height / 2)
                }};
            }}
            return null;
        }}
    """, [tab_name])

    if coords:
        page.mouse.click(coords['cx'], coords['cy'])
        page.wait_for_timeout(wait_ms)
        return True

    return False


def click_show_more(page: Page, max_clicks: int = 3) -> int:
    """
    Click 'Show More' button repeatedly until gone.

    Returns:
        Number of times clicked
    """
    clicks = 0
    for _ in range(max_clicks):
        found = page.evaluate("""
            () => {
                for (const el of document.querySelectorAll('a,button,span')) {
                    if (!el.offsetParent) continue;
                    if (el.textContent.trim() !== 'Show More') continue;
                    const r = el.getBoundingClientRect();
                    return {
                        cx: Math.round(r.x + r.width / 2),
                        cy: Math.round(r.y + r.height / 2)
                    };
                }
                return null;
            }
        """)

        if not found:
            break

        page.mouse.click(found['cx'], found['cy'])
        page.wait_for_timeout(1500)
        clicks += 1

    return clicks


def click_tile(page: Page, tile_name: str, wait_ms: int = 5000) -> bool:
    """
    Click a module/feature tile (Inventory Management, Orders, etc.).

    Args:
        page: Playwright page object
        tile_name: Tile name (e.g., "Inventory Management (Classic)")
        wait_ms: Wait after click

    Returns:
        True if clicked, False if not found
    """
    coords = page.evaluate(f"""
        (tileName) => {{
            for (const el of document.querySelectorAll('a')) {{
                const text = el.textContent.trim().replace(/\\s+/g, ' ');
                if (text !== tileName) continue;
                if (!el.offsetParent) continue;
                const r = el.getBoundingClientRect();
                if (r.y < 150 || r.height < 10) continue;
                const style = window.getComputedStyle(el);
                if (style.visibility === 'hidden' || style.display === 'none') continue;
                let p = el.parentElement;
                for (let i = 0; i < 6 && p; i++) {{
                    const ps = window.getComputedStyle(p);
                    if (ps.visibility === 'hidden' || ps.display === 'none') return null;
                    p = p.parentElement;
                }}
                el.scrollIntoView({{block: 'center', behavior: 'instant'}});
                const r2 = el.getBoundingClientRect();
                return {{
                    cx: Math.round(r2.x + r2.width / 2),
                    cy: Math.round(r2.y + r2.height / 2)
                }};
            }}
            return null;
        }}
    """, [tile_name])

    if coords:
        page.mouse.click(coords['cx'], coords['cy'])
        page.wait_for_timeout(wait_ms)
        return True

    return False


def click_task_link(page: Page, task_name: str, wait_ms: int = 3000) -> bool:
    """
    Click a task link (Manage Cycle Counts, Manage Orders, etc.).

    Args:
        page: Playwright page object
        task_name: Task link text
        wait_ms: Wait after click

    Returns:
        True if clicked, False if not found
    """
    coords = page.evaluate(f"""
        (taskName) => {{
            for (const el of document.querySelectorAll('a,li,span,button')) {{
                if (!el.offsetParent) continue;
                const text = el.textContent.trim();
                if (text !== taskName) continue;
                const r = el.getBoundingClientRect();
                return {{
                    cx: Math.round(r.x + r.width / 2),
                    cy: Math.round(r.y + r.height / 2)
                }};
            }}
            return null;
        }}
    """, [task_name])

    if coords:
        page.mouse.click(coords['cx'], coords['cy'])
        page.wait_for_timeout(wait_ms)
        return True

    return False


# ============================================================================
# STATE DETECTION
# ============================================================================

def is_authenticated(page: Page) -> bool:
    """Check if still authenticated (page title should not contain 'Sign In')."""
    title = page.title()
    return "Sign In" not in title and "Login" not in title


def is_page_loading(page: Page) -> bool:
    """Check if page shows a loading spinner."""
    loading = page.evaluate("""
        () => {
            for (const el of document.querySelectorAll('[aria-busy="true"], .spinner, [class*="loading"]')) {
                if (el.offsetParent) return true;
            }
            return false;
        }
    """)
    return loading


def wait_for_page_ready(page: Page, timeout_ms: int = 10000, check_interval_ms: int = 500) -> bool:
    """
    Wait for page to be ready (not loading, content visible).

    Returns:
        True if ready, False if timeout
    """
    start = time.time()
    while (time.time() - start) * 1000 < timeout_ms:
        if not is_page_loading(page):
            page.wait_for_timeout(check_interval_ms)
            return True
        page.wait_for_timeout(check_interval_ms)
    return False


# ============================================================================
# SESSION MANAGEMENT
# ============================================================================

def load_session_cookies(page: BrowserContext, session_file: Path) -> bool:
    """
    Load cookies from oracle_session.json file.

    Args:
        page: BrowserContext object
        session_file: Path to session JSON file

    Returns:
        True if loaded successfully
    """
    try:
        with open(session_file) as f:
            cookies = json.load(f)

        pw_cookies = []
        for c in cookies:
            ck = {
                "name": c["name"],
                "value": c["value"],
                "domain": c.get("domain", "").lstrip("."),
                "path": c.get("path", "/"),
                "secure": c.get("secure", True),
                "httpOnly": c.get("httpOnly", False)
            }
            if c.get("expires") and c["expires"] > 0:
                ck["expires"] = int(c["expires"])
            pw_cookies.append(ck)

        page.add_cookies(pw_cookies)
        return True
    except Exception as e:
        print(f"ERROR loading session cookies: {e}")
        return False


# ============================================================================
# SCREENSHOT UTILITIES
# ============================================================================

def take_screenshot(page: Page, output_dir: Path, name: str) -> Optional[Path]:
    """
    Take and save a screenshot with timestamp.

    Returns:
        Path to saved screenshot, or None if failed
    """
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        ss_path = output_dir / f"{name}.png"
        page.screenshot(path=str(ss_path))
        print(f"  [screenshot] {name}.png")
        return ss_path
    except Exception as e:
        print(f"  [screenshot error] {name}: {e}")
        return None


def get_page_info(page: Page) -> Dict[str, Any]:
    """
    Get comprehensive page information (title, URL, viewport, etc.).

    Returns:
        Dict with page metadata
    """
    viewport = page.viewport_size
    return {
        "title": page.title(),
        "url": page.url(),
        "viewport_width": viewport['width'] if viewport else None,
        "viewport_height": viewport['height'] if viewport else None,
        "is_authenticated": is_authenticated(page),
        "is_loading": is_page_loading(page),
    }
