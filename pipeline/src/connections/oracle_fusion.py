"""
Oracle Fusion Cloud connector via BI Publisher (BIP) REST API.

Authentication modes:
  sso   — Playwright browser automation. Opens a Chromium window; user completes
           Microsoft Entra SSO once. Captured cookies are cached to
           oracle_session.json and reused. Re-authenticates when cookies expire.
  basic — HTTP Basic Auth with a service-account username/password.
           Set env vars: ORACLE_FUSION_USER, ORACLE_FUSION_PASS
  oauth — IDCS OAuth 2.0 client credentials.
           Set env vars: ORACLE_IDCS_CLIENT_ID, ORACLE_IDCS_CLIENT_SECRET,
                         ORACLE_IDCS_TOKEN_URL

The FSCM REST API endpoint is used for data queries and schema discovery.
BIP REST is used for executing arbitrary SQL against Oracle Fusion's reporting model.
"""

import os
import json
import yaml
import requests
import pandas as pd
from pathlib import Path
from requests import Response


def _load_config() -> dict:
    cfg_path = Path(__file__).parent.parent.parent / "config" / "connections.yaml"
    with open(cfg_path) as f:
        return yaml.safe_load(f)["oracle_fusion"]


class OracleFusionSession:
    """
    Manages an authenticated HTTP session to Oracle Fusion Cloud BIP REST API.
    """

    def __init__(self):
        self.cfg = _load_config()
        self.host = self.cfg["host"].rstrip("/")
        self.bip_base = self.host + self.cfg["bip_endpoint"]
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        self._authenticated = False
        self._session_file = Path(__file__).parent.parent.parent / "oracle_session.json"

    def _response_preview(self, resp: Response, limit: int = 180) -> str:
        body = (resp.text or "").strip().replace("\r", " ").replace("\n", " ")
        return body[:limit] + ("..." if len(body) > limit else "")

    def _is_probably_json(self, resp: Response) -> bool:
        ctype = (resp.headers.get("Content-Type") or "").lower()
        if "json" in ctype:
            return True
        body = (resp.text or "").lstrip()
        return body.startswith("{") or body.startswith("[") or body.startswith('"')

    def _json_or_error(self, resp: Response, context: str) -> dict | list:
        if not self._is_probably_json(resp):
            preview = self._response_preview(resp)
            raise RuntimeError(
                f"{context} returned non-JSON content; Oracle session is likely expired or redirected to login. "
                f"HTTP {resp.status_code}. Response preview: {preview}"
            )
        try:
            return resp.json()
        except ValueError as exc:
            preview = self._response_preview(resp)
            raise RuntimeError(
                f"{context} returned invalid JSON; Oracle session may be stale. "
                f"HTTP {resp.status_code}. Response preview: {preview}"
            ) from exc

    def _clear_cached_session(self) -> None:
        self.session.cookies.clear()
        if self._session_file.exists():
            self._session_file.unlink()

    def _cached_session_valid(self) -> bool:
        resp = self.session.get(
            self.host + self.cfg["anticsrf_endpoint"],
            timeout=20,
            allow_redirects=False,
        )
        if resp.status_code != 200:
            return False
        if not self._is_probably_json(resp):
            return False
        try:
            payload = resp.json()
        except ValueError:
            return False
        token = payload.get("xsrftoken", "") if isinstance(payload, dict) else ""
        if token:
            self.session.headers["X-CSRF-Token"] = token
        return bool(token)

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def connect(self):
        mode = self.cfg.get("auth_mode", "sso")
        if mode == "basic":
            self._auth_basic()
        elif mode == "oauth":
            self._auth_oauth()
        elif mode == "sso":
            self._auth_sso()
        else:
            raise ValueError(f"Unknown auth_mode: {mode}")
        self._authenticated = True
        print(f"[Oracle Fusion] Authenticated via {mode} to {self.host}")

    def _auth_basic(self):
        user = os.environ.get("ORACLE_FUSION_USER")
        pwd = os.environ.get("ORACLE_FUSION_PASS")
        if not user or not pwd:
            raise EnvironmentError(
                "Set ORACLE_FUSION_USER and ORACLE_FUSION_PASS environment variables for basic auth."
            )
        self.session.auth = (user, pwd)
        # Verify credentials
        resp = self.session.get(
            self.host + "/xmlpserver/rest/v1/",
            timeout=self.cfg["request_timeout"],
        )
        resp.raise_for_status()

    def _auth_oauth(self):
        client_id = os.environ.get("ORACLE_IDCS_CLIENT_ID")
        client_secret = os.environ.get("ORACLE_IDCS_CLIENT_SECRET")
        token_url = os.environ.get("ORACLE_IDCS_TOKEN_URL")
        if not all([client_id, client_secret, token_url]):
            raise EnvironmentError(
                "Set ORACLE_IDCS_CLIENT_ID, ORACLE_IDCS_CLIENT_SECRET, and ORACLE_IDCS_TOKEN_URL for OAuth."
            )
        resp = requests.post(
            token_url,
            data={"grant_type": "client_credentials", "scope": "urn:opc:resource:consumer::all"},
            auth=(client_id, client_secret),
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
        self.session.headers["Authorization"] = f"Bearer {token}"

    def _auth_sso(self):
        """
        Playwright-based SSO: opens a visible Chromium browser window.
        User completes Microsoft Entra MFA once; all cookies are captured
        and stored in oracle_session.json for reuse across pipeline runs.
        """
        from playwright.sync_api import sync_playwright

        login_url = self.host + "/fscmUI/faces/FuseWelcome"

        # Try to load a cached session first
        if self._session_file.exists():
            print("[Oracle Fusion] Loading cached session ...")
            with open(self._session_file) as f:
                cookies = json.load(f)
            for c in cookies:
                self.session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))
            if self._cached_session_valid():
                print("[Oracle Fusion] Cached session is valid.")
                return
            print("[Oracle Fusion] Cached session expired — re-authenticating ...")
            self._clear_cached_session()

        print("[Oracle Fusion] Opening browser for Oracle Fusion SSO login ...")
        print("[Oracle Fusion] Sign in with your Astec Industries account (MFA will appear).")
        print("[Oracle Fusion] The browser will close automatically after successful login.\n")

        use_vault = os.environ.get("ASTEC_DISABLE_VAULT") != "1"
        try:
            from . import secrets as _secrets
            stored = _secrets.get_credentials("oracle_fusion") if use_vault else None
        except ImportError:
            stored = None

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False, slow_mo=200)
            context = browser.new_context()
            page = context.new_page()
            page.goto(login_url)

            if stored and stored.get("user") and stored.get("password"):
                try:
                    # Auto-fill Entra ID if we land on Microsoft login
                    page.wait_for_selector("input[type='email']", timeout=5000)
                    page.fill("input[type='email']", stored["user"])
                    page.click("input[type='submit']")
                    
                    page.wait_for_selector("input[type='password']", timeout=5000)
                    page.fill("input[type='password']", stored["password"])
                    page.click("input[type='submit']")
                    print("[Oracle Fusion] Auto-filled Entra ID credentials from vault.")
                except Exception as e:
                    print(f"[Oracle Fusion] Auto-fill incomplete (might have been cached or changed): {e}")

            # Wait until the Oracle Fusion home page loads (SSO complete)
            try:
                page.wait_for_url(
                    lambda url: "FuseWelcome" in url or "homePage" in url or "faces/Home" in url,
                    timeout=180_000,
                )
            except Exception:
                # Also accept if we land on any authenticated Oracle page
                page.wait_for_load_state("networkidle", timeout=60_000)

            print("[Oracle Fusion] Login detected — capturing session cookies ...")
            cookies = context.cookies()
            browser.close()

        # Store cookies for requests.Session
        for c in cookies:
            self.session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

        # Persist to disk
        with open(self._session_file, "w") as f:
            json.dump(cookies, f, indent=2)
        print(f"[Oracle Fusion] Session cached to {self._session_file}")

        # Add CSRF token header if available
        csrf_resp = self.session.get(
            self.host + self.cfg["anticsrf_endpoint"], timeout=20
        )
        if csrf_resp.ok:
            try:
                csrf_token = self._json_or_error(csrf_resp, "Oracle anti-CSRF endpoint").get("xsrftoken", "")
            except Exception:
                csrf_token = csrf_resp.text.strip().strip('"')
            if csrf_token:
                self.session.headers["X-CSRF-Token"] = csrf_token

    # ------------------------------------------------------------------
    # BIP SQL execution
    # ------------------------------------------------------------------

    def execute_sql(self, sql: str, max_rows: int = 10_000) -> pd.DataFrame:
        """
        Execute SQL via BIP REST API and return results as a DataFrame.
        SQL must be valid against Oracle Fusion's BIP/OTBI reporting data model.
        """
        if not self._authenticated:
            raise RuntimeError("Call connect() before execute_sql()")

        url = self.bip_base + "/queryResults"
        payload = {
            "sql": sql,
            "outputFormat": "json",
            "maxRows": max_rows,
        }
        resp = self.session.post(url, json=payload, timeout=self.cfg["request_timeout"])
        resp.raise_for_status()
        data = self._json_or_error(resp, "Oracle BIP queryResults")

        rows = data.get("rows", data.get("data", []))
        columns = data.get("columns", data.get("fields", []))

        if rows and isinstance(rows[0], list):
            return pd.DataFrame(rows, columns=columns)
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Schema discovery via BIP REST API
    # ------------------------------------------------------------------

    def list_subject_areas(self) -> list[str]:
        """List OTBI subject areas (logical schemas in Oracle Fusion BIP)."""
        url = self.bip_base + "/subjectAreas"
        resp = self.session.get(url, timeout=self.cfg["request_timeout"])
        resp.raise_for_status()
        payload = self._json_or_error(resp, "Oracle BIP subjectAreas")
        return payload.get("items", payload) if isinstance(payload, dict) else payload

    def list_tables(self, subject_area: str = None) -> list[dict]:
        """List available tables/views in the BIP catalog."""
        url = self.bip_base + "/catalog"
        params = {}
        if subject_area:
            params["subjectArea"] = subject_area
        resp = self.session.get(url, params=params, timeout=self.cfg["request_timeout"])
        resp.raise_for_status()
        payload = self._json_or_error(resp, "Oracle BIP catalog")
        return payload.get("items", payload) if isinstance(payload, dict) else payload

    def list_columns(self, table: str) -> list[dict]:
        """Describe columns of a BIP table/view."""
        url = self.bip_base + f"/catalog/{table}/columns"
        resp = self.session.get(url, timeout=self.cfg["request_timeout"])
        resp.raise_for_status()
        payload = self._json_or_error(resp, "Oracle BIP catalog columns")
        return payload.get("items", payload) if isinstance(payload, dict) else payload

    # ------------------------------------------------------------------
    # Convenience: run SQL via REST ERP Integration (alternative endpoint)
    # ------------------------------------------------------------------

    def execute_erp_sql(self, sql: str, max_rows: int = 10_000) -> pd.DataFrame:
        """
        Alternative: execute SQL via Oracle ERP Integration REST endpoint.
        Useful when BIP /queryResults is not available.
        """
        url = self.host + "/fscmRestApi/resources/11.13.18.05/erpintegrations"
        # This endpoint typically requires a service/report path; use BIP as primary.
        raise NotImplementedError(
            "Use execute_sql() with BIP endpoint. "
            "Configure auth_mode=basic or auth_mode=oauth for automated pipelines."
        )
