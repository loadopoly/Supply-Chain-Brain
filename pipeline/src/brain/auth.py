"""Lightweight session auth (Phase 4).

Reads `auth.users` from `config/brain.yaml`::

    auth:
      enabled: true
      users:
        agard: { display: "A. Gardner", role: admin }
        analyst: { display: "Analyst", role: viewer }

When ``enabled`` is false (default) every page is anonymous and ``user_id`` is
``"anonymous"``. When enabled, the sidebar shows a one-shot identity picker
(no passwords yet — corporate SSO arrives in the FastAPI deploy).
"""
from __future__ import annotations
import streamlit as st
from . import load_config


def current_user() -> dict:
    cfg = (load_config() or {}).get("auth", {})
    if not cfg.get("enabled", False):
        return {"id": "anonymous", "display": "anonymous", "role": "viewer"}

    if "user_id" not in st.session_state:
        users: dict = cfg.get("users", {})
        choice = st.sidebar.selectbox("👤 Sign in as", list(users.keys()) or ["anonymous"])
        st.session_state["user_id"] = choice
    uid = st.session_state["user_id"]
    user = (cfg.get("users", {}) or {}).get(uid, {})
    return {"id": uid, "display": user.get("display", uid), "role": user.get("role", "viewer")}


def require_role(role: str) -> bool:
    u = current_user()
    if u["role"] != role and u["role"] != "admin":
        st.error(f"Requires role '{role}'. Current: '{u['role']}'.")
        st.stop()
    return True
