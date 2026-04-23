"""OpenRouter HTTP caller for the LLM ensemble.

Registers itself with llm_ensemble when OPENROUTER_API_KEY is present in the
environment. Maps the brain.yaml model IDs (which are canonical labels) to
their current OpenRouter :free slugs. Falls back gracefully when no key is
set — the ensemble will then use the compute-grid caller or offline echo.

Usage:
    Set OPENROUTER_API_KEY in the environment (or .env / secrets manager).
    The caller auto-registers on first import of dbi_rag.

OpenRouter API is OpenAI-compatible:
    POST https://openrouter.ai/api/v1/chat/completions
    Authorization: Bearer <key>
    Body: {"model": "<slug>", "messages": [...], "max_tokens": 350}
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

log = logging.getLogger(__name__)

# ── Model ID → OpenRouter :free slug ────────────────────────────────────────
# brain.yaml uses canonical internal IDs; OpenRouter uses vendor/name:free slugs.
# Update this table as new models are released / slugs change.
_OR_MODEL_MAP: dict[str, str] = {
    "gemma-4":              "google/gemma-3-27b-it:free",
    "glm-5.1":              "thudm/glm-4-9b:free",
    "qwen3.5-397b-a17b":    "qwen/qwen3-235b-a22b:free",
    "deepseek-v3.2":        "deepseek/deepseek-chat-v3-0324:free",
    "kimi-k2.5":            "moonshotai/kimi-vl-a3b-thinking:free",
    "minimax-m2.7":         "minimax/minimax-m1:free",
    "mimo-v2-flash":        "google/gemma-3-4b-it:free",
}
_OR_DEFAULT = "deepseek/deepseek-chat-v3-0324:free"
_OR_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"

_key: str | None = None


def _get_key() -> str | None:
    global _key
    if _key is None:
        _key = (
            os.environ.get("OPENROUTER_API_KEY")
            or os.environ.get("OPENROUTER_KEY")
            or ""
        )
    return _key or None


def openrouter_caller(decision: Any, payload: Any, _cfg: dict) -> Any:
    """Ensemble model caller: POST to OpenRouter using the model slug mapping.

    Accepted payload shapes (from dispatch_parallel):
      - dict with "messages" key  → OpenAI chat format, forwarded verbatim
      - plain str                 → wrapped into a single user message
      - any other dict            → str-coerced into a user message
    """
    import requests  # already a project dep (used by cross_app.py)

    key = _get_key()
    if not key:
        return {"text": f"[{decision.model_id} offline — no OPENROUTER_API_KEY]",
                "confidence": 0.0}

    or_model = _OR_MODEL_MAP.get(decision.model_id, _OR_DEFAULT)

    if isinstance(payload, dict) and "messages" in payload:
        messages = payload["messages"]
    elif isinstance(payload, str):
        messages = [{"role": "user", "content": payload}]
    else:
        messages = [{"role": "user", "content": str(payload)}]

    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://supply-chain-brain.local",
        "X-Title": "Supply Chain Brain DBI",
    }
    body = {
        "model": or_model,
        "messages": messages,
        "max_tokens": 350,
        "temperature": 0.35,
    }

    try:
        r = requests.post(_OR_BASE_URL, headers=headers, json=body, timeout=40)
        r.raise_for_status()
        data = r.json()
        text = data["choices"][0]["message"]["content"].strip()
        log.debug("OpenRouter OK model=%s chars=%d", or_model, len(text))
        return {"text": text, "confidence": 0.9, "model": or_model}
    except Exception as exc:
        log.warning("OpenRouter call failed (brain_id=%s or_model=%s): %s",
                    decision.model_id, or_model, exc)
        raise


def register() -> bool:
    """Register the OpenRouter caller with the ensemble if a key is available.

    Returns True when registration succeeded (key present).
    Idempotent — safe to call multiple times.
    """
    if _get_key():
        from . import llm_ensemble
        llm_ensemble.set_caller(openrouter_caller)
        log.info("OpenRouter caller registered for LLM ensemble.")
        return True
    log.debug(
        "OPENROUTER_API_KEY not set — ensemble will use compute-grid / offline caller."
    )
    return False
