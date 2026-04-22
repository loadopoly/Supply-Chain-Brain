"""Cross-application event bus (signed webhooks).

Phase 4 — link this Supply Chain Brain to other apps (IPS Freight is the first
external subscriber). Outbound: emit HMAC-SHA256 signed JSON payloads to
registered subscribers when a finding is recorded. Inbound: validate the same
signature on incoming requests.

Intentionally framework-agnostic. The Streamlit page calls ``emit()``; a small
FastAPI wrapper (Phase 4 deploy) consumes ``verify()`` to authenticate inbound
calls.
"""
from __future__ import annotations
import hmac, hashlib, json, os, time
from typing import Iterable, Mapping
import requests

try:
    from . import load_config
except Exception:                                                # pragma: no cover
    load_config = lambda: {}                                     # type: ignore


def _subs() -> list[dict]:
    cfg = (load_config() or {}).get("cross_app", {})
    return list(cfg.get("subscribers", []))


def _sign(secret: str, payload: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def emit(event: str, body: Mapping, *, source: str = "scbrain") -> list[dict]:
    """Send to all configured subscribers; returns per-subscriber result rows."""
    out: list[dict] = []
    payload = json.dumps({"event": event, "source": source, "ts": int(time.time()),
                          "body": dict(body)}, sort_keys=True).encode("utf-8")
    for sub in _subs():
        url = sub.get("url"); secret_env = sub.get("secret_env", "SCBRAIN_WEBHOOK_SECRET")
        secret = os.environ.get(secret_env, "")
        if not url:
            out.append({"sub": sub.get("name"), "ok": False, "err": "no url"}); continue
        try:
            sig = _sign(secret, payload) if secret else ""
            r = requests.post(url, data=payload,
                              headers={"Content-Type": "application/json",
                                       "X-SCBrain-Signature": sig,
                                       "X-SCBrain-Event": event},
                              timeout=10)
            out.append({"sub": sub.get("name"), "ok": r.ok, "status": r.status_code})
        except Exception as e:
            out.append({"sub": sub.get("name"), "ok": False, "err": str(e)})
    return out


def verify(secret: str, body: bytes, signature: str) -> bool:
    return hmac.compare_digest(_sign(secret, body), signature or "")
