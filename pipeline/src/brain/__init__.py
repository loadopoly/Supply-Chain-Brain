"""
brain — Supply Chain Brain analytics core.

This package is intentionally Streamlit-free so it can be imported by the
pipeline ingest, batch jobs, or notebooks. UI lives in pipeline/pages/.
"""
from pathlib import Path
import yaml

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "brain.yaml"


def load_config() -> dict:
    """Load config/brain.yaml. Returns empty dict if file is missing."""
    if not CONFIG_PATH.exists():
        return {}
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def select_llm(task: str, **kwargs):
    """Lazy proxy to brain.llm_router.select_llm to avoid import cycles."""
    from .llm_router import select_llm as _impl
    return _impl(task, **kwargs)


def refresh_llm_registry(force: bool = False):
    """Lazy proxy to brain.llm_scout.refresh_llm_registry."""
    from .llm_scout import refresh_llm_registry as _impl
    return _impl(force=force)


__all__ = [
    "load_config",
    "CONFIG_PATH",
    "select_llm",
    "refresh_llm_registry",
]
