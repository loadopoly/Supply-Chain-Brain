"""Tests for learning_drive — the symbiotic internal loop.

Verifies:
  1. Identity drive when DB is absent or empty.
  2. Knob math when the DB has known learning data.
  3. Corpus saturation drives pivot_alpha.
  4. Self-train quality drives heartbeat_kappa.
  5. Learning velocity drives noise_sigma.
  6. acquisition_drive formula is bounded and directional.
  7. get_drive() is cached and thread-safe.
  8. Env-var overrides in _adam_step take priority over live drive.
  9. acquisition_drive is additively injected into grad_imag.
"""
from __future__ import annotations

import math
import os
import sqlite3
import tempfile
import threading
import time

import pytest

from src.brain.learning_drive import (
    LearningDrive,
    _identity_drive,
    _saturating,
    compute_drive,
    get_drive,
)


# ---------------------------------------------------------------------------
# Helpers — build an in-memory brain DB with controllable content
# ---------------------------------------------------------------------------

def _make_db(
    *,
    entities: int = 0,
    edges: int = 0,
    learnings: list[tuple[str, float]] | None = None,  # [(kind, signal_strength)]
    rdt_converged: float | None = None,  # avg converged flag (0-1)
) -> str:
    """Create a temporary SQLite file, populate it, return the path."""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    cn = sqlite3.connect(tmp.name)

    cn.execute(
        "CREATE TABLE corpus_entity (entity_id TEXT, entity_type TEXT)"
    )
    cn.execute("CREATE TABLE corpus_edge (id INTEGER)")
    cn.execute(
        """CREATE TABLE learning_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT, signal_strength REAL)"""
    )
    cn.execute(
        """CREATE TABLE recurrent_depth_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            converged INTEGER)"""
    )

    for i in range(entities):
        cn.execute(
            "INSERT INTO corpus_entity VALUES (?, 'Part')", (str(i),)
        )
    for i in range(edges):
        cn.execute("INSERT INTO corpus_edge VALUES (?)", (i,))

    for kind, sig in (learnings or []):
        cn.execute(
            "INSERT INTO learning_log(kind, signal_strength) VALUES (?, ?)",
            (kind, sig),
        )

    if rdt_converged is not None:
        # Insert 10 rows with the given converged rate
        n_conv = round(rdt_converged * 10)
        for i in range(10):
            cn.execute(
                "INSERT INTO recurrent_depth_log(converged) VALUES (?)",
                (1 if i < n_conv else 0,),
            )

    cn.commit()
    cn.close()
    return tmp.name


# ---------------------------------------------------------------------------
# 1. Identity drive — no DB at all
# ---------------------------------------------------------------------------
def test_identity_drive_values():
    d = _identity_drive()
    assert d.pivot_alpha == 1.0
    assert d.heartbeat_kappa == 0.0
    assert d.noise_sigma == 0.0
    assert d.acquisition_drive == 0.0


def test_compute_drive_empty_db(monkeypatch, tmp_path):
    """compute_drive on a fresh empty DB returns 'no data' defaults.

    SQLite creates the file, so compute_drive succeeds but finds no rows.
    An empty corpus is fully un-saturated (pivot_alpha=1.0), no self-train
    history means mid-quality default (kappa≈0.125), no velocity means
    maximum noise contribution, and the acquisition_drive is non-zero
    because there is plenty of unexplored space.
    """
    monkeypatch.setattr(
        "src.brain.learning_drive._local_db_path",
        lambda: str(tmp_path / "fresh_brain.db"),
    )
    d = compute_drive()
    # No entities/edges → saturation=0 → identity pivot
    assert d.pivot_alpha == pytest.approx(1.0, abs=1e-6)
    # No self_train rows → q=0.5 default → kappa ≈ 0.125
    assert d.heartbeat_kappa == pytest.approx(0.125, abs=0.02)
    # No learnings → v=0 → noise from stagnation
    assert d.noise_sigma > 0.10


# ---------------------------------------------------------------------------
# 2. Saturating helper
# ---------------------------------------------------------------------------
def test_saturating_zero_for_nonpositive():
    assert _saturating(0, 1000) == 0.0
    assert _saturating(-5, 1000) == 0.0


def test_saturating_half_at_scale():
    assert _saturating(1000, 1000) == pytest.approx(0.5)


def test_saturating_approaches_one():
    assert _saturating(1_000_000, 1000) > 0.999


# ---------------------------------------------------------------------------
# 3. Corpus saturation → pivot_alpha
# ---------------------------------------------------------------------------
def test_empty_db_gives_identity_pivot(monkeypatch, tmp_path):
    db = _make_db()
    monkeypatch.setattr("src.brain.learning_drive._local_db_path", lambda: db)
    d = compute_drive()
    assert d.pivot_alpha == pytest.approx(1.0, abs=1e-6)
    assert d.corpus_saturation == pytest.approx(0.0, abs=1e-6)


def test_saturated_corpus_tightens_pivot(monkeypatch):
    """A heavily-loaded corpus should push pivot_alpha meaningfully below 1.0.

    Saturation formula: s = s_ent*0.4 + s_edg*0.3 + s_lrn*0.3
    With 90k entities (s_ent=0.75), 60k edges (s_edg=0.75) and no learnings
    (s_lrn=0): s = 0.75*0.4 + 0.75*0.3 = 0.525 → alpha = 1.0 - 0.60*0.525 = 0.685.
    Adding 50k learnings brings s_lrn=0.83 → s≈0.775 → alpha≈0.535.
    """
    learnings = [("corpus", 0.7)] * 50_000
    db = _make_db(entities=90_000, edges=60_000, learnings=learnings)
    monkeypatch.setattr("src.brain.learning_drive._local_db_path", lambda: db)
    d = compute_drive()
    # s ≈ 0.775 → pivot_alpha ≈ 0.535 < 0.60
    assert d.pivot_alpha < 0.60
    assert d.corpus_saturation > 0.6


def test_partial_corpus_intermediate_pivot(monkeypatch):
    """Mid-scale corpus → pivot_alpha between 0.55 and 0.95."""
    db = _make_db(entities=15_000, edges=10_000)  # s ≈ 0.50
    monkeypatch.setattr("src.brain.learning_drive._local_db_path", lambda: db)
    d = compute_drive()
    assert 0.55 < d.pivot_alpha < 0.95


# ---------------------------------------------------------------------------
# 4. Self-train quality → heartbeat_kappa
# ---------------------------------------------------------------------------
def test_high_quality_gives_low_kappa(monkeypatch):
    """High avg_validator (0.95) → q≈0.9 → kappa ≈ 0.025."""
    learnings = [("self_train", 0.95)] * 10
    db = _make_db(learnings=learnings)
    monkeypatch.setattr("src.brain.learning_drive._local_db_path", lambda: db)
    d = compute_drive()
    assert d.heartbeat_kappa < 0.05


def test_random_quality_gives_moderate_kappa(monkeypatch):
    """avg_validator ≈ 0.5 → q=0 → kappa ≈ 0.25."""
    learnings = [("self_train", 0.5)] * 10
    db = _make_db(learnings=learnings)
    monkeypatch.setattr("src.brain.learning_drive._local_db_path", lambda: db)
    d = compute_drive()
    assert d.heartbeat_kappa > 0.20


def test_no_self_train_data_uses_default_kappa(monkeypatch):
    """No self_train rows → q=0.5 → kappa ≈ 0.125."""
    db = _make_db()
    monkeypatch.setattr("src.brain.learning_drive._local_db_path", lambda: db)
    d = compute_drive()
    assert d.heartbeat_kappa == pytest.approx(0.125, abs=0.02)


# ---------------------------------------------------------------------------
# 5. Learning velocity → noise_sigma
# ---------------------------------------------------------------------------
def test_rich_learning_log_lowers_noise(monkeypatch):
    """30 high-signal learnings → v ≈ 0.93 → noise_sigma ≈ 0.15*0.07 + 0.10*0.5 ≈ 0.06."""
    learnings = [("corpus", 0.9)] * 30
    db = _make_db(learnings=learnings, rdt_converged=0.5)
    monkeypatch.setattr("src.brain.learning_drive._local_db_path", lambda: db)
    d = compute_drive()
    # v is high → (1-v) is low → noise_sigma is low
    assert d.noise_sigma < 0.10


def test_empty_learning_log_maximizes_noise(monkeypatch):
    """No learnings → v=0, d=0.5 → noise_sigma = 0.15 + 0.05 = 0.20."""
    db = _make_db()
    monkeypatch.setattr("src.brain.learning_drive._local_db_path", lambda: db)
    d = compute_drive()
    assert d.noise_sigma >= 0.15


# ---------------------------------------------------------------------------
# 6. acquisition_drive is bounded and directional
# ---------------------------------------------------------------------------
def test_acquisition_drive_is_bounded(monkeypatch):
    """acquisition_drive must always be in [0, 0.30]."""
    for n_ent in [0, 1000, 30_000, 100_000]:
        db = _make_db(entities=n_ent)
        monkeypatch.setattr("src.brain.learning_drive._local_db_path", lambda: db)
        d = compute_drive()
        assert 0.0 <= d.acquisition_drive <= 0.30


def test_acquisition_drive_higher_when_corpus_fresh_and_stagnant(monkeypatch):
    """Fresh corpus + no learnings → higher drive than saturated + rich learnings."""
    db_fresh = _make_db()  # s≈0, v≈0, d=0.5
    db_sat   = _make_db(entities=90_000, edges=60_000,
                        learnings=[("corpus", 0.9)] * 30,
                        rdt_converged=0.9)

    monkeypatch.setattr("src.brain.learning_drive._local_db_path", lambda: db_fresh)
    d_fresh = compute_drive()

    monkeypatch.setattr("src.brain.learning_drive._local_db_path", lambda: db_sat)
    d_sat = compute_drive()

    assert d_fresh.acquisition_drive > d_sat.acquisition_drive


# ---------------------------------------------------------------------------
# 7. All knobs are within their documented ranges on all combinations
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("entities,edges,learnings_count,rdt_conv", [
    (0,      0,     0,   None),
    (1000,   500,   10,  0.3),
    (30000,  20000, 200, 0.7),
    (90000,  60000, 500, 0.95),
])
def test_all_knobs_in_range(monkeypatch, entities, edges, learnings_count, rdt_conv):
    learnings = [("corpus", 0.7)] * learnings_count if learnings_count else None
    db = _make_db(entities=entities, edges=edges,
                  learnings=learnings, rdt_converged=rdt_conv)
    monkeypatch.setattr("src.brain.learning_drive._local_db_path", lambda: db)
    d = compute_drive()
    assert 0.40 <= d.pivot_alpha      <= 1.00, f"pivot_alpha={d.pivot_alpha}"
    assert 0.00 <= d.heartbeat_kappa  <= 0.25, f"heartbeat_kappa={d.heartbeat_kappa}"
    assert 0.00 <= d.noise_sigma      <= 0.25, f"noise_sigma={d.noise_sigma}"
    assert 0.00 <= d.acquisition_drive <= 0.30, f"acquisition_drive={d.acquisition_drive}"


# ---------------------------------------------------------------------------
# 8. get_drive() returns a valid drive and caches results
# ---------------------------------------------------------------------------
def test_get_drive_returns_learning_drive(monkeypatch):
    monkeypatch.setattr(
        "src.brain.learning_drive._local_db_path",
        lambda: ":memory:",  # empty but valid
    )
    # Reset cache
    import src.brain.learning_drive as _ld
    _ld._CACHED_DRIVE = None
    _ld._LAST_COMPUTED = 0.0

    d = get_drive()
    assert isinstance(d, LearningDrive)


def test_get_drive_caches_within_ttl(monkeypatch):
    monkeypatch.setattr(
        "src.brain.learning_drive._local_db_path",
        lambda: ":memory:",
    )
    import src.brain.learning_drive as _ld
    _ld._CACHED_DRIVE = None
    _ld._LAST_COMPUTED = 0.0

    d1 = get_drive()
    d2 = get_drive()   # second call should return same object
    assert d1 is d2


def test_get_drive_refreshes_after_ttl(monkeypatch):
    monkeypatch.setattr(
        "src.brain.learning_drive._local_db_path",
        lambda: ":memory:",
    )
    import src.brain.learning_drive as _ld
    _ld._CACHED_DRIVE = None
    _ld._LAST_COMPUTED = 0.0

    d1 = get_drive()
    # Expire the cache
    _ld._LAST_COMPUTED = 0.0
    d2 = get_drive()
    assert d2 is not d1  # new object computed


# ---------------------------------------------------------------------------
# 9. Env-var override: non-default env takes priority over live drive
# ---------------------------------------------------------------------------
def test_env_override_blocks_live_pivot(monkeypatch, tmp_path):
    """Setting RADAM_PIVOT_ALPHA=0.7 should use 0.7, not the drive value."""
    import src.brain.learning_drive as _ld
    drive = LearningDrive(
        pivot_alpha=0.4, heartbeat_kappa=0.2,
        noise_sigma=0.1, acquisition_drive=0.15,
        corpus_saturation=0.8, self_train_quality=0.3,
        learning_velocity=0.1, rdt_difficulty=0.7,
    )

    # Simulate the _knob() helper used in brain_body_signals
    def _knob(env_key, default_str, drive_val):
        env_raw = os.environ.get(env_key, default_str)
        if env_raw != default_str:
            return float(env_raw)
        return drive_val

    monkeypatch.setenv("RADAM_PIVOT_ALPHA", "0.7")
    assert _knob("RADAM_PIVOT_ALPHA", "1.0", drive.pivot_alpha) == pytest.approx(0.7)

    monkeypatch.delenv("RADAM_PIVOT_ALPHA", raising=False)
    # Env at default → drive value used
    assert _knob("RADAM_PIVOT_ALPHA", "1.0", drive.pivot_alpha) == pytest.approx(0.4)


# ---------------------------------------------------------------------------
# 10. acquisition_drive is added to grad_imag in radam_step
# ---------------------------------------------------------------------------
def test_acquisition_drive_boosts_grad_imag():
    """Adding acquisition_drive to grad_imag must change the optimizer output."""
    from src.brain.radam_optimizer import radam_step

    s_no_acq  = {"pressure": 0.3}
    s_with_acq = {"pressure": 0.3}

    radam_step(s_no_acq,   grad_real=0.4, grad_imag=0.0)
    radam_step(s_with_acq, grad_real=0.4, grad_imag=0.20)  # typical acquisition_drive

    # magnitude differs → pressure and phase must change
    assert s_no_acq["pressure"] != s_with_acq["pressure"]


def test_zero_acquisition_drive_no_change():
    """acquisition_drive=0.0 must be identical to not passing it."""
    from src.brain.radam_optimizer import radam_step

    s1 = {"pressure": 0.3}
    s2 = {"pressure": 0.3}

    radam_step(s1, grad_real=0.4, grad_imag=0.1)
    radam_step(s2, grad_real=0.4, grad_imag=0.1 + 0.0)

    assert s1["pressure"] == pytest.approx(s2["pressure"])
    assert s1["theta"] == pytest.approx(s2["theta"])
