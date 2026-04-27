"""Local SQLite-backed store for Brain-side metadata that should not live in the
replica DW: per-part NLP categorization, OTD owner/comment history, and
action-item bookmarks. The DB lives at `pipeline/local_brain.sqlite`.
"""
from __future__ import annotations
import sqlite3
from contextlib import contextmanager
import os
from pathlib import Path
from typing import Iterable
import pandas as pd

_DB_PATH = Path(__file__).resolve().parents[2] / "local_brain.sqlite"


def db_path() -> Path:
    override = os.environ.get("SCB_DB_PATH")
    if override:
        return Path(override).expanduser().resolve()
    return _DB_PATH


@contextmanager
def _conn():
    cn = sqlite3.connect(_DB_PATH)
    try:
        yield cn
        cn.commit()
    finally:
        cn.close()


def _migrate_part_category(cn: sqlite3.Connection) -> None:
    """Bring part_category up to current schema without dropping data.

    Handles:
    * Rename ``part_id`` → ``part_key`` (legacy column name).
    * Add ``confidence``, ``source``, ``updated_at`` if missing.
    * Add ``provisional``, ``refinement_count``, ``description_cache`` if missing.
    """
    existing = {row[1] for row in cn.execute("PRAGMA table_info(part_category)").fetchall()}

    # Legacy rename: part_id → part_key
    if "part_id" in existing and "part_key" not in existing:
        cn.execute("ALTER TABLE part_category RENAME COLUMN part_id TO part_key")
        existing.discard("part_id")
        existing.add("part_key")

    migrations = [
        ("confidence",       "ALTER TABLE part_category ADD COLUMN confidence REAL DEFAULT 0.0"),
        ("source",           "ALTER TABLE part_category ADD COLUMN source TEXT DEFAULT 'legacy'"),
        ("updated_at",       "ALTER TABLE part_category ADD COLUMN updated_at TIMESTAMP DEFAULT NULL"),
        ("provisional",      "ALTER TABLE part_category ADD COLUMN provisional INTEGER DEFAULT 0"),
        ("refinement_count", "ALTER TABLE part_category ADD COLUMN refinement_count INTEGER DEFAULT 0"),
        ("description_cache","ALTER TABLE part_category ADD COLUMN description_cache TEXT"),
    ]
    for col, sql in migrations:
        if col not in existing:
            cn.execute(sql)


def init_schema() -> None:
    with _conn() as cn:
        cn.executescript(
            """
            CREATE TABLE IF NOT EXISTS part_category (
                part_key          TEXT PRIMARY KEY,
                category          TEXT,
                confidence        REAL,
                source            TEXT,
                provisional       INTEGER DEFAULT 0,
                refinement_count  INTEGER DEFAULT 0,
                description_cache TEXT,
                updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS ix_part_category_cat ON part_category(category);

            CREATE TABLE IF NOT EXISTS otd_ownership (
                row_key            TEXT PRIMARY KEY,
                owner              TEXT,
                owner_comment      TEXT,
                previous_owner     TEXT,
                previous_comment   TEXT,
                needs_review       INTEGER DEFAULT 0,
                previous_needs_review INTEGER DEFAULT 0,
                opened_yesterday   INTEGER DEFAULT 0,
                updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS action_bookmarks (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                stage         TEXT,
                title         TEXT,
                detail        TEXT,
                priority      INTEGER,
                value_score   REAL,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        _migrate_part_category(cn)


# ---------------------------------------------------------------- categories
# Confidence threshold below which a classification is held as provisional
# (test corpus — eligible for future refinement).
_PROV_THRESHOLD: float = 0.15


def upsert_category(part_key: str, category: str, confidence: float = 1.0,
                    source: str = "nlp") -> None:
    """Single-row upsert — always overwrites (used for authoritative external data)."""
    init_schema()
    prov = 0 if confidence >= _PROV_THRESHOLD and category != "Uncategorized" else 1
    with _conn() as cn:
        cn.execute(
            "INSERT INTO part_category"
            "(part_key, category, confidence, source, provisional) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(part_key) DO UPDATE SET category=excluded.category, "
            "confidence=excluded.confidence, source=excluded.source, "
            "provisional=excluded.provisional, updated_at=CURRENT_TIMESTAMP",
            (str(part_key), str(category), float(confidence), str(source), prov),
        )


def upsert_categories(rows: Iterable[tuple]) -> None:
    """Batch upsert — always overwrites (backward-compatible 4-tuple path)."""
    init_schema()
    with _conn() as cn:
        cn.executemany(
            "INSERT INTO part_category(part_key, category, confidence, source, provisional) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(part_key) DO UPDATE SET category=excluded.category, "
            "confidence=excluded.confidence, source=excluded.source, "
            "provisional=excluded.provisional, updated_at=CURRENT_TIMESTAMP",
            [
                (str(r[0]), str(r[1]), float(r[2]), str(r[3]),
                 0 if float(r[2]) >= _PROV_THRESHOLD and str(r[1]) != "Uncategorized" else 1)
                for r in rows
            ],
        )


def upsert_categories_ext(rows: Iterable[tuple]) -> None:
    """Batch upsert with full 6-tuple: (part_key, category, confidence, source,
    provisional, description_cache).

    Uses a *confidence-wins* ON CONFLICT strategy — existing record is updated
    only when the incoming confidence is strictly higher.  This preserves
    hard-won confident classifications against weaker NLP re-runs.
    ``refinement_count`` is incremented on every conflict (not on INSERT).
    """
    init_schema()
    _rows = []
    for r in rows:
        pk, cat, conf, src = str(r[0]), str(r[1]), float(r[2]), str(r[3])
        prov = int(r[4]) if len(r) > 4 else (
            0 if conf >= _PROV_THRESHOLD and cat != "Uncategorized" else 1
        )
        desc = str(r[5]) if len(r) > 5 else ""
        _rows.append((pk, cat, conf, src, prov, desc))

    with _conn() as cn:
        cn.executemany(
            "INSERT INTO part_category"
            "(part_key, category, confidence, source, provisional, description_cache) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(part_key) DO UPDATE SET "
            # Keep whichever classification has higher confidence
            "  category        = CASE WHEN excluded.confidence > part_category.confidence "
            "                         THEN excluded.category ELSE part_category.category END, "
            "  confidence      = MAX(excluded.confidence, part_category.confidence), "
            # Promote from provisional when combined confidence crosses threshold
            "  provisional     = CASE "
            "                      WHEN MAX(excluded.confidence, part_category.confidence) >= "
            f"                          {_PROV_THRESHOLD} "
            "                           AND (CASE WHEN excluded.confidence > part_category.confidence "
            "                                     THEN excluded.category ELSE part_category.category END) "
            "                               != 'Uncategorized' "
            "                      THEN 0 ELSE 1 END, "
            "  refinement_count = part_category.refinement_count + 1, "
            # Cache description if not already stored
            "  description_cache = COALESCE(part_category.description_cache, excluded.description_cache), "
            "  source          = excluded.source, "
            "  updated_at      = CURRENT_TIMESTAMP",
            _rows,
        )


def fetch_categories() -> pd.DataFrame:
    """Return all part_category rows (provisional + confirmed) for DBI use."""
    init_schema()
    with _conn() as cn:
        return pd.read_sql_query("SELECT * FROM part_category", cn)


def fetch_provisional(limit: int = 500) -> pd.DataFrame:
    """Return provisional (low-confidence test-corpus) parts for refinement."""
    init_schema()
    with _conn() as cn:
        return pd.read_sql_query(
            f"SELECT * FROM part_category WHERE provisional=1 "
            f"ORDER BY refinement_count ASC, confidence ASC LIMIT {int(limit)}",
            cn,
        )


def delete_categories(part_keys: Iterable[str]) -> int:
    """Hard-delete rows from part_category by key.

    Used to cull 'Uncategorized' entries after the test corpus has reached
    statistical certainty that they cannot be classified with the current
    taxonomy.  Returns the number of rows deleted.
    """
    keys = [str(k) for k in part_keys]
    if not keys:
        return 0
    init_schema()
    with _conn() as cn:
        # SQLite allows up to 999 host parameters — chunk for safety
        deleted = 0
        for i in range(0, len(keys), 900):
            chunk = keys[i : i + 900]
            placeholders = ",".join(["?"] * len(chunk))
            cur = cn.execute(
                f"DELETE FROM part_category WHERE part_key IN ({placeholders})",
                chunk,
            )
            deleted += cur.rowcount
    return deleted


# ---------------------------------------------------------------- OTD owners
def upsert_otd_owner(row_key: str, owner: str | None = None,
                     owner_comment: str | None = None,
                     needs_review: bool | None = None) -> None:
    init_schema()
    with _conn() as cn:
        prev = cn.execute(
            "SELECT owner, owner_comment, needs_review FROM otd_ownership WHERE row_key=?",
            (str(row_key),),
        ).fetchone()
        prev_owner, prev_comment, prev_nr = (prev if prev else (None, None, 0))

        new_owner = owner if owner is not None else prev_owner
        new_comment = owner_comment if owner_comment is not None else prev_comment
        new_nr = int(needs_review) if needs_review is not None else (prev_nr or 0)

        cn.execute(
            "INSERT INTO otd_ownership(row_key, owner, owner_comment, previous_owner, "
            "previous_comment, needs_review, previous_needs_review) VALUES "
            "(?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(row_key) DO UPDATE SET "
            "previous_owner=otd_ownership.owner, "
            "previous_comment=otd_ownership.owner_comment, "
            "previous_needs_review=otd_ownership.needs_review, "
            "owner=excluded.owner, owner_comment=excluded.owner_comment, "
            "needs_review=excluded.needs_review, "
            "updated_at=CURRENT_TIMESTAMP",
            (str(row_key), new_owner, new_comment, prev_owner, prev_comment, new_nr, prev_nr or 0),
        )


def fetch_otd_owners() -> pd.DataFrame:
    init_schema()
    with _conn() as cn:
        return pd.read_sql_query("SELECT * FROM otd_ownership", cn)


# ---------------------------------------------------------------- bookmarks
def add_bookmark(stage: str, title: str, detail: str, priority: int = 3,
                 value_score: float = 0.0) -> None:
    init_schema()
    with _conn() as cn:
        cn.execute(
            "INSERT INTO action_bookmarks(stage, title, detail, priority, value_score) "
            "VALUES (?, ?, ?, ?, ?)",
            (stage, title, detail, int(priority), float(value_score)),
        )


def fetch_bookmarks() -> pd.DataFrame:
    init_schema()
    with _conn() as cn:
        return pd.read_sql_query(
            "SELECT * FROM action_bookmarks ORDER BY value_score DESC, priority ASC", cn
        )
