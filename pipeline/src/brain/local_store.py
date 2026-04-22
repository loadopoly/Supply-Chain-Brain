"""Local SQLite-backed store for Brain-side metadata that should not live in the
replica DW: per-part NLP categorization, OTD owner/comment history, and
action-item bookmarks. The DB lives at `pipeline/local_brain.sqlite`.
"""
from __future__ import annotations
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable
import pandas as pd

_DB_PATH = Path(__file__).resolve().parents[2] / "local_brain.sqlite"


def db_path() -> Path:
    return _DB_PATH


@contextmanager
def _conn():
    cn = sqlite3.connect(_DB_PATH)
    try:
        yield cn
        cn.commit()
    finally:
        cn.close()


def init_schema() -> None:
    with _conn() as cn:
        cn.executescript(
            """
            CREATE TABLE IF NOT EXISTS part_category (
                part_key      TEXT PRIMARY KEY,
                category      TEXT,
                confidence    REAL,
                source        TEXT,
                updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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


# ---------------------------------------------------------------- categories
def upsert_category(part_key: str, category: str, confidence: float = 1.0,
                    source: str = "nlp") -> None:
    init_schema()
    with _conn() as cn:
        cn.execute(
            "INSERT INTO part_category(part_key, category, confidence, source) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(part_key) DO UPDATE SET category=excluded.category, "
            "confidence=excluded.confidence, source=excluded.source, "
            "updated_at=CURRENT_TIMESTAMP",
            (str(part_key), str(category), float(confidence), str(source)),
        )


def upsert_categories(rows: Iterable[tuple]) -> None:
    init_schema()
    with _conn() as cn:
        cn.executemany(
            "INSERT INTO part_category(part_key, category, confidence, source) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(part_key) DO UPDATE SET category=excluded.category, "
            "confidence=excluded.confidence, source=excluded.source, "
            "updated_at=CURRENT_TIMESTAMP",
            list(rows),
        )


def fetch_categories() -> pd.DataFrame:
    init_schema()
    with _conn() as cn:
        return pd.read_sql_query("SELECT * FROM part_category", cn)


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
