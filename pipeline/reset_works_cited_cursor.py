"""Reset scb_docs_mtime_v2 cursor and re-ingest Works Cited with raised limit."""
import sqlite3
import pathlib
import sys

DB = pathlib.Path("local_brain.sqlite")

# 1. Reset cursor and clear stale data in one transaction
with sqlite3.connect(DB) as cn:
    cn.execute("UPDATE corpus_cursor SET value=0 WHERE key='scb_docs_mtime_v2'")
    n_wc = cn.execute("DELETE FROM corpus_entity WHERE entity_type='WorksCitedReference'").rowcount
    cn.execute("DELETE FROM corpus_edge WHERE rel='GUIDES_EXPANSION'")
    cn.execute("DELETE FROM brain_kv WHERE key='grok_research:bibliography:works_cited'")
    cn.execute("DELETE FROM brain_kv WHERE key='grok_research:pirates_code'")
    n_ll = cn.execute("DELETE FROM learning_log WHERE kind='scb_doc'").rowcount
    cn.commit()
    print(f"Reset: cursor=0, deleted {n_wc} WorksCitedReference, {n_ll} scb_doc learning_log entries")

# 2. Run ONLY the SCB ingest directly (avoids OCW network calls / DB-lock issues)
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from src.brain.knowledge_corpus import init_schema, _conn, _get_cursor, _ingest_scb_docs, _set_cursor, _Stats

init_schema()
stats = _Stats()

with _conn() as cn:
    c_scb = _get_cursor(cn, "scb_docs_mtime_v2")
    print(f"cursor before: {c_scb}")
    c_scb = _ingest_scb_docs(cn, stats, c_scb)
    _set_cursor(cn, "scb_docs_mtime_v2", c_scb)
    cn.commit()
    print(f"cursor after:  {c_scb}")

print(f"stats: entities_added={stats.entities_added}, learnings_logged={stats.learnings_logged}, edges_added={stats.edges_added}")

# 3. Verify
with sqlite3.connect(DB) as cn:
    wc = cn.execute("SELECT COUNT(*) FROM corpus_entity WHERE entity_type='WorksCitedReference'").fetchone()[0]
    pp = cn.execute("SELECT COUNT(*) FROM corpus_entity WHERE entity_type='Paper'").fetchone()[0]
    ge = cn.execute("SELECT COUNT(*) FROM corpus_edge WHERE rel='GUIDES_EXPANSION'").fetchone()[0]
    cur = cn.execute("SELECT value FROM corpus_cursor WHERE key='scb_docs_mtime_v2'").fetchone()[0]
    print(f"\nFinal state:")
    print(f"  WorksCitedReference entities : {wc}")
    print(f"  Paper entities               : {pp}")
    print(f"  GUIDES_EXPANSION edges       : {ge}")
    print(f"  cursor scb_docs_mtime_v2     : {cur}")
