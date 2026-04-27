import sqlite3
cn = sqlite3.connect('local_brain.sqlite')
wc = cn.execute("SELECT COUNT(*) FROM corpus_entity WHERE entity_type='WorksCitedReference'").fetchone()[0]
pp = cn.execute("SELECT COUNT(*) FROM corpus_entity WHERE entity_type='Paper'").fetchone()[0]
ge = cn.execute("SELECT COUNT(*) FROM corpus_edge WHERE rel='GUIDES_EXPANSION'").fetchone()[0]
cur = cn.execute("SELECT value FROM corpus_cursor WHERE key='scb_docs_mtime_v2'").fetchone()
print('WorksCitedReference:', wc)
print('Paper:', pp)
print('GUIDES_EXPANSION edges:', ge)
print('cursor:', cur)
