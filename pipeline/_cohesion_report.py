import sqlite3
cn = sqlite3.connect('local_brain.sqlite')
cn.row_factory = sqlite3.Row

print('=== Corpus Entity Breakdown ===')
for r in cn.execute('SELECT entity_type, COUNT(*) c FROM corpus_entity GROUP BY entity_type ORDER BY c DESC').fetchall():
    print(f'  {r["entity_type"]:<22}: {r["c"]:>6}')

print()
print('=== Corpus Edge Breakdown ===')
for r in cn.execute('SELECT rel, COUNT(*) c FROM corpus_edge GROUP BY rel ORDER BY c DESC LIMIT 25').fetchall():
    print(f'  {r["rel"]:<22}: {r["c"]:>6}')

print()
print('=== Last 10 Corpus Rounds ===')
for r in cn.execute('SELECT id, entities_added, entities_touched, edges_added, edges_touched, learnings_logged FROM corpus_round_log ORDER BY id DESC LIMIT 10').fetchall():
    print(f'  [{r["id"]:3d}] +{r["entities_added"]:>4} ent | touched={r["entities_touched"]:>5} | +{r["edges_added"]:>4} edges | learn={r["learnings_logged"]:>3}')

print()
print('=== Learning Log by Kind ===')
for r in cn.execute('SELECT kind, COUNT(*) c FROM learning_log GROUP BY kind ORDER BY c DESC').fetchall():
    print(f'  {r["kind"]:<24}: {r["c"]:>5}')

print()
print('=== OCW Lattice Edges ===')
for r in cn.execute("SELECT rel, COUNT(*) c FROM corpus_edge WHERE rel IN ('HAS_RESOURCE','REFERENCES','RELATED_TO','TAUGHT_BY','HOSTED_ON','COVERS','INFORMS','BELONGS_TO','TEACHES','GROUNDS') GROUP BY rel ORDER BY c DESC").fetchall():
    print(f'  {r["rel"]:<22}: {r["c"]:>5}')

print()
print('=== Cross-domain Edge Map (top 30) ===')
for r in cn.execute("SELECT src_type, rel, dst_type, COUNT(*) c FROM corpus_edge GROUP BY src_type,rel,dst_type ORDER BY c DESC LIMIT 30").fetchall():
    print(f'  {r["src_type"]:<18} --{r["rel"]:<16}--> {r["dst_type"]:<18} ({r["c"]})')

print()
print('=== DW Cursor Positions ===')
for r in cn.execute("SELECT key, value FROM corpus_cursor WHERE key LIKE 'dw_%' OR key LIKE 'ocw%'").fetchall():
    print(f'  {r["key"]:<28}: {r["value"]}')

print()
print('=== Path: OCWCourse → External world (sample) ===')
for r in cn.execute("""
    SELECT e1.entity_id course, e2.entity_id url, ed.rel
    FROM corpus_edge ed
    JOIN corpus_entity e1 ON e1.entity_id=ed.src_id AND e1.entity_type='OCWCourse'
    JOIN corpus_entity e2 ON e2.entity_id=ed.dst_id AND e2.entity_type='WebResource'
    WHERE ed.rel='REFERENCES'
    LIMIT 8
""").fetchall():
    print(f'  {r["course"][:40]:<42} --REFERENCES--> {r["url"][:60]}')

print()
print('=== Path: Part → DataTable (via DW outreach) ===')
sample = cn.execute("SELECT entity_id, label FROM corpus_entity WHERE entity_type='Part' LIMIT 5").fetchall()
for r in sample:
    print(f'  Part: {r["label"][:60]}')

cn.close()
