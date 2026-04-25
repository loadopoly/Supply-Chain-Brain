import sqlite3, json

cn = sqlite3.connect('local_brain.sqlite', timeout=15)
cn.row_factory = sqlite3.Row

print('=== Corpus entity landscape ===')
for r in cn.execute('SELECT entity_type, COUNT(*) as n FROM corpus_entity GROUP BY entity_type ORDER BY n DESC').fetchall():
    print(f'  {r["entity_type"]:<30}  {r["n"]:5}')

print()
print('=== SCBTopic -> Task/Quest INFORMS edges ===')
for r in cn.execute(
    'SELECT src_id, rel, dst_id, dst_type, weight FROM corpus_edge '
    'WHERE src_type="SCBTopic" AND rel="INFORMS" ORDER BY weight DESC'
).fetchall():
    print(f'  {r["src_id"]:<35} --INFORMS--> [{r["dst_type"]}] {r["dst_id"]}  w={r["weight"]}')

print()
print('=== Learning log kinds (all time) ===')
for r in cn.execute(
    'SELECT kind, COUNT(*) as n, ROUND(AVG(signal_strength),3) as avg_sig '
    'FROM learning_log GROUP BY kind ORDER BY n DESC'
).fetchall():
    print(f'  {r["kind"]:<25}  n={r["n"]:5}  avg_sig={r["avg_sig"]}')

print()
print('=== Edge relationship types (full corpus) ===')
for r in cn.execute(
    'SELECT rel, COUNT(*) as n, ROUND(AVG(weight),3) as w FROM corpus_edge '
    'GROUP BY rel ORDER BY n DESC LIMIT 20'
).fetchall():
    print(f'  {r["rel"]:<25}  n={r["n"]:5}  avg_w={r["w"]}')

print()
print('=== AcademicTopics with EXPLORES inbound edges from GrokConversation ===')
for r in cn.execute(
    'SELECT dst_id, COUNT(*) as sources FROM corpus_edge '
    'WHERE rel="EXPLORES" AND src_type="GrokConversation" '
    'GROUP BY dst_id ORDER BY sources DESC'
).fetchall():
    print(f'  {r["dst_id"]:<40}  inbound={r["sources"]}')

print()
print('=== Task/Quest entities + their total inbound INFORMS weight ===')
for r in cn.execute(
    'SELECT e.dst_id, e.dst_type, COUNT(*) as inbound, ROUND(SUM(e.weight),3) as total_w '
    'FROM corpus_edge e '
    'WHERE e.rel="INFORMS" AND e.dst_type IN ("Task","Quest") '
    'GROUP BY e.dst_id ORDER BY total_w DESC'
).fetchall():
    print(f'  [{r["dst_type"]}] {r["dst_id"]:<35}  inbound={r["inbound"]}  total_w={r["total_w"]}')

print()
print('=== Most recent 10 scb_doc learnings ===')
for r in cn.execute(
    'SELECT logged_at, title, signal_strength, detail FROM learning_log '
    'WHERE kind="scb_doc" ORDER BY id DESC LIMIT 10'
).fetchall():
    d = json.loads(r['detail'] or '{}')
    topics = ', '.join(d.get('topics', []))
    print(f'  [{r["logged_at"][:16]}] sig={r["signal_strength"]:.3f}  {r["title"][:55]}')
    print(f'    topics: {topics}')

print()
print('=== corpus_cursor: all keys ===')
for r in cn.execute('SELECT key, value FROM corpus_cursor ORDER BY key').fetchall():
    print(f'  {r["key"]:<30}  {r["value"]}')

cn.close()
