import sqlite3, json

cn = sqlite3.connect('local_brain.sqlite', timeout=30)
cn.row_factory = sqlite3.Row

print('=== SCB entities ===')
for r in cn.execute(
    "SELECT entity_type, entity_id, label, samples FROM corpus_entity "
    "WHERE entity_type IN ('GrokConversation','SCBTopic','Document') "
    "ORDER BY entity_type, samples DESC"
).fetchall():
    print(f'  [{r["entity_type"]}] {r["entity_id"][:55]}  samp={r["samples"]}  label={r["label"][:40]}')

print()
print('=== SCB edges ===')
for r in cn.execute(
    "SELECT src_type, rel, dst_type, COUNT(*) as n, ROUND(AVG(weight),3) as w "
    "FROM corpus_edge "
    "WHERE src_type IN ('GrokConversation','SCBTopic','Document') "
    "   OR dst_type IN ('GrokConversation','SCBTopic') "
    "GROUP BY src_type, rel, dst_type ORDER BY n DESC"
).fetchall():
    print(f'  {r["src_type"]:18} --{r["rel"]:12}--> {r["dst_type"]:18}  n={r["n"]}  w={r["w"]}')

print()
print('=== scb_doc learnings ===')
for r in cn.execute(
    "SELECT title, signal_strength, detail FROM learning_log WHERE kind='scb_doc' ORDER BY id DESC LIMIT 10"
).fetchall():
    d = json.loads(r['detail'] or '{}')
    print(f'  sig={r["signal_strength"]:.3f}  topics={d.get("topics")}  {r["title"][:60]}')

print()
print('=== scb_docs_mtime cursor ===')
row = cn.execute("SELECT value FROM corpus_cursor WHERE key='scb_docs_mtime'").fetchone()
print(' ', row['value'] if row else 'NOT SET')

cn.close()
