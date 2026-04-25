import sqlite3, json

cn = sqlite3.connect('local_brain.sqlite', timeout=15)
cn.row_factory = sqlite3.Row

print('=== Entity type counts ===')
for r in cn.execute(
    'SELECT entity_type, COUNT(*) as n FROM corpus_entity '
    'WHERE entity_type IN ("GrokConversation","SCBTopic","CivilizationDomain","Document") '
    'GROUP BY entity_type ORDER BY n DESC'
).fetchall():
    print(f'  {r["entity_type"]:<22}  {r["n"]}')

print()
print('=== CivilizationDomain entities + conversation count ===')
for r in cn.execute(
    'SELECT e.entity_id, e.label, COUNT(ed.src_id) as convs '
    'FROM corpus_entity e '
    'LEFT JOIN corpus_edge ed ON ed.dst_id=e.entity_id AND ed.rel="CROSS_POLLINATES" '
    'WHERE e.entity_type="CivilizationDomain" '
    'GROUP BY e.entity_id ORDER BY convs DESC'
).fetchall():
    print(f'  {r["label"]:<35}  convs={r["convs"]}')

print()
print('=== quest:type5_sc inbound edges ===')
for r in cn.execute(
    'SELECT src_type, rel, COUNT(*) as n, ROUND(AVG(weight),3) as w '
    'FROM corpus_edge WHERE dst_id="quest:type5_sc" '
    'GROUP BY src_type, rel ORDER BY n DESC'
).fetchall():
    print(f'  {r["src_type"]:<22} --{r["rel"]:<18}--> quest:type5_sc  n={r["n"]}  avg_w={r["w"]}')

print()
print('=== TRANSCENDS_TO edges from CivilizationDomain ===')
for r in cn.execute(
    'SELECT src_id, dst_id, dst_type, weight FROM corpus_edge '
    'WHERE rel="TRANSCENDS_TO" ORDER BY weight DESC'
).fetchall():
    print(f'  {r["src_id"]:<38} --> [{r["dst_type"]}] {r["dst_id"]}  w={r["weight"]}')

print()
print('=== Total learnings by kind ===')
for r in cn.execute(
    'SELECT kind, COUNT(*) as n, ROUND(AVG(signal_strength),3) as avg_sig '
    'FROM learning_log GROUP BY kind ORDER BY n DESC'
).fetchall():
    print(f'  {r["kind"]:<25}  n={r["n"]:5}  avg_sig={r["avg_sig"]}')

print()
print('=== scb_doc tier breakdown ===')
sc = 0; cd = 0
for r in cn.execute("SELECT detail FROM learning_log WHERE kind='scb_doc'").fetchall():
    d = json.loads(r['detail'] or '{}')
    if d.get('tier') == 'operational_sc':
        sc += 1
    else:
        cd += 1
print(f'  operational_sc: {sc}')
print(f'  cross_domain:   {cd}')

print()
print('=== Task/Quest total inbound INFORMS+TRANSCENDS_TO weight ===')
for r in cn.execute(
    'SELECT dst_id, dst_type, COUNT(*) as inbound, ROUND(SUM(weight),2) as total_w '
    'FROM corpus_edge WHERE rel IN ("INFORMS","TRANSCENDS_TO","INFORMS_VISION","GROUNDS_IN") '
    'AND dst_type IN ("Task","Quest") '
    'GROUP BY dst_id ORDER BY total_w DESC'
).fetchall():
    print(f'  [{r["dst_type"]}] {r["dst_id"]:<40}  inbound={r["inbound"]}  total_w={r["total_w"]}')

cn.close()
