"""Smoke test: deep-fetch one OCW course, persist, ingest, validate corpus."""
import sqlite3
from src.brain.ml_research import deepen_ocw_course, fetch_ocw_course_detail
from src.brain import knowledge_corpus as kc

# 1) Deep-fetch one known course
SLUG = "1-022-introduction-to-network-models-fall-2018"
print(f"=== Deep-fetching {SLUG} ===")
detail = fetch_ocw_course_detail(SLUG)
if not detail:
    print("FAILED: no detail returned (network/SSL issue)")
    raise SystemExit(1)

print(f"  description : {len(detail.get('description',''))} chars")
print(f"  instructors : {detail.get('instructors')}")
print(f"  topics      : {detail.get('topics')[:5]}")
print(f"  level       : {detail.get('level')}")
print(f"  resources   : {len(detail.get('resources',[]))}")
print(f"  related     : {len(detail.get('related_courses',[]))}")
print(f"  external    : {len(detail.get('external_links',[]))}")

# 2) Persist + ingest
print(f"\n=== Persisting + ingesting ===")
res = deepen_ocw_course(SLUG)
print(f"  rows_written: {res.get('rows_written')}")

# Need OCWCourse entity to exist before resources can attach edges.
# If it doesn't exist, the ingester still creates WebResource entities but no edges to course.
# So we ensure it via direct upsert — the regular OCW search would normally do this.
import sqlite3 as _sq3
cn = _sq3.connect('local_brain.sqlite'); cn.row_factory = _sq3.Row
cn.execute(
    "INSERT OR IGNORE INTO corpus_entity(entity_id, entity_type, label, props_json, first_seen, last_seen) "
    "VALUES(?, 'OCWCourse', ?, '{}', datetime('now'), datetime('now'))",
    (SLUG, SLUG),
)
cn.commit(); cn.close()

# 3) Force a corpus refresh
kc._LAST_REFRESH_TS = 0
r = kc.refresh_corpus_round()
print(f"  Round: added={r.get('entities_added')}  edges_added={r.get('edges_added')}  "
      f"learn={r.get('learnings_logged')}  notes={r.get('notes',[])[:3]}")

# 4) Verify
cn = _sq3.connect('local_brain.sqlite'); cn.row_factory = _sq3.Row
counts = {
    et: cn.execute("SELECT COUNT(*) FROM corpus_entity WHERE entity_type=?", (et,)).fetchone()[0]
    for et in ('OCWCourse','WebResource','Instructor','ExternalDomain','AcademicTopic')
}
print(f"\n=== Corpus entity counts ===")
for k,v in counts.items():
    print(f"  {k:<18}: {v}")

edges = cn.execute(
    "SELECT rel, COUNT(*) c FROM corpus_edge WHERE rel IN "
    "('HAS_RESOURCE','REFERENCES','RELATED_TO','TAUGHT_BY','HOSTED_ON','COVERS') "
    "GROUP BY rel ORDER BY c DESC"
).fetchall()
print(f"\n=== Edge counts (OCW lattice) ===")
for e in edges:
    print(f"  {e['rel']:<14}: {e['c']}")

# Sample a few WebResource URLs
print(f"\n=== Sample WebResource URLs (first 5) ===")
for r in cn.execute(
    "SELECT entity_id, label, props_json FROM corpus_entity WHERE entity_type='WebResource' LIMIT 5"
).fetchall():
    print(f"  - {r['label'][:60]:<60} → {r['entity_id'][:80]}")
cn.close()
