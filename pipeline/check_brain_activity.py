from src.brain.local_store import db_path
import sqlite3
cn = sqlite3.connect(str(db_path()))
cn.row_factory = sqlite3.Row

print("=== LEARNING LOG KINDS ===")
for r in cn.execute("SELECT kind, COUNT(*) as n FROM learning_log GROUP BY kind ORDER BY n DESC").fetchall():
    print(f"  {r['kind']:<22} {r['n']}")

print()
print("=== ML RESEARCH / OCW KEYS ===")
for r in cn.execute("SELECT key, value FROM brain_kv WHERE key LIKE '%ml_research%' OR key LIKE '%ocw%'").fetchall():
    print(f"  {r['key']}: {str(r['value'])[:70]}")

print()
print("=== RECENT OCW ENTRIES ===")
for r in cn.execute("SELECT id, logged_at, title FROM learning_log WHERE kind='ocw_course' ORDER BY id DESC LIMIT 10").fetchall():
    print(f"  [{r['id']}] {r['logged_at'][:19]} | {r['title'][:60]}")

cn.close()
