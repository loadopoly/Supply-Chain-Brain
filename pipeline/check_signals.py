import sqlite3
cn = sqlite3.connect('local_brain.sqlite')
cn.row_factory = sqlite3.Row

print("=== LEARNING_LOG SIGNALS (all) ===")
rows = cn.execute('SELECT id, kind, title, signal_strength FROM learning_log ORDER BY id').fetchall()
for r in rows:
    print(f"  id={r['id']} [{r['kind']}] str={r['signal_strength']:.4f}  {r['title'][:60]}")

print()
print("=== SELF_TRAIN_LOG ===")
rows = cn.execute('SELECT id, task, samples, matched, avg_validator FROM llm_self_train_log ORDER BY id DESC LIMIT 10').fetchall()
for r in rows:
    print(f"  task={r['task']}  samples={r['samples']}  matched={r['matched']}  avg_validator={r['avg_validator']}")

print()
print("=== OTD_OWNERSHIP rows ===")
n = cn.execute('SELECT COUNT(*) FROM otd_ownership').fetchone()[0]
print(f"  rows: {n}")

cn.close()
print("Done.")
