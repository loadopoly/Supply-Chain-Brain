"""Retroactively fix signal_strength=0.0 in learning_log where avg_validator
was NULL and the correct value is matched/samples from llm_self_train_log."""
import sqlite3

cn = sqlite3.connect('local_brain.sqlite')
cn.row_factory = sqlite3.Row

# Show BEFORE state
print("BEFORE fix:")
for r in cn.execute(
    "SELECT id, kind, signal_strength, source_row_id FROM learning_log WHERE kind='self_train'"
).fetchall():
    print(f"  id={r['id']}  str={r['signal_strength']}  src={r['source_row_id']}")

# Retroactive fix: join learning_log → llm_self_train_log by source_row_id
# Only update rows where avg_validator was NULL and samples > 0
cn.execute("""
    UPDATE learning_log
    SET signal_strength = COALESCE((
        SELECT CAST(s.matched AS REAL) / MAX(1, CAST(s.samples AS REAL))
        FROM llm_self_train_log s
        WHERE s.id = learning_log.source_row_id
          AND s.avg_validator IS NULL
          AND s.samples > 0
    ), signal_strength, 0.0)
    WHERE kind = 'self_train'
      AND source_table = 'llm_self_train_log'
      AND signal_strength = 0.0
      AND source_row_id IS NOT NULL
""")
cn.commit()

# Show AFTER state
print()
print("AFTER fix:")
for r in cn.execute(
    "SELECT id, kind, title, signal_strength FROM learning_log WHERE kind='self_train'"
).fetchall():
    print(f"  id={r['id']}  str={r['signal_strength']:.4f}  {r['title'][:70]}")

cn.close()
print("Done.")
