"""Patch the NULL signal_strength introduced by the retroactive fix on 0-sample rows."""
import sqlite3

cn = sqlite3.connect('local_brain.sqlite')
cn.row_factory = sqlite3.Row

# Set NULL signal_strength (from 0/0 self-train rows) back to 0.0
result = cn.execute("""
    UPDATE learning_log
    SET signal_strength = 0.0
    WHERE kind = 'self_train'
      AND signal_strength IS NULL
""")
print(f"Patched {result.rowcount} NULL -> 0.0 rows")
cn.commit()

# Final state
print("\nFinal self_train signals:")
for r in cn.execute(
    "SELECT id, title, signal_strength FROM learning_log WHERE kind='self_train'"
).fetchall():
    s = r['signal_strength']
    s_str = f"{s:.4f}" if s is not None else "NULL"
    print(f"  id={r['id']}  str={s_str}  {r['title'][:65]}")

cn.close()
print("Done.")
