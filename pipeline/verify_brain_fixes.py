"""Run all Brain fixes and verify results."""
import sys, json
sys.path.insert(0, '.')

# 1. Initialize the network learner schema (skip observe_network_round which
#    does live TCP/DNS probes to corporate endpoints; those run in the agent cycle).
print("=== NETWORK LEARNER SCHEMA INIT ===")
try:
    from src.brain.network_learner import init_schema
    init_schema()
    print("  Tables created / confirmed OK")
except Exception as e:
    print(f"  ERROR: {e}")

# 2. Run corpus refresh to pick up the network data + test signal_strength fix
print()
print("=== CORPUS REFRESH ===")
try:
    from src.brain.knowledge_corpus import refresh_corpus_round
    kc = refresh_corpus_round()
    print(json.dumps(kc, indent=2, default=str))
except Exception as e:
    print(f"  ERROR: {e}")
    import traceback; traceback.print_exc()

# 3. Verify self-train signal_strength is no longer always 0.0
print()
print("=== RECENT LEARNINGS (signal_strength check) ===")
import sqlite3
from pathlib import Path
db = Path("local_brain.sqlite")
if db.exists():
    cn = sqlite3.connect(db)
    cn.row_factory = sqlite3.Row
    rows = cn.execute(
        "SELECT kind, title, signal_strength FROM learning_log ORDER BY id DESC LIMIT 15"
    ).fetchall()
    for r in rows:
        print(f"  [{r['kind']}] str={r['signal_strength']}  {r['title'][:70]}")
    cn.close()

# 4. Verify network tables now exist
print()
print("=== NETWORK TABLES ===")
if db.exists():
    cn = sqlite3.connect(db)
    for t in ['network_observations', 'network_topology', 'network_promotions']:
        try:
            n = cn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t}: {n} rows")
        except Exception as e:
            print(f"  {t}: MISSING ({e})")
    cn.close()

print()
print("Done.")
