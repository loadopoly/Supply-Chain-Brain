import sqlite3, json, os
from datetime import datetime, timezone

cn = sqlite3.connect('local_brain.sqlite', timeout=15)
cn.row_factory = sqlite3.Row

print('=== Most recent learning_log entries (last 20) ===')
for r in cn.execute(
    'SELECT kind, title, signal_strength, logged_at FROM learning_log '
    'ORDER BY logged_at DESC LIMIT 20'
).fetchall():
    ts = str(r['logged_at'])[:19]
    print(f'  [{ts}] {r["kind"]:<22} sig={r["signal_strength"]:.3f}  {r["title"][:70]}')

print()
print('=== Learning activity by day ===')
for r in cn.execute(
    "SELECT strftime('%Y-%m-%d', logged_at, 'unixepoch') as day, "
    "COUNT(*) as n, ROUND(AVG(signal_strength),3) as avg_s, "
    "GROUP_CONCAT(DISTINCT kind) as kinds "
    "FROM learning_log GROUP BY day ORDER BY day DESC LIMIT 14"
).fetchall():
    print(f'  {r["day"]}  n={r["n"]:5}  avg_sig={r["avg_s"]}  kinds={r["kinds"]}')

print()
print('=== corpus_cursor state ===')
for r in cn.execute('SELECT key, value FROM corpus_cursor ORDER BY key').fetchall():
    print(f'  {r["key"]:<40}  val={r["value"]}')

print()
print('=== brain_kv system keys ===')
try:
    for r in cn.execute('SELECT key, value, updated_at FROM brain_kv ORDER BY updated_at DESC LIMIT 20').fetchall():
        ts = datetime.fromtimestamp(r['updated_at'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC') if r['updated_at'] else 'never'
        print(f'  {r["key"]:<40}  {str(r["value"])[:50]}  [{ts}]')
except Exception as e:
    print(f'  brain_kv error: {e}')

print()
print('=== Autonomous agent last heartbeat ===')
import os
hb_path = 'logs/agent_heartbeat.txt'
if os.path.exists(hb_path):
    with open(hb_path) as f:
        print(f.read()[-2000:])
else:
    print('  (no heartbeat file found)')

cn.close()
