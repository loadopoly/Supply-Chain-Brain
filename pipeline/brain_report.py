"""One-shot Brain learning report — run from pipeline/ directory."""
import sys, json
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / 'local_brain.sqlite'

def db_path():
    return DB_PATH
import sqlite3

db = db_path()
print(f"DB: {db}\n")
cn = sqlite3.connect(db)
cn.row_factory = sqlite3.Row

tables = [r[0] for r in cn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
).fetchall()]
print("TABLES:", tables)
print()

# --- Knowledge Corpus: recent learnings ---
if "learning_log" in tables:
    rows = cn.execute(
        "SELECT kind, title, signal_strength, logged_at FROM learning_log "
        "ORDER BY id DESC LIMIT 30"
    ).fetchall()
    print(f"=== RECENT LEARNINGS ({len(rows)}) ===")
    for r in rows:
        print(f"  [{r['kind']}] {r['title']}  strength={r['signal_strength']}  ts={r['logged_at']}")
    print()

# --- Corpus entities ---
if "corpus_entity" in tables:
    counts = cn.execute(
        "SELECT entity_type, COUNT(*) as n FROM corpus_entity GROUP BY entity_type ORDER BY n DESC"
    ).fetchall()
    print("=== CORPUS ENTITIES ===")
    for r in counts:
        print(f"  {r['entity_type']}: {r['n']}")
    print()

# --- Corpus edges ---
if "corpus_edge" in tables:
    counts = cn.execute(
        "SELECT rel, COUNT(*) as n FROM corpus_edge GROUP BY rel ORDER BY n DESC"
    ).fetchall()
    print("=== CORPUS EDGES ===")
    for r in counts:
        print(f"  {r['rel']}: {r['n']}")
    print()

# --- Self-train log ---
if "llm_self_train_log" in tables:
    rows = cn.execute(
        "SELECT ran_at, task, samples, matched, avg_validator, drift_capped, "
        "diversity_dampened FROM llm_self_train_log ORDER BY id DESC LIMIT 15"
    ).fetchall()
    print(f"=== SELF-TRAIN LOG (last {len(rows)}) ===")
    for r in rows:
        pct = round(r['matched']/r['samples']*100, 1) if r['samples'] else 0
        print(f"  [{r['ran_at']}] task={r['task']}  "
              f"samples={r['samples']}  matched={r['matched']} ({pct}%)  "
              f"avg_validator={r['avg_validator']}  "
              f"drift_capped={r['drift_capped']}  damped={r['diversity_dampened']}")
    print()

# --- LLM weights ---
if "llm_weights" in tables:
    rows = cn.execute(
        "SELECT model_id, task, weight, bias, updated_at FROM llm_weights "
        "ORDER BY task, weight DESC"
    ).fetchall()
    print("=== LLM WEIGHTS (by task) ===")
    cur_task = None
    for r in rows:
        if r['task'] != cur_task:
            cur_task = r['task']
            print(f"  --- {cur_task} ---")
        print(f"    {r['model_id']}: weight={r['weight']:.4f}  bias={r['bias']:.4f}  upd={r['updated_at']}")
    print()

# --- Network observations summary ---
if "network_observations" in tables:
    counts = cn.execute(
        "SELECT protocol, ok, COUNT(*) as n "
        "FROM network_observations GROUP BY protocol, ok ORDER BY protocol, ok"
    ).fetchall()
    print("=== NETWORK OBSERVATIONS (protocol / ok) ===")
    for r in counts:
        print(f"  {r['protocol']}  ok={r['ok']}  n={r['n']}")
    print()

# --- Network topology (top hosts) ---
if "network_topology" in tables:
    rows = cn.execute(
        "SELECT host, ema_latency_ms, ema_success, last_seen, protocol "
        "FROM network_topology ORDER BY ema_success DESC LIMIT 20"
    ).fetchall()
    print("=== NETWORK TOPOLOGY (top 20 by success rate) ===")
    for r in rows:
        print(f"  {r['host']}  rtt={r['ema_latency_ms']}ms  "
              f"success={r['ema_success']}  [{r['protocol']}]  last={r['last_seen']}")
    print()

# --- Network promotions ---
if "network_promotions" in tables:
    rows = cn.execute(
        "SELECT host, promoted_at, target, reason FROM network_promotions "
        "ORDER BY id DESC LIMIT 15"
    ).fetchall()
    print(f"=== PEER PROMOTIONS ({len(rows)}) ===")
    for r in rows:
        print(f"  {r['host']}  -> {r['target']}  {r['promoted_at']}  {r['reason']}")
    print()

# --- Recurrent depth ---
if "recurrent_depth_log" in tables:
    rows = cn.execute(
        "SELECT task, AVG(final_depth) as avg_depth, MAX(final_depth) as max_depth, "
        "AVG(final_kl) as avg_kl, COUNT(*) as n "
        "FROM recurrent_depth_log GROUP BY task ORDER BY n DESC LIMIT 20"
    ).fetchall()
    print("=== RECURRENT DEPTH (per-task learned convergence) ===")
    for r in rows:
        print(f"  {r['task']}  avg_depth={round(r['avg_depth'],2)}  "
              f"max_depth={r['max_depth']}  avg_kl={round(r['avg_kl'],4)}  runs={r['n']}")
    print()

# --- Body directives ---
if "body_directives" in tables:
    rows = cn.execute(
        "SELECT signal_kind, title, priority, status, created_at "
        "FROM body_directives ORDER BY priority DESC, id DESC LIMIT 20"
    ).fetchall()
    print(f"=== BODY DIRECTIVES (open signals) ({len(rows)}) ===")
    for r in rows:
        print(f"  [{r['status']}] P{r['priority']} [{r['signal_kind']}] {r['title']}  {r['created_at']}")
    print()

# --- Part categories ---
if "part_category" in tables:
    cats = cn.execute(
        "SELECT category, COUNT(*) as n FROM part_category GROUP BY category ORDER BY n DESC LIMIT 15"
    ).fetchall()
    print("=== PART CATEGORIES (NLP-derived) ===")
    for r in cats:
        print(f"  {r['category']}: {r['n']}")
    print()

# --- OTD ownership ---
if "otd_ownership" in tables:
    owners = cn.execute(
        "SELECT owner, COUNT(*) as n FROM otd_ownership GROUP BY owner ORDER BY n DESC LIMIT 15"
    ).fetchall()
    print("=== OTD OWNERSHIP (recursive attribution) ===")
    for r in owners:
        print(f"  {r['owner']}: {r['n']}")
    print()

cn.close()
