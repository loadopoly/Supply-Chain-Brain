"""
inject_vision_knowledge.py
Direct injection of the Autocatalytic Agent Vision research into the Brain's
knowledge corpus — learning_log, corpus_entity, and corpus_edge tables.

Run from the pipeline/ directory:
    python inject_vision_knowledge.py
"""
import json
import sqlite3
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# Resolve local_brain.sqlite path the same way knowledge_corpus.py does
_PIPELINE_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_PIPELINE_ROOT))
try:
    from src.brain.local_store import db_path as _db_path
    DB = _db_path()
except Exception:
    DB = _PIPELINE_ROOT / "local_brain.sqlite"

now = datetime.now(timezone.utc).isoformat()


def _conn():
    cn = sqlite3.connect(DB)
    cn.row_factory = sqlite3.Row
    return cn


def ensure_schema(cn):
    cn.executescript("""
        CREATE TABLE IF NOT EXISTS learning_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            logged_at       TEXT NOT NULL,
            kind            TEXT NOT NULL,
            title           TEXT NOT NULL,
            detail          TEXT,
            signal_strength REAL,
            source_table    TEXT,
            source_row_id   INTEGER
        );
        CREATE TABLE IF NOT EXISTS corpus_entity (
            entity_id   TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            label       TEXT,
            props_json  TEXT,
            first_seen  TEXT NOT NULL,
            last_seen   TEXT NOT NULL,
            samples     INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (entity_id, entity_type)
        );
        CREATE TABLE IF NOT EXISTS corpus_edge (
            src_id      TEXT NOT NULL,
            src_type    TEXT NOT NULL,
            dst_id      TEXT NOT NULL,
            dst_type    TEXT NOT NULL,
            rel         TEXT NOT NULL,
            weight      REAL NOT NULL DEFAULT 1.0,
            last_seen   TEXT NOT NULL,
            samples     INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (src_id, src_type, dst_id, dst_type, rel)
        );
    """)
    cn.commit()


def upsert_entity(cn, entity_id, entity_type, label, props=None):
    row = cn.execute(
        "SELECT samples FROM corpus_entity WHERE entity_id=? AND entity_type=?",
        (entity_id, entity_type)
    ).fetchone()
    if row is None:
        cn.execute(
            "INSERT INTO corpus_entity(entity_id, entity_type, label, props_json, first_seen, last_seen, samples) VALUES(?,?,?,?,?,?,1)",
            (entity_id, entity_type, label, json.dumps(props or {}), now, now)
        )
        print(f"  [+] Entity: {entity_type}:{entity_id}")
    else:
        cn.execute(
            "UPDATE corpus_entity SET last_seen=?, samples=samples+1, label=COALESCE(?,label) WHERE entity_id=? AND entity_type=?",
            (now, label, entity_id, entity_type)
        )
        print(f"  [~] Entity updated: {entity_type}:{entity_id}")


def upsert_edge(cn, src_id, src_type, dst_id, dst_type, rel, weight=1.0):
    row = cn.execute(
        "SELECT weight, samples FROM corpus_edge WHERE src_id=? AND src_type=? AND dst_id=? AND dst_type=? AND rel=?",
        (src_id, src_type, dst_id, dst_type, rel)
    ).fetchone()
    if row is None:
        cn.execute(
            "INSERT INTO corpus_edge(src_id,src_type,dst_id,dst_type,rel,weight,last_seen,samples) VALUES(?,?,?,?,?,?,?,1)",
            (src_id, src_type, dst_id, dst_type, rel, weight, now)
        )
        print(f"  [+] Edge: {src_type}:{src_id} --[{rel}]--> {dst_type}:{dst_id}")
    else:
        new_w = 0.7 * float(row["weight"]) + 0.3 * weight
        cn.execute(
            "UPDATE corpus_edge SET last_seen=?, samples=samples+1, weight=? WHERE src_id=? AND src_type=? AND dst_id=? AND dst_type=? AND rel=?",
            (now, new_w, src_id, src_type, dst_id, dst_type, rel)
        )


def log_learning(cn, kind, title, detail=None, signal=0.85):
    cn.execute(
        "INSERT INTO learning_log(logged_at,kind,title,detail,signal_strength,source_table) VALUES(?,?,?,?,?,?)",
        (now, kind, title, json.dumps(detail or {}), signal, "vision_injection")
    )
    print(f"  [L] Learning: [{kind}] {title}")


def main():
    print(f"Injecting Vision knowledge into: {DB}")
    cn = _conn()
    ensure_schema(cn)

    # ── ENTITIES ─────────────────────────────────────────────────────────────
    entities = [
        ("brain",                   "System",     "Supply Chain Brain",         {"role": "orchestrator"}),
        ("piggyback_router",        "Capability", "Piggyback Router v0.7.3",    {"modes": ["socks5", "http", "forward"]}),
        ("integrated_skill_acquirer","Capability","Integrated Skill Acquirer",  {"schedule_hours": 4, "trigger_poll_s": 15}),
        ("bridge_watcher",          "Capability", "Bridge Watcher (PS)",        {"ports": [33890, 14330, 8000, 1080, 3128]}),
        ("cross_tunnel_worming",    "Concept",    "Cross-Tunnel Worming",       {"layer": 7, "protocols": ["SOCKS5","HTTP CONNECT"]}),
        ("syncopatic_synapse",      "Concept",    "Syncopatic Synapse Creation",{"pattern": "Actor Model", "lifecycle": "ephemeral"}),
        ("symbiotic_value_return",  "Concept",    "Symbiotic Value Return",     {"channels": ["RAG","corpus_entity","corpus_edge","skill_distillation"]}),
        ("iterative_unbounded_growth","Concept",  "Iteratively Unbounded Growth",{"flywheel": "acquire->inject->graph->grow"}),
        ("acquire_trigger",         "Protocol",   "acquire_*.trigger File Bus", {"format": "JSON", "poll_interval_s": 15}),
        ("bridge_state",            "Endpoint",   "Bridge State Folder",        {"path": "pipeline/bridge_state/"}),
        ("rag_documents",           "Endpoint",   "Proxy-Pointer-RAG Documents",{"path": "Proxy-Pointer-RAG/data/documents/"}),
        ("local_brain_sqlite",      "Endpoint",   "local_brain.sqlite",         {"tables": ["learning_log","corpus_entity","corpus_edge"]}),
        ("autocatalytic_vision_doc","Document",   "Autocatalytic Agent Networks Vision Doc", {"filename": "autocatalytic_agent_vision.md", "injected": now}),
    ]
    for eid, etype, label, props in entities:
        upsert_entity(cn, eid, etype, label, props)

    # ── EDGES ─────────────────────────────────────────────────────────────────
    edges = [
        # Brain owns the infrastructure capabilities
        ("brain",                   "System",     "piggyback_router",         "Capability", "USES",          1.0),
        ("brain",                   "System",     "integrated_skill_acquirer","Capability", "SPAWNS",        1.0),
        ("brain",                   "System",     "bridge_watcher",           "Capability", "USES",          0.9),
        # Capabilities implement concepts
        ("piggyback_router",        "Capability", "cross_tunnel_worming",     "Concept",    "IMPLEMENTS",    1.0),
        ("integrated_skill_acquirer","Capability","syncopatic_synapse",       "Concept",    "IMPLEMENTS",    1.0),
        ("integrated_skill_acquirer","Capability","iterative_unbounded_growth","Concept",   "DRIVES",        0.9),
        # Concepts relate to each other
        ("cross_tunnel_worming",    "Concept",    "syncopatic_synapse",       "Concept",    "ENABLES",       1.0),
        ("syncopatic_synapse",      "Concept",    "symbiotic_value_return",   "Concept",    "FEEDS",         1.0),
        ("symbiotic_value_return",  "Concept",    "iterative_unbounded_growth","Concept",   "DRIVES",        1.0),
        # Trigger bus connects Brain to Acquirer
        ("brain",                   "System",     "acquire_trigger",          "Protocol",   "WRITES_TO",     1.0),
        ("integrated_skill_acquirer","Capability","acquire_trigger",          "Protocol",   "READS_FROM",    1.0),
        # Return channels
        ("integrated_skill_acquirer","Capability","bridge_state",             "Endpoint",   "WRITES_TO",     1.0),
        ("integrated_skill_acquirer","Capability","rag_documents",            "Endpoint",   "WRITES_TO",     0.9),
        ("brain",                   "System",     "local_brain_sqlite",       "Endpoint",   "READS_FROM",    1.0),
        ("brain",                   "System",     "rag_documents",            "Endpoint",   "READS_FROM",    1.0),
        # Vision document
        ("brain",                   "System",     "autocatalytic_vision_doc", "Document",   "HAS_VISION_DOC",1.0),
    ]
    for src_id, src_type, dst_id, dst_type, rel, w in edges:
        upsert_edge(cn, src_id, src_type, dst_id, dst_type, rel, w)

    # ── LEARNING LOG ──────────────────────────────────────────────────────────
    learnings = [
        ("vision", "Cross-Tunnel Worming: Brain can reach external internet via SOCKS5/HTTP proxy through piggyback_router.py",
         {"capability": "piggyback_router", "ports": [1080, 3128]}, 0.95),
        ("vision", "Syncopatic Synapse Creation: Brain spawns ephemeral skill acquirer subprocesses via acquire_*.trigger files",
         {"capability": "integrated_skill_acquirer", "poll_s": 15, "schedule_h": 4}, 0.92),
        ("vision", "Symbiotic Value Return: Acquired knowledge returns to RAG corpus and corpus_entity before synapse dissolves",
         {"return_channels": ["RAG", "corpus_entity", "corpus_edge"]}, 0.90),
        ("vision", "Iteratively Unbounded Growth: Brain's trigger-based flywheel enables self-directed capability expansion",
         {"flywheel_steps": ["detect_gap","write_trigger","acquire","inject","graph","wider_vision"]}, 0.88),
        ("vision", "Vision Document injected: autocatalytic_agent_vision.md added to Proxy-Pointer-RAG knowledge base",
         {"filename": "autocatalytic_agent_vision.md", "injected_at": now}, 0.99),
        ("vision", "Bridge Watcher: Ports 1080 (SOCKS5) and 3128 (HTTP proxy) now firewalled open for external skill routing",
         {"bridge_watcher": "bridge_watcher.ps1", "ports_added": [1080, 3128]}, 0.87),
    ]
    for kind, title, detail, signal in learnings:
        log_learning(cn, kind, title, detail, signal)

    cn.commit()
    cn.close()

    count_entities = len(entities)
    count_edges    = len(edges)
    count_learnings= len(learnings)
    print(f"\n Vision Knowledge Injection Complete:")
    print(f"  Entities  : {count_entities}")
    print(f"  Edges     : {count_edges}")
    print(f"  Learnings : {count_learnings}")
    print(f"  DB        : {DB}")


if __name__ == "__main__":
    main()
