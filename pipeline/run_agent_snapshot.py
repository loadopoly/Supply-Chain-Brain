"""
run_agent_snapshot.py
Trigger one full autonomous agent round and emit a timestamped neural-mapping
log of everything the Brain observed, learned, and wired into the corpus graph.
"""
from __future__ import annotations
import sqlite3, json, time, sys, traceback
from datetime import datetime, timezone
from pathlib import Path

DB = Path(__file__).parent / "local_brain.sqlite"
LOG = Path(__file__).parent / "logs" / "agent_neural_map.md"
LOG.parent.mkdir(exist_ok=True)

def ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3] + "Z"

def conn():
    cn = sqlite3.connect(DB, timeout=30)
    cn.row_factory = sqlite3.Row
    return cn

# ── Pre-run snapshot ──────────────────────────────────────────────────────────
print(f"[{ts()}] === PRE-RUN SNAPSHOT ===")
with conn() as cn:
    kv = {r["key"]: r["value"] for r in cn.execute("SELECT key, value FROM brain_kv")}
    entity_counts = {r["entity_type"]: r["n"] for r in cn.execute(
        "SELECT entity_type, COUNT(*) as n FROM corpus_entity GROUP BY entity_type")}
    edge_counts = {r["rel"]: r["n"] for r in cn.execute(
        "SELECT rel, COUNT(*) as n FROM corpus_edge GROUP BY rel")}
    total_edges_before = sum(edge_counts.values())
    total_entities_before = sum(entity_counts.values())

    try:
        topo_before = cn.execute("SELECT COUNT(*) FROM network_topology").fetchone()[0]
    except Exception:
        topo_before = 0
    try:
        obs_before = cn.execute("SELECT COUNT(*) FROM network_observations").fetchone()[0]
    except Exception:
        obs_before = 0

print(f"  Entities: {total_entities_before}")
print(f"  Edges   : {total_edges_before}")
print(f"  Topo rows: {topo_before} | Obs rows: {obs_before}")
print(f"  Last corpus refresh: {kv.get('corpus_refresh_last','—')}")
print(f"  Synapse builder:     {kv.get('synapse_builder_last','—')}")
print(f"  Synapse vision:      {kv.get('synapse_vision_last','—')}")

# ── Trigger a manual corpus refresh round ─────────────────────────────────────
print(f"\n[{ts()}] === RUNNING corpus refresh ===")
t0 = time.perf_counter()
try:
    sys.path.insert(0, str(Path(__file__).parent))
    from src.brain.knowledge_corpus import refresh_corpus_round
    result = refresh_corpus_round()
    corpus_elapsed = time.perf_counter() - t0
    print(f"[{ts()}] Corpus refresh done in {corpus_elapsed:.1f}s")
    print(f"  result keys: {list(result.keys())}")
    for k, v in result.items():
        print(f"    {k}: {v}")
except Exception as exc:
    corpus_elapsed = time.perf_counter() - t0
    print(f"[{ts()}] Corpus refresh ERROR ({corpus_elapsed:.1f}s): {exc}")
    traceback.print_exc()
    result = {}

# ── Trigger network vision round (bridge + topology probe) ────────────────────
print(f"\n[{ts()}] === RUNNING network_learner observation ===")
t1 = time.perf_counter()
vision_result = {}
try:
    from src.brain.network_learner import observe_network_round, init_schema
    init_schema()
    vision_result = observe_network_round()
    net_elapsed = time.perf_counter() - t1
    print(f"[{ts()}] Network observation done in {net_elapsed:.1f}s")
    for k, v in vision_result.items():
        print(f"    {k}: {v}")
except Exception as exc:
    net_elapsed = time.perf_counter() - t1
    print(f"[{ts()}] Network observation ERROR ({net_elapsed:.1f}s): {exc}")
    traceback.print_exc()

# ── Bridge RDP probe ──────────────────────────────────────────────────────────
print(f"\n[{ts()}] === RUNNING bridge_rdp probe ===")
t2 = time.perf_counter()
bridge_results = {}
bridge_targets = []
try:
    import bridge_rdp  # type: ignore[import]
    bridge_targets = bridge_rdp.list_targets()
    bridge_results = bridge_rdp.probe_all()
    bridge_elapsed = time.perf_counter() - t2
    print(f"[{ts()}] Bridge probe done in {bridge_elapsed:.1f}s")
    for name, alive in bridge_results.items():
        status = "LIVE" if alive else ("DOWN" if alive is False else "SKIP")
        print(f"    {name}: {status}")
except Exception as exc:
    bridge_elapsed = time.perf_counter() - t2
    print(f"[{ts()}] Bridge probe ERROR ({bridge_elapsed:.1f}s): {exc}")

# ── RAG deepdive round ────────────────────────────────────────────────────────
print(f"\n[{ts()}] === RUNNING RAG knowledge deepdive ===")
t3 = time.perf_counter()
rag_result = {}
try:
    from src.brain.knowledge_corpus import materialize_into_graph
    rag_result = materialize_into_graph(max_entities=15000)
    rag_elapsed = time.perf_counter() - t3
    print(f"[{ts()}] RAG deepdive done in {rag_elapsed:.1f}s")
    for k, v in rag_result.items():
        print(f"    {k}: {v}")
except Exception as exc:
    rag_elapsed = time.perf_counter() - t3
    print(f"[{ts()}] RAG deepdive ERROR ({rag_elapsed:.1f}s): {exc}")
    traceback.print_exc()

# ── Post-run snapshot ─────────────────────────────────────────────────────────
print(f"\n[{ts()}] === POST-RUN SNAPSHOT ===")
with conn() as cn:
    entity_counts_after = {r["entity_type"]: r["n"] for r in cn.execute(
        "SELECT entity_type, COUNT(*) as n FROM corpus_entity GROUP BY entity_type ORDER BY n DESC")}
    edge_counts_after = {r["rel"]: r["n"] for r in cn.execute(
        "SELECT rel, COUNT(*) as n FROM corpus_edge GROUP BY rel ORDER BY n DESC")}
    total_edges_after   = sum(edge_counts_after.values())
    total_entities_after = sum(entity_counts_after.values())

    try:
        topo_after = cn.execute("SELECT COUNT(*) FROM network_topology").fetchone()[0]
    except Exception:
        topo_after = 0
    try:
        obs_after = cn.execute("SELECT COUNT(*) FROM network_observations").fetchone()[0]
    except Exception:
        obs_after = 0

    kv_after = {r["key"]: r["value"] for r in cn.execute("SELECT key, value FROM brain_kv")}

    # Recent RAG edges
    recent_rag = cn.execute("""
        SELECT src_type, src_id, rel, dst_type, dst_id, weight
        FROM corpus_edge
        WHERE rel='RAG_INFERRED'
        ORDER BY last_seen DESC LIMIT 20
    """).fetchall()

    # Structural: entity types with most connectivity
    hub_entities = cn.execute("""
        SELECT e.entity_type, e.entity_id, e.label,
               COUNT(DISTINCT ed.dst_id) as out_deg,
               COUNT(DISTINCT ei.src_id) as in_deg,
               e.samples
        FROM corpus_entity e
        LEFT JOIN corpus_edge ed ON ed.src_id=e.entity_id
        LEFT JOIN corpus_edge ei ON ei.dst_id=e.entity_id
        GROUP BY e.entity_id
        ORDER BY (out_deg+in_deg) DESC
        LIMIT 20
    """).fetchall()

    # Network topology summary
    try:
        topo_rows = cn.execute("""
            SELECT host, protocol, port, capability, ema_success, ema_latency_ms, source
            FROM network_topology ORDER BY ema_success DESC LIMIT 30
        """).fetchall()
    except Exception:
        topo_rows = []

    # Endpoint entities now in corpus
    endpoints = cn.execute("""
        SELECT entity_id, label, props_json FROM corpus_entity
        WHERE entity_type='Endpoint' ORDER BY last_seen DESC
    """).fetchall()

    # LLM weight table — the Brain's learned model preferences
    llm_weights = cn.execute("""
        SELECT task, model_id, weight, bias, n_obs, ema_success, ema_latency, updated_at
        FROM llm_weights ORDER BY task, weight DESC
    """).fetchall()

    # Recent self-train log — gradient updates
    self_train = cn.execute("""
        SELECT ran_at, task, samples, matched, avg_validator, drift_capped, notes
        FROM llm_self_train_log ORDER BY ran_at DESC LIMIT 10
    """).fetchall()

    # Recent learnings
    recent_learnings = cn.execute("""
        SELECT logged_at, kind, title, signal_strength, source_table
        FROM learning_log ORDER BY logged_at DESC LIMIT 20
    """).fetchall()

    # Corpus round log
    round_log = cn.execute("""
        SELECT ran_at, entities_added, entities_touched, edges_added, edges_touched,
               learnings_logged, notes
        FROM corpus_round_log ORDER BY ran_at DESC LIMIT 5
    """).fetchall()

    # Body directives (active action signals)
    body_directives = cn.execute("""
        SELECT title, priority, value_per_year, target_entity, status, created_at
        FROM body_directives WHERE status != 'dismissed' ORDER BY priority ASC LIMIT 15
    """).fetchall()

print(f"  Δ Entities: {total_entities_before} → {total_entities_after} (+{total_entities_after - total_entities_before})")
print(f"  Δ Edges   : {total_edges_before} → {total_edges_after} (+{total_edges_after - total_edges_before})")
print(f"  Δ Topo    : {topo_before} → {topo_after} (+{topo_after - topo_before})")
print(f"  Δ Obs     : {obs_before} → {obs_after} (+{obs_after - obs_before})")

# ── Write the neural-map markdown log ─────────────────────────────────────────
run_ts = datetime.now(timezone.utc).isoformat()
total_elapsed = time.perf_counter() - t0

with open(LOG, "w", encoding="utf-8") as f:
    f.write(f"# Brain Neural Map — Autonomous Agent Run\n\n")
    f.write(f"**Run timestamp:** `{run_ts}`  \n")
    f.write(f"**Total elapsed:** `{total_elapsed:.1f}s`  \n")
    f.write(f"**Version:** `v0.14.9`\n\n")

    f.write("---\n\n## 1. Entity Graph (Nodes)\n\n")
    f.write("| Entity Type | Count | Δ |\n|---|---|---|\n")
    for etype, n in sorted(entity_counts_after.items(), key=lambda x: -x[1]):
        before = entity_counts.get(etype, 0)
        delta = f"+{n-before}" if n > before else ("" if n == before else str(n-before))
        f.write(f"| {etype} | {n} | {delta} |\n")
    f.write(f"\n**Total entities:** {total_entities_after}  (Δ +{total_entities_after-total_entities_before})\n\n")

    f.write("---\n\n## 2. Edge Graph (Relationships)\n\n")
    f.write("| Relationship | Count | Δ |\n|---|---|---|\n")
    for rel, n in sorted(edge_counts_after.items(), key=lambda x: -x[1]):
        before = edge_counts.get(rel, 0)
        delta = f"+{n-before}" if n > before else ("" if n == before else str(n-before))
        f.write(f"| {rel} | {n} | {delta} |\n")
    f.write(f"\n**Total edges:** {total_edges_after}  (Δ +{total_edges_after-total_edges_before})\n\n")

    f.write("---\n\n## 3. Hub Entities (Highest Connectivity — Top 20)\n\n")
    f.write("| Type | ID | Label | Out° | In° | Samples |\n|---|---|---|---|---|---|\n")
    for r in hub_entities:
        label = (r["label"] or "")[:40]
        eid   = (r["entity_id"] or "")[:35]
        f.write(f"| {r['entity_type']} | `{eid}` | {label} | {r['out_deg']} | {r['in_deg']} | {r['samples']} |\n")

    f.write("\n---\n\n## 4. Network Vision (Live Topology)\n\n")
    f.write("### 4a. Bridge RDP Probe Results\n\n")
    if bridge_results:
        f.write("| Bridge | Host | Port | Status |\n|---|---|---|---|\n")
        for t in bridge_targets:
            name  = t.get("name","")
            host  = t.get("target_host","")
            port  = t.get("target_port","")
            alive = bridge_results.get(name)
            status = "🟢 LIVE" if alive else ("🔴 DOWN" if alive is False else "⚪ SKIP")
            f.write(f"| {name} | {host} | {port} | {status} |\n")
    else:
        f.write("_No bridge_rdp module or no targets configured._\n")

    f.write("\n### 4b. Network Topology (EMA-ranked)\n\n")
    if topo_rows:
        f.write("| Host | Protocol | Port | Capability | EMA Success | EMA Latency ms | Source |\n")
        f.write("|---|---|---|---|---|---|---|\n")
        for r in topo_rows:
            ema_s = f"{float(r['ema_success']):.2f}" if r['ema_success'] is not None else "—"
            ema_l = f"{float(r['ema_latency_ms']):.0f}" if r['ema_latency_ms'] is not None else "—"
            f.write(f"| {r['host']} | {r['protocol']} | {r['port']} | {r['capability']} | {ema_s} | {ema_l} | {r['source']} |\n")
    else:
        f.write("_No topology rows yet (first run or network_learner not yet executed)._\n")

    f.write("\n### 4c. Endpoint Entities in Corpus\n\n")
    if endpoints:
        f.write("| Entity ID | Label | Props |\n|---|---|---|\n")
        for r in endpoints:
            props = json.loads(r["props_json"] or "{}")
            props_str = ", ".join(f"{k}={v}" for k,v in list(props.items())[:4])
            f.write(f"| `{r['entity_id']}` | {r['label']} | {props_str} |\n")
    else:
        f.write("_No Endpoint entities yet._\n")

    f.write("\n---\n\n## 5. RAG Deepdive — What the Brain Inferred\n\n")
    f.write(f"**Deepdive result:** `{rag_result}`\n\n")
    f.write("### Most Recent RAG_INFERRED Edges (last 20)\n\n")
    if recent_rag:
        f.write("| Src Type | Src ID | Rel | Dst Type | Dst ID | Weight |\n|---|---|---|---|---|---|\n")
        for r in recent_rag:
            f.write(f"| {r['src_type']} | `{(r['src_id'] or '')[:30]}` | {r['rel']} | {r['dst_type']} | `{(r['dst_id'] or '')[:30]}` | {r['weight']:.3f} |\n")
    else:
        f.write("_No RAG_INFERRED edges found._\n")

    f.write("\n---\n\n## 6. Network Vision Worker Output\n\n")
    f.write(f"**Observation result:** `{vision_result}`\n\n")
    f.write(f"- Live endpoints: `{vision_result.get('live', '—')}`\n")
    f.write(f"- Down endpoints: `{vision_result.get('down', '—')}`\n")
    f.write(f"- Promoted compute peers: `{vision_result.get('promoted', '—')}`\n")
    f.write(f"- By protocol: `{vision_result.get('by_protocol', {})}`\n")
    f.write(f"- By source: `{vision_result.get('by_source', {})}`\n\n")

    f.write("---\n\n## 7. KV Store Heartbeats (After Run)\n\n")
    f.write("| Key | Value |\n|---|---|\n")
    for k, v in sorted(kv_after.items()):
        v_short = (str(v)[:120] + "…") if len(str(v)) > 120 else str(v)
        f.write(f"| `{k}` | {v_short} |\n")

    f.write("\n---\n\n## 8. Corpus Refresh Stats\n\n")
    f.write(f"**Corpus round result:** `{result}`\n\n")
    f.write(f"- Elapsed: `{corpus_elapsed:.1f}s`\n\n")
    f.write("### Corpus Round Log (last 5 rounds)\n\n")
    if round_log:
        f.write("| Ran At | Entities Added | Edges Added | Learnings | Notes |\n|---|---|---|---|---|\n")
        for r in round_log:
            notes = (r["notes"] or "")[:60]
            f.write(f"| {r['ran_at'][:19]} | {r['entities_added']} | {r['edges_added']} | {r['learnings_logged']} | {notes} |\n")

    f.write("\n---\n\n## 9. LLM Neural Weights (Brain's Learned Model Preferences)\n\n")
    f.write("The Brain continuously trains a weight matrix across models per task — "
            "this is the closest analogue to synaptic weight in its LLM structure.\n\n")
    if llm_weights:
        f.write("| Task | Model ID | Weight | Bias | N Obs | EMA Success | EMA Latency ms |\n")
        f.write("|---|---|---|---|---|---|---|\n")
        for r in llm_weights:
            ema_s = f"{float(r['ema_success']):.3f}" if r["ema_success"] is not None else "—"
            ema_l = f"{float(r['ema_latency']):.0f}" if r["ema_latency"] is not None else "—"
            f.write(f"| {r['task']} | `{(r['model_id'] or '')[:35]}` | {r['weight']:.4f} | {r['bias']:.4f} | {r['n_obs']} | {ema_s} | {ema_l} |\n")
    else:
        f.write("_No LLM weights trained yet._\n")

    f.write("\n### Self-Train Log (last 10 gradient updates)\n\n")
    if self_train:
        f.write("| Ran At | Task | Samples | Matched | Avg Validator | Drift Capped |\n|---|---|---|---|---|---|\n")
        for r in self_train:
            avg_v = r['avg_validator']
            avg_str = f"{float(avg_v):.3f}" if avg_v is not None else "—"
            f.write(f"| {r['ran_at'][:19]} | {r['task']} | {r['samples']} | {r['matched']} | {avg_str} | {r['drift_capped']} |\n")
    else:
        f.write("_No self-train log entries._\n")

    f.write("\n---\n\n## 10. Recent Learnings (last 20)\n\n")
    if recent_learnings:
        f.write("| Logged At | Kind | Title | Signal | Source |\n|---|---|---|---|---|\n")
        for r in recent_learnings:
            title = (r["title"] or "")[:55]
            f.write(f"| {r['logged_at'][:19]} | {r['kind']} | {title} | {r['signal_strength']:.2f} | {r['source_table']} |\n")
    else:
        f.write("_No learning log entries._\n")

    f.write("\n---\n\n## 11. Body Directives (Active Action Signals)\n\n")
    if body_directives:
        f.write("| Priority | Title | Value $/yr | Target | Status |\n|---|---|---|---|---|\n")
        for r in body_directives:
            title = (r["title"] or "")[:50]
            val = f"${r['value_per_year']:,.0f}" if r["value_per_year"] else "—"
            f.write(f"| {r['priority']} | {title} | {val} | {r['target_entity'] or '—'} | {r['status']} |\n")
    else:
        f.write("_No active directives._\n")

    f.write("\n---\n\n## 12. Integrated Neural Structure Summary\n\n")
    f.write("```\n")
    f.write("SUPPLY CHAIN BRAIN — Neural Graph Topology\n")
    f.write("=" * 60 + "\n\n")
    f.write(f"  Graph nodes (corpus_entity): {total_entities_after:>7,}\n")
    f.write(f"  Graph edges (corpus_edge)  : {total_edges_after:>7,}\n")
    f.write(f"  Network topo rows          : {topo_after:>7,}\n")
    f.write(f"  Network obs rows           : {obs_after:>7,}\n\n")

    f.write("  Node type distribution:\n")
    for etype, n in sorted(entity_counts_after.items(), key=lambda x: -x[1]):
        bar = "█" * min(40, max(1, int(n / max(total_entities_after,1) * 80)))
        f.write(f"    {etype:<22} {n:>6}  {bar}\n")

    f.write("\n  Edge type distribution:\n")
    for rel, n in sorted(edge_counts_after.items(), key=lambda x: -x[1]):
        bar = "█" * min(40, max(1, int(n / max(total_edges_after,1) * 80)))
        f.write(f"    {rel:<22} {n:>6}  {bar}\n")

    f.write("\n  Dominant hubs (highest degree):\n")
    for r in hub_entities[:8]:
        f.write(f"    [{r['entity_type']:>16}] {(r['label'] or r['entity_id'] or '')[:40]:<42}  deg={r['out_deg']+r['in_deg']}\n")

    f.write("\n  OCW semantic bridge:\n")
    f.write("    AcademicTopic (13) ──INFORMS──► Quest/Task (10)\n")
    f.write("    OCWCourse     (93) ──INFORMS──► Quest/Task (10)\n")
    f.write("    Path: MIT knowledge → SC operational objectives\n\n")

    f.write("  Network Vision path (RDP + piggyback):\n")
    live_ct = sum(1 for v in bridge_results.values() if v)
    down_ct = sum(1 for v in bridge_results.values() if v is False)
    f.write(f"    Bridges probed: {len(bridge_results)}  live={live_ct}  down={down_ct}\n")
    f.write(f"    Endpoint entities in corpus: {len(endpoints)}\n")
    f.write(f"    Topology nodes in DB       : {topo_after}\n\n")

    f.write("  Learning cycle:\n")
    f.write("    corpus refresh   → ingest SC transactions + OCW courses + bridge obs\n")
    f.write("    RAG deepdive     → structural-hole traversal, new RAG_INFERRED edges\n")
    f.write("    network_learner  → EMA-probe all known endpoints → topology table\n")
    f.write("    vision_worker    → promote topo into corpus Endpoint entities\n")
    f.write("    synaptic workers → builder|lookahead|sweeper|convergence|vision threads\n")
    f.write("```\n")

    f.write(f"\n---\n\n_Auto-generated by `run_agent_snapshot.py` at `{run_ts}`  \n")
    f.write(f"Run in `{total_elapsed:.1f}s` — Supply Chain Brain v0.14.9_\n")

print(f"\n[{ts()}] === COMPLETE ===")
print(f"  Total run time: {total_elapsed:.1f}s")
print(f"  Log written to: {LOG}")
print(f"  Δ Entities: +{total_entities_after-total_entities_before}")
print(f"  Δ Edges   : +{total_edges_after-total_edges_before}")
print(f"  Δ Topo    : +{topo_after-topo_before}")
print(f"  Δ Obs     : +{obs_after-obs_before}")
