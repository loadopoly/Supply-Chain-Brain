"""Fiction Anthology Learner — cross-domain intelligence from Robert Aspirin's
Thieves World and MythAdventures universes.

The Brain expands the frontiers of the End State by treating great fictional
anthologies as cross-domain knowledge generators.  Aspirin's universes are
particularly rich sources:

    * Thieves World (Sanctuary) — multi-faction city-state economy, black
      markets, guild hierarchies, smuggling networks, trust-reputation systems,
      disruption effects on supply nodes, port-city trade dynamics.

    * MythAdventures (Skeeve & Aahz) — inter-dimensional commerce, asymmetric
      information, agent-principal problems, brand / reputation capital,
      scaling a small operation (MYTH Inc.), multi-vendor sourcing across
      "dimensions" (analogous to global supplier tiers).

The learner picks concept seeds from the anthology universe catalogue, calls
the LLM to generate a structured cross-domain analysis mapping the fictional
system to supply-chain / complex-systems intelligence, then persists the result
to ``learning_log`` (kind=``fiction_anthology``) and upserts graph entities
and edges into the corpus.

Public API:
    run_anthology_round() -> dict           # run one batch synchronously
    schedule_in_background(interval_s)     # daemon thread
"""
from __future__ import annotations

import json
import logging
import random
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .local_store import db_path as _local_db_path

log = logging.getLogger(__name__)

_PIPELINE_ROOT = Path(__file__).resolve().parents[2]
_LOCK = threading.Lock()
_LAST_RUN: float = 0.0

# How many concept seeds to expand per round.
_SEEDS_PER_ROUND = 3

# ---------------------------------------------------------------------------
# Universe seed catalogue
# ---------------------------------------------------------------------------

# Each seed is:
#   {
#     "universe":  "thieves_world" | "myth_adventures",
#     "concept":   short name,
#     "description": 1–3 sentence world-building fact,
#     "sc_bridge": hint at the supply-chain / systems analogy to pursue,
#   }

_SEEDS: list[dict] = [
    # ── Thieves World — Sanctuary ────────────────────────────────────────────
    {
        "universe": "thieves_world",
        "concept": "Sanctuary's Thieves Guild",
        "description": (
            "The Sanctuary Thieves Guild maintains a monopoly on theft and black-market "
            "trade through territory rights, tiered membership, and graduated tribute. "
            "Guild members pay a cut of proceeds upward through a multi-level hierarchy "
            "in exchange for legal protection, bail bonds, and access to the fence network."
        ),
        "sc_bridge": "guild as a supply-chain intermediary; tribute flows as tier-1/tier-2 cost structure; fence network as a reverse logistics channel",
    },
    {
        "universe": "thieves_world",
        "concept": "Sanctuary's Port and Trade Node",
        "description": (
            "Sanctuary is a decaying port city at the edge of the Rankan Empire, receiving "
            "goods from the sea, the Ilsigi interior, and Beysib merchant fleets. Its "
            "position makes it a transhipment hub where legal and contraband cargo mingle, "
            "customs enforcement is inconsistent, and dwell-time costs are hidden in bribes."
        ),
        "sc_bridge": "port as a multimodal hub; dwell-time variability; informal toll structures as friction cost",
    },
    {
        "universe": "thieves_world",
        "concept": "Rankan Imperial Supply Lines and Garrison Logistics",
        "description": (
            "The Rankan garrison in Sanctuary relies on supply lines running hundreds of "
            "miles through hostile territory. Procurement is centralized at the capital but "
            "last-mile delivery is ad-hoc, creating chronic shortages of strategic materials "
            "that the black market exploits."
        ),
        "sc_bridge": "centralized procurement vs distributed fulfillment; last-mile vulnerability; demand signaling failure across long lead times",
    },
    {
        "universe": "thieves_world",
        "concept": "Multi-Faction Information Asymmetry",
        "description": (
            "Sanctuary hosts five major power factions — Rankan Imperial, Ilsigi resistance "
            "(PFLS), Beysib merchant-nobility, the Mageguild, and the criminal underworld. "
            "Each faction operates its own intelligence network; information is a currency "
            "traded, withheld, and weaponized. Agents routinely receive contradictory "
            "market signals depending on their informant network."
        ),
        "sc_bridge": "multi-source signal conflict; Bayesian fusion of contradictory demand signals; information as competitive moat",
    },
    {
        "universe": "thieves_world",
        "concept": "Black Market Price Discovery",
        "description": (
            "Contraband prices in Sanctuary's Maze district emerge from repeated "
            "small-batch auctions run by the fence Hakiem and others. Prices incorporate "
            "risk premia for seizure probability, spoilage windows, and faction protection "
            "costs. Rapid price spikes signal enforcement crackdowns before official reports."
        ),
        "sc_bridge": "informal market as leading indicator; risk-adjusted pricing; enforcement as demand shock",
    },
    {
        "universe": "thieves_world",
        "concept": "Reputation Capital in the Underworld",
        "description": (
            "In Sanctuary, a criminal's reputation — built through witnessed deals, "
            "guild vouches, and survived confrontations — acts as collateral. High-reputation "
            "actors receive credit terms, access to premium contraband, and first-rights "
            "on new smuggling routes. Reputation loss triggers immediate supply cutoffs."
        ),
        "sc_bridge": "supplier reputation as credit instrument; social collateral in informal supply networks; trust-score decay modeling",
    },
    {
        "universe": "thieves_world",
        "concept": "Beysib Merchant Fleet and Exotic Sourcing",
        "description": (
            "The Beysib fleet introduces serpent-magic goods, rare dyes, and high-value "
            "low-volume cargo from across the sea. Their supply is irregular — driven by "
            "seasonal monsoon windows — and they maintain strict vendor exclusivity: "
            "they sell only to accredited Beysib House factors, bypassing the existing "
            "Sanctuary trade networks entirely."
        ),
        "sc_bridge": "exclusive supplier relationships; seasonal supply volatility; parallel supply channel disruption to incumbents",
    },
    {
        "universe": "thieves_world",
        "concept": "Sanctuary as a Resilience Stress Test",
        "description": (
            "Over the anthology's timeline, Sanctuary absorbs: imperial political "
            "transition, military occupation, plague, mage-war, a slaver revolt, and "
            "foreign fleet blockade — yet the city persists. Informal supply networks "
            "prove more resilient than official ones, rerouting around each disruption "
            "within days while formal channels take months to recover."
        ),
        "sc_bridge": "supply chain resilience metrics; informal network adaptation speed vs formal recovery time; disruption portfolio modeling",
    },
    {
        "universe": "thieves_world",
        "concept": "Stepsons and Military Procurement Corruption",
        "description": (
            "The Stepsons mercenary unit procures arms, horses, and provisions through "
            "a mix of official requisition and gray-market sourcing. Commissary officers "
            "skim margins at every tier; unit commanders tolerate this because reliability "
            "trumps unit cost. The equilibrium: stable supply at 20-30% above market, "
            "which persists until a new commander disrupts the patronage network."
        ),
        "sc_bridge": "procurement corruption equilibrium; total landed cost vs unit cost; patronage network stability analysis",
    },
    {
        "universe": "thieves_world",
        "concept": "Mageguild as a Knowledge Monopoly",
        "description": (
            "Sanctuary's Mageguild controls access to all licensed magical services, "
            "enforces a non-compete across its members, and prices services based on "
            "opaque internal assessments. Non-guild practitioners are coerced or eliminated. "
            "The guild accumulates reserves in rare components — wards, reagents, metals — "
            "that give it market power during crises."
        ),
        "sc_bridge": "knowledge monopoly pricing; strategic input stockpiling; regulatory capture in specialized service markets",
    },

    # ── MythAdventures — Skeeve & Aahz ──────────────────────────────────────
    {
        "universe": "myth_adventures",
        "concept": "MYTH Inc. as a Scaling Services Firm",
        "description": (
            "Skeeve and Aahz start as a two-person operation and grow MYTH Inc. into a "
            "multi-dimensional consulting firm by acquiring specialists (Tananda, Chumley, "
            "Guido, Nunzio) for specific capability gaps. Revenue comes from one-off "
            "high-margin engagements rather than recurring contracts; growth is capacity- "
            "constrained by partner availability."
        ),
        "sc_bridge": "capacity-based growth constraints; specialist sourcing vs in-house capability build; project-based vs retainer revenue models",
    },
    {
        "universe": "myth_adventures",
        "concept": "The Deveels as a Merchant Dimension",
        "description": (
            "The Deveels are interdimensional merchants who travel the Bazaar at Deva, "
            "the universe's most efficient market. They arbitrage price differentials across "
            "dimensions, maintain no fixed inventory (pure broker model), and extract value "
            "through information asymmetry about dimensional access. Their negotiating style "
            "is systematic adversarial anchoring."
        ),
        "sc_bridge": "pure broker vs inventory-holding distributor; arbitrage as supply-chain value creation; adversarial negotiation game theory",
    },
    {
        "universe": "myth_adventures",
        "concept": "The Bazaar at Deva",
        "description": (
            "The Bazaar at Deva is an open-air permanent market spanning a full dimension, "
            "with millions of stalls, no central authority, and self-enforcing reputation "
            "systems. Prices are set through rapid sequential negotiation; no fixed prices "
            "exist. Quality signaling is done through display, reputation, and vouching "
            "chains. Fraud is punished by instant reputation collapse."
        ),
        "sc_bridge": "decentralized market price formation; reputation-based quality signaling; fraud detection in informal markets; auction theory",
    },
    {
        "universe": "myth_adventures",
        "concept": "Aahz's Asymmetric Information Plays",
        "description": (
            "Aahz (Pervect from Perv) routinely exploits information gaps — knowing the "
            "value of an item to the buyer far exceeds what the buyer knows. Classic "
            "plays: feigning disinterest in high-value targets, creating false scarcity, "
            "and using third-party validators to establish credibility. His approach is "
            "systematically documented in his inner monologue as a teachable framework."
        ),
        "sc_bridge": "information asymmetry in procurement; buyer's BATNA manipulation; scarcity signaling; vendor negotiation counter-strategies",
    },
    {
        "universe": "myth_adventures",
        "concept": "The Magicians' Guild Protection Racket (Possiltum)",
        "description": (
            "The kingdom of Possiltum employs MYTH Inc. partly because the alternative — "
            "paying guild rates — costs 10x more per engagement. The guild extracts rents "
            "via licensing, territorial exclusivity, and deliberate obscurity of service "
            "pricing. MYTH Inc.'s disruptive entry demonstrates how transparency and fixed "
            "pricing can erode established rent-seeking middlemen."
        ),
        "sc_bridge": "incumbent rent extraction vs disruptive entrant pricing; transparency as competitive weapon; switching cost analysis",
    },
    {
        "universe": "myth_adventures",
        "concept": "Don Bruce and the Mob as a Supply Chain Enforcer",
        "description": (
            "The Mob (run by Don Bruce the Fairy Godfather) operates as a collection and "
            "enforcement layer across multiple dimensions. They guarantee contract fulfillment "
            "through credible threat of consequences — their value is not the service itself "
            "but the certainty of delivery. Buyers pay a premium for this certainty."
        ),
        "sc_bridge": "third-party contract enforcement; delivery certainty as premium service; tail-risk insurance in supply chains",
    },
    {
        "universe": "myth_adventures",
        "concept": "Dimensional Travel as Supply Chain Mode Selection",
        "description": (
            "Characters choose between D-hop (fast, high-cost, limited cargo), "
            "Dimension Door (medium speed, medium cost, bulk-capable), and walking the "
            "lines (slow, free, unlimited). The mode selection problem mirrors real freight: "
            "air vs ocean vs road trade-offs. Urgency and cargo value determine the "
            "optimal mode in every scene."
        ),
        "sc_bridge": "modal transport selection optimization; urgency × value × weight cost surface; air-sea-ground mode shift models",
    },
    {
        "universe": "myth_adventures",
        "concept": "Skeeve's Learning Curve as a Knowledge Acquisition Model",
        "description": (
            "Skeeve begins with zero magical ability and acquires skills through a mix of "
            "formal instruction (Garkin, Aahz), observed expert behavior, and high-stakes "
            "forced practice. His learning curve is non-linear: slow plateaus punctuated "
            "by crisis-driven exponential jumps. Each crisis is a forcing function that "
            "collapses latent capability into explicit skill."
        ),
        "sc_bridge": "non-linear organizational learning curves; crisis as capability accelerator; tacit-to-explicit knowledge conversion",
    },
    {
        "universe": "myth_adventures",
        "concept": "Tananda and Chumley: Specialist Subcontracting",
        "description": (
            "MYTH Inc. regularly subcontracts specialized work to Tananda (infiltration, "
            "seduction, extraction) and Chumley the Troll (intimidation, heavy enforcement). "
            "Both operate as independent contractors paid per-mission. MYTH Inc. earns a "
            "margin as the orchestrating intermediary, without owning the capability. "
            "This is pure outsourced capability procurement."
        ),
        "sc_bridge": "specialist subcontracting vs in-house build; margin capture as intermediary; capability-as-a-service sourcing",
    },
    {
        "universe": "myth_adventures",
        "concept": "The Dragon Gleep as an Unconventional Asset",
        "description": (
            "Gleep (the baby dragon) is an asset that has high maintenance cost, creates "
            "liability, and is widely seen as useless — yet repeatedly delivers unexpected "
            "strategic value through sensing, surprise intimidation, and unconventional "
            "problem solving. His ROI is unmeasurable by conventional metrics, leading to "
            "repeated calls to dispose of him that are always overridden by outcomes."
        ),
        "sc_bridge": "non-standard asset ROI measurement; option value of unconventional capabilities; liability vs strategic reserve framing",
    },
    {
        "universe": "myth_adventures",
        "concept": "Big Julie's Army: Demand Surge and Inventory Obsolescence",
        "description": (
            "Big Julie commands an army that has won every battle but has no remaining "
            "enemies, creating massive stranded capacity. His soldiers' skills are now "
            "inventory with zero demand. MYTH Inc. must re-purpose the capacity — "
            "retraining it for civilian applications — rather than dissolve it outright."
        ),
        "sc_bridge": "stranded capacity redeployment; workforce retooling as inventory reallocation; demand-gap driven organizational redesign",
    },
    {
        "universe": "myth_adventures",
        "concept": "The Perv Dimension as a High-Trust, High-Cost Supplier",
        "description": (
            "Pervects (Pervs) are feared across the dimensions for their ruthlessness and "
            "intelligence. Hiring one costs a premium, but delivery is guaranteed — a Perv "
            "never fails a contract they accept. Other dimensions use Pervects as a "
            "gold-standard reference check: if a Perv vouches for you, the transaction is "
            "risk-free."
        ),
        "sc_bridge": "premium reliable supplier vs low-cost unreliable alternative; supplier certification as risk hedge; vouching chains in B2B trust",
    },
    {
        "universe": "myth_adventures",
        "concept": "Klahd Dimension as the Low-Complexity Baseline",
        "description": (
            "Klah (Skeeve's home dimension) is technologically and magically primitive by "
            "interdimensional standards. Its goods are cheap, abundant, and considered low "
            "quality by sophisticated dimensions. Yet Klah exports raw materials and "
            "agricultural products that are scarce in higher-tech dimensions — the classic "
            "comparative advantage trade pattern."
        ),
        "sc_bridge": "comparative advantage in inter-regional sourcing; low-tech supplier as strategic raw material source; quality tier segmentation",
    },
    # ── Cross-universe synthesis seeds ──────────────────────────────────────
    {
        "universe": "cross_universe",
        "concept": "Informal Markets as Supply-Chain Resilience Mechanisms",
        "description": (
            "Both Sanctuary (Thieves World) and the Bazaar at Deva (MythAdventures) "
            "demonstrate that informal markets with no central governance consistently "
            "outperform formal regulated markets in speed of recovery from disruption. "
            "The common mechanism: reputation-based credit allows transactions to continue "
            "during formal system failure, while decentralized price discovery adjusts "
            "faster than any central authority."
        ),
        "sc_bridge": "informal network resilience theory; reputation credit as working capital substitute; decentralized vs centralized market recovery speed",
    },
    {
        "universe": "cross_universe",
        "concept": "Guild Monopolies as Rent-Extraction and Innovation Barriers",
        "description": (
            "Both the Sanctuary Mageguild and the Possiltum Magicians' Guild demonstrate "
            "the same guild monopoly dynamics: entry restriction, opaque pricing, and "
            "suppression of alternatives. Both are eventually disrupted by: (1) an outside "
            "entrant who ignores guild territory, and (2) a buyer who values certainty of "
            "outcome over guild legitimacy."
        ),
        "sc_bridge": "guild/cartel dynamics in supplier markets; disruption conditions; buyer-side demand for certainty over legitimacy",
    },
    {
        "universe": "cross_universe",
        "concept": "Multi-Tier Principal-Agent Problems Across Fantasy Hierarchies",
        "description": (
            "In both universes, agents are hired to solve problems but pursue sub-goals: "
            "Sanctuary's mercenaries skim procurement, MYTH Inc.'s contractors optimize "
            "per-engagement fees over client outcomes. The principal always faces the same "
            "problem: monitoring costs scale with distance (dimensional or territorial), "
            "so distant agents extract more rents. The solution in both universes is "
            "outcome-based contracting supplemented by reputation enforcement."
        ),
        "sc_bridge": "multi-tier principal-agent modeling; monitoring cost as function of supply chain distance; outcome-based vs input-based contracts",
    },
]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_db() -> str:
    return str(_local_db_path())


def _ensure_schema(cn: sqlite3.Connection) -> None:
    cn.executescript(
        """
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
        CREATE INDEX IF NOT EXISTS ix_learning_log_kind
            ON learning_log(kind, logged_at);
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
        CREATE TABLE IF NOT EXISTS brain_kv (
            key     TEXT PRIMARY KEY,
            value   TEXT NOT NULL
        );
        """
    )


def _upsert_entity(cn, *, entity_id: str, entity_type: str,
                   label: str = "", props: dict | None = None) -> None:
    if not entity_id:
        return
    now = datetime.now(timezone.utc).isoformat()
    row = cn.execute(
        "SELECT samples FROM corpus_entity WHERE entity_id=? AND entity_type=?",
        (entity_id, entity_type),
    ).fetchone()
    if row is None:
        cn.execute(
            """INSERT INTO corpus_entity(entity_id, entity_type, label, props_json,
               first_seen, last_seen, samples) VALUES (?,?,?,?,?,?,1)""",
            (entity_id, entity_type, label or entity_id,
             json.dumps(props or {}, default=str), now, now),
        )
    else:
        cn.execute(
            """UPDATE corpus_entity SET last_seen=?, samples=samples+1
               WHERE entity_id=? AND entity_type=?""",
            (now, entity_id, entity_type),
        )


def _upsert_edge(cn, *, src_id: str, src_type: str, dst_id: str,
                 dst_type: str, rel: str, weight: float = 0.85) -> None:
    if not src_id or not dst_id:
        return
    now = datetime.now(timezone.utc).isoformat()
    row = cn.execute(
        """SELECT weight FROM corpus_edge
           WHERE src_id=? AND src_type=? AND dst_id=? AND dst_type=? AND rel=?""",
        (src_id, src_type, dst_id, dst_type, rel),
    ).fetchone()
    if row is None:
        cn.execute(
            """INSERT INTO corpus_edge(src_id, src_type, dst_id, dst_type, rel,
               weight, last_seen, samples) VALUES(?,?,?,?,?,?,?,1)""",
            (src_id, src_type, dst_id, dst_type, rel, weight, now),
        )
    else:
        new_w = 0.7 * float(row[0]) + 0.3 * weight
        cn.execute(
            """UPDATE corpus_edge SET weight=?, last_seen=?, samples=samples+1
               WHERE src_id=? AND src_type=? AND dst_id=? AND dst_type=? AND rel=?""",
            (new_w, now, src_id, src_type, dst_id, dst_type, rel),
        )


def _cursor_key(name: str) -> str:
    return f"fiction_anthology_learner:{name}_cursor"


def _get_cursor(cn: sqlite3.Connection, name: str) -> set[str]:
    """Return set of already-processed concept IDs for deduplication."""
    row = cn.execute(
        "SELECT value FROM brain_kv WHERE key=?", (_cursor_key(name),)
    ).fetchone()
    if row:
        try:
            return set(json.loads(row[0]))
        except Exception:
            return set()
    return set()


def _save_cursor(cn: sqlite3.Connection, name: str, seen: set[str]) -> None:
    value = json.dumps(sorted(seen))
    cn.execute(
        "INSERT INTO brain_kv(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (_cursor_key(name), value),
    )


# ---------------------------------------------------------------------------
# LLM call helper
# ---------------------------------------------------------------------------

_LLM_TASK = "cross_domain_synthesis"
_ANTHOLOGY_SYSTEM_PROMPT = (
    "You are the Supply Chain Brain's Cross-Domain Intelligence module. "
    "Your role is to extract deep supply-chain and complex-systems intelligence "
    "from fictional world-building. You reason from first principles about "
    "economic structures, network dynamics, information flows, and resilience "
    "mechanisms — then map those structures onto real supply-chain theory. "
    "Be precise, structured, and focus on novel insight rather than surface metaphor."
)


def _call_llm(seed: dict) -> dict | None:
    """Call the LLM ensemble to generate a structured cross-domain analysis.

    Returns a dict with keys: summary, sc_concepts, entities, edges, signal_strength
    or None on failure.
    """
    prompt = (
        f"UNIVERSE: {seed['universe'].replace('_', ' ').title()}\n"
        f"CONCEPT: {seed['concept']}\n\n"
        f"WORLD-BUILDING FACT:\n{seed['description']}\n\n"
        f"SUPPLY-CHAIN BRIDGE HINT: {seed['sc_bridge']}\n\n"
        "Generate a structured cross-domain intelligence analysis in JSON with these keys:\n"
        "  summary: (2-3 sentences) the core supply-chain insight this fictional system reveals\n"
        "  sc_concepts: list of 3-6 supply-chain/systems-theory concept names this maps to\n"
        "  entities: list of objects, each {id: str, type: str, label: str} — "
        "entities to add to the knowledge graph (use underscore_ids, types like "
        "Concept/Mechanism/Structure/Actor/Market/Network)\n"
        "  edges: list of objects, each {src: str, dst: str, rel: str, weight: float 0-1} "
        "— typed relationships between those entities\n"
        "  signal_strength: float 0-1, how strongly this fictional system maps to real "
        "supply-chain dynamics (1.0 = near-perfect structural analog)\n\n"
        "Respond ONLY with valid JSON, no markdown fencing."
    )

    try:
        from .llm_ensemble import dispatch_parallel
        from .llm_caller_openrouter import openrouter_caller

        result = dispatch_parallel(
            _LLM_TASK,
            {"messages": [
                {"role": "system", "content": _ANTHOLOGY_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ]},
            caller=openrouter_caller,
        )
        raw = result.answer
        if isinstance(raw, str):
            # Strip possible markdown fences
            raw = raw.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.strip()
            parsed = json.loads(raw)
        elif isinstance(raw, dict):
            parsed = raw
        else:
            log.debug(f"fiction_anthology_learner: unexpected LLM response type {type(raw)}")
            return None
        return parsed
    except Exception as exc:
        log.debug(f"fiction_anthology_learner._call_llm: {exc}")
        return None


# ---------------------------------------------------------------------------
# Corpus persistence
# ---------------------------------------------------------------------------

def _persist_learning(cn: sqlite3.Connection, seed: dict, analysis: dict) -> int:
    """Write one learning_log row + corpus entities/edges.  Returns 1 on success."""
    now = datetime.now(timezone.utc).isoformat()
    concept_id = (seed["universe"] + ":" + seed["concept"]).lower().replace(" ", "_")
    title = f"[fiction_anthology] {seed['universe']} — {seed['concept']}"
    signal = float(analysis.get("signal_strength", 0.70))
    signal = max(0.0, min(1.0, signal))

    detail = json.dumps({
        "universe": seed["universe"],
        "concept": seed["concept"],
        "description": seed["description"],
        "sc_bridge": seed["sc_bridge"],
        "summary": analysis.get("summary", ""),
        "sc_concepts": analysis.get("sc_concepts", []),
        "entities": analysis.get("entities", []),
        "edges": analysis.get("edges", []),
    }, default=str)

    # Upsert learning_log
    existing = cn.execute(
        "SELECT id FROM learning_log WHERE kind='fiction_anthology' AND title=?",
        (title,),
    ).fetchone()
    if existing:
        # Refresh — update signal and detail
        cn.execute(
            "UPDATE learning_log SET logged_at=?, detail=?, signal_strength=? WHERE id=?",
            (now, detail, signal, existing[0]),
        )
    else:
        cn.execute(
            """INSERT INTO learning_log(logged_at, kind, title, detail, signal_strength)
               VALUES(?,?,?,?,?)""",
            (now, "fiction_anthology", title, detail, signal),
        )

    # Upsert primary concept entity
    _upsert_entity(
        cn,
        entity_id=concept_id,
        entity_type="Concept",
        label=seed["concept"],
        props={
            "universe": seed["universe"],
            "sc_bridge": seed["sc_bridge"],
            "summary": analysis.get("summary", ""),
        },
    )

    # Universe entity
    universe_id = f"universe:{seed['universe']}"
    _upsert_entity(
        cn, entity_id=universe_id, entity_type="FictionUniverse",
        label=seed["universe"].replace("_", " ").title(),
    )
    _upsert_edge(
        cn, src_id=universe_id, src_type="FictionUniverse",
        dst_id=concept_id, dst_type="Concept",
        rel="CONTAINS_CONCEPT", weight=1.0,
    )

    # SC concept entities + edges from analysis
    for sc_c in analysis.get("sc_concepts", []):
        if not sc_c:
            continue
        sc_id = "sc_concept:" + sc_c.lower().replace(" ", "_").replace("/", "_")
        _upsert_entity(cn, entity_id=sc_id, entity_type="SCConcept", label=sc_c)
        _upsert_edge(
            cn, src_id=concept_id, src_type="Concept",
            dst_id=sc_id, dst_type="SCConcept",
            rel="MAPS_TO_SC", weight=signal,
        )

    # Extra entities from LLM analysis
    entity_map: dict[str, tuple[str, str]] = {}  # id → (entity_id, entity_type)
    for ent in analysis.get("entities", []):
        if not isinstance(ent, dict):
            continue
        eid = str(ent.get("id") or "").strip()
        etype = str(ent.get("type") or "Concept").strip()
        elabel = str(ent.get("label") or eid).strip()
        if not eid:
            continue
        full_eid = f"fiction:{eid}"
        entity_map[eid] = (full_eid, etype)
        _upsert_entity(cn, entity_id=full_eid, entity_type=etype, label=elabel)
        # Link entity to the parent concept
        _upsert_edge(
            cn, src_id=concept_id, src_type="Concept",
            dst_id=full_eid, dst_type=etype,
            rel="HAS_ELEMENT", weight=signal,
        )

    # Extra edges from LLM analysis
    for edge in analysis.get("edges", []):
        if not isinstance(edge, dict):
            continue
        src = str(edge.get("src") or "")
        dst = str(edge.get("dst") or "")
        rel = str(edge.get("rel") or "RELATES_TO")
        w = float(edge.get("weight", signal))
        if not src or not dst:
            continue
        src_full, src_type = entity_map.get(src, (f"fiction:{src}", "Concept"))
        dst_full, dst_type = entity_map.get(dst, (f"fiction:{dst}", "Concept"))
        _upsert_edge(
            cn, src_id=src_full, src_type=src_type,
            dst_id=dst_full, dst_type=dst_type,
            rel=rel, weight=w,
        )

    return 1


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_anthology_round(seeds_per_round: int = _SEEDS_PER_ROUND) -> dict:
    """Pick seeds, call LLM, persist learnings.  Returns a stats dict."""
    stats = {"processed": 0, "written": 0, "skipped": 0, "errors": 0, "titles": []}

    db = _get_db()
    try:
        with sqlite3.connect(db, timeout=20) as cn:
            cn.row_factory = sqlite3.Row
            _ensure_schema(cn)
            seen = _get_cursor(cn, "concepts")

            # Prefer unseen seeds; after full rotation fall back to re-expansion
            unseen = [s for s in _SEEDS if
                      (s["universe"] + ":" + s["concept"]).lower().replace(" ", "_") not in seen]
            pool = unseen if unseen else _SEEDS
            batch = random.sample(pool, min(seeds_per_round, len(pool)))

            for seed in batch:
                concept_id = (seed["universe"] + ":" + seed["concept"]).lower().replace(" ", "_")
                stats["processed"] += 1

                log.info(
                    f"[fiction_anthology] Expanding: [{seed['universe']}] {seed['concept']}"
                )

                analysis = _call_llm(seed)
                if analysis is None:
                    log.warning(
                        f"[fiction_anthology] LLM returned nothing for: {seed['concept']}"
                    )
                    stats["errors"] += 1
                    # Still write a basic entry without LLM enrichment
                    analysis = {
                        "summary": f"Cross-domain analog: {seed['sc_bridge']}",
                        "sc_concepts": [],
                        "entities": [],
                        "edges": [],
                        "signal_strength": 0.55,
                    }

                n = _persist_learning(cn, seed, analysis)
                stats["written"] += n
                seen.add(concept_id)
                stats["titles"].append(
                    f"[{seed['universe']}] {seed['concept']} → sig={analysis.get('signal_strength',0.55):.2f}"
                )

            _save_cursor(cn, "concepts", seen)
            cn.commit()

    except Exception as exc:
        log.error(f"fiction_anthology_learner.run_anthology_round: {exc}")
        stats["errors"] += 1

    log.info(
        f"[fiction_anthology] Round complete: "
        f"processed={stats['processed']} written={stats['written']} errors={stats['errors']}"
    )
    return stats


# ---------------------------------------------------------------------------
# Background scheduler
# ---------------------------------------------------------------------------

_BG_LOCK = threading.Lock()
_BG_THREAD: threading.Thread | None = None


def schedule_in_background(interval_s: int = 2700) -> threading.Thread:
    """Start the anthology learner on a daemon thread.

    Default cadence: every 45 minutes (2700 s) — slower than OCW / arxiv
    because each seed requires an LLM call.  The interval is enforced inside
    the loop so it respects whatever cadence the agent is running at.
    """
    global _BG_THREAD
    with _BG_LOCK:
        if _BG_THREAD and _BG_THREAD.is_alive():
            return _BG_THREAD

        def _loop():
            global _LAST_RUN
            log.info(
                f"[fiction_anthology] Background learner started "
                f"(interval={interval_s}s, seeds_per_round={_SEEDS_PER_ROUND}, "
                f"universe_seeds={len(_SEEDS)})"
            )
            while True:
                try:
                    stats = run_anthology_round()
                    _LAST_RUN = time.time()
                    for title in stats.get("titles", []):
                        log.info(f"  ✓ {title}")
                except Exception as exc:
                    log.error(f"[fiction_anthology] loop error: {exc}")
                time.sleep(interval_s)

        t = threading.Thread(
            target=_loop,
            name="fiction-anthology-learner",
            daemon=True,
        )
        t.start()
        _BG_THREAD = t
        return t
