# Corpus Expansion via Network Reach Protocols and Multidirectional Graph Expansion

## Overview

A knowledge corpus that does not actively expand is a static archive. The Supply Chain Brain's corpus expansion architecture treats every discovered knowledge node as a portal to adjacent knowledge — following hyperlinks, related-course edges, citation chains, and external references to grow the corpus in all directions simultaneously.

The foundational principle is **autocatalytic graph traversal**: each new node discovered feeds back into the discovery process by contributing its own edges to the traversal frontier.

---

## Core Concepts

### 1. Multidirectional Graph Expansion (MGE)

Traditional RAG systems ingest documents then stop. MGE treats the document corpus as a **live graph** where:

- **Nodes** = knowledge units (papers, courses, datasets, external pages)
- **Edges** = hyperlinks, citations, topic tags, related-course pointers, author co-authorship, keyword co-occurrence
- **Expansion** = BFS/DFS traversal from seed nodes, following edges to discover new nodes

A single MIT OCW course page can carry 25–50 outbound edges: lecture notes, related courses from adjacent departments, external journal links, GitHub repositories, university lecture archives. Each of those is a node. Each of those nodes carries its own edges.

**Bounded BFS** (breadth-first search with hop-depth and fan-out limits) is the preferred traversal for corpus expansion because:
- It prevents combinatorial explosion
- It ensures topological breadth before depth (covering more distinct topics)
- It is restartable — any node can be a new seed

### 2. Network Reach Protocols

Network reach protocols define *how* the graph traversal reaches beyond the local corpus boundary into external knowledge domains:

#### a) HTTP Link-Following (Structural Crawl)
Parse every HTML page for `<a href>` tags. Categorize links as:
- **Internal resources** — same-domain subpages (lecture notes, syllabi, assignments)
- **Related nodes** — same-domain sibling pages (related courses on OCW)
- **External references** — off-domain links to journals, repositories, university pages

This is the OCW deep-fetch protocol: `fetch_ocw_course_detail(slug)` → 50+ categorized links per course.

#### b) Citation Graph Traversal
Academic papers expose citation networks via:
- **OpenAlex** (`cited_by_count`, concept co-occurrence)
- **CrossRef** DOI resolution chains
- **arXiv** `related_articles` and author networks

Citation traversal follows the "upstream" direction (papers cited by a paper) and "downstream" direction (papers that cite a paper), creating bidirectional knowledge paths.

#### c) Keyword / Topic Tag Propagation
Each discovered node carries topic tags. Those tags become new search queries for the next discovery cycle. A paper on "inventory optimization deep learning" tags its concepts; those concepts seed queries to arXiv, OpenAlex, and OCW, pulling in adjacent papers the original query would never have reached.

This is **semantic link propagation** — the corpus expands along the semantic graph, not just the hyperlink graph.

#### d) Sitemap-Seeded Scoring
Sitemaps expose all URL slugs for a domain in bulk. Score every slug against a query by token overlap. This gives O(1) full-catalogue access without needing to crawl each page. Used by the OCW sitemap scorer to rank 2500+ courses instantly.

### 3. Autocatalytic Feedback Loop

The corpus expansion process is autocatalytic because:

1. A research cycle discovers N OCW courses (via sitemap scoring)
2. Each course is deep-fetched, yielding M related courses and K external links
3. Related courses become seeds for the next deep-fetch cycle
4. External links (journals, repos, university pages) become seeds for the web-scrape pipeline
5. Papers discovered via arXiv/OpenAlex produce topic tags that seed new OCW queries
6. Each cycle's output is input for the next cycle — the corpus grows faster than linearly

This matches the theoretical behavior of an **autocatalytic network**: the rate of new knowledge discovery is proportional to the current size of the knowledge graph.

### 4. Syncopatic Synapse Creation

When two previously-disconnected knowledge nodes are linked by a traversal:
- A new corpus edge is created (stored as `ocw_resource` or cross-reference in `learning_log`)
- This edge can be traversed by RAG retrieval, connecting answers that span both nodes
- Over time, dense edge clusters form around high-value concept areas (supply chain, systems engineering, operations research)

The density of these clusters mirrors the concept of **synaptic plasticity** in neural systems: frequently co-accessed nodes develop stronger connections (higher `signal_strength` values), while rarely accessed nodes fade.

---

## Architecture Implementation

### Layer 1: Discovery (Broad Surface)
- arXiv API: unlimited preprints, daily updates
- OpenAlex API: 200M+ scholarly works
- CrossRef API: 130M+ DOI-indexed papers
- CORE API: 300M+ open-access full-text links
- Zenodo API: 5,000+ supply-chain datasets
- MIT OCW sitemap: 2,500+ courses, 24-hour cache
- NASA NTRS: 100K+ systems engineering reports

### Layer 2: Deep-Fetch (Link Lattice Absorption)
- `fetch_ocw_course_detail(slug)` — harvests 50+ categorized links per OCW course
- Categorizes: internal resources, related courses, external references
- Stdlib-only HTML parsing (`html.parser`) — no dependency on external libraries

### Layer 3: Graph Traversal (BFS Cascade)
- `cascade_deepen_ocw(seed, hops, fan_out)` — BFS traversal up to N hops
- Bounded by `hops` (depth) × `fan_out` (breadth) to prevent runaway
- Related courses discovered at each hop are enqueued for next hop
- All links persisted as `ocw_resource` nodes with backlinks to seed course

### Layer 4: Auto-Expansion (Background Daemon)
- `auto_deepen_undiscovered(max_courses=3)` — deepens 3 undiscovered courses per research cycle
- Runs automatically every 4 hours via the integrated skill acquirer loop
- No manual trigger required — corpus expands autonomously

### Layer 5: Corpus Integration
- All nodes stored in `learning_log` SQLite table
- `kind` values: `ml_research`, `ocw_course`, `ocw_course_detail`, `ocw_resource`
- Queried by RAG retrieval pipeline alongside session-recall and document-RAG context
- `signal_strength` field enables quality-weighted retrieval

---

## Expansion Frontier Management

To prevent revisiting already-ingested nodes and to prioritize high-value frontiers:

- **De-duplication**: every persist operation checks by title key before insert
- **Cursor rotation**: topic queries rotate through all topics in round-robin (ML + OCW separately)
- **TTL caching**: OCW sitemap cached 24h to avoid redundant fetches
- **Signal weighting**: OCW courses assigned `signal_strength=0.8`, detail nodes `0.85`, external links `0.7`

---

## Interaction Capabilities

The Knowledge Graph tab in the ML Research Hub (page 18) provides:

1. **Live OCW search** — query the sitemap scorer interactively
2. **Deep-fetch button** — crawl any course's full link lattice on demand
3. **Persist to corpus** — write harvested links to `learning_log`
4. **Cascade Deepen** — BFS traversal from a seed slug (configurable hops and fan-out)
5. **Knowledge Graph viewer** — browse all deep-fetched nodes, grouped by course and resource kind, with clickable hyperlinks to every harvested resource

---

## Theoretical Grounding

This architecture draws from:

- **Web crawling theory** (Cho & Garcia-Molina, 2002): optimal crawl ordering for freshness and coverage
- **Graph-based information retrieval** (Kleinberg HITS, PageRank): link structure as relevance signal
- **Autocatalytic set theory** (Kauffman, 1993): systems that generate their own catalysts
- **Spreading activation networks** (Collins & Loftus, 1975): semantic memory as a labelled graph where activation spreads along associative links
- **Hyperlink-Induced Topic Search (HITS)**: hubs (nodes with many outbound links to authorities) and authorities (nodes with many inbound links from hubs) — OCW course pages are hubs; landmark papers are authorities

The combined effect is a corpus that exhibits **unbounded iterative growth** while remaining topically coherent — each expansion step follows semantic edges rather than random crawls, ensuring the corpus depth increases in exactly the domains the Brain needs most.
