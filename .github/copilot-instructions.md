# Supply Chain Brain — Copilot Instructions

## Progressive Session Recall — RUN FIRST ON EVERY PROMPT

**Run `session-recall` FIRST on every prompt before doing anything else.**
It costs ~50 tokens and prevents expensive blind searches.

```bash
session-recall files --json --limit 10   # recently touched files
session-recall files --days 7 --json     # files touched in last 7 days
session-recall list --json --limit 5     # recent sessions
session-recall list --days 2 --json      # sessions from last 2 days
session-recall search '<term>' --json    # full-text search
session-recall checkpoints --days 3      # recent checkpoints
session-recall show <id> --json          # drill into one session
session-recall health                    # 9-dimension health check
session-recall schema-check              # validate DB schema after CLI upgrades
```

Only use filesystem tools (grep, glob, find) if session-recall returns nothing useful.
If `session-recall` errors, continue silently — it's a convenience, not a blocker.

---

## Project Context

This is the **Supply Chain Brain** — a Streamlit analytics platform with:
- **pipeline/** — main app (`app.py`, `pages/`, `src/`)
- **Proxy-Pointer-RAG/** — structural RAG for document analysis (requires `GOOGLE_API_KEY` in `Proxy-Pointer-RAG/.env`)
- Oracle Fusion + Azure SQL as primary data sources

### Key commands
```bash
# Run the app
cd pipeline && streamlit run app.py

# Build Proxy-Pointer RAG index
cd Proxy-Pointer-RAG && python -m src.indexing.build_pp_index --fresh

# Query the RAG bot
cd Proxy-Pointer-RAG && python -m src.agent.pp_rag_bot

# Check auto-memory health
session-recall health
```

### Proxy-Pointer-RAG document flow
1. Place `.md` files in `Proxy-Pointer-RAG/data/documents/`
2. Run `python -m src.indexing.build_pp_index` from `Proxy-Pointer-RAG/`
3. Query via Streamlit page **Document Analysis** (page 17) or CLI bot
