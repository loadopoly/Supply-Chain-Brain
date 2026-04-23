"""Test alternative academic APIs that work through corporate SSL inspection."""
import ssl, truststore, urllib.request, urllib.parse, json, time

ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

def get(url, timeout=8):
    req = urllib.request.Request(url, headers={"User-Agent": "SupplyChainBrain/1.0", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except Exception as e:
        return None, str(e)

q = urllib.parse.quote("supply chain demand forecasting")
tests = [
    # arXiv — free, no auth, XML feed
    ("arXiv", f"https://export.arxiv.org/api/query?search_query=all:{q}&max_results=3&sortBy=relevance"),
    # OpenAlex — free, 200M+ works, JSON
    ("OpenAlex", f"https://api.openalex.org/works?search={q}&per-page=3&select=title,doi,publication_year,cited_by_count"),
    # CrossRef — free, published DOI metadata
    ("CrossRef", f"https://api.crossref.org/works?query={q}&rows=3&select=title,DOI,published"),
    # CORE — open access full text index
    ("CORE", f"https://api.core.ac.uk/v3/search/works?q={q}&limit=3"),
    # Unpaywall — open access PDFs
    ("BASE", f"https://api.base-search.net/cgi-bin/BaseHttpSearchInterface.fcgi?func=PerformSearch&query={q}&hits=3&format=json"),
    # NASA NTRS — systems engineering technical reports
    ("NASA NTRS", f"https://ntrs.nasa.gov/api/citations/search?q={q}&rows=3"),
]

for name, url in tests:
    status, body = get(url)
    if status:
        snippet = body[:200].replace("\n", " ")
        print(f"  ✓ {name:<12} HTTP {status} | {snippet}")
    else:
        print(f"  ✗ {name:<12} FAIL | {body[:120]}")
    time.sleep(0.3)
