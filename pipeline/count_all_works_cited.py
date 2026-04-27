"""Count total unique scholarly Works Cited candidates in the Grok JSON."""
import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from src.brain.knowledge_corpus import (
    _resolve_scb_docs_path, _walk_scb_web_results, _clean_scb_url,
    _is_scb_scholarly_reference, _paper_id_from_reference,
)
import json

scb_path = _resolve_scb_docs_path()
raw = json.loads(scb_path.read_text(encoding="utf-8"))
conversations = raw if isinstance(raw, list) else raw.get("conversations", [])
print(f"Total conversations: {len(conversations)}")

refs = []
seen: set[str] = set()

for conv_index, conv_wrapper in enumerate(conversations):
    responses = conv_wrapper.get("responses", []) or []
    for response_index, item in enumerate(responses):
        response = item.get("response") or {}
        for candidate in _walk_scb_web_results(response):
            url = _clean_scb_url(str(candidate.get("url") or ""))
            title = str(candidate.get("title") or "").strip()
            preview = str(candidate.get("preview") or candidate.get("description") or "").strip()
            if not url or not _is_scb_scholarly_reference(url, title, preview):
                continue
            paper_id, doi, arxiv_id = _paper_id_from_reference(url, title)
            dedupe_key = paper_id or url.lower()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            refs.append({
                "conv_index": conv_index,
                "url": url,
                "title": title[:80],
                "paper_id": paper_id,
            })

print(f"\nTotal unique scholarly candidates (no limit): {len(refs)}")
# Show sample of host distribution
from urllib.parse import urlparse
from collections import Counter
hosts = Counter(urlparse(r["url"]).netloc for r in refs)
print("\nTop 15 hosts:")
for host, count in hosts.most_common(15):
    print(f"  {count:4d}  {host}")
