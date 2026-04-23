"""One-shot fix: replace deprecated use_container_width with width= equivalents."""
from pathlib import Path

root = Path(__file__).parent
updated = 0
for f in root.rglob("*.py"):
    if f.name == Path(__file__).name:
        continue
    try:
        text = f.read_text(encoding="utf-8", errors="replace")
    except Exception:
        continue
    new = text.replace("use_container_width=True", "width='stretch'")
    new = new.replace("use_container_width=False", "width='content'")
    if new != text:
        f.write_text(new, encoding="utf-8")
        print(f"  Updated: {f.relative_to(root)}")
        updated += 1
print(f"\nTotal updated: {updated} files")
