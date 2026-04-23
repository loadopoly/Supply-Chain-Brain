"""Lightweight NLP-based part categorization.

Replaces blind aggregation of dim_part categories with a deduction layer:
- normalizes part description tokens (cleaning),
- maps tokens to a curated taxonomy (Steel, Fasteners, Wiring, Bearings, …)
  using a TF-IDF cosine match when scikit-learn is available, falling back
  to keyword lookup otherwise,
- persists the per-part category as an adjacent local variable in
  `local_brain.sqlite` (`part_category` table) so the rest of the Brain can
  use it as a deterministic relational key.
"""
from __future__ import annotations
import re
from typing import Iterable
import pandas as pd

from .local_store import (
    upsert_categories, upsert_categories_ext,
    fetch_categories, fetch_provisional, delete_categories,
)


# Curated taxonomy seeds – extend as the Brain learns.
TAXONOMY = {
    "Steel & Plate":   ["steel", "plate", "sheet", "bar", "rod", "ingot", "billet"],
    "Fasteners":       ["bolt", "nut", "washer", "screw", "rivet", "stud", "pin"],
    "Wiring & Cable":  ["wire", "cable", "harness", "lead", "conductor"],
    "Bearings":        ["bearing", "race", "roller", "ball"],
    "Hydraulics":      ["hydraulic", "valve", "cylinder", "piston", "pump", "manifold"],
    "Electrical":      ["motor", "switch", "relay", "sensor", "fuse", "breaker"],
    "Filtration":      ["filter", "screen", "mesh", "strainer"],
    "Paint & Coatings":["paint", "coating", "primer", "lacquer"],
    "Belts & Hose":    ["belt", "hose", "tube", "tubing"],
    "Castings":        ["casting", "cast", "forging", "forge"],
    "Bushings":        ["bushing", "sleeve", "spacer", "grommet"],
    "Conveyors":       ["conveyor", "idler", "pulley"],
}

_TOKEN = re.compile(r"[A-Za-z]{3,}")

# Minimum TF-IDF / keyword confidence for a classification to be treated as
# *confirmed*.  Parts below this threshold are held in the provisional
# "test corpus" and re-evaluated on each agent cycle as new data arrives.
CONFIDENCE_THRESHOLD: float = 0.15

# Confidence at which a classification is *realized as truth* — the part
# exits the test corpus as a confirmed, locked-in classification.
CERTAINTY_THRESHOLD: float = 0.70

# Maximum refinement cycles an 'Uncategorized' part may remain in the test
# corpus.  Once exhausted, the part is dropped entirely — statistical certainty
# has been reached that it cannot be classified with the current taxonomy.
_UNCATEGORIZED_MAX_CYCLES: int = 6


def _tokens(text: str) -> list[str]:
    return [t.lower() for t in _TOKEN.findall(str(text or ""))]


def _keyword_match(tokens: list[str]) -> tuple[str, float]:
    best = ("Uncategorized", 0.0)
    for cat, kws in TAXONOMY.items():
        hits = sum(1 for t in tokens if t in kws)
        if hits > 0:
            score = hits / max(len(tokens), 1)
            if score > best[1]:
                best = (cat, min(1.0, 0.5 + score))
    return best


def _tfidf_match(descriptions: pd.Series) -> pd.DataFrame:
    """Use TF-IDF cosine similarity if sklearn is available, else fallback."""
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        import numpy as np
    except Exception:
        # Fallback: keyword scoring per row
        cats, conf = [], []
        for d in descriptions.fillna(""):
            c, sc = _keyword_match(_tokens(d))
            cats.append(c)
            conf.append(sc)
        return pd.DataFrame({"category": cats, "confidence": conf})

    seeds = {cat: " ".join(kws) for cat, kws in TAXONOMY.items()}
    vec = TfidfVectorizer(stop_words="english", ngram_range=(1, 2))
    corpus = list(seeds.values()) + descriptions.fillna("").astype(str).tolist()
    M = vec.fit_transform(corpus)
    seed_M, doc_M = M[: len(seeds)], M[len(seeds):]
    sim = cosine_similarity(doc_M, seed_M)
    cats = list(seeds.keys())
    idx = sim.argmax(axis=1)
    conf = sim.max(axis=1)
    out_cats = [cats[i] if conf[k] > 0.05 else "Uncategorized" for k, i in enumerate(idx)]
    return pd.DataFrame({"category": out_cats, "confidence": conf.round(3)})


def categorize_parts(parts_df: pd.DataFrame, key_col: str = "part_key",
                     desc_cols: Iterable[str] = ("oem_part_desc", "part_description",
                                                 "description", "part_name")) -> pd.DataFrame:
    """Categorize a parts dataframe and persist results to local store.

    Returns a copy of `parts_df` with two adjacent columns:
        nlp_category   – string label (may be 'Uncategorized' for unknowns)
        nlp_confidence – float 0..1

    Parts whose confidence falls below ``CONFIDENCE_THRESHOLD`` are stored as
    *provisional* — they are held as a test corpus and refined on each agent
    cycle as new taxonomy data arrives.  Provisional parts still flow through
    to DBI reports with their best-guess label (including 'Uncategorized').
    """
    if parts_df is None or parts_df.empty or key_col not in parts_df.columns:
        return parts_df

    df = parts_df.copy()
    desc_col = next((c for c in desc_cols if c in df.columns), None)
    if desc_col is None:
        desc_col = key_col
    res = _tfidf_match(df[desc_col].astype(str))
    df["nlp_category"] = res["category"].values
    df["nlp_confidence"] = res["confidence"].values

    # Build 6-tuples: (part_key, category, confidence, source, provisional, desc_cache)
    rows = []
    for k, cat, conf, raw_desc in zip(
        df[key_col], df["nlp_category"], df["nlp_confidence"], df[desc_col].astype(str)
    ):
        is_prov = 1 if (conf < CONFIDENCE_THRESHOLD or cat == "Uncategorized") else 0
        rows.append((str(k), str(cat), float(conf), "tfidf", is_prov, raw_desc[:512]))
    try:
        upsert_categories_ext(rows)
    except Exception:
        pass
    return df


def get_category_map() -> dict[str, str]:
    """Return part_key → category from the local store (all parts, including provisional).

    Provisional parts surface their best-guess label (which may be 'Uncategorized').
    DBI consumers should use this for joins — provisional parts are included so no
    data is silently dropped from reports.
    """
    try:
        df = fetch_categories()
        return dict(zip(df["part_key"].astype(str), df["category"].astype(str)))
    except Exception:
        return {}


def get_provisional_map() -> dict[str, bool]:
    """Return part_key → True when the classification is provisional (low confidence)."""
    try:
        df = fetch_categories()
        return dict(zip(df["part_key"].astype(str), df["provisional"].astype(bool)))
    except Exception:
        return {}


def refine_provisional(batch_size: int = 200) -> int:
    """Re-evaluate the provisional test corpus against the current taxonomy.

    Each part in the test corpus is processed through two refinement signals:

    1. **TF-IDF re-match** — re-run against the current TAXONOMY seeds using
       the cached description.  As the taxonomy grows, formerly unknown parts
       may cross the confidence threshold.

    2. **Neighbourhood density boost** — a small (+0.02 max) confidence lift
       is applied when ≥1 confirmed neighbour exists in the same inferred
       category.  This encodes the intuition that a category is more credible
       when many other confirmed parts share it.

    **Two exit paths from the test corpus:**

    * **Positive certainty** — ``confidence ≥ CERTAINTY_THRESHOLD`` with a
      real (non-Uncategorized) category → the classification is *realized as
      truth*, the row is locked as confirmed (``provisional=0``, source
      ``'truth'``).

    * **Negative certainty** — ``refinement_count ≥ _UNCATEGORIZED_MAX_CYCLES``
      and the category is still 'Uncategorized' → the part is dropped from the
      store entirely.  Statistical certainty has been reached that it cannot be
      classified with the current taxonomy; holding it further wastes compute.

    Returns the count of parts promoted to truth this cycle.
    """
    import logging
    try:
        provisional = fetch_provisional(limit=batch_size)
        if provisional.empty:
            return 0

        # Re-run TF-IDF on cached descriptions
        descs = provisional["description_cache"].fillna("").astype(str)
        res = _tfidf_match(descs)

        # Neighbourhood density signal — confirmed parts per category
        try:
            all_cats = fetch_categories()
            confirmed = all_cats[
                all_cats["provisional"].fillna(1).astype(int) == 0
            ] if "provisional" in all_cats.columns else all_cats
            density = confirmed.groupby("category").size().to_dict()
            max_dens = max(density.values(), default=1)
        except Exception:
            density, max_dens = {}, 1

        promoted = 0
        to_cull: list[str] = []
        rows: list[tuple] = []

        for i, (_, prow) in enumerate(provisional.iterrows()):
            part_key      = str(prow["part_key"])
            old_conf      = float(prow.get("confidence") or 0.0)
            old_cat       = str(prow.get("category") or "Uncategorized")
            ref_count     = int(prow.get("refinement_count") or 0)
            desc_cache    = str(prow.get("description_cache") or "")

            new_cat  = res.iloc[i]["category"]
            new_conf = float(res.iloc[i]["confidence"])

            # Neighbourhood density boost (capped at +0.02)
            boost = 0.02 * (density.get(new_cat, 0) / max(max_dens, 1))
            new_conf = min(1.0, new_conf + boost)

            # Use whichever is stronger — we never regress a classification
            if new_conf <= old_conf:
                new_cat  = old_cat
                new_conf = old_conf

            # --- Exit path 1: positive certainty → realized as truth ---
            if new_conf >= CERTAINTY_THRESHOLD and new_cat != "Uncategorized":
                rows.append((part_key, new_cat, new_conf, "truth", 0, desc_cache))
                promoted += 1
                continue

            # --- Exit path 2: negative certainty → drop ---
            if new_cat == "Uncategorized" and ref_count >= _UNCATEGORIZED_MAX_CYCLES:
                to_cull.append(part_key)
                continue

            # Still in test corpus — update if confidence improved
            if new_conf > old_conf:
                is_prov = 1 if (new_conf < CONFIDENCE_THRESHOLD or new_cat == "Uncategorized") else 0
                rows.append((part_key, new_cat, new_conf, "nlp_refined", is_prov, desc_cache))

        if rows:
            upsert_categories_ext(rows)

        culled = 0
        if to_cull:
            culled = delete_categories(to_cull)

        logging.info(
            f"refine_provisional: evaluated {len(provisional)}, "
            f"promoted to truth {promoted}, culled {culled}, "
            f"improved {len(rows) - promoted}."
        )
        return promoted
    except Exception as exc:
        import logging as _log
        _log.warning(f"refine_provisional failed: {exc}")
        return 0


def drain_unclassified(batch_size: int = 500, connector: str = "azure_sql") -> int:
    """Pull uncategorized parts from the DW and NLP-classify them in batches.

    Queries ``edap_dw_replica.dim_part`` for rows whose ``part_key`` is not
    yet present in the local ``part_category`` store, then runs
    ``categorize_parts()`` in chunks of ``batch_size`` and persists the
    results.  Low-confidence results are stored as *provisional* (test corpus)
    rather than being discarded — they are refined on subsequent calls via
    ``refine_provisional()``.

    After the new-part batch, one refinement pass is always executed so that
    the existing test corpus benefits from each cycle's updated taxonomy.

    Returns the number of *new* parts classified in this call (0 if nothing
    new or DW unreachable).  Promotion of existing provisional parts is
    handled inside ``refine_provisional()`` and not reflected in the count.
    """
    try:
        import logging
        from .local_store import init_schema, fetch_categories
        from .db_registry import read_sql, bootstrap_default_connectors

        init_schema()
        bootstrap_default_connectors()

        # Fetch already-classified keys so we can exclude them.
        try:
            classified = set(fetch_categories()["part_key"].astype(str).tolist())
        except Exception:
            classified = set()

        # Pull the next batch of unclassified parts from the DW.
        sql = (
            "SELECT TOP {n} part_key, oem_part_desc, part_description "
            "FROM [edap_dw_replica].[dim_part] "
            "WHERE part_key IS NOT NULL"
        ).format(n=batch_size + len(classified))

        df = read_sql(connector, sql)
        if df.attrs.get("_error") or df.empty:
            logging.warning(f"drain_unclassified: DW unreachable — {df.attrs.get('_error', 'empty')}")
            # Still run refinement on existing test corpus even if DW is down.
            refine_provisional(batch_size=min(batch_size, 300))
            return 0

        # Normalise column names to lowercase.
        df.columns = [c.lower() for c in df.columns]
        if "part_key" not in df.columns:
            refine_provisional(batch_size=min(batch_size, 300))
            return 0

        df["part_key"] = df["part_key"].astype(str).str.strip()
        df = df[~df["part_key"].isin(classified) & (df["part_key"] != "")]
        df = df.head(batch_size).reset_index(drop=True)

        n = 0
        if not df.empty:
            df = categorize_parts(df, key_col="part_key",
                                  desc_cols=("oem_part_desc", "part_description",
                                             "description", "part_name"))
            n = len(df)
            prov_count = int((df["nlp_confidence"] < CONFIDENCE_THRESHOLD).sum())
            logging.info(
                f"drain_unclassified: classified {n} parts "
                f"({n - prov_count} confirmed, {prov_count} provisional/test-corpus)."
            )

        # Refine existing provisional test corpus regardless of new-part count.
        promoted = refine_provisional(batch_size=min(batch_size, 300))
        if promoted:
            logging.info(f"drain_unclassified: refined test corpus — {promoted} parts promoted.")

        return n
    except Exception as exc:
        import logging as _log
        _log.error(f"drain_unclassified failed: {exc}")
        return 0
