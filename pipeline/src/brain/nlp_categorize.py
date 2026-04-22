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

from .local_store import upsert_categories, fetch_categories


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
        nlp_category   – string label
        nlp_confidence – float 0..1
    """
    if parts_df is None or parts_df.empty or key_col not in parts_df.columns:
        return parts_df

    df = parts_df.copy()
    desc_col = next((c for c in desc_cols if c in df.columns), None)
    if desc_col is None:
        # Use the key itself as token source – low confidence.
        desc_col = key_col
    res = _tfidf_match(df[desc_col].astype(str))
    df["nlp_category"] = res["category"].values
    df["nlp_confidence"] = res["confidence"].values

    rows = [
        (str(k), str(c), float(conf), "tfidf")
        for k, c, conf in zip(df[key_col], df["nlp_category"], df["nlp_confidence"])
    ]
    try:
        upsert_categories(rows)
    except Exception:
        pass
    return df


def get_category_map() -> dict[str, str]:
    """Return part_key → category from the local store."""
    try:
        df = fetch_categories()
        return dict(zip(df["part_key"].astype(str), df["category"].astype(str)))
    except Exception:
        return {}
