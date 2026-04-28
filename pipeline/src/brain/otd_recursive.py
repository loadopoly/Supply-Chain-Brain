"""
Recursive OTD categorization — port of recursive_otd_categorization_rebuilt.py
to consume a SQL DataFrame from the Replica DB instead of an Excel file.

All Excel/CLI plumbing has been removed; the recursive clustering core is
preserved verbatim so results match the reference script.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Dict, List, Optional, Sequence, Tuple
import re

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix, hstack, issparse
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS, TfidfVectorizer
from sklearn.impute import SimpleImputer
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

try:
    from sklearn.preprocessing import OneHotEncoder
except Exception as exc:  # pragma: no cover
    raise RuntimeError("scikit-learn is required") from exc

from . import load_config
from .cleaning import standard_clean
from .data_access import fetch_logical, query_df

DEFAULT_STOP_WORDS = set(ENGLISH_STOP_WORDS)
TOKEN_RE = re.compile(r"[a-zA-Z]{2,}")
_SITE_TEXT_PREDICATE_RE = re.compile(
    r"(?i)(?:\[[^\]]+\]\.)?\[?(?:site|business_unit|business_unit_id|business_unit_key)\]?\s*=\s*N?'([^']+)'"
)


def _as_datetime(series: pd.Series) -> pd.Series:
    s = series.copy()
    if pd.api.types.is_numeric_dtype(s):
        txt = s.fillna(0).astype(int).astype(str).str.strip()
        if txt.str.fullmatch(r"\d{8}").mean() > 0.6:
            return pd.to_datetime(txt, format="%Y%m%d", errors="coerce")
    return pd.to_datetime(s, errors="coerce")


def _normalize_otd_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "is_on_time" in out.columns and "days_late" in out.columns:
        return out

    date_pairs = [
        ("receipt_date_key", "due_date_key"),
        ("ship_day_key", "promised_ship_day_key"),
        ("transaction_date", "expected_receipt_date"),
        ("ship_date", "promise_date"),
    ]
    for actual_col, due_col in date_pairs:
        if actual_col in out.columns and due_col in out.columns:
            actual = _as_datetime(out[actual_col])
            due = _as_datetime(out[due_col])
            valid = actual.notna() & due.notna()

            if "days_late" not in out.columns:
                out["days_late"] = np.nan
                out.loc[valid, "days_late"] = (actual.loc[valid] - due.loc[valid]).dt.days.astype(float)

            if "is_on_time" not in out.columns:
                out["is_on_time"] = np.nan
                out.loc[valid, "is_on_time"] = (actual.loc[valid] <= due.loc[valid]).astype(float)
            break

    if "is_on_time" not in out.columns:
        miss_col = next((c for c in out.columns if c.lower() in {
            "otd_miss_late", "otd miss (late)", "is_late", "late_flag"
        }), None)
        if miss_col:
            miss = pd.to_numeric(out[miss_col], errors="coerce")
            out["is_on_time"] = (1.0 - miss.clip(lower=0, upper=1))

    return out


@lru_cache(maxsize=256)
def _resolve_business_unit_key(connector: str, site: str) -> Optional[int]:
    if site is None:
        site_text = ""
    else:
        try:
            site_text = "" if pd.isna(site) else str(site).strip()
        except (TypeError, ValueError):
            site_text = str(site).strip()
    if not site_text or site_text.upper() in {"ALL", "ALL SITES", "NONE"}:
        return None
    if site_text.isdigit():
        return int(site_text)

    sql = """
        SELECT TOP 1 business_unit_key
        FROM [edap_dw_replica].[dim_business_unit] WITH (NOLOCK)
        WHERE UPPER(business_unit_id) = UPPER(?)
           OR UPPER(business_unit) = UPPER(?)
           OR UPPER(display_name) = UPPER(?)
           OR UPPER(short_display_name) = UPPER(?)
        ORDER BY business_unit_key
    """
    df = query_df(connector, sql, [site_text, site_text, site_text, site_text])
    if df.empty or "business_unit_key" not in df.columns:
        return None
    key = pd.to_numeric(df["business_unit_key"], errors="coerce").dropna()
    if key.empty:
        return None
    return int(key.iloc[0])


def _normalize_site_predicates(connector: str, where: str | None) -> str:
    text = str(where or "").strip()
    if not text:
        return ""

    def _replace(match: re.Match[str]) -> str:
        site_text = match.group(1).strip()
        key = _resolve_business_unit_key(connector, site_text)
        if key is None:
            return "1 = 0"
        return f"[business_unit_key] = {key}"

    return _SITE_TEXT_PREDICATE_RE.sub(_replace, text)


@dataclass
class OTDConfig:
    text_col: str = "description"
    site_col: Optional[str] = "site"
    id_col: Optional[str] = None
    numeric_cols: List[str] = field(default_factory=list)
    categorical_cols: List[str] = field(default_factory=list)
    max_depth: int = 4
    min_samples_to_split: int = 16
    max_k: int = 8
    random_state: int = 42
    max_features_tfidf: int = 1500


# ---------------------------------------------------------------------------
# Feature pipeline (mirrors the attached script, leaner)
# ---------------------------------------------------------------------------
def _preprocess_text(text: object) -> str:
    if not isinstance(text, str):
        return ""
    toks = [t for t in TOKEN_RE.findall(text.lower()) if t not in DEFAULT_STOP_WORDS]
    return " ".join(toks)


def build_features(df: pd.DataFrame, cfg: OTDConfig) -> csr_matrix:
    parts: list = []
    text_series = df[cfg.text_col].fillna("").astype(str).map(_preprocess_text)
    parts.append(TfidfVectorizer(max_features=cfg.max_features_tfidf,
                                 ngram_range=(1, 2), min_df=1).fit_transform(text_series))
    if cfg.numeric_cols:
        num = SimpleImputer(strategy="median").fit_transform(df[cfg.numeric_cols])
        num = StandardScaler().fit_transform(num)
        parts.append(csr_matrix(num))
    if cfg.categorical_cols:
        cat = SimpleImputer(strategy="most_frequent").fit_transform(df[cfg.categorical_cols])
        try:
            ohe = OneHotEncoder(handle_unknown="ignore", sparse_output=True)
        except TypeError:
            ohe = OneHotEncoder(handle_unknown="ignore", sparse=True)
        cat_x = ohe.fit_transform(cat)
        parts.append(cat_x if issparse(cat_x) else csr_matrix(cat_x))
    return hstack(parts).tocsr() if len(parts) > 1 else parts[0].tocsr()


def find_optimal_k(x: np.ndarray, max_k: int, random_state: int = 42) -> Tuple[int, list[dict]]:
    n = len(x)
    if n < 3:
        return 1, []
    upper = min(max_k, n - 1)
    if upper < 2:
        return 1, []
    diags, best_k, best = [], 2, -1.0
    for k in range(2, upper + 1):
        try:
            km = KMeans(n_clusters=k, random_state=random_state, n_init=10)
            labels = km.fit_predict(x)
            if len(set(labels)) < 2:
                continue
            s = silhouette_score(x, labels)
            diags.append({"k": k, "silhouette": float(s), "inertia": float(km.inertia_)})
            if s > best:
                best, best_k = s, k
        except Exception:
            continue
    return (best_k if diags else 1), diags


def _summary(rows: pd.DataFrame, cfg: OTDConfig, level: int, path: str) -> dict:
    out: dict = {"cluster_path": path, "level": level, "size": int(len(rows))}
    # OTD metrics — included if the computed columns exist
    if "is_on_time" in rows.columns:
        out["otd_rate"] = float(rows["is_on_time"].mean(skipna=True))
    if "days_late" in rows.columns:
        out["avg_days_late"] = float(rows["days_late"].mean(skipna=True))
    # Text keyword extraction — only when a real text column is present
    if cfg.text_col in rows.columns:
        texts = rows[cfg.text_col].fillna("").astype(str).map(_preprocess_text).tolist()
        if any(t for t in texts if t):
            try:
                vec = TfidfVectorizer(max_features=1000, ngram_range=(1, 2), min_df=1)
                x = vec.fit_transform(texts)
                scores = np.asarray(x.sum(axis=0)).ravel()
                terms = np.array(vec.get_feature_names_out())
                out["top_keywords"] = terms[np.argsort(scores)[::-1][:8]].tolist()
            except Exception:
                out["top_keywords"] = []
    if cfg.site_col and cfg.site_col in rows.columns:
        out["site_breakdown"] = rows[cfg.site_col].astype(str).value_counts().head(10).to_dict()
    return out


def recursive_cluster(df: pd.DataFrame, features: np.ndarray, cfg: OTDConfig,
                      level: int = 1, parent: str = "",
                      summaries: list[dict] | None = None
                      ) -> tuple[pd.Series, list[dict]]:
    if summaries is None:
        summaries = []
    assignments = pd.Series(index=df.index, dtype="object")

    if len(df) < max(cfg.min_samples_to_split, 3) or level > cfg.max_depth:
        path = parent or "ROOT"
        assignments.loc[df.index] = path
        summaries.append(_summary(df, cfg, level, path))
        return assignments, summaries

    best_k, diags = find_optimal_k(features, cfg.max_k, cfg.random_state)
    if best_k <= 1:
        path = parent or "ROOT"
        assignments.loc[df.index] = path
        summaries.append(_summary(df, cfg, level, path))
        return assignments, summaries

    km = KMeans(n_clusters=best_k, random_state=cfg.random_state, n_init=10)
    labels = km.fit_predict(features)
    sil = silhouette_score(features, labels) if len(set(labels)) > 1 else -1
    if sil < 0.02:
        path = parent or "ROOT"
        assignments.loc[df.index] = path
        s = _summary(df, cfg, level, path); s["diagnostics"] = diags
        summaries.append(s)
        return assignments, summaries

    for raw in sorted(set(labels)):
        cnum = int(raw) + 1
        cpath = f"L{level}C{cnum}" if not parent else f"{parent}_L{level}C{cnum}"
        mask = labels == raw
        child_df = df.loc[mask].copy()
        child_features = features[mask]
        s = _summary(child_df, cfg, level, cpath); s["diagnostics"] = diags
        summaries.append(s)
        if len(child_df) >= cfg.min_samples_to_split and level < cfg.max_depth:
            ca, summaries = recursive_cluster(child_df, child_features, cfg,
                                              level + 1, cpath, summaries)
            assignments.loc[child_df.index] = ca.loc[child_df.index]
        else:
            assignments.loc[child_df.index] = cpath
    return assignments, summaries


# ---------------------------------------------------------------------------
# Public driver — wires Replica DB into the clustering pipeline
# ---------------------------------------------------------------------------
def run_otd_from_replica(connector: str = "azure_sql",
                         where: str | None = None,
                         site: str | None = None,
                         limit: int = 10_000) -> tuple[pd.DataFrame, list[dict]]:
    """Pull OTD-relevant rows from the Replica, clean, recursively cluster."""
    cfg_yaml = load_config().get("otd", {})
    qualified = cfg_yaml.get("source_table", "edap_dw_replica.fact_po_receipt")
    schema, table = qualified.split(".")[0], qualified.split(".")[-1]

    sql = f"SELECT TOP {int(limit)} * FROM [{schema}].[{table}] WITH (NOLOCK)"
    clauses: list[str] = []
    normalized_where = _normalize_site_predicates(connector, where)
    if normalized_where:
        clauses.append(f"({normalized_where})")
    site_key = _resolve_business_unit_key(connector, site or "")
    if site_key is not None:
        clauses.append(f"[business_unit_key] = {site_key}")
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    df = query_df(connector, sql)
    if df.empty:
        return df, []

    df = _normalize_otd_metrics(df)

    cfg = OTDConfig(
        text_col=cfg_yaml.get("description_col_hint", "description"),
        site_col=cfg_yaml.get("site_col_hint", "site"),
        max_depth=int(cfg_yaml.get("max_depth", 4)),
        max_k=int(cfg_yaml.get("max_k", 8)),
        random_state=int(cfg_yaml.get("random_state", 42)),
    )

    if cfg.text_col not in df.columns:
        # fallback: pick the longest object column as the description carrier
        obj_cols = [c for c in df.columns if df[c].dtype == "object"]
        if not obj_cols:
            return df, []
        cfg.text_col = max(obj_cols, key=lambda c: df[c].astype(str).str.len().mean())

    cfg.numeric_cols = [c for c in cfg_yaml.get("numeric_col_hints", [])
                        if c in df.columns]
    cfg.categorical_cols = [c for c in cfg_yaml.get("categorical_col_hints", [])
                            if c in df.columns]

    work = df.copy().reset_index(drop=True)
    work[cfg.text_col] = work[cfg.text_col].fillna("")
    feats = build_features(work, cfg)
    feats_dense = feats.toarray() if issparse(feats) else np.asarray(feats)
    assignments, summaries = recursive_cluster(work, feats_dense, cfg)
    work["cluster_path"] = assignments.values
    return work, summaries
