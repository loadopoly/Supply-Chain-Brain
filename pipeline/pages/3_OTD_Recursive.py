"""Page 3 — OTD Daily Operations Dashboard with Brain recursive clustering.

OTD mechanics learned from OTD file.xlsx Export sheet (11,165 rows, 51 cols):

  * OTD Miss (Late) = 0 -> on time | = 1 -> late  (values >1 are encoding errors)
  * Grace period: Days Late = 1 is NEVER counted as a miss. Threshold = Days Late >= 2.
  * Adjusted Promise Date = Promised Date in this dataset (no adjustments made).
  * Status Code: CLOSED = shipped/completed, OPEN = in progress (Ship Date is NaT).
  * Drop Ship = Yes -> 100% OTD (manufacturer ships direct, no warehouse involvement).

F-Code classification (OTDClass_Updated / F7 SO columns):
  OnTime: on time
  F2:  WH minor delay            -- ~98% still on time (grace/rounding artefact)
  F3:  Mfg not ready, Prime      -- 100% late
  F4:  Mfg not ready, Low Vol    -- 100% late
  F5:  No purchased part, Prime  -- 100% late
  F6:  No purchased part, Low Vol-- 100% late
  F7:  SO blocked by another line-- 93% late
  F8:  WH failed to ship         -- 92% late
  F9:  SO partial block (minor)  -- 15% late
  F10: WH partial delay          -- 76% late
  NaN: open / unclassified lines

Key OTD drivers (effect on on-time %):
  Drop Ship Yes vs No:           100% vs 81%
  Customer Pickup vs Prepaid:    95% vs 81%
  Domestic vs International:     83% vs 71%
  Purchased vs Fabricated:       83% vs 74%
  A/B/C Prime vs Low Volume:     83-85% vs 55-68%
  Single SO date vs Multi-date:  87% vs 70%
  No stock issue vs stock issue: 84% vs 74%
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from scipy.sparse import issparse
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain.db_registry import bootstrap_default_connectors
from src.brain.findings_index import record_findings_bulk
from src.brain.global_filters import date_key_window, get_global_window
from src.brain.label_resolver import enrich_labels
from src.brain.local_store import fetch_otd_owners, upsert_otd_owner
from src.brain.otd_recursive import run_otd_from_replica, OTDConfig, build_features, recursive_cluster
from src.brain.operator_shell import render_operator_sidebar_fallback

st.session_state["_page"] = "otd_recursive"
bootstrap_default_connectors()
render_operator_sidebar_fallback()

_OTD_EXPORT_SHEET = "Export"
_DAILY_WORKLIST_SHEETS = ["Missed Yesterday", "Shipping today", "Opened Yesterday"]
_TODAY = pd.Timestamp.today().normalize()

_FCODE_META: dict[str, dict] = {
    "OnTime": {"label": "On Time",                               "category": "On Time",    "color": "#22c55e"},
    "F2":     {"label": "F2 - WH minor delay (mostly on-time)",  "category": "Warehouse",  "color": "#86efac"},
    "F3":     {"label": "F3 - Mfg not ready (Prime)",            "category": "Production", "color": "#ef4444"},
    "F4":     {"label": "F4 - Mfg not ready (Low Volume)",       "category": "Production", "color": "#f87171"},
    "F5":     {"label": "F5 - No purchased part (Prime)",        "category": "Purchasing", "color": "#f97316"},
    "F6":     {"label": "F6 - No purchased part (Low Volume)",   "category": "Purchasing", "color": "#fb923c"},
    "F7":     {"label": "F7 - SO blocked by another line",       "category": "SO Block",   "color": "#a855f7"},
    "F8":     {"label": "F8 - WH failed to ship",                "category": "Warehouse",  "color": "#6366f1"},
    "F9":     {"label": "F9 - SO partial block (minor)",         "category": "SO Block",   "color": "#818cf8"},
    "F10":    {"label": "F10 - WH partial / delayed ship",       "category": "Warehouse",  "color": "#7c3aed"},
}

_OTD_DETAIL_COLS = [
    "Site", "SO No", "Line No", "Part", "Description", "Supplier Name",
    "Customer", "Status Code", "Ship Date", "Promised Date", "Adjusted Promise Date",
    "OTD Miss (Late)", "Days Late", "OTDClass_Updated", "Failure Reason",
    "Qty", "Available Qty", "On Hand Qty", "Unit Price", "Part Class",
    "Part Pur/Fab", "Freight Terms", "Drop Ship", "Domestic/International",
    "Total Count SO Lines", "Total Count SO Lines Late",
    "SO Has Multiple Ship Dates", "F7 SO", "SO-Line",
    "Review", "Owner", "Owner Comment", "Needs Review", "cluster_path",
]


def _coerce_date_series(series: pd.Series) -> pd.Series:
    s = series.copy()
    if pd.api.types.is_numeric_dtype(s):
        txt = s.fillna(0).astype(int).astype(str).str.strip()
        if txt.str.fullmatch(r"\d{8}").mean() > 0.6:
            return pd.to_datetime(txt, format="%Y%m%d", errors="coerce")
    return pd.to_datetime(s, errors="coerce")


def _is_otd_file_schema(df_in: pd.DataFrame) -> bool:
    return "OTD Miss (Late)" in df_in.columns and "SO No" in df_in.columns


def _clean_otd_df(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    if "Site" in df.columns:
        df = df[df["Site"] != "Total"]
    if "OTD Miss (Late)" in df.columns:
        df["OTD Miss (Late)"] = (
            pd.to_numeric(df["OTD Miss (Late)"], errors="coerce")
            .fillna(0).clip(0, 1).astype(int)
        )
    return df.reset_index(drop=True)


def _compute_completion_pct(df_in: pd.DataFrame) -> pd.Series:
    if "OTD Miss (Late)" in df_in.columns:
        miss = pd.to_numeric(df_in["OTD Miss (Late)"], errors="coerce").fillna(0).clip(0, 1)
        return (1.0 - miss) * 100.0
    if "is_on_time" in df_in.columns and pd.api.types.is_numeric_dtype(df_in["is_on_time"]):
        return pd.to_numeric(df_in["is_on_time"], errors="coerce") * 100.0
    for col in df_in.columns:
        if col.lower() in {"otd_miss_late", "is_late", "late_flag"}:
            miss = pd.to_numeric(df_in[col], errors="coerce")
            return (1.0 - miss.clip(0, 1)) * 100.0
    for actual_col, promised_col in [
        ("ship_day_key", "promised_ship_day_key"),
        ("receipt_date_key", "due_date_key"),
        ("ship_date", "promise_date"),
        ("transaction_date", "expected_receipt_date"),
    ]:
        if actual_col in df_in.columns and promised_col in df_in.columns:
            act = _coerce_date_series(df_in[actual_col])
            pro = _coerce_date_series(df_in[promised_col])
            ok = act.notna() & pro.notna()
            out = pd.Series(np.nan, index=df_in.index)
            out.loc[ok] = (act.loc[ok] <= pro.loc[ok]).astype(float) * 100.0
            return out
    return pd.Series(np.nan, index=df_in.index)


@st.cache_data(ttl=120, show_spinner="Loading OTD file ...")
def _load_otd_file_from_path(path_str: str) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    path = Path(path_str)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path), {}
    xf = pd.ExcelFile(path)
    sheet = _OTD_EXPORT_SHEET if _OTD_EXPORT_SHEET in xf.sheet_names else xf.sheet_names[0]
    df = xf.parse(sheet)
    worklists = {}
    for sn in _DAILY_WORKLIST_SHEETS:
        if sn in xf.sheet_names:
            w = xf.parse(sn).dropna(how="all").dropna(subset=["Order Date"])
            if not w.empty:
                worklists[sn] = w
    return df, worklists


@st.cache_data(ttl=120, show_spinner="Loading uploaded OTD file ...")
def _load_otd_file_from_upload(file_name: str, raw_bytes: bytes) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    from io import BytesIO, StringIO
    if file_name.lower().endswith(".csv"):
        return pd.read_csv(StringIO(raw_bytes.decode("utf-8", errors="ignore"))), {}
    xf = pd.ExcelFile(BytesIO(raw_bytes))
    sheet = _OTD_EXPORT_SHEET if _OTD_EXPORT_SHEET in xf.sheet_names else xf.sheet_names[0]
    df = xf.parse(sheet)
    worklists = {}
    for sn in _DAILY_WORKLIST_SHEETS:
        if sn in xf.sheet_names:
            w = xf.parse(sn).dropna(how="all").dropna(subset=["Order Date"])
            if not w.empty:
                worklists[sn] = w
    return df, worklists


@st.cache_data(ttl=600, show_spinner="Pulling OTD rows from Azure SQL replica + clustering ...")
def _run_replica(lim: int, wh: str, sk: int, ek: int, site: str):
    where = wh if wh else "1=1"
    where += f" AND receipt_date_key BETWEEN {sk} AND {ek}"
    return run_otd_from_replica(where=where, site=site, limit=int(lim))


# ── Page header ────────────────────────────────────────────────────────────────
st.markdown("## OTD Daily Operations")
st.caption(
    "On-Time Delivery intelligence | 1-day grace window | "
    "F-code root-cause classification | Brain recursive clustering"
)

# Early DBI card — renders before data load / clustering so Playwright finds it quickly.
_early_otd_ctx = {k: v for k, v in st.session_state.items()
                  if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight("OTD Recursive", _early_otd_ctx)

_default_otd_file = Path(__file__).resolve().parents[1] / "docs" / "OTD file.xlsx"

with st.expander("Source & query settings", expanded=False):
    _c1, _c2, _c3 = st.columns(3)
    source_mode = _c1.radio(
        "Data source",
        ["OTD file (bundled)", "Live Replica", "Upload OTD file"],
        key="otd_source_mode",
    )
    limit = _c2.number_input("Rows (replica)", 500, 50_000, 5000, step=500, key="otd_limit")
    where = _c3.text_input("WHERE clause (replica)", key="otd_where")

uploaded_otd = None
if source_mode == "Upload OTD file":
    uploaded_otd = st.file_uploader("Upload OTD file", type=["xlsx", "csv"], key="otd_upload_file")

# ── Load raw data ──────────────────────────────────────────────────────────────
df = pd.DataFrame()
summaries: list[dict] = []
daily_worklists = {}

if source_mode == "Live Replica":
    try:
        sk, ek = date_key_window()
        site_value = st.session_state.get("g_site", "ALL")
        if site_value is None or pd.isna(site_value) or not str(site_value).strip():
            site_value = "ALL"
        df, summaries = _run_replica(
            int(st.session_state.get("otd_limit", 5000)),
            st.session_state.get("otd_where", ""),
            sk, ek,
            str(site_value),
        )
    except Exception as exc:
        st.warning(f"Live OTD pull failed ({exc}) -- falling back to bundled file.")
        source_mode = "OTD file (bundled)"

if source_mode == "OTD file (bundled)":
    if not _default_otd_file.exists():
        st.error("Bundled OTD file not found at pipeline/docs/OTD file.xlsx")
        st.stop()
    try:
        df, daily_worklists = _load_otd_file_from_path(str(_default_otd_file))
    except Exception as exc:
        st.error(f"Bundled OTD file error: {exc}")
        st.stop()

if source_mode == "Upload OTD file":
    if uploaded_otd is None:
        st.info("Upload an OTD Excel/CSV file to continue.")
        st.stop()
    try:
        df, daily_worklists = _load_otd_file_from_upload(uploaded_otd.name, uploaded_otd.getvalue())
    except Exception as exc:
        st.error(f"Upload failed: {exc}")
        st.stop()

def _run_local_clustering(df_in: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    cfg = OTDConfig(text_col="Description", site_col="Site", max_depth=4, max_k=8)
    if cfg.text_col not in df_in.columns:
        obj_cols = [c for c in df_in.columns if df_in[c].dtype == "object"]
        if obj_cols:
            cfg.text_col = max(obj_cols, key=lambda c: df_in[c].astype(str).str.len().mean())
        else:
            df_in["cluster_path"] = "ROOT"
            return df_in, [{"cluster_path": "ROOT", "size": len(df_in), "level": 1}]
    cfg.numeric_cols = [c for c in ["Qty", "Available Qty", "Days Late"] if c in df_in.columns]
    cfg.categorical_cols = [c for c in ["Supplier Name", "Customer", "Part Class", "Part Pur/Fab", "Status Code"] if c in df_in.columns]
    work = df_in.copy().reset_index(drop=True)
    work[cfg.text_col] = work[cfg.text_col].fillna("")
    try:
        feats = build_features(work, cfg)
        feats_dense = feats.toarray() if issparse(feats) else np.asarray(feats)
        assignments, summaries = recursive_cluster(work, feats_dense, cfg)
        work["cluster_path"] = assignments.values
        return work, summaries
    except Exception as e:
        df_in["cluster_path"] = "ROOT"
        return df_in, [{"cluster_path": "ROOT", "size": len(df_in), "level": 1}]

# ── Clean + enrich ─────────────────────────────────────────────────────────────
df = _clean_otd_df(df)

if source_mode != "Live Replica":
    s_dt, e_dt = get_global_window()
    _df_all = df.copy()   # preserve full bundled data before date filter
    _date_mask = pd.Series(False, index=df.index)
    _found_date_col = False
    for _dcol in ["Ship Date", "Promised Date", "Adjusted Promise Date", "Order Date"]:
        if _dcol in df.columns:
            _dt_vals = _coerce_date_series(df[_dcol]).dt.date
            _date_mask |= (_dt_vals >= s_dt) & (_dt_vals <= e_dt)
            _found_date_col = True
    if _found_date_col:
        df = df[_date_mask].copy()

    # When the global date window excludes all bundled rows, use the full
    # bundled dataset so the page always renders in demo/preview mode.
    if df.empty and not _df_all.empty:
        st.warning(
            "⚠️ **Demo mode** — Global Timeline window doesn't overlap with "
            "bundled OTD data. Showing all bundled rows for preview."
        )
        df = _df_all

    if not df.empty:
        with st.spinner("Running Brain clustering..."):
            df, summaries = _run_local_clustering(df)
    else:
        summaries = [{"cluster_path": "ROOT", "size": 0, "level": 1}]

if df.empty:
    st.error("No OTD lines found in the selected Global Timeline window.")
    st.stop()

with st.spinner("Resolving labels ..."):
    df = enrich_labels(df)

df["completion_pct"] = _compute_completion_pct(df)
_is_otd_schema = _is_otd_file_schema(df)

# ── Global filters ─────────────────────────────────────────────────────────────
_filter_map = [
    ("Site", "site"),
    ("Part Class", "commodity"),
    ("Part Pur/Fab", "part_fab"),
    ("Supplier Name", "supplier_key"),
    ("Customer", "customer"),
    ("OTDClass_Updated", "buyer"),
    ("Status Code", "status_code"),
    ("Domestic/International", "dom_intl"),
    ("Drop Ship", "drop_ship"),
]
_active_filter_cols: list[str] = []
for export_col, raw in _filter_map:
    if export_col in df.columns:
        _active_filter_cols.append(export_col)
    else:
        for v in [raw + "_label", raw]:
            if v in df.columns:
                _active_filter_cols.append(v)
                break

filtered = df.copy()
if _active_filter_cols:
    with st.expander("Filters", expanded=True):
        _fcols = st.columns(min(len(_active_filter_cols), 4))
        for i, col in enumerate(_active_filter_cols):
            with _fcols[i % 4]:
                opts = sorted(map(str, filtered[col].dropna().unique()))[:200]
                sel = st.multiselect(col, opts, default=[], key=f"otd_f_{col}")
                if sel:
                    filtered = filtered[filtered[col].astype(str).isin(sel)]

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight('OTD Recursive', ctx)

# ── KPI strip ─────────────────────────────────────────────────────────────────
_status_col = "Status Code" if "Status Code" in filtered.columns else None
_closed = filtered[filtered[_status_col] == "CLOSED"] if _status_col else filtered
_open   = filtered[filtered[_status_col] == "OPEN"]   if _status_col else pd.DataFrame()
_late_col = "Days Late" if "Days Late" in filtered.columns else next(
    (c for c in filtered.columns if c.lower() == "days_late"), None
)
_cp = filtered["completion_pct"]
_closed_cp = _closed["completion_pct"] if (not _closed.empty and "completion_pct" in _closed.columns) else _cp
_at_risk_n = int((_open["OTD Miss (Late)"] > 0).sum()) if (_is_otd_schema and not _open.empty) else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Lines", f"{len(filtered):,}")
k2.metric(
    "On-Time %",
    f"{_cp.mean():.1f}%" if _cp.notna().any() else "N/A",
    delta=f"Closed: {_closed_cp.mean():.1f}%" if (_status_col and _closed_cp.notna().any()) else None,
    delta_color="off",
)
k3.metric("Open Lines", f"{len(_open):,}" if _status_col else "--")
k4.metric("Open Late",  f"{_at_risk_n:,}" if _status_col else "--")
if _late_col and pd.api.types.is_numeric_dtype(filtered[_late_col]):
    _avg_late = pd.to_numeric(filtered[_late_col], errors="coerce").mean()
    k5.metric("Avg Days Late", f"{_avg_late:.1f}")
else:
    k5.metric("Clusters", filtered["cluster_path"].nunique() if "cluster_path" in filtered.columns else "--")

st.markdown(
    f"<p style='color:#64748b;font-size:0.75rem;'>"
    f"Source: <b>{source_mode}</b> | {len(filtered):,} of {len(df):,} records shown"
    f"</p>",
    unsafe_allow_html=True,
)
st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN TABS
# ═══════════════════════════════════════════════════════════════════════════════
tab_sc, tab_risk, tab_brain, tab_trend, tab_drivers, tab_detail, tab_daily = st.tabs([
    "Scorecard",
    "At Risk",
    "Brain Clusters",
    "Trend",
    "Drivers",
    "Detail Records",
    "Daily Review",
])

# ── SCORECARD ─────────────────────────────────────────────────────────────────
with tab_sc:
    st.subheader("OTD Scorecard")

    sc1, sc2 = st.columns([1.1, 0.9])

    with sc1:
        st.markdown("##### F-Code Classification Breakdown")
        st.caption(
            "F-codes classify why a line is late. "
            "F3-F6 = 100% late (manufacturing/purchasing failures). "
            "F7/F8 = warehouse and SO-blocking issues. "
            "F9/F10 = partial or minor delays."
        )
        if "OTDClass_Updated" in filtered.columns:
            if _is_otd_schema:
                _fc_grp = (
                    filtered.groupby("OTDClass_Updated", dropna=False)
                    .agg(
                        Lines=("OTD Miss (Late)", "count"),
                        Late=("OTD Miss (Late)", "sum"),
                        OnTime_pct=("completion_pct", "mean"),
                    )
                    .reset_index()
                )
            else:
                _fc_grp = (
                    filtered.groupby("OTDClass_Updated", dropna=False)
                    .agg(Lines=("completion_pct", "count"), OnTime_pct=("completion_pct", "mean"))
                    .reset_index()
                )
                _fc_grp["Late"] = (_fc_grp["Lines"] * (1 - _fc_grp["OnTime_pct"] / 100)).round(0).astype(int)

            _fc_grp["OTDClass_Updated"] = _fc_grp["OTDClass_Updated"].fillna("Unclassified").astype(str)
            _fc_grp["Description"] = _fc_grp["OTDClass_Updated"].map(
                lambda x: _FCODE_META.get(x, {}).get("label", x)
            )
            _fc_grp["Category"] = _fc_grp["OTDClass_Updated"].map(
                lambda x: _FCODE_META.get(x, {}).get("category", "Other")
            )
            _fc_grp["On-Time %"] = _fc_grp["OnTime_pct"].round(1)
            _fc_grp = _fc_grp.sort_values("Lines", ascending=False)

            st.dataframe(
                _fc_grp[["OTDClass_Updated", "Description", "Category", "Lines", "Late", "On-Time %"]],
                use_container_width=True, hide_index=True,
            )

            _fc_plot = _fc_grp[_fc_grp["Lines"] >= 5].copy()
            if not _fc_plot.empty:
                fig_fc = px.bar(
                    _fc_plot.sort_values("On-Time %"),
                    x="On-Time %", y="OTDClass_Updated", orientation="h",
                    color="On-Time %", color_continuous_scale="RdYlGn", range_color=[0, 100],
                    text=_fc_plot.sort_values("On-Time %")["On-Time %"].astype(str) + "%",
                    template="plotly", title="F-Code On-Time Rate",
                    labels={"OTDClass_Updated": ""},
                )
                fig_fc.update_layout(
                    height=300,
                    coloraxis_showscale=False, xaxis_range=[0, 105],
                    margin=dict(t=40, b=10),
                )
                fig_fc.update_traces(textposition="outside")
                st.plotly_chart(fig_fc, use_container_width=True)
        else:
            st.info("OTDClass_Updated column not found in this dataset.")

    with sc2:
        if "Failure Reason" in filtered.columns and _is_otd_schema:
            _late_lines = filtered[filtered["OTD Miss (Late)"] == 1]
            if not _late_lines.empty:
                _fail_grp = (
                    _late_lines["Failure Reason"].fillna("Unspecified")
                    .value_counts().reset_index()
                )
                st.markdown("##### Failure Reasons (Late Lines Only)")
                fig_donut = px.pie(
                    _fail_grp, names="Failure Reason", values="count",
                    hole=0.45, template="plotly",
                    color_discrete_sequence=px.colors.qualitative.Bold,
                )
                fig_donut.update_traces(textposition="outside", textinfo="label+percent")
                fig_donut.update_layout(
                    height=340,
                    showlegend=False, margin=dict(t=20, b=10, l=10, r=10),
                )
                st.plotly_chart(fig_donut, use_container_width=True)

    st.divider()

    if "Supplier Name" in filtered.columns:
        st.markdown("##### Supplier OTD Performance (>= 10 lines)")
        if _is_otd_schema:
            _sup_agg = (
                filtered.groupby("Supplier Name")
                .agg(Lines=("OTD Miss (Late)", "count"), Late=("OTD Miss (Late)", "sum"))
                .reset_index()
            )
            _sup_agg["On-Time %"] = ((1 - _sup_agg["Late"] / _sup_agg["Lines"]) * 100).round(1)
        else:
            _sup_agg = (
                filtered.groupby("Supplier Name")
                .agg(Lines=("completion_pct", "count"), OnTime_pct=("completion_pct", "mean"))
                .reset_index()
            )
            _sup_agg["Late"] = (_sup_agg["Lines"] * (1 - _sup_agg["OnTime_pct"] / 100)).round(0).astype(int)
            _sup_agg["On-Time %"] = _sup_agg["OnTime_pct"].round(1)

        _sup_agg = _sup_agg[_sup_agg["Lines"] >= 10].sort_values("On-Time %")
        st.dataframe(
            _sup_agg[["Supplier Name", "Lines", "Late", "On-Time %"]],
            use_container_width=True, hide_index=True,
        )

# ── AT RISK ───────────────────────────────────────────────────────────────────
with tab_risk:
    st.subheader("Open Lines at Risk")
    if not _is_otd_schema or _status_col is None or _open.empty:
        st.info(
            "At-Risk analysis requires the OTD Export schema "
            "(Status Code + Adjusted Promise Date columns). "
            "Switch to the bundled OTD file or upload an OTD Export."
        )
    else:
        _open_df = _open.copy()

        _prom_col = next(
            (c for c in ["Adjusted Promise Date", "Promised Date"] if c in _open_df.columns),
            None,
        )
        if _prom_col:
            _prom_dt = _coerce_date_series(_open_df[_prom_col])
            _open_df["Days to Promise"] = (_prom_dt - _TODAY).dt.days
        else:
            _open_df["Days to Promise"] = np.nan

        def _urgency_label(row: pd.Series) -> str:
            d = row.get("Days to Promise", np.nan)
            miss = row.get("OTD Miss (Late)", 0)
            if miss == 1 or (pd.notna(d) and d < 0):
                return "OVERDUE"
            if pd.notna(d) and d <= 7:
                return "Due <= 7d"
            if pd.notna(d) and d <= 14:
                return "Due <= 14d"
            return "On Track"

        _open_df["Urgency"] = _open_df.apply(_urgency_label, axis=1)
        _urg_order = ["OVERDUE", "Due <= 7d", "Due <= 14d", "On Track"]
        _urg_counts = _open_df["Urgency"].value_counts()

        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Overdue",    _urg_counts.get("OVERDUE",    0))
        r2.metric("Due <= 7d",  _urg_counts.get("Due <= 7d",  0))
        r3.metric("Due <= 14d", _urg_counts.get("Due <= 14d", 0))
        r4.metric("On Track",   _urg_counts.get("On Track",   0))

        st.markdown("---")

        _urg_filter = st.multiselect(
            "Show urgency levels", _urg_order,
            default=_urg_order[:3], key="otd_urg_f",
        )
        _show_open = (
            _open_df[_open_df["Urgency"].isin(_urg_filter)]
            .sort_values("Days to Promise", ascending=True, na_position="last")
        )

        _risk_cols = [c for c in [
            "Urgency", "Days to Promise", "SO No", "Line No", "Part", "Description",
            "Supplier Name", "Customer", "Part Class", "Part Pur/Fab",
            "Adjusted Promise Date", "OTD Miss (Late)", "Days Late",
            "OTDClass_Updated", "Failure Reason",
            "Qty", "Available Qty", "On Hand Qty",
        ] if c in _show_open.columns]

        st.caption(f"{len(_show_open):,} open lines in selected urgency levels")
        st.dataframe(_show_open[_risk_cols], use_container_width=True, hide_index=True)

# ── BRAIN CLUSTERS ────────────────────────────────────────────────────────────
with tab_brain:
    st.subheader("Brain -- Recursive Cluster Analysis")
    st.caption(
        "The Brain applies TF-IDF + KMeans to build a hierarchical cluster tree. "
        "Each cluster groups similar OTD failure patterns."
    )
    b1, b2 = st.tabs(["Sunburst", "Cluster Bars"])

    with b1:
        rendered = False
        if "cluster_path" in filtered.columns and not filtered.empty:
            _filt_cp = filtered.copy()
            _filt_cp["cluster_path"] = _filt_cp["cluster_path"].fillna("Unknown").astype(str)
            paths = _filt_cp["cluster_path"].str.split("/")
            max_depth = max((len(p) for p in paths), default=1)
            path_cols = []
            for d in range(min(max_depth, 4)):
                cname = f"_lvl_{d}"
                _filt_cp[cname] = _filt_cp["cluster_path"].apply(
                    lambda x, _d=d: x.split("/")[_d] if len(x.split("/")) > _d else ""
                )
                path_cols.append(cname)

            if path_cols:
                try:
                    fig_sun = px.sunburst(
                        _filt_cp, path=path_cols[:3],
                        color=path_cols[-1],
                        title="OTD Cluster Hierarchy", template="plotly",
                    )
                    fig_sun.update_layout(height=600)
                    st.plotly_chart(fig_sun, use_container_width=True)
                    rendered = True
                except Exception as e:
                    st.warning(f"Sunburst error: {e}")

        if not rendered and not filtered.empty:
            grp_col = "cluster_path" if "cluster_path" in filtered.columns else filtered.columns[0]
            sizes = (
                filtered.groupby(grp_col).size()
                .reset_index(name="records").nlargest(40, "records")
            )
            try:
                fig_tm = px.treemap(
                    sizes, path=[grp_col], values="records",
                    color="records", color_continuous_scale="Viridis",
                    template="plotly", title="Top-40 Clusters by Record Count",
                )
                fig_tm.update_layout(height=550)
                st.plotly_chart(fig_tm, use_container_width=True)
            except Exception:
                st.dataframe(sizes, use_container_width=True)

    with b2:
        sum_df = pd.DataFrame(summaries) if summaries else pd.DataFrame()
        if sum_df.empty and not filtered.empty and "cluster_path" in filtered.columns:
            sum_df = filtered.groupby("cluster_path").size().reset_index(name="size")

        if not sum_df.empty and "cluster_path" in sum_df.columns and "size" in sum_df.columns:
            if "cluster_path" in filtered.columns:
                _cp_agg = (
                    filtered.groupby("cluster_path")["completion_pct"]
                    .mean().reset_index(name="otd_pct")
                )
                sum_df = sum_df.merge(_cp_agg, on="cluster_path", how="left")
                sum_df["otd_pct"] = sum_df["otd_pct"].fillna(0)
            else:
                sum_df["otd_pct"] = 0

            fig_bar = px.bar(
                sum_df.nlargest(30, "size"),
                x="cluster_path", y="size",
                color="otd_pct", color_continuous_scale="RdYlGn", range_color=[0, 100],
                title="Top-30 Clusters by Size (color = On-Time %)",
                labels={"cluster_path": "Cluster", "size": "Records", "otd_pct": "On-Time %"},
                template="plotly",
            )
            fig_bar.update_layout(
                xaxis_tickangle=-45, height=450,
            )
            _bar_click = st.plotly_chart(fig_bar, use_container_width=True,
                                         key="otd_bar", on_select="rerun")
            if _bar_click and _bar_click.get("selection", {}).get("points"):
                _pt = _bar_click["selection"]["points"][0]
                st.session_state["otd_selected_cluster"] = _pt.get("x", "")
        else:
            st.info("No cluster summary data.")

    record_findings_bulk(
        "otd_recursive", "cluster",
        [{"key": str(r.get("cluster_path", "?")), "score": float(r.get("size", 0)),
          "payload": {"top_keywords": str(r.get("top_keywords", ""))}}
         for r in summaries[:50]],
    )

# ── TREND ─────────────────────────────────────────────────────────────────────
with tab_trend:
    st.subheader("OTD Delivery Trend")
    st.caption(
        "Trend plotted by Order Date (covers all lines including open). "
        "Ship Date is NaT for unshipped lines and would exclude them."
    )

    _trend_candidates = (
        ["Order Date", "Ship Date", "Adjusted Promise Date", "Promised Date"]
        if _is_otd_schema
        else ["ship_day_key", "receipt_date_key", "order_date_key", "ship_date"]
    )
    _trend_avail = [c for c in _trend_candidates if c in filtered.columns]

    if _trend_avail:
        _trend_sel = st.selectbox("Date axis", _trend_avail, key="otd_trend_date")
        try:
            _trend_df = filtered.copy()
            _trend_df["_dt"] = _coerce_date_series(_trend_df[_trend_sel])
            _trend_df = _trend_df.dropna(subset=["_dt"])
            
            _t_s, _t_e = get_global_window()
            _dt_vals = _trend_df["_dt"].dt.date
            _trend_df = _trend_df[(_dt_vals >= _t_s) & (_dt_vals <= _t_e)]
            
            _trend_df["_ym"] = _trend_df["_dt"].dt.to_period("M").astype(str)
            _t_agg = _trend_df.groupby("_ym", as_index=False).agg(
                on_time_pct=("completion_pct", "mean"),
                volume=("_dt", "count"),
            )

            fig_trend = go.Figure()
            fig_trend.add_bar(x=_t_agg["_ym"], y=_t_agg["volume"],
                              name="Lines", marker_color="#334155")
            fig_trend.add_trace(go.Scatter(
                x=_t_agg["_ym"], y=_t_agg["on_time_pct"],
                name="On-Time %", yaxis="y2",
                line=dict(color="#38bdf8", width=3),
                mode="lines+markers", marker=dict(size=6),
            ))
            fig_trend.update_layout(
                yaxis_title="Lines",
                yaxis2=dict(title="On-Time %", overlaying="y", side="right", range=[0, 110]),
                height=440, template="plotly",
                xaxis_tickangle=-45, title=f"OTD Trend by {_trend_sel}",
            )
            st.plotly_chart(fig_trend, use_container_width=True)

            if not _t_agg.empty:
                _latest = _t_agg.iloc[-1]
                _delta_str = ""
                if len(_t_agg) > 1:
                    _diff = _latest["on_time_pct"] - _t_agg.iloc[-2]["on_time_pct"]
                    _delta_str = f" | delta {_diff:+.1f}pp vs prior month"
                st.caption(
                    f"Latest: {_latest['_ym']} | On-Time {_latest['on_time_pct']:.1f}% "
                    f"across {int(_latest['volume']):,} lines{_delta_str}"
                )
        except Exception as e:
            st.info(f"Trend unavailable: {e}")
    else:
        st.info("No date column detected for trend analysis.")

# ── DRIVERS ───────────────────────────────────────────────────────────────────
with tab_drivers:
    st.subheader("OTD Rate Drivers")
    st.caption(
        "Each chart shows on-time % split by an operational variable. "
        "Color: green = high OTD, red = low OTD. "
        "Key findings: Drop Ship 100%, Customer Pickup ~95%, "
        "Low-Volume parts 55-68%, International 71%, Fabricated 74%, "
        "SO with multiple ship dates 70%."
    )

    def _driver_chart(
        df_in: pd.DataFrame, group_col: str, title: str,
        min_lines: int = 5, top_n: int = 20,
    ) -> None:
        if group_col not in df_in.columns:
            return
        _grp = (
            df_in.groupby(group_col, dropna=False)
            .agg(Lines=("completion_pct", "count"), OnTime_pct=("completion_pct", "mean"))
            .reset_index()
        )
        _grp[group_col] = _grp[group_col].fillna("Unknown").astype(str)
        _grp = _grp[_grp["Lines"] >= min_lines].nlargest(top_n, "Lines")
        if _grp.empty:
            return
        _fig = px.bar(
            _grp.sort_values("OnTime_pct"),
            x="OnTime_pct", y=group_col, orientation="h",
            color="OnTime_pct", color_continuous_scale="RdYlGn", range_color=[50, 100],
            text=_grp.sort_values("OnTime_pct")["OnTime_pct"].round(1).astype(str) + "%",
            title=title, template="plotly",
            labels={"OnTime_pct": "On-Time %", group_col: ""},
        )
        _fig.update_layout(
            height=max(280, len(_grp) * 32 + 80),
            coloraxis_showscale=False, xaxis_range=[0, 105],
            margin=dict(t=50, b=10, l=10, r=10),
        )
        _fig.update_traces(textposition="outside")
        st.plotly_chart(_fig, use_container_width=True)

    d1, d2 = st.columns(2)
    with d1:
        _driver_chart(filtered, "Part Class",             "OTD % by Part Class (ABC / Volume Tier)")
        _driver_chart(filtered, "Part Pur/Fab",           "OTD % by Purchased vs Fabricated")
        _driver_chart(filtered, "Domestic/International", "OTD % by Domestic vs International")
        _driver_chart(filtered, "Drop Ship",              "OTD % by Drop Ship (Yes = direct from mfg)")
    with d2:
        _driver_chart(filtered, "Freight Terms",              "OTD % by Freight Terms")
        _driver_chart(filtered, "OTDClass_Updated",           "OTD % by F-Code Class")
        _driver_chart(filtered, "SO Has Multiple Ship Dates", "OTD % - SO Multi-Ship-Date Flag")

    st.markdown("---")
    if "Supplier Name" in filtered.columns:
        _driver_chart(
            filtered, "Supplier Name",
            "OTD % by Supplier (min 10 lines, top 25 by volume)",
            min_lines=10, top_n=25,
        )

    if "Order Date" in filtered.columns and "Part Class" in filtered.columns:
        st.markdown("---")
        st.markdown("##### OTD % by Part Class x Order Month")
        try:
            _hm = filtered.copy()
            _hm["_ym"] = _coerce_date_series(_hm["Order Date"]).dt.to_period("M").astype(str)
            _pivot = (
                _hm.groupby(["Part Class", "_ym"])["completion_pct"]
                .mean().round(1).unstack(fill_value=np.nan)
            )
            _pivot = _pivot[sorted(_pivot.columns)].iloc[:, -12:]
            fig_hm = px.imshow(
                _pivot, color_continuous_scale="RdYlGn", zmin=50, zmax=100,
                text_auto=".0f", template="plotly",
                labels={"color": "On-Time %"},
                title="On-Time % -- Part Class x Order Month (last 12 months)",
            )
            fig_hm.update_layout(height=300, coloraxis_showscale=True)
            st.plotly_chart(fig_hm, use_container_width=True)
        except Exception as e:
            st.info(f"Heatmap unavailable: {e}")

# ── DETAIL RECORDS ────────────────────────────────────────────────────────────
with tab_detail:
    st.subheader("Detail Records")

    _sel_cluster = st.session_state.get("otd_selected_cluster", "")
    if _sel_cluster and "cluster_path" in filtered.columns:
        st.info(f"Filtered to cluster: **{_sel_cluster}**")
        show_df = filtered[filtered["cluster_path"].astype(str) == _sel_cluster].copy()
    else:
        show_df = filtered.copy()

    if _is_otd_file_schema(show_df):
        detail_cols = [c for c in _OTD_DETAIL_COLS if c in show_df.columns]
    else:
        detail_cols = list(show_df.columns)

    search_term = st.text_input("Search (any text column)", key="otd_detail_search")
    if search_term:
        _mask = pd.Series(False, index=show_df.index)
        for _c in show_df.select_dtypes(include="object").columns:
            _mask |= show_df[_c].astype(str).str.contains(search_term, case=False, na=False)
        show_df = show_df[_mask]

    st.caption(f"{len(show_df):,} rows | {len(detail_cols)} columns")
    st.dataframe(show_df[detail_cols], use_container_width=True, hide_index=True)

# ── OWNERSHIP ─────────────────────────────────────────────────────────────────
with tab_daily:
    st.subheader("Daily Review Worklists")
    if not daily_worklists:
        st.info("No worklist sheets (e.g. 'Missed Yesterday', 'Shipping today', 'Opened Yesterday') found in the provided Excel bundle.")
    else:
        st.markdown("Review and assign ownership for the daily prioritized sheets. Edits are synced to the datastore where available.")
        
        w_tabs = st.tabs(list(daily_worklists.keys()))
        for i, (w_name, w_df) in enumerate(daily_worklists.items()):
            with w_tabs[i]:
                st.caption(f"**{w_name}** ({len(w_df)} records)")
                
                edit_df = w_df.copy()
                if "Review" not in edit_df.columns: edit_df["Review"] = False
                if "Owner" not in edit_df.columns: edit_df["Owner"] = ""
                if "Review Comment" not in edit_df.columns: edit_df["Review Comment"] = ""
                if "Needs Review" not in edit_df.columns: edit_df["Needs Review"] = False
                edit_df["Review"] = edit_df["Review"].fillna(False).astype(bool)
                edit_df["Owner"] = edit_df["Owner"].fillna("").astype(str)
                edit_df["Review Comment"] = edit_df["Review Comment"].fillna("").astype(str)
                edit_df["Needs Review"] = edit_df["Needs Review"].fillna(False).astype(bool)
                
                col_config = {
                    "Review": st.column_config.CheckboxColumn("Review", default=False),
                    "Owner": st.column_config.TextColumn("Owner"),
                    "Review Comment": st.column_config.TextColumn("Review Comment"),
                    "Needs Review": st.column_config.CheckboxColumn("Needs Review?", default=False)
                }
                disabled_cols = [c for c in edit_df.columns if c not in list(col_config.keys())]
                
                edited = st.data_editor(
                    edit_df,
                    use_container_width=True,
                    num_rows="fixed",
                    disabled=disabled_cols,
                    column_config=col_config,
                    key=f"editor_{w_name}"
                )
                
                if st.button(f"Save '{w_name}' Updates", key=f"btn_save_{w_name}"):
                    changes = edit_df.compare(edited) if hasattr(edit_df, "compare") else edited[edited != edit_df].dropna(how="all")
                    if "conn" in st.session_state and not changes.empty:
                        with st.spinner("Syncing to backend..."):
                            for idx2 in changes.index:
                                row_sel = edited.loc[idx2]
                                order_nr = str(row_sel.get("Order Number", ""))
                                part_nr = str(row_sel.get("Part Number", ""))
                                site_v = str(row_sel.get("Site", ""))
                                upsert_otd_owner(
                                    st.session_state.conn,
                                    order_nr, part_nr, site_v,
                                    owner=row_sel.get("Owner", ""),
                                    reason=row_sel.get("Review Comment", ""),
                                    at_risk=row_sel.get("Needs Review", False)
                                )
                        st.success(f"Updates for {w_name} saved!")
                    elif not changes.empty:
                        st.warning("No backend connection available, but your edits are captured locally in the session.")
                    else:
                        st.info("No modifications detected.")