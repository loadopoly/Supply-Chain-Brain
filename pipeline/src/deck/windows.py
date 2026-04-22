"""
Window convention (spec §4).

  T                = analysis-run day (Monday preferred)
  [T-28, T-15]     = Prior 14d     baseline for delta
  [T-14, T-1]      = Past 14d      headline KPI window
  [T,   T+14]      = Future 14d    forecast horizon
  [T-90, T-1]      = 90d           site scorecard baseline

Weekend / holiday exclusion applies to daily KPI aggregations only
(zero-line days otherwise compute as 100%).
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date, timedelta
import pandas as pd


@dataclass(frozen=True)
class Windows:
    T: date

    @property
    def prior_14d(self) -> tuple[date, date]:
        return (self.T - timedelta(days=28), self.T - timedelta(days=15))

    @property
    def past_14d(self) -> tuple[date, date]:
        return (self.T - timedelta(days=14), self.T - timedelta(days=1))

    @property
    def future_14d(self) -> tuple[date, date]:
        return (self.T, self.T + timedelta(days=14))

    @property
    def baseline_90d(self) -> tuple[date, date]:
        return (self.T - timedelta(days=90), self.T - timedelta(days=1))

    def as_dict(self) -> dict:
        return {
            "T": self.T.isoformat(),
            "prior_14d": [d.isoformat() for d in self.prior_14d],
            "past_14d":  [d.isoformat() for d in self.past_14d],
            "future_14d":[d.isoformat() for d in self.future_14d],
            "baseline_90d":[d.isoformat() for d in self.baseline_90d],
        }


def default_anchor(today: date | None = None) -> date:
    """Monday anchor preferred for review cadence (§4)."""
    d = today or date.today()
    return d - timedelta(days=d.weekday())


def mask_between(s: pd.Series, lo: date, hi: date) -> pd.Series:
    """Inclusive date range mask — works on datetime64 columns."""
    s = pd.to_datetime(s, errors="coerce")
    return (s.dt.date >= lo) & (s.dt.date <= hi)


def business_days(lo: date, hi: date) -> int:
    """Weekday count in [lo, hi] inclusive. Holidays ignored."""
    return int(pd.bdate_range(lo, hi).size)
