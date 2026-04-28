"""WIP Aging Review — Lion's manual workflow, reborn as a programmatic engine.

Source: Francesco Bernacchia / Brenda — Manufacturers Rd & Wilson Rd WIP review
process recorded 2026-04-28 (see docs/Lions Lectures/WIP Review).

Public API:
    AGING_BUCKETS, BACKOFFICE_TRANSFER_MODIFIERS
    endpoints.SOURCE_REPORTS
    wip_aging.build_wip_analysis(...)
    wip_aging.compute_kpis(...)
    wip_export.export_workbook(...)
"""

from .wip_aging import (
    AGING_BUCKETS,
    BACKOFFICE_TRANSFER_MODIFIERS,
    build_wip_analysis,
    compute_kpis,
    load_source,
)
from .wip_export import export_workbook
from . import endpoints

__all__ = [
    "AGING_BUCKETS",
    "BACKOFFICE_TRANSFER_MODIFIERS",
    "build_wip_analysis",
    "compute_kpis",
    "load_source",
    "export_workbook",
    "endpoints",
]
