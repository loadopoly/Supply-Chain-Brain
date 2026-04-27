"""
deck — Cross-Dataset Supply-Chain Review deck generator.

Implements the pipeline described in CrossDataset_Agent_Process_Spec.md:
 - §2   canonical dataset contracts (OTD, IFR, ITR, PFEP)
 - §3   ERP translation layer (Epicor 9 / Oracle Fusion / Syteline)
 - §4   fixed window convention (T-28..T-15 vs T-14..T-1)
 - §5   eight-phase pipeline (ingest → KPI → decomp → join → centrality
        → realizations → pathways → render)
 - §6   realization rules R1-R10
 - §7   systemic / operational pathway classification
 - §8a  JSON payload (source of truth)
 - §8b  PPTX renderer (16-slide portfolio / 14-slide single-site)

Entry points
------------
    from src.deck import build_findings, render_pptx
    findings = build_findings(otd_df, ifr_df, itr_df, pfep_df, site="ALL")
    render_pptx(findings, out_path="cross_dataset_review.pptx")

Reproducibility seed = 9 for every stochastic step (see §6).
Anchor policy       = AST-INV-PRO-0001.
"""
from .constants import SEED, ANCHOR_POLICY
from .findings import build_findings
from .builder import render_pptx
from .biweekly_one_pager import render_biweekly_one_pager

__all__ = ["build_findings", "render_pptx", "render_biweekly_one_pager", "SEED", "ANCHOR_POLICY"]
