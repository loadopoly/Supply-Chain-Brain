import sys
import numpy as np
sys.path.insert(0, './pipeline')
import pandas as pd
from scipy.sparse import issparse
from src.brain.otd_recursive import build_features, recursive_cluster, OTDConfig

df = pd.read_excel("pipeline/docs/OTD file.xlsx", nrows=100)

cfg = OTDConfig(text_col="Description", site_col="Site", max_depth=2, max_k=5)
cfg.numeric_cols = ["Qty"]
cfg.categorical_cols = ["Supplier Name"]

work = df.copy().reset_index(drop=True)
work[cfg.text_col] = work[cfg.text_col].fillna("")
feats = build_features(work, cfg)
feats_dense = feats.toarray() if issparse(feats) else np.asarray(feats)
assignments, summaries = recursive_cluster(work, feats_dense, cfg)
print("Clusters assigned:", assignments.nunique())
print("Summaries:", len(summaries))
