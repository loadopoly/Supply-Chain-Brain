import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Set working directory
os.chdir(r"C:\Users\agard\OneDrive - astecindustries.com\VS Code\pipeline")
sys.path.insert(0, '.')

try:
    print("Importing src.brain.graph_context...")
    from src.brain.graph_context import GraphContext, HAS_NX
    
    if not HAS_NX:
        print("NetworkX not available. Skipping graph smoke test.")
        sys.exit(0)
        
    print("\nBuilding synthetic value stream data for Graph tests...")
    rng = np.random.default_rng(42)
    n = 100
    parts = pd.DataFrame({
        'part_key': [f'P{i:05d}' for i in range(n)],
        'part_type': rng.choice(['Make', 'Buy', 'Phantom'], size=n),
        'business_unit_key': rng.choice(['PLANT01', 'PLANT02'], size=n)
    })
    pn = n//2
    po = pd.DataFrame({
        'po_number': [f'PO{i:05d}' for i in range(pn)],
        'part_key': rng.choice(parts['part_key'], size=pn),
        'supplier_key': rng.choice([f'S{i:03d}' for i in range(50)], size=pn),
        'due_date_key': pd.Timestamp('2024-01-01') + pd.to_timedelta(rng.integers(0, 365, pn), unit='D'),
        'is_late': rng.choice([True, False], size=pn),
        'friction': rng.choice([1.0, 2.0], size=pn)
    })
    wo = pd.DataFrame({
        'wo_number': [f'WO{i:05d}' for i in range(pn)],
        'part_key': rng.choice(parts['part_key'], size=pn),
        'due_date_key': pd.Timestamp('2024-01-01') + pd.to_timedelta(rng.integers(0, 365, pn), unit='D'),
        'is_late': rng.choice([True, False], size=pn),
        'friction': rng.choice([1.0, 2.0], size=pn)
    })
    so = pd.DataFrame({
        'sales_order_number': [f'SO{i:05d}' for i in range(pn)],
        'part_key': rng.choice(parts['part_key'], size=pn),
        'customer_key': rng.choice([f'C{i:03d}' for i in range(50)], size=pn),
        'promised_ship_day_key': pd.Timestamp('2024-01-01') + pd.to_timedelta(rng.integers(0, 365, pn), unit='D'),
        'is_late': rng.choice([True, False], size=pn),
        'friction': rng.choice([1.0, 3.0], size=pn)
    })

    print("Instantiating GraphContext...")
    g = GraphContext()
    
    print("Testing add_parts...")
    g.add_parts(parts, id_col='part_key', label_col='part_key')
    
    print("Testing value stream edge creation with formulaic friction...")
    g.add_edges(po, 'buy_order', 'po_number', 'part', 'part_key', 'procured', weight_col='friction')
    g.add_edges(wo, 'make_order', 'wo_number', 'part', 'part_key', 'manufactured', weight_col='friction')
    g.add_edges(so, 'part', 'part_key', 'sell_order', 'sales_order_number', 'sold', weight_col='friction')

    print(f"Graph nodes: {g.g.number_of_nodes()}")
    print(f"Graph edges: {g.g.number_of_edges()}")
    
    # testing MIT centrality
    print("Testing centrality bottleneck mapping...")
    cdf = g.centrality(top_n=5, )
    print(f"Calculated top 5 bottlenecks successfully: {len(cdf)} nodes returned.")
    
    print("\n✓ Graph Value Stream smoke test PASSED")
except Exception as e:
    print(f"\n✗ Graph smoke test FAILED")
    print(f"Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
