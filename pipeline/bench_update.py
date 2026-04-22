import sys
import numpy as np
import pandas as pd

with open('pipeline/bench/bench_brain.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Add a function to generate mock wo/po/so, then benchmark graph building
synth_func = """
def synth_value_stream(n: int, seed: int=42):
    rng = np.random.default_rng(seed)
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
        'due_date_key': pd.Timestamp('2024-01-01') + pd.to_timedelta(rng.integers(0, 365, pn), unit='D')
    })
    wo = pd.DataFrame({
        'wo_number': [f'WO{i:05d}' for i in range(pn)],
        'part_key': rng.choice(parts['part_key'], size=pn),
        'due_date_key': pd.Timestamp('2024-01-01') + pd.to_timedelta(rng.integers(0, 365, pn), unit='D')
    })
    so = pd.DataFrame({
        'sales_order_number': [f'SO{i:05d}' for i in range(pn)],
        'part_key': rng.choice(parts['part_key'], size=pn),
        'customer_key': rng.choice([f'C{i:03d}' for i in range(50)], size=pn),
        'promised_ship_day_key': pd.Timestamp('2024-01-01') + pd.to_timedelta(rng.integers(0, 365, pn), unit='D')
    })
    return parts, po, wo, so
"""

replace_idx_synth = text.find('# --- bench harness')
if replace_idx_synth != -1:
    text = text[:replace_idx_synth] + synth_func + '\n' + text[replace_idx_synth:]

bench_call = """
    parts, po, wo, so = synth_value_stream(n_nodes)
    def bench_vs_graph():
        g2 = NetworkXBackend()
        g2.add_parts(parts, id_col="part_key", label_col="part_key")
        g2.add_edges(po, "buy_order", "po_number", "part", "part_key", "procured")
        g2.add_edges(wo, "make_order", "wo_number", "part", "part_key", "manufactured")
        g2.add_edges(so, "part", "part_key", "sell_order", "sales_order_number", "sold")
        return g2

    bench("graph.value_stream_build", f"nodes={n_nodes}", bench_vs_graph)
"""

replace_idx_bench = text.find('bench("graph.degree_centrality"')
if replace_idx_bench != -1:
    text = text[:replace_idx_bench] + bench_call + '    ' + text[replace_idx_bench:]

with open('pipeline/bench/bench_brain.py', 'w', encoding='utf-8') as f:
    f.write(text)
