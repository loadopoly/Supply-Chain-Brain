#!/usr/bin/env python
"""Smoke test for deck module"""
import sys
import os

# Set working directory
os.chdir(r"C:\Users\agard\OneDrive - astecindustries.com\VS Code\pipeline")
sys.path.insert(0, '.')

try:
    # Import the modules
    print("Importing src.deck.demo.make_all...")
    from src.deck.demo import make_all
    
    print("Importing src.deck.findings.build_findings...")
    from src.deck.findings import build_findings
    
    print("Importing src.deck.builder.dump_findings_json...")
    from src.deck.builder import dump_findings_json
    
    # Build findings with demo data
    print("\nBuilding findings with demo data...")
    demo_data = make_all()
    findings = build_findings(demo_data['otd'], demo_data['ifr'], demo_data['itr'], demo_data['pfep'])
    
    # Ensure snapshots directory exists
    os.makedirs('snapshots', exist_ok=True)
    
    # Write findings to JSON
    output_path = 'snapshots/_demo_findings.json'
    dump_findings_json(findings, output_path)
    
    file_size = os.path.getsize(output_path)
    print(f"\n✓ Smoke test PASSED")
    print(f"✓ All modules imported successfully")
    print(f"✓ Demo findings written to: {output_path}")
    print(f"✓ File size: {file_size} bytes")
    
except Exception as e:
    print(f"\n✗ Smoke test FAILED")
    print(f"Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
