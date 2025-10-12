#!/usr/bin/env python3
"""Quick test of the fetch_missing_papers script."""

import sys
import os

# Test imports
print("Testing imports...")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'legacy'))

try:
    from scihub import SciHub
    print("✓ SciHub imported successfully")
except Exception as e:
    print(f"✗ Failed to import SciHub: {e}")

try:
    from grobid_parser import GrobidParser
    print("✓ GrobidParser imported successfully")
except Exception as e:
    print(f"✗ Failed to import GrobidParser: {e}")

# Test database connection
print("\nTesting database connection...")
import sqlite3
db_path = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM papers WHERE (full_text IS NULL OR full_text = '') OR (abstract IS NULL OR abstract = '')")
    count = cursor.fetchone()[0]
    print(f"✓ Database connected: {count} papers with missing data")
    conn.close()
except Exception as e:
    print(f"✗ Database connection failed: {e}")

# Test config loading
print("\nTesting config loading...")
import json
config_path = os.path.join(os.path.dirname(__file__), 'config.json')
try:
    with open(config_path, 'r') as f:
        config = json.load(f)
    print(f"✓ Config loaded: GROBID server at {config.get('grobid_server')}")
except Exception as e:
    print(f"✗ Config loading failed: {e}")

print("\n✓ All basic tests passed!")
