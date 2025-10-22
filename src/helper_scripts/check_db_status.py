#!/usr/bin/env python3
"""Check database status and JSON availability"""

import sqlite3
import json
import os

# Check database schema
conn = sqlite3.connect('/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db')
cursor = conn.cursor()

# Get column names
cursor.execute('PRAGMA table_info(papers)')
columns = [col[1] for col in cursor.fetchall()]
print('Database columns:', columns)
print()

# Check papers with JSONs but missing full_text
cursor.execute("""
    SELECT doi, abstract, full_text, full_text_sections, parsing_status 
    FROM papers 
    WHERE doi IS NOT NULL AND doi != ''
    LIMIT 10
""")

print('Sample papers:')
for row in cursor.fetchall():
    doi, abstract, full_text, sections, status = row
    print(f'DOI: {doi}')
    print(f'  Has abstract: {bool(abstract and abstract.strip())}')
    print(f'  Has full_text: {bool(full_text and full_text.strip())}')
    print(f'  Has sections: {bool(sections and sections.strip())}')
    print(f'  Status: {status}')
    
    # Check if JSON exists
    doi_file = doi.replace('/', '_')
    json_path = f'/home/diana.z/hack/scihub_api/output/{doi_file}.json'
    fast_path = f'/home/diana.z/hack/scihub_api/output/{doi_file}_fast.json'
    print(f'  Has .json: {os.path.exists(json_path)}')
    print(f'  Has _fast.json: {os.path.exists(fast_path)}')
    print()

# Now check how many papers have JSONs but missing full_text
print("="*70)
print("Checking papers with JSONs but missing full_text...")
print("="*70)

cursor.execute("""
    SELECT doi 
    FROM papers 
    WHERE doi IS NOT NULL AND doi != ''
    AND (full_text IS NULL OR full_text = '')
    AND (full_text_sections IS NULL OR full_text_sections = '')
""")

missing_dois = [row[0] for row in cursor.fetchall()]
print(f"Total papers missing both full_text and full_text_sections: {len(missing_dois)}")

# Check how many have JSONs
has_json = 0
has_fast = 0
has_either = 0

for doi in missing_dois[:100]:  # Check first 100
    doi_file = doi.replace('/', '_')
    json_path = f'/home/diana.z/hack/scihub_api/output/{doi_file}.json'
    fast_path = f'/home/diana.z/hack/scihub_api/output/{doi_file}_fast.json'
    
    if os.path.exists(json_path):
        has_json += 1
        has_either += 1
    elif os.path.exists(fast_path):
        has_fast += 1
        has_either += 1

print(f"\nOut of first 100 papers missing full_text:")
print(f"  Have .json: {has_json}")
print(f"  Have _fast.json: {has_fast}")
print(f"  Have either: {has_either}")

conn.close()
