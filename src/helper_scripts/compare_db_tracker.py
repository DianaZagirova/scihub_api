#!/usr/bin/env python3
"""Compare database and tracker to find missing DOIs."""

import sqlite3
from doi_tracker import DOITracker

DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'

# Database DOIs
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM papers WHERE doi IS NOT NULL AND doi != ''")
db_count = cursor.fetchone()[0]
conn.close()

# Tracker DOIs
tracker = DOITracker('doi_processing_tracker.csv')
tracker._ensure_cache_loaded()
tracker_count = len(tracker._cache)

missing = db_count - tracker_count

print('='*70)
print('DATABASE vs TRACKER')
print('='*70)
print(f'DOIs in database:    {db_count:,}')
print(f'DOIs in tracker:     {tracker_count:,}')
print(f'MISSING in tracker:  {missing:,}')
print('='*70)

if missing > 0:
    print(f'\n⚠️  You have {missing:,} DOIs in the database not in the tracker!')
    print(f'\nTo add them, run:')
    print(f'  python add_new_dois_to_tracker.py')
