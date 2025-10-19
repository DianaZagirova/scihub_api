#!/usr/bin/env python3
"""
Quick script to seed tracker from papers.db
"""
import sys
import sqlite3
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from trackers.doi_tracker_db import DOITracker

def main():
    papers_db = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
    tracker_db = 'processing_tracker.db'
    
    print('Seeding tracker from papers.db...')
    print('='*60)
    
    # Initialize tracker
    tracker = DOITracker(db_path=tracker_db)
    
    # Get existing DOIs
    existing = set(tracker.get_all_statuses().keys())
    print(f'Tracker currently has: {len(existing)} DOIs')
    
    # Get DOIs from papers.db
    papers_conn = sqlite3.connect(papers_db)
    cur = papers_conn.cursor()
    cur.execute("SELECT doi FROM papers WHERE doi IS NOT NULL AND doi != ''")
    papers_dois = {row[0] for row in cur.fetchall()}
    papers_conn.close()
    
    print(f'Papers.db has: {len(papers_dois)} DOIs')
    
    # Find missing
    missing = papers_dois - existing
    print(f'Missing in tracker: {len(missing)} DOIs')
    
    if not missing:
        print('\n✓ All DOIs already in tracker!')
        return 0
    
    print(f'\nSample missing DOIs:')
    for doi in list(missing)[:10]:
        print(f'  - {doi}')
    
    # Seed them
    print(f'\nSeeding {len(missing)} DOIs...')
    for i, doi in enumerate(missing, 1):
        tracker.update_status(doi=doi)
        if i % 1000 == 0:
            print(f'  Progress: {i}/{len(missing)}')
    
    print(f'\n✓ Successfully seeded {len(missing)} DOIs!')
    
    # Verify specific DOIs
    test_dois = [
        '10.1126/sageke.2001.12.vp9',
        '10.1093/gerona/59.6.m606',
        '10.18632/aging.100148'
    ]
    
    print(f'\nVerifying test DOIs:')
    for doi in test_dois:
        status = tracker.get_status(doi)
        if status:
            print(f'  ✓ {doi}')
        else:
            print(f'  ✗ {doi} - NOT FOUND')
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
