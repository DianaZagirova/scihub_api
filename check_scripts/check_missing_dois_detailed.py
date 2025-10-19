#!/usr/bin/env python3
"""Detailed check: Find which specific DOIs are in papers.db but not in tracker."""

import sqlite3
import csv
from doi_tracker import DOITracker

DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
TRACKER_PATH = 'doi_processing_tracker.csv'

def main():
    print("Loading database DOIs...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT doi FROM papers WHERE doi IS NOT NULL AND doi != ''")
    db_dois = set(row[0].strip() for row in cursor.fetchall())
    conn.close()
    
    print(f"  ✓ Loaded {len(db_dois):,} DOIs from database")
    
    print("\nLoading tracker DOIs...")
    # Read tracker directly to avoid cache issues
    tracker_dois = set()
    with open(TRACKER_PATH, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('doi'):
                tracker_dois.add(row['doi'].strip())
    
    print(f"  ✓ Loaded {len(tracker_dois):,} DOIs from tracker")
    
    # Find missing DOIs
    missing_dois = db_dois - tracker_dois
    
    print('='*70)
    print('DETAILED DOI COMPARISON')
    print('='*70)
    print(f'DOIs in database:        {len(db_dois):,}')
    print(f'DOIs in tracker:         {len(tracker_dois):,}')
    print(f'Missing from tracker:    {len(missing_dois):,}')
    print('='*70)
    
    if missing_dois:
        print(f'\n⚠️  {len(missing_dois):,} DOIs are in database but NOT in tracker')
        
        # Show first 20 examples
        print('\nFirst 20 missing DOIs:')
        for i, doi in enumerate(sorted(missing_dois)[:20], 1):
            print(f'  {i:2d}. {doi}')
        
        if len(missing_dois) > 20:
            print(f'  ... and {len(missing_dois) - 20:,} more')
        
        # Save all missing DOIs to file
        output_file = 'missing_dois/missing_from_tracker.txt'
        with open(output_file, 'w') as f:
            for doi in sorted(missing_dois):
                f.write(f'{doi}\n')
        print(f'\n✓ All missing DOIs saved to: {output_file}')
        print(f'\nTo add them to tracker, run:')
        print(f'  python add_new_dois_to_tracker.py')
    else:
        print('\n✅ All database DOIs are present in the tracker!')
    
    # Check for DOIs in tracker but not in database (bonus check)
    extra_dois = tracker_dois - db_dois
    if extra_dois:
        print(f'\nℹ️  Note: {len(extra_dois):,} DOIs are in tracker but NOT in database')
        print('(This may be normal if DOIs were added from other sources)')

if __name__ == '__main__':
    main()
