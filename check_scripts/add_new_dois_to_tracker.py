#!/usr/bin/env python3
"""
Add new DOIs from database to tracker (only new ones, with progress).
"""

import sqlite3
from doi_tracker import DOITracker
from tqdm import tqdm

DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
TRACKER_PATH = 'doi_processing_tracker.csv'

def add_new_dois():
    """Add only new DOIs from database to tracker."""
    
    print("="*70)
    print("ADDING NEW DOIs TO TRACKER")
    print("="*70)
    
    # Load tracker
    print(f"\n1. Loading tracker: {TRACKER_PATH}")
    tracker = DOITracker(TRACKER_PATH)
    tracker._ensure_cache_loaded()
    existing_dois = set(tracker._cache.keys())
    print(f"   ✓ Found {len(existing_dois)} DOIs in tracker")
    
    # Query database
    print(f"\n2. Querying database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT doi, parsing_status 
        FROM papers 
        WHERE doi IS NOT NULL AND doi != ''
    """)
    
    all_dois = cursor.fetchall()
    print(f"   ✓ Found {len(all_dois)} DOIs in database")
    
    # Find new DOIs
    print(f"\n3. Finding new DOIs...")
    new_dois = []
    for doi, parsing_status in all_dois:
        if doi not in existing_dois:
            new_dois.append((doi, parsing_status))
    
    print(f"   ✓ Found {len(new_dois)} NEW DOIs to add")
    
    if not new_dois:
        print("\n✓ No new DOIs to add. Tracker is up to date!")
        conn.close()
        return 0
    
    # Add new DOIs with progress
    print(f"\n4. Adding new DOIs to tracker...")
    
    updates = []
    for doi, parsing_status in tqdm(new_dois, desc="Processing"):
        # Parse parsing_status to determine tracker fields
        status_data = tracker._parse_parsing_status(parsing_status)
        
        if status_data:
            updates.append({
                'doi': doi,
                **status_data
            })
        else:
            # No parsing status - add with defaults
            updates.append({
                'doi': doi,
                'scihub_available': 'unknown',
                'downloaded': 'unknown',
                'pymupdf_status': 'not_attempted',
                'grobid_status': 'not_attempted'
            })
    
    # Bulk update (faster than individual updates)
    print(f"\n5. Writing to tracker (bulk update)...")
    tracker.bulk_update(updates, defer_write=False)
    
    print(f"\n6. Flushing to disk...")
    tracker.flush()
    
    conn.close()
    
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"  Total DOIs in database: {len(all_dois)}")
    print(f"  Already in tracker: {len(existing_dois)}")
    print(f"  Added to tracker: {len(new_dois)}")
    print(f"  New tracker size: {len(existing_dois) + len(new_dois)}")
    print("="*70)
    print("\n✓ Done! Tracker updated successfully.")
    
    return len(new_dois)

if __name__ == '__main__':
    try:
        added = add_new_dois()
        exit(0 if added >= 0 else 1)
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
