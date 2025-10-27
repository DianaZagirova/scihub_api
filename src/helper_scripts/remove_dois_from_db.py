#!/usr/bin/env python3
"""
Remove DOIs from processing_tracker.db based on a list in remove_dois.txt

This script:
1. Reads DOIs from remove_dois.txt
2. Deletes corresponding entries from processing_tracker table
3. Reports statistics on removed entries
"""

import sqlite3
import sys
from pathlib import Path

def read_dois_from_file(filepath: str) -> list[str]:
    """Read DOIs from text file, one per line."""
    dois = []
    with open(filepath, 'r') as f:
        for line in f:
            doi = line.strip()
            if doi:
                dois.append(doi)
    return dois

def remove_dois_from_db(db_path: str, dois: list[str], dry_run: bool = False):
    """Remove DOIs from processing_tracker database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check which DOIs exist in the database
    existing_dois = []
    for doi in dois:
        cursor.execute("SELECT doi FROM processing_tracker WHERE doi = ?", (doi,))
        if cursor.fetchone():
            existing_dois.append(doi)
    
    print(f"\nDOIs to remove: {len(dois)}")
    print(f"DOIs found in database: {len(existing_dois)}")
    print(f"DOIs not in database: {len(dois) - len(existing_dois)}")
    
    if not existing_dois:
        print("\nNo DOIs to remove from database.")
        conn.close()
        return 0
    
    if dry_run:
        print("\n[DRY RUN] Would remove the following DOIs:")
        for doi in existing_dois:
            print(f"  - {doi}")
        conn.close()
        return len(existing_dois)
    
    # Remove DOIs
    removed_count = 0
    for doi in existing_dois:
        cursor.execute("DELETE FROM processing_tracker WHERE doi = ?", (doi,))
        if cursor.rowcount > 0:
            removed_count += 1
            print(f"Removed: {doi}")
    
    conn.commit()
    conn.close()
    
    return removed_count

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Remove DOIs from processing_tracker.db based on remove_dois.txt'
    )
    parser.add_argument(
        '--tracker-db',
        default='processing_tracker.db',
        help='Path to processing_tracker.db (default: processing_tracker.db)'
    )
    parser.add_argument(
        '--dois-file',
        default='remove_dois.txt',
        help='Path to file containing DOIs to remove (default: remove_dois.txt)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be removed without actually removing'
    )
    
    args = parser.parse_args()
    
    # Check if files exist
    if not Path(args.tracker_db).exists():
        print(f"Error: Database file not found: {args.tracker_db}")
        sys.exit(1)
    
    if not Path(args.dois_file).exists():
        print(f"Error: DOIs file not found: {args.dois_file}")
        sys.exit(1)
    
    # Read DOIs
    print(f"Reading DOIs from: {args.dois_file}")
    dois = read_dois_from_file(args.dois_file)
    print(f"Found {len(dois)} DOIs in file")
    
    # Remove from database
    print(f"\nProcessing database: {args.tracker_db}")
    removed = remove_dois_from_db(args.tracker_db, dois, args.dry_run)
    
    if args.dry_run:
        print(f"\n[DRY RUN] Would remove {removed} DOIs from database")
    else:
        print(f"\n✓ Successfully removed {removed} DOIs from database")

if __name__ == '__main__':
    main()
