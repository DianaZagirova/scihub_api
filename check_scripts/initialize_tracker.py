#!/usr/bin/env python3
"""
Initialize the DOI tracker from existing papers.db
"""

import sqlite3
from doi_tracker import DOITracker
from pathlib import Path


def initialize_tracker_from_database(
    db_path: str = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db',
    tracker_file: str = 'doi_processing_tracker.csv'
):
    """
    Initialize the DOI tracker by importing all DOIs from papers.db.
    
    This will:
    1. Read all DOIs and their current parsing_status from papers.db
    2. Populate the tracker with this information
    3. Create a comprehensive tracking file
    """
    
    print("="*70)
    print("INITIALIZING DOI TRACKER FROM DATABASE")
    print("="*70)
    
    # Create tracker
    tracker = DOITracker(tracker_file)
    
    # Connect to database
    print(f"\nConnecting to database: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get total DOIs
    cursor.execute("SELECT COUNT(*) FROM papers WHERE doi IS NOT NULL AND doi != ''")
    total_dois = cursor.fetchone()[0]
    print(f"Total DOIs in database: {total_dois:,}")
    
    # Read all DOIs with their parsing status at once
    print("\nReading all DOI statuses from database...")
    cursor.execute("""
        SELECT doi, parsing_status, abstract, full_text_sections
        FROM papers 
        WHERE doi IS NOT NULL AND doi != ''
    """)
    
    all_rows = cursor.fetchall()
    print(f"Loaded {len(all_rows):,} DOIs from database")
    
    # Build bulk updates list in memory
    print("\nParsing statuses...")
    bulk_updates = []
    
    for i, (doi, parsing_status, abstract, full_text) in enumerate(all_rows):
        if (i + 1) % 10000 == 0:
            print(f"  Parsed {i+1:,}/{len(all_rows):,} DOIs...")
        
        # Parse the parsing_status to determine tracker fields
        status_data = {'doi': doi}
        
        if parsing_status:
            ps_lower = parsing_status.lower()
            
            # Determine if downloaded (has PDF)
            if 'success' in ps_lower or 'parser' in ps_lower:
                status_data['downloaded'] = tracker.AVAILABLE_YES
                status_data['scihub_available'] = tracker.AVAILABLE_YES
            elif 'not_found' in ps_lower:
                status_data['scihub_available'] = tracker.AVAILABLE_NO
                status_data['downloaded'] = tracker.AVAILABLE_NO
            elif 'download_failed' in ps_lower or 'failed' in ps_lower:
                status_data['downloaded'] = tracker.AVAILABLE_NO
            
            # PyMuPDF status
            if 'pymupdf' in ps_lower:
                if 'success' in ps_lower:
                    status_data['pymupdf_status'] = tracker.STATUS_SUCCESS
                elif 'failed' in ps_lower:
                    status_data['pymupdf_status'] = tracker.STATUS_FAILED
                else:
                    status_data['pymupdf_status'] = tracker.STATUS_NOT_ATTEMPTED
            
            # Grobid status
            if 'grobid' in ps_lower:
                if 'success' in ps_lower:
                    status_data['grobid_status'] = tracker.STATUS_SUCCESS
                elif 'failed' in ps_lower:
                    status_data['grobid_status'] = tracker.STATUS_FAILED
                else:
                    status_data['grobid_status'] = tracker.STATUS_NOT_ATTEMPTED
            
            # Already populated papers
            if 'already populated' in ps_lower:
                status_data['downloaded'] = tracker.AVAILABLE_YES
                status_data['scihub_available'] = tracker.AVAILABLE_YES
                # Check if we have abstract/full_text to infer processing
                if abstract or full_text:
                    status_data['pymupdf_status'] = tracker.STATUS_SUCCESS
            
            # No DOI available
            if 'no doi' in ps_lower:
                continue  # Skip these
            
            # Not processed
            if 'not processed' in ps_lower:
                status_data['pymupdf_status'] = tracker.STATUS_NOT_ATTEMPTED
                status_data['grobid_status'] = tracker.STATUS_NOT_ATTEMPTED
        
        # Add to bulk updates list
        if len(status_data) > 1:  # More than just 'doi'
            bulk_updates.append(status_data)
    
    print(f"\nUpdating tracker with {len(bulk_updates):,} DOIs...")
    # Bulk update with deferred write for speed
    tracker.bulk_update(bulk_updates, defer_write=True)
    
    # Write to file once at the end
    print("Writing tracker file...")
    tracker.flush()
    
    conn.close()
    
    print(f"\nTotal DOIs processed: {len(bulk_updates):,}")
    print(f"Tracker file: {tracker_file}")
    
    # Show statistics
    print("\n" + "="*70)
    tracker.print_statistics()
    
    return tracker


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Initialize DOI tracker from papers.db'
    )
    parser.add_argument(
        '--db',
        default='/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db',
        help='Path to papers.db'
    )
    parser.add_argument(
        '--tracker-file',
        default='doi_processing_tracker.csv',
        help='Path to output tracker CSV file'
    )
    
    args = parser.parse_args()
    
    initialize_tracker_from_database(args.db, args.tracker_file)
