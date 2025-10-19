#!/usr/bin/env python3
"""
Migrate current tracker to enhanced schema with OA tracking.
"""

import csv
import sqlite3
from pathlib import Path
from datetime import datetime
from collections import defaultdict

CURRENT_TRACKER = 'doi_processing_tracker.csv'
ENHANCED_TRACKER = 'doi_processing_tracker_enhanced.csv'
BACKUP_TRACKER = f'doi_processing_tracker_backup_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'

def get_oa_urls_from_db():
    """Get DOIs with OA URLs from database."""
    print('\n📊 Loading OA URLs from database...')
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT doi, oa_url,
               CASE WHEN (full_text IS NOT NULL AND full_text != "") 
                    OR (full_text_sections IS NOT NULL AND full_text_sections != "") 
                    THEN 1 ELSE 0 END as has_content
        FROM papers 
        WHERE doi IS NOT NULL AND doi != ""
    """)
    
    oa_info = {}
    for doi, oa_url, has_content in cursor.fetchall():
        oa_info[doi] = {
            'has_oa_url': bool(oa_url and oa_url.strip()),
            'has_content': bool(has_content)
        }
    
    conn.close()
    
    oa_count = sum(1 for info in oa_info.values() if info['has_oa_url'])
    print(f'   ✓ Found {oa_count:,} DOIs with OA URLs')
    
    return oa_info

def migrate_tracker():
    """Migrate tracker to enhanced schema."""
    print('='*70)
    print('MIGRATING TRACKER TO ENHANCED SCHEMA')
    print('='*70)
    
    # Backup
    print(f'\n1. Backing up current tracker...')
    import shutil
    shutil.copy2(CURRENT_TRACKER, BACKUP_TRACKER)
    print(f'   ✓ Backed up to: {BACKUP_TRACKER}')
    
    # Get OA info
    oa_info = get_oa_urls_from_db()
    
    # Load current tracker
    print(f'\n2. Loading current tracker...')
    current_records = []
    with open(CURRENT_TRACKER, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        old_headers = reader.fieldnames
        for row in reader:
            current_records.append(dict(row))
    
    print(f'   ✓ Loaded {len(current_records):,} records')
    print(f'   Old schema: {len(old_headers)} fields')
    
    # Define enhanced schema
    enhanced_headers = [
        'doi',
        'scihub_available',      # yes/no/unknown
        'scihub_downloaded',     # yes/no/unknown (specific to sci-hub)
        'oa_available',          # yes/no (has oa_url in database)
        'oa_downloaded',         # yes/no/unknown (attempted OA download)
        'downloaded',            # yes/no/unknown (any source)
        'download_date',         # ISO format timestamp
        'has_content_in_db',     # yes/no (has full_text or sections in database)
        'pymupdf_status',        # success/failed/not_attempted
        'pymupdf_date',          # ISO format timestamp
        'grobid_status',         # success/failed/not_attempted
        'grobid_date',           # ISO format timestamp
        'last_updated',          # ISO format timestamp
        'error_msg',             # Latest error message
        'retry_count'            # Number of retry attempts
    ]
    
    print(f'   New schema: {len(enhanced_headers)} fields')
    
    # Migrate records
    print(f'\n3. Migrating records...')
    enhanced_records = []
    
    for old_record in current_records:
        doi = old_record['doi']
        db_info = oa_info.get(doi, {'has_oa_url': False, 'has_content': False})
        
        # Build enhanced record
        enhanced_record = {
            'doi': doi,
            'scihub_available': old_record.get('scihub_available', 'unknown'),
            'scihub_downloaded': old_record.get('downloaded', 'unknown'),  # Map old 'downloaded' to scihub_downloaded
            'oa_available': 'yes' if db_info['has_oa_url'] else 'no',
            'oa_downloaded': 'unknown',  # Will be updated by future OA downloads
            'downloaded': old_record.get('downloaded', 'unknown'),
            'download_date': old_record.get('download_date', ''),
            'has_content_in_db': 'yes' if db_info['has_content'] else 'no',
            'pymupdf_status': old_record.get('pymupdf_status', 'not_attempted'),
            'pymupdf_date': old_record.get('pymupdf_date', ''),
            'grobid_status': old_record.get('grobid_status', 'not_attempted'),
            'grobid_date': old_record.get('grobid_date', ''),
            'last_updated': old_record.get('last_updated', datetime.now().isoformat()),
            'error_msg': old_record.get('error_msg', ''),
            'retry_count': old_record.get('retry_count', '0')
        }
        
        enhanced_records.append(enhanced_record)
    
    print(f'   ✓ Migrated {len(enhanced_records):,} records')
    
    # Write enhanced tracker
    print(f'\n4. Writing enhanced tracker...')
    with open(ENHANCED_TRACKER, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=enhanced_headers)
        writer.writeheader()
        writer.writerows(enhanced_records)
    
    print(f'   ✓ Written to: {ENHANCED_TRACKER}')
    
    # Statistics
    print('\n' + '='*70)
    print('MIGRATION STATISTICS')
    print('='*70)
    
    oa_available = sum(1 for r in enhanced_records if r['oa_available'] == 'yes')
    has_content = sum(1 for r in enhanced_records if r['has_content_in_db'] == 'yes')
    downloaded = sum(1 for r in enhanced_records if r['downloaded'] == 'yes')
    
    print(f'Total records: {len(enhanced_records):,}')
    print(f'  OA available: {oa_available:,} ({oa_available/len(enhanced_records)*100:.1f}%)')
    print(f'  Has content in DB: {has_content:,} ({has_content/len(enhanced_records)*100:.1f}%)')
    print(f'  Downloaded: {downloaded:,} ({downloaded/len(enhanced_records)*100:.1f}%)')
    
    print('\n' + '='*70)
    print('NEXT STEPS')
    print('='*70)
    print(f'1. Review enhanced tracker:')
    print(f'   head -20 {ENHANCED_TRACKER}')
    print(f'\n2. If satisfied, activate it:')
    print(f'   mv {CURRENT_TRACKER} doi_processing_tracker_old_schema.csv')
    print(f'   mv {ENHANCED_TRACKER} {CURRENT_TRACKER}')
    print(f'\n3. Backup is saved at:')
    print(f'   {BACKUP_TRACKER}')
    print('='*70)

if __name__ == '__main__':
    import sys
    try:
        migrate_tracker()
        sys.exit(0)
    except Exception as e:
        print(f'\n❌ Error: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
