#!/usr/bin/env python3
"""
Rebuild tracker to ensure 100% database coverage with comprehensive tracking:
- All DOIs from database
- Sci-Hub download attempts
- OA download attempts (from oa_url)
- Parser attempts (PyMuPDF, Grobid)
- Current status from database content
"""

import sqlite3
import csv
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
OLD_TRACKER = 'doi_processing_tracker.csv'
NEW_TRACKER = 'doi_processing_tracker_complete.csv'
BACKUP_TRACKER = f'doi_processing_tracker_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

def main():
    print('='*70)
    print('REBUILDING COMPLETE TRACKER')
    print('='*70)
    
    # Backup existing tracker
    print(f'\n1. Backing up current tracker...')
    import shutil
    shutil.copy2(OLD_TRACKER, BACKUP_TRACKER)
    print(f'   ✓ Backed up to: {BACKUP_TRACKER}')
    
    # Load existing tracker data
    print(f'\n2. Loading existing tracker data...')
    existing_tracker = {}
    with open(OLD_TRACKER, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_tracker[row['doi']] = dict(row)
    print(f'   ✓ Loaded {len(existing_tracker):,} DOIs from existing tracker')
    
    # Load all DOIs from database
    print(f'\n3. Loading DOIs from database...')
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT doi, 
               full_text, 
               full_text_sections,
               oa_url,
               abstract
        FROM papers 
        WHERE doi IS NOT NULL AND doi != ""
    ''')
    
    db_dois = {}
    for doi, full_text, full_text_sections, oa_url, abstract in cursor.fetchall():
        db_dois[doi] = {
            'has_content': bool((full_text and full_text.strip()) or 
                              (full_text_sections and full_text_sections.strip())),
            'has_oa_url': bool(oa_url and oa_url.strip()),
            'has_abstract': bool(abstract and abstract.strip())
        }
    
    conn.close()
    print(f'   ✓ Loaded {len(db_dois):,} DOIs from database')
    
    # Check output files for parser status
    print(f'\n4. Checking output files...')
    output_dir = Path('./output')
    
    grobid_completed = set()
    pymupdf_completed = set()
    
    if output_dir.exists():
        # Grobid JSONs (not _fast)
        for json_file in output_dir.glob('*.json'):
            if not json_file.name.endswith('_fast.json'):
                # Extract DOI from filename (may need adjustment based on your naming)
                doi = json_file.stem.replace('_', '/')
                grobid_completed.add(doi)
        
        # PyMuPDF JSONs (_fast)
        for json_file in output_dir.glob('*_fast.json'):
            doi = json_file.stem.replace('_fast', '').replace('_', '/')
            pymupdf_completed.add(doi)
    
    print(f'   ✓ Found {len(grobid_completed):,} Grobid outputs')
    print(f'   ✓ Found {len(pymupdf_completed):,} PyMuPDF outputs')
    
    # Build comprehensive tracker
    print(f'\n5. Building comprehensive tracker...')
    
    headers = [
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
    
    # Merge data
    all_dois = set(db_dois.keys()) | set(existing_tracker.keys())
    print(f'   Total unique DOIs to track: {len(all_dois):,}')
    
    complete_tracker = []
    
    for doi in tqdm(sorted(all_dois), desc='Processing'):
        # Get existing tracker data if available
        old_data = existing_tracker.get(doi, {})
        db_data = db_dois.get(doi, {})
        
        # Build new record
        record = {
            'doi': doi,
            'scihub_available': old_data.get('scihub_available', 'unknown'),
            'scihub_downloaded': old_data.get('downloaded', 'unknown'),  # Map old 'downloaded' to scihub_downloaded
            'oa_available': 'yes' if db_data.get('has_oa_url') else 'no',
            'oa_downloaded': 'unknown',  # Will need to infer or track separately
            'downloaded': old_data.get('downloaded', 'unknown'),
            'download_date': old_data.get('download_date', ''),
            'has_content_in_db': 'yes' if db_data.get('has_content') else 'no',
            'pymupdf_status': old_data.get('pymupdf_status', 'not_attempted'),
            'pymupdf_date': old_data.get('pymupdf_date', ''),
            'grobid_status': old_data.get('grobid_status', 'not_attempted'),
            'grobid_date': old_data.get('grobid_date', ''),
            'last_updated': old_data.get('last_updated', datetime.now().isoformat()),
            'error_msg': old_data.get('error_msg', ''),
            'retry_count': old_data.get('retry_count', '0')
        }
        
        # Update parser status from output files if more recent
        if doi in grobid_completed and record['grobid_status'] != 'success':
            record['grobid_status'] = 'success'
        
        if doi in pymupdf_completed and record['pymupdf_status'] != 'success':
            record['pymupdf_status'] = 'success'
        
        # If content exists in DB but no download record, mark as downloaded
        if db_data.get('has_content') and record['downloaded'] == 'unknown':
            record['downloaded'] = 'yes'
        
        complete_tracker.append(record)
    
    # Write new tracker
    print(f'\n6. Writing new tracker...')
    with open(NEW_TRACKER, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(complete_tracker)
    
    print(f'   ✓ Written {len(complete_tracker):,} records')
    
    # Statistics
    print('\n' + '='*70)
    print('STATISTICS')
    print('='*70)
    
    db_coverage = sum(1 for r in complete_tracker if r['doi'] in db_dois)
    scihub_yes = sum(1 for r in complete_tracker if r['scihub_available'] == 'yes')
    oa_yes = sum(1 for r in complete_tracker if r['oa_available'] == 'yes')
    downloaded = sum(1 for r in complete_tracker if r['downloaded'] == 'yes')
    has_content = sum(1 for r in complete_tracker if r['has_content_in_db'] == 'yes')
    pymupdf_success = sum(1 for r in complete_tracker if r['pymupdf_status'] == 'success')
    grobid_success = sum(1 for r in complete_tracker if r['grobid_status'] == 'success')
    
    print(f'Total DOIs tracked: {len(complete_tracker):,}')
    print(f'  From database: {db_coverage:,} ({db_coverage/len(complete_tracker)*100:.1f}%)')
    print(f'\nDownload sources:')
    print(f'  Sci-Hub available: {scihub_yes:,} ({scihub_yes/len(complete_tracker)*100:.1f}%)')
    print(f'  OA available: {oa_yes:,} ({oa_yes/len(complete_tracker)*100:.1f}%)')
    print(f'  Downloaded (any): {downloaded:,} ({downloaded/len(complete_tracker)*100:.1f}%)')
    print(f'\nContent:')
    print(f'  Has content in DB: {has_content:,} ({has_content/len(complete_tracker)*100:.1f}%)')
    print(f'\nParsers:')
    print(f'  PyMuPDF success: {pymupdf_success:,} ({pymupdf_success/len(complete_tracker)*100:.1f}%)')
    print(f'  Grobid success: {grobid_success:,} ({grobid_success/len(complete_tracker)*100:.1f}%)')
    
    print('\n' + '='*70)
    print('NEXT STEPS')
    print('='*70)
    print(f'1. Review new tracker: {NEW_TRACKER}')
    print(f'2. If satisfied, replace old tracker:')
    print(f'   mv {NEW_TRACKER} {OLD_TRACKER}')
    print(f'3. Backup is saved at: {BACKUP_TRACKER}')
    print('='*70)

if __name__ == '__main__':
    main()
