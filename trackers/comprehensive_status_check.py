#!/usr/bin/env python3
"""
Comprehensive status check for all DOIs:
- Database DOIs
- Tracker coverage
- Download sources (Sci-Hub, OA)
- Parser status
"""

import sqlite3
import csv
from pathlib import Path
from collections import defaultdict

DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
TRACKER_PATH = 'doi_processing_tracker.csv'
OUTPUT_DIR = Path('./output')
PDF_DIR = Path('./papers')

def main():
    print('='*70)
    print('COMPREHENSIVE STATUS CHECK')
    print('='*70)
    
    # Load database DOIs
    print('\n📊 Loading database...')
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM papers WHERE doi IS NOT NULL AND doi != ""')
    total_db_dois = cursor.fetchone()[0]
    print(f'   Total DOIs in database: {total_db_dois:,}')
    
    # Get DOIs with content
    cursor.execute('''
        SELECT doi, 
               CASE WHEN (full_text IS NOT NULL AND full_text != "") 
                    OR (full_text_sections IS NOT NULL AND full_text_sections != "") 
                    THEN 1 ELSE 0 END as has_content,
               CASE WHEN oa_url IS NOT NULL AND oa_url != "" THEN 1 ELSE 0 END as has_oa_url
        FROM papers 
        WHERE doi IS NOT NULL AND doi != ""
    ''')
    
    db_status = {}
    dois_with_content = 0
    dois_with_oa_url = 0
    
    for doi, has_content, has_oa_url in cursor.fetchall():
        db_status[doi] = {
            'has_content': bool(has_content),
            'has_oa_url': bool(has_oa_url)
        }
        if has_content:
            dois_with_content += 1
        if has_oa_url:
            dois_with_oa_url += 1
    
    conn.close()
    
    print(f'   DOIs with content (full_text or sections): {dois_with_content:,} ({dois_with_content/total_db_dois*100:.1f}%)')
    print(f'   DOIs with OA URL: {dois_with_oa_url:,} ({dois_with_oa_url/total_db_dois*100:.1f}%)')
    
    # Load tracker
    print('\n📋 Loading tracker...')
    tracker_status = {}
    
    with open(TRACKER_PATH, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            doi = row['doi']
            tracker_status[doi] = {
                'scihub_available': row.get('scihub_available', 'unknown'),
                'downloaded': row.get('downloaded', 'unknown'),
                'pymupdf_status': row.get('pymupdf_status', 'not_attempted'),
                'grobid_status': row.get('grobid_status', 'not_attempted'),
            }
    
    print(f'   Total DOIs in tracker: {len(tracker_status):,}')
    
    # Analyze tracker coverage of database DOIs
    db_dois_set = set(db_status.keys())
    tracker_dois_set = set(tracker_status.keys())
    
    in_both = db_dois_set & tracker_dois_set
    only_in_db = db_dois_set - tracker_dois_set
    only_in_tracker = tracker_dois_set - db_dois_set
    
    print(f'   DOIs in both DB and tracker: {len(in_both):,}')
    print(f'   DOIs only in database: {len(only_in_db):,}')
    print(f'   DOIs only in tracker: {len(only_in_tracker):,}')
    
    # Analyze download sources
    print('\n📥 Download source analysis (for DOIs in both)...')
    
    scihub_stats = defaultdict(int)
    download_stats = defaultdict(int)
    
    for doi in in_both:
        ts = tracker_status[doi]
        scihub_stats[ts['scihub_available']] += 1
        download_stats[ts['downloaded']] += 1
    
    print('   Sci-Hub availability:')
    for status, count in sorted(scihub_stats.items()):
        print(f'     {status}: {count:,} ({count/len(in_both)*100:.1f}%)')
    
    print('   Downloaded status:')
    for status, count in sorted(download_stats.items()):
        print(f'     {status}: {count:,} ({count/len(in_both)*100:.1f}%)')
    
    # Analyze parser status
    print('\n🔧 Parser status analysis (for DOIs in both)...')
    
    pymupdf_stats = defaultdict(int)
    grobid_stats = defaultdict(int)
    
    for doi in in_both:
        ts = tracker_status[doi]
        pymupdf_stats[ts['pymupdf_status']] += 1
        grobid_stats[ts['grobid_status']] += 1
    
    print('   PyMuPDF status:')
    for status, count in sorted(pymupdf_stats.items()):
        print(f'     {status}: {count:,} ({count/len(in_both)*100:.1f}%)')
    
    print('   Grobid status:')
    for status, count in sorted(grobid_stats.items()):
        print(f'     {status}: {count:,} ({count/len(in_both)*100:.1f}%)')
    
    # Check output files
    print('\n📁 Output files...')
    
    if OUTPUT_DIR.exists():
        grobid_jsons = [f for f in OUTPUT_DIR.glob('*.json') if not f.name.endswith('_fast.json')]
        fast_jsons = list(OUTPUT_DIR.glob('*_fast.json'))
        
        print(f'   Grobid JSONs: {len(grobid_jsons):,}')
        print(f'   Fast JSONs: {len(fast_jsons):,}')
    else:
        print('   ⚠️  Output directory not found')
    
    # Check PDFs
    if PDF_DIR.exists():
        pdfs = list(PDF_DIR.glob('*.pdf'))
        print(f'   PDFs: {len(pdfs):,}')
    else:
        print('   ⚠️  PDFs directory not found')
    
    # Summary
    print('\n' + '='*70)
    print('SUMMARY')
    print('='*70)
    print(f'Database: {total_db_dois:,} DOIs')
    print(f'  - With content: {dois_with_content:,} ({dois_with_content/total_db_dois*100:.1f}%)')
    print(f'  - With OA URL: {dois_with_oa_url:,}')
    
    print(f'\nTracker: {len(tracker_status):,} DOIs')
    print(f'  - Overlap with DB: {len(in_both):,} ({len(in_both)/total_db_dois*100:.1f}%)')
    print(f'  - Missing from tracker: {len(only_in_db):,}')
    
    if len(only_in_db) > 0:
        print(f'\n⚠️  ACTION NEEDED: Add {len(only_in_db):,} database DOIs to tracker')
        print(f'   Run: python add_new_dois_to_tracker.py')
    else:
        print(f'\n✅ All database DOIs are in the tracker!')
    
    print('='*70)

if __name__ == '__main__':
    main()
