#!/usr/bin/env python3
"""Check tracker status quickly."""

from doi_tracker import DOITracker

tracker = DOITracker('doi_processing_tracker.csv')
tracker._ensure_cache_loaded()

total = len(tracker._cache)
needs_download = 0
needs_pymupdf = 0
needs_grobid = 0
complete = 0

for doi, status in tracker._cache.items():
    downloaded = status.get('downloaded', '')
    pymupdf_status = status.get('pymupdf_status', '')
    grobid_status = status.get('grobid_status', '')
    
    if downloaded != 'yes':
        needs_download += 1
    
    if pymupdf_status not in ['success']:
        needs_pymupdf += 1
    
    if grobid_status not in ['success']:
        needs_grobid += 1
    
    if pymupdf_status == 'success' and grobid_status == 'success':
        complete += 1

print('='*70)
print('TRACKER STATUS')
print('='*70)
print(f'Total DOIs: {total:,}')
print(f'Needs download: {needs_download:,}')
print(f'Needs PyMuPDF parsing: {needs_pymupdf:,}')
print(f'Needs Grobid parsing: {needs_grobid:,}')
print(f'Fully complete (both parsers): {complete:,}')
print('='*70)
