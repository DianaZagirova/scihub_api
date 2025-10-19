#!/usr/bin/env python3
"""
Analyze the two PDF directories (papers/ and pdfs/) to understand:
- Which files are duplicates
- Which files are unique to each
- Recommend consolidation strategy
"""

from pathlib import Path
from collections import defaultdict
import os

PAPERS_DIR = Path('./papers')
PDFS_DIR = Path('./pdfs')

def get_pdf_files(directory):
    """Get all PDF files in a directory with their sizes."""
    if not directory.exists():
        return {}
    
    files = {}
    for pdf in directory.glob('*.pdf'):
        filename = pdf.name
        files[filename] = {
            'path': pdf,
            'size': pdf.stat().st_size,
            'mtime': pdf.stat().st_mtime
        }
    return files

def main():
    print('='*70)
    print('PDF DIRECTORIES ANALYSIS')
    print('='*70)
    
    # Get files from both directories
    print('\n1. Scanning directories...')
    papers_files = get_pdf_files(PAPERS_DIR)
    pdfs_files = get_pdf_files(PDFS_DIR)
    
    print(f'   papers/: {len(papers_files):,} PDFs')
    print(f'   pdfs/:   {len(pdfs_files):,} PDFs')
    
    # Calculate sizes
    papers_size = sum(f['size'] for f in papers_files.values())
    pdfs_size = sum(f['size'] for f in pdfs_files.values())
    
    print(f'\n   papers/ size: {papers_size / 1024**3:.2f} GB')
    print(f'   pdfs/   size: {pdfs_size / 1024**3:.2f} GB')
    
    # Find duplicates and uniques
    print('\n2. Analyzing overlap...')
    
    papers_names = set(papers_files.keys())
    pdfs_names = set(pdfs_files.keys())
    
    duplicates = papers_names & pdfs_names
    only_in_papers = papers_names - pdfs_names
    only_in_pdfs = pdfs_names - papers_names
    
    print(f'   Duplicates (in both): {len(duplicates):,}')
    print(f'   Only in papers/: {len(only_in_papers):,}')
    print(f'   Only in pdfs/: {len(only_in_pdfs):,}')
    
    # Check if duplicates are actually the same file
    if duplicates:
        print('\n3. Checking duplicate file sizes...')
        same_size = 0
        different_size = 0
        
        for filename in list(duplicates)[:100]:  # Check first 100
            papers_size = papers_files[filename]['size']
            pdfs_size = pdfs_files[filename]['size']
            
            if papers_size == pdfs_size:
                same_size += 1
            else:
                different_size += 1
        
        print(f'   Same size (likely identical): {same_size}')
        print(f'   Different size (may differ): {different_size}')
    
    # Which directory is newer?
    print('\n4. Checking which directory is more recent...')
    
    if papers_files and pdfs_files:
        # Get most recent modification time from each
        papers_latest = max(f['mtime'] for f in papers_files.values())
        pdfs_latest = max(f['mtime'] for f in pdfs_files.values())
        
        from datetime import datetime
        papers_date = datetime.fromtimestamp(papers_latest)
        pdfs_date = datetime.fromtimestamp(pdfs_latest)
        
        print(f'   papers/ most recent file: {papers_date}')
        print(f'   pdfs/   most recent file: {pdfs_date}')
        
        if papers_latest > pdfs_latest:
            print(f'   → papers/ is more actively used')
        else:
            print(f'   → pdfs/ is more actively used')
    
    # Recommendations
    print('\n' + '='*70)
    print('RECOMMENDATIONS')
    print('='*70)
    
    total_unique = len(only_in_papers) + len(only_in_pdfs)
    
    if len(papers_files) > len(pdfs_files) * 10:
        print('\n✅ RECOMMENDED: Consolidate to papers/')
        print(f'   - papers/ has {len(papers_files):,} PDFs (bulk of collection)')
        print(f'   - pdfs/ has only {len(pdfs_files):,} PDFs')
        print(f'\n   Actions:')
        print(f'   1. Move unique files from pdfs/ to papers/:')
        print(f'      rsync -av --ignore-existing pdfs/ papers/')
        print(f'   2. Update all scripts to use papers/ directory')
        print(f'   3. Remove pdfs/ directory once verified')
        
    elif len(pdfs_files) > len(papers_files) * 2:
        print('\n✅ RECOMMENDED: Consolidate to pdfs/')
        print(f'   - pdfs/ has {len(pdfs_files):,} PDFs')
        print(f'   - papers/ has {len(papers_files):,} PDFs')
        print(f'\n   Actions:')
        print(f'   1. Move unique files from papers/ to pdfs/:')
        print(f'      rsync -av --ignore-existing papers/ pdfs/')
        print(f'   2. Scripts already use pdfs/ directory')
        print(f'   3. Remove papers/ directory once verified')
    
    else:
        print('\n⚠️  RECOMMENDED: Consolidate to papers/ (historical standard)')
        print(f'   - Total unique PDFs: {total_unique:,}')
        print(f'   - Duplicates: {len(duplicates):,}')
        print(f'\n   Actions:')
        print(f'   1. Move files from pdfs/ to papers/:')
        print(f'      rsync -av --ignore-existing pdfs/ papers/')
        print(f'   2. Update newer scripts to use papers/ directory')
    
    print('\n📝 Note: Use rsync with --ignore-existing to avoid overwriting')
    print('   Test with --dry-run first: rsync -av --dry-run --ignore-existing pdfs/ papers/')
    print('='*70)

if __name__ == '__main__':
    main()
