#!/usr/bin/env python3
"""Quick status check - database and output files."""

import sqlite3
import json
from pathlib import Path

DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
OUTPUT_DIR = Path('./output')

print('='*70)
print('QUICK STATUS CHECK')
print('='*70)

# Database checks
print('\n📊 DATABASE STATISTICS')
print('-'*70)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Total DOIs
cursor.execute('SELECT COUNT(*) FROM papers WHERE doi IS NOT NULL AND doi != ""')
total_dois = cursor.fetchone()[0]
print(f'Total DOIs in database: {total_dois:,}')

# Missing both full_text and full_text_sections
cursor.execute('''
    SELECT COUNT(*) 
    FROM papers 
    WHERE doi IS NOT NULL AND doi != ""
    AND (full_text IS NULL OR full_text = "")
    AND (full_text_sections IS NULL OR full_text_sections = "")
''')
missing_both = cursor.fetchone()[0]
print(f'Missing full_text AND full_text_sections: {missing_both:,} ({missing_both/total_dois*100:.1f}%)')

# Has full_text OR full_text_sections
has_content = total_dois - missing_both
print(f'Has full_text OR full_text_sections: {has_content:,} ({has_content/total_dois*100:.1f}%)')

conn.close()

# Output directory checks
print('\n📁 OUTPUT DIRECTORY (/output)')
print('-'*70)

if not OUTPUT_DIR.exists():
    print('❌ Output directory does not exist!')
else:
    # Count Grobid JSONs
    grobid_jsons = list(OUTPUT_DIR.glob('*.json'))
    # Filter out fast JSONs
    grobid_only = [f for f in grobid_jsons if not f.name.endswith('_fast.json')]
    fast_jsons = [f for f in grobid_jsons if f.name.endswith('_fast.json')]
    
    print(f'Grobid JSONs (*.json, not _fast): {len(grobid_only):,}')
    print(f'Fast parse JSONs (*_fast.json): {len(fast_jsons):,}')
    print(f'Total JSON files: {len(grobid_jsons):,}')
    
    # Calculate coverage
    if total_dois > 0:
        grobid_coverage = len(grobid_only) / total_dois * 100
        fast_coverage = len(fast_jsons) / total_dois * 100
        print(f'\nCoverage vs database:')
        print(f'  Grobid: {grobid_coverage:.1f}% of DOIs')
        print(f'  Fast parse: {fast_coverage:.1f}% of DOIs')

print('\n' + '='*70)
print('SUMMARY')
print('='*70)
print(f'Database: {total_dois:,} DOIs')
print(f'  - With content: {has_content:,} ({has_content/total_dois*100:.1f}%)')
print(f'  - Missing content: {missing_both:,} ({missing_both/total_dois*100:.1f}%)')
print(f'\nParsing outputs:')
print(f'  - Grobid JSONs: {len(grobid_only):,}')
print(f'  - Fast JSONs: {len(fast_jsons):,}')
print('='*70)
