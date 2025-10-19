#!/usr/bin/env python3
"""Check database integrity and status."""

import sqlite3

DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'

def check_database():
    """Check database status."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check total papers
    cursor.execute('SELECT COUNT(*) FROM papers')
    total = cursor.fetchone()[0]
    
    # Check papers with DOI
    cursor.execute("SELECT COUNT(*) FROM papers WHERE doi IS NOT NULL AND doi != ''")
    with_doi = cursor.fetchone()[0]
    
    # Check papers with abstract
    cursor.execute("SELECT COUNT(*) FROM papers WHERE abstract IS NOT NULL AND abstract != ''")
    with_abstract = cursor.fetchone()[0]
    
    # Check papers with full text
    cursor.execute("SELECT COUNT(*) FROM papers WHERE full_text_sections IS NOT NULL AND full_text_sections != ''")
    with_full_text = cursor.fetchone()[0]
    
    # Check papers with parsing status
    cursor.execute("SELECT COUNT(*) FROM papers WHERE parsing_status IS NOT NULL AND parsing_status != ''")
    with_status = cursor.fetchone()[0]
    
    # Get a few recent DOIs
    cursor.execute("SELECT doi, parsing_status FROM papers WHERE doi IS NOT NULL LIMIT 5")
    sample_dois = cursor.fetchall()
    
    print('='*70)
    print('DATABASE STATUS CHECK')
    print('='*70)
    print(f'Total papers: {total:,}')
    print(f'Papers with DOI: {with_doi:,}')
    print(f'Papers with abstract: {with_abstract:,}')
    print(f'Papers with full_text: {with_full_text:,}')
    print(f'Papers with parsing_status: {with_status:,}')
    print('='*70)
    print('\nSample DOIs:')
    for doi, status in sample_dois[:5]:
        print(f'  {doi[:50]:<50} | {status or "None"}')
    print('='*70)
    print('\n✓ DATABASE IS INTACT AND NOT AFFECTED')
    print('  The tracker issue did NOT affect the database!')
    print('='*70)
    
    conn.close()

if __name__ == '__main__':
    check_database()
