#!/usr/bin/env python3
"""
Quick script to check how many papers do not have parsing_status
"""

import sqlite3

DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'

def check_missing_parsing_status():
    """Check how many papers are missing parsing_status."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Total papers
    cursor.execute("SELECT COUNT(*) FROM papers")
    total_papers = cursor.fetchone()[0]
    
    # Papers WITHOUT parsing_status (NULL or empty string)
    cursor.execute("""
        SELECT COUNT(*) 
        FROM papers 
        WHERE parsing_status IS NULL OR parsing_status = ''
    """)
    without_status = cursor.fetchone()[0]
    
    # Papers WITH parsing_status
    cursor.execute("""
        SELECT COUNT(*) 
        FROM papers 
        WHERE parsing_status IS NOT NULL AND parsing_status != ''
    """)
    with_status = cursor.fetchone()[0]
    
    # Print results
    print("="*70)
    print("PARSING STATUS CHECK")
    print("="*70)
    print(f"\nTotal papers in database: {total_papers:,}")
    print(f"\nPapers WITH parsing status: {with_status:,} ({with_status/total_papers*100:.2f}%)")
    print(f"Papers WITHOUT parsing status: {without_status:,} ({without_status/total_papers*100:.2f}%)")
    
    # Show breakdown of parsing statuses
    print("\n" + "-"*70)
    print("PARSING STATUS BREAKDOWN")
    print("-"*70)
    cursor.execute("""
        SELECT parsing_status, COUNT(*) as count
        FROM papers
        GROUP BY parsing_status
        ORDER BY count DESC
    """)
    
    for status, count in cursor.fetchall():
        status_display = status if status else "[NULL/EMPTY]"
        print(f"  {status_display}: {count:,}")
    
    print("="*70)
    
    conn.close()

if __name__ == '__main__':
    check_missing_parsing_status()
