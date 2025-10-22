#!/usr/bin/env python3
"""
Check deleted DOIs for:
1. Missing full_text/full_text_sections in papers.db
2. Present in paper_evaluations with specific criteria

Usage:
    python check_deleted_dois_priority.py --deleted-list missing_dois/deleted_dois_extracted.txt
"""

import sqlite3
import argparse
from pathlib import Path
from typing import Set

DEFAULT_PAPERS_DB = '/home/diana.z/hack/download_papers_pubmed/aging_theories_collection/data/papers.db'
DEFAULT_EVAL_DB ='/home/diana.z/hack/llm_judge/data/evaluations.db'



def load_dois_from_file(file_path: Path) -> Set[str]:
    """Load DOIs from file (one per line)."""
    dois = set()
    with open(file_path, 'r') as f:
        for line in f:
            doi = line.strip()
            if doi:
                dois.add(doi)
    return dois


def check_missing_fulltext(papers_db: str, dois: Set[str]) -> Set[str]:
    """
    Check which DOIs are missing full_text/full_text_sections in papers.db.
    
    Returns:
        Set of DOIs that are either not in DB or missing full text
    """
    conn = sqlite3.connect(papers_db)
    cur = conn.cursor()
    
    missing = set()
    
    for doi in dois:
        cur.execute(
            """
            SELECT full_text, full_text_sections
            FROM papers
            WHERE doi = ?
            """,
            (doi,)
        )
        row = cur.fetchone()
        
        if not row:
            # Not in papers.db
            missing.add(doi)
            continue
        
        full_text, full_text_sections = row
        
        # Check if both are empty/None
        has_full_text = full_text and full_text.strip()
        has_full_text_sections = full_text_sections and full_text_sections.strip()
        
        if not (has_full_text or has_full_text_sections):
            missing.add(doi)
    
    conn.close()
    return missing


def get_evaluated_dois(eval_db: str) -> Set[str]:
    """
    Get DOIs from paper_evaluations with specific criteria:
    - result IN ('valid', 'doubted')
    - OR (result='not_valid' AND confidence_score <= 7)
    
    Returns:
        Set of DOIs matching criteria
    """
    conn = sqlite3.connect(eval_db)
    cur = conn.cursor()
    
    rows = cur.execute("""
        SELECT DISTINCT doi
        FROM paper_evaluations
        WHERE doi IS NOT NULL AND doi != ''
          AND (
            result IN ('valid','doubted')
            OR (result='not_valid' AND COALESCE(confidence_score,999) <= 7)
          )
    """).fetchall()
    
    conn.close()
    
    return {row[0] for row in rows}


def main():
    parser = argparse.ArgumentParser(
        description='Check deleted DOIs for missing full text and evaluation status'
    )
    parser.add_argument(
        '--deleted-list',
        required=True,
        help='Path to file containing deleted DOIs (one per line)'
    )
    parser.add_argument(
        '--papers-db',
        default=DEFAULT_PAPERS_DB,
        help=f'Path to papers.db (default: {DEFAULT_PAPERS_DB})'
    )
    parser.add_argument(
        '--eval-db',
        default=DEFAULT_EVAL_DB,
        help=f'Path to evaluations papers.db (default: {DEFAULT_EVAL_DB})'
    )
    parser.add_argument(
        '--output',
        help='Output file for priority DOIs (optional)'
    )
    
    args = parser.parse_args()
    
    deleted_file = Path(args.deleted_list)
    
    if not deleted_file.exists():
        print(f"Error: File not found: {deleted_file}")
        return 1
    
    # Load deleted DOIs
    print("="*80)
    print("Loading deleted DOIs...")
    print("="*80)
    deleted_dois = load_dois_from_file(deleted_file)
    print(f"Loaded {len(deleted_dois)} deleted DOIs")
    
    # Check 1: Missing full text
    print("\n" + "="*80)
    print("CHECK 1: Missing full_text/full_text_sections in papers.db")
    print("="*80)
    missing_fulltext = check_missing_fulltext(args.papers_db, deleted_dois)
    print(f"DOIs missing full text: {len(missing_fulltext)}")
    
    # Check 2: In evaluations
    print("\n" + "="*80)
    print("CHECK 2: Present in paper_evaluations with criteria")
    print("="*80)
    evaluated_dois = get_evaluated_dois(args.eval_db)
    print(f"Total evaluated DOIs (all): {len(evaluated_dois)}")
    
    # Intersection: deleted AND missing fulltext AND evaluated
    priority_dois = missing_fulltext & evaluated_dois
    print(f"Deleted DOIs in evaluations: {len(deleted_dois & evaluated_dois)}")
    
    # Results
    print("\n" + "="*80)
    print("RESULTS")
    print("="*80)
    print(f"Deleted DOIs missing full text: {len(missing_fulltext)}")
    print(f"  - Also in evaluations: {len(priority_dois)}")
    print(f"  - Not in evaluations: {len(missing_fulltext - evaluated_dois)}")
    
    # Priority DOIs (missing full text AND in evaluations)
    if priority_dois:
        print("\n" + "="*80)
        print(f"PRIORITY DOIs ({len(priority_dois)} total)")
        with open("./missng_dois/priority_dois_deleted.txt", "w") as f:
            for doi in set(priority_dois):
                f.write(doi + "\n")
        print("="*80)
        print("These DOIs need re-parsing and are in evaluations:")
        
        for doi in sorted(priority_dois)[:20]:
            print(f"  {doi}")
        
        if len(priority_dois) > 20:
            print(f"  ... and {len(priority_dois) - 20} more")
        
        # Save to file if requested
        if args.output:
            output_file = Path(args.output)
            with open(output_file, 'w') as f:
                for doi in sorted(priority_dois):
                    f.write(f"{doi}\n")
            print(f"\n✓ Priority DOIs saved to: {output_file}")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total deleted DOIs: {len(deleted_dois)}")
    print(f"Missing full text: {len(missing_fulltext)} ({len(missing_fulltext)/len(deleted_dois)*100:.1f}%)")
    print(f"In evaluations: {len(deleted_dois & evaluated_dois)} ({len(deleted_dois & evaluated_dois)/len(deleted_dois)*100:.1f}%)")
    print(f"PRIORITY (both): {len(priority_dois)} ({len(priority_dois)/len(deleted_dois)*100:.1f}%)")
    print("="*80)
    
    return 0


if __name__ == '__main__':
    exit(main())
