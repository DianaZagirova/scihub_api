#!/usr/bin/env python3
"""
Identify papers with missing abstract or full text that are not in existing missing_dois list.
"""

import sqlite3
import os
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def identify_missing_content(
    db_path: str,
    exclude_dois_file: str,
    output_file: str
):
    """
    Find papers with missing abstract OR full text, excluding already-listed DOIs.
    
    Args:
        db_path: Path to papers.db
        exclude_dois_file: File with DOIs to exclude (already processed)
        output_file: Output file for new missing DOIs
    """
    logger.info("="*70)
    logger.info("IDENTIFYING PAPERS WITH MISSING CONTENT")
    logger.info("="*70)
    
    # Read DOIs to exclude
    exclude_dois = set()
    if os.path.exists(exclude_dois_file):
        with open(exclude_dois_file, 'r', encoding='utf-8') as f:
            exclude_dois = set(line.strip() for line in f if line.strip())
        logger.info(f"Loaded {len(exclude_dois):,} DOIs to exclude from {exclude_dois_file}")
    else:
        logger.warning(f"Exclude file not found: {exclude_dois_file}")
    
    # Connect to database
    logger.info(f"Connecting to database: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get total papers
    cursor.execute("SELECT COUNT(*) FROM papers")
    total_papers = cursor.fetchone()[0]
    logger.info(f"Total papers in database: {total_papers:,}")
    
    # Find papers with missing content
    # Missing abstract OR missing full text
    query = """
        SELECT doi, title, 
               CASE WHEN abstract IS NULL OR abstract = '' THEN 1 ELSE 0 END as missing_abstract,
               CASE WHEN full_text_sections IS NULL OR full_text_sections = '' THEN 1 ELSE 0 END as missing_full_text
        FROM papers
        WHERE doi IS NOT NULL 
          AND doi != ''
          AND (
              abstract IS NULL OR abstract = '' 
              OR full_text_sections IS NULL OR full_text_sections = ''
          )
        ORDER BY doi
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    logger.info(f"Papers with missing abstract OR full text: {len(results):,}")
    
    # Filter out DOIs already in exclude list
    new_missing = []
    stats = {
        'total_missing': len(results),
        'already_in_batch1': 0,
        'new_missing': 0,
        'missing_abstract': 0,
        'missing_full_text': 0,
        'missing_both': 0
    }
    
    for doi, title, missing_abstract, missing_full_text in results:
        if doi in exclude_dois:
            stats['already_in_batch1'] += 1
            continue
        
        new_missing.append(doi)
        stats['new_missing'] += 1
        
        if missing_abstract and missing_full_text:
            stats['missing_both'] += 1
        elif missing_abstract:
            stats['missing_abstract'] += 1
        else:
            stats['missing_full_text'] += 1
    
    conn.close()
    
    # Create output directory if needed
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    # Write new missing DOIs
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(new_missing))
    
    logger.info("")
    logger.info("="*70)
    logger.info("RESULTS")
    logger.info("="*70)
    logger.info(f"Total papers with missing content: {stats['total_missing']:,}")
    logger.info(f"  Already in batch 1: {stats['already_in_batch1']:,}")
    logger.info(f"  New missing papers: {stats['new_missing']:,}")
    logger.info("")
    logger.info("New missing papers breakdown:")
    logger.info(f"  Missing both abstract AND full text: {stats['missing_both']:,}")
    logger.info(f"  Missing abstract only: {stats['missing_abstract']:,}")
    logger.info(f"  Missing full text only: {stats['missing_full_text']:,}")
    logger.info("")
    logger.info(f"Output saved to: {output_file}")
    logger.info("="*70)
    
    return stats['new_missing']


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Identify papers with missing content for batch processing'
    )
    parser.add_argument(
        '--db',
        default='/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db',
        help='Path to papers.db database'
    )
    parser.add_argument(
        '--exclude',
        default='/home/diana.z/hack/scihub_api/missing_dois/batch_1_Initial query/missing_dois.txt',
        help='File with DOIs to exclude (already processed)'
    )
    parser.add_argument(
        '--output',
        default='/home/diana.z/hack/scihub_api/missing_dois/batch_2_broad_aging/missing_dois.txt',
        help='Output file for new missing DOIs'
    )
    
    args = parser.parse_args()
    
    count = identify_missing_content(
        db_path=args.db,
        exclude_dois_file=args.exclude,
        output_file=args.output
    )
    
    print(f"\n✓ Found {count:,} new papers with missing content")
    print(f"✓ Ready to process with: python download_papers.py -f {args.output} --parser fast -w 5 --delay 2.0")
