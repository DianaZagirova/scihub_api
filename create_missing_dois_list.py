#!/usr/bin/env python3
"""
Create missing_dois-2.txt with papers that lack abstract OR full text,
excluding papers already listed in multiple batch files.
"""

import sqlite3
import os
import logging
from pathlib import Path
from typing import List, Set

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_exclude_dois(exclude_files: List[str]) -> Set[str]:
    """
    Load DOIs from multiple exclude files.
    
    Args:
        exclude_files: List of file paths containing DOIs to exclude
    
    Returns:
        Set of DOIs to exclude
    """
    exclude_dois = set()
    
    for file_path in exclude_files:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                file_dois = set(line.strip() for line in f if line.strip())
                exclude_dois.update(file_dois)
                logger.info(f"Loaded {len(file_dois):,} DOIs from {file_path}")
        else:
            logger.warning(f"Exclude file not found: {file_path}")
    
    logger.info(f"Total DOIs to exclude: {len(exclude_dois):,}")
    return exclude_dois


def identify_missing_content(
    db_path: str,
    exclude_files: List[str],
    output_file: str
):
    """
    Find papers with missing abstract OR full text, excluding DOIs from multiple files.
    
    Args:
        db_path: Path to papers.db
        exclude_files: List of files with DOIs to exclude
        output_file: Output file for new missing DOIs
    """
    logger.info("="*70)
    logger.info("IDENTIFYING PAPERS WITH MISSING CONTENT")
    logger.info("="*70)
    
    # Load all DOIs to exclude
    exclude_dois = load_exclude_dois(exclude_files)
    
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
    
    # Filter out DOIs already in exclude lists
    new_missing = []
    stats = {
        'total_missing': len(results),
        'already_excluded': 0,
        'new_missing': 0,
        'missing_abstract': 0,
        'missing_full_text': 0,
        'missing_both': 0
    }
    
    for doi, title, missing_abstract, missing_full_text in results:
        if doi in exclude_dois:
            stats['already_excluded'] += 1
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
    logger.info(f"  Already in exclude lists: {stats['already_excluded']:,}")
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
        description='Identify papers with missing content, excluding multiple batch files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default settings
  python create_missing_dois_2.py
  
  # Custom database and output location
  python create_missing_dois_2.py --db /path/to/papers.db --output ./missing_dois/batch_2_broad_aging/missing_dois_2.txt
  
  # Add additional exclude files
  python create_missing_dois_2.py --exclude-files file1.txt file2.txt file3.txt
        """
    )
    
    parser.add_argument(
        '--db',
        default='/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db',
        help='Path to papers.db database'
    )
    parser.add_argument(
        '--exclude-files',
        nargs='+',
        default=[
            'missing_dois/batch_2_broad_aging/missing_dois.txt',
            'missing_dois/batch_1_Initial query/missing_dois_2.txt',
            'missing_dois/batch_1_Initial query/missing_dois.txt'
        ],
        help='Files with DOIs to exclude (space-separated list)'
    )
    parser.add_argument(
        '--output',
        default='./missing_dois/batch_2_broad_aging/missing_dois_2.txt',
        help='Output file for new missing DOIs'
    )
    
    args = parser.parse_args()
    
    # Convert relative paths to absolute paths based on script location
    script_dir = Path(__file__).parent
    exclude_files_abs = []
    for file_path in args.exclude_files:
        if not os.path.isabs(file_path):
            file_path = str(script_dir / file_path)
        exclude_files_abs.append(file_path)
    
    output_path = args.output
    if not os.path.isabs(output_path):
        output_path = str(script_dir / output_path)
    
    count = identify_missing_content(
        db_path=args.db,
        exclude_files=exclude_files_abs,
        output_file=output_path
    )
    
    print(f"\n✓ Found {count:,} new papers with missing content")
    print(f"✓ Output saved to: {output_path}")
    print(f"✓ Ready to process with: python download_papers.py -f {output_path} --parser fast -w 5 --delay 2.0")
