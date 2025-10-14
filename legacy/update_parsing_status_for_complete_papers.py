#!/usr/bin/env python3
"""
Script to update parsing_status for papers that already had abstract and full text
(i.e., papers NOT in missing_dois.txt)
"""

import sqlite3
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def update_parsing_status_for_complete_papers(
    db_path: str,
    missing_dois_file: str = 'missing_dois.txt'
):
    """
    Update parsing_status for papers not in missing_dois.txt
    
    Args:
        db_path: Path to the SQLite database
        missing_dois_file: Path to missing_dois.txt
    """
    logger.info(f"Reading DOIs from {missing_dois_file}")
    
    try:
        # Read missing DOIs
        with open(missing_dois_file, 'r', encoding='utf-8') as f:
            missing_dois = set(line.strip() for line in f if line.strip())
        
        logger.info(f"Found {len(missing_dois)} DOIs in missing_dois.txt")
        
    except FileNotFoundError:
        logger.error(f"File not found: {missing_dois_file}")
        return
    
    logger.info(f"Connecting to database: {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if parsing_status column exists
        cursor.execute("PRAGMA table_info(papers)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'parsing_status' not in columns:
            logger.error("parsing_status column does not exist in papers table")
            conn.close()
            return
        
        # Get all DOIs from database
        cursor.execute("SELECT doi FROM papers")
        all_dois = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Total papers in database: {len(all_dois)}")
        
        # Find papers NOT in missing_dois.txt
        complete_papers = [doi for doi in all_dois if doi not in missing_dois]
        
        logger.info(f"Papers NOT in missing_dois.txt: {len(complete_papers)}")
        
        # Update parsing_status for these papers
        # Only update if parsing_status is NULL or empty
        updated_count = 0
        
        for doi in complete_papers:
            cursor.execute(
                "SELECT parsing_status FROM papers WHERE doi = ?",
                (doi,)
            )
            row = cursor.fetchone()
            
            if row and (row[0] is None or row[0] == ''):
                cursor.execute(
                    "UPDATE papers SET parsing_status = ? WHERE doi = ?",
                    ("not required - already populated", doi)
                )
                updated_count += 1
        
        # Commit changes
        conn.commit()
        
        logger.info(f"\n" + "="*60)
        logger.info("UPDATE SUMMARY")
        logger.info("="*60)
        logger.info(f"Papers not in missing_dois.txt: {len(complete_papers)}")
        logger.info(f"Papers updated with 'not required' status: {updated_count}")
        logger.info("="*60)
        
        # Verify the update
        logger.info("\n" + "="*60)
        logger.info("VERIFICATION - Parsing Status Distribution")
        logger.info("="*60)
        
        cursor.execute("""
            SELECT parsing_status, COUNT(*) 
            FROM papers 
            GROUP BY parsing_status
            ORDER BY COUNT(*) DESC
        """)
        
        for status, count in cursor.fetchall():
            status_display = status if status else "NULL/Empty"
            logger.info(f"  {status_display}: {count} papers")
        
        logger.info("="*60)
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        raise


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Update parsing_status for papers already populated with data'
    )
    parser.add_argument(
        '--db',
        default='/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db',
        help='Path to papers.db database'
    )
    parser.add_argument(
        '--missing-dois',
        default='missing_dois.txt',
        help='Path to missing_dois.txt file'
    )
    
    args = parser.parse_args()
    
    update_parsing_status_for_complete_papers(
        db_path=args.db,
        missing_dois_file=args.missing_dois
    )
