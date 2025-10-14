#!/usr/bin/env python3
"""
Script to check database status - verify abstracts and full texts are populated
"""

import sqlite3
import json
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_database_status(db_path: str):
    """
    Check the status of the database - abstracts and full text sections.
    
    Args:
        db_path: Path to the SQLite database
    """
    logger.info(f"Connecting to database: {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get total number of papers
        cursor.execute("SELECT COUNT(*) FROM papers")
        total_papers = cursor.fetchone()[0]
        
        # Check abstracts
        cursor.execute("SELECT COUNT(*) FROM papers WHERE abstract IS NOT NULL AND abstract != ''")
        papers_with_abstract = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM papers WHERE abstract IS NULL OR abstract = ''")
        papers_without_abstract = cursor.fetchone()[0]
        
        # Check full_text_sections
        cursor.execute("SELECT COUNT(*) FROM papers WHERE full_text_sections IS NOT NULL AND full_text_sections != ''")
        papers_with_full_text = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM papers WHERE full_text_sections IS NULL OR full_text_sections = ''")
        papers_without_full_text = cursor.fetchone()[0]
        
        # Check parsing_status column exists
        cursor.execute("PRAGMA table_info(papers)")
        columns = [col[1] for col in cursor.fetchall()]
        has_parsing_status = 'parsing_status' in columns
        
        if has_parsing_status:
            cursor.execute("SELECT COUNT(*) FROM papers WHERE parsing_status IS NOT NULL AND parsing_status != ''")
            papers_with_status = cursor.fetchone()[0]
        else:
            papers_with_status = 0
        
        # Print statistics
        logger.info("\n" + "="*70)
        logger.info("DATABASE STATUS REPORT")
        logger.info("="*70)
        logger.info(f"\nTotal papers in database: {total_papers:,}")
        
        logger.info(f"\n--- ABSTRACTS ---")
        logger.info(f"Papers WITH abstract: {papers_with_abstract:,} ({papers_with_abstract/total_papers*100:.2f}%)")
        logger.info(f"Papers WITHOUT abstract: {papers_without_abstract:,} ({papers_without_abstract/total_papers*100:.2f}%)")
        
        logger.info(f"\n--- FULL TEXT SECTIONS ---")
        logger.info(f"Papers WITH full text: {papers_with_full_text:,} ({papers_with_full_text/total_papers*100:.2f}%)")
        logger.info(f"Papers WITHOUT full text: {papers_without_full_text:,} ({papers_without_full_text/total_papers*100:.2f}%)")
        
        logger.info(f"\n--- PARSING STATUS ---")
        if has_parsing_status:
            logger.info(f"Papers WITH parsing status: {papers_with_status:,} ({papers_with_status/total_papers*100:.2f}%)")
        else:
            logger.info("parsing_status column does NOT exist")
        
        # Sample some papers with full text
        logger.info(f"\n--- SAMPLE PAPERS WITH FULL TEXT ---")
        cursor.execute("""
            SELECT doi, title, 
                   LENGTH(abstract) as abstract_len,
                   LENGTH(full_text_sections) as full_text_len,
                   parsing_status
            FROM papers 
            WHERE full_text_sections IS NOT NULL 
            AND full_text_sections != ''
            LIMIT 5
        """)
        
        for i, row in enumerate(cursor.fetchall(), 1):
            doi, title, abs_len, ft_len, status = row
            logger.info(f"\n{i}. DOI: {doi}")
            logger.info(f"   Title: {title[:80]}..." if title and len(title) > 80 else f"   Title: {title}")
            logger.info(f"   Abstract length: {abs_len if abs_len else 0} chars")
            logger.info(f"   Full text length: {ft_len if ft_len else 0} chars")
            logger.info(f"   Status: {status}")
        
        # Check a specific full_text_sections structure
        logger.info(f"\n--- SAMPLE FULL TEXT STRUCTURE ---")
        cursor.execute("""
            SELECT doi, full_text_sections
            FROM papers 
            WHERE full_text_sections IS NOT NULL 
            AND full_text_sections != ''
            LIMIT 1
        """)
        
        row = cursor.fetchone()
        if row:
            doi, full_text = row
            logger.info(f"DOI: {doi}")
            try:
                sections = json.loads(full_text)
                logger.info(f"Number of sections: {len(sections)}")
                logger.info(f"Section titles: {list(sections.keys())[:10]}")  # First 10 sections
                # Show first section content preview
                if sections:
                    first_section = list(sections.keys())[0]
                    content = sections[first_section]
                    logger.info(f"\nFirst section '{first_section}' preview:")
                    logger.info(f"{content[:200]}..." if len(content) > 200 else content)
            except json.JSONDecodeError:
                logger.error("Failed to parse full_text_sections as JSON")
        
        # Check papers that should have been updated (from missing_dois.txt if it exists)
        logger.info(f"\n--- CHECKING UPDATE STATUS ---")
        try:
            # Check if missing_dois.txt exists
            with open('missing_dois.txt', 'r') as f:
                missing_dois = [line.strip() for line in f if line.strip()][:10]  # First 10
            
            logger.info(f"Checking first 10 DOIs from missing_dois.txt:")
            for doi in missing_dois:
                cursor.execute("""
                    SELECT doi,
                           CASE WHEN abstract IS NOT NULL AND abstract != '' THEN 'YES' ELSE 'NO' END as has_abstract,
                           CASE WHEN full_text_sections IS NOT NULL AND full_text_sections != '' THEN 'YES' ELSE 'NO' END as has_full_text,
                           parsing_status
                    FROM papers 
                    WHERE doi = ?
                """, (doi,))
                row = cursor.fetchone()
                if row:
                    doi, has_abs, has_ft, status = row
                    logger.info(f"  {doi}: Abstract={has_abs}, FullText={has_ft}, Status={status}")
                else:
                    logger.info(f"  {doi}: NOT FOUND in database")
        except FileNotFoundError:
            logger.info("missing_dois.txt not found - skipping update verification")
        
        logger.info("\n" + "="*70)
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error checking database: {e}")
        raise


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Check database status - verify abstracts and full texts'
    )
    parser.add_argument(
        '--db',
        default='/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db',
        help='Path to papers.db database'
    )
    
    args = parser.parse_args()
    
    check_database_status(db_path=args.db)
