#!/usr/bin/env python3
"""
Script to update parsing_status for papers based on log files
- If DOI appears in logs, update status based on log Result
- If paper has no DOI, mark it clearly
"""

import sqlite3
import re
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_log_files(log_files: list) -> dict:
    """
    Parse all log files to extract DOI processing status.
    
    Returns:
        dict: {doi: (result, parser_type, timestamp)}
    """
    doi_status = {}
    
    for log_file in log_files:
        if not Path(log_file).exists():
            logger.warning(f"Log file not found: {log_file}")
            continue
        
        logger.info(f"Parsing log file: {log_file}")
        
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Pattern to match log entries
            # Looking for: DOI/Identifier: <doi> ... Timestamp: <timestamp> ... Result: <status>
            # Also capture Parser type if available
            pattern = r'DOI/Identifier:\s*([^\n]+)\s+Timestamp:\s*([^\n]+).*?Result:\s*([^\n]+?)(?:\s+Parser:\s*([^\n]+))?(?=\n|$)'
            
            matches = re.findall(pattern, content, re.DOTALL)
            
            for doi, timestamp, result, parser in matches:
                doi = doi.strip()
                timestamp = timestamp.strip()
                result = result.strip()
                parser = parser.strip() if parser else None
                
                # Keep the latest entry for each DOI (by timestamp)
                if doi not in doi_status or timestamp > doi_status[doi][2]:
                    doi_status[doi] = (result, parser, timestamp)
            
            logger.info(f"  Found {len(matches)} entries in {log_file}")
        
        except Exception as e:
            logger.error(f"Error parsing log file {log_file}: {e}")
    
    logger.info(f"\nTotal unique DOIs found in logs: {len(doi_status)}")
    return doi_status


def update_parsing_status_from_logs(
    db_path: str,
    log_files: list
):
    """
    Update parsing_status for papers based on log files.
    
    Args:
        db_path: Path to the SQLite database
        log_files: List of log file paths
    """
    # Parse all log files
    logger.info("="*70)
    logger.info("PARSING LOG FILES")
    logger.info("="*70)
    doi_status = parse_log_files(log_files)
    
    # Connect to database
    logger.info(f"\n" + "="*70)
    logger.info("CONNECTING TO DATABASE")
    logger.info("="*70)
    logger.info(f"Database: {db_path}")
    
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
        
        # Get all papers with NULL or empty parsing_status
        cursor.execute("""
            SELECT doi 
            FROM papers 
            WHERE parsing_status IS NULL OR parsing_status = ''
        """)
        papers_without_status = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Papers without parsing status: {len(papers_without_status)}")
        
        # Statistics
        stats = {
            'updated_from_logs': 0,
            'no_doi': 0,
            'not_in_logs': 0,
            'already_has_status': 0
        }
        
        # Process each paper without status
        logger.info(f"\n" + "="*70)
        logger.info("UPDATING PARSING STATUS")
        logger.info("="*70)
        
        for doi in papers_without_status:
            # Check if DOI is NULL or empty
            if not doi or doi.strip() == '':
                cursor.execute(
                    "UPDATE papers SET parsing_status = ? WHERE doi IS NULL OR doi = ''",
                    ("no DOI available",)
                )
                stats['no_doi'] += 1
                continue
            
            # Check if DOI appears in logs
            if doi in doi_status:
                result, parser, timestamp = doi_status[doi]
                
                # Format status
                if parser:
                    status = f"{result} (parser: {parser})"
                else:
                    status = result
                
                cursor.execute(
                    "UPDATE papers SET parsing_status = ? WHERE doi = ?",
                    (status, doi)
                )
                stats['updated_from_logs'] += 1
                
                if stats['updated_from_logs'] <= 5:  # Show first 5
                    logger.info(f"  Updated {doi}: {status}")
            else:
                # DOI not found in any log
                cursor.execute(
                    "UPDATE papers SET parsing_status = ? WHERE doi = ?",
                    ("not processed - not found in logs", doi)
                )
                stats['not_in_logs'] += 1
        
        # Commit changes
        conn.commit()
        
        # Print summary
        logger.info(f"\n" + "="*70)
        logger.info("UPDATE SUMMARY")
        logger.info("="*70)
        logger.info(f"Papers without parsing status (before): {len(papers_without_status)}")
        logger.info(f"  Updated from logs: {stats['updated_from_logs']}")
        logger.info(f"  No DOI available: {stats['no_doi']}")
        logger.info(f"  Not found in logs: {stats['not_in_logs']}")
        
        # Show current distribution
        logger.info(f"\n" + "="*70)
        logger.info("CURRENT PARSING STATUS DISTRIBUTION")
        logger.info("="*70)
        
        cursor.execute("""
            SELECT parsing_status, COUNT(*) 
            FROM papers 
            GROUP BY parsing_status
            ORDER BY COUNT(*) DESC
        """)
        
        total_with_status = 0
        for status, count in cursor.fetchall():
            status_display = status if status else "NULL/Empty"
            logger.info(f"  {status_display}: {count} papers")
            if status:
                total_with_status += count
        
        # Get total papers
        cursor.execute("SELECT COUNT(*) FROM papers")
        total_papers = cursor.fetchone()[0]
        
        logger.info(f"\nTotal papers: {total_papers}")
        logger.info(f"Papers with parsing status: {total_with_status} ({total_with_status/total_papers*100:.2f}%)")
        logger.info("="*70)
        
        # Show some examples of papers still without status
        cursor.execute("""
            SELECT doi, title
            FROM papers 
            WHERE parsing_status IS NULL OR parsing_status = ''
            LIMIT 5
        """)
        
        remaining = cursor.fetchall()
        if remaining:
            logger.info(f"\nSample papers still without status:")
            for doi, title in remaining:
                doi_display = doi if doi else "[NO DOI]"
                title_display = title[:60] + "..." if title and len(title) > 60 else title
                logger.info(f"  {doi_display}: {title_display}")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        raise


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Update parsing_status from log files'
    )
    parser.add_argument(
        '--db',
        default='/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db',
        help='Path to papers.db database'
    )
    parser.add_argument(
        '--logs',
        nargs='+',
        default=[
            'logs/comprehensive_log_20251012_191215.log',
            'logs/comprehensive_log_20251012_193823.log',
            'logs/comprehensive_log_20251013_081309.log',
            'logs/comprehensive_log_20251013_083748.log',
            'logs/comprehensive_log_20251013_085654.log'
        ],
        help='Paths to log files'
    )
    
    args = parser.parse_args()
    
    update_parsing_status_from_logs(
        db_path=args.db,
        log_files=args.logs
    )
