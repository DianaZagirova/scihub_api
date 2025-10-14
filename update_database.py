#!/usr/bin/env python3
"""
Unified database updater for papers.db

Combines functionality from:
- update_database_from_jsons.py (extract data from JSONs)
- update_parsing_status_for_complete_papers.py (mark already-complete papers)
- update_parsing_status_from_logs.py (update status from logs)

This provides a single comprehensive solution for database updates.
"""

import sqlite3
import json
import os
import re
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UnifiedDatabaseUpdater:
    """Comprehensive database updater for papers.db"""
    
    def __init__(self, db_path: str, output_dir: str = './output'):
        """
        Initialize the updater.
        
        Args:
            db_path: Path to papers.db
            output_dir: Directory containing JSON outputs
        """
        self.db_path = db_path
        self.output_dir = output_dir
        self.conn = None
        self.cursor = None
        
        # Overall statistics
        self.stats = {
            'total_papers': 0,
            'json_updates': 0,
            'abstract_updated': 0,
            'sections_updated': 0,
            'status_from_jsons': 0,
            'status_complete_papers': 0,
            'status_from_logs': 0,
            'status_no_doi': 0,
            'errors': 0
        }
    
    def connect(self):
        """Connect to database and ensure schema is ready."""
        logger.info(f"Connecting to database: {self.db_path}")
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # Ensure parsing_status column exists
        self.cursor.execute("PRAGMA table_info(papers)")
        columns = [col[1] for col in self.cursor.fetchall()]
        
        if 'parsing_status' not in columns:
            logger.info("Adding parsing_status column to papers table")
            self.cursor.execute("ALTER TABLE papers ADD COLUMN parsing_status TEXT")
            self.conn.commit()
        
        # Get total papers
        self.cursor.execute("SELECT COUNT(*) FROM papers")
        self.stats['total_papers'] = self.cursor.fetchone()[0]
        logger.info(f"Total papers in database: {self.stats['total_papers']:,}")
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
    
    # ==================== JSON DATA EXTRACTION ====================
    
    def normalize_doi_to_filename(self, doi: str) -> str:
        """Convert DOI to filename format (replace / with _)."""
        return doi.replace('/', '_')
    
    def find_json_for_doi(self, doi: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Find JSON file for DOI.
        
        Returns:
            Tuple of (json_path, parser_type) or (None, None)
        """
        normalized = self.normalize_doi_to_filename(doi)
        
        # Try GROBID format first (no suffix)
        grobid_path = os.path.join(self.output_dir, f'{normalized}.json')
        if os.path.exists(grobid_path):
            return grobid_path, 'grobid'
        
        # Try PyMuPDF format (_fast suffix)
        fast_path = os.path.join(self.output_dir, f'{normalized}_fast.json')
        if os.path.exists(fast_path):
            return fast_path, 'PyMuPDF'
        
        return None, None
    
    def extract_grobid_data(self, json_path: str) -> Tuple[Optional[str], Optional[Dict]]:
        """Extract abstract and body from GROBID JSON."""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract abstract
            abstract = data.get('metadata', {}).get('abstract')
            
            # Extract and reformat body
            body_list = data.get('full_text', {}).get('body', [])
            body_dict = {}
            
            for section in body_list:
                title = section.get('title', 'Unnamed Section')
                content = section.get('content', '')
                
                if isinstance(content, list):
                    content = '\n\n'.join(content)
                
                body_dict[title] = content
            
            return abstract, body_dict if body_dict else None
            
        except Exception as e:
            logger.error(f"Error extracting GROBID data from {json_path}: {e}")
            return None, None
    
    def extract_pymupdf_data(self, json_path: str) -> Tuple[Optional[str], Optional[Dict]]:
        """Extract abstract and sections from PyMuPDF JSON."""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # PyMuPDF doesn't extract abstract separately
            abstract = None
            
            # Extract and reformat sections
            sections_list = data.get('structured_text', {}).get('sections', [])
            sections_dict = {}
            
            for section in sections_list:
                title = section.get('title', 'Unnamed Section')
                content = section.get('content', [])
                
                if isinstance(content, list):
                    content = '\n\n'.join(content)
                
                sections_dict[title] = content
            
            return abstract, sections_dict if sections_dict else None
            
        except Exception as e:
            logger.error(f"Error extracting PyMuPDF data from {json_path}: {e}")
            return None, None
    
    def update_from_jsons(self, dois_file: str, log_files: List[str] = None):
        """
        Update database with data extracted from JSONs.
        
        Args:
            dois_file: File containing DOIs to process (usually missing_dois.txt)
            log_files: Optional log files to extract parsing status
        """
        logger.info("\n" + "="*70)
        logger.info("STEP 1: UPDATING FROM JSON FILES")
        logger.info("="*70)
        
        # Read DOIs
        try:
            with open(dois_file, 'r', encoding='utf-8') as f:
                dois = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            logger.error(f"DOIs file not found: {dois_file}")
            return
        
        logger.info(f"Processing {len(dois)} DOIs from {dois_file}")
        
        # Parse logs if provided
        log_status_map = {}
        if log_files:
            log_status_map = self._parse_log_files(log_files)
        
        # Process each DOI
        for i, doi in enumerate(dois, 1):
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{len(dois)} DOIs processed")
            
            try:
                # Find JSON
                json_path, parser_type = self.find_json_for_doi(doi)
                
                if not json_path:
                    continue
                
                self.stats['json_updates'] += 1
                
                # Extract data based on parser type
                if parser_type == 'grobid':
                    abstract, sections = self.extract_grobid_data(json_path)
                else:  # PyMuPDF
                    abstract, sections = self.extract_pymupdf_data(json_path)
                
                # Get parsing status from logs
                result_status = log_status_map.get(doi, (None, None, None))[0]
                parsing_status = f"{result_status} (parser: {parser_type})" if result_status else f"parser: {parser_type}"
                
                # Check current database state
                self.cursor.execute(
                    "SELECT abstract, full_text_sections FROM papers WHERE doi = ?",
                    (doi,)
                )
                row = self.cursor.fetchone()
                
                if not row:
                    continue
                
                current_abstract, current_sections = row
                
                # Prepare updates
                updates = []
                params = []
                
                # Update abstract if missing and we have data
                if (not current_abstract or current_abstract.strip() == '') and abstract:
                    updates.append("abstract = ?")
                    params.append(abstract)
                    self.stats['abstract_updated'] += 1
                
                # Update full_text_sections if missing and we have data
                if (not current_sections or current_sections.strip() == '') and sections:
                    updates.append("full_text_sections = ?")
                    params.append(json.dumps(sections, ensure_ascii=False))
                    self.stats['sections_updated'] += 1
                
                # Always update parsing_status
                updates.append("parsing_status = ?")
                params.append(parsing_status)
                self.stats['status_from_jsons'] += 1
                
                # Execute update
                if updates:
                    sql = f"UPDATE papers SET {', '.join(updates)} WHERE doi = ?"
                    params.append(doi)
                    self.cursor.execute(sql, params)
            
            except Exception as e:
                logger.error(f"Error processing DOI {doi}: {e}")
                self.stats['errors'] += 1
        
        self.conn.commit()
        logger.info(f"Updated {self.stats['json_updates']} papers from JSON files")
    
    # ==================== COMPLETE PAPERS STATUS ====================
    
    def mark_complete_papers(self, missing_dois_file: str):
        """
        Mark papers NOT in missing_dois.txt as 'not required - already populated'.
        
        Args:
            missing_dois_file: File containing DOIs that needed processing
        """
        logger.info("\n" + "="*70)
        logger.info("STEP 2: MARKING ALREADY-COMPLETE PAPERS")
        logger.info("="*70)
        
        # Read missing DOIs
        try:
            with open(missing_dois_file, 'r', encoding='utf-8') as f:
                missing_dois = set(line.strip() for line in f if line.strip())
            logger.info(f"Found {len(missing_dois)} DOIs in {missing_dois_file}")
        except FileNotFoundError:
            logger.error(f"File not found: {missing_dois_file}")
            return
        
        # Get all DOIs from database
        self.cursor.execute("SELECT doi FROM papers")
        all_dois = [row[0] for row in self.cursor.fetchall()]
        
        # Find papers NOT in missing_dois.txt
        complete_papers = [doi for doi in all_dois if doi and doi not in missing_dois]
        
        logger.info(f"Papers NOT in {missing_dois_file}: {len(complete_papers):,}")
        
        # Update parsing_status for these papers (only if NULL or empty)
        for doi in complete_papers:
            self.cursor.execute(
                "SELECT parsing_status FROM papers WHERE doi = ?",
                (doi,)
            )
            row = self.cursor.fetchone()
            
            if row and (row[0] is None or row[0] == ''):
                self.cursor.execute(
                    "UPDATE papers SET parsing_status = ? WHERE doi = ?",
                    ("not required - already populated", doi)
                )
                self.stats['status_complete_papers'] += 1
        
        self.conn.commit()
        logger.info(f"Marked {self.stats['status_complete_papers']} papers as already complete")
    
    # ==================== LOG FILE PROCESSING ====================
    
    def _parse_log_files(self, log_files: List[str]) -> Dict[str, Tuple[str, str, str]]:
        """
        Parse log files to extract DOI processing status.
        
        Returns:
            dict: {doi: (result, parser_type, timestamp)}
        """
        doi_status = {}
        
        for log_file in log_files:
            if not Path(log_file).exists():
                logger.warning(f"Log file not found: {log_file}")
                continue
            
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Pattern to match log entries
                pattern = r'DOI/Identifier:\s*([^\n]+)\s+Timestamp:\s*([^\n]+).*?Result:\s*([^\n]+?)(?:\s+Parser:\s*([^\n]+))?(?=\n|$)'
                
                matches = re.findall(pattern, content, re.DOTALL)
                
                for doi, timestamp, result, parser in matches:
                    doi = doi.strip()
                    timestamp = timestamp.strip()
                    result = result.strip()
                    parser = parser.strip() if parser else None
                    
                    # Keep the latest entry for each DOI
                    if doi not in doi_status or timestamp > doi_status[doi][2]:
                        doi_status[doi] = (result, parser, timestamp)
            
            except Exception as e:
                logger.error(f"Error parsing log file {log_file}: {e}")
        
        return doi_status
    
    def update_from_logs(self, log_files: List[str]):
        """
        Update parsing_status from log files for papers without status.
        
        Args:
            log_files: List of log file paths
        """
        logger.info("\n" + "="*70)
        logger.info("STEP 3: UPDATING FROM LOG FILES")
        logger.info("="*70)
        
        # Parse log files
        doi_status = self._parse_log_files(log_files)
        logger.info(f"Found {len(doi_status):,} unique DOIs in log files")
        
        # Get papers without parsing_status
        self.cursor.execute("""
            SELECT doi 
            FROM papers 
            WHERE (parsing_status IS NULL OR parsing_status = '')
            AND (doi IS NOT NULL AND doi != '')
        """)
        papers_without_status = [row[0] for row in self.cursor.fetchall()]
        
        logger.info(f"Papers without status: {len(papers_without_status):,}")
        
        # Update from logs
        updated_count = 0
        not_in_logs_count = 0
        
        for doi in papers_without_status:
            if doi in doi_status:
                result, parser, timestamp = doi_status[doi]
                
                # Format status
                if parser:
                    status = f"{result} (parser: {parser})"
                else:
                    status = result
                
                self.cursor.execute(
                    "UPDATE papers SET parsing_status = ? WHERE doi = ?",
                    (status, doi)
                )
                updated_count += 1
            else:
                # DOI not found in any log
                self.cursor.execute(
                    "UPDATE papers SET parsing_status = ? WHERE doi = ?",
                    ("not processed - not found in logs", doi)
                )
                not_in_logs_count += 1
        
        self.stats['status_from_logs'] = updated_count + not_in_logs_count
        
        self.conn.commit()
        logger.info(f"Updated from logs: {updated_count:,}")
        logger.info(f"Not found in logs: {not_in_logs_count:,}")
    
    # ==================== NO DOI HANDLING ====================
    
    def mark_papers_without_doi(self):
        """Mark papers that have no DOI."""
        logger.info("\n" + "="*70)
        logger.info("STEP 4: MARKING PAPERS WITHOUT DOI")
        logger.info("="*70)
        
        self.cursor.execute("""
            UPDATE papers 
            SET parsing_status = 'no DOI available'
            WHERE (doi IS NULL OR doi = '')
            AND (parsing_status IS NULL OR parsing_status = '')
        """)
        
        self.stats['status_no_doi'] = self.cursor.rowcount
        self.conn.commit()
        
        logger.info(f"Marked {self.stats['status_no_doi']:,} papers without DOI")
    
    # ==================== REPORTING ====================
    
    def generate_report(self):
        """Generate comprehensive status report."""
        logger.info("\n" + "="*70)
        logger.info("COMPREHENSIVE DATABASE UPDATE REPORT")
        logger.info("="*70)
        
        # Overall statistics
        logger.info(f"\nTotal papers in database: {self.stats['total_papers']:,}")
        logger.info(f"\nData updates:")
        logger.info(f"  Papers with JSON found: {self.stats['json_updates']:,}")
        logger.info(f"  Abstracts updated: {self.stats['abstract_updated']:,}")
        logger.info(f"  Full text sections updated: {self.stats['sections_updated']:,}")
        
        logger.info(f"\nParsing status updates:")
        logger.info(f"  From JSON processing: {self.stats['status_from_jsons']:,}")
        logger.info(f"  Complete papers marked: {self.stats['status_complete_papers']:,}")
        logger.info(f"  From log files: {self.stats['status_from_logs']:,}")
        logger.info(f"  Papers without DOI: {self.stats['status_no_doi']:,}")
        
        logger.info(f"\nErrors: {self.stats['errors']:,}")
        
        # Parsing status distribution
        logger.info("\n" + "-"*70)
        logger.info("PARSING STATUS DISTRIBUTION")
        logger.info("-"*70)
        
        self.cursor.execute("""
            SELECT parsing_status, COUNT(*) 
            FROM papers 
            GROUP BY parsing_status
            ORDER BY COUNT(*) DESC
        """)
        
        total_with_status = 0
        for status, count in self.cursor.fetchall():
            status_display = status if status else "NULL/Empty"
            logger.info(f"  {status_display}: {count:,} papers")
            if status:
                total_with_status += count
        
        coverage = (total_with_status / self.stats['total_papers'] * 100) if self.stats['total_papers'] > 0 else 0
        logger.info(f"\nTotal with status: {total_with_status:,} ({coverage:.2f}%)")
        
        # Content coverage
        logger.info("\n" + "-"*70)
        logger.info("CONTENT COVERAGE")
        logger.info("-"*70)
        
        self.cursor.execute("SELECT COUNT(*) FROM papers WHERE abstract IS NOT NULL AND abstract != ''")
        with_abstract = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM papers WHERE full_text_sections IS NOT NULL AND full_text_sections != ''")
        with_full_text = self.cursor.fetchone()[0]
        
        logger.info(f"  Papers with abstract: {with_abstract:,} ({with_abstract/self.stats['total_papers']*100:.2f}%)")
        logger.info(f"  Papers with full text: {with_full_text:,} ({with_full_text/self.stats['total_papers']*100:.2f}%)")
        
        logger.info("\n" + "="*70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Unified database updater for papers.db',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Operations (can be combined):
  --update-from-jsons    Extract data from JSON files and update database
  --mark-complete        Mark already-complete papers (not in missing DOIs)
  --update-from-logs     Update parsing status from log files
  --mark-no-doi          Mark papers without DOI

Examples:
  # Run all updates (recommended)
  python update_database.py --all
  
  # Only update from JSONs
  python update_database.py --update-from-jsons
  
  # Update from JSONs and mark complete papers
  python update_database.py --update-from-jsons --mark-complete
  
  # Only update parsing status
  python update_database.py --update-from-logs --mark-no-doi
        """
    )
    
    # Database and file paths
    parser.add_argument(
        '--db',
        default='/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db',
        help='Path to papers.db database'
    )
    parser.add_argument(
        '--dois',
        default='missing_dois.txt',
        help='Path to file containing DOIs (one per line)'
    )
    parser.add_argument(
        '--output-dir',
        default='./output',
        help='Directory containing JSON files'
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
    
    # Operation flags
    parser.add_argument('--all', action='store_true',
                       help='Run all update operations (recommended)')
    parser.add_argument('--update-from-jsons', action='store_true',
                       help='Update database from JSON files')
    parser.add_argument('--mark-complete', action='store_true',
                       help='Mark already-complete papers')
    parser.add_argument('--update-from-logs', action='store_true',
                       help='Update parsing status from log files')
    parser.add_argument('--mark-no-doi', action='store_true',
                       help='Mark papers without DOI')
    
    args = parser.parse_args()
    
    # If --all is specified, enable all operations
    if args.all:
        args.update_from_jsons = True
        args.mark_complete = True
        args.update_from_logs = True
        args.mark_no_doi = True
    
    # Check if at least one operation is specified
    if not any([args.update_from_jsons, args.mark_complete, 
                args.update_from_logs, args.mark_no_doi]):
        parser.print_help()
        print("\nError: Please specify at least one operation or use --all")
        return 1
    
    # Create updater
    updater = UnifiedDatabaseUpdater(
        db_path=args.db,
        output_dir=args.output_dir
    )
    
    try:
        # Connect to database
        updater.connect()
        
        # Run requested operations in logical order
        if args.update_from_jsons:
            updater.update_from_jsons(args.dois, args.logs)
        
        if args.mark_complete:
            updater.mark_complete_papers(args.dois)
        
        if args.update_from_logs:
            updater.update_from_logs(args.logs)
        
        if args.mark_no_doi:
            updater.mark_papers_without_doi()
        
        # Generate comprehensive report
        updater.generate_report()
        
    finally:
        updater.close()
    
    logger.info("\nDatabase update complete!")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
