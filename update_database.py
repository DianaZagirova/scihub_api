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
from trackers.doi_tracker_db import DOITracker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UnifiedDatabaseUpdater:
    """Comprehensive database updater for papers.db"""
    
    def __init__(self, db_path: str, output_dir: str = './output', tracker_db: str = 'processing_tracker.db'):
        """
        Initialize the updater.
        
        Args:
            db_path: Path to papers.db
            output_dir: Directory containing JSON outputs
            tracker_file: Path to DOI tracker CSV file
        """
        self.db_path = db_path
        self.output_dir = output_dir
        self.tracker_db = tracker_db
        self.tracker = DOITracker(db_path=self.tracker_db)
        self.conn = None
        self.cursor = None
        
        # Overall statistics
        self.stats = {
            'total_papers': 0,
            'json_updates': 0,
            'skipped_already_complete': 0,
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
        
        # Enable WAL mode for better write performance
        self.cursor.execute("PRAGMA journal_mode=WAL")
        # Increase cache size for better performance (negative = KB, 10MB cache)
        self.cursor.execute("PRAGMA cache_size=-10000")
        # Disable synchronous for much faster writes (less safe but acceptable for this use case)
        self.cursor.execute("PRAGMA synchronous=NORMAL")
        
        # Ensure parsing_status column exists
        self.cursor.execute("PRAGMA table_info(papers)")
        columns = [col[1] for col in self.cursor.fetchall()]
        
        if 'parsing_status' not in columns:
            logger.info("Adding parsing_status column to papers table")
            self.cursor.execute("ALTER TABLE papers ADD COLUMN parsing_status TEXT")
            self.conn.commit()
        
        # Create index on doi for faster lookups if not exists
        logger.info("Ensuring database indices for performance...")
        try:
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_papers_doi ON papers(doi)")
            self.conn.commit()
        except Exception as e:
            logger.warning(f"Could not create index: {e}")
        
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
        Find JSON file for DOI. Prefers GROBID if it has content, otherwise PyMuPDF.
        
        Returns:
            Tuple of (json_path, parser_type) or (None, None)
        """
        normalized = self.normalize_doi_to_filename(doi)
        
        grobid_path = os.path.join(self.output_dir, f'{normalized}.json')
        fast_path = os.path.join(self.output_dir, f'{normalized}_fast.json')
        
        grobid_exists = os.path.exists(grobid_path)
        fast_exists = os.path.exists(fast_path)
        
        # If only one exists, return it
        if grobid_exists and not fast_exists:
            return grobid_path, 'grobid'
        if fast_exists and not grobid_exists:
            return fast_path, 'PyMuPDF'
        
        # If both exist, prefer GROBID but check if it has content
        if grobid_exists and fast_exists:
            try:
                with open(grobid_path, 'r', encoding='utf-8') as f:
                    grobid_data = json.load(f)
                body_list = grobid_data.get('full_text', {}).get('body', [])
                
                # If GROBID has body content, use it
                if body_list and len(body_list) > 0:
                    return grobid_path, 'grobid'
                
                # Otherwise, use PyMuPDF
                return fast_path, 'PyMuPDF'
            except:
                # If error reading GROBID, fall back to PyMuPDF
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
    
    def update_from_jsons(self, dois_files: List[str] = None):
        """
        Update database with data extracted from JSONs.
        
        NEW LOGIC:
        - Automatically finds DOIs from database that need updating:
          1. Papers missing abstract OR full_text, OR
          2. Papers parsed with non-Grobid (if Grobid JSON available)
        - Checks for corresponding JSON files
        - Extracts and updates data
        - Applies Grobid priority logic
        - Uses DOI tracker for parsing status (not logs)
        
        Args:
            dois_files: Optional list of files with DOIs (legacy support, can be None)
        """
        logger.info("\n" + "="*70)
        logger.info("STEP 1: UPDATING FROM JSON FILES (with tracker)")
        logger.info("="*70)
        
        # NEW: Get DOIs directly from database that need updating
        logger.info("Querying database for DOIs that need updating...")
        
        self.cursor.execute("""
            SELECT doi, abstract, full_text, full_text_sections, parsing_status
            FROM papers
            WHERE doi IS NOT NULL AND doi != ''
            AND (
                -- Missing abstract OR full text
                (abstract IS NULL OR abstract = '' OR 
                 full_text IS NULL OR full_text = '' OR
                 full_text_sections IS NULL OR full_text_sections = '')
                OR
                -- Parsed but NOT with Grobid (to apply Grobid priority)
                (parsing_status IS NOT NULL AND parsing_status != '' 
                 AND parsing_status NOT LIKE '%grobid%')
            )
        """)
        
        all_rows = self.cursor.fetchall()
        logger.info(f"Found {len(all_rows):,} DOIs in database that need updating")
        
        # Build DOI list and state dict
        dois = []
        db_state = {}
        
        for row in all_rows:
            doi, abstract, full_text, full_text_sections, parsing_status = row
            dois.append(doi)
            db_state[doi] = (abstract, full_text, full_text_sections, parsing_status)
        
        if not dois:
            logger.info("No DOIs found that need updating. Database is up-to-date!")
            return
        
        logger.info(f"Processing {len(dois):,} DOIs from database")
        logger.info(f"Using tracker for parsing status (DB): {self.tracker_db}")
        
        # Process each DOI
        processed = 0
        skipped_no_json = 0
        for i, doi in enumerate(dois, 1):
            if i % 1000 == 0:
                logger.info(f"Progress: {i}/{len(dois)} DOIs checked, {processed} JSONs found, {skipped_no_json} skipped (no JSON)")
            
            try:
                # Find JSON first (fast file check)
                json_path, parser_type = self.find_json_for_doi(doi)
                
                if not json_path:
                    skipped_no_json += 1
                    continue
                
                processed += 1
                self.stats['json_updates'] += 1
                
                # Get current database state from pre-loaded cache
                if doi not in db_state:
                    continue
                
                current_abstract, current_full_text, current_sections, current_parsing_status = db_state[doi]
                
                # Check what's missing in the database
                has_abstract = current_abstract and current_abstract.strip() != ''
                has_full_text = (current_full_text and current_full_text.strip() != '') or (current_sections and current_sections.strip() != '')
                
                # SPECIAL CASE: If parsed with non-Grobid but Grobid JSON exists,
                # prefer Grobid for full_text (and abstract if missing)
                check_grobid_override = False
                if current_parsing_status and 'grobid' not in current_parsing_status.lower():
                    # Paper was parsed with something else (PyMuPDF, etc)
                    # Check if Grobid JSON exists
                    normalized = self.normalize_doi_to_filename(doi)
                    grobid_path = os.path.join(self.output_dir, f'{normalized}.json')
                    
                    if os.path.exists(grobid_path):
                        # Grobid JSON exists - use it to override PyMuPDF data
                        # Note: find_json_for_doi already prefers GROBID, so parser_type
                        # would already be 'grobid' if both files exist
                        json_path = grobid_path
                        parser_type = 'grobid'
                        check_grobid_override = True
                
                # Skip if paper already has BOTH abstract AND full text (unless Grobid override)
                if has_abstract and has_full_text and not check_grobid_override:
                    self.stats['skipped_already_complete'] += 1
                    continue
                
                # Extract data based on parser type
                if parser_type == 'grobid':
                    abstract, sections = self.extract_grobid_data(json_path)
                else:  # PyMuPDF
                    abstract, sections = self.extract_pymupdf_data(json_path)
                
                # Get parsing status from TRACKER (not logs)
                tracker_status = self.tracker.get_status(doi)
                
                # Determine parsing status based on tracker and current parsing
                if check_grobid_override and current_parsing_status:
                    # Grobid override: append Grobid to existing
                    parsing_status = f"{current_parsing_status} | grobid: success"
                elif tracker_status:
                    # Use tracker status
                    pymupdf_status = tracker_status.get('pymupdf_status', '')
                    grobid_status = tracker_status.get('grobid_status', '')
                    
                    status_parts = []
                    if pymupdf_status == self.tracker.STATUS_SUCCESS:
                        status_parts.append("success (parser: PyMuPDF)")
                    if grobid_status == self.tracker.STATUS_SUCCESS:
                        status_parts.append("grobid: success" if status_parts else "success (parser: grobid)")
                    
                    parsing_status = " | ".join(status_parts) if status_parts else f"parser: {parser_type}"
                else:
                    # Fallback: basic parser info
                    parsing_status = f"parser: {parser_type}"
                
                # Prepare updates
                updates = []
                params = []
                
                # Update abstract if missing and we have data
                if (not current_abstract or current_abstract.strip() == '') and abstract:
                    updates.append("abstract = ?")
                    params.append(abstract)
                    self.stats['abstract_updated'] += 1
                
                # Update full_text and full_text_sections if:
                # 1. Missing and we have data, OR
                # 2. Grobid override (replace non-Grobid full text with Grobid)
                if sections:
                    # Convert sections dict to full_text string
                    full_text_str = '\n\n'.join([f"{title}\n{content}" for title, content in sections.items()])
                    
                    # Update full_text if missing or Grobid override
                    if ((not current_full_text or current_full_text.strip() == '') and full_text_str) or \
                       (check_grobid_override and full_text_str):
                        updates.append("full_text = ?")
                        params.append(full_text_str)
                        self.stats['sections_updated'] += 1
                    
                    # Update full_text_sections if missing or Grobid override
                    if ((not current_sections or current_sections.strip() == '') and sections) or \
                       (check_grobid_override and sections):
                        updates.append("full_text_sections = ?")
                        params.append(json.dumps(sections, ensure_ascii=False))
                
                # Update parsing_status
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
            
            # Commit every 5000 records for better performance
            if i % 5000 == 0:
                self.conn.commit()
        
        self.conn.commit()
        logger.info(f"\nProcessing complete:")
        logger.info(f"  Total DOIs checked: {len(dois):,}")
        logger.info(f"  JSONs found: {self.stats['json_updates']:,}")
        logger.info(f"  Skipped (no JSON): {skipped_no_json:,}")
        logger.info(f"  Papers updated: {self.stats['abstract_updated']:,}")
    
    # ==================== COMPLETE PAPERS STATUS ====================
    
    def mark_complete_papers(self, missing_dois_files: List[str]):
        """
        Mark papers NOT in missing_dois files as 'not required - already populated'.
        
        Args:
            missing_dois_files: List of files containing DOIs that needed processing
        """
        logger.info("\n" + "="*70)
        logger.info("STEP 2: MARKING ALREADY-COMPLETE PAPERS")
        logger.info("="*70)
        
        # Read missing DOIs from all files
        missing_dois = set()
        for dois_file in missing_dois_files:
            try:
                with open(dois_file, 'r', encoding='utf-8') as f:
                    file_dois = set(line.strip() for line in f if line.strip())
                    missing_dois.update(file_dois)
                    logger.info(f"Loaded {len(file_dois)} DOIs from {dois_file}")
            except FileNotFoundError:
                logger.error(f"File not found: {dois_file}")
        
        logger.info(f"Total {len(missing_dois)} unique DOIs across all files")
        
        # Get all DOIs from database
        self.cursor.execute("SELECT doi FROM papers")
        all_dois = [row[0] for row in self.cursor.fetchall()]
        
        # Find papers NOT in missing_dois files
        complete_papers = [doi for doi in all_dois if doi and doi not in missing_dois]
        
        logger.info(f"Papers NOT in missing_dois files: {len(complete_papers):,}")
        
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
    
    def update_from_logs(self, log_files: List[str] = None):
        """
        Update parsing_status from log files for papers without status.
        
        Args:
            log_files: List of log file paths (optional)
        """
        if not log_files:
            logger.warning("No log files specified, skipping log-based status update")
            return
        
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
        logger.info(f"  Papers skipped (already have abstract): {self.stats['skipped_already_complete']:,}")
        logger.info(f"  Abstracts updated: {self.stats['abstract_updated']:,}")
        logger.info(f"  Full text sections updated (disabled): {self.stats['sections_updated']:,}")
        
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
                         (Auto-discovers DOIs + uses tracker for status)
  --mark-complete        Mark already-complete papers (requires --dois)
  --update-from-logs     Update parsing status from log files (LEGACY - tracker preferred)
  --mark-no-doi          Mark papers without DOI

NEW BEHAVIOR:
  --update-from-jsons now automatically queries the database for DOIs that need
  updating (missing abstract/full_text OR parsed with non-Grobid). 
  
  Parsing status is read from doi_processing_tracker.csv (the source of truth),
  not from log files. The tracker is updated by download_papers_optimized.py,
  grobid_tracker_integration.py, and reconcile_all_status.py.

Examples:
  # Run all updates (recommended) - auto-discovers DOIs, uses tracker
  python update_database.py --all
  
  # Only update from JSONs (auto-discovery with tracker status)
  python update_database.py --update-from-jsons
  
  # Update with specific DOI files (legacy mode)
  python update_database.py --update-from-jsons --dois file1.txt file2.txt
  
  # Mark complete papers (requires DOI files)
  python update_database.py --mark-complete --dois missing_dois.txt
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
        nargs='+',
        default=None,
        help='(Optional) Path(s) to file(s) containing DOIs. '
             'Only required for --mark-complete. '
             '--update-from-jsons auto-discovers DOIs from database if not specified.'
    )
    parser.add_argument(
        '--output-dir',
        default='./output',
        help='Directory containing JSON files'
    )
    parser.add_argument(
        '--logs',
        nargs='+',
        default=None,
        help='(Optional) Paths to log files for parsing status extraction'
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
        args.mark_complete = False  # Skip mark_complete as it requires --dois
        args.update_from_logs = True
        args.mark_no_doi = True
    
    # Check if at least one operation is specified
    if not any([args.update_from_jsons, args.mark_complete, 
                args.update_from_logs, args.mark_no_doi]):
        parser.print_help()
        print("\nError: Please specify at least one operation or use --all")
        return 1
    
    # Validate mark_complete requires DOI files
    if args.mark_complete and not args.dois:
        print("\nError: --mark-complete requires --dois to specify DOI files")
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
            # Pass None for dois to enable auto-discovery, unless user specified --dois
            updater.update_from_jsons(args.dois)
        
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
