#!/usr/bin/env python3
"""
Script to update papers.db with extracted JSON data and parsing status.

Process:
1. Read DOIs from missing_dois.txt
2. Find corresponding JSONs (GROBID or PyMuPDF)
3. Extract abstract and full_text sections
4. Parse logs for extraction status
5. Update database with missing data
"""

import sqlite3
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseUpdater:
    """Updates papers.db with extracted data from JSONs and logs."""
    
    def __init__(
        self,
        db_path: str,
        dois_file: str,
        output_dir: str = './output',
        log_files: List[str] = None
    ):
        """Initialize the updater."""
        self.db_path = db_path
        self.dois_file = dois_file
        self.output_dir = output_dir
        self.log_files = log_files or []
        
        # Statistics
        self.stats = {
            'total_dois': 0,
            'json_found': 0,
            'abstract_updated': 0,
            'sections_updated': 0,
            'status_updated': 0,
            'no_json': 0,
            'errors': 0
        }
    
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
        """
        Extract abstract and body from GROBID JSON.
        
        Returns:
            Tuple of (abstract, body_dict)
        """
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
                
                # Handle content as string or list
                if isinstance(content, list):
                    content = '\n\n'.join(content)
                
                body_dict[title] = content
            
            return abstract, body_dict if body_dict else None
            
        except Exception as e:
            logger.error(f"Error extracting GROBID data from {json_path}: {e}")
            return None, None
    
    def extract_pymupdf_data(self, json_path: str) -> Tuple[Optional[str], Optional[Dict]]:
        """
        Extract abstract and sections from PyMuPDF JSON.
        
        Returns:
            Tuple of (abstract, sections_dict)
        """
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
                
                # Content is a list of strings, join them
                if isinstance(content, list):
                    content = '\n\n'.join(content)
                
                sections_dict[title] = content
            
            return abstract, sections_dict if sections_dict else None
            
        except Exception as e:
            logger.error(f"Error extracting PyMuPDF data from {json_path}: {e}")
            return None, None
    
    def parse_logs_for_status(self, doi: str) -> Optional[str]:
        """
        Parse log files to find the LAST Result status for a DOI.
        
        Returns:
            Result status string or None
        """
        last_result = None
        last_timestamp = None
        
        for log_file in self.log_files:
            if not os.path.exists(log_file):
                logger.warning(f"Log file not found: {log_file}")
                continue
            
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Find all entries for this DOI
                # Pattern: DOI/Identifier: <doi> ... Timestamp: <timestamp> ... Result: <status>
                pattern = rf'DOI/Identifier: {re.escape(doi)}\s+Timestamp: ([^\n]+).*?Result: ([^\n]+)'
                matches = re.findall(pattern, content, re.DOTALL)
                
                for timestamp, result in matches:
                    if last_timestamp is None or timestamp > last_timestamp:
                        last_timestamp = timestamp.strip()
                        last_result = result.strip()
            
            except Exception as e:
                logger.error(f"Error parsing log {log_file}: {e}")
        
        return last_result
    
    def update_database(self):
        """Main method to update the database."""
        logger.info(f"Connecting to database: {self.db_path}")
        
        # Check if parsing_status column exists, if not add it
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Check if column exists
            cursor.execute("PRAGMA table_info(papers)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'parsing_status' not in columns:
                logger.info("Adding parsing_status column to papers table")
                cursor.execute("ALTER TABLE papers ADD COLUMN parsing_status TEXT")
                conn.commit()
        except Exception as e:
            logger.error(f"Error checking/adding parsing_status column: {e}")
        
        # Read DOIs
        logger.info(f"Reading DOIs from {self.dois_file}")
        with open(self.dois_file, 'r', encoding='utf-8') as f:
            dois = [line.strip() for line in f if line.strip()]
        
        self.stats['total_dois'] = len(dois)
        logger.info(f"Processing {len(dois)} DOIs")
        
        # Process each DOI
        for i, doi in enumerate(dois, 1):
            if i % 50 == 0:
                logger.info(f"Progress: {i}/{len(dois)} DOIs processed")
            
            try:
                # Find JSON
                json_path, parser_type = self.find_json_for_doi(doi)
                
                if not json_path:
                    self.stats['no_json'] += 1
                    logger.debug(f"No JSON found for DOI: {doi}")
                    continue
                
                self.stats['json_found'] += 1
                
                # Extract data based on parser type
                if parser_type == 'grobid':
                    abstract, sections = self.extract_grobid_data(json_path)
                else:  # PyMuPDF
                    abstract, sections = self.extract_pymupdf_data(json_path)
                
                # Get parsing status from logs
                result_status = self.parse_logs_for_status(doi)
                parsing_status = f"{result_status} (parser: {parser_type})" if result_status else f"parser: {parser_type}"
                
                # Check current database state
                cursor.execute(
                    "SELECT abstract, full_text_sections FROM papers WHERE doi = ?",
                    (doi,)
                )
                row = cursor.fetchone()
                
                if not row:
                    logger.warning(f"DOI not found in database: {doi}")
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
                self.stats['status_updated'] += 1
                
                # Execute update
                if updates:
                    sql = f"UPDATE papers SET {', '.join(updates)} WHERE doi = ?"
                    params.append(doi)
                    cursor.execute(sql, params)
            
            except Exception as e:
                logger.error(f"Error processing DOI {doi}: {e}")
                self.stats['errors'] += 1
        
        # Commit and close
        conn.commit()
        conn.close()
        
        # Print statistics
        logger.info("\n" + "="*60)
        logger.info("UPDATE STATISTICS")
        logger.info("="*60)
        logger.info(f"Total DOIs processed: {self.stats['total_dois']}")
        logger.info(f"JSONs found: {self.stats['json_found']}")
        logger.info(f"No JSON found: {self.stats['no_json']}")
        logger.info(f"Abstracts updated: {self.stats['abstract_updated']}")
        logger.info(f"Full text sections updated: {self.stats['sections_updated']}")
        logger.info(f"Parsing status updated: {self.stats['status_updated']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info("="*60)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Update papers.db with extracted JSON data and parsing status'
    )
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
    
    args = parser.parse_args()
    
    # Create updater and run
    updater = DatabaseUpdater(
        db_path=args.db,
        dois_file=args.dois,
        output_dir=args.output_dir,
        log_files=args.logs
    )
    
    updater.update_database()
    
    logger.info("Database update complete!")


if __name__ == '__main__':
    main()
