#!/usr/bin/env python3
"""
Comprehensive reconciliation script to sync:
  1. Filesystem (JSON files in output/)
  2. DOI Tracker (doi_processing_tracker.csv)
  3. Database (papers.db)

This is the SINGLE SOURCE OF TRUTH updater - run this to catch everything.
"""

import os
import sqlite3
import json
import logging
from pathlib import Path
from typing import Dict, Set, Tuple
from doi_tracker import DOITracker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StatusReconciler:
    """Reconcile status across filesystem, tracker, and database."""
    
    def __init__(
        self,
        output_dir: str = './output',
        tracker_file: str = 'doi_processing_tracker.csv',
        db_path: str = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
    ):
        self.output_dir = Path(output_dir)
        self.tracker = DOITracker(tracker_file)
        self.db_path = db_path
        
        self.stats = {
            'json_files_found': 0,
            'tracker_updated': 0,
            'database_updated': 0,
            'grobid_files': 0,
            'pymupdf_files': 0,
            'discrepancies_found': 0
        }
    
    def filename_to_doi(self, filename: str) -> Tuple[str, str]:
        """
        Convert filename back to DOI.
        
        Returns:
            (doi, parser_type): 'grobid', 'pymupdf', or 'unknown'
        """
        # Remove extension
        name = filename.replace('.json', '')
        
        # Check if PyMuPDF (ends with _fast)
        if name.endswith('_fast'):
            name = name[:-5]  # Remove _fast
            parser = 'pymupdf'
        else:
            parser = 'grobid'
        
        # Convert _ back to /
        doi = name.replace('_', '/')
        
        return doi, parser
    
    def scan_output_directory(self) -> Dict[str, Set[str]]:
        """
        Scan output directory for all JSON files.
        
        Returns:
            dict: {doi: set of parsers}
        """
        logger.info(f"Scanning {self.output_dir} for JSON files...")
        
        dois_found = {}
        
        if not self.output_dir.exists():
            logger.warning(f"Output directory not found: {self.output_dir}")
            return dois_found
        
        for json_file in self.output_dir.glob('*.json'):
            self.stats['json_files_found'] += 1
            
            doi, parser = self.filename_to_doi(json_file.name)
            
            if doi not in dois_found:
                dois_found[doi] = set()
            
            dois_found[doi].add(parser)
            
            if parser == 'grobid':
                self.stats['grobid_files'] += 1
            elif parser == 'pymupdf':
                self.stats['pymupdf_files'] += 1
        
        logger.info(f"Found {self.stats['json_files_found']} JSON files")
        logger.info(f"  - Grobid: {self.stats['grobid_files']}")
        logger.info(f"  - PyMuPDF: {self.stats['pymupdf_files']}")
        logger.info(f"  - Unique DOIs: {len(dois_found)}")
        
        return dois_found
    
    def update_tracker_from_filesystem(self, dois_found: Dict[str, Set[str]]):
        """Update tracker based on filesystem scan."""
        logger.info("\nUpdating tracker from filesystem...")
        
        bulk_updates = []
        
        for doi, parsers in dois_found.items():
            status = self.tracker.get_status(doi)
            needs_update = False
            update_data = {'doi': doi}
            
            # Mark as downloaded (if we have JSON, we must have had the PDF)
            if not status or status.get('downloaded') != self.tracker.AVAILABLE_YES:
                update_data['downloaded'] = self.tracker.AVAILABLE_YES
                update_data['scihub_available'] = self.tracker.AVAILABLE_YES
                needs_update = True
            
            # Update PyMuPDF status
            if 'pymupdf' in parsers:
                if not status or status.get('pymupdf_status') != self.tracker.STATUS_SUCCESS:
                    update_data['pymupdf_status'] = self.tracker.STATUS_SUCCESS
                    needs_update = True
            
            # Update Grobid status
            if 'grobid' in parsers:
                if not status or status.get('grobid_status') != self.tracker.STATUS_SUCCESS:
                    update_data['grobid_status'] = self.tracker.STATUS_SUCCESS
                    needs_update = True
            
            if needs_update:
                bulk_updates.append(update_data)
        
        if bulk_updates:
            logger.info(f"Updating {len(bulk_updates)} DOIs in tracker...")
            self.tracker.bulk_update(bulk_updates, defer_write=True)
            self.tracker.flush()
            self.stats['tracker_updated'] = len(bulk_updates)
            logger.info(f"Tracker updated: {len(bulk_updates)} DOIs")
        else:
            logger.info("Tracker is up-to-date")
    
    def update_database_from_jsons(self, dois_found: Dict[str, Set[str]]):
        """
        Update database with data from JSON files.
        Extracts abstract and full text from JSONs.
        """
        logger.info("\nUpdating database from JSON files...")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all DOIs from database
        cursor.execute("SELECT doi FROM papers WHERE doi IS NOT NULL AND doi != ''")
        db_dois = set(row[0] for row in cursor.fetchall())
        
        updated = 0
        not_in_db = 0
        
        for doi, parsers in dois_found.items():
            # Check if DOI exists in database
            if doi not in db_dois:
                not_in_db += 1
                continue
            
            # Get current database state
            cursor.execute(
                "SELECT abstract, full_text_sections, parsing_status FROM papers WHERE doi = ?",
                (doi,)
            )
            row = cursor.fetchone()
            if not row:
                continue
            
            current_abstract, current_sections, current_status = row
            
            # Determine what needs updating
            needs_update = False
            update_fields = {}
            
            # GROBID PRIORITY LOGIC:
            # If paper was parsed with non-Grobid, but Grobid JSON exists,
            # use Grobid for full_text (and abstract if missing)
            json_path = None
            parser_used = None
            use_grobid_for_fulltext = False
            
            # Check if we should prioritize Grobid for full text
            if current_status and 'grobid' not in current_status.lower() and 'grobid' in parsers:
                # Paper parsed with other parser, but Grobid JSON exists
                # Use Grobid for full text update
                json_filename = doi.replace('/', '_') + '.json'
                json_path = self.output_dir / json_filename
                parser_used = 'grobid'
                use_grobid_for_fulltext = True
            elif 'grobid' in parsers:
                # Normal Grobid processing
                json_filename = doi.replace('/', '_') + '.json'
                json_path = self.output_dir / json_filename
                parser_used = 'grobid'
            elif 'pymupdf' in parsers:
                json_filename = doi.replace('/', '_') + '_fast.json'
                json_path = self.output_dir / json_filename
                parser_used = 'pymupdf'
            
            if json_path and json_path.exists():
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Extract abstract (if missing)
                    if not current_abstract or current_abstract.strip() == '':
                        if parser_used == 'grobid':
                            abstract = data.get('metadata', {}).get('abstract')
                            if abstract:
                                update_fields['abstract'] = abstract
                                needs_update = True
                    
                    # Extract full text if:
                    # 1. Missing, OR
                    # 2. Grobid override (replace with better Grobid data)
                    should_update_fulltext = (not current_sections or current_sections.strip() == '') or use_grobid_for_fulltext
                    
                    if should_update_fulltext:
                        sections_dict = {}
                        
                        if parser_used == 'grobid':
                            body_list = data.get('full_text', {}).get('body', [])
                            for section in body_list:
                                title = section.get('title', 'Unnamed Section')
                                content = section.get('content', '')
                                if isinstance(content, list):
                                    content = '\n\n'.join(content)
                                sections_dict[title] = content
                        
                        elif parser_used == 'pymupdf':
                            sections_list = data.get('structured_text', {}).get('sections', [])
                            for section in sections_list:
                                title = section.get('title', 'Unnamed Section')
                                content = section.get('content', [])
                                if isinstance(content, list):
                                    content = '\n\n'.join(content)
                                sections_dict[title] = content
                        
                        if sections_dict:
                            update_fields['full_text_sections'] = json.dumps(sections_dict, ensure_ascii=False)
                            needs_update = True
                    
                    # Update parsing_status
                    if use_grobid_for_fulltext and current_status:
                        # Append Grobid to existing status
                        new_status = f"{current_status} | grobid: success"
                    else:
                        new_status = f"success (parser: {parser_used})"
                    
                    if current_status != new_status:
                        update_fields['parsing_status'] = new_status
                        needs_update = True
                    
                except Exception as e:
                    logger.error(f"Error processing {json_path}: {e}")
            
            # Perform update
            if needs_update and update_fields:
                set_clause = ', '.join([f"{k} = ?" for k in update_fields.keys()])
                values = list(update_fields.values()) + [doi]
                
                cursor.execute(
                    f"UPDATE papers SET {set_clause} WHERE doi = ?",
                    values
                )
                updated += 1
        
        conn.commit()
        conn.close()
        
        self.stats['database_updated'] = updated
        logger.info(f"Database updated: {updated} papers")
        if not_in_db > 0:
            logger.info(f"  (Skipped {not_in_db} DOIs not in database)")
    
    def find_discrepancies(self) -> Dict[str, list]:
        """
        Find discrepancies between tracker and filesystem.
        
        Returns:
            dict with lists of discrepancies
        """
        logger.info("\nChecking for discrepancies...")
        
        # Scan filesystem
        dois_in_files = self.scan_output_directory()
        
        # Get tracker status
        discrepancies = {
            'in_files_not_tracker': [],
            'tracker_wrong_status': []
        }
        
        for doi, parsers_in_files in dois_in_files.items():
            status = self.tracker.get_status(doi)
            
            if not status:
                discrepancies['in_files_not_tracker'].append(doi)
                self.stats['discrepancies_found'] += 1
                continue
            
            # Check if tracker status matches filesystem
            if 'pymupdf' in parsers_in_files:
                if status.get('pymupdf_status') != self.tracker.STATUS_SUCCESS:
                    discrepancies['tracker_wrong_status'].append({
                        'doi': doi,
                        'parser': 'pymupdf',
                        'tracker_status': status.get('pymupdf_status'),
                        'should_be': 'success'
                    })
                    self.stats['discrepancies_found'] += 1
            
            if 'grobid' in parsers_in_files:
                if status.get('grobid_status') != self.tracker.STATUS_SUCCESS:
                    discrepancies['tracker_wrong_status'].append({
                        'doi': doi,
                        'parser': 'grobid',
                        'tracker_status': status.get('grobid_status'),
                        'should_be': 'success'
                    })
                    self.stats['discrepancies_found'] += 1
        
        return discrepancies
    
    def full_reconciliation(self):
        """Perform full reconciliation of all systems."""
        logger.info("="*70)
        logger.info("COMPREHENSIVE STATUS RECONCILIATION")
        logger.info("="*70)
        
        # Step 1: Scan filesystem
        dois_found = self.scan_output_directory()
        
        # Step 2: Update tracker
        self.update_tracker_from_filesystem(dois_found)
        
        # Step 3: Update database
        self.update_database_from_jsons(dois_found)
        
        # Step 4: Report
        logger.info("\n" + "="*70)
        logger.info("RECONCILIATION SUMMARY")
        logger.info("="*70)
        logger.info(f"JSON files scanned: {self.stats['json_files_found']}")
        logger.info(f"  - Grobid: {self.stats['grobid_files']}")
        logger.info(f"  - PyMuPDF: {self.stats['pymupdf_files']}")
        logger.info(f"Tracker updated: {self.stats['tracker_updated']} DOIs")
        logger.info(f"Database updated: {self.stats['database_updated']} papers")
        logger.info(f"Discrepancies found: {self.stats['discrepancies_found']}")
        logger.info("="*70)
        
        return self.stats


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Reconcile status across filesystem, tracker, and database'
    )
    parser.add_argument('--output-dir', default='./output',
                       help='Output directory containing JSON files')
    parser.add_argument('--tracker-file', default='doi_processing_tracker.csv',
                       help='DOI tracker file')
    parser.add_argument('--db', 
                       default='/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db',
                       help='Path to papers.db')
    parser.add_argument('--check-only', action='store_true',
                       help='Only check for discrepancies, do not update')
    
    args = parser.parse_args()
    
    reconciler = StatusReconciler(
        output_dir=args.output_dir,
        tracker_file=args.tracker_file,
        db_path=args.db
    )
    
    if args.check_only:
        discrepancies = reconciler.find_discrepancies()
        
        if discrepancies['in_files_not_tracker']:
            print(f"\nDOIs in files but not in tracker: {len(discrepancies['in_files_not_tracker'])}")
            for doi in discrepancies['in_files_not_tracker'][:10]:
                print(f"  {doi}")
            if len(discrepancies['in_files_not_tracker']) > 10:
                print(f"  ... and {len(discrepancies['in_files_not_tracker']) - 10} more")
        
        if discrepancies['tracker_wrong_status']:
            print(f"\nTracker has wrong status: {len(discrepancies['tracker_wrong_status'])}")
            for item in discrepancies['tracker_wrong_status'][:10]:
                print(f"  {item['doi']} - {item['parser']}: {item['tracker_status']} should be {item['should_be']}")
            if len(discrepancies['tracker_wrong_status']) > 10:
                print(f"  ... and {len(discrepancies['tracker_wrong_status']) - 10} more")
    else:
        reconciler.full_reconciliation()


if __name__ == '__main__':
    main()
