#!/usr/bin/env python3
"""
Unified DOI Processing Tracker

Tracks the complete lifecycle of DOI processing:
- Sci-Hub availability check
- Download status
- PyMuPDF processing
- Grobid processing
- Errors and retries

File format: CSV with the following columns:
doi, scihub_available, downloaded, download_date, pymupdf_status, pymupdf_date, 
grobid_status, grobid_date, last_updated, error_msg, retry_count
"""

import csv
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
from collections import defaultdict
import threading


class DOITracker:
    """Centralized tracker for DOI processing status across all scripts."""
    
    # Status values
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"
    STATUS_NOT_ATTEMPTED = "not_attempted"
    STATUS_IN_PROGRESS = "in_progress"
    
    # Availability values
    AVAILABLE_YES = "yes"
    AVAILABLE_NO = "no"
    AVAILABLE_UNKNOWN = "unknown"
    
    def __init__(self, tracker_file: str = "doi_processing_tracker.csv"):
        """
        Initialize the DOI tracker.
        
        Args:
            tracker_file: Path to the CSV tracking file
        """
        self.tracker_file = Path(tracker_file)
        self.lock = threading.Lock()
        
        # In-memory cache for fast lookups
        self._cache: Dict[str, Dict] = {}
        self._cache_loaded = False
        
        # Initialize file if it doesn't exist
        if not self.tracker_file.exists():
            self._create_tracker_file()
        else:
            self._load_cache()
    
    def _create_tracker_file(self):
        """Create the tracker file with headers."""
        headers = [
            'doi',
            'scihub_available',      # yes/no/unknown
            'downloaded',            # yes/no/unknown
            'download_date',         # ISO format timestamp
            'pymupdf_status',        # success/failed/not_attempted/in_progress
            'pymupdf_date',          # ISO format timestamp
            'grobid_status',         # success/failed/not_attempted/in_progress
            'grobid_date',           # ISO format timestamp
            'last_updated',          # ISO format timestamp
            'error_msg',             # Latest error message
            'retry_count'            # Number of retry attempts
        ]
        
        with open(self.tracker_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
    
    def _load_cache(self):
        """Load all DOI statuses into memory cache."""
        with self.lock:
            self._cache.clear()
            
            with open(self.tracker_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    doi = row['doi']
                    self._cache[doi] = dict(row)
            
            self._cache_loaded = True
    
    def _ensure_cache_loaded(self):
        """Ensure cache is loaded."""
        if not self._cache_loaded:
            self._load_cache()
    
    def get_status(self, doi: str) -> Optional[Dict]:
        """
        Get the current status of a DOI.
        
        Args:
            doi: The DOI to look up
            
        Returns:
            Dict with status information or None if not tracked
        """
        self._ensure_cache_loaded()
        return self._cache.get(doi)
    
    def update_status(self, doi: str, **kwargs):
        """
        Update the status of a DOI.
        
        Args:
            doi: The DOI to update
            **kwargs: Fields to update (scihub_available, downloaded, pymupdf_status, etc.)
        """
        self._ensure_cache_loaded()
        
        with self.lock:
            # Get existing record or create new one
            if doi in self._cache:
                record = self._cache[doi].copy()
            else:
                record = {
                    'doi': doi,
                    'scihub_available': self.AVAILABLE_UNKNOWN,
                    'downloaded': self.AVAILABLE_UNKNOWN,
                    'download_date': '',
                    'pymupdf_status': self.STATUS_NOT_ATTEMPTED,
                    'pymupdf_date': '',
                    'grobid_status': self.STATUS_NOT_ATTEMPTED,
                    'grobid_date': '',
                    'last_updated': '',
                    'error_msg': '',
                    'retry_count': '0'
                }
            
            # Update fields
            for key, value in kwargs.items():
                if key in record:
                    record[key] = str(value) if value is not None else ''
            
            # Update timestamp
            record['last_updated'] = datetime.now().isoformat()
            
            # Update cache
            self._cache[doi] = record
            
            # Append to file (will rewrite periodically for cleanup)
            self._append_or_update_file(doi, record)
    
    def _append_or_update_file(self, doi: str, record: Dict):
        """Append or update a record in the file."""
        # For now, we'll do a full rewrite periodically
        # In production, you might want to append and consolidate periodically
        self._rewrite_file()
    
    def _rewrite_file(self):
        """Rewrite the entire file from cache (consolidates updates)."""
        headers = [
            'doi', 'scihub_available', 'downloaded', 'download_date',
            'pymupdf_status', 'pymupdf_date', 'grobid_status', 'grobid_date',
            'last_updated', 'error_msg', 'retry_count'
        ]
        
        with open(self.tracker_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
            for doi in sorted(self._cache.keys()):
                writer.writerow(self._cache[doi])
    
    def flush(self):
        """Force write all cached data to file."""
        with self.lock:
            self._rewrite_file()
    
    def bulk_update(self, updates: List[Dict], defer_write: bool = False):
        """
        Bulk update multiple DOIs at once.
        
        Args:
            updates: List of dicts, each containing 'doi' and fields to update
            defer_write: If True, don't write to file until explicitly called
        """
        self._ensure_cache_loaded()
        
        with self.lock:
            for update in updates:
                doi = update.get('doi')
                if not doi:
                    continue
                
                # Get existing record or create new one
                if doi in self._cache:
                    record = self._cache[doi].copy()
                else:
                    record = {
                        'doi': doi,
                        'scihub_available': self.AVAILABLE_UNKNOWN,
                        'downloaded': self.AVAILABLE_UNKNOWN,
                        'download_date': '',
                        'pymupdf_status': self.STATUS_NOT_ATTEMPTED,
                        'pymupdf_date': '',
                        'grobid_status': self.STATUS_NOT_ATTEMPTED,
                        'grobid_date': '',
                        'last_updated': '',
                        'error_msg': '',
                        'retry_count': '0'
                    }
                
                # Update fields
                for key, value in update.items():
                    if key != 'doi' and key in record:
                        record[key] = str(value) if value is not None else ''
                
                # Update timestamp
                record['last_updated'] = datetime.now().isoformat()
                
                # Update cache
                self._cache[doi] = record
            
            # Write to file if not deferred
            if not defer_write:
                self._rewrite_file()
    
    def mark_scihub_found(self, doi: str, available: bool):
        """Mark whether a DOI was found in Sci-Hub."""
        self.update_status(
            doi,
            scihub_available=self.AVAILABLE_YES if available else self.AVAILABLE_NO
        )
    
    def mark_downloaded(self, doi: str, success: bool, error_msg: str = None):
        """Mark a DOI as downloaded (or failed to download)."""
        update_data = {
            'downloaded': self.AVAILABLE_YES if success else self.AVAILABLE_NO,
            'download_date': datetime.now().isoformat() if success else ''
        }
        if error_msg:
            update_data['error_msg'] = error_msg
        
        self.update_status(doi, **update_data)
    
    def mark_pymupdf_processed(self, doi: str, success: bool, error_msg: str = None):
        """Mark PyMuPDF processing status."""
        update_data = {
            'pymupdf_status': self.STATUS_SUCCESS if success else self.STATUS_FAILED,
            'pymupdf_date': datetime.now().isoformat()
        }
        if error_msg:
            update_data['error_msg'] = error_msg
            # Increment retry count if failed
            current = self.get_status(doi)
            if current:
                update_data['retry_count'] = str(int(current.get('retry_count', 0)) + 1)
        
        self.update_status(doi, **update_data)
    
    def mark_grobid_processed(self, doi: str, success: bool, error_msg: str = None):
        """Mark Grobid processing status."""
        update_data = {
            'grobid_status': self.STATUS_SUCCESS if success else self.STATUS_FAILED,
            'grobid_date': datetime.now().isoformat()
        }
        if error_msg:
            update_data['error_msg'] = error_msg
            # Increment retry count if failed
            current = self.get_status(doi)
            if current:
                update_data['retry_count'] = str(int(current.get('retry_count', 0)) + 1)
        
        self.update_status(doi, **update_data)
    
    def get_dois_needing_download(self) -> List[str]:
        """Get DOIs that are available in Sci-Hub but not downloaded."""
        self._ensure_cache_loaded()
        
        return [
            doi for doi, status in self._cache.items()
            if status.get('scihub_available') == self.AVAILABLE_YES
            and status.get('downloaded') != self.AVAILABLE_YES
        ]
    
    def get_dois_needing_pymupdf(self) -> List[str]:
        """Get DOIs that are downloaded but not processed with PyMuPDF."""
        self._ensure_cache_loaded()
        
        return [
            doi for doi, status in self._cache.items()
            if status.get('downloaded') == self.AVAILABLE_YES
            and status.get('pymupdf_status') in [self.STATUS_NOT_ATTEMPTED, self.STATUS_FAILED]
        ]
    
    def get_dois_needing_grobid(self) -> List[str]:
        """Get DOIs that are downloaded but not processed with Grobid."""
        self._ensure_cache_loaded()
        
        return [
            doi for doi, status in self._cache.items()
            if status.get('downloaded') == self.AVAILABLE_YES
            and status.get('grobid_status') in [self.STATUS_NOT_ATTEMPTED, self.STATUS_FAILED]
        ]
    
    def get_failed_dois(self, max_retries: int = 3) -> Dict[str, List[str]]:
        """
        Get DOIs that have failed processing.
        
        Args:
            max_retries: Maximum number of retries before giving up
            
        Returns:
            Dict with categories: 'download', 'pymupdf', 'grobid'
        """
        self._ensure_cache_loaded()
        
        failed = {
            'download': [],
            'pymupdf': [],
            'grobid': []
        }
        
        for doi, status in self._cache.items():
            retry_count = int(status.get('retry_count', 0))
            
            if retry_count < max_retries:
                if status.get('downloaded') == self.AVAILABLE_NO:
                    failed['download'].append(doi)
                if status.get('pymupdf_status') == self.STATUS_FAILED:
                    failed['pymupdf'].append(doi)
                if status.get('grobid_status') == self.STATUS_FAILED:
                    failed['grobid'].append(doi)
        
        return failed
    
    def get_statistics(self) -> Dict:
        """Get comprehensive statistics about DOI processing."""
        self._ensure_cache_loaded()
        
        stats = {
            'total_dois': len(self._cache),
            'scihub_available': 0,
            'scihub_not_found': 0,
            'downloaded': 0,
            'download_failed': 0,
            'pymupdf_success': 0,
            'pymupdf_failed': 0,
            'pymupdf_pending': 0,
            'grobid_success': 0,
            'grobid_failed': 0,
            'grobid_pending': 0,
            'fully_processed': 0
        }
        
        for doi, status in self._cache.items():
            # Sci-Hub availability
            if status.get('scihub_available') == self.AVAILABLE_YES:
                stats['scihub_available'] += 1
            elif status.get('scihub_available') == self.AVAILABLE_NO:
                stats['scihub_not_found'] += 1
            
            # Download status
            if status.get('downloaded') == self.AVAILABLE_YES:
                stats['downloaded'] += 1
            elif status.get('downloaded') == self.AVAILABLE_NO:
                stats['download_failed'] += 1
            
            # PyMuPDF status
            if status.get('pymupdf_status') == self.STATUS_SUCCESS:
                stats['pymupdf_success'] += 1
            elif status.get('pymupdf_status') == self.STATUS_FAILED:
                stats['pymupdf_failed'] += 1
            elif status.get('pymupdf_status') == self.STATUS_NOT_ATTEMPTED:
                stats['pymupdf_pending'] += 1
            
            # Grobid status
            if status.get('grobid_status') == self.STATUS_SUCCESS:
                stats['grobid_success'] += 1
            elif status.get('grobid_status') == self.STATUS_FAILED:
                stats['grobid_failed'] += 1
            elif status.get('grobid_status') == self.STATUS_NOT_ATTEMPTED:
                stats['grobid_pending'] += 1
            
            # Fully processed (at least one parser succeeded)
            if (status.get('pymupdf_status') == self.STATUS_SUCCESS or 
                status.get('grobid_status') == self.STATUS_SUCCESS):
                stats['fully_processed'] += 1
        
        return stats
    
    def export_to_database(self, db_path: str):
        """
        Export tracker data to sync with papers.db.
        Updates the parsing_status field based on tracker data.
        
        Args:
            db_path: Path to papers.db
        """
        self._ensure_cache_loaded()
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        updated = 0
        for doi, status in self._cache.items():
            # Determine parsing status based on tracker
            parsing_status = self._determine_parsing_status(status)
            
            cursor.execute(
                "UPDATE papers SET parsing_status = ? WHERE doi = ?",
                (parsing_status, doi)
            )
            if cursor.rowcount > 0:
                updated += 1
        
        conn.commit()
        conn.close()
        
        return updated
    
    def import_from_database(self, db_path: str):
        """
        Import existing status from papers.db to initialize tracker.
        
        Args:
            db_path: Path to papers.db
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT doi, parsing_status 
            FROM papers 
            WHERE doi IS NOT NULL AND doi != ''
        """)
        
        for doi, parsing_status in cursor.fetchall():
            # Parse existing status to populate tracker
            status_data = self._parse_parsing_status(parsing_status)
            if status_data:
                self.update_status(doi, **status_data)
        
        conn.close()
    
    def _determine_parsing_status(self, status: Dict) -> str:
        """Determine parsing_status string from tracker data."""
        parts = []
        
        # Check Grobid first (preferred)
        if status.get('grobid_status') == self.STATUS_SUCCESS:
            parts.append('success (parser: grobid)')
        elif status.get('pymupdf_status') == self.STATUS_SUCCESS:
            parts.append('success (parser: PyMuPDF)')
        elif status.get('grobid_status') == self.STATUS_FAILED:
            parts.append('processing_failed (parser: grobid)')
        elif status.get('pymupdf_status') == self.STATUS_FAILED:
            parts.append('processing_failed (parser: PyMuPDF)')
        elif status.get('downloaded') == self.AVAILABLE_NO:
            parts.append('download_failed')
        elif status.get('scihub_available') == self.AVAILABLE_NO:
            parts.append('not_found')
        else:
            parts.append('not_processed')
        
        return ' | '.join(parts) if parts else 'unknown'
    
    def _parse_parsing_status(self, parsing_status: str) -> Optional[Dict]:
        """Parse existing parsing_status string into tracker fields."""
        if not parsing_status:
            return None
        
        status_data = {}
        
        # Parse common patterns
        if 'success' in parsing_status.lower():
            if 'grobid' in parsing_status.lower():
                status_data['grobid_status'] = self.STATUS_SUCCESS
                status_data['downloaded'] = self.AVAILABLE_YES
            elif 'pymupdf' in parsing_status.lower():
                status_data['pymupdf_status'] = self.STATUS_SUCCESS
                status_data['downloaded'] = self.AVAILABLE_YES
        
        if 'failed' in parsing_status.lower():
            if 'grobid' in parsing_status.lower():
                status_data['grobid_status'] = self.STATUS_FAILED
            elif 'pymupdf' in parsing_status.lower():
                status_data['pymupdf_status'] = self.STATUS_FAILED
        
        if 'not_found' in parsing_status.lower():
            status_data['scihub_available'] = self.AVAILABLE_NO
        
        return status_data if status_data else None
    
    def print_statistics(self):
        """Print comprehensive statistics."""
        stats = self.get_statistics()
        
        print("="*70)
        print("DOI PROCESSING TRACKER STATISTICS")
        print("="*70)
        print(f"\nTotal DOIs tracked: {stats['total_dois']:,}")
        
        print(f"\n--- Sci-Hub Availability ---")
        print(f"  Available: {stats['scihub_available']:,}")
        print(f"  Not found: {stats['scihub_not_found']:,}")
        print(f"  Unknown: {stats['total_dois'] - stats['scihub_available'] - stats['scihub_not_found']:,}")
        
        print(f"\n--- Download Status ---")
        print(f"  Downloaded: {stats['downloaded']:,}")
        print(f"  Failed: {stats['download_failed']:,}")
        
        print(f"\n--- PyMuPDF Processing ---")
        print(f"  Success: {stats['pymupdf_success']:,}")
        print(f"  Failed: {stats['pymupdf_failed']:,}")
        print(f"  Pending: {stats['pymupdf_pending']:,}")
        
        print(f"\n--- Grobid Processing ---")
        print(f"  Success: {stats['grobid_success']:,}")
        print(f"  Failed: {stats['grobid_failed']:,}")
        print(f"  Pending: {stats['grobid_pending']:,}")
        
        print(f"\n--- Overall ---")
        print(f"  Fully processed (any parser): {stats['fully_processed']:,}")
        
        completion_rate = (stats['fully_processed'] / stats['total_dois'] * 100) if stats['total_dois'] > 0 else 0
        print(f"  Completion rate: {completion_rate:.2f}%")
        print("="*70)


def main():
    """Example usage and testing."""
    import argparse
    
    parser = argparse.ArgumentParser(description='DOI Processing Tracker')
    parser.add_argument('--tracker-file', default='doi_processing_tracker.csv',
                       help='Path to tracker CSV file')
    parser.add_argument('--import-from-db', 
                       help='Import existing data from papers.db')
    parser.add_argument('--export-to-db',
                       help='Export tracker data to papers.db')
    parser.add_argument('--stats', action='store_true',
                       help='Show statistics')
    parser.add_argument('--get-pending-pymupdf', action='store_true',
                       help='Get DOIs needing PyMuPDF processing')
    parser.add_argument('--get-pending-grobid', action='store_true',
                       help='Get DOIs needing Grobid processing')
    
    args = parser.parse_args()
    
    tracker = DOITracker(args.tracker_file)
    
    if args.import_from_db:
        print(f"Importing from database: {args.import_from_db}")
        tracker.import_from_database(args.import_from_db)
        print("Import complete!")
    
    if args.export_to_db:
        print(f"Exporting to database: {args.export_to_db}")
        updated = tracker.export_to_database(args.export_to_db)
        print(f"Updated {updated} records in database")
    
    if args.stats:
        tracker.print_statistics()
    
    if args.get_pending_pymupdf:
        dois = tracker.get_dois_needing_pymupdf()
        print(f"\nDOIs needing PyMuPDF processing: {len(dois)}")
        for doi in dois[:10]:
            print(f"  {doi}")
        if len(dois) > 10:
            print(f"  ... and {len(dois) - 10} more")
    
    if args.get_pending_grobid:
        dois = tracker.get_dois_needing_grobid()
        print(f"\nDOIs needing Grobid processing: {len(dois)}")
        for doi in dois[:10]:
            print(f"  {doi}")
        if len(dois) > 10:
            print(f"  ... and {len(dois) - 10} more")


if __name__ == '__main__':
    main()
