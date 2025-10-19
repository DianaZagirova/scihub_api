#!/usr/bin/env python3
"""
Integration helper for standalone Grobid parsing scripts.

Use this to update the tracker when you run Grobid parsing separately.

Two modes:
1. Real-time: Import and call after each PDF is parsed
2. Batch: Run this script to scan output/ and update tracker with all Grobid JSONs
"""

import os
import sys
import logging
from pathlib import Path
from typing import List, Tuple
from doi_tracker import DOITracker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GrobidTrackerUpdater:
    """Updates tracker with Grobid parsing results."""
    
    def __init__(self, tracker_file='doi_processing_tracker.csv', output_dir='./output'):
        self.tracker = DOITracker(tracker_file)
        self.output_dir = Path(output_dir)
    
    def doi_from_filename(self, filename: str) -> str:
        """Convert Grobid JSON filename back to DOI."""
        # Remove .json extension
        name = filename.replace('.json', '')
        # Don't process _fast files (those are PyMuPDF)
        if name.endswith('_fast'):
            return None
        # Convert _ back to /
        doi = name.replace('_', '/')
        return doi
    
    def update_single_doi(self, doi: str, success: bool, error_msg: str = None):
        """
        Update tracker for a single DOI after Grobid processing.
        
        Args:
            doi: The DOI that was processed
            success: True if Grobid parsing succeeded
            error_msg: Optional error message if failed
        """
        self.tracker.mark_grobid_processed(doi, success=success, error_msg=error_msg)
        logger.info(f"Updated tracker: {doi} - {'success' if success else 'failed'}")
    
    def scan_and_update_all(self) -> Tuple[int, int]:
        """
        Scan output directory for all Grobid JSON files and update tracker.
        
        Returns:
            (updated_count, skipped_count)
        """
        logger.info(f"Scanning {self.output_dir} for Grobid JSON files...")
        
        updated = 0
        skipped = 0
        
        if not self.output_dir.exists():
            logger.warning(f"Output directory not found: {self.output_dir}")
            return 0, 0
        
        # Find all Grobid JSONs (not ending with _fast.json)
        grobid_files = []
        for json_file in self.output_dir.glob('*.json'):
            if not json_file.name.endswith('_fast.json'):
                grobid_files.append(json_file)
        
        logger.info(f"Found {len(grobid_files)} Grobid JSON files")
        
        # Batch update
        bulk_updates = []
        
        for json_file in grobid_files:
            doi = self.doi_from_filename(json_file.name)
            if not doi:
                continue
            
            # Check current status
            status = self.tracker.get_status(doi)
            
            # Skip if already marked as Grobid success
            if status and status.get('grobid_status') == self.tracker.STATUS_SUCCESS:
                skipped += 1
                continue
            
            # Add to bulk update
            bulk_updates.append({
                'doi': doi,
                'downloaded': self.tracker.AVAILABLE_YES,
                'scihub_available': self.tracker.AVAILABLE_YES,
                'grobid_status': self.tracker.STATUS_SUCCESS
            })
            updated += 1
        
        if bulk_updates:
            logger.info(f"Updating {len(bulk_updates)} DOIs in tracker...")
            self.tracker.bulk_update(bulk_updates, defer_write=True)
            self.tracker.flush()
        
        logger.info(f"Updated: {updated}, Skipped: {skipped}")
        return updated, skipped
    
    def watch_and_update_new(self, last_scan_file='.last_grobid_scan'):
        """
        Update tracker with only NEW Grobid files since last scan.
        Stores timestamp to avoid re-processing same files.
        
        Args:
            last_scan_file: File to store last scan timestamp
        """
        import time
        
        # Get last scan time
        last_scan_time = 0
        if os.path.exists(last_scan_file):
            try:
                with open(last_scan_file, 'r') as f:
                    last_scan_time = float(f.read().strip())
            except:
                pass
        
        logger.info(f"Checking for Grobid files newer than {time.ctime(last_scan_time)}")
        
        new_files = []
        for json_file in self.output_dir.glob('*.json'):
            if json_file.name.endswith('_fast.json'):
                continue
            
            # Check if file is newer than last scan
            if json_file.stat().st_mtime > last_scan_time:
                new_files.append(json_file)
        
        if not new_files:
            logger.info("No new Grobid files found")
            return 0
        
        logger.info(f"Found {len(new_files)} new Grobid files")
        
        # Update tracker
        bulk_updates = []
        for json_file in new_files:
            doi = self.doi_from_filename(json_file.name)
            if doi:
                bulk_updates.append({
                    'doi': doi,
                    'downloaded': self.tracker.AVAILABLE_YES,
                    'scihub_available': self.tracker.AVAILABLE_YES,
                    'grobid_status': self.tracker.STATUS_SUCCESS
                })
        
        if bulk_updates:
            self.tracker.bulk_update(bulk_updates, defer_write=True)
            self.tracker.flush()
            logger.info(f"Updated tracker with {len(bulk_updates)} new DOIs")
        
        # Update last scan time
        with open(last_scan_file, 'w') as f:
            f.write(str(time.time()))
        
        return len(bulk_updates)


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

def example_realtime_integration():
    """
    Example: How to integrate into your Grobid parsing script.
    
    Add this to your existing Grobid parser script:
    """
    from grobid_tracker_integration import GrobidTrackerUpdater
    
    updater = GrobidTrackerUpdater()
    
    # In your parsing loop:
    for doi in dois_to_process:
        try:
            # Your existing Grobid parsing code
            pdf_path = get_pdf_path(doi)
            json_path = parse_with_grobid(pdf_path)
            
            # Update tracker - SUCCESS
            updater.update_single_doi(doi, success=True)
            
        except Exception as e:
            # Update tracker - FAILED
            updater.update_single_doi(doi, success=False, error_msg=str(e))


def example_batch_scan():
    """
    Example: Scan all Grobid files and update tracker.
    """
    updater = GrobidTrackerUpdater()
    updated, skipped = updater.scan_and_update_all()
    print(f"Updated: {updated}, Already tracked: {skipped}")


def example_watch_new_files():
    """
    Example: Only process NEW Grobid files since last run.
    """
    updater = GrobidTrackerUpdater()
    count = updater.watch_and_update_new()
    print(f"Processed {count} new files")


# ============================================================================
# CLI
# ============================================================================

def main():
    """Command-line interface."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Update DOI tracker with Grobid parsing results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scan all Grobid JSON files and update tracker
  python grobid_tracker_integration.py --scan-all
  
  # Update only NEW Grobid files since last scan
  python grobid_tracker_integration.py --watch-new
  
  # Update a single DOI
  python grobid_tracker_integration.py --doi 10.1234/example --success
  
  # Mark a DOI as failed
  python grobid_tracker_integration.py --doi 10.1234/failed --failed --error "Parse error"

Integration in your Grobid script:
  from grobid_tracker_integration import GrobidTrackerUpdater
  
  updater = GrobidTrackerUpdater()
  updater.update_single_doi(doi, success=True)
        """
    )
    
    parser.add_argument('--tracker-file', default='doi_processing_tracker.csv',
                       help='Path to tracker CSV file')
    parser.add_argument('--output-dir', default='./output',
                       help='Directory containing JSON files')
    
    # Operation modes
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--scan-all', action='store_true',
                      help='Scan all Grobid files and update tracker')
    group.add_argument('--watch-new', action='store_true',
                      help='Update only new files since last scan')
    group.add_argument('--doi', help='Update a single DOI')
    
    # For single DOI updates
    parser.add_argument('--success', action='store_true',
                       help='Mark DOI as successfully processed')
    parser.add_argument('--failed', action='store_true',
                       help='Mark DOI as failed')
    parser.add_argument('--error', help='Error message for failed DOI')
    
    args = parser.parse_args()
    
    updater = GrobidTrackerUpdater(
        tracker_file=args.tracker_file,
        output_dir=args.output_dir
    )
    
    if args.scan_all:
        logger.info("Scanning all Grobid files...")
        updated, skipped = updater.scan_and_update_all()
        print(f"\n✓ Updated: {updated} DOIs")
        print(f"  Skipped (already tracked): {skipped}")
        
    elif args.watch_new:
        logger.info("Checking for new Grobid files...")
        count = updater.watch_and_update_new()
        print(f"\n✓ Processed {count} new files")
        
    elif args.doi:
        if args.success:
            updater.update_single_doi(args.doi, success=True)
            print(f"✓ Marked {args.doi} as Grobid success")
        elif args.failed:
            updater.update_single_doi(args.doi, success=False, error_msg=args.error)
            print(f"✓ Marked {args.doi} as Grobid failed")
        else:
            print("Error: Must specify --success or --failed for single DOI")
            return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
