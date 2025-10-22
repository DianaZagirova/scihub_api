#!/usr/bin/env python3
"""
Reset parsing status in processing_tracker.db for DOIs whose JSON files are missing.

This script:
1. Scans output/ directory for existing JSON files
2. Queries processing_tracker.db for DOIs marked as successfully parsed
3. Resets parsing status to 'not_attempted' if corresponding JSON is missing
4. Reports statistics on reset operations

Usage:
    python reset_missing_json_status.py
    python reset_missing_json_status.py --output ./output --tracker-db processing_tracker.db --dry-run
"""

import os
import sys
import sqlite3
import logging
import argparse
from pathlib import Path
from typing import Set, Dict

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from trackers.doi_tracker_db import DOITracker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = './output'
DEFAULT_TRACKER_DB = 'processing_tracker.db'


def normalize_doi_from_filename(filename: str) -> str:
    """Convert filename back to DOI format."""
    # Remove .json extension
    name = filename[:-5] if filename.endswith('.json') else filename
    # Remove _fast suffix if present
    if name.endswith('_fast'):
        name = name[:-5]
    # Convert underscores back to slashes
    return name.replace('_', '/')


def scan_existing_jsons(output_dir: Path) -> Dict[str, Set[str]]:
    """
    Scan output directory and return {doi: {parsers}} for existing valid JSONs.
    
    Returns:
        Dict mapping DOI to set of parser types ('pymupdf', 'grobid')
    """
    dois: Dict[str, Set[str]] = {}
    
    if not output_dir.exists():
        logger.warning(f"Output directory not found: {output_dir}")
        return dois
    
    for json_file in output_dir.glob('*.json'):
        filename = json_file.name
        
        # Determine parser type and DOI
        if filename.endswith('_fast.json'):
            parser = 'pymupdf'
            doi = normalize_doi_from_filename(filename)
        else:
            parser = 'grobid'
            doi = normalize_doi_from_filename(filename)
        
        # Add to tracking
        if doi not in dois:
            dois[doi] = set()
        dois[doi].add(parser)
    
    return dois


def get_dois_with_success_status(tracker: DOITracker) -> Dict[str, Dict[str, str]]:
    """
    Get all DOIs from tracker that have success parsing status.
    
    Returns:
        Dict mapping DOI to {'pymupdf': status, 'grobid': status}
    """
    all_statuses = tracker.get_all_statuses()
    success_dois = {}
    
    for doi, status in all_statuses.items():
        pymupdf_status = status.get('pymupdf_status', '')
        grobid_status = status.get('grobid_status', '')
        
        if pymupdf_status == 'success' or grobid_status == 'success':
            success_dois[doi] = {
                'pymupdf': pymupdf_status,
                'grobid': grobid_status
            }
    
    return success_dois


def reset_parsing_status(tracker: DOITracker, doi: str, parser: str, dry_run: bool = False):
    """Reset parsing status for a specific DOI and parser."""
    field_name = f'{parser}_status'
    
    if dry_run:
        logger.info(f"[DRY RUN] Would reset {parser} status for: {doi}")
    else:
        tracker.update_status(doi=doi, **{field_name: 'not_attempted'})
        logger.info(f"Reset {parser} status to 'not_attempted' for: {doi}")


def main():
    parser = argparse.ArgumentParser(
        description='Reset parsing status in tracker for DOIs with missing JSON files'
    )
    parser.add_argument(
        '--output',
        default=DEFAULT_OUTPUT_DIR,
        help=f'Output directory containing JSON files (default: {DEFAULT_OUTPUT_DIR})'
    )
    parser.add_argument(
        '--tracker-db',
        default=DEFAULT_TRACKER_DB,
        help=f'Path to processing_tracker.db (default: {DEFAULT_TRACKER_DB})'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be reset without making changes'
    )
    
    args = parser.parse_args()
    
    output_dir = Path(args.output)
    
    # Initialize tracker
    logger.info(f"Loading tracker from: {args.tracker_db}")
    tracker = DOITracker(db_path=args.tracker_db)
    
    # Scan filesystem for existing JSONs
    logger.info(f"Scanning output directory: {output_dir}")
    existing_jsons = scan_existing_jsons(output_dir)
    logger.info(f"Found {len(existing_jsons)} DOIs with JSON files in output/")
    
    # Get DOIs marked as successfully parsed in tracker
    logger.info("Querying tracker for DOIs with success parsing status...")
    success_dois = get_dois_with_success_status(tracker)
    logger.info(f"Found {len(success_dois)} DOIs marked as successfully parsed in tracker")
    
    # Find mismatches
    reset_count = {'pymupdf': 0, 'grobid': 0}
    
    logger.info("\n" + "="*80)
    logger.info("Checking for mismatches between tracker and filesystem...")
    logger.info("="*80 + "\n")
    
    for doi, statuses in success_dois.items():
        existing_parsers = existing_jsons.get(doi, set())
        
        # Check PyMuPDF
        if statuses['pymupdf'] == 'success' and 'pymupdf' not in existing_parsers:
            reset_parsing_status(tracker, doi, 'pymupdf', dry_run=args.dry_run)
            reset_count['pymupdf'] += 1
        
        # Check Grobid
        if statuses['grobid'] == 'success' and 'grobid' not in existing_parsers:
            reset_parsing_status(tracker, doi, 'grobid', dry_run=args.dry_run)
            reset_count['grobid'] += 1
    
    # Flush tracker to disk
    if not args.dry_run:
        logger.info("\nFlushing tracker to disk...")
        tracker.flush()
    
    # Print summary
    logger.info("\n" + "="*80)
    logger.info("SUMMARY")
    logger.info("="*80)
    logger.info(f"Total DOIs in output/: {len(existing_jsons)}")
    logger.info(f"Total DOIs with success status in tracker: {len(success_dois)}")
    logger.info(f"PyMuPDF statuses reset: {reset_count['pymupdf']}")
    logger.info(f"Grobid statuses reset: {reset_count['grobid']}")
    logger.info(f"Total resets: {sum(reset_count.values())}")
    
    if args.dry_run:
        logger.info("\n[DRY RUN] No changes were made to the tracker.")
        logger.info("Run without --dry-run to apply changes.")
    else:
        logger.info("\n✓ Tracker updated successfully")
    
    logger.info("="*80)


if __name__ == '__main__':
    main()
