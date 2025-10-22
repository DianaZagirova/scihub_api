#!/usr/bin/env python3
"""
Identify deleted JSONs by comparing tracker status with filesystem.

This script:
1. Queries processing_tracker.db for DOIs marked as successfully parsed
2. Checks if corresponding JSON files exist in output/
3. Lists DOIs whose JSONs are missing (likely deleted)

Usage:
    python identify_deleted_jsons.py > deleted_dois.txt
    python identify_deleted_jsons.py --output ./output --tracker-db processing_tracker.db
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Set, Dict

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from trackers.doi_tracker_db import DOITracker


def normalize_doi_to_filename(doi: str, parser: str) -> str:
    """Convert DOI to expected filename format."""
    # Replace / with _
    safe_name = doi.replace('/', '_')
    
    # Add suffix based on parser
    if parser == 'pymupdf':
        return f"{safe_name}_fast.json"
    else:  # grobid
        return f"{safe_name}.json"


def get_existing_jsons(output_dir: Path) -> Dict[str, Set[str]]:
    """
    Scan output directory and return {doi: {parsers}} for existing JSONs.
    """
    dois: Dict[str, Set[str]] = {}
    
    if not output_dir.exists():
        print(f"Warning: Output directory not found: {output_dir}", file=sys.stderr)
        return dois
    
    for json_file in output_dir.glob('*.json'):
        filename = json_file.name
        
        # Determine parser type and DOI
        if filename.endswith('_fast.json'):
            parser = 'pymupdf'
            name = filename[:-10]  # Remove _fast.json
        else:
            parser = 'grobid'
            name = filename[:-5]  # Remove .json
        
        # Convert filename back to DOI
        doi = name.replace('_', '/')
        
        if doi not in dois:
            dois[doi] = set()
        dois[doi].add(parser)
    
    return dois


def get_successfully_parsed_dois(tracker: DOITracker) -> Dict[str, Set[str]]:
    """
    Get DOIs marked as successfully parsed in tracker.
    
    Returns:
        Dict mapping DOI to set of parsers that succeeded
    """
    all_statuses = tracker.get_all_statuses()
    parsed_dois: Dict[str, Set[str]] = {}
    
    for doi, status in all_statuses.items():
        parsers = set()
        
        pymupdf_status = status.get('pymupdf_status', '')
        if pymupdf_status == 'success':
            parsers.add('pymupdf')
        
        grobid_status = status.get('grobid_status', '')
        if grobid_status == 'success':
            parsers.add('grobid')
        
        if parsers:
            parsed_dois[doi] = parsers
    
    return parsed_dois


def main():
    parser = argparse.ArgumentParser(
        description='Identify deleted JSONs by comparing tracker with filesystem'
    )
    parser.add_argument(
        '--output',
        default='./output',
        help='Output directory containing JSON files (default: ./output)'
    )
    parser.add_argument(
        '--tracker-db',
        default='processing_tracker.db',
        help='Path to processing_tracker.db (default: processing_tracker.db)'
    )
    parser.add_argument(
        '--format',
        choices=['doi', 'filename', 'both'],
        default='doi',
        help='Output format: doi, filename, or both (default: doi)'
    )
    
    args = parser.parse_args()
    
    output_dir = Path(args.output)
    
    # Initialize tracker
    print(f"Loading tracker from: {args.tracker_db}", file=sys.stderr)
    tracker = DOITracker(db_path=args.tracker_db)
    
    # Get successfully parsed DOIs from tracker
    print("Querying tracker for successfully parsed DOIs...", file=sys.stderr)
    parsed_dois = get_successfully_parsed_dois(tracker)
    print(f"Found {len(parsed_dois)} DOIs marked as successfully parsed", file=sys.stderr)
    
    # Get existing JSONs from filesystem
    print(f"Scanning output directory: {output_dir}", file=sys.stderr)
    existing_jsons = get_existing_jsons(output_dir)
    print(f"Found {len(existing_jsons)} DOIs with JSON files", file=sys.stderr)
    
    # Find missing JSONs
    print("\n" + "="*80, file=sys.stderr)
    print("Deleted/Missing JSONs:", file=sys.stderr)
    print("="*80, file=sys.stderr)
    
    deleted_count = {'pymupdf': 0, 'grobid': 0}
    deleted_dois = []
    
    for doi, parsers in sorted(parsed_dois.items()):
        existing_parsers = existing_jsons.get(doi, set())
        
        for parser in parsers:
            if parser not in existing_parsers:
                deleted_count[parser] += 1
                
                if args.format == 'doi':
                    print(doi)
                elif args.format == 'filename':
                    filename = normalize_doi_to_filename(doi, parser)
                    print(filename)
                else:  # both
                    filename = normalize_doi_to_filename(doi, parser)
                    print(f"{doi}\t{filename}")
                
                deleted_dois.append((doi, parser))
    
    # Print summary
    print("\n" + "="*80, file=sys.stderr)
    print("SUMMARY", file=sys.stderr)
    print("="*80, file=sys.stderr)
    print(f"Total DOIs in tracker with success status: {len(parsed_dois)}", file=sys.stderr)
    print(f"Total DOIs with existing JSONs: {len(existing_jsons)}", file=sys.stderr)
    print(f"PyMuPDF JSONs deleted: {deleted_count['pymupdf']}", file=sys.stderr)
    print(f"Grobid JSONs deleted: {deleted_count['grobid']}", file=sys.stderr)
    print(f"Total deleted: {sum(deleted_count.values())}", file=sys.stderr)
    print("="*80, file=sys.stderr)


if __name__ == '__main__':
    main()
