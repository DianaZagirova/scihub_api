#!/usr/bin/env python3
"""
Extract list of deleted JSON files from terminal output or log.

This script parses the warning messages from sync_processing_state_to_db.py
to identify which JSON files were deleted/quarantined.

Usage:
    # From saved terminal output
    python extract_deleted_jsons.py < terminal_output.txt > deleted_dois.txt
    
    # From clipboard (paste and press Ctrl+D)
    python extract_deleted_jsons.py > deleted_dois.txt
    
    # From a log file
    grep "Removed invalid JSON" logfile.txt | python extract_deleted_jsons.py > deleted_dois.txt
"""

import sys
import re
from pathlib import Path


def extract_doi_from_filename(filename):
    """
    Extract DOI from JSON filename.
    
    Examples:
        10.1016_j.fct.2015.05.021_fast.json -> 10.1016/j.fct.2015.05.021
        10.1093_gerona_glq001_fast.json -> 10.1093/gerona/glq001
        10.1259_dmfr.20180398.json -> 10.1259/dmfr.20180398
    """
    # Remove path prefix if present
    name = Path(filename).name
    
    # Remove .json extension
    if name.endswith('.json'):
        name = name[:-5]
    
    # Remove _fast suffix if present
    if name.endswith('_fast'):
        name = name[:-5]
    
    # Convert underscores back to slashes for DOI
    doi = name.replace('_', '/')
    
    return doi


def main():
    """Parse stdin for deleted JSON warnings and output DOIs."""
    deleted_dois = set()
    
    # Pattern to match the warning message
    # Example: "2025-10-19 08:23:46,683 - WARNING - Removed invalid JSON: output/10.1016_j.fct.2015.05.021_fast.json"
    pattern = re.compile(r'(?:Removed invalid JSON|Quarantined invalid JSON):\s*(?:output/)?(.+\.json)')
    
    for line in sys.stdin:
        match = pattern.search(line)
        if match:
            filename = match.group(1)
            doi = extract_doi_from_filename(filename)
            deleted_dois.add(doi)
    
    # Output sorted DOIs
    for doi in sorted(deleted_dois):
        print(doi)
    
    # Print summary to stderr
    print(f"\n# Total deleted/quarantined: {len(deleted_dois)}", file=sys.stderr)


if __name__ == '__main__':
    main()
