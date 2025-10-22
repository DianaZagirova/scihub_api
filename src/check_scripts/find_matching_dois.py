#!/usr/bin/env python3
"""
Find DOIs that:
1. Have .json or _fast.json in ./output
2. Do not have full_text_sections AND full_text (none or '') in papers_export.json
3. Are validated in papers.db
"""

import json
import os
import sqlite3
from pathlib import Path

# Paths
OUTPUT_DIR = '/home/diana.z/hack/scihub_api/output'
EVALUATIONS_DB = '/home/diana.z/hack/llm_judge/data/evaluations.db'
PAPERS_DB = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'

def get_validated_dois():
    """Get validated DOIs from evaluations.db"""
    conn = sqlite3.connect(EVALUATIONS_DB)
    cursor = conn.cursor()
    
    dois_ev = cursor.execute("""
        SELECT DISTINCT doi
        FROM paper_evaluations
        WHERE doi IS NOT NULL AND doi != ''
          AND (
            result IN ('valid','doubted')
            OR (result='not_valid' AND COALESCE(confidence_score,999) <= 7)
          )
    """).fetchall()
    
    conn.close()
    
    # Extract DOIs from tuples and normalize
    validated_dois = {doi[0].lower().strip() for doi in dois_ev if doi[0]}
    print(f"Found {len(validated_dois)} validated DOIs in papers.db")
    return validated_dois

def get_dois_with_output_files():
    """Get DOIs that have .json or _fast.json files in ./output"""
    output_path = Path(OUTPUT_DIR)
    dois_with_files = set()
    
    if not output_path.exists():
        print(f"Warning: {OUTPUT_DIR} does not exist")
        return dois_with_files
    
    for file in output_path.iterdir():
        if file.is_file() and (file.name.endswith('.json') or file.name.endswith('_fast.json')):
            # Extract DOI from filename
            # Remove _fast.json or .json suffix
            doi_part = file.stem
            if doi_part.endswith('_fast'):
                doi_part = doi_part[:-5]  # Remove '_fast'
            
            # Convert filename back to DOI format (replace _ with /)
            # Assuming DOI format like 10.1001/something
            if doi_part.startswith('10.'):
                # Find the first underscore after '10.' to replace with '/'
                parts = doi_part.split('_', 1)
                if len(parts) == 2:
                    doi = f"{parts[0]}/{parts[1].replace('_', '.')}"
                    dois_with_files.add(doi.lower().strip())
    
    print(f"Found {len(dois_with_files)} DOIs with output files")
    return dois_with_files

def get_dois_missing_full_text():
    """Get DOIs that don't have full_text_sections AND full_text in papers.db"""
    conn = sqlite3.connect(PAPERS_DB)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT doi
        FROM papers
        WHERE doi IS NOT NULL AND doi != ''
        AND (full_text IS NULL OR full_text = '')
        AND (full_text_sections IS NULL OR full_text_sections = '')
    """)
    
    dois_missing_text = {doi[0].lower().strip() for doi in cursor.fetchall() if doi[0]}
    conn.close()
    
    print(f"Found {len(dois_missing_text)} DOIs missing full text in papers.db")
    return dois_missing_text

def main():
    print("=" * 80)
    print("Finding DOIs matching all three criteria...")
    print("=" * 80)
    
    # Get all three sets
    validated_dois = get_validated_dois()
    dois_with_files = get_dois_with_output_files()
    dois_missing_text = get_dois_missing_full_text()
    
    # Find intersection of all three
    matching_dois = validated_dois & dois_with_files & dois_missing_text
    
    print("\n" + "=" * 80)
    print(f"RESULTS: Found {len(matching_dois)} DOIs matching ALL criteria")
    print("=" * 80)
    
    if matching_dois:
        print("\nMatching DOIs:")
        for doi in sorted(matching_dois):
            print(f"  - {doi}")
        
        # Save to file
        output_file = 'matching_dois.txt'
        with open(output_file, 'w') as f:
            for doi in sorted(matching_dois):
                f.write(f"{doi}\n")
        print(f"\nSaved to {output_file}")
    else:
        print("\nNo DOIs match all three criteria.")
    
    # Show breakdown
    print("\n" + "=" * 80)
    print("BREAKDOWN:")
    print("=" * 80)
    print(f"Validated DOIs: {len(validated_dois)}")
    print(f"DOIs with output files: {len(dois_with_files)}")
    print(f"DOIs missing full text: {len(dois_missing_text)}")
    print(f"Validated ∩ With files: {len(validated_dois & dois_with_files)}")
    print(f"Validated ∩ Missing text: {len(validated_dois & dois_missing_text)}")
    print(f"With files ∩ Missing text: {len(dois_with_files & dois_missing_text)}")
    print(f"All three: {len(matching_dois)}")

if __name__ == '__main__':
    main()
