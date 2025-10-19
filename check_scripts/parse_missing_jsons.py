#!/usr/bin/env python3
"""
Parse PDFs with GROBID that don't have JSON outputs yet.
Only processes papers that exist in ./papers and don't have .json in ./output/
(ignores _fast.json files)
"""

import os
import sys
from pathlib import Path
from src.grobid_parser import GrobidParser
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_pdfs_without_json(papers_dir='./papers', output_dir='./output'):
    """
    Find PDFs in papers_dir that don't have corresponding .json files in output_dir.
    
    Args:
        papers_dir: Directory containing PDF files
        output_dir: Directory containing JSON outputs
        
    Returns:
        list: Paths to PDFs that need processing
    """
    papers_path = Path(papers_dir)
    output_path = Path(output_dir)
    
    if not papers_path.exists():
        logger.error(f"Papers directory not found: {papers_dir}")
        return []
    
    # Get all PDF files
    pdf_files = list(papers_path.glob('*.pdf'))
    logger.info(f"Found {len(pdf_files)} PDF files in {papers_dir}")
    
    # Filter PDFs that don't have JSON outputs (excluding _fast.json)
    pdfs_to_process = []
    for pdf_path in pdf_files:
        # Get base filename without extension
        base_name = pdf_path.stem
        
        # Check if corresponding .json exists (not _fast.json)
        json_path = output_path / f"{base_name}.json"
        
        if not json_path.exists():
            pdfs_to_process.append(str(pdf_path))
            logger.debug(f"Need to process: {pdf_path.name}")
        else:
            logger.debug(f"Already has JSON: {pdf_path.name}")
    
    logger.info(f"{len(pdfs_to_process)} PDFs need processing (missing .json in output/)")
    return pdfs_to_process

def main():
    """Main function to parse PDFs without JSON outputs."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Parse PDFs with GROBID that are missing JSON outputs'
    )
    parser.add_argument(
        '--papers-dir',
        default='./papers',
        help='Directory containing PDF files (default: ./papers)'
    )
    parser.add_argument(
        '--output-dir',
        default='./output',
        help='Directory for JSON outputs (default: ./output)'
    )
    parser.add_argument(
        '--config',
        default='./config.json',
        help='Path to config file (default: ./config.json)'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Maximum number of PDFs to process (default: process all)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show which PDFs would be processed without actually processing'
    )
    
    args = parser.parse_args()
    
    # Get PDFs that need processing
    pdfs_to_process = get_pdfs_without_json(args.papers_dir, args.output_dir)
    
    if not pdfs_to_process:
        logger.info("No PDFs need processing. All papers already have JSON outputs.")
        return 0
    
    # Apply limit if specified
    if args.limit and args.limit > 0:
        original_count = len(pdfs_to_process)
        pdfs_to_process = pdfs_to_process[:args.limit]
        logger.info(f"Limiting to first {args.limit} PDFs (out of {original_count} total)")
    
    logger.info(f"PDFs to process:")
    for pdf in pdfs_to_process[:10]:  # Show first 10
        logger.info(f"  - {Path(pdf).name}")
    if len(pdfs_to_process) > 10:
        logger.info(f"  ... and {len(pdfs_to_process) - 10} more")
    
    if args.dry_run:
        logger.info("Dry run - no actual processing performed")
        return 0
    
    # Initialize GROBID parser
    logger.info("Initializing GROBID parser...")
    grobid_parser = GrobidParser(config_path=args.config)
    
    # Process PDFs in parallel
    logger.info(f"Starting GROBID processing with {args.workers} parallel workers...")
    
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from tqdm import tqdm
    
    success_count = 0
    failed_count = 0
    failed_pdfs = []  # Track failed PDFs
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        future_to_pdf = {
            executor.submit(grobid_parser.process_and_save, pdf_path, args.output_dir): pdf_path 
            for pdf_path in pdfs_to_process
        }
        
        # Process results as they complete
        for future in tqdm(as_completed(future_to_pdf), total=len(pdfs_to_process), desc="Processing PDFs"):
            pdf_path = future_to_pdf[future]
            try:
                result = future.result()
                if result:
                    success_count += 1
                    logger.info(f"✓ Processed: {Path(pdf_path).name}")
                else:
                    failed_count += 1
                    failed_pdfs.append(pdf_path)
                    logger.error(f"✗ Failed: {Path(pdf_path).name}")
            except Exception as e:
                failed_count += 1
                failed_pdfs.append(pdf_path)
                logger.error(f"✗ Error processing {Path(pdf_path).name}: {e}")
    
    # Summary
    logger.info(f"\n{'='*70}")
    logger.info(f"Processing complete:")
    logger.info(f"  Total PDFs: {len(pdfs_to_process)}")
    logger.info(f"  Successful: {success_count}")
    logger.info(f"  Failed: {failed_count}")
    logger.info(f"{'='*70}")
    
    # Save failed PDFs list for review
    if failed_pdfs:
        failed_list_path = Path(args.output_dir) / 'failed_pdfs.txt'
        try:
            with open(failed_list_path, 'w') as f:
                for pdf_path in failed_pdfs:
                    f.write(f"{pdf_path}\n")
            logger.info(f"Saved list of {len(failed_pdfs)} failed PDFs to: {failed_list_path}")
        except Exception as e:
            logger.error(f"Error saving failed PDFs list: {e}")
    
    return 0 if failed_count == 0 else 1

if __name__ == '__main__':
    sys.exit(main())
