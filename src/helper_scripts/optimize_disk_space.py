#!/usr/bin/env python3
"""
Disk space optimizer for paper processing pipeline.

Strategies:
1. Delete PDFs after successful JSON extraction (saves ~85% space)
2. Compress JSONs (saves additional ~70% of JSON size)
3. Archive to compressed archives for long-term storage
"""

import os
import json
import gzip
import shutil
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def verify_json_valid(json_path):
    """
    Verify that JSON file is valid and contains extracted data.
    
    Args:
        json_path: Path to JSON file
        
    Returns:
        bool: True if valid, False otherwise
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Check if JSON has meaningful content
        # For GROBID: check for metadata or full_text
        # For Fast parser: check for structured_text
        has_content = (
            ('metadata' in data and data['metadata']) or
            ('full_text' in data and data['full_text']) or
            ('structured_text' in data and data['structured_text'])
        )
        
        return has_content
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Invalid JSON {json_path}: {e}")
        return False


def delete_pdf_if_json_exists(papers_dir='./papers', output_dir='./output', dry_run=False):
    """
    Delete PDF files that have corresponding valid JSON files.
    
    Args:
        papers_dir: Directory containing PDFs
        output_dir: Directory containing JSONs
        dry_run: If True, only simulate (don't actually delete)
        
    Returns:
        dict: Statistics about the operation
    """
    stats = {
        'total_pdfs': 0,
        'pdfs_with_json': 0,
        'pdfs_deleted': 0,
        'space_freed': 0,
        'errors': 0
    }
    
    if not os.path.exists(papers_dir):
        logger.error(f"Papers directory not found: {papers_dir}")
        return stats
    
    # Find all PDFs
    pdf_files = list(Path(papers_dir).glob('*.pdf'))
    stats['total_pdfs'] = len(pdf_files)
    
    logger.info(f"Found {len(pdf_files)} PDF files")
    logger.info(f"Mode: {'DRY RUN (simulation)' if dry_run else 'LIVE (will delete files)'}")
    
    for pdf_path in pdf_files:
        try:
            # Get base name without extension
            base_name = pdf_path.stem
            
            # Check for corresponding JSON files (both GROBID and Fast parser formats)
            json_grobid = Path(output_dir) / f"{base_name}.json"
            json_fast = Path(output_dir) / f"{base_name}_fast.json"
            
            json_path = None
            if json_grobid.exists():
                json_path = json_grobid
            elif json_fast.exists():
                json_path = json_fast
            
            if json_path:
                # Verify JSON is valid
                if verify_json_valid(json_path):
                    stats['pdfs_with_json'] += 1
                    pdf_size = pdf_path.stat().st_size
                    
                    if dry_run:
                        logger.info(f"[DRY RUN] Would delete: {pdf_path.name} ({pdf_size / 1024:.1f} KB)")
                        stats['pdfs_deleted'] += 1
                        stats['space_freed'] += pdf_size
                    else:
                        logger.info(f"Deleting: {pdf_path.name} ({pdf_size / 1024:.1f} KB)")
                        pdf_path.unlink()
                        stats['pdfs_deleted'] += 1
                        stats['space_freed'] += pdf_size
        
        except Exception as e:
            logger.error(f"Error processing {pdf_path.name}: {e}")
            stats['errors'] += 1
    
    return stats


def compress_json_files(output_dir='./output', dry_run=False, keep_original=False):
    """
    Compress JSON files with gzip.
    
    Args:
        output_dir: Directory containing JSONs
        dry_run: If True, only simulate
        keep_original: If True, keep original JSON files
        
    Returns:
        dict: Statistics about the operation
    """
    stats = {
        'total_jsons': 0,
        'compressed': 0,
        'space_saved': 0,
        'errors': 0
    }
    
    if not os.path.exists(output_dir):
        logger.error(f"Output directory not found: {output_dir}")
        return stats
    
    # Find all JSON files (not already compressed)
    json_files = [f for f in Path(output_dir).glob('*.json') if not f.name.endswith('.json.gz')]
    stats['total_jsons'] = len(json_files)
    
    logger.info(f"Found {len(json_files)} JSON files to compress")
    logger.info(f"Mode: {'DRY RUN (simulation)' if dry_run else 'LIVE'}")
    logger.info(f"Keep original: {keep_original}")
    
    for json_path in json_files:
        try:
            original_size = json_path.stat().st_size
            compressed_path = Path(str(json_path) + '.gz')
            
            if dry_run:
                # Estimate compression (assume 70% reduction based on test)
                estimated_compressed = int(original_size * 0.30)
                savings = original_size - estimated_compressed
                logger.info(f"[DRY RUN] Would compress: {json_path.name} "
                          f"({original_size / 1024:.1f} KB → ~{estimated_compressed / 1024:.1f} KB)")
                stats['compressed'] += 1
                stats['space_saved'] += savings if not keep_original else 0
            else:
                # Compress the file
                with open(json_path, 'rb') as f_in:
                    with gzip.open(compressed_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                compressed_size = compressed_path.stat().st_size
                savings = original_size - compressed_size
                
                logger.info(f"Compressed: {json_path.name} "
                          f"({original_size / 1024:.1f} KB → {compressed_size / 1024:.1f} KB, "
                          f"{100 * compressed_size / original_size:.1f}%)")
                
                # Delete original if requested
                if not keep_original:
                    json_path.unlink()
                    stats['space_saved'] += savings
                
                stats['compressed'] += 1
        
        except Exception as e:
            logger.error(f"Error compressing {json_path.name}: {e}")
            stats['errors'] += 1
    
    return stats


def create_archive(papers_dir='./papers', output_dir='./output', archive_dir='./archives'):
    """
    Create compressed archives of papers and JSONs for long-term storage.
    
    Args:
        papers_dir: Directory containing PDFs
        output_dir: Directory containing JSONs
        archive_dir: Directory to store archives
        
    Returns:
        str: Path to created archive
    """
    os.makedirs(archive_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    archive_name = f"papers_archive_{timestamp}.tar.gz"
    archive_path = os.path.join(archive_dir, archive_name)
    
    logger.info(f"Creating archive: {archive_path}")
    
    import tarfile
    
    with tarfile.open(archive_path, "w:gz") as tar:
        if os.path.exists(papers_dir):
            logger.info(f"Adding {papers_dir} to archive...")
            tar.add(papers_dir, arcname="papers")
        
        if os.path.exists(output_dir):
            logger.info(f"Adding {output_dir} to archive...")
            tar.add(output_dir, arcname="output")
    
    archive_size = os.path.getsize(archive_path)
    logger.info(f"Archive created: {archive_size / 1024 / 1024:.1f} MB")
    
    return archive_path


def print_statistics(stats, operation_name):
    """Print statistics in a formatted way."""
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"{operation_name} - STATISTICS")
    logger.info("=" * 70)
    for key, value in stats.items():
        if 'space' in key.lower() or 'size' in key.lower():
            logger.info(f"  {key}: {value / 1024 / 1024:.2f} MB")
        else:
            logger.info(f"  {key}: {value}")
    logger.info("=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Optimize disk space usage for paper processing pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to see what would be deleted
  python optimize_disk_space.py --delete-pdfs --dry-run
  
  # Actually delete PDFs with valid JSONs
  python optimize_disk_space.py --delete-pdfs
  
  # Compress JSONs (keep originals)
  python optimize_disk_space.py --compress-jsons --keep-original
  
  # Compress JSONs (delete originals)
  python optimize_disk_space.py --compress-jsons
  
  # Full optimization (delete PDFs + compress JSONs)
  python optimize_disk_space.py --delete-pdfs --compress-jsons
  
  # Create archive for long-term storage
  python optimize_disk_space.py --archive
        """
    )
    
    parser.add_argument('--delete-pdfs', action='store_true',
                       help='Delete PDFs that have valid JSON files')
    parser.add_argument('--compress-jsons', action='store_true',
                       help='Compress JSON files with gzip')
    parser.add_argument('--archive', action='store_true',
                       help='Create compressed archive of all files')
    parser.add_argument('--dry-run', action='store_true',
                       help='Simulate without actually modifying files')
    parser.add_argument('--keep-original', action='store_true',
                       help='Keep original files when compressing')
    parser.add_argument('--papers-dir', default='./papers',
                       help='Directory containing PDF files')
    parser.add_argument('--output-dir', default='./output',
                       help='Directory containing JSON files')
    parser.add_argument('--archive-dir', default='./archives',
                       help='Directory for archives')
    
    args = parser.parse_args()
    
    if not any([args.delete_pdfs, args.compress_jsons, args.archive]):
        parser.print_help()
        print("\nError: Please specify at least one operation")
        return 1
    
    total_space_freed = 0
    
    # Delete PDFs
    if args.delete_pdfs:
        logger.info("\n" + "=" * 70)
        logger.info("DELETING PDFs WITH VALID JSONs")
        logger.info("=" * 70)
        stats = delete_pdf_if_json_exists(
            papers_dir=args.papers_dir,
            output_dir=args.output_dir,
            dry_run=args.dry_run
        )
        print_statistics(stats, "PDF DELETION")
        total_space_freed += stats['space_freed']
    
    # Compress JSONs
    if args.compress_jsons:
        logger.info("\n" + "=" * 70)
        logger.info("COMPRESSING JSON FILES")
        logger.info("=" * 70)
        stats = compress_json_files(
            output_dir=args.output_dir,
            dry_run=args.dry_run,
            keep_original=args.keep_original
        )
        print_statistics(stats, "JSON COMPRESSION")
        total_space_freed += stats['space_saved']
    
    # Create archive
    if args.archive:
        if args.dry_run:
            logger.info("\n[DRY RUN] Would create archive")
        else:
            archive_path = create_archive(
                papers_dir=args.papers_dir,
                output_dir=args.output_dir,
                archive_dir=args.archive_dir
            )
            logger.info(f"\nArchive created: {archive_path}")
    
    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total space freed: {total_space_freed / 1024 / 1024:.2f} MB "
               f"({total_space_freed / 1024 / 1024 / 1024:.2f} GB)")
    if args.dry_run:
        logger.info("\n⚠️  This was a DRY RUN - no files were modified")
        logger.info("Run without --dry-run to actually optimize disk space")
    logger.info("=" * 70)
    
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
