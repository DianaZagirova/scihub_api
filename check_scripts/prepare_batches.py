#!/usr/bin/env python3
"""
Script to prepare DOI batches for safe processing of large datasets
"""

import argparse
import os
from pathlib import Path


def split_file_into_batches(input_file, batch_size, output_dir='batches'):
    """
    Split a file of DOIs into smaller batch files.
    
    Args:
        input_file: Path to input file with DOIs (one per line)
        batch_size: Number of DOIs per batch
        output_dir: Directory to store batch files
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Read all DOIs
    with open(input_file, 'r', encoding='utf-8') as f:
        dois = [line.strip() for line in f if line.strip()]
    
    total_dois = len(dois)
    num_batches = (total_dois + batch_size - 1) // batch_size  # Ceiling division
    
    print(f"Total DOIs: {total_dois:,}")
    print(f"Batch size: {batch_size:,}")
    print(f"Number of batches: {num_batches}")
    print(f"Output directory: {output_dir}/")
    print()
    
    # Split into batches
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_dois)
        batch_dois = dois[start_idx:end_idx]
        
        # Create batch file
        batch_filename = f"batch_{i+1:03d}_of_{num_batches:03d}.txt"
        batch_path = os.path.join(output_dir, batch_filename)
        
        with open(batch_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(batch_dois))
        
        print(f"Created {batch_filename}: {len(batch_dois):,} DOIs (lines {start_idx+1}-{end_idx})")
    
    print()
    print(f"✓ Successfully created {num_batches} batch files in {output_dir}/")
    print()
    print("Next steps:")
    print("1. Process each batch with:")
    print(f"   python download_papers.py -f {output_dir}/batch_001_of_{num_batches:03d}.txt --parser grobid -w 4 --delay 2.0")
    print()
    print("2. Monitor disk space during processing:")
    print("   watch -n 60 'df -h /'")
    print()
    print("3. Track progress in logs/ directory")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Split DOI file into batches for safe processing'
    )
    parser.add_argument(
        'input_file',
        help='Input file containing DOIs (one per line)'
    )
    parser.add_argument(
        '-b', '--batch-size',
        type=int,
        default=5000,
        help='Number of DOIs per batch (default: 5000)'
    )
    parser.add_argument(
        '-o', '--output-dir',
        default='batches',
        help='Output directory for batch files (default: batches)'
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_file):
        print(f"Error: Input file not found: {args.input_file}")
        exit(1)
    
    split_file_into_batches(
        input_file=args.input_file,
        batch_size=args.batch_size,
        output_dir=args.output_dir
    )
