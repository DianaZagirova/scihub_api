#!/usr/bin/env python3
"""
Simple CLI for downloading and processing papers from Sci-Hub.
This is the new, cleaner entry point using the restructured codebase.
"""

import sys
import argparse
from pathlib import Path

# Add legacy to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'legacy'))

from scihub_fast_downloader import SciHubFastDownloader
from scihub_grobid_downloader import SciHubGrobidDownloader


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Download and process academic papers from Sci-Hub',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download and process with fast parser
  python download_papers.py 10.1038/s41586-019-1750-x
  
  # Download from file with GROBID
  python download_papers.py -f dois.txt --parser grobid
  
  # Process existing PDFs only
  python download_papers.py -p --mode full
  
  # Use simple mode (fastest)
  python download_papers.py -f dois.txt --mode simple
        """
    )
    
    # Input options
    parser.add_argument('dois', nargs='*', help='DOIs to download and process')
    parser.add_argument('-f', '--file', help='File containing DOIs (one per line)')
    parser.add_argument('-p', '--process-only', action='store_true', 
                       help='Process existing papers only (no download)')
    
    # Parser selection
    parser.add_argument('--parser', choices=['fast', 'grobid'], default='fast',
                       help='PDF parser to use (default: fast)')
    
    # Processing mode
    parser.add_argument('-m', '--mode', choices=['simple', 'structured', 'full'],
                       default='structured', help='PDF parsing mode (default: structured)')
    
    # Output options
    parser.add_argument('-o', '--output', help='Output directory for downloaded papers')
    parser.add_argument('-c', '--config', help='Path to configuration file')
    
    # Other options
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Select appropriate downloader
    if args.parser == 'grobid':
        downloader = SciHubGrobidDownloader(
            output_dir=args.output,
            config_path=args.config
        )
    else:
        downloader = SciHubFastDownloader(
            output_dir=args.output,
            parse_mode=args.mode
        )
    
    # Process existing papers only
    if args.process_only:
        results = downloader.process_existing_papers()
        success_count = sum(1 for r in results if r.get('status') == 'success')
        print(f"\nProcessed {len(results)} papers: {success_count} succeeded")
        return 0
    
    # Collect DOIs
    dois = []
    if args.dois:
        dois.extend(args.dois)
    
    if args.file:
        try:
            with open(args.file, 'r') as f:
                dois.extend([line.strip() for line in f if line.strip()])
        except Exception as e:
            print(f"Error reading file: {e}")
            return 1
    
    if not dois:
        print("Error: No DOIs provided")
        parser.print_help()
        return 1
    
    # Process DOIs
    results = downloader.batch_download_and_process(dois)
    
    # Print summary
    success = sum(1 for r in results if r.get('status') == 'success')
    not_found = sum(1 for r in results if r.get('status') == 'not_found')
    failed = sum(1 for r in results if r.get('status') == 'processing_failed')
    
    print(f"\n{'='*50}")
    print(f"Total: {len(results)} | Success: {success} | Not found: {not_found} | Failed: {failed}")
    print(f"{'='*50}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
