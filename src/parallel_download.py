#!/usr/bin/env python3
"""
Parallel Paper Downloader
-------------------------
Download and process multiple papers concurrently for faster batch processing.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Add legacy to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'legacy'))

from scihub_fast_downloader import SciHubFastDownloader
from scihub_grobid_downloader import SciHubGrobidDownloader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ParallelDownloader:
    """Parallel paper downloader with progress tracking."""
    
    def __init__(self, parser_type='fast', output_dir=None, parse_mode='structured', 
                 config_path=None, max_workers=4):
        """
        Initialize parallel downloader.
        
        Args:
            parser_type (str): 'fast' or 'grobid'
            output_dir (str): Output directory for papers
            parse_mode (str): Parsing mode (for fast parser)
            config_path (str): Config path (for GROBID)
            max_workers (int): Number of parallel workers
        """
        self.parser_type = parser_type
        self.max_workers = max_workers
        
        # Create downloader instance for each worker (thread-safe)
        if parser_type == 'grobid':
            self.downloader_class = SciHubGrobidDownloader
            self.downloader_kwargs = {
                'output_dir': output_dir,
                'config_path': config_path
            }
        else:
            self.downloader_class = SciHubFastDownloader
            self.downloader_kwargs = {
                'output_dir': output_dir,
                'parse_mode': parse_mode
            }
    
    def process_single(self, identifier):
        """
        Process a single identifier.
        
        Args:
            identifier (str): DOI, PMID, or title
            
        Returns:
            dict: Result with status
        """
        # Create a new downloader instance for this thread
        downloader = self.downloader_class(**self.downloader_kwargs)
        
        try:
            pdf_path, extracted_data, status = downloader.download_and_process(identifier)
            
            return {
                'identifier': identifier,
                'pdf_path': pdf_path,
                'status': status,
                'success': status == 'success'
            }
        except Exception as e:
            logger.error(f"Error processing {identifier}: {e}")
            return {
                'identifier': identifier,
                'pdf_path': None,
                'status': 'error',
                'success': False,
                'error': str(e)
            }
    
    def process_batch(self, identifiers):
        """
        Process multiple identifiers in parallel.
        
        Args:
            identifiers (list): List of DOIs, PMIDs, or titles
            
        Returns:
            dict: Summary of results
        """
        results = []
        success_count = 0
        failed_count = 0
        
        print(f"\n{'='*60}")
        print(f"Processing {len(identifiers)} papers with {self.max_workers} parallel workers")
        print(f"Parser: {self.parser_type.upper()}")
        print(f"{'='*60}\n")
        
        # Use ThreadPoolExecutor for I/O-bound tasks (downloading)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_identifier = {
                executor.submit(self.process_single, identifier): identifier 
                for identifier in identifiers
            }
            
            # Process completed tasks with progress bar
            with tqdm(total=len(identifiers), desc="Processing papers", unit="paper") as pbar:
                for future in as_completed(future_to_identifier):
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        success_count += 1
                    else:
                        failed_count += 1
                    
                    pbar.update(1)
                    pbar.set_postfix({
                        'Success': success_count,
                        'Failed': failed_count
                    })
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"Processing Complete!")
        print(f"{'='*60}")
        print(f"Total: {len(identifiers)}")
        print(f"  ✓ Success: {success_count}")
        print(f"  ✗ Failed: {failed_count}")
        print(f"{'='*60}\n")
        
        return {
            'results': results,
            'total': len(identifiers),
            'success': success_count,
            'failed': failed_count
        }
    
    def save_comprehensive_report(self, results, output_file):
        """
        Save a comprehensive report of all processing results.
        
        Args:
            results (list): List of result dictionaries
            output_file (str): Path to save the report
        """
        import datetime
        
        # Categorize results
        successful = [r for r in results if r['status'] == 'success']
        not_found = [r for r in results if r['status'] == 'not_found']
        processing_failed = [r for r in results if r['status'] == 'processing_failed']
        errors = [r for r in results if r['status'] == 'error']
        
        with open(output_file, 'w', encoding='utf-8') as f:
            # Header
            f.write("="*80 + "\n")
            f.write("COMPREHENSIVE PROCESSING REPORT\n")
            f.write("="*80 + "\n")
            f.write(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Parser Type: {self.parser_type.upper()}\n")
            f.write(f"Workers: {self.max_workers}\n")
            f.write("\n")
            
            # Summary Statistics
            f.write("-"*80 + "\n")
            f.write("SUMMARY STATISTICS\n")
            f.write("-"*80 + "\n")
            f.write(f"Total Papers Processed: {len(results)}\n")
            f.write(f"  ✓ Successfully Downloaded & Processed: {len(successful)} ({len(successful)/len(results)*100:.1f}%)\n")
            f.write(f"  ✗ Not Found on Sci-Hub: {len(not_found)} ({len(not_found)/len(results)*100:.1f}%)\n")
            f.write(f"  ⚠ Downloaded but Processing Failed: {len(processing_failed)} ({len(processing_failed)/len(results)*100:.1f}%)\n")
            if errors:
                f.write(f"  ⚠ Errors: {len(errors)} ({len(errors)/len(results)*100:.1f}%)\n")
            f.write("\n")
            
            # Successful Downloads
            f.write("="*80 + "\n")
            f.write(f"✓ SUCCESSFULLY PROCESSED ({len(successful)} papers)\n")
            f.write("="*80 + "\n")
            if successful:
                for i, result in enumerate(successful, 1):
                    f.write(f"{i:4d}. {result['identifier']}\n")
                    f.write(f"       → PDF: {result['pdf_path']}\n")
                    f.write("\n")
            else:
                f.write("None\n\n")
            
            # Not Found on Sci-Hub
            f.write("="*80 + "\n")
            f.write(f"✗ NOT FOUND ON SCI-HUB ({len(not_found)} papers)\n")
            f.write("="*80 + "\n")
            f.write("These papers were not available on Sci-Hub or could not be downloaded.\n")
            f.write("You may need to access them through institutional subscriptions or other means.\n")
            f.write("\n")
            if not_found:
                for i, result in enumerate(not_found, 1):
                    f.write(f"{i:4d}. {result['identifier']}\n")
                f.write("\n")
            else:
                f.write("None\n\n")
            
            # Processing Failed
            f.write("="*80 + "\n")
            f.write(f"⚠ DOWNLOADED BUT PROCESSING FAILED ({len(processing_failed)} papers)\n")
            f.write("="*80 + "\n")
            f.write("These papers were downloaded but could not be processed (PDF extraction failed).\n")
            f.write("The PDF files are available but metadata/text extraction failed.\n")
            f.write("\n")
            if processing_failed:
                for i, result in enumerate(processing_failed, 1):
                    f.write(f"{i:4d}. {result['identifier']}\n")
                    f.write(f"       → PDF: {result['pdf_path']}\n")
                    f.write("\n")
            else:
                f.write("None\n\n")
            
            # Errors
            if errors:
                f.write("="*80 + "\n")
                f.write(f"⚠ PROCESSING ERRORS ({len(errors)} papers)\n")
                f.write("="*80 + "\n")
                f.write("These papers encountered unexpected errors during processing.\n")
                f.write("\n")
                for i, result in enumerate(errors, 1):
                    f.write(f"{i:4d}. {result['identifier']}\n")
                    if 'error' in result:
                        f.write(f"       → Error: {result['error']}\n")
                    f.write("\n")
            
            # Retry List
            f.write("="*80 + "\n")
            f.write("RETRY LIST (Not Found Papers)\n")
            f.write("="*80 + "\n")
            f.write("Copy the identifiers below to a file and retry:\n")
            f.write("  python parallel_download.py -f retry.txt -w 4\n")
            f.write("\n")
            if not_found:
                for result in not_found:
                    f.write(f"{result['identifier']}\n")
            else:
                f.write("(No papers to retry)\n")
            f.write("\n")
            
            # Footer
            f.write("="*80 + "\n")
            f.write("END OF REPORT\n")
            f.write("="*80 + "\n")
        
        logger.info(f"Comprehensive report saved to: {output_file}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Parallel paper downloader and processor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process 10 papers in parallel with fast parser
  python parallel_download.py -f dois.txt -w 10
  
  # Process with GROBID parser (4 workers)
  python parallel_download.py -f identifiers.txt --parser grobid -w 4
  
  # Process with 8 workers in structured mode
  python parallel_download.py -f papers.txt -w 8 --mode structured
  
Note: 
  - More workers = faster processing but higher load
  - Recommended: 4-8 workers for fast parser, 2-4 for GROBID
  - GROBID is CPU-intensive, adjust GROBID server threads accordingly
        """
    )
    
    # Input options
    parser.add_argument('identifiers', nargs='*', 
                       help='DOIs, PMIDs, or titles to download and process')
    parser.add_argument('-f', '--file', required=False,
                       help='File containing identifiers (one per line)')
    parser.add_argument('-w', '--workers', type=int, default=4,
                       help='Number of parallel workers (default: 4)')
    
    # Parser selection
    parser.add_argument('--parser', choices=['fast', 'grobid'], default='fast',
                       help='PDF parser to use (default: fast)')
    
    # Processing mode (for fast parser)
    parser.add_argument('-m', '--mode', choices=['simple', 'structured', 'full'],
                       default='structured', help='PDF parsing mode (default: structured)')
    
    # Output options
    parser.add_argument('-o', '--output', help='Output directory for downloaded papers')
    parser.add_argument('-c', '--config', help='Path to configuration file (for GROBID)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Collect identifiers
    identifiers = []
    if args.identifiers:
        identifiers.extend(args.identifiers)
    
    if args.file:
        try:
            with open(args.file, 'r') as f:
                identifiers.extend([line.strip() for line in f if line.strip()])
        except Exception as e:
            print(f"Error reading file: {e}")
            return 1
    
    if not identifiers:
        print("Error: No identifiers provided")
        parser.print_help()
        return 1
    
    print(f"Loaded {len(identifiers)} identifiers")
    
    # Create parallel downloader
    downloader = ParallelDownloader(
        parser_type=args.parser,
        output_dir=args.output,
        parse_mode=args.mode,
        config_path=args.config,
        max_workers=args.workers
    )
    
    # Process batch
    summary = downloader.process_batch(identifiers)
    
    # Save comprehensive report
    import datetime
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    report_filename = f"processing_report_{timestamp}.txt"
    report_path = os.path.join('logs', report_filename)
    
    # Ensure logs directory exists
    os.makedirs('logs', exist_ok=True)
    
    downloader.save_comprehensive_report(summary['results'], report_path)
    
    print(f"\n📄 Comprehensive report saved: {report_path}")
    print(f"   View with: cat {report_path}\n")
    
    # Exit with appropriate code
    return 0 if summary['failed'] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
