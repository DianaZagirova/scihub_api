#!/usr/bin/env python3
"""
Simple CLI for downloading and processing papers from Sci-Hub.
This is the new, cleaner entry point using the restructured codebase.
"""

import sys
import os
import argparse
import json
import time
import datetime
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Add legacy to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from scihub_fast_downloader import SciHubFastDownloader
from scihub_grobid_downloader import SciHubGrobidDownloader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def normalize_identifier_to_filename(identifier):
    """
    Normalize an identifier to match the filename format used by the downloader.
    Handles DOI normalization to ensure consistent filename checking.
    
    Args:
        identifier: DOI, PMID, or title
    
    Returns:
        str: Normalized filename (without extension) or None if can't normalize
    """
    # Remove common DOI prefixes
    doi_prefixes = [
        'doi:', 'doi.org/', 'dx.doi.org/', 
        'http://dx.doi.org/', 'https://dx.doi.org/', 
        'http://doi.org/', 'https://doi.org/', 
        'https://www.doi.org/', 'http://www.doi.org/'
    ]
    
    normalized = identifier.strip()
    normalized_lower = normalized.lower()
    
    # Remove prefixes
    for prefix in doi_prefixes:
        if normalized_lower.startswith(prefix.lower()):
            normalized = normalized[len(prefix):].strip()
            break
    
    # Check if it's a DOI (starts with 10.)
    if normalized.startswith('10.') and '/' in normalized:
        # It's a DOI - convert / to _ for filename
        # Example: 10.1080/09658211.2015.1021257 -> 10.1080_09658211.2015.1021257
        safe_name = normalized.replace('/', '_')
        return safe_name
    
    # Not a DOI we can normalize
    return None


def should_skip_processing(identifier, parser_type, parse_mode, papers_dir='./papers', output_dir='./output'):
    """
    Check if we should skip processing based on existing files.
    
    Args:
        identifier: DOI, PMID, or title
        parser_type: 'fast' or 'grobid'
        parse_mode: Parse mode for fast parser
        papers_dir: Directory where PDFs are stored
        output_dir: Directory where JSON outputs are stored
    
    Returns:
        tuple: (skip_download, skip_parsing, pdf_path, json_path)
    """
    # Normalize identifier to filename format
    safe_name = normalize_identifier_to_filename(identifier)
    
    if not safe_name:
        # For other identifiers, we can't predict the filename easily
        # So we won't skip
        return False, False, None, None
    
    # Check if PDF exists
    pdf_path = os.path.join(papers_dir, f'{safe_name}.pdf')
    pdf_exists = os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0
    
    # Check if JSON exists
    # Note: Fast parser always saves with _fast.json suffix (see fast_pdf_parser.py line 358)
    # GROBID parser saves with .json suffix (no _fast)
    # Example: 10.1080_09658211.2015.1021257_fast.json (Fast) or 10.1080_09658211.2015.1021257.json (GROBID)
    if parser_type == 'fast':
        json_filename = f'{safe_name}_fast.json'
    else:  # grobid
        json_filename = f'{safe_name}.json'
    
    json_path = os.path.join(output_dir, json_filename)
    json_exists = os.path.exists(json_path) and os.path.getsize(json_path) > 0
    
    skip_download = pdf_exists
    skip_parsing = json_exists
    
    logger.debug(f"File check for {identifier}:")
    logger.debug(f"  PDF: {pdf_path} (exists: {pdf_exists})")
    logger.debug(f"  JSON: {json_path} (exists: {json_exists})")
    
    return skip_download, skip_parsing, pdf_path if pdf_exists else None, json_path if json_exists else None


def process_single_identifier(downloader, identifier, parser_type, parse_mode, log_lock, log_file):
    """
    Process a single identifier with comprehensive logging.
    
    Args:
        downloader: Downloader instance
        identifier: DOI, PMID, or title
        parser_type: 'fast' or 'grobid'
        parse_mode: Parse mode for fast parser
        log_lock: Threading lock for log file
        log_file: Path to comprehensive log file
    
    Returns:
        dict: Result with detailed status
    """
    result = {
        'identifier': identifier,
        'pdf_path': None,
        'json_path': None,
        'status': None,
        'download_status': None,
        'download_error': None,
        'parsing_status': None,
        'parsing_error': None,
        'parser_used': parser_type,
        'parse_mode': parse_mode if parser_type == 'fast' else None,
        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Check if we should skip
    skip_download, skip_parsing, existing_pdf, existing_json = should_skip_processing(
        identifier, parser_type, parse_mode
    )
    
    # Handle download
    if skip_download:
        logger.info(f"Skipping download for {identifier} - PDF already exists: {existing_pdf}")
        result['pdf_path'] = existing_pdf
        result['download_status'] = 'skipped_exists'
        pdf_path = existing_pdf
    else:
        # Download the paper
        try:
            if hasattr(downloader, 'downloader'):
                # It's a SciHubFastDownloader or SciHubGrobidDownloader
                pdf_path = downloader.downloader.download_paper(identifier)
            else:
                pdf_path = downloader.download_paper(identifier)
            
            if pdf_path:
                result['pdf_path'] = pdf_path
                result['download_status'] = 'success'
                logger.info(f"Successfully downloaded: {identifier} -> {pdf_path}")
            else:
                result['download_status'] = 'failed'
                result['download_error'] = 'Not found on Sci-Hub or download failed'
                result['status'] = 'not_found'
                logger.error(f"Failed to download: {identifier}")
                
                # Write to log
                write_to_comprehensive_log(log_lock, log_file, result)
                return result
        except Exception as e:
            result['download_status'] = 'error'
            result['download_error'] = str(e)
            result['status'] = 'not_found'
            logger.error(f"Error downloading {identifier}: {e}")
            
            # Write to log
            write_to_comprehensive_log(log_lock, log_file, result)
            return result
    
    # Handle parsing
    if skip_parsing:
        logger.info(f"Skipping parsing for {identifier} - JSON already exists: {existing_json}")
        result['json_path'] = existing_json
        result['parsing_status'] = 'skipped_exists'
        result['status'] = 'success'
    else:
        # Parse the paper
        try:
            # Determine output filename (must match what the parser actually produces)
            safe_name = os.path.splitext(os.path.basename(result['pdf_path']))[0]
            if parser_type == 'fast':
                # Fast parser always saves with _fast.json suffix
                json_filename = f'{safe_name}_fast.json'
            else:  # grobid
                json_filename = f'{safe_name}.json'
            
            output_dir = './output'
            json_path = os.path.join(output_dir, json_filename)
            
            # Parse using the downloader's parser
            if hasattr(downloader, 'parser'):
                if parser_type == 'fast':
                    # Fast parser: process_and_save(pdf_path, mode, output_dir)
                    extracted_data = downloader.parser.process_and_save(
                        result['pdf_path'],
                        mode=parse_mode,
                        output_dir=output_dir
                    )
                else:  # grobid
                    # GROBID parser: process_and_save(pdf_path, output_dir)
                    extracted_data = downloader.parser.process_and_save(
                        result['pdf_path'],
                        output_dir=output_dir
                    )
            else:
                # Parser not available
                extracted_data = None
                logger.warning(f"Parser not available for {identifier}")
            
            if extracted_data:
                result['json_path'] = json_path
                result['parsing_status'] = 'success'
                result['status'] = 'success'
                logger.info(f"Successfully parsed: {identifier}")
            else:
                result['parsing_status'] = 'failed'
                result['parsing_error'] = 'Processing returned no data'
                result['status'] = 'processing_failed'
                logger.error(f"Failed to parse: {identifier}")
        except Exception as e:
            result['parsing_status'] = 'error'
            result['parsing_error'] = str(e)
            result['status'] = 'processing_failed'
            logger.error(f"Error parsing {identifier}: {e}")
    
    # Write to log
    write_to_comprehensive_log(log_lock, log_file, result)
    return result


def write_to_comprehensive_log(log_lock, log_file, result):
    """
    Write detailed result to comprehensive log file.
    
    Args:
        log_lock: Threading lock
        log_file: Path to log file
        result: Result dictionary
    """
    with log_lock:
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"DOI/Identifier: {result['identifier']}\n")
            f.write(f"Timestamp: {result['timestamp']}\n")
            f.write(f"Parser Used: {result['parser_used'].upper()}")
            if result['parse_mode']:
                f.write(f" (mode: {result['parse_mode']})")
            f.write("\n")
            f.write(f"\n--- Download Status ---\n")
            f.write(f"Status: {result['download_status']}\n")
            if result['download_status'] == 'success':
                f.write(f"PDF Path: {result['pdf_path']}\n")
            elif result['download_status'] == 'skipped_exists':
                f.write(f"PDF Path: {result['pdf_path']} (already exists, skipped)\n")
            elif result['download_status'] == 'failed':
                f.write(f"Error: {result['download_error']}\n")
            elif result['download_status'] == 'error':
                f.write(f"Exception: {result['download_error']}\n")
            
            f.write(f"\n--- Parsing Status ---\n")
            if result['parsing_status']:
                f.write(f"Status: {result['parsing_status']}\n")
                if result['parsing_status'] == 'success':
                    f.write(f"JSON Path: {result['json_path']}\n")
                elif result['parsing_status'] == 'skipped_exists':
                    f.write(f"JSON Path: {result['json_path']} (already exists, skipped)\n")
                elif result['parsing_status'] in ['failed', 'error']:
                    f.write(f"Error: {result['parsing_error']}\n")
            else:
                f.write("Status: Not attempted (download failed)\n")
            
            f.write(f"\n--- Overall Status ---\n")
            f.write(f"Result: {result['status']}\n")


def process_with_parallel_and_logging(downloader, identifiers, num_workers, delay, log_file, parser_type, parse_mode):
    """
    Process identifiers with parallel execution and comprehensive logging.
    
    Args:
        downloader: Downloader instance
        identifiers: List of identifiers
        num_workers: Number of parallel workers
        delay: Delay between requests
        log_file: Path to comprehensive log file
        parser_type: 'fast' or 'grobid'
        parse_mode: Parse mode for fast parser
    
    Returns:
        list: List of results
    """
    # Initialize log file with header
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"{'='*80}\n")
        f.write(f"COMPREHENSIVE PROCESSING LOG\n")
        f.write(f"{'='*80}\n")
        f.write(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Parser: {parser_type.upper()}\n")
        if parser_type == 'fast':
            f.write(f"Parse Mode: {parse_mode}\n")
        f.write(f"Workers: {num_workers}\n")
        f.write(f"Total Identifiers: {len(identifiers)}\n")
        f.write(f"{'='*80}\n")
    
    log_lock = Lock()
    results = []
    last_request_time = [0]  # Use list to allow modification in nested function
    time_lock = Lock()
    
    def rate_limited_process(identifier):
        """Process with rate limiting."""
        # Process the identifier
        result = process_single_identifier(
            downloader, identifier, parser_type, parse_mode, log_lock, log_file
        )
        
        # Apply rate limiting ONLY if we actually downloaded from Sci-Hub
        # (not when skipping existing files)
        if result['download_status'] in ['success', 'failed', 'error']:
            with time_lock:
                current_time = time.time()
                time_since_last = current_time - last_request_time[0]
                if time_since_last < delay:
                    sleep_time = delay - time_since_last
                    time.sleep(sleep_time)
                last_request_time[0] = time.time()
        
        return result
    
    print(f"\nProcessing {len(identifiers)} identifiers with {num_workers} worker(s)...")
    print(f"Rate limit: {delay}s delay between requests\n")
    
    if num_workers == 1:
        # Sequential processing
        for i, identifier in enumerate(identifiers, 1):
            print(f"[{i}/{len(identifiers)}] Processing: {identifier}")
            result = rate_limited_process(identifier)
            results.append(result)
    else:
        # Parallel processing
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_identifier = {
                executor.submit(rate_limited_process, identifier): identifier
                for identifier in identifiers
            }
            
            for i, future in enumerate(as_completed(future_to_identifier), 1):
                identifier = future_to_identifier[future]
                print(f"[{i}/{len(identifiers)}] Completed: {identifier}")
                result = future.result()
                results.append(result)
    
    # Write summary to log
    with log_lock:
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n\n{'='*80}\n")
            f.write(f"SUMMARY\n")
            f.write(f"{'='*80}\n")
            f.write(f"Total Processed: {len(results)}\n")
            f.write(f"Success: {sum(1 for r in results if r['status'] == 'success')}\n")
            f.write(f"Not Found: {sum(1 for r in results if r['status'] == 'not_found')}\n")
            f.write(f"Processing Failed: {sum(1 for r in results if r['status'] == 'processing_failed')}\n")
            f.write(f"{'='*80}\n")
    
    return results


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Download and process academic papers from Sci-Hub using DOI, PMID, or title',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download by DOI with fast parser
  python download_papers.py 10.1038/s41586-019-1750-x
  
  # Download by PMID
  python download_papers.py 32265220
  
  # Download by title
  python download_papers.py "Deep learning for protein structure prediction"
  
  # Download from file with GROBID (sequential)
  python download_papers.py -f identifiers.txt --parser grobid
  
  # Parallel processing with fast parser (4 workers)
  python download_papers.py -f dois.txt --parser fast -w 4 --delay 2.0
  
  # Parallel processing with GROBID (2-4 workers recommended)
  python download_papers.py -f dois.txt --parser grobid -w 3 --delay 2.0
  
  # Process existing PDFs only
  python download_papers.py -p --mode full
  
  # Use simple mode (fastest) with parallel processing
  python download_papers.py -f identifiers.txt --mode simple -w 4
        """
    )
    
    # Input options
    parser.add_argument('identifiers', nargs='*', help='DOIs, PMIDs, or titles to download and process')
    parser.add_argument('-f', '--file', help='File containing identifiers (one per line - DOI, PMID, or title)')
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
    parser.add_argument('--log-dir', help='Directory for log files (default: auto-detect from input file or ./logs)')
    
    # Parallel processing options
    parser.add_argument('-w', '--workers', type=int, default=1,
                       help='Number of parallel workers (default: 1 for sequential). '
                            'Note: For GROBID, limit workers to 2-4 due to CPU intensity')
    parser.add_argument('--delay', type=float, default=2.0,
                       help='Delay between requests in seconds to avoid hitting Sci-Hub rate limits (default: 2.0)')
    
    # Other options
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Determine logs directory
    if args.log_dir:
        # Use explicitly specified log directory
        logs_dir = args.log_dir
    elif args.file:
        # Auto-detect from input file: use directory of input file + /logs
        input_file_dir = os.path.dirname(os.path.abspath(args.file))
        logs_dir = os.path.join(input_file_dir, 'logs')
    else:
        # Default to ./logs
        logs_dir = os.path.join(os.getcwd(), 'logs')
    
    os.makedirs(logs_dir, exist_ok=True)
    logger.info(f"Logs will be saved to: {logs_dir}")
    
    # Generate comprehensive log file
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    comprehensive_log_file = os.path.join(logs_dir, f'comprehensive_log_{timestamp}.log')
    
    # Select appropriate downloader
    if args.parser == 'grobid':
        downloader = SciHubGrobidDownloader(
            output_dir=args.output,
            config_path=args.config
        )
        # Warn if using too many workers with GROBID
        if args.workers > 4:
            logger.warning(f"Using {args.workers} workers with GROBID may cause high CPU usage and memory consumption.")
            logger.warning("Recommended: 2-4 workers for GROBID processing.")
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
        print("Error: No identifiers provided (DOI, PMID, or title)")
        parser.print_help()
        return 1
    
    # Process identifiers with parallel processing and comprehensive logging
    results = process_with_parallel_and_logging(
        downloader=downloader,
        identifiers=identifiers,
        num_workers=args.workers,
        delay=args.delay,
        log_file=comprehensive_log_file,
        parser_type=args.parser,
        parse_mode=args.mode
    )
    
    # Print summary
    success = sum(1 for r in results if r.get('status') == 'success')
    not_found = sum(1 for r in results if r.get('status') == 'not_found')
    failed = sum(1 for r in results if r.get('status') == 'processing_failed')
    
    print(f"\n{'='*50}")
    print(f"Total: {len(results)} | Success: {success} | Not found: {not_found} | Failed: {failed}")
    print(f"Comprehensive log: {comprehensive_log_file}")
    print(f"{'='*50}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
