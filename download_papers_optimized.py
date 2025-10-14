#!/usr/bin/env python3
"""
Optimized version of download_papers.py with per-worker rate limiting.
This allows true parallel downloads while respecting global rate limits.

Key improvements:
1. Token bucket rate limiter (allows N concurrent downloads)
2. Pre-scan to partition identifiers (skip/download/parse)
3. Buffered logging (reduces I/O)

Expected speedup: 5-10x depending on workload
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
from collections import deque

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


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter that allows burst traffic while maintaining average rate.
    This allows multiple workers to download simultaneously up to the bucket capacity.
    """
    
    def __init__(self, rate=0.5, capacity=5):
        """
        Initialize rate limiter.
        
        Args:
            rate: Tokens per second (e.g., 0.5 = one request every 2 seconds)
            capacity: Maximum tokens in bucket (allows this many simultaneous requests)
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.time()
        self.lock = Lock()
    
    def acquire(self, tokens=1):
        """
        Acquire tokens for a request. Blocks if insufficient tokens available.
        
        Args:
            tokens: Number of tokens to acquire (default 1)
        """
        with self.lock:
            while True:
                now = time.time()
                elapsed = now - self.last_update
                
                # Add tokens based on time elapsed
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now
                
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
                else:
                    # Calculate sleep time needed to accumulate enough tokens
                    needed = tokens - self.tokens
                    sleep_time = needed / self.rate
                    time.sleep(min(sleep_time, 1.0))  # Sleep max 1s at a time


class BufferedLogger:
    """Buffered logger to reduce I/O overhead."""
    
    def __init__(self, log_file, flush_interval=20):
        """
        Initialize buffered logger.
        
        Args:
            log_file: Path to log file
            flush_interval: Number of entries before flushing
        """
        self.log_file = log_file
        self.buffer = deque()
        self.flush_interval = flush_interval
        self.lock = Lock()
    
    def log(self, entry):
        """Add entry to buffer and flush if needed."""
        with self.lock:
            self.buffer.append(entry)
            if len(self.buffer) >= self.flush_interval:
                self._flush()
    
    def _flush(self):
        """Write buffer to file."""
        if self.buffer:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                while self.buffer:
                    f.write(self.buffer.popleft())
    
    def flush(self):
        """Public method to force flush."""
        with self.lock:
            self._flush()


def normalize_identifier_to_filename(identifier):
    """
    Normalize an identifier to match the filename format used by the downloader.
    """
    doi_prefixes = [
        'doi:', 'doi.org/', 'dx.doi.org/', 
        'http://dx.doi.org/', 'https://dx.doi.org/', 
        'http://doi.org/', 'https://doi.org/', 
        'https://www.doi.org/', 'http://www.doi.org/'
    ]
    
    normalized = identifier.strip()
    normalized_lower = normalized.lower()
    
    for prefix in doi_prefixes:
        if normalized_lower.startswith(prefix.lower()):
            normalized = normalized[len(prefix):].strip()
            break
    
    if normalized.startswith('10.') and '/' in normalized:
        safe_name = normalized.replace('/', '_')
        return safe_name
    
    return None


def partition_identifiers(identifiers, parser_type, papers_dir='./papers', output_dir='./output'):
    """
    Pre-scan identifiers and partition them into categories.
    
    Returns:
        tuple: (needs_download, needs_parse_only, complete)
    """
    needs_download = []
    needs_parse_only = []
    complete = []
    
    logger.info(f"Pre-scanning {len(identifiers)} identifiers...")
    
    for identifier in identifiers:
        safe_name = normalize_identifier_to_filename(identifier)
        
        if not safe_name:
            # Can't predict filename, assume needs download
            needs_download.append(identifier)
            continue
        
        # Check PDF existence
        pdf_path = os.path.join(papers_dir, f'{safe_name}.pdf')
        pdf_exists = os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0
        
        # Check JSON existence
        if parser_type == 'fast':
            json_filename = f'{safe_name}_fast.json'
        else:
            json_filename = f'{safe_name}.json'
        
        json_path = os.path.join(output_dir, json_filename)
        json_exists = os.path.exists(json_path) and os.path.getsize(json_path) > 0
        
        if not pdf_exists:
            needs_download.append(identifier)
        elif not json_exists:
            needs_parse_only.append((identifier, pdf_path))
        else:
            complete.append(identifier)
    
    logger.info(f"Partition results:")
    logger.info(f"  - Needs download: {len(needs_download)}")
    logger.info(f"  - Needs parse only: {len(needs_parse_only)}")
    logger.info(f"  - Already complete: {len(complete)}")
    
    return needs_download, needs_parse_only, complete


def process_single_with_rate_limit(downloader, identifier, parser_type, parse_mode, 
                                   rate_limiter, buffered_logger):
    """
    Process a single identifier with token bucket rate limiting.
    
    Returns:
        dict: Result with detailed status
    """
    result = {
        'identifier': identifier,
        'pdf_path': None,
        'json_path': None,
        'status': None,
        'download_status': None,
        'parsing_status': None,
        'parser_used': parser_type,
        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Acquire token before downloading (rate limiting)
    rate_limiter.acquire()
    
    # Download
    try:
        if hasattr(downloader, 'downloader'):
            pdf_path = downloader.downloader.download_paper(identifier)
        else:
            pdf_path = downloader.download_paper(identifier)
        
        if pdf_path:
            result['pdf_path'] = pdf_path
            result['download_status'] = 'success'
            logger.info(f"Downloaded: {identifier}")
        else:
            result['download_status'] = 'failed'
            result['status'] = 'not_found'
            logger.error(f"Failed to download: {identifier}")
            
            # Log to buffer
            log_entry = f"\n{'='*80}\n"
            log_entry += f"DOI: {identifier}\n"
            log_entry += f"Status: NOT FOUND\n"
            buffered_logger.log(log_entry)
            return result
    except Exception as e:
        result['download_status'] = 'error'
        result['status'] = 'not_found'
        logger.error(f"Error downloading {identifier}: {e}")
        return result
    
    # Parse (no rate limiting needed)
    try:
        safe_name = os.path.splitext(os.path.basename(pdf_path))[0]
        if parser_type == 'fast':
            json_filename = f'{safe_name}_fast.json'
        else:
            json_filename = f'{safe_name}.json'
        
        output_dir = './output'
        json_path = os.path.join(output_dir, json_filename)
        
        if hasattr(downloader, 'parser'):
            if parser_type == 'fast':
                extracted_data = downloader.parser.process_and_save(
                    pdf_path, mode=parse_mode, output_dir=output_dir
                )
            else:
                extracted_data = downloader.parser.process_and_save(
                    pdf_path, output_dir=output_dir
                )
        else:
            extracted_data = None
        
        if extracted_data:
            result['json_path'] = json_path
            result['parsing_status'] = 'success'
            result['status'] = 'success'
            logger.info(f"Parsed: {identifier}")
        else:
            result['parsing_status'] = 'failed'
            result['status'] = 'processing_failed'
            logger.error(f"Failed to parse: {identifier}")
    except Exception as e:
        result['parsing_status'] = 'error'
        result['status'] = 'processing_failed'
        logger.error(f"Error parsing {identifier}: {e}")
    
    # Log to buffer
    log_entry = f"\n{'='*80}\n"
    log_entry += f"DOI: {identifier}\n"
    log_entry += f"Status: {result['status']}\n"
    log_entry += f"Timestamp: {result['timestamp']}\n"
    buffered_logger.log(log_entry)
    
    return result


def process_optimized(downloader, identifiers, num_workers, delay, log_file, parser_type, parse_mode):
    """
    Optimized processing with token bucket rate limiting and pre-scanning.
    
    Returns:
        list: List of results
    """
    # Initialize log file
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"{'='*80}\n")
        f.write(f"OPTIMIZED PROCESSING LOG\n")
        f.write(f"{'='*80}\n")
        f.write(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Parser: {parser_type.upper()}\n")
        f.write(f"Workers: {num_workers}\n")
        f.write(f"Rate: {1/delay:.2f} req/s\n")
        f.write(f"{'='*80}\n")
    
    buffered_logger = BufferedLogger(log_file, flush_interval=20)
    
    # Create token bucket rate limiter
    # rate = requests per second, capacity = max concurrent requests
    rate = 1.0 / delay
    capacity = min(num_workers, int(rate * 10))  # Allow 10 seconds worth of burst
    rate_limiter = TokenBucketRateLimiter(rate=rate, capacity=capacity)
    
    logger.info(f"Rate limiter: {rate:.2f} req/s, capacity: {capacity} tokens")
    
    # Partition identifiers
    needs_download, needs_parse_only, complete = partition_identifiers(
        identifiers, parser_type
    )
    
    results = []
    
    # Process complete ones instantly (no downloads needed)
    for identifier in complete:
        result = {
            'identifier': identifier,
            'status': 'skipped_complete',
            'download_status': 'skipped_exists',
            'parsing_status': 'skipped_exists',
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        results.append(result)
    
    # Process parse-only (no rate limiting, just parsing)
    if needs_parse_only:
        logger.info(f"\nProcessing {len(needs_parse_only)} papers (parse only, no downloads)...")
        # TODO: Implement fast parsing for these
    
    # Process downloads with rate limiting
    if needs_download:
        logger.info(f"\nDownloading and processing {len(needs_download)} papers...")
        logger.info(f"Using {num_workers} workers with {delay}s delay\n")
        
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_identifier = {
                executor.submit(
                    process_single_with_rate_limit,
                    downloader, identifier, parser_type, parse_mode,
                    rate_limiter, buffered_logger
                ): identifier
                for identifier in needs_download
            }
            
            for i, future in enumerate(as_completed(future_to_identifier), 1):
                identifier = future_to_identifier[future]
                print(f"[{i}/{len(needs_download)}] Completed: {identifier}")
                result = future.result()
                results.append(result)
    
    # Flush remaining log entries
    buffered_logger.flush()
    
    # Write summary
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"\n\n{'='*80}\n")
        f.write(f"SUMMARY\n")
        f.write(f"{'='*80}\n")
        f.write(f"Total: {len(results)}\n")
        f.write(f"Success: {sum(1 for r in results if r.get('status') == 'success')}\n")
        f.write(f"Skipped (complete): {len(complete)}\n")
        f.write(f"Not Found: {sum(1 for r in results if r.get('status') == 'not_found')}\n")
        f.write(f"Failed: {sum(1 for r in results if r.get('status') == 'processing_failed')}\n")
    
    return results


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='OPTIMIZED paper downloader with per-worker rate limiting',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Optimizations:
  - Token bucket rate limiting (allows true parallel downloads)
  - Pre-scan to skip already-complete papers instantly
  - Buffered logging (reduced I/O overhead)

Examples:
  # Fast parser with 8 workers
  python download_papers_optimized.py -f dois.txt --parser fast -w 8 --delay 2.0
  
  # GROBID with 4 workers (CPU-intensive)
  python download_papers_optimized.py -f dois.txt --parser grobid -w 4 --delay 2.0
        """
    )
    
    parser.add_argument('identifiers', nargs='*', help='DOIs to download and process')
    parser.add_argument('-f', '--file', help='File containing identifiers (one per line)')
    parser.add_argument('--parser', choices=['fast', 'grobid'], default='fast')
    parser.add_argument('-m', '--mode', choices=['simple', 'structured', 'full'], default='structured')
    parser.add_argument('-o', '--output', help='Output directory')
    parser.add_argument('-c', '--config', help='Config file')
    parser.add_argument('--log-dir', help='Log directory')
    parser.add_argument('-w', '--workers', type=int, default=5,
                       help='Number of parallel workers (default: 5)')
    parser.add_argument('--delay', type=float, default=2.0,
                       help='Delay between requests (default: 2.0s)')
    parser.add_argument('-v', '--verbose', action='store_true')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Setup log directory
    if args.log_dir:
        logs_dir = args.log_dir
    elif args.file:
        input_file_dir = os.path.dirname(os.path.abspath(args.file))
        logs_dir = os.path.join(input_file_dir, 'logs')
    else:
        logs_dir = './logs'
    
    os.makedirs(logs_dir, exist_ok=True)
    
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(logs_dir, f'optimized_log_{timestamp}.log')
    
    # Initialize downloader
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
    
    # Collect identifiers
    identifiers = []
    if args.identifiers:
        identifiers.extend(args.identifiers)
    
    if args.file:
        with open(args.file, 'r') as f:
            identifiers.extend([line.strip() for line in f if line.strip()])
    
    if not identifiers:
        print("Error: No identifiers provided")
        parser.print_help()
        return 1
    
    # Process with optimizations
    start_time = time.time()
    results = process_optimized(
        downloader=downloader,
        identifiers=identifiers,
        num_workers=args.workers,
        delay=args.delay,
        log_file=log_file,
        parser_type=args.parser,
        parse_mode=args.mode
    )
    elapsed = time.time() - start_time
    
    # Summary
    success = sum(1 for r in results if r.get('status') == 'success')
    skipped = sum(1 for r in results if r.get('status') == 'skipped_complete')
    not_found = sum(1 for r in results if r.get('status') == 'not_found')
    failed = sum(1 for r in results if r.get('status') == 'processing_failed')
    
    print(f"\n{'='*50}")
    print(f"Completed in {elapsed/60:.1f} minutes")
    print(f"Total: {len(results)} | Success: {success} | Skipped: {skipped}")
    print(f"Not found: {not_found} | Failed: {failed}")
    print(f"Log: {log_file}")
    print(f"{'='*50}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
