#!/usr/bin/env python3
"""
Fetch Missing Papers from Database
-----------------------------------
Analyzes papers.db for papers missing full_text or abstract fields,
then fetches and processes them using Sci-Hub + GROBID in parallel.

This script:
1. Queries the database for papers with missing full_text OR abstract
2. Downloads papers from Sci-Hub using DOI/PMID
3. Processes PDFs with GROBID to extract text
4. Updates the database with extracted data
"""

import os
import sys
import sqlite3
import argparse
import logging
import datetime
import time
import random
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Add legacy to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'legacy'))

from scihub_grobid_downloader import SciHubGrobidDownloader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class MissingPapersFetcher:
    """Fetches and processes papers with missing data from the database."""
    
    def __init__(self, db_path, output_dir=None, config_path=None, max_workers=4, min_delay=2.0, max_delay=5.0):
        """
        Initialize the missing papers fetcher.
        
        Args:
            db_path (str): Path to papers.db database
            output_dir (str): Directory to save downloaded papers
            config_path (str): Path to GROBID configuration file
            max_workers (int): Number of parallel workers
            min_delay (float): Minimum delay between requests (seconds)
            max_delay (float): Maximum delay between requests (seconds)
        """
        self.db_path = db_path
        self.output_dir = output_dir or os.path.join(os.getcwd(), 'papers_missing')
        self.config_path = config_path
        self.max_workers = max_workers
        self.min_delay = min_delay
        self.max_delay = max_delay
        
        # Thread-safe rate limiting
        self._request_lock = threading.Lock()
        self._last_request_time = 0
        
        # Create output directories
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"PDFs will be saved to: {self.output_dir}")
        
        # GROBID processed files directory
        self.processed_dir = os.path.join(os.getcwd(), 'output')
        os.makedirs(self.processed_dir, exist_ok=True)
        logger.info(f"GROBID processed files (JSON/TEI) will be saved to: {self.processed_dir}")
        
        # Create logs directory
        self.logs_dir = os.path.join(os.getcwd(), 'logs')
        os.makedirs(self.logs_dir, exist_ok=True)
        
        # Initialize downloader class (will be instantiated per thread)
        self.downloader_kwargs = {
            'output_dir': self.output_dir,
            'config_path': self.config_path
        }
    
    def connect_db(self):
        """Create a database connection."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise
    
    def analyze_missing_papers(self):
        """
        Analyze the database to find papers with missing full_text or abstract.
        
        Returns:
            dict: Statistics about missing papers
        """
        conn = self.connect_db()
        cursor = conn.cursor()
        
        # Count total papers
        cursor.execute("SELECT COUNT(*) as total FROM papers")
        total_papers = cursor.fetchone()['total']
        
        # Count papers missing full_text
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM papers 
            WHERE full_text IS NULL OR full_text = ''
        """)
        missing_full_text = cursor.fetchone()['count']
        
        # Count papers missing abstract
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM papers 
            WHERE abstract IS NULL OR abstract = ''
        """)
        missing_abstract = cursor.fetchone()['count']
        
        # Count papers missing either full_text OR abstract
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM papers 
            WHERE (full_text IS NULL OR full_text = '') 
               OR (abstract IS NULL OR abstract = '')
        """)
        missing_either = cursor.fetchone()['count']
        
        # Count papers missing both
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM papers 
            WHERE (full_text IS NULL OR full_text = '') 
              AND (abstract IS NULL OR abstract = '')
        """)
        missing_both = cursor.fetchone()['count']
        
        conn.close()
        
        stats = {
            'total_papers': total_papers,
            'missing_full_text': missing_full_text,
            'missing_abstract': missing_abstract,
            'missing_either': missing_either,
            'missing_both': missing_both
        }
        
        return stats
    
    def get_missing_papers(self, limit=None):
        """
        Get papers with missing full_text or abstract from the database.
        
        Args:
            limit (int): Maximum number of papers to fetch (None for all)
            
        Returns:
            list: List of paper records with missing data
        """
        conn = self.connect_db()
        cursor = conn.cursor()
        
        query = """
            SELECT pmid, doi, title, abstract, full_text
            FROM papers 
            WHERE (full_text IS NULL OR full_text = '') 
               OR (abstract IS NULL OR abstract = '')
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query)
        papers = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        logger.info(f"Found {len(papers)} papers with missing data")
        return papers
    
    def update_paper_in_db(self, pmid, abstract=None, full_text=None):
        """
        Update a paper in the database with extracted data.
        
        Args:
            pmid (str): PMID of the paper
            abstract (str): Extracted abstract
            full_text (str): Extracted full text
        """
        conn = self.connect_db()
        cursor = conn.cursor()
        
        update_fields = []
        params = []
        
        if abstract:
            update_fields.append("abstract = ?")
            params.append(abstract)
        
        if full_text:
            update_fields.append("full_text = ?")
            params.append(full_text)
        
        if update_fields:
            params.append(pmid)
            query = f"UPDATE papers SET {', '.join(update_fields)} WHERE pmid = ?"
            cursor.execute(query, params)
            conn.commit()
            logger.debug(f"Updated paper {pmid} in database")
        
        conn.close()
    
    def _rate_limited_sleep(self):
        """
        Enforce rate limiting between Sci-Hub requests (thread-safe).
        """
        with self._request_lock:
            current_time = time.time()
            time_since_last = current_time - self._last_request_time
            
            # Calculate required delay
            required_delay = random.uniform(self.min_delay, self.max_delay)
            
            if time_since_last < required_delay:
                sleep_time = required_delay - time_since_last
                time.sleep(sleep_time)
            
            self._last_request_time = time.time()
    
    def process_single_paper(self, paper):
        """
        Process a single paper: download from Sci-Hub and extract with GROBID.
        
        Args:
            paper (dict): Paper record from database
            
        Returns:
            dict: Result with status and extracted data
        """
        pmid = paper['pmid']
        doi = paper['doi']
        title = paper['title']
        
        # Determine identifier to use (prefer DOI, fallback to title, never PMID)
        if doi:
            identifier = doi
            logger.debug(f"Using DOI: {doi}")
        elif title:
            identifier = title
            logger.info(f"No DOI available, using title: {title[:50]}...")
        else:
            logger.error(f"Paper {pmid} has no DOI or title, skipping")
            return {
                'pmid': pmid,
                'doi': doi,
                'identifier': None,
                'pdf_path': None,
                'status': 'error',
                'success': False,
                'error': 'No DOI or title available',
                'updated_db': False
            }
        
        # Rate limit before making request to Sci-Hub
        self._rate_limited_sleep()
        
        # Create a new downloader instance for this thread
        downloader = SciHubGrobidDownloader(**self.downloader_kwargs)
        
        try:
            # Download and process the paper
            pdf_path, extracted_data, status = downloader.download_and_process(identifier)
            
            result = {
                'pmid': pmid,
                'doi': doi,
                'identifier': identifier,
                'pdf_path': pdf_path,
                'status': status,
                'success': status == 'success',
                'extracted_data': extracted_data
            }
            
            # If successful, update the database
            if status == 'success' and extracted_data:
                abstract = None
                full_text = None
                
                # Extract abstract from GROBID data
                if 'abstract' in extracted_data:
                    abstract = extracted_data['abstract']
                
                # Extract full text from GROBID data
                if 'full_text' in extracted_data:
                    full_text = extracted_data['full_text']
                elif 'body' in extracted_data:
                    full_text = extracted_data['body']
                
                # Update database
                if abstract or full_text:
                    self.update_paper_in_db(pmid, abstract=abstract, full_text=full_text)
                    result['updated_db'] = True
                else:
                    result['updated_db'] = False
                    logger.warning(f"No abstract or full_text extracted for {identifier}")
            else:
                result['updated_db'] = False
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing paper {identifier}: {e}")
            return {
                'pmid': pmid,
                'doi': doi,
                'identifier': identifier,
                'pdf_path': None,
                'status': 'error',
                'success': False,
                'error': str(e),
                'updated_db': False
            }
    
    def process_missing_papers(self, limit=None):
        """
        Process all papers with missing data in parallel.
        
        Args:
            limit (int): Maximum number of papers to process (None for all)
            
        Returns:
            dict: Summary of results
        """
        # Get papers with missing data
        papers = self.get_missing_papers(limit=limit)
        
        if not papers:
            logger.info("No papers with missing data found!")
            return {
                'results': [],
                'total': 0,
                'success': 0,
                'failed': 0,
                'updated_db': 0
            }
        
        results = []
        success_count = 0
        failed_count = 0
        updated_db_count = 0
        
        print(f"\n{'='*60}")
        print(f"Processing {len(papers)} papers with missing data")
        print(f"Using {self.max_workers} parallel workers with GROBID")
        print(f"{'='*60}\n")
        
        # Use ThreadPoolExecutor for I/O-bound tasks
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_paper = {
                executor.submit(self.process_single_paper, paper): paper 
                for paper in papers
            }
            
            # Process completed tasks with progress bar
            with tqdm(total=len(papers), desc="Processing papers", unit="paper") as pbar:
                for future in as_completed(future_to_paper):
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        success_count += 1
                        if result.get('updated_db', False):
                            updated_db_count += 1
                    else:
                        failed_count += 1
                    
                    pbar.update(1)
                    pbar.set_postfix({
                        'Success': success_count,
                        'Failed': failed_count,
                        'DB Updated': updated_db_count
                    })
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"Processing Complete!")
        print(f"{'='*60}")
        print(f"Total: {len(papers)}")
        print(f"  ✓ Success: {success_count}")
        print(f"  ✗ Failed: {failed_count}")
        print(f"  📝 Database Updated: {updated_db_count}")
        print(f"{'='*60}\n")
        
        return {
            'results': results,
            'total': len(papers),
            'success': success_count,
            'failed': failed_count,
            'updated_db': updated_db_count
        }
    
    def save_report(self, results, stats, output_file):
        """
        Save a comprehensive report of the processing results.
        
        Args:
            results (dict): Processing results
            stats (dict): Database statistics
            output_file (str): Path to save the report
        """
        with open(output_file, 'w', encoding='utf-8') as f:
            # Header
            f.write("="*80 + "\n")
            f.write("MISSING PAPERS PROCESSING REPORT\n")
            f.write("="*80 + "\n")
            f.write(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Database: {self.db_path}\n")
            f.write(f"Workers: {self.max_workers}\n")
            f.write("\n")
            
            # Database Statistics
            f.write("-"*80 + "\n")
            f.write("DATABASE STATISTICS\n")
            f.write("-"*80 + "\n")
            f.write(f"Total Papers in Database: {stats['total_papers']}\n")
            f.write(f"Papers Missing Full Text: {stats['missing_full_text']} ({stats['missing_full_text']/stats['total_papers']*100:.1f}%)\n")
            f.write(f"Papers Missing Abstract: {stats['missing_abstract']} ({stats['missing_abstract']/stats['total_papers']*100:.1f}%)\n")
            f.write(f"Papers Missing Either: {stats['missing_either']} ({stats['missing_either']/stats['total_papers']*100:.1f}%)\n")
            f.write(f"Papers Missing Both: {stats['missing_both']} ({stats['missing_both']/stats['total_papers']*100:.1f}%)\n")
            f.write("\n")
            
            # Processing Summary
            f.write("-"*80 + "\n")
            f.write("PROCESSING SUMMARY\n")
            f.write("-"*80 + "\n")
            f.write(f"Papers Processed: {results['total']}\n")
            f.write(f"  ✓ Successfully Downloaded & Processed: {results['success']} ({results['success']/results['total']*100:.1f}%)\n")
            f.write(f"  ✗ Failed: {results['failed']} ({results['failed']/results['total']*100:.1f}%)\n")
            f.write(f"  📝 Database Updated: {results['updated_db']} ({results['updated_db']/results['total']*100:.1f}%)\n")
            f.write("\n")
            
            # Categorize results
            successful = [r for r in results['results'] if r['status'] == 'success']
            not_found = [r for r in results['results'] if r['status'] == 'not_found']
            processing_failed = [r for r in results['results'] if r['status'] == 'processing_failed']
            errors = [r for r in results['results'] if r['status'] == 'error']
            
            # Successful Downloads
            f.write("="*80 + "\n")
            f.write(f"✓ SUCCESSFULLY PROCESSED ({len(successful)} papers)\n")
            f.write("="*80 + "\n")
            if successful:
                for i, result in enumerate(successful, 1):
                    f.write(f"{i:4d}. PMID: {result['pmid']} | DOI: {result['doi']}\n")
                    f.write(f"       Identifier: {result['identifier']}\n")
                    f.write(f"       PDF: {result['pdf_path']}\n")
                    f.write(f"       DB Updated: {result.get('updated_db', False)}\n")
                    f.write("\n")
            else:
                f.write("None\n\n")
            
            # Not Found on Sci-Hub
            f.write("="*80 + "\n")
            f.write(f"✗ NOT FOUND ON SCI-HUB ({len(not_found)} papers)\n")
            f.write("="*80 + "\n")
            if not_found:
                for i, result in enumerate(not_found, 1):
                    f.write(f"{i:4d}. PMID: {result['pmid']} | DOI: {result['doi']}\n")
                    f.write(f"       Identifier: {result['identifier']}\n")
                    f.write("\n")
            else:
                f.write("None\n\n")
            
            # Processing Failed
            f.write("="*80 + "\n")
            f.write(f"⚠ DOWNLOADED BUT PROCESSING FAILED ({len(processing_failed)} papers)\n")
            f.write("="*80 + "\n")
            if processing_failed:
                for i, result in enumerate(processing_failed, 1):
                    f.write(f"{i:4d}. PMID: {result['pmid']} | DOI: {result['doi']}\n")
                    f.write(f"       Identifier: {result['identifier']}\n")
                    f.write(f"       PDF: {result['pdf_path']}\n")
                    f.write("\n")
            else:
                f.write("None\n\n")
            
            # Errors
            if errors:
                f.write("="*80 + "\n")
                f.write(f"⚠ PROCESSING ERRORS ({len(errors)} papers)\n")
                f.write("="*80 + "\n")
                for i, result in enumerate(errors, 1):
                    f.write(f"{i:4d}. PMID: {result['pmid']} | DOI: {result['doi']}\n")
                    f.write(f"       Identifier: {result['identifier']}\n")
                    if 'error' in result:
                        f.write(f"       Error: {result['error']}\n")
                    f.write("\n")
            
            # Footer
            f.write("="*80 + "\n")
            f.write("END OF REPORT\n")
            f.write("="*80 + "\n")
        
        logger.info(f"Report saved to: {output_file}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Fetch missing papers from database using Sci-Hub + GROBID',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze database and show statistics
  python fetch_missing_db_papers.py /path/to/papers.db --analyze-only
  
  # Process first 10 missing papers
  python fetch_missing_db_papers.py /path/to/papers.db --limit 10
  
  # Process all missing papers with 8 workers
  python fetch_missing_db_papers.py /path/to/papers.db -w 8
  
  # Process with custom output directory
  python fetch_missing_db_papers.py /path/to/papers.db -o ./downloaded_papers -w 4

Note:
  - This script uses GROBID for PDF processing (ensure GROBID server is running)
  - Recommended: 2-4 workers for GROBID processing
  - The database will be updated with extracted abstract and full_text
        """
    )
    
    # Required arguments
    parser.add_argument('database', help='Path to papers.db database file')
    
    # Processing options
    parser.add_argument('-w', '--workers', type=int, default=4,
                       help='Number of parallel workers (default: 4)')
    parser.add_argument('-l', '--limit', type=int, default=None,
                       help='Maximum number of papers to process (default: all)')
    parser.add_argument('--analyze-only', action='store_true',
                       help='Only analyze and show statistics, do not process')
    
    # Output options
    parser.add_argument('-o', '--output', help='Output directory for downloaded papers')
    parser.add_argument('-c', '--config', help='Path to GROBID configuration file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check if database exists
    if not os.path.exists(args.database):
        print(f"Error: Database file not found: {args.database}")
        return 1
    
    print("\nOutput Directories:")
    pdf_dir = args.output or os.path.join(os.getcwd(), 'papers_missing')
    processed_dir = os.path.join(os.getcwd(), 'output')
    print(f"  PDFs: {pdf_dir}")
    print(f"  GROBID JSON/TEI: {processed_dir}")
    print(f"  Logs: {os.path.join(os.getcwd(), 'logs')}\n")
    
    # Create fetcher instance
    fetcher = MissingPapersFetcher(
        db_path=args.database,
        output_dir=args.output,
        config_path=args.config,
        max_workers=args.workers
    )
    
    # Analyze database
    print(f"\n{'='*60}")
    print(f"Analyzing database: {args.database}")
    print(f"{'='*60}\n")
    
    stats = fetcher.analyze_missing_papers()
    
    print(f"Database Statistics:")
    print(f"  Total Papers: {stats['total_papers']}")
    print(f"  Missing Full Text: {stats['missing_full_text']} ({stats['missing_full_text']/stats['total_papers']*100:.1f}%)")
    print(f"  Missing Abstract: {stats['missing_abstract']} ({stats['missing_abstract']/stats['total_papers']*100:.1f}%)")
    print(f"  Missing Either: {stats['missing_either']} ({stats['missing_either']/stats['total_papers']*100:.1f}%)")
    print(f"  Missing Both: {stats['missing_both']} ({stats['missing_both']/stats['total_papers']*100:.1f}%)")
    print()
    
    # If analyze-only, exit here
    if args.analyze_only:
        print("Analysis complete (--analyze-only flag set)")
        return 0
    
    # Process missing papers
    results = fetcher.process_missing_papers(limit=args.limit)
    
    # Save report
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    report_filename = f"missing_papers_report_{timestamp}.txt"
    report_path = os.path.join(fetcher.logs_dir, report_filename)
    
    fetcher.save_report(results, stats, report_path)
    
    print(f"\n📄 Report saved: {report_path}")
    print(f"   View with: cat {report_path}\n")
    
    # Exit with appropriate code
    return 0 if results['failed'] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
