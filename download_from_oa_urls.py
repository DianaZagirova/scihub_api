#!/usr/bin/env python3
"""
Download PDFs from Open Access URLs when Sci-Hub fails.
Fallback to oa_url from database for failed Sci-Hub downloads.
"""

import os
import sys
import time
import sqlite3
import logging
import requests
import argparse
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from doi_tracker_db import DOITracker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
OUTPUT_DIR = './output'
PDF_DIR = './papers'

class OADownloader:
    """Download PDFs from Open Access URLs."""
    
    def __init__(self, db_path, output_dir, pdf_dir, tracker):
        """Initialize downloader."""
        self.db_path = db_path
        self.output_dir = output_dir
        self.pdf_dir = pdf_dir
        self.tracker = tracker
        
        # Create directories
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(pdf_dir, exist_ok=True)
        
        # Session for requests with better headers to avoid 403
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept': 'application/pdf,application/octet-stream,*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
    
    def get_failed_dois_with_oa_urls(self):
        """
        Get DOIs where:
        1. Have DOI and oa_url
        2. Missing full_text OR abstract in database
        3. Failed by Sci-Hub OR not tried yet
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get DOIs with oa_url that are missing content (full_text or abstract)
        cursor.execute("""
            SELECT doi, oa_url, full_text, abstract
            FROM papers 
            WHERE doi IS NOT NULL 
            AND doi != ''
            AND oa_url IS NOT NULL 
            AND oa_url != ''
            AND (
                full_text IS NULL OR full_text = '' 
                OR abstract IS NULL OR abstract = ''
            )
        """)
        
        all_with_oa = cursor.fetchall()
        conn.close()
        
        logger.info(f"Found {len(all_with_oa)} papers with oa_url and missing content in database")
        
        # Filter for those that failed/weren't tried on Sci-Hub
        failed_dois = []
        for doi, oa_url, full_text, abstract in all_with_oa:
            status = self.tracker.get_status(doi)
            
            if not status:
                # Not in tracker - not tried yet, add it
                failed_dois.append((doi, oa_url))
                logger.debug(f"Adding {doi}: not in tracker")
            else:
                downloaded = status.get('downloaded', '')
                scihub_available = status.get('scihub_available', '')
                
                # Only add if:
                # 1. Sci-Hub failed (scihub_available='no')
                # 2. OR not downloaded yet (downloaded != 'yes')
                # 3. OR Sci-Hub unknown (scihub_available='unknown')
                if scihub_available == 'no':
                    # Sci-Hub explicitly failed
                    failed_dois.append((doi, oa_url))
                    logger.debug(f"Adding {doi}: Sci-Hub unavailable")
                elif scihub_available == 'unknown' and downloaded != 'yes':
                    # Not tried on Sci-Hub yet
                    failed_dois.append((doi, oa_url))
                    logger.debug(f"Adding {doi}: Not tried on Sci-Hub yet")
                elif downloaded != 'yes':
                    # Download attempted but failed
                    failed_dois.append((doi, oa_url))
                    logger.debug(f"Adding {doi}: Download failed")
        
        logger.info(f"Found {len(failed_dois)} papers to try downloading from OA URLs")
        logger.info(f"  (Papers missing content AND failed/not-tried on Sci-Hub)")
        return failed_dois
    
    def download_pdf_from_oa_url(self, doi, oa_url):
        """
        Download PDF from Open Access URL.
        
        Returns:
            str: Path to downloaded PDF, or None if failed
        """
        try:
            # Clean DOI for filename
            safe_doi = doi.replace('/', '_')
            pdf_path = os.path.join(self.pdf_dir, f"{safe_doi}.pdf")
            
            # Skip if already exists
            if os.path.exists(pdf_path):
                logger.debug(f"PDF already exists: {pdf_path}")
                return pdf_path
            
            # Download with timeout and follow redirects
            logger.info(f"Downloading from OA URL: {oa_url}")
            
            # Some publishers need Referer header
            headers = {'Referer': oa_url.rsplit('/', 1)[0] if '/' in oa_url else oa_url}
            
            response = self.session.get(oa_url, timeout=30, allow_redirects=True, headers=headers)
            
            if response.status_code != 200:
                logger.warning(f"Failed to download (status {response.status_code}): {oa_url}")
                return None
            
            # Check if it's a PDF
            content_type = response.headers.get('Content-Type', '').lower()
            if 'pdf' not in content_type and not response.content.startswith(b'%PDF'):
                logger.warning(f"Downloaded content is not a PDF: {content_type}")
                return None
            
            # Save PDF
            with open(pdf_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Successfully downloaded PDF: {pdf_path}")
            return pdf_path
            
        except requests.Timeout:
            logger.warning(f"Timeout downloading from: {oa_url}")
            return None
        except Exception as e:
            logger.error(f"Error downloading from {oa_url}: {e}")
            return None
    
    def parse_with_pymupdf(self, pdf_path, doi):
        """Parse PDF with PyMuPDF (fast parser)."""
        try:
            from src.fast_pdf_parser import FastPDFParser
            
            parser = FastPDFParser()
            result = parser.process_pdf(pdf_path, mode='structured')
            
            if result and result.get('metadata'):
                # Save to JSON
                safe_doi = doi.replace('/', '_')
                json_path = os.path.join(self.output_dir, f"{safe_doi}_fast.json")
                
                import json
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                
                logger.info(f"PyMuPDF parsing successful: {json_path}")
                return True
            else:
                logger.warning(f"PyMuPDF parsing returned no data")
                return False
                
        except Exception as e:
            logger.error(f"PyMuPDF parsing error: {e}")
            return False
    
    def parse_with_grobid(self, pdf_path, doi):
        """Parse PDF with Grobid."""
        try:
            from src.grobid_parser import GrobidParser
            
            parser = GrobidParser()
            result = parser.process_and_save(pdf_path, output_dir=self.output_dir)
            
            if result:
                logger.info(f"Grobid parsing successful")
                return True
            else:
                logger.warning(f"Grobid parsing failed")
                return False
                
        except Exception as e:
            logger.error(f"Grobid parsing error: {e}")
            return False
    
    def process_doi(self, doi, oa_url, parse_pymupdf=True, parse_grobid=False):
        """
        Process a single DOI: download from OA URL and parse.
        
        Returns:
            dict: Result summary
        """
        result = {
            'doi': doi,
            'oa_url': oa_url,
            'download_success': False,
            'pymupdf_success': False,
            'grobid_success': False,
            'error': None
        }
        
        try:
            # Download PDF
            pdf_path = self.download_pdf_from_oa_url(doi, oa_url)
            
            if not pdf_path:
                result['error'] = 'Download failed'
                # Mark as failed in tracker
                self.tracker.update_status(
                    doi,
                    scihub_available='no',  # OA URL also failed
                    downloaded='no'
                )
                return result
            
            result['download_success'] = True
            
            # Update tracker - downloaded successfully
            self.tracker.mark_downloaded(doi, success=True)
            
            # Parse with PyMuPDF
            if parse_pymupdf:
                pymupdf_success = self.parse_with_pymupdf(pdf_path, doi)
                result['pymupdf_success'] = pymupdf_success
                
                if pymupdf_success:
                    self.tracker.mark_pymupdf_processed(doi, success=True)
                else:
                    self.tracker.mark_pymupdf_processed(doi, success=False)
            
            # Parse with Grobid
            if parse_grobid:
                grobid_success = self.parse_with_grobid(pdf_path, doi)
                result['grobid_success'] = grobid_success
                
                if grobid_success:
                    self.tracker.mark_grobid_processed(doi, success=True)
                else:
                    self.tracker.mark_grobid_processed(doi, success=False)
            
        except Exception as e:
            logger.error(f"Error processing {doi}: {e}")
            result['error'] = str(e)
        
        return result
    
    def batch_process(self, max_workers=4, parse_pymupdf=True, parse_grobid=False, limit=None):
        """
        Process multiple DOIs in parallel.
        
        Args:
            max_workers: Number of parallel workers
            parse_pymupdf: Parse with PyMuPDF
            parse_grobid: Parse with Grobid
            limit: Limit number of papers to process (for testing)
        """
        # Get DOIs to process
        dois_to_process = self.get_failed_dois_with_oa_urls()
        
        if not dois_to_process:
            logger.info("No DOIs to process")
            return []
        
        if limit:
            dois_to_process = dois_to_process[:limit]
            logger.info(f"Limited to {limit} DOIs for processing")
        
        logger.info(f"Starting batch processing with {max_workers} workers")
        
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks
            future_to_doi = {
                executor.submit(
                    self.process_doi, 
                    doi, 
                    oa_url,
                    parse_pymupdf,
                    parse_grobid
                ): doi 
                for doi, oa_url in dois_to_process
            }
            
            # Process results with progress bar
            for future in tqdm(as_completed(future_to_doi), total=len(future_to_doi), desc="Processing"):
                doi = future_to_doi[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Small delay to avoid overwhelming servers
                    time.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Exception processing {doi}: {e}")
                    results.append({
                        'doi': doi,
                        'error': str(e),
                        'download_success': False
                    })
        
        # Flush tracker updates
        self.tracker.flush()
        
        # Summary
        success_count = sum(1 for r in results if r['download_success'])
        pymupdf_count = sum(1 for r in results if r.get('pymupdf_success'))
        grobid_count = sum(1 for r in results if r.get('grobid_success'))
        
        logger.info("\n" + "="*70)
        logger.info("PROCESSING SUMMARY")
        logger.info("="*70)
        logger.info(f"Total processed: {len(results)}")
        logger.info(f"Download success: {success_count}")
        logger.info(f"PyMuPDF parsed: {pymupdf_count}")
        logger.info(f"Grobid parsed: {grobid_count}")
        logger.info("="*70)
        
        return results


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Download PDFs from Open Access URLs when Sci-Hub fails'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )
    parser.add_argument(
        '--pymupdf',
        action='store_true',
        default=True,
        help='Parse with PyMuPDF (default: True)'
    )
    parser.add_argument(
        '--grobid',
        action='store_true',
        help='Also parse with Grobid (default: False)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of papers to process (for testing)'
    )
    parser.add_argument(
        '--analyze-only',
        action='store_true',
        help='Only analyze how many papers need OA download'
    )
    
    args = parser.parse_args()
    
    # Load tracker
    logger.info("Loading DB-backed tracker...")
    tracker = DOITracker('processing_tracker.db')
    
    # Initialize downloader
    downloader = OADownloader(
        db_path=DB_PATH,
        output_dir=OUTPUT_DIR,
        pdf_dir=PDF_DIR,
        tracker=tracker
    )
    
    if args.analyze_only:
        # Just count how many need OA download
        dois = downloader.get_failed_dois_with_oa_urls()
        logger.info(f"\n{len(dois)} papers can be downloaded from OA URLs")
        return 0
    
    # Process DOIs
    results = downloader.batch_process(
        max_workers=args.workers,
        parse_pymupdf=args.pymupdf,
        parse_grobid=args.grobid,
        limit=args.limit
    )
    
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logger.info("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
