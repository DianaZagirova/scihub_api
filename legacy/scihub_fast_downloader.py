#!/usr/bin/env python3
"""
Sci-Hub Paper Downloader with Fast PDF Parser Integration
----------------------------------------------------------
A tool to download academic papers from Sci-Hub using DOIs and process them with fast PDF parser.
"""

import os
import sys
import time
import random
import logging
import argparse
import datetime
from scihub_downloader import SciHubDownloader
from fast_pdf_parser import FastPDFParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class SciHubFastDownloader:
    """Class to handle downloading papers from Sci-Hub and processing them with fast PDF parser."""
    
    def __init__(self, output_dir=None, skip_existing=True, log_failed=True, parse_mode='structured'):
        """
        Initialize the SciHub downloader with fast PDF parser integration.
        
        Args:
            output_dir (str): Directory to save downloaded papers
            skip_existing (bool): Whether to skip downloading papers that already exist
            log_failed (bool): Whether to log failed DOIs to a file
            parse_mode (str): PDF parsing mode ('simple', 'structured', or 'full')
        """
        self.output_dir = output_dir or os.path.join(os.getcwd(), 'papers')
        self.skip_existing = skip_existing
        self.log_failed = log_failed
        self.parse_mode = parse_mode
        
        # Initialize the SciHub downloader
        self.downloader = SciHubDownloader(
            output_dir=self.output_dir,
            skip_existing=self.skip_existing,
            log_failed=self.log_failed
        )
        
        # Initialize the fast PDF parser
        self.parser = FastPDFParser()
        
        # Create output directory for processed data if it doesn't exist
        self.processed_dir = os.path.join(os.getcwd(), 'output')
        if not os.path.exists(self.processed_dir):
            os.makedirs(self.processed_dir)
            logger.info(f"Created processed data directory: {self.processed_dir}")
        
        # Create separate log files for different failure types
        self.logs_dir = os.path.join(os.getcwd(), 'logs')
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.not_found_log = os.path.join(self.logs_dir, f"not_found_scihub_{timestamp}.log")
        self.processing_failed_log = os.path.join(self.logs_dir, f"processing_failed_{timestamp}.log")
        self.success_log = os.path.join(self.logs_dir, f"success_{timestamp}.log")
        
        # Initialize log files with headers
        if self.log_failed:
            with open(self.not_found_log, 'w') as f:
                f.write(f"# Papers Not Found on Sci-Hub - Created at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("# Format: [Timestamp] DOI - Reason\n\n")
            
            with open(self.processing_failed_log, 'w') as f:
                f.write(f"# Papers Downloaded but Failed Processing - Created at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("# Format: [Timestamp] DOI - PDF Path - Error\n\n")
            
            with open(self.success_log, 'w') as f:
                f.write(f"# Successfully Processed Papers - Created at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("# Format: [Timestamp] DOI - PDF Path\n\n")
            
            logger.info(f"Created log files in {self.logs_dir}")
    
    def log_entry(self, log_file, doi, message):
        """Log an entry to a specific log file."""
        if not self.log_failed:
            return
        
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {doi} - {message}\n"
        
        try:
            with open(log_file, 'a') as f:
                f.write(log_entry)
        except Exception as e:
            logger.error(f"Error writing to log file: {e}")
    
    def download_and_process(self, doi):
        """
        Download a paper from Sci-Hub and process it with fast PDF parser.
        
        Args:
            doi (str): DOI of the paper to download
            
        Returns:
            tuple: (pdf_path, extracted_data, status) where status is 'success', 'not_found', or 'processing_failed'
        """
        # Download the paper
        pdf_path = self.downloader.download_paper(doi)
        
        if not pdf_path:
            logger.error(f"Failed to download paper with DOI: {doi}")
            self.log_entry(self.not_found_log, doi, "Not found on Sci-Hub or download failed")
            return None, None, 'not_found'
        
        logger.info(f"Successfully downloaded paper to: {pdf_path}")
        
        # Process the paper with fast PDF parser
        try:
            extracted_data = self.parser.process_and_save(
                pdf_path, 
                mode=self.parse_mode,
                output_dir=self.processed_dir
            )
            
            if extracted_data:
                logger.info(f"Successfully processed paper with fast parser: {doi}")
                self.log_entry(self.success_log, doi, pdf_path)
                return pdf_path, extracted_data, 'success'
            else:
                logger.error(f"Failed to process paper with fast parser: {doi}")
                self.log_entry(self.processing_failed_log, doi, f"{pdf_path} - Processing returned no data")
                return pdf_path, None, 'processing_failed'
                
        except Exception as e:
            logger.error(f"Error processing paper with fast parser: {e}")
            self.log_entry(self.processing_failed_log, doi, f"{pdf_path} - {str(e)}")
            return pdf_path, None, 'processing_failed'
    
    def batch_download_and_process(self, dois):
        """
        Download and process multiple papers.
        
        Args:
            dois (list): List of DOIs to download and process
            
        Returns:
            list: List of results with status
        """
        results = []
        
        for i, doi in enumerate(dois):
            # Calculate and display progress
            progress = (i / len(dois)) * 100 if len(dois) > 0 else 0
            
            # Log the current DOI being processed
            logger.info(f"Processing DOI {i+1}/{len(dois)} ({progress:.1f}%): {doi}")
            
            # Download and process the paper
            pdf_path, extracted_data, status = self.download_and_process(doi)
            
            # Record the result
            result = {
                'doi': doi,
                'pdf_path': pdf_path,
                'processed': extracted_data is not None,
                'status': status,
                'metadata': extracted_data.get('metadata', {}) if extracted_data else None
            }
            
            results.append(result)
            
            # Add a small delay between requests to avoid overloading the servers
            if i < len(dois) - 1:
                delay = random.uniform(1, 3)
                logger.debug(f"Waiting {delay:.2f} seconds before next request...")
                time.sleep(delay)
        
        # Print summary
        success_count = sum(1 for r in results if r['status'] == 'success')
        not_found_count = sum(1 for r in results if r['status'] == 'not_found')
        processing_failed_count = sum(1 for r in results if r['status'] == 'processing_failed')
        
        logger.info(f"\n=== Processing Summary ===")
        logger.info(f"Total DOIs: {len(dois)}")
        logger.info(f"Successfully processed: {success_count}")
        logger.info(f"Not found on Sci-Hub: {not_found_count}")
        logger.info(f"Downloaded but failed processing: {processing_failed_count}")
        logger.info(f"\nLog files created in: {self.logs_dir}")
        logger.info(f"  - Not found: {os.path.basename(self.not_found_log)}")
        logger.info(f"  - Processing failed: {os.path.basename(self.processing_failed_log)}")
        logger.info(f"  - Success: {os.path.basename(self.success_log)}")
        
        return results
    
    def process_existing_papers(self, pdf_dir=None):
        """
        Process existing PDF papers with fast PDF parser.
        
        Args:
            pdf_dir (str): Directory containing PDF files to process
            
        Returns:
            list: List of processing results
        """
        if not pdf_dir:
            pdf_dir = self.output_dir
        
        logger.info(f"Processing existing papers in {pdf_dir}")
        
        # Process the papers with fast PDF parser
        results = self.parser.batch_process(pdf_dir, mode=self.parse_mode, output_dir=self.processed_dir)
        
        return results


def main():
    """Main function to handle command line interface."""
    parser = argparse.ArgumentParser(description='Download papers from Sci-Hub and process with fast PDF parser')
    parser.add_argument('dois', nargs='*', help='DOIs to download and process')
    parser.add_argument('-f', '--file', help='File containing DOIs (one per line)')
    parser.add_argument('-o', '--output', help='Output directory for downloaded papers')
    parser.add_argument('-p', '--process-only', action='store_true', help='Process existing papers only (no download)')
    parser.add_argument('-m', '--mode', choices=['simple', 'structured', 'full'], 
                       default='structured', help='PDF parsing mode')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Initialize downloader with fast PDF parser integration
    downloader = SciHubFastDownloader(output_dir=args.output, parse_mode=args.mode)
    
    # Process existing papers only
    if args.process_only:
        results = downloader.process_existing_papers()
        success_count = sum(1 for r in results if r['status'] == 'success')
        logger.info(f"Processed {len(results)} existing papers: {success_count} succeeded, {len(results) - success_count} failed")
        return 0
    
    # Collect DOIs
    dois = []
    
    # Add DOIs from command line
    if args.dois:
        dois.extend(args.dois)
    
    # Add DOIs from file
    if args.file:
        try:
            with open(args.file, 'r') as f:
                file_dois = [line.strip() for line in f if line.strip()]
                dois.extend(file_dois)
        except Exception as e:
            logger.error(f"Error reading DOI file: {e}")
            return 1
    
    # Check if we have any DOIs to process
    if not dois:
        logger.error("No DOIs provided. Use command line arguments or a file.")
        parser.print_help()
        return 1
    
    # Clean and filter DOIs
    cleaned_dois = []
    for doi in dois:
        normalized = downloader.downloader.normalize_doi(doi)
        if normalized and downloader.downloader.validate_doi(normalized):
            cleaned_dois.append(normalized)
        elif doi:  # Only log if doi is not empty
            logger.warning(f"Skipping invalid DOI: {doi}")
    
    # Check if we have any valid DOIs to process
    if not cleaned_dois:
        logger.error("No valid DOIs found. Please check your input.")
        return 1
    
    # Download and process papers
    start_time = time.time()
    results = downloader.batch_download_and_process(cleaned_dois)
    
    # Print summary
    total_time = time.time() - start_time
    minutes, seconds = divmod(total_time, 60)
    
    success_count = sum(1 for r in results if r['status'] == 'success')
    not_found_count = sum(1 for r in results if r['status'] == 'not_found')
    processing_failed_count = sum(1 for r in results if r['status'] == 'processing_failed')
    
    print(f"\n{'='*50}")
    print(f"Download and processing complete in {int(minutes)}m {int(seconds)}s")
    print(f"{'='*50}")
    print(f"Total DOIs processed: {len(cleaned_dois)}")
    print(f"  ✓ Successfully processed: {success_count}")
    print(f"  ✗ Not found on Sci-Hub: {not_found_count}")
    print(f"  ⚠ Downloaded but failed processing: {processing_failed_count}")
    print(f"{'='*50}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
