#!/usr/bin/env python3
"""
Sci-Hub Paper Downloader with GROBID Integration
------------------------------------------------
A tool to download academic papers from Sci-Hub using DOIs and process them with GROBID.
"""

import os
import sys
import time
import random
import logging
import argparse
import datetime
from scihub_downloader import SciHubDownloader
from grobid_parser import GrobidParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class SciHubGrobidDownloader:
    """Class to handle downloading papers from Sci-Hub and processing them with GROBID."""
    
    def __init__(self, output_dir=None, skip_existing=True, log_failed=True, config_path=None):
        """
        Initialize the SciHub downloader with GROBID integration.
        
        Args:
            output_dir (str): Directory to save downloaded papers
            skip_existing (bool): Whether to skip downloading papers that already exist
            log_failed (bool): Whether to log failed DOIs to a file
            config_path (str): Path to the configuration file
        """
        self.output_dir = output_dir or os.path.join(os.getcwd(), 'papers')
        self.skip_existing = skip_existing
        self.log_failed = log_failed
        self.config_path = config_path
        
        # Initialize the SciHub downloader
        self.downloader = SciHubDownloader(
            output_dir=self.output_dir,
            skip_existing=self.skip_existing,
            log_failed=self.log_failed
        )
        
        # Initialize the GROBID parser
        self.parser = GrobidParser(config_path=self.config_path)
        
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
        self.processing_failed_log = os.path.join(self.logs_dir, f"grobid_processing_failed_{timestamp}.log")
        self.success_log = os.path.join(self.logs_dir, f"grobid_success_{timestamp}.log")
        
        # Initialize log files with headers
        if self.log_failed:
            with open(self.not_found_log, 'w') as f:
                f.write(f"# Papers Not Found on Sci-Hub - Created at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("# Format: [Timestamp] DOI - Reason\n\n")
            
            with open(self.processing_failed_log, 'w') as f:
                f.write(f"# Papers Downloaded but Failed GROBID Processing - Created at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("# Format: [Timestamp] DOI - PDF Path - Error\n\n")
            
            with open(self.success_log, 'w') as f:
                f.write(f"# Successfully Processed Papers with GROBID - Created at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
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
    
    def download_and_process(self, identifier):
        """
        Download a paper from Sci-Hub and process it with GROBID.
        
        Args:
            identifier (str): DOI, PMID, or title of the paper to download
            
        Returns:
            tuple: (pdf_path, extracted_data, status) where status is 'success', 'not_found', or 'processing_failed'
        """
        # Download the paper
        pdf_path = self.downloader.download_paper(identifier)
        
        if not pdf_path:
            logger.error(f"Failed to download paper with identifier: {identifier}")
            self.log_entry(self.not_found_log, identifier, "Not found on Sci-Hub or download failed")
            return None, None, 'not_found'
        
        logger.info(f"Successfully downloaded paper to: {pdf_path}")
        
        # Process the paper with GROBID
        try:
            extracted_data = self.parser.process_and_save(pdf_path, output_dir=self.processed_dir)
            
            if extracted_data:
                logger.info(f"Successfully processed paper with GROBID: {identifier}")
                self.log_entry(self.success_log, identifier, pdf_path)
                return pdf_path, extracted_data, 'success'
            else:
                logger.error(f"Failed to process paper with GROBID: {identifier}")
                self.log_entry(self.processing_failed_log, identifier, f"{pdf_path} - GROBID processing returned no data")
                return pdf_path, None, 'processing_failed'
                
        except Exception as e:
            logger.error(f"Error processing paper with GROBID: {e}")
            self.log_entry(self.processing_failed_log, identifier, f"{pdf_path} - {str(e)}")
            return pdf_path, None, 'processing_failed'
    
    def batch_download_and_process(self, identifiers):
        """
        Download and process multiple papers.
        
        Args:
            identifiers (list): List of DOIs, PMIDs, or titles to download and process
            
        Returns:
            list: List of results with status
        """
        results = []
        
        for i, identifier in enumerate(identifiers):
            # Calculate and display progress
            progress = (i / len(identifiers)) * 100 if len(identifiers) > 0 else 0
            
            # Log the current identifier being processed
            logger.info(f"Processing identifier {i+1}/{len(identifiers)} ({progress:.1f}%): {identifier}")
            
            # Download and process the paper
            pdf_path, extracted_data, status = self.download_and_process(identifier)
            
            # Record the result
            result = {
                'identifier': identifier,
                'pdf_path': pdf_path,
                'processed': extracted_data is not None,
                'status': status,
                'metadata': extracted_data.get('metadata', {}) if extracted_data else None
            }
            
            results.append(result)
            
            # Add a small delay between requests to avoid overloading the servers
            if i < len(identifiers) - 1:
                delay = random.uniform(1, 3)
                logger.debug(f"Waiting {delay:.2f} seconds before next request...")
                time.sleep(delay)
        
        # Print summary
        success_count = sum(1 for r in results if r['status'] == 'success')
        not_found_count = sum(1 for r in results if r['status'] == 'not_found')
        processing_failed_count = sum(1 for r in results if r['status'] == 'processing_failed')
        
        logger.info(f"\n=== Processing Summary ===")
        logger.info(f"Total identifiers: {len(identifiers)}")
        logger.info(f"Successfully processed: {success_count}")
        logger.info(f"Not found on Sci-Hub: {not_found_count}")
        logger.info(f"Downloaded but failed GROBID processing: {processing_failed_count}")
        logger.info(f"\nLog files created in: {self.logs_dir}")
        logger.info(f"  - Not found: {os.path.basename(self.not_found_log)}")
        logger.info(f"  - Processing failed: {os.path.basename(self.processing_failed_log)}")
        logger.info(f"  - Success: {os.path.basename(self.success_log)}")
        
        return results
    
    def process_existing_papers(self, pdf_dir=None):
        """
        Process existing PDF papers with GROBID.
        
        Args:
            pdf_dir (str): Directory containing PDF files to process
            
        Returns:
            list: List of processing results
        """
        if not pdf_dir:
            pdf_dir = self.output_dir
        
        logger.info(f"Processing existing papers in {pdf_dir}")
        
        # Process the papers with GROBID
        results = self.parser.batch_process(pdf_dir, output_dir=self.processed_dir)
        
        return results

def main():
    """Main function to handle command line interface."""
    parser = argparse.ArgumentParser(description='Download papers from Sci-Hub using DOI, PMID, or title and process with GROBID')
    parser.add_argument('identifiers', nargs='*', help='DOIs, PMIDs, or titles to download and process')
    parser.add_argument('-f', '--file', help='File containing identifiers (one per line)')
    parser.add_argument('-o', '--output', help='Output directory for downloaded papers')
    parser.add_argument('-p', '--process-only', action='store_true', help='Process existing papers only (no download)')
    parser.add_argument('-c', '--config', help='Path to configuration file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Initialize downloader with GROBID integration
    downloader = SciHubGrobidDownloader(output_dir=args.output, config_path=args.config)
    
    # Process existing papers only
    if args.process_only:
        results = downloader.process_existing_papers()
        success_count = sum(1 for r in results if r['status'] == 'success')
        logger.info(f"Processed {len(results)} existing papers: {success_count} succeeded, {len(results) - success_count} failed")
        return 0
    
    # Collect identifiers
    identifiers = []
    
    # Add identifiers from command line
    if args.identifiers:
        identifiers.extend(args.identifiers)
    
    # Add identifiers from file
    if args.file:
        try:
            with open(args.file, 'r') as f:
                file_identifiers = [line.strip() for line in f if line.strip()]
                identifiers.extend(file_identifiers)
        except Exception as e:
            logger.error(f"Error reading identifier file: {e}")
            return 1
    
    # Check if we have any identifiers to process
    if not identifiers:
        logger.error("No identifiers provided. Use command line arguments or a file.")
        parser.print_help()
        return 1
    
    # Validate identifiers
    valid_identifiers = []
    for identifier in identifiers:
        id_type = downloader.downloader.detect_identifier_type(identifier)
        is_valid = False
        
        if id_type == 'doi':
            normalized = downloader.downloader.normalize_doi(identifier)
            is_valid = downloader.downloader.validate_doi(normalized)
        elif id_type == 'pmid':
            normalized = downloader.downloader.normalize_pmid(identifier)
            is_valid = downloader.downloader.validate_pmid(normalized)
        else:  # title
            is_valid = bool(identifier.strip())
        
        if is_valid:
            valid_identifiers.append(identifier)
        elif identifier:
            logger.warning(f"Skipping invalid identifier: {identifier}")
    
    # Check if we have any valid identifiers to process
    if not valid_identifiers:
        logger.error("No valid identifiers found. Please check your input.")
        return 1
    
    # Download and process papers
    start_time = time.time()
    results = downloader.batch_download_and_process(valid_identifiers)
    
    # Print summary
    total_time = time.time() - start_time
    minutes, seconds = divmod(total_time, 60)
    
    success_count = sum(1 for r in results if r['status'] == 'success')
    not_found_count = sum(1 for r in results if r['status'] == 'not_found')
    processing_failed_count = sum(1 for r in results if r['status'] == 'processing_failed')
    
    print(f"\n{'='*50}")
    print(f"Download and processing complete in {int(minutes)}m {int(seconds)}s")
    print(f"{'='*50}")
    print(f"Total identifiers processed: {len(valid_identifiers)}")
    print(f"  ✓ Successfully processed: {success_count}")
    print(f"  ✗ Not found on Sci-Hub: {not_found_count}")
    print(f"  ⚠ Downloaded but failed GROBID processing: {processing_failed_count}")
    print(f"{'='*50}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
