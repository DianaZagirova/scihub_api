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
    
    def download_and_process(self, doi):
        """
        Download a paper from Sci-Hub and process it with GROBID.
        
        Args:
            doi (str): DOI of the paper to download
            
        Returns:
            tuple: (pdf_path, extracted_data) or (None, None) if failed
        """
        # Download the paper
        pdf_path = self.downloader.download_paper(doi)
        
        if not pdf_path:
            logger.error(f"Failed to download paper with DOI: {doi}")
            return None, None
        
        logger.info(f"Successfully downloaded paper to: {pdf_path}")
        
        # Process the paper with GROBID
        try:
            extracted_data = self.parser.process_and_save(pdf_path, output_dir=self.processed_dir)
            
            if extracted_data:
                logger.info(f"Successfully processed paper with GROBID: {doi}")
                return pdf_path, extracted_data
            else:
                logger.error(f"Failed to process paper with GROBID: {doi}")
                return pdf_path, None
                
        except Exception as e:
            logger.error(f"Error processing paper with GROBID: {e}")
            return pdf_path, None
    
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
            pdf_path, extracted_data = self.download_and_process(doi)
            
            # Record the result
            result = {
                'doi': doi,
                'pdf_path': pdf_path,
                'processed': extracted_data is not None,
                'metadata': extracted_data.get('metadata', {}) if extracted_data else None
            }
            
            results.append(result)
            
            # Add a small delay between requests to avoid overloading the servers
            if i < len(dois) - 1:
                delay = random.uniform(1, 3)
                logger.debug(f"Waiting {delay:.2f} seconds before next request...")
                time.sleep(delay)
        
        # Print summary
        success_download = sum(1 for r in results if r['pdf_path'])
        success_process = sum(1 for r in results if r['processed'])
        
        logger.info(f"Download summary: {success_download}/{len(dois)} papers downloaded successfully")
        logger.info(f"Processing summary: {success_process}/{len(dois)} papers processed successfully")
        
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
    parser = argparse.ArgumentParser(description='Download papers from Sci-Hub and process with GROBID')
    parser.add_argument('dois', nargs='*', help='DOIs to download and process')
    parser.add_argument('-f', '--file', help='File containing DOIs (one per line)')
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
    
    success_download = sum(1 for r in results if r['pdf_path'])
    success_process = sum(1 for r in results if r['processed'])
    
    print(f"\nDownload and processing complete in {int(minutes)}m {int(seconds)}s")
    print(f"Download results: {success_download}/{len(cleaned_dois)} papers downloaded successfully")
    print(f"Processing results: {success_process}/{len(cleaned_dois)} papers processed with GROBID successfully")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
