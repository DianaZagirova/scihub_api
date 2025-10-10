#!/usr/bin/env python3
"""
Sci-Hub Paper Downloader using scihub.py API
--------------------------------------------
A tool to download academic papers from Sci-Hub using DOIs via the scihub.py library.
"""

import os
import sys
import re
import time
import random
import logging
import argparse
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Add the current directory to the path to import the local scihub.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from scihub import SciHub
except ImportError as e:
    logger.error(f"Error importing scihub.py: {e}")
    sys.exit(1)

class SciHubAPIDownloader:
    """Class to handle downloading papers from Sci-Hub using scihub.py API."""
    
    def __init__(self, output_dir=None):
        """
        Initialize the SciHub API downloader.
        
        Args:
            output_dir (str): Directory to save downloaded papers
        """
        self.output_dir = output_dir or os.path.join(os.getcwd(), 'papers')
        self.sh = SciHub()
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
    
    def normalize_doi(self, doi):
        """
        Normalize a DOI string by removing prefixes and cleaning it.
        
        Args:
            doi (str): DOI string to normalize
            
        Returns:
            str: Normalized DOI or None if invalid
        """
        if not doi:
            return None
            
        # Remove common prefixes and handle more edge cases
        prefixes = [
            'doi:', 'doi.org/', 'dx.doi.org/', 
            'http://dx.doi.org/', 'https://dx.doi.org/', 
            'http://doi.org/', 'https://doi.org/', 
            'DOI:', 'https://doi.org/doi:', 'doi: ',
            'https://www.doi.org/', 'http://www.doi.org/',
            'www.doi.org/', 'doi.org/doi:'
        ]
        
        normalized = doi.strip()
        
        # Convert to lowercase for case-insensitive matching
        normalized_lower = normalized.lower()
        
        # Try each prefix
        for prefix in prefixes:
            if normalized_lower.startswith(prefix.lower()):
                normalized = normalized[len(prefix):].strip()
                break
                
        # Handle URL parameters if present
        if '?' in normalized:
            normalized = normalized.split('?')[0]
            
        # Remove any trailing punctuation or whitespace
        normalized = normalized.rstrip('.,;: ')
        
        # Return None if we don't have a valid-looking DOI after normalization
        if not normalized or not normalized.startswith('10.'):
            return None
            
        return normalized
    
    def validate_doi(self, doi):
        """
        Validate if the provided string is a valid DOI.
        
        Args:
            doi (str): DOI to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        # First normalize the DOI
        normalized_doi = self.normalize_doi(doi)
        if not normalized_doi:
            return False
            
        # DOI regex pattern (comprehensive)
        # Format: 10.NNNN/suffix where NNNN is 4+ digits and suffix can contain various characters
        # Based on CrossRef and DataCite DOI patterns
        doi_pattern = r'^10\.\d{4,9}/[-._;()/:A-Z0-9]+[-._;()/:A-Z0-9]*$'
        
        # Clean the DOI
        normalized_doi = normalized_doi.strip().upper()
        
        # Check if it matches the pattern
        if re.match(doi_pattern, normalized_doi, re.IGNORECASE):
            return True
        return False
    
    def download_paper(self, identifier):
        """
        Download a paper from Sci-Hub using its DOI, PMID, or URL.
        
        Args:
            identifier (str): DOI, PMID, or URL of the paper to download
            
        Returns:
            str: Path to the downloaded file or None if failed
        """
        try:
            # Check if the identifier is a DOI and validate it
            is_doi = False
            normalized_doi = None
            
            if not identifier.startswith(('http://', 'https://')):
                normalized_doi = self.normalize_doi(identifier)
                if normalized_doi and self.validate_doi(normalized_doi):
                    is_doi = True
                    identifier = normalized_doi  # Use the normalized DOI for download
                    logger.info(f"Valid DOI detected: {normalized_doi}")
            
            # Generate filename from identifier
            if is_doi:
                # It's a validated DOI
                filename = normalized_doi.replace('/', '_') + '.pdf'
            elif identifier.startswith(('http://', 'https://')):
                # It's a URL
                parsed_url = urlparse(identifier)
                path_parts = parsed_url.path.split('/')
                
                # Try to extract DOI from URL if possible
                for part in path_parts:
                    potential_doi = self.normalize_doi(part)
                    if potential_doi and self.validate_doi(potential_doi):
                        filename = potential_doi.replace('/', '_') + '.pdf'
                        break
                else:  # No DOI found in URL
                    filename = path_parts[-1] if path_parts and path_parts[-1] else f"paper_{hash(identifier) % 10000}.pdf"
                    if not filename or '.' not in filename:
                        filename = f"paper_{hash(identifier) % 10000}.pdf"
            else:
                # Assume it's a PMID or other identifier
                filename = f"paper_{identifier.replace('/', '_')}.pdf"
            
            filepath = os.path.join(self.output_dir, filename)
            
            # Check if file already exists
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                logger.info(f"Paper already exists: {filepath}. Skipping download.")
                return filepath
            
            logger.info(f"Attempting to download {identifier}")
            
            # Download the paper
            result = self.sh.download(identifier, path=filepath)
            
            if result['err']:
                logger.error(f"Error downloading {identifier}: {result['err']}")
                return None
                
            # Verify the file was saved and has content
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                logger.info(f"Successfully downloaded paper to: {filepath}")
                return filepath
            else:
                logger.error("File was not saved properly or is empty")
                if os.path.exists(filepath):
                    os.remove(filepath)  # Remove empty or corrupt file
                return None
            
        except Exception as e:
            logger.error(f"Unexpected error downloading {identifier}: {e}")
            return None
    
    def search_and_download(self, query, limit=5):
        """
        Search for papers on Google Scholar and download them from Sci-Hub.
        
        Args:
            query (str): Search query
            limit (int): Maximum number of papers to download
            
        Returns:
            list: Paths to the downloaded files
        """
        try:
            logger.info(f"Searching for '{query}' (limit: {limit})")
            results = self.sh.search(query, limit)
            
            if not results or 'papers' not in results or not results['papers']:
                logger.warning(f"No papers found for query: {query}")
                return []
            
            downloaded_paths = []
            for i, paper in enumerate(results['papers']):
                logger.info(f"Processing paper {i+1}/{len(results['papers'])}: {paper.get('title', 'Unknown title')}")
                
                # Try to download using URL
                filepath = self.download_paper(paper['url'])
                
                if filepath:
                    downloaded_paths.append(filepath)
                
                # Add a small delay between requests
                if i < len(results['papers']) - 1:
                    delay = random.uniform(1, 3)
                    logger.debug(f"Waiting {delay:.2f} seconds before next request...")
                    time.sleep(delay)
            
            return downloaded_paths
            
        except Exception as e:
            logger.error(f"Error during search and download: {e}")
            return []

def main():
    """Main function to handle command line interface."""
    parser = argparse.ArgumentParser(description='Download papers from Sci-Hub using DOIs via scihub.py API')
    parser.add_argument('identifiers', nargs='*', help='DOIs, PMIDs, or URLs to download')
    parser.add_argument('-f', '--file', help='File containing identifiers (one per line)')
    parser.add_argument('-s', '--search', help='Search query for Google Scholar')
    parser.add_argument('-l', '--limit', type=int, default=5, help='Maximum number of search results to download')
    parser.add_argument('-o', '--output', help='Output directory for downloaded papers')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Initialize downloader
    downloader = SciHubAPIDownloader(output_dir=args.output)
    
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
    
    # Process search query if provided
    if args.search:
        logger.info(f"Searching for: {args.search}")
        downloaded_paths = downloader.search_and_download(args.search, args.limit)
        if downloaded_paths:
            logger.info(f"Search and download complete: {len(downloaded_paths)} papers downloaded")
        else:
            logger.warning("No papers downloaded from search results")
        
        # If we only had a search query and no identifiers, we're done
        if not identifiers:
            return 0
    
    # Check if we have any identifiers to process
    if not identifiers:
        logger.error("No identifiers provided. Use command line arguments, a file, or a search query.")
        parser.print_help()
        return 1
    
    # Process each identifier
    success_count = 0
    for i, identifier in enumerate(identifiers):
        logger.info(f"Processing identifier {i+1}/{len(identifiers)}: {identifier}")
        result = downloader.download_paper(identifier)
        
        if result:
            success_count += 1
        
        # Add a small delay between requests
        if i < len(identifiers) - 1:
            delay = random.uniform(1, 3)
            logger.debug(f"Waiting {delay:.2f} seconds before next request...")
            time.sleep(delay)
    
    # Print summary
    logger.info(f"Download complete: {success_count}/{len(identifiers)} papers downloaded successfully")
    return 0

if __name__ == "__main__":
    sys.exit(main())
