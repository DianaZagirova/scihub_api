#!/usr/bin/env python3
"""
Sci-Hub Paper Downloader
------------------------
A tool to download academic papers from Sci-Hub using DOIs.
"""

import os
import re
import sys
import time
import random
import logging
import argparse
import datetime
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# List of potential Sci-Hub domains (these may change over time)
SCIHUB_DOMAINS = [
    'https://sci-hub.se',
    'https://sci-hub.st',
    'https://sci-hub.ru',
    # Add more domains as needed
]

class SciHubDownloader:
    """Class to handle downloading papers from Sci-Hub."""
    
    def __init__(self, output_dir=None, skip_existing=True, log_failed=True):
        """
        Initialize the SciHub downloader.
        
        Args:
            output_dir (str): Directory to save downloaded papers
            skip_existing (bool): Whether to skip downloading papers that already exist
            log_failed (bool): Whether to log failed DOIs to a file
        """
        self.output_dir = output_dir or os.path.join(os.getcwd(), 'papers')
        self.skip_existing = skip_existing
        self.log_failed = log_failed
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
            
        # Initialize failed DOIs list
        self.failed_dois = []
        
        # Create logs directory if it doesn't exist
        self.logs_dir = os.path.join(os.getcwd(), 'logs')
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)
            logger.info(f"Created logs directory: {self.logs_dir}")
        
        # Generate timestamp for log file
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.failed_log_file = os.path.join(self.logs_dir, f"failed_dois_{self.timestamp}.log")
        
        # Create the log file with header
        if self.log_failed:
            with open(self.failed_log_file, 'w') as f:
                f.write(f"# Failed DOIs Log - Created at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("# Format: [Timestamp] DOI - Reason\n\n")
            logger.info(f"Created failed DOIs log file: {self.failed_log_file}")
    
    def log_failed_doi(self, doi, reason):
        """
        Log a failed DOI to the log file with timestamp
        
        Args:
            doi (str): The DOI that failed
            reason (str): The reason for failure
        """
        if not self.log_failed:
            return
            
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {doi} - {reason}\n"
        
        try:
            with open(self.failed_log_file, 'a') as f:
                f.write(log_entry)
        except Exception as e:
            logger.error(f"Error writing to failed DOIs log file: {e}")
    
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
    
    def find_working_domain(self):
        """
        Find a working Sci-Hub domain.
        
        Returns:
            str: URL of a working domain or None if none found
        """
        for domain in SCIHUB_DOMAINS:
            try:
                logger.info(f"Trying domain: {domain}")
                response = self.session.get(domain, timeout=10)
                if response.status_code == 200:
                    logger.info(f"Found working domain: {domain}")
                    return domain
            except requests.RequestException as e:
                logger.warning(f"Domain {domain} failed: {e}")
        
        logger.error("No working Sci-Hub domains found")
        return None
    
    def download_paper(self, doi):
        """
        Download a paper from Sci-Hub using its DOI.
        
        Args:
            doi (str): DOI of the paper to download
            
        Returns:
            str: Path to the downloaded file or None if failed
        """
        # Normalize the DOI first
        normalized_doi = self.normalize_doi(doi)
        
        # Validate DOI
        if not self.validate_doi(normalized_doi):
            error_msg = "Invalid DOI format"
            logger.error(f"{error_msg}: {doi}")
            self.failed_dois.append((doi, error_msg))
            self.log_failed_doi(doi, error_msg)
            return None
            
        # Use the normalized DOI for further processing
        doi = normalized_doi
        
        # Generate filename from DOI
        filename = doi.replace('/', '_') + '.pdf'
        filepath = os.path.join(self.output_dir, filename)
        
        # Check if file already exists
        if self.skip_existing and os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            logger.info(f"Paper already exists: {filepath}. Skipping download.")
            return filepath
        
        # Maximum number of attempts for the entire download process
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                # Find a working domain
                domain = self.find_working_domain()
                if not domain:
                    error_msg = "No working Sci-Hub domains found"
                    logger.error(error_msg)
                    self.failed_dois.append((doi, error_msg))
                    self.log_failed_doi(doi, error_msg)
                    return None
                
                # Construct the URL
                url = f"{domain}/{doi}"
                logger.info(f"Attempt {attempt+1}/{max_attempts}: Fetching paper from: {url}")
                
                # Get the Sci-Hub page with timeout and retry logic
                try:
                    response = self.session.get(url, timeout=30)
                    response.raise_for_status()  # Raise exception for 4XX/5XX responses
                except requests.exceptions.RequestException as e:
                    if attempt < max_attempts - 1:
                        wait_time = random.uniform(2, 5)
                        logger.warning(f"Request failed: {e}. Retrying in {wait_time:.2f} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        error_msg = f"Failed to access Sci-Hub page after {max_attempts} attempts: {e}"
                        logger.error(error_msg)
                        self.failed_dois.append((doi, error_msg))
                        self.log_failed_doi(doi, error_msg)
                        return None
                
                # Parse the page to find the PDF link
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check if CAPTCHA is present
                captcha_elements = soup.find_all(string=lambda text: 'captcha' in text.lower() if text else False)
                if captcha_elements:
                    error_msg = "CAPTCHA detected on the page. Cannot proceed automatically."
                    logger.error(error_msg)
                    self.failed_dois.append((doi, error_msg))
                    self.log_failed_doi(doi, error_msg)
                    return None
                
                # Try multiple methods to find the PDF URL
                pdf_url = None
                
                # Method 1: Look for the PDF iframe (traditional method)
                iframe = soup.find('iframe')
                if iframe and iframe.get('src'):
                    pdf_url = iframe.get('src')
                    logger.info("Found PDF URL in iframe")
                
                # Method 2: Look for download button or link
                if not pdf_url:
                    download_buttons = soup.find_all('a', href=True)
                    for button in download_buttons:
                        href = button.get('href')
                        if href and ('.pdf' in href or 'download' in href.lower()):
                            pdf_url = href
                            logger.info("Found PDF URL in download link")
                            break
                
                # Method 3: Look for embed tags
                if not pdf_url:
                    embed = soup.find('embed')
                    if embed and embed.get('src'):
                        pdf_url = embed.get('src')
                        logger.info("Found PDF URL in embed tag")
                
                # Method 4: Look for object tags
                if not pdf_url:
                    obj = soup.find('object')
                    if obj and obj.get('data'):
                        pdf_url = obj.get('data')
                        logger.info("Found PDF URL in object tag")
                
                # Method 5: Look for div with specific classes that might contain the PDF URL
                if not pdf_url:
                    for div in soup.find_all('div', class_=True):
                        if div.get('class') and div.get('class')[0] and 'pdf' in div.get('class')[0].lower():
                            links = div.find_all('a', href=True)
                            for link in links:
                                if '.pdf' in link.get('href', ''):
                                    pdf_url = link.get('href')
                                    logger.info("Found PDF URL in div with PDF class")
                                    break
                
                # Check if we found a PDF URL
                if not pdf_url:
                    if attempt < max_attempts - 1:
                        logger.warning(f"Could not find PDF URL on attempt {attempt+1}. Trying again with a different domain...")
                        # Remove the current domain from the list to try a different one
                        if domain in SCIHUB_DOMAINS:
                            SCIHUB_DOMAINS.remove(domain)
                        time.sleep(random.uniform(2, 5))
                        continue
                    else:
                        error_msg = "Could not find PDF URL on the page using any method after multiple attempts"
                        logger.error(error_msg)
                        self.failed_dois.append((doi, error_msg))
                        self.log_failed_doi(doi, error_msg)
                        return None
                
                # If the URL is relative, make it absolute
                if pdf_url.startswith('//'):
                    pdf_url = 'https:' + pdf_url
                elif not pdf_url.startswith(('http://', 'https://')):
                    # Handle relative URLs
                    base_url = urlparse(domain)
                    pdf_url = f"{base_url.scheme}://{base_url.netloc}{pdf_url}"
                
                logger.info(f"Found PDF URL: {pdf_url}")
                
                # Download the PDF with retry logic
                pdf_download_attempts = 3
                for pdf_attempt in range(pdf_download_attempts):
                    try:
                        pdf_response = self.session.get(pdf_url, stream=True, timeout=60)
                        pdf_response.raise_for_status()
                        
                        # Check if the content is actually a PDF
                        content_type = pdf_response.headers.get('Content-Type', '')
                        is_pdf = False
                        
                        if 'application/pdf' in content_type or pdf_url.endswith('.pdf'):
                            is_pdf = True
                        else:
                            # Try to check the first few bytes for PDF signature
                            first_bytes = next(pdf_response.iter_content(4), None)
                            if first_bytes == b'%PDF':
                                is_pdf = True
                                logger.info("Confirmed PDF by signature check")
                        
                        if not is_pdf:
                            if pdf_attempt < pdf_download_attempts - 1:
                                logger.warning(f"Downloaded content does not appear to be a PDF. Retrying... ({pdf_attempt+1}/{pdf_download_attempts})")
                                time.sleep(random.uniform(2, 5))
                                continue
                            else:
                                error_msg = "Downloaded content does not appear to be a PDF after multiple attempts"
                                logger.error(error_msg)
                                self.failed_dois.append((doi, error_msg))
                                self.log_failed_doi(doi, error_msg)
                                return None
                        
                        # Save the PDF with error handling
                        try:
                            with open(filepath, 'wb') as f:
                                for chunk in pdf_response.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)
                            
                            # Verify the file was saved and has content
                            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                                logger.info(f"Successfully downloaded paper to: {filepath}")
                                return filepath
                            else:
                                if pdf_attempt < pdf_download_attempts - 1:
                                    logger.warning(f"File was not saved properly or is empty. Retrying... ({pdf_attempt+1}/{pdf_download_attempts})")
                                    if os.path.exists(filepath):
                                        os.remove(filepath)  # Remove empty or corrupt file
                                    time.sleep(random.uniform(2, 5))
                                    continue
                                else:
                                    error_msg = "File was not saved properly or is empty after multiple attempts"
                                    logger.error(error_msg)
                                    self.failed_dois.append((doi, error_msg))
                                    self.log_failed_doi(doi, error_msg)
                                    if os.path.exists(filepath):
                                        os.remove(filepath)  # Remove empty or corrupt file
                                    return None
                                    
                        except (IOError, OSError) as e:
                            if pdf_attempt < pdf_download_attempts - 1:
                                logger.warning(f"Error saving PDF file: {e}. Retrying... ({pdf_attempt+1}/{pdf_download_attempts})")
                                if os.path.exists(filepath):
                                    os.remove(filepath)  # Remove potentially corrupt file
                                time.sleep(random.uniform(2, 5))
                                continue
                            else:
                                error_msg = f"Error saving PDF file after multiple attempts: {e}"
                                logger.error(error_msg)
                                self.failed_dois.append((doi, error_msg))
                                self.log_failed_doi(doi, error_msg)
                                return None
                                
                    except requests.exceptions.RequestException as e:
                        if pdf_attempt < pdf_download_attempts - 1:
                            logger.warning(f"Error downloading PDF: {e}. Retrying... ({pdf_attempt+1}/{pdf_download_attempts})")
                            time.sleep(random.uniform(2, 5))
                            continue
                        else:
                            error_msg = f"Error downloading PDF after multiple attempts: {e}"
                            logger.error(error_msg)
                            self.failed_dois.append((doi, error_msg))
                            self.log_failed_doi(doi, error_msg)
                            return None
                
                # If we get here, all PDF download attempts failed
                error_msg = "All PDF download attempts failed for unknown reasons"
                logger.error(error_msg)
                self.failed_dois.append((doi, error_msg))
                self.log_failed_doi(doi, error_msg)
                return None
                
            except requests.RequestException as e:
                if attempt < max_attempts - 1:
                    wait_time = random.uniform(2, 5)
                    logger.warning(f"Request exception: {e}. Retrying in {wait_time:.2f} seconds... ({attempt+1}/{max_attempts})")
                    time.sleep(wait_time)
                    continue
                else:
                    error_msg = f"Request exception after {max_attempts} attempts: {e}"
                    logger.error(error_msg)
                    self.failed_dois.append((doi, error_msg))
                    self.log_failed_doi(doi, error_msg)
                    return None
            except Exception as e:
                if attempt < max_attempts - 1:
                    wait_time = random.uniform(2, 5)
                    logger.warning(f"Unexpected error: {e}. Retrying in {wait_time:.2f} seconds... ({attempt+1}/{max_attempts})")
                    time.sleep(wait_time)
                    continue
                else:
                    error_msg = f"Unexpected error after {max_attempts} attempts: {e}"
                    logger.error(error_msg)
                    self.failed_dois.append((doi, error_msg))
                    self.log_failed_doi(doi, error_msg)
                    return None
        
        # If we get here, all attempts failed
        error_msg = "All download attempts failed for unknown reasons"
        logger.error(error_msg)
        self.failed_dois.append((doi, error_msg))
        self.log_failed_doi(doi, error_msg)
        return None

def main():
    """Main function to handle command line interface."""
    parser = argparse.ArgumentParser(description='Download papers from Sci-Hub using DOIs')
    parser.add_argument('dois', nargs='*', help='DOIs to download')
    parser.add_argument('-f', '--file', help='File containing DOIs (one per line)')
    parser.add_argument('-o', '--output', help='Output directory for downloaded papers')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Initialize downloader
    downloader = SciHubDownloader(output_dir=args.output)
    
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
        normalized = downloader.normalize_doi(doi)
        if normalized and downloader.validate_doi(normalized):
            cleaned_dois.append(normalized)
        elif doi:  # Only log if doi is not empty
            logger.warning(f"Skipping invalid DOI: {doi}")
    
    # Check if we have any valid DOIs to process
    if not cleaned_dois:
        logger.error("No valid DOIs found. Please check your input.")
        return 1
    
    # Process each valid DOI with improved progress reporting
    success_count = 0
    failed_count = 0
    start_time = time.time()
    
    print(f"\nStarting download of {len(cleaned_dois)} papers...\n")
    
    for i, doi in enumerate(cleaned_dois):
        # Calculate and display progress
        progress = (i / len(cleaned_dois)) * 100 if len(cleaned_dois) > 0 else 0
        elapsed_time = time.time() - start_time
        papers_per_minute = (i / elapsed_time) * 60 if elapsed_time > 0 else 0
        eta_minutes = ((len(cleaned_dois) - i) / papers_per_minute) if papers_per_minute > 0 else 0
        
        # Progress bar (50 characters wide)
        bar_length = 50
        filled_length = int(bar_length * i // len(cleaned_dois))
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        # Print progress information
        print(f"\r[{bar}] {progress:.1f}% | {i}/{len(cleaned_dois)} | " 
              f"Success: {success_count} | Failed: {failed_count} | " 
              f"ETA: {eta_minutes:.1f} min", end='')
        
        # Log the current DOI being processed
        logger.info(f"Processing DOI {i+1}/{len(cleaned_dois)}: {doi}")
        
        # Download the paper
        result = downloader.download_paper(doi)
        
        if result:
            success_count += 1
        else:
            failed_count += 1
        
        # Add a small delay between requests to avoid overloading the server
        if i < len(cleaned_dois) - 1:
            delay = random.uniform(1, 3)
            logger.debug(f"Waiting {delay:.2f} seconds before next request...")
            time.sleep(delay)
    
    # Complete the progress bar
    print(f"\r[{'█' * bar_length}] 100.0% | {len(cleaned_dois)}/{len(cleaned_dois)} | " 
          f"Success: {success_count} | Failed: {failed_count} | Complete!{' ' * 20}")
    
    # Print summary
    total_time = time.time() - start_time
    minutes, seconds = divmod(total_time, 60)
    print(f"\nDownload complete in {int(minutes)}m {int(seconds)}s")
    print(f"Results: {success_count}/{len(cleaned_dois)} papers downloaded successfully")
    
    if failed_count > 0 and downloader.failed_dois:
        print("\nFailed DOIs and reasons:")
        for doi, reason in downloader.failed_dois:
            print(f"  - {doi}: {reason}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
