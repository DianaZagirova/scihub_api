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
from urllib.parse import urlparse, quote
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# List of potential Sci-Hub domains (these may change over time)
# Updated: 2025-10-11
SCIHUB_DOMAINS = [
    'https://sci-hub.st',
    'https://sci-hub.wf',
    'https://sci-hub.se',    
    'https://sci-hub.ru',
    'https://sci-hub.ee',
    'https://sci-hub.ren',

    'https://sci-hub.sh'
    
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
    
    def detect_identifier_type(self, identifier):
        """
        Detect the type of identifier (DOI, PMID, or title).
        
        Args:
            identifier (str): Identifier string to detect
            
        Returns:
            str: 'doi', 'pmid', or 'title'
        """
        if not identifier:
            return 'title'
        
        identifier = identifier.strip()
        
        # Check if it's a PMID (all digits, typically 8 digits)
        if identifier.isdigit() and len(identifier) >= 6:
            return 'pmid'
        
        # Check for PMID prefix
        if identifier.lower().startswith('pmid:') or identifier.lower().startswith('pmid '):
            return 'pmid'
        
        # Check if it starts with DOI patterns
        doi_indicators = ['10.', 'doi:', 'doi.org/', 'dx.doi.org/']
        for indicator in doi_indicators:
            if identifier.lower().startswith(indicator.lower()) or indicator.lower() in identifier.lower():
                return 'doi'
        
        # Otherwise treat as title
        return 'title'
    
    def title_to_doi(self, title):
        """
        Convert paper title to DOI using CrossRef API.
        
        Args:
            title (str): Paper title to search
            
        Returns:
            str: DOI if found with high confidence, None otherwise
        """
        try:
            # Use CrossRef API to search for the title
            url = "https://api.crossref.org/works"
            params = {
                'query.title': title,
                'rows': 1  # Get only the top result
            }
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract DOI from the top result
            if 'message' in data and 'items' in data['message'] and len(data['message']['items']) > 0:
                top_result = data['message']['items'][0]
                doi = top_result.get('DOI')
                result_title = top_result.get('title', [''])[0] if 'title' in top_result else ''
                score = top_result.get('score', 0)
                
                if doi:
                    logger.info(f"Found potential match for title '{title[:50]}...'")
                    logger.info(f"  → Result: '{result_title[:50]}...' (DOI: {doi}, score: {score})")
                    
                    # Only accept high-confidence matches
                    if score > 35.0:  # CrossRef score threshold
                        return doi
                    else:
                        logger.warning(f"Low confidence match (score: {score}), skipping")
            
            logger.warning(f"Could not find DOI for title: {title[:50]}...")
            return None
        except Exception as e:
            logger.error(f"Error converting title to DOI: {e}")
            return None
    
    def pmid_to_doi(self, pmid):
        """
        Convert PMID to DOI using PubMed E-utilities API.
        
        Args:
            pmid (str): PMID to convert
            
        Returns:
            str: DOI if found, None otherwise
        """
        try:
            # Use PubMed E-utilities to get article details
            url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id={pmid}&retmode=json"
            
            # Add NCBI credentials for better rate limiting
            params = Config.get_ncbi_params({'db': 'pubmed', 'id': pmid, 'retmode': 'json'})
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract DOI from the response
            if 'result' in data and pmid in data['result']:
                article_ids = data['result'][pmid].get('articleids', [])
                for article_id in article_ids:
                    if article_id.get('idtype') == 'doi':
                        doi = article_id.get('value')
                        logger.info(f"Converted PMID {pmid} to DOI: {doi}")
                        return doi
            
            logger.warning(f"Could not find DOI for PMID {pmid}")
            return None
        except Exception as e:
            logger.error(f"Error converting PMID to DOI: {e}")
            return None
    
    def normalize_pmid(self, pmid):
        """
        Normalize a PMID string by removing prefixes and cleaning it.
        
        Args:
            pmid (str): PMID string to normalize
            
        Returns:
            str: Normalized PMID or None if invalid
        """
        if not pmid:
            return None
        
        normalized = pmid.strip()
        
        # Remove PMID prefix if present
        if normalized.lower().startswith('pmid:'):
            normalized = normalized[5:].strip()
        elif normalized.lower().startswith('pmid '):
            normalized = normalized[5:].strip()
        
        # PMID should be all digits
        if normalized.isdigit():
            return normalized
        
        return None
    
    def validate_pmid(self, pmid):
        """
        Validate if the provided string is a valid PMID.
        
        Args:
            pmid (str): PMID to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        normalized_pmid = self.normalize_pmid(pmid)
        if not normalized_pmid:
            return False
        
        # PMID should be all digits and typically between 6-8 digits (but can be longer)
        return normalized_pmid.isdigit() and len(normalized_pmid) >= 6
    
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
        # Allows all printable ASCII except whitespace (DOI spec allows <>;# and other special chars)
        doi_pattern = r'^10\.\d{4,9}/[!-~]+$'
        
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
    
    def download_paper(self, identifier):
        """
        Download a paper from Sci-Hub using DOI, PMID, or title.
        
        Args:
            identifier (str): DOI, PMID, or title of the paper to download
            
        Returns:
            str: Path to the downloaded file or None if failed
        """
        # Detect identifier type
        id_type = self.detect_identifier_type(identifier)
        logger.info(f"Detected identifier type: {id_type}")
        
        # Normalize based on type
        if id_type == 'doi':
            normalized_id = self.normalize_doi(identifier)
            if not self.validate_doi(normalized_id):
                error_msg = "Invalid DOI format"
                logger.error(f"{error_msg}: {identifier}")
                self.failed_dois.append((identifier, error_msg))
                self.log_failed_doi(identifier, error_msg)
                return None
        elif id_type == 'pmid':
            normalized_pmid = self.normalize_pmid(identifier)
            if not self.validate_pmid(normalized_pmid):
                error_msg = "Invalid PMID format"
                logger.error(f"{error_msg}: {identifier}")
                self.failed_dois.append((identifier, error_msg))
                self.log_failed_doi(identifier, error_msg)
                return None
            
            # Convert PMID to DOI for Sci-Hub download
            logger.info(f"Converting PMID {normalized_pmid} to DOI...")
            doi = self.pmid_to_doi(normalized_pmid)
            if not doi:
                error_msg = "Could not convert PMID to DOI"
                logger.error(f"{error_msg}: {identifier}")
                self.failed_dois.append((identifier, error_msg))
                self.log_failed_doi(identifier, error_msg)
                return None
            
            # Use the DOI for download
            normalized_id = doi
            id_type = 'doi'  # Switch to DOI type for the rest of the download process
        else:  # title
            # For titles, clean up whitespace
            title = ' '.join(identifier.strip().split())
            if not title:
                error_msg = "Empty title"
                logger.error(f"{error_msg}")
                self.failed_dois.append((identifier, error_msg))
                self.log_failed_doi(identifier, error_msg)
                return None
            
            # Convert title to DOI using CrossRef
            logger.info(f"Searching for DOI using title: {title[:50]}...")
            doi = self.title_to_doi(title)
            if not doi:
                error_msg = "Could not find DOI for title"
                logger.error(f"{error_msg}: {title[:50]}...")
                self.failed_dois.append((identifier, error_msg))
                self.log_failed_doi(identifier, error_msg)
                return None
            
            # Use the DOI for download
            normalized_id = doi
            id_type = 'doi'  # Switch to DOI type for the rest of the download process
        
        # Generate safe filename (at this point normalized_id is always a DOI)
        filename = normalized_id.replace('/', '_') + '.pdf'
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
                    self.failed_dois.append((identifier, error_msg))
                    self.log_failed_doi(identifier, error_msg)
                    return None
                
                # Construct the URL (at this point we always have a DOI)
                # For DOI, minimal encoding (keep slashes and dots)
                encoded_id = quote(normalized_id, safe='/:.')
                url = f"{domain}/{encoded_id}"
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
                        self.failed_dois.append((identifier, error_msg))
                        self.log_failed_doi(identifier, error_msg)
                        return None
                
                # Parse the page to find the PDF link
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check if CAPTCHA is present
                captcha_elements = soup.find_all(string=lambda text: 'captcha' in text.lower() if text else False)
                if captcha_elements:
                    error_msg = "CAPTCHA detected on the page. Cannot proceed automatically."
                    logger.error(error_msg)
                    self.failed_dois.append((identifier, error_msg))
                    self.log_failed_doi(identifier, error_msg)
                    return None
                
                # Check if it's an article not found page
                not_found_indicators = [
                    'article not found',
                    'no results found',
                    'not found in database',
                    'нет в базе'  # Russian: not in database
                ]
                page_text = soup.get_text().lower()
                for indicator in not_found_indicators:
                    if indicator in page_text:
                        error_msg = f"Article not found on Sci-Hub (indicator: '{indicator}')"
                        logger.error(error_msg)
                        self.failed_dois.append((identifier, error_msg))
                        self.log_failed_doi(identifier, error_msg)
                        return None
                
                # Try multiple methods to find the PDF URL
                pdf_url = None
                
                # Method 1: Look for the PDF iframe (traditional method)
                iframe = soup.find('iframe')
                if iframe and iframe.get('src'):
                    pdf_url = iframe.get('src')
                    logger.info("Found PDF URL in iframe")
                
                # Method 2: Look for the save/download button
                if not pdf_url:
                    # Sci-Hub often uses a button with id="save" or onclick with location
                    save_button = soup.find('button', {'id': 'save'})
                    if save_button and save_button.get('onclick'):
                        onclick = save_button.get('onclick')
                        # Extract URL from onclick="location.href='...'"
                        match = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", onclick)
                        if match:
                            pdf_url = match.group(1)
                            logger.info("Found PDF URL in save button onclick")
                
                # Method 3: Look for download button or link
                if not pdf_url:
                    download_buttons = soup.find_all('a', href=True)
                    for button in download_buttons:
                        href = button.get('href')
                        if href and ('.pdf' in href or 'download' in href.lower()):
                            pdf_url = href
                            logger.info("Found PDF URL in download link")
                            break
                
                # Method 4: Look for embed tags
                if not pdf_url:
                    embed = soup.find('embed')
                    if embed and embed.get('src'):
                        pdf_url = embed.get('src')
                        logger.info("Found PDF URL in embed tag")
                
                # Method 5: Look for object tags
                if not pdf_url:
                    obj = soup.find('object')
                    if obj and obj.get('data'):
                        pdf_url = obj.get('data')
                        logger.info("Found PDF URL in object tag")
                
                # Method 6: Look for div with specific classes that might contain the PDF URL
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
                    # Save HTML for debugging
                    debug_html_path = os.path.join(self.logs_dir, f"debug_response_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
                    try:
                        with open(debug_html_path, 'w', encoding='utf-8') as f:
                            f.write(response.text)
                        logger.info(f"Saved HTML response to {debug_html_path} for debugging")
                    except:
                        pass
                    
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
                        logger.error(f"Check {debug_html_path} for the HTML response")
                        self.failed_dois.append((identifier, error_msg))
                        self.log_failed_doi(identifier, error_msg)
                        return None
                
                # If the URL is relative, make it absolute
                if pdf_url.startswith('//'):
                    pdf_url = 'https:' + pdf_url
                elif not pdf_url.startswith(('http://', 'https://')):
                    # Handle relative URLs
                    base_url = urlparse(domain)
                    pdf_url = f"{base_url.scheme}://{base_url.netloc}{pdf_url}"
                
                # Remove URL fragments (e.g., #navpanes=0&view=FitH) that can cause 404 errors
                if '#' in pdf_url:
                    pdf_url = pdf_url.split('#')[0]
                    logger.debug(f"Removed URL fragment from PDF URL")
                
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
                                self.failed_dois.append((identifier, error_msg))
                                self.log_failed_doi(identifier, error_msg)
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
                                    self.failed_dois.append((identifier, error_msg))
                                    self.log_failed_doi(identifier, error_msg)
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
                                self.failed_dois.append((identifier, error_msg))
                                self.log_failed_doi(identifier, error_msg)
                                return None
                                
                    except requests.exceptions.RequestException as e:
                        if pdf_attempt < pdf_download_attempts - 1:
                            logger.warning(f"Error downloading PDF: {e}. Retrying... ({pdf_attempt+1}/{pdf_download_attempts})")
                            time.sleep(random.uniform(2, 5))
                            continue
                        else:
                            error_msg = f"Error downloading PDF after multiple attempts: {e}"
                            logger.error(error_msg)
                            self.failed_dois.append((identifier, error_msg))
                            self.log_failed_doi(identifier, error_msg)
                            return None
                
                # If we get here, all PDF download attempts failed
                error_msg = "All PDF download attempts failed for unknown reasons"
                logger.error(error_msg)
                self.failed_dois.append((identifier, error_msg))
                self.log_failed_doi(identifier, error_msg)
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
                    self.failed_dois.append((identifier, error_msg))
                    self.log_failed_doi(identifier, error_msg)
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
                    self.failed_dois.append((identifier, error_msg))
                    self.log_failed_doi(identifier, error_msg)
                    return None
        
        # If we get here, all attempts failed
        error_msg = "All download attempts failed for unknown reasons"
        logger.error(error_msg)
        self.failed_dois.append((identifier, error_msg))
        self.log_failed_doi(identifier, error_msg)
        return None

def main():
    """Main function to handle command line interface."""
    parser = argparse.ArgumentParser(description='Download papers from Sci-Hub using DOI, PMID, or title')
    parser.add_argument('identifiers', nargs='*', help='DOIs, PMIDs, or titles to download')
    parser.add_argument('-f', '--file', help='File containing identifiers (one per line)')
    parser.add_argument('-o', '--output', help='Output directory for downloaded papers')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Initialize downloader
    downloader = SciHubDownloader(output_dir=args.output)
    
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
        id_type = downloader.detect_identifier_type(identifier)
        is_valid = False
        
        if id_type == 'doi':
            normalized = downloader.normalize_doi(identifier)
            is_valid = downloader.validate_doi(normalized)
        elif id_type == 'pmid':
            normalized = downloader.normalize_pmid(identifier)
            is_valid = downloader.validate_pmid(normalized)
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
    
    # Process each valid identifier with improved progress reporting
    success_count = 0
    failed_count = 0
    start_time = time.time()
    
    print(f"\nStarting download of {len(valid_identifiers)} papers...\n")
    
    for i, identifier in enumerate(valid_identifiers):
        # Calculate and display progress
        progress = (i / len(valid_identifiers)) * 100 if len(valid_identifiers) > 0 else 0
        elapsed_time = time.time() - start_time
        papers_per_minute = (i / elapsed_time) * 60 if elapsed_time > 0 else 0
        eta_minutes = ((len(valid_identifiers) - i) / papers_per_minute) if papers_per_minute > 0 else 0
        
        # Progress bar (50 characters wide)
        bar_length = 50
        filled_length = int(bar_length * i // len(valid_identifiers))
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        # Print progress information
        print(f"\r[{bar}] {progress:.1f}% | {i}/{len(valid_identifiers)} | " 
              f"Success: {success_count} | Failed: {failed_count} | " 
              f"ETA: {eta_minutes:.1f} min", end='')
        
        # Log the current identifier being processed
        logger.info(f"Processing identifier {i+1}/{len(valid_identifiers)}: {identifier}")
        
        # Download the paper
        result = downloader.download_paper(identifier)
        
        if result:
            success_count += 1
        else:
            failed_count += 1
        
        # Add a small delay between requests to avoid overloading the server
        if i < len(valid_identifiers) - 1:
            delay = random.uniform(1, 3)
            logger.debug(f"Waiting {delay:.2f} seconds before next request...")
            time.sleep(delay)
    
    # Complete the progress bar
    print(f"\r[{'█' * bar_length}] 100.0% | {len(valid_identifiers)}/{len(valid_identifiers)} | " 
          f"Success: {success_count} | Failed: {failed_count} | Complete!{' ' * 20}")
    
    # Print summary
    total_time = time.time() - start_time
    minutes, seconds = divmod(total_time, 60)
    print(f"\nDownload complete in {int(minutes)}m {int(seconds)}s")
    print(f"Results: {success_count}/{len(valid_identifiers)} papers downloaded successfully")
    
    if failed_count > 0 and downloader.failed_dois:
        print("\nFailed identifiers and reasons:")
        for identifier, reason in downloader.failed_dois:
            print(f"  - {identifier}: {reason}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
