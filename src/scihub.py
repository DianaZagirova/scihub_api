#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sci-Hub downloader - Modified version without external dependencies
"""

import os
import re
import time
import random
import logging
import hashlib
import argparse
import datetime
import requests
from bs4 import BeautifulSoup
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# log config
logging.basicConfig()
logger = logging.getLogger('Sci-Hub')
logger.setLevel(logging.INFO)

# constants
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0'
}
AVAILABLE_SCIHUB_BASE_URL = [
    'https://sci-hub.se',
    'https://sci-hub.st',
    'https://sci-hub.ru',
]


class SciHub(object):
    """
    SciHub class
    """

    def __init__(self):
        self.sess = requests.Session()
        self.sess.headers.update(HEADERS)
        self.available_base_url_list = AVAILABLE_SCIHUB_BASE_URL.copy()
        self.base_url = self.available_base_url_list[0] + '/'

    def _change_base_url(self):
        if not self.available_base_url_list:
            raise Exception('No more available base urls')
        del self.available_base_url_list[0]
        self.base_url = self.available_base_url_list[0] + '/'
        logger.info("Changed base url to {0}".format(self.available_base_url_list[0]))

    def fetch(self, identifier):
        """
        Fetch the paper by identifier
        """
        try:
            url = self._get_direct_url(identifier)
            return self._fetch_url(url)
        except Exception as e:
            logger.error(e)
            return {
                'pdf': None,
                'url': identifier,
                'name': None,
                'err': str(e),
            }

    def _get_direct_url(self, identifier):
        """
        Get direct PDF url by identifier with improved URL handling
        """
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                # Process the identifier to create a proper URL
                if self._is_url(identifier):
                    url = self._normalize_url(identifier)
                else:
                    # Handle DOI or other identifier
                    url = self.base_url + identifier.strip()
                
                logger.info(f"Attempt {attempt+1}/{max_attempts}: Accessing {url}")
                
                # Get the page with proper error handling
                try:
                    res = self.sess.get(url, verify=False, allow_redirects=True, timeout=30)
                    res.raise_for_status()
                except Exception as req_err:
                    logger.error(f"Request error: {req_err}")
                    if attempt < max_attempts - 1:
                        if str(req_err).startswith('Failed to establish a new connection'):
                            self._change_base_url()
                        wait_time = random.uniform(2, 5)
                        logger.info(f"Retrying in {wait_time:.2f} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise req_err
                
                # Parse the page
                s = BeautifulSoup(res.content, 'lxml')
                
                # Check for CAPTCHA
                captcha_elements = s.find_all(string=lambda text: 'captcha' in text.lower() if text else False)
                if captcha_elements:
                    logger.warning("CAPTCHA detected on the page")
                    if attempt < max_attempts - 1:
                        self._change_base_url()
                        wait_time = random.uniform(3, 7)
                        logger.info(f"Trying a different domain in {wait_time:.2f} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception('CAPTCHA detected on all attempted domains')
                
                # Try multiple methods to find the PDF URL
                pdf_url = None
                
                # Method 1: Look for the PDF iframe (traditional method)
                iframe = s.find('iframe')
                if iframe and iframe.get('src'):
                    pdf_url = iframe.get('src')
                    logger.info("Found PDF URL in iframe")
                
                # Method 2: Look for download button or link
                if not pdf_url:
                    for a in s.find_all('a', href=True):
                        href = a.get('href')
                        if href and ('.pdf' in href.lower() or 'download' in a.text.lower()):
                            pdf_url = href
                            logger.info("Found PDF URL in download link")
                            break
                
                # Method 3: Look for embedded PDF
                if not pdf_url:
                    embed = s.find('embed')
                    if embed and embed.get('src'):
                        pdf_url = embed.get('src')
                        logger.info("Found PDF URL in embed tag")
                
                # Method 4: Check for object tag
                if not pdf_url:
                    obj = s.find('object')
                    if obj and obj.get('data'):
                        pdf_url = obj.get('data')
                        logger.info("Found PDF URL in object tag")
                
                # Method 5: Look for divs with PDF class
                if not pdf_url:
                    for div in s.find_all('div', class_=True):
                        if div.get('class') and div.get('class')[0] and 'pdf' in div.get('class')[0].lower():
                            links = div.find_all('a', href=True)
                            for link in links:
                                if '.pdf' in link.get('href', '').lower():
                                    pdf_url = link.get('href')
                                    logger.info("Found PDF URL in div with PDF class")
                                    break
                            if pdf_url:
                                break
                
                # If we found a PDF URL, normalize it and return
                if pdf_url:
                    return self._normalize_pdf_url(pdf_url, url)
                
                # If nothing found, try another domain if available
                if attempt < max_attempts - 1:
                    logger.warning("PDF link not found on this domain")
                    self._change_base_url()
                    wait_time = random.uniform(2, 5)
                    logger.info(f"Trying a different domain in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    # If all attempts failed, raise error
                    raise Exception('PDF link not found in page after multiple attempts')
                    
            except Exception as e:
                logger.error(f"Error in attempt {attempt+1}: {e}")
                if attempt < max_attempts - 1:
                    if str(e).startswith('Failed to establish a new connection'):
                        self._change_base_url()
                    wait_time = random.uniform(2, 5)
                    logger.info(f"Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    raise e
        
        # If we get here, all attempts failed
        raise Exception('All attempts to get direct URL failed')
    
    def _is_url(self, text):
        """
        Check if the given text is a URL
        """
        return text.startswith(('http://', 'https://', 'www.'))
    
    def _normalize_url(self, url):
        """
        Normalize a URL to ensure it has a proper scheme
        """
        if url.startswith('www.'):
            return 'https://' + url
        return url
    
    def _normalize_pdf_url(self, pdf_url, base_url):
        """
        Normalize a PDF URL to ensure it's absolute
        """
        # If it's already absolute, return it
        if pdf_url.startswith(('http://', 'https://')):
            return pdf_url
            
        # If it starts with //, add https:
        if pdf_url.startswith('//'):
            return 'https:' + pdf_url
            
        # If it's relative, make it absolute
        from urllib.parse import urljoin
        return urljoin(base_url, pdf_url)

    def _fetch_url(self, url):
        """
        Fetch url with retry mechanism and robust PDF verification
        """
        max_attempts = 10
        for attempt in range(max_attempts):
            try:
                # Ensure URL has proper scheme
                if not url.startswith('http') and not url.startswith('https'):
                    if url.startswith('//'):
                        url = 'https:' + url
                    else:
                        url = 'https://' + url.lstrip('/')
                
                # Use stream=True to avoid loading entire PDF into memory at once
                res = self.sess.get(url, verify=False, stream=True, timeout=60)
                res.raise_for_status()  # Raise exception for 4XX/5XX responses
                
                # Enhanced PDF verification
                is_pdf = self._verify_pdf(res)
                
                if not is_pdf:
                    if attempt < max_attempts - 1:
                        wait_time = random.uniform(1, 3)
                        logger.warning(f"Response doesn't appear to be a PDF. Retrying in {wait_time:.2f} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception('Not a valid PDF file after multiple attempts')
                
                # Get the full content only after verification
                pdf_content = b''
                for chunk in res.iter_content(chunk_size=8192):
                    if chunk:
                        pdf_content += chunk
                
                # Final verification on complete PDF
                if not pdf_content.startswith(b'%PDF'):
                    if attempt < max_attempts - 1:
                        logger.warning("Complete content doesn't have PDF signature. Retrying...")
                        continue
                    else:
                        raise Exception('Downloaded content is not a valid PDF')
                
                return {
                    'pdf': pdf_content,
                    'url': url,
                    'name': self._generate_name(res),
                    'err': None,
                }
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{max_attempts} failed: {e}")
                if attempt < max_attempts - 1:
                    wait_time = random.uniform(2, 5)
                    logger.info(f"Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    raise e
    
    def _verify_pdf(self, response):
        """
        Verify if the response is a PDF using multiple methods
        
        Args:
            response: The requests response object
            
        Returns:
            bool: True if it's a PDF, False otherwise
        """
        # Method 1: Check Content-Type header
        content_type = response.headers.get('Content-Type', '').lower()
        if 'application/pdf' in content_type:
            logger.info("PDF verified by Content-Type header")
            return True
            
        # Method 2: Check URL extension
        if response.url.lower().endswith('.pdf'):
            logger.info("PDF verified by URL extension")
            return True
            
        # Method 3: Check Content-Disposition header
        content_disp = response.headers.get('Content-Disposition', '').lower()
        if 'filename=' in content_disp and '.pdf' in content_disp:
            logger.info("PDF verified by Content-Disposition header")
            return True
            
        # Method 4: Check PDF signature in first bytes
        try:
            # Save the current position in the stream
            first_bytes = next(response.iter_content(4), None)
            if first_bytes == b'%PDF':
                logger.info("PDF verified by signature check")
                return True
        except Exception as e:
            logger.warning(f"Error checking PDF signature: {e}")
            
        # If we get here, we couldn't verify it's a PDF
        return False

    def _generate_name(self, res):
        """
        Generate filename for the paper with improved handling
        
        Args:
            res: The requests response object
            
        Returns:
            str: A suitable filename for the paper
        """
        # Try to get filename from Content-Disposition header (most reliable method)
        if 'Content-Disposition' in res.headers:
            disposition = res.headers['Content-Disposition']
            if disposition and 'filename=' in disposition:
                try:
                    # Try to extract filename using regex
                    filename_match = re.findall('filename=["\']?([^"\';]+)', disposition)
                    if filename_match:
                        name = filename_match[0].strip()
                        # Ensure it has a .pdf extension
                        if not name.lower().endswith('.pdf'):
                            name += '.pdf'
                        return name
                except Exception as e:
                    logger.warning(f"Error extracting filename from Content-Disposition: {e}")
        
        # Try to get filename from URL
        try:
            from urllib.parse import urlparse
            path = urlparse(res.url).path
            if path and '/' in path:
                filename = path.split('/')[-1]
                if filename and '.' in filename:
                    # Ensure it has a .pdf extension
                    if not filename.lower().endswith('.pdf'):
                        filename += '.pdf'
                    return filename
        except Exception as e:
            logger.warning(f"Error extracting filename from URL: {e}")
        
        # Generate a unique filename based on URL or timestamp if we couldn't extract one
        try:
            # Use URL hash instead of content hash (which may not be available in streaming mode)
            url_hash = hashlib.md5(res.url.encode('utf-8')).hexdigest()[:10]
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            return f"paper_{url_hash}_{timestamp}.pdf"
        except Exception as e:
            logger.warning(f"Error generating filename hash: {e}")
            # Last resort fallback
            return f"paper_{int(time.time())}.pdf"

    def download(self, identifier, path=None):
        """
        Download the paper by identifier
        """
        data = self.fetch(identifier)
        if not data['pdf']:
            return data
        if not path:
            path = data['name']
        with open(path, 'wb') as f:
            f.write(data['pdf'])
        return data

    def search(self, query, limit=10):
        """
        Search for papers on Google Scholars
        """
        try:
            url = 'https://scholar.google.com/scholar'
            params = {
                'q': query,
                'hl': 'en',
            }
            res = self.sess.get(url, params=params, verify=False)
            s = BeautifulSoup(res.content, 'lxml')
            papers = []
            for paper in s.find_all('div', class_='gs_r'):
                if len(papers) >= limit:
                    break
                paper_data = {}
                paper_data['title'] = paper.find('h3').text
                paper_data['url'] = paper.find('h3').find('a').get('href')
                paper_data['excerpt'] = paper.find('div', class_='gs_rs').text
                paper_data['citation_count'] = 0
                citation_tag = paper.find('div', class_='gs_fl').find_all('a')[2]
                if citation_tag:
                    citation_count = re.findall(r'\d+', citation_tag.text)
                    if citation_count:
                        paper_data['citation_count'] = int(citation_count[0])
                papers.append(paper_data)
            return {
                'papers': papers,
            }
        except Exception as e:
            logger.error(e)
            return {
                'papers': [],
            }


def main():
    """
    Main function with improved error handling and user feedback
    """
    parser = argparse.ArgumentParser(description='SciHub - To remove all barriers in the way of science.')
    parser.add_argument('-d', '--download', metavar='(DOI|PMID|URL)', help='tries to find and download the paper')
    parser.add_argument('-f', '--file', metavar='path', help='pass file with list of identifiers and download each')
    parser.add_argument('-s', '--search', metavar='query', help='search Google Scholars')
    parser.add_argument('-sd', '--search_download', metavar='query', help='search Google Scholars and download if possible')
    parser.add_argument('-l', '--limit', metavar='N', type=int, default=10, help='the number of search results to limit to (default: 10)')
    parser.add_argument('-o', '--output', metavar='path', help='directory to store papers')
    parser.add_argument('-v', '--verbose', action='store_true', help='increase output verbosity')
    parser.add_argument('-p', '--proxy', help='set proxy')
    parser.add_argument('--skip-existing', action='store_true', help='skip downloading files that already exist')

    args = parser.parse_args()

    # Set up logging based on verbosity
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Check if any action was specified
    if not any([args.download, args.file, args.search, args.search_download]):
        parser.print_help()
        logger.error("No action specified. Please use one of the available options.")
        return 1

    # Initialize SciHub
    try:
        sh = SciHub()
    except Exception as e:
        logger.error(f"Failed to initialize SciHub: {e}")
        return 1

    # Set proxy if specified
    if args.proxy:
        try:
            sh.sess.proxies = {
                'http': args.proxy,
                'https': args.proxy,
            }
            logger.info(f"Using proxy: {args.proxy}")
        except Exception as e:
            logger.error(f"Failed to set proxy: {e}")
            return 1

    # Create output directory if specified
    if args.output:
        try:
            os.makedirs(args.output, exist_ok=True)
            logger.info(f"Output directory: {args.output}")
        except Exception as e:
            logger.error(f"Failed to create output directory: {e}")
            return 1

    # Handle single download
    if args.download:
        try:
            logger.info(f"Downloading paper with identifier: {args.download}")
            result = sh.download(args.download, args.output)
            if 'err' in result and result['err']:
                logger.error(f"Download failed: {result['err']}")
                return 1
            else:
                logger.info(f"Successfully downloaded paper with identifier: {args.download}")
                logger.info(f"Saved to: {result.get('name', 'unknown')}")
                return 0
        except Exception as e:
            logger.error(f"Unexpected error during download: {e}")
            return 1

    # Handle search
    elif args.search:
        try:
            logger.info(f"Searching for: {args.search} (limit: {args.limit})")
            results = sh.search(args.search, args.limit)
            if 'err' in results and results['err']:
                logger.error(f"Search failed: {results['err']}")
                return 1
            elif not results.get('papers'):
                logger.warning("No papers found matching your search criteria")
                return 0
            else:
                papers = results['papers']
                logger.info(f"Found {len(papers)} papers:")
                for i, paper in enumerate(papers):
                    logger.info(f"[{i+1}] {paper.get('title', 'Unknown title')}")
                    if 'citation_count' in paper:
                        logger.info(f"    Citations: {paper['citation_count']}")
                    if 'url' in paper:
                        logger.info(f"    URL: {paper['url']}")
                    if 'excerpt' in paper:
                        logger.info(f"    Excerpt: {paper['excerpt'][:100]}...")
                return 0
        except Exception as e:
            logger.error(f"Unexpected error during search: {e}")
            return 1

    # Handle search and download
    elif args.search_download:
        try:
            logger.info(f"Searching for and downloading: {args.search_download} (limit: {args.limit})")
            results = sh.search(args.search_download, args.limit)
            if 'err' in results and results['err']:
                logger.error(f"Search failed: {results['err']}")
                return 1
            elif not results.get('papers'):
                logger.warning("No papers found matching your search criteria")
                return 0
            else:
                papers = results['papers']
                logger.info(f"Found {len(papers)} papers. Downloading...")
                success_count = 0
                for i, paper in enumerate(papers):
                    try:
                        logger.info(f"[{i+1}/{len(papers)}] Downloading: {paper.get('title', 'Unknown title')}")
                        result = sh.download(paper['url'], args.output)
                        if 'err' in result and result['err']:
                            logger.error(f"  Failed: {result['err']}")
                        else:
                            logger.info(f"  Success! Saved to: {result.get('name', 'unknown')}")
                            success_count += 1
                        # Add a small delay between downloads
                        if i < len(papers) - 1:
                            time.sleep(random.uniform(1, 3))
                    except Exception as e:
                        logger.error(f"  Error downloading paper: {e}")
                logger.info(f"Download complete: {success_count}/{len(papers)} papers downloaded successfully")
                return 0 if success_count > 0 else 1
        except Exception as e:
            logger.error(f"Unexpected error during search and download: {e}")
            return 1

    # Handle file with identifiers
    elif args.file:
        try:
            # Check if file exists
            if not os.path.exists(args.file):
                logger.error(f"File not found: {args.file}")
                return 1
                
            # Read identifiers from file
            with open(args.file, 'r') as f:
                identifiers = [line.strip() for line in f if line.strip()]
                
            if not identifiers:
                logger.error("No identifiers found in the file")
                return 1
                
            logger.info(f"Found {len(identifiers)} identifiers in {args.file}")
            success_count = 0
            
            # Process each identifier
            for i, identifier in enumerate(identifiers):
                try:
                    logger.info(f"[{i+1}/{len(identifiers)}] Downloading: {identifier}")
                    
                    # Check if output file already exists
                    if args.skip_existing and args.output:
                        potential_filename = identifier.replace('/', '_') + '.pdf'
                        potential_path = os.path.join(args.output, potential_filename)
                        if os.path.exists(potential_path) and os.path.getsize(potential_path) > 0:
                            logger.info(f"  Skipping: File already exists at {potential_path}")
                            success_count += 1
                            continue
                    
                    # Download the paper
                    result = sh.download(identifier, args.output)
                    if 'err' in result and result['err']:
                        logger.error(f"  Failed: {result['err']}")
                    else:
                        logger.info(f"  Success! Saved to: {result.get('name', 'unknown')}")
                        success_count += 1
                        
                    # Add a small delay between downloads
                    if i < len(identifiers) - 1:
                        time.sleep(random.uniform(1, 3))
                        
                except Exception as e:
                    logger.error(f"  Error downloading identifier {identifier}: {e}")
                    
            # Print summary
            logger.info(f"Download complete: {success_count}/{len(identifiers)} papers downloaded successfully")
            return 0 if success_count > 0 else 1
            
        except Exception as e:
            logger.error(f"Unexpected error processing file: {e}")
            return 1
            
    return 0


if __name__ == '__main__':
    main()
