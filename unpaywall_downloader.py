#!/usr/bin/env python3
"""
Unpaywall PDF Downloader

Uses the Unpaywall REST API to find and download Open Access PDFs.
API Documentation: https://unpaywall.org/products/api

Features:
- DOI lookup for OA status and PDF URLs
- Title search with OA filtering
- Rate limiting (100k calls/day)
- Robust error handling and retries
- PDF validation
"""

import os
import sys
import time
import requests
import logging
from pathlib import Path
from typing import Optional, Dict, List, Any
from urllib.parse import quote

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class UnpaywallDownloader:
    """
    Downloader for Open Access papers using Unpaywall API.
    
    API requires email parameter for all requests.
    Rate limit: 100,000 calls per day.
    """
    
    BASE_URL = "https://api.unpaywall.org/v2"
    
    def __init__(self, email: str = None, output_dir: str = './papers'):
        """
        Initialize Unpaywall downloader.
        
        Args:
            email: Your email address (required by Unpaywall API). If None, uses config.
            output_dir: Directory to save downloaded PDFs
        """
        self.email = email or Config.UNPAYWALL_EMAIL
        if not self.email:
            raise ValueError("Email is required for Unpaywall API. Set UNPAYWALL_EMAIL environment variable or pass email parameter.")
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'
        })
        
        # Rate limiting: 100k/day = ~1.16 per second, use 1 per second to be safe
        self.min_request_interval = 1.0
        self.last_request_time = 0
        
        logger.info(f"Initialized Unpaywall downloader with email: {email}")
    
    def _rate_limit(self):
        """Enforce rate limiting between API requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()
    
    def get_doi_metadata(self, doi: str, timeout: int = 15) -> Optional[Dict[str, Any]]:
        """
        Get metadata and OA status for a DOI.
        
        Args:
            doi: DOI to lookup
            timeout: Request timeout in seconds
            
        Returns:
            Dict with full DOI object, or None if not found
            
        Example response fields:
            - doi: The DOI
            - is_oa: Boolean, is this OA?
            - oa_status: 'gold', 'green', 'hybrid', 'bronze', 'closed'
            - best_oa_location: Dict with 'url_for_pdf', 'url', 'version'
            - oa_locations: List of all OA locations
            - title: Paper title
            - year: Publication year
            - journal_name: Journal name
        """
        self._rate_limit()
        
        try:
            doi_encoded = quote(doi, safe='')
            url = f"{self.BASE_URL}/{doi_encoded}"
            params = {'email': self.email}
            
            logger.debug(f"Fetching metadata for DOI: {doi}")
            response = self.session.get(url, params=params, timeout=timeout)
            
            if response.status_code == 404:
                logger.warning(f"DOI not found in Unpaywall: {doi}")
                return None
            
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"DOI {doi}: is_oa={data.get('is_oa')}, oa_status={data.get('oa_status')}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching metadata for {doi}: {e}")
            return None
    
    def get_pdf_urls(self, doi: str) -> List[str]:
        """
        Get all available PDF URLs for a DOI.
        
        Args:
            doi: DOI to lookup
            
        Returns:
            List of PDF URLs, prioritized by quality
        """
        metadata = self.get_doi_metadata(doi)
        if not metadata:
            return []
        
        if not metadata.get('is_oa'):
            logger.info(f"DOI {doi} is not Open Access")
            return []
        
        pdf_urls = []
        
        # 1. Best OA location (highest priority)
        best_loc = metadata.get('best_oa_location')
        if best_loc:
            pdf_url = best_loc.get('url_for_pdf') or best_loc.get('pdf_url')
            if pdf_url:
                pdf_urls.append(pdf_url)
                logger.info(f"Found best OA PDF: {pdf_url[:80]}...")
            elif best_loc.get('url'):
                # Sometimes 'url' points to landing page, but worth trying
                pdf_urls.append(best_loc['url'])
        
        # 2. All other OA locations
        for loc in metadata.get('oa_locations', []):
            pdf_url = loc.get('url_for_pdf') or loc.get('pdf_url')
            if pdf_url and pdf_url not in pdf_urls:
                pdf_urls.append(pdf_url)
            elif loc.get('url') and loc['url'] not in pdf_urls:
                pdf_urls.append(loc['url'])
        
        logger.info(f"Found {len(pdf_urls)} PDF URL(s) for {doi}")
        return pdf_urls
    
    def download_pdf(self, doi: str, output_path: Optional[str] = None, 
                     timeout: int = 30) -> Optional[str]:
        """
        Download PDF for a DOI.
        
        Args:
            doi: DOI to download
            output_path: Custom output path (default: {output_dir}/{doi_safe}.pdf)
            timeout: Download timeout in seconds
            
        Returns:
            Path to downloaded PDF, or None if failed
        """
        # Get PDF URLs
        pdf_urls = self.get_pdf_urls(doi)
        if not pdf_urls:
            logger.warning(f"No OA PDF URLs found for {doi}")
            return None
        
        # Determine output path
        if not output_path:
            safe_doi = doi.replace('/', '_')
            output_path = self.output_dir / f"{safe_doi}.pdf"
        else:
            output_path = Path(output_path)
        
        # Try each URL until one succeeds
        for i, url in enumerate(pdf_urls, 1):
            logger.info(f"Attempting download {i}/{len(pdf_urls)}: {url[:80]}...")
            
            try:
                response = self.session.get(url, timeout=timeout, stream=True, 
                                           allow_redirects=True)
                
                if response.status_code != 200:
                    logger.warning(f"HTTP {response.status_code} for {url[:80]}...")
                    continue
                
                # Validate content type
                content_type = response.headers.get('Content-Type', '').lower()
                
                # Read first chunk to check for PDF magic number
                first_chunk = b''
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        first_chunk = chunk[:5]
                        break
                
                # Validate it's actually a PDF
                if 'pdf' not in content_type and not first_chunk.startswith(b'%PDF-'):
                    logger.warning(f"Not a PDF (Content-Type: {content_type})")
                    continue
                
                # Write to file
                with open(output_path, 'wb') as f:
                    f.write(first_chunk)
                    for chunk in response.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)
                
                file_size = output_path.stat().st_size
                logger.info(f"✓ Downloaded {doi} ({file_size:,} bytes) -> {output_path}")
                return str(output_path)
                
            except Exception as e:
                logger.warning(f"Failed to download from {url[:80]}...: {e}")
                continue
        
        logger.error(f"All download attempts failed for {doi}")
        return None
    
    def search_by_title(self, query: str, is_oa: Optional[bool] = True, 
                       page: int = 1, timeout: int = 15) -> List[Dict[str, Any]]:
        """
        Search for papers by title.
        
        Args:
            query: Search query (words are AND-ed, use "quotes" for phrases, OR, -)
            is_oa: Filter by OA status (True=OA only, False=non-OA, None=all)
            page: Page number (50 results per page)
            timeout: Request timeout
            
        Returns:
            List of results, each containing:
                - response: Full DOI object
                - score: Relevance score
                - snippet: HTML snippet showing match
        """
        self._rate_limit()
        
        try:
            url = f"{self.BASE_URL}/search"
            params = {
                'query': query,
                'email': self.email,
                'page': page
            }
            
            if is_oa is not None:
                params['is_oa'] = 'true' if is_oa else 'false'
            
            logger.info(f"Searching: '{query}' (OA={is_oa}, page={page})")
            response = self.session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            
            results = response.json().get('results', [])
            logger.info(f"Found {len(results)} results")
            
            return results
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Search error: {e}")
            return []
    
    def batch_download(self, dois: List[str], max_workers: int = 1) -> Dict[str, Optional[str]]:
        """
        Download multiple DOIs.
        
        Args:
            dois: List of DOIs to download
            max_workers: Number of parallel workers (keep at 1 for rate limiting)
            
        Returns:
            Dict mapping DOI -> downloaded path (or None if failed)
        """
        results = {}
        
        logger.info(f"Starting batch download of {len(dois)} DOIs")
        
        for i, doi in enumerate(dois, 1):
            logger.info(f"[{i}/{len(dois)}] Processing {doi}")
            path = self.download_pdf(doi)
            results[doi] = path
            
            if path:
                logger.info(f"✓ Success: {doi}")
            else:
                logger.warning(f"✗ Failed: {doi}")
        
        success_count = sum(1 for p in results.values() if p)
        logger.info(f"Batch complete: {success_count}/{len(dois)} successful")
        
        return results


def main():
    """Example usage."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Download Open Access papers using Unpaywall API'
    )
    parser.add_argument('--email', default=None, help='Your email (required by API). If not provided, uses UNPAYWALL_EMAIL environment variable.')
    parser.add_argument('--doi', help='Single DOI to download')
    parser.add_argument('--file', help='File with DOIs (one per line)')
    parser.add_argument('--search', help='Search by title')
    parser.add_argument('--output', default='./papers', help='Output directory')
    parser.add_argument('--oa-only', action='store_true', help='Search: OA papers only')
    
    args = parser.parse_args()
    
    # Initialize downloader
    downloader = UnpaywallDownloader(email=args.email, output_dir=args.output)
    
    # Single DOI
    if args.doi:
        result = downloader.download_pdf(args.doi)
        if result:
            print(f"✓ Downloaded: {result}")
            return 0
        else:
            print(f"✗ Failed to download {args.doi}")
            return 1
    
    # Batch from file
    elif args.file:
        with open(args.file) as f:
            dois = [line.strip() for line in f if line.strip()]
        
        results = downloader.batch_download(dois)
        success = sum(1 for p in results.values() if p)
        print(f"\nResults: {success}/{len(dois)} successful")
        return 0 if success > 0 else 1
    
    # Search by title
    elif args.search:
        results = downloader.search_by_title(args.search, is_oa=args.oa_only)
        
        if not results:
            print("No results found")
            return 1
        
        print(f"\nFound {len(results)} results:\n")
        for i, item in enumerate(results, 1):
            resp = item['response']
            score = item['score']
            snippet = item.get('snippet', resp.get('title', 'N/A'))
            
            print(f"{i}. [{score:.1f}] {snippet}")
            print(f"   DOI: {resp['doi']}")
            print(f"   OA: {resp.get('is_oa')} ({resp.get('oa_status')})")
            print(f"   Year: {resp.get('year')}")
            print()
        
        return 0
    
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())
