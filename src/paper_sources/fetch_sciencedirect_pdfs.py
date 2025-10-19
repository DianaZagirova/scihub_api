#!/usr/bin/env python3
"""
Fetch PDFs directly from ScienceDirect for Open Access papers.
Constructs direct PDF URLs from DOI/PII.
"""

import sqlite3
import requests
import re
import logging
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'

def extract_pii_from_doi(doi):
    """
    Extract PII from DOI.
    Example: 10.1016/j.arr.2024.102527 -> S1568163724003453
    """
    # For Elsevier/ScienceDirect DOIs starting with 10.1016
    if not doi.startswith('10.1016/'):
        return None
    
    # Try to get PII from the ScienceDirect page
    url = f"https://www.sciencedirect.com/science/article/pii/{doi}"
    
    try:
        response = requests.get(f"https://doi.org/{doi}", 
                              allow_redirects=True, 
                              timeout=10,
                              headers={'User-Agent': 'Mozilla/5.0'})
        
        if response.status_code == 200:
            # Check if we're on ScienceDirect
            if 'sciencedirect.com' in response.url:
                # Extract PII from URL
                match = re.search(r'/pii/([A-Z0-9]+)', response.url)
                if match:
                    return match.group(1)
                
                # Try to extract from page content
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for PII in meta tags
                pii_meta = soup.find('meta', {'name': 'citation_pii'})
                if pii_meta:
                    return pii_meta.get('content')
                
                # Look for PII in the page
                pii_match = re.search(r'PII:\s*([A-Z0-9]+)', response.text)
                if pii_match:
                    return pii_match.group(1)
    
    except Exception as e:
        logger.debug(f"Error extracting PII for {doi}: {e}")
    
    return None

def build_sciencedirect_pdf_url(pii):
    """
    Build direct PDF URL from PII.
    Note: This may require authentication for non-OA papers.
    """
    # Try multiple URL patterns
    urls = [
        f"https://www.sciencedirect.com/science/article/pii/{pii}/pdfft",
        f"https://www.sciencedirect.com/science/article/pii/{pii}/pdf",
    ]
    
    return urls

def check_pdf_accessible(url):
    """Check if PDF is accessible (returns PDF content-type)."""
    try:
        response = requests.head(url, 
                                allow_redirects=True, 
                                timeout=10,
                                headers={'User-Agent': 'Mozilla/5.0'})
        
        content_type = response.headers.get('Content-Type', '')
        
        if 'application/pdf' in content_type:
            return True, url
        elif response.status_code == 200:
            # Try GET to check actual content
            response = requests.get(url, 
                                   stream=True, 
                                   timeout=10,
                                   headers={'User-Agent': 'Mozilla/5.0'})
            content_type = response.headers.get('Content-Type', '')
            if 'application/pdf' in content_type:
                return True, url
    
    except Exception as e:
        logger.debug(f"Error checking PDF accessibility: {e}")
    
    return False, None

def update_oa_url(doi, oa_url):
    """Update oa_url in database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE papers 
        SET oa_url = ?
        WHERE doi = ?
    """, (oa_url, doi))
    
    conn.commit()
    conn.close()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Fetch PDF URLs from ScienceDirect',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test with specific DOI
  python fetch_sciencedirect_pdfs.py --doi 10.1016/j.arr.2024.102527
  
  # Test with specific PII
  python fetch_sciencedirect_pdfs.py --pii S1568163724003453
        """
    )
    parser.add_argument(
        '--doi',
        help='Test with a specific DOI'
    )
    parser.add_argument(
        '--pii',
        help='Test with a specific PII'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be updated without actually updating'
    )
    
    args = parser.parse_args()
    
    print('='*70)
    print('FETCHING PDF URLS FROM SCIENCEDIRECT')
    print('='*70)
    
    # Test with DOI
    if args.doi:
        logger.info(f"Testing DOI: {args.doi}")
        
        # Extract PII
        pii = extract_pii_from_doi(args.doi)
        
        if pii:
            print(f"\n✓ Extracted PII: {pii}")
            
            # Build PDF URLs
            pdf_urls = build_sciencedirect_pdf_url(pii)
            
            print(f"\nTrying PDF URLs:")
            for url in pdf_urls:
                print(f"  {url}")
                accessible, final_url = check_pdf_accessible(url)
                
                if accessible:
                    print(f"  ✅ PDF accessible!")
                    
                    if not args.dry_run:
                        update_oa_url(args.doi, final_url)
                        print(f"  Updated database with PDF URL")
                    
                    return 0
                else:
                    print(f"  ❌ Not accessible")
            
            print(f"\n⚠️  No accessible PDF found")
            print(f"   This paper may require institutional access")
        else:
            print(f"\n❌ Could not extract PII from DOI")
        
        return 0
    
    # Test with PII
    if args.pii:
        logger.info(f"Testing PII: {args.pii}")
        
        pdf_urls = build_sciencedirect_pdf_url(args.pii)
        
        print(f"\nTrying PDF URLs:")
        for url in pdf_urls:
            print(f"  {url}")
            accessible, final_url = check_pdf_accessible(url)
            
            if accessible:
                print(f"  ✅ PDF accessible!")
                print(f"  URL: {final_url}")
                return 0
            else:
                print(f"  ❌ Not accessible")
        
        print(f"\n⚠️  No accessible PDF found")
        return 0
    
    print("\n❌ Please provide --doi or --pii")
    return 1

if __name__ == '__main__':
    import sys
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f'\n❌ Error: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
