#!/usr/bin/env python3
"""
Fetch Open Access URLs from Unpaywall API for DOIs that don't have real OA links.
Updates the oa_url field in the database with actual downloadable PDF URLs.
"""

import sqlite3
import requests
import time
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
EMAIL = 'your-email@example.com'  # Required by Unpaywall API

def get_dois_needing_oa_urls(limit=None):
    """
    Get DOIs that need real OA URLs:
    - Missing content in database
    - No valid oa_url (NULL, empty, or just points to DOI)
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    query = """
        SELECT doi, oa_url
        FROM papers 
        WHERE doi IS NOT NULL AND doi != ''
        AND (
            (full_text IS NULL OR full_text = '')
            AND (full_text_sections IS NULL OR full_text_sections = '')
        )
        AND (
            oa_url IS NULL 
            OR oa_url = '' 
            OR oa_url LIKE 'https://doi.org/%'
            OR oa_url LIKE 'http://dx.doi.org/%'
        )
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    dois = cursor.fetchall()
    conn.close()
    
    return dois

def fetch_unpaywall_data(doi, email):
    """
    Fetch OA information from Unpaywall API.
    
    Returns:
        dict: {
            'is_oa': bool,
            'best_oa_location': {
                'url_for_pdf': str,
                'version': str (publishedVersion, acceptedVersion, submittedVersion),
                'host_type': str (publisher, repository)
            }
        }
    """
    url = f"https://api.unpaywall.org/v2/{doi}?email={email}"
    
    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return data
        elif response.status_code == 404:
            logger.debug(f"DOI not found in Unpaywall: {doi}")
            return None
        else:
            logger.warning(f"Unpaywall API error {response.status_code} for {doi}")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching Unpaywall data for {doi}: {e}")
        return None

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
        description='Fetch Open Access URLs from Unpaywall API'
    )
    parser.add_argument(
        '--email',
        required=True,
        help='Your email (required by Unpaywall API)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of DOIs to process (for testing)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.1,
        help='Delay between API requests in seconds (default: 0.1)'
    )
    
    args = parser.parse_args()
    
    print('='*70)
    print('FETCHING OPEN ACCESS URLS FROM UNPAYWALL')
    print('='*70)
    
    # Get DOIs needing OA URLs
    logger.info("Finding DOIs that need OA URLs...")
    dois = get_dois_needing_oa_urls(limit=args.limit)
    
    if not dois:
        logger.info("✅ No DOIs need OA URLs!")
        return 0
    
    logger.info(f"Found {len(dois):,} DOIs to check")
    
    # Process each DOI
    stats = {
        'total': len(dois),
        'found_oa': 0,
        'no_oa': 0,
        'errors': 0
    }
    
    for i, (doi, current_oa_url) in enumerate(dois, 1):
        if i % 100 == 0:
            logger.info(f"Progress: {i}/{len(dois)} ({i/len(dois)*100:.1f}%)")
        
        # Fetch from Unpaywall
        data = fetch_unpaywall_data(doi, args.email)
        
        if data and data.get('is_oa'):
            best_location = data.get('best_oa_location')
            
            if best_location and best_location.get('url_for_pdf'):
                pdf_url = best_location['url_for_pdf']
                version = best_location.get('version', 'unknown')
                host_type = best_location.get('host_type', 'unknown')
                
                # Update database
                update_oa_url(doi, pdf_url)
                stats['found_oa'] += 1
                
                logger.info(f"✓ {doi}: Found OA PDF ({version}, {host_type})")
                logger.debug(f"  URL: {pdf_url}")
            else:
                stats['no_oa'] += 1
                logger.debug(f"✗ {doi}: OA but no PDF URL")
        else:
            stats['no_oa'] += 1
            logger.debug(f"✗ {doi}: Not OA")
        
        # Rate limiting
        time.sleep(args.delay)
    
    # Summary
    print('\n' + '='*70)
    print('SUMMARY')
    print('='*70)
    print(f"Total DOIs checked: {stats['total']:,}")
    print(f"  Found OA PDFs: {stats['found_oa']:,} ({stats['found_oa']/stats['total']*100:.1f}%)")
    print(f"  No OA available: {stats['no_oa']:,} ({stats['no_oa']/stats['total']*100:.1f}%)")
    print(f"  Errors: {stats['errors']:,}")
    print('='*70)
    
    if stats['found_oa'] > 0:
        print(f"\n✅ Updated {stats['found_oa']:,} DOIs with OA URLs!")
        print("   Run your processing script again to download these papers.")
    
    return 0

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
