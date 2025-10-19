#!/usr/bin/env python3
"""
Fetch PDF URLs from Semantic Scholar API for papers missing content.
Semantic Scholar often has PDFs for recent papers that Sci-Hub doesn't have yet.
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
SEMANTIC_SCHOLAR_API = 'https://api.semanticscholar.org/graph/v1/paper'

def get_dois_needing_pdfs(limit=None, year_filter=None):
    """
    Get DOIs that need PDFs:
    - Missing content in database
    - No valid oa_url or oa_url points to DOI
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    query = """
        SELECT doi, oa_url, year
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
    
    if year_filter:
        query += f" AND year >= '{year_filter}'"
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    dois = cursor.fetchall()
    conn.close()
    
    return dois

def fetch_semantic_scholar_pdf(doi):
    """
    Fetch PDF URL from Semantic Scholar API.
    
    Returns:
        dict: {
            'pdf_url': str or None,
            'open_access': bool,
            'title': str,
            'year': int,
            'venue': str,
            'pdf_source': str (where PDF is hosted)
        }
    """
    # Semantic Scholar API endpoint
    url = f"{SEMANTIC_SCHOLAR_API}/DOI:{doi}"
    
    # Request specific fields including publicationTypes
    params = {
        'fields': 'title,year,venue,openAccessPdf,isOpenAccess,externalIds,publicationTypes'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            result = {
                'pdf_url': None,
                'open_access': data.get('isOpenAccess', False),
                'title': data.get('title', ''),
                'year': data.get('year'),
                'venue': data.get('venue', ''),
                'pdf_source': None
            }
            
            # Check for PDF URL
            open_access_pdf = data.get('openAccessPdf')
            if open_access_pdf and open_access_pdf.get('url'):
                pdf_url = open_access_pdf['url']
                
                # Only accept if it's a real PDF URL (not DOI redirect)
                if not pdf_url.startswith('https://doi.org/') and \
                   not pdf_url.startswith('http://dx.doi.org/'):
                    result['pdf_url'] = pdf_url
                    
                    # Determine source
                    if 'arxiv.org' in pdf_url:
                        result['pdf_source'] = 'arXiv'
                    elif 'biorxiv.org' in pdf_url or 'medrxiv.org' in pdf_url:
                        result['pdf_source'] = 'bioRxiv/medRxiv'
                    elif 'ncbi.nlm.nih.gov/pmc' in pdf_url:
                        result['pdf_source'] = 'PubMed Central'
                    elif 'europepmc.org' in pdf_url:
                        result['pdf_source'] = 'Europe PMC'
                    elif 'researchgate.net' in pdf_url:
                        result['pdf_source'] = 'ResearchGate'
                    else:
                        result['pdf_source'] = 'Other repository'
            
            return result
            
        elif response.status_code == 404:
            logger.debug(f"DOI not found in Semantic Scholar: {doi}")
            return None
        elif response.status_code == 429:
            logger.warning("Rate limited by Semantic Scholar API. Waiting 60 seconds...")
            time.sleep(60)
            return fetch_semantic_scholar_pdf(doi)  # Retry
        else:
            logger.warning(f"Semantic Scholar API error {response.status_code} for {doi}")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching Semantic Scholar data for {doi}: {e}")
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
        description='Fetch PDF URLs from Semantic Scholar API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check recent papers (2024)
  python fetch_semantic_scholar_pdfs.py --year 2024 --limit 100
  
  # Check all pending papers
  python fetch_semantic_scholar_pdfs.py
  
  # Test with specific DOI
  python fetch_semantic_scholar_pdfs.py --doi 10.1016/j.arr.2024.102527
        """
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of DOIs to process (for testing)'
    )
    parser.add_argument(
        '--year',
        help='Only check papers from this year onwards (e.g., 2024)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.5,
        help='Delay between API requests in seconds (default: 0.5)'
    )
    parser.add_argument(
        '--doi',
        help='Test with a specific DOI'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be updated without actually updating'
    )
    
    args = parser.parse_args()
    
    print('='*70)
    print('FETCHING PDF URLS FROM SEMANTIC SCHOLAR')
    print('='*70)
    
    # Test single DOI
    if args.doi:
        logger.info(f"Testing DOI: {args.doi}")
        result = fetch_semantic_scholar_pdf(args.doi)
        
        if result:
            print(f"\nTitle: {result['title']}")
            print(f"Year: {result['year']}")
            print(f"Venue: {result['venue']}")
            print(f"Open Access: {result['open_access']}")
            print(f"PDF URL: {result['pdf_url']}")
            if result['pdf_source']:
                print(f"PDF Source: {result['pdf_source']}")
            
            if result['pdf_url']:
                print("\n✅ PDF available!")
                if not args.dry_run:
                    update_oa_url(args.doi, result['pdf_url'])
                    print("   Updated database with PDF URL")
            else:
                print("\n❌ No downloadable PDF (may be paywalled)")
        else:
            print("\n❌ DOI not found in Semantic Scholar")
        
        return 0
    
    # Get DOIs needing PDFs
    logger.info("Finding DOIs that need PDF URLs...")
    dois = get_dois_needing_pdfs(limit=args.limit, year_filter=args.year)
    
    if not dois:
        logger.info("✅ No DOIs need PDF URLs!")
        return 0
    
    logger.info(f"Found {len(dois):,} DOIs to check")
    if args.year:
        logger.info(f"Filtering for papers from {args.year} onwards")
    
    # Process each DOI
    stats = {
        'total': len(dois),
        'found_pdf': 0,
        'no_pdf': 0,
        'not_found': 0,
        'errors': 0
    }
    
    found_pdfs = []
    
    for i, (doi, current_oa_url, year) in enumerate(dois, 1):
        if i % 50 == 0:
            logger.info(f"Progress: {i}/{len(dois)} ({i/len(dois)*100:.1f}%) - Found {stats['found_pdf']} PDFs")
        
        # Fetch from Semantic Scholar
        result = fetch_semantic_scholar_pdf(doi)
        
        if result is None:
            stats['not_found'] += 1
            logger.debug(f"✗ {doi}: Not in Semantic Scholar")
        elif result['pdf_url']:
            stats['found_pdf'] += 1
            found_pdfs.append({
                'doi': doi,
                'pdf_url': result['pdf_url'],
                'title': result['title'],
                'year': result['year']
            })
            
            logger.info(f"✓ {doi} ({result['year']}): Found PDF")
            logger.debug(f"  URL: {result['pdf_url']}")
            
            # Update database
            if not args.dry_run:
                update_oa_url(doi, result['pdf_url'])
        else:
            stats['no_pdf'] += 1
            logger.debug(f"✗ {doi}: In Semantic Scholar but no PDF")
        
        # Rate limiting
        time.sleep(args.delay)
    
    # Summary
    print('\n' + '='*70)
    print('SUMMARY')
    print('='*70)
    print(f"Total DOIs checked: {stats['total']:,}")
    print(f"  Found PDFs: {stats['found_pdf']:,} ({stats['found_pdf']/stats['total']*100:.1f}%)")
    print(f"  No PDF available: {stats['no_pdf']:,}")
    print(f"  Not in Semantic Scholar: {stats['not_found']:,}")
    print(f"  Errors: {stats['errors']:,}")
    
    if stats['found_pdf'] > 0:
        print(f"\n✅ Found {stats['found_pdf']:,} PDFs!")
        
        if args.dry_run:
            print("   (DRY RUN - database not updated)")
            print("\nExample PDFs found:")
            for item in found_pdfs[:5]:
                print(f"  - {item['doi']} ({item['year']})")
                print(f"    {item['pdf_url']}")
        else:
            print("   Updated database with PDF URLs")
            print("   Run your processing script to download these papers:")
            print("   python process_pending_with_priority.py --run --priority high")
    
    print('='*70)
    
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
