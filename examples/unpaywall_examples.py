#!/usr/bin/env python3
"""
Unpaywall API Usage Examples

Demonstrates various ways to use the Unpaywall downloader.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from unpaywall_downloader import UnpaywallDownloader


def example_1_single_doi():
    """Example 1: Download a single DOI."""
    print("=" * 60)
    print("Example 1: Download single DOI")
    print("=" * 60)
    
    email = "your.email@example.com"  # Replace with your email
    downloader = UnpaywallDownloader(email=email, output_dir='./papers')
    
    doi = "10.1038/nature12373"
    print(f"\nDownloading: {doi}")
    
    result = downloader.download_pdf(doi)
    
    if result:
        print(f"✓ Success! Downloaded to: {result}")
    else:
        print(f"✗ Failed to download {doi}")


def example_2_get_metadata():
    """Example 2: Get metadata without downloading."""
    print("\n" + "=" * 60)
    print("Example 2: Get metadata only")
    print("=" * 60)
    
    email = "your.email@example.com"
    downloader = UnpaywallDownloader(email=email)
    
    doi = "10.1038/nature12373"
    print(f"\nFetching metadata for: {doi}")
    
    metadata = downloader.get_doi_metadata(doi)
    
    if metadata:
        print(f"\nTitle: {metadata.get('title')}")
        print(f"Year: {metadata.get('year')}")
        print(f"Journal: {metadata.get('journal_name')}")
        print(f"Is OA: {metadata.get('is_oa')}")
        print(f"OA Status: {metadata.get('oa_status')}")
        
        if metadata.get('best_oa_location'):
            loc = metadata['best_oa_location']
            print(f"\nBest OA Location:")
            print(f"  URL: {loc.get('url_for_pdf') or loc.get('url')}")
            print(f"  Version: {loc.get('version')}")
            print(f"  License: {loc.get('license')}")


def example_3_batch_download():
    """Example 3: Download multiple DOIs."""
    print("\n" + "=" * 60)
    print("Example 3: Batch download")
    print("=" * 60)
    
    email = "your.email@example.com"
    downloader = UnpaywallDownloader(email=email, output_dir='./papers')
    
    dois = [
        "10.1038/nature12373",
        "10.1371/journal.pone.0000000",  # Invalid DOI for testing
        "10.1186/s13059-014-0550-8",
    ]
    
    print(f"\nDownloading {len(dois)} DOIs...")
    results = downloader.batch_download(dois)
    
    print("\nResults:")
    for doi, path in results.items():
        status = "✓" if path else "✗"
        print(f"  {status} {doi}: {path or 'Failed'}")


def example_4_search_by_title():
    """Example 4: Search for papers by title."""
    print("\n" + "=" * 60)
    print("Example 4: Search by title")
    print("=" * 60)
    
    email = "your.email@example.com"
    downloader = UnpaywallDownloader(email=email)
    
    # Search for papers about CRISPR
    query = "CRISPR gene editing"
    print(f"\nSearching for: '{query}' (OA only)")
    
    results = downloader.search_by_title(query, is_oa=True, page=1)
    
    print(f"\nFound {len(results)} results:\n")
    for i, item in enumerate(results[:5], 1):  # Show first 5
        resp = item['response']
        score = item['score']
        
        print(f"{i}. Score: {score:.1f}")
        print(f"   Title: {resp.get('title')}")
        print(f"   DOI: {resp['doi']}")
        print(f"   Year: {resp.get('year')}")
        print(f"   OA Status: {resp.get('oa_status')}")
        print()


def example_5_check_oa_availability():
    """Example 5: Check OA availability for multiple DOIs."""
    print("\n" + "=" * 60)
    print("Example 5: Check OA availability")
    print("=" * 60)
    
    email = "your.email@example.com"
    downloader = UnpaywallDownloader(email=email)
    
    dois = [
        "10.1038/nature12373",
        "10.1126/science.1260419",
        "10.1016/j.cell.2013.05.039",
    ]
    
    print("\nChecking OA status for DOIs:\n")
    
    for doi in dois:
        metadata = downloader.get_doi_metadata(doi)
        
        if metadata:
            is_oa = metadata.get('is_oa')
            oa_status = metadata.get('oa_status')
            pdf_urls = downloader.get_pdf_urls(doi)
            
            print(f"DOI: {doi}")
            print(f"  OA: {is_oa} ({oa_status})")
            print(f"  PDF URLs: {len(pdf_urls)}")
            
            if pdf_urls:
                print(f"  Best URL: {pdf_urls[0][:80]}...")
        else:
            print(f"DOI: {doi}")
            print(f"  Not found in Unpaywall")
        
        print()


def example_6_advanced_search():
    """Example 6: Advanced search with operators."""
    print("\n" + "=" * 60)
    print("Example 6: Advanced search")
    print("=" * 60)
    
    email = "your.email@example.com"
    downloader = UnpaywallDownloader(email=email)
    
    # Search examples with different operators
    queries = [
        '"machine learning" cancer',  # Phrase + word
        'covid OR coronavirus',        # OR operator
        'climate change -model',       # Negation
    ]
    
    for query in queries:
        print(f"\nQuery: {query}")
        results = downloader.search_by_title(query, is_oa=True, page=1)
        print(f"Results: {len(results)}")
        
        if results:
            top = results[0]['response']
            print(f"Top result: {top.get('title')[:80]}...")


if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("Unpaywall API Examples")
    print("=" * 60)
    print("\nNote: Replace 'your.email@example.com' with your actual email")
    print("      before running these examples.\n")
    
    # Run all examples
    try:
        example_1_single_doi()
        example_2_get_metadata()
        example_3_batch_download()
        example_4_search_by_title()
        example_5_check_oa_availability()
        example_6_advanced_search()
        
        print("\n" + "=" * 60)
        print("All examples completed!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError: {e}")
        print("Make sure to set your email address in the examples.")
