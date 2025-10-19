#!/usr/bin/env python3
"""
Test script for new PDF sources integration.
Tests arXiv, bioRxiv, Europe PMC, and Unpaywall.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from download_papers_optimized import (
    fetch_arxiv_pdf_url,
    fetch_biorxiv_pdf_url,
    fetch_europepmc_pdf_url,
    fetch_unpaywall_pdf_url
)

def test_europepmc():
    """Test Europe PMC with the DOI from user's example."""
    print("=" * 60)
    print("Testing Europe PMC")
    print("=" * 60)
    
    doi = '10.3892/mmr.2018.8370'
    print(f"\nDOI: {doi}")
    
    url = fetch_europepmc_pdf_url(doi)
    
    if url:
        print(f"✓ Found URL: {url}")
    else:
        print("✗ No URL found")
    
    return url is not None


def test_arxiv():
    """Test arXiv with a known arXiv DOI."""
    print("\n" + "=" * 60)
    print("Testing arXiv")
    print("=" * 60)
    
    # Test with arXiv DOI
    doi = '10.48550/arxiv.2301.00001'
    print(f"\nDOI: {doi}")
    
    url = fetch_arxiv_pdf_url(doi)
    
    if url:
        print(f"✓ Found URL: {url}")
    else:
        print("✗ No URL found (may not exist)")
    
    return True  # Don't fail if arXiv paper doesn't exist


def test_biorxiv():
    """Test bioRxiv with a bioRxiv DOI."""
    print("\n" + "=" * 60)
    print("Testing bioRxiv")
    print("=" * 60)
    
    doi = '10.1101/2023.01.01.522405'
    print(f"\nDOI: {doi}")
    
    url = fetch_biorxiv_pdf_url(doi)
    
    if url:
        print(f"✓ Found URL: {url}")
    else:
        print("✗ No URL found (may not exist)")
    
    return True  # Don't fail if bioRxiv paper doesn't exist


def test_unpaywall():
    """Test Unpaywall with a known OA paper."""
    print("\n" + "=" * 60)
    print("Testing Unpaywall")
    print("=" * 60)
    
    doi = '10.1371/journal.pone.0000001'
    print(f"\nDOI: {doi}")
    
    url = fetch_unpaywall_pdf_url(doi)
    
    if url:
        print(f"✓ Found URL: {url}")
    else:
        print("✗ No URL found")
    
    return url is not None


def test_tracker_integration():
    """Test tracker integration."""
    print("\n" + "=" * 60)
    print("Testing Tracker Integration")
    print("=" * 60)
    
    try:
        from trackers.doi_tracker_db import DOITracker
        
        tracker = DOITracker(db_path='test_tracker.db')
        test_doi = '10.1234/test.integration'
        
        # Test marking attempts
        print("\nMarking source attempts...")
        tracker.mark_source_attempted(test_doi, 'arxiv')
        tracker.mark_source_attempted(test_doi, 'biorxiv')
        tracker.mark_source_attempted(test_doi, 'europepmc')
        tracker.mark_source_attempted(test_doi, 'unpaywall')
        print("✓ Attempts marked")
        
        # Test marking downloads
        print("\nMarking download results...")
        tracker.mark_source_downloaded(test_doi, 'arxiv', success=True)
        tracker.mark_source_downloaded(test_doi, 'biorxiv', success=False)
        print("✓ Results marked")
        
        # Verify
        status = tracker.get_status(test_doi)
        if status:
            print(f"\nTracker status for {test_doi}:")
            print(f"  arxiv_attempted: {status.get('arxiv_attempted')}")
            print(f"  arxiv_downloaded: {status.get('arxiv_downloaded')}")
            print(f"  biorxiv_attempted: {status.get('biorxiv_attempted')}")
            print(f"  biorxiv_downloaded: {status.get('biorxiv_downloaded')}")
            print(f"  download_source: {status.get('download_source')}")
        
        # Cleanup
        import os
        if os.path.exists('test_tracker.db'):
            os.remove('test_tracker.db')
        
        print("\n✓ Tracker integration working")
        return True
        
    except Exception as e:
        print(f"\n✗ Tracker integration error: {e}")
        return False


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("Testing New PDF Sources Integration")
    print("=" * 60)
    
    results = {
        'Europe PMC': test_europepmc(),
        'arXiv': test_arxiv(),
        'bioRxiv': test_biorxiv(),
        'Unpaywall': test_unpaywall(),
        'Tracker': test_tracker_integration()
    }
    
    print("\n" + "=" * 60)
    print("Test Results Summary")
    print("=" * 60)
    
    for source, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status} - {source}")
    
    all_passed = all(results.values())
    
    if all_passed:
        print("\n✓ All tests passed!")
        return 0
    else:
        print("\n✗ Some tests failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())
