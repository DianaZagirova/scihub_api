#!/usr/bin/env python3
"""
Examples of how to integrate DOI tracker into existing scripts

This demonstrates:
1. How to check if a DOI needs processing before attempting
2. How to update tracker after each processing step
3. How to get lists of DOIs for specific processing stages
"""

from doi_tracker import DOITracker
from pathlib import Path


# ============================================================================
# EXAMPLE 1: Integration with download script
# ============================================================================

def example_download_papers_with_tracker():
    """
    Example: How to modify download_papers.py to use the tracker.
    """
    tracker = DOITracker('doi_processing_tracker.csv')
    
    # Read DOIs from input file
    with open('dois_to_download.txt', 'r') as f:
        dois = [line.strip() for line in f if line.strip()]
    
    print(f"Processing {len(dois)} DOIs...")
    
    for doi in dois:
        # Check if already processed successfully
        status = tracker.get_status(doi)
        
        if status:
            # Skip if already downloaded
            if status.get('downloaded') == tracker.AVAILABLE_YES:
                print(f"Skipping {doi} - already downloaded")
                continue
            
            # Skip if marked as not available in Sci-Hub
            if status.get('scihub_available') == tracker.AVAILABLE_NO:
                print(f"Skipping {doi} - not in Sci-Hub")
                continue
        
        # Attempt to check Sci-Hub availability
        print(f"Checking Sci-Hub for {doi}...")
        # available = check_scihub_availability(doi)  # Your actual function
        available = True  # Example
        
        tracker.mark_scihub_found(doi, available)
        
        if not available:
            print(f"  Not found in Sci-Hub")
            continue
        
        # Attempt download
        print(f"Downloading {doi}...")
        try:
            # success = download_from_scihub(doi)  # Your actual function
            success = True  # Example
            
            if success:
                tracker.mark_downloaded(doi, success=True)
                print(f"  Downloaded successfully")
            else:
                tracker.mark_downloaded(doi, success=False, error_msg="Download failed")
                print(f"  Download failed")
        
        except Exception as e:
            tracker.mark_downloaded(doi, success=False, error_msg=str(e))
            print(f"  Error: {e}")


# ============================================================================
# EXAMPLE 2: Integration with PyMuPDF parser
# ============================================================================

def example_parse_with_pymupdf():
    """
    Example: How to use tracker with PyMuPDF parsing.
    """
    tracker = DOITracker('doi_processing_tracker.csv')
    
    # Get DOIs that need PyMuPDF processing
    dois_to_process = tracker.get_dois_needing_pymupdf()
    
    print(f"Found {len(dois_to_process)} DOIs needing PyMuPDF processing")
    
    for doi in dois_to_process[:10]:  # Process first 10 for example
        print(f"\nProcessing {doi} with PyMuPDF...")
        
        # Mark as in progress
        tracker.update_status(doi, pymupdf_status=tracker.STATUS_IN_PROGRESS)
        
        try:
            # pdf_path = get_pdf_path(doi)  # Your function
            # result = parse_with_pymupdf(pdf_path)  # Your function
            
            # If successful
            tracker.mark_pymupdf_processed(doi, success=True)
            print(f"  Successfully parsed with PyMuPDF")
            
        except Exception as e:
            tracker.mark_pymupdf_processed(doi, success=False, error_msg=str(e))
            print(f"  PyMuPDF parsing failed: {e}")


# ============================================================================
# EXAMPLE 3: Integration with Grobid parser
# ============================================================================

def example_parse_with_grobid():
    """
    Example: How to use tracker with Grobid parsing.
    """
    tracker = DOITracker('doi_processing_tracker.csv')
    
    # Get DOIs that need Grobid processing
    dois_to_process = tracker.get_dois_needing_grobid()
    
    print(f"Found {len(dois_to_process)} DOIs needing Grobid processing")
    
    for doi in dois_to_process[:10]:  # Process first 10 for example
        print(f"\nProcessing {doi} with Grobid...")
        
        # Mark as in progress
        tracker.update_status(doi, grobid_status=tracker.STATUS_IN_PROGRESS)
        
        try:
            # pdf_path = get_pdf_path(doi)  # Your function
            # result = parse_with_grobid(pdf_path)  # Your function
            
            # If successful
            tracker.mark_grobid_processed(doi, success=True)
            print(f"  Successfully parsed with Grobid")
            
        except Exception as e:
            tracker.mark_grobid_processed(doi, success=False, error_msg=str(e))
            print(f"  Grobid parsing failed: {e}")


# ============================================================================
# EXAMPLE 4: Retry failed processing
# ============================================================================

def example_retry_failed():
    """
    Example: How to retry failed processing with limits.
    """
    tracker = DOITracker('doi_processing_tracker.csv')
    
    # Get failed DOIs (with retry limits)
    failed = tracker.get_failed_dois(max_retries=3)
    
    print("DOIs that failed and can be retried (< 3 attempts):")
    print(f"  Download failures: {len(failed['download'])}")
    print(f"  PyMuPDF failures: {len(failed['pymupdf'])}")
    print(f"  Grobid failures: {len(failed['grobid'])}")
    
    # Retry PyMuPDF failures
    for doi in failed['pymupdf'][:5]:
        print(f"\nRetrying PyMuPDF for {doi}...")
        # ... retry logic ...


# ============================================================================
# EXAMPLE 5: Generate reports and export lists
# ============================================================================

def example_generate_reports():
    """
    Example: Generate various reports and export DOI lists.
    """
    tracker = DOITracker('doi_processing_tracker.csv')
    
    # Show statistics
    tracker.print_statistics()
    
    # Export DOIs needing different processing stages
    dois_need_download = tracker.get_dois_needing_download()
    with open('dois_need_download.txt', 'w') as f:
        f.write('\n'.join(dois_need_download))
    print(f"\nExported {len(dois_need_download)} DOIs needing download")
    
    dois_need_pymupdf = tracker.get_dois_needing_pymupdf()
    with open('dois_need_pymupdf.txt', 'w') as f:
        f.write('\n'.join(dois_need_pymupdf))
    print(f"Exported {len(dois_need_pymupdf)} DOIs needing PyMuPDF")
    
    dois_need_grobid = tracker.get_dois_needing_grobid()
    with open('dois_need_grobid.txt', 'w') as f:
        f.write('\n'.join(dois_need_grobid))
    print(f"Exported {len(dois_need_grobid)} DOIs needing Grobid")


# ============================================================================
# EXAMPLE 6: Sync with papers.db
# ============================================================================

def example_sync_with_database():
    """
    Example: Sync tracker data with papers.db.
    """
    tracker = DOITracker('doi_processing_tracker.csv')
    
    db_path = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
    
    # Export tracker data to database
    print("Syncing tracker data to papers.db...")
    updated = tracker.export_to_database(db_path)
    print(f"Updated {updated} records in database")


# ============================================================================
# EXAMPLE 7: Bulk update from log files
# ============================================================================

def example_bulk_update_from_logs():
    """
    Example: Parse log files and bulk update tracker.
    """
    tracker = DOITracker('doi_processing_tracker.csv')
    
    # Parse a log file (simplified example)
    updates = []
    
    # Example log parsing
    log_data = [
        {'doi': '10.1234/example1', 'pymupdf_status': 'success'},
        {'doi': '10.1234/example2', 'pymupdf_status': 'failed'},
    ]
    
    tracker.bulk_update(log_data)
    print(f"Bulk updated {len(log_data)} records from logs")


# ============================================================================
# EXAMPLE 8: Check DOI before processing pipeline
# ============================================================================

def example_smart_pipeline():
    """
    Example: Smart processing pipeline that checks tracker at each step.
    """
    tracker = DOITracker('doi_processing_tracker.csv')
    
    doi = "10.1234/example"
    
    # Get current status
    status = tracker.get_status(doi)
    
    if not status:
        print(f"DOI {doi} not in tracker - starting from scratch")
        # Check Sci-Hub
        # ...
    else:
        print(f"DOI {doi} status:")
        print(f"  Sci-Hub: {status.get('scihub_available')}")
        print(f"  Downloaded: {status.get('downloaded')}")
        print(f"  PyMuPDF: {status.get('pymupdf_status')}")
        print(f"  Grobid: {status.get('grobid_status')}")
        
        # Determine next step
        if status.get('downloaded') != tracker.AVAILABLE_YES:
            print("  Next step: Download PDF")
        elif status.get('pymupdf_status') not in [tracker.STATUS_SUCCESS, tracker.STATUS_FAILED]:
            print("  Next step: Process with PyMuPDF")
        elif status.get('grobid_status') not in [tracker.STATUS_SUCCESS, tracker.STATUS_FAILED]:
            print("  Next step: Process with Grobid")
        else:
            print("  Status: Fully processed")


if __name__ == '__main__':
    print("DOI Tracker Integration Examples")
    print("="*70)
    print("\nThis file demonstrates how to integrate the tracker.")
    print("Uncomment the examples below to test them:")
    print()
    
    # Uncomment to run examples:
    # example_download_papers_with_tracker()
    # example_parse_with_pymupdf()
    # example_parse_with_grobid()
    # example_retry_failed()
    # example_generate_reports()
    # example_sync_with_database()
    # example_smart_pipeline()
    
    # Show tracker statistics
    tracker = DOITracker('doi_processing_tracker.csv')
    
    if Path('doi_processing_tracker.csv').exists() and Path('doi_processing_tracker.csv').stat().st_size > 100:
        tracker.print_statistics()
    else:
        print("\nTracker not initialized yet. Run:")
        print("  python initialize_tracker.py")
