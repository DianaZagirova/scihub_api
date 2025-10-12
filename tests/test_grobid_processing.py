#!/usr/bin/env python3
"""Test GROBID processing with a sample paper."""

import sys
import os
import json
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'legacy'))
from grobid_parser import GrobidParser

# Test with a sample DOI that we'll download directly
TEST_DOI = "10.1371/journal.pone.0000000"  # Example DOI

def test_grobid_withь_sample():
    """Test GROBID processing."""
    
    print("=" * 60)
    print("GROBID Processing Test")
    print("=" * 60)
    
    # Check if we have any existing PDFs to test with
    papers_dir = os.path.join(os.path.dirname(__file__), 'papers')
    if os.path.exists(papers_dir):
        pdf_files = [f for f in os.listdir(papers_dir) if f.endswith('.pdf')]
        if pdf_files:
            print(f"\n✓ Found {len(pdf_files)} existing PDFs to test with")
            test_pdf = os.path.join(papers_dir, pdf_files[0])
            print(f"  Testing with: {pdf_files[0]}")
        else:
            print("\n✗ No PDFs found in papers/ directory")
            print("  Let's create a test by downloading a sample paper...")
            test_pdf = download_sample_paper()
    else:
        print("\n✗ papers/ directory doesn't exist")
        print("  Let's create a test by downloading a sample paper...")
        os.makedirs(papers_dir, exist_ok=True)
        test_pdf = download_sample_paper()
    
    if not test_pdf or not os.path.exists(test_pdf):
        print("\n✗ No test PDF available")
        print("\nTo test manually:")
        print("  1. Place a PDF file in the papers/ directory")
        print("  2. Run this script again")
        return False
    
    # Initialize GROBID parser
    print("\n" + "-" * 60)
    print("Initializing GROBID parser...")
    try:
        parser = GrobidParser(config_path='config.json')
        print("✓ GROBID parser initialized")
    except Exception as e:
        print(f"✗ Failed to initialize GROBID parser: {e}")
        return False
    
    # Process the PDF
    print("\n" + "-" * 60)
    print(f"Processing PDF with GROBID...")
    print(f"  File: {os.path.basename(test_pdf)}")
    print(f"  Size: {os.path.getsize(test_pdf) / 1024:.1f} KB")
    
    try:
        tei_content = parser.process_pdf(test_pdf, output_format='tei')
        if not tei_content:
            print("✗ Failed to process PDF")
            return False
        
        print(f"✓ PDF processed successfully")
        print(f"  TEI content length: {len(tei_content)} characters")
        
    except Exception as e:
        print(f"✗ Error processing PDF: {e}")
        return False
    
    # Extract metadata
    print("\n" + "-" * 60)
    print("Extracting metadata...")
    try:
        metadata = parser.extract_metadata(tei_content)
        print("✓ Metadata extracted:")
        print(f"  Title: {metadata.get('title', 'N/A')[:80]}...")
        print(f"  DOI: {metadata.get('doi', 'N/A')}")
        print(f"  Authors: {len(metadata.get('authors', []))} found")
        print(f"  Abstract: {len(metadata.get('abstract', ''))} characters")
        print(f"  Journal: {metadata.get('journal', 'N/A')}")
        print(f"  Year: {metadata.get('year', 'N/A')}")
    except Exception as e:
        print(f"✗ Error extracting metadata: {e}")
        return False
    
    # Extract full text
    print("\n" + "-" * 60)
    print("Extracting full text...")
    try:
        full_text_data = parser.extract_full_text(tei_content)
        body_sections = full_text_data.get('body', [])
        references = full_text_data.get('references', [])
        
        print("✓ Full text extracted:")
        print(f"  Body sections: {len(body_sections)}")
        for i, section in enumerate(body_sections[:5], 1):
            title = section.get('title', 'Unnamed')
            content_len = len(section.get('content', ''))
            print(f"    {i}. {title}: {content_len} characters")
        if len(body_sections) > 5:
            print(f"    ... and {len(body_sections) - 5} more sections")
        
        print(f"  References: {len(references)}")
        
        # Calculate total text
        total_text = sum(len(s.get('content', '')) for s in body_sections)
        print(f"  Total text length: {total_text} characters")
        
    except Exception as e:
        print(f"✗ Error extracting full text: {e}")
        return False
    
    print("\n" + "=" * 60)
    print("✓ ALL TESTS PASSED!")
    print("=" * 60)
    print("\nGROBID processing is working correctly.")
    print("The fetch_missing_papers.py script should work once Sci-Hub access is resolved.")
    
    return True

def download_sample_paper():
    """Try to download a sample paper for testing."""
    print("\nAttempting to download a sample paper...")
    
    # Try a few open access papers
    test_urls = [
        ("https://arxiv.org/pdf/1706.03762.pdf", "attention_is_all_you_need.pdf"),  # Transformer paper
        ("https://www.biorxiv.org/content/10.1101/2020.03.01.972935v1.full.pdf", "sample_biorxiv.pdf"),
    ]
    
    papers_dir = os.path.join(os.path.dirname(__file__), 'papers')
    os.makedirs(papers_dir, exist_ok=True)
    
    for url, filename in test_urls:
        try:
            print(f"  Trying: {url}")
            response = requests.get(url, timeout=30, allow_redirects=True)
            if response.status_code == 200 and response.content.startswith(b'%PDF'):
                pdf_path = os.path.join(papers_dir, filename)
                with open(pdf_path, 'wb') as f:
                    f.write(response.content)
                print(f"  ✓ Downloaded: {filename}")
                return pdf_path
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            continue
    
    print("  ✗ Could not download a sample paper")
    return None

if __name__ == '__main__':
    success = test_grobid_with_sample()
    sys.exit(0 if success else 1)
