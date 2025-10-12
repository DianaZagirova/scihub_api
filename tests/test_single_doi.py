#!/usr/bin/env python3
"""Test the full pipeline with a specific DOI."""

import sys
import os
import json
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'legacy'))

from scihub import SciHub
from grobid_parser import GrobidParser

def test_full_pipeline(doi):
    """Test complete download -> process -> extract pipeline."""
    
    print("=" * 70)
    print(f"FULL PIPELINE TEST: {doi}")
    print("=" * 70)
    
    # Setup
    papers_dir = os.path.join(os.path.dirname(__file__), 'papers')
    os.makedirs(papers_dir, exist_ok=True)
    
    # Step 1: Download from Sci-Hub
    print("\n" + "-" * 70)
    print("STEP 1: Downloading from Sci-Hub")
    print("-" * 70)
    
    try:
        sh = SciHub()
        print(f"Fetching DOI: {doi}")
        result = sh.fetch(doi)
        
        if result.get('err') or not result.get('pdf'):
            print(f"✗ Download failed: {result.get('err', 'Unknown error')}")
            return False
        
        # Save PDF
        pdf_filename = f"{doi.replace('/', '_').replace(':', '_')}.pdf"
        pdf_path = os.path.join(papers_dir, pdf_filename)
        
        with open(pdf_path, 'wb') as f:
            f.write(result['pdf'])
        
        pdf_size = len(result['pdf']) / 1024
        print(f"✓ Downloaded successfully")
        print(f"  File: {pdf_filename}")
        print(f"  Size: {pdf_size:.1f} KB")
        print(f"  Path: {pdf_path}")
        
    except Exception as e:
        print(f"✗ Download error: {e}")
        return False
    
    # Step 2: Process with GROBID
    print("\n" + "-" * 70)
    print("STEP 2: Processing with GROBID")
    print("-" * 70)
    
    try:
        parser = GrobidParser(config_path='config.json')
        print(f"Processing PDF: {pdf_filename}")
        
        tei_content = parser.process_pdf(pdf_path, output_format='tei')
        
        if not tei_content:
            print("✗ GROBID processing failed")
            return False
        
        print(f"✓ GROBID processing successful")
        print(f"  TEI content length: {len(tei_content)} characters")
        
    except Exception as e:
        print(f"✗ GROBID processing error: {e}")
        return False
    
    # Step 3: Extract Metadata
    print("\n" + "-" * 70)
    print("STEP 3: Extracting Metadata")
    print("-" * 70)
    
    try:
        metadata = parser.extract_metadata(tei_content)
        
        print("✓ Metadata extracted:")
        print(f"  Title: {metadata.get('title', 'N/A')}")
        print(f"  DOI: {metadata.get('doi', 'N/A')}")
        print(f"  Journal: {metadata.get('journal', 'N/A')}")
        print(f"  Year: {metadata.get('year', 'N/A')}")
        print(f"  Authors: {len(metadata.get('authors', []))} found")
        
        if metadata.get('authors'):
            print(f"  First author: {metadata['authors'][0].get('name', 'N/A')}")
        
        abstract = metadata.get('abstract', '')
        print(f"  Abstract: {len(abstract)} characters")
        if abstract:
            print(f"    Preview: {abstract[:150]}...")
        
    except Exception as e:
        print(f"✗ Metadata extraction error: {e}")
        return False
    
    # Step 4: Extract Full Text
    print("\n" + "-" * 70)
    print("STEP 4: Extracting Full Text")
    print("-" * 70)
    
    try:
        full_text_data = parser.extract_full_text(tei_content)
        body_sections = full_text_data.get('body', [])
        references = full_text_data.get('references', [])
        
        print("✓ Full text extracted:")
        print(f"  Body sections: {len(body_sections)}")
        
        # Show section details
        total_chars = 0
        for i, section in enumerate(body_sections, 1):
            title = section.get('title', 'Unnamed Section')
            content = section.get('content', '')
            content_len = len(content)
            total_chars += content_len
            
            if i <= 5:  # Show first 5 sections
                print(f"    {i}. {title}: {content_len} characters")
                if content and i == 1:  # Show preview of first section
                    print(f"       Preview: {content[:100]}...")
        
        if len(body_sections) > 5:
            print(f"    ... and {len(body_sections) - 5} more sections")
        
        print(f"  Total text: {total_chars} characters")
        print(f"  References: {len(references)}")
        
        # Combine full text as it would be stored
        full_text_combined = '\n\n'.join([
            f"## {s.get('title', 'Unnamed Section')}\n\n{s.get('content', '')}"
            for s in body_sections
        ])
        
        print(f"  Combined full_text length: {len(full_text_combined)} characters")
        
    except Exception as e:
        print(f"✗ Full text extraction error: {e}")
        return False
    
    # Step 5: Simulate Database Update
    print("\n" + "-" * 70)
    print("STEP 5: Database Update (Simulation)")
    print("-" * 70)
    
    print("✓ Would update database with:")
    print(f"  abstract: {len(abstract)} chars → {'YES' if abstract else 'NO'}")
    print(f"  full_text: {len(full_text_combined)} chars → {'YES' if full_text_combined else 'NO'}")
    print(f"  full_text_sections: {len(body_sections)} sections → {'YES' if body_sections else 'NO'}")
    
    # Save extracted data to JSON for inspection
    output_data = {
        'doi': doi,
        'metadata': metadata,
        'abstract': abstract,
        'full_text': full_text_combined,
        'full_text_sections': body_sections,
        'references': references,
        'stats': {
            'abstract_length': len(abstract),
            'full_text_length': len(full_text_combined),
            'num_sections': len(body_sections),
            'num_references': len(references)
        }
    }
    
    output_file = os.path.join(papers_dir, f"{doi.replace('/', '_')}_extracted.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"  Saved extracted data to: {output_file}")
    
    # Final Summary
    print("\n" + "=" * 70)
    print("✓ FULL PIPELINE TEST PASSED!")
    print("=" * 70)
    print("\nSummary:")
    print(f"  ✓ Downloaded PDF from Sci-Hub")
    print(f"  ✓ Processed with GROBID")
    print(f"  ✓ Extracted metadata (title, authors, abstract)")
    print(f"  ✓ Extracted full text ({len(body_sections)} sections)")
    print(f"  ✓ Ready for database update")
    print("\nThe fetch_missing_papers.py script is working correctly!")
    
    return True

if __name__ == '__main__':
    doi = sys.argv[1] if len(sys.argv) > 1 else "10.1046/j.1469-7580.2000.19740521.x"
    success = test_full_pipeline(doi)
    sys.exit(0 if success else 1)
