#!/usr/bin/env python3
"""Test GROBID GPU usage by processing papers."""

import sys
import time
from pathlib import Path

# Add legacy to path
sys.path.insert(0, str(Path(__file__).parent / 'legacy'))

from scihub_grobid_downloader import SciHubGrobidDownloader

def main():
    print("="*70)
    print("GROBID GPU LOAD TEST")
    print("="*70)
    print()
    print("This will process 3 papers with GROBID.")
    print("Open another terminal and run: watch -n 1 nvidia-smi")
    print("Watch for GPU memory increase during processing!")
    print()
    print("Waiting 5 seconds for you to start nvidia-smi...")
    time.sleep(5)
    
    print("\n" + "="*70)
    print("Starting GROBID processing...")
    print("="*70)
    
    # Create downloader
    downloader = SciHubGrobidDownloader()
    
    # Test with the papers we already have downloaded
    test_identifiers = [
        "10.1016/j.arr.2016.06.005",
        "10.1038/nature12373",
        "10.1126/science.1242072"
    ]
    
    print(f"\nProcessing {len(test_identifiers)} papers...")
    print("WATCH nvidia-smi NOW - GPU memory should increase!\n")
    
    for i, identifier in enumerate(test_identifiers, 1):
        print(f"\n[{i}/{len(test_identifiers)}] Processing: {identifier}")
        start = time.time()
        
        pdf_path, extracted_data, status = downloader.download_and_process(identifier)
        
        elapsed = time.time() - start
        print(f"  Status: {status}")
        print(f"  Time: {elapsed:.2f}s")
        
        if status == 'success':
            print(f"  ✓ Successfully processed")
        else:
            print(f"  ✗ Failed: {status}")
    
    print("\n" + "="*70)
    print("Test Complete!")
    print("="*70)
    print("\nDid you see GPU memory increase in nvidia-smi?")
    print("  YES → GROBID is using GPU ✓")
    print("  NO  → GROBID is using CPU only (see GROBID_GPU_SETUP.md)")
    print()

if __name__ == "__main__":
    main()
