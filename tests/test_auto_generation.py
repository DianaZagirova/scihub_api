#!/usr/bin/env python3
"""
Test script to verify auto-generation of DOI file works
"""
import os
import sys
import datetime
import glob

# Test the auto-generation logic
print("Testing auto-generation of DOI file...")
print("="*60)

# Check if the create script exists
create_script = 'missing_dois/cerate_missing_eval.py'
if not os.path.exists(create_script):
    print(f"✗ Script not found: {create_script}")
    sys.exit(1)

print(f"✓ Found script: {create_script}")

# Simulate what download_papers_optimized.py does
timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
auto_file = f'missing_dois/dois_to_process_{timestamp}.txt'

print(f"\nGenerating timestamped file: {auto_file}")

try:
    # Read the original script
    with open(create_script, 'r') as f:
        script_content = f.read()
    
    # Replace the output filename with timestamped version
    modified_script = script_content.replace(
        "out = 'missing_dois/dois_to_process.txt'",
        f"out = '{auto_file}'"
    )
    
    print("Executing script...")
    # Execute the modified script
    exec(modified_script, {'__name__': '__main__'})
    
    if not os.path.exists(auto_file):
        print(f"✗ Failed to create {auto_file}")
        sys.exit(1)
    
    # Check file contents
    with open(auto_file, 'r') as f:
        dois = [line.strip() for line in f if line.strip()]
    
    print(f"✓ Created {auto_file}")
    print(f"✓ Contains {len(dois)} DOIs")
    
    if dois:
        print(f"\nFirst 5 DOIs:")
        for doi in dois[:5]:
            print(f"  - {doi}")
    
    # List all timestamped files
    print(f"\nAll timestamped DOI files:")
    pattern = 'missing_dois/dois_to_process_*.txt'
    timestamped_files = sorted(glob.glob(pattern))
    for f in timestamped_files:
        size = os.path.getsize(f)
        print(f"  - {f} ({size} bytes)")
    
    print("\n" + "="*60)
    print("✓ Auto-generation test PASSED")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
