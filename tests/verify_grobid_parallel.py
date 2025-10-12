#!/usr/bin/env python3
"""Verify GROBID parallel processing and configuration."""

import requests
import time
import sys
from concurrent.futures import ThreadPoolExecutor

# Configuration
GROBID_URL = "http://10.223.131.158:8072"  # Update with your GROBID server


def check_grobid_alive():
    """Check if GROBID is running."""
    print("Checking GROBID server...")
    try:
        r = requests.get(f"{GROBID_URL}/api/isalive", timeout=5)
        if r.status_code == 200:
            print(f"✓ GROBID server is running at {GROBID_URL}")
            return True
        else:
            print(f"✗ GROBID server returned status {r.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print(f"✗ Cannot connect to GROBID at {GROBID_URL}")
        print("  Make sure GROBID is running:")
        print("  cd /path/to/grobid && ./gradlew run")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def check_grobid_version():
    """Check GROBID version."""
    try:
        r = requests.get(f"{GROBID_URL}/api/version", timeout=5)
        if r.status_code == 200:
            print(f"✓ GROBID version: {r.text.strip()}")
            return True
    except:
        pass
    return False


def test_concurrent_requests(num_workers=4, num_requests=8):
    """Test concurrent GROBID requests."""
    print(f"\nTesting {num_workers} concurrent workers with {num_requests} total requests...")
    
    def make_request(i):
        """Make a test request to GROBID."""
        start = time.time()
        try:
            r = requests.get(f"{GROBID_URL}/api/isalive", timeout=10)
            elapsed = time.time() - start
            return {
                'success': r.status_code == 200,
                'time': elapsed,
                'request_id': i
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'request_id': i
            }
    
    # Measure sequential time
    print(f"\n  Sequential baseline (1 worker)...")
    start = time.time()
    for i in range(min(4, num_requests)):
        make_request(i)
    sequential_time = time.time() - start
    print(f"  Time for {min(4, num_requests)} requests: {sequential_time:.2f}s")
    
    # Measure parallel time
    print(f"\n  Parallel test ({num_workers} workers)...")
    start = time.time()
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(make_request, i) for i in range(num_requests)]
        results = [f.result() for f in futures]
    parallel_time = time.time() - start
    
    # Analyze results
    successful = [r for r in results if r.get('success', False)]
    failed = [r for r in results if not r.get('success', False)]
    
    print(f"\n  Results:")
    print(f"    Total requests: {num_requests}")
    print(f"    Successful: {len(successful)}")
    print(f"    Failed: {len(failed)}")
    print(f"    Time: {parallel_time:.2f}s")
    
    if len(successful) > 0:
        avg_time = sum(r['time'] for r in successful) / len(successful)
        print(f"    Avg response time: {avg_time:.3f}s")
    
    # Calculate speedup
    if sequential_time > 0 and parallel_time > 0:
        expected_speedup = min(4, num_requests) / parallel_time * sequential_time
        print(f"\n  Speedup: {expected_speedup:.2f}x")
        print(f"  Expected: ~{num_workers}x for {num_workers} workers")
        
        if expected_speedup >= num_workers * 0.7:
            print(f"  ✓ Parallel processing is working well!")
            return True
        elif expected_speedup >= 2:
            print(f"  ⚠ Parallel processing is working but not optimal")
            print(f"     Check GROBID concurrency settings")
            return True
        else:
            print(f"  ✗ Parallel processing may not be working")
            print(f"     Speedup is too low")
            return False
    
    return len(successful) == num_requests


def check_grobid_config_suggestions():
    """Provide configuration suggestions."""
    print("\n" + "="*60)
    print("GROBID Configuration Recommendations")
    print("="*60)
    
    print("\n1. GROBID Server Config (grobid.yaml):")
    print("   grobid:")
    print("     concurrency: 8")
    print("     poolSize: 8")
    print("     jvmOptions: \"-Xmx8G\"")
    
    print("\n2. Application Config (config.json):")
    print("   {")
    print('     "max_workers": 8,')
    print('     "grobid_server": "' + GROBID_URL + '",')
    print('     "timeout": 300')
    print("   }")
    
    print("\n3. For GPU acceleration:")
    print("   grobid:")
    print("     delft:")
    print("       enabled: true")
    print("       use_gpu: true")
    print("       gpu_device: 0")
    
    print("\n4. Verify GPU usage:")
    print("   nvidia-smi -l 1")
    print("   (Watch GPU memory while processing)")


def main():
    """Main verification function."""
    print("="*60)
    print("GROBID Parallel Processing Verification")
    print("="*60)
    print(f"\nServer: {GROBID_URL}")
    print()
    
    # Step 1: Check if GROBID is running
    if not check_grobid_alive():
        print("\n✗ GROBID server is not accessible!")
        print("\nTo start GROBID:")
        print("  cd /path/to/grobid")
        print("  ./gradlew run")
        sys.exit(1)
    
    # Step 2: Check version
    check_grobid_version()
    
    # Step 3: Test concurrent requests
    print("\n" + "="*60)
    print("Testing Parallel Processing")
    print("="*60)
    
    result = test_concurrent_requests(num_workers=4, num_requests=8)
    
    # Step 4: Configuration suggestions
    check_grobid_config_suggestions()
    
    # Summary
    print("\n" + "="*60)
    if result:
        print("✓ GROBID is configured for parallel processing!")
        print("="*60)
        print("\nYou can now use:")
        print("  python parallel_download.py -f dois.txt -w 4 --parser grobid")
    else:
        print("⚠ GROBID may need configuration for optimal parallel processing")
        print("="*60)
        print("\nReview the recommendations above and restart GROBID")
    
    sys.exit(0 if result else 1)


if __name__ == "__main__":
    main()
