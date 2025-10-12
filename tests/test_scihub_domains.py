#!/usr/bin/env python3
"""
Test Sci-Hub Domains
--------------------
Quickly test which Sci-Hub domains are currently working.
"""

import requests
import sys

SCIHUB_DOMAINS = [
    'https://sci-hub.wf',
    'https://sci-hub.se',
    'https://sci-hub.st',
    'https://sci-hub.ru',
    'https://sci-hub.ee',
    'https://sci-hub.ren',
    'https://sci-hub.sh',
    'https://sci-hub.tw',
    'https://sci-hub.tf',
    'https://sci-hub.nz',
]

def test_domain(domain):
    """Test if a Sci-Hub domain is accessible."""
    try:
        response = requests.get(domain, timeout=5, allow_redirects=True)
        if response.status_code == 200:
            return True, response.status_code
        else:
            return False, response.status_code
    except requests.exceptions.Timeout:
        return False, "Timeout"
    except requests.exceptions.RequestException as e:
        return False, str(e)[:50]

def main():
    print("Testing Sci-Hub Domains...")
    print("=" * 60)
    
    working_domains = []
    failed_domains = []
    
    for domain in SCIHUB_DOMAINS:
        print(f"\nTesting: {domain}", end=" ... ")
        is_working, status = test_domain(domain)
        
        if is_working:
            print(f"✓ WORKING (Status: {status})")
            working_domains.append(domain)
        else:
            print(f"✗ FAILED ({status})")
            failed_domains.append((domain, status))
    
    print("\n" + "=" * 60)
    print(f"\nWorking Domains ({len(working_domains)}):")
    if working_domains:
        for domain in working_domains:
            print(f"  ✓ {domain}")
    else:
        print("  None found!")
    
    print(f"\nFailed Domains ({len(failed_domains)}):")
    for domain, status in failed_domains:
        print(f"  ✗ {domain} - {status}")
    
    print("\n" + "=" * 60)
    
    if working_domains:
        print(f"\nRecommendation: Use {working_domains[0]}")
        return 0
    else:
        print("\nWarning: No working Sci-Hub domains found!")
        print("This could be due to:")
        print("  - Network connectivity issues")
        print("  - ISP/firewall blocking Sci-Hub")
        print("  - All domains temporarily down")
        return 1

if __name__ == "__main__":
    sys.exit(main())
