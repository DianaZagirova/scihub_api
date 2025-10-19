#!/usr/bin/env python3
"""Test DOITracker loading behavior."""

import csv
from pathlib import Path

TRACKER_FILE = 'doi_processing_tracker.csv'

print('Test 1: Direct CSV reading')
print('=' * 70)
with open(TRACKER_FILE, 'r', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    count = 0
    for row in reader:
        count += 1
print(f'Rows read: {count}')

print('\nTest 2: Simulating DOITracker loading')
print('=' * 70)
cache = {}
with open(TRACKER_FILE, 'r', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        doi = row['doi']
        cache[doi] = dict(row)
print(f'Cache size: {len(cache)}')

print('\nTest 3: Actual DOITracker')
print('=' * 70)
from doi_tracker import DOITracker

# Force a fresh load
tracker = DOITracker.__new__(DOITracker)
tracker.tracker_file = Path(TRACKER_FILE)
import threading
tracker.lock = threading.Lock()
tracker._cache = {}
tracker._cache_loaded = False

# Now load
tracker._load_cache()
print(f'DOITracker cache size: {len(tracker._cache)}')

# Check if there are duplicate DOIs
print('\nTest 4: Check for duplicates in file')
print('=' * 70)
dois = []
with open(TRACKER_FILE, 'r', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        dois.append(row['doi'])

unique_dois = set(dois)
print(f'Total DOIs in file: {len(dois)}')
print(f'Unique DOIs: {len(unique_dois)}')
if len(dois) != len(unique_dois):
    print(f'Duplicates: {len(dois) - len(unique_dois)}')
