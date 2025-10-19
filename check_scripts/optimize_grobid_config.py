#!/usr/bin/env python3
"""
Quick script to optimize Grobid configuration for maximum speed.
"""

import json
import shutil
from datetime import datetime

CONFIG_FILE = 'config.json'

def backup_config():
    """Backup current config."""
    backup_name = f'config.json.backup.{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    shutil.copy(CONFIG_FILE, backup_name)
    print(f"✓ Backed up config to {backup_name}")
    return backup_name

def optimize_config():
    """Apply optimization settings."""
    print("="*70)
    print("OPTIMIZING GROBID CONFIGURATION")
    print("="*70)
    
    # Backup first
    backup = backup_config()
    
    # Load current config
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
    
    print("\nCurrent Settings:")
    print(f"  sleep_time: {config.get('sleep_time', 'N/A')}")
    print(f"  max_workers: {config.get('max_workers', 'N/A')}")
    print(f"  consolidate_header: {config.get('consolidate_header', 'N/A')}")
    print(f"  timeout: {config.get('timeout', 'N/A')}")
    
    # Apply optimizations
    changes = []
    
    # 1. Remove sleep time (HUGE win - saves ~48 hours!)
    if config.get('sleep_time', 0) > 0:
        old_val = config['sleep_time']
        config['sleep_time'] = 0
        changes.append(f"sleep_time: {old_val} → 0 (saves ~48 hours!)")
    
    # 2. Increase workers (if reasonable)
    if config.get('max_workers', 4) < 12:
        old_val = config.get('max_workers', 4)
        config['max_workers'] = 12
        changes.append(f"max_workers: {old_val} → 12 (3x faster)")
    
    # 3. Disable consolidation (already done, but verify)
    if config.get('consolidate_header', 1) != 0:
        old_val = config['consolidate_header']
        config['consolidate_header'] = 0
        changes.append(f"consolidate_header: {old_val} → 0 (2-3x faster)")
    
    # 4. Reduce timeout for faster failure
    if config.get('timeout', 180) > 90:
        old_val = config['timeout']
        config['timeout'] = 90
        changes.append(f"timeout: {old_val} → 90 (fail faster)")
    
    # Save optimized config
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)
    
    print("\nOptimizations Applied:")
    for change in changes:
        print(f"  ✓ {change}")
    
    if not changes:
        print("  ✓ Config already optimized!")
    
    print("\n" + "="*70)
    print("ESTIMATED PERFORMANCE")
    print("="*70)
    print(f"\nWith optimized settings:")
    print(f"  Workers: {config['max_workers']}")
    print(f"  Sleep: {config['sleep_time']}s")
    print(f"  Consolidation: {'disabled' if config['consolidate_header'] == 0 else 'enabled'}")
    print(f"\nExpected throughput:")
    print(f"  ~24-40 papers/minute")
    print(f"  34,657 papers in ~14-24 hours")
    print(f"\nTo restore:")
    print(f"  mv {backup} {CONFIG_FILE}")
    print("\n" + "="*70)

if __name__ == '__main__':
    optimize_config()
