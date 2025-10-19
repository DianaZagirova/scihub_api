#!/usr/bin/env python3
"""
Clean invalid and empty JSON files from /output/ directory.
- Move invalid/empty JSONs to ./invalid_jsons/
- Update tracker to mark parsing as failed
- Log all actions for review
"""

import json
import shutil
from pathlib import Path
from datetime import datetime
from doi_tracker import DOITracker
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path('./output')
INVALID_DIR = Path('./invalid_jsons')
TRACKER_FILE = 'doi_processing_tracker.csv'

def filename_to_doi(filename):
    """Convert filename to DOI."""
    name = filename.replace('.json', '').replace('_fast', '')
    return name.replace('_', '/')

def check_json_validity(json_path):
    """
    Check if JSON file is valid and has content.
    
    Returns:
        (is_valid, has_content, error_msg)
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Check if it has meaningful content
        if json_path.name.endswith('_fast.json'):
            # PyMuPDF JSON
            sections = data.get('structured_text', {}).get('sections', [])
            has_content = len(sections) > 0 and any(
                section.get('content') for section in sections
            )
            parser = 'pymupdf'
        else:
            # Grobid JSON
            body = data.get('full_text', {}).get('body', [])
            has_content = len(body) > 0 and any(
                section.get('content') for section in body
            )
            parser = 'grobid'
        
        if not has_content:
            return True, False, 'Empty content (no text extracted)', parser
        
        return True, True, None, parser
        
    except json.JSONDecodeError as e:
        # Determine parser from filename
        parser = 'pymupdf' if json_path.name.endswith('_fast.json') else 'grobid'
        return False, False, f'Invalid JSON: {str(e)}', parser
    except Exception as e:
        parser = 'pymupdf' if json_path.name.endswith('_fast.json') else 'grobid'
        return False, False, f'Error reading file: {str(e)}', parser

def main():
    print('='*70)
    print('CLEANING INVALID/EMPTY JSON FILES')
    print('='*70)
    
    # Create invalid directory
    INVALID_DIR.mkdir(exist_ok=True)
    logger.info(f"Invalid JSONs will be moved to: {INVALID_DIR}")
    
    # Create log file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = INVALID_DIR / f'cleanup_log_{timestamp}.txt'
    
    # Initialize tracker
    tracker = DOITracker(TRACKER_FILE)
    
    # Scan output directory
    print('\n📁 Scanning /output/ directory...')
    
    if not OUTPUT_DIR.exists():
        print('   ❌ Output directory not found!')
        return 1
    
    json_files = list(OUTPUT_DIR.glob('*.json'))
    print(f'   ✓ Found {len(json_files):,} JSON files')
    
    # Check each JSON
    print('\n🔍 Checking JSON files...')
    
    stats = {
        'total': len(json_files),
        'valid': 0,
        'invalid': 0,
        'empty': 0,
        'moved': 0,
        'tracker_updated': 0,
        'errors': 0
    }
    
    issues = []
    
    for json_file in json_files:
        is_valid, has_content, error_msg, parser = check_json_validity(json_file)
        
        if is_valid and has_content:
            stats['valid'] += 1
            continue
        
        # Found an issue
        doi = filename_to_doi(json_file.name)
        
        if not is_valid:
            stats['invalid'] += 1
            issue_type = 'INVALID'
        else:
            stats['empty'] += 1
            issue_type = 'EMPTY'
        
        issues.append({
            'doi': doi,
            'file': json_file,
            'parser': parser,
            'type': issue_type,
            'error': error_msg
        })
    
    print(f'   ✓ Valid JSONs: {stats["valid"]:,}')
    print(f'   ⚠️  Invalid JSONs: {stats["invalid"]:,}')
    print(f'   ⚠️  Empty JSONs: {stats["empty"]:,}')
    
    if not issues:
        print('\n✅ No invalid or empty JSON files found!')
        return 0
    
    # Process issues
    print(f'\n🔧 Processing {len(issues)} problematic JSON files...')
    
    with open(log_file, 'w') as log:
        log.write(f'JSON Cleanup Log - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
        log.write('='*70 + '\n\n')
        
        for issue in issues:
            doi = issue['doi']
            json_file = issue['file']
            parser = issue['parser']
            issue_type = issue['type']
            error_msg = issue['error']
            
            # Log the issue
            log.write(f'DOI: {doi}\n')
            log.write(f'File: {json_file.name}\n')
            log.write(f'Parser: {parser}\n')
            log.write(f'Issue: {issue_type}\n')
            log.write(f'Error: {error_msg}\n')
            
            try:
                # Move file to invalid directory
                dest = INVALID_DIR / json_file.name
                shutil.move(str(json_file), str(dest))
                stats['moved'] += 1
                log.write(f'Action: Moved to {dest}\n')
                
                # Update tracker
                status_field = f'{parser}_status'
                date_field = f'{parser}_date'
                
                update_data = {
                    'doi': doi,
                    status_field: tracker.STATUS_FAILED,
                    date_field: datetime.now().isoformat(),
                    'error_msg': f'{parser}: {error_msg}',
                    'last_updated': datetime.now().isoformat()
                }
                
                # Increment retry count
                current_status = tracker.get_status(doi)
                if current_status:
                    retry_count = int(current_status.get('retry_count', 0) or 0)
                    update_data['retry_count'] = str(retry_count + 1)
                
                tracker.update_status(update_data)
                stats['tracker_updated'] += 1
                log.write(f'Tracker: Updated {status_field} to failed\n')
                
                logger.info(f'Processed {doi} ({parser}): {issue_type}')
                
            except Exception as e:
                stats['errors'] += 1
                log.write(f'ERROR: {str(e)}\n')
                logger.error(f'Error processing {doi}: {e}')
            
            log.write('\n' + '-'*70 + '\n\n')
    
    # Flush tracker updates
    tracker.flush()
    
    # Summary
    print('\n' + '='*70)
    print('SUMMARY')
    print('='*70)
    print(f'Total JSON files scanned: {stats["total"]:,}')
    print(f'  Valid: {stats["valid"]:,}')
    print(f'  Invalid: {stats["invalid"]:,}')
    print(f'  Empty: {stats["empty"]:,}')
    print(f'\nActions:')
    print(f'  Files moved to {INVALID_DIR}: {stats["moved"]:,}')
    print(f'  Tracker entries updated: {stats["tracker_updated"]:,}')
    print(f'  Errors: {stats["errors"]:,}')
    print(f'\nLog file: {log_file}')
    print('='*70)
    
    if stats['moved'] > 0:
        print('\n✅ Cleanup complete!')
        print(f'   {stats["moved"]} invalid/empty JSON files moved to {INVALID_DIR}')
        print(f'   Tracker updated to mark parsing as failed')
        print(f'\n📝 Review log file for details: {log_file}')
    
    return 0

if __name__ == '__main__':
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print(f'\n❌ Error: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
