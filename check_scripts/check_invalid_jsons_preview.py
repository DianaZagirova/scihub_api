#!/usr/bin/env python3
"""
Preview what clean_invalid_jsons.py would do (dry run).
Shows which files would be moved without actually moving them.
"""

import json
from pathlib import Path
from collections import defaultdict

OUTPUT_DIR = Path('./output')

def filename_to_doi(filename):
    """Convert filename to DOI."""
    name = filename.replace('.json', '').replace('_fast', '')
    return name.replace('_', '/')

def check_json_validity(json_path):
    """Check if JSON file is valid and has content."""
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
            return True, False, 'Empty content', parser
        
        return True, True, None, parser
        
    except json.JSONDecodeError as e:
        parser = 'pymupdf' if json_path.name.endswith('_fast.json') else 'grobid'
        return False, False, f'Invalid JSON: {str(e)[:50]}', parser
    except Exception as e:
        parser = 'pymupdf' if json_path.name.endswith('_fast.json') else 'grobid'
        return False, False, f'Error: {str(e)[:50]}', parser

def main():
    print('='*70)
    print('PREVIEW: INVALID/EMPTY JSON FILES')
    print('='*70)
    
    if not OUTPUT_DIR.exists():
        print('   ❌ Output directory not found!')
        return 1
    
    json_files = list(OUTPUT_DIR.glob('*.json'))
    print(f'\n📁 Scanning {len(json_files):,} JSON files...')
    
    stats = defaultdict(int)
    issues_by_type = {
        'invalid_grobid': [],
        'invalid_pymupdf': [],
        'empty_grobid': [],
        'empty_pymupdf': []
    }
    
    for json_file in json_files:
        is_valid, has_content, error_msg, parser = check_json_validity(json_file)
        
        if is_valid and has_content:
            stats['valid'] += 1
            continue
        
        # Found an issue
        doi = filename_to_doi(json_file.name)
        
        if not is_valid:
            stats[f'invalid_{parser}'] += 1
            issues_by_type[f'invalid_{parser}'].append({
                'doi': doi,
                'file': json_file.name,
                'error': error_msg
            })
        else:
            stats[f'empty_{parser}'] += 1
            issues_by_type[f'empty_{parser}'].append({
                'doi': doi,
                'file': json_file.name,
                'error': error_msg
            })
    
    # Summary
    print('\n' + '='*70)
    print('SUMMARY')
    print('='*70)
    print(f'Total JSON files: {len(json_files):,}')
    print(f'  ✅ Valid with content: {stats["valid"]:,}')
    print(f'\nProblematic files:')
    print(f'  ❌ Invalid Grobid JSONs: {stats["invalid_grobid"]}')
    print(f'  ❌ Invalid PyMuPDF JSONs: {stats["invalid_pymupdf"]}')
    print(f'  ⚠️  Empty Grobid JSONs: {stats["empty_grobid"]}')
    print(f'  ⚠️  Empty PyMuPDF JSONs: {stats["empty_pymupdf"]}')
    
    total_issues = (stats['invalid_grobid'] + stats['invalid_pymupdf'] + 
                   stats['empty_grobid'] + stats['empty_pymupdf'])
    
    if total_issues == 0:
        print('\n✅ No invalid or empty JSON files found!')
        return 0
    
    # Show examples
    print('\n' + '='*70)
    print('EXAMPLES (first 5 of each type)')
    print('='*70)
    
    for issue_type, issues in issues_by_type.items():
        if issues:
            parser, status = issue_type.split('_', 1)
            print(f'\n{status.upper()} {parser.upper()} JSONs ({len(issues)} total):')
            for item in issues[:5]:
                print(f'  - {item["file"]}')
                print(f'    DOI: {item["doi"]}')
                print(f'    Error: {item["error"]}')
            if len(issues) > 5:
                print(f'  ... and {len(issues) - 5} more')
    
    # What would happen
    print('\n' + '='*70)
    print('ACTIONS THAT WOULD BE TAKEN')
    print('='*70)
    print(f'1. Create directory: ./invalid_jsons/')
    print(f'2. Move {total_issues} files to ./invalid_jsons/')
    print(f'3. Update tracker for {total_issues} DOIs:')
    print(f'   - Set parser_status to "failed"')
    print(f'   - Add error message')
    print(f'   - Increment retry_count')
    print(f'4. Create cleanup log in ./invalid_jsons/')
    
    print('\n📝 To execute cleanup, run:')
    print('   python clean_invalid_jsons.py')
    print('='*70)
    
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
