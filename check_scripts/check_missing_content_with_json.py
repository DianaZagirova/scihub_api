#!/usr/bin/env python3
"""
Check for papers in database that are missing full_text/full_text_sections
but have valid JSON files in /output/ directory.
"""

import sqlite3
import json
from pathlib import Path
from collections import defaultdict

DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
OUTPUT_DIR = Path('./output')

def filename_to_doi(filename):
    """Convert filename to DOI."""
    name = filename.replace('.json', '').replace('_fast', '')
    return name.replace('_', '/')

def doi_to_filename(doi):
    """Convert DOI to filename."""
    return doi.replace('/', '_')

def check_json_validity(json_path):
    """Check if JSON file is valid and has content."""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Check if it has meaningful content
        if json_path.name.endswith('_fast.json'):
            # PyMuPDF JSON
            sections = data.get('structured_text', {}).get('sections', [])
            has_content = len(sections) > 0
        else:
            # Grobid JSON
            body = data.get('full_text', {}).get('body', [])
            has_content = len(body) > 0
        
        return True, has_content
    except Exception as e:
        return False, False

def main():
    print('='*70)
    print('CHECKING FOR MISSING CONTENT WITH VALID JSON')
    print('='*70)
    
    # 1. Get papers missing content from database
    print('\n📊 Loading papers missing content from database...')
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT doi, full_text, full_text_sections
        FROM papers 
        WHERE doi IS NOT NULL AND doi != ''
        AND (
            (full_text IS NULL OR full_text = '')
            AND (full_text_sections IS NULL OR full_text_sections = '')
        )
    """)
    
    missing_content = {}
    for doi, full_text, full_text_sections in cursor.fetchall():
        missing_content[doi] = {
            'full_text': full_text,
            'full_text_sections': full_text_sections
        }
    
    conn.close()
    
    print(f'   ✓ Found {len(missing_content):,} papers missing content')
    
    # 2. Scan output directory for JSON files
    print('\n📁 Scanning /output/ directory...')
    
    grobid_jsons = {}
    pymupdf_jsons = {}
    
    if not OUTPUT_DIR.exists():
        print('   ❌ Output directory not found!')
        return 1
    
    for json_file in OUTPUT_DIR.glob('*.json'):
        doi = filename_to_doi(json_file.name)
        
        if json_file.name.endswith('_fast.json'):
            pymupdf_jsons[doi] = json_file
        else:
            grobid_jsons[doi] = json_file
    
    print(f'   ✓ Found {len(grobid_jsons):,} Grobid JSONs')
    print(f'   ✓ Found {len(pymupdf_jsons):,} PyMuPDF JSONs')
    
    # 3. Find papers missing content but have JSON
    print('\n🔍 Finding papers with JSON but missing DB content...')
    
    issues = {
        'has_grobid': [],
        'has_pymupdf': [],
        'has_both': [],
        'json_invalid': [],
        'json_empty': []
    }
    
    for doi in missing_content.keys():
        has_grobid = doi in grobid_jsons
        has_pymupdf = doi in pymupdf_jsons
        
        if has_grobid or has_pymupdf:
            # Check JSON validity
            if has_grobid:
                valid, has_content = check_json_validity(grobid_jsons[doi])
                if not valid:
                    issues['json_invalid'].append({
                        'doi': doi,
                        'json_file': grobid_jsons[doi],
                        'parser': 'grobid'
                    })
                    continue
                elif not has_content:
                    issues['json_empty'].append({
                        'doi': doi,
                        'json_file': grobid_jsons[doi],
                        'parser': 'grobid'
                    })
                    continue
            
            if has_pymupdf:
                valid, has_content = check_json_validity(pymupdf_jsons[doi])
                if not valid:
                    issues['json_invalid'].append({
                        'doi': doi,
                        'json_file': pymupdf_jsons[doi],
                        'parser': 'pymupdf'
                    })
                    continue
                elif not has_content:
                    issues['json_empty'].append({
                        'doi': doi,
                        'json_file': pymupdf_jsons[doi],
                        'parser': 'pymupdf'
                    })
                    continue
            
            # Valid JSON with content
            if has_grobid and has_pymupdf:
                issues['has_both'].append(doi)
            elif has_grobid:
                issues['has_grobid'].append(doi)
            elif has_pymupdf:
                issues['has_pymupdf'].append(doi)
    
    # 4. Report findings
    print('\n' + '='*70)
    print('FINDINGS')
    print('='*70)
    
    total_with_json = len(issues['has_grobid']) + len(issues['has_pymupdf']) + len(issues['has_both'])
    
    print(f'\nPapers missing content in DB: {len(missing_content):,}')
    print(f'\nPapers with VALID JSON but missing DB content: {total_with_json:,}')
    print(f'  - Has Grobid JSON only: {len(issues["has_grobid"]):,}')
    print(f'  - Has PyMuPDF JSON only: {len(issues["has_pymupdf"]):,}')
    print(f'  - Has both JSONs: {len(issues["has_both"]):,}')
    
    print(f'\nJSON file issues:')
    print(f'  - Invalid JSON (parse error): {len(issues["json_invalid"]):,}')
    print(f'  - Empty JSON (no content): {len(issues["json_empty"]):,}')
    
    # Show examples
    if total_with_json > 0:
        print('\n⚠️  ISSUE: Papers have valid JSON but content not in database!')
        print('\nExamples (first 10):')
        
        examples = (issues['has_both'][:5] + 
                   issues['has_grobid'][:5] + 
                   issues['has_pymupdf'][:5])[:10]
        
        for doi in examples:
            has_g = 'Grobid' if doi in grobid_jsons else ''
            has_p = 'PyMuPDF' if doi in pymupdf_jsons else ''
            parsers = ' + '.join(filter(None, [has_g, has_p]))
            print(f'  - {doi} ({parsers})')
        
        print('\n📝 RECOMMENDED ACTION:')
        print('   Run: python ensure_all_content_loaded.py')
        print('   This will load all JSON content into the database.')
        
        # Save to file for processing
        output_file = 'pending_dois/missing_content_with_json.txt'
        Path('pending_dois').mkdir(exist_ok=True)
        
        all_dois = issues['has_grobid'] + issues['has_pymupdf'] + issues['has_both']
        with open(output_file, 'w') as f:
            for doi in all_dois:
                f.write(f'{doi}\n')
        
        print(f'\n   DOIs saved to: {output_file}')
    else:
        print('\n✅ NO ISSUES FOUND!')
        print('   All papers with valid JSON have content in database.')
    
    # Invalid/empty JSON report
    if issues['json_invalid']:
        print(f'\n⚠️  Found {len(issues["json_invalid"])} invalid JSON files:')
        for item in issues['json_invalid'][:5]:
            print(f'  - {item["json_file"].name} ({item["parser"]})')
        if len(issues['json_invalid']) > 5:
            print(f'  ... and {len(issues["json_invalid"]) - 5} more')
    
    if issues['json_empty']:
        print(f'\n⚠️  Found {len(issues["json_empty"])} empty JSON files:')
        for item in issues['json_empty'][:5]:
            print(f'  - {item["json_file"].name} ({item["parser"]})')
        if len(issues['json_empty']) > 5:
            print(f'  ... and {len(issues["json_empty"]) - 5} more')
    
    print('='*70)
    
    return 0 if total_with_json == 0 else 1

if __name__ == '__main__':
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print(f'\n❌ Error: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
