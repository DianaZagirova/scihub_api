#!/usr/bin/env python3
"""
Load content for papers that have valid JSON but missing database content.
"""

import sqlite3
import json
from pathlib import Path

DB_PATH = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
OUTPUT_DIR = Path('./output')
MISSING_DOIS_FILE = 'pending_dois/missing_content_with_json.txt'

def doi_to_filename(doi):
    """Convert DOI to filename."""
    return doi.replace('/', '_')

def load_content_from_json(doi):
    """Load content from JSON files for a DOI."""
    safe_doi = doi_to_filename(doi)
    
    # Try Grobid first (priority)
    grobid_path = OUTPUT_DIR / f'{safe_doi}.json'
    pymupdf_path = OUTPUT_DIR / f'{safe_doi}_fast.json'
    
    content = {
        'abstract': None,
        'full_text_sections': None,
        'parsing_status': None
    }
    
    # Try Grobid
    if grobid_path.exists():
        try:
            with open(grobid_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract abstract
            abstract = data.get('metadata', {}).get('abstract')
            if abstract and abstract.strip():
                content['abstract'] = abstract
            
            # Extract full text sections
            body_list = data.get('full_text', {}).get('body', [])
            if body_list:
                sections_dict = {}
                for section in body_list:
                    title = section.get('title', 'Unnamed Section')
                    text = section.get('content', '')
                    if isinstance(text, list):
                        text = '\n\n'.join(text)
                    if text.strip():
                        sections_dict[title] = text
                
                if sections_dict:
                    content['full_text_sections'] = json.dumps(sections_dict, ensure_ascii=False)
                    content['parsing_status'] = 'success (parser: grobid)'
                    return content
        except Exception as e:
            print(f'  ⚠️  Error reading Grobid JSON for {doi}: {e}')
    
    # Try PyMuPDF
    if pymupdf_path.exists():
        try:
            with open(pymupdf_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract abstract
            if not content['abstract']:
                abstract = data.get('abstract')
                if abstract and abstract.strip():
                    content['abstract'] = abstract
            
            # Extract full text sections
            sections_list = data.get('structured_text', {}).get('sections', [])
            if sections_list:
                sections_dict = {}
                for section in sections_list:
                    title = section.get('title', 'Unnamed Section')
                    text = section.get('content', [])
                    if isinstance(text, list):
                        text = '\n\n'.join(text)
                    if text.strip():
                        sections_dict[title] = text
                
                if sections_dict:
                    content['full_text_sections'] = json.dumps(sections_dict, ensure_ascii=False)
                    content['parsing_status'] = 'success (parser: pymupdf)'
                    return content
        except Exception as e:
            print(f'  ⚠️  Error reading PyMuPDF JSON for {doi}: {e}')
    
    return content

def main():
    print('='*70)
    print('LOADING MISSING CONTENT FROM JSON')
    print('='*70)
    
    # Load DOIs
    if not Path(MISSING_DOIS_FILE).exists():
        print(f'\n❌ File not found: {MISSING_DOIS_FILE}')
        print('   Run: python check_missing_content_with_json.py')
        return 1
    
    with open(MISSING_DOIS_FILE, 'r') as f:
        dois = [line.strip() for line in f if line.strip()]
    
    print(f'\n📋 Loading content for {len(dois)} DOIs...')
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    updated = 0
    errors = 0
    
    for doi in dois:
        print(f'\n  Processing: {doi}')
        
        # Load content from JSON
        content = load_content_from_json(doi)
        
        if not content['full_text_sections']:
            print(f'    ⚠️  No content found in JSON files')
            errors += 1
            continue
        
        # Update database
        update_fields = {}
        
        if content['abstract']:
            update_fields['abstract'] = content['abstract']
            print(f'    ✓ Abstract: {len(content["abstract"])} chars')
        
        if content['full_text_sections']:
            update_fields['full_text_sections'] = content['full_text_sections']
            sections = json.loads(content['full_text_sections'])
            print(f'    ✓ Sections: {len(sections)} sections')
        
        if content['parsing_status']:
            update_fields['parsing_status'] = content['parsing_status']
            print(f'    ✓ Status: {content["parsing_status"]}')
        
        if update_fields:
            set_clause = ', '.join([f"{k} = ?" for k in update_fields.keys()])
            values = list(update_fields.values()) + [doi]
            
            cursor.execute(
                f"UPDATE papers SET {set_clause} WHERE doi = ?",
                values
            )
            updated += 1
            print(f'    ✅ Updated in database')
    
    conn.commit()
    conn.close()
    
    # Summary
    print('\n' + '='*70)
    print('SUMMARY')
    print('='*70)
    print(f'DOIs processed: {len(dois)}')
    print(f'Successfully updated: {updated}')
    print(f'Errors: {errors}')
    print('='*70)
    
    if updated > 0:
        print('\n✅ Content loaded successfully!')
        print('   Run: python check_missing_content_with_json.py')
        print('   to verify.')
    
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
