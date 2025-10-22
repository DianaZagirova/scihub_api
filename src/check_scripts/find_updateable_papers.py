#!/usr/bin/env python3
"""Find papers that have JSONs with actual content but aren't in the database"""

import sqlite3
import json
import os

conn = sqlite3.connect('/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db')
cursor = conn.cursor()

# Get papers missing full_text
cursor.execute("""
    SELECT doi, abstract, full_text, full_text_sections, parsing_status 
    FROM papers 
    WHERE doi IS NOT NULL AND doi != ''
    AND (full_text IS NULL OR full_text = '')
    AND (full_text_sections IS NULL OR full_text_sections = '')
""")

all_rows = cursor.fetchall()
print(f"Total papers missing full_text: {len(all_rows)}")
print("Checking for JSONs with actual content...")
print("="*70)

found_with_content = 0
found_empty = 0
no_json = 0

for i, row in enumerate(all_rows):
    if i >= 1000:  # Check first 1000
        break
        
    doi, abstract, full_text, sections, status = row
    doi_file = doi.replace('/', '_')
    json_path = f'/home/diana.z/hack/scihub_api/output/{doi_file}.json'
    fast_path = f'/home/diana.z/hack/scihub_api/output/{doi_file}_fast.json'
    
    has_json = os.path.exists(json_path)
    has_fast = os.path.exists(fast_path)
    
    if not has_json and not has_fast:
        no_json += 1
        continue
    
    # Try to read the JSON
    json_file = json_path if has_json else fast_path
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        has_content = False
        
        # Check GROBID format
        if has_json:
            body_list = data.get('full_text', {}).get('body', [])
            if body_list and len(body_list) > 0:
                # Check if any section has actual content
                for section in body_list:
                    content = section.get('content', '')
                    if content and len(str(content).strip()) > 50:
                        has_content = True
                        break
        
        # Check PyMuPDF format
        if has_fast and not has_content:
            sections_list = data.get('structured_text', {}).get('sections', [])
            if sections_list and len(sections_list) > 0:
                for section in sections_list:
                    content = section.get('content', [])
                    if content and len(str(content)) > 50:
                        has_content = True
                        break
        
        if has_content:
            found_with_content += 1
            if found_with_content <= 5:
                print(f"\n✓ DOI with content: {doi}")
                print(f"  Has .json: {has_json}, Has _fast.json: {has_fast}")
                print(f"  Status: {status}")
        else:
            found_empty += 1
    
    except Exception as e:
        print(f"ERROR reading {doi}: {e}")

print("\n" + "="*70)
print(f"Results (first 1000 papers):")
print(f"  Papers with JSONs containing content: {found_with_content}")
print(f"  Papers with empty JSONs: {found_empty}")
print(f"  Papers without JSONs: {no_json}")

conn.close()
