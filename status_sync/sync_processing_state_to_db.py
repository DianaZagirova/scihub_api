#!/usr/bin/env python3
"""
Sync current processing state into the database and verify JSON/PDF consistency.

- Validates files BEFORE syncing:
  - Deletes empty/invalid PDFs and JSONs so they are not recorded as success
- Reads tracker state from processing_tracker.db (DB-backed tracker)
- Scans output/ directory to verify presence of Grobid and PyMuPDF JSONs
- Updates tracker statuses when files exist and are valid but tracker is out-of-sync
- Writes a summarized state table into papers.db: processing_state
- Reports mismatches and counts

Run examples:
  python sync_processing_state_to_db.py
  python sync_processing_state_to_db.py --output ./output --papers-db /path/to/papers.db --tracker-db processing_tracker.db
"""

import os
import sys
import sqlite3
import json
import logging
import argparse
from pathlib import Path
from typing import Dict, Set

# Ensure project root is on sys.path to import trackers/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from trackers.doi_tracker_db import DOITracker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DEFAULT_PAPERS_DB = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
DEFAULT_TRACKER_DB = 'processing_tracker.db'
DEFAULT_OUTPUT_DIR = './output'
DEFAULT_PAPERS_DIR = './papers'


def is_valid_pdf(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size < 1024:
            return False
        with path.open('rb') as f:
            header = f.read(5)
            if header != b'%PDF-':
                return False
            try:
                f.seek(-4096, os.SEEK_END)
                tail = f.read(4096)
            except OSError:
                f.seek(0, os.SEEK_SET)
                tail = f.read()
        if b'%%EOF' not in tail:
            return False
        return True
    except Exception:
        return False


def is_valid_json(path: Path, parser_hint: str) -> bool:
    try:
        if not path.exists() or path.stat().st_size < 10:
            return False
        with path.open('r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict) or not data:
            return False
        if parser_hint == 'grobid':
            md = data.get('metadata') if isinstance(data, dict) else None
            title_ok = False
            authors_ok = False
            if isinstance(md, dict):
                title = md.get('title')
                if isinstance(title, str) and len(title.strip()) >= 5:
                    title_ok = True
                authors = md.get('authors') or md.get('author')
                if isinstance(authors, list) and len(authors) > 0:
                    authors_ok = True
            # Check full_text - can be string or dict
            full_text = data.get('full_text') or data.get('fullText')
            fulltext_ok = False
            if isinstance(full_text, str):
                # String format: full_text is a plain string
                fulltext_ok = len(full_text.strip()) >= 50
            elif isinstance(full_text, dict):
                # Dict format: full_text = {body: [...], references: [...]}
                # where body is a list of {title, content} dicts
                body = full_text.get('body', [])
                if isinstance(body, list):
                    total_content = 0
                    for item in body:
                        if isinstance(item, dict):
                            content = item.get('content', '')
                            if isinstance(content, str):
                                total_content += len(content.strip())
                    fulltext_ok = total_content >= 50
            sections = data.get('sections')
            sections_ok = False
            if isinstance(sections, list) and len(sections) > 0:
                for s in sections:
                    txt = s.get('text') if isinstance(s, dict) else None
                    if isinstance(txt, str) and len(txt.strip()) >= 50:
                        sections_ok = True
                        break
            return bool(title_ok or authors_ok or fulltext_ok or sections_ok)
        if parser_hint == 'pymupdf':
            st = data.get('structured_text') if isinstance(data, dict) else None
            if not st:
                return False
            
            # Check full_text field first (most reliable)
            full_text = st.get('full_text', '')
            if isinstance(full_text, str) and len(full_text.strip()) >= 50:
                return True
            
            # Check sections with content arrays
            sections = st.get('sections', [])
            if isinstance(sections, list) and len(sections) > 0:
                total_content = 0
                for section in sections:
                    if isinstance(section, dict):
                        content = section.get('content', [])
                        if isinstance(content, list):
                            for item in content:
                                if isinstance(item, str):
                                    total_content += len(item.strip())
                if total_content >= 50:
                    return True
            
            # Fallback: accumulate any 'text' keys in nested structure
            total_text = 0
            def accumulate_text(node):
                nonlocal total_text
                if isinstance(node, dict):
                    for k, v in node.items():
                        if k == 'text' and isinstance(v, str):
                            total_text += len(v.strip())
                        else:
                            accumulate_text(v)
                elif isinstance(node, list):
                    for v in node:
                        accumulate_text(v)
            accumulate_text(st)
            if total_text >= 50:
                return True
            
            return False
        return True
    except Exception:
        return False


def scan_output(output_dir: Path) -> Dict[str, Set[str]]:
    """Scan output directory for JSONs and return {doi: {parsers}}."""
    dois: Dict[str, Set[str]] = {}
    if not output_dir.exists():
        logger.warning(f"Output dir not found: {output_dir}")
        return dois
    
    # Create quarantine directory for invalid JSONs
    quarantine_dir = output_dir / 'invalid_jsons'
    quarantine_dir.mkdir(exist_ok=True)
    
    for p in output_dir.glob('*.json'):
        name = p.name[:-5] if p.name.endswith('.json') else p.name
        if name.endswith('_fast'):
            doi = name[:-5].replace('_', '/')
            parser = 'pymupdf'
            valid = is_valid_json(p, 'pymupdf')
        else:
            doi = name.replace('_', '/')
            parser = 'grobid'
            valid = is_valid_json(p, 'grobid')
        if not valid:
            try:
                # Move to quarantine instead of deleting
                quarantine_path = quarantine_dir / p.name
                p.rename(quarantine_path)
                logger.warning(f"Quarantined invalid JSON: {p.name} -> {quarantine_path}")
            except Exception as e:
                logger.error(f"Failed to quarantine invalid JSON {p}: {e}")
            continue
        s = dois.setdefault(doi, set())
        s.add(parser)
    return dois


def scan_papers(papers_dir: Path) -> Set[str]:
    """Scan papers directory for PDFs and return set of DOIs."""
    dois: Set[str] = set()
    if not papers_dir.exists():
        logger.warning(f"Papers dir not found: {papers_dir}")
        return dois
    
    # Create quarantine directory for invalid PDFs
    quarantine_dir = papers_dir / 'invalid_pdfs'
    quarantine_dir.mkdir(exist_ok=True)
    
    for p in papers_dir.glob('*.pdf'):
        if not is_valid_pdf(p):
            try:
                # Move to quarantine instead of deleting
                quarantine_path = quarantine_dir / p.name
                p.rename(quarantine_path)
                logger.warning(f"Quarantined invalid PDF: {p.name} -> {quarantine_path}")
            except Exception as e:
                logger.error(f"Failed to quarantine invalid PDF {p}: {e}")
            continue
        doi = p.stem.replace('_', '/')
        dois.add(doi)
    return dois


def ensure_processing_state_table(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processing_state (
            doi TEXT PRIMARY KEY,
            scihub_available TEXT,
            scihub_downloaded TEXT,
            oa_available TEXT,
            oa_downloaded TEXT,
            downloaded TEXT,
            download_date TEXT,
            pymupdf_status TEXT,
            grobid_status TEXT,
            pymupdf_attempts INTEGER,
            grobid_attempts INTEGER,
            scihub_attempts INTEGER,
            oa_attempts INTEGER,
            files_pymupdf INTEGER,
            files_grobid INTEGER,
            mismatch_files_tracker INTEGER,
            last_updated TEXT
        )
        """
    )
    conn.commit()


def count_attempts(tracker_conn: sqlite3.Connection, doi: str) -> Dict[str, int]:
    cur = tracker_conn.cursor()
    cur.execute(
        "SELECT event_type, status_to FROM tracker_events WHERE doi = ?",
        (doi,)
    )
    rows = cur.fetchall()
    # heuristics: count event types
    scihub_attempts = 0
    oa_attempts = 0
    pym_attempts = 0
    grob_attempts = 0
    for ev, to in rows:
        if ev == 'download':
            # We recorded source or success/failed text in status_to
            if to == 'scihub':
                scihub_attempts += 1
            elif to == 'oa':
                oa_attempts += 1
            else:
                # unknown_source or success/failed; skip counting
                pass
        elif ev == 'pymupdf':
            pym_attempts += 1
        elif ev == 'grobid':
            grob_attempts += 1
    return {
        'scihub_attempts': scihub_attempts,
        'oa_attempts': oa_attempts,
        'pymupdf_attempts': pym_attempts,
        'grobid_attempts': grob_attempts,
    }


def upsert_processing_state(papers_conn: sqlite3.Connection, state: dict):
    cur = papers_conn.cursor()
    cols = list(state.keys())
    placeholders = ','.join(['?'] * len(cols))
    updates = ','.join([f"{c} = excluded.{c}" for c in cols if c != 'doi'])
    cur.execute(
        f"""
        INSERT INTO processing_state ({','.join(cols)})
        VALUES ({placeholders})
        ON CONFLICT(doi) DO UPDATE SET {updates}
        """,
        [state[c] for c in cols]
    )


def main():
    parser = argparse.ArgumentParser(description='Sync processing state to database and verify JSON consistency')
    parser.add_argument('--papers-db', default=DEFAULT_PAPERS_DB, help='Path to papers.db')
    parser.add_argument('--tracker-db', default=DEFAULT_TRACKER_DB, help='Path to processing_tracker.db')
    parser.add_argument('--output', default=DEFAULT_OUTPUT_DIR, help='Output directory with JSONs')
    parser.add_argument('--papers', default=DEFAULT_PAPERS_DIR, help='Papers directory with PDFs')
    parser.add_argument('--seed-missing', action='store_true', help='Auto-create tracker rows for DOIs found in files but missing in tracker')
    parser.add_argument('--seed-from-papers-db', action='store_true', help='Seed tracker with all DOIs from papers.db that are missing in tracker')
    args = parser.parse_args()

    output_dir = Path(args.output)

    # Tracker (DB-backed)
    tracker = DOITracker(db_path=args.tracker_db)

    # Scan filesystem
    files_map = scan_output(output_dir)
    pdf_dois = scan_papers(Path(args.papers))
    logger.info(f"Found {sum(len(v) for v in files_map.values())} JSONs across {len(files_map)} DOIs")
    logger.info(f"Found {len(pdf_dois)} PDFs in {args.papers}")

    # Open DBs
    papers_conn = sqlite3.connect(args.papers_db)
    tracker_conn = sqlite3.connect(args.tracker_db)

    ensure_processing_state_table(papers_conn)

    # Optionally seed missing DOIs from papers.db
    if args.seed_from_papers_db:
        print("Seeding tracker from papers.db...")
        logger.info("Seeding tracker from papers.db...")
        existing = set(tracker.get_all_statuses().keys())
        print(f"Tracker has {len(existing)} DOIs")
        logger.info(f"Tracker has {len(existing)} DOIs")
        
        # Get all DOIs from papers.db
        cur = papers_conn.cursor()
        cur.execute("SELECT doi FROM papers WHERE doi IS NOT NULL AND doi != ''")
        papers_dois = {row[0] for row in cur.fetchall()}
        print(f"Papers.db has {len(papers_dois)} DOIs")
        logger.info(f"Papers.db has {len(papers_dois)} DOIs")
        
        missing = papers_dois - existing
        print(f"Missing: {len(missing)} DOIs to seed")
        logger.info(f"Missing: {len(missing)} DOIs to seed")
        
        seeded = 0
        for doi in missing:
            tracker.update_status(doi=doi)
            seeded += 1
            if seeded % 1000 == 0:
                print(f"  Progress: {seeded}/{len(missing)}")
                logger.info(f"Progress: {seeded}/{len(missing)}")
        
        if seeded:
            print(f"✓ Seeded {seeded} DOIs from papers.db into tracker")
            logger.info(f"✓ Seeded {seeded} DOIs from papers.db into tracker")
        else:
            print("No new DOIs to seed from papers.db")
            logger.info("No new DOIs to seed from papers.db")

    # Optionally seed missing DOIs from filesystem (papers/output)
    if args.seed_missing:
        existing = set(tracker.get_all_statuses().keys())
        detected = set(files_map.keys()) | set(pdf_dois)
        missing = detected - existing
        seeded = 0
        for doi in missing:
            # Create a minimal row
            tracker.update_status(doi=doi)
            # Seed downloaded based on presence in ./papers
            if doi in pdf_dois:
                tracker.mark_downloaded(doi, success=True)
            seeded += 1
        if seeded:
            logger.info(f"Seeded {seeded} missing DOIs into tracker from filesystem")


if __name__ == '__main__':
    main()