#!/usr/bin/env python3
"""
Rebuild processing tracker DB (processing_tracker.db) from scratch using:
  1) papers.db content (parsing usage and content presence)
  2) PDFs in ./papers/  -> Sci-Hub success
  3) PDFs in ./pdfs/    -> OA (OpenAlex/other OA source) success
  4) JSONs in ./output/ -> grobid success (.json), fast success (_fast.json)

This script DROPS existing tables in processing_tracker.db and recreates them.
It also (re)builds the summary table processing_state in papers.db.
"""

import os
import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Set
import argparse

from trackers.doi_tracker_db import DOITracker, AVAILABLE_YES, AVAILABLE_NO

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PAPERS_DB = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
TRACKER_DB = 'processing_tracker.db'
OUTPUT_DIR = Path('./output')
SCI_HUB_PDF_DIR = Path('./papers')
OA_PDF_DIR = Path('./pdfs')


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


def drop_tracker_tables(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS processing_tracker")
    cur.execute("DROP TABLE IF EXISTS tracker_events")
    conn.commit()
    conn.close()


def scan_output_parsers(output_dir: Path) -> Dict[str, Set[str]]:
    parsers: Dict[str, Set[str]] = {}
    if output_dir.exists():
        for p in output_dir.glob('*.json'):
            name = p.name[:-5]
            if name.endswith('_fast'):
                doi = name[:-5].replace('_', '/')
                parser = 'pymupdf'
            else:
                doi = name.replace('_', '/')
                parser = 'grobid'
            parsers.setdefault(doi, set()).add(parser)
    return parsers


def scan_pdf_dir(pdf_dir: Path) -> Set[str]:
    s: Set[str] = set()
    if pdf_dir.exists():
        for p in pdf_dir.glob('*.pdf'):
            s.add(p.stem.replace('_', '/'))
    return s


def read_papers_db_info(db_path: str) -> Dict[str, dict]:
    """Read parser/content info from papers.db for all DOIs."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # minimal fields used for determining content presence
    cur.execute(
        """
        SELECT doi, abstract, full_text, full_text_sections, parsing_status
        FROM papers
        WHERE doi IS NOT NULL AND doi != ''
        """
    )
    rows = cur.fetchall()
    conn.close()
    info = {}
    for doi, abstract, full_text, full_sections, parsing_status in rows:
        info[doi] = {
            'has_abstract': bool(abstract and str(abstract).strip()),
            'has_full_text': bool(full_text and str(full_text).strip()),
            'has_sections': bool(full_sections and str(full_sections).strip()),
            'parsing_status': parsing_status or '',
        }
    return info


def upsert_processing_state(conn: sqlite3.Connection, state: dict):
    cols = list(state.keys())
    placeholders = ','.join(['?'] * len(cols))
    updates = ','.join([f"{c} = excluded.{c}" for c in cols if c != 'doi'])
    conn.execute(
        f"""
        INSERT INTO processing_state ({','.join(cols)})
        VALUES ({placeholders})
        ON CONFLICT(doi) DO UPDATE SET {updates}
        """,
        [state[c] for c in cols]
    )

def apply_pragmas(conn: sqlite3.Connection, turbo: bool = False):
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")  # ~200MB
    cur.execute("PRAGMA synchronous={};".format('OFF' if turbo else 'NORMAL'))
    conn.commit()


def ensure_tracker_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processing_tracker (
            doi TEXT PRIMARY KEY,
            scihub_available TEXT,
            scihub_downloaded TEXT,
            oa_available TEXT,
            oa_downloaded TEXT,
            downloaded TEXT,
            download_date TEXT,
            has_content_in_db TEXT,
            pymupdf_status TEXT,
            pymupdf_date TEXT,
            grobid_status TEXT,
            grobid_date TEXT,
            last_updated TEXT,
            error_msg TEXT,
            retry_count INTEGER DEFAULT 0
        )
        """
    )
    conn.commit()


def rebuild(turbo: bool = False, batch_size: int = 5000):
    logger.info("Dropping and recreating tracker tables...")
    drop_tracker_tables(TRACKER_DB)

    logger.info("Scanning filesystem and database...")
    parsers_map = scan_output_parsers(OUTPUT_DIR)
    sci_pdfs = scan_pdf_dir(SCI_HUB_PDF_DIR)
    oa_pdfs = scan_pdf_dir(OA_PDF_DIR)
    db_info = read_papers_db_info(PAPERS_DB)

    logger.info(f"Output JSONs: {sum(len(v) for v in parsers_map.values())} across {len(parsers_map)} DOIs")
    logger.info(f"Sci-Hub PDFs in {SCI_HUB_PDF_DIR}: {len(sci_pdfs)}")
    logger.info(f"OA PDFs in {OA_PDF_DIR}: {len(oa_pdfs)}")
    logger.info(f"DOIs in papers.db: {len(db_info)}")

    # Union of all seen DOIs
    all_dois = list(set(db_info.keys()) | set(parsers_map.keys()) | sci_pdfs | oa_pdfs)

    # Open connections and apply PRAGMAs
    tracker_conn = sqlite3.connect(TRACKER_DB)
    papers_conn = sqlite3.connect(PAPERS_DB)
    apply_pragmas(tracker_conn, turbo=turbo)
    apply_pragmas(papers_conn, turbo=turbo)
    ensure_tracker_schema(tracker_conn)
    ensure_processing_state_table(papers_conn)

    now = datetime.now().isoformat()

    # Prepare bulk rows
    tracker_rows = []
    state_rows = []

    for doi in all_dois:
        parsers = parsers_map.get(doi, set())
        in_sci = doi in sci_pdfs
        in_oa = doi in oa_pdfs
        db_row = db_info.get(doi, {})
        has_content = bool(db_row.get('has_abstract') or db_row.get('has_full_text') or db_row.get('has_sections')) if db_row else False

        downloaded = 'yes' if (in_sci or in_oa) else None
        scihub_available = 'yes' if in_sci else None
        oa_available = 'yes' if in_oa else None
        scihub_downloaded = 'yes' if in_sci else None
        oa_downloaded = 'yes' if in_oa else None
        grobid_status = 'success' if 'grobid' in parsers else None
        pymupdf_status = 'success' if 'pymupdf' in parsers else None
        grobid_date = now if grobid_status == 'success' else None
        pymupdf_date = now if pymupdf_status == 'success' else None

        tracker_rows.append((
            doi,
            scihub_available,
            scihub_downloaded,
            oa_available,
            oa_downloaded,
            downloaded,
            now if downloaded == 'yes' else None,
            'yes' if has_content else 'no',
            pymupdf_status,
            pymupdf_date,
            grobid_status,
            grobid_date,
            now,
            None,
            0,
        ))

        state_rows.append((
            doi,
            scihub_available,
            scihub_downloaded,
            oa_available,
            oa_downloaded,
            downloaded,
            now if downloaded == 'yes' else None,
            pymupdf_status,
            grobid_status,
            1 if pymupdf_status == 'success' else 0,
            1 if grobid_status == 'success' else 0,
            1 if in_sci else 0,
            1 if in_oa else 0,
            1 if pymupdf_status == 'success' else 0,
            1 if grobid_status == 'success' else 0,
            0,
            now,
        ))

    # Bulk UPSERTs inside single transactions
    tracker_cur = tracker_conn.cursor()
    papers_cur = papers_conn.cursor()

    tracker_cur.execute("BEGIN IMMEDIATE;")
    papers_cur.execute("BEGIN IMMEDIATE;")

    tracker_sql = (
        "INSERT INTO processing_tracker (doi, scihub_available, scihub_downloaded, oa_available, oa_downloaded, downloaded, download_date, has_content_in_db, pymupdf_status, pymupdf_date, grobid_status, grobid_date, last_updated, error_msg, retry_count) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(doi) DO UPDATE SET "
        "scihub_available=excluded.scihub_available, scihub_downloaded=excluded.scihub_downloaded, oa_available=excluded.oa_available, oa_downloaded=excluded.oa_downloaded, downloaded=excluded.downloaded, download_date=excluded.download_date, has_content_in_db=excluded.has_content_in_db, pymupdf_status=excluded.pymupdf_status, pymupdf_date=excluded.pymupdf_date, grobid_status=excluded.grobid_status, grobid_date=excluded.grobid_date, last_updated=excluded.last_updated, error_msg=excluded.error_msg, retry_count=excluded.retry_count"
    )

    # Batch executemany to avoid huge single batch memory
    for i in range(0, len(tracker_rows), batch_size):
        tracker_cur.executemany(tracker_sql, tracker_rows[i:i+batch_size])

    state_sql = (
        "INSERT INTO processing_state (doi, scihub_available, scihub_downloaded, oa_available, oa_downloaded, downloaded, download_date, pymupdf_status, grobid_status, pymupdf_attempts, grobid_attempts, scihub_attempts, oa_attempts, files_pymupdf, files_grobid, mismatch_files_tracker, last_updated) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(doi) DO UPDATE SET "
        "scihub_available=excluded.scihub_available, scihub_downloaded=excluded.scihub_downloaded, oa_available=excluded.oa_available, oa_downloaded=excluded.oa_downloaded, downloaded=excluded.downloaded, download_date=excluded.download_date, pymupdf_status=excluded.pymupdf_status, grobid_status=excluded.grobid_status, pymupdf_attempts=excluded.pymupdf_attempts, grobid_attempts=excluded.grobid_attempts, scihub_attempts=excluded.scihub_attempts, oa_attempts=excluded.oa_attempts, files_pymupdf=excluded.files_pymupdf, files_grobid=excluded.files_grobid, mismatch_files_tracker=excluded.mismatch_files_tracker, last_updated=excluded.last_updated"
    )

    for i in range(0, len(state_rows), batch_size):
        papers_cur.executemany(state_sql, state_rows[i:i+batch_size])

    tracker_conn.commit()
    papers_conn.commit()
    tracker_conn.close()
    papers_conn.close()

    logger.info("\n" + '='*70)
    logger.info("REBUILD SUMMARY")
    logger.info('='*70)
    logger.info(f"DOIs processed: {len(all_dois)}")
    logger.info(f"Tracker rows created/updated: {len(tracker_rows)}")
    logger.info(f"processing_state rows upserted: {len(state_rows)}")
    logger.info('='*70)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Rebuild processing tracker DB quickly from sources')
    parser.add_argument('--turbo', action='store_true', help='Aggressive PRAGMAs (synchronous=OFF) for maximum speed')
    parser.add_argument('--batch', type=int, default=5000, help='Batch size for executemany')
    args = parser.parse_args()

    rebuild(turbo=args.turbo, batch_size=args.batch)

if __name__ == '__main__':
    main()
