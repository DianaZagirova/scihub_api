#!/usr/bin/env python3
"""
SQLite-backed DOI tracker (drop-in replacement for CSV-based tracker).

- Stores tracker state in SQLite for reliability and atomic updates
- Optionally mirrors to CSV on demand for inspection/export

Schema (table: processing_tracker):
    doi TEXT PRIMARY KEY
    scihub_available TEXT
    scihub_downloaded TEXT
    oa_available TEXT
    oa_downloaded TEXT
    downloaded TEXT
    download_date TEXT
    has_content_in_db TEXT
    pymupdf_status TEXT
    pymupdf_date TEXT
    grobid_status TEXT
    grobid_date TEXT
    last_updated TEXT
    error_msg TEXT
    retry_count INTEGER DEFAULT 0

Additionally, an event log table (tracker_events) for auditing.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Dict, Optional, Any, List

# Reuse the same constants as CSV tracker for compatibility
AVAILABLE_YES = 'yes'
AVAILABLE_NO = 'no'
AVAILABLE_UNKNOWN = 'unknown'

STATUS_SUCCESS = 'success'
STATUS_FAILED = 'failed'
STATUS_NOT_ATTEMPTED = 'not_attempted'

DEFAULT_DB = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'


class DOITracker:
    def __init__(self, db_path: str = DEFAULT_DB):
        self.db_path = db_path
        self._ensure_schema()
        # Simple in-memory cache for compatibility with CSV tracker users
        self._cache = {}
        self._load_cache()
        # Expose constants as attributes for compatibility
        self.AVAILABLE_YES = AVAILABLE_YES
        self.AVAILABLE_NO = AVAILABLE_NO
        self.AVAILABLE_UNKNOWN = AVAILABLE_UNKNOWN
        self.STATUS_SUCCESS = STATUS_SUCCESS
        self.STATUS_FAILED = STATUS_FAILED
        self.STATUS_NOT_ATTEMPTED = STATUS_NOT_ATTEMPTED

    # ----------------------
    # Schema management
    # ----------------------
    def _ensure_schema(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS processing_tracker (
                doi TEXT PRIMARY KEY,
                scihub_available TEXT,
                scihub_downloaded TEXT,
                oa_available TEXT,
                oa_downloaded TEXT,
                arxiv_attempted TEXT,
                arxiv_downloaded TEXT,
                biorxiv_attempted TEXT,
                biorxiv_downloaded TEXT,
                europepmc_attempted TEXT,
                europepmc_downloaded TEXT,
                unpaywall_attempted TEXT,
                unpaywall_downloaded TEXT,
                downloaded TEXT,
                download_date TEXT,
                download_source TEXT,
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
        
        # Add new columns to existing tables (migration)
        try:
            cur.execute("ALTER TABLE processing_tracker ADD COLUMN arxiv_attempted TEXT")
        except:
            pass
        try:
            cur.execute("ALTER TABLE processing_tracker ADD COLUMN arxiv_downloaded TEXT")
        except:
            pass
        try:
            cur.execute("ALTER TABLE processing_tracker ADD COLUMN biorxiv_attempted TEXT")
        except:
            pass
        try:
            cur.execute("ALTER TABLE processing_tracker ADD COLUMN biorxiv_downloaded TEXT")
        except:
            pass
        try:
            cur.execute("ALTER TABLE processing_tracker ADD COLUMN europepmc_attempted TEXT")
        except:
            pass
        try:
            cur.execute("ALTER TABLE processing_tracker ADD COLUMN europepmc_downloaded TEXT")
        except:
            pass
        try:
            cur.execute("ALTER TABLE processing_tracker ADD COLUMN unpaywall_attempted TEXT")
        except:
            pass
        try:
            cur.execute("ALTER TABLE processing_tracker ADD COLUMN unpaywall_downloaded TEXT")
        except:
            pass
        try:
            cur.execute("ALTER TABLE processing_tracker ADD COLUMN download_source TEXT")
        except:
            pass

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tracker_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doi TEXT NOT NULL,
                event_type TEXT NOT NULL,
                status_from TEXT,
                status_to TEXT,
                message TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        conn.commit()
        conn.close()

    # ----------------------
    # Helpers
    # ----------------------
    def _now(self) -> str:
        return datetime.now().isoformat()

    def _upsert(self, doi: str, updates: Dict[str, Any]):
        # Ensure row exists
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        cur.execute("SELECT doi FROM processing_tracker WHERE doi = ?", (doi,))
        exists = cur.fetchone() is not None

        if not exists:
            cur.execute("INSERT INTO processing_tracker (doi, last_updated, retry_count) VALUES (?, ?, 0)", (doi, self._now()))

        # Build update query
        updates = {**updates, 'last_updated': self._now()}
        cols = ", ".join([f"{k} = ?" for k in updates.keys()])
        vals = list(updates.values()) + [doi]
        cur.execute(f"UPDATE processing_tracker SET {cols} WHERE doi = ?", vals)

        conn.commit()
        conn.close()

    def _log_event(self, doi: str, event_type: str, status_from: Optional[str] = None, status_to: Optional[str] = None, message: Optional[str] = None):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO tracker_events (doi, event_type, status_from, status_to, message, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (doi, event_type, status_from, status_to, message or '', self._now()),
        )
        conn.commit()
        conn.close()

    def _load_cache(self):
        """Load all statuses into an in-memory cache (compatibility)."""
        self._cache = self.get_all_statuses()

    def _ensure_cache_loaded(self):
        if not isinstance(self._cache, dict) or not self._cache:
            self._load_cache()

    # ----------------------
    # Public API (compatible subset)
    # ----------------------
    def get_status(self, doi: str) -> Optional[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT doi, scihub_available, scihub_downloaded, oa_available, oa_downloaded,
                   arxiv_attempted, arxiv_downloaded, biorxiv_attempted, biorxiv_downloaded,
                   europepmc_attempted, europepmc_downloaded, unpaywall_attempted, unpaywall_downloaded,
                   downloaded, download_date, download_source, has_content_in_db,
                   pymupdf_status, pymupdf_date, grobid_status, grobid_date,
                   last_updated, error_msg, retry_count
            FROM processing_tracker WHERE doi = ?
            """,
            (doi,),
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        keys = [
            'doi','scihub_available','scihub_downloaded','oa_available','oa_downloaded',
            'arxiv_attempted','arxiv_downloaded','biorxiv_attempted','biorxiv_downloaded',
            'europepmc_attempted','europepmc_downloaded','unpaywall_attempted','unpaywall_downloaded',
            'downloaded','download_date','download_source','has_content_in_db',
            'pymupdf_status','pymupdf_date','grobid_status','grobid_date',
            'last_updated','error_msg','retry_count'
        ]
        return dict(zip(keys, row))

    def update_status(self, updates: Dict[str, Any] | None = None, /, **kwargs):
        """
        Backward-compatible update_status.
        Supports:
          - update_status({'doi': doi, 'field': 'value'})
          - update_status(doi='...', field='value')
          - update_status(doi, field='value')  [not supported; callers pass kwargs]
        """
        # If called like update_status({'doi':..., ...})
        if isinstance(updates, dict) and 'doi' in updates:
            doi = updates['doi']
            payload = {k: v for k, v in updates.items() if k != 'doi'}
        else:
            # Expect doi in kwargs
            if updates is not None and isinstance(updates, str):
                # Gracefully accept update_status(doi, key=val) pattern
                kwargs = {**kwargs, 'doi': updates}
            doi = kwargs.get('doi')
            if not doi:
                raise ValueError('updates must include doi')
            payload = {k: v for k, v in kwargs.items() if k != 'doi'}
        prev = self.get_status(doi)
        self._upsert(doi, payload)
        self._log_event(doi, 'update', str(prev), str(self.get_status(doi)), None)

    def increment_retry(self, doi: str):
        status = self.get_status(doi) or {'retry_count': 0}
        new_retry = int(status.get('retry_count') or 0) + 1
        self._upsert(doi, {'retry_count': new_retry})
        self._log_event(doi, 'retry_increment', None, str(new_retry), None)

    def mark_scihub_available(self, doi: str, available: bool):
        self._upsert(doi, {'scihub_available': AVAILABLE_YES if available else AVAILABLE_NO})
        self._log_event(doi, 'scihub_available', None, AVAILABLE_YES if available else AVAILABLE_NO, None)

    # Backward-compat alias used by some scripts
    def mark_scihub_found(self, doi: str, available: bool):
        return self.mark_scihub_available(doi, available)

    def mark_oa_available(self, doi: str, available: bool):
        self._upsert(doi, {'oa_available': AVAILABLE_YES if available else AVAILABLE_NO})
        self._log_event(doi, 'oa_available', None, AVAILABLE_YES if available else AVAILABLE_NO, None)

    def mark_source_attempted(self, doi: str, source: str):
        """Mark that a download source was attempted."""
        field_map = {
            'arxiv': 'arxiv_attempted',
            'biorxiv': 'biorxiv_attempted',
            'europepmc': 'europepmc_attempted',
            'unpaywall': 'unpaywall_attempted'
        }
        if source in field_map:
            self._upsert(doi, {field_map[source]: AVAILABLE_YES})
            self._log_event(doi, f'{source}_attempted', None, None, None)
    
    def mark_source_downloaded(self, doi: str, source: str, success: bool):
        """Mark download result from a specific source."""
        field_map = {
            'arxiv': 'arxiv_downloaded',
            'biorxiv': 'biorxiv_downloaded',
            'europepmc': 'europepmc_downloaded',
            'unpaywall': 'unpaywall_downloaded'
        }
        if source in field_map:
            updates = {
                field_map[source]: AVAILABLE_YES if success else AVAILABLE_NO
            }
            if success:
                updates['downloaded'] = AVAILABLE_YES
                updates['download_date'] = self._now()
                updates['download_source'] = source
            self._upsert(doi, updates)
            self._log_event(doi, f'{source}_download', None, 'success' if success else 'failed', None)

    def mark_downloaded(self, doi: str, source: str | None = None, success: bool | None = None):
        """
        Backward-compatible:
          - mark_downloaded(doi, source='scihub'|'oa'|'arxiv'|'biorxiv'|'europepmc'|'unpaywall')
          - mark_downloaded(doi, success=True|False)  -> sets downloaded yes/no
        """
        updates = {'download_date': self._now()}
        
        # Handle new sources
        if source in ('arxiv', 'biorxiv', 'europepmc', 'unpaywall'):
            self.mark_source_downloaded(doi, source, success=True)
            return
        
        if source in ('scihub', 'oa'):
            updates['downloaded'] = AVAILABLE_YES
            updates['download_source'] = source
            if source == 'scihub':
                updates['scihub_downloaded'] = AVAILABLE_YES
            else:
                updates['oa_downloaded'] = AVAILABLE_YES
            self._upsert(doi, updates)
            self._log_event(doi, 'download', None, source, None)
            return
        if success is not None:
            updates['downloaded'] = AVAILABLE_YES if success else AVAILABLE_NO
            self._upsert(doi, updates)
            self._log_event(doi, 'download', None, 'success' if success else 'failed', None)
            return
        # Default: mark as downloaded without source detail
        updates['downloaded'] = AVAILABLE_YES
        self._upsert(doi, updates)
        self._log_event(doi, 'download', None, 'unknown_source', None)

    def mark_pymupdf_processed(self, doi: str, success: bool):
        self._upsert(doi, {
            'pymupdf_status': STATUS_SUCCESS if success else STATUS_FAILED,
            'pymupdf_date': self._now(),
        })
        self._log_event(doi, 'pymupdf', None, STATUS_SUCCESS if success else STATUS_FAILED, None)

    def mark_grobid_processed(self, doi: str, success: bool):
        self._upsert(doi, {
            'grobid_status': STATUS_SUCCESS if success else STATUS_FAILED,
            'grobid_date': self._now(),
        })
        self._log_event(doi, 'grobid', None, STATUS_SUCCESS if success else STATUS_FAILED, None)

    def set_error(self, doi: str, message: str):
        self._upsert(doi, {'error_msg': message})
        self._log_event(doi, 'error', None, None, message)

    def get_all_statuses(self, dois: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        if dois:
            placeholders = ','.join('?' * len(dois))
            cur.execute(f"SELECT * FROM processing_tracker WHERE doi IN ({placeholders})", dois)
        else:
            cur.execute("SELECT * FROM processing_tracker")
        rows = cur.fetchall()
        conn.close()
        keys = [
            'doi','scihub_available','scihub_downloaded','oa_available','oa_downloaded',
            'arxiv_attempted','arxiv_downloaded','biorxiv_attempted','biorxiv_downloaded',
            'europepmc_attempted','europepmc_downloaded','unpaywall_attempted','unpaywall_downloaded',
            'downloaded','download_date','download_source','has_content_in_db',
            'pymupdf_status','pymupdf_date','grobid_status','grobid_date',
            'last_updated','error_msg','retry_count'
        ]
        return {row[0]: dict(zip(keys, row)) for row in rows}

    def flush(self):
        """No-op for compatibility with CSV tracker implementations."""
        return

    def bulk_update(self, updates: List[Dict[str, Any]], defer_write: bool = False):
        """
        Bulk upsert updates. 'updates' should be a list of dicts including 'doi'.
        'defer_write' is accepted for compatibility but ignored (SQLite is atomic).
        """
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        now = self._now()
        for upd in updates:
            doi = upd.get('doi')
            if not doi:
                continue
            # ensure row exists
            cur.execute("INSERT OR IGNORE INTO processing_tracker (doi, last_updated, retry_count) VALUES (?, ?, 0)", (doi, now))
            payload = {k: v for k, v in upd.items() if k != 'doi'}
            if not payload:
                continue
            payload['last_updated'] = now
            cols = ", ".join([f"{k} = ?" for k in payload.keys()])
            vals = list(payload.values()) + [doi]
            cur.execute(f"UPDATE processing_tracker SET {cols} WHERE doi = ?", vals)
            # log event
            self._log_event(doi, 'bulk_update', None, None, None)
        conn.commit()
        conn.close()

    def reset_doi(self, doi: str):
        """
        Reset all tracking fields for a DOI to initial state.
        This clears download status, parsing status, and retry count.
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)  # 30 second timeout
            cur = conn.cursor()
            
            # Enable WAL mode for better concurrent access
            cur.execute("PRAGMA journal_mode=WAL")
            
            # Check if row exists
            cur.execute("SELECT doi FROM processing_tracker WHERE doi = ?", (doi,))
            exists = cur.fetchone() is not None
            
            if exists:
                # Reset all fields to initial state
                cur.execute(
                    """
                    UPDATE processing_tracker 
                    SET scihub_available = NULL,
                        scihub_downloaded = NULL,
                        oa_available = NULL,
                        oa_downloaded = NULL,
                        arxiv_attempted = NULL,
                        arxiv_downloaded = NULL,
                        biorxiv_attempted = NULL,
                        biorxiv_downloaded = NULL,
                        europepmc_attempted = NULL,
                        europepmc_downloaded = NULL,
                        unpaywall_attempted = NULL,
                        unpaywall_downloaded = NULL,
                        downloaded = NULL,
                        download_date = NULL,
                        download_source = NULL,
                        has_content_in_db = NULL,
                        pymupdf_status = NULL,
                        pymupdf_date = NULL,
                        grobid_status = NULL,
                        grobid_date = NULL,
                        error_msg = NULL,
                        retry_count = 0,
                        last_updated = ?
                    WHERE doi = ?
                    """,
                    (self._now(), doi)
                )
            else:
                # Create new row with default values
                cur.execute(
                    "INSERT INTO processing_tracker (doi, last_updated, retry_count) VALUES (?, ?, 0)",
                    (doi, self._now())
                )
            
            conn.commit()
            
            # Log event in separate connection to avoid keeping main connection open
            self._log_event(doi, 'reset', None, None, 'DOI tracking reset to initial state' if exists else 'New DOI entry created')
        finally:
            if conn:
                conn.close()
