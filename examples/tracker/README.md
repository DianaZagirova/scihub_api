# Processing Tracker Database

## Overview

The **Processing Tracker** is a comprehensive system that monitors and records every step of the paper download and parsing pipeline. It serves as the **single source of truth** for tracking which papers have been processed, which sources were tried, and what the outcomes were.

## Purpose

The tracker solves several critical problems:

1. **Avoid Redundant Work**: Skip papers that have already been successfully processed
2. **Track Download Sources**: Record which source (Sci-Hub, Unpaywall, arXiv, etc.) provided each paper
3. **Monitor Success Rates**: Analyze which sources are most reliable
4. **Enable Recovery**: Resume processing after interruptions without losing progress
5. **Audit Trail**: Maintain complete history of all processing attempts
6. **Intelligent Retry**: Track failed attempts and retry counts to avoid infinite loops

## How It Works

### Workflow Integration

The tracker is automatically updated at each stage of the pipeline:

```
1. DOI Identified → Tracker: Record DOI
2. Download Attempt → Tracker: Mark source attempted
3. Download Success → Tracker: Mark downloaded, record source
4. PDF Validation → Tracker: Validate file integrity
5. Parsing Attempt → Tracker: Mark parser attempted
6. Parsing Success → Tracker: Mark parsing complete
7. DB Import → Tracker: Mark content in database
```

### Multi-Source Tracking

The tracker monitors **6 download sources** independently:

- **Sci-Hub**: Primary source for paywalled papers
- **Unpaywall**: Open Access aggregator
- **arXiv**: Preprint repository
- **bioRxiv**: Biology preprints
- **Europe PMC**: European open access repository
- **Semantic Scholar**: Academic search engine

For each source, it tracks:
- Whether the source was **attempted**
- Whether the download **succeeded**

This enables intelligent fallback logic and source reliability analysis.

## Storage Location

The processing tracker is stored in the main `papers.db` SQLite database in the `processing_tracker` table.

```sql
CREATE TABLE processing_tracker (
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
```

## Status Values

### Availability/Download Status
- `yes` - Available/Successful
- `no` - Not available/Failed
- `unknown` - Not yet attempted

### Parsing Status
- `success` - Parsing completed successfully
- `failed` - Parsing failed
- `not_attempted` - Not yet attempted

## Example Records

See **`example_tracker_records.json`** for detailed examples including:

1. **Successful Sci-Hub download + fast parsing**
2. **Open Access download + dual parser (fast + GROBID)**
3. **Failed download (all sources exhausted)**
4. **Downloaded but parsing failed (corrupted PDF)**
5. **In progress (downloaded, parsing pending)**

### Quick Example

```json
{
  "doi": "10.1016/j.cell.2023.01.001",
  "scihub_available": "yes",
  "scihub_downloaded": "yes",
  "downloaded": "yes",
  "download_date": "2024-10-27 04:30:15",
  "download_source": "scihub",
  "has_content_in_db": "yes",
  "pymupdf_status": "success",
  "pymupdf_date": "2024-10-27 04:31:22",
  "grobid_status": "not_attempted",
  "last_updated": "2024-10-27 04:31:22",
  "retry_count": 0
}
```

## Database Location

The tracker database is part of the main Stage 1 database:
- Default path: `/path/to/papers.db`
- Table: `processing_tracker`
- Events log: `tracker_events` (audit trail)

## Usage

The tracker is automatically updated by:
- `download_papers_optimized.py` - During download and parsing
- `sync_processing_state_to_db.py` - During filesystem validation
- `grobid_tracker_integration.py` - During GROBID processing
- `reconcile_all_status.py` - During manual reconciliation

Query example:
```python
from trackers.doi_tracker_db import DOITracker

tracker = DOITracker('/path/to/papers.db')
status = tracker.get_status('10.1234/example.2024')
print(status)
```
