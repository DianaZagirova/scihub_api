#!/usr/bin/env python3
"""
Optimized version of download_papers.py with per-worker rate limiting.
This allows true parallel downloads while respecting global rate limits.

Key improvements:
1. Token bucket rate limiter (allows N concurrent downloads)
2. Pre-scan to partition identifiers (skip/download/parse)
3. Buffered logging (reduces I/O)

Expected speedup: 5-10x depending on workload
"""

import sys
import os
import sqlite3
import requests
import argparse
import json
import time

# Add src to path for imports
sys.path.insert(0, str(os.path.dirname(os.path.abspath(__file__)) + '/src'))
from config import Config
import datetime
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import deque
import re
from urllib.parse import urlparse, quote

# Import validation functions from sync script
sys.path.insert(0, str(Path(__file__).parent / 'status_sync'))

# Add legacy to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from scihub_fast_downloader import SciHubFastDownloader
from scihub_grobid_downloader import SciHubGrobidDownloader
from trackers.doi_tracker_db import DOITracker

# Import validation functions
try:
    from sync_processing_state_to_db import is_valid_pdf, is_valid_json
except ImportError:
    # Fallback if import fails
    def is_valid_pdf(path):
        try:
            p = Path(path)
            if not p.exists() or p.stat().st_size < 1024:
                return False
            with p.open('rb') as f:
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
    
    def is_valid_json(path, parser_hint):
        try:
            if not Path(path).exists() or Path(path).stat().st_size < 10:
                return False
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not isinstance(data, dict) or not data:
                return False
            return True
        except Exception:
            return False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter that allows burst traffic while maintaining average rate.
    This allows multiple workers to download simultaneously up to the bucket capacity.
    """
    
    def __init__(self, rate=0.5, capacity=5):
        """
        Initialize rate limiter.
        
        Args:
            rate: Tokens per second (e.g., 0.5 = one request every 2 seconds)
            capacity: Maximum tokens in bucket (allows this many simultaneous requests)
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.time()
        self.lock = Lock()
    
    def acquire(self, tokens=1):
        """
        Acquire tokens for a request. Blocks if insufficient tokens available.
        
        Args:
            tokens: Number of tokens to acquire (default 1)
        """
        with self.lock:
            while True:
                now = time.time()
                elapsed = now - self.last_update
                
                # Add tokens based on time elapsed
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now
                
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
                else:
                    # Calculate sleep time needed to accumulate enough tokens
                    needed = tokens - self.tokens
                    sleep_time = needed / self.rate
                    time.sleep(min(sleep_time, 1.0))  # Sleep max 1s at a time


class BufferedLogger:
    """Buffered logger to reduce I/O overhead."""
    
    def __init__(self, log_file, flush_interval=20):
        """
        Initialize buffered logger.
        
        Args:
            log_file: Path to log file
            flush_interval: Number of entries before flushing
        """
        self.log_file = log_file
        self.buffer = deque()
        self.flush_interval = flush_interval
        self.lock = Lock()
    
    def log(self, entry):
        """Add entry to buffer and flush if needed."""
        with self.lock:
            self.buffer.append(entry)
            if len(self.buffer) >= self.flush_interval:
                self._flush()
    
    def _flush(self):
        """Write buffer to file."""
        if self.buffer:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                while self.buffer:
                    f.write(self.buffer.popleft())
    
    def flush(self):
        """Public method to force flush."""
        with self.lock:
            self._flush()


def normalize_identifier(identifier):
    """
    Normalize an identifier to a clean DOI.
    Handles DOI prefixes and converts De Gruyter XML paths to DOIs.
    Returns None if not a valid DOI.
    """
    doi_prefixes = [
        'doi:', 'doi.org/', 'dx.doi.org/', 
        'http://dx.doi.org/', 'https://dx.doi.org/', 
        'http://doi.org/', 'https://doi.org/', 
        'https://www.doi.org/', 'http://www.doi.org/'
    ]
    
    normalized = identifier.strip()
    normalized_lower = normalized.lower()
    
    # Handle De Gruyter XML paths: /j/{journal}/{article-id}/{article-id}.xml -> 10.1515/{article-id}
    if normalized.startswith('/j/') and normalized.endswith('.xml'):
        match = re.search(r'/([a-z0-9\-]+)/\1\.xml$', normalized)
        if match:
            article_id = match.group(1)
            normalized = f"10.1515/{article_id}"
            logger.info(f"Converted De Gruyter path to DOI: {identifier} -> {normalized}")
    
    for prefix in doi_prefixes:
        if normalized_lower.startswith(prefix.lower()):
            normalized = normalized[len(prefix):].strip()
            break
    
    # Validate it's a DOI
    if normalized.startswith('10.') and '/' in normalized:
        return normalized
    
    return None


def normalize_identifier_to_filename(identifier):
    """
    Normalize an identifier to match the filename format used by the downloader.
    """
    doi = normalize_identifier(identifier)
    if doi:
        return doi.replace('/', '_')
    return None


PAPERS_DB = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'

def get_oa_url_for_doi(doi: str) -> str | None:
    try:
        conn = sqlite3.connect(PAPERS_DB)
        cur = conn.cursor()
        cur.execute("SELECT oa_url FROM papers WHERE doi = ?", (doi,))
        row = cur.fetchone()
        conn.close()
        if row and row[0]:
            return row[0]
    except Exception:
        pass
    return None


def try_download_pdf_from_oa(doi: str, oa_url: str, papers_dir: str = './papers', tracker=None, output_dir: str = './output') -> str | None:
    try:
        safe_name = doi.replace('/', '_')
        pdf_path = os.path.join(papers_dir, f"{safe_name}.pdf")
        if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
            # Validate existing file; if invalid, remove it and continue
            try:
                if _is_valid_pdf(pdf_path):
                    return pdf_path
                else:
                    os.remove(pdf_path)
            except Exception:
                try:
                    os.remove(pdf_path)
                except Exception:
                    pass
        sess = requests.Session()
        sess.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept': 'application/pdf,application/octet-stream,*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        headers = {'Referer': oa_url.rsplit('/', 1)[0] if '/' in oa_url else oa_url}
        resp = sess.get(oa_url, timeout=30, allow_redirects=True, headers=headers, stream=True)
        if resp.status_code != 200:
            return None
        ct = resp.headers.get('Content-Type', '').lower()
        first_chunk = b''
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                first_chunk = chunk[:5]
                break
        if ('pdf' not in ct) and (not first_chunk.startswith(b'%PDF-')):
            return None
        with open(pdf_path, 'wb') as f:
            f.write(first_chunk)
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
        # Validate the saved PDF; if invalid by header/EOF, try quick parse as lenient check
        if _is_valid_pdf(pdf_path):
            return pdf_path
        if _quick_parse_validation(doi, pdf_path, save_json=True, output_dir=output_dir, tracker=tracker):
            return pdf_path
        # On failure, remove bad PDF
        try:
            os.remove(pdf_path)
        except Exception:
            pass
        return None
    except Exception as e:
        logger.debug(f"OA download error for {doi}: {e}")
    return None


def _is_sciencedirect_host(url: str) -> bool:
    try:
        return 'sciencedirect.com' in urlparse(url).netloc
    except Exception:
        return False


def _extract_pii_from_sciencedirect_url(url: str) -> str | None:
    try:
        m = re.search(r"/pii/([A-Z0-9]+)", url, re.IGNORECASE)
        return m.group(1) if m else None
    except Exception:
        return None


def resolve_sciencedirect_pdf_url(source_url_or_pii: str, timeout: int = 20) -> str | None:
    try:
        pii = source_url_or_pii
        if '/' in source_url_or_pii:
            pii = _extract_pii_from_sciencedirect_url(source_url_or_pii)
        if not pii:
            return None
        candidates = [
            f"https://www.sciencedirect.com/science/article/pii/{pii}/pdfft",
            f"https://www.sciencedirect.com/science/article/pii/{pii}/pdf",
        ]
        sess = requests.Session()
        sess.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'
        })
        for u in candidates:
            try:
                r = sess.get(u, allow_redirects=True, timeout=timeout, stream=True)
                ct = r.headers.get('Content-Type', '').lower()
                if 'pdf' in ct:
                    first = next(r.iter_content(5), b'')
                    if first.startswith(b'%PDF-'):
                        return r.url
            except Exception:
                continue
    except Exception:
        return None
    return None


def fetch_unpaywall_pdf_url(doi: str, timeout: int = 15) -> str | None:
    try:
        email = Config.UNPAYWALL_EMAIL
        if not email:
            return None
        doi_encoded = quote(doi, safe='')
        url = f"https://api.unpaywall.org/v2/{doi_encoded}?email={email}"
        r = requests.get(url, timeout=timeout)
        if not r.ok:
            return None
        data = r.json()
        locs = []
        if 'best_oa_location' in data and data['best_oa_location']:
            locs.append(data['best_oa_location'])
        if 'oa_locations' in data and data['oa_locations']:
            locs.extend(data['oa_locations'])
        for loc in locs:
            pdf_url = loc.get('url_for_pdf') or loc.get('pdf_url') or loc.get('url')
            if pdf_url:
                return pdf_url
    except Exception:
        return None
    return None


def fetch_openalex_pdf_url(doi: str, timeout: int = 15) -> str | None:
    try:
        doi_norm = doi.lower()
        if not doi_norm.startswith('10.'):
            return None
        doi_encoded = quote(doi, safe='')
        url = f"https://api.openalex.org/works/https://doi.org/{doi_encoded}"
        r = requests.get(url, timeout=timeout)
        if not r.ok:
            return None
        data = r.json()
        # primary_location or locations with pdf_url
        pl = data.get('primary_location') or {}
        pdf = (pl.get('pdf_url') or (pl.get('source') or {}).get('pdf_url'))
        if pdf:
            return pdf
        for loc in data.get('locations', []) or data.get('oa_locations', []) or []:
            pdf = loc.get('pdf_url') or (loc.get('source') or {}).get('pdf_url')
            if pdf:
                return pdf
    except Exception:
        return None
    return None


def fetch_semanticscholar_pdf_url(doi: str, timeout: int = 15) -> str | None:
    try:
        doi_encoded = quote(doi, safe='')
        url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi_encoded}?fields=openAccessPdf"
        r = requests.get(url, timeout=timeout)
        if not r.ok:
            return None
        data = r.json()
        pdf = (data.get('openAccessPdf') or {}).get('url')
        return pdf
    except Exception:
        return None


def fetch_arxiv_pdf_url(doi: str, timeout: int = 40) -> str | None:
    """
    Get arXiv PDF URL from DOI.
    Handles both arXiv DOIs (10.48550/arxiv.*) and searches by DOI.
    """
    try:
        # Check if it's an arXiv DOI
        if '10.48550/arxiv' in doi.lower() or 'arxiv' in doi.lower():
            arxiv_id = doi.split('/')[-1].replace('arxiv.', '')
            return f"https://arxiv.org/pdf/{arxiv_id}.pdf"
        
        # Search arXiv by DOI
        url = "http://export.arxiv.org/api/query"
        params = {'search_query': f'doi:{doi}', 'max_results': 1}
        r = requests.get(url, params=params, timeout=timeout)
        
        if r.status_code == 200 and '<entry>' in r.text:
            import re
            m = re.search(r'<id>http://arxiv.org/abs/([^<]+)</id>', r.text)
            if m:
                arxiv_id = m.group(1)
                return f"https://arxiv.org/pdf/{arxiv_id}.pdf"
    except Exception as e:
        logger.debug(f"arXiv lookup error for {doi}: {e}")
    return None


def fetch_biorxiv_pdf_url(doi: str, timeout: int = 40) -> str | None:
    """
    Get bioRxiv/medRxiv PDF URL from DOI.
    bioRxiv DOIs: 10.1101/*
    """
    try:
        if '10.1101/' in doi:
            # bioRxiv/medRxiv DOIs can be accessed directly
            return f"https://www.biorxiv.org/content/{doi}v1.full.pdf"
    except Exception as e:
        logger.debug(f"bioRxiv lookup error for {doi}: {e}")
    return None


def fetch_europepmc_pdf_url(doi: str, timeout: int = 40) -> str | None:
    """
    Get Europe PMC free full-text URL.
    Returns first available free URL from fullTextUrlList.
    Note: Some papers marked as 'isOpenAccess: N' still have free URLs available.
    """
    try:
        url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
        params = {'query': f'DOI:"{doi}"', 'format': 'json', 'resultType': 'core'}
        r = requests.get(url, params=params, timeout=timeout)
        
        if not r.ok:
            return None
        
        data = r.json()
        results = data.get('resultList', {}).get('result', [])
        
        if not results:
            return None
        
        # Get free full-text URLs (check regardless of isOpenAccess flag)
        full_text_urls = results[0].get('fullTextUrlList', {}).get('fullTextUrl', [])
        
        for url_entry in full_text_urls:
            if url_entry.get('availability', '') == 'Free':
                pdf_url = url_entry.get('url')
                if pdf_url:
                    # Europe PMC often provides landing pages, try to get PDF
                    if 'europepmc.org' in pdf_url and 'PMC' in pdf_url:
                        # Convert to PDF render URL
                        pmc_id = results[0].get('pmcid')
                        if pmc_id:
                            return f"https://europepmc.org/articles/{pmc_id}?pdf=render"
                    return pdf_url
    except Exception as e:
        logger.debug(f"Europe PMC lookup error for {doi}: {e}")
    return None


def resolve_doi_pdf_url(doi: str, timeout: int = 15) -> str | None:
    try:
        headers = {
            'Accept': 'application/pdf',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'
        }
        doi_encoded = quote(doi, safe='')
        r = requests.get(f"https://doi.org/{doi_encoded}", headers=headers, allow_redirects=True, timeout=timeout)
        if not r.ok:
            return None
        if 'pdf' in r.headers.get('Content-Type', '').lower():
            return r.url
        # Some publishers return HTML but redirect URL contains known PDF endpoints
        m = re.search(r"(https://[^\s'\"]+\.pdf)", r.text, re.IGNORECASE)
        if m:
            return m.group(1)
    except Exception:
        return None
    return None


def _is_valid_pdf(path: str) -> bool:
    try:
        p = Path(path)
        if not p.exists() or p.stat().st_size < 1024:
            return False
        with p.open('rb') as f:
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


def _quick_parse_validation(doi: str, pdf_path: str, save_json: bool = True, output_dir: str = './output', tracker=None) -> bool:
    """Lenient PDF validation using fast parser. If parser extracts meaningful content,
    consider the PDF acceptable even if header/EOF checks fail.
    - Returns True to keep PDF (and JSON if save_json).
    - Returns False to indicate removal is advised (also deletes JSON if created).
    Also marks tracker PyMuPDF status if tracker provided.
    """
    try:
        # Lazy import to avoid overhead unless needed
        sys.path.insert(0, str(Path(__file__).parent / 'src'))
        from fast_pdf_parser import FastPDFParser  # type: ignore

        parser = FastPDFParser(output_dir=output_dir)
        # Use process_and_save when save_json else process_pdf
        if save_json:
            res = parser.process_and_save(pdf_path, mode='structured', output_dir=output_dir)
        else:
            res = parser.process_pdf(pdf_path, mode='structured')
        ok = False
        if isinstance(res, dict):
            st = res.get('structured_text') or {}
            if isinstance(st, dict):
                full_text = st.get('full_text') or ''
                page_count = int(st.get('page_count') or 0)
                # Heuristic: accept if we have some text or at least 1 page parsed
                ok = (len(full_text.strip()) >= 200) or (page_count >= 1)
        # Tracker update
        if tracker is not None:
            try:
                tracker.mark_pymupdf_processed(doi, success=ok)
            except Exception:
                pass
        if ok:
            return True
        # Cleanup JSON if produced but weak
        try:
            base = os.path.splitext(os.path.basename(pdf_path))[0]
            json_path = os.path.join(output_dir, f"{base}_fast.json")
            if os.path.exists(json_path):
                os.remove(json_path)
        except Exception:
            pass
        return False
    except Exception:
        # Parsing failed, consider invalid
        try:
            if tracker is not None:
                tracker.mark_pymupdf_processed(doi, success=False)
        except Exception:
            pass
        return False


def try_download_from_url(doi: str, url: str, papers_dir: str = './papers', tracker=None, output_dir: str = './output') -> str | None:
    try:
        safe_name = doi.replace('/', '_')
        pdf_path = os.path.join(papers_dir, f"{safe_name}.pdf")
        sess = requests.Session()
        sess.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'})
        r = sess.get(url, allow_redirects=True, timeout=30, stream=True)
        if not r.ok:
            return None
        ct = r.headers.get('Content-Type', '').lower()
        first = next(r.iter_content(5), b'')
        if ('pdf' not in ct) and (not first.startswith(b'%PDF-')):
            return None
        with open(pdf_path, 'wb') as f:
            f.write(first)
            for chunk in r.iter_content(65536):
                if chunk:
                    f.write(chunk)
        # Validate saved PDF; if basic check fails, try quick parse to decide keep/remove
        if _is_valid_pdf(pdf_path):
            return pdf_path
        if _quick_parse_validation(doi, pdf_path, save_json=True, output_dir=output_dir, tracker=tracker):
            return pdf_path
        try:
            os.remove(pdf_path)
        except Exception:
            pass
        return None
    except Exception:
        return None


def attempt_multi_source_pdf(doi: str, oa_url: str | None, papers_dir: str = './papers', tracker=None) -> tuple[str | None, str | None]:
    # Returns (pdf_path, source_label)
    logger.info(f"[OA Fallback] Starting multi-source PDF retrieval for {doi}")
    
    # 1) DB OA URL (plus ScienceDirect resolver if applicable)
    if oa_url:
        logger.info(f"[OA Fallback] Trying DB OA URL: {oa_url[:80]}...")
        if _is_sciencedirect_host(oa_url):
            resolved = resolve_sciencedirect_pdf_url(oa_url)
            if resolved:
                pdf_path = try_download_from_url(doi, resolved, papers_dir, tracker=tracker)
                if pdf_path:
                    logger.info(f"[OA Fallback] ✓ Success via oa_sciencedirect")
                    return pdf_path, 'oa_sciencedirect'
        # Try direct OA URL as-is
        pdf_path = try_download_pdf_from_oa(doi, oa_url, papers_dir, tracker=tracker)
        if pdf_path:
            logger.info(f"[OA Fallback] ✓ Success via oa_direct")
            return pdf_path, 'oa_direct'
    
    # 2) OpenAlex (keep early as it's fast and comprehensive)
    logger.info(f"[OA Fallback] Trying OpenAlex API...")
    oa = fetch_openalex_pdf_url(doi)
    if oa:
        logger.info(f"[OA Fallback] Found OpenAlex URL: {oa[:80]}...")
        if _is_sciencedirect_host(oa):
            resolved = resolve_sciencedirect_pdf_url(oa)
            if resolved:
                pdf_path = try_download_from_url(doi, resolved, papers_dir, tracker=tracker)
                if pdf_path:
                    logger.info(f"[OA Fallback] ✓ Success via openalex_sciencedirect")
                    return pdf_path, 'openalex_sciencedirect'
        pdf_path = try_download_from_url(doi, oa, papers_dir, tracker=tracker)
        if pdf_path:
            logger.info(f"[OA Fallback] ✓ Success via openalex")
            return pdf_path, 'openalex'
    
    # 3) arXiv (preprints - high success rate)
    logger.info(f"[OA Fallback] Trying arXiv...")
    if tracker:
        tracker.mark_source_attempted(doi, 'arxiv')
    arxiv_url = fetch_arxiv_pdf_url(doi)
    if arxiv_url:
        logger.info(f"[OA Fallback] Found arXiv URL: {arxiv_url[:80]}...")
        pdf_path = try_download_from_url(doi, arxiv_url, papers_dir, tracker=tracker)
        if pdf_path:
            logger.info(f"[OA Fallback] ✓ Success via arxiv")
            if tracker:
                tracker.mark_source_downloaded(doi, 'arxiv', success=True)
            return pdf_path, 'arxiv'
        if tracker:
            tracker.mark_source_downloaded(doi, 'arxiv', success=False)
    
    # 4) bioRxiv/medRxiv (biology/medicine preprints)
    logger.info(f"[OA Fallback] Trying bioRxiv/medRxiv...")
    if tracker:
        tracker.mark_source_attempted(doi, 'biorxiv')
    biorxiv_url = fetch_biorxiv_pdf_url(doi)
    if biorxiv_url:
        logger.info(f"[OA Fallback] Found bioRxiv URL: {biorxiv_url[:80]}...")
        pdf_path = try_download_from_url(doi, biorxiv_url, papers_dir, tracker=tracker)
        if pdf_path:
            logger.info(f"[OA Fallback] ✓ Success via biorxiv")
            if tracker:
                tracker.mark_source_downloaded(doi, 'biorxiv', success=True)
            return pdf_path, 'biorxiv'
        if tracker:
            tracker.mark_source_downloaded(doi, 'biorxiv', success=False)
    
    # 5) Europe PMC (life sciences repository)
    logger.info(f"[OA Fallback] Trying Europe PMC...")
    if tracker:
        tracker.mark_source_attempted(doi, 'europepmc')
    epmc_url = fetch_europepmc_pdf_url(doi)
    if epmc_url:
        logger.info(f"[OA Fallback] Found Europe PMC URL: {epmc_url[:80]}...")
        pdf_path = try_download_from_url(doi, epmc_url, papers_dir, tracker=tracker)
        if pdf_path:
            logger.info(f"[OA Fallback] ✓ Success via europepmc")
            if tracker:
                tracker.mark_source_downloaded(doi, 'europepmc', success=True)
            return pdf_path, 'europepmc'
        if tracker:
            tracker.mark_source_downloaded(doi, 'europepmc', success=False)
    
    # 6) Unpaywall (comprehensive OA aggregator)
    logger.info(f"[OA Fallback] Trying Unpaywall API...")
    if tracker:
        tracker.mark_source_attempted(doi, 'unpaywall')
    up = fetch_unpaywall_pdf_url(doi)
    if up:
        logger.info(f"[OA Fallback] Found Unpaywall URL: {up[:80]}...")
        # ScienceDirect handling if needed
        if _is_sciencedirect_host(up):
            resolved = resolve_sciencedirect_pdf_url(up)
            if resolved:
                pdf_path = try_download_from_url(doi, resolved, papers_dir, tracker=tracker)
                if pdf_path:
                    logger.info(f"[OA Fallback] ✓ Success via unpaywall_sciencedirect")
                    if tracker:
                        tracker.mark_source_downloaded(doi, 'unpaywall', success=True)
                    return pdf_path, 'unpaywall'
        pdf_path = try_download_from_url(doi, up, papers_dir, tracker=tracker)
        if pdf_path:
            logger.info(f"[OA Fallback] ✓ Success via unpaywall")
            if tracker:
                tracker.mark_source_downloaded(doi, 'unpaywall', success=True)
            return pdf_path, 'unpaywall'
        if tracker:
            tracker.mark_source_downloaded(doi, 'unpaywall', success=False)
    
    # 7) Semantic Scholar
    logger.info(f"[OA Fallback] Trying Semantic Scholar API...")
    ss = fetch_semanticscholar_pdf_url(doi)
    if ss:
        logger.info(f"[OA Fallback] Found Semantic Scholar URL: {ss[:80]}...")
        pdf_path = try_download_from_url(doi, ss, papers_dir, tracker=tracker)
        if pdf_path:
            logger.info(f"[OA Fallback] ✓ Success via semanticscholar")
            return pdf_path, 'semanticscholar'
    
    # 8) DOI content negotiation (last resort)
    logger.info(f"[OA Fallback] Trying DOI content negotiation...")
    cn = resolve_doi_pdf_url(doi)
    if cn:
        logger.info(f"[OA Fallback] Found DOI redirect URL: {cn[:80]}...")
        if _is_sciencedirect_host(cn):
            resolved = resolve_sciencedirect_pdf_url(cn)
            if resolved:
                pdf_path = try_download_from_url(doi, resolved, papers_dir, tracker=tracker)
                if pdf_path:
                    logger.info(f"[OA Fallback] ✓ Success via doi_sciencedirect")
                    return pdf_path, 'doi_sciencedirect'
        pdf_path = try_download_from_url(doi, cn, papers_dir, tracker=tracker)
        if pdf_path:
            logger.info(f"[OA Fallback] ✓ Success via doi_content_neg")
            return pdf_path, 'doi_content_neg'
    
    logger.warning(f"[OA Fallback] ✗ All OA sources exhausted for {doi}")
    return None, None

def partition_identifiers(identifiers, parser_type, papers_dir='./papers', output_dir='./output', tracker=None):
    """
    Pre-scan identifiers and partition them into categories.
    Uses tracker to make intelligent decisions about skipping/retrying.
    
    Returns:
        tuple: (needs_download, needs_parse_only, complete, skipped_failed)
    """
    needs_download = []
    needs_parse_only = []
    complete = []
    skipped_failed = []
    RETRIED_ALLOW_SCIHUB = 1
    RETRIED_ALLOW_PARSER= 2
    
    logger.info(f"Pre-scanning {len(identifiers)} identifiers with tracker intelligence...")
    
    for identifier in identifiers:
        # Check tracker first for intelligent decisions
        if tracker:
            status = tracker.get_status(identifier)
            if status:
                # 1. Check if all known sources appear exhausted
                scihub_no = status.get('scihub_available') == tracker.AVAILABLE_NO or status.get('scihub_downloaded') == tracker.AVAILABLE_NO
                oa_no = status.get('oa_available') == tracker.AVAILABLE_NO or status.get('oa_downloaded') == tracker.AVAILABLE_NO
                arxiv_no = (status.get('arxiv_attempted') == tracker.AVAILABLE_YES and status.get('arxiv_downloaded') == tracker.AVAILABLE_NO)
                biorxiv_no = (status.get('biorxiv_attempted') == tracker.AVAILABLE_YES and status.get('biorxiv_downloaded') == tracker.AVAILABLE_NO)
                epmc_no = (status.get('europepmc_attempted') == tracker.AVAILABLE_YES and status.get('europepmc_downloaded') == tracker.AVAILABLE_NO)
                upw_no = (status.get('unpaywall_attempted') == tracker.AVAILABLE_YES and status.get('unpaywall_downloaded') == tracker.AVAILABLE_NO)
                downloaded_no = status.get('downloaded') == tracker.AVAILABLE_NO
                exhausted = (
                    scihub_no and oa_no and (arxiv_no or status.get('arxiv_attempted') == tracker.AVAILABLE_YES) 
                    and (biorxiv_no or status.get('biorxiv_attempted') == tracker.AVAILABLE_YES)
                    and (epmc_no or status.get('europepmc_attempted') == tracker.AVAILABLE_YES)
                    and (upw_no or status.get('unpaywall_attempted') == tracker.AVAILABLE_YES)
                    and downloaded_no
                )
                if exhausted:
                    retry_count = int(status.get('retry_count', 0))
                    if retry_count >= RETRIED_ALLOW_SCIHUB:
                        logger.debug(f"Skip {identifier} - all sources exhausted (retries: {retry_count})")
                        skipped_failed.append(identifier)
                        continue
                
                # 2. Check if already downloaded successfully
                downloaded = status.get('downloaded') == tracker.AVAILABLE_YES
                
                # 3. Check parsing status for requested parser
                if parser_type == 'fast':
                    parse_status = status.get('pymupdf_status', '')
                    # Skip if already successfully parsed with PyMuPDF
                    if parse_status == tracker.STATUS_SUCCESS:
                        logger.debug(f"Skip {identifier} - already parsed with PyMuPDF")
                        complete.append(identifier)
                        continue
                    # Allow retry if failed (will try again)
                    if parse_status == tracker.STATUS_FAILED:
                        retry_count = int(status.get('retry_count', 0))
                        if retry_count >= RETRIED_ALLOW_PARSER:
                            logger.debug(f"Skip {identifier} - PyMuPDF failed {retry_count} times")
                            skipped_failed.append(identifier)
                            continue
                        else:
                            logger.debug(f"Retry {identifier} - PyMuPDF failed {retry_count} times")
                            # Will process below
                else:  # grobid
                    parse_status = status.get('grobid_status', '')
                    # Skip if already successfully parsed with Grobid
                    if parse_status == tracker.STATUS_SUCCESS:
                        logger.debug(f"Skip {identifier} - already parsed with Grobid")
                        complete.append(identifier)
                        continue
                    # Allow retry if failed (will try again)
                    if parse_status == tracker.STATUS_FAILED:
                        retry_count = int(status.get('retry_count', 0))
                        if retry_count >= RETRIED_ALLOW_PARSER:
                            logger.debug(f"Skip {identifier} - Grobid failed {retry_count} times")
                            skipped_failed.append(identifier)
                            continue
                        else:
                            logger.debug(f"Retry {identifier} - Grobid failed {retry_count} times")
                            # Will process below
                
                # 4. If downloaded but needs parsing, add to parse-only
                if downloaded:
                    # Check if PDF still exists
                    safe_name = normalize_identifier_to_filename(identifier)
                    if safe_name:
                        pdf_path = os.path.join(papers_dir, f'{safe_name}.pdf')
                        if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
                            needs_parse_only.append((identifier, pdf_path))
                            continue
                    # PDF missing, need to redownload
                    needs_download.append(identifier)
                    continue
        
        safe_name = normalize_identifier_to_filename(identifier)
        
        if not safe_name:
            # Can't predict filename, assume needs download
            needs_download.append(identifier)
            continue
        
        # Check PDF existence
        pdf_path = os.path.join(papers_dir, f'{safe_name}.pdf')
        pdf_exists = os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0
        
        # Check JSON existence
        if parser_type == 'fast':
            json_filename = f'{safe_name}_fast.json'
        else:
            json_filename = f'{safe_name}.json'
        
        json_path = os.path.join(output_dir, json_filename)
        json_exists = os.path.exists(json_path) and os.path.getsize(json_path) > 0
        
        if not pdf_exists:
            needs_download.append(identifier)
        elif not json_exists:
            needs_parse_only.append((identifier, pdf_path))
        else:
            complete.append(identifier)
    
    logger.info(f"Partition results:")
    logger.info(f"  - Needs download: {len(needs_download)}")
    logger.info(f"  - Needs parse only: {len(needs_parse_only)}")
    logger.info(f"  - Already complete: {len(complete)}. Examples: {complete[:15]}")
    logger.info(f"  - Skipped (failed/unavailable): {len(skipped_failed)}")
    
    return needs_download, needs_parse_only, complete, skipped_failed


def process_single_with_rate_limit(downloader, identifier, parser_type, parse_mode, 
                                   rate_limiter, buffered_logger, tracker=None):
    """
    Process a single identifier with token bucket rate limiting.
    Updates tracker with status at each step.
    
    Returns:
        dict: Result with detailed status
    """
    # Normalize identifier to clean DOI
    clean_doi = normalize_identifier(identifier)
    if not clean_doi:
        logger.warning(f"Skipping invalid identifier: {identifier}")
        return {
            'identifier': identifier,
            'status': 'invalid_identifier',
            'download_status': 'skipped',
            'parsing_status': 'skipped',
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    result = {
        'identifier': clean_doi,
        'original_identifier': identifier,
        'pdf_path': None,
        'json_path': None,
        'status': None,
        'download_status': None,
        'parsing_status': None,
        'parser_used': parser_type,
        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Acquire token before downloading (rate limiting)
    rate_limiter.acquire()
    
    # Download
    try:
        if hasattr(downloader, 'downloader'):
            pdf_path = downloader.downloader.download_paper(clean_doi)
        else:
            pdf_path = downloader.download_paper(clean_doi)
        
        if pdf_path:
            result['pdf_path'] = pdf_path
            result['download_status'] = 'success'
            logger.info(f"Downloaded: {clean_doi}")
            
            # Update tracker: downloaded successfully
            if tracker:
                tracker.mark_downloaded(clean_doi, source='scihub')
        else:
            result['download_status'] = 'failed'
            result['status'] = 'not_found'
            logger.error(f"Failed to download: {clean_doi}")
            
            # Update tracker: not found in Sci-Hub
            if tracker:
                tracker.mark_scihub_found(clean_doi, available=False)
            # OA/API fallbacks (robust multi-source resolution)
            oa_url = get_oa_url_for_doi(clean_doi)
            logger.info(f"Trying OA/API fallbacks for {clean_doi}...")
            pdf_path, source_label = attempt_multi_source_pdf(clean_doi, oa_url, papers_dir='./papers', tracker=tracker)
            if pdf_path:
                result['pdf_path'] = pdf_path
                result['download_status'] = 'success'
                result['status'] = None  # continue to parse
                logger.info(f"Fallback download succeeded for {clean_doi} via {source_label}")
                if tracker:
                    tracker.mark_oa_available(clean_doi, available=True)
                    tracker.mark_downloaded(clean_doi, source=(source_label or 'oa'))
            else:
                # Check for manually added PDFs before giving up
                safe_doi = re.sub(r'[^\w\-_.]', '_', clean_doi)
                manual_pdf_paths = [
                    f'./papers/{safe_doi}.pdf',
                    f'./add_manually/{safe_doi}.pdf',
                    f'./pdfs/{safe_doi}.pdf'
                ]
                
                for manual_path in manual_pdf_paths:
                    if os.path.exists(manual_path):
                        logger.info(f"Found manually added PDF: {manual_path}")
                        pdf_path = manual_path
                        result['pdf_path'] = pdf_path
                        result['download_status'] = 'manual'
                        result['status'] = None  # continue to parse
                        if tracker:
                            tracker.mark_downloaded(clean_doi, source='manual')
                        break
                
                if not pdf_path:
                    # Log and return if all fallbacks failed
                    if tracker:
                        tracker.mark_oa_available(clean_doi, available=False)
                        tracker.increment_retry(clean_doi)
                    log_entry = f"\n{'='*80}\n"
                    log_entry += f"DOI: {clean_doi}\n"
                    log_entry += f"Status: NOT FOUND (Sci-Hub + OA/APIs + Manual)\n"
                    buffered_logger.log(log_entry)
                    return result
    except Exception as e:
        result['download_status'] = 'error'
        result['status'] = 'not_found'
        logger.error(f"Error downloading {clean_doi}: {e}")
        return result
    
    # Parse (no rate limiting needed)
    try:
        safe_name = os.path.splitext(os.path.basename(pdf_path))[0]
        if parser_type == 'fast':
            json_filename = f'{safe_name}_fast.json'
        else:
            json_filename = f'{safe_name}.json'
        
        output_dir = './output'
        json_path = os.path.join(output_dir, json_filename)
        
        if hasattr(downloader, 'parser'):
            if parser_type == 'fast':
                extracted_data = downloader.parser.process_and_save(
                    pdf_path, mode=parse_mode, output_dir=output_dir
                )
            else:
                extracted_data = downloader.parser.process_and_save(
                    pdf_path, output_dir=output_dir
                )
        else:
            extracted_data = None
        
        if extracted_data:
            result['json_path'] = json_path
            result['parsing_status'] = 'success'
            result['status'] = 'success'
            logger.info(f"Parsed: {clean_doi}")
            
            # Update tracker: parsing succeeded
            if tracker:
                if parser_type == 'fast':
                    tracker.mark_pymupdf_processed(clean_doi, success=True)
                else:
                    tracker.mark_grobid_processed(clean_doi, success=True)
        else:
            result['parsing_status'] = 'failed'
            result['status'] = 'processing_failed'
            logger.error(f"Failed to parse: {clean_doi}")
            
            # Update tracker: parsing failed
            if tracker:
                if parser_type == 'fast':
                    tracker.mark_pymupdf_processed(clean_doi, success=False)
                else:
                    tracker.mark_grobid_processed(clean_doi, success=False)
    except Exception as e:
        result['parsing_status'] = 'error'
        result['status'] = 'processing_failed'
        logger.error(f"Error parsing {clean_doi}: {e}")
        
        # Update tracker: parsing error
        if tracker:
            if parser_type == 'fast':
                tracker.mark_pymupdf_processed(clean_doi, success=False, error_msg=str(e))
            else:
                tracker.mark_grobid_processed(clean_doi, success=False, error_msg=str(e))
    
    # Log to buffer
    log_entry = f"\n{'='*80}\n"
    log_entry += f"DOI: {clean_doi}\n"
    if identifier != clean_doi:
        log_entry += f"Original: {identifier}\n"
    log_entry += f"Status: {result['status']}\n"
    log_entry += f"Timestamp: {result['timestamp']}\n"
    buffered_logger.log(log_entry)
    
    return result


def process_optimized(downloader, identifiers, num_workers, delay, log_file, parser_type, parse_mode, tracker=None):
    """
    Optimized processing with token bucket rate limiting and pre-scanning.
    Updates DOI tracker with processing status.
    
    Returns:
        list: List of results
    """
    # Initialize log file
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"{'='*80}\n")
        f.write(f"OPTIMIZED PROCESSING LOG\n")
        f.write(f"{'='*80}\n")
        f.write(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Parser: {parser_type.upper()}\n")
        f.write(f"Workers: {num_workers}\n")
        f.write(f"Rate: {1/delay:.2f} req/s\n")
        f.write(f"{'='*80}\n")
    
    buffered_logger = BufferedLogger(log_file, flush_interval=20)
    
    # Create token bucket rate limiter
    # rate = requests per second, capacity = max concurrent requests
    rate = 1.0 / delay
    capacity = min(num_workers, int(rate * 10))  # Allow 10 seconds worth of burst
    rate_limiter = TokenBucketRateLimiter(rate=rate, capacity=capacity)
    
    logger.info(f"Rate limiter: {rate:.2f} req/s, capacity: {capacity} tokens")
    
    # Partition identifiers (with tracker awareness)
    needs_download, needs_parse_only, complete, skipped_failed = partition_identifiers(
        identifiers, parser_type, tracker=tracker
    )
    
    results = []
    
    # Process complete ones instantly (no downloads needed)
    for identifier in complete:
        result = {
            'identifier': identifier,
            'status': 'skipped_complete',
            'download_status': 'skipped_exists',
            'parsing_status': 'skipped_exists',
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        results.append(result)
    
    # Track skipped/failed ones
    for identifier in skipped_failed:
        result = {
            'identifier': identifier,
            'status': 'skipped_failed',
            'download_status': 'skipped_unavailable',
            'parsing_status': 'skipped_failed',
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        results.append(result)
    
    # Process parse-only (no rate limiting, just parsing)
    if needs_parse_only:
        logger.info(f"\nProcessing {len(needs_parse_only)} papers (parse only, no downloads)...")
        
        for identifier, pdf_path in needs_parse_only:
            try:
                # Normalize DOI
                clean_doi = normalize_identifier(identifier)
                
                # Prepare result
                result = {
                    'identifier': identifier,
                    'pdf_path': pdf_path,
                    'json_path': None,
                    'status': None,
                    'download_status': 'skipped_exists',
                    'parsing_status': None,
                    'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Determine output filename
                safe_name = os.path.splitext(os.path.basename(pdf_path))[0]
                if parser_type == 'fast':
                    json_filename = f'{safe_name}_fast.json'
                else:
                    json_filename = f'{safe_name}.json'
                
                output_dir = './output'
                json_path = os.path.join(output_dir, json_filename)
                
                # Parse the PDF
                if hasattr(downloader, 'parser'):
                    if parser_type == 'fast':
                        extracted_data = downloader.parser.process_and_save(
                            pdf_path, mode=parse_mode, output_dir=output_dir
                        )
                    else:
                        extracted_data = downloader.parser.process_and_save(
                            pdf_path, output_dir=output_dir
                        )
                else:
                    extracted_data = None
                
                if extracted_data:
                    result['json_path'] = json_path
                    result['parsing_status'] = 'success'
                    result['status'] = 'success'
                    logger.info(f"Parsed (parse-only): {clean_doi}")
                    
                    # Update tracker: parsing succeeded
                    if tracker:
                        if parser_type == 'fast':
                            tracker.mark_pymupdf_processed(clean_doi, success=True)
                        else:
                            tracker.mark_grobid_processed(clean_doi, success=True)
                else:
                    result['parsing_status'] = 'failed'
                    result['status'] = 'processing_failed'
                    logger.error(f"Failed to parse (parse-only): {clean_doi}")
                    
                    # Update tracker: parsing failed
                    if tracker:
                        if parser_type == 'fast':
                            tracker.mark_pymupdf_processed(clean_doi, success=False)
                        else:
                            tracker.mark_grobid_processed(clean_doi, success=False)
                
                # Log to buffer
                log_entry = f"\n{'='*80}\n"
                log_entry += f"DOI: {clean_doi}\n"
                if identifier != clean_doi:
                    log_entry += f"Original: {identifier}\n"
                log_entry += f"Mode: Parse-only (PDF exists)\n"
                log_entry += f"Status: {result['status']}\n"
                log_entry += f"Timestamp: {result['timestamp']}\n"
                buffered_logger.log(log_entry)
                
                results.append(result)
                
            except Exception as e:
                logger.error(f"Error parsing (parse-only) {identifier}: {e}")
                result = {
                    'identifier': identifier,
                    'pdf_path': pdf_path,
                    'json_path': None,
                    'status': 'processing_failed',
                    'download_status': 'skipped_exists',
                    'parsing_status': 'error',
                    'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                results.append(result)
                
                # Update tracker: parsing error
                if tracker:
                    clean_doi = normalize_identifier(identifier)
                    if parser_type == 'fast':
                        tracker.mark_pymupdf_processed(clean_doi, success=False, error_msg=str(e))
                    else:
                        tracker.mark_grobid_processed(clean_doi, success=False, error_msg=str(e))
    
    # Process downloads with rate limiting
    if needs_download:
        logger.info(f"\nDownloading and processing {len(needs_download)} papers...")
        logger.info(f"Using {num_workers} workers with {delay}s delay\n")
        
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_identifier = {
                executor.submit(
                    process_single_with_rate_limit,
                    downloader, identifier, parser_type, parse_mode,
                    rate_limiter, buffered_logger, tracker
                ): identifier
                for identifier in needs_download
            }
            
            for i, future in enumerate(as_completed(future_to_identifier), 1):
                identifier = future_to_identifier[future]
                print(f"[{i}/{len(needs_download)}] Completed: {identifier}")
                result = future.result()
                results.append(result)
    
    # Flush remaining log entries
    buffered_logger.flush()
    
    # Flush tracker to disk
    if tracker:
        logger.info("Flushing tracker to disk...")
        tracker.flush()
    
    # Write summary
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"\n\n{'='*80}\n")
        f.write(f"SUMMARY\n")
        f.write(f"{'='*80}\n")
        f.write(f"Total: {len(results)}\n")
        f.write(f"Success: {sum(1 for r in results if r.get('status') == 'success')}\n")
        f.write(f"Skipped (complete): {len(complete)}\n")
        f.write(f"Skipped (failed/unavailable): {len(skipped_failed)}\n")
        f.write(f"Not Found: {sum(1 for r in results if r.get('status') == 'not_found')}\n")
        f.write(f"Failed: {sum(1 for r in results if r.get('status') == 'processing_failed')}\n")
    
    return results


def reset_dois_for_list(dois, papers_dir='./papers', output_dir='./output', tracker=None, input_file=None):
    """
    Reset tracking and validate/delete invalid files for specific DOIs.
    
    Steps:
    0. Remove previously reset DOIs from the list
    1. Reset tracker for these DOIs in database
    2. Check PDFs - if invalid, delete PDF and corresponding JSONs, reset tracker
    3. Check JSONs - if invalid, delete them, reset parser status in tracker
    4. Save the reset DOI list to reseted_dois/ with timestamp
    
    Args:
        dois: List of DOI identifiers to reset
        papers_dir: Directory containing PDFs
        output_dir: Directory containing JSONs
        tracker: DOITracker instance
        input_file: Path to input file (for copying to reseted_dois/)
    
    Returns:
        dict: Statistics about reset operations
    """
    logger.info("\n" + "="*70)
    logger.info("RESET MODE: Validating and resetting DOIs")
    logger.info("="*70)
    
    # Create reseted_dois directory if it doesn't exist
    reseted_dois_dir = Path('./reseted_dois')
    reseted_dois_dir.mkdir(exist_ok=True)
    
    # Step 0: Check for previously reset DOIs
    previously_reset = set()
    reset_history_file = reseted_dois_dir / 'reset_history.txt'
    
    if reset_history_file.exists():
        logger.info("Checking for previously reset DOIs...")
        with open(reset_history_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    previously_reset.add(line)
        logger.info(f"Found {len(previously_reset)} previously reset DOIs")
    
    # Filter out previously reset DOIs
    original_count = len(dois)
    dois_to_reset = []
    skipped_already_reset = []
    
    for doi in dois:
        clean_doi = normalize_identifier(doi)
        if clean_doi and clean_doi in previously_reset:
            skipped_already_reset.append(doi)
            logger.debug(f"Skipping already reset DOI: {clean_doi}")
        else:
            dois_to_reset.append(doi)
    
    if skipped_already_reset:
        logger.info(f"Skipped {len(skipped_already_reset)} DOIs that were previously reset")
        logger.info(f"Processing {len(dois_to_reset)} DOIs (out of {original_count} total)")
    
    if not dois_to_reset:
        logger.warning("No DOIs to reset after filtering. All DOIs were previously reset.")
        return {
            'total_dois': 0,
            'skipped_previously_reset': len(skipped_already_reset),
            'tracker_reset': 0,
            'invalid_pdfs': 0,
            'invalid_grobid_jsons': 0,
            'invalid_pymupdf_jsons': 0,
            'pdfs_deleted': 0,
            'jsons_deleted': 0
        }
    
    # Save the list of DOIs being reset to reseted_dois/ with timestamp
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    reset_list_file = reseted_dois_dir / f'reset_list_{timestamp}.txt'
    
    logger.info(f"Saving reset list to: {reset_list_file}")
    with open(reset_list_file, 'w', encoding='utf-8') as f:
        f.write(f"# Reset performed at: {datetime.datetime.now().isoformat()}\n")
        f.write(f"# Total DOIs: {len(dois_to_reset)}\n")
        if input_file:
            f.write(f"# Source file: {input_file}\n")
        f.write(f"# Skipped (previously reset): {len(skipped_already_reset)}\n")
        f.write("\n")
        for doi in dois_to_reset:
            f.write(f"{doi}\n")
    
    logger.info(f"✓ Saved {len(dois_to_reset)} DOIs to {reset_list_file}")
    
    stats = {
        'total_dois': len(dois_to_reset),
        'skipped_previously_reset': len(skipped_already_reset),
        'tracker_reset': 0,
        'invalid_pdfs': 0,
        'invalid_grobid_jsons': 0,
        'invalid_pymupdf_jsons': 0,
        'pdfs_deleted': 0,
        'jsons_deleted': 0
    }
    
    papers_path = Path(papers_dir)
    output_path = Path(output_dir)
    
    logger.info(f"\nProcessing {len(dois_to_reset)} DOIs for reset...")
    
    # Track successfully reset DOIs for history
    successfully_reset = []
    
    for i, identifier in enumerate(dois_to_reset, 1):
        if i % 100 == 0:
            logger.info(f"Progress: {i}/{len(dois_to_reset)} DOIs processed")
        
        # Normalize DOI
        clean_doi = normalize_identifier(identifier)
        if not clean_doi:
            logger.warning(f"Could not normalize identifier: {identifier}")
            continue
        
        safe_name = clean_doi.replace('/', '_')
        
        # Step 1: Reset tracker for this DOI
        if tracker:
            # Retry logic for database locks
            max_retries = 5
            retry_delay = 0.1  # Start with 100ms
            
            for attempt in range(max_retries):
                try:
                    # Reset all tracking fields for this DOI
                    tracker.reset_doi(clean_doi)
                    stats['tracker_reset'] += 1
                    successfully_reset.append(clean_doi)
                    logger.debug(f"Reset tracker for {clean_doi}")
                    break  # Success, exit retry loop
                except Exception as e:
                    if 'database is locked' in str(e) and attempt < max_retries - 1:
                        # Database locked, retry with exponential backoff
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        logger.debug(f"Database locked for {clean_doi}, retrying (attempt {attempt + 2}/{max_retries})...")
                    else:
                        # Final failure or non-lock error
                        logger.error(f"Failed to reset tracker for {clean_doi}: {e}")
                        break
        
        # Small delay to reduce database contention
        time.sleep(0.01)  # 10ms between DOIs
        
        # Step 2: Check PDF validity
        pdf_path = papers_path / f"{safe_name}.pdf"
        pdf_invalid = False
        
        if pdf_path.exists():
            if not is_valid_pdf(pdf_path):
                pdf_invalid = True
                stats['invalid_pdfs'] += 1
                logger.warning(f"Invalid PDF found: {pdf_path.name}")
                
                # Delete invalid PDF
                try:
                    pdf_path.unlink()
                    stats['pdfs_deleted'] += 1
                    logger.info(f"Deleted invalid PDF: {pdf_path.name}")
                except Exception as e:
                    logger.error(f"Failed to delete PDF {pdf_path}: {e}")
                
                # Delete corresponding JSONs (both fast and regular)
                grobid_json = output_path / f"{safe_name}.json"
                pymupdf_json = output_path / f"{safe_name}_fast.json"
                
                for json_path in [grobid_json, pymupdf_json]:
                    if json_path.exists():
                        try:
                            json_path.unlink()
                            stats['jsons_deleted'] += 1
                            logger.info(f"Deleted JSON (PDF invalid): {json_path.name}")
                        except Exception as e:
                            logger.error(f"Failed to delete JSON {json_path}: {e}")
                
                # Reset tracker again after deletion
                if tracker:
                    try:
                        tracker.reset_doi(clean_doi)
                    except Exception as e:
                        logger.error(f"Failed to reset tracker after PDF deletion for {clean_doi}: {e}")
        
        # Step 3: Check JSON validity (only if PDF was valid or doesn't exist)
        if not pdf_invalid:
            # Check Grobid JSON
            grobid_json = output_path / f"{safe_name}.json"
            if grobid_json.exists():
                if not is_valid_json(grobid_json, 'grobid'):
                    stats['invalid_grobid_jsons'] += 1
                    logger.warning(f"Invalid Grobid JSON found: {grobid_json.name}")
                    
                    # Delete invalid JSON
                    try:
                        grobid_json.unlink()
                        stats['jsons_deleted'] += 1
                        logger.info(f"Deleted invalid Grobid JSON: {grobid_json.name}")
                    except Exception as e:
                        logger.error(f"Failed to delete Grobid JSON {grobid_json}: {e}")
                    
                    # Reset Grobid parser status in tracker
                    if tracker:
                        try:
                            tracker.mark_grobid_processed(clean_doi, success=False)
                            logger.debug(f"Reset Grobid status for {clean_doi}")
                        except Exception as e:
                            logger.error(f"Failed to reset Grobid status for {clean_doi}: {e}")
            
            # Check PyMuPDF JSON
            pymupdf_json = output_path / f"{safe_name}_fast.json"
            if pymupdf_json.exists():
                if not is_valid_json(pymupdf_json, 'pymupdf'):
                    stats['invalid_pymupdf_jsons'] += 1
                    logger.warning(f"Invalid PyMuPDF JSON found: {pymupdf_json.name}")
                    
                    # Delete invalid JSON
                    try:
                        pymupdf_json.unlink()
                        stats['jsons_deleted'] += 1
                        logger.info(f"Deleted invalid PyMuPDF JSON: {pymupdf_json.name}")
                    except Exception as e:
                        logger.error(f"Failed to delete PyMuPDF JSON {pymupdf_json}: {e}")
                    
                    # Reset PyMuPDF parser status in tracker
                    if tracker:
                        try:
                            tracker.mark_pymupdf_processed(clean_doi, success=False)
                            logger.debug(f"Reset PyMuPDF status for {clean_doi}")
                        except Exception as e:
                            logger.error(f"Failed to reset PyMuPDF status for {clean_doi}: {e}")
    
    # Flush tracker to persist changes
    if tracker:
        logger.info("Flushing tracker to disk...")
        tracker.flush()
    
    # Update reset history file
    if successfully_reset:
        logger.info(f"Updating reset history with {len(successfully_reset)} DOIs...")
        with open(reset_history_file, 'a', encoding='utf-8') as f:
            for doi in successfully_reset:
                f.write(f"{doi}\n")
        logger.info(f"✓ Updated {reset_history_file}")
    
    # Print summary
    logger.info("\n" + "="*70)
    logger.info("RESET SUMMARY")
    logger.info("="*70)
    logger.info(f"Original DOI count: {original_count}")
    logger.info(f"Skipped (previously reset): {stats['skipped_previously_reset']}")
    logger.info(f"DOIs processed: {stats['total_dois']}")
    logger.info(f"Tracker entries reset: {stats['tracker_reset']}")
    logger.info(f"Invalid PDFs found: {stats['invalid_pdfs']}")
    logger.info(f"Invalid Grobid JSONs found: {stats['invalid_grobid_jsons']}")
    logger.info(f"Invalid PyMuPDF JSONs found: {stats['invalid_pymupdf_jsons']}")
    logger.info(f"PDFs deleted: {stats['pdfs_deleted']}")
    logger.info(f"JSONs deleted: {stats['jsons_deleted']}")
    logger.info(f"\nReset list saved to: {reset_list_file}")
    logger.info(f"Reset history updated: {reset_history_file}")
    logger.info("="*70 + "\n")
    
    return stats


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='OPTIMIZED paper downloader with per-worker rate limiting',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Optimizations:
  - Token bucket rate limiting (allows true parallel downloads)
  - Tracker-based intelligent skipping:
    * Skip DOIs not in Sci-Hub (already failed)
    * Skip already successfully parsed papers
    * Retry failed parsings (up to 3 attempts)
    * Skip after 3 failed attempts
  - Pre-scan to partition work instantly
  - Buffered logging (reduced I/O overhead)
  - Real-time tracker updates

Examples:
  # Auto-generate DOI list and run with fast parser
  python download_papers_optimized.py --parser fast -w 8 --delay 2.0
  
  # Use specific DOI file with GROBID parser
  python download_papers_optimized.py -f dois.txt --parser grobid -w 4 --delay 2.0
  
  # Auto-generate with GROBID (creates timestamped file)
  python download_papers_optimized.py --parser grobid -w 4 --delay 2.0
  
  # Reset mode: validate and clean files for specific DOIs
  python download_papers_optimized.py -f problematic_dois.txt --reset-for-list
        """
    )
    
    parser.add_argument('identifiers', nargs='*', help='DOIs to download and process')
    parser.add_argument('-f', '--file', default=None, help='File containing identifiers (one per line). If not provided, automatically runs src/helper_scripts/create_missing_eval.py to generate a timestamped file.')
    parser.add_argument('--parser', choices=['fast', 'grobid'], default='fast')
    parser.add_argument('-m', '--mode', choices=['simple', 'structured', 'full'], default='structured')
    parser.add_argument('-o', '--output', help='Output directory')
    parser.add_argument('-c', '--config', help='Config file')
    parser.add_argument('--log-dir', help='Log directory')
    parser.add_argument('-w', '--workers', type=int, default=5,
                       help='Number of parallel workers (default: 5)')
    parser.add_argument('--delay', type=float, default=2.0,
                       help='Delay between requests (default: 2.0s)')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--reset-for-list', action='store_true',
                       help='Reset mode: validate and clean files for DOIs in the list. '
                            'Resets tracker, deletes invalid PDFs/JSONs, and resets parser status.')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Auto-generate DOI file if not provided
    if not args.file and not args.identifiers:
        logger.info("No input file specified. Auto-generating DOI list from src/helper_scripts/create_missing_eval.py...")
        
        # Create timestamped output filename
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        auto_file = f'missing_dois/dois_to_process_{timestamp}.txt'
        
        # Run the missing DOI creation script
        create_script = 'src/helper_scripts/create_missing_eval.py'
        if not os.path.exists(create_script):
            logger.error(f"Missing DOI creation script not found: {create_script}")
            return 1
        
        try:
            # Import and run the script logic
            import subprocess
            
            # Modify the script to use timestamped output
            logger.info(f"Running {create_script}...")
            
            # Read the original script
            with open(create_script, 'r') as f:
                script_content = f.read()
            
            # Replace the output filename with timestamped version
            modified_script = script_content.replace(
                "out = 'missing_dois/dois_to_process.txt'",
                f"out = '{auto_file}'"
            )
            
            # Execute the modified script
            exec(modified_script, {'__name__': '__main__'})
            
            if not os.path.exists(auto_file):
                logger.error(f"Failed to create {auto_file}")
                return 1
            
            logger.info(f"✓ Created {auto_file}")
            args.file = auto_file
            
        except Exception as e:
            logger.error(f"Failed to auto-generate DOI file: {e}")
            import traceback
            traceback.print_exc()
            return 1
    
    # Setup log directory
    if args.log_dir:
        logs_dir = args.log_dir
    elif args.file:
        input_file_dir = os.path.dirname(os.path.abspath(args.file))
        logs_dir = os.path.join(input_file_dir, 'logs')
    else:
        logs_dir = './logs'
    
    os.makedirs(logs_dir, exist_ok=True)
    
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(logs_dir, f'optimized_log_{timestamp}.log')
    
    # Initialize downloader
    if args.parser == 'grobid':
        downloader = SciHubGrobidDownloader(
            output_dir=args.output,
            config_path=args.config
        )
    else:
        downloader = SciHubFastDownloader(
            output_dir=args.output,
            parse_mode=args.mode
        )
    
    # Collect identifiers
    identifiers = []
    if args.identifiers:
        identifiers.extend(args.identifiers)
    
    if args.file:
        with open(args.file, 'r') as f:
            identifiers.extend([line.strip() for line in f if line.strip()])
    
    if not identifiers:
        print("Error: No identifiers provided")
        parser.print_help()
        return 1
    
    # Initialize DOI tracker
    logger.info("Initializing DB-backed DOI tracker...")
    tracker = DOITracker('processing_tracker.db')
    logger.info(f"Tracker ready (DB: processing_tracker.db)\n")
    
    # Handle reset mode
    if args.reset_for_list:
        logger.info("\n*** RESET MODE ACTIVATED ***\n")
        reset_stats = reset_dois_for_list(
            dois=identifiers,
            papers_dir='./papers',
            output_dir=args.output if args.output else './output',
            tracker=tracker,
            input_file=args.file
        )
        logger.info("Reset complete. Exiting.")
        return 0
    
    # Process with optimizations
    start_time = time.time()
    results = process_optimized(
        downloader=downloader,
        identifiers=identifiers,
        num_workers=args.workers,
        delay=args.delay,
        log_file=log_file,
        parser_type=args.parser,
        parse_mode=args.mode,
        tracker=tracker
    )
    elapsed = time.time() - start_time
    
    # Summary
    success = sum(1 for r in results if r.get('status') == 'success')
    skipped = sum(1 for r in results if r.get('status') == 'skipped_complete')
    not_found = sum(1 for r in results if r.get('status') == 'not_found')
    failed = sum(1 for r in results if r.get('status') == 'processing_failed')
    
    print(f"\n{'='*50}")
    print(f"Completed in {elapsed/60:.1f} minutes")
    print(f"Total: {len(results)} | Success: {success} | Skipped: {skipped}")
    print(f"Not found: {not_found} | Failed: {failed}")
    print(f"Log: {log_file}")
    print(f"{'='*50}")
    
    return 0


if __name__ == "__main__":
    # /home/diana.z/hack/scihub_api/missing_dois/test.txt
    # python download_papers_optimized.py -f /home/diana.z/hack/scihub_api/missing_dois/test.txt --parser grobid -w 1 --delay 2.0
    sys.exit(main())
