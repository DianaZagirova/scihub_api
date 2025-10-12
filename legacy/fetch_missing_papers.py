#!/usr/bin/env python3
"""
Fetch Missing Papers Script
---------------------------
Analyzes the papers database, identifies papers missing full_text or abstract,
downloads them from Sci-Hub, processes with GROBID in parallel, and updates the database.
"""

import os
import sys
import json
import time
import sqlite3
import logging
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional

# Add legacy directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'legacy'))

from scihub_downloader import SciHubDownloader
from grobid_parser import GrobidParser

# Configure logging with force and stream handler
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class MissingPapersFetcher:
    """Class to fetch and process papers missing full_text or abstract."""
    
    def __init__(self, db_path: str, config_path: str = None, output_dir: str = None):
        """
        Initialize the fetcher.
        
        Args:
            db_path: Path to the papers database
            config_path: Path to configuration file
            output_dir: Directory to store downloaded PDFs
        """
        self.db_path = db_path
        self.config_path = config_path or os.path.join(os.path.dirname(__file__), 'config.json')
        self.output_dir = output_dir or os.path.join(os.path.dirname(__file__), 'papers')
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Load configuration
        self.config = self._load_config()
        
        # Initialize Sci-Hub and GROBID
        self.scihub = SciHubDownloader(
            output_dir=self.output_dir,
            skip_existing=True,
            log_failed=True
        )
        self.grobid = GrobidParser(config_path=self.config_path)
        
        # Statistics
        self.stats = {
            'total_missing': 0,
            'downloaded': 0,
            'processed': 0,
            'updated': 0,
            'failed_download': 0,
            'failed_processing': 0,
            'failed_update': 0
        }
    
    def _load_config(self) -> dict:
        """Load configuration from file."""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"Loaded configuration from {self.config_path}")
            return config
        except Exception as e:
            logger.warning(f"Error loading configuration: {e}. Using defaults.")
            return {
                'grobid_server': 'http://10.223.131.158:8072',
                'max_workers': 4,
                'timeout': 180
            }
    
    def analyze_database(self) -> List[Dict]:
        """
        Analyze database to find papers missing full_text or abstract.
        
        Returns:
            List of paper records with missing data
        """
        logger.info("Analyzing database for missing papers...")
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Query for papers missing full_text OR abstract
            query = """
                SELECT pmid, pmcid, doi, title, abstract, full_text
                FROM papers
                WHERE (full_text IS NULL OR full_text = '')
                   OR (abstract IS NULL OR abstract = '')
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Convert to list of dicts
            papers = []
            for row in rows:
                paper = dict(row)
                paper['missing_full_text'] = not paper.get('full_text')
                paper['missing_abstract'] = not paper.get('abstract')
                papers.append(paper)
            
            conn.close()
            
            self.stats['total_missing'] = len(papers)
            logger.info(f"Found {len(papers)} papers with missing data")
            logger.info(f"  - Missing full_text: {sum(1 for p in papers if p['missing_full_text'])}")
            logger.info(f"  - Missing abstract: {sum(1 for p in papers if p['missing_abstract'])}")
            logger.info(f"  - Missing both: {sum(1 for p in papers if p['missing_full_text'] and p['missing_abstract'])}")
            
            return papers
            
        except Exception as e:
            logger.error(f"Error analyzing database: {e}")
            return []
    
    def get_identifier(self, paper: Dict) -> Optional[str]:
        """
        Get the best identifier for downloading the paper.
        Priority: DOI → Title (PMID is NOT used as scihub_downloader handles conversion internally)
        
        Args:
            paper: Paper record
            
        Returns:
            Identifier string (DOI or Title) or None
        """
        # Priority 1: DOI (most reliable for Sci-Hub)
        if paper.get('doi'):
            return paper['doi']
        
        # Priority 2: Title (scihub_downloader will convert to DOI via CrossRef)
        elif paper.get('title'):
            return paper['title']
        
        # No usable identifier
        else:
            logger.warning(f"Paper has no DOI or title: PMID={paper.get('pmid')}")
            return None
    
    def download_paper(self, paper: Dict) -> Optional[str]:
        """
        Download a paper from Sci-Hub.
        
        Args:
            paper: Paper record
            
        Returns:
            Path to downloaded PDF or None if failed
        """
        identifier = self.get_identifier(paper)
        if not identifier:
            logger.warning(f"No identifier found for paper: {paper.get('title', 'Unknown')}")
            return None
        
        try:
            logger.info(f"Downloading paper: {identifier}")
            pdf_path = self.scihub.download_paper(identifier)
            
            if not pdf_path:
                logger.error(f"Failed to download {identifier}")
                self.stats['failed_download'] += 1
                return None
            
            logger.info(f"Downloaded to: {pdf_path}")
            self.stats['downloaded'] += 1
            
            return pdf_path
            
        except Exception as e:
            logger.error(f"Error downloading paper {identifier}: {e}")
            self.stats['failed_download'] += 1
            return None
    
    def process_with_grobid(self, pdf_path: str) -> Optional[Dict]:
        """
        Process a PDF with GROBID to extract text and metadata.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Dictionary with extracted data or None if failed
        """
        try:
            logger.info(f"Processing with GROBID: {pdf_path}")
            
            # Process PDF with GROBID
            tei_content = self.grobid.process_pdf(pdf_path, output_format='tei')
            if not tei_content:
                logger.error(f"Failed to process PDF with GROBID: {pdf_path}")
                self.stats['failed_processing'] += 1
                return None
            
            # Extract metadata and full text
            metadata = self.grobid.extract_metadata(tei_content)
            full_text_data = self.grobid.extract_full_text(tei_content)
            
            # Combine abstract and full text
            abstract = metadata.get('abstract', '')
            
            # Combine all body sections into full text
            full_text_sections = []
            for section in full_text_data.get('body', []):
                section_text = f"## {section.get('title', 'Unnamed Section')}\n\n{section.get('content', '')}"
                full_text_sections.append(section_text)
            
            full_text = '\n\n'.join(full_text_sections)
            
            # Serialize full_text_sections for storage
            full_text_sections_json = json.dumps(full_text_data.get('body', []))
            
            self.stats['processed'] += 1
            
            return {
                'abstract': abstract,
                'full_text': full_text,
                'full_text_sections': full_text_sections_json,
                'metadata': metadata
            }
            
        except Exception as e:
            logger.error(f"Error processing PDF with GROBID: {e}")
            self.stats['failed_processing'] += 1
            return None
    
    def update_database(self, paper: Dict, extracted_data: Dict) -> bool:
        """
        Update the database with extracted data.
        
        Args:
            paper: Original paper record
            extracted_data: Extracted data from GROBID
            
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Prepare update fields
            updates = []
            values = []
            
            # Only update fields that were missing
            if paper.get('missing_abstract') and extracted_data.get('abstract'):
                updates.append("abstract = ?")
                values.append(extracted_data['abstract'])
            
            if paper.get('missing_full_text') and extracted_data.get('full_text'):
                updates.append("full_text = ?")
                values.append(extracted_data['full_text'])
                
                if extracted_data.get('full_text_sections'):
                    updates.append("full_text_sections = ?")
                    values.append(extracted_data['full_text_sections'])
            
            # If no updates needed, skip
            if not updates:
                logger.warning(f"No updates needed for paper: {paper.get('pmid', 'Unknown')}")
                return False
            
            # Build and execute update query
            update_query = f"UPDATE papers SET {', '.join(updates)} WHERE pmid = ?"
            values.append(paper['pmid'])
            
            cursor.execute(update_query, values)
            conn.commit()
            conn.close()
            
            self.stats['updated'] += 1
            logger.info(f"Updated database for paper: {paper.get('pmid', 'Unknown')}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating database: {e}")
            self.stats['failed_update'] += 1
            return False
    
    def process_paper(self, paper: Dict) -> Dict:
        """
        Process a single paper: download, extract, and update.
        
        Args:
            paper: Paper record
            
        Returns:
            Result dictionary with status
        """
        identifier = self.get_identifier(paper)
        result = {
            'pmid': paper.get('pmid'),
            'identifier': identifier,
            'status': 'pending',
            'error': None
        }
        
        try:
            # Step 1: Download from Sci-Hub
            pdf_path = self.download_paper(paper)
            if not pdf_path:
                result['status'] = 'failed_download'
                result['error'] = 'Failed to download from Sci-Hub'
                return result
            
            # Step 2: Process with GROBID
            extracted_data = self.process_with_grobid(pdf_path)
            if not extracted_data:
                result['status'] = 'failed_processing'
                result['error'] = 'Failed to process with GROBID'
                return result
            
            # Step 3: Update database
            update_success = self.update_database(paper, extracted_data)
            if not update_success:
                result['status'] = 'failed_update'
                result['error'] = 'Failed to update database'
                return result
            
            result['status'] = 'success'
            return result
            
        except Exception as e:
            logger.error(f"Error processing paper {identifier}: {e}")
            result['status'] = 'error'
            result['error'] = str(e)
            return result
    
    def process_papers_parallel(self, papers: List[Dict], max_workers: int = None) -> List[Dict]:
        """
        Process multiple papers in parallel.
        
        This mimics download_papers.py functionality but adds parallel processing
        using ThreadPoolExecutor for faster batch processing.
        
        Args:
            papers: List of paper records
            max_workers: Maximum number of parallel workers
            
        Returns:
            List of result dictionaries
        """
        if max_workers is None:
            max_workers = self.config.get('max_workers', 4)
        
        logger.info(f"Processing {len(papers)} papers with {max_workers} parallel workers")
        
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_paper = {
                executor.submit(self.process_paper, paper): paper 
                for paper in papers
            }
            
            # Process as they complete
            for i, future in enumerate(as_completed(future_to_paper), 1):
                paper = future_to_paper[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Log progress
                    logger.info(f"Progress: {i}/{len(papers)} - Status: {result['status']}")
                    
                except Exception as e:
                    logger.error(f"Error processing paper: {e}")
                    results.append({
                        'pmid': paper.get('pmid'),
                        'identifier': self.get_identifier(paper),
                        'status': 'error',
                        'error': str(e)
                    })
                
                # Small delay to avoid overwhelming servers
                time.sleep(0.5)
        
        return results
    
    def save_results(self, results: List[Dict], output_file: str = None):
        """
        Save processing results to a JSON file.
        
        Args:
            results: List of result dictionaries
            output_file: Path to output file
        """
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(self.output_dir, f'processing_results_{timestamp}.json')
        
        try:
            with open(output_file, 'w') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'statistics': self.stats,
                    'results': results
                }, f, indent=2)
            
            logger.info(f"Saved results to: {output_file}")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")
    
    def print_summary(self):
        """Print a summary of the processing results."""
        logger.info("\n" + "="*60)
        logger.info("PROCESSING SUMMARY")
        logger.info("="*60)
        logger.info(f"Total papers with missing data: {self.stats['total_missing']}")
        logger.info(f"Successfully downloaded: {self.stats['downloaded']}")
        logger.info(f"Successfully processed: {self.stats['processed']}")
        logger.info(f"Successfully updated: {self.stats['updated']}")
        logger.info(f"Failed downloads: {self.stats['failed_download']}")
        logger.info(f"Failed processing: {self.stats['failed_processing']}")
        logger.info(f"Failed updates: {self.stats['failed_update']}")
        logger.info("="*60)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Fetch and process papers missing full_text or abstract'
    )
    parser.add_argument(
        '--db',
        default='/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db',
        help='Path to papers database'
    )
    parser.add_argument(
        '--config',
        default=None,
        help='Path to configuration file'
    )
    parser.add_argument(
        '--output',
        default=None,
        help='Directory to store downloaded PDFs'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of papers to process (for testing)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Only analyze database without processing'
    )
    
    args = parser.parse_args()
    
    # Initialize fetcher
    fetcher = MissingPapersFetcher(
        db_path=args.db,
        config_path=args.config,
        output_dir=args.output
    )
    
    # Analyze database
    papers = fetcher.analyze_database()
    
    if not papers:
        logger.info("No papers with missing data found.")
        return 0
    
    # Apply limit if specified
    if args.limit:
        papers = papers[:args.limit]
        logger.info(f"Limited to {args.limit} papers for processing")
    
    # Dry run - just show analysis
    if args.dry_run:
        logger.info("Dry run mode - no processing will be done")
        return 0
    
    # Process papers
    results = fetcher.process_papers_parallel(papers, max_workers=args.workers)
    
    # Save results
    fetcher.save_results(results)
    
    # Print summary
    fetcher.print_summary()
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
