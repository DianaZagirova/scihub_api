#!/usr/bin/env python3
"""
GROBID Parser Module
-------------------
A module to extract text and metadata from PDF papers using GROBID.
"""

import os
import json
import time
import logging
import requests
import json5
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class GrobidParser:
    """Class to handle parsing PDF papers using GROBID."""
    
    def __init__(self, config_path=None, offline_mode=False):
        """
        Initialize the GROBID parser.
        
        Args:
            config_path (str): Path to the configuration file
            offline_mode (bool): If True, don't try to connect to GROBID server
        """
        self.config = self._load_config(config_path)
        self.grobid_server = self.config.get('grobid_server', 'http://localhost:8070')
        self.batch_size = self.config.get('batch_size', 1000)
        self.timeout = self.config.get('timeout', 180)
        self.sleep_time = self.config.get('sleep_time', 5)
        self.coordinates = self.config.get('coordinates', [])
        self.max_workers = self.config.get('max_workers', 4)
        self.consolidate_header = self.config.get('consolidate_header', 1)
        self.consolidate_citations = self.config.get('consolidate_citations', 0)
        self.offline_mode = offline_mode
        
        # Create output directory if it doesn't exist
        self.output_dir = os.path.join(os.getcwd(), 'output')
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
            
        # Check if GROBID server is running if not in offline mode
        if not offline_mode:
            server_running = self._check_grobid_server()
            if not server_running:
                logger.warning("GROBID server is not running. Some functionality will be limited.")
                logger.warning("To run GROBID server, follow instructions at https://grobid.readthedocs.io/")
                logger.warning("You can still use basic PDF metadata extraction in offline mode.")
    
    def _load_config(self, config_path=None):
        """
        Load configuration from file.
        
        Args:
            config_path (str): Path to the configuration file
            
        Returns:
            dict: Configuration dictionary
        """
        if not config_path:
            config_path = os.path.join(os.getcwd(), 'config.json')
        
        try:
            with open(config_path, 'r') as f:
                config = json5.load(f)
            logger.info(f"Loaded configuration from {config_path}")
            return config
        except Exception as e:
            logger.warning(f"Error loading configuration: {e}")
            logger.info("Using default configuration")
            # Return default configuration
            return {
                'grobid_server': 'http://localhost:8070',
                'batch_size': 1000,
                'timeout': 180,
                'sleep_time': 5,
                'coordinates': [],
                'max_workers': 4,
                'consolidate_header': 1,
                'consolidate_citations': 0
            }
    
    def _check_grobid_server(self):
        """
        Check if GROBID server is running.
        
        Returns:
            bool: True if server is running, False otherwise
        """
        try:
            response = requests.get(f"{self.grobid_server}/api/isalive", timeout=10)
            if response.status_code == 200:
                logger.info("GROBID server is running")
                return True
            else:
                logger.error(f"GROBID server returned status code {response.status_code}")
                return False
        except requests.RequestException as e:
            logger.error(f"Error connecting to GROBID server: {e}")
            return False
    
    def process_pdf(self, pdf_path, output_format='tei'):
        """
        Process a single PDF file with GROBID.
        
        Args:
            pdf_path (str): Path to the PDF file
            output_format (str): Output format ('tei', 'json', etc.)
            
        Returns:
            dict: Extracted data or None if failed
        """
        if self.offline_mode:
            logger.warning("Running in offline mode. Cannot process PDF with GROBID.")
            logger.error("GROBID processing failed - server not available")
            return None
            
        if not self._check_grobid_server():
            logger.error("GROBID server is not running. Cannot process PDF.")
            return None
        
        if not os.path.exists(pdf_path):
            logger.error(f"PDF file not found: {pdf_path}")
            return None
        
        try:
            # Determine which GROBID service to use based on output format
            if output_format == 'tei':
                service = 'processFulltextDocument'
            else:
                service = 'processHeaderDocument'
            
            # Prepare the request
            url = f"{self.grobid_server}/api/{service}"
            
            # Prepare coordinates parameter if needed
            coord_param = ""
            if self.coordinates and output_format == 'tei':
                coord_param = ",".join(self.coordinates)
            
            # Prepare the files and data for the request
            files = {'input': open(pdf_path, 'rb')}
            data = {
                'consolidateHeader': str(self.consolidate_header),
                'consolidateCitations': str(self.consolidate_citations)
            }
            
            if coord_param:
                data['teiCoordinates'] = coord_param
            
            # Send the request
            logger.info(f"Processing {pdf_path} with GROBID")
            response = requests.post(url, files=files, data=data, timeout=self.timeout)
            
            # Close the file
            files['input'].close()
            
            if response.status_code != 200:
                logger.error(f"GROBID returned status code {response.status_code}")
                return None
            
            # Return the response content
            if output_format == 'tei':
                return response.text
            else:
                return response.json()
                
        except Exception as e:
            logger.error(f"Error processing PDF with GROBID: {e}")
            return None
    
    def extract_metadata(self, tei_content):
        """
        Extract metadata from TEI content.
        
        Args:
            tei_content (str): TEI XML content
            
        Returns:
            dict: Extracted metadata
        """
        try:
            from bs4 import BeautifulSoup
            
            soup = BeautifulSoup(tei_content, 'xml')
            
            # Initialize metadata dictionary
            metadata = {
                'doi': None,
                'title': None,
                'abstract': None,
                'authors': [],
                'journal': None,
                'volume': None,
                'issue': None,
                'year': None,
                'pages': None,
                'keywords': []
            }
            
            # Extract DOI
            doi_tag = soup.find('idno', {'type': 'DOI'})
            if doi_tag:
                metadata['doi'] = doi_tag.text.strip()
            
            # Extract title
            title_tag = soup.find('titleStmt').find('title') if soup.find('titleStmt') else None
            if title_tag:
                metadata['title'] = title_tag.text.strip()
            
            # Extract abstract
            abstract_tag = soup.find('abstract')
            if abstract_tag:
                metadata['abstract'] = ' '.join([p.text.strip() for p in abstract_tag.find_all('p')])
            
            # Extract authors
            author_tags = soup.find_all('author')
            for author_tag in author_tags:
                author = {}
                
                # Get author name
                persname = author_tag.find('persName')
                if persname:
                    forename = persname.find('forename')
                    surname = persname.find('surname')
                    
                    if forename and surname:
                        author['name'] = f"{forename.text.strip()} {surname.text.strip()}"
                    elif surname:
                        author['name'] = surname.text.strip()
                
                # Get author affiliation
                affiliation = author_tag.find('affiliation')
                if affiliation:
                    author['affiliation'] = affiliation.text.strip()
                
                # Get author email
                email = author_tag.find('email')
                if email:
                    author['email'] = email.text.strip()
                
                if author:
                    metadata['authors'].append(author)
            
            # Extract journal information
            journal_tag = soup.find('monogr')
            if journal_tag:
                # Journal title
                journal_title = journal_tag.find('title')
                if journal_title:
                    metadata['journal'] = journal_title.text.strip()
                
                # Volume
                volume = journal_tag.find('biblScope', {'unit': 'volume'})
                if volume:
                    metadata['volume'] = volume.text.strip()
                
                # Issue
                issue = journal_tag.find('biblScope', {'unit': 'issue'})
                if issue:
                    metadata['issue'] = issue.text.strip()
                
                # Pages
                pages = journal_tag.find('biblScope', {'unit': 'page'})
                if pages:
                    metadata['pages'] = pages.text.strip()
                
                # Year
                date = journal_tag.find('date')
                if date and date.get('when'):
                    metadata['year'] = date.get('when')[:4]  # Extract year from date
            
            # Extract keywords
            keyword_tags = soup.find_all('term')
            for keyword_tag in keyword_tags:
                if keyword_tag.text.strip():
                    metadata['keywords'].append(keyword_tag.text.strip())
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error extracting metadata from TEI: {e}")
            return {}
    
    def extract_full_text(self, tei_content):
        """
        Extract full text from TEI content.
        
        Args:
            tei_content (str): TEI XML content
            
        Returns:
            dict: Extracted full text by sections
        """
        try:
            from bs4 import BeautifulSoup
            
            soup = BeautifulSoup(tei_content, 'xml')
            
            # Initialize full text dictionary
            full_text = {
                'body': [],
                'references': []
            }
            
            # Extract body text by sections
            body = soup.find('body')
            if body:
                for div in body.find_all('div'):
                    section = {}
                    
                    # Get section title
                    head = div.find('head')
                    if head:
                        section['title'] = head.text.strip()
                    else:
                        section['title'] = 'Unnamed Section'
                    
                    # Get section content
                    paragraphs = []
                    for p in div.find_all('p'):
                        paragraphs.append(p.text.strip())
                    
                    section['content'] = '\n\n'.join(paragraphs)
                    
                    if section['content']:
                        full_text['body'].append(section)
            
            # Extract references
            back = soup.find('back')
            if back:
                ref_list = back.find('listBibl')
                if ref_list:
                    for ref in ref_list.find_all('biblStruct'):
                        reference = {}
                        
                        # Try to get structured reference info
                        title = ref.find('title')
                        if title:
                            reference['title'] = title.text.strip()
                        
                        # Get authors
                        authors = []
                        for author in ref.find_all('author'):
                            persname = author.find('persName')
                            if persname:
                                forename = persname.find('forename')
                                surname = persname.find('surname')
                                
                                if forename and surname:
                                    authors.append(f"{forename.text.strip()} {surname.text.strip()}")
                                elif surname:
                                    authors.append(surname.text.strip())
                        
                        if authors:
                            reference['authors'] = authors
                        
                        # Get year
                        date = ref.find('date')
                        if date and date.get('when'):
                            reference['year'] = date.get('when')[:4]
                        
                        # Get raw reference text as fallback
                        if not reference:
                            reference['raw'] = ref.text.strip()
                        
                        full_text['references'].append(reference)
            
            return full_text
            
        except Exception as e:
            logger.error(f"Error extracting full text from TEI: {e}")
            return {'body': [], 'references': []}
    
    def process_and_save(self, pdf_path, output_dir=None):
        """
        Process a PDF and save the extracted data.
        
        Args:
            pdf_path (str): Path to the PDF file
            output_dir (str): Directory to save the output
            
        Returns:
            dict: Extracted data or None if failed
        """
        if not output_dir:
            output_dir = self.output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Get the base filename without extension
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        
        # Process the PDF
        tei_content = self.process_pdf(pdf_path, output_format='tei')
        if not tei_content:
            logger.error(f"Failed to process PDF: {pdf_path}")
            return None
        
        # Save the raw TEI content
        tei_path = os.path.join(output_dir, f"{base_name}.tei.xml")
        try:
            with open(tei_path, 'w', encoding='utf-8') as f:
                f.write(tei_content)
            logger.info(f"Saved TEI content to {tei_path}")
        except Exception as e:
            logger.error(f"Error saving TEI content: {e}")
        
        # Extract metadata
        metadata = self.extract_metadata(tei_content)
        
        # Extract full text
        full_text = self.extract_full_text(tei_content)
        
        # Combine metadata and full text
        extracted_data = {
            'metadata': metadata,
            'full_text': full_text
        }
        
        # Save the extracted data as JSON
        json_path = os.path.join(output_dir, f"{base_name}.json")
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved extracted data to {json_path}")
        except Exception as e:
            logger.error(f"Error saving extracted data: {e}")
        
        return extracted_data
    
    def batch_process(self, pdf_dir, output_dir=None, max_workers=None):
        """
        Process multiple PDFs in a directory.
        
        Args:
            pdf_dir (str): Directory containing PDF files
            output_dir (str): Directory to save the output
            max_workers (int): Maximum number of worker threads (default: from config)
            
        Returns:
            list: List of processed files with their status
        """
        if not output_dir:
            output_dir = self.output_dir
        
        if max_workers is None:
            max_workers = self.max_workers
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Get all PDF files in the directory
        pdf_files = [os.path.join(pdf_dir, f) for f in os.listdir(pdf_dir) 
                    if f.lower().endswith('.pdf') and os.path.isfile(os.path.join(pdf_dir, f))]
        
        if not pdf_files:
            logger.warning(f"No PDF files found in {pdf_dir}")
            return []
        
        logger.info(f"Found {len(pdf_files)} PDF files to process with {max_workers} parallel workers")
        
        # Process files in parallel
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_pdf = {executor.submit(self.process_and_save, pdf, output_dir): pdf for pdf in pdf_files}
            
            # Process as they complete
            for future in tqdm(as_completed(future_to_pdf), total=len(pdf_files), desc="Processing PDFs"):
                pdf = future_to_pdf[future]
                try:
                    data = future.result()
                    status = "success" if data else "failed"
                    results.append({
                        'file': pdf,
                        'status': status,
                        'doi': data['metadata']['doi'] if data and data.get('metadata', {}).get('doi') else None
                    })
                except Exception as e:
                    logger.error(f"Error processing {pdf}: {e}")
                    results.append({
                        'file': pdf,
                        'status': 'error',
                        'error': str(e)
                    })
                
                # Sleep to avoid overloading the server (only if sleep_time > 0)
                if self.sleep_time > 0:
                    time.sleep(self.sleep_time)
        
        # Generate a summary report
        success_count = sum(1 for r in results if r['status'] == 'success')
        failed_count = len(results) - success_count
        
        logger.info(f"Processing complete: {success_count} succeeded, {failed_count} failed")
        
        # Save results to CSV
        try:
            df = pd.DataFrame(results)
            csv_path = os.path.join(output_dir, f"processing_results_{time.strftime('%Y%m%d_%H%M%S')}.csv")
            df.to_csv(csv_path, index=False)
            logger.info(f"Saved processing results to {csv_path}")
        except Exception as e:
            logger.error(f"Error saving processing results: {e}")
        
        return results

# Main function for standalone usage
    def _extract_metadata_from_filename(self, pdf_path):
        """
        Extract basic metadata from PDF filename when GROBID is not available.
        
        Args:
            pdf_path (str): Path to the PDF file
            
        Returns:
            str: Simple XML with basic metadata
        """
        try:
            # Get the filename without extension
            filename = os.path.basename(pdf_path)
            base_name = os.path.splitext(filename)[0]
            
            # Try to extract DOI from filename (assuming format like 10.1038_s41586-019-1750-x.pdf)
            doi = base_name.replace('_', '/')
            
            # Create a simple XML with basic metadata
            xml = f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"""
            xml += f"""<TEI xmlns=\"http://www.tei-c.org/ns/1.0\">\n"""
            xml += f"""  <teiHeader>\n"""
            xml += f"""    <fileDesc>\n"""
            xml += f"""      <titleStmt>\n"""
            xml += f"""        <title>Metadata extracted from filename</title>\n"""
            xml += f"""      </titleStmt>\n"""
            xml += f"""      <sourceDesc>\n"""
            xml += f"""        <biblStruct>\n"""
            xml += f"""          <analytic>\n"""
            xml += f"""            <idno type=\"DOI\">{doi}</idno>\n"""
            xml += f"""          </analytic>\n"""
            xml += f"""        </biblStruct>\n"""
            xml += f"""      </sourceDesc>\n"""
            xml += f"""    </fileDesc>\n"""
            xml += f"""  </teiHeader>\n"""
            xml += f"""  <text>\n"""
            xml += f"""    <body>\n"""
            xml += f"""      <div>\n"""
            xml += f"""        <p>GROBID processing not available. Only basic metadata extracted from filename.</p>\n"""
            xml += f"""      </div>\n"""
            xml += f"""    </body>\n"""
            xml += f"""  </text>\n"""
            xml += f"""</TEI>\n"""
            
            return xml
            
        except Exception as e:
            logger.error(f"Error extracting metadata from filename: {e}")
            return None

def main():
    """Main function to handle command line interface."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process PDF papers with GROBID')
    parser.add_argument('--pdf', help='Path to a single PDF file to process')
    parser.add_argument('--dir', help='Directory containing PDF files to process')
    parser.add_argument('--output', help='Output directory for extracted data')
    parser.add_argument('--config', help='Path to configuration file')
    parser.add_argument('--workers', type=int, default=4, help='Maximum number of worker threads')
    parser.add_argument('--offline', action='store_true', help='Run in offline mode without GROBID server')
    
    args = parser.parse_args()
    
    # Initialize the parser
    parser = GrobidParser(config_path=args.config, offline_mode=args.offline)
    
    # Process a single PDF
    if args.pdf:
        if not os.path.exists(args.pdf):
            logger.error(f"PDF file not found: {args.pdf}")
            return 1
        
        result = parser.process_and_save(args.pdf, output_dir=args.output)
        if result:
            logger.info(f"Successfully processed {args.pdf}")
            return 0
        else:
            logger.error(f"Failed to process {args.pdf}")
            return 1
    
    # Process a directory of PDFs
    elif args.dir:
        if not os.path.exists(args.dir):
            logger.error(f"Directory not found: {args.dir}")
            return 1
        
        results = parser.batch_process(args.dir, output_dir=args.output, max_workers=args.workers)
        success_count = sum(1 for r in results if r['status'] == 'success')
        
        if results:
            logger.info(f"Processed {len(results)} files: {success_count} succeeded, {len(results) - success_count} failed")
            return 0
        else:
            logger.error("No files were processed")
            return 1
    
    else:
        logger.error("No input specified. Use --pdf or --dir")
        parser.print_help()
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
