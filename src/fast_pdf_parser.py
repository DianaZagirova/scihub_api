#!/usr/bin/env python3
"""
Fast PDF Parser Module
----------------------
A high-performance PDF parser using PyMuPDF (fitz) that extracts text and structure
from PDF papers while being significantly faster than GROBID.
"""

import os
import re
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

try:
    import fitz  # PyMuPDF
except ImportError:
    raise ImportError("PyMuPDF is not installed. Install it with: pip install PyMuPDF")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class FastPDFParser:
    """Fast PDF parser using PyMuPDF with structure preservation."""
    
    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize the fast PDF parser.
        
        Args:
            output_dir: Directory to save extracted data
        """
        self.output_dir = output_dir or os.path.join(os.getcwd(), 'output')
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
    
    def extract_metadata(self, pdf_path: str) -> Dict:
        """
        Extract metadata from PDF.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Dictionary containing metadata
        """
        try:
            doc = fitz.open(pdf_path)
            metadata = doc.metadata
            
            # Extract DOI from filename if available
            filename = os.path.basename(pdf_path)
            base_name = os.path.splitext(filename)[0]
            doi = base_name.replace('_', '/')
            
            # Build metadata dictionary
            extracted_metadata = {
                'doi': doi if '/' in doi else None,
                'title': metadata.get('title', None),
                'author': metadata.get('author', None),
                'subject': metadata.get('subject', None),
                'keywords': metadata.get('keywords', '').split(',') if metadata.get('keywords') else [],
                'creator': metadata.get('creator', None),
                'producer': metadata.get('producer', None),
                'creation_date': metadata.get('creationDate', None),
                'modification_date': metadata.get('modDate', None),
                'page_count': doc.page_count,
                'file_size': os.path.getsize(pdf_path)
            }
            
            doc.close()
            return extracted_metadata
            
        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")
            return {}
    
    def _detect_heading(self, block: Dict, avg_font_size: float) -> bool:
        """
        Detect if a text block is likely a heading based on font size and formatting.
        
        Args:
            block: Text block dictionary
            avg_font_size: Average font size in the document
            
        Returns:
            True if the block is likely a heading
        """
        if 'lines' not in block:
            return False
        
        # Check if font size is larger than average
        for line in block['lines']:
            for span in line['spans']:
                font_size = span.get('size', 0)
                flags = span.get('flags', 0)
                
                # Check if bold (flag 16) or larger font
                is_bold = flags & 16
                is_larger = font_size > avg_font_size * 1.2
                
                if is_bold or is_larger:
                    return True
        
        return False
    
    def _calculate_avg_font_size(self, doc: fitz.Document) -> float:
        """
        Calculate average font size in the document.
        
        Args:
            doc: PyMuPDF document object
            
        Returns:
            Average font size
        """
        font_sizes = []
        
        # Sample first few pages to calculate average
        sample_pages = min(5, doc.page_count)
        
        for page_num in range(sample_pages):
            page = doc[page_num]
            blocks = page.get_text("dict")["blocks"]
            
            for block in blocks:
                if block.get("type") == 0:  # Text block
                    for line in block.get("lines", []):
                        for span in line.get("spans", []):
                            font_sizes.append(span.get("size", 0))
        
        return sum(font_sizes) / len(font_sizes) if font_sizes else 12.0
    
    def _extract_references(self, text: str) -> List[str]:
        """
        Extract references section from text.
        
        Args:
            text: Full text of the document
            
        Returns:
            List of reference strings
        """
        # Common reference section headers
        ref_patterns = [
            r'\n\s*REFERENCES\s*\n',
            r'\n\s*References\s*\n',
            r'\n\s*BIBLIOGRAPHY\s*\n',
            r'\n\s*Bibliography\s*\n',
            r'\n\s*LITERATURE CITED\s*\n'
        ]
        
        references = []
        ref_section = None
        
        # Find the references section
        for pattern in ref_patterns:
            match = re.search(pattern, text)
            if match:
                ref_section = text[match.end():]
                break
        
        if ref_section:
            # Split by common reference patterns (numbers or authors)
            # Pattern: [1], (1), 1., or Author et al.
            ref_lines = re.split(r'\n\s*(?:\[\d+\]|\(\d+\)|\d+\.)', ref_section)
            references = [ref.strip() for ref in ref_lines if ref.strip()]
        
        return references
    
    def extract_text_with_structure(self, pdf_path: str) -> Dict:
        """
        Extract text from PDF while preserving structure.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Dictionary containing structured text
        """
        try:
            doc = fitz.open(pdf_path)
            avg_font_size = self._calculate_avg_font_size(doc)
            
            sections = []
            current_section = None
            full_text = []
            
            for page_num in range(doc.page_count):
                page = doc[page_num]
                blocks = page.get_text("dict")["blocks"]
                
                for block in blocks:
                    if block.get("type") == 0:  # Text block
                        # Extract text from block
                        block_text = ""
                        for line in block.get("lines", []):
                            line_text = ""
                            for span in line.get("spans", []):
                                line_text += span.get("text", "")
                            block_text += line_text + " "
                        
                        block_text = block_text.strip()
                        
                        if not block_text:
                            continue
                        
                        full_text.append(block_text)
                        
                        # Check if this is a heading
                        is_heading = self._detect_heading(block, avg_font_size)
                        
                        if is_heading and len(block_text.split()) < 15:
                            # Start a new section
                            if current_section:
                                sections.append(current_section)
                            
                            current_section = {
                                'title': block_text,
                                'content': []
                            }
                        else:
                            # Add to current section or create default section
                            if current_section is None:
                                current_section = {
                                    'title': 'Introduction',
                                    'content': []
                                }
                            
                            current_section['content'].append(block_text)
            
            # Add the last section
            if current_section:
                sections.append(current_section)
            
            # Join full text
            full_text_str = '\n\n'.join(full_text)
            
            # Extract references
            references = self._extract_references(full_text_str)
            
            # Build structured output
            structured_text = {
                'sections': sections,
                'references': references,
                'full_text': full_text_str,
                'page_count': doc.page_count
            }
            
            doc.close()
            return structured_text
            
        except Exception as e:
            logger.error(f"Error extracting text with structure: {e}")
            return {}
    
    def extract_text_simple(self, pdf_path: str) -> str:
        """
        Extract plain text from PDF (fastest method).
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Plain text string
        """
        try:
            doc = fitz.open(pdf_path)
            text = ""
            
            for page in doc:
                text += page.get_text()
            
            doc.close()
            return text
            
        except Exception as e:
            logger.error(f"Error extracting plain text: {e}")
            return ""
    
    def process_pdf(self, pdf_path: str, mode: str = 'structured') -> Dict:
        """
        Process a PDF file and extract data.
        
        Args:
            pdf_path: Path to the PDF file
            mode: Extraction mode ('simple', 'structured', or 'full')
            
        Returns:
            Dictionary containing extracted data
        """
        if not os.path.exists(pdf_path):
            logger.error(f"PDF file not found: {pdf_path}")
            return None
        
        logger.info(f"Processing {pdf_path} in {mode} mode")
        
        result = {
            'file': pdf_path,
            'mode': mode,
            'timestamp': datetime.now().isoformat()
        }
        
        # Extract metadata
        result['metadata'] = self.extract_metadata(pdf_path)
        
        # Extract text based on mode
        if mode == 'simple':
            result['text'] = self.extract_text_simple(pdf_path)
        elif mode == 'structured':
            result['structured_text'] = self.extract_text_with_structure(pdf_path)
        elif mode == 'full':
            result['metadata'] = self.extract_metadata(pdf_path)
            result['structured_text'] = self.extract_text_with_structure(pdf_path)
        
        return result
    
    def process_and_save(self, pdf_path: str, mode: str = 'structured', output_dir: Optional[str] = None) -> Dict:
        """
        Process a PDF and save the extracted data.
        
        Args:
            pdf_path: Path to the PDF file
            mode: Extraction mode ('simple', 'structured', or 'full')
            output_dir: Directory to save output (uses default if None)
            
        Returns:
            Dictionary containing extracted data
        """
        if not output_dir:
            output_dir = self.output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Process the PDF
        result = self.process_pdf(pdf_path, mode)
        
        if not result:
            return None
        
        # Get the base filename
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        
        # Save as JSON
        json_path = os.path.join(output_dir, f"{base_name}_fast.json")
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved extracted data to {json_path}")
        except Exception as e:
            logger.error(f"Error saving extracted data: {e}")
        
        return result
    
    def batch_process(self, pdf_dir: str, mode: str = 'structured', output_dir: Optional[str] = None) -> List[Dict]:
        """
        Process multiple PDFs in a directory.
        
        Args:
            pdf_dir: Directory containing PDF files
            mode: Extraction mode
            output_dir: Directory to save output
            
        Returns:
            List of processing results
        """
        if not output_dir:
            output_dir = self.output_dir
        
        # Get all PDF files
        pdf_files = [os.path.join(pdf_dir, f) for f in os.listdir(pdf_dir) 
                    if f.lower().endswith('.pdf') and os.path.isfile(os.path.join(pdf_dir, f))]
        
        if not pdf_files:
            logger.warning(f"No PDF files found in {pdf_dir}")
            return []
        
        logger.info(f"Found {len(pdf_files)} PDF files to process")
        
        results = []
        for i, pdf_path in enumerate(pdf_files, 1):
            logger.info(f"Processing {i}/{len(pdf_files)}: {os.path.basename(pdf_path)}")
            
            try:
                result = self.process_and_save(pdf_path, mode, output_dir)
                results.append({
                    'file': pdf_path,
                    'status': 'success' if result else 'failed',
                    'result': result
                })
            except Exception as e:
                logger.error(f"Error processing {pdf_path}: {e}")
                results.append({
                    'file': pdf_path,
                    'status': 'error',
                    'error': str(e)
                })
        
        # Summary
        success_count = sum(1 for r in results if r['status'] == 'success')
        logger.info(f"Processing complete: {success_count}/{len(pdf_files)} succeeded")
        
        return results


def main():
    """Main function for command line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fast PDF parser using PyMuPDF')
    parser.add_argument('--pdf', help='Path to a single PDF file to process')
    parser.add_argument('--dir', help='Directory containing PDF files to process')
    parser.add_argument('--output', help='Output directory for extracted data')
    parser.add_argument('--mode', choices=['simple', 'structured', 'full'], 
                       default='structured', help='Extraction mode')
    
    args = parser.parse_args()
    
    # Initialize parser
    pdf_parser = FastPDFParser(output_dir=args.output)
    
    # Process single PDF
    if args.pdf:
        if not os.path.exists(args.pdf):
            logger.error(f"PDF file not found: {args.pdf}")
            return 1
        
        result = pdf_parser.process_and_save(args.pdf, mode=args.mode)
        if result:
            logger.info(f"Successfully processed {args.pdf}")
            return 0
        else:
            logger.error(f"Failed to process {args.pdf}")
            return 1
    
    # Process directory
    elif args.dir:
        if not os.path.exists(args.dir):
            logger.error(f"Directory not found: {args.dir}")
            return 1
        
        results = pdf_parser.batch_process(args.dir, mode=args.mode)
        success_count = sum(1 for r in results if r['status'] == 'success')
        
        if results:
            logger.info(f"Processed {len(results)} files: {success_count} succeeded")
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
