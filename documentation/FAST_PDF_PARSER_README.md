# Fast PDF Parser

A high-performance PDF parser using PyMuPDF (fitz) that extracts text and structure from PDF papers. This parser is significantly faster than GROBID while still preserving document structure.

## Features

- **Fast Processing**: 10-100x faster than GROBID depending on document size
- **Structure Preservation**: Detects headings, sections, and paragraphs
- **Metadata Extraction**: Extracts PDF metadata, DOI, title, authors, etc.
- **Reference Extraction**: Automatically identifies and extracts references
- **Multiple Modes**: Simple, structured, or full extraction modes
- **Batch Processing**: Process multiple PDFs efficiently
- **No Server Required**: Works locally without external dependencies

## Installation

Install the required dependency:

```bash
pip install PyMuPDF
```

Or install all project dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Standalone Fast PDF Parser

#### Process a single PDF:

```bash
# Structured mode (default) - preserves document structure
python fast_pdf_parser.py --pdf papers/10.1038_s41586-019-1750-x.pdf

# Simple mode - fastest, plain text only
python fast_pdf_parser.py --pdf papers/10.1038_s41586-019-1750-x.pdf --mode simple

# Full mode - everything (metadata + structured text)
python fast_pdf_parser.py --pdf papers/10.1038_s41586-019-1750-x.pdf --mode full
```

#### Process a directory of PDFs:

```bash
python fast_pdf_parser.py --dir papers/ --mode structured --output output/
```

### Integrated with Sci-Hub Downloader

#### Download and process papers with fast parser:

```bash
# Download and process a single paper
python scihub_fast_downloader.py 10.1038/s41586-019-1750-x

# Download and process multiple papers from a file
python scihub_fast_downloader.py -f test_dois.txt --mode structured

# Process existing PDFs only (no download)
python scihub_fast_downloader.py -p --mode full
```

## Extraction Modes

### 1. Simple Mode
- **Speed**: Fastest
- **Output**: Plain text only
- **Use case**: When you only need the text content

### 2. Structured Mode (Default)
- **Speed**: Fast
- **Output**: Text organized by sections with headings
- **Use case**: When you need document structure preserved
- **Features**:
  - Detects headings based on font size and formatting
  - Organizes content into sections
  - Extracts references separately
  - Preserves document hierarchy

### 3. Full Mode
- **Speed**: Fast (same as structured)
- **Output**: Complete metadata + structured text
- **Use case**: When you need everything
- **Features**: All structured mode features + comprehensive metadata

## Output Format

The parser saves results as JSON files with the following structure:

```json
{
  "file": "/path/to/paper.pdf",
  "mode": "structured",
  "timestamp": "2025-10-10T20:00:00",
  "metadata": {
    "doi": "10.1038/s41586-019-1750-x",
    "title": "Paper Title",
    "author": "Authors",
    "page_count": 15,
    "keywords": ["keyword1", "keyword2"]
  },
  "structured_text": {
    "sections": [
      {
        "title": "Introduction",
        "content": ["paragraph 1", "paragraph 2"]
      },
      {
        "title": "Methods",
        "content": ["paragraph 1", "paragraph 2"]
      }
    ],
    "references": ["ref1", "ref2", "..."],
    "full_text": "complete text...",
    "page_count": 15
  }
}
```

## Performance Comparison

| Parser | Speed | Structure | Accuracy | Server Required |
|--------|-------|-----------|----------|-----------------|
| **Fast PDF Parser** | ⚡⚡⚡ Very Fast | ✓ Good | ✓ Good | ✗ No |
| **GROBID** | 🐌 Slow | ✓✓ Excellent | ✓✓ Excellent | ✓ Yes |

### Typical Processing Times (per paper):
- **Fast PDF Parser**: 0.1-2 seconds
- **GROBID**: 5-30 seconds

## Python API

```python
from fast_pdf_parser import FastPDFParser

# Initialize parser
parser = FastPDFParser(output_dir='output/')

# Process a single PDF
result = parser.process_pdf('paper.pdf', mode='structured')

# Process and save
result = parser.process_and_save('paper.pdf', mode='full')

# Batch process
results = parser.batch_process('papers/', mode='structured')

# Extract just metadata
metadata = parser.extract_metadata('paper.pdf')

# Extract just text (fastest)
text = parser.extract_text_simple('paper.pdf')

# Extract structured text
structured = parser.extract_text_with_structure('paper.pdf')
```

## When to Use Fast PDF Parser vs GROBID

### Use Fast PDF Parser when:
- You need quick results
- Processing large batches of papers
- You don't have access to a GROBID server
- Basic structure preservation is sufficient
- You're working on a local machine

### Use GROBID when:
- You need highest accuracy for citations
- You need detailed author affiliations
- You need precise reference parsing
- You have access to a GROBID server
- Processing time is not critical

## Tips for Best Results

1. **Font-based heading detection**: The parser detects headings based on font size and bold formatting. Works best with well-formatted PDFs.

2. **Reference extraction**: The parser looks for common reference section headers (REFERENCES, Bibliography, etc.). Ensure your PDFs have clear section markers.

3. **Batch processing**: Use batch mode for multiple files to take advantage of efficient processing.

4. **Output organization**: Files are saved with `_fast.json` suffix to distinguish from GROBID output.

## Troubleshooting

### PyMuPDF not found
```bash
pip install PyMuPDF
```

### Poor structure detection
- Some PDFs may not have clear font size differences for headings
- Try using `simple` mode if structure detection is problematic
- Consider using GROBID for better structure preservation

### Missing references
- The parser looks for standard reference section headers
- If references aren't detected, they may be in the full_text field

## Combining with GROBID

You can use both parsers in your workflow:

1. Use **Fast PDF Parser** for initial quick processing
2. Use **GROBID** for papers that need detailed analysis

```bash
# Quick pass with fast parser
python scihub_fast_downloader.py -f dois.txt --mode structured

# Detailed pass with GROBID for specific papers
python scihub_grobid_downloader.py important_paper.doi
```

## License

This parser uses PyMuPDF, which is licensed under AGPL-3.0. For commercial use, consider PyMuPDF's commercial license options.
