# Sci-Hub Paper Downloader

A Python tool to download academic papers from Sci-Hub using DOIs (Digital Object Identifiers) and extract full text and metadata using GROBID.

This repository contains three implementations:
1. `scihub_downloader.py` - A standalone implementation that directly scrapes Sci-Hub
2. `scihub_api_downloader.py` - An implementation that uses the `scihub.py` library
3. `scihub_grobid_downloader.py` - An implementation that downloads papers and processes them with GROBID

## Features

- Download papers from Sci-Hub using DOIs, PMIDs, or URLs
- Validate DOI format
- Support for multiple Sci-Hub domains (in case some are blocked)
- Command-line interface for easy use
- Batch download from a file containing multiple identifiers
- Search for papers on Google Scholar and download them
- Customizable output directory
- Extract full text and metadata from PDFs using GROBID
- Save extracted data in structured JSON format
- Process existing PDFs with GROBID without downloading

## Installation

1. Clone this repository or download the script files

2. (Recommended) Create a virtual environment:

   ```bash
   # Using venv (Python 3.3+)
   python -m venv venv
   
   # Activate the virtual environment
   # On macOS/Linux:
   source venv/bin/activate
   # On Windows:
   # venv\Scripts\activate
   ```

3. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Set up GROBID server (required for GROBID functionality):

   - Install and run GROBID server following instructions at [https://grobid.readthedocs.io/](https://grobid.readthedocs.io/)
   - By default, the scripts expect GROBID to be running at http://localhost:8070
   - You can configure the GROBID server URL in `config.json`

5. You're ready to use the scripts!

## Usage

### Implementation 1: Direct Sci-Hub Scraper

#### Basic Usage

Download a single paper by providing its DOI:

```bash
python scihub_downloader.py 10.1038/s41586-019-1750-x
```

#### Multiple DOIs

Download multiple papers by providing multiple DOIs:

```bash
python scihub_downloader.py 10.1038/s41586-019-1750-x 10.1126/science.aau2582
```

#### From a File

Download papers from a file containing DOIs (one per line):

```bash
python scihub_downloader.py -f dois.txt
```

#### Specify Output Directory

```bash
python scihub_downloader.py -o /path/to/output/directory 10.1038/s41586-019-1750-x
```

### Implementation 2: Using scihub.py Library

#### Basic Usage

Download a single paper by providing its DOI, PMID, or URL:

```bash
python scihub_api_downloader.py 10.1038/s41586-019-1750-x
```

#### Multiple Identifiers

Download multiple papers by providing multiple identifiers:

```bash
python scihub_api_downloader.py 10.1038/s41586-019-1750-x https://doi.org/10.1126/science.aau2582
```

#### From a File

Download papers from a file containing identifiers (one per line):

```bash
python scihub_api_downloader.py -f identifiers.txt
```

#### Search and Download

Search for papers on Google Scholar and download them:

```bash
python scihub_api_downloader.py -s "machine learning" -l 5
```

### Implementation 3: Using GROBID Integration

#### Basic Usage

Download a paper and process it with GROBID:

```bash
python scihub_grobid_downloader.py 10.1038/s41586-019-1750-x
```

#### Multiple DOIs

Download and process multiple papers:

```bash
python scihub_grobid_downloader.py 10.1038/s41586-019-1750-x 10.1126/science.aau2582
```

#### From a File

Download and process papers from a file containing DOIs:

```bash
python scihub_grobid_downloader.py -f dois.txt
```

#### Process Existing Papers

Process existing PDF files in the papers directory without downloading:

```bash
python scihub_grobid_downloader.py -p
```

#### Specify Output Directory

```bash
python scihub_grobid_downloader.py -o /path/to/output/directory 10.1038/s41586-019-1750-x
```

### Implementation 4: Direct Use of scihub.py

You can also use the `scihub.py` script directly, which provides the core functionality:

#### Download a Paper

```bash
python scihub.py -d 10.1038/s41586-019-1750-x -o papers/output.pdf
```

#### Download Papers from a File

```bash
python scihub.py -f identifiers.txt -o papers/
```

#### Search on Google Scholar

```bash
python scihub.py -s "quantum computing"
```

#### Search and Download

```bash
python scihub.py -sd "artificial intelligence" -l 3
```

#### Using a Proxy

```bash
python scihub.py -d 10.1038/s41586-019-1750-x -p http://your-proxy:port
```

### Common Options for All Implementations

#### Verbose Output

For more detailed logging:

```bash
python scihub_downloader.py -v 10.1038/s41586-019-1750-x
python scihub_api_downloader.py -v 10.1038/s41586-019-1750-x
python scihub.py -d 10.1038/s41586-019-1750-x -v
```

#### Full Help

```bash
python scihub_downloader.py --help
python scihub_api_downloader.py --help
python scihub.py --help
```

## Notes

- All implementations automatically try multiple Sci-Hub domains if one fails
- Downloaded papers are saved as PDF files named after their identifiers
- By default, papers are saved to a 'papers' directory in the current working directory
- The `scihub_api_downloader.py` script wraps the functionality of `scihub.py` in a more user-friendly interface
- The direct `scihub.py` script provides the core functionality and can be used standalone
- The `scihub_downloader.py` script is a completely independent implementation that doesn't rely on the other scripts
- The `scihub_grobid_downloader.py` script integrates with GROBID to extract full text and metadata
- Extracted data is saved in the 'output' directory in both TEI XML and JSON formats

## Troubleshooting

- If one implementation fails, try the other one as they use different methods to extract PDFs
- Sci-Hub occasionally shows CAPTCHAs which can block automated downloads
- Some papers may not be available on Sci-Hub
- Make sure the GROBID server is running before using GROBID functionality
- If GROBID processing fails, check the GROBID server logs for more information
- You can adjust GROBID settings in the `config.json` file

## Disclaimer

This tool is for educational and research purposes only. Please respect copyright laws and the terms of service of the sources you access. Always cite the papers you use in your research.

## License

This project is open source and available under the MIT License.

## Commands
python scihub_fast_downloader.py -f test_dois.txt --mode structured

This will:

Read the DOI from 
test_dois.txt
 (currently: DOI: 10.1093/geronj/11.3.298)
Download the PDF from Sci-Hub
Process it with the fast PDF parser in structured mode
Save the PDF to the papers/ directory
Save the extracted structured data to the output/ directory

If you want to use simple mode (fastest, plain text only) instead:
python scihub_fast_downloader.py -f test_dois.txt --mode simple

Or for full mode (everything - metadata + structured text):
python scihub_fast_downloader.py -f test_dois.txt --mode full


Simple Mode - Fastest
Extracts: Plain text only
Speed: ~0.1-0.5 seconds per paper
Output: Just the raw text content from the PDF
Use when: You only need the text and don't care about structure
Example output:
json
{
  "text": "Introduction This paper discusses... Methods We used..."
}
Structured Mode - Fast (Default)
Extracts: Text organized by sections + references
Speed: ~0.5-1 second per paper
Output:
Sections with headings (Introduction, Methods, etc.)
Paragraphs organized under each section
References extracted separately
Basic metadata
Use when: You want to preserve document structure
Example output:
json
{
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
    "references": ["ref1", "ref2", "..."]
  }
}
Full Mode - Fast
Extracts: Everything (metadata + structured text)
Speed: ~0.5-1 second per paper (same as structured)
Output: Complete metadata + all structured text features
Use when: You need comprehensive information
Example output:
json
{
  "metadata": {
    "doi": "10.1038/...",
    "title": "Paper Title",
    "author": "Authors",
    "page_count": 15,
    "keywords": ["keyword1", "keyword2"]
  },
  "structured_text": {
    "sections": [...],
    "references": [...]
  }
}
Quick Comparison
Mode	Speed	Metadata	Structure	References
simple	⚡⚡⚡ Fastest	✗	✗	✗
structured	⚡⚡ Fast	Basic	✓	✓
full	⚡⚡ Fast	✓ Complete	✓	✓
Recommendation: Use structured mode (default) for most cases - it gives you good structure preservation with fast speed.


