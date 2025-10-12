# Sci-Hub Paper Downloader

A Python tool to download academic papers from Sci-Hub using **DOI (Digital Object Identifier)**, **PMID (PubMed ID)**, or **paper title** and extract full text and metadata using GROBID or fast PDF parser.

This repository contains multiple implementations:
1. `scihub_downloader.py` - A standalone implementation that directly scrapes Sci-Hub
2. `scihub_api_downloader.py` - An implementation that uses the `scihub.py` library
3. `scihub_fast_downloader.py` - Downloads papers and processes them with fast PDF parser
4. `scihub_grobid_downloader.py` - Downloads papers and processes them with GROBID
5. `download_papers.py` - Main CLI that provides unified access to all downloaders

## Features

- **Download papers from Sci-Hub using:**
  - **DOI (Digital Object Identifier)** - e.g., `10.1038/s41586-019-1750-x` - Direct download
  - **PMID (PubMed ID)** - e.g., `27353257` - Automatically converts to DOI via PubMed API
  - **Paper title** - e.g., `"A synopsis on aging—Theories, mechanisms and future prospects"` - Automatically finds DOI via CrossRef API
- **Automatic identifier detection** - the system automatically detects what type of identifier you provide
- **Smart conversion**: PMIDs and titles are automatically converted to DOIs using public APIs (PubMed E-utilities and CrossRef)
- Validate DOI and PMID formats
- Support for multiple Sci-Hub domains (in case some are blocked)
- Command-line interface for easy use
- Batch download from a file containing multiple identifiers (can mix DOIs, PMIDs, and titles)
- Search for papers on Google Scholar and download them
- Customizable output directory
- **Two PDF parsing options:**
  - **Fast PDF parser** - Quick extraction with structured text (recommended)
  - **GROBID** - Comprehensive extraction with detailed metadata
- Save extracted data in structured JSON format
- Process existing PDFs without downloading

### How It Works

1. **DOI**: Downloads directly from Sci-Hub
2. **PMID**: Queries PubMed E-utilities API to get the DOI, then downloads from Sci-Hub
3. **Title**: Queries CrossRef API to find matching papers and their DOIs (high-confidence matches only), then downloads from Sci-Hub

**Note**: Rate limits apply to PubMed and CrossRef APIs. If you're processing many papers, DOI is the fastest method.

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

### Quick Start (Recommended)

The simplest way to use this tool is via the main CLI `download_papers.py`:

#### Download by DOI
```bash
python download_papers.py 10.1038/s41586-019-1750-x
```

#### Download by PMID
```bash
python download_papers.py 32265220
```

#### Download by Title
```bash
python download_papers.py "Deep learning for protein structure prediction"
```

#### Download from File (Mixed Identifiers)
Create a file `identifiers.txt` with one identifier per line (can mix DOIs, PMIDs, and titles):
```
10.1038/s41586-019-1750-x
32265220
Deep learning applications in biology
10.1126/science.aau2582
```

Then run:
```bash
python download_papers.py -f identifiers.txt
```

#### Use GROBID Parser
```bash
python download_papers.py 10.1038/s41586-019-1750-x --parser grobid
```

#### Use Fast Parser with Different Modes
```bash
# Simple mode (text only, fastest)
python download_papers.py 10.1038/s41586-019-1750-x --mode simple

# Structured mode (sections + references, default)
python download_papers.py 10.1038/s41586-019-1750-x --mode structured

# Full mode (metadata + structured text)
python download_papers.py 10.1038/s41586-019-1750-x --mode full
```

---

### 🚀 **Parallel Processing (For Large Batches)**

For processing **many papers** (100+), use the parallel downloader for **6-8x faster** processing:

#### Fast Parallel Processing
```bash
# Process 100 papers with 8 parallel workers
python parallel_download.py -f dois.txt -w 8

# Expected: ~2-3 hours for 1000 papers (vs 16-20 hours sequential)
```

#### GROBID Parallel Processing
```bash
# Process with 4 workers (GROBID is CPU-intensive)
python parallel_download.py -f dois.txt -w 4 --parser grobid

# Expected: ~4-6 hours for 1000 papers
```

#### 📊 Automatic Comprehensive Reports

Each run automatically generates a detailed report (`logs/processing_report_*.txt`) with:
- ✅ Summary statistics (success rate, failures)
- ✅ Complete list of processed papers with PDF paths
- ✅ Papers not found on Sci-Hub
- ✅ Processing failures
- ✅ Ready-to-use retry list

```bash
# View the report
cat logs/processing_report_*.txt
```

**See `PERFORMANCE_GUIDE.md` for detailed optimization strategies.**

#### 🚀 GPU Acceleration (GROBID)

Enable GPU for **1.5-2x faster** GROBID processing:

```bash
# Configure GROBID with GPU support (grobid.yaml)
grobid:
  delft:
    enabled: true
    use_gpu: true
    gpu_device: 0
  concurrency: 8
  poolSize: 8

# Verify GPU is working
python verify_grobid_parallel.py

# Process with GPU
python parallel_download.py -f dois.txt -w 6 --parser grobid
```

**Performance**: CPU (4 workers): ~8 papers/min | GPU (6 workers): ~15-20 papers/min

**See `GROBID_GPU_SETUP.md` for detailed GPU setup and verification.**

---

### Implementation 1: Direct Sci-Hub Scraper

#### Basic Usage

Download a single paper by providing its DOI, PMID, or title:

```bash
python scihub_downloader.py 10.1038/s41586-019-1750-x
python scihub_downloader.py 32265220
python scihub_downloader.py "Machine learning in genomics"
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


#C
ommands
# 1. Run parallel download
python parallel_download.py -f my_papers.txt -w 8

# 2. View the report
cat logs/processing_report_20251011_200206.txt

# 3. Extract failures for retry
grep -A 100 "RETRY LIST" logs/processing_report_*.txt | tail -n +4 > retry.txt

# 4. Retry failed papers
python parallel_download.py -f retry.txt -w 4

