# Legacy Files

This folder contains the original implementation files that have been moved from the root directory for better organization.

## Files

- `scihub.py` - Original Sci-Hub implementation
- `scihub_api_downloader.py` - API-based downloader
- `scihub_downloader.py` - Core downloading logic
- `scihub_fast_downloader.py` - Fast parser workflow
- `scihub_grobid_downloader.py` - GROBID parser workflow
- `fast_pdf_parser.py` - PyMuPDF-based parser
- `grobid_parser.py` - GROBID-based parser

## Usage

These files are still functional and are used by the main `download_papers.py` script.

You can also run them directly:

```bash
# From project root
python legacy/scihub_fast_downloader.py -f test_dois.txt --mode structured
python legacy/scihub_grobid_downloader.py -f test_dois.txt
```

## Note

These files will be gradually refactored into the new `src/` structure for better maintainability.
