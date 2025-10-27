# PDF Storage

Downloaded PDF files are stored in the `papers/` directory.

## File Naming Convention

PDFs are named using sanitized DOI strings:
- Original DOI: `10.1234/example.2024`
- Filename: `10.1234_example.2024.pdf`
- Special characters (`/`, `:`, etc.) are replaced with underscores

## Directory Structure

```
papers/
├── 10.1016_j.cell.2023.01.001.pdf
├── 10.1038_nature12345.pdf
├── 10.1093_geronj_45.2.123.pdf
└── ... (thousands of PDFs)
```

## Storage Requirements

- Average PDF size: 1-5 MB
- For 10,000 papers: ~20-50 GB
- For 100,000 papers: ~200-500 GB

## Why Not Committed to Git

PDF files are **NOT committed to the repository** because:
1. **Size**: Large binary files (GBs of data)
2. **Copyright**: Papers may be copyrighted material
3. **Reproducibility**: Can be re-downloaded using the pipeline
4. **Git Performance**: Binary files bloat repository history

## Validation

PDFs are validated during sync:
- Must have valid PDF header (`%PDF-`)
- Must have EOF marker (`%%EOF`)
- Minimum size: 1 KB

## Example

A typical downloaded PDF would be stored as:
```
papers/10.1234_example.2024.pdf
```

And can be parsed to generate:
```
output/10.1234_example.2024_fast.json
```
or
```
output/10.1234_example.2024_grobid.json
```
