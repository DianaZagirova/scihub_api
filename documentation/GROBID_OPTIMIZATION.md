# GROBID Performance Optimization Guide

## 1. Speed Up GROBID Processing

### Client-Side Configuration (`config.json`)

```json
{
  "grobid_server": "http://10.223.131.158:8072",
  "batch_size": 1000,
  "timeout": 180,
  "sleep_time": 0.5,  // Reduce from 5 to 0.5 for faster parallel processing
  "max_workers": 8,   // Number of parallel threads (add this)
  "coordinates": [...] // See "Control Output" section
}
```

**Key Parameters:**
- `sleep_time`: Delay between requests (seconds). Lower = faster, but may overload server
  - Fast: `0.1-0.5` (if server has high concurrency)
  - Balanced: `1-2` 
  - Safe: `5` (default)
- `timeout`: Max time to wait for GROBID response (seconds)
- `max_workers`: Number of parallel processing threads (default: 4)

### Server-Side Configuration (GROBID Server)

If you control the GROBID server, edit `grobid-home/config/grobid.yaml`:

```yaml
grobid:
  # Increase concurrency for parallel processing
  concurrency: 20  # Default: 10, set to ~number of CPU cores
  
  # Preload models at startup (faster first request)
  modelPreload: true
  
  # Pool wait time
  poolMaxWait: 1
```

**Docker Command with Performance Tuning:**
```bash
docker run --rm -p 8072:8070 \
  -e JAVA_OPTS="-Xmx8g -XX:+UseG1GC" \
  grobid/grobid:0.8.2
```

## 2. Control Output Format

### Minimal Output (Fastest)
Remove coordinates to get minimal TEI output:

```json
{
  "coordinates": []
}
```

### Standard Output (Recommended)
```json
{
  "coordinates": [
    "persName",
    "biblStruct",
    "ref"
  ]
}
```

### Full Output (Slowest, Most Detailed)
```json
{
  "coordinates": [
    "title",
    "persName",
    "affiliation",
    "orgName",
    "formula",
    "figure",
    "ref",
    "biblStruct",
    "head",
    "p",
    "s",
    "note"
  ]
}
```

### API Parameters

You can control output via API parameters:

**Header Only (Fastest):**
```bash
curl --form input=@paper.pdf \
  http://10.223.131.158:8072/api/processHeaderDocument
```

**Full Document:**
```bash
curl --form input=@paper.pdf \
  --form consolidateHeader=1 \
  --form consolidateCitations=0 \
  --form teiCoordinates="persName,ref" \
  http://10.223.131.158:8072/api/processFulltextDocument
```

**Parameters:**
- `consolidateHeader`: 0=off, 1=full (default), 2=DOI only, 3=extracted DOI only
- `consolidateCitations`: 0=off, 1=on (slower but better references)
- `consolidateFunders`: 0=off, 1=on
- `includeRawCitations`: 0=off, 1=on
- `teiCoordinates`: Comma-separated list of elements to include coordinates for
- `segmentSentences`: true/false - segment sentences (slower)
- `start`: Start page number (default: 1)
- `end`: End page number (default: -1 = all)

## 3. Model Selection

### Available Engines

GROBID supports two engines (configured server-side in `grobid.yaml`):

**CRF (Wapiti) - Faster:**
```yaml
models:
  - name: "citation"
    engine: "wapiti"
    wapiti:
      epsilon: 0.00001
      window: 30
      nbMaxIterations: 2000
```

**Deep Learning (DeLFT) - More Accurate but Slower:**
```yaml
models:
  - name: "citation"
    engine: "delft"
    delft:
      architecture: "BidLSTM_CRF"  # or "BERT", "BERT-CRF"
      runtime:
        batch_size: 20
        max_sequence_length: 3000
```

**Available Architectures:**
- `BidLSTM_CRF` - Fast, good accuracy
- `BidLSTM_CRF_FEATURES` - Balanced
- `BERT` - Slow, best accuracy
- `BERT-CRF` - Slow, best accuracy
- `BERT_CRF_FEATURES` - Slowest, highest accuracy

## 4. Parallel Processing

### Using the Batch Processor

```bash
# Process existing PDFs with 8 parallel workers
python legacy/grobid_parser.py \
  --dir papers/ \
  --output output/ \
  --workers 8
```

### Using Download Script with Parallel Processing

```bash
# Download and process with parallel GROBID
python download_papers.py \
  -f dois.txt \
  --parser grobid
```

The code uses `ThreadPoolExecutor` with configurable workers.

### Optimal Worker Count

**Formula:** `max_workers = min(GROBID_concurrency, CPU_cores)`

Example:
- GROBID server concurrency: 20
- Your CPU cores: 8
- Recommended `max_workers`: 8

## 5. Performance Comparison

| Configuration | Speed | Accuracy | Use Case |
|--------------|-------|----------|----------|
| Header only + CRF | ⚡⚡⚡ Fastest | ⭐⭐ Good | Metadata extraction only |
| Full text + CRF + no consolidation | ⚡⚡ Fast | ⭐⭐⭐ Good | Full text, no reference lookup |
| Full text + CRF + consolidation | ⚡ Medium | ⭐⭐⭐⭐ Very Good | Production use |
| Full text + BERT + consolidation | 🐌 Slow | ⭐⭐⭐⭐⭐ Excellent | Research/high accuracy |

## 6. Practical Examples

### Example 1: Fast Metadata Extraction
```python
from legacy.grobid_parser import GrobidParser

parser = GrobidParser(config_path='config.json')

# Process header only (fastest)
result = parser.process_pdf('paper.pdf', output_format='header')
```

### Example 2: Parallel Batch Processing
```python
from legacy.grobid_parser import GrobidParser

parser = GrobidParser(config_path='config.json')

# Process 100 papers with 10 parallel workers
results = parser.batch_process(
    pdf_dir='papers/',
    output_dir='output/',
    max_workers=10
)
```

### Example 3: Custom API Call with Minimal Output
```python
import requests

url = "http://10.223.131.158:8072/api/processFulltextDocument"
files = {'input': open('paper.pdf', 'rb')}
data = {
    'consolidateHeader': '0',      # No consolidation
    'consolidateCitations': '0',   # No citation consolidation
    'teiCoordinates': '',          # No coordinates
    'start': '1',
    'end': '10'                    # Only first 10 pages
}

response = requests.post(url, files=files, data=data, timeout=180)
```

## 7. Monitoring Performance

### Check GROBID Server Status
```bash
curl http://10.223.131.158:8072/api/isalive
```

### Check Server Load
```bash
curl http://10.223.131.158:8072/api/admin/ping
```

## 8. Troubleshooting

### 503 Errors (Server Overloaded)
- Reduce `max_workers` in your client
- Increase `sleep_time` between requests
- Increase GROBID server `concurrency`

### Timeout Errors
- Increase `timeout` in config.json
- Process smaller page ranges with `start`/`end` parameters
- Use header-only processing for large PDFs

### Slow Processing
- Reduce `coordinates` list
- Disable consolidation (`consolidateHeader=0`)
- Use CRF instead of BERT models
- Process only needed pages with `start`/`end`

## 9. Recommended Settings

### For Speed (Metadata Only)
```json
{
  "grobid_server": "http://10.223.131.158:8072",
  "timeout": 60,
  "sleep_time": 0.2,
  "max_workers": 10,
  "coordinates": []
}
```

### For Balance (Production)
```json
{
  "grobid_server": "http://10.223.131.158:8072",
  "timeout": 180,
  "sleep_time": 0.5,
  "max_workers": 8,
  "coordinates": ["persName", "biblStruct", "ref"]
}
```

### For Accuracy (Research)
```json
{
  "grobid_server": "http://10.223.131.158:8072",
  "timeout": 300,
  "sleep_time": 1,
  "max_workers": 4,
  "coordinates": ["title", "persName", "affiliation", "orgName", "formula", "figure", "ref", "biblStruct", "head", "p", "s", "note"]
}
```
