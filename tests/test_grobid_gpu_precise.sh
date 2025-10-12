#!/bin/bash
# Precise GROBID GPU test

echo "========================================"
echo "PRECISE GROBID GPU TEST"
echo "========================================"
echo ""

# Get baseline GPU memory
echo "1. Baseline GPU memory (before GROBID processing):"
nvidia-smi --query-gpu=index,memory.used --format=csv,noheader,nounits | head -4
echo ""

# Store baseline
baseline=$(nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits | head -1)
echo "GPU 0 baseline: ${baseline} MiB"
echo ""

echo "2. Starting GROBID processing (1 paper)..."
echo "   Watch for memory increase..."
echo ""

# Process one paper
python -c "
import sys
from pathlib import Path
sys.path.insert(0, str(Path('.') / 'legacy'))
from scihub_grobid_downloader import SciHubGrobidDownloader
import time

downloader = SciHubGrobidDownloader()
start = time.time()
pdf_path, data, status = downloader.download_and_process('10.1038/nature12373')
elapsed = time.time() - start
print(f'\nProcessing time: {elapsed:.2f}s')
print(f'Status: {status}')
"

echo ""
echo "3. GPU memory after GROBID processing:"
nvidia-smi --query-gpu=index,memory.used --format=csv,noheader,nounits | head -4
echo ""

# Check if memory increased
after=$(nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits | head -1)
increase=$((after - baseline))

echo "========================================"
echo "RESULT:"
echo "========================================"
echo "GPU 0 baseline: ${baseline} MiB"
echo "GPU 0 after:    ${after} MiB"
echo "Change:         ${increase} MiB"
echo ""

if [ $increase -gt 100 ]; then
    echo "✓ GPU memory INCREASED significantly!"
    echo "  → GROBID IS using GPU"
elif [ $increase -gt 10 ]; then
    echo "⚠ GPU memory increased slightly"
    echo "  → GROBID might be using GPU minimally"
else
    echo "✗ GPU memory DID NOT increase"
    echo "  → GROBID is NOT using GPU (CPU only)"
    echo ""
    echo "To enable GPU in GROBID:"
    echo "1. Build GROBID with DeLFT: ./gradlew clean assemble -Pdelft=true"
    echo "2. Edit grobid.yaml to enable GPU (see GROBID_GPU_SETUP.md)"
    echo "3. Restart GROBID server"
fi
echo ""
