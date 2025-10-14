#!/bin/bash
# Monitor processing progress and system resources

echo "=========================================="
echo "Paper Processing Monitor"
echo "=========================================="
echo ""

# Function to format bytes to human readable
format_size() {
    numfmt --to=iec-i --suffix=B $1 2>/dev/null || echo "$1 bytes"
}

# Infinite monitoring loop
while true; do
    clear
    echo "=========================================="
    echo "Paper Processing Monitor - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "=========================================="
    echo ""
    
    # Disk space
    echo "--- DISK SPACE ---"
    df -h / | grep -v Filesystem
    FREE_GB=$(df -BG / | tail -1 | awk '{print $4}' | sed 's/G//')
    if [ $FREE_GB -lt 50 ]; then
        echo "⚠️  WARNING: Less than 50GB free!"
    fi
    echo ""
    
    # Directory sizes
    echo "--- STORAGE USAGE ---"
    if [ -d "papers/" ]; then
        PAPERS_SIZE=$(du -sh papers/ 2>/dev/null | cut -f1)
        PAPERS_COUNT=$(find papers/ -name "*.pdf" 2>/dev/null | wc -l)
        echo "PDFs:   $PAPERS_SIZE ($PAPERS_COUNT files)"
    fi
    
    if [ -d "output/" ]; then
        OUTPUT_SIZE=$(du -sh output/ 2>/dev/null | cut -f1)
        OUTPUT_COUNT=$(find output/ -name "*.json" 2>/dev/null | wc -l)
        echo "JSONs:  $OUTPUT_SIZE ($OUTPUT_COUNT files)"
    fi
    
    if [ -d "logs/" ]; then
        LOGS_SIZE=$(du -sh logs/ 2>/dev/null | cut -f1)
        echo "Logs:   $LOGS_SIZE"
    fi
    echo ""
    
    # Memory usage
    echo "--- MEMORY ---"
    free -h | grep -E "Mem:|Swap:"
    echo ""
    
    # GROBID server status
    echo "--- GROBID SERVER ---"
    if curl -s --max-time 2 http://10.223.131.158:8072/api/isalive > /dev/null 2>&1; then
        VERSION=$(curl -s --max-time 2 http://10.223.131.158:8072/api/version 2>/dev/null | grep -o '"version":"[^"]*"' | cut -d'"' -f4)
        echo "Status: ✓ ONLINE (v$VERSION)"
    else
        echo "Status: ✗ OFFLINE or UNREACHABLE"
    fi
    echo ""
    
    # Latest log file
    echo "--- LATEST LOG ---"
    LATEST_LOG=$(ls -t logs/comprehensive_log_*.log 2>/dev/null | head -1)
    if [ -n "$LATEST_LOG" ]; then
        echo "File: $LATEST_LOG"
        echo "Size: $(du -sh "$LATEST_LOG" 2>/dev/null | cut -f1)"
        echo ""
        echo "Last 5 entries:"
        grep "^DOI/Identifier:" "$LATEST_LOG" 2>/dev/null | tail -5 | sed 's/DOI\/Identifier: /  • /'
    else
        echo "No log files found"
    fi
    echo ""
    
    # Running processes
    echo "--- RUNNING PROCESSES ---"
    PYTHON_PROCS=$(ps aux | grep -E "download_papers.py|grobid" | grep -v grep | wc -l)
    if [ $PYTHON_PROCS -gt 0 ]; then
        echo "Active processes: $PYTHON_PROCS"
        ps aux | grep -E "download_papers.py" | grep -v grep | awk '{print "  • PID", $2, "-", $11, $12, $13}'
    else
        echo "No active processing detected"
    fi
    echo ""
    
    echo "=========================================="
    echo "Refreshing in 30 seconds... (Ctrl+C to exit)"
    echo "=========================================="
    
    sleep 30
done
