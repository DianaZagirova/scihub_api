#!/bin/bash
# Safe batch processor with disk space monitoring and error handling

set -e  # Exit on error

# Configuration
MIN_FREE_SPACE_GB=50
PARSER="grobid"
WORKERS=4
DELAY=2.0
BATCH_DIR="batches"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check disk space
check_disk_space() {
    FREE_GB=$(df -BG / | tail -1 | awk '{print $4}' | sed 's/G//')
    if [ $FREE_GB -lt $MIN_FREE_SPACE_GB ]; then
        echo -e "${RED}ERROR: Insufficient disk space!${NC}"
        echo "  Free space: ${FREE_GB}GB"
        echo "  Required: ${MIN_FREE_SPACE_GB}GB minimum"
        return 1
    fi
    echo -e "${GREEN}✓${NC} Disk space OK: ${FREE_GB}GB free"
    return 0
}

# Function to check GROBID server
check_grobid() {
    if curl -s --max-time 5 http://10.223.131.158:8072/api/isalive > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} GROBID server is online"
        return 0
    else
        echo -e "${RED}ERROR: GROBID server is not responding!${NC}"
        return 1
    fi
}

# Function to process a single batch
process_batch() {
    local batch_file=$1
    local batch_name=$(basename "$batch_file" .txt)
    
    echo ""
    echo "=========================================="
    echo "Processing: $batch_name"
    echo "=========================================="
    echo ""
    
    # Pre-flight checks
    echo "Running pre-flight checks..."
    check_disk_space || return 1
    if [ "$PARSER" == "grobid" ]; then
        check_grobid || return 1
    fi
    echo ""
    
    # Count DOIs in batch
    local num_dois=$(wc -l < "$batch_file")
    echo "DOIs in this batch: $num_dois"
    echo ""
    
    # Start processing
    echo "Starting processing..."
    echo "Command: python download_papers.py -f $batch_file --parser $PARSER -w $WORKERS --delay $DELAY"
    echo ""
    
    # Run the download script
    if python download_papers.py -f "$batch_file" --parser "$PARSER" -w "$WORKERS" --delay "$DELAY"; then
        echo ""
        echo -e "${GREEN}✓ Batch completed successfully: $batch_name${NC}"
        
        # Post-processing check
        check_disk_space
        
        return 0
    else
        echo ""
        echo -e "${RED}✗ Batch failed: $batch_name${NC}"
        return 1
    fi
}

# Main script
main() {
    echo "=========================================="
    echo "Safe Batch Processor for Paper Download"
    echo "=========================================="
    echo ""
    echo "Configuration:"
    echo "  Parser: $PARSER"
    echo "  Workers: $WORKERS"
    echo "  Delay: ${DELAY}s"
    echo "  Min free space: ${MIN_FREE_SPACE_GB}GB"
    echo "  Batch directory: $BATCH_DIR"
    echo ""
    
    # Check if batch directory exists
    if [ ! -d "$BATCH_DIR" ]; then
        echo -e "${RED}ERROR: Batch directory not found: $BATCH_DIR${NC}"
        echo ""
        echo "Please run prepare_batches.py first:"
        echo "  python prepare_batches.py your_dois.txt -b 5000 -o $BATCH_DIR"
        exit 1
    fi
    
    # Find all batch files
    BATCH_FILES=($(ls "$BATCH_DIR"/batch_*.txt 2>/dev/null | sort))
    
    if [ ${#BATCH_FILES[@]} -eq 0 ]; then
        echo -e "${RED}ERROR: No batch files found in $BATCH_DIR${NC}"
        exit 1
    fi
    
    echo "Found ${#BATCH_FILES[@]} batch files"
    echo ""
    
    # Ask for confirmation
    read -p "Process all ${#BATCH_FILES[@]} batches? (y/N) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cancelled by user"
        exit 0
    fi
    
    # Process each batch
    SUCCESS_COUNT=0
    FAIL_COUNT=0
    FAILED_BATCHES=()
    
    for batch_file in "${BATCH_FILES[@]}"; do
        if process_batch "$batch_file"; then
            ((SUCCESS_COUNT++))
            
            # Sleep between batches
            echo ""
            echo "Waiting 60 seconds before next batch..."
            sleep 60
        else
            ((FAIL_COUNT++))
            FAILED_BATCHES+=("$batch_file")
            
            # Ask whether to continue
            echo ""
            read -p "Continue with next batch? (y/N) " -n 1 -r
            echo ""
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                echo "Stopping batch processing"
                break
            fi
        fi
    done
    
    # Final summary
    echo ""
    echo "=========================================="
    echo "BATCH PROCESSING COMPLETE"
    echo "=========================================="
    echo "Total batches: ${#BATCH_FILES[@]}"
    echo -e "${GREEN}Successful: $SUCCESS_COUNT${NC}"
    if [ $FAIL_COUNT -gt 0 ]; then
        echo -e "${RED}Failed: $FAIL_COUNT${NC}"
        echo ""
        echo "Failed batches:"
        for failed in "${FAILED_BATCHES[@]}"; do
            echo "  - $(basename "$failed")"
        done
    fi
    echo ""
    
    # Final disk space check
    check_disk_space
    echo ""
    
    # Show storage summary
    echo "Storage summary:"
    [ -d "papers/" ] && echo "  PDFs: $(du -sh papers/ 2>/dev/null | cut -f1) ($(find papers/ -name "*.pdf" 2>/dev/null | wc -l) files)"
    [ -d "output/" ] && echo "  JSONs: $(du -sh output/ 2>/dev/null | cut -f1) ($(find output/ -name "*.json" 2>/dev/null | wc -l) files)"
    [ -d "logs/" ] && echo "  Logs: $(du -sh logs/ 2>/dev/null | cut -f1)"
    echo ""
    
    if [ $FAIL_COUNT -eq 0 ]; then
        echo -e "${GREEN}All batches processed successfully!${NC}"
        exit 0
    else
        echo -e "${YELLOW}Some batches failed. Check logs for details.${NC}"
        exit 1
    fi
}

# Parse command line arguments
while getopts "p:w:d:b:m:" opt; do
    case $opt in
        p) PARSER="$OPTARG" ;;
        w) WORKERS="$OPTARG" ;;
        d) DELAY="$OPTARG" ;;
        b) BATCH_DIR="$OPTARG" ;;
        m) MIN_FREE_SPACE_GB="$OPTARG" ;;
        *) 
            echo "Usage: $0 [-p parser] [-w workers] [-d delay] [-b batch_dir] [-m min_space_gb]"
            echo ""
            echo "Options:"
            echo "  -p  Parser type (fast|grobid, default: grobid)"
            echo "  -w  Number of workers (default: 4)"
            echo "  -d  Delay between requests in seconds (default: 2.0)"
            echo "  -b  Batch directory (default: batches)"
            echo "  -m  Minimum free space in GB (default: 50)"
            exit 1
            ;;
    esac
done

# Run main
main
