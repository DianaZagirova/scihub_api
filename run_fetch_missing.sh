#!/bin/bash
# Helper script to run fetch_missing_papers.py with common configurations

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Default values
DB_PATH="/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db"
WORKERS=4
LIMIT=""
DRY_RUN=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --workers|-w)
            WORKERS="$2"
            shift 2
            ;;
        --limit|-l)
            LIMIT="--limit $2"
            shift 2
            ;;
        --dry-run|-d)
            DRY_RUN="--dry-run"
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -w, --workers N     Number of parallel workers (default: 4)"
            echo "  -l, --limit N       Limit number of papers to process"
            echo "  -d, --dry-run       Only analyze without processing"
            echo "  -h, --help          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                          # Process all papers with 4 workers"
            echo "  $0 --workers 8              # Process with 8 workers"
            echo "  $0 --limit 10 --dry-run     # Dry run with 10 papers"
            echo "  $0 --workers 8 --limit 100  # Process 100 papers with 8 workers"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Check if GROBID server is running
echo "Checking GROBID server..."
if ! curl -s -f "http://10.223.131.158:8072/api/isalive" > /dev/null 2>&1; then
    echo "ERROR: GROBID server is not running!"
    echo "Please start GROBID server first. See GROBID_SETUP.md for instructions."
    exit 1
fi
echo "✓ GROBID server is running"

# Check if database exists
if [ ! -f "$DB_PATH" ]; then
    echo "ERROR: Database not found at $DB_PATH"
    exit 1
fi
echo "✓ Database found"

# Run the script
echo ""
echo "Starting fetch_missing_papers.py..."
echo "  Workers: $WORKERS"
if [ -n "$LIMIT" ]; then
    echo "  Limit: ${LIMIT#--limit }"
fi
if [ -n "$DRY_RUN" ]; then
    echo "  Mode: DRY RUN (no actual processing)"
fi
echo ""

python3 fetch_missing_papers.py \
    --db "$DB_PATH" \
    --workers "$WORKERS" \
    $LIMIT \
    $DRY_RUN

echo ""
echo "Done!"
