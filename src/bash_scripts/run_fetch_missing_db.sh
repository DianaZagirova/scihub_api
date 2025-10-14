#!/bin/bash
# Quick start script for fetching missing papers from database

DB_PATH="/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db"

echo "=========================================="
echo "Fetch Missing Papers from Database"
echo "=========================================="
echo ""
echo "This script will:"
echo "  1. Analyze papers.db for missing full_text or abstract"
echo "  2. Download papers from Sci-Hub"
echo "  3. Process with GROBID to extract text"
echo "  4. Update the database with extracted data"
echo ""

# Check if database exists
if [ ! -f "$DB_PATH" ]; then
    echo "Error: Database not found at $DB_PATH"
    exit 1
fi

# Check if GROBID is running
echo "Checking GROBID server..."
if curl -s http://localhost:8070/api/isalive > /dev/null 2>&1; then
    echo "✓ GROBID server is running"
else
    echo "✗ GROBID server is not running!"
    echo "  Please start GROBID first:"
    echo "  docker start grobid"
    exit 1
fi

echo ""
echo "Running analysis..."
python3 fetch_missing_db_papers.py "$DB_PATH" --analyze-only

echo ""
read -p "Do you want to process missing papers? (y/n) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    read -p "How many workers? (default: 4) " workers
    workers=${workers:-4}
    
    read -p "Limit number of papers? (press Enter for all) " limit
    
    if [ -z "$limit" ]; then
        echo "Processing ALL missing papers with $workers workers..."
        python3 fetch_missing_db_papers.py "$DB_PATH" -w "$workers"
    else
        echo "Processing $limit papers with $workers workers..."
        python3 fetch_missing_db_papers.py "$DB_PATH" -w "$workers" --limit "$limit"
    fi
else
    echo "Cancelled."
fi
