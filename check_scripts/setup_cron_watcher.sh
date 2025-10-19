#!/bin/bash
# Set up automated cron job for Grobid tracking

echo "========================================================================"
echo "SETUP AUTOMATED GROBID WATCHER"
echo "========================================================================"
echo ""
echo "This will add a cron job to check for new Grobid files every 30 minutes."
echo ""
echo "The cron job will:"
echo "  - Check ./output/ for new Grobid JSONs"
echo "  - Update tracker with new files"
echo "  - Log results to logs/grobid_watcher.log"
echo ""
echo "Press Ctrl+C to cancel, or Enter to continue..."
read

# Get current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Create cron entry
CRON_CMD="*/30 * * * * cd $SCRIPT_DIR && python grobid_tracker_integration.py --watch-new >> logs/grobid_watcher.log 2>&1"

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "grobid_tracker_integration.py --watch-new"; then
    echo "⚠️  Cron job already exists!"
    echo ""
    echo "Current cron jobs:"
    crontab -l | grep "grobid_tracker_integration.py"
    echo ""
    echo "Do you want to replace it? (y/n)"
    read REPLACE
    
    if [ "$REPLACE" != "y" ]; then
        echo "Cancelled."
        exit 0
    fi
    
    # Remove old entry
    crontab -l | grep -v "grobid_tracker_integration.py --watch-new" | crontab -
fi

# Add new entry
(crontab -l 2>/dev/null; echo "$CRON_CMD") | crontab -

echo ""
echo "✓ Cron job added successfully!"
echo ""
echo "Schedule: Every 30 minutes"
echo "Log file: $SCRIPT_DIR/logs/grobid_watcher.log"
echo ""
echo "To view cron jobs:"
echo "  crontab -l"
echo ""
echo "To view logs:"
echo "  tail -f logs/grobid_watcher.log"
echo ""
echo "To test manually:"
echo "  python grobid_tracker_integration.py --watch-new"
echo ""
echo "========================================================================"
