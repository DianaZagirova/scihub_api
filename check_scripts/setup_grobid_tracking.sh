#!/bin/bash
# Quick setup script for Grobid tracking

echo "========================================================================"
echo "GROBID TRACKING SETUP"
echo "========================================================================"
echo ""

# Make scripts executable
chmod +x grobid_tracker_integration.py reconcile_all_status.py doi_tracker.py

# Create logs directory
mkdir -p logs

echo "Step 1: Scanning existing Grobid files..."
python grobid_tracker_integration.py --scan-all
echo ""

echo "Step 2: Checking tracker status..."
python doi_tracker.py --stats
echo ""

echo "========================================================================"
echo "SETUP COMPLETE!"
echo "========================================================================"
echo ""
echo "Your Grobid files have been scanned and tracker updated."
echo ""
echo "Next steps:"
echo ""
echo "Option A: Add real-time tracking to your Grobid script"
echo "  Add these lines to your script:"
echo "    from grobid_tracker_integration import GrobidTrackerUpdater"
echo "    updater = GrobidTrackerUpdater()"
echo "    updater.update_single_doi(doi, success=True)"
echo ""
echo "Option B: Set up automated watcher (recommended)"
echo "  Run: ./setup_cron_watcher.sh"
echo ""
echo "Option C: Run manual scan after each batch"
echo "  python grobid_tracker_integration.py --scan-all"
echo ""
echo "========================================================================"
