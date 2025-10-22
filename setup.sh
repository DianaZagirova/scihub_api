#!/bin/bash

# Sci-Hub API Setup Script
# ========================
# This script sets up the environment for the Sci-Hub API project
# for aging research paper collection and processing.

set -e  # Exit on any error

echo "🧬 Sci-Hub API Setup Script"
echo "=========================="
echo "Setting up environment for aging research paper collection..."
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Python 3.8+ is available
check_python() {
    print_status "Checking Python version..."
    
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
        PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
        PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)
        
        if [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -ge 8 ]; then
            print_success "Python $PYTHON_VERSION found (✓ 3.8+ required)"
            PYTHON_CMD="python3"
        else
            print_error "Python 3.8+ required, found $PYTHON_VERSION"
            exit 1
        fi
    elif command -v python &> /dev/null; then
        PYTHON_VERSION=$(python -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
        PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
        PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)
        
        if [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -ge 8 ]; then
            print_success "Python $PYTHON_VERSION found (✓ 3.8+ required)"
            PYTHON_CMD="python"
        else
            print_error "Python 3.8+ required, found $PYTHON_VERSION"
            exit 1
        fi
    else
        print_error "Python not found. Please install Python 3.8 or higher."
        exit 1
    fi
}

# Create virtual environment
create_venv() {
    print_status "Creating virtual environment..."
    
    if [ -d "venv" ]; then
        print_warning "Virtual environment already exists. Removing old one..."
        rm -rf venv
    fi
    
    $PYTHON_CMD -m venv venv
    print_success "Virtual environment created"
}

# Activate virtual environment
activate_venv() {
    print_status "Activating virtual environment..."
    
    if [ -f "venv/bin/activate" ]; then
        source venv/bin/activate
        print_success "Virtual environment activated"
    else
        print_error "Failed to activate virtual environment"
        exit 1
    fi
}

# Install dependencies
install_dependencies() {
    print_status "Installing Python dependencies..."
    
    # Upgrade pip first
    pip install --upgrade pip
    
    # Install requirements
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
        print_success "Dependencies installed from requirements.txt"
    else
        print_error "requirements.txt not found"
        exit 1
    fi
}

# Create necessary directories
create_directories() {
    print_status "Creating necessary directories..."
    
    mkdir -p logs
    mkdir -p papers
    mkdir -p output
    mkdir -p demo_output
    mkdir -p demo_papers
    mkdir -p failed_dois
    mkdir -p missing_dois
    
    print_success "Directories created"
}

# Check test database
check_test_database() {
    print_status "Checking test database..."
    
    TEST_DB_PATH="/home/diana.z/hack/download_papers_pubmed/paper_collection_test/data/papers.db"
    
    if [ -f "$TEST_DB_PATH" ]; then
        print_success "Test database found at $TEST_DB_PATH"
        
        # Get paper count
        PAPER_COUNT=$(sqlite3 "$TEST_DB_PATH" "SELECT COUNT(*) FROM papers;" 2>/dev/null || echo "0")
        print_status "Test database contains $PAPER_COUNT papers"
    else
        print_warning "Test database not found at $TEST_DB_PATH"
        print_warning "Demo will still work but may have limited functionality"
    fi
}

# Create configuration file
create_config() {
    print_status "Creating configuration file..."
    
    if [ ! -f "config.json" ]; then
        cat > config.json << EOF
{
  "grobid_server": "http://localhost:8070",
  "batch_size": 1000,
  "timeout": 90,
  "sleep_time": 0,
  "max_workers": 12,
  "consolidate_header": 0,
  "consolidate_citations": 0,
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
  ],
  "logging": {
    "level": "INFO",
    "format": "%(asctime)s - %(levelname)s - %(message)s",
    "console": true,
    "file": null,
    "max_file_size": "10MB",
    "backup_count": 3
  }
}
EOF
        print_success "Configuration file created"
    else
        print_status "Configuration file already exists"
    fi
}

# Test installation
test_installation() {
    print_status "Testing installation..."
    
    # Test Python imports
    python -c "
import sys
try:
    import requests
    import fitz
    import pandas
    import numpy
    import tqdm
    print('✓ All required packages imported successfully')
except ImportError as e:
    print(f'✗ Import error: {e}')
    sys.exit(1)
"
    
    if [ $? -eq 0 ]; then
        print_success "Installation test passed"
    else
        print_error "Installation test failed"
        exit 1
    fi
}

# Run demo test
run_demo_test() {
    print_status "Running demo test..."
    
    if [ -f "demo.py" ]; then
        print_status "Running demo script (this may take a few minutes)..."
        python demo.py
        print_success "Demo completed successfully"
    else
        print_warning "Demo script not found, skipping demo test"
    fi
}

# Main setup function
main() {
    echo ""
    print_status "Starting Sci-Hub API setup..."
    echo ""
    
    # Step 1: Check Python
    check_python
    echo ""
    
    # Step 2: Create virtual environment
    create_venv
    echo ""
    
    # Step 3: Activate virtual environment
    activate_venv
    echo ""
    
    # Step 4: Install dependencies
    install_dependencies
    echo ""
    
    # Step 5: Create directories
    create_directories
    echo ""
    
    # Step 6: Check test database
    check_test_database
    echo ""
    
    # Step 7: Create configuration
    create_config
    echo ""
    
    # Step 8: Test installation
    test_installation
    echo ""
    
    # Step 9: Run demo test
    run_demo_test
    echo ""
    
    # Success message
    echo "🎉 Setup completed successfully!"
    echo ""
    echo "Next steps:"
    echo "1. Activate the virtual environment: source venv/bin/activate"
    echo "2. Run the demo: python demo.py"
    echo "3. Check the README.md for detailed usage instructions"
    echo ""
    echo "For help, see the documentation in the documentation/ directory"
    echo ""
}

# Run main function
main "$@"
