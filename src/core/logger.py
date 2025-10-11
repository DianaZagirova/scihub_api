"""Centralized logging utilities for the Sci-Hub downloader."""

import os
import logging
import datetime
from pathlib import Path
from typing import Optional


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Set up a logger with consistent formatting.
    
    Args:
        name: Logger name
        level: Logging level
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Only add handler if logger doesn't have one
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


class LogManager:
    """Manages different types of log files for tracking paper processing."""
    
    def __init__(self, logs_dir: Optional[str] = None, enabled: bool = True):
        """
        Initialize the log manager.
        
        Args:
            logs_dir: Directory to store log files
            enabled: Whether logging is enabled
        """
        self.enabled = enabled
        self.logs_dir = logs_dir or os.path.join(os.getcwd(), 'logs')
        
        if self.enabled:
            # Create logs directory
            Path(self.logs_dir).mkdir(parents=True, exist_ok=True)
            
            # Generate timestamp for log files
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Define log file paths
            self.not_found_log = os.path.join(self.logs_dir, f"not_found_scihub_{timestamp}.log")
            self.processing_failed_log = os.path.join(self.logs_dir, f"processing_failed_{timestamp}.log")
            self.success_log = os.path.join(self.logs_dir, f"success_{timestamp}.log")
            
            # Initialize log files
            self._init_log_files()
    
    def _init_log_files(self):
        """Initialize log files with headers."""
        headers = {
            self.not_found_log: (
                "# Papers Not Found on Sci-Hub",
                "# Format: [Timestamp] DOI - Reason"
            ),
            self.processing_failed_log: (
                "# Papers Downloaded but Failed Processing",
                "# Format: [Timestamp] DOI - PDF Path - Error"
            ),
            self.success_log: (
                "# Successfully Processed Papers",
                "# Format: [Timestamp] DOI - PDF Path"
            )
        }
        
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for log_file, (title, format_info) in headers.items():
            with open(log_file, 'w') as f:
                f.write(f"{title} - Created at {timestamp}\n")
                f.write(f"{format_info}\n\n")
    
    def log_entry(self, log_type: str, doi: str, message: str):
        """
        Log an entry to a specific log file.
        
        Args:
            log_type: Type of log ('not_found', 'processing_failed', or 'success')
            doi: DOI of the paper
            message: Log message
        """
        if not self.enabled:
            return
        
        log_files = {
            'not_found': self.not_found_log,
            'processing_failed': self.processing_failed_log,
            'success': self.success_log
        }
        
        log_file = log_files.get(log_type)
        if not log_file:
            return
        
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {doi} - {message}\n"
        
        try:
            with open(log_file, 'a') as f:
                f.write(log_entry)
        except Exception as e:
            logger = setup_logger(__name__)
            logger.error(f"Error writing to log file: {e}")
    
    def get_log_paths(self) -> dict:
        """Get paths to all log files."""
        return {
            'not_found': self.not_found_log,
            'processing_failed': self.processing_failed_log,
            'success': self.success_log
        }
    
    def print_summary(self, results: list):
        """
        Print a summary of processing results.
        
        Args:
            results: List of result dictionaries with 'status' key
        """
        success_count = sum(1 for r in results if r.get('status') == 'success')
        not_found_count = sum(1 for r in results if r.get('status') == 'not_found')
        processing_failed_count = sum(1 for r in results if r.get('status') == 'processing_failed')
        
        logger = setup_logger(__name__)
        logger.info(f"\n=== Processing Summary ===")
        logger.info(f"Total DOIs: {len(results)}")
        logger.info(f"Successfully processed: {success_count}")
        logger.info(f"Not found on Sci-Hub: {not_found_count}")
        logger.info(f"Downloaded but failed processing: {processing_failed_count}")
        
        if self.enabled:
            logger.info(f"\nLog files created in: {self.logs_dir}")
            logger.info(f"  - Not found: {os.path.basename(self.not_found_log)}")
            logger.info(f"  - Processing failed: {os.path.basename(self.processing_failed_log)}")
            logger.info(f"  - Success: {os.path.basename(self.success_log)}")
