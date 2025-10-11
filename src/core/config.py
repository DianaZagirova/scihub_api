"""Configuration management for the Sci-Hub downloader."""

import os
import json
import json5
from typing import Dict, Optional
from pathlib import Path


class ConfigManager:
    """Manages configuration loading and access."""
    
    DEFAULT_CONFIG = {
        'grobid_server': 'http://localhost:8070',
        'batch_size': 1000,
        'timeout': 180,
        'sleep_time': 5,
        'coordinates': [
            'title', 'persName', 'affiliation', 'orgName',
            'formula', 'figure', 'ref', 'biblStruct',
            'head', 'p', 's', 'note'
        ],
        'logging': {
            'level': 'INFO',
            'format': '%(asctime)s - %(levelname)s - %(message)s',
            'console': True,
            'file': None,
            'max_file_size': '10MB',
            'backup_count': 3
        }
    }
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path or os.path.join(os.getcwd(), 'config.json')
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """
        Load configuration from file.
        
        Returns:
            Configuration dictionary
        """
        if not os.path.exists(self.config_path):
            return self.DEFAULT_CONFIG.copy()
        
        try:
            with open(self.config_path, 'r') as f:
                config = json5.load(f)
            return config
        except Exception as e:
            from .logger import setup_logger
            logger = setup_logger(__name__)
            logger.warning(f"Error loading configuration: {e}")
            logger.info("Using default configuration")
            return self.DEFAULT_CONFIG.copy()
    
    def get(self, key: str, default=None):
        """Get configuration value by key."""
        return self.config.get(key, default)
    
    def get_grobid_server(self) -> str:
        """Get GROBID server URL."""
        return self.config.get('grobid_server', 'http://localhost:8070')
    
    def get_timeout(self) -> int:
        """Get request timeout in seconds."""
        return self.config.get('timeout', 180)
    
    def get_sleep_time(self) -> int:
        """Get sleep time between requests in seconds."""
        return self.config.get('sleep_time', 5)
    
    def save(self, config_path: Optional[str] = None):
        """
        Save current configuration to file.
        
        Args:
            config_path: Path to save configuration (uses default if None)
        """
        path = config_path or self.config_path
        
        try:
            with open(path, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            from .logger import setup_logger
            logger = setup_logger(__name__)
            logger.error(f"Error saving configuration: {e}")
