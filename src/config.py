#!/usr/bin/env python3
"""
Configuration module for Sci-Hub API
Handles environment variables and credentials
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration class for managing environment variables and credentials."""
    
    # Unpaywall API Configuration
    UNPAYWALL_EMAIL = os.getenv('UNPAYWALL_EMAIL', 'diana.zagirova@skoltech.ru')
    
    # NCBI E-utilities Configuration
    NCBI_API_KEY = os.getenv('NCBI_API_KEY')
    NCBI_EMAIL = os.getenv('NCBI_EMAIL', 'diana.zagirova@skoltech.ru')
    
    # Sci-Hub Configuration
    SCIHUB_PROXY_URL = os.getenv('SCIHUB_PROXY_URL')
    SCIHUB_USER_AGENT = os.getenv('SCIHUB_USER_AGENT', 'SciHub-API/1.0')
    
    # Database Configuration
    TEST_DB_PATH = os.getenv('TEST_DB_PATH', '/home/diana.z/hack/download_papers_pubmed/paper_collection_test/data/papers.db')
    
    @classmethod
    def validate_required_credentials(cls):
        """Validate that required credentials are present."""
        missing = []
        
        if not cls.UNPAYWALL_EMAIL:
            missing.append('UNPAYWALL_EMAIL')
        
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    @classmethod
    def get_ncbi_headers(cls):
        """Get headers for NCBI E-utilities API requests."""
        headers = {}
        
        if cls.NCBI_API_KEY:
            headers['api_key'] = cls.NCBI_API_KEY
        
        if cls.NCBI_EMAIL:
            headers['email'] = cls.NCBI_EMAIL
            
        return headers
    
    @classmethod
    def get_ncbi_params(cls, base_params=None):
        """Get parameters for NCBI E-utilities API requests."""
        params = base_params or {}
        
        if cls.NCBI_API_KEY:
            params['api_key'] = cls.NCBI_API_KEY
        
        if cls.NCBI_EMAIL:
            params['email'] = cls.NCBI_EMAIL
            
        return params
