#!/usr/bin/env python3
"""
Script to create a dataset of 30 random abstracts from papers.db
"""

import sqlite3
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_abstract_dataset(
    db_path: str = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db',
    output_file: str = 'input_abstracts.csv',
    n_samples: int = 30
):
    """
    Extract n random abstracts from the database and save to CSV.
    
    Args:
        db_path: Path to the SQLite database
        output_file: Path to the output CSV file
        n_samples: Number of abstracts to extract
    """
    logger.info(f"Connecting to database: {db_path}")
    
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        
        # Query for random abstracts (only non-null, non-empty)
        query = """
        SELECT abstract
        FROM papers
        WHERE abstract IS NOT NULL 
        AND abstract != ''
        AND TRIM(abstract) != ''
        ORDER BY RANDOM()
        LIMIT ?
        """
        
        logger.info(f"Fetching {n_samples} random abstracts...")
        df = pd.read_sql_query(query, conn, params=(n_samples,))
        
        # Close connection
        conn.close()
        
        # Check if we got enough abstracts
        if len(df) == 0:
            logger.error("No abstracts found in the database!")
            return
        
        if len(df) < n_samples:
            logger.warning(f"Only found {len(df)} abstracts (requested {n_samples})")
        else:
            logger.info(f"Successfully fetched {len(df)} abstracts")
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        logger.info(f"Saved abstracts to {output_file}")
        
        # Print sample statistics
        logger.info(f"\nDataset Statistics:")
        logger.info(f"Number of abstracts: {len(df)}")
        logger.info(f"Average abstract length: {df['abstract'].str.len().mean():.0f} characters")
        logger.info(f"Min abstract length: {df['abstract'].str.len().min()} characters")
        logger.info(f"Max abstract length: {df['abstract'].str.len().max()} characters")
        
    except Exception as e:
        logger.error(f"Error creating abstract dataset: {e}")
        raise


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Create a dataset of random abstracts from papers.db'
    )
    parser.add_argument(
        '--db',
        default='/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db',
        help='Path to papers.db database'
    )
    parser.add_argument(
        '--output',
        default='input_abstracts.csv',
        help='Output CSV file path'
    )
    parser.add_argument(
        '--n-samples',
        type=int,
        default=30,
        help='Number of abstracts to extract'
    )
    
    args = parser.parse_args()
    
    create_abstract_dataset(
        db_path=args.db,
        output_file=args.output,
        n_samples=args.n_samples
    )
