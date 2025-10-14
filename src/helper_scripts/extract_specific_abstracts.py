#!/usr/bin/env python3
"""
Script to extract titles and abstracts for specific DOIs from papers.db
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

# DOIs for positive samples
POSITIVE_DOIS = [
    "10.1007/s10522-015-9584-x",
    "10.1007/s10522-024-10143-5",
    "10.1111/acel.70207",
    "10.1016/j.exger.2010.09.007",
    "10.3390/cells11050917",
    "10.1016/j.arr.2021.101557",
    "10.1038/s43587-022-00252-6",
    "10.1016/j.metabol.2025.156158",
    "10.1038/s41556-025-01698-7",
    "10.1073/pnas.2300624120",
    "10.1038/s43587-024-00716-x",
    "10.1038/s43587-023-00527-6",
    "10.1038/s41576-018-0004-3",
    "10.1042/CS20160897",
    "10.3389/fcell.2021.671208",
    "10.3389/fcell.2020.575645",
    "10.1016/j.mad.2021.111583",
    "10.1152/physrev.1998.78.2.547",
    "10.1093/geront/4.4.195",
    "10.1016/s0891-5849(00)00317-8",
    "10.1182/blood.2021014299",
    "10.1016/B978-0-12-394625-6.00001-5",
    "10.1093/geront/34.6.756",
    "10.4161/cc.9.16.13120",
    "10.1093/geront/29.2.183"
]

# DOIs for negative samples
NEGATIVE_DOIS = [
    "10.1016/S0140-6736(13)61489-0",
    "10.1017/S2045796017000324",
    "10.1038/s41574-020-0335-y",
    "10.1177/0022034510377791",
    "10.1038/s41586-024-08285-0",
    "10.1098/rstb.2019.0727",
    "10.1093/gerona/glae210",
    "10.1093/geront/gnad049",
    "10.1371/journal.pone.0312149",
    "10.1038/s41556-022-01062-z",
    "10.1111/1467-9566.13675",
    "10.1007/s40656-021-00402-w",
    "10.15171/apb.2019.042",
    "10.26633/RPSP.2025.83",
    "10.1055/a-1761-8481",
    "10.1007/s11904-010-0041-9",
    "10.1097/GOX.0b013e31828ed1da",
    "10.1007/s10943-009-9319-x",
    "10.1016/j.gaitpost.2014.05.062",
    "10.1093/humrep/deg377",
    "10.1684/pnv.2020.0862",
    "10.1155/2012/420637",
    "10.1038/s41398-024-03004-9",
    "10.1371/journal.pone.0233384",
    "10.1111/acel.14283"
]


def extract_abstracts_by_dois(
    db_path: str,
    dois: list,
    output_file: str
):
    """
    Extract titles and abstracts for specific DOIs and save to CSV.
    
    Args:
        db_path: Path to the SQLite database
        dois: List of DOIs to extract
        output_file: Path to the output CSV file
    """
    logger.info(f"Connecting to database: {db_path}")
    
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Prepare results
        results = []
        missing_dois = []
        
        logger.info(f"Extracting {len(dois)} DOIs...")
        
        # Query each DOI
        for doi in dois:
            cursor.execute(
                "SELECT doi, title, abstract FROM papers WHERE doi = ?",
                (doi,)
            )
            row = cursor.fetchone()
            
            if row and row[2]:  # Check if abstract exists and is not None
                results.append({
                    'doi': row[0],
                    'title': row[1],
                    'abstract': row[2]
                })
            else:
                missing_dois.append(doi)
                logger.warning(f"Abstract not found for DOI: {doi}")
        
        # Close connection
        conn.close()
        
        # Create DataFrame
        if results:
            df = pd.DataFrame(results)
            # Save to CSV
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {len(results)} abstracts to {output_file}")
            
            # Statistics
            logger.info(f"\nStatistics for {output_file}:")
            logger.info(f"  Successfully extracted: {len(results)}/{len(dois)}")
            logger.info(f"  Missing or empty: {len(missing_dois)}")
            if df['abstract'].notna().any():
                logger.info(f"  Average abstract length: {df['abstract'].str.len().mean():.0f} characters")
        else:
            logger.error(f"No abstracts found for any DOIs in {output_file}")
        
        if missing_dois:
            logger.warning(f"\nMissing DOIs ({len(missing_dois)}):")
            for doi in missing_dois:
                logger.warning(f"  - {doi}")
        
        return len(results), missing_dois
        
    except Exception as e:
        logger.error(f"Error extracting abstracts: {e}")
        raise


def main():
    """Main entry point."""
    db_path = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'
    
    logger.info("="*60)
    logger.info("EXTRACTING POSITIVE SAMPLES")
    logger.info("="*60)
    pos_count, pos_missing = extract_abstracts_by_dois(
        db_path=db_path,
        dois=POSITIVE_DOIS,
        output_file='input_positive.csv'
    )
    
    logger.info("\n" + "="*60)
    logger.info("EXTRACTING NEGATIVE SAMPLES")
    logger.info("="*60)
    neg_count, neg_missing = extract_abstracts_by_dois(
        db_path=db_path,
        dois=NEGATIVE_DOIS,
        output_file='input_negatives.csv'
    )
    
    logger.info("\n" + "="*60)
    logger.info("SUMMARY")
    logger.info("="*60)
    logger.info(f"Positive samples: {pos_count}/{len(POSITIVE_DOIS)} extracted")
    logger.info(f"Negative samples: {neg_count}/{len(NEGATIVE_DOIS)} extracted")
    logger.info(f"Total extracted: {pos_count + neg_count}/{len(POSITIVE_DOIS) + len(NEGATIVE_DOIS)}")


if __name__ == '__main__':
    main()
