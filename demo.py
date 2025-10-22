#!/usr/bin/env python3
"""
Sci-Hub API Demo Script
=======================

This demo script showcases the advanced functionality of the Sci-Hub API project
for aging research paper collection and processing. It demonstrates:

1. Multi-source paper retrieval (Sci-Hub, Unpaywall, PubMed)
2. Advanced PDF parsing with GROBID and PyMuPDF
3. Parallel processing capabilities
4. Comprehensive tracking and monitoring
5. Integration with test database

Usage:
    python demo.py

Requirements:
    - Virtual environment activated
    - Test database at /home/diana.z/hack/download_papers_pubmed/paper_collection_test/
"""

import os
import sys
import json
import time
import logging
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from scihub_fast_downloader import SciHubFastDownloader
from scihub_grobid_downloader import SciHubGrobidDownloader
from unpaywall_downloader import UnpaywallDownloader
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class SciHubAPIDemo:
    """Demo class showcasing Sci-Hub API capabilities."""
    
    def __init__(self, test_db_path=None):
        """
        Initialize the demo with test database path.
        
        Args:
            test_db_path (str, optional): Path to the test database. If None, uses default path.
        """
        # Default test database path - can be overridden
        self.test_db_path = test_db_path or "/home/diana.z/hack/download_papers_pubmed/paper_collection_test/data/papers.db"
        self.demo_output_dir = "./demo_output"
        self.demo_papers_dir = "./demo_papers"
        
        # Create demo directories
        os.makedirs(self.demo_output_dir, exist_ok=True)
        os.makedirs(self.demo_papers_dir, exist_ok=True)
        
        # Sample DOIs for demonstration (aging-related papers)
        self.demo_dois = [
            "10.1038/s41586-019-1750-x",  # Nature paper
            "10.1126/science.aau2582",    # Science paper
            "10.1016/j.cell.2019.05.031", # Cell paper
            "10.1038/nature12373",        # Nature aging paper
            "10.1016/j.cell.2016.11.052"  # Cell aging paper
        ]
        
        logger.info("🚀 Sci-Hub API Demo Initialized")
        logger.info(f"📁 Demo output directory: {self.demo_output_dir}")
        logger.info(f"📁 Demo papers directory: {self.demo_papers_dir}")
    
    def check_test_database(self) -> Dict[str, Any]:
        """Check and analyze the test database."""
        logger.info("🔍 Analyzing test database...")
        
        if not os.path.exists(self.test_db_path):
            logger.warning(f"⚠️  Test database not found at {self.test_db_path}")
            return {"status": "not_found", "count": 0}
        
        try:
            conn = sqlite3.connect(self.test_db_path)
            cursor = conn.cursor()
            
            # Get table info
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Count papers
            if 'papers' in tables:
                cursor.execute("SELECT COUNT(*) FROM papers")
                paper_count = cursor.fetchone()[0]
                
                # Get sample papers
                cursor.execute("SELECT doi, title, abstract FROM papers LIMIT 5")
                sample_papers = cursor.fetchall()
                
                conn.close()
                
                logger.info(f"✅ Test database found with {paper_count} papers")
                logger.info(f"📊 Available tables: {', '.join(tables)}")
                
                return {
                    "status": "found",
                    "count": paper_count,
                    "tables": tables,
                    "sample_papers": sample_papers
                }
            else:
                conn.close()
                logger.warning("⚠️  No 'papers' table found in test database")
                return {"status": "no_papers_table", "count": 0}
                
        except Exception as e:
            logger.error(f"❌ Error accessing test database: {e}")
            return {"status": "error", "error": str(e), "count": 0}
    
    def demonstrate_fast_parser(self) -> Dict[str, Any]:
        """Demonstrate fast PDF parser capabilities."""
        logger.info("⚡ Demonstrating Fast PDF Parser...")
        
        try:
            # Initialize fast downloader
            downloader = SciHubFastDownloader(
                output_dir=self.demo_papers_dir,
                parse_mode='structured'
            )
            
            results = []
            for i, doi in enumerate(self.demo_dois[:2], 1):  # Test first 2 DOIs
                logger.info(f"📄 Processing {i}/2: {doi}")
                
                start_time = time.time()
                result = downloader.download_and_process(doi)
                processing_time = time.time() - start_time
                
                if result:
                    # Handle both dict and tuple results
                    if isinstance(result, dict):
                        pdf_path = result.get("pdf_path")
                        json_path = result.get("json_path")
                    else:
                        # If result is a tuple, assume it's (pdf_path, extracted_data, status)
                        if len(result) >= 3:
                            pdf_path, extracted_data, status = result
                            # Find the JSON file path based on PDF path
                            if pdf_path:
                                pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
                                json_path = os.path.join(self.demo_output_dir, f"{pdf_name}_fast.json")
                            else:
                                json_path = None
                        else:
                            pdf_path, json_path = result if len(result) >= 2 else (result[0], None)
                    
                    results.append({
                        "doi": doi,
                        "status": "success",
                        "processing_time": processing_time,
                        "pdf_path": pdf_path,
                        "json_path": json_path
                    })
                    logger.info(f"✅ Success: {doi} ({processing_time:.2f}s)")
                else:
                    results.append({
                        "doi": doi,
                        "status": "failed",
                        "processing_time": processing_time
                    })
                    logger.warning(f"❌ Failed: {doi}")
            
            success_count = sum(1 for r in results if r["status"] == "success")
            avg_time = sum(r["processing_time"] for r in results) / len(results)
            
            logger.info(f"📊 Fast Parser Results: {success_count}/{len(results)} successful")
            logger.info(f"⏱️  Average processing time: {avg_time:.2f}s per paper")
            
            return {
                "parser": "fast",
                "results": results,
                "success_count": success_count,
                "total_count": len(results),
                "average_time": avg_time
            }
            
        except Exception as e:
            logger.error(f"❌ Fast parser demo failed: {e}")
            return {"parser": "fast", "error": str(e)}
    
    def demonstrate_advanced_fast_parser(self) -> Dict[str, Any]:
        """Demonstrate advanced fast parser capabilities with different modes."""
        logger.info("⚡ Demonstrating Advanced Fast Parser Modes...")
        
        try:
            results = []
            modes = ['simple', 'structured', 'full']
            
            for mode in modes:
                logger.info(f"🔧 Testing {mode} mode...")
                
                # Initialize fast downloader with specific mode
                downloader = SciHubFastDownloader(
                    output_dir=self.demo_papers_dir,
                    parse_mode=mode
                )
                
                mode_results = []
                for i, doi in enumerate(self.demo_dois[:1], 1):  # Test 1 DOI per mode
                    logger.info(f"📄 Processing {mode} mode {i}/1: {doi}")
                    
                    start_time = time.time()
                    result = downloader.download_and_process(doi)
                    processing_time = time.time() - start_time
                    
                    if result:
                        # Handle both dict and tuple results
                        if isinstance(result, dict):
                            pdf_path = result.get("pdf_path")
                            json_path = result.get("json_path")
                        else:
                            # If result is a tuple, assume it's (pdf_path, extracted_data, status)
                            if len(result) >= 3:
                                pdf_path, extracted_data, status = result
                                # Find the JSON file path based on PDF path
                                if pdf_path:
                                    pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
                                    json_path = os.path.join(self.demo_output_dir, f"{pdf_name}_fast.json")
                                else:
                                    json_path = None
                            else:
                                pdf_path, json_path = result if len(result) >= 2 else (result[0], None)
                        
                        mode_results.append({
                            "doi": doi,
                            "status": "success",
                            "processing_time": processing_time,
                            "pdf_path": pdf_path,
                            "json_path": json_path,
                            "mode": mode
                        })
                        logger.info(f"✅ Success ({mode}): {doi} ({processing_time:.2f}s)")
                    else:
                        mode_results.append({
                            "doi": doi,
                            "status": "failed",
                            "processing_time": processing_time,
                            "mode": mode
                        })
                        logger.warning(f"❌ Failed ({mode}): {doi}")
                
                results.extend(mode_results)
            
            success_count = sum(1 for r in results if r["status"] == "success")
            avg_time = sum(r["processing_time"] for r in results) / len(results) if results else 0
            
            logger.info(f"📊 Advanced Fast Parser Results: {success_count}/{len(results)} successful")
            logger.info(f"⏱️  Average processing time: {avg_time:.2f}s per paper")
            
            return {
                "parser": "advanced_fast",
                "modes_tested": modes,
                "results": results,
                "success_count": success_count,
                "total_count": len(results),
                "average_time": avg_time
            }
            
        except Exception as e:
            logger.error(f"❌ Advanced fast parser demo failed: {e}")
            return {"parser": "advanced_fast", "error": str(e)}
    
    def demonstrate_unpaywall(self) -> Dict[str, Any]:
        """Demonstrate Unpaywall Open Access downloader."""
        logger.info("🌐 Demonstrating Unpaywall Open Access Downloader...")
        
        try:
            # Initialize Unpaywall downloader
            downloader = UnpaywallDownloader(
                email=Config.UNPAYWALL_EMAIL,
                output_dir=self.demo_papers_dir
            )
            
            results = []
            for i, doi in enumerate(self.demo_dois[:2], 1):  # Test first 2 DOIs
                logger.info(f"📄 Checking OA status {i}/2: {doi}")
                
                start_time = time.time()
                
                # Check OA status
                metadata = downloader.get_doi_metadata(doi)
                if metadata and metadata.get('is_oa'):
                    # Try to download
                    pdf_path = downloader.download_pdf(doi)
                    processing_time = time.time() - start_time
                    
                    if pdf_path:
                        results.append({
                            "doi": doi,
                            "status": "success",
                            "oa_status": metadata.get('oa_status'),
                            "processing_time": processing_time,
                            "pdf_path": pdf_path
                        })
                        logger.info(f"✅ OA Download Success: {doi} ({metadata.get('oa_status')})")
                    else:
                        results.append({
                            "doi": doi,
                            "status": "download_failed",
                            "oa_status": metadata.get('oa_status'),
                            "processing_time": processing_time
                        })
                        logger.warning(f"⚠️  OA available but download failed: {doi}")
                else:
                    processing_time = time.time() - start_time
                    results.append({
                        "doi": doi,
                        "status": "not_oa",
                        "oa_status": metadata.get('oa_status') if metadata else "unknown",
                        "processing_time": processing_time
                    })
                    logger.info(f"ℹ️  Not Open Access: {doi}")
            
            success_count = sum(1 for r in results if r["status"] == "success")
            oa_count = sum(1 for r in results if r.get("oa_status") in ["gold", "green", "hybrid", "bronze"])
            
            logger.info(f"📊 Unpaywall Results: {success_count}/{len(results)} downloaded")
            logger.info(f"🌐 Open Access papers found: {oa_count}/{len(results)}")
            
            return {
                "source": "unpaywall",
                "results": results,
                "success_count": success_count,
                "oa_count": oa_count,
                "total_count": len(results)
            }
            
        except Exception as e:
            logger.error(f"❌ Unpaywall demo failed: {e}")
            return {"source": "unpaywall", "error": str(e)}
    
    def demonstrate_parallel_processing(self) -> Dict[str, Any]:
        """Demonstrate parallel processing capabilities."""
        logger.info("🚀 Demonstrating Parallel Processing...")
        
        try:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import threading
            
            # Thread-safe results collection
            results = []
            results_lock = threading.Lock()
            
            def process_doi(doi):
                """Process a single DOI."""
                try:
                    downloader = SciHubFastDownloader(
                        output_dir=self.demo_papers_dir,
                        parse_mode='simple'  # Use simple mode for speed
                    )
                    
                    start_time = time.time()
                    result = downloader.download_and_process(doi)
                    processing_time = time.time() - start_time
                    
                    with results_lock:
                        results.append({
                            "doi": doi,
                            "status": "success" if result else "failed",
                            "processing_time": processing_time
                        })
                    
                    return result
                except Exception as e:
                    with results_lock:
                        results.append({
                            "doi": doi,
                            "status": "error",
                            "error": str(e),
                            "processing_time": 0
                        })
                    return None
            
            # Process DOIs in parallel
            start_time = time.time()
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_doi = {
                    executor.submit(process_doi, doi): doi 
                    for doi in self.demo_dois[:3]  # Test 3 DOIs in parallel
                }
                
                for future in as_completed(future_to_doi):
                    doi = future_to_doi[future]
                    try:
                        future.result()
                        logger.info(f"✅ Completed parallel processing: {doi}")
                    except Exception as e:
                        logger.error(f"❌ Parallel processing failed for {doi}: {e}")
            
            total_time = time.time() - start_time
            success_count = sum(1 for r in results if r["status"] == "success")
            avg_time = sum(r["processing_time"] for r in results) / len(results)
            
            logger.info(f"📊 Parallel Processing Results: {success_count}/{len(results)} successful")
            logger.info(f"⏱️  Total time: {total_time:.2f}s, Average per paper: {avg_time:.2f}s")
            
            return {
                "mode": "parallel",
                "workers": 3,
                "results": results,
                "success_count": success_count,
                "total_count": len(results),
                "total_time": total_time,
                "average_time": avg_time
            }
            
        except Exception as e:
            logger.error(f"❌ Parallel processing demo failed: {e}")
            return {"mode": "parallel", "error": str(e)}
    
    def generate_demo_report(self, all_results: Dict[str, Any]) -> str:
        """Generate a comprehensive demo report."""
        report_path = os.path.join(self.demo_output_dir, f"demo_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        # Create comprehensive report
        report = {
            "demo_info": {
                "timestamp": datetime.now().isoformat(),
                "demo_version": "1.0",
                "test_database_path": self.test_db_path,
                "demo_dois": self.demo_dois
            },
            "test_database": all_results.get("test_database", {}),
            "parsers": {
                "fast_parser": all_results.get("fast_parser", {}),
                "grobid_parser": all_results.get("grobid_parser", {})
            },
            "sources": {
                "unpaywall": all_results.get("unpaywall", {})
            },
            "processing": {
                "parallel": all_results.get("parallel", {})
            },
            "summary": {
                "total_demos": len([k for k in all_results.keys() if k != "test_database"]),
                "successful_demos": len([k for k, v in all_results.items() 
                                       if k != "test_database" and v.get("success_count", 0) > 0])
            }
        }
        
        # Save report
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📋 Demo report saved: {report_path}")
        return report_path
    
    def run_demo(self):
        """Run the complete demo."""
        logger.info("🎯 Starting Sci-Hub API Comprehensive Demo")
        logger.info("=" * 60)
        
        all_results = {}
        
        # 1. Check test database
        logger.info("\n1️⃣  TEST DATABASE ANALYSIS")
        logger.info("-" * 30)
        all_results["test_database"] = self.check_test_database()
        
        # 2. Demonstrate Fast Parser
        logger.info("\n2️⃣  FAST PDF PARSER DEMONSTRATION")
        logger.info("-" * 30)
        all_results["fast_parser"] = self.demonstrate_fast_parser()
        
        # 3. Demonstrate Advanced Fast Parser
        logger.info("\n3️⃣  ADVANCED FAST PARSER DEMONSTRATION")
        logger.info("-" * 30)
        all_results["advanced_fast_parser"] = self.demonstrate_advanced_fast_parser()
        
        # 4. Demonstrate Unpaywall
        logger.info("\n4️⃣  UNPAYWALL OPEN ACCESS DEMONSTRATION")
        logger.info("-" * 30)
        all_results["unpaywall"] = self.demonstrate_unpaywall()
        
        # 5. Demonstrate Parallel Processing
        logger.info("\n5️⃣  PARALLEL PROCESSING DEMONSTRATION")
        logger.info("-" * 30)
        all_results["parallel"] = self.demonstrate_parallel_processing()
        
        # 6. Generate comprehensive report
        logger.info("\n6️⃣  GENERATING DEMO REPORT")
        logger.info("-" * 30)
        report_path = self.generate_demo_report(all_results)
        
        # 7. Print summary
        logger.info("\n🎉 DEMO COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"📁 Demo files saved to: {self.demo_output_dir}")
        logger.info(f"📄 Demo report: {report_path}")
        logger.info(f"📚 Papers downloaded to: {self.demo_papers_dir}")
        
        # Print quick stats
        fast_success = all_results.get("fast_parser", {}).get("success_count", 0)
        advanced_fast_success = all_results.get("advanced_fast_parser", {}).get("success_count", 0)
        unpaywall_success = all_results.get("unpaywall", {}).get("success_count", 0)
        parallel_success = all_results.get("parallel", {}).get("success_count", 0)
        
        logger.info(f"\n📊 QUICK STATS:")
        logger.info(f"   Fast Parser: {fast_success} papers processed")
        logger.info(f"   Advanced Fast Parser: {advanced_fast_success} papers processed")
        logger.info(f"   Unpaywall OA: {unpaywall_success} papers downloaded")
        logger.info(f"   Parallel Processing: {parallel_success} papers processed")
        
        return all_results

def main():
    """Main demo function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Sci-Hub API Demo for Aging Research',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default test database path
  python demo.py
  
  # Run with custom test database path
  python demo.py --test-db /path/to/your/test_database.db
  
  # Run with test database from Stage 1 (from running demo.py from https://github.com/DianaZagirova/download_agent)
  python demo.py --test-db /path/to/download_agent/paper_collection_test/data/papers.db
        """
    )
    
    parser.add_argument(
        '--test-db', 
        type=str, 
        default=None,
        help='Path to the test database (default: /home/diana.z/hack/download_papers_pubmed/paper_collection_test/data/papers.db)'
    )
    
    args = parser.parse_args()
    
    print("🧬 Sci-Hub API Demo for Aging Research")
    print("=====================================")
    print("This demo showcases advanced paper collection and processing capabilities")
    print("for aging research and theory identification.")
    print()
    
    # Check if running in virtual environment
    if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("⚠️  WARNING: Not running in virtual environment!")
        print("   For reproducibility, please activate your virtual environment first:")
        print("   source venv/bin/activate")
        print()
    
    try:
        demo = SciHubAPIDemo(test_db_path=args.test_db)
        results = demo.run_demo()
        
        print("\n✅ Demo completed successfully!")
        print("   Check the demo_output/ directory for detailed results.")
        
    except KeyboardInterrupt:
        print("\n⏹️  Demo interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        logger.exception("Demo failed with exception:")
        sys.exit(1)

if __name__ == "__main__":
    main()
