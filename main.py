#!/usr/bin/env python3
"""Production-ready PDF Bank Statement Processing System.

This script processes encrypted PDF bank statements, extracts transaction data,
and generates comprehensive Excel reports with monthly summaries.

Usage:
    python main.py --pdf-file <path_to_pdf> [--password <password>] [--output-dir <dir>]
    
    python main.py --batch-dir <directory_with_pdfs> [--password-file <file>] [--output-dir <dir>]
    
    python main.py --daemon  # Run as background service
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional

from src.config.settings import REPORTS_DIR
from src.excel_generator.converter import ExcelConverter
from src.excel_generator.summarizer import MonthlySummarizer
from src.pdf_processor.extractor import PDFExtractor, PDFExtractionError
from src.utils.logger import get_logger


class BankStatementProcessor:
    """Main processor for bank statement PDF files."""
    
    def __init__(self) -> None:
        """Initialize the processor."""
        self.logger = get_logger(__name__)
        self.extractor = PDFExtractor()
        self.summarizer = MonthlySummarizer()
        self.converter = ExcelConverter()
    
    def process_single_pdf(
        self,
        pdf_path: str,
        password: Optional[str] = None,
        output_dir: Optional[str] = None
    ) -> Optional[str]:
        """Process a single PDF file and generate Excel report.
        
        Args:
            pdf_path: Path to the PDF file.
            password: Optional password for encrypted PDF.
            output_dir: Optional output directory for reports.
            
        Returns:
            Path to generated Excel file or None if processing failed.
        """
        try:
            self.logger.info(f"Processing PDF: {pdf_path}")
            
            # Validate input file
            if not os.path.exists(pdf_path):
                self.logger.error(f"PDF file not found: {pdf_path}")
                return None
            
            # Extract transactions
            transactions = self.extractor.extract_all_transactions(pdf_path, password)
            
            if not transactions:
                self.logger.warning(f"No transactions found in {pdf_path}")
                return None
            
            self.logger.info(f"Extracted {len(transactions)} transactions")
            
            # Generate summary
            summary = self.summarizer.generate_comprehensive_summary(transactions)
            
            # Create Excel report
            output_path = self.converter.create_summary_excel(
                summary_data=summary,
                transactions=transactions,
                output_path=output_dir or REPORTS_DIR
            )
            
            self.logger.info(f"Excel report created: {output_path}")
            return output_path
            
        except PDFExtractionError as e:
            self.logger.error(f"PDF extraction failed for {pdf_path}: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Processing failed for {pdf_path}: {str(e)}")
            return None
    
    def process_batch(
        self,
        batch_dir: str,
        password_file: Optional[str] = None,
        output_dir: Optional[str] = None
    ) -> List[str]:
        """Process multiple PDF files in a directory.
        
        Args:
            batch_dir: Directory containing PDF files.
            password_file: Optional file containing passwords (one per line).
            output_dir: Optional output directory for reports.
            
        Returns:
            List of paths to generated Excel files.
        """
        self.logger.info(f"Processing batch directory: {batch_dir}")
        
        # Load passwords if provided
        passwords = {}
        if password_file and os.path.exists(password_file):
            with open(password_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and '=' in line:
                        filename, pwd = line.split('=', 1)
                        passwords[filename.strip()] = pwd.strip()
        
        # Find all PDF files
        pdf_files = list(Path(batch_dir).glob("*.pdf"))
        if not pdf_files:
            self.logger.warning(f"No PDF files found in {batch_dir}")
            return []
        
        self.logger.info(f"Found {len(pdf_files)} PDF files")
        
        # Process each PDF
        successful_reports = []
        for pdf_file in pdf_files:
            password = passwords.get(pdf_file.name)
            report_path = self.process_single_pdf(
                str(pdf_file), password, output_dir
            )
            if report_path:
                successful_reports.append(report_path)
        
        self.logger.info(f"Successfully processed {len(successful_reports)}/{len(pdf_files)} files")
        return successful_reports
    
    def start_daemon(self) -> None:
        """Start the processor as a background daemon."""
        self.logger.info("Starting bank statement processor daemon")
        
        # Import here to avoid circular imports
        from src.tasks.celery_app import celery_app
        
        # Start Celery worker
        celery_app.start(['worker', '--loglevel=info'])


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.
    
    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Process PDF bank statements and generate Excel reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Process single PDF with password
    python main.py --pdf-file statement.pdf --password mypassword
    
    # Process single PDF with default output directory
    python main.py --pdf-file statement.pdf
    
    # Process multiple PDFs in directory
    python main.py --batch-dir ./statements --password-file passwords.txt
    
    # Run as background service
    python main.py --daemon
        """
    )
    
    # Create mutually exclusive group for main operations
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--pdf-file',
        type=str,
        help='Path to single PDF file to process'
    )
    group.add_argument(
        '--batch-dir',
        type=str,
        help='Directory containing multiple PDF files to process'
    )
    group.add_argument(
        '--daemon',
        action='store_true',
        help='Run as background daemon service'
    )
    
    # Optional arguments
    parser.add_argument(
        '--password',
        type=str,
        help='Password for encrypted PDF (only used with --pdf-file)'
    )
    parser.add_argument(
        '--password-file',
        type=str,
        help='File containing passwords for batch processing (format: filename=password)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help=f'Output directory for reports (default: {REPORTS_DIR})'
    )
    
    return parser.parse_args()


def main() -> int:
    """Main entry point.
    
    Returns:
        Exit code (0 for success, 1 for error).
    """
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Initialize processor
        processor = BankStatementProcessor()
        
        # Execute based on operation mode
        if args.daemon:
            processor.start_daemon()
        elif args.pdf_file:
            # Validate password argument
            if args.password is None:
                # Try to read from password.txt if no password provided
                password_file = 'password.txt'
                if os.path.exists(password_file):
                    with open(password_file, 'r', encoding='utf-8') as f:
                        args.password = f.read().strip()
            
            output_path = processor.process_single_pdf(
                pdf_path=args.pdf_file,
                password=args.password,
                output_dir=args.output_dir
            )
            
            if output_path:
                print(f"Success! Report created: {output_path}")
                return 0
            else:
                print("Error: Processing failed. Check logs for details.")
                return 1
                
        elif args.batch_dir:
            output_paths = processor.process_batch(
                batch_dir=args.batch_dir,
                password_file=args.password_file,
                output_dir=args.output_dir
            )
            
            if output_paths:
                print(f"Success! Created {len(output_paths)} reports:")
                for path in output_paths:
                    print(f"  - {path}")
                return 0
            else:
                print("Error: No files were processed successfully. Check logs for details.")
                return 1
        
        return 0
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        return 130
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())