"""Celery application and task definitions for PDF processing."""

import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any

from celery import Celery
from celery.exceptions import Retry

from src.config.settings import (
    CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND,
    CELERY_TASK_SERIALIZER,
    CELERY_RESULT_SERIALIZER,
    CELERY_ACCEPT_CONTENT,
    CELERY_TIMEZONE,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
    ensure_directories,
)
from src.excel_generator.converter import ExcelConverter, ExcelConversionError
from src.excel_generator.summarizer import MonthlySummarizer, SummaryCalculationError
from src.pdf_processor.chunker import PDFChunker, PDFChunkingError
from src.pdf_processor.decryptor import PDFDecryptor, PDFDecryptionError
from src.pdf_processor.extractor import PDFExtractor, PDFExtractionError
from src.utils.logger import setup_logger, ProcessingLogger
from src.utils.validators import ValidationError, validate_pdf_file

# Ensure required directories exist
ensure_directories()

# Initialize Celery app
celery_app = Celery(
    "pdf_processor",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    task_serializer=CELERY_TASK_SERIALIZER,
    result_serializer=CELERY_RESULT_SERIALIZER,
    accept_content=CELERY_ACCEPT_CONTENT,
    timezone=CELERY_TIMEZONE,
)

# Celery configuration
celery_app.conf.update(
    task_routes={
        "src.tasks.celery_app.process_pdf_statement": {"queue": "pdf_processing"},
        "src.tasks.celery_app.process_pdf_chunk": {"queue": "chunk_processing"},
        "src.tasks.celery_app.generate_excel_report": {"queue": "report_generation"},
    },
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=1000,
)

# Setup logger
logger = setup_logger("celery_tasks")


@celery_app.task(bind=True, name="process_pdf_statement")
def process_pdf_statement(
    self,
    pdf_path: str,
    password: Optional[str] = None,
    generate_summary: bool = True,
    output_filename: Optional[str] = None
) -> Dict[str, Any]:
    """Process PDF bank statement and generate Excel report.
    
    Args:
        self: Celery task instance.
        pdf_path: Path to PDF file.
        password: Optional PDF password.
        generate_summary: Whether to generate monthly summary.
        output_filename: Optional output filename.
        
    Returns:
        Dictionary with processing results.
    """
    task_id = self.request.id
    processing_logger = ProcessingLogger(task_id)
    
    try:
        # Validate input
        validate_pdf_file(pdf_path)
        processing_logger.log_start(pdf_path)
        
        # Initialize components
        decryptor = PDFDecryptor()
        chunker = PDFChunker()
        extractor = PDFExtractor()
        excel_converter = ExcelConverter()
        summarizer = MonthlySummarizer()
        
        # Decrypt PDF
        processing_logger.log_progress("Decrypting PDF...")
        reader = decryptor.decrypt_pdf(pdf_path, password)
        
        if not decryptor.verify_decryption(reader):
            raise PDFDecryptionError("PDF decryption verification failed")
        
        # Get PDF info
        pdf_info = decryptor.get_pdf_info(reader)
        processing_logger.log_progress(f"PDF loaded: {pdf_info['page_count']} pages")
        
        # Check if chunking is needed
        chunking_strategy = chunker.get_chunking_strategy(pdf_path, reader)
        
        all_transactions = []
        
        if chunking_strategy["should_chunk"]:
            # Process in chunks
            processing_logger.log_progress(f"Processing {chunking_strategy['chunk_count']} chunks...")
            
            chunk_tasks = []
            for start_page, end_page in chunking_strategy["chunk_ranges"]:
                chunk_task = process_pdf_chunk.delay(
                    pdf_path, start_page, end_page, password, task_id
                )
                chunk_tasks.append(chunk_task)
            
            # Collect results from chunk tasks
            for chunk_task in chunk_tasks:
                try:
                    chunk_result = chunk_task.get(timeout=300)  # 5 minutes timeout
                    if chunk_result["success"]:
                        all_transactions.extend(chunk_result["transactions"])
                    else:
                        processing_logger.log_error(
                            Exception(chunk_result["error"]),
                            f"Chunk processing failed for pages {chunk_result.get('start_page')}-{chunk_result.get('end_page')}"
                        )
                except Exception as e:
                    processing_logger.log_error(e, "Failed to get chunk result")
                    raise
        
        else:
            # Process entire PDF at once
            processing_logger.log_progress("Extracting transactions from PDF...")
            all_transactions = extractor.extract_all_transactions(pdf_path)
        
        if not all_transactions:
            raise PDFExtractionError("No transactions extracted from PDF")
        
        processing_logger.log_progress(f"Extracted {len(all_transactions)} transactions")
        
        # Generate Excel report
        processing_logger.log_progress("Generating Excel report...")
        
        metadata = {
            "source_file": os.path.basename(pdf_path),
            "processing_date": datetime.now().isoformat(),
            "task_id": task_id,
            "total_transactions": len(all_transactions),
            "pdf_info": pdf_info,
            "chunking_strategy": chunking_strategy,
        }
        
        if generate_summary:
            # Generate comprehensive summary
            processing_logger.log_progress("Generating monthly summary...")
            summary_data = summarizer.generate_comprehensive_summary(all_transactions)
            
            output_path = excel_converter.create_summary_excel(
                summary_data=summary_data,
                transactions=all_transactions,
                filename=output_filename
            )
            
            result = {
                "success": True,
                "output_path": output_path,
                "summary": summary_data,
                "metadata": metadata,
            }
        else:
            # Generate transactions-only Excel
            output_path = excel_converter.convert_to_excel(
                transactions=all_transactions,
                filename=output_filename,
                metadata=metadata
            )
            
            result = {
                "success": True,
                "output_path": output_path,
                "transaction_count": len(all_transactions),
                "metadata": metadata,
            }
        
        processing_logger.log_completion(output_path)
        return result
        
    except (ValidationError, PDFDecryptionError, PDFExtractionError, 
            PDFChunkingError, ExcelConversionError, SummaryCalculationError) as e:
        processing_logger.log_error(e, "PDF processing failed")
        
        # Retry on transient errors
        if self.request.retries < MAX_RETRIES:
            processing_logger.log_progress(f"Retrying task (attempt {self.request.retries + 1}/{MAX_RETRIES})")
            raise self.retry(countdown=RETRY_DELAY_SECONDS, exc=e)
        
        return {
            "success": False,
            "error": str(e),
            "task_id": task_id,
            "retries": self.request.retries,
        }
    
    except Exception as e:
        processing_logger.log_error(e, "Unexpected error during PDF processing")
        
        if self.request.retries < MAX_RETRIES:
            raise self.retry(countdown=RETRY_DELAY_SECONDS, exc=e)
        
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}",
            "task_id": task_id,
            "retries": self.request.retries,
        }


@celery_app.task(bind=True, name="process_pdf_chunk")
def process_pdf_chunk(
    self,
    pdf_path: str,
    start_page: int,
    end_page: int,
    password: Optional[str] = None,
    parent_task_id: Optional[str] = None
) -> Dict[str, Any]:
    """Process a chunk of PDF pages.
    
    Args:
        self: Celery task instance.
        pdf_path: Path to PDF file.
        start_page: Starting page number (1-based).
        end_page: Ending page number (1-based).
        password: Optional PDF password.
        parent_task_id: ID of parent task for logging.
        
    Returns:
        Dictionary with chunk processing results.
    """
    task_id = self.request.id
    task_logger = ProcessingLogger(task_id)
    
    try:
        # Initialize components
        decryptor = PDFDecryptor()
        chunker = PDFChunker()
        extractor = PDFExtractor()
        
        # Decrypt PDF
        reader = decryptor.decrypt_pdf(pdf_path, password)
        
        # Create chunk
        chunk_path = chunker.create_chunk(reader, start_page, end_page)
        
        try:
            # Extract transactions from chunk
            transactions = extractor.extract_all_transactions(chunk_path)
            
            result = {
                "success": True,
                "transactions": [t.to_dict() for t in transactions],
                "start_page": start_page,
                "end_page": end_page,
                "transaction_count": len(transactions),
                "task_id": task_id,
                "parent_task_id": parent_task_id,
            }
            
            task_logger.log_progress(f"Extracted {len(transactions)} transactions from pages {start_page}-{end_page}")
            return result
            
        finally:
            # Cleanup temporary chunk file
            try:
                os.remove(chunk_path)
            except OSError:
                pass
        
    except Exception as e:
        task_logger.log_error(e, f"Chunk processing failed for pages {start_page}-{end_page}")
        
        if self.request.retries < MAX_RETRIES:
            raise self.retry(countdown=RETRY_DELAY_SECONDS, exc=e)
        
        return {
            "success": False,
            "error": str(e),
            "start_page": start_page,
            "end_page": end_page,
            "task_id": task_id,
            "parent_task_id": parent_task_id,
            "retries": self.request.retries,
        }


@celery_app.task(bind=True, name="generate_excel_report")
def generate_excel_report(
    self,
    transactions: List[Dict[str, Any]],
    summary_data: Optional[Dict[str, Any]] = None,
    output_filename: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Generate Excel report from transaction data.
    
    Args:
        self: Celery task instance.
        transactions: List of transaction dictionaries.
        summary_data: Optional summary data.
        output_filename: Optional output filename.
        metadata: Optional metadata.
        
    Returns:
        Dictionary with report generation results.
    """
    task_id = self.request.id
    task_logger = ProcessingLogger(task_id)
    
    try:
        # Convert dictionaries back to TransactionData objects
        from src.pdf_processor.extractor import TransactionData
        from decimal import Decimal
        
        transaction_objects = []
        for tx_dict in transactions:
            transaction = TransactionData(
                date=tx_dict["Date"],
                description=tx_dict["Description"],
                debit=Decimal(str(tx_dict["Debit"])) if tx_dict["Debit"] else None,
                credit=Decimal(str(tx_dict["Credit"])) if tx_dict["Credit"] else None,
                balance=Decimal(str(tx_dict["Balance"])) if tx_dict["Balance"] else None,
                reference=tx_dict.get("Reference"),
            )
            transaction_objects.append(transaction)
        
        # Initialize Excel converter
        excel_converter = ExcelConverter()
        
        # Generate Excel report
        if summary_data:
            output_path = excel_converter.create_summary_excel(
                summary_data=summary_data,
                transactions=transaction_objects,
                filename=output_filename
            )
        else:
            output_path = excel_converter.convert_to_excel(
                transactions=transaction_objects,
                filename=output_filename,
                metadata=metadata
            )
        
        result = {
            "success": True,
            "output_path": output_path,
            "transaction_count": len(transaction_objects),
            "task_id": task_id,
        }
        
        task_logger.log_completion(output_path)
        return result
        
    except Exception as e:
        task_logger.log_error(e, "Excel report generation failed")
        
        if self.request.retries < MAX_RETRIES:
            raise self.retry(countdown=RETRY_DELAY_SECONDS, exc=e)
        
        return {
            "success": False,
            "error": str(e),
            "task_id": task_id,
            "retries": self.request.retries,
        }


@celery_app.task(name="cleanup_temp_files")
def cleanup_temp_files() -> Dict[str, Any]:
    """Clean up temporary files.
    
    Returns:
        Dictionary with cleanup results.
    """
    try:
        import tempfile
        import glob
        
        temp_dir = tempfile.gettempdir()
        pattern = os.path.join(temp_dir, "pdf_chunk_*.pdf")
        temp_files = glob.glob(pattern)
        
        cleaned_count = 0
        for temp_file in temp_files:
            try:
                os.remove(temp_file)
                cleaned_count += 1
            except OSError:
                pass
        
        logger.info(f"Cleaned up {cleaned_count} temporary files")
        
        return {
            "success": True,
            "cleaned_files": cleaned_count,
            "temp_directory": temp_dir,
        }
        
    except Exception as e:
        logger.error(f"Failed to cleanup temp files: {str(e)}")
        return {
            "success": False,
            "error": str(e),
        }


# Periodic task configuration
celery_app.conf.beat_schedule = {
    "cleanup-temp-files": {
        "task": "cleanup_temp_files",
        "schedule": 3600.0,  # Run every hour
    },
}


def get_task_status(task_id: str) -> Dict[str, Any]:
    """Get status of a Celery task.
    
    Args:
        task_id: ID of the task to check.
        
    Returns:
        Dictionary with task status information.
    """
    try:
        result = celery_app.AsyncResult(task_id)
        
        return {
            "task_id": task_id,
            "status": result.status,
            "result": result.result if result.ready() else None,
            "traceback": result.traceback if result.failed() else None,
            "date_done": result.date_done,
            "ready": result.ready(),
            "successful": result.successful(),
            "failed": result.failed(),
        }
        
    except Exception as e:
        return {
            "task_id": task_id,
            "status": "UNKNOWN",
            "error": str(e),
        }


def revoke_task(task_id: str, terminate: bool = False) -> Dict[str, Any]:
    """Revoke a Celery task.
    
    Args:
        task_id: ID of the task to revoke.
        terminate: Whether to terminate the task if running.
        
    Returns:
        Dictionary with revocation result.
    """
    try:
        celery_app.control.revoke(task_id, terminate=terminate)
        
        return {
            "task_id": task_id,
            "revoked": True,
            "terminated": terminate,
        }
        
    except Exception as e:
        return {
            "task_id": task_id,
            "revoked": False,
            "error": str(e),
        }