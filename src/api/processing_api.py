"""API interface for PDF Bank Statement Processing System integration."""

import os
import json
import asyncio
import time
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, asdict

from src.config.settings import Settings
from src.pdf_processor import PDFProcessor
from src.excel_generator import ExcelGenerator
from src.tasks import BackgroundProcessor
from src.monitoring.health_checker import HealthChecker
from src.utils.logger import get_logger
from src.utils.exceptions import PDFProcessingError, ExcelGenerationError


class ProcessingStatus(Enum):
    """Processing status enumeration."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ProcessingRequest:
    """Processing request data structure."""
    id: str
    pdf_file: str
    password: Optional[str] = None
    output_format: str = "xlsx"
    include_summary: bool = True
    include_monthly_breakdown: bool = True
    metadata: Optional[Dict[str, Any]] = None
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


@dataclass
class ProcessingResult:
    """Processing result data structure."""
    request_id: str
    status: ProcessingStatus
    output_file: Optional[str] = None
    error_message: Optional[str] = None
    processing_time: Optional[float] = None
    transactions_extracted: int = 0
    summary_data: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: datetime = None
    completed_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


class ProcessingAPI:
    """API interface for PDF Bank Statement Processing System."""
    
    def __init__(self, settings: Settings):
        """Initialize the processing API."""
        self.settings = settings
        self.logger = get_logger(self.__class__.__name__)
        
        # Initialize components
        self.pdf_processor = PDFProcessor(settings)
        self.excel_generator = ExcelGenerator(settings)
        self.background_processor = BackgroundProcessor(settings)
        self.health_checker = HealthChecker(settings)
        
        # Request storage
        self.requests: Dict[str, ProcessingRequest] = {}
        self.results: Dict[str, ProcessingResult] = {}
        
        # API configuration
        self.max_concurrent_requests = int(os.getenv('MAX_CONCURRENT_REQUESTS', '10'))
        self.request_timeout = int(os.getenv('REQUEST_TIMEOUT', '3600'))  # 1 hour
        self.cleanup_interval = int(os.getenv('CLEANUP_INTERVAL', '1800'))  # 30 minutes
    
    async def submit_processing_request(
        self,
        request: ProcessingRequest
    ) -> ProcessingResult:
        """Submit a PDF processing request."""
        self.logger.info(f"Submitting processing request: {request.id}")
        
        # Validate request
        if not self._validate_request(request):
            result = ProcessingResult(
                request_id=request.id,
                status=ProcessingStatus.FAILED,
                error_message="Invalid request parameters"
            )
            self.results[request.id] = result
            return result
        
        # Check concurrent request limit
        if len(self._get_active_requests()) >= self.max_concurrent_requests:
            result = ProcessingResult(
                request_id=request.id,
                status=ProcessingStatus.FAILED,
                error_message="Too many concurrent requests"
            )
            self.results[request.id] = result
            return result
        
        # Store request
        self.requests[request.id] = request
        
        # Create result entry
        result = ProcessingResult(
            request_id=request.id,
            status=ProcessingStatus.PENDING
        )
        self.results[request.id] = result
        
        # Start processing in background
        asyncio.create_task(self._process_request_async(request, result))
        
        return result
    
    def get_processing_status(self, request_id: str) -> Optional[ProcessingResult]:
        """Get processing status for a request."""
        return self.results.get(request_id)
    
    def list_processing_requests(
        self,
        status: Optional[ProcessingStatus] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[ProcessingResult]:
        """List processing requests with optional filtering."""
        results = list(self.results.values())
        
        if status:
            results = [r for r in results if r.status == status]
        
        # Sort by creation time (newest first)
        results.sort(key=lambda x: x.created_at, reverse=True)
        
        # Apply pagination
        return results[offset:offset + limit]
    
    def cancel_processing_request(self, request_id: str) -> bool:
        """Cancel a processing request."""
        if request_id not in self.requests:
            return False
        
        request = self.requests[request_id]
        result = self.results.get(request_id)
        
        if result and result.status in [ProcessingStatus.PENDING, ProcessingStatus.PROCESSING]:
            result.status = ProcessingStatus.CANCELLED
            result.completed_at = datetime.now()
            self.logger.info(f"Cancelled request: {request_id}")
            return True
        
        return False
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get processing statistics."""
        all_results = list(self.results.values())
        
        status_counts = {}
        for status in ProcessingStatus:
            status_counts[status.value] = sum(1 for r in all_results if r.status == status)
        
        total_processing_time = sum(
            r.processing_time for r in all_results 
            if r.processing_time is not None
        )
        
        avg_processing_time = (
            total_processing_time / len(all_results) 
            if all_results else 0
        )
        
        return {
            'total_requests': len(all_results),
            'status_distribution': status_counts,
            'average_processing_time': round(avg_processing_time, 2),
            'active_requests': len(self._get_active_requests()),
            'success_rate': (
                status_counts.get(ProcessingStatus.COMPLETED.value, 0) / 
                max(len(all_results), 1) * 100
            ),
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check."""
        return self.health_checker.run_health_check()
    
    async def _process_request_async(
        self,
        request: ProcessingRequest,
        result: ProcessingResult
    ) -> None:
        """Process request asynchronously."""
        start_time = time.time()
        result.status = ProcessingStatus.PROCESSING
        
        try:
            self.logger.info(f"Starting processing for request: {request.id}")
            
            # Process PDF
            tables = await self._process_pdf_async(request)
            
            # Extract transactions
            transactions = await self._extract_transactions_async(tables, request)
            
            # Generate Excel report
            output_file = await self._generate_report_async(transactions, request)
            
            # Calculate summary
            summary_data = await self._calculate_summary_async(transactions, request)
            
            # Update result
            result.status = ProcessingStatus.COMPLETED
            result.output_file = output_file
            result.transactions_extracted = len(transactions)
            result.summary_data = summary_data
            result.metadata = request.metadata
            result.processing_time = time.time() - start_time
            result.completed_at = datetime.now()
            
            self.logger.info(f"Processing completed for request: {request.id}")
            
        except Exception as e:
            self.logger.error(f"Processing failed for request {request.id}: {e}")
            result.status = ProcessingStatus.FAILED
            result.error_message = str(e)
            result.processing_time = time.time() - start_time
            result.completed_at = datetime.now()
    
    async def _process_pdf_async(self, request: ProcessingRequest) -> List[List[List[str]]]:
        """Process PDF asynchronously."""
        loop = asyncio.get_event_loop()
        
        def process():
            return self.pdf_processor.process_pdf(request.pdf_file, request.password)
        
        return await loop.run_in_executor(None, process)
    
    async def _extract_transactions_async(
        self,
        tables: List[List[List[str]]],
        request: ProcessingRequest
    ) -> List[Dict[str, Any]]:
        """Extract transactions asynchronously."""
        loop = asyncio.get_event_loop()
        
        def extract():
            # Filter transaction tables
            transaction_tables = self.pdf_processor.filter_transaction_tables(tables)
            
            # Extract transaction data
            transactions = []
            for table in transaction_tables:
                transactions.extend(self.pdf_processor.extract_transactions_from_table(table))
            
            return transactions
        
        return await loop.run_in_executor(None, extract)
    
    async def _generate_report_async(
        self,
        transactions: List[Dict[str, Any]],
        request: ProcessingRequest
    ) -> str:
        """Generate Excel report asynchronously."""
        loop = asyncio.get_event_loop()
        
        def generate():
            # Determine output file path
            output_dir = Path(request.output_file).parent if request.output_file else Path(self.settings.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"bank_statement_{request.id}_{timestamp}.xlsx"
            output_path = output_dir / filename
            
            return self.excel_generator.generate_excel_report(
                transactions,
                str(output_path)
            )
        
        return await loop.run_in_executor(None, generate)
    
    async def _calculate_summary_async(
        self,
        transactions: List[Dict[str, Any]],
        request: ProcessingRequest
    ) -> Optional[Dict[str, Any]]:
        """Calculate summary asynchronously."""
        if not request.include_summary:
            return None
        
        loop = asyncio.get_event_loop()
        
        def calculate():
            summarizer = self.excel_generator.summarizer
            summary = summarizer.calculate_summary(transactions)
            
            if request.include_monthly_breakdown:
                monthly_breakdown = summarizer.calculate_monthly_breakdown(transactions)
                summary['monthly_breakdown'] = monthly_breakdown
            
            return summary
        
        return await loop.run_in_executor(None, calculate)
    
    def _validate_request(self, request: ProcessingRequest) -> bool:
        """Validate processing request."""
        try:
            # Check if PDF file exists
            if not Path(request.pdf_file).exists():
                self.logger.error(f"PDF file not found: {request.pdf_file}")
                return False
            
            # Check file extension
            if not request.pdf_file.lower().endswith('.pdf'):
                self.logger.error(f"Invalid file type: {request.pdf_file}")
                return False
            
            # Check output format
            if request.output_format not in ['xlsx', 'csv', 'json']:
                self.logger.error(f"Invalid output format: {request.output_format}")
                return False
            
            # Check file size (if large, use background processing)
            file_size = Path(request.pdf_file).stat().st_size
            if file_size > 50 * 1024 * 1024:  # 50MB
                self.logger.warning(f"Large file detected: {file_size} bytes")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Request validation failed: {e}")
            return False
    
    def _get_active_requests(self) -> List[ProcessingRequest]:
        """Get list of active (non-terminal) requests."""
        active_statuses = [ProcessingStatus.PENDING, ProcessingStatus.PROCESSING]
        return [
            req for req in self.requests.values()
            if self.results.get(req.id, ProcessingResult(
                request_id=req.id, 
                status=ProcessingStatus.FAILED
            )).status in active_statuses
        ]
    
    def cleanup_old_requests(self, max_age_hours: int = 24) -> int:
        """Clean up old completed requests."""
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        
        old_request_ids = [
            req_id for req_id, result in self.results.items()
            if result.completed_at and result.completed_at < cutoff_time
            and result.status in [ProcessingStatus.COMPLETED, ProcessingStatus.FAILED, ProcessingStatus.CANCELLED]
        ]
        
        for req_id in old_request_ids:
            del self.requests[req_id]
            del self.results[req_id]
        
        self.logger.info(f"Cleaned up {len(old_request_ids)} old requests")
        return len(old_request_ids)
    
    def export_results(self, output_path: str, format: str = 'json') -> str:
        """Export processing results to file."""
        if format not in ['json', 'csv']:
            raise ValueError("Format must be 'json' or 'csv'")
        
        if format == 'json':
            return self._export_json(output_path)
        else:
            return self._export_csv(output_path)
    
    def _export_json(self, output_path: str) -> str:
        """Export results as JSON."""
        export_data = {
            'export_timestamp': datetime.now().isoformat(),
            'statistics': self.get_processing_statistics(),
            'requests': [asdict(r) for r in self.results.values()],
        }
        
        with open(output_path, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        return output_path
    
    def _export_csv(self, output_path: str) -> str:
        """Export results as CSV."""
        import csv
        
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow([
                'Request ID', 'Status', 'Output File', 'Error Message',
                'Processing Time', 'Transactions Extracted', 'Created At', 'Completed At'
            ])
            
            # Write data
            for result in self.results.values():
                writer.writerow([
                    result.request_id,
                    result.status.value,
                    result.output_file or '',
                    result.error_message or '',
                    result.processing_time or '',
                    result.transactions_extracted,
                    result.created_at.isoformat() if result.created_at else '',
                    result.completed_at.isoformat() if result.completed_at else '',
                ])
        
        return output_path


class ProcessingAPIClient:
    """Client for interacting with ProcessingAPI from other systems."""
    
    def __init__(self, base_url: str = "http://localhost:8000", api_key: Optional[str] = None):
        """Initialize API client."""
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.logger = get_logger(self.__class__.__name__)
    
    async def process_pdf(
        self,
        pdf_path: str,
        password: Optional[str] = None,
        output_dir: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Process a PDF file via API."""
        # This would be a real HTTP client implementation
        # For now, this is a placeholder for the API interface
        
        request_data = {
            'pdf_path': pdf_path,
            'password': password,
            'output_dir': output_dir,
            'options': options or {},
        }
        
        self.logger.info(f"Processing PDF via API: {pdf_path}")
        
        # Placeholder for actual API call
        # response = await self._make_request('POST', '/process', request_data)
        # return response
        
        return {
            'status': 'submitted',
            'message': 'API call would be made here',
            'data': request_data,
        }
    
    async def get_status(self, request_id: str) -> Dict[str, Any]:
        """Get processing status via API."""
        self.logger.info(f"Getting status for request: {request_id}")
        
        # Placeholder for actual API call
        # response = await self._make_request('GET', f'/status/{request_id}')
        # return response
        
        return {
            'request_id': request_id,
            'status': 'pending',
            'message': 'API call would be made here',
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check via API."""
        self.logger.info("Performing health check via API")
        
        # Placeholder for actual API call
        # response = await self._make_request('GET', '/health')
        # return response
        
        return {
            'status': 'healthy',
            'message': 'API call would be made here',
        }
    
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make HTTP request to API."""
        # This would implement the actual HTTP client logic
        # using aiohttp or similar
        pass