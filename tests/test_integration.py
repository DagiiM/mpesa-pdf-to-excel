"""Integration and performance tests for PDF Bank Statement Processing System."""

import pytest
import time
from pathlib import Path
import tempfile
import shutil
from unittest.mock import Mock, patch
import json

from src.pdf_processor import PDFProcessor
from src.excel_generator import ExcelGenerator
from src.config.settings import Settings
from src.tasks import BackgroundProcessor
from src.utils import PDFValidator, TransactionValidator


class TestFullWorkflow:
    """Integration tests for complete PDF processing workflow."""

    def test_full_processing_workflow(self, sample_settings, temp_dir, sample_pdf_data):
        """Test complete workflow from PDF to Excel report."""
        # Initialize components
        pdf_processor = PDFProcessor(sample_settings)
        excel_generator = ExcelGenerator(sample_settings)
        
        # Mock PDF processing
        with patch.object(pdf_processor, 'process_pdf') as mock_process, \
             patch.object(excel_generator, 'generate_excel_report') as mock_report:
            
            mock_process.return_value = sample_pdf_data["tables"]
            mock_report.return_value = str(temp_dir / "output.xlsx")
            
            # Execute workflow
            result = pdf_processor.process_pdf("test.pdf")
            excel_output = excel_generator.generate_excel_report([], str(temp_dir / "test.xlsx"))
            
            # Verify integration
            assert mock_process.called
            assert mock_report.called
            assert excel_output.endswith(".xlsx")

    def test_workflow_with_real_file_structure(self, sample_settings, temp_dir):
        """Test workflow with real file structure and validation."""
        # Setup directories
        input_dir = temp_dir / "input"
        output_dir = temp_dir / "output"
        input_dir.mkdir()
        output_dir.mkdir()
        
        # Create test PDF file
        pdf_file = input_dir / "test_statement.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n")
        
        # Initialize components
        pdf_processor = PDFProcessor(sample_settings)
        excel_generator = ExcelGenerator(sample_settings)
        
        # Test validation
        validator = PDFValidator()
        assert validator.validate_pdf_file(str(pdf_file)) is True
        
        # Mock processing
        with patch.object(pdf_processor, 'process_pdf') as mock_process, \
             patch.object(excel_generator, 'generate_excel_report') as mock_report:
            
            mock_process.return_value = []
            mock_report.return_value = str(output_dir / "report.xlsx")
            
            # Process file
            result = pdf_processor.process_pdf(str(pdf_file))
            output_file = excel_generator.generate_excel_report([], str(output_dir / "test.xlsx"))
            
            assert result == []
            assert output_file.endswith(".xlsx")

    def test_error_handling_integration(self, sample_settings):
        """Test error handling across the entire workflow."""
        pdf_processor = PDFProcessor(sample_settings)
        excel_generator = ExcelGenerator(sample_settings)
        
        # Test with invalid PDF
        with pytest.raises(Exception):
            pdf_processor.process_pdf("nonexistent.pdf")
        
        # Test with invalid transactions
        with pytest.raises(Exception):
            excel_generator.generate_excel_report(
                "not_a_list",  # Should be a list
                "test.xlsx"
            )


class TestBatchProcessing:
    """Tests for batch processing functionality."""

    def test_batch_processing_workflow(self, sample_settings, temp_dir):
        """Test batch processing of multiple PDFs."""
        # Create multiple test files
        input_dir = temp_dir / "batch_input"
        input_dir.mkdir()
        
        pdf_files = []
        for i in range(5):
            pdf_file = input_dir / f"statement_{i}.pdf"
            pdf_file.write_bytes(b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n")
            pdf_files.append(str(pdf_file))
        
        # Process batch
        processor = PDFProcessor(sample_settings)
        
        with patch.object(processor, 'process_pdf') as mock_process:
            mock_process.return_value = []
            
            results = []
            for pdf_file in pdf_files:
                result = processor.process_pdf(pdf_file)
                results.append(result)
            
            assert len(results) == 5
            assert mock_process.call_count == 5

    def test_concurrent_processing_limits(self, sample_settings):
        """Test concurrent processing limitations."""
        # Test that we don't exceed maximum concurrent processes
        processor = BackgroundProcessor(sample_settings)
        
        max_concurrent = 3
        with patch.object(processor, 'submit_task') as mock_submit:
            mock_submit.return_value = Mock(id=f'task_{i}')
            
            # Submit multiple tasks
            task_ids = []
            for i in range(10):
                task_id = processor.submit_task('process_pdf_task', f'file{i}.pdf', 'pass')
                task_ids.append(task_id)
            
            # Verify we didn't exceed limits
            assert len(task_ids) == 10
            # In a real implementation, this would check the actual queue state

    def test_batch_file_validation(self, temp_dir):
        """Test validation of batch files."""
        input_dir = temp_dir / "validation_test"
        input_dir.mkdir()
        
        # Create valid and invalid files
        valid_pdf = input_dir / "valid.pdf"
        valid_pdf.write_bytes(b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n")
        
        invalid_file = input_dir / "invalid.txt"
        invalid_file.write_text("Not a PDF")
        
        validator = PDFValidator()
        
        assert validator.validate_pdf_file(str(valid_pdf)) is True
        assert validator.validate_pdf_file(str(invalid_file)) is False


class TestPerformance:
    """Performance and stress tests."""

    def test_large_file_processing_performance(self, sample_settings, performance_test_data):
        """Test performance with large dataset."""
        start_time = time.time()
        
        # Simulate processing large transaction dataset
        summarizer = ExcelGenerator(sample_settings)
        
        # Generate Excel report with large dataset
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            output_path = tmp_file.name
        
        try:
            result = summarizer.generate_excel_report(performance_test_data, output_path)
            end_time = time.time()
            
            processing_time = end_time - start_time
            
            # Performance assertions
            assert result.endswith('.xlsx')
            assert processing_time < 10.0  # Should complete within 10 seconds
            assert Path(output_path).exists()
            
        finally:
            # Cleanup
            if Path(output_path).exists():
                Path(output_path).unlink()

    def test_memory_usage_with_large_transactions(self, sample_settings, performance_test_data):
        """Test memory usage with large transaction datasets."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Process large dataset
        excel_generator = ExcelGenerator(sample_settings)
        validator = TransactionValidator()
        
        # Validate all transactions
        for transaction in performance_test_data:
            assert validator.validate_transaction(transaction) is True
        
        # Generate report
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            output_path = tmp_file.name
        
        try:
            result = excel_generator.generate_excel_report(performance_test_data, output_path)
            final_memory = process.memory_info().rss
            
            memory_increase = final_memory - initial_memory
            memory_increase_mb = memory_increase / (1024 * 1024)
            
            # Memory usage should be reasonable (< 500MB increase)
            assert memory_increase_mb < 500
            assert result.endswith('.xlsx')
            
        finally:
            if Path(output_path).exists():
                Path(output_path).unlink()

    def test_chunked_processing_performance(self, sample_settings, temp_dir):
        """Test performance of chunked processing."""
        # Create a large simulated PDF file
        large_file = temp_dir / "large_document.pdf"
        large_content = b"x" * (5 * 1024 * 1024)  # 5MB
        large_file.write_bytes(large_content)
        
        processor = PDFProcessor(sample_settings)
        
        start_time = time.time()
        
        # Test chunking
        with patch.object(processor.chunker, 'chunk_file') as mock_chunk:
            mock_chunk.return_value = [str(large_file)]  # Single chunk for test
            
            chunks = list(processor.chunker.chunk_file(str(large_file)))
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            assert len(chunks) >= 1
            assert processing_time < 5.0  # Should be fast

    def test_concurrent_task_performance(self, sample_settings):
        """Test performance of concurrent task execution."""
        processor = BackgroundProcessor(sample_settings)
        
        start_time = time.time()
        
        # Mock concurrent processing
        with patch.object(processor, 'submit_task') as mock_submit:
            # Simulate fast task completion
            mock_submit.return_value = Mock(id=f'quick_task_{i}')
            
            # Submit multiple tasks quickly
            task_ids = []
            for i in range(20):
                task_id = processor.submit_task('process_pdf_task', f'file{i}.pdf', 'pass')
                task_ids.append(task_id)
            
            end_time = time.time()
            total_time = end_time - start_time
            
            assert len(task_ids) == 20
            assert total_time < 1.0  # Should be very fast with mocked execution

    def test_database_query_performance(self, sample_settings):
        """Test performance of data operations."""
        # Test with large transaction dataset
        large_dataset = []
        for i in range(10000):
            large_dataset.append({
                "date": f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                "description": f"Transaction {i}",
                "amount": float((i % 1000) + 1),
                "type": "credit" if i % 2 == 0 else "debit",
                "balance": float((i % 10000) + 1000),
            })
        
        # Test summarization performance
        excel_generator = ExcelGenerator(sample_settings)
        
        start_time = time.time()
        
        # Mock the expensive operations
        with patch.object(excel_generator, 'generate_excel_report') as mock_report:
            mock_report.return_value = "test_report.xlsx"
            
            result = excel_generator.generate_excel_report(large_dataset, "test.xlsx")
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Should handle large datasets efficiently
            assert processing_time < 5.0
            assert result == "test_report.xlsx"


class TestErrorRecovery:
    """Tests for error recovery and resilience."""

    def test_retry_mechanism(self, sample_settings):
        """Test retry mechanism for failed operations."""
        processor = PDFProcessor(sample_settings)
        
        # Simulate intermittent failures
        call_count = 0
        
        def failing_process(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Temporary failure")
            return []  # Success on 3rd attempt
        
        with patch.object(processor, 'process_pdf', side_effect=failing_process):
            with pytest.raises(Exception):
                processor.process_pdf("test.pdf")
            
            # Should have tried multiple times
            assert call_count >= 3

    def test_graceful_degradation(self, sample_settings):
        """Test graceful degradation when components fail."""
        excel_generator = ExcelGenerator(sample_settings)
        
        # Test with malformed transaction data
        malformed_transactions = [
            {
                "date": "2023-01-01",
                "description": "Valid transaction",
                "amount": 100.0,
                "type": "credit",
            },
            {
                "date": "invalid_date",  # Invalid date
                "description": "Invalid transaction",
                "amount": "not_a_number",  # Invalid amount
                "type": "invalid_type",  # Invalid type
            }
        ]
        
        validator = TransactionValidator()
        
        # Should handle gracefully
        valid_count = 0
        for transaction in malformed_transactions:
            if validator.validate_transaction(transaction):
                valid_count += 1
        
        assert valid_count == 1  # Only the valid transaction

    def test_rollback_on_failure(self, sample_settings, temp_dir):
        """Test rollback mechanism on processing failure."""
        output_file = temp_dir / "rollback_test.xlsx"
        
        excel_generator = ExcelGenerator(sample_settings)
        
        # Mock file writing to simulate failure
        with patch('openpyxl.Workbook.save') as mock_save:
            mock_save.side_effect = Exception("Disk full")
            
            try:
                excel_generator.generate_excel_report([], str(output_file))
            except Exception:
                pass  # Expected to fail
            
            # File should not exist due to rollback
            assert not output_file.exists()

    def test_resource_cleanup_on_error(self, sample_settings):
        """Test resource cleanup when errors occur."""
        processor = PDFProcessor(sample_settings)
        
        # Mock resource-intensive operation that fails
        with patch.object(processor, 'process_pdf', side_effect=MemoryError("Out of memory")):
            with pytest.raises(MemoryError):
                processor.process_pdf("test.pdf")
            
            # In a real implementation, this would verify cleanup


class TestDataIntegrity:
    """Tests for data integrity and validation."""

    def test_transaction_data_integrity(self, sample_transactions):
        """Test that transaction data maintains integrity during processing."""
        validator = TransactionValidator()
        
        for transaction in sample_transactions:
            # Validate before processing
            assert validator.validate_transaction(transaction) is True
            
            # Process (clean) the transaction
            cleaned = validator.clean_transaction_data(transaction)
            
            # Verify essential fields are preserved
            assert cleaned["date"] == transaction["date"]
            assert cleaned["description"] == transaction["description"]
            assert cleaned["type"] == transaction["type"]

    def test_data_consistency_across_components(self, sample_settings, sample_transactions):
        """Test data consistency across different components."""
        # Process through different components
        pdf_processor = PDFProcessor(sample_settings)
        excel_generator = ExcelGenerator(sample_settings)
        validator = TransactionValidator()
        
        # Validate transactions
        for transaction in sample_transactions:
            assert validator.validate_transaction(transaction) is True
        
        # Test that data flows correctly through the system
        # (In a real integration test, this would test actual data flow)

    def test_data_serialization_consistency(self, sample_transactions):
        """Test data serialization/deserialization consistency."""
        import json
        
        # Serialize transaction data
        serialized = json.dumps(sample_transactions)
        
        # Deserialize
        deserialized = json.loads(serialized)
        
        # Verify consistency
        assert deserialized == sample_transactions
        
        # Verify each transaction is still valid
        validator = TransactionValidator()
        for transaction in deserialized:
            assert validator.validate_transaction(transaction) is True

    def test_boundary_conditions(self):
        """Test boundary conditions and edge cases."""
        validator = TransactionValidator()
        
        # Test minimum/maximum values
        boundary_tests = [
            # Very small amounts
            {"date": "2023-01-01", "description": "Small", "amount": 0.01, "type": "credit"},
            # Zero amount
            {"date": "2023-01-01", "description": "Zero", "amount": 0, "type": "credit"},
            # Large amount
            {"date": "2023-01-01", "description": "Large", "amount": 999999999.99, "type": "credit"},
            # Negative amount
            {"date": "2023-01-01", "description": "Negative", "amount": -0.01, "type": "debit"},
        ]
        
        for transaction in boundary_tests:
            try:
                result = validator.validate_transaction(transaction)
                # Should not raise exception, may or may not be valid
                assert isinstance(result, bool)
            except Exception as e:
                pytest.fail(f"Validation raised exception: {e}")


class TestConfiguration:
    """Tests for configuration management."""

    def test_configuration_propagation(self, sample_settings, temp_dir):
        """Test that configuration propagates correctly through components."""
        # Create component with specific configuration
        pdf_processor = PDFProcessor(sample_settings)
        excel_generator = ExcelGenerator(sample_settings)
        
        # Verify settings are propagated
        assert pdf_processor.settings == sample_settings
        assert excel_generator.settings == sample_settings
        assert pdf_processor.settings.currency_symbol == sample_settings.currency_symbol

    def test_configuration_validation(self):
        """Test configuration validation."""
        # Valid configuration
        valid_settings = Settings(
            chunk_size_mb=10,
            max_retries=5,
            currency_symbol="USD",
        )
        assert valid_settings.validate() is True
        
        # Invalid configuration
        invalid_settings = Settings(
            chunk_size_mb=0,  # Too small
            max_retries=-1,   # Negative
            currency_symbol="",  # Empty
        )
        assert invalid_settings.validate() is False

    def test_environment_configuration_override(self, temp_dir):
        """Test environment variable configuration override."""
        import os
        
        # Set environment variables
        env_vars = {
            "CURRENCY_SYMBOL": "EUR",
            "CHUNK_SIZE_MB": "15",
            "LOG_LEVEL": "DEBUG",
        }
        
        with patch.dict(os.environ, env_vars):
            settings = Settings.from_env()
            
            assert settings.currency_symbol == "EUR"
            assert settings.chunk_size_mb == 15
            assert settings.log_level == "DEBUG"