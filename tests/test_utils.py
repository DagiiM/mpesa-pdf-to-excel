"""Tests for utility modules."""

import pytest
import logging
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import tempfile
import os

from src.utils.logger import setup_logging, get_logger
from src.utils.validators import PDFValidator, TransactionValidator
from src.utils.exceptions import ValidationError


class TestLogger:
    """Test cases for logging functionality."""

    def test_setup_logging(self, temp_dir):
        """Test logging setup."""
        log_file = temp_dir / "test.log"
        
        logger = setup_logging(
            log_level="INFO",
            log_file=str(log_file),
            log_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        
        assert logger is not None
        assert isinstance(logger, logging.Logger)
        assert logger.level == logging.INFO

    def test_setup_logging_with_file(self, temp_dir):
        """Test logging setup with file output."""
        log_file = temp_dir / "test.log"
        
        logger = setup_logging(
            log_level="DEBUG",
            log_file=str(log_file),
            console_output=False
        )
        
        logger.info("Test log message")
        
        # Check if log file was created
        assert log_file.exists()
        
        # Check if message was written to file
        log_file_content = log_file.read_text()
        assert "Test log message" in log_file_content

    def test_get_logger(self):
        """Test logger retrieval."""
        logger = get_logger("test_logger")
        assert logger.name == "test_logger"
        
        logger2 = get_logger("test_logger")
        assert logger is logger2  # Should return same instance

    def test_log_levels(self, temp_dir):
        """Test different log levels."""
        log_file = temp_dir / "test.log"
        
        logger = setup_logging(
            log_level="WARNING",
            log_file=str(log_file),
            console_output=False
        )
        
        logger.debug("Debug message")  # Should not be logged
        logger.info("Info message")    # Should not be logged
        logger.warning("Warning message")  # Should be logged
        logger.error("Error message")      # Should be logged
        
        log_content = log_file.read_text()
        assert "Warning message" in log_content
        assert "Error message" in log_content
        assert "Debug message" not in log_content
        assert "Info message" not in log_content

    def test_log_formatting(self, temp_dir):
        """Test log message formatting."""
        log_file = temp_dir / "test.log"
        
        logger = setup_logging(
            log_level="INFO",
            log_file=str(log_file),
            log_format="%(levelname)s - %(message)s"
        )
        
        logger.info("Formatted message")
        
        log_content = log_file.read_text()
        assert "INFO - Formatted message" in log_content


class TestPDFValidator:
    """Test cases for PDF validation functionality."""

    def test_init(self):
        """Test PDFValidator initialization."""
        validator = PDFValidator()
        assert validator is not None

    def test_validate_pdf_file_exists(self, temp_dir):
        """Test PDF file validation when file exists."""
        validator = PDFValidator()
        
        # Create a mock PDF file
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n")
        
        result = validator.validate_pdf_file(str(pdf_file))
        assert result is True

    def test_validate_pdf_file_not_exists(self):
        """Test PDF file validation when file doesn't exist."""
        validator = PDFValidator()
        
        result = validator.validate_pdf_file("nonexistent.pdf")
        assert result is False

    def test_validate_pdf_file_invalid_extension(self, temp_dir):
        """Test PDF validation with invalid file extension."""
        validator = PDFValidator()
        
        # Create file with wrong extension
        txt_file = temp_dir / "test.txt"
        txt_file.write_text("Not a PDF")
        
        result = validator.validate_pdf_file(str(txt_file))
        assert result is False

    def test_validate_pdf_content(self, temp_dir):
        """Test PDF content validation."""
        validator = PDFValidator()
        
        # Create a valid PDF content
        pdf_content = b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n"
        
        result = validator.validate_pdf_content(pdf_content)
        assert result is True

    def test_validate_pdf_content_invalid(self):
        """Test PDF content validation with invalid content."""
        validator = PDFValidator()
        
        # Invalid PDF content
        invalid_content = b"This is not a PDF"
        
        result = validator.validate_pdf_content(invalid_content)
        assert result is False

    def test_validate_file_size(self, temp_dir):
        """Test file size validation."""
        validator = PDFValidator(max_size_mb=1)
        
        # Create small file
        small_file = temp_dir / "small.pdf"
        small_file.write_bytes(b"x" * 1024)  # 1KB
        
        assert validator.validate_file_size(str(small_file)) is True
        
        # Create large file (simulated)
        large_file = temp_dir / "large.pdf"
        large_file.write_bytes(b"x" * (2 * 1024 * 1024))  # 2MB
        
        assert validator.validate_file_size(str(large_file)) is False

    def test_check_pdf_encryption(self, temp_dir):
        """Test PDF encryption detection."""
        validator = PDFValidator()
        
        # Create encrypted-like PDF content
        encrypted_pdf = b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Encrypt 2 0 R\n/Pages 3 0 R\n>>\nendobj\n"
        
        result = validator.check_pdf_encryption(encrypted_pdf)
        assert result is True

    def test_check_pdf_encryption_not_encrypted(self, temp_dir):
        """Test PDF encryption detection when not encrypted."""
        validator = PDFValidator()
        
        # Create non-encrypted PDF content
        normal_pdf = b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n"
        
        result = validator.check_pdf_encryption(normal_pdf)
        assert result is False

    def test_validate_password_strength(self):
        """Test password strength validation."""
        validator = PDFValidator()
        
        # Strong password
        assert validator.validate_password_strength("SecurePass123!") is True
        
        # Weak password
        assert validator.validate_password_strength("123") is False
        assert validator.validate_password_strength("password") is False
        assert validator.validate_password_strength("") is False

    def test_sanitize_filename(self):
        """Test filename sanitization."""
        validator = PDFValidator()
        
        # Test various filenames
        assert validator.sanitize_filename("normal_file.pdf") == "normal_file.pdf"
        assert validator.sanitize_filename("file with spaces.pdf") == "file_with_spaces.pdf"
        assert validator.sanitize_filename("file/with\\invalid:chars.pdf") == "file_with_invalid_chars.pdf"
        assert validator.sanitize_filename("   .pdf") == "unnamed.pdf"


class TestTransactionValidator:
    """Test cases for transaction validation functionality."""

    def test_init(self):
        """Test TransactionValidator initialization."""
        validator = TransactionValidator()
        assert validator is not None

    def test_validate_transaction_success(self, sample_transactions):
        """Test successful transaction validation."""
        validator = TransactionValidator()
        
        for transaction in sample_transactions:
            result = validator.validate_transaction(transaction)
            assert result is True

    def test_validate_transaction_missing_fields(self):
        """Test transaction validation with missing fields."""
        validator = TransactionValidator()
        
        # Missing required fields
        invalid_transaction = {
            "date": "2023-01-01",
            "description": "Test",
            # Missing "amount" and "type"
        }
        
        result = validator.validate_transaction(invalid_transaction)
        assert result is False

    def test_validate_transaction_invalid_type(self):
        """Test transaction validation with invalid type."""
        validator = TransactionValidator()
        
        invalid_transaction = {
            "date": "2023-01-01",
            "description": "Test",
            "amount": 100.00,
            "type": "invalid_type"  # Should be "credit" or "debit"
        }
        
        result = validator.validate_transaction(invalid_transaction)
        assert result is False

    def test_validate_transaction_invalid_amount(self):
        """Test transaction validation with invalid amount."""
        validator = TransactionValidator()
        
        invalid_transaction = {
            "date": "2023-01-01",
            "description": "Test",
            "amount": "not_a_number",
            "type": "credit"
        }
        
        result = validator.validate_transaction(invalid_transaction)
        assert result is False

    def test_validate_transaction_invalid_date(self):
        """Test transaction validation with invalid date."""
        validator = TransactionValidator()
        
        invalid_transaction = {
            "date": "invalid_date",
            "description": "Test",
            "amount": 100.00,
            "type": "credit"
        }
        
        result = validator.validate_transaction(invalid_transaction)
        assert result is False

    def test_validate_transaction_list(self, sample_transactions):
        """Test validation of transaction list."""
        validator = TransactionValidator()
        
        result = validator.validate_transaction_list(sample_transactions)
        assert result is True
        
        # Test with mixed valid/invalid transactions
        mixed_transactions = sample_transactions + [
            {
                "date": "invalid",
                "description": "Bad",
                "amount": "not_a_number",
                "type": "invalid"
            }
        ]
        
        result = validator.validate_transaction_list(mixed_transactions)
        assert result is False

    def test_sanitize_description(self):
        """Test description sanitization."""
        validator = TransactionValidator()
        
        # Test various descriptions
        assert validator.sanitize_description("Normal description") == "Normal description"
        assert validator.sanitize_description("  Extra   spaces  ") == "Extra   spaces"
        assert validator.sanitize_description("") == ""

    def test_parse_amount(self):
        """Test amount parsing."""
        validator = TransactionValidator()
        
        # Test various amount formats
        assert validator.parse_amount("1,234.56") == 1234.56
        assert validator.parse_amount("1234.56") == 1234.56
        assert validator.parse_amount("1.234,56") == 1234.56
        assert validator.parse_amount("1,234") == 1234.0
        
        # Test invalid amounts
        with pytest.raises(ValidationError):
            validator.parse_amount("not_a_number")
        
        with pytest.raises(ValidationError):
            validator.parse_amount("")

    def test_parse_date(self):
        """Test date parsing."""
        validator = TransactionValidator()
        
        # Test various date formats
        assert validator.parse_date("01/01/2023") == "2023-01-01"
        assert validator.parse_date("2023-01-01") == "2023-01-01"
        assert validator.parse_date("01 Jan 2023") == "2023-01-01"
        
        # Test invalid dates
        with pytest.raises(ValidationError):
            validator.parse_date("invalid_date")
        
        with pytest.raises(ValidationError):
            validator.parse_date("")

    def test_detect_transaction_type(self):
        """Test transaction type detection."""
        validator = TransactionValidator()
        
        # Test credit transactions
        assert validator.detect_transaction_type({"amount": 100.00}) == "credit"
        assert validator.detect_transaction_type({"amount": 0}) == "credit"
        
        # Test debit transactions
        assert validator.detect_transaction_type({"amount": -100.00}) == "debit"
        assert validator.detect_transaction_type({"amount": 0.01}) == "credit"

    def test_validate_balance(self):
        """Test balance validation."""
        validator = TransactionValidator()
        
        # Valid balance
        assert validator.validate_balance(1000.00) is True
        assert validator.validate_balance(0) is True
        
        # Invalid balance
        assert validator.validate_balance(None) is False
        assert validator.validate_balance("not_a_number") is False

    def test_clean_transaction_data(self, sample_transactions):
        """Test transaction data cleaning."""
        validator = TransactionValidator()
        
        dirty_transaction = {
            "date": "01/01/2023  ",
            "description": "  Test Transaction  ",
            "amount": "1,234.56",
            "type": "credit",
            "balance": "2,345.67",
        }
        
        cleaned = validator.clean_transaction_data(dirty_transaction)
        
        assert cleaned["date"] == "01/01/2023  "  # Preserved original
        assert cleaned["description"] == "  Test Transaction  "  # Preserved original
        assert cleaned["amount"] == "1,234.56"  # Not converted in this method
        assert cleaned["type"] == "credit"
        assert cleaned["balance"] == "2,345.67"