"""Pytest configuration and fixtures for PDF Bank Statement Processing System."""

import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch
import pytest
import pandas as pd
from typing import Dict, Any, List

from src.config.settings import Settings


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_path = tempfile.mkdtemp()
    yield Path(temp_path)
    shutil.rmtree(temp_path)


@pytest.fixture
def sample_settings():
    """Create sample settings for testing."""
    return Settings(
        pdf_password="test123",
        chunk_size_mb=5,
        max_retries=3,
        output_dir="test_reports",
        log_level="INFO",
        currency_symbol="KES",
        default_currency="KES",
    )


@pytest.fixture
def sample_pdf_data():
    """Create sample PDF data for testing."""
    return {
        "text": "Receipt No. Completion Time Details Transaction Status Paid In Withdrawn Balance\n"
                "ABC123 01/01/2023 Test Transaction Completed 100.00 0.00 1000.00\n"
                "DEF456 02/01/2023 Another Transaction Completed 0.00 50.00 950.00",
        "tables": [
            [
                ["Receipt No.", "Completion Time", "Details", "Transaction Status", "Paid In", "Withdrawn", "Balance"],
                ["ABC123", "01/01/2023", "Test Transaction", "Completed", "100.00", "0.00", "1000.00"],
                ["DEF456", "02/01/2023", "Another Transaction", "Completed", "0.00", "50.00", "950.00"],
            ]
        ]
    }


@pytest.fixture
def sample_transactions():
    """Create sample transaction data for testing."""
    return [
        {
            "date": "2023-01-01",
            "description": "Test Transaction",
            "amount": 100.00,
            "type": "credit",
            "balance": 1000.00,
        },
        {
            "date": "2023-01-02",
            "description": "Another Transaction",
            "amount": -50.00,
            "type": "debit",
            "balance": 950.00,
        },
    ]


@pytest.fixture
def sample_excel_data():
    """Create sample Excel data for testing."""
    return {
        "summary": {
            "total_credits": 100.00,
            "total_debits": 50.00,
            "net_amount": 50.00,
            "transaction_count": 2,
            "period_start": "2023-01-01",
            "period_end": "2023-01-02",
        },
        "monthly_breakdown": {
            "2023-01": {
                "credits": 100.00,
                "debits": 50.00,
                "net": 50.00,
                "transactions": 2,
            }
        },
        "transactions": [
            {"date": "2023-01-01", "description": "Test Transaction", "amount": 100.00},
            {"date": "2023-01-02", "description": "Another Transaction", "amount": -50.00},
        ],
    }


@pytest.fixture
def mock_pdf_reader():
    """Create a mock PDF reader for testing."""
    mock_reader = Mock()
    mock_page = Mock()
    mock_page.extract_text.return_value = "Sample PDF text content"
    mock_reader.pages = [mock_page]
    return mock_reader


@pytest.fixture
def mock_pdfplumber():
    """Create a mock pdfplumber for testing."""
    mock_pdf = Mock()
    mock_page = Mock()
    mock_table = [
        ["Receipt No.", "Completion Time", "Details", "Transaction Status", "Paid In", "Withdrawn", "Balance"],
        ["ABC123", "01/01/2023", "Test Transaction", "Completed", "100.00", "0.00", "1000.00"],
    ]
    mock_page.extract_tables.return_value = [mock_table]
    mock_pdf.pages = [mock_page]
    return mock_pdf


@pytest.fixture
def sample_password_file(temp_dir):
    """Create a sample password file for testing."""
    password_file = temp_dir / "test_password.txt"
    password_file.write_text("test123\n")
    return str(password_file)


@pytest.fixture
def sample_pdf_file(temp_dir):
    """Create a sample PDF file for testing."""
    pdf_file = temp_dir / "test.pdf"
    # Create a minimal PDF file (this would normally be a real PDF)
    pdf_file.write_bytes(b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n")
    return str(pdf_file)


@pytest.fixture
def mock_celery_app():
    """Create a mock Celery app for testing."""
    with patch('src.tasks.celery_app.celery_app') as mock_app:
        mock_app.conf.task_always_eager = True
        mock_app.conf.task_eager_propagates = True
        yield mock_app


@pytest.fixture
def sample_environment():
    """Create sample environment variables for testing."""
    env_vars = {
        "PDF_PASSWORD": "test123",
        "CURRENCY_SYMBOL": "KES",
        "DEFAULT_CURRENCY": "KES",
        "LOG_LEVEL": "INFO",
        "OUTPUT_DIR": "test_reports",
        "CHUNK_SIZE_MB": "5",
        "MAX_RETRIES": "3",
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def error_sample_data():
    """Create sample data that will cause errors for testing error handling."""
    return {
        "invalid_pdf": b"Not a PDF file",
        "corrupted_table": [
            ["Header1", "Header2"],
            ["Row1Col1"],  # Missing column
            ["Row2Col1", "Row2Col2", "Row2Col3"],  # Extra column
        ],
        "invalid_dates": ["32/13/2023", "01/01/9999", "invalid_date"],
        "invalid_amounts": ["not_a_number", "1,234.56.78", ""],
    }


@pytest.fixture
def performance_test_data():
    """Create large dataset for performance testing."""
    transactions = []
    for i in range(1000):
        transactions.append({
            "date": f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
            "description": f"Transaction {i}",
            "amount": float((i % 100) + 1),
            "type": "credit" if i % 2 == 0 else "debit",
            "balance": float((i % 1000) + 100),
        })
    return transactions


@pytest.fixture
def mock_redis():
    """Create a mock Redis connection for testing."""
    with patch('redis.Redis') as mock_redis:
        mock_client = Mock()
        mock_redis.return_value = mock_client
        yield mock_client


@pytest.fixture(autouse=True)
def cleanup_temp_files():
    """Automatically clean up temporary files after each test."""
    yield
    # Clean up any temporary files that might have been created
    temp_patterns = ["temp_*.pdf", "test_*.xlsx", "*.tmp"]
    for pattern in temp_patterns:
        for file in Path(".").glob(pattern):
            try:
                file.unlink()
            except (OSError, PermissionError):
                pass  # Ignore cleanup errors