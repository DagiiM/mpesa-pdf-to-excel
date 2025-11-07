"""Tests for PDF processing modules."""

import pytest
from unittest.mock import Mock, patch, mock_open
from pathlib import Path
import PyPDF2
import pdfplumber

from src.pdf_processor.decryptor import PDFDecryptor
from src.pdf_processor.chunker import PDFChunker
from src.pdf_processor.extractor import TableExtractor
from src.pdf_processor import PDFProcessor
from src.utils.exceptions import PDFProcessingError, DecryptionError


class TestPDFDecryptor:
    """Test cases for PDFDecryptor class."""

    def test_init_with_password(self, sample_settings):
        """Test PDFDecryptor initialization with password."""
        decryptor = PDFDecryptor(sample_settings.pdf_password)
        assert decryptor.password == sample_settings.pdf_password

    def test_decrypt_success(self, sample_settings, mock_pdf_reader):
        """Test successful PDF decryption."""
        decryptor = PDFDecryptor(sample_settings.pdf_password)
        
        with patch('PyPDF2.PdfReader', return_value=mock_pdf_reader):
            result = decryptor.decrypt("test.pdf")
            assert result == mock_pdf_reader

    def test_decrypt_with_wrong_password(self, sample_settings):
        """Test PDF decryption with wrong password."""
        decryptor = PDFDecryptor("wrong_password")
        
        mock_reader = Mock()
        mock_reader.decrypt.return_value = 0  # Failed decryption
        
        with patch('PyPDF2.PdfReader', return_value=mock_reader):
            with pytest.raises(DecryptionError):
                decryptor.decrypt("test.pdf")

    def test_decrypt_file_not_found(self, sample_settings):
        """Test PDF decryption with non-existent file."""
        decryptor = PDFDecryptor(sample_settings.pdf_password)
        
        with pytest.raises(PDFProcessingError):
            decryptor.decrypt("nonexistent.pdf")

    def test_decrypt_corrupted_file(self, sample_settings):
        """Test PDF decryption with corrupted file."""
        decryptor = PDFDecryptor(sample_settings.pdf_password)
        
        with patch('PyPDF2.PdfReader', side_effect=PyPDF2.PdfReadError("Corrupted PDF")):
            with pytest.raises(PDFProcessingError):
                decryptor.decrypt("corrupted.pdf")


class TestPDFChunker:
    """Test cases for PDFChunker class."""

    def test_init(self, sample_settings):
        """Test PDFChunker initialization."""
        chunker = PDFChunker(chunk_size_mb=sample_settings.chunk_size_mb)
        assert chunker.chunk_size_mb == sample_settings.chunk_size_mb

    def test_chunk_small_file(self, sample_settings, temp_dir):
        """Test chunking a small file (should return single chunk)."""
        chunker = PDFChunker(chunk_size_mb=1)  # 1MB chunks
        
        # Create a small test file
        test_file = temp_dir / "small.pdf"
        test_file.write_bytes(b"small content")
        
        chunks = list(chunker.chunk_file(str(test_file)))
        assert len(chunks) == 1
        assert chunks[0] == str(test_file)

    def test_chunk_large_file(self, sample_settings, temp_dir):
        """Test chunking a large file."""
        chunker = PDFChunker(chunk_size_mb=1)  # 1MB chunks
        
        # Create a large test file (simulated)
        test_file = temp_dir / "large.pdf"
        large_content = b"x" * (2 * 1024 * 1024)  # 2MB
        test_file.write_bytes(large_content)
        
        chunks = list(chunker.chunk_file(str(test_file)))
        assert len(chunks) >= 2  # Should be split into at least 2 chunks

    def test_chunk_nonexistent_file(self, sample_settings):
        """Test chunking a non-existent file."""
        chunker = PDFChunker()
        
        with pytest.raises(PDFProcessingError):
            list(chunker.chunk_file("nonexistent.pdf"))


class TestTableExtractor:
    """Test cases for TableExtractor class."""

    def test_init(self, sample_settings):
        """Test TableExtractor initialization."""
        extractor = TableExtractor()
        assert extractor is not None

    def test_extract_tables_success(self, sample_pdf_data):
        """Test successful table extraction."""
        extractor = TableExtractor()
        
        with patch('pdfplumber.open') as mock_pdf:
            mock_pdf.return_value.__enter__.return_value.pages = [
                Mock(extract_tables=lambda: sample_pdf_data["tables"])
            ]
            
            result = extractor.extract_tables("test.pdf")
            assert len(result) == 1
            assert len(result[0]) == 3  # Header + 2 data rows

    def test_extract_tables_no_tables(self):
        """Test table extraction when no tables are found."""
        extractor = TableExtractor()
        
        with patch('pdfplumber.open') as mock_pdf:
            mock_pdf.return_value.__enter__.return_value.pages = [
                Mock(extract_tables=lambda: [])
            ]
            
            result = extractor.extract_tables("test.pdf")
            assert result == []

    def test_extract_tables_with_merge_cells(self):
        """Test table extraction with merged cells."""
        extractor = TableExtractor()
        
        tables_with_merges = [
            [
                ["Header 1", "Header 2"],
                ["Data 1", None],  # Merged cell
                ["Data 2", "Data 3"],
            ]
        ]
        
        with patch('pdfplumber.open') as mock_pdf:
            mock_pdf.return_value.__enter__.return_value.pages = [
                Mock(extract_tables=lambda: tables_with_merges)
            ]
            
            result = extractor.extract_tables("test.pdf")
            assert len(result) == 1
            assert result[0][1][1] is None  # Merged cell should be None

    def test_extract_tables_corrupted_pdf(self):
        """Test table extraction from corrupted PDF."""
        extractor = TableExtractor()
        
        with patch('pdfplumber.open', side_effect=Exception("Corrupted PDF")):
            with pytest.raises(PDFProcessingError):
                extractor.extract_tables("corrupted.pdf")


class TestPDFProcessor:
    """Test cases for PDFProcessor class."""

    def test_init(self, sample_settings):
        """Test PDFProcessor initialization."""
        processor = PDFProcessor(sample_settings)
        assert processor.settings == sample_settings
        assert processor.decryptor is not None
        assert processor.chunker is not None
        assert processor.extractor is not None

    def test_process_pdf_success(self, sample_settings, sample_pdf_data):
        """Test successful PDF processing."""
        processor = PDFProcessor(sample_settings)
        
        with patch.object(processor.decryptor, 'decrypt') as mock_decrypt, \
             patch.object(processor.extractor, 'extract_tables') as mock_extract:
            
            mock_decrypt.return_value = Mock()
            mock_extract.return_value = sample_pdf_data["tables"]
            
            result = processor.process_pdf("test.pdf")
            assert result == sample_pdf_data["tables"]

    def test_process_pdf_with_password_retry(self, sample_settings, sample_pdf_data):
        """Test PDF processing with password retry."""
        processor = PDFProcessor(sample_settings)
        
        with patch.object(processor.decryptor, 'decrypt') as mock_decrypt, \
             patch.object(processor.extractor, 'extract_tables') as mock_extract:
            
            # First attempt fails, second succeeds
            mock_decrypt.side_effect = [
                DecryptionError("Wrong password"),
                Mock()  # Success on retry
            ]
            mock_extract.return_value = sample_pdf_data["tables"]
            
            result = processor.process_pdf("test.pdf")
            assert result == sample_pdf_data["tables"]
            assert mock_decrypt.call_count == 2

    def test_process_pdf_max_retries_exceeded(self, sample_settings):
        """Test PDF processing when max retries are exceeded."""
        processor = PDFProcessor(sample_settings)
        
        with patch.object(processor.decryptor, 'decrypt') as mock_decrypt:
            mock_decrypt.side_effect = DecryptionError("Wrong password")
            
            with pytest.raises(PDFProcessingError):
                processor.process_pdf("test.pdf")
            assert mock_decrypt.call_count == sample_settings.max_retries

    def test_process_chunked_pdf(self, sample_settings, sample_pdf_data):
        """Test processing a chunked PDF."""
        processor = PDFProcessor(sample_settings)
        
        with patch.object(processor.chunker, 'chunk_file') as mock_chunk, \
             patch.object(processor, '_process_chunk') as mock_process_chunk:
            
            mock_chunk.return_value = ["chunk1.pdf", "chunk2.pdf"]
            mock_process_chunk.side_effect = [
                sample_pdf_data["tables"][:1],  # First chunk
                sample_pdf_data["tables"][1:],  # Second chunk
            ]
            
            result = processor.process_chunked_pdf("large.pdf")
            assert len(result) == 2
            assert mock_process_chunk.call_count == 2

    def test_is_transaction_table_valid(self, sample_settings):
        """Test transaction table validation."""
        processor = PDFProcessor(sample_settings)
        
        # Valid transaction table
        valid_table = [
            ["Receipt No.", "Completion Time", "Details", "Transaction Status", "Paid In", "Withdrawn", "Balance"],
            ["ABC123", "01/01/2023", "Test", "Completed", "100.00", "0.00", "1000.00"],
        ]
        
        assert processor.is_transaction_table(valid_table) is True

    def test_is_transaction_table_invalid_headers(self, sample_settings):
        """Test transaction table validation with invalid headers."""
        processor = PDFProcessor(sample_settings)
        
        # Invalid table (wrong headers)
        invalid_table = [
            ["Header1", "Header2", "Header3"],
            ["Data1", "Data2", "Data3"],
        ]
        
        assert processor.is_transaction_table(invalid_table) is False

    def test_is_transaction_table_insufficient_columns(self, sample_settings):
        """Test transaction table validation with insufficient columns."""
        processor = PDFProcessor(sample_settings)
        
        # Invalid table (insufficient columns)
        invalid_table = [
            ["Receipt No.", "Completion Time"],  # Only 2 columns
            ["ABC123", "01/01/2023"],
        ]
        
        assert processor.is_transaction_table(invalid_table) is False

    def test_filter_transaction_tables(self, sample_settings):
        """Test filtering transaction tables from mixed tables."""
        processor = PDFProcessor(sample_settings)
        
        mixed_tables = [
            # Valid transaction table
            [
                ["Receipt No.", "Completion Time", "Details", "Transaction Status", "Paid In", "Withdrawn", "Balance"],
                ["ABC123", "01/01/2023", "Test", "Completed", "100.00", "0.00", "1000.00"],
            ],
            # Invalid table
            [
                ["Header1", "Header2"],
                ["Data1", "Data2"],
            ],
            # Another valid transaction table
            [
                ["Receipt No.", "Completion Time", "Details", "Transaction Status", "Paid In", "Withdrawn", "Balance"],
                ["DEF456", "02/01/2023", "Test2", "Completed", "200.00", "0.00", "1200.00"],
            ],
        ]
        
        result = processor.filter_transaction_tables(mixed_tables)
        assert len(result) == 2  # Should filter to only valid transaction tables