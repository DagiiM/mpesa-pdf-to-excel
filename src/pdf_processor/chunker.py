"""PDF chunking utilities for processing large bank statements."""

import os
import tempfile
from typing import Generator, List, Optional

import PyPDF2
from PyPDF2 import PdfReader, PdfWriter

from src.config.settings import MAX_CHUNK_SIZE_MB, MAX_PAGES_PER_CHUNK
from src.utils.logger import get_logger
from src.utils.validators import ValidationError, validate_page_range


class PDFChunkingError(Exception):
    """Custom exception for PDF chunking errors."""
    pass


class PDFChunker:
    """Handles PDF chunking operations for large files."""
    
    def __init__(self) -> None:
        """Initialize PDF chunker."""
        self.logger = get_logger(__name__)
    
    def get_pdf_size_mb(self, pdf_path: str) -> float:
        """Get PDF file size in MB.
        
        Args:
            pdf_path: Path to PDF file.
            
        Returns:
            File size in MB.
        """
        size_bytes = os.path.getsize(pdf_path)
        return size_bytes / (1024 * 1024)
    
    def calculate_optimal_chunks(
        self,
        total_pages: int,
        file_size_mb: float,
        max_chunk_size_mb: int = MAX_CHUNK_SIZE_MB,
        max_pages_per_chunk: int = MAX_PAGES_PER_CHUNK
    ) -> List[tuple]:
        """Calculate optimal chunk ranges based on file size and page count.
        
        Args:
            total_pages: Total number of pages in PDF.
            file_size_mb: File size in MB.
            max_chunk_size_mb: Maximum chunk size in MB.
            max_pages_per_chunk: Maximum pages per chunk.
            
        Returns:
            List of (start_page, end_page) tuples for each chunk.
        """
        # Determine chunking strategy based on file size
        if file_size_mb <= max_chunk_size_mb:
            # Small file - no chunking needed
            return [(1, total_pages)]
        
        # Calculate chunks based on both size and page limits
        estimated_pages_per_size_chunk = max(
            1,
            int(total_pages * (max_chunk_size_mb / file_size_mb))
        )
        
        # Use the smaller of the two limits
        pages_per_chunk = min(estimated_pages_per_size_chunk, max_pages_per_chunk)
        
        chunks = []
        start_page = 1
        
        while start_page <= total_pages:
            end_page = min(start_page + pages_per_chunk - 1, total_pages)
            chunks.append((start_page, end_page))
            start_page = end_page + 1
        
        self.logger.info(
            f"Calculated {len(chunks)} chunks for {total_pages} pages "
            f"({file_size_mb:.2f}MB file)"
        )
        
        return chunks
    
    def create_chunk(
        self,
        reader: PdfReader,
        start_page: int,
        end_page: int,
        output_path: Optional[str] = None
    ) -> str:
        """Create a PDF chunk containing specified pages.
        
        Args:
            reader: PdfReader object.
            start_page: Starting page number (1-based).
            end_page: Ending page number (1-based).
            output_path: Optional output path for chunk file.
            
        Returns:
            Path to created chunk file.
            
        Raises:
            PDFChunkingError: If chunk creation fails.
        """
        try:
            validate_page_range(start_page, end_page, len(reader.pages))
            
            writer = PdfWriter()
            
            # Add pages to chunk (convert to 0-based index)
            for page_num in range(start_page - 1, end_page):
                writer.add_page(reader.pages[page_num])
            
            # Determine output path
            if output_path is None:
                temp_dir = tempfile.gettempdir()
                output_path = os.path.join(
                    temp_dir,
                    f"pdf_chunk_{start_page}_{end_page}.pdf"
                )
            
            # Write chunk to file
            with open(output_path, 'wb') as output_file:
                writer.write(output_file)
            
            self.logger.debug(f"Created chunk: pages {start_page}-{end_page}")
            return output_path
            
        except ValidationError as e:
            raise PDFChunkingError(f"Invalid page range: {str(e)}")
        except Exception as e:
            raise PDFChunkingError(f"Failed to create chunk: {str(e)}")
    
    def generate_chunks(
        self,
        pdf_path: str,
        reader: PdfReader,
        chunk_ranges: Optional[List[tuple]] = None,
        cleanup: bool = True
    ) -> Generator[str, None, None]:
        """Generate PDF chunks for processing.
        
        Args:
            pdf_path: Original PDF file path.
            reader: PdfReader object.
            chunk_ranges: Optional list of (start_page, end_page) tuples.
            cleanup: Whether to cleanup temporary files after use.
            
        Yields:
            Paths to chunk files.
            
        Raises:
            PDFChunkingError: If chunk generation fails.
        """
        temp_files = []
        
        try:
            if chunk_ranges is None:
                file_size_mb = self.get_pdf_size_mb(pdf_path)
                chunk_ranges = self.calculate_optimal_chunks(
                    len(reader.pages), file_size_mb
                )
            
            for start_page, end_page in chunk_ranges:
                chunk_path = self.create_chunk(reader, start_page, end_page)
                temp_files.append(chunk_path)
                yield chunk_path
            
        except Exception as e:
            # Cleanup on error
            if cleanup:
                self._cleanup_temp_files(temp_files)
            raise PDFChunkingError(f"Chunk generation failed: {str(e)}")
    
    def _cleanup_temp_files(self, file_paths: List[str]) -> None:
        """Clean up temporary chunk files.
        
        Args:
            file_paths: List of file paths to cleanup.
        """
        for file_path in file_paths:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    self.logger.debug(f"Cleaned up temporary file: {file_path}")
            except OSError as e:
                self.logger.warning(f"Failed to cleanup {file_path}: {str(e)}")
    
    def should_chunk(self, pdf_path: str) -> bool:
        """Determine if PDF should be chunked based on size.
        
        Args:
            pdf_path: Path to PDF file.
            
        Returns:
            True if PDF should be chunked.
        """
        file_size_mb = self.get_pdf_size_mb(pdf_path)
        return file_size_mb > MAX_CHUNK_SIZE_MB
    
    def get_chunking_strategy(
        self,
        pdf_path: str,
        reader: PdfReader
    ) -> dict:
        """Get chunking strategy information.
        
        Args:
            pdf_path: Path to PDF file.
            reader: PdfReader object.
            
        Returns:
            Dictionary containing chunking strategy details.
        """
        file_size_mb = self.get_pdf_size_mb(pdf_path)
        total_pages = len(reader.pages)
        should_chunk_file = self.should_chunk(pdf_path)
        
        if should_chunk_file:
            chunk_ranges = self.calculate_optimal_chunks(total_pages, file_size_mb)
        else:
            chunk_ranges = [(1, total_pages)]
        
        return {
            "file_size_mb": file_size_mb,
            "total_pages": total_pages,
            "should_chunk": should_chunk_file,
            "chunk_count": len(chunk_ranges),
            "chunk_ranges": chunk_ranges,
            "max_chunk_size_mb": MAX_CHUNK_SIZE_MB,
            "max_pages_per_chunk": MAX_PAGES_PER_CHUNK,
        }