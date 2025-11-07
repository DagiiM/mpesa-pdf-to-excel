"""Validation utilities for PDF processing system."""

import os
from typing import List, Optional

from src.config.settings import (
    MAX_FILE_SIZE_MB,
    SUPPORTED_PDF_FORMATS,
)


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


def validate_file_path(file_path: str) -> None:
    """Validate that a file path exists and is accessible.
    
    Args:
        file_path: Path to the file to validate.
        
    Raises:
        ValidationError: If file path is invalid.
    """
    if not file_path:
        raise ValidationError("File path cannot be empty")
    
    if not os.path.exists(file_path):
        raise ValidationError(f"File does not exist: {file_path}")
    
    if not os.path.isfile(file_path):
        raise ValidationError(f"Path is not a file: {file_path}")
    
    if not os.access(file_path, os.R_OK):
        raise ValidationError(f"File is not readable: {file_path}")


def validate_file_size(file_path: str, max_size_mb: int = MAX_FILE_SIZE_MB) -> None:
    """Validate file size against maximum allowed size.
    
    Args:
        file_path: Path to the file to validate.
        max_size_mb: Maximum allowed file size in MB.
        
    Raises:
        ValidationError: If file size exceeds limit.
    """
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)
    
    if file_size_mb > max_size_mb:
        raise ValidationError(
            f"File size {file_size_mb:.2f}MB exceeds maximum "
            f"allowed size {max_size_mb}MB"
        )


def validate_file_extension(
    file_path: str,
    supported_formats: List[str] = SUPPORTED_PDF_FORMATS
) -> None:
    """Validate file extension against supported formats.
    
    Args:
        file_path: Path to the file to validate.
        supported_formats: List of supported file extensions.
        
    Raises:
        ValidationError: If file extension is not supported.
    """
    _, ext = os.path.splitext(file_path.lower())
    
    if ext not in supported_formats:
        raise ValidationError(
            f"File extension '{ext}' not supported. "
            f"Supported formats: {', '.join(supported_formats)}"
        )


def validate_pdf_file(file_path: str) -> None:
    """Perform comprehensive PDF file validation.
    
    Args:
        file_path: Path to the PDF file to validate.
        
    Raises:
        ValidationError: If any validation fails.
    """
    validate_file_path(file_path)
    validate_file_size(file_path)
    validate_file_extension(file_path)


def validate_directory_path(dir_path: str) -> None:
    """Validate that a directory path exists and is writable.
    
    Args:
        dir_path: Path to the directory to validate.
        
    Raises:
        ValidationError: If directory path is invalid.
    """
    if not dir_path:
        raise ValidationError("Directory path cannot be empty")
    
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path, exist_ok=True)
        except OSError as e:
            raise ValidationError(f"Cannot create directory {dir_path}: {str(e)}")
    
    if not os.path.isdir(dir_path):
        raise ValidationError(f"Path is not a directory: {dir_path}")
    
    if not os.access(dir_path, os.W_OK):
        raise ValidationError(f"Directory is not writable: {dir_path}")


def validate_password(password: str) -> None:
    """Validate PDF password.
    
    Args:
        password: Password to validate.
        
    Raises:
        ValidationError: If password is invalid.
    """
    if not isinstance(password, str):
        raise ValidationError("Password must be a string")
    
    if len(password.strip()) == 0:
        raise ValidationError("Password cannot be empty or whitespace only")


def validate_chunk_size(chunk_size: int, max_size: int = 100) -> None:
    """Validate chunk size parameter.
    
    Args:
        chunk_size: Chunk size to validate.
        max_size: Maximum allowed chunk size.
        
    Raises:
        ValidationError: If chunk size is invalid.
    """
    if not isinstance(chunk_size, int):
        raise ValidationError("Chunk size must be an integer")
    
    if chunk_size <= 0:
        raise ValidationError("Chunk size must be positive")
    
    if chunk_size > max_size:
        raise ValidationError(
            f"Chunk size {chunk_size} exceeds maximum allowed size {max_size}"
        )


def validate_page_range(
    start_page: int,
    end_page: Optional[int] = None,
    total_pages: Optional[int] = None
) -> None:
    """Validate page range parameters.
    
    Args:
        start_page: Starting page number (1-based).
        end_page: Optional ending page number.
        total_pages: Optional total number of pages in document.
        
    Raises:
        ValidationError: If page range is invalid.
    """
    if not isinstance(start_page, int):
        raise ValidationError("Start page must be an integer")
    
    if start_page < 1:
        raise ValidationError("Start page must be at least 1")
    
    if end_page is not None:
        if not isinstance(end_page, int):
            raise ValidationError("End page must be an integer")
        
        if end_page < start_page:
            raise ValidationError("End page must be greater than or equal to start page")
        
        if total_pages is not None and end_page > total_pages:
            raise ValidationError(
                f"End page {end_page} exceeds total pages {total_pages}"
            )