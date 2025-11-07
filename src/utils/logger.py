"""Logging configuration and utilities for PDF processing system."""

import logging
import os
from typing import Optional

from src.config.settings import LOG_LEVEL, LOG_FORMAT, LOGS_DIR


def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: str = LOG_LEVEL
) -> logging.Logger:
    """Set up a logger with console and file handlers.
    
    Args:
        name: Logger name.
        log_file: Optional log file name. If None, uses logger name.
        level: Logging level.
        
    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    if log_file is None:
        log_file = f"{name}.log"
    
    log_path = os.path.join(LOGS_DIR, log_file)
    os.makedirs(LOGS_DIR, exist_ok=True)
    
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance.
    
    Args:
        name: Logger name.
        
    Returns:
        Logger instance.
    """
    return logging.getLogger(name)


class ProcessingLogger:
    """Specialized logger for PDF processing operations."""
    
    def __init__(self, task_id: str) -> None:
        """Initialize processing logger.
        
        Args:
            task_id: Unique identifier for the processing task.
        """
        self.task_id = task_id
        self.logger = setup_logger(f"processing.{task_id}")
    
    def log_start(self, file_path: str) -> None:
        """Log processing start.
        
        Args:
            file_path: Path to the file being processed.
        """
        self.logger.info(f"Started processing task {self.task_id} for file: {file_path}")
    
    def log_progress(self, message: str) -> None:
        """Log processing progress.
        
        Args:
            message: Progress message.
        """
        self.logger.info(f"Task {self.task_id}: {message}")
    
    def log_error(self, error: Exception, context: str = "") -> None:
        """Log processing error.
        
        Args:
            error: Exception that occurred.
            context: Additional context information.
        """
        error_msg = f"Task {self.task_id}: Error in {context}: {str(error)}"
        self.logger.error(error_msg, exc_info=True)
    
    def log_completion(self, output_path: str) -> None:
        """Log processing completion.
        
        Args:
            output_path: Path to the generated output file.
        """
        self.logger.info(f"Task {self.task_id}: Completed successfully. Output: {output_path}")