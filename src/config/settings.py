"""Configuration settings for PDF processing system."""

import os
import json
from typing import Dict, Any, Optional, List
from pathlib import Path
from dataclasses import dataclass, asdict, field, fields
from datetime import datetime, timedelta

# Environment variables
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
DEBUG = os.getenv("DEBUG", "False").lower() == "true"

# PDF Processing Configuration
MAX_CHUNK_SIZE_MB = int(os.getenv("MAX_CHUNK_SIZE_MB", "10"))
MAX_PAGES_PER_CHUNK = int(os.getenv("MAX_PAGES_PER_CHUNK", "50"))
SUPPORTED_PDF_FORMATS = [".pdf"]

# Celery Configuration
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TIMEZONE = "UTC"

# File Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REPORTS_DIR = os.getenv("REPORTS_DIR", os.path.join(BASE_DIR, "reports"))
LOGS_DIR = os.getenv("LOGS_DIR", os.path.join(BASE_DIR, "logs"))
TEMP_DIR = os.getenv("TEMP_DIR", os.path.join(BASE_DIR, "temp"))

# Security
DEFAULT_PASSWORD_FILE = os.getenv("DEFAULT_PASSWORD_FILE", "password.txt")
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "100"))

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Excel Output Configuration
EXCEL_OUTPUT_FORMAT = os.getenv("EXCEL_OUTPUT_FORMAT", "xlsx")
INCLUDE_METADATA = os.getenv("INCLUDE_METADATA", "True").lower() == "true"

# Currency Configuration
DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "KES")
CURRENCY_SYMBOL = os.getenv("CURRENCY_SYMBOL", "KES")

# Processing Configuration
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "60"))
CONCURRENT_WORKERS = int(os.getenv("CONCURRENT_WORKERS", "4"))


@dataclass
class Settings:
    """Configuration settings class."""
    
    # PDF Processing
    pdf_password: Optional[str] = None
    chunk_size_mb: int = 5
    max_retries: int = 3
    max_pages_per_chunk: int = 50
    
    # Output Configuration
    output_dir: str = "reports"
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Currency Configuration
    currency_symbol: str = "KES"
    default_currency: str = "KES"
    
    # File System
    base_dir: str = BASE_DIR
    reports_dir: str = REPORTS_DIR
    logs_dir: str = LOGS_DIR
    temp_dir: str = TEMP_DIR
    
    # Security
    default_password_file: str = DEFAULT_PASSWORD_FILE
    max_file_size_mb: int = 100
    supported_pdf_formats: List[str] = field(default_factory=lambda: SUPPORTED_PDF_FORMATS.copy())
    
    # Celery Configuration
    celery_broker_url: str = CELERY_BROKER_URL
    celery_result_backend: str = CELERY_RESULT_BACKEND
    celery_task_serializer: str = CELERY_TASK_SERIALIZER
    celery_result_serializer: str = CELERY_RESULT_SERIALIZER
    celery_accept_content: List[str] = field(default_factory=lambda: CELERY_ACCEPT_CONTENT.copy())
    celery_timezone: str = CELERY_TIMEZONE
    
    # Processing Configuration
    excel_output_format: str = EXCEL_OUTPUT_FORMAT
    include_metadata: bool = INCLUDE_METADATA
    retry_delay_seconds: int = RETRY_DELAY_SECONDS
    concurrent_workers: int = CONCURRENT_WORKERS
    
    @classmethod
    def from_env(cls) -> "Settings":
        """Create Settings from environment variables."""
        return cls(
            pdf_password=os.getenv("PDF_PASSWORD"),
            chunk_size_mb=int(os.getenv("CHUNK_SIZE_MB", "5")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            max_pages_per_chunk=int(os.getenv("MAX_PAGES_PER_CHUNK", "50")),
            output_dir=os.getenv("OUTPUT_DIR", "reports"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_format=os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
            currency_symbol=os.getenv("CURRENCY_SYMBOL", "KES"),
            default_currency=os.getenv("DEFAULT_CURRENCY", "KES"),
            base_dir=os.getenv("BASE_DIR", BASE_DIR),
            reports_dir=os.getenv("REPORTS_DIR", REPORTS_DIR),
            logs_dir=os.getenv("LOGS_DIR", LOGS_DIR),
            temp_dir=os.getenv("TEMP_DIR", TEMP_DIR),
            default_password_file=os.getenv("DEFAULT_PASSWORD_FILE", "password.txt"),
            max_file_size_mb=int(os.getenv("MAX_FILE_SIZE_MB", "100")),
            celery_broker_url=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
            celery_result_backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0"),
            excel_output_format=os.getenv("EXCEL_OUTPUT_FORMAT", "xlsx"),
            include_metadata=os.getenv("INCLUDE_METADATA", "True").lower() == "true",
            retry_delay_seconds=int(os.getenv("RETRY_DELAY_SECONDS", "60")),
            concurrent_workers=int(os.getenv("CONCURRENT_WORKERS", "4")),
        )
    
    def validate(self) -> bool:
        """Validate settings."""
        return (
            self.chunk_size_mb > 0 and
            self.max_retries >= 0 and
            self.max_pages_per_chunk > 0 and
            len(self.currency_symbol) > 0 and
            self.max_file_size_mb > 0 and
            self.concurrent_workers > 0
        )
    
    def get_log_level(self) -> str:
        """Get log level as string."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() in valid_levels:
            return self.log_level.upper()
        return "INFO"
    
    def get_output_path(self, filename: str) -> str:
        """Get full output path for filename."""
        return os.path.join(self.output_dir, filename)
    
    def create_directories(self) -> None:
        """Create necessary directories."""
        directories = [self.output_dir, self.logs_dir, self.reports_dir, self.temp_dir]
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Settings":
        """Create settings from dictionary."""
        return cls(**data)
    
    def update(self, data: Dict[str, Any]) -> None:
        """Update settings from dictionary."""
        for key, value in data.items():
            if hasattr(self, key):
                setattr(self, key, value)
    
    def clone(self) -> "Settings":
        """Create a copy of settings."""
        return self.from_dict(self.to_dict())
    
    def get_currency_display(self, amount: Optional[float] = None) -> str:
        """Get currency display string."""
        if amount is not None:
            return f"{self.currency_symbol} {amount:,.2f}"
        return self.currency_symbol
    
    def get_max_chunk_size_bytes(self) -> int:
        """Get max chunk size in bytes."""
        return self.chunk_size_mb * 1024 * 1024
    
    def is_debug_enabled(self) -> bool:
        """Check if debug mode is enabled."""
        return self.log_level.upper() == "DEBUG"
    
    def get_retry_delay(self, attempt: int) -> int:
        """Get retry delay for attempt number."""
        # Exponential backoff with minimum of 1 second
        return max(1, 2 ** (attempt - 1))


def get_config() -> Dict[str, Any]:
    """Get all configuration as a dictionary.
    
    Returns:
        Dictionary containing all configuration values.
    """
    return {
        "environment": ENVIRONMENT,
        "debug": DEBUG,
        "max_chunk_size_mb": MAX_CHUNK_SIZE_MB,
        "max_pages_per_chunk": MAX_PAGES_PER_CHUNK,
        "celery_broker_url": CELERY_BROKER_URL,
        "celery_result_backend": CELERY_RESULT_BACKEND,
        "reports_dir": REPORTS_DIR,
        "logs_dir": LOGS_DIR,
        "temp_dir": TEMP_DIR,
        "default_password_file": DEFAULT_PASSWORD_FILE,
        "max_file_size_mb": MAX_FILE_SIZE_MB,
        "log_level": LOG_LEVEL,
        "excel_output_format": EXCEL_OUTPUT_FORMAT,
        "include_metadata": INCLUDE_METADATA,
        "max_retries": MAX_RETRIES,
        "retry_delay_seconds": RETRY_DELAY_SECONDS,
        "concurrent_workers": CONCURRENT_WORKERS,
    }


def ensure_directories() -> None:
    """Ensure all required directories exist."""
    directories = [REPORTS_DIR, LOGS_DIR, TEMP_DIR]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)


def load_config_from_file(file_path: str) -> Dict[str, Any]:
    """Load configuration from file."""
    with open(file_path, 'r') as f:
        return json.load(f)


def save_config_to_file(config: Dict[str, Any], file_path: str) -> None:
    """Save configuration to file."""
    with open(file_path, 'w') as f:
        json.dump(config, f, indent=2)


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration dictionary."""
    required_fields = ['chunk_size_mb', 'max_retries', 'log_level']
    return all(field in config for field in required_fields)


def get_default_config() -> Dict[str, Any]:
    """Get default configuration."""
    return {
        "pdf_password": None,
        "chunk_size_mb": 5,
        "max_retries": 3,
        "output_dir": "reports",
        "log_level": "INFO",
        "currency_symbol": "KES",
        "default_currency": "KES",
    }


def merge_configs(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Merge two configuration dictionaries."""
    result = base.copy()
    result.update(override)
    return result


def get_environment_config() -> Dict[str, Any]:
    """Get environment-specific configuration."""
    env = ENVIRONMENT.lower()
    
    if env == "production":
        return {
            "log_level": "INFO",
            "max_retries": 5,
            "concurrent_workers": 8,
        }
    elif env == "development":
        return {
            "log_level": "DEBUG",
            "max_retries": 1,
            "concurrent_workers": 2,
        }
    else:
        return {
            "log_level": "INFO",
            "max_retries": 3,
            "concurrent_workers": 4,
        }


def load_workspace_config() -> Optional[Dict[str, Any]]:
    """Load workspace-specific configuration."""
    workspace_config = ".pdf_processor_config"
    
    if os.path.exists(workspace_config):
        return load_config_from_file(workspace_config)
    
    return None