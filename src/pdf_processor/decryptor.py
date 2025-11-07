"""PDF decryption utilities for processing encrypted bank statements."""

import os
from typing import Optional

import PyPDF2
from PyPDF2 import PdfReader

from src.config.settings import DEFAULT_PASSWORD_FILE
from src.utils.logger import get_logger
from src.utils.validators import ValidationError, validate_password


class PDFDecryptionError(Exception):
    """Custom exception for PDF decryption errors."""
    pass


class PDFDecryptor:
    """Handles PDF decryption operations."""
    
    def __init__(self) -> None:
        """Initialize PDF decryptor."""
        self.logger = get_logger(__name__)
    
    def load_default_password(self, password_file: str = DEFAULT_PASSWORD_FILE) -> str:
        """Load default password from file.
        
        Args:
            password_file: Path to password file.
            
        Returns:
            Default password string.
            
        Raises:
            PDFDecryptionError: If password cannot be loaded.
        """
        try:
            if not os.path.exists(password_file):
                raise PDFDecryptionError(f"Password file not found: {password_file}")
            
            with open(password_file, 'r', encoding='utf-8') as f:
                password_line = f.readline().strip()
                
            # Extract password from format like "password-\"110281\""
            if password_line.startswith('password-'):
                password = password_line[9:]  # Remove "password-" prefix
                password = password.strip('"')  # Remove quotes
            else:
                password = password_line
            
            validate_password(password)
            self.logger.info(f"Loaded default password from {password_file}")
            return password
            
        except (OSError, ValidationError) as e:
            raise PDFDecryptionError(f"Failed to load password: {str(e)}")
    
    def is_encrypted(self, pdf_path: str) -> bool:
        """Check if PDF file is encrypted.
        
        Args:
            pdf_path: Path to PDF file.
            
        Returns:
            True if PDF is encrypted, False otherwise.
            
        Raises:
            PDFDecryptionError: If PDF cannot be read.
        """
        try:
            with open(pdf_path, 'rb') as file:
                reader = PdfReader(file)
                return reader.is_encrypted
                
        except Exception as e:
            raise PDFDecryptionError(f"Failed to check encryption status: {str(e)}")
    
    def decrypt_pdf(
        self,
        pdf_path: str,
        password: Optional[str] = None,
        use_default: bool = True
    ) -> PdfReader:
        """Decrypt PDF file and return reader object.
        
        Args:
            pdf_path: Path to encrypted PDF file.
            password: Optional password to try.
            use_default: Whether to try default password if provided one fails.
            
        Returns:
            Decrypted PdfReader object.
            
        Raises:
            PDFDecryptionError: If decryption fails.
        """
        try:
            with open(pdf_path, 'rb') as file:
                reader = PdfReader(file)
                
                if not reader.is_encrypted:
                    self.logger.info(f"PDF {pdf_path} is not encrypted")
                    return reader
                
                passwords_to_try = []
                
                if password:
                    passwords_to_try.append(password)
                
                if use_default:
                    try:
                        default_password = self.load_default_password()
                        if default_password not in passwords_to_try:
                            passwords_to_try.append(default_password)
                    except PDFDecryptionError:
                        self.logger.warning("Could not load default password")
                
                # Try each password
                for pwd in passwords_to_try:
                    if reader.decrypt(pwd):
                        self.logger.info(f"Successfully decrypted PDF with password")
                        return reader
                
                # If no password worked, raise error
                raise PDFDecryptionError(
                    "Failed to decrypt PDF with provided passwords"
                )
                
        except PyPDF2.PdfReadError as e:
            raise PDFDecryptionError(f"PDF read error: {str(e)}")
        except Exception as e:
            raise PDFDecryptionError(f"Unexpected error during decryption: {str(e)}")
    
    def verify_decryption(self, reader: PdfReader) -> bool:
        """Verify that PDF is properly decrypted.
        
        Args:
            reader: PdfReader object to verify.
            
        Returns:
            True if PDF is properly decrypted.
        """
        try:
            # Try to access first page to verify decryption
            if len(reader.pages) > 0:
                _ = reader.pages[0]
                return True
            return False
            
        except Exception:
            return False
    
    def get_pdf_info(self, reader: PdfReader) -> dict:
        """Extract metadata from decrypted PDF.
        
        Args:
            reader: Decrypted PdfReader object.
            
        Returns:
            Dictionary containing PDF metadata.
        """
        try:
            metadata = reader.metadata or {}
            
            info = {
                "title": metadata.get('/Title', ''),
                "author": metadata.get('/Author', ''),
                "subject": metadata.get('/Subject', ''),
                "creator": metadata.get('/Creator', ''),
                "producer": metadata.get('/Producer', ''),
                "creation_date": metadata.get('/CreationDate', ''),
                "modification_date": metadata.get('/ModDate', ''),
                "page_count": len(reader.pages),
                "is_encrypted": reader.is_encrypted,
            }
            
            return info
            
        except Exception as e:
            self.logger.warning(f"Failed to extract PDF metadata: {str(e)}")
            return {
                "page_count": len(reader.pages) if reader.pages else 0,
                "is_encrypted": reader.is_encrypted if reader else False,
            }