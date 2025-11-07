"""PDF data extraction utilities for bank statement processing."""

import re
from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal

import pdfplumber
import pandas as pd
from PyPDF2 import PdfReader

from src.utils.logger import get_logger


class PDFExtractionError(Exception):
    """Custom exception for PDF extraction errors."""
    pass


class TransactionData:
    """Data class for transaction information."""
    
    def __init__(
        self,
        date: str,
        description: str,
        debit: Optional[Decimal] = None,
        credit: Optional[Decimal] = None,
        balance: Optional[Decimal] = None,
        reference: Optional[str] = None
    ) -> None:
        """Initialize transaction data.
        
        Args:
            date: Transaction date.
            description: Transaction description.
            debit: Debit amount (optional).
            credit: Credit amount (optional).
            balance: Running balance (optional).
            reference: Transaction reference (optional).
        """
        self.date = date
        self.description = description
        self.debit = debit
        self.credit = credit
        self.balance = balance
        self.reference = reference
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert transaction to dictionary.
        
        Returns:
            Dictionary representation of transaction.
        """
        return {
            "Date": self.date,
            "Description": self.description,
            "Debit": float(self.debit) if self.debit else None,
            "Credit": float(self.credit) if self.credit else None,
            "Balance": float(self.balance) if self.balance else None,
            "Reference": self.reference,
        }


class PDFExtractor:
    """Handles data extraction from PDF bank statements."""
    
    def __init__(self) -> None:
        """Initialize PDF extractor."""
        self.logger = get_logger(__name__)
        
        # Common patterns for transaction extraction
        self.date_patterns = [
            r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b',  # DD/MM/YYYY or DD-MM-YYYY
            r'\b(\d{4}[/-]\d{1,2}[/-]\d{1,2})\b',  # YYYY/MM/DD or YYYY-MM-DD
            r'\b(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{2,4})\b',  # DD Mon YYYY
        ]
        
        self.amount_patterns = [
            r'\b(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\b',  # 1,234.56
            r'\b(\d+(?:\.\d{2})?)\b',  # 1234.56
        ]
        
        # Keywords to identify transaction sections
        self.transaction_keywords = [
            'transaction', 'debit', 'credit', 'balance', 'withdrawal', 'deposit',
            'payment', 'transfer', 'charge', 'fee', 'interest'
        ]
    
    def extract_text_from_pdf(self, pdf_path: str, password: Optional[str] = None) -> List[str]:
        """Extract text content from PDF file.
        
        Args:
            pdf_path: Path to PDF file.
            
        Returns:
            List of text strings, one per page.
            
        Raises:
            PDFExtractionError: If text extraction fails.
        """
        try:
            text_content = []
            
            with pdfplumber.open(pdf_path, password=password) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    try:
                        page_text = page.extract_text()
                        if page_text:
                            text_content.append(page_text)
                            self.logger.debug(f"Extracted text from page {page_num}")
                        else:
                            self.logger.warning(f"No text found on page {page_num}")
                    except Exception as e:
                        self.logger.warning(f"Failed to extract text from page {page_num}: {str(e)}")
                        continue
            
            if not text_content:
                raise PDFExtractionError("No text content extracted from PDF")
            
            self.logger.info(f"Extracted text from {len(text_content)} pages")
            return text_content
            
        except Exception as e:
            raise PDFExtractionError(f"Failed to extract text from PDF: {str(e)}")
    
    def extract_tables_from_pdf(self, pdf_path: str, password: Optional[str] = None) -> List[List[List[str]]]:
        """Extract table data from PDF file.
        
        Args:
            pdf_path: Path to PDF file.
            
        Returns:
            List of tables, where each table is a list of rows and each row is a list of cells.
            
        Raises:
            PDFExtractionError: If table extraction fails.
        """
        try:
            all_tables = []
            
            with pdfplumber.open(pdf_path, password=password) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    try:
                        tables = page.extract_tables()
                        if tables:
                            all_tables.extend(tables)
                            self.logger.debug(f"Extracted {len(tables)} tables from page {page_num}")
                    except Exception as e:
                        self.logger.warning(f"Failed to extract tables from page {page_num}: {str(e)}")
                        continue
            
            self.logger.info(f"Extracted total of {len(all_tables)} tables")
            return all_tables
            
        except Exception as e:
            raise PDFExtractionError(f"Failed to extract tables from PDF: {str(e)}")
    
    def parse_amount(self, amount_str: str) -> Optional[Decimal]:
        """Parse amount string to Decimal.
        
        Args:
            amount_str: String containing amount.
            
        Returns:
            Parsed Decimal amount or None if parsing fails.
        """
        try:
            if not amount_str or amount_str.strip() in ['', '-', '0.00', '0']:
                return None
                
            # Remove currency symbols and whitespace, but keep negative sign
            clean_amount = re.sub(r'[^\d.,-]', '', amount_str.strip())
            
            if not clean_amount:
                return None
            
            # Handle different decimal separators
            if ',' in clean_amount and '.' in clean_amount:
                # If both exist, assume comma is thousands separator
                clean_amount = clean_amount.replace(',', '')
            elif ',' in clean_amount:
                # Check if comma is decimal separator (last occurrence)
                last_comma = clean_amount.rfind(',')
                if len(clean_amount) - last_comma - 1 <= 2:
                    # Likely decimal separator
                    clean_amount = clean_amount.replace(',', '.')
                else:
                    # Likely thousands separator
                    clean_amount = clean_amount.replace(',', '')
            
            return Decimal(str(clean_amount))
            
        except (ValueError, TypeError):
            return None
    
    def parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to standardized format.
        
        Args:
            date_str: String containing date.
            
        Returns:
            Standardized date string (YYYY-MM-DD) or None if parsing fails.
        """
        try:
            from datetime import datetime
            
            # Try different date patterns
            for pattern in self.date_patterns:
                match = re.search(pattern, date_str, re.IGNORECASE)
                if match:
                    date_value = match.group(1)
                    # Normalize date format
                    date_value = date_value.replace('/', '-').replace(' ', '-')
                    
                    # Handle different formats
                    parts = date_value.split('-')
                    if len(parts) == 3:
                        # Try to determine format
                        if len(parts[0]) == 4:  # YYYY-MM-DD
                            year = parts[0]
                            month = parts[1]
                            day = parts[2]
                        elif len(parts[2]) == 4:  # DD-MM-YYYY
                            year = parts[2]
                            month = parts[1]
                            day = parts[0]
                        elif len(parts[2]) == 2:  # DD-MM-YY
                            year = f"20{parts[2]}" if int(parts[2]) < 50 else f"19{parts[2]}"
                            month = parts[1]
                            day = parts[0]
                        else:
                            continue
                        
                        # Handle month names (Nov, Jan, etc.)
                        if not month.isdigit():
                            # Convert month name to number
                            try:
                                month_num = datetime.strptime(month, '%b').month
                                month = f"{month_num:02d}"
                            except ValueError:
                                continue
                        else:
                            month = month.zfill(2)
                        
                        day = day.zfill(2)
                        
                        # Validate the date
                        try:
                            datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d')
                            return f"{year}-{month}-{day}"
                        except ValueError:
                            continue
            
            return None
            
        except Exception:
            return None
    
    def extract_transactions_from_table(self, table: List[List[str]]) -> List[TransactionData]:
        """Extract transactions from table data.
        
        Args:
            table: Table data as list of rows.
            
        Returns:
            List of TransactionData objects.
        """
        transactions = []
        
        try:
            if not table or len(table) < 2:
                return transactions
            
            # Find header row to understand column structure
            header_row = None
            header_row_idx = -1
            
            for idx, row in enumerate(table):
                if row and len(row) >= 6:  # Expect at least 6 columns for M-PESA format
                    row_str = ' '.join([str(cell).upper() for cell in row])
                    if ('PAID IN' in row_str and ('WITHDRAWN' in row_str or 'PAID OUT' in row_str) and 'BALANCE' in row_str):
                        header_row = row
                        header_row_idx = idx
                        break
            
            if not header_row:
                # Don't log warning for tables that don't contain transaction data
                # This is normal for summary tables, verification codes, etc.
                return transactions
            
            # Map column indices
            col_mapping = {}
            for idx, header in enumerate(header_row):
                header_str = str(header).upper().strip()
                if 'COMPLETION TIME' in header_str or 'TIME' in header_str:
                    col_mapping['date'] = idx
                elif 'DETAILS' in header_str:
                    col_mapping['description'] = idx
                elif 'PAID IN' in header_str:
                    col_mapping['credit'] = idx
                elif 'WITHDRAWN' in header_str or 'PAID OUT' in header_str:
                    col_mapping['debit'] = idx
                elif 'BALANCE' in header_str:
                    col_mapping['balance'] = idx
            
            # Extract transaction rows (skip header and summary rows)
            for row_num in range(header_row_idx + 1, len(table)):
                row = table[row_num]
                
                # Skip rows that don't have enough columns
                if len(row) < len(header_row):
                    continue
                
                # Skip summary rows
                row_str = ' '.join([str(cell) for cell in row]).upper()
                if any(keyword in row_str for keyword in ['TOTAL', 'SUMMARY', 'STATEMENT PERIOD']):
                    continue
                
                try:
                    # Extract date from completion time column
                    date_str = ""
                    if 'date' in col_mapping:
                        date_cell = str(row[col_mapping['date']]).strip()
                        # Extract date part from datetime string
                        if ' ' in date_cell:
                            date_str = date_cell.split(' ')[0]
                        else:
                            date_str = date_cell
                    
                    parsed_date = self.parse_date(date_str)
                    if not parsed_date:
                        continue
                    
                    # Extract description
                    description = ""
                    if 'description' in col_mapping:
                        description = str(row[col_mapping['description']]).strip()
                    
                    # Extract amounts
                    credit = None
                    debit = None
                    balance = None
                    
                    # Extract credit (Paid In)
                    if 'credit' in col_mapping:
                        credit_str = str(row[col_mapping['credit']]).strip()
                        if credit_str and credit_str not in ['', '-', '0.00', '0']:
                            credit = self.parse_amount(credit_str)
                    
                    # Extract debit (Withdrawn)
                    if 'debit' in col_mapping:
                        debit_str = str(row[col_mapping['debit']]).strip()
                        if debit_str and debit_str not in ['', '-', '0.00', '0']:
                            # Handle negative values in debit column
                            if debit_str.startswith('-'):
                                debit = self.parse_amount(debit_str[1:])  # Remove minus sign
                            else:
                                debit = self.parse_amount(debit_str)
                    
                    # Extract balance
                    if 'balance' in col_mapping:
                        balance_str = str(row[col_mapping['balance']]).strip()
                        if balance_str and balance_str not in ['', '-', '0.00', '0']:
                            balance = self.parse_amount(balance_str)
                    
                    # Create transaction if we have meaningful data
                    if credit or debit:
                        transaction = TransactionData(
                            date=parsed_date,
                            description=description,
                            debit=debit,
                            credit=credit,
                            balance=balance
                        )
                        transactions.append(transaction)
                
                except Exception as e:
                    self.logger.debug(f"Failed to process row {row_num}: {str(e)}")
                    continue
            
            return transactions
            
        except Exception as e:
            self.logger.warning(f"Failed to extract transactions from table: {str(e)}")
            return []
    
    def extract_transactions_from_text(self, text_content: List[str]) -> List[TransactionData]:
        """Extract transactions from text content.
        
        Args:
            text_content: List of text strings from PDF pages.
            
        Returns:
            List of TransactionData objects.
        """
        transactions = []
        
        try:
            for page_text in text_content:
                lines = page_text.split('\n')
                
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Look for transaction patterns
                    date_match = None
                    for pattern in self.date_patterns:
                        match = re.search(pattern, line, re.IGNORECASE)
                        if match:
                            date_match = match
                            break
                    
                    if date_match:
                        parsed_date = self.parse_date(date_match.group(1))
                        if parsed_date:
                            # Extract remaining information from the line
                            parts = line.split()
                            description_parts = []
                            amounts = []
                            
                            for part in parts:
                                # Check if part looks like an amount
                                if re.search(r'\d+(?:[.,]\d{2})?', part):
                                    amount = self.parse_amount(part)
                                    if amount:
                                        amounts.append(amount)
                                else:
                                    description_parts.append(part)
                            
                            description = ' '.join(description_parts)
                            
                            # Determine debit/credit from amounts
                            debit = None
                            credit = None
                            balance = None
                            
                            if len(amounts) >= 1:
                                # Simple heuristic: if there are multiple amounts,
                                # first might be transaction amount, second might be balance
                                if len(amounts) == 2:
                                    if amounts[1] > amounts[0]:
                                        credit = amounts[0]
                                        balance = amounts[1]
                                    else:
                                        debit = amounts[0]
                                        balance = amounts[1]
                                else:
                                    # Single amount - determine if debit or credit
                                    # This logic may need refinement based on bank format
                                    if 'debit' in description.lower() or 'withdrawal' in description.lower():
                                        debit = amounts[0]
                                    else:
                                        credit = amounts[0]
                            
                            transaction = TransactionData(
                                date=parsed_date,
                                description=description,
                                debit=debit,
                                credit=credit,
                                balance=balance
                            )
                            
                            transactions.append(transaction)
            
            return transactions
            
        except Exception as e:
            self.logger.warning(f"Failed to extract transactions from text: {str(e)}")
            return []
    
    def extract_all_transactions(self, pdf_path: str, password: Optional[str] = None) -> List[TransactionData]:
        """Extract all transactions from PDF using multiple methods.
        
        Args:
            pdf_path: Path to PDF file.
            
        Returns:
            List of all TransactionData objects found.
            
        Raises:
            PDFExtractionError: If extraction fails completely.
        """
        all_transactions = []
        
        try:
            # Try table extraction first (usually more reliable)
            self.logger.info("Attempting table extraction...")
            tables = self.extract_tables_from_pdf(pdf_path, password)
            
            for table in tables:
                transactions = self.extract_transactions_from_table(table)
                all_transactions.extend(transactions)
            
            # If no transactions found from tables, try text extraction
            if not all_transactions:
                self.logger.info("No transactions from tables, trying text extraction...")
                text_content = self.extract_text_from_pdf(pdf_path, password)
                transactions = self.extract_transactions_from_text(text_content)
                all_transactions.extend(transactions)
            
            if not all_transactions:
                raise PDFExtractionError("No transactions could be extracted from PDF")
            
            # Remove duplicates and sort by date
            unique_transactions = self._deduplicate_transactions(all_transactions)
            sorted_transactions = sorted(
                unique_transactions,
                key=lambda x: (x.date, x.description)
            )
            
            self.logger.info(f"Extracted {len(sorted_transactions)} unique transactions")
            return sorted_transactions
            
        except Exception as e:
            raise PDFExtractionError(f"Failed to extract transactions: {str(e)}")
    
    def _deduplicate_transactions(self, transactions: List[TransactionData]) -> List[TransactionData]:
        """Remove duplicate transactions based on date, description, and amount.
        
        Args:
            transactions: List of transactions to deduplicate.
            
        Returns:
            List of unique transactions.
        """
        seen = set()
        unique_transactions = []
        
        for transaction in transactions:
            # Create a unique key for the transaction
            key = (
                transaction.date,
                transaction.description.lower().strip(),
                transaction.debit,
                transaction.credit
            )
            
            if key not in seen:
                seen.add(key)
                unique_transactions.append(transaction)
        
        return unique_transactions
    
    def transactions_to_dataframe(self, transactions: List[TransactionData]) -> pd.DataFrame:
        """Convert transactions list to pandas DataFrame.
        
        Args:
            transactions: List of TransactionData objects.
            
        Returns:
            pandas DataFrame containing transaction data.
        """
        data = [transaction.to_dict() for transaction in transactions]
        return pd.DataFrame(data)