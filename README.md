# PDF Bank Statement Processing System

A production-ready Python application for processing encrypted PDF bank statements, extracting transaction data, and generating comprehensive monthly summaries in Excel format.

## Features

- **PDF Processing**: Extract data from encrypted PDF bank statements
- **Transaction Extraction**: Intelligent extraction of transaction data from tables and text
- **Date Parsing**: Support for multiple date formats (DD/MM/YYYY, YYYY-MM-DD, DD Mon YYYY, etc.)
- **Amount Processing**: Handle various currency formats and decimal separators
- **Monthly Summaries**: Generate comprehensive monthly financial summaries
- **Excel Reports**: Create professional Excel reports with formatting and analysis
- **Batch Processing**: Process multiple PDF files in a directory
- **Background Processing**: Celery-based task queue for large-scale processing
- **Error Handling**: Comprehensive error handling and logging
- **Production Ready**: Configurable, secure, and scalable architecture

## Quick Start

### Prerequisites

- Python 3.9+
- pip (Python package manager)

### Installation

1. **Clone or download the project** to your desired directory

2. **Set up the virtual environment**:
   ```bash
   python -m venv venv
   
   # Windows
   venv\Scripts\activate
   
   # Unix/macOS
   source venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Basic Usage

#### Process a Single PDF

```bash
# With explicit password
python main.py --pdf-file statement.pdf --password yourpassword

# Automatically read password from password.txt
python main.py --pdf-file statement.pdf
```

#### Process Multiple PDFs

```bash
# Process all PDFs in a directory
python main.py --batch-dir ./statements --password-file passwords.txt
```

#### Run as Background Service

```bash
python main.py --daemon
```

## Project Structure

```
mpesa/
├── src/                          # Source code
│   ├── config/                   # Configuration settings
│   │   ├── __init__.py
│   │   └── settings.py           # Application configuration
│   ├── pdf_processor/            # PDF processing modules
│   │   ├── __init__.py
│   │   ├── decryptor.py          # PDF decryption utilities
│   │   ├── chunker.py            # Large file chunking
│   │   └── extractor.py          # Data extraction from PDFs
│   ├── excel_generator/          # Excel report generation
│   │   ├── __init__.py
│   │   ├── converter.py          # Excel conversion utilities
│   │   └── summarizer.py         # Monthly summary calculations
│   ├── tasks/                    # Background task processing
│   │   ├── __init__.py
│   │   └── celery_app.py         # Celery configuration
│   └── utils/                    # Utility modules
│       ├── __init__.py
│       ├── logger.py             # Logging configuration
│       └── validators.py         # Input validation
├── reports/                      # Generated Excel reports
├── logs/                         # Application logs
├── tests/                        # Test files
├── venv/                         # Virtual environment
├── main.py                       # Main application entry point
├── requirements.txt              # Python dependencies
├── password.txt                  # Default password file
├── start_service.py              # Service startup script
├── stop_service.py               # Service shutdown script
└── README.md                     # This file
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Redis configuration (for Celery)
REDIS_URL=redis://localhost:6379/0

# Logging level
LOG_LEVEL=INFO

# Output directory for reports
REPORTS_DIR=./reports

# Excel output format
EXCEL_OUTPUT_FORMAT=xlsx
```

### Password Management

#### Single PDF Password
Create a `password.txt` file with the password:
```
yourpassword
```

#### Batch Processing Passwords
Create a `passwords.txt` file with filename-password pairs:
```
statement1.pdf=password1
statement2.pdf=password2
```

## Command Line Options

### Main Commands

- `--pdf-file <path>`: Process a single PDF file
- `--batch-dir <directory>`: Process all PDFs in a directory
- `--daemon`: Run as background service

### Optional Arguments

- `--password <password>`: Password for encrypted PDF
- `--password-file <file>`: File containing passwords for batch processing
- `--output-dir <directory>`: Output directory for reports (default: ./reports)

### Examples

```bash
# Process single PDF with password
python main.py --pdf-file bank_statement.pdf --password mypassword

# Process single PDF (reads from password.txt)
python main.py --pdf-file bank_statement.pdf

# Batch process with custom output directory
python main.py --batch-dir ./statements --output-dir ./my_reports

# Batch process with password file
python main.py --batch-dir ./statements --password-file passwords.txt
```

## Output Reports

The system generates comprehensive Excel reports with the following sheets:

### Summary Sheet
- Total transactions count
- Overall credits and debits
- Net amount
- Analysis period
- Monthly breakdowns
- Top transactions
- Category analysis

### Transactions Sheet
- Complete transaction list
- Date, description, amounts
- Running balance
- Sortable and filterable

### Metadata Sheet (if enabled)
- Processing statistics
- File information
- Generation timestamp

## API Reference

### BankStatementProcessor Class

Main processor class for handling bank statement operations.

#### Methods

- `process_single_pdf(pdf_path, password=None, output_dir=None)`: Process a single PDF
- `process_batch(batch_dir, password_file=None, output_dir=None)`: Process multiple PDFs
- `start_daemon()`: Start background service

### PDFExtractor Class

Handles data extraction from PDF files.

#### Methods

- `extract_all_transactions(pdf_path, password=None)`: Extract all transactions
- `extract_text_from_pdf(pdf_path, password=None)`: Extract text content
- `extract_tables_from_pdf(pdf_path, password=None)`: Extract table data

### MonthlySummarizer Class

Calculates monthly summaries and analysis.

#### Methods

- `generate_comprehensive_summary(transactions)`: Generate full summary
- `group_transactions_by_month(transactions)`: Group by month
- `calculate_monthly_summary(month_key, transactions)`: Monthly analysis

### ExcelConverter Class

Converts data to Excel format.

#### Methods

- `create_summary_excel(summary_data, transactions, output_dir=None)`: Create report
- `convert_to_excel(transactions, output_path=None, filename=None)`: Basic conversion

## Error Handling

The system includes comprehensive error handling:

- **PDF Extraction Errors**: Handles corrupted or encrypted PDFs
- **Data Validation**: Validates extracted data formats
- **File System Errors**: Handles permission and disk space issues
- **Network Errors**: Handles Redis connection issues (if using Celery)
- **Graceful Degradation**: Continues processing when possible

All errors are logged with appropriate context for debugging.

## Logging

Logs are written to the `logs/` directory with the following levels:

- **INFO**: General processing information
- **WARNING**: Non-critical issues
- **ERROR**: Processing failures
- **DEBUG**: Detailed debugging information

Log files are rotated daily and kept for 30 days.

## Performance Considerations

### Memory Usage
- Large PDFs are processed in 10MB chunks
- Generators used for memory-efficient processing
- Automatic cleanup of temporary files

### Processing Speed
- Parallel processing available via Celery
- Optimized PDF parsing algorithms
- Caching of frequently accessed data

### Scalability
- Background task queue for large batches
- Configurable worker processes
- Redis-based task distribution

## Security

- **Password Protection**: No hardcoded passwords
- **Input Validation**: All inputs are validated and sanitized
- **Secure Logging**: Sensitive data is not logged
- **File Access**: Controlled file system access

## Troubleshooting

### Common Issues

#### PDF Extraction Fails
- Check if PDF is password-protected
- Verify PDF is not corrupted
- Ensure sufficient disk space

#### Excel Generation Fails
- Check output directory permissions
- Ensure sufficient disk space
- Verify Excel file is not open in another program

#### Performance Issues
- Increase available memory
- Use batch processing for multiple files
- Consider running as daemon for large workloads

### Debug Mode

Enable debug logging by setting the environment variable:
```bash
export LOG_LEVEL=DEBUG
python main.py --pdf-file statement.pdf
```

## Development

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run tests
pytest tests/

# Run with coverage
pytest --cov=src tests/
```

### Code Quality

```bash
# Format code
black src/

# Type checking
mypy src/

# Linting
pylint src/
```

## Contributing

1. Follow the coding standards defined in `AGENTS.md`
2. Add tests for new functionality
3. Update documentation
4. Ensure all tests pass before submitting

## License

This project is proprietary and confidential.

## Support

For support and issues, please refer to the project documentation or contact the development team.

---

**Version**: 1.0.0  
**Last Updated**: 2025-11-07  
**Python Version**: 3.9+