"""PDF Bank Statement Processing System - Setup Configuration."""

from setuptools import setup, find_packages
import os

# Read the contents of README file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md"), encoding="utf-8") as fh:
    long_description = fh.read()

# Read the contents of requirements file
with open(os.path.join(this_directory, "requirements.txt"), encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="pdf-bank-statement-processor",
    version="1.0.0",
    author="PDF Processing Team",
    author_email="support@example.com",
    description="Production-ready Python application for processing encrypted PDF bank statements",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/example/pdf-bank-statement-processor",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Office/Business :: Financial",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Text Processing",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "mypy>=1.0.0",
            "pylint>=2.17.0",
            "pre-commit>=3.0.0",
        ],
        "test": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-mock>=3.10.0",
            "coverage>=7.0.0",
        ],
        "monitoring": [
            "prometheus-client>=0.16.0",
            "psutil>=5.9.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "pdf-processor=main:main",
            "pdf-processor-daemon=main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.md", "*.txt", "*.yml", "*.yaml", "*.json"],
        "config": ["*.yml", "*.yaml", "*.json"],
        "docs": ["*.md", "*.rst", "*.txt"],
        "scripts": ["*.sh", "*.bat"],
    },
    zip_safe=False,
    keywords="pdf bank statement processing financial transactions excel",
    project_urls={
        "Bug Reports": "https://github.com/example/pdf-bank-statement-processor/issues",
        "Source": "https://github.com/example/pdf-bank-statement-processor",
        "Documentation": "https://pdf-bank-statement-processor.readthedocs.io/",
    },
)