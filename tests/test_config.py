"""Tests for configuration and settings modules."""

import pytest
import os
from unittest.mock import patch, Mock
from pathlib import Path
import tempfile

from src.config.settings import Settings
from src.config import settings


class TestSettings:
    """Test cases for Settings class."""

    def test_settings_init_default(self):
        """Test Settings initialization with default values."""
        settings_obj = Settings()
        
        assert settings_obj.pdf_password is None
        assert settings_obj.chunk_size_mb == 5
        assert settings_obj.max_retries == 3
        assert settings_obj.output_dir == "reports"
        assert settings_obj.log_level == "INFO"
        assert settings_obj.currency_symbol == "KES"
        assert settings_obj.default_currency == "KES"

    def test_settings_init_custom(self):
        """Test Settings initialization with custom values."""
        settings_obj = Settings(
            pdf_password="test123",
            chunk_size_mb=10,
            max_retries=5,
            output_dir="custom_reports",
            log_level="DEBUG",
            currency_symbol="USD",
            default_currency="USD",
        )
        
        assert settings_obj.pdf_password == "test123"
        assert settings_obj.chunk_size_mb == 10
        assert settings_obj.max_retries == 5
        assert settings_obj.output_dir == "custom_reports"
        assert settings_obj.log_level == "DEBUG"
        assert settings_obj.currency_symbol == "USD"
        assert settings_obj.default_currency == "USD"

    def test_settings_from_env_default(self, sample_environment):
        """Test Settings creation from environment variables (default)."""
        settings_obj = Settings.from_env()
        
        assert settings_obj.pdf_password == "test123"
        assert settings_obj.chunk_size_mb == 5
        assert settings_obj.max_retries == 3
        assert settings_obj.output_dir == "test_reports"
        assert settings_obj.log_level == "INFO"
        assert settings_obj.currency_symbol == "KES"
        assert settings_obj.default_currency == "KES"

    def test_settings_from_env_partial(self):
        """Test Settings creation from partial environment variables."""
        env_vars = {
            "PDF_PASSWORD": "env_password",
            "LOG_LEVEL": "DEBUG",
        }
        
        with patch.dict(os.environ, env_vars):
            settings_obj = Settings.from_env()
            
            assert settings_obj.pdf_password == "env_password"
            assert settings_obj.log_level == "DEBUG"
            # Other values should be defaults
            assert settings_obj.chunk_size_mb == 5
            assert settings_obj.currency_symbol == "KES"

    def test_settings_from_env_invalid_values(self):
        """Test Settings creation with invalid environment values."""
        env_vars = {
            "CHUNK_SIZE_MB": "invalid_number",
            "MAX_RETRIES": "zero",
        }
        
        with patch.dict(os.environ, env_vars):
            with pytest.raises(ValueError):
                Settings.from_env()

    def test_settings_validate(self):
        """Test Settings validation."""
        # Valid settings
        valid_settings = Settings(
            pdf_password="test123",
            chunk_size_mb=5,
            max_retries=3,
        )
        assert valid_settings.validate() is True
        
        # Invalid settings
        invalid_settings = Settings(
            chunk_size_mb=0,  # Invalid: too small
            max_retries=-1,   # Invalid: negative
        )
        assert invalid_settings.validate() is False

    def test_settings_get_log_level(self):
        """Test log level retrieval."""
        settings_obj = Settings(log_level="DEBUG")
        assert settings_obj.get_log_level() == "DEBUG"
        
        settings_obj.log_level = "invalid_level"
        assert settings_obj.get_log_level() == "INFO"  # Default fallback

    def test_settings_get_output_path(self, temp_dir):
        """Test output path generation."""
        settings_obj = Settings(output_dir=str(temp_dir))
        
        # Default filename
        default_path = settings_obj.get_output_path("test_report")
        assert str(temp_dir) in default_path
        assert "test_report" in default_path
        
        # Custom filename
        custom_path = settings_obj.get_output_path("custom.xlsx")
        assert str(temp_dir) in custom_path
        assert "custom.xlsx" in custom_path

    def test_settings_create_directories(self, temp_dir):
        """Test directory creation."""
        settings_obj = Settings(output_dir=str(temp_dir / "reports"))
        
        settings_obj.create_directories()
        
        assert (temp_dir / "reports").exists()
        assert (temp_dir / "logs").exists()

    def test_settings_to_dict(self):
        """Test settings to dictionary conversion."""
        settings_obj = Settings(
            pdf_password="test123",
            chunk_size_mb=10,
            log_level="DEBUG",
        )
        
        settings_dict = settings_obj.to_dict()
        
        assert settings_dict["pdf_password"] == "test123"
        assert settings_dict["chunk_size_mb"] == 10
        assert settings_dict["log_level"] == "DEBUG"
        assert "pdf_password" not in settings_dict.get("to_dict_exclude", [])

    def test_settings_from_dict(self):
        """Test settings from dictionary creation."""
        settings_dict = {
            "pdf_password": "dict_password",
            "chunk_size_mb": 15,
            "max_retries": 7,
        }
        
        settings_obj = Settings.from_dict(settings_dict)
        
        assert settings_obj.pdf_password == "dict_password"
        assert settings_obj.chunk_size_mb == 15
        assert settings_obj.max_retries == 7
        # Other values should be defaults
        assert settings_obj.log_level == "INFO"

    def test_settings_repr(self):
        """Test Settings string representation."""
        settings_obj = Settings(pdf_password="test123")
        
        repr_str = repr(settings_obj)
        assert "Settings" in repr_str
        assert "pdf_password" in repr_str

    def test_settings_eq(self):
        """Test Settings equality comparison."""
        settings1 = Settings(pdf_password="test123")
        settings2 = Settings(pdf_password="test123")
        settings3 = Settings(pdf_password="other")
        
        assert settings1 == settings2
        assert settings1 != settings3

    def test_settings_hash(self):
        """Test Settings hashing."""
        settings1 = Settings(pdf_password="test123")
        settings2 = Settings(pdf_password="test123")
        
        assert hash(settings1) == hash(settings2)

    def test_settings_update(self):
        """Test Settings update method."""
        settings_obj = Settings(pdf_password="original")
        
        updates = {
            "pdf_password": "updated",
            "log_level": "DEBUG",
        }
        
        settings_obj.update(updates)
        
        assert settings_obj.pdf_password == "updated"
        assert settings_obj.log_level == "DEBUG"
        # Other values should remain unchanged
        assert settings_obj.chunk_size_mb == 5

    def test_settings_clone(self):
        """Test Settings cloning."""
        original = Settings(
            pdf_password="test123",
            chunk_size_mb=10,
            log_level="DEBUG",
        )
        
        cloned = original.clone()
        
        assert cloned.pdf_password == original.pdf_password
        assert cloned.chunk_size_mb == original.chunk_size_mb
        assert cloned.log_level == original.log_level
        assert cloned is not original  # Different objects

    def test_settings_get_currency_display(self):
        """Test currency display formatting."""
        settings_obj = Settings(currency_symbol="KES")
        
        assert settings_obj.get_currency_display() == "KES"
        assert settings_obj.get_currency_display(1234.56) == "KES 1,234.56"

    def test_settings_get_max_chunk_size_bytes(self):
        """Test max chunk size in bytes calculation."""
        settings_obj = Settings(chunk_size_mb=5)
        
        assert settings_obj.get_max_chunk_size_bytes() == 5 * 1024 * 1024

    def test_settings_is_debug_enabled(self):
        """Test debug mode detection."""
        settings_obj = Settings(log_level="DEBUG")
        assert settings_obj.is_debug_enabled() is True
        
        settings_obj.log_level = "INFO"
        assert settings_obj.is_debug_enabled() is False

    def test_settings_get_retry_delay(self):
        """Test retry delay calculation."""
        settings_obj = Settings(max_retries=3)
        
        # Test exponential backoff
        delay1 = settings_obj.get_retry_delay(1)  # First retry
        delay2 = settings_obj.get_retry_delay(2)  # Second retry
        delay3 = settings_obj.get_retry_delay(3)  # Third retry
        
        assert delay1 < delay2 < delay3  # Increasing delays
        assert delay1 >= 1  # Minimum delay of 1 second


class TestConfigModule:
    """Test cases for config module-level functions."""

    def test_load_config_from_file(self, temp_dir):
        """Test loading configuration from file."""
        config_file = temp_dir / "test_config.json"
        config_content = {
            "pdf_password": "file_password",
            "chunk_size_mb": 10,
            "log_level": "DEBUG",
        }
        
        import json
        config_file.write_text(json.dumps(config_content))
        
        loaded_config = settings.load_config_from_file(str(config_file))
        
        assert loaded_config["pdf_password"] == "file_password"
        assert loaded_config["chunk_size_mb"] == 10
        assert loaded_config["log_level"] == "DEBUG"

    def test_load_config_from_file_not_exists(self):
        """Test loading configuration from non-existent file."""
        with pytest.raises(FileNotFoundError):
            settings.load_config_from_file("nonexistent_config.json")

    def test_save_config_to_file(self, temp_dir):
        """Test saving configuration to file."""
        config_file = temp_dir / "save_test.json"
        config_data = {
            "pdf_password": "save_password",
            "chunk_size_mb": 15,
        }
        
        settings.save_config_to_file(config_data, str(config_file))
        
        assert config_file.exists()
        
        import json
        saved_config = json.loads(config_file.read_text())
        assert saved_config["pdf_password"] == "save_password"
        assert saved_config["chunk_size_mb"] == 15

    def test_validate_config(self):
        """Test configuration validation."""
        # Valid config
        valid_config = {
            "pdf_password": "test123",
            "chunk_size_mb": 5,
            "max_retries": 3,
        }
        assert settings.validate_config(valid_config) is True
        
        # Invalid config
        invalid_config = {
            "chunk_size_mb": 0,  # Invalid value
            "max_retries": -1,   # Invalid value
        }
        assert settings.validate_config(invalid_config) is False

    def test_get_default_config(self):
        """Test getting default configuration."""
        default_config = settings.get_default_config()
        
        assert "pdf_password" in default_config
        assert "chunk_size_mb" in default_config
        assert "max_retries" in default_config
        assert "log_level" in default_config
        assert default_config["chunk_size_mb"] == 5
        assert default_config["max_retries"] == 3

    def test_merge_configs(self):
        """Test configuration merging."""
        base_config = {
            "pdf_password": "base_pass",
            "chunk_size_mb": 5,
            "log_level": "INFO",
        }
        
        override_config = {
            "pdf_password": "override_pass",
            "max_retries": 10,
        }
        
        merged = settings.merge_configs(base_config, override_config)
        
        assert merged["pdf_password"] == "override_pass"  # Overridden
        assert merged["chunk_size_mb"] == 5  # From base
        assert merged["log_level"] == "INFO"  # From base
        assert merged["max_retries"] == 10  # From override

    def test_config_environment_specific(self):
        """Test environment-specific configuration loading."""
        # Mock different environments
        with patch.dict(os.environ, {"APP_ENV": "production"}):
            prod_config = settings.get_environment_config()
            assert "production" in str(prod_config.get("log_level", "").lower())
        
        with patch.dict(os.environ, {"APP_ENV": "development"}):
            dev_config = settings.get_environment_config()
            assert "debug" in str(dev_config.get("log_level", "").lower())

    def test_config_workspace_aware(self, temp_dir):
        """Test workspace-aware configuration."""
        # Create a test workspace
        workspace_dir = temp_dir / "workspace"
        workspace_dir.mkdir()
        
        config_file = workspace_dir / ".pdf_processor_config"
        config_content = {
            "pdf_password": "workspace_password",
            "chunk_size_mb": 8,
        }
        
        import json
        config_file.write_text(json.dumps(config_content))
        
        # Test loading from workspace
        with patch('os.getcwd', return_value=str(workspace_dir)):
            workspace_config = settings.load_workspace_config()
            assert workspace_config["pdf_password"] == "workspace_password"
            assert workspace_config["chunk_size_mb"] == 8