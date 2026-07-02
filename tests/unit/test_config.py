"""
Unit tests for SDKConfig.

Tests include:
- Config initialization with defaults
- Config loading from environment variables
- Validation of log level and format
- Path handling
- Edge cases
"""

import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from kbm_ledsas_sdk.runtime.config import SDKConfig


class TestSDKConfigDefaults:
    """Test SDKConfig default values."""

    def test_minimal_config(self):
        """Config with only required fields uses defaults."""
        config = SDKConfig(service_name="test_service")

        assert config.service_name == "test_service"
        assert config.tenant is None
        assert config.rabbitmq_url is None
        assert config.blob_conn_string is None
        assert config.blob_container == "dev"
        assert config.prefetch == 10
        assert config.concurrency == 4
        assert config.log_level == "INFO"
        assert config.log_format == "json"
        # Error-handling and retry defaults
        assert config.handler_timeout == 1800
        assert config.max_retries == 3
        assert config.generic_errors is False
        # Health-host default is loopback only.
        assert config.health_host == "127.0.0.1"

    def test_config_with_all_fields(self):
        """Config with all fields specified."""
        config = SDKConfig(
            service_name="my_service",
            tenant="acme-corp",
            rabbitmq_url="amqp://localhost",
            blob_conn_string="DefaultEndpointsProtocol=http;...",
            blob_container="dev-overrides",
            prefetch=20,
            concurrency=10,
            log_level="DEBUG",
            log_format="text",
        )

        assert config.service_name == "my_service"
        assert config.tenant == "acme-corp"
        assert config.rabbitmq_url == "amqp://localhost"
        assert config.blob_conn_string == "DefaultEndpointsProtocol=http;..."
        assert config.blob_container == "dev-overrides"
        assert config.prefetch == 20
        assert config.concurrency == 10
        assert config.log_level == "DEBUG"
        assert config.log_format == "text"


class TestSDKConfigFromEnv:
    """Test loading config from environment variables."""

    def test_from_env_defaults(self):
        """from_env with no env vars uses provided defaults."""
        with patch.dict(os.environ, {}, clear=True):
            config = SDKConfig.from_env("my_service")

            assert config.service_name == "my_service"
            assert config.tenant is None
            assert config.log_level == "INFO"
            assert config.log_format == "json"
            # Error-handling and retry defaults
            assert config.handler_timeout == 1800
            assert config.max_retries == 3
            assert config.generic_errors is False

    def test_from_env_with_service_name_override(self):
        """KBM_LEDSAS_SERVICE_NAME overrides provided service_name."""
        with patch.dict(os.environ, {"KBM_LEDSAS_SERVICE_NAME": "env_service"}, clear=True):
            config = SDKConfig.from_env("default_service")

            assert config.service_name == "env_service"

    def test_from_env_with_tenant(self):
        """KBM_LEDSAS_TENANT sets tenant."""
        with patch.dict(os.environ, {"KBM_LEDSAS_TENANT": "acme-corp"}, clear=True):
            config = SDKConfig.from_env("my_service")

            assert config.tenant == "acme-corp"

    def test_from_env_with_empty_tenant(self):
        """Empty tenant string treated as None."""
        with patch.dict(os.environ, {"KBM_LEDSAS_TENANT": ""}, clear=True):
            config = SDKConfig.from_env("my_service")

            assert config.tenant is None

    def test_from_env_rabbitmq_url(self):
        """KBM_LEDSAS_RABBITMQ_URL is read into the config."""
        with patch.dict(
            os.environ,
            {"KBM_LEDSAS_RABBITMQ_URL": "amqp://localhost:5672"},
            clear=True,
        ):
            config = SDKConfig.from_env("my_service")

            assert config.rabbitmq_url == "amqp://localhost:5672"

    def test_from_env_blob_config(self):
        """Blob configuration from env vars."""
        with patch.dict(
            os.environ,
            {
                "KBM_LEDSAS_BLOB_CONN_STRING": "DefaultEndpointsProtocol=http;...",
                "KBM_LEDSAS_CONTAINER": "production",
            },
            clear=True,
        ):
            config = SDKConfig.from_env("my_service")

            assert config.blob_conn_string == "DefaultEndpointsProtocol=http;..."
            assert config.blob_container == "production"

    def test_from_env_performance_tuning(self):
        """Performance tuning from env vars."""
        with patch.dict(
            os.environ,
            {
                "KBM_LEDSAS_PREFETCH": "50",
                "KBM_LEDSAS_CONCURRENCY": "20",
            },
            clear=True,
        ):
            config = SDKConfig.from_env("my_service")

            assert config.prefetch == 50
            assert config.concurrency == 20

    def test_from_env_logging_config(self):
        """Logging configuration from env vars."""
        with patch.dict(
            os.environ,
            {
                "KBM_LEDSAS_LOG_LEVEL": "DEBUG",
                "KBM_LEDSAS_LOG_FORMAT": "text",
            },
            clear=True,
        ):
            config = SDKConfig.from_env("my_service")

            assert config.log_level == "DEBUG"
            assert config.log_format == "text"

    def test_from_env_all_vars(self):
        """All env vars set at once."""
        with patch.dict(
            os.environ,
            {
                "KBM_LEDSAS_SERVICE_NAME": "env_service",
                "KBM_LEDSAS_TENANT": "acme-corp",
                "KBM_LEDSAS_RABBITMQ_URL": "amqp://localhost",
                "KBM_LEDSAS_BLOB_CONN_STRING": "DefaultEndpointsProtocol=http;...",
                "KBM_LEDSAS_CONTAINER": "test",
                "KBM_LEDSAS_PREFETCH": "100",
                "KBM_LEDSAS_CONCURRENCY": "50",
                "KBM_LEDSAS_LOG_LEVEL": "ERROR",
                "KBM_LEDSAS_LOG_FORMAT": "text",
            },
            clear=True,
        ):
            config = SDKConfig.from_env("default_service")

            assert config.service_name == "env_service"
            assert config.tenant == "acme-corp"
            assert config.rabbitmq_url == "amqp://localhost"
            assert config.prefetch == 100
            assert config.concurrency == 50
            assert config.log_level == "ERROR"
            assert config.log_format == "text"

    def test_from_env_handler_timeout(self):
        """KBM_LEDSAS_HANDLER_TIMEOUT sets handler timeout."""
        with patch.dict(os.environ, {"KBM_LEDSAS_HANDLER_TIMEOUT": "300"}, clear=True):
            config = SDKConfig.from_env("my_service")

            assert config.handler_timeout == 300

    def test_from_env_handler_timeout_disabled(self):
        """KBM_LEDSAS_HANDLER_TIMEOUT=0 disables timeout."""
        with patch.dict(os.environ, {"KBM_LEDSAS_HANDLER_TIMEOUT": "0"}, clear=True):
            config = SDKConfig.from_env("my_service")

            assert config.handler_timeout == 0

    def test_from_env_max_retries(self):
        """KBM_LEDSAS_MAX_RETRIES sets max retries."""
        with patch.dict(os.environ, {"KBM_LEDSAS_MAX_RETRIES": "5"}, clear=True):
            config = SDKConfig.from_env("my_service")

            assert config.max_retries == 5

    def test_from_env_max_retries_zero(self):
        """KBM_LEDSAS_MAX_RETRIES=0 means no retries."""
        with patch.dict(os.environ, {"KBM_LEDSAS_MAX_RETRIES": "0"}, clear=True):
            config = SDKConfig.from_env("my_service")

            assert config.max_retries == 0

    def test_from_env_generic_errors_true(self):
        """KBM_LEDSAS_GENERIC_ERRORS=true enables generic errors."""
        with patch.dict(os.environ, {"KBM_LEDSAS_GENERIC_ERRORS": "true"}, clear=True):
            config = SDKConfig.from_env("my_service")

            assert config.generic_errors is True

    def test_from_env_generic_errors_false(self):
        """KBM_LEDSAS_GENERIC_ERRORS=false disables generic errors."""
        with patch.dict(os.environ, {"KBM_LEDSAS_GENERIC_ERRORS": "false"}, clear=True):
            config = SDKConfig.from_env("my_service")

            assert config.generic_errors is False

    def test_from_env_generic_errors_case_insensitive(self):
        """KBM_LEDSAS_GENERIC_ERRORS is case-insensitive."""
        with patch.dict(os.environ, {"KBM_LEDSAS_GENERIC_ERRORS": "TRUE"}, clear=True):
            config = SDKConfig.from_env("my_service")

            assert config.generic_errors is True

        with patch.dict(os.environ, {"KBM_LEDSAS_GENERIC_ERRORS": "True"}, clear=True):
            config = SDKConfig.from_env("my_service")

            assert config.generic_errors is True

    def test_from_env_error_handling_all_vars(self):
        """All error handling env vars set at once."""
        with patch.dict(
            os.environ,
            {
                "KBM_LEDSAS_HANDLER_TIMEOUT": "600",
                "KBM_LEDSAS_MAX_RETRIES": "10",
                "KBM_LEDSAS_GENERIC_ERRORS": "true",
            },
            clear=True,
        ):
            config = SDKConfig.from_env("my_service")

            assert config.handler_timeout == 600
            assert config.max_retries == 10
            assert config.generic_errors is True


class TestSDKConfigValidation:
    """Test config validation."""

    def test_invalid_log_level(self):
        """Invalid log level raises ValidationError."""
        with pytest.raises(ValidationError, match="Invalid log level"):
            SDKConfig(service_name="test", log_level="INVALID")

    def test_valid_log_levels(self):
        """All valid log levels work."""
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            config = SDKConfig(service_name="test", log_level=level)
            assert config.log_level == level

    def test_log_level_case_insensitive(self):
        """Log level is case-insensitive."""
        config = SDKConfig(service_name="test", log_level="debug")
        assert config.log_level == "DEBUG"

        config = SDKConfig(service_name="test", log_level="Info")
        assert config.log_level == "INFO"

    def test_invalid_log_format(self):
        """Invalid log format raises ValidationError."""
        with pytest.raises(ValidationError, match="Invalid log format"):
            SDKConfig(service_name="test", log_format="invalid")

    def test_valid_log_formats(self):
        """All valid log formats work."""
        for fmt in ["json", "text"]:
            config = SDKConfig(service_name="test", log_format=fmt)
            assert config.log_format == fmt

    def test_log_format_case_insensitive(self):
        """Log format is case-insensitive."""
        config = SDKConfig(service_name="test", log_format="JSON")
        assert config.log_format == "json"

        config = SDKConfig(service_name="test", log_format="Text")
        assert config.log_format == "text"

    def test_prefetch_validation(self):
        """Prefetch must be in range 1-1000."""
        # Valid
        config = SDKConfig(service_name="test", prefetch=1)
        assert config.prefetch == 1

        config = SDKConfig(service_name="test", prefetch=1000)
        assert config.prefetch == 1000

        # Invalid
        with pytest.raises(ValidationError):
            SDKConfig(service_name="test", prefetch=0)

        with pytest.raises(ValidationError):
            SDKConfig(service_name="test", prefetch=1001)

    def test_concurrency_validation(self):
        """Concurrency must be in range 1-100."""
        # Valid
        config = SDKConfig(service_name="test", concurrency=1)
        assert config.concurrency == 1

        config = SDKConfig(service_name="test", concurrency=100)
        assert config.concurrency == 100

        # Invalid
        with pytest.raises(ValidationError):
            SDKConfig(service_name="test", concurrency=0)

        with pytest.raises(ValidationError):
            SDKConfig(service_name="test", concurrency=101)

    def test_handler_timeout_validation(self):
        """Handler timeout must be >= 0."""
        # Valid: 0 (disabled)
        config = SDKConfig(service_name="test", handler_timeout=0)
        assert config.handler_timeout == 0

        # Valid: positive value
        config = SDKConfig(service_name="test", handler_timeout=3600)
        assert config.handler_timeout == 3600

        # Invalid: negative
        with pytest.raises(ValidationError):
            SDKConfig(service_name="test", handler_timeout=-1)

    def test_max_retries_validation(self):
        """Max retries must be in range 0-100."""
        # Valid: 0 (no retries)
        config = SDKConfig(service_name="test", max_retries=0)
        assert config.max_retries == 0

        # Valid: 100
        config = SDKConfig(service_name="test", max_retries=100)
        assert config.max_retries == 100

        # Invalid: negative
        with pytest.raises(ValidationError):
            SDKConfig(service_name="test", max_retries=-1)

        # Invalid: > 100
        with pytest.raises(ValidationError):
            SDKConfig(service_name="test", max_retries=101)

    def test_generic_errors_boolean(self):
        """Generic errors must be a boolean."""
        config = SDKConfig(service_name="test", generic_errors=True)
        assert config.generic_errors is True

        config = SDKConfig(service_name="test", generic_errors=False)
        assert config.generic_errors is False


class TestSDKConfigRepr:
    """Test config __repr__."""

    def test_repr_minimal(self):
        """Repr with minimal config."""
        config = SDKConfig(service_name="test")
        repr_str = repr(config)

        assert "SDKConfig" in repr_str
        assert "test" in repr_str
        assert "tenant=None" in repr_str

    def test_repr_full(self):
        """Repr with full config surfaces the expected fields."""
        config = SDKConfig(
            service_name="my_service",
            tenant="acme",
            concurrency=20,
            log_level="DEBUG",
        )
        repr_str = repr(config)

        assert "my_service" in repr_str
        assert "acme" in repr_str
        assert "concurrency=20" in repr_str
        assert "log_level=DEBUG" in repr_str
