"""
Environment Variable Drift Prevention Tests.

These tests ensure that:
1. All documented env vars are implemented in code
2. Default values in registry match actual code defaults
3. The retired dual-mode flags (KBM_LEDSAS_MODE / KBM_LEDSAS_DEV) are gone
   from the customer-facing registry and accepted-but-ignored at runtime
4. Internal-only variables are never advertised

These serve as the AUTOMATED VERIFICATION that documentation and code
stay in sync. The SDK ships a single direct-mode transport; there is no
runtime mode flag.
"""

import logging
import os
from unittest.mock import patch

import pytest

from kbm_ledsas_sdk.runtime.config import SDKConfig
from kbm_ledsas_sdk.runtime.env_vars import (
    ENV_VAR_REGISTRY,
    get_all_var_names,
    get_deprecated_vars,
    get_env_var,
)

# Azurite-style connection string used for direct-wheel construction.
_BLOB_CONN = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=xxx;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1"
)


class TestEnvVarRegistry:
    """Test the environment variable registry itself."""

    def test_registry_not_empty(self):
        """Registry should have environment variables defined."""
        assert len(ENV_VAR_REGISTRY) >= 10

    def test_all_vars_have_names(self):
        """All registry entries should have non-empty names."""
        for spec in ENV_VAR_REGISTRY:
            assert spec.name, f"Empty name in registry: {spec}"
            assert spec.name.startswith("KBM_LEDSAS_"), f"Bad prefix: {spec.name}"

    def test_all_vars_have_descriptions(self):
        """All registry entries should have descriptions."""
        for spec in ENV_VAR_REGISTRY:
            assert spec.description, f"Missing description for {spec.name}"

    def test_get_env_var_found(self):
        """get_env_var should return spec for known variables."""
        spec = get_env_var("KBM_LEDSAS_SERVICE_NAME")
        assert spec is not None
        assert spec.name == "KBM_LEDSAS_SERVICE_NAME"

    def test_get_env_var_not_found(self):
        """get_env_var should return None for unknown variables."""
        spec = get_env_var("KBM_LEDSAS_UNKNOWN")
        assert spec is None

    def test_get_all_var_names(self):
        """get_all_var_names should return all registered names."""
        names = get_all_var_names()
        assert "KBM_LEDSAS_SERVICE_NAME" in names
        assert len(names) == len(ENV_VAR_REGISTRY)

    def test_get_deprecated_vars(self):
        """get_deprecated_vars returns only entries marked deprecated."""
        deprecated = get_deprecated_vars()
        assert all(v.deprecated for v in deprecated)

    def test_registry_lists_only_customer_vars(self):
        """Registry must not advertise internal-only or retired env vars."""
        names = get_all_var_names()
        # Retired dual-mode flags must not appear in the registry.
        assert "KBM_LEDSAS_MODE" not in names
        assert "KBM_LEDSAS_DEV" not in names


class TestDocumentedDefaultsMatchCode:
    """Verify documented defaults in registry match actual code defaults."""

    def test_log_format_default_is_json(self):
        spec = get_env_var("KBM_LEDSAS_LOG_FORMAT")
        assert spec.default == "json"
        config = SDKConfig(service_name="test")
        assert config.log_format == "json"

    def test_log_level_default_is_info(self):
        spec = get_env_var("KBM_LEDSAS_LOG_LEVEL")
        assert spec.default == "INFO"
        config = SDKConfig(service_name="test")
        assert config.log_level == "INFO"

    def test_prefetch_default_is_10(self):
        spec = get_env_var("KBM_LEDSAS_PREFETCH")
        assert spec.default == "10"
        config = SDKConfig(service_name="test")
        assert config.prefetch == 10

    def test_concurrency_default_is_4(self):
        spec = get_env_var("KBM_LEDSAS_CONCURRENCY")
        assert spec.default == "4"
        config = SDKConfig(service_name="test")
        assert config.concurrency == 4

    def test_container_default_is_dev(self):
        spec = get_env_var("KBM_LEDSAS_CONTAINER")
        assert spec.default == "dev"
        config = SDKConfig(service_name="test")
        assert config.blob_container == "dev"


class TestRetiredModeVars:
    """The dual-mode flags are retired: accepted but ignored,
    with a one-time warning, and absent from the SDKConfig model."""

    def test_config_has_no_mode_fields(self):
        """SDKConfig no longer carries mode / dev_mode fields."""
        fields = set(SDKConfig.model_fields.keys())
        assert "mode" not in fields
        assert "dev_mode" not in fields

    @pytest.mark.parametrize(
        "var,value",
        [
            ("KBM_LEDSAS_MODE", "direct"),
            ("KBM_LEDSAS_MODE", "production"),
            ("KBM_LEDSAS_DEV", "1"),
            ("KBM_LEDSAS_INTERNAL", "1"),
        ],
    )
    def test_retired_var_is_ignored_not_fatal(self, var, value):
        """Setting a retired flag (any value) never raises — it is ignored."""
        env = {
            var: value,
            "KBM_LEDSAS_RABBITMQ_URL": "amqp://guest:guest@127.0.0.1:5672/",
            "KBM_LEDSAS_BLOB_CONN_STRING": _BLOB_CONN,
        }
        with patch.dict(os.environ, env, clear=True):
            config = SDKConfig.from_env("svc")
        # Direct wheel (autouse) still reads its credentials.
        assert config.rabbitmq_url == "amqp://guest:guest@127.0.0.1:5672/"

    def test_retired_var_warns_once(self, caplog):
        """A single warning lists the retired vars; a second from_env is silent."""
        env = {
            "KBM_LEDSAS_MODE": "direct",
            "KBM_LEDSAS_DEV": "1",
            "KBM_LEDSAS_RABBITMQ_URL": "amqp://guest:guest@127.0.0.1:5672/",
            "KBM_LEDSAS_BLOB_CONN_STRING": _BLOB_CONN,
        }
        with patch.dict(os.environ, env, clear=True):
            with caplog.at_level(logging.WARNING):
                SDKConfig.from_env("svc")
                SDKConfig.from_env("svc")  # second call: once-per-process guard
        warnings = [r for r in caplog.records if "no longer used" in r.getMessage()]
        assert len(warnings) == 1
        # The single warning names whichever retired vars were present.
        assert "KBM_LEDSAS_MODE" in warnings[0].getMessage()
        assert "KBM_LEDSAS_DEV" in warnings[0].getMessage()

    def test_no_retired_var_no_warning(self, caplog):
        """No retired var set -> no deprecation warning."""
        env = {
            "KBM_LEDSAS_RABBITMQ_URL": "amqp://guest:guest@127.0.0.1:5672/",
            "KBM_LEDSAS_BLOB_CONN_STRING": _BLOB_CONN,
        }
        with patch.dict(os.environ, env, clear=True):
            with caplog.at_level(logging.WARNING):
                SDKConfig.from_env("svc")
        warnings = [r for r in caplog.records if "no longer used" in r.getMessage()]
        assert warnings == []


class TestNoUnimplementedVariables:
    """Ensure we don't document variables that aren't implemented."""

    def test_no_trace_sampling_in_registry(self):
        names = get_all_var_names()
        assert "KBM_LEDSAS_TRACE_SAMPLING" not in names

    def test_no_max_inline_bytes_in_registry(self):
        names = get_all_var_names()
        assert "KBM_LEDSAS_MAX_INLINE_BYTES" not in names


class TestCustomerJourneyScenario:
    """Simulate the exact scenario a customer follows from documentation."""

    def test_customer_direct_setup_reads_credentials(self):
        """Customer following the README setup should connect.

        from_env reads the connection credentials into the config.
        """
        env = {
            "KBM_LEDSAS_RABBITMQ_URL": "amqp://guest:guest@127.0.0.1:5672/",
            "KBM_LEDSAS_BLOB_CONN_STRING": _BLOB_CONN,
        }
        with patch.dict(os.environ, env, clear=True):
            config = SDKConfig.from_env("csv-processor")
            assert config.rabbitmq_url == "amqp://guest:guest@127.0.0.1:5672/"
            assert config.blob_conn_string == _BLOB_CONN


class TestRegistryValidValues:
    """Verify valid_values constraints in registry match code behavior."""

    def test_log_level_valid_values(self):
        spec = get_env_var("KBM_LEDSAS_LOG_LEVEL")
        assert set(spec.valid_values) == {
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
        }

    def test_log_format_valid_values(self):
        spec = get_env_var("KBM_LEDSAS_LOG_FORMAT")
        assert set(spec.valid_values) == {"json", "text"}

    def test_mode_and_dev_not_in_registry(self):
        """The retired dual-mode flags are absent from the registry."""
        assert get_env_var("KBM_LEDSAS_MODE") is None
        assert get_env_var("KBM_LEDSAS_DEV") is None
