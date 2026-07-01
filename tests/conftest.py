"""Shared fixtures for the SDK test suite.

The SDK ships a single direct-mode transport (RabbitMQ + Azure Blob), so
the suite exercises direct-mode behavior throughout.
"""

import pytest

from kbm_ledsas_sdk.runtime import config as _config


@pytest.fixture(autouse=True)
def _reset_legacy_mode_warning():
    """Reset the one-time legacy-var warning guard around every test.

    ``config._warn_legacy_mode_vars_once`` flips a module global the first
    time a retired mode env var is seen; without resetting it, the first
    test to set one would suppress the warning for every later test.
    """
    _config._legacy_mode_warned = False
    yield
    _config._legacy_mode_warned = False
