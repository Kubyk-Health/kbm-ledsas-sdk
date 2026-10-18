"""Shared fixtures for the SDK test suite."""

import logging

import pytest

from kbm_ledsas_sdk.transport import _build_direct


@pytest.fixture(autouse=True)
def _restore_sdk_logger_state():
    """Restore the shared "kbm_ledsas_sdk" logger after every test.

    ``setup_logging()`` (called directly by the logging tests and indirectly
    by every ``ServiceApp(...)``) clears the logger's handlers, installs a
    StreamHandler bound to the current ``sys.stderr`` (under pytest: the
    per-test capture stream, which is closed when the test ends) and sets
    ``propagate=False``. Left unrestored, later tests' ``caplog`` assertions
    on SDK loggers see nothing and the dead handler reports "I/O operation
    on closed file".
    """
    logger = logging.getLogger("kbm_ledsas_sdk")
    saved_handlers = list(logger.handlers)
    saved_level = logger.level
    saved_propagate = logger.propagate
    saved_filters = list(logger.filters)
    yield
    logger.handlers[:] = saved_handlers
    logger.setLevel(saved_level)
    logger.propagate = saved_propagate
    logger.filters[:] = saved_filters


@pytest.fixture(autouse=True)
def _no_container_memory_limit(monkeypatch):
    """The transport warns at start when the command bodies it may hold
    parsed can exceed the container's memory limit, which it reads from the
    cgroup. A test run inside a container with a memory limit would otherwise
    get that WARNING in every test that builds the transport. Tests of the
    warning patch the reader themselves."""
    monkeypatch.setattr(_build_direct, "_CGROUP_MEMORY_LIMIT_FILES", ())
    yield
