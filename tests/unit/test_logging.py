"""Tests for structured logging utilities."""

import json
import logging

from kbm_ledsas_sdk.utils.logging import (
    ContextAdapter,
    JSONFormatter,
    ServiceNameFilter,
    TextFormatter,
    get_logger,
    setup_logging,
)


class TestJSONFormatter:
    """Test JSON log formatter."""

    def test_format_basic_message(self):
        """Format a basic log message as JSON."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)

        # Parse JSON
        log_entry = json.loads(result)

        assert log_entry["message"] == "Test message"
        assert log_entry["level"] == "INFO"
        assert log_entry["logger"] == "test"
        assert "timestamp" in log_entry

    def test_format_with_extra_fields(self):
        """Format log message with extra fields."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="test.py",
            lineno=10,
            msg="Test warning",
            args=(),
            exc_info=None,
        )
        # Add extra fields that the formatter checks for
        record.correlation_id = "corr-123"
        record.trace_id = "trace-456"
        record.span_id = "span-789"
        record.service_name = "test_service"

        result = formatter.format(record)
        log_entry = json.loads(result)

        assert log_entry["message"] == "Test warning"
        assert log_entry["level"] == "WARNING"
        assert log_entry["correlation_id"] == "corr-123"
        assert log_entry["trace_id"] == "trace-456"
        assert log_entry["span_id"] == "span-789"
        assert log_entry["service_name"] == "test_service"

    def test_format_with_exception(self):
        """Format log message with exception info."""
        formatter = JSONFormatter()

        try:
            raise ValueError("Test error")
        except ValueError:
            import sys

            exc_info = sys.exc_info()

            record = logging.LogRecord(
                name="test",
                level=logging.ERROR,
                pathname="test.py",
                lineno=10,
                msg="An error occurred",
                args=(),
                exc_info=exc_info,
            )
            result = formatter.format(record)
            log_entry = json.loads(result)

            assert log_entry["message"] == "An error occurred"
            assert log_entry["level"] == "ERROR"
            assert "exception" in log_entry
            assert "ValueError: Test error" in log_entry["exception"]
            assert "Traceback" in log_entry["exception"]

    def test_format_different_levels(self):
        """Format messages at different log levels."""
        formatter = JSONFormatter()

        for level_name, level_value in [
            ("DEBUG", logging.DEBUG),
            ("INFO", logging.INFO),
            ("WARNING", logging.WARNING),
            ("ERROR", logging.ERROR),
            ("CRITICAL", logging.CRITICAL),
        ]:
            record = logging.LogRecord(
                name="test",
                level=level_value,
                pathname="test.py",
                lineno=10,
                msg=f"{level_name} message",
                args=(),
                exc_info=None,
            )
            result = formatter.format(record)
            log_entry = json.loads(result)

            assert log_entry["level"] == level_name
            assert log_entry["message"] == f"{level_name} message"


class TestTextFormatter:
    """Test text log formatter."""

    def test_format_basic_message(self):
        """Format a basic log message as text."""
        formatter = TextFormatter(
            fmt="%(levelname)s %(name)s: %(message)s",
        )
        record = logging.LogRecord(
            name="test.module",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)

        # Check that message is formatted (level name gets padded/colored)
        assert "test.module: Test message" in result
        assert "INFO" in result

    def test_format_with_correlation_id(self):
        """Format log message with correlation_id."""
        formatter = TextFormatter(
            fmt="%(levelname)s %(name)s: %(message)s",
        )
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="test.py",
            lineno=10,
            msg="Test warning",
            args=(),
            exc_info=None,
        )
        record.correlation_id = "corr-123"

        result = formatter.format(record)

        assert "WARNING" in result
        assert "Test warning" in result
        assert "corr=corr-123" in result

    def test_format_with_trace_id(self):
        """Format log message with trace_id."""
        formatter = TextFormatter(
            fmt="%(levelname)s %(message)s",
        )
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        record.trace_id = "00-1234567890abcdef1234567890abcdef-0123456789abcdef-01"

        result = formatter.format(record)

        # Trace ID should be truncated to first 16 chars
        assert "trace=00-1234567890ab" in result

    def test_format_different_levels(self):
        """Format messages at different log levels."""
        formatter = TextFormatter(
            fmt="%(levelname)s: %(message)s",
        )

        for level_name, level_value in [
            ("DEBUG", logging.DEBUG),
            ("INFO", logging.INFO),
            ("WARNING", logging.WARNING),
            ("ERROR", logging.ERROR),
            ("CRITICAL", logging.CRITICAL),
        ]:
            record = logging.LogRecord(
                name="test",
                level=level_value,
                pathname="test.py",
                lineno=10,
                msg=f"{level_name} message",
                args=(),
                exc_info=None,
            )
            result = formatter.format(record)

            assert level_name in result
            assert "message" in result


class TestSetupLogging:
    """Test logging setup function."""

    def test_setup_json_logging(self):
        """Setup logging with JSON format."""
        setup_logging(log_level="DEBUG", log_format="json")
        logger = logging.getLogger("kbm_ledsas_sdk")

        assert logger.level == logging.DEBUG
        assert len(logger.handlers) > 0

        # Check handler has JSON formatter
        handler = logger.handlers[0]
        assert isinstance(handler.formatter, JSONFormatter)

    def test_setup_text_logging(self):
        """Setup logging with text format."""
        setup_logging(log_level="INFO", log_format="text")
        logger = logging.getLogger("kbm_ledsas_sdk")

        assert logger.level == logging.INFO
        assert len(logger.handlers) > 0

        # Check handler has text formatter
        handler = logger.handlers[0]
        assert isinstance(handler.formatter, TextFormatter)

    def test_setup_with_service_name(self):
        """Setup logging attaches ServiceNameFilter to the handler.

        Logger-level filters only fire on records originated at that
        exact logger; handler-level filters fire on every record that
        reaches the handler, including those propagated up from child
        loggers (``kbm_ledsas_sdk.amqp.consumer``, etc.). The filter
        moved from the top-level logger to the handler so child-logger
        records carry the service_name field.
        """
        setup_logging(log_level="WARNING", log_format="json", service_name="test_service")
        logger = logging.getLogger("kbm_ledsas_sdk")

        assert logger.level == logging.WARNING
        # The filter is now attached to the handler, not the logger.
        assert len(logger.handlers) > 0
        handler = logger.handlers[0]
        assert len(handler.filters) > 0
        filter_obj = next(f for f in handler.filters if isinstance(f, ServiceNameFilter))
        assert filter_obj.service_name == "test_service"

    def test_setup_different_levels(self):
        """Setup logging at different levels."""
        for level_name, level_value in [
            ("DEBUG", logging.DEBUG),
            ("INFO", logging.INFO),
            ("WARNING", logging.WARNING),
            ("ERROR", logging.ERROR),
            ("CRITICAL", logging.CRITICAL),
        ]:
            setup_logging(log_level=level_name, log_format="text")
            logger = logging.getLogger("kbm_ledsas_sdk")
            assert logger.level == level_value

    def test_setup_clears_existing_handlers(self):
        """Setup logging clears existing handlers."""
        # Setup once
        setup_logging(log_level="INFO", log_format="json")
        logger = logging.getLogger("kbm_ledsas_sdk")
        initial_handler_count = len(logger.handlers)

        # Setup again
        setup_logging(log_level="DEBUG", log_format="text")

        # Should have same number of handlers (old ones cleared)
        assert len(logger.handlers) == initial_handler_count


class TestGetLogger:
    """Test get_logger function."""

    def test_get_logger_adds_namespace(self):
        """get_logger adds kbm_ledsas_sdk namespace."""
        logger = get_logger("test.module")
        assert logger.name == "kbm_ledsas_sdk.test.module"

    def test_get_logger_different_names(self):
        """get_logger works with different names."""
        logger1 = get_logger("handler")
        logger2 = get_logger("transport")

        assert logger1.name == "kbm_ledsas_sdk.handler"
        assert logger2.name == "kbm_ledsas_sdk.transport"


class TestContextAdapter:
    """Test ContextAdapter for adding context fields."""

    def test_context_adapter_adds_fields(self):
        """ContextAdapter adds context fields to logs."""
        base_logger = get_logger("test")
        adapter = ContextAdapter(
            base_logger, {"correlation_id": "corr-123", "trace_id": "trace-456"}
        )

        assert adapter.extra["correlation_id"] == "corr-123"
        assert adapter.extra["trace_id"] == "trace-456"

    def test_context_adapter_process(self):
        """ContextAdapter.process adds extra fields."""
        base_logger = get_logger("test")
        adapter = ContextAdapter(base_logger, {"correlation_id": "abc"})

        msg, kwargs = adapter.process("test message", {})

        assert "extra" in kwargs
        assert kwargs["extra"]["correlation_id"] == "abc"


class TestServiceNameFilter:
    """Test ServiceNameFilter."""

    def test_service_name_filter_adds_field(self):
        """ServiceNameFilter adds service_name to log records."""
        filter_obj = ServiceNameFilter("my_service")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        result = filter_obj.filter(record)

        assert result is True
        assert record.service_name == "my_service"
