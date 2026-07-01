"""
Structured logging for LEDSAS SDK.

Provides JSON-formatted logging (default) and human-readable text logging,
with W3C trace context integration.
"""

import json
import logging
import sys
from typing import Any


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.

    Emits one JSON object per record with:
    - ``timestamp`` / ``level`` / ``logger`` / ``message`` (always)
    - ``exception`` (when ``exc_info`` is set)
    - every ``extra={...}`` key the caller attached (dynamic; not a
      hardcoded whitelist — caller fields go straight through so
      structured context like ``correlation_id``, ``trace_id``,
      ``exchange``, ``routing_key``, business identifiers, etc. all
      end up in the output without the formatter needing to know
      about them in advance).

    Non-JSON-serialisable values fall back to ``repr()`` so a stray
    object never breaks logging.
    """

    # Attributes the stdlib already sets on every LogRecord. Anything
    # NOT in this set was added by the caller via ``extra={...}`` and
    # should appear in the output.
    _STANDARD_RECORD_ATTRS = frozenset(
        {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "asctime",
            "taskName",
        }
    )

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Surface every caller-supplied extra= field.
        for key, value in record.__dict__.items():
            if key in self._STANDARD_RECORD_ATTRS:
                continue
            if key in log_data:
                # Don't let an extra= key overwrite our own canonical keys.
                continue
            log_data[key] = value

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, default=repr)


class TextFormatter(logging.Formatter):
    """
    Human-readable text formatter for development.

    Outputs logs with colors and readable formatting.
    """

    # ANSI color codes
    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
        "RESET": "\033[0m",
    }

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as colored text."""
        # Add color to level name
        levelname = record.levelname
        if sys.stderr.isatty():
            color = self.COLORS.get(levelname, "")
            reset = self.COLORS["RESET"]
            colored_levelname = f"{color}{levelname:8s}{reset}"
        else:
            colored_levelname = f"{levelname:8s}"

        # Base format
        message = super().format(record)

        # Add correlation_id if present
        if hasattr(record, "correlation_id"):
            message = f"{message} [corr={record.correlation_id}]"

        # Add trace_id if present
        if hasattr(record, "trace_id"):
            message = f"{message} [trace={record.trace_id[:16]}]"

        # Replace level name with colored version
        message = message.replace(levelname, colored_levelname, 1)

        return message


def setup_logging(
    log_level: str = "INFO",
    log_format: str = "json",
    service_name: str | None = None,
) -> None:
    """
    Configure structured logging for the SDK.

    Args:
        log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Format (json or text)
        service_name: Service name to add to all logs

    Example:
        setup_logging(log_level="DEBUG", log_format="text", service_name="my_service")
    """
    # Get root logger for kbm_ledsas_sdk
    logger = logging.getLogger("kbm_ledsas_sdk")
    logger.setLevel(getattr(logging, log_level.upper()))

    # Remove existing handlers
    logger.handlers.clear()

    # Create console handler
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(getattr(logging, log_level.upper()))

    # Set formatter based on format
    if log_format == "json":
        formatter = JSONFormatter(datefmt="%Y-%m-%dT%H:%M:%S")
    else:
        formatter = TextFormatter(
            fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    # Attach the service-name filter to the HANDLER (not the logger).
    # Logger-level filters only fire on records originated at that exact
    # logger; handler-level filters fire on every record that reaches
    # the handler, including those propagated up from child loggers
    # (``kbm_ledsas_sdk.amqp.consumer``, ``kbm_ledsas_sdk.blob.azure_client``,
    # etc.). That's the behaviour callers expect from "tag every SDK
    # log with my service name."
    if service_name:
        handler.addFilter(ServiceNameFilter(service_name))

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Don't propagate to root logger
    logger.propagate = False


class ServiceNameFilter(logging.Filter):
    """
    Logging filter that adds service_name to all log records.
    """

    def __init__(self, service_name: str):
        """Initialize with service name."""
        super().__init__()
        self.service_name = service_name

    def filter(self, record: logging.LogRecord) -> bool:
        """Add service_name to record."""
        record.service_name = self.service_name
        return True


def json_log_formatter(datefmt: str = "%Y-%m-%dT%H:%M:%S") -> logging.Formatter:
    """Return the SDK's JSON log formatter for use on YOUR OWN handlers.

    The canonical handler-logging guidance — put structured fields in
    ``extra={...}``, never interpolate caller-controlled values into the
    message string — is invisible under ``logging.basicConfig``'s percent
    format, which never renders ``extra=`` fields. Attach this formatter
    to your own handler to see them, in the same output shape as the
    SDK's own loggers in ``KBM_LEDSAS_LOG_FORMAT=json`` mode:

        import logging
        import kbm_ledsas_sdk

        handler = logging.StreamHandler()
        handler.setFormatter(kbm_ledsas_sdk.json_log_formatter())
        logging.basicConfig(level=logging.INFO, handlers=[handler])

    Args:
        datefmt: strftime format for the ``timestamp`` field.

    Returns:
        A ``logging.Formatter`` that emits one JSON object per record,
        including every caller-supplied ``extra={...}`` key.
    """
    return JSONFormatter(datefmt=datefmt)


def get_logger(name: str) -> logging.Logger:
    """
    Get logger with SDK namespace.

    Args:
        name: Logger name (will be prefixed with kbm_ledsas_sdk.)

    Returns:
        Logger instance

    Example:
        logger = get_logger("handler.ProcessDataset")
        # Creates logger: kbm_ledsas_sdk.handler.ProcessDataset
    """
    return logging.getLogger(f"kbm_ledsas_sdk.{name}")


class ContextAdapter(logging.LoggerAdapter):
    """
    Logger adapter that adds context fields to all log records.

    Useful for adding correlation_id, trace_id, etc. to all logs within a scope.

    Example:
        logger = get_logger("handler")
        ctx_logger = ContextAdapter(logger, {"correlation_id": "abc-123"})
        ctx_logger.info("Processing command")
        # Logs will include correlation_id field
    """

    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """Add context to log record."""
        # Add context fields to 'extra'
        extra = kwargs.get("extra", {})
        extra.update(self.extra)
        kwargs["extra"] = extra
        return msg, kwargs
