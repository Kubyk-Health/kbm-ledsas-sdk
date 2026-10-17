"""
Structured logging for LEDSAS SDK.

Provides JSON-formatted logging (default) and human-readable text logging,
with W3C trace context integration.
"""

import json
import logging
import math
import re
import sys
from datetime import UTC, datetime
from typing import Any

# The datefmt older releases passed for the JSON ``timestamp`` (local time,
# whole seconds, no offset). Treated like "no datefmt": the timestamp is
# then UTC ISO 8601 with milliseconds.
_LEGACY_JSON_DATEFMT = "%Y-%m-%dT%H:%M:%S"


def _safe_repr(value: Any) -> str:
    """``repr(value)``, or ``<TypeName (repr failed)>`` when the value's
    own ``__repr__`` raises: a log record is never lost to a broken
    ``__repr__`` (an ORM object detached from its session, say)."""
    try:
        return repr(value)
    except Exception:
        try:
            name = type(value).__name__
        except Exception:  # pragma: no cover - a broken class object
            name = "object"
        return f"<{name} (repr failed)>"


def _safe_str(value: Any) -> str:
    """``str(value)``, falling back to ``_safe_repr`` when it raises."""
    try:
        return str(value)
    except Exception:
        return _safe_repr(value)


def utc_timestamp(created: float) -> str:
    """``created`` (seconds since the epoch) as UTC ISO 8601 with
    milliseconds and a ``Z`` suffix, e.g. ``2026-10-14T05:43:15.123Z``."""
    stamp = datetime.fromtimestamp(created, UTC)
    return stamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


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
    object never breaks logging: every record is one valid JSON line
    (RFC 8259). NaN and Infinity become the strings ``"NaN"``,
    ``"Infinity"`` and ``"-Infinity"``; a field that still cannot be
    written (a dict with tuple keys, a container that contains itself) is
    written as its ``repr()``, and a value whose own ``__repr__`` raises
    as ``"<TypeName (repr failed)>"``. A message whose ``%`` arguments
    cannot be formatted is written as its template.

    ``timestamp`` is UTC ISO 8601 with milliseconds and a ``Z`` suffix
    (``2026-10-14T05:43:15.123Z``), so records from hosts in different
    time zones line up and records within the same second keep their
    order. A formatter built with an explicit ``datefmt`` uses that
    format instead (local time, as ``logging.Formatter`` does).
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
        if self.datefmt in (None, _LEGACY_JSON_DATEFMT):
            timestamp = utc_timestamp(record.created)
        else:
            timestamp = self.formatTime(record, self.datefmt)
        log_data: dict[str, Any] = {
            "timestamp": timestamp,
            "level": record.levelname,
            "logger": record.name,
            "message": _record_message(record),
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

        try:
            line = json.dumps(log_data, default=_safe_repr, allow_nan=False)
        except Exception:
            # A field has no JSON form (NaN / Infinity, tuple keys, a
            # container that contains itself, a value that raises while
            # it is written): rewrite only the fields that fail, so the
            # record is still one valid JSON line.
            log_data = {key: _json_safe_field(value) for key, value in log_data.items()}
            line = json.dumps(log_data, default=_safe_repr, allow_nan=False)
        if "\\ud" in line:
            # A lone surrogate (a "\ud800" escape a JSON request can carry,
            # quoted into a message or a field) would be written as an
            # escape that strict JSON readers refuse and that has no UTF-8
            # form. Python text holds a character outside the BMP as one
            # code point, so every surrogate here is lone: each becomes
            # U+FFFD, as in error replies. "\ud" also starts the escapes of
            # characters outside the BMP, which pass through unchanged.
            line = json.dumps(
                _without_surrogates(log_data),
                default=lambda value: _without_surrogates(_safe_repr(value)),
                allow_nan=False,
            )
        return line


# A UTF-16 surrogate code point: in Python text always a lone one.
_SURROGATE = re.compile("[\ud800-\udfff]")


def _without_surrogates(value: Any, depth: int = 0) -> Any:
    """``value`` with every lone surrogate in its text (strings, dict keys,
    list and tuple items, at any depth up to 100) replaced by U+FFFD."""
    if isinstance(value, str):
        return _SURROGATE.sub("\ufffd", value)
    if depth > 100:
        return value
    if isinstance(value, dict):
        return {
            _without_surrogates(k, depth + 1): _without_surrogates(v, depth + 1)
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_without_surrogates(v, depth + 1) for v in value]
    return value


def _record_message(record: logging.LogRecord) -> str:
    """The record's message; its template when the ``%`` arguments cannot
    be formatted (a wrong placeholder, an argument whose ``__str__``
    raises), as the text format does."""
    try:
        return record.getMessage()
    except Exception:
        return _safe_str(record.msg)


def _non_finite_as_text(value: Any, depth: int = 0) -> Any:
    """``value`` with NaN / Infinity floats (at any depth) as the strings
    ``"NaN"``, ``"Infinity"``, ``"-Infinity"``."""
    if isinstance(value, float) and not math.isfinite(value):
        return "NaN" if value != value else ("Infinity" if value > 0 else "-Infinity")
    if depth > 100:
        return value
    if isinstance(value, dict):
        return {k: _non_finite_as_text(v, depth + 1) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_non_finite_as_text(v, depth + 1) for v in value]
    return value


def _json_safe_field(value: Any) -> Any:
    """One log field in a form ``json.dumps(..., allow_nan=False)``
    accepts: unchanged when it can be written, NaN / Infinity as text,
    otherwise its ``repr()``."""
    try:
        json.dumps(value, default=_safe_repr, allow_nan=False)
        return value
    except Exception:
        pass
    try:
        converted = _non_finite_as_text(value)
        json.dumps(converted, default=_safe_repr, allow_nan=False)
        return converted
    except Exception:
        pass
    return _safe_repr(value)


# The datefmt older releases passed for the text format (local time, whole
# seconds, no zone). Treated like "no datefmt": the timestamp is then UTC
# ISO 8601 with milliseconds, as in the JSON format.
_LEGACY_TEXT_DATEFMT = "%Y-%m-%d %H:%M:%S"

# C0 control characters, DEL and the C1 range (\x9b is a one-byte terminal
# escape introducer), plus the Unicode line and paragraph separators.
_CONTROL_CHARS = re.compile("[\x00-\x1f\x7f-\x9f\u2028\u2029]")
# The same, except the newline (kept between the lines of a traceback).
_CONTROL_CHARS_BUT_NEWLINE = re.compile("[\x00-\x09\x0b-\x1f\x7f-\x9f\u2028\u2029]")
# Plain values that need no quotes in a key=value pair.
_PLAIN_VALUE = re.compile(r"^[A-Za-z0-9_.:/@+-]+$")


def _escape_control(match: "re.Match[str]") -> str:
    ch = match.group(0)
    named = {"\n": "\\n", "\r": "\\r", "\t": "\\t"}
    if ch in named:
        return named[ch]
    code = ord(ch)
    return f"\\x{code:02x}" if code <= 0xFF else f"\\u{code:04x}"


def _escape_text(text: str) -> str:
    """``text`` with control characters written as escapes (``\\x1b``,
    ``\\n``), so a value can neither drive the terminal nor start a new,
    forged log line."""
    return _CONTROL_CHARS.sub(_escape_control, text)


def _format_value(value: Any) -> str:
    """One key=value value: plain tokens as they are, anything else quoted
    as a JSON string (which escapes quotes and control characters)."""
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, default=_safe_repr)
        except Exception:
            text = _safe_repr(value)
    if _PLAIN_VALUE.match(text):
        return text
    return json.dumps(text)


class TextFormatter(logging.Formatter):
    """
    Human-readable text formatter for development.

    One line per record: ``<UTC timestamp> <LEVEL> <logger>: <message>``,
    followed by ``[corr=...]`` / ``[trace=...]`` and every other
    ``extra={...}`` field as ``key=value`` (the same fields the JSON format
    carries, e.g. ``error_message``, ``error_class``, ``amqp_message_id``,
    ``body_bytes``). The timestamp is UTC ISO 8601 with milliseconds and a
    ``Z`` suffix, as in the JSON format. Control characters in the message,
    the field values and an exception's text are escaped, so no value can
    move the cursor, recolor the terminal or forge a line; a traceback
    follows the line, each of its lines indented by ``  | ``. The level
    name is colored when stderr is a terminal.
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

    # Extras rendered in their own bracket (correlation_id, trace_id) or
    # that are the same on every line (service_name).
    _NOT_AS_PAIRS = frozenset({"correlation_id", "trace_id", "service_name"})

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as one escaped line (plus any traceback)."""
        if self.datefmt in (None, _LEGACY_TEXT_DATEFMT):
            timestamp = utc_timestamp(record.created)
        else:
            timestamp = self.formatTime(record, self.datefmt)

        levelname = record.levelname
        if sys.stderr.isatty():
            color = self.COLORS.get(levelname, "")
            reset = self.COLORS["RESET"]
            shown_level = f"{color}{levelname:8s}{reset}"
        else:
            shown_level = f"{levelname:8s}"

        message = _record_message(record)
        line = f"{timestamp} {shown_level} {record.name}: {_escape_text(message)}"

        correlation_id = getattr(record, "correlation_id", None)
        if correlation_id is not None:
            line += f" [corr={_escape_text(_safe_str(correlation_id))}]"
        trace_id = getattr(record, "trace_id", None)
        if trace_id is not None:
            line += f" [trace={_escape_text(_safe_str(trace_id))[:16]}]"

        pairs = []
        for key, value in record.__dict__.items():
            if key in JSONFormatter._STANDARD_RECORD_ATTRS or key in self._NOT_AS_PAIRS:
                continue
            pairs.append(f"{_escape_text(str(key))}={_format_value(value)}")
        if pairs:
            line += " " + " ".join(pairs)

        extra_lines = []
        if record.exc_info:
            extra_lines.append(self.formatException(record.exc_info))
        elif record.exc_text:
            extra_lines.append(record.exc_text)
        if record.stack_info:
            extra_lines.append(self.formatStack(record.stack_info))
        for block in extra_lines:
            escaped = _CONTROL_CHARS_BUT_NEWLINE.sub(_escape_control, block)
            line += "".join(f"\n  | {ln}" for ln in escaped.splitlines())
        return line


# Secrets that exception texts commonly quote: the user info of a URL
# ("amqp://user:password@host"), and the credential parts of a storage
# connection string or SAS URL ("AccountKey=...", "SharedAccessSignature=...",
# "sig=..."). Masked by redact_secrets().
_URL_USERINFO = re.compile(r"(\b[A-Za-z][A-Za-z0-9+.-]*://)[^\s/@'\"<>]+@")
_CONNECTION_STRING_SECRET = re.compile(
    r"(?i)\b(AccountKey|SharedAccessSignature|SharedAccessKey|Password|Pwd)=[^;\s'\"]+"
)
_SAS_SIGNATURE = re.compile(r"(?i)([?&]sig=)[^&\s'\"]+")


def redact_secrets(text: str) -> str:
    """``text`` with the secrets it may quote masked: the user name and
    password of a URL (``amqp://***:***@host``), and the values of
    ``AccountKey=``, ``SharedAccessSignature=``, ``SharedAccessKey=``,
    ``Password=`` / ``Pwd=`` and of a ``sig=`` query parameter (``***``).
    Used on texts the SDK logs or serves that come from code it does not
    control, such as the exception a health check raised."""
    if not isinstance(text, str):
        return text
    text = _URL_USERINFO.sub(r"\1***:***@", text)
    text = _CONNECTION_STRING_SECRET.sub(lambda m: f"{m.group(1)}=***", text)
    return _SAS_SIGNATURE.sub(r"\1***", text)


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
        formatter = JSONFormatter()
    else:
        formatter = TextFormatter()

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


def json_log_formatter(datefmt: str | None = None) -> logging.Formatter:
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
        datefmt: Optional strftime format for the ``timestamp`` field. By
            default the timestamp is UTC ISO 8601 with milliseconds
            (``2026-10-14T05:43:15.123Z``).

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
