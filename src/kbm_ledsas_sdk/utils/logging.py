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
            # A traceback already rendered for the record (exc_text: the
            # masked one of a handler failure, see SecretMaskFilter) is used
            # as it is.
            log_data["exception"] = record.exc_text or self.formatException(record.exc_info)

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
_PLAIN_VALUE = re.compile(r"[A-Za-z0-9_.:/@+-]+")  # used with fullmatch


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
    if _PLAIN_VALUE.fullmatch(text):
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
        if record.exc_text:
            # Already rendered (the masked traceback of a handler failure,
            # see SecretMaskFilter).
            extra_lines.append(record.exc_text)
        elif record.exc_info:
            extra_lines.append(self.formatException(record.exc_info))
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
# A user info already masked ("***@", "***:***@") is left as it is.
# Any URL quoted in a text (up to a space, a quote or an angle bracket);
# its user info is found like a broker URL's (see _userinfo_end_any).
_ANY_URL = re.compile(r"\b([A-Za-z][A-Za-z0-9+.-]*://)([^\s'\"<>]+)")
# A value already masked ("password=***" in a broker URL's query, possibly
# followed by its masked fragment "#***" and by the punctuation that ends
# the sentence, as in "...?password=***)." ) is left as it is: it is not
# taken for the start of a value running to the next ";", which would drop
# that punctuation.
_ALREADY_MASKED_VALUE = r"(?!\*\*\*(?:#\*\*\*)?[.,:!?)\]}]*(?:[&;\s'\"<>]|$))"
_SECRET_KEYS = r"AccountKey|SharedAccessSignature|SharedAccessKey|Password|Pwd|account_key"
_CONNECTION_STRING_SECRET = re.compile(
    r"(?i)\b(" + _SECRET_KEYS + r")=" + _ALREADY_MASKED_VALUE + r"[^;\s'\"]+"
)
# The same keys with a quoted value, in any of the forms a configuration
# dump or a repr shows them: AccountKey="...", 'account_key': '...',
# "AccountKey": "...".
_QUOTED_SECRET = re.compile(
    r"(?i)\b(" + _SECRET_KEYS + r")([\"']?\s*[=:]\s*)([\"'])(?!\*\*\*\3)[^\"'\n]*\3"
)
# A bearer token: any token after an "Authorization:" header's "Bearer"
# ("Authorization: Bearer eyJ..."), and elsewhere a "Bearer" followed by
# something shaped like a token (at least 16 token characters, or a JWT's
# dotted form), so prose ("the bearer of bad news") is left alone.
_BEARER_TOKEN_CHARS = r"[A-Za-z0-9\-._~+/]+=*"
_BEARER_HEADER_TOKEN = re.compile(
    r"(?i)\b(Authorization[\"']?\s*[:=]\s*[\"']?Bearer\s+)(?!\*\*\*)" + _BEARER_TOKEN_CHARS
)
_BEARER_TOKEN = re.compile(
    r"(?i)\b(Bearer\s+)(?!\*\*\*)"
    r"(?=[A-Za-z0-9\-._~+/]{16}|[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.)" + _BEARER_TOKEN_CHARS
)
# Query parameters that carry a credential in a URL of any scheme: a SAS
# signature, an OAuth code or token.
_SAS_SIGNATURE = re.compile(
    r"(?i)([?&](?:sig|code|access_token|token)=)" + _ALREADY_MASKED_VALUE + r"[^&\s'\"]+"
)
# A broker URL quoted in a text (up to a space, a quote or an angle bracket).
_BROKER_URL = re.compile(r"(?i)\bamqps?://[^\s'\"<>]+")
# Punctuation that ends a sentence rather than the URL it follows.
_URL_TRAILING_PUNCTUATION = ".,;:!?)]}"
# A query parameter name shown as it is; any other name is masked too.
_PLAIN_QUERY_KEY = re.compile(r"[A-Za-z0-9_.\-]{1,64}")
_QUERY_ITEM = re.compile(r"[^&;]+")


def _mask_query(query: str) -> str:
    """The query string ``query`` (without its ``?``) with every value
    masked (``name=***``), separators kept; an item without ``=`` or whose
    name is not a plain token is masked whole (``***``)."""

    def item(match: "re.Match[str]") -> str:
        key, eq, _value = match.group(0).partition("=")
        if eq and _PLAIN_QUERY_KEY.fullmatch(key):
            return f"{key}=***"
        return "***"

    return _QUERY_ITEM.sub(item, query)


def _masked_userinfo(userinfo: str) -> str:
    """``***:***``, or the user info as it is when it is already masked
    (``***`` or ``***:***``)."""
    return userinfo if userinfo in ("***", "***:***") else "***:***"


def _query_or_fragment_start(text: str) -> int:
    """The index of the first ``?`` or ``#`` of ``text`` (its length when
    there is none)."""
    cut = len(text)
    for char in "?#":
        index = text.find(char)
        if index != -1:
            cut = min(cut, index)
    return cut


def _userinfo_end(rest: str) -> int:
    """The index of the ``@`` that ends the user info of ``rest`` (a URL
    after its ``scheme://``), or -1. Found before the query and the
    fragment are split off, because a password may hold an unencoded
    ``?``, ``#`` or ``/``; it fails closed (masks more rather than less):

    - the last ``@`` before the first ``/`` (a password with ``?`` or
      ``#``: ``amqp://user:pa?ss@host/``);
    - else the last ``@`` before the first ``?`` or ``#`` (a password with
      ``/``: ``amqp://user:pa/ss@host/``);
    - else the last ``@`` of all (a password with ``/`` and then ``?`` or
      ``#``); so a ``@`` in the query or the fragment of a URL with no
      user info masks its host and path as well."""
    slash = rest.find("/")
    at = rest.rfind("@", 0, len(rest) if slash == -1 else slash)
    if at == -1:
        at = rest.rfind("@", 0, _query_or_fragment_start(rest))
    if at == -1:
        at = rest.rfind("@")
    return at


# A host and port ("127.0.0.1:10000", "acct.blob.core.windows.net:443",
# "[::1]:10000"): the authority of a URL without user info.
_HOST_AND_PORT = re.compile(r"(?:\[[0-9A-Fa-f:.]*\]|[A-Za-z0-9.\-]+):\d*")


def _userinfo_end_any(rest: str) -> int:
    """The index of the ``@`` that ends the user info of ``rest`` (a URL
    of any scheme after its ``scheme://``), or -1: the last ``@`` before
    the first ``/``; else, when the text before the first ``/`` holds a
    ``:`` (a password with an unencoded ``/``: ``https://svc:pa/ss@host/``),
    the last ``@`` before the query and the fragment. A ``@`` in the path
    of a URL without user info (``https://host/a@b``) is left alone, as is
    one in the path of a URL with a port (``http://host:10000/c/a@b``):
    when the text before the first ``/`` is a host and a port, the ``@`` is
    taken for the end of a user info only when no other ``/`` comes
    between (``https://svc:12/34@host/``)."""
    slash = rest.find("/")
    at = rest.rfind("@", 0, len(rest) if slash == -1 else slash)
    if at == -1 and slash != -1 and ":" in rest[:slash]:
        at = rest.rfind("@", 0, _query_or_fragment_start(rest))
        if at != -1 and _HOST_AND_PORT.fullmatch(rest[:slash]) and "/" in rest[slash + 1 : at]:
            at = -1
    return at


def _mask_any_url_userinfo(match: "re.Match[str]") -> str:
    scheme, rest = match.group(1), match.group(2)
    at = _userinfo_end_any(rest)
    if at <= 0:
        return match.group(0)
    return f"{scheme}{_masked_userinfo(rest[:at])}@{rest[at + 1:]}"


def mask_url_secrets(url: str) -> str:
    """``url`` with what may carry a secret masked, read as text (it works
    on a URL no parser accepts):

    - the user info: everything before the last ``@`` of the part after
      ``scheme://`` up to the first ``/`` becomes ``***:***`` (a ``?`` or
      ``#`` in the password does not end it); with no ``@`` there,
      everything before the last ``@`` ahead of the query and the fragment
      (a password with an unencoded ``/``); with none there either,
      everything before the last ``@`` of the URL (a password with ``/``
      and ``?`` or ``#``: it fails closed, so a ``@`` in the query of a URL
      without user info masks its host too);
    - every value of the query string (``?name=***&other=***``; an item
      without ``=`` or with an unusual name becomes ``***``);
    - the fragment (``#***``).

    The scheme, the host, the port and the path are kept. A broker URL has
    no secret in its query by design, but the client library reads some
    settings from it and an operator may put anything there."""
    scheme, sep, rest = url.partition("://")
    if not sep:
        scheme, rest = "", url
    userinfo = ""
    at = _userinfo_end(rest)
    if at != -1:
        userinfo = _masked_userinfo(rest[:at]) + "@"
        rest = rest[at + 1 :]
    cut = _query_or_fragment_start(rest)
    head, tail = rest[:cut], rest[cut:]
    if tail.startswith("?"):
        query, hash_, fragment = tail[1:].partition("#")
        tail = "?" + _mask_query(query) + (hash_ + "***" if hash_ else "")
    elif tail.startswith("#"):
        tail = "#***"
    return f"{scheme}{sep}{userinfo}{head}{tail}"


def _mask_broker_url(match: "re.Match[str]") -> str:
    url = match.group(0)
    trailing = ""
    while url and url[-1] in _URL_TRAILING_PUNCTUATION:
        url, trailing = url[:-1], url[-1] + trailing
    return mask_url_secrets(url) + trailing


def redact_secrets(text: str) -> str:
    """``text`` with the secrets it may quote masked: the user name and
    password of a URL (``amqp://***:***@host``), the query values and the
    fragment of a broker URL (``amqp://host/?password=***``, see
    :func:`mask_url_secrets`), the user info of a URL of any other scheme
    (up to the last ``@`` before the host, so a password holding ``@`` or
    ``/`` is masked whole), the values of ``AccountKey=``,
    ``SharedAccessSignature=``, ``SharedAccessKey=``, ``Password=`` /
    ``Pwd=`` and ``account_key=`` (quoted or not), of a ``Bearer`` token
    and of the ``sig=``, ``code=``, ``access_token=`` and ``token=`` query
    parameters (``***``). Used on texts the
    SDK logs or serves that come from code it does not control, such as the
    exception a health check raised."""
    if not isinstance(text, str):
        return text
    if "://" in text:
        text = _BROKER_URL.sub(_mask_broker_url, text)
    if "://" in text:
        text = _ANY_URL.sub(_mask_any_url_userinfo, text)
    text = _QUOTED_SECRET.sub(
        lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}***{m.group(3)}", text
    )
    text = _CONNECTION_STRING_SECRET.sub(lambda m: f"{m.group(1)}=***", text)
    text = _BEARER_HEADER_TOKEN.sub(r"\1***", text)
    text = _BEARER_TOKEN.sub(r"\1***", text)
    return _SAS_SIGNATURE.sub(r"\1***", text)


# What a record carries in place of a text that could not be masked: the
# masking fails closed, so the text itself is never written.
_WITHHELD_TEXT = "<withheld: secret masking failed>"
_WITHHELD_MESSAGE = "<record text withheld: secret masking failed>"
_WITHHELD_TRACEBACK = "<traceback withheld: secret masking failed>"


class SecretMaskFilter(logging.Filter):
    """Masks the secrets (see :func:`redact_secrets`) in the text a record
    may quote from code the SDK does not control: its message, every text
    field (``extra={...}``: ``error``, ``error_message``,
    ``internal_message``, ``reason``, ``uri``, ...), its traceback and its
    stack. Fields that are not text (numbers, lists, dicts) are kept as they
    are.

    Attached to the logger of every SDK module (:func:`masked_logger`; a
    logger's filter only sees the records created at that logger, not those
    of its children) and to the handlers the SDK installs
    (:func:`setup_logging`, :func:`install_root_handler`): an exception text
    that quotes a connection URL or a storage key is logged masked, by
    default, in every log format.

    It fails closed: a field that cannot be masked is replaced by
    ``<withheld: secret masking failed>``, and when the message or the
    traceback cannot be masked, the record keeps its level, logger and
    fields, with ``<record text withheld: secret masking failed>`` as its
    message (and ``<traceback withheld: ...>`` as its traceback). The record
    itself is never dropped."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            self._mask(record)
        except Exception:
            self._withhold(record)
        return True

    @staticmethod
    def _text_fields(record: logging.LogRecord) -> list:
        return [
            (name, value)
            for name, value in list(record.__dict__.items())
            if name not in JSONFormatter._STANDARD_RECORD_ATTRS and isinstance(value, str)
        ]

    def _mask(self, record: logging.LogRecord) -> None:
        for name, value in self._text_fields(record):
            try:
                masked = redact_secrets(value)
            except Exception:
                masked = _WITHHELD_TEXT
            if masked != value:
                record.__dict__[name] = masked
        try:
            message = record.getMessage()
        except Exception:
            # The formatters then write the template: mask that.
            message = _safe_str(record.msg)
        masked = redact_secrets(message)
        if masked != message:
            record.msg = masked
            record.args = None
        if record.exc_info and record.exc_info[1] is not None:
            text = record.exc_text or logging.Formatter().formatException(record.exc_info)
            masked = redact_secrets(text)
            if masked != text:
                # Rendered once, masked; the formatters use it as it is.
                record.exc_text = masked
        if isinstance(record.stack_info, str):
            masked = redact_secrets(record.stack_info)
            if masked != record.stack_info:
                record.stack_info = masked

    @classmethod
    def _withhold(cls, record: logging.LogRecord) -> None:
        """Fail closed: replace every text of ``record`` that may quote a
        secret by a placeholder."""
        record.msg = _WITHHELD_MESSAGE
        record.args = None
        if record.exc_info:
            record.exc_text = _WITHHELD_TRACEBACK
        record.stack_info = None
        try:
            for name, _value in cls._text_fields(record):
                record.__dict__[name] = _WITHHELD_TEXT
        except Exception:  # pragma: no cover - a record whose __dict__ fails
            pass


def masked_logger(name: str) -> logging.Logger:
    """``logging.getLogger(name)`` with a :class:`SecretMaskFilter` attached
    (once). Every SDK module takes its logger from here, so the secrets in
    its records are masked whichever handler writes them (the SDK's own or
    one the application configured)."""
    logger = logging.getLogger(name)
    if not any(isinstance(f, SecretMaskFilter) for f in logger.filters):
        logger.addFilter(SecretMaskFilter())
    return logger


def _command_fields(envelope: Any) -> dict:
    """The fields that identify a command in every per-command record:
    ``correlation_id``, ``message_id`` (the key of the dead-letter queue
    message and of a log search across services), ``command_name``,
    ``trace_id`` (shown as ``[trace=...]`` in the text format),
    ``idempotency_key`` and, when the command carries one, ``job_id``. The
    envelope of a reply or a status update carries the same values as its
    command's."""
    fields = {
        "correlation_id": getattr(envelope, "correlation_id", None),
        "message_id": getattr(envelope, "message_id", None),
        "command_name": getattr(envelope, "name", None),
        "trace_id": getattr(envelope, "trace_id", None),
        "idempotency_key": getattr(envelope, "idempotency_key", None),
    }
    job_id = getattr(envelope, "job_id", None)
    if job_id is not None:
        fields["job_id"] = job_id
    return fields


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

    # Secrets are masked on the way out as well (handler-level, so it
    # covers the records of every child logger, including one that has no
    # filter of its own); the SDK modules' loggers mask them already.
    handler.addFilter(SecretMaskFilter())

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


# Marks the last-resort handler install_root_handler sets.
_LAST_RESORT_MARK = "_kbm_ledsas_last_resort"


class _StderrHandler(logging.Handler):
    """Writes each record as one line to the ``sys.stderr`` of the moment
    (looked up for each record, as Python's own last-resort handler does),
    so a later redirection of stderr is followed."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            stream = sys.stderr
            stream.write(self.format(record) + "\n")
            stream.flush()
        except RecursionError:  # pragma: no cover - as logging.StreamHandler
            raise
        except Exception:
            self.handleError(record)


class _WarningsFallback(logging.Handler):
    """On the ``py.warnings`` logger: hands the record of a Python warning
    to the last-resort handler when no handler of the application takes it.

    ``logging.captureWarnings(True)`` gives ``py.warnings`` a ``NullHandler``
    when it has none, so Python never hands a warning to its last resort.
    This handler stands in for that ``NullHandler``; a handler the
    application adds (to ``py.warnings``, a parent or the root logger, at
    any time) takes the record instead, so it is never written twice."""

    def __init__(self) -> None:
        super().__init__(logging.WARNING)

    def emit(self, record: logging.LogRecord) -> None:
        logger: logging.Logger | None = logging.getLogger(record.name)
        while logger is not None:
            for handler in logger.handlers:
                if handler is self or isinstance(handler, (logging.NullHandler, _WarningsFallback)):
                    continue
                return  # the application's own handler takes the record
            if not logger.propagate:
                break
            logger = logger.parent
        last = logging.lastResort
        if last is not None and record.levelno >= last.level:
            last.handle(record)


def _is_python_last_resort(handler: object) -> bool:
    """True when ``handler`` is Python's own last-resort handler (the one
    that writes the bare message), or one ``install_root_handler`` set."""
    if getattr(handler, _LAST_RESORT_MARK, False):
        return True
    default = getattr(logging, "_defaultLastResort", None)
    if default is not None and handler is default:
        return True
    stdlib_class = getattr(logging, "_StderrHandler", None)
    return isinstance(stdlib_class, type) and type(handler) is stdlib_class


def install_root_handler(
    log_format: str = "json",
    service_name: str | None = None,
) -> bool:
    """Write the records no handler takes, and Python warnings, in the SDK's
    log format, when the application has not configured logging itself.

    When the root logger has no handler (the application did not call
    ``logging.basicConfig`` or add a handler to the root logger), Python's
    last-resort handler (``logging.lastResort``, which writes only the
    message of a WARNING or higher record that no handler takes) is
    replaced with one that writes in the SDK's format (``log_format``: json
    or text) to stderr, and Python warnings are routed through logging
    (``logging.captureWarnings(True)``; ``py.warnings`` gets a handler
    that passes a warning on to that last resort when no handler of the
    application takes it, see ``_WarningsFallback``). The records of the
    asyncio event loop ("Task exception was never retrieved", with its
    traceback in the record), of the client libraries, of the application's
    loggers that have no handler and of warnings then go out in that format,
    one per line, instead of as plain multi-line text.

    Which records come out is unchanged: those at WARNING or higher that
    their logger's level lets through and that no handler takes. A logger
    the application gave a handler of its own is written by that handler
    only, so no record is written twice; INFO records still need a logging
    configuration of the application's own. A ``logging.basicConfig(...)``
    called later still configures the root logger, and from then on the
    last resort is not used. Nothing is changed when the root logger
    already has a handler, or when the application replaced
    ``logging.lastResort`` itself.

    Returns:
        True when the SDK's handler is now the last resort.
    """
    if logging.getLogger().handlers:
        return False
    if not _is_python_last_resort(logging.lastResort):
        return False
    handler = _StderrHandler(logging.WARNING)
    handler.setFormatter(JSONFormatter() if log_format == "json" else TextFormatter())
    # The records it writes come from other code (the event loop, the
    # client libraries): the secrets they quote are masked.
    handler.addFilter(SecretMaskFilter())
    if service_name:
        handler.addFilter(ServiceNameFilter(service_name))
    setattr(handler, _LAST_RESORT_MARK, True)
    logging.lastResort = handler
    warnings_logger = logging.getLogger("py.warnings")
    if not any(isinstance(h, _WarningsFallback) for h in warnings_logger.handlers):
        # Before captureWarnings, which adds a NullHandler only when the
        # logger has no handler.
        warnings_logger.addHandler(_WarningsFallback())
    logging.captureWarnings(True)
    return True


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
        datefmt: Optional strftime format for the ``timestamp`` field.
            It overrides the default UTC ISO 8601 format with
            milliseconds and a ``Z`` suffix (``2026-10-14T05:43:15.123Z``):
            the timestamp is then formatted by ``logging.Formatter``
            (local time, no milliseconds unless the format asks for
            them). Leave it unset to keep UTC timestamps.

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
