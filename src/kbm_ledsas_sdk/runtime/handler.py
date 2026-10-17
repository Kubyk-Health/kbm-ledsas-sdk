"""
Handler registry and execution for LEDSAS SDK.

The HandlerRegistry manages command handlers and executes them with proper
error handling, response generation, and error classification.

Handlers are async functions with the signature:
    async def handler(ctx: ExecutionContext, payload: dict) -> dict

The registry:
- Stores handlers keyed by (command_name, version)
- Executes handlers with ExecutionContext
- Wraps results in Response messages
- Classifies errors (Retryable, Permanent, DeadlineExceeded)
- Handles uncaught exceptions
"""

import asyncio
import collections
import collections.abc
import dataclasses
import enum
import inspect
import json
import logging
import os
import re
import sys
import traceback
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, NamedTuple

from pydantic import TypeAdapter, ValidationError

from ..blob.operations import (
    NON_RETRYABLE_MARKER,
    RETRYABLE_MARKER,
    _find_lone_surrogate,
    _lone_surrogate_in,
    _mark_non_retryable,
)
from ..models.envelope import Envelope
from ..models.errors import DeadlineExceeded, Permanent, Retryable
from ..models.messages import Command, Response
from .context import ExecutionContext

# Type alias for handler functions. A handler returns a JSON-serializable
# dict (the reply payload); returning None is accepted and treated as an
# empty result ({}), which suits fire-and-forget handlers.
HandlerFunc = Callable[[ExecutionContext, dict], Awaitable[dict | None]]

logger = logging.getLogger(__name__)


class HandlerOutcome(NamedTuple):
    """How one attempt of a command ended, as the handler layer decided it.

    ``failed`` is True when the reply is an error reply the SDK built: the
    handler raised, timed out, was not found, or returned an invalid
    result. ``retryable`` then says whether another attempt may succeed.
    A handler's own result is never read for this: a result that carries
    an ``"error"`` key (``{"valid": False, "error": "field x missing"}``)
    is a successful result like any other, replied and acknowledged.
    """

    response: Response
    failed: bool = False
    retryable: bool = False


# Command names and message versions a handler can be registered under.
# They mirror the envelope's ``name`` / ``message_version`` constraints
# (models/envelope.py): a handler registered under any other value could
# never receive a command, because the envelope of such a command fails
# validation and is dead-lettered before handler lookup.
_COMMAND_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_COMMAND_NAME_MAX_LENGTH = 64
_MESSAGE_VERSION_PATTERN = re.compile(r"^\d+\.\d+$")

# Default generic error messages for customer-facing responses
# Used when generic_errors=True to hide internal error details from callers.
# A reply is only sent once the SDK has finished with a command, so none of
# these promise a further attempt: "Processing deadline exceeded" covers
# both DeadlineExceeded cases (the deadline had passed when the command
# arrived, or it passed while the handler ran; resend with a fresh
# deadline); the others describe a failure the SDK has stopped retrying.
GENERIC_ERROR_MESSAGES = {
    "Retryable": "Processing failed temporarily",
    "Permanent": "Processing failed",
    "DeadlineExceeded": "Processing deadline exceeded",
    "Timeout": "Processing timed out",
    "UnexpectedError": "An unexpected error occurred",
    "HandlerNotFound": "Handler not available",
}

# HTTP statuses in the 4xx range that ARE worth retrying: request timeout
# and throttling. Every other 4xx from blob storage (missing blob, invalid
# container or blob name, authentication/authorization failure, conflict)
# fails the same way on every attempt.
_RETRYABLE_4XX = frozenset({408, 429})

# HTTP statuses from blob storage that mean "temporarily unavailable":
# request timeout, throttling and the server-side 5xx errors a retry can get
# past.
_TRANSIENT_STORAGE_STATUS = frozenset({408, 429, 500, 502, 503, 504})

# user:password@ in a URL quoted by an exception message.
_URL_USERINFO = re.compile(r"(\w+://)[^/\s@]+@")

# Python's default limit on the digits of an int converted to text; a reply
# value or key longer than this cannot be written as JSON.
_INT_MAX_STR_DIGITS = 4300


def _safe_str(exc: BaseException) -> str:
    """``str(exc)``, or a placeholder naming the class when the exception's
    own ``__str__`` raises (some third-party exception classes do). The
    SDK formats exception text on every failure path; a failing
    ``__str__`` must not stop the command from getting its one reply."""
    try:
        return str(exc)
    except Exception:
        try:
            name = type(exc).__name__
        except Exception:  # pragma: no cover - a broken class object
            name = "exception"
        return f"<{name}: str() failed>"


# ``_safe_user_message`` result for a user_message that was set but cannot
# be sent (not a string, or reading it raised): the caller then gets the
# generic message of the error's code, never the internal message.
_UNUSABLE_USER_MESSAGE = object()


def _safe_user_message(exc: BaseException) -> Any:
    """``exc.user_message``: None when it is not set (absent or None), the
    string when it is one (an empty string included: it is the message the
    handler chose), else ``_UNUSABLE_USER_MESSAGE`` (another type, which
    the SDK's error classes refuse at construction but a subclass or an
    assignment can set, or an attribute whose reading raises)."""
    try:
        value = getattr(exc, "user_message", None)
    except Exception:
        return _UNUSABLE_USER_MESSAGE
    if value is None or isinstance(value, str):
        return value
    return _UNUSABLE_USER_MESSAGE


def _encodable_text(text: str) -> str:
    """``text`` with each lone surrogate (a ``\\ud800`` escape a JSON
    request can carry, quoted into an error message) replaced by U+FFFD,
    so the error reply is text every JSON reader can decode (strict
    readers refuse an escaped lone surrogate, as for result payloads)."""
    if _lone_surrogate_in(text) is None:
        return text
    return "".join("\ufffd" if 0xD800 <= ord(c) <= 0xDFFF else c for c in text)


def _short_text(exc: BaseException) -> str:
    """First line of ``str(exc)``, shortened, URL credentials removed."""
    text = _safe_str(exc)
    first = text.splitlines()[0] if text else ""
    return _URL_USERINFO.sub(r"\1***@", first)[:200]


def _format_seconds(seconds: float) -> str:
    """``1800s`` for whole seconds, ``2.5s`` otherwise."""
    if float(seconds).is_integer():
        return f"{int(seconds)}s"
    return f"{seconds:.1f}s"


class _ResultProblem(Exception):
    """A handler result that cannot become a JSON reply; the text names
    where (``payload['a'][3]``) and why."""


def _json_key(key: Any, path: str) -> str:
    """The JSON object key ``json.dumps`` writes for ``key``.

    ``str`` keys are kept; ``int`` / ``float`` / ``bool`` / ``None`` become
    the text ``json.dumps`` writes for them (``1`` -> ``"1"``, ``True`` ->
    ``"true"``, ``None`` -> ``"null"``, ``float("nan")`` -> ``"NaN"``).
    Any other key type has no JSON form and is refused, naming where.
    """
    if isinstance(key, str):
        return key if type(key) is str else str.__str__(key)
    if isinstance(key, bool):
        return "true" if key else "false"
    if isinstance(key, int):
        try:
            return str(key)
        except ValueError:
            raise _ResultProblem(
                f"{path}: a key is an integer with more than " f"{_INT_MAX_STR_DIGITS} digits"
            ) from None
    if isinstance(key, float) or key is None:
        return json.dumps(key)
    shown = _safe_repr(key)
    raise _ResultProblem(
        f"{path}: the key {shown} ({type(key).__name__}) is not a JSON object "
        f"key; use str, int, float, bool or None keys"
    )


def _safe_repr(value: Any, limit: int = 40) -> str:
    try:
        text = repr(value)
    except Exception:
        text = f"<{type(value).__name__}>"
    text = text.encode("unicode_escape", "replace").decode("ascii", "replace")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _bytes_problem(value: Any, path: str) -> _ResultProblem:
    """The refusal of binary data (``bytes``, ``bytearray``, ``memoryview``)
    in a result, whatever its content: the reply serializer would decode
    bytes that happen to be UTF-8 text and fail on any other bytes, so a
    handler that passes its tests with text-like bytes would fail on real
    binary data. The text names the remedy."""
    kind = "bytes" if isinstance(value, bytes) else f"a {type(value).__name__}"
    return _ResultProblem(
        f"{path} is {kind}; a reply carries text, not binary data: decode "
        f"it if it is text (data.decode()), base64-encode it otherwise "
        f"(base64.b64encode(data).decode()), or store it in a blob and "
        f"return the reference"
    )


def _normalize_result(value: Any, path: str, active: set[int]) -> Any:
    """Return ``value`` with every dict key converted as ``json.dumps``
    converts it (see :func:`_json_key`), at every level, so a top-level key
    and a nested key are treated alike. Values are left to the reply
    serializer, except binary data (``bytes``, ``bytearray``,
    ``memoryview``), which is refused whatever its content (see
    :func:`_bytes_problem`).

    Everything the serializer writes as a JSON object or array is walked:
    dicts, lists, tuples, sets, frozensets, deques, a dataclass's fields
    (the dataclass becomes the dict of its fields), an ``Enum``'s value, and
    generators and other iterators, which are read here once into a list
    (the reply carries that list, so they are never read twice). A pydantic
    model (or pydantic dataclass) is not walked: it is written with its own
    serialization settings. Other containers are copied only where
    something changes.

    Raises _ResultProblem for a key with no JSON form, two keys that would
    become the same JSON key, binary data, a container that contains
    itself, or an iterator whose reading raises.
    """
    if isinstance(value, _BINARY):
        raise _bytes_problem(value, path)
    if isinstance(value, dict):
        if id(value) in active:
            raise _ResultProblem(f"{path}: the dict contains itself")
        active.add(id(value))
        try:
            out: dict[str, Any] = {}
            changed = False
            for key, item in value.items():
                json_key = key if type(key) is str else _json_key(key, path)
                if json_key in out:
                    raise _ResultProblem(
                        f"{path}: two keys become the same JSON key " f"{json.dumps(json_key)}"
                    )
                if type(item) not in _LEAF_TYPES and _walked(item):
                    new_item = _normalize_result(item, f"{path}[{json.dumps(json_key)}]", active)
                else:
                    new_item = item
                changed = changed or json_key is not key or new_item is not item
                out[json_key] = new_item
        finally:
            active.discard(id(value))
        return out if changed else value
    if isinstance(value, _SEQUENCES):
        # Lists, tuples, sets, frozensets and deques are written as JSON
        # arrays in their iteration order; the index is the place in that
        # array. Set items are hashable, so they hold no dict directly,
        # but a tuple, a dataclass or a generator among them can.
        if id(value) in active:
            raise _ResultProblem(f"{path}: the list contains itself")
        active.add(id(value))
        try:
            # Values of a leaf type are skipped with one set lookup
            # (results can hold millions of them).
            items: list | None = None
            for i, item in enumerate(value):
                if type(item) not in _LEAF_TYPES and _walked(item):
                    new_item = _normalize_result(item, f"{path}[{i}]", active)
                    if new_item is not item:
                        if items is None:
                            items = list(value)
                        items[i] = new_item
        finally:
            active.discard(id(value))
        return value if items is None else items
    if hasattr(type(value), "__pydantic_serializer__"):
        # A pydantic model or pydantic dataclass: its own serializer and
        # settings (ser_json_bytes, field serializers) write it.
        return value
    if isinstance(value, enum.Enum):
        # Written as its value.
        inner = value.value
        if type(inner) in _LEAF_TYPES or not _walked(inner):
            return value
        new_inner = _normalize_result(inner, f"{path}.value", active)
        return value if new_inner is inner else new_inner
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        # Written as a JSON object of its fields, in their order: it is
        # replaced by that dict (the same JSON), so the later checks of
        # the reply see inside it.
        if id(value) in active:
            raise _ResultProblem(f"{path}: the {type(value).__name__} contains itself")
        active.add(id(value))
        try:
            fields: dict[str, Any] = {}
            for field in dataclasses.fields(value):
                try:
                    item = getattr(value, field.name)
                except Exception as e:
                    raise _ResultProblem(
                        f"{path}: reading its field {json.dumps(field.name)} "
                        f"raised {type(e).__name__}"
                    ) from None
                if type(item) not in _LEAF_TYPES and _walked(item):
                    new_item = _normalize_result(item, f"{path}[{json.dumps(field.name)}]", active)
                else:
                    new_item = item
                fields[field.name] = new_item
        finally:
            active.discard(id(value))
        return fields
    if isinstance(value, collections.abc.Iterator):
        # A generator, map(), zip(), an open file, ...: written as the
        # list of its items. Read once, here; the reply carries this list.
        try:
            items = list(value)
        except Exception as e:
            raise _ResultProblem(
                f"{path}: reading this {type(value).__name__} raised "
                f"{type(e).__name__}: {_short_text(e)}"
            ) from None
        return _normalize_result(items, path, active)
    return value


# Binary data, refused in a result whatever its content (_bytes_problem).
_BINARY = (bytes, bytearray, memoryview)

# Containers written as a JSON array.
_SEQUENCES = (list, tuple, set, frozenset, collections.deque)

# Types whose values hold nothing to walk (no key, no binary data).
_LEAF_TYPES = frozenset({str, int, float, bool, type(None)})

# The types _normalize_result looks into, besides dataclass instances.
_WALKED_TYPES = (dict, *_SEQUENCES, *_BINARY, enum.Enum, collections.abc.Iterator)


def _walked(value: Any) -> bool:
    """Whether :func:`_normalize_result` looks into ``value`` (see there)."""
    return isinstance(value, _WALKED_TYPES) or (
        dataclasses.is_dataclass(value) and not isinstance(value, type)
    )


# Serializes one value the way a reply payload's values are serialized.
_JSONABLE: TypeAdapter[Any] = TypeAdapter(Any)


# "<class 'pkg.module.Name'>" in a library error text: the caller is told
# the class name only, never the handler's module path.
_CLASS_REPR = re.compile(r"<class '(?:[\w.]+\.)?(\w+)'>")


def _type_name(value: Any) -> str:
    try:
        return type(value).__name__
    except Exception:  # pragma: no cover - a broken class object
        return "object"


def _value_problem(value: Any) -> str | None:
    """Why a single (non-container) reply value has no JSON form, or None.

    The text is the SDK's own wording (``an instance of Weird, which has no
    JSON form``: article-neutral, so it reads right for every class name,
    ``object`` included), never the serializer's message, and never names
    the module the value's class is defined in."""
    try:
        json.dumps(_JSONABLE.dump_python(value, mode="json"))
        return None
    except UnicodeDecodeError:
        # Binary data outside a pydantic model is refused before this
        # point (_normalize_result); a model writes its bytes as UTF-8
        # text unless it sets ser_json_bytes.
        return (
            f"an instance of {_type_name(value)} holding bytes that are not "
            f"UTF-8 text (a pydantic model writes bytes as UTF-8 text unless "
            f"it sets ser_json_bytes); base64-encode them, or store them in "
            f"a blob and return the reference"
        )
    except UnicodeEncodeError:
        # A dict key with a lone surrogate inside an object the SDK does
        # not walk (a pydantic model).
        return (
            f"an instance of {_type_name(value)} holding text with a lone "
            f"surrogate, which UTF-8 cannot encode"
        )
    except ValueError as e:
        text = _safe_str(e)
        if "digits" in text:
            return (
                f"an integer with more than {_INT_MAX_STR_DIGITS} digits; "
                f"return it as a string or in a blob"
            )
        if "Unable to serialize unknown type" in text:
            # The serializer's "unknown type" error (pydantic's
            # PydanticSerializationError, a ValueError): an arbitrary
            # object.
            return f"an instance of {_type_name(value)}, which has no JSON form"
        return _CLASS_REPR.sub(r"\1", text.split(";")[0])[:120]
    except Exception:
        return f"an instance of {_type_name(value)}, which has no JSON form"


def _locate_unserializable(value: Any, path: str = "payload", depth: int = 0) -> str | None:
    """``<path>: <reason>`` for the first value of a result that has no
    JSON form, or None if every single value has one (the failure is then
    about the whole structure, e.g. its depth)."""
    if depth > 300:
        return None
    if isinstance(value, dict):
        for key, item in value.items():
            found = _locate_unserializable(item, f"{path}[{json.dumps(str(key))}]", depth + 1)
            if found:
                return found
        return None
    if isinstance(value, (list, tuple)):
        for i, item in enumerate(value):
            found = _locate_unserializable(item, f"{path}[{i}]", depth + 1)
            if found:
                return found
        return None
    reason = _value_problem(value)
    return f"{path} is {reason}" if reason else None


def _transient_reason(exc: BaseException) -> str | None:
    """Return a short reason when an uncaught handler exception is a
    transient storage or network failure (one a later attempt is expected to
    get past, e.g. storage briefly unreachable), else None.

    Covered: blob-storage client errors for a request that could not be sent
    or answered (connection refused or reset, DNS failure, read timeout), a
    storage response with HTTP 408, 429, 500, 502, 503 or 504
    (``_TRANSIENT_STORAGE_STATUS``; other 5xx such as 501, 505 or 507 are
    not transient), Python's ``ConnectionError`` family, and an error the
    SDK's blob helpers mark with ``RETRYABLE_MARKER`` (a blob overwritten
    while ``download_stream`` read it). Such an error
    is still retried as an ``UnexpectedError`` (the reply is unchanged), but
    it is logged as one concise WARNING per attempt, never with a traceback:
    its stack says nothing the one line does not. Detection is duck-typed
    (no import of the storage client library).
    """
    # Marked by the SDK's blob helpers: an error a new attempt gets past
    # although its class looks permanent (the blob changed while
    # download_stream read it).
    marker = getattr(exc, RETRYABLE_MARKER, None)
    if isinstance(marker, str) and marker:
        return marker
    storage_classes = {
        cls.__name__ for cls in type(exc).__mro__ if (cls.__module__ or "").startswith("azure.")
    }
    if storage_classes & {"ServiceRequestError", "ServiceResponseError"}:
        return f"blob storage not reachable ({type(exc).__name__}: {_short_text(exc)})"
    status = getattr(exc, "status_code", None)
    if "HttpResponseError" in storage_classes and status in _TRANSIENT_STORAGE_STATUS:
        code = getattr(exc, "error_code", None)
        detail = f"HTTP {status}, {code}" if code else f"HTTP {status}"
        return f"blob storage temporarily unavailable ({detail})"
    if isinstance(exc, ConnectionError):
        return f"connection error ({type(exc).__name__}: {_short_text(exc)})"
    return None


def _error_location(exc: BaseException) -> str | None:
    """``file.py:line in function`` of the innermost frame of ``exc``'s
    traceback (where it was raised), or None."""
    frames = traceback.extract_tb(exc.__traceback__) if exc.__traceback__ else []
    if not frames:
        return None
    frame = frames[-1]
    return f"{os.path.basename(frame.filename)}:{frame.lineno} in {frame.name}"


def _non_retryable_reason(exc: BaseException) -> str | None:
    """Return a short reason when an uncaught handler exception can never
    succeed on a retry, else None (the exception is then retried as an
    UnexpectedError).

    Covered: blob-storage client errors (HTTP 4xx other than 408/429, e.g.
    a missing blob), an invalid blob URI, and every error the SDK marks
    with ``NON_RETRYABLE_MARKER``: blob content that is not valid JSON
    (``download_json``) or not UTF-8 text (``download_text``), invalid
    arguments to an SDK call (``upload_text`` given a non-string,
    ``upload_json`` given an object with no JSON form, ``emit_status``
    progress outside [0, 1]), and a handler that is not async. Detection
    is duck-typed (no import of the storage client library), so it also
    works where that library is not installed.
    """
    # An error the SDK marked worth retrying (the blob changed while
    # download_stream read it: HTTP 412 of its own consistency check) is
    # retried even though it is a storage 4xx.
    retry_marker = getattr(exc, RETRYABLE_MARKER, None)
    if isinstance(retry_marker, str) and retry_marker:
        return None
    status = getattr(exc, "status_code", None)
    module = type(exc).__module__ or ""
    if (
        module.startswith("azure.")
        and isinstance(status, int)
        and 400 <= status < 500
        and status not in _RETRYABLE_4XX
    ):
        return f"blob storage rejected the request (HTTP {status}, {type(exc).__name__})"
    if isinstance(exc, ValidationError) and getattr(exc, "title", "") == "BlobRef":
        errors = exc.errors()
        msg = str(errors[0].get("msg", "")) if errors else ""
        msg = msg.removeprefix("Value error, ")
        return f"invalid blob URI ({msg})" if msg else "invalid blob URI"
    # Set by the SDK's blob helpers on errors that depend only on the
    # blob's content (e.g. download_json on content that is not JSON).
    marker = getattr(exc, NON_RETRYABLE_MARKER, None)
    if isinstance(marker, str) and marker:
        return marker
    return None


def _not_async_reason(handler: Any) -> str | None:
    """Return why ``handler`` cannot be awaited by the SDK, else None.

    A handler must be an ``async def`` function (a coroutine function).
    Plain functions, lambdas and ``async def`` generators (an ``async
    def`` that uses ``yield``) are refused. Other callables (a
    ``functools.partial`` of an ``async def``, an object with an
    ``async def __call__``, a test double) are accepted here; if calling
    one returns something that is not awaitable, :meth:`HandlerRegistry.execute`
    reports it once as a ``Permanent`` error.
    """
    if not callable(handler):
        return f"got {type(handler).__name__}, which is not callable"
    if inspect.iscoroutinefunction(handler):
        return None
    try:
        unwrapped = inspect.unwrap(handler)
    except ValueError:  # pragma: no cover - a __wrapped__ cycle
        unwrapped = handler
    if inspect.iscoroutinefunction(unwrapped):
        return None
    if inspect.isasyncgenfunction(handler) or inspect.isasyncgenfunction(unwrapped):
        return "it is an async generator (an 'async def' that uses 'yield')"
    if inspect.isfunction(handler) or inspect.ismethod(handler) or inspect.isbuiltin(handler):
        return "it is a regular function (defined with 'def', or a lambda)"
    return None


class HandlerRegistry:
    """
    Registry for command handlers.

    Handlers are registered by command name and version, then executed when
    matching commands are received.

    Example:
        registry = HandlerRegistry()

        async def process_dataset(ctx: ExecutionContext, payload: dict) -> dict:
            # Handler logic here
            return {"result": "success"}

        registry.register("ProcessDataset", "1.0", process_dataset)

        # Execute handler
        command = Command(...)
        ctx = ExecutionContext(...)
        response = await registry.execute(ctx, command)
    """

    def __init__(self, generic_errors: bool = False, max_reply_bytes: int = 0):
        """
        Initialize handler registry.

        Args:
            generic_errors: If True, return generic error messages to callers
                           instead of specific error details. Defaults to False.
            max_reply_bytes: Largest serialized reply (bytes) a successful
                result may produce when the command asks for a reply
                (``reply_to`` set); a larger one becomes a Permanent error
                reply instead (the message broker would refuse it). A
                command without ``reply_to`` publishes no reply, so its
                result is not size-checked. 0 disables the check.
                ``ServiceApp`` sets it from ``KBM_LEDSAS_MAX_PAYLOAD_BYTES``.
        """
        self._handlers: dict[tuple[str, str], HandlerFunc] = {}
        self._generic_errors = generic_errors
        self.max_reply_bytes = max_reply_bytes

    def register(self, command_name: str, version: str, handler: HandlerFunc) -> None:
        """
        Register a handler for a command.

        Args:
            command_name: Command name (e.g., "ProcessDataset")
            version: Message version (e.g., "1.0")
            handler: Async handler function

        Raises:
            ValueError: If handler already registered for this (name, version),
                or if ``command_name`` / ``version`` is not a value a
                command envelope can carry (``command_name`` must match
                ``^[A-Za-z][A-Za-z0-9_]*$``, at most 64 characters, e.g.
                ``ProcessDataset``; ``version`` must look like ``1.0``).
                Such a handler could never be invoked: a command with that
                name or version fails envelope validation and is
                dead-lettered, so the SDK refuses it at registration.
            TypeError: If ``handler`` is not an ``async def`` function
                (a plain ``def``, a lambda, or an async generator). The
                SDK awaits every handler; a plain function would run on
                every attempt and then fail, so it is refused here.

        Example:
            registry.register("ProcessDataset", "1.0", my_handler)
        """
        if (
            not isinstance(command_name, str)
            or len(command_name) > _COMMAND_NAME_MAX_LENGTH
            or not _COMMAND_NAME_PATTERN.fullmatch(command_name)
        ):
            raise ValueError(
                f"Invalid command name {command_name!r}: use a letter "
                f"followed by letters, digits or underscores (at most "
                f"{_COMMAND_NAME_MAX_LENGTH} characters), e.g. "
                f"'ProcessDataset' or 'process_data'. Command envelopes "
                f"with any other name are rejected, so the handler could "
                f"never be called."
            )
        if not isinstance(version, str) or not _MESSAGE_VERSION_PATTERN.fullmatch(version):
            raise ValueError(
                f"Invalid message version {version!r} for {command_name}: "
                f"use '<major>.<minor>' digits, e.g. '1.0'."
            )
        not_async = _not_async_reason(handler)
        if not_async is not None:
            raise TypeError(
                f"Handler for {command_name} v{version} must be an async "
                f"function ('async def'): {not_async}. The SDK awaits every "
                f"handler; run blocking code inside it with "
                f"'await asyncio.to_thread(...)'."
            )
        key = (command_name, version)
        if key in self._handlers:
            raise ValueError(f"Handler already registered for {command_name} v{version}")
        self._handlers[key] = handler
        # Not logged here: a handler is registered when its module is
        # imported (the @app.handler decorator), before ServiceApp.run()
        # has applied KBM_LEDSAS_LOG_LEVEL / KBM_LEDSAS_LOG_FORMAT, so the
        # record would go out in whatever format the root logger has.
        # ServiceApp logs the registered handlers once it starts
        # (log_registered).

    def log_registered(self) -> None:
        """One INFO record per registered handler (``Registered handler for
        <name> v<version>``). Called by ``ServiceApp`` once its logging is
        set up, so the records use the SDK's log format and level."""
        for command_name, version in self._handlers:
            logger.info(
                "Registered handler for %s v%s",
                command_name,
                version,
                extra={"command_name": command_name, "message_version": version},
            )

    def get(self, command_name: str, version: str) -> HandlerFunc | None:
        """
        Get handler for a command.

        Args:
            command_name: Command name
            version: Message version

        Returns:
            Handler function or None if not found
        """
        return self._handlers.get((command_name, version))

    def list_handlers(self) -> list[tuple[str, str]]:
        """
        List all registered handlers.

        Returns:
            List of (command_name, version) tuples
        """
        return list(self._handlers.keys())

    def _get_error_message(self, code: str, specific_message: str, user_message: Any = None) -> str:
        """
        Resolve the message the caller will see.

        Precedence (v0.1.7+):
        1. If the handler supplied ``user_message`` (a string, an empty
           one included), the caller always sees that. A user_message that
           was set but cannot be sent (``_UNUSABLE_USER_MESSAGE``) gives
           the generic message: once a user_message was given, the
           internal message is never sent.
        2. Otherwise, if ``generic_errors`` is enabled, the caller sees the
           generic fallback (e.g. "Processing failed").
        3. Otherwise, the caller sees the raw internal ``specific_message``.

        A lone surrogate in the text (quoted from a request value) is
        replaced by U+FFFD. The internal ``specific_message`` is always
        available in logs regardless of these settings.
        """
        if user_message is _UNUSABLE_USER_MESSAGE:
            return GENERIC_ERROR_MESSAGES.get(code, "An error occurred")
        if isinstance(user_message, str):
            return _encodable_text(user_message)
        if self._generic_errors:
            return GENERIC_ERROR_MESSAGES.get(code, "An error occurred")
        return _encodable_text(specific_message)

    def _calculate_timeout(self, envelope: Envelope, config_timeout: int) -> float | None:
        """
        Calculate effective timeout (minimum of deadline remaining and config).

        Priority:
        1. If deadline is set and config_timeout > 0: use minimum
        2. If only deadline is set: use remaining time
        3. If only config_timeout > 0: use config_timeout
        4. If neither: return None (no timeout)

        Args:
            envelope: Command envelope (may contain deadline)
            config_timeout: Configured timeout in seconds (0=disabled)

        Returns:
            Effective timeout in seconds, or None if no timeout should be applied

        Raises:
            DeadlineExceeded: When ``envelope.deadline`` is in the past at
                computation time. Callers should invoke this from inside
                the same try-block that catches DeadlineExceeded so the
                response is classified terminal (not retryable Timeout).
        """
        if envelope.deadline is not None:
            remaining = (envelope.deadline - datetime.now(UTC)).total_seconds()
            if remaining <= 0:
                # Deadline already in the past (e.g. raced past the
                # explicit pre-flight check). Raise so the response is
                # classified DeadlineExceeded (terminal) — using a tiny
                # positive timeout would misclassify this as Timeout
                # (retryable), which would have the orchestrator retry
                # against the same expired deadline.
                raise DeadlineExceeded(f"Deadline {envelope.deadline.isoformat()} already passed")
            if config_timeout > 0:
                return min(remaining, config_timeout)
            return remaining
        return config_timeout if config_timeout > 0 else None

    async def execute(
        self,
        ctx: ExecutionContext,
        command: Command,
        timeout_seconds: int = 0,
        *,
        final_attempt: bool | None = None,
    ) -> Response:
        """Execute the handler for a command and return its reply.

        The same as :meth:`execute_with_outcome`, returning only the
        ``Response``. Whether the attempt failed is not something to read
        from the reply's payload (a successful result may carry an
        ``"error"`` key): use :meth:`execute_with_outcome` for that.
        """
        outcome = await self.execute_with_outcome(
            ctx, command, timeout_seconds, final_attempt=final_attempt
        )
        return outcome.response

    async def execute_with_outcome(
        self,
        ctx: ExecutionContext,
        command: Command,
        timeout_seconds: int = 0,
        *,
        final_attempt: bool | None = None,
    ) -> HandlerOutcome:
        """
        Execute handler for a command and return the attempt's outcome.

        This method:
        1. Looks up the handler for the command
        2. Checks deadline before starting
        3. Executes the handler with error handling and timeout enforcement
        4. Wraps result in Response message
        5. Classifies errors (Retryable/Permanent/Deadline/Timeout)

        Args:
            ctx: Execution context
            command: Command to execute
            timeout_seconds: Handler timeout in seconds (0=disabled, uses deadline if set)
            final_attempt: Whether a retryable failure of this attempt ends
                the command (its retries are spent). An unexpected
                exception is logged as one WARNING per attempt; the
                traceback is attached only when this is True, so a command
                carries at most one traceback, on its last attempt (a
                transient storage or network error never carries one).
                ``None`` (no retry context, e.g. a transport that keeps no
                retry count) keeps the ERROR with the traceback on every
                call.

        Returns:
            HandlerOutcome: the reply (success or error) and, decided here
            from how the handler ended (never from its result's keys),
            whether the attempt failed and whether a retry may succeed.
            Handler exceptions (Retryable, Permanent, DeadlineExceeded, a
            timeout, any other exception) become error replies; they are
            not raised to the caller.

        Example:
            outcome = await registry.execute_with_outcome(ctx, command, timeout_seconds=30)
            if outcome.failed and outcome.retryable:
                ...  # retry later
        """
        envelope = command.envelope
        payload = command.payload

        # Look up handler
        handler = self.get(envelope.name, envelope.message_version)
        if handler is None:
            # No handler registered - permanent error
            logger.error(
                f"No handler for {envelope.name} v{envelope.message_version}",
                extra={"correlation_id": envelope.correlation_id},
            )
            specific_msg = f"No handler registered for {envelope.name} v{envelope.message_version}"
            return self._error_outcome(
                envelope,
                {
                    "code": "HandlerNotFound",
                    "message": self._get_error_message("HandlerNotFound", specific_msg),
                    "retryable": False,
                },
            )

        # Execute handler. Deadline-pre-flight and timeout-calculation both
        # live inside this try block so a deadline-already-past finding
        # lands in the DeadlineExceeded branch (terminal classification)
        # rather than escaping execute() as an uncaught exception.
        effective_timeout: float | None = None
        # The SDK's own timeout context (None when no timeout applies). Its
        # expired() tells an SDK timeout apart from a TimeoutError the
        # handler raised itself.
        timeout_cm: Any = None
        # True when the effective timeout is the time left to
        # envelope.deadline (not KBM_LEDSAS_HANDLER_TIMEOUT): its expiry
        # means the deadline passed, a terminal DeadlineExceeded.
        deadline_bound = False
        try:
            # Deadline pre-flight: raises DeadlineExceeded if already in
            # the past. Caught below to produce a terminal Response.
            if envelope.deadline is not None:
                now = datetime.now(UTC)
                if now >= envelope.deadline:
                    logger.warning(
                        "Deadline already exceeded",
                        extra={
                            "correlation_id": envelope.correlation_id,
                            "command_name": envelope.name,
                            "deadline": envelope.deadline.isoformat(),
                            "now": now.isoformat(),
                        },
                    )
                    raise DeadlineExceeded(
                        f"Deadline {envelope.deadline.isoformat()} already passed"
                    )

            # Calculate effective timeout (may also raise DeadlineExceeded
            # for a tiny race window between the pre-flight check and now).
            effective_timeout = self._calculate_timeout(envelope, timeout_seconds)
            deadline_bound = (
                envelope.deadline is not None
                and effective_timeout is not None
                and (timeout_seconds <= 0 or effective_timeout < timeout_seconds)
            )

            logger.info(
                "Executing handler",
                extra={
                    "correlation_id": envelope.correlation_id,
                    "command_name": envelope.name,
                    "timeout": effective_timeout,
                },
            )

            call = handler(ctx, payload)
            if not inspect.isawaitable(call):
                # Registration refuses plain functions; another callable
                # that returns a value instead of an awaitable has already
                # run its body, and would run it again on every retry
                # only to fail the same way: report it once, not retried.
                err = TypeError(
                    f"handler returned {type(call).__name__} instead of an "
                    f"awaitable; define it with 'async def'"
                )
                _mark_non_retryable(err, f"the handler is not async ({err})")
                raise err

            # Apply timeout if configured. asyncio.timeout() (Python 3.11+)
            # runs the handler in this task, and its expired() tells the
            # SDK's own timeout apart from a TimeoutError the handler
            # raised itself.
            if effective_timeout is not None and effective_timeout > 0:
                async with asyncio.timeout(effective_timeout) as timeout_cm:
                    result = await call
            else:
                result = await call

            # Build the success response. An invalid return value (not a
            # dict, or not JSON-serializable) yields a Permanent error
            # response instead of raising: the handler body already ran,
            # so a retry would re-run its side effects for the same result.
            return self._result_outcome(envelope, result)

        except TimeoutError:
            e = sys.exc_info()[1]
            if timeout_cm is not None and timeout_cm.expired():
                if deadline_bound:
                    # The time left to the command's deadline ran out while
                    # the handler ran: the deadline has passed, so this is
                    # terminal (a retry would arrive after it too).
                    return self._deadline_exceeded_outcome(
                        envelope,
                        DeadlineExceeded(
                            f"Deadline {envelope.deadline.isoformat()} passed while "
                            f"the handler ran (after {_format_seconds(effective_timeout)})"
                        ),
                    )
                # The SDK's KBM_LEDSAS_HANDLER_TIMEOUT expired - retryable.
                logger.warning(
                    "Handler timeout",
                    extra={
                        "correlation_id": envelope.correlation_id,
                        "command_name": envelope.name,
                        "timeout_seconds": effective_timeout,
                    },
                )
                specific = f"Handler timed out after {_format_seconds(effective_timeout)}"
            else:
                # The handler raised TimeoutError itself (e.g. a client
                # library's own timeout): reported as code Timeout, as
                # documented, without claiming the SDK's timeout elapsed.
                extra = {
                    "correlation_id": envelope.correlation_id,
                    "command_name": envelope.name,
                    "error_class": type(e).__name__,
                    "error_message": _safe_str(e),
                }
                location = _error_location(e)
                if location is not None:
                    extra["error_location"] = location
                logger.warning("Handler raised TimeoutError", extra=extra)
                specific = "Handler raised TimeoutError"
            return self._error_outcome(
                envelope,
                {
                    "code": "Timeout",
                    "message": self._get_error_message("Timeout", specific),
                    "retryable": True,
                },
            )

        except Retryable as e:
            # Retryable error - will be retried with backoff. Caller-controlled
            # text travels via extra={"error_message": ...}: the JSON
            # formatter escapes any control characters, and the text
            # formatter escapes them in its key=value pairs.
            text = _safe_str(e)
            logger.warning(
                "Retryable error in handler",
                extra={
                    "correlation_id": envelope.correlation_id,
                    "command_name": envelope.name,
                    "error_message": text,
                },
            )
            return self._error_outcome(
                envelope,
                {
                    "code": "Retryable",
                    "message": self._get_error_message("Retryable", text, _safe_user_message(e)),
                    "retryable": True,
                },
            )

        except Permanent as e:
            # Permanent error - goes to DLQ. See Retryable branch above for
            # why ``error_message`` rides in ``extra`` rather than the
            # message string.
            text = _safe_str(e)
            logger.error(
                "Permanent error in handler",
                extra={
                    "correlation_id": envelope.correlation_id,
                    "command_name": envelope.name,
                    "error_message": text,
                },
            )
            return self._error_outcome(
                envelope,
                {
                    "code": "Permanent",
                    "message": self._get_error_message("Permanent", text, _safe_user_message(e)),
                    "retryable": False,
                },
            )

        except DeadlineExceeded as e:
            # Deadline exceeded - NOT retryable (deadline already passed).
            return self._deadline_exceeded_outcome(envelope, e)

        except asyncio.CancelledError as e:
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                # This task itself is being cancelled (by the SDK or the
                # event loop): not the handler's failure. Let it propagate;
                # the caller settles the delivery for redelivery.
                raise
            # The handler raised CancelledError while nothing cancelled this
            # task (e.g. it awaited a sub-task or future that was
            # cancelled). It is a handler failure like any other uncaught
            # exception: retried, then dead-lettered with one reply.
            internal_msg = (
                "Unexpected error (CancelledError): the handler raised "
                "asyncio.CancelledError without the SDK cancelling it (for "
                "example it awaited a task or future that was cancelled)"
            )
            return self._unexpected_error_outcome(e, envelope, internal_msg, final_attempt)

        except (SystemExit, KeyboardInterrupt) as e:
            # A handler must not end the process: contained as a Permanent
            # handler error (one ERROR, one reply, dead-lettered, never
            # retried), so the service keeps consuming instead of exiting
            # and crash-looping on the redelivered command.
            reason = (
                f"the handler raised {type(e).__name__} "
                f"({_short_text(e) or 'no message'}); a handler must not exit "
                f"the process"
            )
            extra = {
                "correlation_id": envelope.correlation_id,
                "command_name": envelope.name,
                "error_class": type(e).__name__,
            }
            location = _error_location(e)
            if location is not None:
                extra["error_location"] = location
            logger.error("Non-retryable error in handler (not retried): %s", reason, extra=extra)
            return self._error_outcome(
                envelope,
                {
                    "code": "Permanent",
                    "message": self._get_error_message("Permanent", reason),
                    "retryable": False,
                },
            )

        except Exception as e:
            # Uncaught exceptions that can never succeed on a retry
            # (missing blob, invalid blob URI, non-JSON blob content in
            # download_json — see _non_retryable_reason) are classified
            # Permanent: one concise ERROR line, no traceback, no retries.
            reason = _non_retryable_reason(e)
            if reason is not None:
                logger.error(
                    "Non-retryable error in handler (not retried): %s",
                    reason,
                    extra={
                        "correlation_id": envelope.correlation_id,
                        "command_name": envelope.name,
                        "error_class": type(e).__name__,
                    },
                )
                return self._error_outcome(
                    envelope,
                    {
                        "code": "Permanent",
                        "message": self._get_error_message("Permanent", reason),
                        "retryable": False,
                    },
                )

            # Any other uncaught exception - treat as retryable. Do NOT pass str(e)
            # to the customer-facing message path: the exception's str
            # repr often leaks internal field names
            # (KeyError("authorization_token") becomes
            # "'authorization_token'") that disclose handler state. The
            # full detail still goes to logs via ``internal_message`` in
            # the extra fields. Callers always see the generic message;
            # handlers that want richer caller-visible text should raise
            # errors.Permanent / Retryable with user_message= explicitly.
            internal_msg = f"Unexpected error ({type(e).__name__}): {_safe_str(e)}"
            return self._unexpected_error_outcome(e, envelope, internal_msg, final_attempt)

        except BaseException as e:
            # Any other BaseException a handler raises (a custom
            # BaseException subclass, GeneratorExit): an uncaught handler
            # failure like any Exception - retried, then dead-lettered with
            # one reply - so it can never leave the command unsettled.
            internal_msg = f"Unexpected error ({type(e).__name__}): {_safe_str(e)}"
            return self._unexpected_error_outcome(e, envelope, internal_msg, final_attempt)

    def _error_outcome(self, envelope: Envelope, error: dict) -> HandlerOutcome:
        """A failed attempt: the error reply (``{"error": {"code",
        "message", "retryable"}}``, the wire format) for the ``error`` the
        branch that caught the failure built, and the outcome the caller
        acts on, taken from that same SDK-built object (never from a
        handler's result)."""
        response = Response(
            envelope=self._build_response_envelope(envelope),
            payload={"error": error},
        )
        return HandlerOutcome(response, failed=True, retryable=bool(error["retryable"]))

    def _deadline_exceeded_outcome(
        self, envelope: Envelope, error: BaseException
    ) -> HandlerOutcome:
        """The terminal DeadlineExceeded reply (one WARNING, not retried)."""
        # Caller-controlled text via extra; see the Retryable branch.
        text = _safe_str(error)
        logger.warning(
            "Deadline exceeded in handler",
            extra={
                "correlation_id": envelope.correlation_id,
                "command_name": envelope.name,
                "error_message": text,
            },
        )
        return self._error_outcome(
            envelope,
            {
                "code": "DeadlineExceeded",
                "message": self._get_error_message(
                    "DeadlineExceeded", text, _safe_user_message(error)
                ),
                "retryable": False,  # Deadlines are terminal — see errors.py
            },
        )

    def _unexpected_error_outcome(
        self,
        error: BaseException,
        envelope: Envelope,
        internal_msg: str,
        final_attempt: bool | None,
    ) -> HandlerOutcome:
        """Log an uncaught handler failure and build its retryable
        ``UnexpectedError`` reply (the caller sees the generic message)."""
        self._log_unexpected_error(error, envelope, internal_msg, final_attempt)
        return self._error_outcome(
            envelope,
            {
                "code": "UnexpectedError",
                "message": GENERIC_ERROR_MESSAGES["UnexpectedError"],
                "retryable": True,
            },
        )

    @staticmethod
    def _log_unexpected_error(
        error: BaseException,
        envelope: Envelope,
        internal_msg: str,
        final_attempt: bool | None,
    ) -> None:
        """Log an uncaught, retryable handler exception for one attempt.

        One WARNING per attempt; the failed command itself gets its one
        ERROR (``Max retries (N) exceeded``) when its retries are spent:

        - a transient storage or network error (see ``_transient_reason``):
          ``Transient error in handler: <reason>``, never a traceback;
        - any other exception: ``Uncaught exception in handler`` with the
          error class, message and the line that raised it. The traceback
          is attached on the command's last attempt only, so a command
          that fails every attempt logs it once, and one that succeeds on
          a retry logs none.

        Without a retry context (``final_attempt`` is None) the record is
        an ERROR with the traceback, as for a single call.
        """
        extra = {
            "correlation_id": envelope.correlation_id,
            "command_name": envelope.name,
            "internal_message": internal_msg,
            "error_class": type(error).__name__,
        }
        if final_attempt is None:
            logger.error("Uncaught exception in handler", exc_info=error, extra=extra)
            return
        extra["will_retry"] = not final_attempt
        transient = _transient_reason(error)
        if transient is not None:
            logger.warning("Transient error in handler: %s", transient, extra=extra)
            return
        location = _error_location(error)
        if location is not None:
            extra["error_location"] = location
        logger.warning(
            "Uncaught exception in handler",
            exc_info=error if final_attempt else None,
            extra=extra,
        )

    def _build_success_response(self, envelope: Envelope, result: Any) -> Response:
        """The reply for a handler's return value (see :meth:`_result_outcome`)."""
        return self._result_outcome(envelope, result).response

    def _result_outcome(self, envelope: Envelope, result: Any) -> HandlerOutcome:
        """Wrap a handler's return value in a success Response.

        ``None`` is accepted and treated as ``{}`` (handlers that only have
        side effects, e.g. fire-and-forget commands, often return nothing).
        Anything else that is not a JSON-serializable ``dict`` is a
        programming error in the handler: it becomes a Permanent error
        response (one ERROR line, no traceback). It is never retried, since
        the handler body already ran and a retry would repeat its side
        effects only to return the same value.

        The reply's payload is the result's JSON form as checked here
        (values converted once, as listed in the API reference): the
        transport sends exactly what was checked, so a value that can be
        read only once (a generator) is not sent empty.
        """
        response_envelope = self._build_response_envelope(envelope)
        if result is None:
            result = {}
        problem: str | None = None
        response: Response | None = None
        if inspect.isawaitable(result):
            # ``return helper()`` without ``await``: the returned coroutine
            # never ran. Close it so Python does not print its own
            # "coroutine ... was never awaited" warning outside the SDK's
            # log format, and name the likely fix.
            close = getattr(result, "close", None)
            if inspect.iscoroutine(result) and callable(close):
                try:
                    close()
                except Exception:  # pragma: no cover - close() of a coroutine
                    pass
            problem = (
                f"returned {type(result).__name__} (an awaitable that was "
                f"never awaited; did you forget 'await'?); a handler must "
                f"return a dict (or None)"
            )
        elif not isinstance(result, dict):
            problem = (
                f"returned {type(result).__name__}; a handler must return a " f"dict (or None)"
            )
        else:
            payload: Any = result
            try:
                # Dict keys follow json.dumps at every level (a top-level
                # int key is treated like a nested one; None -> "null").
                payload = _normalize_result(result, "payload", set())
                response = Response(envelope=response_envelope, payload=payload)
                # Serialize once here, exactly as the reply publish does,
                # so a value with no JSON form (an arbitrary object, ...)
                # is reported as the handler's result problem instead of
                # surfacing later as a failed reply publish.
                dumped = response.model_dump(mode="json")
                body = json.dumps(dumped)
            except _ResultProblem as e:
                problem = f"returned a dict that is not JSON-serializable ({e})"
                response = None
            except RecursionError:
                problem = (
                    "returned a dict that is not JSON-serializable (Circular "
                    "reference detected (depth exceeded))"
                )
                response = None
            except (
                Exception
            ) as e:  # ValidationError, PydanticSerializationError, TypeError, ValueError
                where = None
                try:
                    if isinstance(e, UnicodeEncodeError):
                        # A nested dict key with a lone surrogate: the
                        # serializer cannot encode it.
                        where = _find_lone_surrogate(payload, "payload")
                    if where is None:
                        where = _locate_unserializable(payload)
                except Exception:  # pragma: no cover - never fail the reply
                    where = None
                if where is None:
                    text = _safe_str(e)
                    detail = text.splitlines()[0] if text else type(e).__name__
                    # Interpreter advice (sys.set_int_max_str_digits) is
                    # not for the caller.
                    detail = detail.split("; use sys.")[0]
                    where = detail[:200]
                problem = f"returned a dict that is not JSON-serializable ({where})"
                response = None
            else:
                size = len(body)  # json.dumps output is ASCII: chars == bytes
                # Text with a lone surrogate (a "\ud800" escape a JSON
                # request can carry) is written by json.dumps as an escape
                # that strict JSON readers refuse (.NET throws when the
                # string is read), and in a top-level key the serializer
                # silently replaces it with U+FFFD: refused like any other
                # value with no JSON form. Only a body holding "\ud" (an
                # escaped surrogate) or "\ufffd" can come from one. The
                # keys are looked for in the result as returned; the
                # values in the JSON form just built, which also holds
                # what the walk does not look into (set items, a pydantic
                # model's fields).
                where = (
                    _find_lone_surrogate(payload, "payload")
                    or _find_lone_surrogate(dumped["payload"], "payload")
                    if "\\ud" in body or "\\ufffd" in body
                    else None
                )
                if where is not None:
                    problem = f"returned a dict that is not JSON-serializable ({where})"
                    response = None
                # Only a reply that will be published is size-checked: a
                # command without reply_to (fire-and-forget) sends none, so
                # a large result there is not an error.
                elif envelope.reply_to and self.max_reply_bytes and size > self.max_reply_bytes:
                    problem = (
                        f"returned a result too large to send as a reply "
                        f"({size} bytes serialized; the limit is "
                        f"{self.max_reply_bytes} bytes, "
                        f"KBM_LEDSAS_MAX_PAYLOAD_BYTES). Store a large result "
                        f"in blob storage and return its reference"
                    )
                    response = None
        if response is not None:
            # The JSON form just checked becomes the payload (already
            # JSON-native, so serializing it again gives the same body).
            return HandlerOutcome(
                Response.model_construct(envelope=response.envelope, payload=dumped["payload"])
            )

        logger.error(
            "Handler returned an invalid result (not retried): %s",
            problem,
            extra={
                "correlation_id": envelope.correlation_id,
                "command_name": envelope.name,
            },
        )
        specific_msg = f"Handler for {envelope.name} {problem}"
        return self._error_outcome(
            envelope,
            {
                "code": "Permanent",
                "message": self._get_error_message("Permanent", specific_msg),
                "retryable": False,
            },
        )

    def _build_response_envelope(self, command_envelope: Envelope) -> Envelope:
        """
        Build response envelope from command envelope.

        Copies metadata from command envelope and sets type to "response".
        Preserves job_id for business-level tracking.
        Note: Deadline is not copied to response (deadlines apply to commands).

        Args:
            command_envelope: Original command envelope

        Returns:
            Response envelope
        """
        return Envelope(
            schema_version=command_envelope.schema_version,
            type="response",
            name=command_envelope.name,
            message_version=command_envelope.message_version,
            message_id=command_envelope.message_id,
            correlation_id=command_envelope.correlation_id,
            idempotency_key=command_envelope.idempotency_key,
            sent_at=datetime.now(UTC),
            deadline=None,  # Deadlines apply to commands, not responses
            trace_id=command_envelope.trace_id,
            reply_to=command_envelope.reply_to,
            priority=command_envelope.priority,
            job_id=command_envelope.job_id,
        )

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"HandlerRegistry(handlers={len(self._handlers)})"
