"""Tolerant decoding of AMQP short strings and table keys.

The AMQP frame decoder the client library uses (``pamqp``) decodes every
short string of a frame (a message's ``message_id``, ``correlation_id``,
``reply_to``, ``type`` ... properties, and the keys of its ``headers``
table) as strict UTF-8. A sender that puts bytes that are not UTF-8 in
one of them (``message_id=b"\\xff\\xfe"``) makes the decoder raise on the
frame, the client drops the connection, and the broker delivers the same
message again on the next connection: consumption stalls on that one
message for good.

:func:`install` replaces those two decoders with versions that, only when
strict UTF-8 fails, decode with ``surrogateescape`` (each byte that is not
UTF-8 becomes one lone surrogate, ``\\udcff``). The frame then decodes,
and the consumer dead-letters the message once, naming the property (see
:func:`undecodable_properties`). Valid UTF-8 decodes exactly as before.

Works with pamqp 3.x and 4.x, which look the decoders up at call time in
``pamqp.decode.METHODS`` (frame fields) and ``pamqp.decode.TABLE_MAPPING``
(nested tables). Without pamqp installed, :func:`install` does nothing.

The decoders are process-wide: the SDK installs them when a service's
transport starts (``DirectTransport.start``), not when the package is
imported, so importing ``kbm_ledsas_sdk`` leaves them as they are.
"""

from __future__ import annotations

import struct
from collections.abc import Callable
from typing import Any

# The AMQP basic-class properties that are short strings.
SHORT_STRING_PROPERTIES = (
    "message_id",
    "correlation_id",
    "reply_to",
    "content_type",
    "content_encoding",
    "type",
    "user_id",
    "app_id",
    "expiration",
    "cluster_id",
)

_installed = False


def _has_lone_surrogate(text: str) -> bool:
    return any(0xD800 <= ord(c) <= 0xDFFF for c in text)


def shown(text: str) -> str:
    """``text`` as it can be logged: the bytes a tolerant decode kept as
    lone surrogates are written as ``\\xNN`` escapes."""
    if not _has_lone_surrogate(text):
        return text
    raw = text.encode("utf-8", "surrogateescape")
    return raw.decode("utf-8", "backslashreplace")


def undecodable_properties(message: Any) -> list[str]:
    """The names of the short-string properties of ``message`` (and
    ``headers[<key>]`` for its header keys) whose bytes were not UTF-8
    (the tolerant decode kept them as lone surrogates); empty for an
    ordinary message."""
    found = []
    for name in SHORT_STRING_PROPERTIES:
        value = getattr(message, name, None)
        if isinstance(value, str) and _has_lone_surrogate(value):
            found.append(name)
    headers = getattr(message, "headers", None)
    if isinstance(headers, dict):
        for key in headers:
            if isinstance(key, str) and _has_lone_surrogate(key):
                found.append(f"headers[{shown(key)}]")
    return found


def _tolerant_short_str(
    original: Callable[[bytes], tuple[int, str]],
) -> Callable[[bytes], tuple[int, str]]:
    def short_str(value: bytes) -> tuple[int, str]:
        try:
            return original(value)
        except UnicodeDecodeError:
            length = value[0]
            return length + 1, bytes(value[1 : length + 1]).decode("utf-8", "surrogateescape")

    short_str.__wrapped__ = original  # type: ignore[attr-defined]
    return short_str


def _tolerant_field_table(
    original: Callable[[bytes], tuple[int, Any]], decode: Any
) -> Callable[[bytes], tuple[int, Any]]:
    def field_table(value: bytes) -> tuple[int, Any]:
        try:
            return original(value)
        except UnicodeDecodeError:
            pass
        try:
            (length,) = struct.unpack(">I", value[0:4])
            offset = 4
            data: dict[str, Any] = {}
            end = offset + length
            while offset < end:
                key_length = value[offset]
                offset += 1
                key = bytes(value[offset : offset + key_length]).decode("utf-8", "surrogateescape")
                offset += key_length
                consumed, result = decode.embedded_value(value[offset:])
                offset += consumed
                data[key] = result
            return end, data
        except (TypeError, IndexError, struct.error) as err:
            raise ValueError("Could not unpack data") from err

    field_table.__wrapped__ = original  # type: ignore[attr-defined]
    return field_table


def install() -> bool:
    """Install the tolerant decoders (once per process). Returns True when
    they are installed (now or before), False without pamqp."""
    global _installed
    if _installed:
        return True
    try:
        from pamqp import decode
    except ImportError:
        return False
    methods = getattr(decode, "METHODS", None)
    mapping = getattr(decode, "TABLE_MAPPING", None)
    if not isinstance(methods, dict) or not isinstance(mapping, dict):
        return False
    original_short = methods.get("shortstr", getattr(decode, "short_str", None))
    original_table = methods.get("table", getattr(decode, "field_table", None))
    if original_short is None or original_table is None:
        return False
    short_str = _tolerant_short_str(original_short)
    field_table = _tolerant_field_table(original_table, decode)
    methods["shortstr"] = short_str
    methods["table"] = field_table
    for key, function in list(mapping.items()):
        if function is original_table:
            mapping[key] = field_table
    decode.short_str = short_str
    decode.field_table = field_table
    _installed = True
    return True
