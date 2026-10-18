"""
Envelope model for LEDSAS messages.

The envelope is a fixed wrapper around all message types (command, response, status, error)
containing metadata for routing, correlation, tracing, and deadlines.
"""

import copy as _copy
import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

# The start of an ISO 8601 date and time ("2026-10-15T10:00"; a space is
# accepted between date and time, as RFC 3339 allows).
_ISO_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}")


_M = TypeVar("_M", bound="_ValidatedCopyModel")


class _ValidatedCopyModel(BaseModel):
    """A model whose copies are checked like a new one: pydantic's
    ``model_copy(update=...)`` (and ``copy.replace()`` and the deprecated
    ``copy(update=...)``, which use it) would set the updated fields
    without running the validators, so an envelope that no constructor
    accepts could be built and only be refused on the wire."""

    def model_copy(self: _M, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> _M:
        """A copy of this message; the fields in ``update`` replace the
        copied ones and the result is validated as a new message is
        (``pydantic.ValidationError`` when it is not a valid one).

        As with pydantic's own ``model_copy``, the copy keeps which fields
        were set (``model_fields_set``, so ``model_dump(exclude_unset=True)``
        gives the same keys), plus the fields named in ``update``."""
        data = {name: getattr(self, name) for name in type(self).model_fields}
        if deep:
            data = _copy.deepcopy(data)
        update = dict(update) if update else {}
        data.update(update)
        copied = type(self).model_validate(data)
        fields_set = set(self.model_fields_set) | (set(update) & set(type(self).model_fields))
        object.__setattr__(copied, "__pydantic_fields_set__", fields_set)
        return copied

    def copy(
        self: _M,
        *,
        include: Any = None,
        exclude: Any = None,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> _M:
        """Deprecated pydantic alias of :meth:`model_copy` (validated too;
        ``include`` / ``exclude`` are not supported)."""
        if include is not None or exclude is not None:
            raise TypeError(
                f"{type(self).__name__}.copy() takes no include / exclude; use model_copy()"
            )
        return self.model_copy(update=update, deep=deep)


class Envelope(_ValidatedCopyModel):
    """
    Fixed envelope for all LEDSAS messages.

    The envelope wraps all message types and provides:
    - Message metadata (ID, correlation, timestamps)
    - Routing information (reply_to, priority)
    - Distributed tracing (trace_id)
    - Deadline enforcement
    - Schema versioning

    All fields are required unless marked optional.
    """

    schema_version: str = Field(
        ...,
        description="Envelope schema version (not payload version)",
        pattern=r"^[0-9]{1,4}\.[0-9]{1,4}$",
        max_length=9,
    )

    # Drop the unused ``"error"`` value. The SDK
    # emits only command / response / status; an ``"error"`` envelope was
    # never produced and is rejected on the command exchange by the envelope-type guard.
    type: Literal["command", "response", "status"] = Field(..., description="Message category")

    name: str = Field(
        ...,
        description="Message name in PascalCase (e.g., ProcessDataset, ValidateImage)",
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z][A-Za-z0-9_]*$",
    )

    message_version: str = Field(
        ...,
        description="Version of the payload schema for this message name",
        pattern=r"^[0-9]{1,4}\.[0-9]{1,4}$",
        max_length=9,
    )

    # Accept mixed-case hex in UUID fields. RFC 4122
    # §3 specifies lowercase as the canonical *output* form but says inputs
    # are case-insensitive — and producers in other ecosystems (.NET,
    # operator-pasted IDs from a UI) routinely emit uppercase.
    # ``message_id`` is normalized to lowercase (the service keys its
    # in-flight commands on it). ``correlation_id`` is kept exactly as the
    # caller sent it, because replies and status updates echo it and the
    # caller matches them against the value it sent.
    message_id: str = Field(
        ...,
        description="Unique message identifier (UUID v4 format, case-insensitive)",
        pattern=r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    )

    correlation_id: str = Field(
        ...,
        description=(
            "Correlation ID linking command to responses/status (UUID v4 "
            "format, either letter case; echoed in replies exactly as sent)"
        ),
        pattern=r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    )

    idempotency_key: str = Field(
        ...,
        description=(
            "Idempotency key for replay safety. Must be URL-safe: "
            "alphanumerics, dot, underscore, hyphen, colon. Length "
            "1..128. Handlers commonly interpolate this key into blob "
            "paths (the idempotency-key replay pattern), so the "
            "character set is restricted to avoid path injection."
        ),
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9_\-:.]+$",
    )

    sent_at: datetime = Field(
        ...,
        description=(
            "When the message was published: an ISO 8601 / RFC 3339 "
            "timestamp string with a timezone (a number such as a Unix "
            "time is refused)"
        ),
    )

    deadline: datetime | None = Field(
        None,
        description=(
            "Handler should finish before this time: an ISO 8601 / RFC "
            "3339 timestamp string with a timezone"
        ),
    )

    trace_id: str = Field(
        ...,
        description=(
            "W3C traceparent for distributed tracing. Must be URL-safe: "
            "alphanumerics, dot, underscore, hyphen, colon. Length 1..255. "
            "Same character-set restriction as idempotency_key — handlers "
            "and downstream tooling commonly interpolate trace_id into "
            "paths, log lines, and search queries, so the SDK constrains "
            "the field to close the path-traversal / control-char / "
            "log-injection footgun."
        ),
        min_length=1,
        max_length=255,
        pattern=r"^[A-Za-z0-9_\-:.]+$",
    )

    reply_to: str = Field(
        default="",
        description=(
            "Exchange to send response/status to. Must be URL-safe — "
            "alphanumerics, dot, underscore, hyphen, colon — and at most "
            "127 chars. The SDK logs this value on every reply-publish "
            "(success and failure), so the character set is locked down "
            "to the same shape as trace_id / idempotency_key / job_id to "
            "close the log-injection footgun. The 127-char cap matches "
            "AMQP's protocol-level limit on exchange-name length; a "
            "longer name would be accepted by the schema but later "
            "rejected by the broker with 'Max length exceeded for "
            "exchange' AFTER the handler had already run. A name "
            "starting with 'cmd.' or 'dlq.' (a service's command or "
            "dead-letter exchange) is refused. Empty string (or "
            "JSON null, read as empty) is the explicit fire-and-forget "
            "value (no response sent)."
        ),
        max_length=127,
        pattern=r"^([A-Za-z0-9_\-:.]+)?$",
    )

    # strict: an integer only ("5", 5.0 and true are refused instead of
    # being converted).
    priority: int | None = Field(
        None,
        description="Message priority (an integer 0-9, higher is more urgent)",
        ge=0,
        le=9,
        strict=True,
    )

    job_id: str | None = Field(
        None,
        description=(
            "Optional orchestrator-controlled job identifier for "
            "business-level tracking. Unlike correlation_id "
            "(SDK-controlled), job_id is set by the orchestrator and "
            "echoed in all responses for workflow tracking. Same "
            "URL-safe character set as idempotency_key / trace_id "
            "(`^[A-Za-z0-9_\\-:.]+$`, length 1..128) so handlers can "
            "safely interpolate it into paths."
        ),
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9_\-:.]+$",
    )

    @field_validator("message_id", mode="before")
    @classmethod
    def _normalize_uuid_case(cls, v):
        """Lowercase ``message_id``.

        The accepted pattern is case-insensitive hex (RFC 4122 allows
        either case on input); the service keys its in-flight commands on
        the canonical lowercase form. ``correlation_id`` is not changed:
        it is echoed in replies exactly as the caller sent it.
        """
        if isinstance(v, str):
            return v.lower()
        return v

    @field_validator("reply_to", mode="before")
    @classmethod
    def _null_reply_to_is_fire_and_forget(cls, v: Any) -> Any:
        """Read a JSON ``null`` ``reply_to`` as ``""`` (fire-and-forget).

        The other optional envelope fields (``deadline``, ``priority``,
        ``job_id``) accept ``null``, and a caller whose serializer writes
        ``null`` for an absent optional field must get the same outcome as
        omitting it, not a dead-lettered command its handler never ran.
        """
        return "" if v is None else v

    @field_validator("reply_to")
    @classmethod
    def _reply_to_is_not_a_service_exchange(cls, v: str) -> str:
        """Refuse a reply_to that names a LEDSAS service exchange.

        Service command exchanges are ``cmd.[<tenant>.]<service>.v1`` and
        dead-letter exchanges ``dlq.[<tenant>.]<service>.v1``. A reply
        published there would be injected into a service's command queue
        or dead-letter queue (this service's own or another's), so such a
        command is refused like any other invalid envelope: dead-lettered
        before its handler runs. A reply exchange is the caller's own.

        The broker's reserved exchanges (``amq.*``: ``amq.topic``,
        ``amq.fanout``, ...) are refused too: anything bound to them would
        receive the result.
        """
        if v.startswith(("cmd.", "dlq.", "amq.")):
            raise ValueError(
                "reply_to must name the caller's own reply exchange, not a "
                "service command or dead-letter exchange (cmd.* / dlq.*) "
                "or a reserved broker exchange (amq.*)"
            )
        return v

    @field_validator("sent_at", "deadline", mode="before")
    @classmethod
    def _timestamp_text(cls, v: Any) -> Any:
        """Accept a timestamp as ISO 8601 text (or a datetime object built
        in Python), never as a number: without this, a Unix time (0,
        1760000000) or a numeric string ("0") would be read as a date."""
        if v is None or isinstance(v, datetime):
            return v
        if isinstance(v, str) and _ISO_TIMESTAMP.match(v):
            return v
        raise ValueError(
            "must be an ISO 8601 timestamp string with a timezone, e.g. " "2026-10-15T10:00:00Z"
        )

    @field_validator("sent_at", "deadline")
    @classmethod
    def validate_timezone_aware(cls, v: datetime | None) -> datetime | None:
        """Ensure datetime fields are timezone-aware (preferably UTC)."""
        if v is not None and v.tzinfo is None:
            raise ValueError("Datetime must be timezone-aware (use datetime.timezone.utc)")
        return v

    @field_validator("deadline")
    @classmethod
    def validate_deadline_after_sent_at(cls, v: datetime | None, info) -> datetime | None:
        """Ensure deadline is after sent_at if both are present."""
        if v is not None and info.data.get("sent_at") is not None:
            sent_at = info.data["sent_at"]
            if v <= sent_at:
                raise ValueError(f"Deadline ({v}) must be after sent_at ({sent_at})")
        return v

    # Frozen: a handler that assigns an envelope field (ctx.envelope.
    # message_id = ...) gets a ValidationError instead of silently
    # changing the ids the SDK acknowledges and replies with. Build a
    # changed copy with model_copy(update=...), which validates it.
    model_config = ConfigDict(
        validate_assignment=True,
        frozen=True,
    )

    @field_serializer("sent_at", "deadline", when_used="json")
    def _serialize_datetime(self, v: datetime | None) -> str | None:
        return v.isoformat() if v is not None else None

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        job_part = f", job_id={self.job_id!r}" if self.job_id else ""
        return (
            f"Envelope(type={self.type!r}, name={self.name!r}, "
            f"message_id={self.message_id!r}, correlation_id={self.correlation_id!r}{job_part})"
        )
